/**
 * Unified Grain-Flour Inventory Sync Service
 *
 * Square is the absolute source of truth for grain inventory.
 * The 1 LB per-lb count in Square = total pounds available.
 *
 * This service:
 *   1. Listens for BC order.created webhooks → deducts lbs from Square
 *   2. Listens for Square inventory.count.updated webhooks → recalculates BC
 *   3. Runs a 15-minute reconciliation poll as a safety net
 *
 * Inventory calculation from total_lbs:
 *   - Grain 1 LB variant  = available_lbs
 *   - Grain 5 LB variant  = floor(available_lbs / 5)
 *   - Grain 10 LB variant = floor(available_lbs / 10)
 *   - Grain 25 LB variant = floor(available_lbs / 25)
 *   - Flour 1 LB variant  = available_lbs  (shares same pool)
 *   - Flour 5 LB variant  = floor(available_lbs / 5)
 *   - Flour 10 LB variant = floor(available_lbs / 10)
 *
 * "available_lbs" = Square total - reserved (pending/unshipped BC orders)
 */

const express = require('express');
const fetch = require('node-fetch');
const grainMapping = require('./grain-mapping.json');

const app = express();
app.use(express.json());

// ── Config ──────────────────────────────────────────────────────────────────
const BC_STORE_HASH   = process.env.BC_STORE_HASH || 'h1uvrm9fjd';
const BC_ACCESS_TOKEN = process.env.BC_ACCESS_TOKEN;
const SQ_ACCESS_TOKEN = process.env.SQ_ACCESS_TOKEN;
const SQ_LOCATION_ID  = process.env.SQ_LOCATION_ID || 'D7QJPMPVZME4K';
const PORT            = process.env.PORT || 3000;
const RECONCILE_MINS  = parseInt(process.env.RECONCILE_MINS || '15', 10);

const BC_API = `https://api.bigcommerce.com/stores/${BC_STORE_HASH}`;
const SQ_API = 'https://connect.squareup.com/v2';

// ── Reverse lookups (populated at startup) ──────────────────────────────────
// Map Square variation_id → grain base SKU (for Square webhook handler)
const sqVariationToGrain = {};
// Map BC variant_id → { grainSku, lbs, type:'grain'|'flour' }
const bcVariantToGrain = {};
// Map grain SKU → flour SKU prefix (for matching flour variants to grain)
const flourSkuToGrain = {};

// Build static lookups from grain-mapping.json (grain + Square data)
for (const [grainSku, entry] of Object.entries(grainMapping)) {
  sqVariationToGrain[entry.square_variation_id] = grainSku;

  for (const [lbs, vData] of Object.entries(entry.bc_grain.variants)) {
    bcVariantToGrain[vData.variant_id] = { grainSku, lbs: parseInt(lbs, 10), type: 'grain' };
  }

  // Build flour SKU → grain mapping for auto-discovery
  // Flour SKUs follow pattern: FM-{grainSku} or FM-{grainSku}-{lbs}
  if (entry.bc_flour && entry.bc_flour.variants) {
    for (const [lbs, vData] of Object.entries(entry.bc_flour.variants)) {
      if (vData.sku) {
        flourSkuToGrain[vData.sku] = { grainSku, lbs: parseInt(lbs, 10) };
      }
    }
  }
}

/**
 * Auto-discover flour product/variant IDs from BigCommerce at startup.
 * Queries category 557 (Freshly Milled Flour), matches FM- SKUs to grain mapping.
 * Updates grainMapping and bcVariantToGrain in-place.
 */
async function discoverFlourVariants() {
  console.log('  Discovering flour variant IDs from BigCommerce...');
  let discovered = 0;

  try {
    let page = 1;
    while (true) {
      const data = await bcGet(`/v3/catalog/products?categories:in=557&include=variants&limit=50&page=${page}`);
      for (const product of data.data) {
        for (const variant of (product.variants || [])) {
          const sku = variant.sku;
          if (!sku || !sku.startsWith('FM-')) continue;

          const lookup = flourSkuToGrain[sku];
          if (!lookup) {
            console.warn(`    Unknown flour SKU: ${sku}`);
            continue;
          }

          const { grainSku, lbs } = lookup;
          const entry = grainMapping[grainSku];
          if (!entry || !entry.bc_flour || !entry.bc_flour.variants) continue;

          const lbsStr = String(lbs);
          if (entry.bc_flour.variants[lbsStr]) {
            entry.bc_flour.variants[lbsStr].bc_product_id = product.id;
            entry.bc_flour.variants[lbsStr].bc_variant_id = variant.id;
          }

          // Register in reverse lookup
          bcVariantToGrain[variant.id] = { grainSku, lbs, type: 'flour' };
          discovered++;
        }
      }

      if (!data.meta.pagination.links.next) break;
      page++;
    }

    console.log(`  ✓ Discovered ${discovered} flour variants from BigCommerce`);
  } catch (e) {
    console.error(`  ✗ Flour discovery failed: ${e.message}`);
    console.error('    Flour inventory sync will be limited until IDs are available.');
  }
}

// ── Helpers: BigCommerce ────────────────────────────────────────────────────
function bcHeaders() {
  return {
    'X-Auth-Token': BC_ACCESS_TOKEN,
    'Content-Type': 'application/json',
    'Accept': 'application/json'
  };
}

async function bcGet(path) {
  const res = await fetch(`${BC_API}${path}`, { headers: bcHeaders() });
  if (!res.ok) throw new Error(`BC GET ${path}: ${res.status}`);
  return res.json();
}

async function bcPut(path, body) {
  const res = await fetch(`${BC_API}${path}`, {
    method: 'PUT', headers: bcHeaders(), body: JSON.stringify(body)
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`BC PUT ${path}: ${res.status} - ${text}`);
  }
  return res.json();
}

/** Fetch all pending/unshipped BC orders and sum reserved lbs per grain SKU */
async function getReservedLbs() {
  const reserved = {};  // grainSku → total lbs reserved

  // status_id 1=Pending, 9=Awaiting Shipment, 11=Awaiting Fulfillment, 12=Manual Verification Required
  const statuses = [1, 9, 11, 12];

  for (const statusId of statuses) {
    let page = 1;
    while (true) {
      let orders;
      try {
        orders = await bcGet(`/v2/orders?status_id=${statusId}&page=${page}&limit=50`);
      } catch (e) {
        // 204 No Content = no orders with this status
        break;
      }
      if (!orders || !Array.isArray(orders) || orders.length === 0) break;

      for (const order of orders) {
        let products;
        try {
          products = await bcGet(`/v2/orders/${order.id}/products`);
        } catch (e) {
          console.warn(`  Could not fetch products for order #${order.id}: ${e.message}`);
          continue;
        }

        for (const item of products) {
          const vid = item.variant_id;
          const lookup = bcVariantToGrain[vid];
          if (!lookup) continue;

          const lbsPerUnit = lookup.lbs;
          const qty = item.quantity;
          const totalLbs = lbsPerUnit * qty;

          if (!reserved[lookup.grainSku]) reserved[lookup.grainSku] = 0;
          reserved[lookup.grainSku] += totalLbs;
        }
      }

      if (orders.length < 50) break;
      page++;
    }
  }

  return reserved;
}

/** Update a single BC variant's inventory_level */
async function setBcVariantStock(productId, variantId, level) {
  await bcPut(`/v3/catalog/products/${productId}/variants/${variantId}`, {
    inventory_level: Math.max(0, level)
  });
}

// ── Helpers: Square ─────────────────────────────────────────────────────────
function sqHeaders() {
  return {
    'Square-Version': '2025-01-23',
    'Authorization': `Bearer ${SQ_ACCESS_TOKEN}`,
    'Content-Type': 'application/json'
  };
}

/** Get current inventory count for a Square variation (per-lb = total pounds) */
async function getSquareCount(variationId) {
  const MAX_RETRIES = 4;
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    const res = await fetch(`${SQ_API}/inventory/batch-retrieve-counts`, {
      method: 'POST',
      headers: sqHeaders(),
      body: JSON.stringify({
        catalog_object_ids: [variationId],
        location_ids: [SQ_LOCATION_ID],
        states: ['IN_STOCK']
      })
    });
    if (!res.ok) {
      const text = await res.text();
      throw new Error(`Square batch-retrieve-counts: ${res.status} - ${text}`);
    }
    const data = await res.json();
    if (!data.counts || data.counts.length === 0) {
      // Empty response — could be rate-limiting during bulk reconciliation.
      // Retry with backoff instead of immediately returning 0.
      const delay = Math.min(1000 * Math.pow(2, attempt - 1), 8000);
      console.warn(`getSquareCount: attempt ${attempt}/${MAX_RETRIES} empty counts for ${variationId}, retrying in ${delay}ms`);
      if (attempt < MAX_RETRIES) {
        await new Promise(r => setTimeout(r, delay));
        continue;
      }
      return 0;
    }

    // Validate response is for the requested variation (Square API sometimes
    // returns ALL counts instead of filtering by catalog_object_ids)
    const matchedCount = data.counts.find(c => c.catalog_object_id === variationId);
    if (matchedCount) {
      return Math.floor(parseFloat(matchedCount.quantity)) || 0;
    }

    // API returned unfiltered results — page through them to find our item
    if (data.cursor) {
      console.warn(`getSquareCount: attempt ${attempt} got ${data.counts.length} unfiltered counts for ${variationId}, paging through...`);
      let cursor = data.cursor;
      let pageNum = 2;
      while (cursor && pageNum <= 20) { // safety cap at 20 pages
        const pageRes = await fetch(`${SQ_API}/inventory/batch-retrieve-counts`, {
          method: 'POST',
          headers: sqHeaders(),
          body: JSON.stringify({
            catalog_object_ids: [variationId],
            location_ids: [SQ_LOCATION_ID],
            states: ['IN_STOCK'],
            cursor
          })
        });
        if (!pageRes.ok) break;
        const pageData = await pageRes.json();
        if (pageData.counts) {
          const found = pageData.counts.find(c => c.catalog_object_id === variationId);
          if (found) {
            console.log(`getSquareCount: found ${variationId} on page ${pageNum} with qty ${found.quantity}`);
            return Math.floor(parseFloat(found.quantity)) || 0;
          }
        }
        cursor = pageData.cursor;
        pageNum++;
      }
    }

    // Mismatch — API returned wrong data, retry with exponential backoff
    const delay = Math.min(1000 * Math.pow(2, attempt - 1), 8000); // 1s, 2s, 4s, 8s
    console.warn(`getSquareCount: attempt ${attempt}/${MAX_RETRIES} mismatch for ${variationId}, got ${data.counts.length} unrelated counts, retrying in ${delay}ms`);
    if (attempt < MAX_RETRIES) {
      await new Promise(r => setTimeout(r, delay));
    }
  }
  // All batch retries returned mismatched data — fall back to single-item GET endpoint.
  // This uses a completely different API path that doesn't share the batch endpoint's
  // caching/filtering issues during bulk reconciliation.
  console.warn(`getSquareCount: batch retries exhausted for ${variationId}, trying single-item GET fallback...`);
  try {
    await new Promise(r => setTimeout(r, 2000)); // brief cooldown before fallback
    const singleRes = await fetch(
      `${SQ_API}/inventory/${variationId}?location_ids=${SQ_LOCATION_ID}`,
      { method: 'GET', headers: sqHeaders() }
    );
    if (singleRes.ok) {
      const singleData = await singleRes.json();
      if (singleData.counts && singleData.counts.length > 0) {
        const match = singleData.counts.find(c =>
          c.catalog_object_id === variationId && c.state === 'IN_STOCK'
        );
        if (match) {
          const qty = Math.floor(parseFloat(match.quantity)) || 0;
          console.log(`getSquareCount: single-item GET fallback succeeded for ${variationId}, qty=${qty}`);
          return qty;
        }
      }
      console.warn(`getSquareCount: single-item GET returned no IN_STOCK count for ${variationId}`);
    } else {
      const errText = await singleRes.text();
      console.error(`getSquareCount: single-item GET failed ${singleRes.status} for ${variationId}: ${errText}`);
    }
  } catch (fallbackErr) {
    console.error(`getSquareCount: single-item GET exception for ${variationId}: ${fallbackErr.message}`);
  }
  console.error(`getSquareCount: all methods failed for ${variationId}, returning 0`);
  return 0;
}

/** Adjust Square inventory (negative = deduct) */
async function adjustSquareInventory(variationId, adjustment) {
  const idempotencyKey = `adj-${variationId}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

  const change = {
    type: 'ADJUSTMENT',
    adjustment: {
      catalog_object_id: variationId,
      location_id: SQ_LOCATION_ID,
      quantity: String(adjustment),
      from_state: adjustment < 0 ? 'IN_STOCK' : 'NONE',
      to_state: adjustment < 0 ? 'NONE' : 'IN_STOCK',
      occurred_at: new Date().toISOString()
    }
  };

  const res = await fetch(`${SQ_API}/inventory/changes/batch-create`, {
    method: 'POST',
    headers: sqHeaders(),
    body: JSON.stringify({
      idempotency_key: idempotencyKey,
      changes: [change]
    })
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Square batch-create changes: ${res.status} - ${text}`);
  }
  return res.json();
}

// ── Core: Recalculate all BC variants from Square truth ─────────────────────
async function recalculateGrain(grainSku, options = {}) {
  const entry = grainMapping[grainSku];
  if (!entry) {
    console.warn(`recalculateGrain: unknown grain SKU "${grainSku}"`);
    return;
  }

  // 1. Get total lbs from Square
  const totalLbs = await getSquareCount(entry.square_variation_id);

  // 2. Get reserved lbs from pending BC orders (optionally passed in to avoid refetching)
  const reservedMap = options.reservedMap || await getReservedLbs();
  const reservedLbs = reservedMap[grainSku] || 0;

  const availableLbs = Math.max(0, totalLbs - reservedLbs);

  console.log(`  [${entry.name}] Square=${totalLbs} lbs, reserved=${reservedLbs}, available=${availableLbs}`);

  // 3. Calculate and set BC grain variants
  const grainLevels = { '1': availableLbs, '5': Math.floor(availableLbs / 5), '10': Math.floor(availableLbs / 10), '25': Math.floor(availableLbs / 25) };

  for (const [lbs, level] of Object.entries(grainLevels)) {
    const vData = entry.bc_grain.variants[lbs];
    if (!vData) continue;
    try {
      await setBcVariantStock(entry.bc_grain.product_id, vData.variant_id, level);
    } catch (e) {
      console.error(`    ERROR setting grain ${lbs}LB (vid ${vData.variant_id}): ${e.message}`);
    }
  }

  // 4. Calculate and set BC flour variants (only 1, 5, 10)
  if (entry.bc_flour && Object.keys(entry.bc_flour.variants).length > 0) {
    const flourLevels = { '1': availableLbs, '5': Math.floor(availableLbs / 5), '10': Math.floor(availableLbs / 10) };

    for (const [lbs, level] of Object.entries(flourLevels)) {
      const vData = entry.bc_flour.variants[lbs];
      if (!vData) continue;
      try {
        await setBcVariantStock(vData.bc_product_id, vData.bc_variant_id, level);
      } catch (e) {
        console.error(`    ERROR setting flour ${lbs}LB (vid ${vData.bc_variant_id}): ${e.message}`);
      }
    }
  }

  console.log(`  [${entry.name}] BC updated: grain=${JSON.stringify(grainLevels)}, flour 1/5/10=${availableLbs}/${Math.floor(availableLbs / 5)}/${Math.floor(availableLbs / 10)}`);
}

// ── Handler 1: BC order.created webhook ─────────────────────────────────────
app.post('/webhooks/order-created', async (req, res) => {
  res.status(200).json({ received: true });

  try {
    const orderId = req.body?.data?.id;
    if (!orderId) return;

    console.log(`\n=== BC Order #${orderId} created ===`);

    const products = await bcGet(`/v2/orders/${orderId}/products`);
    const affectedGrains = new Set();

    for (const item of products) {
      const vid = item.variant_id;
      const lookup = bcVariantToGrain[vid];
      if (!lookup) continue;

      const lbsToDeduct = lookup.lbs * item.quantity;
      const entry = grainMapping[lookup.grainSku];

      console.log(`  ${lookup.type} SKU ${item.sku} x${item.quantity} = ${lbsToDeduct} lbs → deduct from Square`);

      try {
        await adjustSquareInventory(entry.square_variation_id, -lbsToDeduct);
        console.log(`  → Square deducted ${lbsToDeduct} lbs from ${entry.name}`);
        affectedGrains.add(lookup.grainSku);
      } catch (e) {
        console.error(`  ERROR deducting from Square for ${item.sku}: ${e.message}`);
      }
    }

    if (affectedGrains.size === 0) {
      console.log(`  No grain/flour items in order, skipping recalculation`);
      return;
    }

    // Recalculate all affected grains
    console.log(`  Recalculating ${affectedGrains.size} grain(s)...`);
    const reservedMap = await getReservedLbs();
    for (const grainSku of affectedGrains) {
      await recalculateGrain(grainSku, { reservedMap });
    }

    console.log(`=== Order #${orderId} processing complete ===\n`);
  } catch (e) {
    console.error(`Error processing BC order webhook: ${e.message}`);
  }
});

// ── Handler 2: Square inventory webhook ─────────────────────────────────────
app.post('/webhooks/square-inventory', async (req, res) => {
  res.status(200).json({ received: true });

  try {
    const payload = req.body;
    // Square inventory webhook sends entity_id = catalog_object_id (variation ID)
    const entityId = payload?.data?.id || payload?.data?.object?.inventory_counts?.[0]?.catalog_object_id;

    if (!entityId) {
      console.log('Square inventory webhook: no entity ID found');
      return;
    }

    const grainSku = sqVariationToGrain[entityId];
    if (!grainSku) {
      console.log(`Square inventory webhook: variation ${entityId} not in our mapping, skipping`);
      return;
    }

    console.log(`\n=== Square inventory changed for ${grainMapping[grainSku].name} ===`);

    // Small delay to let Square settle
    await new Promise(r => setTimeout(r, 2000));

    await recalculateGrain(grainSku);

    console.log(`=== Square inventory sync complete ===\n`);
  } catch (e) {
    console.error(`Error processing Square inventory webhook: ${e.message}`);
  }
});

// ── Handler 3: Full reconciliation (called by cron or manually) ─────────────
async function fullReconciliation() {
  console.log(`\n╔══════════════════════════════════════════╗`);
  console.log(`║   FULL RECONCILIATION - ${new Date().toISOString()}   ║`);
  console.log(`╚══════════════════════════════════════════╝`);

  try {
    // Get all reserved lbs once (shared across all grains)
    const reservedMap = await getReservedLbs();
    console.log(`Reserved lbs from pending orders:`, reservedMap);

    let processed = 0;
    let errors = 0;

    for (const grainSku of Object.keys(grainMapping)) {
      try {
        await recalculateGrain(grainSku, { reservedMap });
        processed++;
      } catch (e) {
        console.error(`  ERROR reconciling ${grainSku}: ${e.message}`);
        errors++;
      }

      // Rate limit: pause between grains to avoid Square API returning unfiltered results
      await new Promise(r => setTimeout(r, 3000));
    }

    console.log(`\nReconciliation complete: ${processed} OK, ${errors} errors\n`);
  } catch (e) {
    console.error(`Fatal reconciliation error: ${e.message}`);
  }
}

app.post('/reconcile', async (req, res) => {
  res.json({ started: true, timestamp: new Date().toISOString() });
  fullReconciliation();
});

app.get('/reconcile', async (req, res) => {
  res.json({ started: true, timestamp: new Date().toISOString() });
  fullReconciliation();
});

// ── Debug endpoint: raw Square inventory response ──────────────────────────
app.get('/debug/square-count/:variationId', async (req, res) => {
  try {
    const variationId = req.params.variationId;
    const apiRes = await fetch(`${SQ_API}/inventory/batch-retrieve-counts`, {
      method: 'POST',
      headers: sqHeaders(),
      body: JSON.stringify({
        catalog_object_ids: [variationId],
        location_ids: [SQ_LOCATION_ID],
        states: ['IN_STOCK']
      })
    });
    const status = apiRes.status;
    const rawText = await apiRes.text();
    let parsed;
    try { parsed = JSON.parse(rawText); } catch { parsed = null; }
    res.json({
      requested_variation_id: variationId,
      location_id: SQ_LOCATION_ID,
      sq_token_prefix: SQ_ACCESS_TOKEN ? SQ_ACCESS_TOKEN.substring(0, 10) + '...' : 'NOT SET',
      http_status: status,
      raw_response: parsed || rawText,
      parsed_quantity: parsed?.counts?.[0]?.quantity || null,
      final_value: parsed?.counts?.[0] ? Math.floor(parseFloat(parsed.counts[0].quantity)) : 0
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── Debug endpoint: dump in-memory mapping + test Square call for problem SKUs
app.get('/debug/mapping-check', async (req, res) => {
  const skus = req.query.skus ? req.query.skus.split(',') : ['974842J', 'Z042202', 'A819863'];
  const results = {};

  for (const sku of skus) {
    const entry = grainMapping[sku];
    if (!entry) {
      results[sku] = { error: 'NOT FOUND in grainMapping' };
      continue;
    }

    const varId = entry.square_variation_id;
    const varIdType = typeof varId;
    const varIdJson = JSON.stringify(varId);

    let squareResult = null;
    let rawBody = null;
    try {
      const bodyObj = {
        catalog_object_ids: [varId],
        location_ids: [SQ_LOCATION_ID],
        states: ['IN_STOCK']
      };
      rawBody = JSON.stringify(bodyObj);
      const apiRes = await fetch(`${SQ_API}/inventory/batch-retrieve-counts`, {
        method: 'POST',
        headers: sqHeaders(),
        body: rawBody
      });
      const text = await apiRes.text();
      squareResult = { status: apiRes.status, response: JSON.parse(text) };
    } catch (e) {
      squareResult = { error: e.message };
    }

    results[sku] = {
      name: entry.name,
      square_variation_id: varId,
      square_variation_id_type: varIdType,
      square_variation_id_json: varIdJson,
      request_body_sent: rawBody,
      square_result: squareResult,
      has_bc_flour: !!entry.bc_flour,
      bc_flour_variants_keys: entry.bc_flour ? Object.keys(entry.bc_flour.variants) : [],
      entry_keys: Object.keys(entry)
    };
  }

  res.json(results);
});

// ── Health check ────────────────────────────────────────────────────────────
app.get('/health', (req, res) => {
  const flourCount = Object.values(bcVariantToGrain).filter(v => v.type === 'flour').length;
  const grainCount = Object.values(bcVariantToGrain).filter(v => v.type === 'grain').length;
  res.json({
    status: 'ok',
    store: BC_STORE_HASH,
    square_location: SQ_LOCATION_ID,
    grain_products: Object.keys(grainMapping).length,
    bc_grain_variants: grainCount,
    bc_flour_variants: flourCount,
    bc_total_variants: Object.keys(bcVariantToGrain).length,
    has_bc_token: !!BC_ACCESS_TOKEN,
    has_sq_token: !!SQ_ACCESS_TOKEN,
    reconcile_interval_mins: RECONCILE_MINS,
    uptime_seconds: Math.floor(process.uptime())
  });
});

// ── Startup ─────────────────────────────────────────────────────────────────
// === One-time webhook registration endpoint ===
app.post('/register-webhooks', async (req, res) => {
  const baseUrl = req.body.base_url || `https://${req.headers.host}`;
  const results = { bc: null, square: null };

  // Register BigCommerce webhook
  try {
    const bcRes = await fetch(`https://api.bigcommerce.com/stores/${BC_STORE_HASH}/v3/hooks`, {
      method: 'POST',
      headers: {
        'X-Auth-Token': BC_ACCESS_TOKEN,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        scope: 'store/order/created',
        destination: `${baseUrl}/webhooks/order-created`,
        is_active: true,
        headers: {}
      })
    });
    results.bc = await bcRes.json();
    console.log('BC webhook registered:', JSON.stringify(results.bc));
  } catch (e) {
    results.bc = { error: e.message };
    console.error('BC webhook registration failed:', e.message);
  }

  // Register Square webhook
  try {
    const sqRes = await fetch('https://connect.squareup.com/v2/webhooks/subscriptions', {
      method: 'POST',
      headers: {
        'Square-Version': '2024-01-18',
        'Authorization': `Bearer ${SQ_ACCESS_TOKEN}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        subscription: {
          name: 'Flour Inventory Sync',
          event_types: ['inventory.count.updated'],
          notification_url: `${baseUrl}/webhooks/square-inventory`,
          enabled: true
        }
      })
    });
    results.square = await sqRes.json();
    console.log('Square webhook registered:', JSON.stringify(results.square));
  } catch (e) {
    results.square = { error: e.message };
    console.error('Square webhook registration failed:', e.message);
  }

  res.json(results);
});

if (!BC_ACCESS_TOKEN) {
  console.error('ERROR: BC_ACCESS_TOKEN is required');
  process.exit(1);
}
if (!SQ_ACCESS_TOKEN) {
  console.error('ERROR: SQ_ACCESS_TOKEN is required');
  process.exit(1);
}

app.listen(PORT, async () => {
  console.log(`\n🌾 Grain-Flour Inventory Sync running on port ${PORT}`);
  console.log(`   BC store: ${BC_STORE_HASH}`);
  console.log(`   Square location: ${SQ_LOCATION_ID}`);
  console.log(`   Grain mappings: ${Object.keys(grainMapping).length}`);
  console.log(`   Reconciliation interval: ${RECONCILE_MINS} minutes`);
  console.log(`\n   Endpoints:`);
  console.log(`     POST /webhooks/order-created      (BC order webhook)`);
  console.log(`     POST /webhooks/square-inventory    (Square inventory webhook)`);
  console.log(`     GET  /reconcile                    (trigger full reconciliation)`);
  console.log(`     POST /reconcile                    (trigger full reconciliation)`);
  console.log(`     GET  /health                       (health check)\n`);

  // Auto-discover flour variant IDs from BigCommerce
  await discoverFlourVariants();
  console.log(`   BC variant lookups: ${Object.keys(bcVariantToGrain).length} total`);

  // Schedule recurring reconciliation
  if (RECONCILE_MINS > 0) {
    setInterval(fullReconciliation, RECONCILE_MINS * 60 * 1000);
    console.log(`   ⏰ First reconciliation in ${RECONCILE_MINS} minutes`);

    // Run initial reconciliation 30 seconds after boot
    setTimeout(fullReconciliation, 30000);
    console.log(`   ⏰ Initial reconciliation in 30 seconds\n`);
  }
});
