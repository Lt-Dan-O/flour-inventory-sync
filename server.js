/**
 * Unified Grain-Flour-Coffee-Mill Inventory Sync Service
 *
 * Square is the absolute source of truth for inventory.
 *
 * GRAIN/FLOUR: The per-lb count in Square = total pounds available.
 * COFFEE: The per-oz count in Square = total ounces available.
 * MILLS: The unit count in Square = total units available (1:1 sync).
 *
 * This service:
 *   1. Listens for BC order.created webhooks â deducts lbs/oz/units from Square
 *   2. Listens for Square inventory.count.updated webhooks â recalculates BC
 *   3. Runs a 15-minute reconciliation poll as a safety net
 *
 * Grain inventory calculation from total_lbs:
 *   - Grain 1 LB variant  = available_lbs
 *   - Grain 5 LB variant  = floor(available_lbs / 5)
 *   - Grain 10 LB variant = floor(available_lbs / 10)
 *   - Grain 25 LB variant = floor(available_lbs / 25)
 *   - Flour 1 LB variant  = available_lbs  (shares same pool)
 *   - Flour 5 LB variant  = floor(available_lbs / 5)
 *   - Flour 10 LB variant = floor(available_lbs / 10)
 *
 * Coffee inventory calculation from total_oz:
 *   - Per-oz variant = available_oz  (1:1 sync, only per-oz variant)
 *   - 1lb/5lb bags handled separately, NOT synced
 *
 * Mill inventory calculation from total_units:
 *   - BC "Current Stock" variant = available_units  (1:1 sync)
 *   - PreOrder variants handled separately, NOT synced
 *
 * "available" = Square total - reserved (pending/unshipped BC orders)
 */

const express = require('express');
const fetch = require('node-fetch');
const grainMapping = require('./grain-mapping.json');
const coffeeMapping = require('./coffee-mapping.json');
const millMapping = require('./mill-mapping.json');

const app = express();
app.use(express.json());

// ââ Config ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
const BC_STORE_HASH   = process.env.BC_STORE_HASH || 'h1uvrm9fjd';
const BC_ACCESS_TOKEN = process.env.BC_ACCESS_TOKEN;
const SQ_ACCESS_TOKEN = process.env.SQ_ACCESS_TOKEN;
const SQ_LOCATION_ID  = process.env.SQ_LOCATION_ID || 'D7QJPMPVZME4K';
const PORT            = process.env.PORT || 3000;
const RECONCILE_MINS  = parseInt(process.env.RECONCILE_MINS || '15', 10);

const BC_API = `https://api.bigcommerce.com/stores/${BC_STORE_HASH}`;
const SQ_API = 'https://connect.squareup.com/v2';

// ââ Reverse lookups (populated at startup) ââââââââââââââââââââââââââââââââââ
// Map Square variation_id â grain base SKU (for Square webhook handler)
const sqVariationToGrain = {};
// Map BC variant_id â { grainSku, lbs, type:'grain'|'flour' }
const bcVariantToGrain = {};
// Map grain SKU â flour SKU prefix (for matching flour variants to grain)
const flourSkuToGrain = {};

// Coffee reverse lookups
// Map Square variation_id â coffee SKU (for Square webhook handler)
const sqVariationToCoffee = {};
// Map BC variant_id â { coffeeSku, oz: 1, type: 'coffee' }
const bcVariantToCoffee = {};

// Mill reverse lookups
// Map Square variation_id â mill SKU (for Square webhook handler)
const sqVariationToMill = {};
// Map BC variant_id â { millSku, units: 1, type: 'mill' }
const bcVariantToMill = {};

// Build static lookups from grain-mapping.json (grain + Square data)
for (const [grainSku, entry] of Object.entries(grainMapping)) {
  sqVariationToGrain[entry.square_variation_id] = grainSku;

  for (const [lbs, vData] of Object.entries(entry.bc_grain.variants)) {
    bcVariantToGrain[vData.variant_id] = { grainSku, lbs: parseInt(lbs, 10), type: 'grain' };
  }

  // Build flour SKU â grain mapping for auto-discovery
  // Flour SKUs follow pattern: FM-{grainSku} or FM-{grainSku}-{lbs}
  if (entry.bc_flour && entry.bc_flour.variants) {
    for (const [lbs, vData] of Object.entries(entry.bc_flour.variants)) {
      if (vData.sku) {
        flourSkuToGrain[vData.sku] = { grainSku, lbs: parseInt(lbs, 10) };
      }
    }
  }
}

// Build static lookups from coffee-mapping.json
for (const [coffeeSku, entry] of Object.entries(coffeeMapping)) {
  sqVariationToCoffee[entry.square_variation_id] = coffeeSku;

  // Only the per-oz variant is synced
  if (entry.bc_per_oz) {
    bcVariantToCoffee[entry.bc_per_oz.variant_id] = { coffeeSku, oz: 1, type: 'coffee' };
  }
}

// Build static lookups from mill-mapping.json
for (const [millSku, entry] of Object.entries(millMapping)) {
  sqVariationToMill[entry.square_variation_id] = millSku;
  bcVariantToMill[entry.bc_variant_id] = { millSku, units: 1, type: 'mill' };
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

    console.log(`  â Discovered ${discovered} flour variants from BigCommerce`);
  } catch (e) {
    console.error(`  â Flour discovery failed: ${e.message}`);
    console.error('    Flour inventory sync will be limited until IDs are available.');
  }
}

// ââ Helpers: BigCommerce ââââââââââââââââââââââââââââââââââââââââââââââââââââ
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
  const reserved = {};  // grainSku â total lbs reserved

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

// ââ Helpers: Square âââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
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
      // Empty response â could be rate-limiting during bulk reconciliation.
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

    // API returned unfiltered results â page through them to find our item
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

    // Mismatch â API returned wrong data, retry with exponential backoff
    const delay = Math.min(1000 * Math.pow(2, attempt - 1), 8000); // 1s, 2s, 4s, 8s
    console.warn(`getSquareCount: attempt ${attempt}/${MAX_RETRIES} mismatch for ${variationId}, got ${data.counts.length} unrelated counts, retrying in ${delay}ms`);
    if (attempt < MAX_RETRIES) {
      await new Promise(r => setTimeout(r, delay));
    }
  }
  // All batch retries returned mismatched data â try a long-cooldown final attempt.
  // The batch endpoint's cache/filter issue clears after ~15s of inactivity.
  console.warn(`getSquareCount: batch retries exhausted for ${variationId}, waiting 15s for cache clear...`);
  try {
    await new Promise(r => setTimeout(r, 15000)); // long cooldown to let API cache clear
    const finalRes = await fetch(`${SQ_API}/inventory/batch-retrieve-counts`, {
      method: 'POST',
      headers: sqHeaders(),
      body: JSON.stringify({
        catalog_object_ids: [variationId],
        location_ids: [SQ_LOCATION_ID],
        states: ['IN_STOCK']
      })
    });
    if (finalRes.ok) {
      const finalData = await finalRes.json();
      if (finalData.counts && finalData.counts.length > 0) {
        const match = finalData.counts.find(c => c.catalog_object_id === variationId);
        if (match) {
          const qty = Math.floor(parseFloat(match.quantity)) || 0;
          console.log(`getSquareCount: long-cooldown retry succeeded for ${variationId}, qty=${qty}`);
          return qty;
        }
      }
      console.warn(`getSquareCount: long-cooldown retry still no match for ${variationId}, counts=${finalData.counts?.length || 0}`);
    } else {
      const errText = await finalRes.text();
      console.error(`getSquareCount: long-cooldown retry failed ${finalRes.status} for ${variationId}: ${errText}`);
    }
  } catch (fallbackErr) {
    console.error(`getSquareCount: long-cooldown retry exception for ${variationId}: ${fallbackErr.message}`);
  }
  console.error(`getSquareCount: all methods failed for ${variationId}, returning 0`);
  return 0;
}

/**
 * Bulk-fetch inventory counts for ALL variations in a single API call.
 * Returns a Map of variationId â quantity (floored integer).
 * This avoids the per-item sequential call pattern that triggers Square's
 * unfiltered-response bug during bulk reconciliation.
 */
async function bulkFetchSquareCounts(variationIds) {
  const countMap = new Map();
  try {
    // Phase 1: Bulk fetch all IDs at once
    let cursor = null;
    let pageNum = 1;
    do {
      const body = {
        catalog_object_ids: variationIds,
        location_ids: [SQ_LOCATION_ID],
        states: ['IN_STOCK']
      };
      if (cursor) body.cursor = cursor;
      const res = await fetch(`${SQ_API}/inventory/batch-retrieve-counts`, {
        method: 'POST',
        headers: sqHeaders(),
        body: JSON.stringify(body)
      });
      if (!res.ok) {
        const text = await res.text();
        console.error(`bulkFetchSquareCounts: page ${pageNum} failed ${res.status}: ${text}`);
        break;
      }
      const data = await res.json();
      if (data.counts) {
        for (const c of data.counts) {
          if (variationIds.includes(c.catalog_object_id)) {
            countMap.set(c.catalog_object_id, Math.floor(parseFloat(c.quantity)) || 0);
          }
        }
      }
      cursor = data.cursor;
      pageNum++;
      if (countMap.size >= variationIds.length) break;
    } while (cursor && pageNum <= 30);
    console.log(`bulkFetchSquareCounts: bulk phase fetched ${countMap.size}/${variationIds.length} counts in ${pageNum - 1} page(s)`);

    // Phase 2: Retry missing IDs individually with delays
    const missingIds = variationIds.filter(id => !countMap.has(id));
    if (missingIds.length > 0) {
      console.log(`bulkFetchSquareCounts: ${missingIds.length} IDs missing from bulk response, retrying individually...`);
      await new Promise(r => setTimeout(r, 3000)); // 3s cooldown before individual calls

      for (const missingId of missingIds) {
        for (let attempt = 1; attempt <= 3; attempt++) {
          try {
            const res = await fetch(`${SQ_API}/inventory/batch-retrieve-counts`, {
              method: 'POST',
              headers: sqHeaders(),
              body: JSON.stringify({
                catalog_object_ids: [missingId],
                location_ids: [SQ_LOCATION_ID],
                states: ['IN_STOCK']
              })
            });
            if (res.ok) {
              const data = await res.json();
              if (data.counts && data.counts.length > 0) {
                const match = data.counts.find(c => c.catalog_object_id === missingId);
                if (match) {
                  const qty = Math.floor(parseFloat(match.quantity)) || 0;
                  countMap.set(missingId, qty);
                  console.log(`bulkFetchSquareCounts: recovered ${missingId} = ${qty} on attempt ${attempt}`);
                  break;
                }
              }
              // Got response but no matching count - item may have 0 inventory
              if (attempt === 3) {
                console.log(`bulkFetchSquareCounts: ${missingId} returned no counts after ${attempt} attempts (likely 0 stock)`);
              }
            }
          } catch (e) {
            console.error(`bulkFetchSquareCounts: individual retry error for ${missingId}: ${e.message}`);
          }
          if (attempt < 3) await new Promise(r => setTimeout(r, 2000 * attempt)); // increasing delay
        }
        await new Promise(r => setTimeout(r, 2000)); // 2s between each missing item
      }
      console.log(`bulkFetchSquareCounts: after individual retries: ${countMap.size}/${variationIds.length} total`);
    }
  } catch (e) {
    console.error(`bulkFetchSquareCounts: exception: ${e.message}`);
  }
  return countMap;
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

/** Fetch all pending/unshipped BC orders and sum reserved oz per coffee SKU */
async function getReservedOz() {
  const reserved = {};  // coffeeSku â total oz reserved

  const statuses = [1, 9, 11, 12];

  for (const statusId of statuses) {
    let page = 1;
    while (true) {
      let orders;
      try {
        orders = await bcGet(`/v2/orders?status_id=${statusId}&page=${page}&limit=50`);
      } catch (e) {
        break;
      }
      if (!orders || !Array.isArray(orders) || orders.length === 0) break;

      for (const order of orders) {
        let products;
        try {
          products = await bcGet(`/v2/orders/${order.id}/products`);
        } catch (e) {
          continue;
        }

        for (const item of products) {
          const vid = item.variant_id;
          const lookup = bcVariantToCoffee[vid];
          if (!lookup) continue;

          // Per-oz variant: each unit = 1 oz
          const totalOz = lookup.oz * item.quantity;
          if (!reserved[lookup.coffeeSku]) reserved[lookup.coffeeSku] = 0;
          reserved[lookup.coffeeSku] += totalOz;
        }
      }

      if (orders.length < 50) break;
      page++;
    }
  }

  return reserved;
}

// ââ Core: Recalculate all BC variants from Square truth âââââââââââââââââââââ
async function recalculateGrain(grainSku, options = {}) {
  const entry = grainMapping[grainSku];
  if (!entry) {
    console.warn(`recalculateGrain: unknown grain SKU "${grainSku}"`);
    return;
  }

  // 1. Get total lbs from Square (use pre-fetched bulk counts if available)
  let totalLbs;
  if (options.bulkCounts && options.bulkCounts.has(entry.square_variation_id)) {
    totalLbs = options.bulkCounts.get(entry.square_variation_id);
  } else {
    totalLbs = await getSquareCount(entry.square_variation_id);
  }

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

// ââ Core: Recalculate coffee BC per-oz variant from Square truth ââââââââââââ
async function recalculateCoffee(coffeeSku, options = {}) {
  const entry = coffeeMapping[coffeeSku];
  if (!entry) {
    console.warn(`recalculateCoffee: unknown coffee SKU "${coffeeSku}"`);
    return;
  }

  // 1. Get total oz from Square (use pre-fetched bulk counts if available)
  let totalOz;
  if (options.bulkCounts && options.bulkCounts.has(entry.square_variation_id)) {
    totalOz = options.bulkCounts.get(entry.square_variation_id);
  } else {
    totalOz = await getSquareCount(entry.square_variation_id);
  }

  // 2. Get reserved oz from pending BC orders
  const reservedMap = options.reservedOzMap || await getReservedOz();
  const reservedOz = reservedMap[coffeeSku] || 0;

  const availableOz = Math.max(0, totalOz - reservedOz);

  console.log(`  [COFFEE: ${entry.name}] Square=${totalOz} oz, reserved=${reservedOz}, available=${availableOz}`);

  // 3. Set BC per-oz variant stock (1:1 sync)
  if (entry.bc_per_oz) {
    try {
      await setBcVariantStock(entry.bc_per_oz.product_id, entry.bc_per_oz.variant_id, availableOz);
    } catch (e) {
      console.error(`    ERROR setting coffee per-oz (vid ${entry.bc_per_oz.variant_id}): ${e.message}`);
    }
  }

  console.log(`  [COFFEE: ${entry.name}] BC per-oz updated: ${availableOz}`);
}

/** Fetch all pending/unshipped BC orders and sum reserved units per mill SKU */
async function getReservedUnits() {
  const reserved = {};  // millSku â total units reserved

  const statuses = [1, 9, 11, 12];

  for (const statusId of statuses) {
    let page = 1;
    while (true) {
      let orders;
      try {
        orders = await bcGet(`/v2/orders?status_id=${statusId}&page=${page}&limit=50`);
      } catch (e) {
        break;
      }
      if (!orders || !Array.isArray(orders) || orders.length === 0) break;

      for (const order of orders) {
        let products;
        try {
          products = await bcGet(`/v2/orders/${order.id}/products`);
        } catch (e) {
          continue;
        }

        for (const item of products) {
          const vid = item.variant_id;
          const lookup = bcVariantToMill[vid];
          if (!lookup) continue;

          // Each unit = 1 mill
          if (!reserved[lookup.millSku]) reserved[lookup.millSku] = 0;
          reserved[lookup.millSku] += item.quantity;
        }
      }

      if (orders.length < 50) break;
      page++;
    }
  }

  return reserved;
}

// ââ Core: Recalculate mill BC variant from Square truth âââââââââââââââââââ
async function recalculateMill(millSku, options = {}) {
  const entry = millMapping[millSku];
  if (!entry) {
    console.warn(`recalculateMill: unknown mill SKU "${millSku}"`);
    return;
  }

  // 1. Get total units from Square (use pre-fetched bulk counts if available)
  let totalUnits;
  if (options.bulkCounts && options.bulkCounts.has(entry.square_variation_id)) {
    totalUnits = options.bulkCounts.get(entry.square_variation_id);
  } else {
    totalUnits = await getSquareCount(entry.square_variation_id);
  }

  // 2. Get reserved units from pending BC orders
  const reservedMap = options.reservedUnitsMap || await getReservedUnits();
  const reservedUnits = reservedMap[millSku] || 0;

  const availableUnits = Math.max(0, totalUnits - reservedUnits);

  console.log(`  [MILL: ${entry.name}] Square=${totalUnits} units, reserved=${reservedUnits}, available=${availableUnits}`);

  // 3. Set BC variant stock (1:1 sync)
  try {
    await setBcVariantStock(entry.bc_product_id, entry.bc_variant_id, availableUnits);
  } catch (e) {
    console.error(`    ERROR setting mill (vid ${entry.bc_variant_id}): ${e.message}`);
  }

  console.log(`  [MILL: ${entry.name}] BC updated: ${availableUnits}`);
}

// ââ Handler 1: BC order.created webhook âââââââââââââââââââââââââââââââââââââ
app.post('/webhooks/order-created', async (req, res) => {
  res.status(200).json({ received: true });

  try {
    const orderId = req.body?.data?.id;
    if (!orderId) return;

    console.log(`\n=== BC Order #${orderId} created ===`);

    const products = await bcGet(`/v2/orders/${orderId}/products`);
    const affectedGrains = new Set();
    const affectedCoffees = new Set();
    const affectedMills = new Set();

    for (const item of products) {
      const vid = item.variant_id;

      // Check grain/flour first
      const grainLookup = bcVariantToGrain[vid];
      if (grainLookup) {
        const lbsToDeduct = grainLookup.lbs * item.quantity;
        const entry = grainMapping[grainLookup.grainSku];

        console.log(`  ${grainLookup.type} SKU ${item.sku} x${item.quantity} = ${lbsToDeduct} lbs â deduct from Square`);

        try {
          await adjustSquareInventory(entry.square_variation_id, -lbsToDeduct);
          console.log(`  â Square deducted ${lbsToDeduct} lbs from ${entry.name}`);
          affectedGrains.add(grainLookup.grainSku);
        } catch (e) {
          console.error(`  ERROR deducting from Square for ${item.sku}: ${e.message}`);
        }
        continue;
      }

      // Check coffee
      const coffeeLookup = bcVariantToCoffee[vid];
      if (coffeeLookup) {
        const ozToDeduct = coffeeLookup.oz * item.quantity;
        const entry = coffeeMapping[coffeeLookup.coffeeSku];

        console.log(`  coffee SKU ${item.sku} x${item.quantity} = ${ozToDeduct} oz â deduct from Square`);

        try {
          await adjustSquareInventory(entry.square_variation_id, -ozToDeduct);
          console.log(`  â Square deducted ${ozToDeduct} oz from ${entry.name}`);
          affectedCoffees.add(coffeeLookup.coffeeSku);
        } catch (e) {
          console.error(`  ERROR deducting from Square for ${item.sku}: ${e.message}`);
        }
        continue;
      }

      // Check mill
      const millLookup = bcVariantToMill[vid];
      if (millLookup) {
        const unitsToDeduct = item.quantity;
        const entry = millMapping[millLookup.millSku];

        console.log(`  mill SKU ${item.sku} x${item.quantity} = ${unitsToDeduct} unit(s) â deduct from Square`);

        try {
          await adjustSquareInventory(entry.square_variation_id, -unitsToDeduct);
          console.log(`  â Square deducted ${unitsToDeduct} unit(s) from ${entry.name}`);
          affectedMills.add(millLookup.millSku);
        } catch (e) {
          console.error(`  ERROR deducting from Square for ${item.sku}: ${e.message}`);
        }
      }
    }

    if (affectedGrains.size === 0 && affectedCoffees.size === 0 && affectedMills.size === 0) {
      console.log(`  No grain/flour/coffee/mill items in order, skipping recalculation`);
      return;
    }

    // Recalculate all affected grains
    if (affectedGrains.size > 0) {
      console.log(`  Recalculating ${affectedGrains.size} grain(s)...`);
      const reservedMap = await getReservedLbs();
      for (const grainSku of affectedGrains) {
        await recalculateGrain(grainSku, { reservedMap });
      }
    }

    // Recalculate all affected coffees
    if (affectedCoffees.size > 0) {
      console.log(`  Recalculating ${affectedCoffees.size} coffee(s)...`);
      const reservedOzMap = await getReservedOz();
      for (const coffeeSku of affectedCoffees) {
        await recalculateCoffee(coffeeSku, { reservedOzMap });
      }
    }

    // Recalculate all affected mills
    if (affectedMills.size > 0) {
      console.log(`  Recalculating ${affectedMills.size} mill(s)...`);
      const reservedUnitsMap = await getReservedUnits();
      for (const millSku of affectedMills) {
        await recalculateMill(millSku, { reservedUnitsMap });
      }
    }

    console.log(`=== Order #${orderId} processing complete ===\n`);
  } catch (e) {
    console.error(`Error processing BC order webhook: ${e.message}`);
  }
});

// ââ Handler 2: Square inventory webhook âââââââââââââââââââââââââââââââââââââ
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

    // Check grain mapping first
    const grainSku = sqVariationToGrain[entityId];
    if (grainSku) {
      console.log(`\n=== Square inventory changed for grain: ${grainMapping[grainSku].name} ===`);
      await new Promise(r => setTimeout(r, 2000));
      await recalculateGrain(grainSku);
      console.log(`=== Square inventory sync complete ===\n`);
      return;
    }

    // Check coffee mapping
    const coffeeSku = sqVariationToCoffee[entityId];
    if (coffeeSku) {
      console.log(`\n=== Square inventory changed for coffee: ${coffeeMapping[coffeeSku].name} ===`);
      await new Promise(r => setTimeout(r, 2000));
      await recalculateCoffee(coffeeSku);
      console.log(`=== Square inventory sync complete ===\n`);
      return;
    }

    // Check mill mapping
    const millSku = sqVariationToMill[entityId];
    if (millSku) {
      console.log(`\n=== Square inventory changed for mill: ${millMapping[millSku].name} ===`);
      await new Promise(r => setTimeout(r, 2000));
      await recalculateMill(millSku);
      console.log(`=== Square inventory sync complete ===\n`);
      return;
    }

    console.log(`Square inventory webhook: variation ${entityId} not in our mapping, skipping`);
  } catch (e) {
    console.error(`Error processing Square inventory webhook: ${e.message}`);
  }
});

// ââ Handler 3: Full reconciliation (called by cron or manually) âââââââââââââ
async function fullReconciliation() {
  console.log(`\nââââââââââââââââââââââââââââââââââââââââââââ`);
  console.log(`â   FULL RECONCILIATION - ${new Date().toISOString()}   â`);
  console.log(`ââââââââââââââââââââââââââââââââââââââââââââ`);

  try {
    // Get all reserved lbs/oz/units once (shared across all products)
    const reservedMap = await getReservedLbs();
    const reservedOzMap = await getReservedOz();
    const reservedUnitsMap = await getReservedUnits();
    console.log(`Reserved lbs from pending orders:`, reservedMap);
    console.log(`Reserved oz from pending coffee orders:`, reservedOzMap);
    console.log(`Reserved units from pending mill orders:`, reservedUnitsMap);

    // Pre-fetch ALL Square counts (grain + coffee + mill) in a single bulk call
    const grainVariationIds = Object.values(grainMapping).map(g => g.square_variation_id);
    const coffeeVariationIds = Object.values(coffeeMapping).map(c => c.square_variation_id);
    const millVariationIds = Object.values(millMapping).map(m => m.square_variation_id);
    const allVariationIds = [...grainVariationIds, ...coffeeVariationIds, ...millVariationIds];
    const bulkCounts = await bulkFetchSquareCounts(allVariationIds);

    let processed = 0;
    let errors = 0;

    // Track items that got 0 from Square (potential API failures)
    const zeroItems = [];

    for (const grainSku of Object.keys(grainMapping)) {
      try {
        const entry = grainMapping[grainSku];
        const sqCount = bulkCounts.has(entry.square_variation_id)
          ? bulkCounts.get(entry.square_variation_id)
          : null;
        await recalculateGrain(grainSku, { reservedMap, bulkCounts });
        processed++;
        // Track items where Square returned 0 or was missing from bulk
        if (sqCount === null || sqCount === 0) {
          zeroItems.push(grainSku);
        }
      } catch (e) {
        console.error(`  ERROR reconciling ${grainSku}: ${e.message}`);
        errors++;
      }

      // Short delay for BC rate limiting (Square reads are pre-fetched)
      await new Promise(r => setTimeout(r, 500));
    }

    console.log(`\nGrain reconciliation pass 1 complete: ${processed} OK, ${errors} errors`);

    // ââ Coffee reconciliation ââââââââââââââââââââââââââââââââââââââââââââââ
    console.log(`\n--- Coffee Reconciliation (${Object.keys(coffeeMapping).length} products) ---`);
    let coffeeProcessed = 0;
    let coffeeErrors = 0;
    const zeroCoffeeItems = [];

    for (const coffeeSku of Object.keys(coffeeMapping)) {
      try {
        const entry = coffeeMapping[coffeeSku];
        const sqCount = bulkCounts.has(entry.square_variation_id)
          ? bulkCounts.get(entry.square_variation_id)
          : null;
        await recalculateCoffee(coffeeSku, { reservedOzMap, bulkCounts });
        coffeeProcessed++;
        if (sqCount === null || sqCount === 0) {
          zeroCoffeeItems.push(coffeeSku);
        }
      } catch (e) {
        console.error(`  ERROR reconciling coffee ${coffeeSku}: ${e.message}`);
        coffeeErrors++;
      }

      await new Promise(r => setTimeout(r, 500));
    }

    console.log(`Coffee reconciliation complete: ${coffeeProcessed} OK, ${coffeeErrors} errors`);

    // ââ Mill reconciliation âââââââââââââââââââââââââââââââââââââââââââââââ
    console.log(`\n--- Mill Reconciliation (${Object.keys(millMapping).length} products) ---`);
    let millProcessed = 0;
    let millErrors = 0;
    const zeroMillItems = [];

    for (const millSku of Object.keys(millMapping)) {
      try {
        const entry = millMapping[millSku];
        const sqCount = bulkCounts.has(entry.square_variation_id)
          ? bulkCounts.get(entry.square_variation_id)
          : null;
        await recalculateMill(millSku, { reservedUnitsMap, bulkCounts });
        millProcessed++;
        if (sqCount === null || sqCount === 0) {
          zeroMillItems.push(millSku);
        }
      } catch (e) {
        console.error(`  ERROR reconciling mill ${millSku}: ${e.message}`);
        millErrors++;
      }

      await new Promise(r => setTimeout(r, 500));
    }

    console.log(`Mill reconciliation complete: ${millProcessed} OK, ${millErrors} errors`);

    // Combine zero items for deferred retry (grain + coffee + mill)
    const zeroCoffeeForRetry = zeroCoffeeItems.map(sku => ({ sku, type: 'coffee' }));
    const zeroGrainForRetry = zeroItems.map(sku => ({ sku, type: 'grain' }));

    // Deferred retry pass for coffee items with 0 counts
    if (zeroCoffeeItems.length > 0) {
      console.log(`\n--- Coffee deferred retry: ${zeroCoffeeItems.length} items with 0 counts ---`);
      await new Promise(r => setTimeout(r, 30000)); // 30s cooldown

      for (const coffeeSku of zeroCoffeeItems) {
        const entry = coffeeMapping[coffeeSku];
        try {
          const res = await fetch(`${SQ_API}/inventory/batch-retrieve-counts`, {
            method: 'POST',
            headers: sqHeaders(),
            body: JSON.stringify({
              catalog_object_ids: [entry.square_variation_id],
              location_ids: [SQ_LOCATION_ID],
              states: ['IN_STOCK']
            })
          });
          if (res.ok) {
            const data = await res.json();
            if (data.counts && data.counts.length > 0) {
              const match = data.counts.find(c => c.catalog_object_id === entry.square_variation_id);
              if (match) {
                const qty = Math.floor(parseFloat(match.quantity)) || 0;
                if (qty > 0) {
                  console.log(`  Coffee retry: ${entry.name} recovered Square=${qty} oz`);
                  const fixedCounts = new Map([[entry.square_variation_id, qty]]);
                  await recalculateCoffee(coffeeSku, { reservedOzMap, bulkCounts: fixedCounts });
                }
              }
            }
          }
        } catch (e) {
          console.error(`  Coffee retry error for ${entry.name}: ${e.message}`);
        }
        await new Promise(r => setTimeout(r, 3000));
      }
    }

    // Deferred retry pass for mill items with 0 counts
    if (zeroMillItems.length > 0) {
      console.log(`\n--- Mill deferred retry: ${zeroMillItems.length} items with 0 counts ---`);
      await new Promise(r => setTimeout(r, 30000)); // 30s cooldown

      for (const millSku of zeroMillItems) {
        const entry = millMapping[millSku];
        try {
          const res = await fetch(`${SQ_API}/inventory/batch-retrieve-counts`, {
            method: 'POST',
            headers: sqHeaders(),
            body: JSON.stringify({
              catalog_object_ids: [entry.square_variation_id],
              location_ids: [SQ_LOCATION_ID],
              states: ['IN_STOCK']
            })
          });
          if (res.ok) {
            const data = await res.json();
            if (data.counts && data.counts.length > 0) {
              const match = data.counts.find(c => c.catalog_object_id === entry.square_variation_id);
              if (match) {
                const qty = Math.floor(parseFloat(match.quantity)) || 0;
                if (qty > 0) {
                  console.log(`  Mill retry: ${entry.name} recovered Square=${qty} units`);
                  const fixedCounts = new Map([[entry.square_variation_id, qty]]);
                  await recalculateMill(millSku, { reservedUnitsMap, bulkCounts: fixedCounts });
                }
              }
            }
          }
        } catch (e) {
          console.error(`  Mill retry error for ${entry.name}: ${e.message}`);
        }
        await new Promise(r => setTimeout(r, 3000));
      }
    }

    // Deferred retry pass for grain items: re-fetch items that got 0 from Square
    // Wait for the API to recover from the "1000 unfiltered counts" degraded state
    if (zeroItems.length > 0) {
      let remaining = [...zeroItems];
      let retryFixed = 0;
      const retryRounds = [
        { delay: 60, label: 'Round 1 (60s cooldown)' },
        { delay: 60, label: 'Round 2 (120s total)' },
        { delay: 60, label: 'Round 3 (180s total)' }
      ];

      for (const round of retryRounds) {
        if (remaining.length === 0) break;

        console.log(`\n--- Deferred retry ${round.label}: ${remaining.length} items, waiting ${round.delay}s ---`);
        await new Promise(r => setTimeout(r, round.delay * 1000));

        const stillFailed = [];
        for (const grainSku of remaining) {
          const entry = grainMapping[grainSku];
          try {
            const res = await fetch(`${SQ_API}/inventory/batch-retrieve-counts`, {
              method: 'POST',
              headers: sqHeaders(),
              body: JSON.stringify({
                catalog_object_ids: [entry.square_variation_id],
                location_ids: [SQ_LOCATION_ID],
                states: ['IN_STOCK']
              })
            });

            if (res.ok) {
              const data = await res.json();
              if (data.counts && data.counts.length > 0) {
                // Check if unfiltered (more than our 1 requested ID)
                const match = data.counts.find(c => c.catalog_object_id === entry.square_variation_id);
                if (match) {
                  const qty = Math.floor(parseFloat(match.quantity)) || 0;
                  if (qty > 0) {
                    console.log(`  Deferred retry: ${entry.name} recovered Square=${qty} lbs`);
                    const fixedCounts = new Map([[entry.square_variation_id, qty]]);
                    await recalculateGrain(grainSku, { reservedMap, bulkCounts: fixedCounts });
                    retryFixed++;
                  } else {
                    console.log(`  Deferred retry: ${entry.name} match found but qty=0 (genuinely zero)`);
                  }
                } else if (data.counts.length > 10) {
                  // Got unfiltered dump - page through ALL counts looking for our ID
                  console.log(`  Deferred retry: ${entry.name} got ${data.counts.length} unfiltered counts, searching...`);
                  let found = false;
                  let cursor = data.cursor;

                  // Search current page
                  const pageMatch = data.counts.find(c => c.catalog_object_id === entry.square_variation_id);
                  if (pageMatch) {
                    const qty = Math.floor(parseFloat(pageMatch.quantity)) || 0;
                    if (qty > 0) {
                      console.log(`  Deferred retry: ${entry.name} found in unfiltered page! Square=${qty} lbs`);
                      const fixedCounts = new Map([[entry.square_variation_id, qty]]);
                      await recalculateGrain(grainSku, { reservedMap, bulkCounts: fixedCounts });
                      retryFixed++;
                      found = true;
                    }
                  }

                  // Page through remaining if not found on first page
                  let pages = 1;
                  while (!found && cursor && pages < 5) {
                    await new Promise(r => setTimeout(r, 1000));
                    const pageRes = await fetch(`${SQ_API}/inventory/batch-retrieve-counts`, {
                      method: 'POST',
                      headers: sqHeaders(),
                      body: JSON.stringify({
                        catalog_object_ids: [entry.square_variation_id],
                        location_ids: [SQ_LOCATION_ID],
                        states: ['IN_STOCK'],
                        cursor
                      })
                    });
                    if (pageRes.ok) {
                      const pageData = await pageRes.json();
                      cursor = pageData.cursor;
                      if (pageData.counts) {
                        const pm = pageData.counts.find(c => c.catalog_object_id === entry.square_variation_id);
                        if (pm) {
                          const qty = Math.floor(parseFloat(pm.quantity)) || 0;
                          if (qty > 0) {
                            console.log(`  Deferred retry: ${entry.name} found on page ${pages + 1}! Square=${qty} lbs`);
                            const fixedCounts = new Map([[entry.square_variation_id, qty]]);
                            await recalculateGrain(grainSku, { reservedMap, bulkCounts: fixedCounts });
                            retryFixed++;
                            found = true;
                          }
                        }
                      }
                    } else {
                      break;
                    }
                    pages++;
                  }

                  if (!found) {
                    console.log(`  Deferred retry: ${entry.name} not found in ${pages} pages of unfiltered data`);
                    stillFailed.push(grainSku);
                  }
                } else {
                  console.log(`  Deferred retry: ${entry.name} no matching count in ${data.counts.length} results`);
                  stillFailed.push(grainSku);
                }
              } else {
                console.log(`  Deferred retry: ${entry.name} empty response (genuinely zero stock)`);
              }
            } else {
              console.log(`  Deferred retry: ${entry.name} API error ${res.status}`);
              stillFailed.push(grainSku);
            }
          } catch (e) {
            console.error(`  Deferred retry error for ${entry.name}: ${e.message}`);
            stillFailed.push(grainSku);
          }

          await new Promise(r => setTimeout(r, 5000));
        }

        remaining = stillFailed;
        console.log(`  ${round.label} done: ${retryFixed} recovered, ${remaining.length} still pending`);
      }

      console.log(`Deferred retry complete: ${retryFixed}/${zeroItems.length} items recovered\n`);
      if (remaining.length > 0) {
        console.log(`  Still unresolved: ${remaining.map(s => grainMapping[s].name).join(', ')}`);
      }
    } else {
      console.log(`All items had non-zero Square counts - no deferred retry needed\n`);
    }
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

// ââ Debug endpoint: raw Square inventory response ââââââââââââââââââââââââââ
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

// ââ Debug endpoint: dump in-memory mapping + test Square call for problem SKUs
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

// Brewing Grain Discovery (on-demand)
app.get('/discover-brewing-grains', async (req, res) => {
  const { execFile } = require('child_process');
  const scriptPath = require('path').join(__dirname, 'discover-brewing-grains.js');
  const env = { ...process.env, SQUARE_ACCESS_TOKEN: SQ_ACCESS_TOKEN };

  execFile('node', [scriptPath], { env, timeout: 120000 }, (err, stdout, stderr) => {
    if (err) {
      console.error('Brewing grain discovery failed:', err.message);
      return res.status(500).json({ error: err.message, stdout, stderr });
    }
    console.log('Brewing grain discovery completed:\n', stdout);
    res.json({ status: 'ok', output: stdout, errors: stderr || null });
  });
});

// ââ Health check ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
app.get('/health', (req, res) => {
  const flourCount = Object.values(bcVariantToGrain).filter(v => v.type === 'flour').length;
  const grainCount = Object.values(bcVariantToGrain).filter(v => v.type === 'grain').length;
  const coffeeCount = Object.keys(bcVariantToCoffee).length;
  const millCount = Object.keys(bcVariantToMill).length;
  res.json({
    status: 'ok',
    store: BC_STORE_HASH,
    square_location: SQ_LOCATION_ID,
    grain_products: Object.keys(grainMapping).length,
    coffee_products: Object.keys(coffeeMapping).length,
    mill_products: Object.keys(millMapping).length,
    bc_grain_variants: grainCount,
    bc_flour_variants: flourCount,
    bc_coffee_variants: coffeeCount,
    bc_mill_variants: millCount,
    bc_total_variants: Object.keys(bcVariantToGrain).length + coffeeCount + millCount,
    has_bc_token: !!BC_ACCESS_TOKEN,
    has_sq_token: !!SQ_ACCESS_TOKEN,
    reconcile_interval_mins: RECONCILE_MINS,
    uptime_seconds: Math.floor(process.uptime())
  });
});

// ââ Startup âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
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
  console.log(`\nð¾âð§ Grain-Flour-Coffee-Mill Inventory Sync running on port ${PORT}`);
  console.log(`   BC store: ${BC_STORE_HASH}`);
  console.log(`   Square location: ${SQ_LOCATION_ID}`);
  console.log(`   Grain mappings: ${Object.keys(grainMapping).length}`);
  console.log(`   Coffee mappings: ${Object.keys(coffeeMapping).length}`);
  console.log(`   Mill mappings: ${Object.keys(millMapping).length}`);
  console.log(`   Reconciliation interval: ${RECONCILE_MINS} minutes`);
  console.log(`\n   Endpoints:`);
  console.log(`     POST /webhooks/order-created      (BC order webhook)`);
  console.log(`     POST /webhooks/square-inventory    (Square inventory webhook)`);
  console.log(`     GET  /reconcile                    (trigger full reconciliation)`);
  console.log(`     POST /reconcile                    (trigger full reconciliation)`);
  console.log(`     GET  /health                       (health check)\n`);

  // Auto-discover flour variant IDs from BigCommerce
  await discoverFlourVariants();
  console.log(`   BC grain/flour variant lookups: ${Object.keys(bcVariantToGrain).length}`);
  console.log(`   BC coffee variant lookups: ${Object.keys(bcVariantToCoffee).length}`);
  console.log(`   BC mill variant lookups: ${Object.keys(bcVariantToMill).length}`);

  // Schedule recurring reconciliation
  if (RECONCILE_MINS > 0) {
    setInterval(fullReconciliation, RECONCILE_MINS * 60 * 1000);
    console.log(`   â° First reconciliation in ${RECONCILE_MINS} minutes`);

    // Run initial reconciliation 30 seconds after boot
    setTimeout(fullReconciliation, 30000);
    console.log(`   â° Initial reconciliation in 30 seconds\n`);
  }
});
