#!/usr/bin/env node
/**
 * Brewing Grain Mapping Builder + BC Discovery
 *
 * Self-contained script that:
 * 1. Queries Square catalog for all items in the Brewing Grains category
 * 2. Builds the mapping with bulk/bag variant identification
 * 3. Queries BigCommerce to find matching "by the OUNCE" variants
 * 4. Saves the complete brewing-grain-mapping.json
 *
 * Usage (on Render where env vars are set):
 *   node discover-brewing-grains.js
 *
 * Or locally:
 *   SQUARE_ACCESS_TOKEN=xxx BC_ACCESS_TOKEN=xxx node discover-brewing-grains.js
 *
 * Environment variables:
 *   SQUARE_ACCESS_TOKEN  - Square API token
 *   BC_ACCESS_TOKEN      - BigCommerce API token
 *   BC_STORE_HASH        - BC store hash (default: h1uvrm9fjd)
#!/usr/bin/env node
/**
 * Brewing Grain Mapping Builder + BC Discovery
  *
   * Self-contained script that:
    * 1. Queries Square catalog for all items in the Brewing Grains category
     * 2. Builds the mapping with bulk/bag variant identification
      * 3. Queries BigCommerce to find matching "by the OUNCE" variants
       * 4. Saves the complete brewing-grain-mapping.json
        *
         * Usage (on Render where env vars are set):
          *   node discover-brewing-grains.js
           *
            * Or locally:
             *   SQUARE_ACCESS_TOKEN=xxx BC_ACCESS_TOKEN=xxx node discover-brewing-grains.js
              *
               * Environment variables:
                *   SQUARE_ACCESS_TOKEN  - Square API token
                 *   BC_ACCESS_TOKEN      - BigCommerce API token
                  *   BC_STORE_HASH        - BC store hash (default: h1uvrm9fjd)
                   */
  
  const fetch = require('node-fetch');
const fs = require('fs');
const path = require('path');

// --- Config ---
const SQ_TOKEN = process.env.SQUARE_ACCESS_TOKEN || process.env.SQ_ACCESS_TOKEN;
const BC_TOKEN = process.env.BC_ACCESS_TOKEN;
const BC_STORE  = process.env.BC_STORE_HASH || 'h1uvrm9fjd';
const SQ_API   = 'https://connect.squareup.com/v2';
const BC_API   = `https://api.bigcommerce.com/stores/${BC_STORE}`;
const LOCATION = 'D7QJPMPVZME4K';
const BREWING_GRAINS_CATEGORY = 'CQM5GLJF3UQ6H6TUUGP5C6C2';
const MEASUREMENT_UNIT_OZ = 'XAZRNKPDPUK47YOPZO6TZG6Q';
const OUTPUT   = path.join(__dirname, 'brewing-grain-mapping.json');

if (!SQ_TOKEN) { console.error('ERROR: SQUARE_ACCESS_TOKEN required'); process.exit(1); }
  if (!BC_TOKEN) { console.error('ERROR: BC_ACCESS_TOKEN required'); process.exit(1); }
    
    const sleep = ms => new Promise(r => setTimeout(r, ms));

// --- Square helpers ---
async function sqPost(endpoint, body) {
    const res = await fetch(`${SQ_API}${endpoint}`, {
          method: 'POST', headers: { 'Authorization': `Bearer ${SQ_TOKEN}`, 'Content-Type': 'application/json' },
          body: JSON.stringify(body)
    });
    if (!res.ok) throw new Error(`Square ${endpoint}: ${res.status} - ${await res.text()}`);
        return res.json();
}

// --- BC helpers ---
async function bcGet(urlPath) {
    const res = await fetch(`${BC_API}${urlPath}`, {
          headers: { 'X-Auth-Token': BC_TOKEN, 'Content-Type': 'application/json', 'Accept': 'application/json' }
    });
    if (res.status === 404) return null;
        if (res.status === 429) {
              const wait = parseInt(res.headers.get('X-Rate-Limit-Time-Reset-Ms') || '2000', 10);
              console.log(`  Rate limited, waiting ${wait}ms...`);
              await sleep(wait);
              return bcGet(urlPath);
        }
            if (!res.ok) throw new Error(`BC GET ${urlPath}: ${res.status} - ${await res.text()}`);
                return res.json();
}

// --- Step 1: Build mapping from Square catalog ---
async function buildMappingFromSquare() {
    console.log('=== Step 1: Querying Square catalog for Brewing Grains ===\n');
  
    // Search for all items in the Brewing Grains category
    let cursor = null;
    const allItems = [];
    do {
          const body = {
                  object_types: ['ITEM'],
                  query: {
                            exact_query: { attribute_name: 'category_id', attribute_value: BREWING_GRAINS_CATEGORY }
                  },
                  limit: 100
          };
          if (cursor) body.cursor = cursor;
                const result = await sqPost('/catalog/search', body);
          if (result.objects) allItems.push(...result.objects);
                cursor = result.cursor;
    } while (cursor);
  
    console.log(`Found ${allItems.length} items in Brewing Grains category.\n`);
  
    const mapping = {};
  
    for (const item of allItems) {
          const itemName = item.item_data?.name || 'Unknown';
          const variations = item.item_data?.variations || [];
      
          // Identify bulk variant (has measurement_unit_id for oz) and bag variants (no measurement_unit)
          let bulkVariant = null;
          const bagVariants = [];
      
          for (const v of variations) {
                  const vData = v.item_variation_data || {};
                  const muId = vData.measurement_unit_id;
            
                  if (muId === MEASUREMENT_UNIT_OZ) {
                            bulkVariant = v;
                  } else if (!muId) {
                            // Check if this is a bag variant (name contains "bag" or "lbs" or weight pattern)
                            const vName = (vData.name || '').toLowerCase();
                            const isBag = vName.includes('bag') || vName.includes('lbs') || vName.includes('lb');
                            if (isBag) {
                                        // Determine bag size
                                        let bagLbs = 50; // default
                                        let bagOz = 800;
                                        if (vName.includes('55lb') || vName.includes('55 lb')) { bagLbs = 55; bagOz = 880; }
                                                    else if (vName.match(/50\s*lb/)) { bagLbs = 50; bagOz = 800; }
                                                                else if (vName.match(/25\s*lb/)) { bagLbs = 25; bagOz = 400; }
                                                                            else if (vName.match(/10\s*lb/)) { bagLbs = 10; bagOz = 160; }
                                                                                        // 55lbs bags pattern
                                        if (vName.includes('55lbs')) { bagLbs = 55; bagOz = 880; }
                                          
                                                    bagVariants.push({
                                                                  square_variation_id: v.id,
                                                                  name: vData.name,
                                                                  bag_lbs: bagLbs,
                                                                  bag_oz: bagOz
                                                    });
                            }
                  }
          }
      
          if (!bulkVariant) {
                  console.log(`  SKIP: ${itemName} - no bulk (oz) variant found`);
                  continue;
          }
            
                const bulkSku = bulkVariant.item_variation_data?.sku || '';
          if (!bulkSku) {
                  console.log(`  WARN: ${itemName} - bulk variant has no SKU`);
                  continue;
          }
            
                mapping[bulkSku] = {
                        name: itemName,
                        square_item_id: item.id,
                        square_bulk_variation_id: bulkVariant.id,
                        square_bulk_sku: bulkSku,
                        square_location_id: LOCATION,
                        bag_variants: bagVariants,
                        bc_per_oz: null
                };
      
          const bagInfo = bagVariants.length > 0
                  ? ` (${bagVariants.length} bag variants: ${bagVariants.map(b => b.bag_lbs + 'lb').join(', ')})`
                  : '';
          console.log(`  ✓ ${itemName} [SKU: ${bulkSku}]${bagInfo}`);
    }
  
    console.log(`\nBuilt mapping with ${Object.keys(mapping).length} entries.\n`);
    return mapping;
}

// --- Step 2: Discover BC variants ---
async function discoverBCVariants(mapping) {
    console.log('=== Step 2: Discovering BigCommerce variants ===\n');
  
    let matched = 0, notFound = 0, ambiguous = 0;
  
    for (const [sku, entry] of Object.entries(mapping)) {
          // Query BC for variants matching this SKU
          const data = await bcGet(`/v3/catalog/variants?sku=${encodeURIComponent(sku)}&include_fields=id,product_id,sku,option_values`);
      
          if (!data || !data.data || data.data.length === 0) {
                  notFound++;
                  console.log(`  ✗ ${entry.name} (SKU: ${sku}) — not in BC`);
                  await sleep(200);
                  continue;
          }
            
                // Prefer variant whose option label includes "OUNCE"
          let best = data.data[0];
          if (data.data.length > 1) {
                  const ozMatch = data.data.find(v =>
                            (v.option_values || []).some(ov => ov.label && ov.label.includes('OUNCE'))
                          );
                  if (ozMatch) best = ozMatch;
                          else { ambiguous++; }
          }
            
                mapping[sku].bc_per_oz = {
                        product_id: best.product_id,
                        variant_id: best.id,
                        sku: best.sku
                };
          matched++;
          console.log(`  ✓ ${entry.name} → BC product ${best.product_id} / variant ${best.id}`);
          await sleep(200);
    }
  
    console.log(`\n=== BC Discovery Results ===`);
    console.log(`Matched: ${matched}`);
    console.log(`Not found: ${notFound}`);
    if (ambiguous > 0) console.log(`Ambiguous (used first): ${ambiguous}`);
}

// --- Main ---
async function main() {
    // Load existing mapping if it exists, otherwise build from Square
    let mapping;
    if (fs.existsSync(OUTPUT)) {
          console.log('Found existing brewing-grain-mapping.json, loading...\n');
          mapping = JSON.parse(fs.readFileSync(OUTPUT, 'utf8'));
          console.log(`Loaded ${Object.keys(mapping).length} entries.\n`);
      
          // Check if any bc_per_oz are missing
          const missing = Object.values(mapping).filter(e => !e.bc_per_oz).length;
          if (missing === 0) {
                  console.log('All entries already have bc_per_oz. Nothing to do!');
                  return;
          }
                console.log(`${missing} entries missing bc_per_oz, running BC discovery...\n`);
    } else {
          console.log('No existing mapping found. Building from Square catalog...\n');
          mapping = await buildMappingFromSquare();
    }
  
    // Discover BC variants
    await discoverBCVariants(mapping);
  
    // --- Manual corrections (user-verified mismatches) ---
    console.log('\n=== Step 3: Applying manual corrections ===\n');
  
    // 1. Chocolate Rye ≠ Rye Malt - break the link
    for (const [sku, entry] of Object.entries(mapping)) {
          if (entry.name && entry.name.toLowerCase().includes('chocolate rye')) {
                  if (entry.bc_per_oz) {
                            console.log(`  ✗ BREAK: ${entry.name} (SKU: ${sku}) — was incorrectly linked, clearing bc_per_oz`);
                            entry.bc_per_oz = null;
                  }
          }
    }
  
    // 2. Pilsner Malt → German Pilsner Malt - Weyermann (SKU: PILS)
    for (const [sku, entry] of Object.entries(mapping)) {
          if (entry.name && /^pilsner\s+malt$/i.test(entry.name.trim())) {
                  console.log(`  → Looking up BC variant for Pilsner Malt (SKU: PILS)...`);
                  const data = await bcGet(`/v3/catalog/variants?sku=${encodeURIComponent('PILS')}&include_fields=id,product_id,sku,option_values`);
                  if (data && data.data && data.data.length > 0) {
                            const best = data.data.find(v =>
                                        (v.option_values || []).some(ov => ov.label && ov.label.includes('OUNCE'))
                                      ) || data.data[0];
                            mapping[sku].bc_per_oz = { product_id: best.product_id, variant_id: best.id, sku: best.sku };
                            console.log(`  ✓ LINK: ${entry.name} → BC product ${best.product_id} / variant ${best.id} (SKU: PILS)`);
                  } else {
                            console.log(`  ✗ Could not find BC variant with SKU=PILS`);
                  }
                  await sleep(200);
          }
    }
  
    // 3. Roasted Barley 300L → Roasted Barley - Briess Malting (match by existing SKU)
    //    Roasted Barley 500L → no pair (clear any link)
    for (const [sku, entry] of Object.entries(mapping)) {
          if (entry.name && entry.name.toLowerCase().includes('roasted barley')) {
                  if (entry.name.includes('500L') || entry.name.includes('500 L')) {
                            if (entry.bc_per_oz) {
                                        console.log(`  ✗ BREAK: ${entry.name} (SKU: ${sku}) — removing pair per user instruction`);
                                        entry.bc_per_oz = null;
                            }
                  }
                          // 300L keeps whatever the discovery found (Roasted Barley - Briess matched by SKU)
                  // If it didn't match, try to find it
                  if ((entry.name.includes('300L') || entry.name.includes('300 L')) && !entry.bc_per_oz) {
                            console.log(`  → Looking up BC variant for Roasted Barley 300L (SKU: ${sku})...`);
                            const data = await bcGet(`/v3/catalog/variants?sku=${encodeURIComponent(sku)}&include_fields=id,product_id,sku,option_values`);
                            if (data && data.data && data.data.length > 0) {
                                        const best = data.data.find(v =>
                                                      (v.option_values || []).some(ov => ov.label && ov.label.includes('OUNCE'))
                                                    ) || data.data[0];
                                        mapping[sku].bc_per_oz = { product_id: best.product_id, variant_id: best.id, sku: best.sku };
                                        console.log(`  ✓ LINK: ${entry.name} → BC product ${best.product_id} / variant ${best.id}`);
                            }
                                      await sleep(200);
                  }
          }
    }
  
    console.log('  Corrections applied.\n');
  
    // Save
    fs.writeFileSync(OUTPUT, JSON.stringify(mapping, null, 2));
    console.log(`\nSaved ${Object.keys(mapping).length} entries to ${OUTPUT}`);
  
    // Summary
    const withBC = Object.values(mapping).filter(e => e.bc_per_oz).length;
    const withBags = Object.values(mapping).filter(e => e.bag_variants.length > 0).length;
    console.log(`\nFinal: ${Object.keys(mapping).length} total, ${withBC} with BC mapping, ${withBags} with bag correction`);
}

main().catch(err => { console.error('FATAL:', err.message); process.exit(1); });*/

const fetch = require('node-fetch');
const fs = require('fs');
const path = require('path');

// --- Config ---
const SQ_TOKEN = process.env.SQUARE_ACCESS_TOKEN || process.env.SQ_ACCESS_TOKEN;
const BC_TOKEN = process.env.BC_ACCESS_TOKEN;
const BC_STORE  = process.env.BC_STORE_HASH || 'h1uvrm9fjd';
const SQ_API   = 'https://connect.squareup.com/v2';
const BC_API   = `https://api.bigcommerce.com/stores/${BC_STORE}`;
const LOCATION = 'D7QJPMPVZME4K';
const BREWING_GRAINS_CATEGORY = 'CQM5GLJF3UQ6H6TUUGP5C6C2';
const MEASUREMENT_UNIT_OZ = 'XAZRNKPDPUK47YOPZO6TZG6Q';
const OUTPUT   = path.join(__dirname, 'brewing-grain-mapping.json');

if (!SQ_TOKEN) { console.error('ERROR: SQUARE_ACCESS_TOKEN required'); process.exit(1); }
if (!BC_TOKEN) { console.error('ERROR: BC_ACCESS_TOKEN required'); process.exit(1); }

const sleep = ms => new Promise(r => setTimeout(r, ms));

// --- Square helpers ---
async function sqPost(endpoint, body) {
  const res = await fetch(`${SQ_API}${endpoint}`, {
    method: 'POST', headers: { 'Authorization': `Bearer ${SQ_TOKEN}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(body)
  });
  if (!res.ok) throw new Error(`Square ${endpoint}: ${res.status} - ${await res.text()}`);
  return res.json();
}

// --- BC helpers ---
async function bcGet(urlPath) {
  const res = await fetch(`${BC_API}${urlPath}`, {
    headers: { 'X-Auth-Token': BC_TOKEN, 'Content-Type': 'application/json', 'Accept': 'application/json' }
  });
  if (res.status === 404) return null;
  if (res.status === 429) {
    const wait = parseInt(res.headers.get('X-Rate-Limit-Time-Reset-Ms') || '2000', 10);
    console.log(`  Rate limited, waiting ${wait}ms...`);
    await sleep(wait);
    return bcGet(urlPath);
  }
  if (!res.ok) throw new Error(`BC GET ${urlPath}: ${res.status} - ${await res.text()}`);
  return res.json();
}

// --- Step 1: Build mapping from Square catalog ---
async function buildMappingFromSquare() {
  console.log('=== Step 1: Querying Square catalog for Brewing Grains ===\n');

  // Search for all items in the Brewing Grains category
  let cursor = null;
  const allItems = [];
  do {
    const body = {
      object_types: ['ITEM'],
      query: {
        exact_query: { attribute_name: 'category_id', attribute_value: BREWING_GRAINS_CATEGORY }
      },
      limit: 100
    };
    if (cursor) body.cursor = cursor;
    const result = await sqPost('/catalog/search', body);
    if (result.objects) allItems.push(...result.objects);
    cursor = result.cursor;
  } while (cursor);

  console.log(`Found ${allItems.length} items in Brewing Grains category.\n`);

  const mapping = {};

  for (const item of allItems) {
    const itemName = item.item_data?.name || 'Unknown';
    const variations = item.item_data?.variations || [];

    // Identify bulk variant (has measurement_unit_id for oz) and bag variants (no measurement_unit)
    let bulkVariant = null;
    const bagVariants = [];

    for (const v of variations) {
      const vData = v.item_variation_data || {};
      const muId = vData.measurement_unit_id;

      if (muId === MEASUREMENT_UNIT_OZ) {
        bulkVariant = v;
      } else if (!muId) {
        // Check if this is a bag variant (name contains "bag" or "lbs" or weight pattern)
        const vName = (vData.name || '').toLowerCase();
        const isBag = vName.includes('bag') || vName.includes('lbs') || vName.includes('lb');
        if (isBag) {
          // Determine bag size
          let bagLbs = 50; // default
          let bagOz = 800;
          if (vName.includes('55lb') || vName.includes('55 lb')) { bagLbs = 55; bagOz = 880; }
          else if (vName.match(/50\s*lb/)) { bagLbs = 50; bagOz = 800; }
          else if (vName.match(/25\s*lb/)) { bagLbs = 25; bagOz = 400; }
          else if (vName.match(/10\s*lb/)) { bagLbs = 10; bagOz = 160; }
          // 55lbs bags pattern
          if (vName.includes('55lbs')) { bagLbs = 55; bagOz = 880; }

          bagVariants.push({
            square_variation_id: v.id,
            name: vData.name,
            bag_lbs: bagLbs,
            bag_oz: bagOz
          });
        }
      }
    }

    if (!bulkVariant) {
      console.log(`  SKIP: ${itemName} - no bulk (oz) variant found`);
      continue;
    }

    const bulkSku = bulkVariant.item_variation_data?.sku || '';
    if (!bulkSku) {
      console.log(`  WARN: ${itemName} - bulk variant has no SKU`);
      continue;
    }

    mapping[bulkSku] = {
      name: itemName,
      square_item_id: item.id,
      square_bulk_variation_id: bulkVariant.id,
      square_bulk_sku: bulkSku,
      square_location_id: LOCATION,
      bag_variants: bagVariants,
      bc_per_oz: null
    };

    const bagInfo = bagVariants.length > 0
      ? ` (${bagVariants.length} bag variants: ${bagVariants.map(b => b.bag_lbs + 'lb').join(', ')})`
      : '';
    console.log(`  ✓ ${itemName} [SKU: ${bulkSku}]${bagInfo}`);
  }

  console.log(`\nBuilt mapping with ${Object.keys(mapping).length} entries.\n`);
  return mapping;
}

// --- Step 2: Discover BC variants ---
async function discoverBCVariants(mapping) {
  console.log('=== Step 2: Discovering BigCommerce variants ===\n');

  let matched = 0, notFound = 0, ambiguous = 0;

  for (const [sku, entry] of Object.entries(mapping)) {
    // Query BC for variants matching this SKU
    const data = await bcGet(`/v3/catalog/variants?sku=${encodeURIComponent(sku)}&include_fields=id,product_id,sku,option_values`);

    if (!data || !data.data || data.data.length === 0) {
      notFound++;
      console.log(`  ✗ ${entry.name} (SKU: ${sku}) — not in BC`);
      await sleep(200);
      continue;
    }

    // Prefer variant whose option label includes "OUNCE"
    let best = data.data[0];
    if (data.data.length > 1) {
      const ozMatch = data.data.find(v =>
        (v.option_values || []).some(ov => ov.label && ov.label.includes('OUNCE'))
      );
      if (ozMatch) best = ozMatch;
      else { ambiguous++; }
    }

    mapping[sku].bc_per_oz = {
      product_id: best.product_id,
      variant_id: best.id,
      sku: best.sku
    };
    matched++;
    console.log(`  ✓ ${entry.name} → BC product ${best.product_id} / variant ${best.id}`);
    await sleep(200);
  }

  console.log(`\n=== BC Discovery Results ===`);
  console.log(`Matched: ${matched}`);
  console.log(`Not found: ${notFound}`);
  if (ambiguous > 0) console.log(`Ambiguous (used first): ${ambiguous}`);
}

// --- Main ---
async function main() {
  // Load existing mapping if it exists, otherwise build from Square
  let mapping;
  if (fs.existsSync(OUTPUT)) {
    console.log('Found existing brewing-grain-mapping.json, loading...\n');
    mapping = JSON.parse(fs.readFileSync(OUTPUT, 'utf8'));
    console.log(`Loaded ${Object.keys(mapping).length} entries.\n`);

    // Check if any bc_per_oz are missing
    const missing = Object.values(mapping).filter(e => !e.bc_per_oz).length;
    if (missing === 0) {
      console.log('All entries already have bc_per_oz. Nothing to do!');
      return;
    }
    console.log(`${missing} entries missing bc_per_oz, running BC discovery...\n`);
  } else {
    console.log('No existing mapping found. Building from Square catalog...\n');
    mapping = await buildMappingFromSquare();
  }

  // Discover BC variants
  await discoverBCVariants(mapping);

  // Save
  fs.writeFileSync(OUTPUT, JSON.stringify(mapping, null, 2));
  console.log(`\nSaved ${Object.keys(mapping).length} entries to ${OUTPUT}`);

  // Summary
  const withBC = Object.values(mapping).filter(e => e.bc_per_oz).length;
  const withBags = Object.values(mapping).filter(e => e.bag_variants.length > 0).length;
  console.log(`\nFinal: ${Object.keys(mapping).length} total, ${withBC} with BC mapping, ${withBags} with bag correction`);
}

main().catch(err => { console.error('FATAL:', err.message); process.exit(1); });
