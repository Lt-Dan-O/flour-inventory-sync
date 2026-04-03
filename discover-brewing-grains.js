#!/usr/bin/env node
/**
 * BC Discovery Script for Brewing Grain Mapping
 *
 * Searches BigCommerce for brewing grain products and matches them to Square
 * bulk variants by SKU. Populates the bc_per_oz field in brewing-grain-mapping.json.
 *
 * Usage:
 *   BC_ACCESS_TOKEN=xxx node discover-brewing-grains.js
 *
 * Or if running on Render where env vars are already set:
 *   node discover-brewing-grains.js
 *
 * Strategy: For each Square SKU in the mapping, query BC's "get variants by SKU"
 * endpoint to find the matching variant directly (much faster than scanning all products).
 */

const fetch = require('node-fetch');
const fs = require('fs');
const path = require('path');

const BC_STORE_HASH   = process.env.BC_STORE_HASH || 'h1uvrm9fjd';
const BC_ACCESS_TOKEN = process.env.BC_ACCESS_TOKEN;
const BC_API = `https://api.bigcommerce.com/stores/${BC_STORE_HASH}`;

if (!BC_ACCESS_TOKEN) {
  console.error('ERROR: BC_ACCESS_TOKEN environment variable is required.');
  console.error('Usage: BC_ACCESS_TOKEN=xxx node discover-brewing-grains.js');
  process.exit(1);
}

function bcHeaders() {
  return {
    'X-Auth-Token': BC_ACCESS_TOKEN,
    'Content-Type': 'application/json',
    'Accept': 'application/json'
  };
}

async function bcGet(urlPath) {
  const res = await fetch(`${BC_API}${urlPath}`, { headers: bcHeaders() });
  if (res.status === 404) return null;
  if (res.status === 429) {
    // Rate limited — wait and retry
    const retryAfter = parseInt(res.headers.get('X-Rate-Limit-Time-Reset-Ms') || '1500', 10);
    console.log(`  Rate limited, waiting ${retryAfter}ms...`);
    await new Promise(r => setTimeout(r, retryAfter));
    return bcGet(urlPath);
  }
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`BC GET ${urlPath}: ${res.status} - ${text}`);
  }
  return res.json();
}

async function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function discoverBrewingGrains() {
  // Load the existing mapping (Square-side data)
  const mappingPath = path.join(__dirname, 'brewing-grain-mapping.json');
  if (!fs.existsSync(mappingPath)) {
    console.error('ERROR: brewing-grain-mapping.json not found in', __dirname);
    process.exit(1);
  }

  const mapping = JSON.parse(fs.readFileSync(mappingPath, 'utf8'));
  const entries = Object.entries(mapping);

  console.log(`Loaded ${entries.length} grain entries from mapping.\n`);
  console.log('Looking up each SKU in BigCommerce...\n');

  let matched = 0;
  let noVariant = [];
  let notFound = [];
  let alreadySet = 0;

  for (const [key, entry] of entries) {
    const sku = entry.square_bulk_sku;
    if (!sku) {
      notFound.push({ key, name: entry.name, reason: 'No Square SKU' });
      continue;
    }

    // Skip if bc_per_oz is already populated
    if (entry.bc_per_oz) {
      alreadySet++;
      console.log(`  – ${entry.name} (SKU: ${sku}) — already mapped`);
      continue;
    }

    // Query BC for variants matching this SKU
    // The V3 catalog search endpoint can filter by SKU
    const data = await bcGet(`/v3/catalog/variants?sku=${encodeURIComponent(sku)}&include_fields=id,product_id,sku,option_values`);

    if (!data || !data.data || data.data.length === 0) {
      notFound.push({ key, name: entry.name, sku });
      console.log(`  ✗ ${entry.name} (SKU: ${sku}) — not found in BC`);
      await sleep(200); // gentle rate limiting
      continue;
    }

    // Find the variant — if multiple results, prefer one whose option label includes "OUNCE"
    let bestVariant = data.data[0];
    if (data.data.length > 1) {
      const ozMatch = data.data.find(v =>
        (v.option_values || []).some(ov => ov.label && ov.label.includes('OUNCE'))
      );
      if (ozMatch) bestVariant = ozMatch;
    }

    // Check that this variant is the "by the OUNCE" variant
    const labels = (bestVariant.option_values || []).map(ov => ov.label).join(' ');
    const isOzVariant = labels.includes('OUNCE') || data.data.length === 1;

    if (!isOzVariant) {
      noVariant.push({
        key, name: entry.name, sku,
        bc_product_id: bestVariant.product_id,
        labels
      });
      console.log(`  ? ${entry.name} (SKU: ${sku}) — found but not OUNCE variant (labels: ${labels})`);
      await sleep(200);
      continue;
    }

    // Populate bc_per_oz
    mapping[key].bc_per_oz = {
      product_id: bestVariant.product_id,
      variant_id: bestVariant.id,
      sku: bestVariant.sku
    };
    matched++;
    console.log(`  ✓ ${entry.name} → BC product ${bestVariant.product_id} / variant ${bestVariant.id} (SKU: ${bestVariant.sku})`);

    await sleep(200); // gentle rate limiting
  }

  // Results summary
  console.log(`\n=== Discovery Results ===`);
  console.log(`Already mapped: ${alreadySet}`);
  console.log(`Newly matched: ${matched}`);
  console.log(`Not found in BC: ${notFound.length}`);
  console.log(`Found but not OUNCE variant: ${noVariant.length}`);

  if (notFound.length > 0) {
    console.log(`\n--- Not Found in BC (${notFound.length}) ---`);
    for (const u of notFound) {
      console.log(`  ${u.name} (SKU: ${u.sku || 'NULL'})`);
    }
  }

  if (noVariant.length > 0) {
    console.log(`\n--- Found but Not OUNCE Variant (${noVariant.length}) ---`);
    for (const u of noVariant) {
      console.log(`  ${u.name} (SKU: ${u.sku}, BC Product: ${u.bc_product_id}, Labels: ${u.labels})`);
    }
  }

  // Save updated mapping
  fs.writeFileSync(mappingPath, JSON.stringify(mapping, null, 2));
  console.log(`\nUpdated mapping saved to ${mappingPath}`);
}

discoverBrewingGrains().catch(err => {
  console.error('Discovery failed:', err.message);
  process.exit(1);
});
