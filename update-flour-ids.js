/**
 * Auto-discover flour product/variant IDs from BigCommerce after CSV import.
 *
 * Run this ONCE after importing the flour products CSV into BigCommerce.
 * It searches for all products with FM- prefix SKUs and updates grain-mapping.json
 * with the correct BC product and variant IDs for flour items.
 *
 * Usage:
 *   BC_ACCESS_TOKEN=xxx node update-flour-ids.js
 */

const fetch = require('node-fetch');
const fs = require('fs');
const path = require('path');

const STORE_HASH = process.env.BC_STORE_HASH || 'h1uvrm9fjd';
const ACCESS_TOKEN = process.env.BC_ACCESS_TOKEN;
const BC_API = `https://api.bigcommerce.com/stores/${STORE_HASH}`;

if (!ACCESS_TOKEN) {
  console.error('ERROR: Set BC_ACCESS_TOKEN');
  process.exit(1);
}

function headers() {
  return {
    'X-Auth-Token': ACCESS_TOKEN,
    'Content-Type': 'application/json',
    'Accept': 'application/json'
  };
}

async function getAllProducts() {
  const products = [];
  let page = 1;
  while (true) {
    const res = await fetch(`${BC_API}/v3/catalog/products?limit=50&page=${page}&include=variants`, { headers: headers() });
    if (!res.ok) throw new Error(`Failed: ${res.status}`);
    const data = await res.json();
    products.push(...data.data);
    if (!data.meta.pagination.links.next) break;
    page++;
  }
  return products;
}

async function run() {
  console.log('Fetching all BC products with variants...');
  const products = await getAllProducts();
  console.log(`Found ${products.length} total products`);

  // Find flour products (variants with FM- prefix SKUs)
  const flourSkuMap = {};  // FM-SKU → { bc_product_id, bc_variant_id }

  for (const product of products) {
    if (!product.variants) continue;
    for (const variant of product.variants) {
      if (variant.sku && variant.sku.startsWith('FM-')) {
        flourSkuMap[variant.sku] = {
          bc_product_id: product.id,
          bc_variant_id: variant.id
        };
      }
    }
  }

  console.log(`Found ${Object.keys(flourSkuMap).length} flour variants with FM- prefix`);

  // Load and update grain-mapping.json
  const mappingPath = path.join(__dirname, 'grain-mapping.json');
  const mapping = JSON.parse(fs.readFileSync(mappingPath, 'utf8'));

  let updated = 0;
  for (const [grainSku, entry] of Object.entries(mapping)) {
    if (!entry.bc_flour || !entry.bc_flour.variants) continue;

    for (const [lbs, vData] of Object.entries(entry.bc_flour.variants)) {
      const fmSku = vData.sku;
      const found = flourSkuMap[fmSku];
      if (found) {
        vData.bc_product_id = found.bc_product_id;
        vData.bc_variant_id = found.bc_variant_id;
        updated++;
        console.log(`  ${fmSku} → product ${found.bc_product_id}, variant ${found.bc_variant_id}`);
      } else {
        console.warn(`  WARNING: No BC product found for flour SKU "${fmSku}"`);
      }
    }
  }

  // Save updated mapping
  fs.writeFileSync(mappingPath, JSON.stringify(mapping, null, 2));
  console.log(`\nUpdated ${updated} flour variant IDs in grain-mapping.json`);
}

run().catch(e => console.error(e));
