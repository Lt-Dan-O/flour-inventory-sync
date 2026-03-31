/**
 * Test the unified sync service locally.
 *
 * Tests:
 *   1. BC order webhook (simulates a flour + grain order)
 *   2. Square inventory webhook (simulates inventory change)
 *   3. Manual reconciliation trigger
 *
 * Usage:
 *   Terminal 1: npm start
 *   Terminal 2: node test-local.js [bc|square|reconcile|all]
 */

const fetch = require('node-fetch');
const PORT = process.env.PORT || 3000;
const BASE = `http://localhost:${PORT}`;

// Test 1: Simulate BC order with flour and grain items
async function testBcOrder() {
  console.log('=== Test: BC Order Webhook ===');
  const payload = {
    scope: 'store/order/created',
    store_id: 'h1uvrm9fjd',
    data: { type: 'order', id: 99999 },
    hash: 'test-hash',
    created_at: new Date().toISOString(),
    producer: 'stores/h1uvrm9fjd'
  };

  try {
    const res = await fetch(`${BASE}/webhooks/order-created`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
    console.log(`Response: ${res.status}`);
    const body = await res.json();
    console.log(`Body: ${JSON.stringify(body)}`);
    console.log('(Check server logs - will fail on order #99999 unless it exists)\n');
  } catch (e) {
    console.error(`Failed: ${e.message}\n`);
  }
}

// Test 2: Simulate Square inventory change (Warthog per-lb variation)
async function testSquareWebhook() {
  console.log('=== Test: Square Inventory Webhook ===');
  const payload = {
    merchant_id: 'test',
    type: 'inventory.count.updated',
    event_id: 'test-event',
    data: {
      type: 'inventory.count.updated',
      id: 'HH3KMOSQRZXL7NEDRYCJLNVT',  // Warthog per-lb variation
      object: {
        inventory_counts: [{
          catalog_object_id: 'HH3KMOSQRZXL7NEDRYCJLNVT',
          location_id: 'D7QJPMPVZME4K',
          state: 'IN_STOCK',
          quantity: '22'
        }]
      }
    }
  };

  try {
    const res = await fetch(`${BASE}/webhooks/square-inventory`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
    console.log(`Response: ${res.status}`);
    const body = await res.json();
    console.log(`Body: ${JSON.stringify(body)}`);
    console.log('(Check server logs for Warthog recalculation)\n');
  } catch (e) {
    console.error(`Failed: ${e.message}\n`);
  }
}

// Test 3: Trigger manual reconciliation
async function testReconcile() {
  console.log('=== Test: Manual Reconciliation ===');
  try {
    const res = await fetch(`${BASE}/reconcile`);
    console.log(`Response: ${res.status}`);
    const body = await res.json();
    console.log(`Body: ${JSON.stringify(body)}`);
    console.log('(Check server logs for full reconciliation output)\n');
  } catch (e) {
    console.error(`Failed: ${e.message}\n`);
  }
}

// Test 4: Health check
async function testHealth() {
  console.log('=== Test: Health Check ===');
  try {
    const res = await fetch(`${BASE}/health`);
    const body = await res.json();
    console.log(JSON.stringify(body, null, 2));
    console.log('');
  } catch (e) {
    console.error(`Failed: ${e.message}\n`);
  }
}

async function run() {
  const test = process.argv[2] || 'all';

  await testHealth();

  if (test === 'bc' || test === 'all') await testBcOrder();
  if (test === 'square' || test === 'all') await testSquareWebhook();
  if (test === 'reconcile' || test === 'all') await testReconcile();
}

run();
