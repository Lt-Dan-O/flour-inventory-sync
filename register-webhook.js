/**
 * Register webhooks with BigCommerce and Square.
 *
 * Usage:
 *   BC_ACCESS_TOKEN=xxx SQ_ACCESS_TOKEN=xxx WEBHOOK_URL=https://your-app.com node register-webhook.js
 *
 * This registers:
 *   1. BC store/order/created → /webhooks/order-created
 *   2. Square inventory.count.updated → /webhooks/square-inventory
 */

const fetch = require('node-fetch');

const STORE_HASH     = process.env.BC_STORE_HASH || 'h1uvrm9fjd';
const BC_TOKEN       = process.env.BC_ACCESS_TOKEN;
const SQ_TOKEN       = process.env.SQ_ACCESS_TOKEN;
const WEBHOOK_URL    = process.env.WEBHOOK_URL;  // e.g., https://flour-sync.onrender.com

if (!WEBHOOK_URL) {
  console.error('ERROR: Set WEBHOOK_URL (e.g., https://flour-sync.onrender.com)');
  process.exit(1);
}

// ── BigCommerce Webhook ─────────────────────────────────────────────────────
async function registerBcWebhook() {
  if (!BC_TOKEN) {
    console.warn('SKIP: No BC_ACCESS_TOKEN, skipping BigCommerce webhook');
    return;
  }

  const url = `https://api.bigcommerce.com/stores/${STORE_HASH}/v3/hooks`;
  const body = {
    scope: 'store/order/created',
    destination: `${WEBHOOK_URL}/webhooks/order-created`,
    is_active: true,
    headers: {}
  };

  console.log('Registering BigCommerce webhook...');
  console.log(`  Scope: ${body.scope}`);
  console.log(`  Destination: ${body.destination}`);

  const res = await fetch(url, {
    method: 'POST',
    headers: {
      'X-Auth-Token': BC_TOKEN,
      'Content-Type': 'application/json',
      'Accept': 'application/json'
    },
    body: JSON.stringify(body)
  });

  const data = await res.json();
  if (res.ok) {
    console.log(`  ✓ Registered! ID: ${data.data.id}\n`);
  } else {
    console.error(`  ✗ Failed:`, JSON.stringify(data, null, 2), '\n');
  }

  // List existing
  const listRes = await fetch(url, { headers: { 'X-Auth-Token': BC_TOKEN, 'Accept': 'application/json' } });
  const listData = await listRes.json();
  console.log('Existing BC webhooks:');
  for (const h of (listData.data || [])) {
    console.log(`  [${h.id}] ${h.scope} → ${h.destination} (active: ${h.is_active})`);
  }
  console.log('');
}

// ── Square Webhook ──────────────────────────────────────────────────────────
async function registerSquareWebhook() {
  if (!SQ_TOKEN) {
    console.warn('SKIP: No SQ_ACCESS_TOKEN, skipping Square webhook');
    return;
  }

  const url = 'https://connect.squareup.com/v2/webhooks/subscriptions';

  const body = {
    idempotency_key: `grain-sync-${Date.now()}`,
    subscription: {
      name: 'Grain Inventory Sync',
      event_types: ['inventory.count.updated'],
      notification_url: `${WEBHOOK_URL}/webhooks/square-inventory`,
      enabled: true
    }
  };

  console.log('Registering Square webhook...');
  console.log(`  Event: inventory.count.updated`);
  console.log(`  Destination: ${body.subscription.notification_url}`);

  const res = await fetch(url, {
    method: 'POST',
    headers: {
      'Square-Version': '2025-01-23',
      'Authorization': `Bearer ${SQ_TOKEN}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(body)
  });

  const data = await res.json();
  if (res.ok) {
    console.log(`  ✓ Registered! ID: ${data.subscription?.id}\n`);
  } else {
    console.error(`  ✗ Failed:`, JSON.stringify(data, null, 2), '\n');
  }

  // List existing
  const listRes = await fetch(url, {
    headers: {
      'Square-Version': '2025-01-23',
      'Authorization': `Bearer ${SQ_TOKEN}`,
      'Content-Type': 'application/json'
    }
  });
  const listData = await listRes.json();
  console.log('Existing Square webhooks:');
  for (const s of (listData.subscriptions || [])) {
    console.log(`  [${s.id}] ${s.event_types?.join(', ')} → ${s.notification_url} (enabled: ${s.enabled})`);
  }
  console.log('');
}

(async () => {
  await registerBcWebhook();
  await registerSquareWebhook();
  console.log('Done!');
})();
