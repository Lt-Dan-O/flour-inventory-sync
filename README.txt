UNIFIED GRAIN-FLOUR INVENTORY SYNC
===================================

Square is the ABSOLUTE SOURCE OF TRUTH for grain inventory.
The per-lb count in Square = total pounds available.

All BigCommerce variant levels (grain AND flour) are calculated from that total,
minus any lbs reserved by pending/unshipped BC orders.

  Example: Warthog has 22 lbs in Square, 0 lbs reserved
    -> Grain: 1 LB=22, 5 LB=4, 10 LB=2, 25 LB=0
    -> Flour: 1 LB=22, 5 LB=4, 10 LB=2

  Example: Warthog has 22 lbs in Square, 5 lbs reserved (pending order for 5 LB bag)
    -> Available = 17 lbs
    -> Grain: 1 LB=17, 5 LB=3, 10 LB=1, 25 LB=0
    -> Flour: 1 LB=17, 5 LB=3, 10 LB=1


THREE SYNC TRIGGERS
-------------------

  1. BC ORDER WEBHOOK (POST /webhooks/order-created)
     When a customer places an order on BigCommerce (grain OR flour),
     this deducts the equivalent lbs from Square, then recalculates
     all BC variant levels.

  2. SQUARE INVENTORY WEBHOOK (POST /webhooks/square-inventory)
     When Square inventory changes (manual adjustment, POS sale, etc.),
     this recalculates all BC variant levels from the new Square count.

  3. 15-MINUTE RECONCILIATION (automatic + GET/POST /reconcile)
     Safety net that re-syncs ALL 25 grain products every 15 minutes.
     Also runs 30 seconds after server boot.


SETUP (~20 minutes)
-------------------

1. GET API TOKENS

   BigCommerce:
     - Admin > Settings > API > API Accounts > Create API Account
     - Name: "Inventory Sync"
     - Scopes: Orders (read-only), Products (modify)
     - Save and copy the Access Token

   Square:
     - developer.squareup.com > Applications > your app > Credentials
     - Copy the Access Token (Production)
     - Needs: INVENTORY_READ, INVENTORY_WRITE permissions

2. DEPLOY TO RENDER.COM

   - Push this folder to a GitHub repo
   - Go to render.com > New Web Service > Connect repo
   - Set environment variables:
       BC_STORE_HASH     = h1uvrm9fjd
       BC_ACCESS_TOKEN   = (your BigCommerce token)
       SQ_ACCESS_TOKEN   = (your Square token)
       SQ_LOCATION_ID    = D7QJPMPVZME4K
       RECONCILE_MINS    = 15
   - Deploy. Note the URL (e.g., https://flour-sync.onrender.com)

3. REGISTER WEBHOOKS

   After deploying, run:

     BC_ACCESS_TOKEN=xxx SQ_ACCESS_TOKEN=xxx \
     WEBHOOK_URL=https://flour-sync.onrender.com \
     node register-webhook.js

   This registers both the BC and Square webhooks.

4. TEST IT

   - Health check: curl https://flour-sync.onrender.com/health
   - Manual sync: curl https://flour-sync.onrender.com/reconcile
   - Place a test order on BigCommerce, check server logs


FILES
-----

  server.js             - Main sync service (3 handlers + reconciliation + auto-discovery)
  grain-mapping.json    - Mapping: Square IDs + BC grain IDs (flour IDs auto-discovered)
  sku-mapping.json      - Legacy flour→grain mapping (kept for reference)
  register-webhook.js   - Register BC + Square webhooks
  update-flour-ids.js   - Optional: manually update flour IDs in grain-mapping.json
  test-local.js         - Local testing (npm start, then node test-local.js)
  package.json          - Dependencies (express, node-fetch)
  .env.example          - Environment variable template

  NOTE: Flour variant IDs are AUTO-DISCOVERED at startup by querying BC
  category 557 (Freshly Milled Flour). No manual mapping step needed.
  The health endpoint shows how many flour variants were discovered.


RENDER.COM FREE TIER NOTES
---------------------------

  - Free tier services spin down after 15 min of inactivity
  - When a webhook arrives, Render spins up the service (~30 sec cold start)
  - BC will retry webhooks if the first attempt times out, so this is OK
  - The 15-minute reconciliation acts as a keep-alive AND a safety net
  - If you need always-on, upgrade to Render's $7/mo Starter plan


TROUBLESHOOTING
---------------

  "Square batch-retrieve-counts: 401"
    -> SQ_ACCESS_TOKEN is invalid or expired. Get a new one.

  "BC GET /v2/orders/...: 401"
    -> BC_ACCESS_TOKEN is invalid. Regenerate it.

  "No grain/flour items in order, skipping"
    -> Normal. The order had no grain or flour products.

  "variation XXX not in our mapping, skipping"
    -> Square inventory changed for a non-grain item. Normal.

  BC levels don't match expected calculation:
    -> Check pending orders: there may be reserved lbs.
    -> Trigger manual reconciliation: GET /reconcile
