/**
 * Daily Sync Report — sends an email summarizing inventory changes
 * made by the sync service (Connect to IMB BigCommerce) yesterday.
 *
 * Self-contained module: reads env vars and mapping files directly.
 * Called from server.js via: require('./daily-report')(app)
 */

const fetch = require('node-fetch');
const nodemailer = require('nodemailer');

// ── Config from environment ────────────────────────────────────────────────
const SQ_API        = 'https://connect.squareup.com/v2';
const SQ_ACCESS     = process.env.SQ_ACCESS_TOKEN;
const SQ_LOCATION   = process.env.SQ_LOCATION_ID || 'D7QJPMPVZME4K';
const EMAIL_USER    = process.env.EMAIL_USER || '';
const EMAIL_PASS    = process.env.EMAIL_PASS || '';
const REPORT_TO     = process.env.REPORT_TO  || 'dannickels4@yahoo.com';
const SYNC_APP_ID   = 'sq0idp-5SrSlq7mn0lCWZQKk3G_cg';

function sqHeaders() {
  return {
    'Authorization': `Bearer ${SQ_ACCESS}`,
    'Content-Type':  'application/json',
    'Square-Version': '2025-01-23'
  };
}

// ── Email transporter ──────────────────────────────────────────────────────
let transporter = null;
if (EMAIL_USER && EMAIL_PASS) {
  transporter = nodemailer.createTransport({
    service: 'gmail',
    auth: { user: EMAIL_USER, pass: EMAIL_PASS }
  });
  console.log(`[REPORT] Email configured: ${EMAIL_USER} → ${REPORT_TO}`);
} else {
  console.log('[REPORT] Email not configured (set EMAIL_USER + EMAIL_PASS)');
}

// ── Load mapping files for product name lookups ────────────────────────────
let grainMapping = {}, coffeeMapping = {}, millMapping = {}, brewGrainMapping = {};
try { grainMapping     = require('./grain-mapping.json');          } catch (e) {}
try { coffeeMapping    = require('./coffee-mapping.json');         } catch (e) {}
try { millMapping      = require('./mill-mapping.json');           } catch (e) {}
try { brewGrainMapping = require('./brewing-grain-mapping.json');  } catch (e) {}

function buildNameLookup() {
  const names = {};

  for (const [, entry] of Object.entries(grainMapping)) {
    if (entry.square_variation_id) names[entry.square_variation_id] = entry.name;
    if (entry.flour_variants) {
      for (const fv of entry.flour_variants) {
        names[fv.square_variation_id] = `${entry.name} - ${fv.name}`;
      }
    }
  }
  for (const [, entry] of Object.entries(coffeeMapping)) {
    if (entry.square_per_oz_variation_id)
      names[entry.square_per_oz_variation_id] = `${entry.name} (per oz)`;
  }
  for (const [, entry] of Object.entries(millMapping)) {
    if (entry.square_variation_id) names[entry.square_variation_id] = entry.name;
  }
  for (const [, entry] of Object.entries(brewGrainMapping)) {
    if (entry.square_bulk_variation_id)
      names[entry.square_bulk_variation_id] = `${entry.name} (bulk oz)`;
    if (entry.bag_variants) {
      for (const bag of entry.bag_variants)
        names[bag.square_variation_id] = `${entry.name} - ${bag.name}`;
    }
  }
  return names;
}

// ── Central Time helpers ───────────────────────────────────────────────────
function ctOffset(date) {
  const y = date.getUTCFullYear();
  const mar1  = new Date(Date.UTC(y, 2, 1));
  const mar2Sun = new Date(Date.UTC(y, 2, 8 + (7 - mar1.getUTCDay()) % 7, 8));
  const nov1  = new Date(Date.UTC(y, 10, 1));
  const nov1Sun = new Date(Date.UTC(y, 10, 1 + (7 - nov1.getUTCDay()) % 7, 7));
  return (date >= mar2Sun && date < nov1Sun) ? 5 : 6;   // CDT=5, CST=6
}

function yesterdayRange() {
  const now = new Date();
  const off = ctOffset(now);
  const todayMid = new Date(now);
  todayMid.setUTCHours(off, 0, 0, 0);
  if (now < todayMid) todayMid.setUTCDate(todayMid.getUTCDate() - 1);
  const yestMid = new Date(todayMid);
  yestMid.setUTCDate(yestMid.getUTCDate() - 1);
  return { after: yestMid.toISOString(), before: todayMid.toISOString(), offset: off };
}

function formatDate(isoStr, off) {
  const d = new Date(new Date(isoStr).getTime() + off * 3600000);
  const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
  return `${months[d.getUTCMonth()]} ${d.getUTCDate()}, ${d.getUTCFullYear()}`;
}

// ── Core report logic ──────────────────────────────────────────────────────
async function generateAndSendReport() {
  if (!transporter) return { sent: false, reason: 'email not configured' };

  console.log('\n=== Generating Daily Sync Report ===');
  try {
    const { after, before, offset } = yesterdayRange();
    const dateStr = formatDate(after, offset);
    console.log(`[REPORT] Range: ${after} → ${before}  (${dateStr})`);

    // 1. Fetch all inventory changes for yesterday
    let allChanges = [], cursor = null;
    do {
      const body = {
        location_ids: [SQ_LOCATION],
        updated_after: after,
        updated_before: before,
        types: ['PHYSICAL_COUNT', 'ADJUSTMENT'],
        limit: 100
      };
      if (cursor) body.cursor = cursor;

      const res = await fetch(`${SQ_API}/inventory/changes/batch-retrieve`, {
        method: 'POST', headers: sqHeaders(), body: JSON.stringify(body)
      });
      if (!res.ok) throw new Error(`Square API ${res.status}: ${await res.text()}`);
      const data = await res.json();
      if (data.changes) allChanges = allChanges.concat(data.changes);
      cursor = data.cursor || null;
    } while (cursor);

    // 2. Keep only sync-service changes
    const syncChanges = allChanges.filter(c => {
      const src = (c.physical_count || c.adjustment)?.source;
      return src && src.application_id === SYNC_APP_ID;
    });
    console.log(`[REPORT] ${allChanges.length} total changes, ${syncChanges.length} from sync`);

    // 3. Look up product names
    const names = buildNameLookup();

    // For unknown IDs, batch-fetch from catalog
    const unknownIds = [...new Set(
      syncChanges
        .map(c => (c.physical_count || c.adjustment).catalog_object_id)
        .filter(id => id && !names[id])
    )];
    for (let i = 0; i < unknownIds.length; i += 100) {
      const batch = unknownIds.slice(i, i + 100);
      try {
        const r = await fetch(`${SQ_API}/catalog/batch-retrieve`, {
          method: 'POST', headers: sqHeaders(),
          body: JSON.stringify({ object_ids: batch, include_related_objects: true })
        });
        if (r.ok) {
          const d = await r.json();
          const itemNames = {};
          (d.related_objects || []).forEach(o => {
            if (o.type === 'ITEM') itemNames[o.id] = o.item_data?.name || o.id;
          });
          (d.objects || []).forEach(o => {
            const vn = o.item_variation_data?.name || '';
            const iName = itemNames[o.item_variation_data?.item_id] || 'Unknown';
            names[o.id] = vn ? `${iName} - ${vn}` : iName;
          });
        }
      } catch (e) { /* skip catalog lookup failures */ }
    }

    // 4. Format lines
    const lines = syncChanges.map(c => {
      const isPhys = c.type === 'PHYSICAL_COUNT';
      const d = isPhys ? c.physical_count : c.adjustment;
      const name = names[d.catalog_object_id] || d.catalog_object_id;
      if (isPhys) return `  ${name}: set to ${d.quantity}`;
      if (d.from_state === 'NONE' && d.to_state === 'IN_STOCK') return `  ${name}: +${d.quantity}`;
      if (d.to_state === 'SOLD' || d.to_state === 'WASTE') return `  ${name}: -${d.quantity}`;
      return `  ${name}: ${d.quantity} (${d.from_state} → ${d.to_state})`;
    });

    // 5. Build & send email
    const subject = `IMB Sync Changes - ${dateStr}`;
    let body;
    if (syncChanges.length === 0) {
      body = `No sync changes yesterday (${dateStr}).`;
    } else {
      const products = new Set(syncChanges.map(c => (c.physical_count || c.adjustment).catalog_object_id));
      body  = `Inventory changes made by the sync service on ${dateStr}:\n\n`;
      body += lines.join('\n');
      body += `\n\n${syncChanges.length} change(s) across ${products.size} product(s).`;
    }

    await transporter.sendMail({ from: EMAIL_USER, to: REPORT_TO, subject, text: body });
    console.log(`[REPORT] Sent "${subject}" → ${REPORT_TO}`);
    return { sent: true, subject, changes: syncChanges.length };

  } catch (e) {
    console.error(`[REPORT] ERROR: ${e.message}`);
    return { sent: false, error: e.message };
  }
}

// ── Scheduler (6 AM CT) ───────────────────────────────────────────────────
function startScheduler() {
  let lastDate = null;
  setInterval(() => {
    if (!transporter) return;
    const now = new Date();
    const off = ctOffset(now);
    const ctHour = (now.getUTCHours() + 24 - off) % 24;
    const ctDay  = new Date(now.getTime() - off * 3600000).toISOString().slice(0, 10);
    if (ctHour === 6 && now.getUTCMinutes() < 5 && lastDate !== ctDay) {
      lastDate = ctDay;
      console.log('[REPORT] 6 AM CT — triggering daily report');
      generateAndSendReport().catch(e => console.error(`[REPORT] ${e.message}`));
    }
  }, 60000);
  console.log('[REPORT] Scheduler active (6 AM CT daily)');
}

// ── Export: call from server.js as  require('./daily-report')(app)  ────────
module.exports = function (app) {
  // GET + POST endpoint for manual testing
  app.get('/daily-report',  async (_req, res) => res.json(await generateAndSendReport()));
  app.post('/daily-report', async (_req, res) => res.json(await generateAndSendReport()));
  // Start cron
  startScheduler();
};
