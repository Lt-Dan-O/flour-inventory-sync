/**
 * Daily Sync Report — sends an email summarizing inventory changes
 * made BY the sync service (webhooks + reconciliation) from yesterday.
 *
 * Only includes changes where source.application_id matches our app.
 * POS sales, manual dashboard edits, etc. are excluded.
 *
 * Uses Resend API (HTTPS) to send email. No SMTP needed.
 * Called from server.js via: require('./daily-report')(app)
 */

const fs = require('fs');
const path = require('path');
const fetch = require('node-fetch');

// ── Config from environment ────────────────────────────────────────────────
const SQ_API        = 'https://connect.squareup.com/v2';
const SQ_ACCESS     = process.env.SQ_ACCESS_TOKEN;
const SQ_LOCATION   = process.env.SQ_LOCATION_ID || 'D7QJPMPVZME4K';
const RESEND_API_KEY = process.env.RESEND_API_KEY || '';
const REPORT_FROM   = process.env.REPORT_FROM || 'onboarding@resend.dev';
const REPORT_TO     = process.env.REPORT_TO   || 'dannickels4@yahoo.com';
const SYNC_APP_ID   = 'sq0idp-5SrSlq7mn0lCWZQKk3G_cg';

// File-based dedup — survives in-process restarts on Render
const LAST_SENT_FILE = path.join(__dirname, '.last-report-date');

if (RESEND_API_KEY) {
  console.log(`[REPORT] Email configured via Resend → ${REPORT_TO}`);
} else {
  console.log('[REPORT] Email not configured (set RESEND_API_KEY)');
}

function sqHeaders() {
  return {
    'Authorization': `Bearer ${SQ_ACCESS}`,
    'Content-Type':  'application/json',
    'Square-Version': '2025-01-23'
  };
}

// ── Send email via Resend HTTPS API ────────────────────────────────────────
async function sendEmail(to, subject, body) {
  const recipients = typeof to === 'string' ? to.split(',').map(s => s.trim()) : to;
  const res = await fetch('https://api.resend.com/emails', {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${RESEND_API_KEY}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      from: REPORT_FROM,
      to: recipients,
      subject: subject,
      text: body
    })
  });
  if (!res.ok) {
    const err = await res.text();
    throw new Error(`Resend API ${res.status}: ${err}`);
  }
  return res.json();
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
      for (const fv of entry.flour_variants)
        names[fv.square_variation_id] = `${entry.name} - ${fv.name}`;
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
  return (date >= mar2Sun && date < nov1Sun) ? 5 : 6;
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

// ── Dedup helpers ──────────────────────────────────────────────────────────
function alreadySentToday(ctDay) {
  try {
    const last = fs.readFileSync(LAST_SENT_FILE, 'utf8').trim();
    return last === ctDay;
  } catch (e) { return false; }
}

function markSentToday(ctDay) {
  try { fs.writeFileSync(LAST_SENT_FILE, ctDay, 'utf8'); } catch (e) {
    console.error(`[REPORT] Could not write dedup file: ${e.message}`);
  }
}

// ── Core report logic ──────────────────────────────────────────────────────
async function generateAndSendReport(opts = {}) {
  if (!RESEND_API_KEY) return { sent: false, reason: 'email not configured' };

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

    console.log(`[REPORT] ${allChanges.length} total inventory changes found`);

    // 2. Filter to ONLY sync-service changes
    function isSyncChange(change) {
      const d = change.physical_count || change.adjustment;
      const src = d?.source;
      return src && src.application_id === SYNC_APP_ID;
    }

    const syncChanges = allChanges.filter(isSyncChange);
    const otherCount  = allChanges.length - syncChanges.length;

    console.log(`[REPORT] ${syncChanges.length} sync-service changes, ${otherCount} other (excluded)`);

    // 3. Look up product names for sync changes only
    const names = buildNameLookup();
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
      } catch (e) { /* skip */ }
    }

    // 4. Group sync changes by type (physical count vs adjustment)
    const physCounts = syncChanges.filter(c => c.type === 'PHYSICAL_COUNT');
    const adjustments = syncChanges.filter(c => c.type === 'ADJUSTMENT');

    // 5. Format each change line
    function formatChange(c) {
      const isPhys = c.type === 'PHYSICAL_COUNT';
      const d = isPhys ? c.physical_count : c.adjustment;
      const name = names[d.catalog_object_id] || d.catalog_object_id;
      if (isPhys) return `  ${name}: set to ${d.quantity}`;
      if (d.from_state === 'NONE' && d.to_state === 'IN_STOCK') return `  ${name}: +${d.quantity}`;
      if (d.to_state === 'SOLD' || d.to_state === 'WASTE') return `  ${name}: -${d.quantity}`;
      return `  ${name}: ${d.quantity} (${d.from_state} → ${d.to_state})`;
    }

    // 6. Build & send email
    const subject = `IMB Sync Report - ${dateStr}`;
    let emailBody;

    if (syncChanges.length === 0) {
      emailBody = `No sync-service inventory changes yesterday (${dateStr}).\n`;
      emailBody += `(${otherCount} other change(s) from POS/Dashboard excluded)`;
    } else {
      const products = new Set(syncChanges.map(c => (c.physical_count || c.adjustment).catalog_object_id));
      emailBody = `Sync service activity on ${dateStr}:\n`;
      emailBody += `${syncChanges.length} change(s) across ${products.size} product(s)\n`;

      if (physCounts.length > 0) {
        emailBody += `\n--- Inventory Sets (${physCounts.length}) ---\n`;
        emailBody += physCounts.map(formatChange).join('\n') + '\n';
      }

      if (adjustments.length > 0) {
        emailBody += `\n--- Adjustments (${adjustments.length}) ---\n`;
        emailBody += adjustments.map(formatChange).join('\n') + '\n';
      }

      emailBody += `\n(${otherCount} other change(s) from POS/Dashboard excluded)`;
    }

    await sendEmail(REPORT_TO, subject, emailBody);
    console.log(`[REPORT] Sent "${subject}" → ${REPORT_TO}`);
    return { sent: true, subject, syncChanges: syncChanges.length, excluded: otherCount };

  } catch (e) {
    console.error(`[REPORT] ERROR: ${e.message}`);
    return { sent: false, error: e.message };
  }
}

// ── Scheduler (7 AM CT) ───────────────────────────────────────────────────
function startScheduler() {
  setInterval(() => {
    if (!RESEND_API_KEY) return;
    const now = new Date();
    const off = ctOffset(now);
    const ctHour = (now.getUTCHours() + 24 - off) % 24;
    const ctDay  = new Date(now.getTime() - off * 3600000).toISOString().slice(0, 10);

    if (ctHour === 7 && now.getUTCMinutes() < 5) {
      // File-based dedup — prevents double-send on Render restarts
      if (alreadySentToday(ctDay)) return;
      markSentToday(ctDay);
      console.log('[REPORT] 7 AM CT — triggering daily sync report');
      generateAndSendReport().catch(e => console.error(`[REPORT] ${e.message}`));
    }
  }, 60000);
  console.log('[REPORT] Scheduler active (7 AM CT daily)');
}

// ── Export ──────────────────────────────────────────────────────────────────
module.exports = function (app) {
  // Manual trigger endpoints (for testing / on-demand)
  app.get('/daily-report',  async (_req, res) => res.json(await generateAndSendReport()));
  app.post('/daily-report', async (_req, res) => res.json(await generateAndSendReport()));
  startScheduler();
};
