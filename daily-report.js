/**
 * Daily Sync Report — sends an email summarizing inventory changes
 * made BY the sync service (webhooks + reconciliation) from yesterday.
 *
 * Only includes changes where source.application_id matches our app.
 * POS sales, manual dashboard edits, etc. are excluded.
 *
 * Uses Resend API (HTTPS) to send email. No SMTP needed.
 * Called from server.js via: require('./daily-report')(app)
 *
 * Schedule: 7 AM Eastern Time, once per day. Strong dedup (in-memory +
 * file-backed) protects against:
 *   - the in-process scheduler firing twice in the same minute
 *   - external pings to GET /daily-report or POST /daily-report
 *   - service restarts in the middle of the trigger window
 *
 * Manual override: GET/POST /daily-report?force=1 to re-send today.
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

// Auto-detect Render Persistent Disk at /var/data; fall back to __dirname.
// Must match the same logic used in server.js so both processes read/write
// the same files.
function pickPersistentDir() {
  try {
    if (fs.existsSync('/var/data') && fs.statSync('/var/data').isDirectory()) {
      const probe = path.join('/var/data', '.write-probe');
      fs.writeFileSync(probe, '');
      fs.unlinkSync(probe);
      return '/var/data';
    }
  } catch (e) { /* fall through */ }
  return __dirname;
}
const PERSIST_DIR = pickPersistentDir();

// File-based dedup — when on a persistent disk, survives restarts permanently
const LAST_SENT_FILE = path.join(PERSIST_DIR, '.last-report-date');

// BC-write audit log written by server.js (one JSON line per BC inventory PUT)
const SYNC_BC_LOG_FILE = path.join(PERSIST_DIR, '.sync-bc-writes.log');

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
let grainMapping = {}, coffeeMapping = {}, millMapping = {}, brewGrainMapping = {}, hopsMapping = {};
try { grainMapping     = require('./grain-mapping.json');          } catch (e) {}
try { coffeeMapping    = require('./coffee-mapping.json');         } catch (e) {}
try { millMapping      = require('./mill-mapping.json');           } catch (e) {}
try { brewGrainMapping = require('./brewing-grain-mapping.json');  } catch (e) {}
try { hopsMapping      = require('./brewing-hops-mapping.json');   } catch (e) {}

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
  for (const [, entry] of Object.entries(hopsMapping)) {
    if (entry.square_variation_id) names[entry.square_variation_id] = `${entry.name} (per oz)`;
  }
  return names;
}

// ── Eastern Time helpers ───────────────────────────────────────────────────
// US Eastern Time DST: 2nd Sunday in March → 1st Sunday in November.
// Returns hours offset from UTC: 4 during EDT, 5 during EST.
function etOffset(date) {
  const y = date.getUTCFullYear();
  const mar1    = new Date(Date.UTC(y, 2, 1));
  const mar2Sun = new Date(Date.UTC(y, 2, 8 + (7 - mar1.getUTCDay()) % 7, 7));   // 2 AM EST = 7 UTC
  const nov1    = new Date(Date.UTC(y, 10, 1));
  const nov1Sun = new Date(Date.UTC(y, 10, 1 + (7 - nov1.getUTCDay()) % 7, 6));  // 2 AM EDT = 6 UTC
  return (date >= mar2Sun && date < nov1Sun) ? 4 : 5;
}

function yesterdayRange() {
  const now = new Date();
  const off = etOffset(now);
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

// Returns the "ET day" string for a given Date (e.g. "2026-04-24").
function etDayString(date) {
  const off = etOffset(date);
  return new Date(date.getTime() - off * 3600000).toISOString().slice(0, 10);
}

// ── Dedup helpers ──────────────────────────────────────────────────────────
// In-memory primary dedup (fast, no I/O). File-backed secondary dedup
// (survives in-process restarts when the disk is persistent — on Render's
// free tier the disk is ephemeral, but the in-memory check still catches
// duplicates within a single boot, and external triggers landing within
// the same boot will be blocked).
let lastSentDay = null;

function alreadySent(etDay) {
  if (lastSentDay === etDay) return true;
  try {
    const last = fs.readFileSync(LAST_SENT_FILE, 'utf8').trim();
    if (last === etDay) {
      lastSentDay = etDay;
      return true;
    }
  } catch (e) { /* file missing — first run */ }
  return false;
}

function markSent(etDay) {
  lastSentDay = etDay;
  try { fs.writeFileSync(LAST_SENT_FILE, etDay, 'utf8'); } catch (e) {
    console.error(`[REPORT] Could not write dedup file: ${e.message}`);
  }
}

// ── Read BC-write audit log for a date range ───────────────────────────────
// Returns a list of { ts, kind, sku, name, size, product_id, variant_id, level }
function readBcWritesInRange(afterIso, beforeIso) {
  let raw = '';
  try { raw = fs.readFileSync(SYNC_BC_LOG_FILE, 'utf8'); }
  catch (e) { return []; }
  const out = [];
  for (const line of raw.split('\n')) {
    const s = line.trim();
    if (!s) continue;
    try {
      const e = JSON.parse(s);
      if (e.ts >= afterIso && e.ts < beforeIso) out.push(e);
    } catch (_) { /* skip malformed line */ }
  }
  return out;
}

// Group BC writes by product, comparing FIRST vs LAST level per variant.
// Only variants where the level actually changed are included. Variants whose
// 96-per-day reconciliation writes all hit the same value are silently
// excluded (they're noise — they confirm nothing changed).
//
// Returns a list of products with at least one changed variant. Each product
// includes:
//   { kind, sku, name, variants: [{ size, first, last, delta }], totalWrites,
//     silentWrites }   // silentWrites = writes for variants that didn't change
function summarizeBcWrites(entries) {
  // Per-variant aggregate: first, last, all writes
  const byVariant = new Map();   // key = `${product_id}::${variant_id}`
  for (const e of entries) {
    const k = `${e.product_id}::${e.variant_id}`;
    let cur = byVariant.get(k);
    if (!cur) {
      cur = {
        kind: e.kind, sku: e.sku, name: e.name, size: e.size,
        product_id: e.product_id, variant_id: e.variant_id,
        firstTs: e.ts, firstLevel: e.level,
        lastTs:  e.ts, lastLevel:  e.level,
        writes: 1,
      };
      byVariant.set(k, cur);
    } else {
      cur.writes += 1;
      if (e.ts < cur.firstTs) { cur.firstTs = e.ts; cur.firstLevel = e.level; }
      if (e.ts > cur.lastTs)  { cur.lastTs  = e.ts; cur.lastLevel  = e.level; }
    }
  }

  // Group by product (kind + sku + name) so we can show variants together
  const byProduct = new Map();
  for (const v of byVariant.values()) {
    const pk = `${v.kind}::${v.sku}::${v.name}`;
    if (!byProduct.has(pk)) {
      byProduct.set(pk, {
        kind: v.kind, sku: v.sku, name: v.name,
        variants: [],          // changed variants only
        totalWrites: 0,        // total writes (changed + silent)
        silentWrites: 0,       // writes against variants that didn't change
        changedWrites: 0,      // writes against variants that did change
      });
    }
    const bucket = byProduct.get(pk);
    bucket.totalWrites += v.writes;
    const changed = v.firstLevel !== v.lastLevel;
    if (changed) {
      bucket.changedWrites += v.writes;
      bucket.variants.push({
        size: v.size,
        first: v.firstLevel, last: v.lastLevel,
        delta: v.lastLevel - v.firstLevel,
        writes: v.writes,
      });
    } else {
      bucket.silentWrites += v.writes;
    }
  }

  // Drop products with no changed variants
  const withChanges = [...byProduct.values()].filter(p => p.variants.length > 0);

  // Sort each product's variants by size
  function sizeRank(s) {
    const m = (s || '').match(/(\d+)\s*LB/i);
    if (m) return parseInt(m[1], 10);
    if (/per oz/i.test(s)) return 0.5;
    if (/unit/i.test(s)) return 0;
    return 999;
  }
  for (const p of withChanges) {
    p.variants.sort((a, b) => sizeRank(a.size) - sizeRank(b.size));
  }

  // Sort products by kind then name
  withChanges.sort((a, b) =>
    a.kind.localeCompare(b.kind) || a.name.localeCompare(b.name));

  // Compute totals across ALL products (including ones with no change), so
  // the email can report the silent-write count
  let allTotalWrites = 0, allSilentWrites = 0, allChangedWrites = 0;
  let totalProducts = byProduct.size;
  let totalVariants = byVariant.size;
  for (const p of byProduct.values()) {
    allTotalWrites   += p.totalWrites;
    allSilentWrites  += p.silentWrites;
    allChangedWrites += p.changedWrites;
  }

  return {
    changedProducts: withChanges,
    totals: {
      products: totalProducts, variants: totalVariants,
      totalWrites: allTotalWrites,
      changedWrites: allChangedWrites,
      silentWrites: allSilentWrites,
    },
  };
}

// ── Core report logic ──────────────────────────────────────────────────────
async function generateAndSendReport(opts = {}) {
  if (!RESEND_API_KEY) return { sent: false, reason: 'email not configured' };

  const force = !!opts.force;
  const etDay = etDayString(new Date());

  if (!force && alreadySent(etDay)) {
    console.log(`[REPORT] Already sent for ${etDay} — skipping (pass force=true to override)`);
    return { sent: false, reason: 'already_sent_today', etDay };
  }

  // Mark sent BEFORE doing the work, so a slow Square fetch can't be
  // double-triggered if a second request lands while the first is running.
  markSent(etDay);

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

    // 6. Read BC-write audit log for the same date range (Square → BC sync direction)
    const bcWrites = readBcWritesInRange(after, before);
    const bcSummary = summarizeBcWrites(bcWrites);
    console.log(`[REPORT] ${bcSummary.totals.totalWrites} BC writes — ` +
                `${bcSummary.changedProducts.length} product(s) actually changed, ` +
                `${bcSummary.totals.silentWrites} silent reconciliation write(s) excluded`);

    // 7. Build & send email
    const subject = `IMB Sync Report - ${dateStr}`;
    const lines = [];
    lines.push(`Sync activity on ${dateStr}:`);

    // Section A — Square → BC writes that ACTUALLY CHANGED inventory.
    // The sync's 15-minute reconciliation writes to BC every cycle whether or
    // not anything changed; those writes are noise and excluded here.
    lines.push('');
    lines.push('=== Square → BC writes ===');
    lines.push('(Inventory levels that changed yesterday — POS sales, manual');
    lines.push(' Square edits, and Brewmaster updates that moved a count)');
    if (bcSummary.changedProducts.length === 0) {
      lines.push('  (no BC inventory levels changed)');
    } else {
      const totalDeltas = bcSummary.changedProducts.reduce(
        (s, p) => s + p.variants.length, 0);
      lines.push(`  ${bcSummary.changedProducts.length} product(s), ${totalDeltas} variant change(s)`);
      for (const p of bcSummary.changedProducts) {
        lines.push('');
        lines.push(`  ${p.name}  [${p.kind}]`);
        for (const v of p.variants) {
          const sign = v.delta > 0 ? '+' : '';
          lines.push(`     ${(v.size || '').padEnd(8)} ${String(v.first).padStart(5)} → ${String(v.last).padEnd(5)}  (${sign}${v.delta})`);
        }
      }
    }
    if (bcSummary.totals.silentWrites > 0) {
      lines.push('');
      lines.push(`  (${bcSummary.totals.silentWrites} silent reconciliation write(s) excluded — confirmed no change)`);
    }

    // Section B — BC → Square deductions (the sync's response to BC orders).
    // These ARE visible in Square's inventory log under our app_id.
    lines.push('');
    lines.push('=== Sync → Square deductions ===');
    lines.push('(BC online orders that decremented Square inventory)');
    if (syncChanges.length === 0) {
      lines.push('  (no BC orders yesterday)');
    } else {
      const products = new Set(syncChanges.map(c => (c.physical_count || c.adjustment).catalog_object_id));
      lines.push(`  ${syncChanges.length} change(s) across ${products.size} product(s)`);
      if (physCounts.length > 0) {
        lines.push('');
        lines.push(`  -- Inventory Sets (${physCounts.length}) --`);
        for (const c of physCounts) lines.push('  ' + formatChange(c));
      }
      if (adjustments.length > 0) {
        lines.push('');
        lines.push(`  -- Adjustments (${adjustments.length}) --`);
        for (const c of adjustments) lines.push('  ' + formatChange(c));
      }
    }

    lines.push('');
    lines.push(`(${otherCount} other Square change(s) from POS/Dashboard excluded from sync section)`);

    const emailBody = lines.join('\n');

    await sendEmail(REPORT_TO, subject, emailBody);
    console.log(`[REPORT] Sent "${subject}" → ${REPORT_TO}`);
    return { sent: true, subject,
             syncChanges: syncChanges.length,
             bcChangedProducts: bcSummary.changedProducts.length,
             bcChangedWrites: bcSummary.totals.changedWrites,
             bcSilentWrites: bcSummary.totals.silentWrites,
             excluded: otherCount, etDay };

  } catch (e) {
    // If the report failed to generate, allow a future attempt today —
    // unmark the dedup so a manual retry isn't blocked.
    lastSentDay = null;
    console.error(`[REPORT] ERROR: ${e.message}`);
    return { sent: false, error: e.message };
  }
}

// ── Scheduler (7 AM Eastern Time) ──────────────────────────────────────────
function startScheduler() {
  setInterval(() => {
    if (!RESEND_API_KEY) return;
    const now = new Date();
    const off = etOffset(now);
    const etHour = (now.getUTCHours() + 24 - off) % 24;

    if (etHour === 7 && now.getUTCMinutes() < 5) {
      const etDay = etDayString(now);
      if (alreadySent(etDay)) return;
      console.log('[REPORT] 7 AM ET — triggering daily sync report');
      generateAndSendReport().catch(e => console.error(`[REPORT] ${e.message}`));
    }
  }, 60000);
  console.log('[REPORT] Scheduler active (7 AM ET daily)');
}

// ── Export ──────────────────────────────────────────────────────────────────
module.exports = function (app) {
  // Manual trigger endpoints. By default they respect today's dedup so an
  // external pinger or a Render Cron Job hitting these endpoints won't
  // produce a second copy of the daily email. Pass ?force=1 to re-send.
  app.get('/daily-report',  async (req, res) => {
    const force = req.query && (req.query.force === '1' || req.query.force === 'true');
    res.json(await generateAndSendReport({ force }));
  });
  app.post('/daily-report', async (req, res) => {
    const force = (req.query && (req.query.force === '1' || req.query.force === 'true'))
               || (req.body  && (req.body.force === true || req.body.force === '1'));
    res.json(await generateAndSendReport({ force }));
  });
  startScheduler();
};
