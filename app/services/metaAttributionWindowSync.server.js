// Meta attribution-window ingestion - per-ad per-day purchase conversions
// decomposed by attribution window (1d_click, 7d_click, 28d_click, 1d_view,
// 1d_ev) alongside the ad set's configured default.
//
// STORAGE ONLY (2026-07-30): nothing reads MetaAttributionWindow yet. The
// point is to accumulate history now so future reports (credited-vs-touched
// estimates, window-mix per campaign, engage-through share) have data from
// day one. See "Future plan" in the v2 attribution blueprint.
//
// Design notes:
// - DAILY level only. Windows + hourly breakdown is not documented-safe on
//   the Insights API, and window decomposition is a daily-grain question.
//   This dataset NEVER touches MetaInsight or the matcher.
// - Explicit windows WITHOUT "default" in action_attribution_windows: when
//   ad sets with different attribution settings aggregate, Meta drops
//   metrics unless the request omits the default window. The configured
//   setting still arrives as the "value" key on every action stat.
// - action_report_time=conversion to match MetaInsight day-bucketing, so a
//   future join of this table onto MetaInsight per (adId, date) lines up.
//   NOTE: under conversion-time reporting a day-row is essentially final
//   within ~days (conversions land on their conversion date, not the click
//   date). The rolling 35-day re-pull is restatement insurance (dedup, IVT
//   removals), not window-maturation - rows are upserts either way.
// - Meta redefined 1d_ev (engaged-view -> engage-through) and click windows
//   (link-clicks-only) in March 2026 under the SAME API keys. Historical
//   backfill rows before 2026-03-01 carry the old semantics.
//
// Fault isolation: syncMetaAll wraps this in try/catch - a failure here must
// never take down the core insights sync.

import db from "../db.server";
import { setProgress, completeProgress, failProgress } from "./progress.server";
import { prefetchExchangeRates } from "./exchangeRate.server";
import { fetchAllPages, ReduceDataError } from "./metaFetch.server";

const PAGE_LIMIT = 1000;
const DB_BATCH_SIZE = 500;
const INTER_CHUNK_PAUSE_MS = 3000;
// Chunk-size ladder for ReduceDataError fallback, mirroring metaSync Pass 1.
const CHUNK_LADDER = [30, 7, 1];

// Windows we request. NO "default" (see header note); "value" always comes
// back as the configured-setting column. 7d_view/28d_view are dead since
// Jan 2026 (silent empties) - not requested.
const WINDOWS = ["1d_click", "7d_click", "28d_click", "1d_view", "1d_ev"];

const FIELDS = [
  "date_start", "ad_id", "ad_name", "campaign_id", "campaign_name",
  "adset_id", "adset_name", "actions", "action_values",
].join(",");

const PURCHASE = "offsite_conversion.fb_pixel_purchase";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ------------------------------------------------------------------
// Parsing
// ------------------------------------------------------------------

function findPurchase(stats) {
  if (!Array.isArray(stats)) return null;
  return stats.find((s) => s.action_type === PURCHASE) || null;
}

function windowsOf(stat, parse) {
  return {
    default: parse(stat?.value),
    "1d_click": parse(stat?.["1d_click"]),
    "7d_click": parse(stat?.["7d_click"]),
    "28d_click": parse(stat?.["28d_click"]),
    "1d_view": parse(stat?.["1d_view"]),
    "1d_ev": parse(stat?.["1d_ev"]),
  };
}

const toInt = (v) => parseInt(v || "0", 10) || 0;
const toFloat = (v) => parseFloat(v || "0") || 0;

// Future-proofing: capture window keys we don't recognise. Meta has changed
// this surface twice recently (Jan 2026 removals, Mar 2026 redefinition) -
// when a NEW window key appears we store it in extraWindows JSON and log a
// tripwire, so history accrues before we promote it to a real column.
const KNOWN_STAT_KEYS = new Set([
  "action_type", "value", ...WINDOWS,
  // dead-but-documented windows Meta may still echo as empties
  "7d_view", "28d_view", "28d_view_first_conversion", "7d_view_first_conversion",
  "7d_view_all_conversions", "28d_view_all_conversions",
]);
const seenUnknownKeys = (globalThis.__lucidlyUnknownWindowKeys ||= new Set());

function collectExtraWindows(pStat, vStat) {
  const extra = {};
  for (const stat of [pStat, vStat]) {
    if (!stat) continue;
    for (const key of Object.keys(stat)) {
      if (KNOWN_STAT_KEYS.has(key)) continue;
      const num = parseFloat(stat[key]);
      if (!Number.isFinite(num) || num === 0) continue;
      if (!extra[key]) extra[key] = { purchases: 0, value: 0 };
      if (stat === pStat) extra[key].purchases = toInt(stat[key]);
      else extra[key].value = toFloat(stat[key]);
      if (!seenUnknownKeys.has(key)) {
        seenUnknownKeys.add(key);
        console.warn(`[AttrWindowSync] ⚠️ UNKNOWN Meta attribution window key "${key}" detected - capturing to extraWindows. Meta may have added a new window type; consider promoting it to a first-class bucket.`);
      }
    }
  }
  return Object.keys(extra).length > 0 ? JSON.stringify(extra) : null;
}

function parseRow(row) {
  const pStat = findPurchase(row.actions);
  const vStat = findPurchase(row.action_values);
  const p = windowsOf(pStat, toInt);
  const v = windowsOf(vStat, toFloat);
  return {
    extraWindows: collectExtraWindows(pStat, vStat),
    date: new Date(row.date_start),
    campaignId: row.campaign_id, campaignName: row.campaign_name,
    adSetId: row.adset_id, adSetName: row.adset_name,
    adId: row.ad_id, adName: row.ad_name,
    purchasesDefault: p.default,
    purchases1dClick: p["1d_click"], purchases7dClick: p["7d_click"],
    purchases28dClick: p["28d_click"], purchases1dView: p["1d_view"],
    purchases1dEv: p["1d_ev"],
    valueDefault: v.default,
    value1dClick: v["1d_click"], value7dClick: v["7d_click"],
    value28dClick: v["28d_click"], value1dView: v["1d_view"],
    value1dEv: v["1d_ev"],
  };
}

// Rows with zero purchases across every window are noise - the vast
// majority of ad-days have no conversions. Skip them to keep the table lean.
// extraWindows counts too: an unknown-window-only row is exactly the case
// the future-proof capture exists for.
function hasAnyPurchase(r) {
  return r.purchasesDefault > 0 || r.purchases1dClick > 0 || r.purchases7dClick > 0
    || r.purchases28dClick > 0 || r.purchases1dView > 0 || r.purchases1dEv > 0
    || r.extraWindows !== null;
}

// ------------------------------------------------------------------
// DB writes - same retry-forever-on-socket-timeout stance as metaSync
// ------------------------------------------------------------------

function isSocketTimeout(err) {
  const msg = String(err?.message || "");
  return msg.includes("Socket timeout") || msg.includes("connection pool")
    || msg.includes("P2024") || msg.includes("P1008");
}

async function withDbRetry(label, fn) {
  let attempt = 0;
  while (true) {
    try {
      return await fn();
    } catch (err) {
      if (!isSocketTimeout(err)) throw err;
      attempt++;
      const backoff = Math.min(30_000, 1000 * attempt);
      console.warn(`[AttrWindowSync] DB busy on ${label} (attempt ${attempt}), retrying in ${backoff}ms`);
      await sleep(backoff);
    }
  }
}

async function batchUpsert(shopDomain, rows, ratesByDate) {
  let written = 0;
  for (let i = 0; i < rows.length; i += DB_BATCH_SIZE) {
    const batch = rows.slice(i, i + DB_BATCH_SIZE);
    await withDbRetry(`upsert ${batch.length} window rows`, () =>
      db.$transaction(
        batch.map((r) => {
          const dateKey = r.date.toISOString().split("T")[0];
          const rate = ratesByDate[dateKey] || 1.0;
          const round2 = (x) => Math.round(x * rate * 100) / 100;
          const data = {
            campaignName: r.campaignName, adSetName: r.adSetName, adName: r.adName,
            purchasesDefault: r.purchasesDefault,
            purchases1dClick: r.purchases1dClick, purchases7dClick: r.purchases7dClick,
            purchases28dClick: r.purchases28dClick, purchases1dView: r.purchases1dView,
            purchases1dEv: r.purchases1dEv,
            valueDefault: round2(r.valueDefault),
            value1dClick: round2(r.value1dClick), value7dClick: round2(r.value7dClick),
            value28dClick: round2(r.value28dClick), value1dView: round2(r.value1dView),
            value1dEv: round2(r.value1dEv),
            extraWindows: r.extraWindows,
          };
          return db.metaAttributionWindow.upsert({
            where: { shopDomain_date_adId: { shopDomain, date: r.date, adId: r.adId } },
            create: {
              shopDomain, date: r.date,
              campaignId: r.campaignId, adSetId: r.adSetId, adId: r.adId,
              ...data,
            },
            update: data,
          });
        }),
        { timeout: 30_000 }
      )
    );
    written += batch.length;
  }
  return written;
}

// ------------------------------------------------------------------
// Fetch - daily ad-level with explicit attribution windows
// ------------------------------------------------------------------

async function fetchWindowRange(token, adAccountId, since, until) {
  const params = new URLSearchParams({
    fields: FIELDS, level: "ad",
    time_range: JSON.stringify({ since, until }),
    time_increment: "1", limit: String(PAGE_LIMIT),
    action_report_time: "conversion",
    action_breakdowns: "action_type",
    action_attribution_windows: JSON.stringify(WINDOWS),
    access_token: token,
  });
  const url = `https://graph.facebook.com/v21.0/${adAccountId}/insights?${params.toString()}`;
  const apiRows = await fetchAllPages(url, "AttrWindowSync");
  return apiRows.map(parseRow).filter(hasAnyPurchase);
}

// Fetch a [since..until] span, stepping down the chunk ladder on
// ReduceDataError. Returns parsed rows for the whole span.
async function fetchSpanWithLadder(token, adAccountId, days, ladderIdx = 0) {
  const size = CHUNK_LADDER[ladderIdx];
  const rows = [];
  for (let i = 0; i < days.length; i += size) {
    const slice = days.slice(i, i + size);
    try {
      rows.push(...await fetchWindowRange(token, adAccountId, slice[0], slice[slice.length - 1]));
    } catch (err) {
      if (err instanceof ReduceDataError && ladderIdx < CHUNK_LADDER.length - 1) {
        console.warn(`[AttrWindowSync] ReduceData at ${size}d chunk - descending ladder`);
        rows.push(...await fetchSpanWithLadder(token, adAccountId, slice, ladderIdx + 1));
      } else {
        throw err;
      }
    }
    await sleep(INTER_CHUNK_PAUSE_MS);
  }
  return rows;
}

function dayList(daysBack, endOffset = 0) {
  const days = [];
  for (let i = daysBack; i >= endOffset; i--) {
    const d = new Date();
    d.setUTCDate(d.getUTCDate() - i);
    days.push(d.toISOString().split("T")[0]);
  }
  return days;
}

// ------------------------------------------------------------------
// Ad set attribution_spec - what setting is each ad set actually using
// ------------------------------------------------------------------

// Upserts MetaEntity(adset).attributionSpec. Update path touches ONLY
// attributionSpec + entityName so we never clobber metaEntitySync's fields.
export async function syncAdsetAttributionSpecs(shopDomain) {
  const shop = await db.shop.findUnique({ where: { shopDomain } });
  if (!shop?.metaAccessToken || !shop?.metaAdAccountId) return { updated: 0 };

  const url = `https://graph.facebook.com/v21.0/${shop.metaAdAccountId}/adsets?fields=id,name,attribution_spec&limit=500&access_token=${shop.metaAccessToken}`;
  const adsets = await fetchAllPages(url, "AttrWindowSync/specs");

  let updated = 0;
  for (let i = 0; i < adsets.length; i += DB_BATCH_SIZE) {
    const batch = adsets.slice(i, i + DB_BATCH_SIZE);
    await withDbRetry(`upsert ${batch.length} adset specs`, () =>
      db.$transaction(
        batch.map((a) =>
          db.metaEntity.upsert({
            where: { shopDomain_entityType_entityId: { shopDomain, entityType: "adset", entityId: a.id } },
            create: {
              shopDomain, entityType: "adset", entityId: a.id, entityName: a.name,
              attributionSpec: a.attribution_spec ? JSON.stringify(a.attribution_spec) : null,
            },
            update: {
              entityName: a.name,
              attributionSpec: a.attribution_spec ? JSON.stringify(a.attribution_spec) : null,
            },
          })
        ),
        { timeout: 30_000 }
      )
    );
    updated += batch.length;
  }
  console.log(`[AttrWindowSync] attribution_spec refreshed for ${updated} ad sets`);
  return { updated };
}

// ------------------------------------------------------------------
// Rolling sync - last N days (default 35: longest window is 28d_click,
// so day-35 rows are final; everything newer is still maturing)
// ------------------------------------------------------------------

export async function syncAttributionWindows(shopDomain, { daysBack = 35, progressKey = null } = {}) {
  const shop = await db.shop.findUnique({ where: { shopDomain } });
  if (!shop) throw new Error("Shop not found");
  if (!shop.metaAccessToken || !shop.metaAdAccountId) throw new Error("Meta Ads not connected");

  const days = dayList(daysBack, 0);
  const note = (msg) => {
    console.log(`[AttrWindowSync] ${msg}`);
    if (progressKey) setProgress(progressKey, { status: "running", message: msg });
  };

  note(`Attribution windows: pulling ${days.length} days...`);
  const ratesByDate = await prefetchExchangeRates(days, shop.metaCurrency, shop.shopifyCurrency, () => {});
  const rows = await fetchSpanWithLadder(shop.metaAccessToken, shop.metaAdAccountId, days);
  const written = await batchUpsert(shopDomain, rows, ratesByDate);
  note(`Attribution windows: ${written} ad-day rows stored`);

  // Refresh the per-ad-set configured setting alongside the numbers.
  // Non-fatal: spec data is a nice-to-have annotation.
  try {
    await syncAdsetAttributionSpecs(shopDomain);
  } catch (err) {
    console.warn(`[AttrWindowSync] adset spec refresh failed (non-fatal): ${err?.message}`);
  }

  return { rows: written, days: days.length };
}

// ------------------------------------------------------------------
// Per-order mechanism labeling (Andy's delta insight, 2026-08-01)
//
// The click windows NEST: 1d_click ⊂ 7d_click ⊂ 28d_click. So window
// counts decompose EXACTLY into disjoint mechanism buckets:
//   click_1d   = 1d_click            (clicked ≤1 day before purchase)
//   click_7d   = 7d_click - 1d_click (clicked 1-7 days before)
//   click_28d  = 28d_click - 7d_click(clicked 7-28 days before)
//   view_1d    = 1d_view             (view-through, never clicked)
//   engage_1d  = 1d_ev               (engage-through, never clicked)
// A positive cycle-over-cycle delta in exactly one bucket = the new
// conversion's mechanism, ground truth. Mirrors enrichFromDelta
// (demographics): exact when unambiguous, flagged when not.
// ------------------------------------------------------------------

const BUCKETS = ["click_1d", "click_7d", "click_28d", "view_1d", "engage_1d"];

export function bucketCounts(c) {
  return {
    click_1d: c.purchases1dClick,
    click_7d: Math.max(0, c.purchases7dClick - c.purchases1dClick),
    click_28d: Math.max(0, c.purchases28dClick - c.purchases7dClick),
    view_1d: c.purchases1dView,
    engage_1d: c.purchases1dEv,
  };
}

function positiveBuckets(b) {
  return BUCKETS.filter((k) => b[k] > 0);
}

// Hourly-cycle pull for [yesterday, today]: fetch fresh window rows, diff
// against the stored MetaAttributionWindow rows (they ARE the previous
// snapshot), upsert the fresh values, return per-ad positive bucket deltas.
//
// deltaMap: adId -> { buckets: {click_1d..engage_1d}, deltaDefault }
// (summed across the 1-2 days of the cycle - a conversion appears once, and
// day-boundary skew between our clock and Meta's account tz washes out).
export async function syncTodayWindows(shopDomain, token, adAccountId, days, rate = 1.0) {
  const stored = await db.metaAttributionWindow.findMany({
    where: { shopDomain, date: { in: days.map((d) => new Date(d)) } },
  });
  const prevByKey = {};
  for (const s of stored) prevByKey[`${s.adId}|${s.date.toISOString().split("T")[0]}`] = s;

  const fresh = await fetchWindowRange(token, adAccountId, days[0], days[days.length - 1]);

  const deltaMap = new Map();
  for (const row of fresh) {
    const dateKey = row.date.toISOString().split("T")[0];
    const prev = prevByKey[`${row.adId}|${dateKey}`] || {
      purchases1dClick: 0, purchases7dClick: 0, purchases28dClick: 0,
      purchases1dView: 0, purchases1dEv: 0, purchasesDefault: 0,
    };
    const nowB = bucketCounts(row);
    const prevB = bucketCounts(prev);
    const d = {};
    let any = false;
    for (const k of BUCKETS) {
      d[k] = Math.max(0, nowB[k] - prevB[k]);
      if (d[k] > 0) any = true;
    }
    if (!any) continue;
    const agg = deltaMap.get(row.adId) || {
      buckets: { click_1d: 0, click_7d: 0, click_28d: 0, view_1d: 0, engage_1d: 0 },
      deltaDefault: 0,
    };
    for (const k of BUCKETS) agg.buckets[k] += d[k];
    agg.deltaDefault += Math.max(0, row.purchasesDefault - prev.purchasesDefault);
    deltaMap.set(row.adId, agg);
  }

  // Persist fresh values (same write path as the daily sync).
  const ratesByDate = {};
  for (const d of days) ratesByDate[d] = rate;
  const written = await batchUpsert(shopDomain, fresh, ratesByDate);

  return { deltaMap, rows: written };
}

// Assign windowLabel/windowExact to attributions matched in THIS cycle.
// Single positive bucket for the ad -> every matched order on that ad gets
// that label, exact=true (mechanism is unambiguous no matter how many
// orders). Multiple buckets -> largest bucket, exact=false.
export async function labelWindowsFromDelta(shopDomain, deltaMap, matchedOrderIds) {
  if (!matchedOrderIds?.length || !deltaMap || deltaMap.size === 0) {
    return { labeled: 0, exact: 0 };
  }
  const attrs = await db.attribution.findMany({
    where: {
      shopDomain,
      shopifyOrderId: { in: matchedOrderIds },
      confidence: { gt: 0 },
      metaAdId: { not: null },
    },
    select: { id: true, metaAdId: true },
  });

  let labeled = 0, exact = 0;
  for (const attr of attrs) {
    const agg = deltaMap.get(attr.metaAdId);
    if (!agg) continue;
    const pos = positiveBuckets(agg.buckets);
    if (pos.length === 0) continue;
    const isExact = pos.length === 1;
    const label = isExact
      ? pos[0]
      : pos.reduce((a, b) => (agg.buckets[b] > agg.buckets[a] ? b : a));
    await withDbRetry(`label attr ${attr.id}`, () =>
      db.attribution.update({
        where: { id: attr.id },
        data: { windowLabel: label, windowExact: isExact },
      })
    );
    labeled++;
    if (isExact) exact++;
  }
  return { labeled, exact };
}

// Catch-up + retro labeler. For attributions still windowLabel=NULL (window
// data lagged the cycle, or the order pre-dates the labeling feature), look
// at the ad's stored DAY-ROW totals for the order date (falling back to the
// day before - Meta's account-tz day can differ from the order's UTC day):
//   - single-bucket day  -> that label, exact=true (whole day is one mechanism)
//   - dominant bucket >= 80% of the day -> that label, exact=false
//   - genuinely mixed day -> stays NULL (honest)
// daysBack=2 for the hourly catch-up; daysBack=400 turns this into the
// one-time retro pass over historical matches (internal button).
export async function catchUpWindowLabels(shopDomain, daysBack = 2) {
  const cutoff = new Date(Date.now() - daysBack * 86400000);
  // Hourly catch-up (small daysBack): bound by matchedAt so we don't rescan
  // every permanently-NULL historical row each cycle. Retro pass (large
  // daysBack): no matchedAt bound - old matches are exactly the target.
  const candidates = await db.attribution.findMany({
    where: {
      shopDomain, windowLabel: null, confidence: { gt: 0 }, metaAdId: { not: null },
      ...(daysBack <= 7 ? { matchedAt: { gte: cutoff } } : {}),
    },
    select: { id: true, shopifyOrderId: true, metaAdId: true },
  });
  if (candidates.length === 0) return { labeled: 0, exact: 0, checked: 0 };

  let labeled = 0, exactCount = 0, checked = 0;
  for (let i = 0; i < candidates.length; i += 500) {
    const batch = candidates.slice(i, i + 500);
    const orders = await db.order.findMany({
      where: {
        shopDomain,
        shopifyOrderId: { in: batch.map((a) => a.shopifyOrderId) },
        createdAt: { gte: cutoff },
      },
      select: { shopifyOrderId: true, createdAt: true },
    });
    const orderDate = {};
    for (const o of orders) orderDate[o.shopifyOrderId] = o.createdAt.toISOString().split("T")[0];

    const todayUTC = new Date().toISOString().split("T")[0];
    for (const attr of batch) {
      const dateKey = orderDate[attr.shopifyOrderId];
      if (!dateKey) continue; // outside window / order missing
      // Only trust COMPLETED days. Today's day-row is still filling - a
      // "pure so far" morning could get exact labels invalidated by an
      // evening view conversion. Today's NULLs wait for tomorrow's pass.
      if (dateKey >= todayUTC) continue;
      checked++;
      const noon = new Date(dateKey + "T12:00:00Z").getTime();
      const dayBefore = new Date(noon - 86400000).toISOString().split("T")[0];
      const dayAfter = new Date(noon + 86400000).toISOString().split("T")[0];
      const rows = await db.metaAttributionWindow.findMany({
        where: {
          shopDomain, adId: attr.metaAdId,
          date: { in: [new Date(dateKey), new Date(dayBefore), new Date(dayAfter)] },
        },
      });
      // Prefer the order-date row. Order dates are UTC while Meta rows are
      // account-tz days, so a late-night order can land on the neighbouring
      // Meta day (tz-ahead accounts -> day after; tz-behind -> day before).
      // Fall back to a neighbour only when exactly ONE has purchase data -
      // two candidates is ambiguous, so we honestly leave the label NULL.
      let row = rows.find((r) => r.date.toISOString().split("T")[0] === dateKey);
      if (!row) {
        const usable = rows.filter((r) =>
          r.date.toISOString().split("T")[0] < todayUTC && // completed days only
          positiveBuckets(bucketCounts(r)).length > 0
        );
        row = usable.length === 1 ? usable[0] : null;
      }
      if (!row) continue;
      const b = bucketCounts(row);
      const pos = positiveBuckets(b);
      if (pos.length === 0) continue;
      const total = pos.reduce((s, k) => s + b[k], 0);
      let label = null, isExact = false;
      if (pos.length === 1) {
        label = pos[0]; isExact = true;
      } else {
        const top = pos.reduce((a, k) => (b[k] > b[a] ? k : a));
        if (b[top] / total >= 0.8) { label = top; isExact = false; }
      }
      if (!label) continue;
      await withDbRetry(`catchup label ${attr.id}`, () =>
        db.attribution.update({
          where: { id: attr.id },
          data: { windowLabel: label, windowExact: isExact },
        })
      );
      labeled++;
      if (isExact) exactCount++;
    }
  }
  return { labeled, exact: exactCount, checked };
}

// ------------------------------------------------------------------
// ensureWindowHistory - idempotent "backfill if missing" entry point.
// Shared by: post-onboarding deferred task (ingestOrchestrator), the
// daily-sweep self-heal (scheduler), and the env-gated boot trigger.
//
// Done-markers (either -> no-op):
//   1. Window rows older than 40 days exist -> a backfill already ran.
//   2. MetaInsight has NO rows older than 40 days -> the ad account itself
//      has no deep history, so the rolling 35-day sync already covers
//      everything there is. (Prevents young accounts re-running 12 empty
//      chunks every day.)
// ------------------------------------------------------------------

export async function ensureWindowHistory(shopDomain, monthsBack = 12) {
  const shop = await db.shop.findUnique({ where: { shopDomain } });
  if (!shop?.metaAccessToken || !shop?.metaAdAccountId) return { status: "not-connected" };

  const marker = new Date(Date.now() - 40 * 86400000);
  const already = await db.metaAttributionWindow.count({
    where: { shopDomain, date: { lt: marker } },
  });
  if (already > 0) return { status: "already-done", historicalRows: already };

  const oldInsights = await db.metaInsight.count({
    where: { shopDomain, date: { lt: marker } },
  });
  if (oldInsights === 0) return { status: "no-deep-history" };

  console.log(`[AttrWindowSync] ensureWindowHistory: starting ${monthsBack}mo backfill for ${shopDomain}`);
  const bf = await backfillAttributionWindows(shopDomain, monthsBack);
  const retro = await catchUpWindowLabels(shopDomain, monthsBack * 31);
  console.log(`[AttrWindowSync] ensureWindowHistory complete for ${shopDomain}: ${bf.rows} rows, retro ${retro.labeled} labeled (${retro.exact} exact) of ${retro.checked} checked`);
  return { status: "backfilled", rows: bf.rows, days: bf.days, retro };
}

// ------------------------------------------------------------------
// One-time historical backfill - default 12 months, newest-first so the
// most recently useful data lands first. Triggered from the dashboard
// (internal-only button); fire-and-forget with its own progress key.
// ------------------------------------------------------------------

export async function backfillAttributionWindows(shopDomain, monthsBack = 12, progressKey = null) {
  const key = progressKey || `backfillAttributionWindows:${shopDomain}`;
  const shop = await db.shop.findUnique({ where: { shopDomain } });
  if (!shop) throw new Error("Shop not found");
  if (!shop.metaAccessToken || !shop.metaAdAccountId) throw new Error("Meta Ads not connected");

  const totalDays = Math.min(monthsBack * 30, 1095); // API hard cap 37 months; we default ~360d
  const allDays = dayList(totalDays, 0);

  // Newest-first 30-day chunks.
  const chunks = [];
  for (let i = allDays.length; i > 0; i -= 30) {
    chunks.push(allDays.slice(Math.max(0, i - 30), i));
  }

  try {
    setProgress(key, { status: "running", current: 0, total: chunks.length, message: `Backfilling attribution windows (${totalDays} days)...` });
    const ratesByDate = await prefetchExchangeRates(allDays, shop.metaCurrency, shop.shopifyCurrency, () => {});

    let totalRows = 0;
    for (let c = 0; c < chunks.length; c++) {
      const chunk = chunks[c];
      const rows = await fetchSpanWithLadder(shop.metaAccessToken, shop.metaAdAccountId, chunk);
      totalRows += await batchUpsert(shopDomain, rows, ratesByDate);
      setProgress(key, {
        status: "running", current: c + 1, total: chunks.length,
        message: `Attribution windows: ${chunk[0]} → ${chunk[chunk.length - 1]} done (${totalRows} rows so far)`,
      });
    }

    // Capture the ad-set settings once the numbers are in.
    try {
      await syncAdsetAttributionSpecs(shopDomain);
    } catch (err) {
      console.warn(`[AttrWindowSync] adset spec refresh failed (non-fatal): ${err?.message}`);
    }

    completeProgress(key, { rows: totalRows, days: totalDays });
    console.log(`[AttrWindowSync] Backfill complete for ${shopDomain}: ${totalRows} rows over ${totalDays} days`);
    return { rows: totalRows, days: totalDays };
  } catch (err) {
    console.error(`[AttrWindowSync] Backfill failed for ${shopDomain}: ${err?.message}`);
    failProgress(key, err?.message || String(err));
    throw err;
  }
}
