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
// - Conversions keep accruing until the longest window closes (28 days), so
//   the rolling sync re-pulls a 35-day lookback; rows are upserts.
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

function parseRow(row) {
  const p = windowsOf(findPurchase(row.actions), toInt);
  const v = windowsOf(findPurchase(row.action_values), toFloat);
  return {
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
function hasAnyPurchase(r) {
  return r.purchasesDefault > 0 || r.purchases1dClick > 0 || r.purchases7dClick > 0
    || r.purchases28dClick > 0 || r.purchases1dView > 0 || r.purchases1dEv > 0;
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
