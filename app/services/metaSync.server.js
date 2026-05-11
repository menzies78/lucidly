// Meta Insights ingestion - two-pass strategy.
//
// PASS 1 (daily probe): Fetch the entire window in 90-day daily-aggregate
// chunks. Cheap, low row count, low risk of throttle. Persisted as
// hourSlot=-1 rows. Tells us which days actually had Meta-attributed
// conversions, so Pass 2 only spends API quota on days that matter.
//
// PASS 2 (hourly enrich): For every day where Pass 1 saw conversions > 0,
// fetch single-day hourly. Before writing the 0..23 rows we delete the
// daily-aggregate row for that ad/day (hourSlot=-1) so the dataset never
// double-counts.
//
// Concurrency: there is NO Promise.all in the hot path. The fetch layer
// (metaFetch.server.js -> withAccountLock) enforces one in-flight Meta call
// per ad account at a time, so any parallelism here is dishonest - it just
// queues at the fetch layer. We make this explicit by walking the work
// sequentially.
//
// Reduce-data: Pass 1 uses a chunk-size ladder (90 → 30 → 7 → 1) and
// recursively halves on ReduceDataError. Pass 2 is already single-day so
// "reduce data" can only mean a true API issue - we surface it.
//
// Socket timeouts: batchUpsertInsights retries indefinitely with backoff.
// Per Andy: "We can't do timeouts and move on - that kills the app. No
// moving on. we need the full data."

import db from "../db.server";
import { setProgress, completeProgress, failProgress } from "./progress.server";
import { syncMetaBreakdowns } from "./metaBreakdownSync.server";
import { prefetchExchangeRates, convertMetaFields } from "./exchangeRate.server";
import { fetchAllPages, ReduceDataError } from "./metaFetch.server";
import { snapshot as governorSnapshot } from "./metaGovernor.server.js";

const PAGE_LIMIT = 1000;            // rows per API page
const DB_BATCH_SIZE = 500;          // rows per Prisma $transaction
const HOURLY_LIMIT_DAYS = 395;      // Meta only allows hourly within ~13 months

// Pass 1 chunk-size ladder, descending. Used ONLY by the ReduceDataError
// fallback path - we step DOWN to the next size when Meta says "reduce the
// amount of data". 1 is the floor.
const PASS1_LADDER = [90, 30, 7, 1];

// Adaptive ramp-up ladder for Pass 1. We START at 1 day, then step UP on
// consecutive successful chunks (no governor block triggered).
//
// Why ramp up instead of starting at 7 days?
//
// Meta has two distinct throttle channels:
//   1. Budget util (X-App-Usage, X-Business-Use-Case-Usage). Tracked by
//      metaGovernor. This is what util% measures.
//   2. Load throttle (error subcode 1504022, "Ad-Account temporarily blocked
//      due to high load"). Triggers on per-CALL cost AND on the account's
//      recent activity, not budget %. The very first call on a cold start
//      is the riskiest one: Meta's server-side load counter still remembers
//      our previous bursts (across deploys, the previous merchant install,
//      etc.) - so even a moderate first call can immediately return 1504022.
//
// Starting at 1 day makes the first call as cheap as possible. After 2
// consecutive non-blocked chunks we double up (1 → 3 → 7), so we get to
// full speed within ~6 chunks while never exposing the merchant to the
// "paused for 19s (rate limit)" message on chunk 1.
//
// On any governor block (blockedFor > 0 detected after a chunk), we drop
// back to the previous rung.
const PASS1_RAMP_LADDER = [1, 3, 7];
const PASS1_RAMP_STEP_UP_AFTER = 2; // successful chunks before stepping up

// Mandatory pause between chunks. The 1504022 load score decays over time;
// back-to-back calls keep the account warm even if each individual call is
// small. A short pause gives the next call a cooler launch.
const INTER_CHUNK_PAUSE_MS = 3000;

// Fields for Pass 1 (daily aggregates - need every metric).
const FIELDS_DAILY = [
  "date_start", "ad_id", "ad_name", "campaign_id", "campaign_name",
  "adset_id", "adset_name", "impressions", "clicks", "spend",
  "reach", "frequency", "cpc", "cpm",
  "actions", "action_values", "outbound_clicks",
  "video_p25_watched_actions", "video_p50_watched_actions",
  "video_p75_watched_actions", "video_p100_watched_actions",
].join(",");

// Fields for Pass 2 (hourly). reach/frequency are not returned with hourly
// breakdown anyway; video_p* metrics aren't useful at hour granularity for
// our reporting. Trimmed to keep the row payload minimal.
const FIELDS_HOURLY = [
  "date_start", "ad_id", "ad_name", "campaign_id", "campaign_name",
  "adset_id", "adset_name", "impressions", "clicks", "spend",
  "cpc", "cpm",
  "actions", "action_values", "outbound_clicks",
].join(",");

// ------------------------------------------------------------------
// Action / row parsers
// ------------------------------------------------------------------

function parseActionValue(actions, actionType) {
  if (!actions) return 0;
  for (const a of actions) {
    if (a.action_type === actionType) return parseInt(a.value || "0", 10);
  }
  return 0;
}

function parseActionFloat(actions, actionType) {
  if (!actions) return 0;
  for (const a of actions) {
    if (a.action_type === actionType) return parseFloat(a.value || "0");
  }
  return 0;
}

function parseOutboundClicks(outboundClicks) {
  if (!outboundClicks) return 0;
  for (const a of outboundClicks) {
    if (a.action_type === "outbound_click") return parseInt(a.value || "0", 10);
  }
  return 0;
}

function parseVideoWatched(videoActions) {
  if (!videoActions) return 0;
  for (const a of videoActions) {
    if (a.value) return parseInt(a.value, 10);
  }
  return 0;
}

// hourSlot = -1 for daily-aggregate rows (Pass 1), 0..23 for hourly (Pass 2).
function parseRow(row, hourSlot) {
  return {
    date: new Date(row.date_start),
    hourSlot,
    campaignId: row.campaign_id, campaignName: row.campaign_name,
    adSetId: row.adset_id, adSetName: row.adset_name,
    adId: row.ad_id, adName: row.ad_name,
    impressions: parseInt(row.impressions || "0"),
    clicks: parseInt(row.clicks || "0"),
    spend: parseFloat(row.spend || "0"),
    conversions: parseActionValue(row.actions, "offsite_conversion.fb_pixel_purchase"),
    conversionValue: parseActionFloat(row.action_values, "offsite_conversion.fb_pixel_purchase"),
    reach: parseInt(row.reach || "0"),
    frequency: parseFloat(row.frequency || "0"),
    cpc: parseFloat(row.cpc || "0"),
    cpm: parseFloat(row.cpm || "0"),
    outboundClicks: parseOutboundClicks(row.outbound_clicks),
    linkClicks: parseActionValue(row.actions, "link_click"),
    landingPageViews: parseActionValue(row.actions, "landing_page_view"),
    addToCart: parseActionValue(row.actions, "offsite_conversion.fb_pixel_add_to_cart"),
    initiateCheckout: parseActionValue(row.actions, "offsite_conversion.fb_pixel_initiate_checkout"),
    viewContent: parseActionValue(row.actions, "offsite_conversion.fb_pixel_view_content"),
    videoP25: parseVideoWatched(row.video_p25_watched_actions),
    videoP50: parseVideoWatched(row.video_p50_watched_actions),
    videoP75: parseVideoWatched(row.video_p75_watched_actions),
    videoP100: parseVideoWatched(row.video_p100_watched_actions),
  };
}

// ------------------------------------------------------------------
// DB writes - retry-on-socket-timeout, NEVER skip rows
// ------------------------------------------------------------------

function isSocketTimeout(err) {
  const msg = String(err?.message || "");
  return msg.includes("Socket timeout") || msg.includes("connection pool")
    || msg.includes("P2024") || msg.includes("P1008");
}

// Run a Prisma operation with indefinite retry on socket-timeout-style
// failures. Real errors (P2002 unique violation, schema mismatch, etc.)
// throw immediately so we don't loop on a permanent fault.
async function withDbRetry(label, fn) {
  let attempt = 0;
  while (true) {
    try {
      return await fn();
    } catch (err) {
      if (!isSocketTimeout(err)) throw err;
      attempt++;
      const wait = Math.min(60_000, 1000 * Math.pow(2, attempt - 1));
      console.warn(`[MetaSync] ${label}: DB socket timeout (attempt ${attempt}), retrying in ${Math.round(wait / 1000)}s`);
      await new Promise(r => setTimeout(r, wait));
    }
  }
}

async function batchUpsertInsights(shopDomain, parsedRows, ratesByDate) {
  for (let i = 0; i < parsedRows.length; i += DB_BATCH_SIZE) {
    const batch = parsedRows.slice(i, i + DB_BATCH_SIZE);
    await withDbRetry(`upsert ${batch.length} rows`, () =>
      db.$transaction(
        batch.map(r => {
          const dateKey = r.date.toISOString().split("T")[0];
          const rate = ratesByDate[dateKey] || 1.0;
          const insightData = {
            campaignName: r.campaignName, adSetName: r.adSetName, adName: r.adName,
            impressions: r.impressions, clicks: r.clicks,
            spend: r.spend, conversions: r.conversions, conversionValue: r.conversionValue,
            reach: r.reach, frequency: r.frequency, cpc: r.cpc, cpm: r.cpm,
            outboundClicks: r.outboundClicks, linkClicks: r.linkClicks,
            landingPageViews: r.landingPageViews,
            addToCart: r.addToCart, initiateCheckout: r.initiateCheckout,
            viewContent: r.viewContent,
            videoP25: r.videoP25, videoP50: r.videoP50, videoP75: r.videoP75, videoP100: r.videoP100,
          };
          convertMetaFields(insightData, rate);

          return db.metaInsight.upsert({
            where: { shopDomain_date_hourSlot_adId: { shopDomain, date: r.date, hourSlot: r.hourSlot, adId: r.adId } },
            create: {
              shopDomain, date: r.date, hourSlot: r.hourSlot,
              campaignId: r.campaignId, adSetId: r.adSetId, adId: r.adId,
              ...insightData,
            },
            update: insightData,
          });
        }),
        { timeout: 30_000 } // give SQLite headroom on the larger batches
      )
    );
  }
}

// Before Pass 2 writes hourly rows for a day we delete the Pass 1 daily
// aggregate row(s) for that day, so totals don't double-count.
async function deleteDailyForDay(shopDomain, dateKey) {
  const dayStart = new Date(dateKey + "T00:00:00.000Z");
  const dayEnd = new Date(dateKey + "T23:59:59.999Z");
  await withDbRetry(`delete daily ${dateKey}`, () =>
    db.metaInsight.deleteMany({
      where: { shopDomain, hourSlot: -1, date: { gte: dayStart, lte: dayEnd } },
    })
  );
}

// ------------------------------------------------------------------
// Fetchers
// ------------------------------------------------------------------

async function fetchDailyRange(metaAccessToken, metaAdAccountId, since, until) {
  const params = new URLSearchParams({
    fields: FIELDS_DAILY, level: "ad",
    time_range: JSON.stringify({ since, until }),
    time_increment: "1", limit: String(PAGE_LIMIT), action_report_time: "conversion",
    action_breakdowns: "action_type", use_unified_attribution_setting: "true",
    action_attribution_windows: JSON.stringify(["1d_view", "7d_click"]),
    access_token: metaAccessToken,
  });
  const url = `https://graph.facebook.com/v21.0/${metaAdAccountId}/insights?${params.toString()}`;
  const apiRows = await fetchAllPages(url, "MetaSync/Pass1");
  return apiRows.map(row => parseRow(row, -1));
}

async function fetchHourlyDay(metaAccessToken, metaAdAccountId, dateKey) {
  const params = new URLSearchParams({
    fields: FIELDS_HOURLY,
    breakdowns: "hourly_stats_aggregated_by_advertiser_time_zone",
    level: "ad",
    time_range: JSON.stringify({ since: dateKey, until: dateKey }),
    time_increment: "1", limit: String(PAGE_LIMIT), action_report_time: "conversion",
    action_breakdowns: "action_type", use_unified_attribution_setting: "true",
    action_attribution_windows: JSON.stringify(["1d_view", "7d_click"]),
    access_token: metaAccessToken,
  });
  const url = `https://graph.facebook.com/v21.0/${metaAdAccountId}/insights?${params.toString()}`;
  const apiRows = await fetchAllPages(url, "MetaSync/Pass2");
  const parsed = [];
  for (const row of apiRows) {
    const hourSlot = parseInt(String(row.hourly_stats_aggregated_by_advertiser_time_zone).split(":")[0], 10);
    if (Number.isNaN(hourSlot) || hourSlot < 0 || hourSlot > 23) continue;
    parsed.push(parseRow(row, hourSlot));
  }
  return parsed;
}

// ------------------------------------------------------------------
// Date helpers
// ------------------------------------------------------------------

function buildDayList(daysBack) {
  const days = [];
  for (let i = daysBack; i >= 1; i--) {
    const d = new Date();
    d.setUTCDate(d.getUTCDate() - i);
    days.push(d.toISOString().split("T")[0]);
  }
  return days;
}

function chunkDays(days, size) {
  const chunks = [];
  for (let i = 0; i < days.length; i += size) {
    const slice = days.slice(i, i + size);
    chunks.push({ since: slice[0], until: slice[slice.length - 1], days: slice });
  }
  return chunks;
}

function nextLadderSize(currentSize) {
  const idx = PASS1_LADDER.indexOf(currentSize);
  if (idx < 0 || idx === PASS1_LADDER.length - 1) return null;
  return PASS1_LADDER[idx + 1];
}

function formatElapsed(ms) {
  const totalSec = Math.round(ms / 1000);
  if (totalSec < 60) return `${totalSec}s`;
  const min = Math.floor(totalSec / 60);
  const sec = totalSec % 60;
  if (min < 60) return `${min}m ${sec}s`;
  const hr = Math.floor(min / 60);
  return `${hr}h ${min % 60}m`;
}

// ------------------------------------------------------------------
// Pass 1: daily aggregates with reduce-data fallback
// ------------------------------------------------------------------
//
// Returns the parsed rows AND a Set of dateKeys where conversions > 0,
// so Pass 2 can target only those days.
async function runPass1(metaAccessToken, metaAdAccountId, shopDomain, allDays, ratesByDate, onProgress) {
  const conversionDays = new Set();
  let totalRows = 0;
  let daysDone = 0;

  // Walk the ladder. We start at the largest chunk size; on ReduceDataError
  // for a single chunk we recurse with the next-smaller size on just that
  // chunk's days (not the whole pass).
  async function fetchChunk(since, until, chunkDayList, sizeAtCall) {
    try {
      const rows = await fetchDailyRange(metaAccessToken, metaAdAccountId, since, until);
      return rows;
    } catch (err) {
      if (!(err instanceof ReduceDataError)) throw err;
      const smaller = nextLadderSize(sizeAtCall);
      if (!smaller) {
        // Already at floor (1 day) and Meta still says reduce - real error.
        throw new Error(`MetaSync/Pass1: reduce-data at single-day chunk ${since} - ${err.message}`);
      }
      console.warn(`[MetaSync/Pass1] reduce-data at size=${sizeAtCall}, splitting ${since}→${until} into ${smaller}-day sub-chunks`);
      const subChunks = chunkDays(chunkDayList, smaller);
      const all = [];
      for (const sub of subChunks) {
        const subRows = await fetchChunk(sub.since, sub.until, sub.days, smaller);
        all.push(...subRows);
      }
      return all;
    }
  }

  // Adaptive walk. We don't pre-chunk - sizes are decided per iteration based
  // on whether the previous chunk triggered a governor block.
  const acctKey = `act_${String(metaAdAccountId).replace(/^act_/, "")}`;
  let cursor = 0;
  let rungIdx = 0;                          // start at the smallest rung
  let consecutiveSuccesses = 0;
  let chunkIdx = 0;
  const estTotalChunks = Math.ceil(allDays.length / PASS1_RAMP_LADDER[PASS1_RAMP_LADDER.length - 1]);

  console.log(`[MetaSync/Pass1] ${allDays.length} days, adaptive ramp ${PASS1_RAMP_LADDER.join("→")}d (step up after ${PASS1_RAMP_STEP_UP_AFTER} clean chunks)`);

  while (cursor < allDays.length) {
    chunkIdx++;
    const size = PASS1_RAMP_LADDER[rungIdx];
    const slice = allDays.slice(cursor, cursor + size);
    const since = slice[0];
    const until = slice[slice.length - 1];

    onProgress({
      detail: `Scanning ${since} → ${until} · ${size}-day chunk · ${totalRows.toLocaleString()} rows so far`,
      daysDone,
      totalRows,
    });

    const chunkStart = Date.now();
    const rows = await fetchChunk(since, until, slice, size);
    const chunkMs = Date.now() - chunkStart;

    // Persist immediately (don't accumulate in memory across the whole window).
    if (rows.length > 0) {
      await batchUpsertInsights(shopDomain, rows, ratesByDate);
      totalRows += rows.length;
      for (const r of rows) {
        if (r.conversions > 0) {
          conversionDays.add(r.date.toISOString().split("T")[0]);
        }
      }
    }

    // Three adaptive signals after each chunk:
    //   • blockedFor > 0  → load throttle (subcode 1504022) tripped. Hard
    //     step-down: we need to be cheaper next call.
    //   • util ≥ 75       → BUC budget heating up. Step down BEFORE we get
    //     blocked, so we never plateau at the danger zone (this was the
    //     116% bug - the old logic only reacted on block, by which time
    //     the window was already overcommitted).
    //   • otherwise success — step up after N clean chunks.
    const snap = governorSnapshot();
    const acctSnap = snap.accounts?.[acctKey];
    const blockedFor = acctSnap?.blockedFor || 0;
    const acctUtil = Math.max(acctSnap?.bucMaxPct || 0, acctSnap?.insightsAccPct || 0);
    const tripped = blockedFor > 0;
    const hot = acctUtil >= 75;

    if (tripped || hot) {
      const prev = rungIdx;
      rungIdx = Math.max(0, rungIdx - 1);
      consecutiveSuccesses = 0;
      if (rungIdx !== prev) {
        const reason = tripped ? `throttle tripped (blockedFor=${blockedFor}s)` : `util ${acctUtil}% (>=75)`;
        console.log(`[MetaSync/Pass1] ${reason}, stepping down ${PASS1_RAMP_LADDER[prev]}d → ${PASS1_RAMP_LADDER[rungIdx]}d`);
      }
    } else {
      consecutiveSuccesses++;
      if (consecutiveSuccesses >= PASS1_RAMP_STEP_UP_AFTER && rungIdx < PASS1_RAMP_LADDER.length - 1) {
        const prev = rungIdx;
        rungIdx++;
        consecutiveSuccesses = 0;
        console.log(`[MetaSync/Pass1] ${PASS1_RAMP_STEP_UP_AFTER} clean chunks, stepping up ${PASS1_RAMP_LADDER[prev]}d → ${PASS1_RAMP_LADDER[rungIdx]}d`);
      }
    }

    daysDone += slice.length;
    cursor += slice.length;

    console.log(`[MetaSync/Pass1] chunk ${chunkIdx} ${since}→${until} (${size}d) done: +${rows.length} rows in ${Math.round(chunkMs/1000)}s, total=${totalRows}, nextRung=${PASS1_RAMP_LADDER[rungIdx]}d`);
    onProgress({
      detail: `Imported ${since} → ${until} · +${rows.length.toLocaleString()} rows · ${totalRows.toLocaleString()} total`,
      daysDone,
      totalRows,
    });

    // Cooldown before the next chunk. Skipped on the last iteration so we
    // don't sit idle at 100%.
    if (cursor < allDays.length) {
      await new Promise(r => setTimeout(r, INTER_CHUNK_PAUSE_MS));
    }
  }

  return { totalRows, conversionDays };
}

// ------------------------------------------------------------------
// Pass 2: hourly enrich for conversion days
// ------------------------------------------------------------------

async function runPass2(metaAccessToken, metaAdAccountId, shopDomain, conversionDays, ratesByDate, onProgress) {
  // Honour Meta's 13-month hourly window. Anything older stays at daily-only.
  const cutoff = new Date();
  cutoff.setUTCDate(cutoff.getUTCDate() - HOURLY_LIMIT_DAYS);
  const cutoffKey = cutoff.toISOString().split("T")[0];

  const daysToEnrich = [...conversionDays].filter(d => d >= cutoffKey).sort();
  console.log(`[MetaSync/Pass2] ${daysToEnrich.length} conversion days to enrich (filtered ${conversionDays.size - daysToEnrich.length} pre-13mo)`);

  let totalRows = 0;
  let daysDone = 0;

  let idx = 0;
  for (const dateKey of daysToEnrich) {
    idx++;
    onProgress({
      detail: `Hourly detail for ${dateKey} · day ${idx}/${daysToEnrich.length}`,
      daysDone,
      totalRows,
    });

    const rows = await fetchHourlyDay(metaAccessToken, metaAdAccountId, dateKey);

    // Replace daily aggregate with hourly rows atomically-ish: delete the
    // hourSlot=-1 row(s) for this day, then upsert hourly. There's a brief
    // window where the day has no rows; acceptable since neither read path
    // assumes presence (loaders already tolerate gaps).
    await deleteDailyForDay(shopDomain, dateKey);
    if (rows.length > 0) {
      await batchUpsertInsights(shopDomain, rows, ratesByDate);
      totalRows += rows.length;
    }

    daysDone++;
    onProgress({
      detail: `Hourly ${dateKey} · +${rows.length} rows · ${totalRows.toLocaleString()} total`,
      daysDone,
      totalRows,
    });

    // Same cooling pause as Pass 1. Pass 2 is single-day per call so the
    // per-call cost is already small, but ad-account load is cumulative -
    // we still want to back off between calls so 1504022 doesn't fire.
    if (idx < daysToEnrich.length) {
      await new Promise(r => setTimeout(r, INTER_CHUNK_PAUSE_MS));
    }
  }

  return { totalRows, daysEnriched: daysToEnrich.length };
}

// ------------------------------------------------------------------
// Public entry points
// ------------------------------------------------------------------

// Shared setup: resolve shop, validate Meta connection, prefetch exchange
// rates. Pass 1 and Pass 2 both need this. Idempotent - safe to call twice
// (rate prefetch is cached).
async function setupShopAndRates(shopDomain, daysBack, progressKey) {
  const shop = await db.shop.findUnique({ where: { shopDomain } });
  if (!shop?.metaAccessToken || !shop?.metaAdAccountId) throw new Error("Meta Ads not connected");

  const { metaAccessToken, metaAdAccountId } = shop;
  const needsConversion = shop.metaCurrency !== shop.shopifyCurrency;

  const allDays = buildDayList(daysBack);

  let ratesByDate = {};
  if (needsConversion) {
    setProgress(progressKey, {
      status: "running", current: 0, total: daysBack, unitLabel: "days",
      totalIsApproximate: true,
      detail: `Fetching ${shop.metaCurrency}→${shop.shopifyCurrency} exchange rates`,
    });
    ratesByDate = await prefetchExchangeRates(allDays, shop.metaCurrency, shop.shopifyCurrency, (msg) => {
      setProgress(progressKey, {
        status: "running", current: 0, total: daysBack, unitLabel: "days",
        totalIsApproximate: true,
        detail: `Exchange rates: ${msg}`,
      });
    });
  }

  return { shop, metaAccessToken, metaAdAccountId, allDays, ratesByDate };
}

/**
 * Pass 1 only - daily aggregates for the whole window. Writes hourSlot=-1
 * rows. Returns the list of dates where conversions > 0 so the caller (or
 * Pass 2 itself, via DB self-discovery) knows which days need hourly detail.
 *
 * Used standalone by the phased orchestrator so Pass 1 / Pass 2 render as
 * two separate progress rows in the onboarding UI. The legacy
 * syncMetaInsights() wrapper below chains both into one progress key.
 */
export async function syncMetaPass1(shopDomain, daysBack = 7, progressKey = null) {
  const key = progressKey || `syncMetaPass1:${shopDomain}`;
  const startTime = Date.now();
  const { metaAccessToken, metaAdAccountId, allDays, ratesByDate } =
    await setupShopAndRates(shopDomain, daysBack, key);

  const result = await runPass1(metaAccessToken, metaAdAccountId, shopDomain, allDays, ratesByDate, ({ detail, daysDone, totalRows }) => {
    setProgress(key, {
      status: "running",
      current: daysDone,
      total: daysBack,
      unitLabel: "days",
      rowsImported: totalRows,
      detail,
    });
  });
  console.log(`[MetaSync/Pass1] complete: ${result.totalRows} daily rows, ${result.conversionDays.size} conversion days, ${formatElapsed(Date.now() - startTime)}`);
  return { totalRows: result.totalRows, conversionDays: [...result.conversionDays] };
}

/**
 * Pass 2 only - hourly enrich for days that had conversions in Pass 1.
 *
 * Self-discovers which days to enrich by reading MetaInsight rows that are
 * still at hourSlot=-1 with conversions > 0 within the daysBack window. This
 * makes Pass 2 idempotent and recoverable: if the process dies mid-pass,
 * resume picks up only the un-enriched days (already-enriched days have had
 * their hourSlot=-1 row deleted, so they no longer match the discovery query).
 */
export async function syncMetaPass2(shopDomain, daysBack = 7, progressKey = null) {
  const key = progressKey || `syncMetaPass2:${shopDomain}`;
  const startTime = Date.now();
  const { metaAccessToken, metaAdAccountId, ratesByDate } =
    await setupShopAndRates(shopDomain, daysBack, key);

  // Discover conversion days from MetaInsight: any day still at hourSlot=-1
  // with conversions>0 within the daysBack window needs enriching.
  const cutoff = new Date();
  cutoff.setUTCDate(cutoff.getUTCDate() - daysBack);
  const rows = await db.metaInsight.findMany({
    where: {
      shopDomain,
      hourSlot: -1,
      conversions: { gt: 0 },
      date: { gte: cutoff },
    },
    select: { date: true },
    distinct: ["date"],
  });
  const conversionDays = new Set(rows.map(r => r.date.toISOString().split("T")[0]));
  console.log(`[MetaSync/Pass2] discovered ${conversionDays.size} conversion days from DB`);

  if (conversionDays.size === 0) {
    setProgress(key, {
      status: "running",
      current: 0,
      total: 0,
      unitLabel: "days",
      detail: "No conversion days to enrich",
    });
    await db.shop.update({ where: { shopDomain }, data: { lastMetaSync: new Date() } });
    return { totalRows: 0, daysEnriched: 0 };
  }

  const totalDays = conversionDays.size;
  const result = await runPass2(metaAccessToken, metaAdAccountId, shopDomain, conversionDays, ratesByDate, ({ detail, daysDone, totalRows }) => {
    setProgress(key, {
      status: "running",
      current: daysDone,
      total: totalDays,
      unitLabel: "days",
      rowsImported: totalRows,
      detail,
    });
  });
  console.log(`[MetaSync/Pass2] complete: ${result.totalRows} hourly rows over ${result.daysEnriched} days, ${formatElapsed(Date.now() - startTime)}`);

  await db.shop.update({ where: { shopDomain }, data: { lastMetaSync: new Date() } });
  return { totalRows: result.totalRows, daysEnriched: result.daysEnriched };
}

/**
 * Legacy combined entry-point. Kept for syncMetaAll, incrementalSync, and the
 * admin buttons that still expect a single progress key. Internally chains
 * Pass 1 then Pass 2 against the same progressKey - so the merged behaviour
 * is unchanged for those callers. The phased orchestrator uses syncMetaPass1
 * and syncMetaPass2 directly so they render as two progress rows.
 */
export async function syncMetaInsights(shopDomain, daysBack = 7, progressKey = null) {
  const key = progressKey || `syncMeta:${shopDomain}`;
  const startTime = Date.now();
  const p1 = await syncMetaPass1(shopDomain, daysBack, key);
  const p2 = await syncMetaPass2(shopDomain, daysBack, key);
  console.log(`[MetaSync] ${shopDomain} insights complete in ${formatElapsed(Date.now() - startTime)}`);
  return { totalRows: p1.totalRows + p2.totalRows };
}

// Combined sync: insights + breakdowns + entity sync. Same signature as
// before so the orchestrator and the legacy admin buttons keep working.
export async function syncMetaAll(shopDomain, daysBack = 7, progressKey = null) {
  const key = progressKey || `syncMeta:${shopDomain}`;
  const breakdownDays = Math.min(daysBack, HOURLY_LIMIT_DAYS);

  setProgress(key, { status: "running", current: 0, total: 100, message: `Step 1/3 · Starting insights sync (${daysBack} days)...` });
  const insightResult = await syncMetaInsights(shopDomain, daysBack, key);

  setProgress(key, { status: "running", current: 0, total: 100, message: `Step 2/3 · Starting breakdowns sync (${breakdownDays} days)...` });
  const breakdownResult = await syncMetaBreakdowns(shopDomain, key, breakdownDays);

  setProgress(key, { status: "running", message: `Step 3/3 · Syncing campaign entities...` });
  const { syncMetaEntities } = await import("./metaEntitySync.server");
  await syncMetaEntities(shopDomain);

  completeProgress(key, {
    totalInsightRows: insightResult.totalRows,
    totalBreakdownRows: breakdownResult.totalRows,
  });

  return { ...insightResult, breakdownRows: breakdownResult.totalRows };
}
