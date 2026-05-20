import db from "../db.server";
import { setProgress, completeProgress, failProgress } from "./progress.server";
import { prefetchExchangeRates, convertMetaFields } from "./exchangeRate.server";
import { fetchAllPages, getMetaApiUsage } from "./metaFetch.server";

const CONCURRENCY = 10; // Match insights concurrency
const BREAKDOWN_RANGE_SIZE = 14; // 14 days per API call (breakdowns are lighter than hourly)
const DB_BATCH_SIZE = 100; // SQLite is single-writer; 500-row tx was timing out under load
const TX_TIMEOUT_MS = 30_000; // Default 5s wasn't enough for 100 upserts under contention
const DB_RETRY_BACKOFF_MS = [2_000, 5_000, 10_000, 20_000]; // 4 retries on socket timeout

// Map raw Meta breakdown type strings to human-readable labels for the
// onboarding progress UI ("Analysing audience by country" reads cleaner
// than "Analysing country").
function humaniseBreakdown(type) {
  const map = {
    country: "audience by country",
    publisher_platform: "audience by platform",
    platform_position: "audience by placement",
    age: "audience by age",
    gender: "audience by gender",
    age_gender: "audience by age + gender",
  };
  return map[type] || `audience by ${type}`;
}

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

export const BREAKDOWN_CONFIGS = [
  { type: "country", breakdown: "country", valueField: "country" },
  { type: "publisher_platform", breakdown: "publisher_platform", valueField: "publisher_platform" },
  { type: "platform_position", breakdown: "publisher_platform,platform_position", valueField: (row) => `${row.publisher_platform}|${row.platform_position}` },
  { type: "age", breakdown: "age", valueField: "age" },
  { type: "gender", breakdown: "gender", valueField: "gender" },
  { type: "age_gender", breakdown: "age,gender", valueField: (row) => `${row.age}|${row.gender}` },
];

// Fetch a breakdown for a date range, return parsed rows
async function fetchBreakdownRange(metaAccessToken, metaAdAccountId, since, until, breakdownConfig) {
  const fields = [
    "date_start", "ad_id", "ad_name", "campaign_id", "campaign_name",
    "adset_id", "adset_name", "impressions", "clicks", "spend", "reach",
    "actions", "action_values", "outbound_clicks",
  ].join(",");

  const params = new URLSearchParams({
    fields, breakdowns: breakdownConfig.breakdown, level: "ad",
    time_range: JSON.stringify({ since, until }),
    time_increment: "1", limit: "1000", action_report_time: "conversion",
    action_breakdowns: "action_type", use_unified_attribution_setting: "true",
    action_attribution_windows: JSON.stringify(["1d_view", "7d_click"]),
    access_token: metaAccessToken,
  });

  const url = `https://graph.facebook.com/v21.0/${metaAdAccountId}/insights?${params.toString()}`;
  const apiRows = await fetchAllPages(url, "BreakdownSync");

  return apiRows.map(row => {
    const breakdownValue = typeof breakdownConfig.valueField === "function"
      ? breakdownConfig.valueField(row)
      : row[breakdownConfig.valueField] || "unknown";

    return {
      date: new Date(row.date_start),
      campaignId: row.campaign_id, campaignName: row.campaign_name,
      adSetId: row.adset_id, adSetName: row.adset_name,
      adId: row.ad_id, adName: row.ad_name,
      breakdownType: breakdownConfig.type,
      breakdownValue,
      impressions: parseInt(row.impressions || "0"),
      clicks: parseInt(row.clicks || "0"),
      spend: parseFloat(row.spend || "0"),
      reach: parseInt(row.reach || "0"),
      conversions: parseActionValue(row.actions, "offsite_conversion.fb_pixel_purchase"),
      conversionValue: parseActionFloat(row.action_values, "offsite_conversion.fb_pixel_purchase"),
      linkClicks: parseActionValue(row.actions, "link_click"),
      landingPageViews: parseActionValue(row.actions, "landing_page_view"),
      addToCart: parseActionValue(row.actions, "offsite_conversion.fb_pixel_add_to_cart"),
      initiateCheckout: parseActionValue(row.actions, "offsite_conversion.fb_pixel_initiate_checkout"),
      viewContent: parseActionValue(row.actions, "offsite_conversion.fb_pixel_view_content"),
      outboundClicks: parseOutboundClicks(row.outbound_clicks),
    };
  });
}

// Batch upsert breakdown rows using $transaction.
//
// SQLite has a single writer lock. The historical backfill writes ~21k rows
// across 6 breakdown types — if multiple workers race the writer they pile up
// behind it and Prisma's per-query socket timeout (default 5s) fires, killing
// the whole phase. We mitigate this by:
//   - Small batches (DB_BATCH_SIZE=100): each transaction holds the writer
//     lock for a few hundred ms instead of seconds.
//   - Explicit per-tx timeout: 30s so a brief queue burst doesn't kill us.
//   - Per-batch retry on socket timeout: if we lose the race for the writer
//     lock, wait and retry the same batch instead of failing the whole phase.
async function batchUpsertBreakdowns(shopDomain, parsedRows, ratesByDate) {
  for (let i = 0; i < parsedRows.length; i += DB_BATCH_SIZE) {
    const batch = parsedRows.slice(i, i + DB_BATCH_SIZE);
    let attempt = 0;
    while (true) {
      try {
        await db.$transaction(
          batch.map(row => {
            const dateKey = row.date.toISOString().split("T")[0];
            const rate = ratesByDate[dateKey] || 1.0;
            convertMetaFields(row, rate);

            const insightData = {
              campaignName: row.campaignName, adSetName: row.adSetName, adName: row.adName,
              impressions: row.impressions, clicks: row.clicks, spend: row.spend, reach: row.reach,
              conversions: row.conversions, conversionValue: row.conversionValue,
              linkClicks: row.linkClicks, landingPageViews: row.landingPageViews,
              addToCart: row.addToCart, initiateCheckout: row.initiateCheckout,
              viewContent: row.viewContent, outboundClicks: row.outboundClicks,
            };

            return db.metaBreakdown.upsert({
              where: {
                shopDomain_date_adId_breakdownType_breakdownValue: {
                  shopDomain, date: row.date, adId: row.adId,
                  breakdownType: row.breakdownType, breakdownValue: row.breakdownValue,
                },
              },
              create: {
                shopDomain, date: row.date,
                campaignId: row.campaignId, adSetId: row.adSetId, adId: row.adId,
                breakdownType: row.breakdownType, breakdownValue: row.breakdownValue,
                ...insightData,
              },
              update: insightData,
            });
          }),
          { timeout: TX_TIMEOUT_MS, maxWait: TX_TIMEOUT_MS }
        );
        break;
      } catch (err) {
        const msg = err?.message || "";
        const isTransient = msg.includes("Socket timeout")
          || msg.includes("database is locked")
          || msg.includes("SQLITE_BUSY")
          || msg.includes("Transaction not found")
          || msg.includes("Transaction API error");
        if (!isTransient || attempt >= DB_RETRY_BACKOFF_MS.length) throw err;
        const wait = DB_RETRY_BACKOFF_MS[attempt];
        console.warn(`[BreakdownSync] DB upsert transient error (batch ${batch.length} rows, retry ${attempt + 1}/${DB_RETRY_BACKOFF_MS.length} in ${wait}ms): ${msg.slice(0, 120)}`);
        await new Promise(r => setTimeout(r, wait));
        attempt++;
      }
    }
  }
}

function buildRanges(days, size) {
  const ranges = [];
  for (let i = 0; i < days.length; i += size) {
    const chunk = days.slice(i, i + size);
    ranges.push({ since: chunk[0], until: chunk[chunk.length - 1], dayCount: chunk.length, days: chunk });
  }
  return ranges;
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

// Backward-compatible single-day fetch (used by incrementalSync)
export async function fetchBreakdown(metaAccessToken, metaAdAccountId, day, breakdownConfig) {
  return fetchBreakdownRange(metaAccessToken, metaAdAccountId, day, day, breakdownConfig);
}

export async function syncMetaBreakdowns(shopDomain, progressKey, daysBack = 7) {
  const key = progressKey || `syncBreakdowns:${shopDomain}`;

  const shop = await db.shop.findUnique({ where: { shopDomain } });
  if (!shop?.metaAccessToken || !shop?.metaAdAccountId) throw new Error("Meta Ads not connected");

  const { metaAccessToken, metaAdAccountId } = shop;
  const needsConversion = shop.metaCurrency !== shop.shopifyCurrency;

  const days = [];
  for (let i = daysBack; i >= 1; i--) {
    const d = new Date();
    d.setUTCDate(d.getUTCDate() - i);
    days.push(d.toISOString().split("T")[0]);
  }

  const ranges = buildRanges(days, BREAKDOWN_RANGE_SIZE);
  const totalWork = ranges.length * BREAKDOWN_CONFIGS.length;

  console.log(`[BreakdownSync] Starting for ${shopDomain}: ${days.length} days in ${ranges.length} ranges × ${BREAKDOWN_CONFIGS.length} breakdowns = ${totalWork} API batches, concurrency=${CONCURRENCY}`);

  // Bulk pre-fetch exchange rates (single ECB API call instead of N individual calls)
  let ratesByDate = {};
  if (needsConversion) {
    ratesByDate = await prefetchExchangeRates(days, shop.metaCurrency, shop.shopifyCurrency);
  }

  let totalRows = 0;
  let workCompleted = 0;
  const startTime = Date.now();

  // For each breakdown config, fetch all ranges in parallel batches
  for (const config of BREAKDOWN_CONFIGS) {
    for (let b = 0; b < ranges.length; b += CONCURRENCY) {
      const batch = ranges.slice(b, b + CONCURRENCY);

      setProgress(key, {
        status: "running",
        current: workCompleted,
        total: totalWork,
        unitLabel: "breakdowns",
        rowsImported: totalRows,
        detail: `Analysing ${humaniseBreakdown(config.type)}`,
      });

      const results = await Promise.all(
        batch.map(range => fetchBreakdownRange(metaAccessToken, metaAdAccountId, range.since, range.until, config))
      );

      // Write batch results to DB sequentially. SQLite is single-writer — racing
      // 10 result sets through the writer at once caused the "Socket timeout"
      // failures we saw during onboarding (Vollebak 2026-05-19 breakdowns-historical).
      // Sequential writes only cost us back the API-fetch parallelism, which is
      // negligible since fetches are network-bound.
      for (const rows of results) {
        await batchUpsertBreakdowns(shopDomain, rows, ratesByDate);
        totalRows += rows.length;
        workCompleted++;
      }
    }
  }

  const totalElapsed = formatElapsed(Date.now() - startTime);
  console.log(`[BreakdownSync] Complete: ${totalRows} breakdown rows in ${totalElapsed}`);
  return { totalRows };
}
