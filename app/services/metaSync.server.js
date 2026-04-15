import db from "../db.server";
import { setProgress, completeProgress, failProgress } from "./progress.server";
import { syncMetaBreakdowns } from "./metaBreakdownSync.server";
import { getExchangeRate, prefetchExchangeRates, convertMetaFields } from "./exchangeRate.server";
import { fetchAllPages, getMetaApiUsage, ReduceDataError } from "./metaFetch.server";

const CONCURRENCY = 10; // 10 parallel API calls per batch
const HOURLY_LIMIT_DAYS = 395; // ~13 months — Meta's limit for hourly time slot data
const DAILY_RANGE_SIZE = 90; // 90 days per API call for daily aggregates (Meta max)
const HOURLY_RANGE_SIZE = 14; // 14 days per hourly call — auto-splits to single days if Meta says too large
const DB_BATCH_SIZE = 500; // 500 rows per $transaction — SQLite handles this fine
const PAGE_LIMIT = 1000; // rows per API page — fewer round-trips

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

function parseVideoWatched(videoActions, pct) {
  if (!videoActions) return 0;
  for (const a of videoActions) {
    if (a.action_type === `video_view` && a.value) return 0;
    if (a.action_type === `video_p${pct}_watched_actions`) return parseInt(a.value || "0", 10);
  }
  return 0;
}

function parseRow(row, hourSlot) {
  return {
    date: new Date(row.date_start),
    hourSlot,
    campaignId: row.campaign_id, campaignName: row.campaign_name,
    adSetId: row.adset_id, adSetName: row.adset_name,
    adId: row.ad_id, adName: row.ad_name,
    impressions: parseInt(row.impressions || "0"), clicks: parseInt(row.clicks || "0"),
    spend: parseFloat(row.spend || "0"),
    conversions: parseActionValue(row.actions, "offsite_conversion.fb_pixel_purchase"),
    conversionValue: parseActionFloat(row.action_values, "offsite_conversion.fb_pixel_purchase"),
    reach: parseInt(row.reach || "0"), frequency: parseFloat(row.frequency || "0"),
    cpc: parseFloat(row.cpc || "0"), cpm: parseFloat(row.cpm || "0"),
    outboundClicks: parseOutboundClicks(row.outbound_clicks),
    linkClicks: parseActionValue(row.actions, "link_click"),
    landingPageViews: parseActionValue(row.actions, "landing_page_view"),
    addToCart: parseActionValue(row.actions, "offsite_conversion.fb_pixel_add_to_cart"),
    initiateCheckout: parseActionValue(row.actions, "offsite_conversion.fb_pixel_initiate_checkout"),
    viewContent: parseActionValue(row.actions, "offsite_conversion.fb_pixel_view_content"),
    videoP25: parseVideoWatched(row.video_p25_watched_actions, 25),
    videoP50: parseVideoWatched(row.video_p50_watched_actions, 50),
    videoP75: parseVideoWatched(row.video_p75_watched_actions, 75),
    videoP100: parseVideoWatched(row.video_p100_watched_actions, 100),
  };
}

// Batch upsert rows using $transaction for dramatically faster DB writes
async function batchUpsertInsights(shopDomain, parsedRows, ratesByDate) {
  for (let i = 0; i < parsedRows.length; i += DB_BATCH_SIZE) {
    const batch = parsedRows.slice(i, i + DB_BATCH_SIZE);
    await db.$transaction(
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
      })
    );
  }
}

// Fetch a date range as daily aggregates (no hourly breakdown) — used for data >13 months old
async function fetchDailyRange(metaAccessToken, metaAdAccountId, since, until, fields) {
  const params = new URLSearchParams({
    fields, level: "ad",
    time_range: JSON.stringify({ since, until }),
    time_increment: "1", limit: String(PAGE_LIMIT), action_report_time: "conversion",
    action_breakdowns: "action_type", use_unified_attribution_setting: "true",
    action_attribution_windows: JSON.stringify(["1d_view", "7d_click"]),
    access_token: metaAccessToken,
  });

  const url = `https://graph.facebook.com/v21.0/${metaAdAccountId}/insights?${params.toString()}`;
  const apiRows = await fetchAllPages(url, "MetaSync");
  return apiRows.map(row => parseRow(row, -1));
}

// Fetch a date range with hourly breakdowns — used for recent data
// If Meta returns "reduce data" error, automatically splits into single-day requests
async function fetchHourlyRange(metaAccessToken, metaAdAccountId, since, until, fields) {
  try {
    return await fetchHourlyRangeInner(metaAccessToken, metaAdAccountId, since, until, fields);
  } catch (err) {
    if (err instanceof ReduceDataError && since !== until) {
      // Split range into individual days and fetch each separately
      console.warn(`[MetaSync] Range ${since}→${until} too large, splitting into single days`);
      const days = [];
      const d0 = new Date(since + "T00:00:00Z"), d1 = new Date(until + "T00:00:00Z");
      for (let d = new Date(d0); d <= d1; d.setUTCDate(d.getUTCDate() + 1)) {
        days.push(d.toISOString().split("T")[0]);
      }
      const allParsed = [];
      for (const day of days) {
        const rows = await fetchHourlyRangeInner(metaAccessToken, metaAdAccountId, day, day, fields);
        allParsed.push(...rows);
      }
      return allParsed;
    }
    throw err;
  }
}

async function fetchHourlyRangeInner(metaAccessToken, metaAdAccountId, since, until, fields) {
  const params = new URLSearchParams({
    fields, breakdowns: "hourly_stats_aggregated_by_advertiser_time_zone", level: "ad",
    time_range: JSON.stringify({ since, until }),
    time_increment: "1", limit: String(PAGE_LIMIT), action_report_time: "conversion",
    action_breakdowns: "action_type", use_unified_attribution_setting: "true",
    action_attribution_windows: JSON.stringify(["1d_view", "7d_click"]),
    access_token: metaAccessToken,
  });

  const url = `https://graph.facebook.com/v21.0/${metaAdAccountId}/insights?${params.toString()}`;
  const apiRows = await fetchAllPages(url, "MetaSync");
  const parsed = [];
  for (const row of apiRows) {
    const hourSlot = parseInt(String(row.hourly_stats_aggregated_by_advertiser_time_zone).split(":")[0], 10);
    if (Number.isNaN(hourSlot) || hourSlot < 0 || hourSlot > 23) continue;
    parsed.push(parseRow(row, hourSlot));
  }
  return parsed;
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

export async function syncMetaInsights(shopDomain, daysBack = 7, progressKey = null) {
  const shop = await db.shop.findUnique({ where: { shopDomain } });
  if (!shop?.metaAccessToken || !shop?.metaAdAccountId) throw new Error("Meta Ads not connected");

  const { metaAccessToken, metaAdAccountId } = shop;
  const needsConversion = shop.metaCurrency !== shop.shopifyCurrency;

  const allDays = [];
  for (let i = daysBack; i >= 1; i--) {
    const d = new Date();
    d.setUTCDate(d.getUTCDate() - i);
    allDays.push(d.toISOString().split("T")[0]);
  }

  const dailyDays = allDays.filter((_, i) => (daysBack - i) > HOURLY_LIMIT_DAYS);
  const hourlyDays = allDays.filter((_, i) => (daysBack - i) <= HOURLY_LIMIT_DAYS);

  const dailyRanges = buildRanges(dailyDays, DAILY_RANGE_SIZE);
  const hourlyRanges = buildRanges(hourlyDays, HOURLY_RANGE_SIZE);
  const totalRanges = dailyRanges.length + hourlyRanges.length;

  console.log(`[MetaSync] Starting for ${shopDomain}: ${dailyDays.length} daily (${dailyRanges.length} ranges) + ${hourlyDays.length} hourly (${hourlyRanges.length} ranges), concurrency=${CONCURRENCY}`);

  const fields = [
    "date_start", "ad_id", "ad_name", "campaign_id", "campaign_name",
    "adset_id", "adset_name", "impressions", "clicks", "spend",
    "reach", "frequency", "cpc", "cpm",
    "actions", "action_values", "outbound_clicks",
    "video_p25_watched_actions", "video_p50_watched_actions",
    "video_p75_watched_actions", "video_p100_watched_actions",
  ].join(",");

  let totalRows = 0;
  let rangesCompleted = 0;
  const key = progressKey || `syncMeta:${shopDomain}`;
  const startTime = Date.now();

  if (needsConversion) console.log(`[MetaSync] Will convert ${shop.metaCurrency}→${shop.shopifyCurrency}`);

  // Pre-fetch exchange rates in bulk with progress reporting
  let ratesByDate = {};
  if (needsConversion) {
    setProgress(key, {
      status: "running", current: 0, total: totalRanges,
      message: `Step 1/3 · Fetching exchange rates (${shop.metaCurrency}→${shop.shopifyCurrency})...`,
    });
    ratesByDate = await prefetchExchangeRates(allDays, shop.metaCurrency, shop.shopifyCurrency, (msg) => {
      setProgress(key, {
        status: "running", current: 0, total: totalRanges,
        message: `Step 1/3 · Exchange rates: ${msg}`,
      });
    });
  }

  function updateProgress(phase, rangeLabel) {
    const pct = Math.round((rangesCompleted / totalRanges) * 100);
    const elapsed = Date.now() - startTime;
    const elapsedStr = formatElapsed(elapsed);
    let etaStr = "calculating...";
    if (rangesCompleted > 2) {
      const msPerRange = elapsed / rangesCompleted;
      const remaining = (totalRanges - rangesCompleted) * msPerRange;
      etaStr = `~${formatElapsed(remaining)} left`;
    }
    const usage = getMetaApiUsage();
    const rowRate = elapsed > 0 ? Math.round(totalRows / (elapsed / 1000)) : 0;
    setProgress(key, {
      status: "running",
      current: rangesCompleted,
      total: totalRanges,
      message: `Step 1/3 · Insights (${phase}): ${rangeLabel} · ${pct}% (${rangesCompleted}/${totalRanges} ranges) · ${totalRows.toLocaleString()} rows (${rowRate}/s) · ${elapsedStr}, ${etaStr} · API ${usage}%`,
    });
  }

  // Process daily ranges in parallel batches
  if (dailyRanges.length > 0) {
    console.log(`[MetaSync] Fetching ${dailyDays.length} days as daily aggregates in ${dailyRanges.length} ranges...`);
    for (let b = 0; b < dailyRanges.length; b += CONCURRENCY) {
      const batch = dailyRanges.slice(b, b + CONCURRENCY);
      updateProgress("daily", `${batch[0].since} → ${batch[batch.length - 1].until}`);

      const results = await Promise.all(
        batch.map(range => fetchDailyRange(metaAccessToken, metaAdAccountId, range.since, range.until, fields))
      );

      // Write all batch results to DB (sequential to avoid SQLite contention)
      for (let i = 0; i < results.length; i++) {
        await batchUpsertInsights(shopDomain, results[i], ratesByDate);
        totalRows += results[i].length;
        rangesCompleted++;
      }
      updateProgress("daily", `${batch[batch.length - 1].since} → ${batch[batch.length - 1].until}`);
    }
  }

  // Process hourly ranges in parallel batches
  if (hourlyRanges.length > 0) {
    console.log(`[MetaSync] Fetching ${hourlyDays.length} days with hourly breakdowns in ${hourlyRanges.length} ranges...`);
    for (let b = 0; b < hourlyRanges.length; b += CONCURRENCY) {
      const batch = hourlyRanges.slice(b, b + CONCURRENCY);
      updateProgress("hourly", `${batch[0].since} → ${batch[batch.length - 1].until}`);

      const results = await Promise.all(
        batch.map(range => fetchHourlyRange(metaAccessToken, metaAdAccountId, range.since, range.until, fields))
      );

      for (let i = 0; i < results.length; i++) {
        await batchUpsertInsights(shopDomain, results[i], ratesByDate);
        totalRows += results[i].length;
        rangesCompleted++;
      }
      updateProgress("hourly", `${batch[batch.length - 1].since} → ${batch[batch.length - 1].until}`);
    }
  }

  await db.shop.update({ where: { shopDomain }, data: { lastMetaSync: new Date() } });
  const totalElapsed = formatElapsed(Date.now() - startTime);
  console.log(`[MetaSync] Insights complete: ${totalRows} rows in ${totalElapsed} (${dailyRanges.length} daily + ${hourlyRanges.length} hourly ranges)`);
  return { totalRows };
}

// Combined sync: insights + breakdowns + entity sync
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
