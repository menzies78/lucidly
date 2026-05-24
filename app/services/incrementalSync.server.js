import db from "../db.server";
import { setProgress, completeProgress, failProgress } from "./progress.server";
import { BREAKDOWN_CONFIGS, fetchBreakdown } from "./metaBreakdownSync.server";
import { getExchangeRate, convertMetaFields } from "./exchangeRate.server";
import { fetchWithRetry, fetchAllPages as fetchAllPagesShared } from "./metaFetch.server";
import { invalidateShop } from "./queryCache.server";
import { shopLocalToday, shopDayBounds } from "../utils/shopTime.server";

const PADDING_MINUTES = 6;
const ROLLUP_REBUILD_MIN_INTERVAL_MS = 24 * 60 * 60 * 1000; // 24h

// Per-shop timestamp of the last full rollup rebuild, stored on globalThis so
// the value survives HMR but resets on a fresh process boot (a deploy should
// legitimately force one rebuild to pick up code/schema changes).
const lastRollupRebuildAt = /** @type {Map<string, number>} */ (
  globalThis.__lucidlyLastRollupRebuildAt || (globalThis.__lucidlyLastRollupRebuildAt = new Map())
);

// Track which yesterday dates we've already lookback-checked this process,
// so we don't re-fetch on every hourly cycle (saves memory + API calls).
const previousDayChecked = /** @type {Set<string>} */ (
  globalThis.__lucidlyPrevDayChecked || (globalThis.__lucidlyPrevDayChecked = new Set())
);

/**
 * Rebuild all three rollup tables for a shop, unless the previous rebuild was
 * less than 24 hours ago AND the caller has no new data to justify another
 * pass. New-conversion cycles always force a rebuild; idle cycles throttle to
 * one rebuild per day so the hourly scheduler doesn't saturate SQLite with
 * 13-minute rebuild storms whose output is identical to what's already there.
 *
 * @param {string} shopDomain
 * @param {{ force: boolean }} opts
 * @returns {Promise<boolean>} true if a rebuild actually ran
 */
async function rebuildAllRollups(shopDomain, { force }) {
  // Check in-memory timestamp first; if empty (fresh boot), check the DB.
  let last = lastRollupRebuildAt.get(shopDomain) || 0;
  if (last === 0) {
    const shop = await db.shop.findUnique({ where: { shopDomain }, select: { lastRollupRebuild: true } });
    if (shop?.lastRollupRebuild) {
      last = new Date(shop.lastRollupRebuild).getTime();
      lastRollupRebuildAt.set(shopDomain, last);
    }
  }
  const ageMs = Date.now() - last;
  if (!force && last > 0 && ageMs < ROLLUP_REBUILD_MIN_INTERVAL_MS) {
    const minutes = Math.round(ageMs / 60000);
    console.log(`[IncrementalSync] Skipping rollup rebuild for ${shopDomain} - last rebuild ${minutes}m ago, no new conversions`);
    return false;
  }
  // NOTE: this function runs fire-and-forget AFTER the user-facing sync has
  // already called completeProgress on `incrementalSync:${shopDomain}`.
  // Calling setProgress here would overwrite the "complete" state with
  // "running", and since rebuildAllRollups never calls completeProgress
  // itself, the UI would stay stuck at the last "Rebuilding..." message
  // forever. Background rollup status is tracked via the lastRollupRebuild
  // DB column, not the in-memory progress map. (Force Rebuild Rollups in
  // app._index.tsx uses a separate "forceRollups" task with its own
  // setProgress/completeProgress lifecycle - see actionType === "forceRollups".)
  console.log(`[IncrementalSync] Rebuilding product rollups for ${shopDomain}...`);
  try {
    const { rebuildProductRollups } = await import("./productRollups.server.js");
    await rebuildProductRollups(shopDomain);
  } catch (err) {
    console.error(`[IncrementalSync] Product rollup rebuild failed (non-fatal): ${err.message}`);
  }
  if (global.gc) global.gc();

  // Campaign rollups: ONLY when new conversions arrived (force=true).
  // This is the expensive one (~340s for Vollebak, 621k insight rows → 36k rollup rows).
  // Running it hourly with 0 new conversions monopolizes the SQLite writer lock
  // and makes the entire app unresponsive. The nightly 3am sweep (which always has
  // new data from the 7-day lookback) handles the daily refresh.
  if (force) {
    console.log(`[IncrementalSync] Rebuilding campaign rollups for ${shopDomain}...`);
    try {
      const { rebuildCampaignRollups } = await import("./campaignRollups.server.js");
      await rebuildCampaignRollups(shopDomain);
    } catch (err) {
      console.error(`[IncrementalSync] Campaign rollup rebuild failed (non-fatal): ${err.message}`);
    }
    if (global.gc) global.gc();

    console.log(`[IncrementalSync] Rebuilding ad demographic rollups for ${shopDomain}...`);
    try {
      const { rebuildAdDemographicRollups } = await import("./adDemographicRollups.server.js");
      await rebuildAdDemographicRollups(shopDomain);
    } catch (err) {
      console.error(`[IncrementalSync] Ad demographic rollup rebuild failed (non-fatal): ${err.message}`);
    }
    if (global.gc) global.gc();

    console.log(`[IncrementalSync] Rebuilding geo rollups for ${shopDomain}...`);
    try {
      const { rebuildGeoRollups } = await import("./geoRollups.server.js");
      await rebuildGeoRollups(shopDomain);
    } catch (err) {
      console.error(`[IncrementalSync] Geo rollup rebuild failed (non-fatal): ${err.message}`);
    }
    if (global.gc) global.gc();

    console.log(`[IncrementalSync] Rebuilding dashboard match accuracy for ${shopDomain}...`);
    try {
      const { rebuildMatchAccuracy } = await import("./dashboardRollups.server.js");
      await rebuildMatchAccuracy(shopDomain);
    } catch (err) {
      console.error(`[IncrementalSync] Match accuracy rebuild failed (non-fatal): ${err.message}`);
    }
    if (global.gc) global.gc();
  }

  console.log(`[IncrementalSync] Rebuilding customer rollups for ${shopDomain}...`);
  try {
    const { rebuildCustomerSegments, rebuildCustomerRollups, rebuildCustomerGenderDaily } = await import("./customerRollups.server.js");
    await rebuildCustomerSegments(shopDomain);
    await rebuildCustomerRollups(shopDomain);
    await rebuildCustomerGenderDaily(shopDomain);
  } catch (err) {
    console.error(`[IncrementalSync] Customer rollup rebuild failed (non-fatal): ${err.message}`);
  }
  if (global.gc) global.gc();
  const now = Date.now();
  lastRollupRebuildAt.set(shopDomain, now);
  // Persist to DB so OOM restarts don't trigger unnecessary rebuilds
  await db.shop.update({ where: { shopDomain }, data: { lastRollupRebuild: new Date(now) } }).catch(() => {});
  return true;
}
// Fallback padding - tried only when the 6-minute window returned no
// Shopify candidates for a conversion. Starting at 6 keeps the candidate
// set tight for the common case; widening to 10 rescues the occasional
// order where the pixel fired ≥7 minutes after checkout.
const WIDE_PADDING_MINUTES = 10;
const REVENUE_TOLERANCE = 0.02;

/**
 * Get the UTC offset in minutes for a given IANA timezone on a specific date.
 * E.g. "Europe/London" in summer (BST) = 60, "America/Los_Angeles" in winter = -480
 */
function getTimezoneOffsetMinutes(timezone, dateStr) {
  if (!timezone) return 0;
  try {
    // Create a date at noon on the given day to avoid DST boundary edge cases
    const dt = new Date(`${dateStr}T12:00:00Z`);
    // Format in the target timezone to find its local time
    const parts = new Intl.DateTimeFormat("en-US", {
      timeZone: timezone,
      hour: "numeric", minute: "numeric", hour12: false,
      year: "numeric", month: "numeric", day: "numeric",
    }).formatToParts(dt);
    const hour = parseInt(parts.find(p => p.type === "hour")?.value || "12", 10);
    const minute = parseInt(parts.find(p => p.type === "minute")?.value || "0", 10);
    // UTC was 12:00, local is hour:minute → offset = (local - UTC) in minutes
    return (hour * 60 + minute) - (12 * 60);
  } catch {
    return 0;
  }
}

/**
 * Convert a Meta hour slot from Meta's timezone to UTC minutes range.
 * Meta reports hourly_stats_aggregated_by_advertiser_time_zone - we need UTC
 * to compare against Shopify order.createdAt (which is UTC).
 *
 * Padding is backward-only: the Shopify order is placed BEFORE Meta logs the
 * conversion (pixel fires after checkout). So an order a few minutes before
 * the Meta hour could have its conversion logged in that hour. But an order
 * placed after the Meta hour would be logged in a later hour.
 */
function hourToMinuteRange(hour, metaOffsetMinutes = 0, paddingMinutes = PADDING_MINUTES) {
  // Convert Meta local hour to UTC minutes
  let utcStart = hour * 60 - metaOffsetMinutes;
  let utcEnd = utcStart + 59;
  // Backward padding only - order is placed before Meta logs the conversion
  utcStart -= paddingMinutes;
  // Wrap around midnight
  if (utcStart < 0) utcStart += 1440;
  if (utcEnd < 0) utcEnd += 1440;
  if (utcStart >= 1440) utcStart -= 1440;
  if (utcEnd >= 1440) utcEnd -= 1440;
  return { start: utcStart, end: utcEnd };
}

function minuteInRange(minute, start, end) {
  if (start <= end) return minute >= start && minute <= end;
  return minute >= start || minute <= end;
}

function dateToMinute(date) {
  return date.getUTCHours() * 60 + date.getUTCMinutes();
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

function parseVideoWatched(videoActions) {
  if (!videoActions) return 0;
  for (const a of videoActions) {
    if (a.value) return parseInt(a.value || "0", 10);
  }
  return 0;
}

async function fetchTodayMeta(metaAccessToken, metaAdAccountId, today) {
  const fields = [
    "date_start", "ad_id", "ad_name", "campaign_id", "campaign_name",
    "adset_id", "adset_name", "impressions", "clicks", "spend",
    "reach", "frequency", "cpc", "cpm",
    "actions", "action_values", "outbound_clicks",
    "video_p25_watched_actions", "video_p50_watched_actions",
    "video_p75_watched_actions", "video_p100_watched_actions",
  ].join(",");
  const breakdown = "hourly_stats_aggregated_by_advertiser_time_zone";

  const params = new URLSearchParams({
    fields, breakdowns: breakdown, level: "ad",
    time_range: JSON.stringify({ since: today, until: today }),
    time_increment: "1", limit: "200", action_report_time: "conversion",
    action_breakdowns: "action_type", use_unified_attribution_setting: "true",
    action_attribution_windows: JSON.stringify(["1d_view", "7d_click"]),
    access_token: metaAccessToken,
  });

  let url = `https://graph.facebook.com/v21.0/${metaAdAccountId}/insights?${params.toString()}`;
  const rows = [];

  while (url) {
    const data = await fetchWithRetry(url);
    if (!data.data) break;
    for (const row of (data.data || [])) {
      const hourRaw = row.hourly_stats_aggregated_by_advertiser_time_zone;
      const hourSlot = parseInt(String(hourRaw).split(":")[0], 10);
      if (Number.isNaN(hourSlot) || hourSlot < 0 || hourSlot > 23) continue;

      rows.push({
        adId: row.ad_id, adName: row.ad_name, campaignId: row.campaign_id,
        campaignName: row.campaign_name, adSetId: row.adset_id, adSetName: row.adset_name,
        impressions: parseInt(row.impressions || "0"), clicks: parseInt(row.clicks || "0"),
        spend: parseFloat(row.spend || "0"), hourSlot,
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
        videoP25: parseVideoWatched(row.video_p25_watched_actions),
        videoP50: parseVideoWatched(row.video_p50_watched_actions),
        videoP75: parseVideoWatched(row.video_p75_watched_actions),
        videoP100: parseVideoWatched(row.video_p100_watched_actions),
      });
    }
    url = data.paging?.next || null;
  }
  return rows;
}

async function findNewConversions(shopDomain, today, currentData) {
  const prevSnapshots = await db.metaSnapshot.findMany({ where: { shopDomain, date: today } });
  const prevMap = {};
  for (const s of prevSnapshots) prevMap[`${s.adId}|${s.hourSlot}`] = { conversions: s.conversions, conversionValue: s.conversionValue };

  const newConversions = [];
  for (const row of currentData) {
    if (row.conversions === 0) continue;
    const key = `${row.adId}|${row.hourSlot}`;
    const prev = prevMap[key] || { conversions: 0, conversionValue: 0 };
    const deltaConv = row.conversions - prev.conversions;
    const deltaValue = row.conversionValue - prev.conversionValue;
    if (deltaConv > 0) newConversions.push({ ...row, deltaConversions: deltaConv, deltaValue: Math.round(deltaValue * 100) / 100 });
  }
  return newConversions;
}

async function saveSnapshot(shopDomain, today, currentData) {
  // The snapshot represents MATCHED conversions, not Meta-reported conversions.
  // Subtract any unmatched placeholders for this ad/slot so that if a delayed Shopify
  // order arrives later (e.g. webhook lag), the next cycle still sees delta>0 and retries
  // the match. Without this, the snapshot advances past the slot and the gap becomes
  // permanently invisible to subsequent hourly cycles (race observed 2026-05-12 VBK1142756).
  for (const row of currentData) {
    if (row.conversions === 0) continue;
    const placeholderCount = await db.attribution.count({
      where: {
        shopDomain,
        confidence: 0,
        shopifyOrderId: { startsWith: `unmatched_${row.adId}_${today}_${row.hourSlot}_` },
      },
    });
    const effectiveConv = Math.max(0, row.conversions - placeholderCount);
    const effectiveValue = row.conversions > 0
      ? Math.round(row.conversionValue * (effectiveConv / row.conversions) * 100) / 100
      : 0;
    await db.metaSnapshot.upsert({
      where: { shopDomain_date_adId_hourSlot: { shopDomain, date: today, adId: row.adId, hourSlot: row.hourSlot } },
      create: { shopDomain, date: today, adId: row.adId, hourSlot: row.hourSlot, conversions: effectiveConv, conversionValue: effectiveValue },
      update: { conversions: effectiveConv, conversionValue: effectiveValue },
    });
  }
}

/**
 * Fetch today's country breakdown at ad level (one row per ad×country, daily totals).
 * Used together with MetaCountrySnapshot to derive per-cycle country deltas.
 */
async function fetchTodayCountryBreakdown(metaAccessToken, metaAdAccountId, today) {
  const fields = ["ad_id", "actions", "action_values"].join(",");
  const params = new URLSearchParams({
    fields, breakdowns: "country", level: "ad",
    time_range: JSON.stringify({ since: today, until: today }),
    time_increment: "1", limit: "1000", action_report_time: "conversion",
    action_breakdowns: "action_type", use_unified_attribution_setting: "true",
    action_attribution_windows: JSON.stringify(["1d_view", "7d_click"]),
    access_token: metaAccessToken,
  });
  let url = `https://graph.facebook.com/v21.0/${metaAdAccountId}/insights?${params.toString()}`;
  const rows = [];
  while (url) {
    const data = await fetchWithRetry(url);
    if (!data.data) break;
    for (const row of (data.data || [])) {
      rows.push({
        adId: row.ad_id,
        country: (row.country || "").toUpperCase(),
        conversions: parseActionValue(row.actions, "offsite_conversion.fb_pixel_purchase"),
        conversionValue: parseActionFloat(row.action_values, "offsite_conversion.fb_pixel_purchase"),
      });
    }
    url = data.paging?.next || null;
  }
  return rows;
}

/**
 * Diff current country totals vs last MetaCountrySnapshot. Returns a map keyed by
 * adId → Set<country> of countries whose conversion count increased this cycle.
 * When exactly one country ticked up for an ad, that's deterministic per-conversion
 * country attribution. When several did, each is included - the matcher treats any
 * of them as a strong signal (rank 2).
 */
async function computeCountryDeltas(shopDomain, today, currentRows) {
  const prev = await db.metaCountrySnapshot.findMany({ where: { shopDomain, date: today } });
  const prevMap = {};
  for (const s of prev) prevMap[`${s.adId}|${s.country}`] = s.conversions;

  const deltasByAd = {};
  for (const row of currentRows) {
    if (!row.country) continue;
    const key = `${row.adId}|${row.country}`;
    const prevC = prevMap[key] || 0;
    if (row.conversions > prevC) {
      if (!deltasByAd[row.adId]) deltasByAd[row.adId] = new Set();
      deltasByAd[row.adId].add(row.country);
    }
  }
  return deltasByAd;
}

async function saveCountrySnapshot(shopDomain, today, currentRows) {
  for (const row of currentRows) {
    if (!row.country) continue;
    if (row.conversions === 0) continue;
    await db.metaCountrySnapshot.upsert({
      where: {
        shopDomain_date_adId_country: {
          shopDomain, date: today, adId: row.adId, country: row.country,
        },
      },
      create: {
        shopDomain, date: today, adId: row.adId, country: row.country,
        conversions: row.conversions, conversionValue: row.conversionValue,
      },
      update: { conversions: row.conversions, conversionValue: row.conversionValue },
    });
  }
}

async function saveInsights(shopDomain, today, currentData) {
  for (const row of currentData) {
    const date = new Date(today);
    const insightData = {
      campaignName: row.campaignName, adSetName: row.adSetName, adName: row.adName,
      impressions: row.impressions, clicks: row.clicks, spend: row.spend,
      conversions: row.conversions, conversionValue: row.conversionValue,
      reach: row.reach, frequency: row.frequency, cpc: row.cpc, cpm: row.cpm,
      outboundClicks: row.outboundClicks, linkClicks: row.linkClicks,
      landingPageViews: row.landingPageViews, addToCart: row.addToCart,
      initiateCheckout: row.initiateCheckout, viewContent: row.viewContent,
      videoP25: row.videoP25, videoP50: row.videoP50,
      videoP75: row.videoP75, videoP100: row.videoP100,
    };
    await db.metaInsight.upsert({
      where: { shopDomain_date_hourSlot_adId: { shopDomain, date, hourSlot: row.hourSlot, adId: row.adId } },
      create: {
        shopDomain, date, hourSlot: row.hourSlot,
        campaignId: row.campaignId, adSetId: row.adSetId, adId: row.adId,
        ...insightData,
      },
      update: insightData,
    });
  }
}

const RIVAL_VALUE_TOLERANCE = 0.02;

/**
 * LAYER 1 - UTM ground truth pass.
 *
 * Runs BEFORE the Layer 2 statistical matcher. For each order with
 * utmConfirmedMeta=true, writes an Attribution row (layer=1, confidence=100,
 * matchMethod="utm") using the order's already-linked metaAdId/campaign hierarchy.
 *
 * If the order fits a Meta conversion slot reported in this cycle (same adId,
 * same hour window), we DECREMENT that conv's deltaConversions/deltaValue so
 * Layer 2 solves the residual. If no slot exists (Meta didn't report this
 * conversion - common: dropped pixel, iOS, ad-blocker) we still write the
 * Layer 1 row - it's a confirmed Meta order regardless.
 *
 * Existing confident attributions on the same orderId are UPGRADED to Layer 1
 * (UTM is authoritative over statistical guesses).
 *
 * @returns { layer1Written: number, slotCreditsConsumed: number }
 */
export async function runUtmLayer1Pass(shopDomain, dayStr, dayOrders, newConversions, metaOffsetMinutes) {
  const utmOrders = dayOrders.filter(o =>
    o.utmConfirmedMeta === true && ((o.metaAdId && o.metaAdId.length > 0) || (o.metaAdIdFromUtm && o.metaAdIdFromUtm.length > 0))
  );
  if (utmOrders.length === 0) return { layer1Written: 0, slotCreditsConsumed: 0 };

  // Build an ad-catalog lookup so we can validate that whatever IDs were
  // captured at order-import time actually resolve to a real Ad. Vollebak's
  // UTM template puts the AdSet ID in utm_term, which previously got stored
  // as metaAdIdFromUtm and then written here as the Layer 1 metaAdId - a
  // ghost ID that never resolves anywhere downstream. Caught via VBK1142602.
  const adInsightRows = await db.metaInsight.findMany({
    where: { shopDomain },
    select: { adId: true, adName: true },
    distinct: ["adId"],
  });
  const validAdIds = new Set();
  const adNameToId = {};
  for (const row of adInsightRows) {
    if (row.adId) validAdIds.add(row.adId);
    if (row.adName && row.adId && !adNameToId[row.adName]) adNameToId[row.adName] = row.adId;
  }
  const resolveAdId = (order) => {
    if (order.metaAdId && validAdIds.has(order.metaAdId)) return order.metaAdId;
    if (order.metaAdIdFromUtm && validAdIds.has(order.metaAdIdFromUtm)) return order.metaAdIdFromUtm;
    if (order.utmContent && adNameToId[order.utmContent]) return adNameToId[order.utmContent];
    if (order.metaAdName && adNameToId[order.metaAdName]) return adNameToId[order.metaAdName];
    return null;
  };

  // Precompute slot ranges for each conv so we don't recompute per order.
  const convsByAd = {};
  for (const conv of newConversions) {
    if (!conv.adId) continue;
    if (!convsByAd[conv.adId]) convsByAd[conv.adId] = [];
    const { start, end } = hourToMinuteRange(conv.hourSlot, metaOffsetMinutes);
    convsByAd[conv.adId].push({ conv, start, end });
  }

  // Skip orders that already have ANY confident attribution. This covers:
  //   (a) UTM rows written in prior cycles (idempotent skip), and
  //   (b) statistical rows that overrode a prior UTM when the Layer 2 pick
  //       was unambiguous - re-running Layer 1 over those would undo the
  //       override and flip the attribution back to UTM every cycle.
  const orderIdList = utmOrders.map(o => o.shopifyOrderId);
  const existing = await db.attribution.findMany({
    where: { shopDomain, shopifyOrderId: { in: orderIdList } },
    select: { shopifyOrderId: true, confidence: true },
  });
  const alreadyAttributed = new Set(
    existing.filter(a => a.confidence > 0).map(a => a.shopifyOrderId)
  );

  let layer1Written = 0;
  let slotCreditsConsumed = 0;

  for (const order of utmOrders) {
    if (alreadyAttributed.has(order.shopifyOrderId)) continue;

    const orderTotal = order.frozenTotalPrice || 0;
    const orderMinute = dateToMinute(order.createdAt);
    // Effective ad ID: only trust an ID that resolves to a real ad in
    // MetaInsight. Falls through to utm_content (ad name) lookup, mirroring
    // utmLinkage.server.js. If nothing resolves we still write the Layer 1
    // row (utmConfirmedMeta is a strong signal) but with metaAdId=null so
    // the statistical matcher can fill in the right ad later instead of us
    // poisoning the Attribution row with an AdSet ID dressed up as an Ad ID.
    const effectiveAdId = resolveAdId(order);

    // Try to find a Meta conversion slot this cycle for the same ad whose
    // time window contains this order. Prefer slots with remaining capacity.
    const adConvs = effectiveAdId ? (convsByAd[effectiveAdId] || []) : [];
    let claimedSlot = null;
    for (const entry of adConvs) {
      if (entry.conv.deltaConversions <= 0) continue;
      if (minuteInRange(orderMinute, entry.start, entry.end)) {
        claimedSlot = entry.conv;
        break;
      }
    }

    // Capture the per-conversion Meta value BEFORE decrementing the slot
    let metaPerConv = 0;
    if (claimedSlot) {
      metaPerConv = claimedSlot.deltaConversions > 0
        ? Math.round((claimedSlot.deltaValue / claimedSlot.deltaConversions) * 100) / 100 : 0;
      claimedSlot.deltaConversions -= 1;
      claimedSlot.deltaValue = Math.max(0, claimedSlot.deltaValue - orderTotal);
      slotCreditsConsumed++;
    }

    await db.attribution.upsert({
      where: { shopDomain_shopifyOrderId: { shopDomain, shopifyOrderId: order.shopifyOrderId } },
      create: {
        shopDomain, shopifyOrderId: order.shopifyOrderId,
        layer: 1, confidence: 100, rivalCount: 0,
        metaCampaignId: order.metaCampaignId || claimedSlot?.campaignId || null,
        metaCampaignName: order.metaCampaignName || claimedSlot?.campaignName || null,
        metaAdSetId: order.metaAdSetId || claimedSlot?.adSetId || null,
        metaAdSetName: order.metaAdSetName || claimedSlot?.adSetName || null,
        metaAdId: effectiveAdId || claimedSlot?.adId || null,
        metaAdName: order.metaAdName || claimedSlot?.adName || null,
        isNewCustomer: order.customerOrderCountAtPurchase === 1, isNewToMeta: order.customerOrderCountAtPurchase === 1,
        matchMethod: "utm",
        metaConversionValue: metaPerConv,
      },
      update: {
        layer: 1, confidence: 100, rivalCount: 0,
        metaCampaignId: order.metaCampaignId || claimedSlot?.campaignId || null,
        metaCampaignName: order.metaCampaignName || claimedSlot?.campaignName || null,
        metaAdSetId: order.metaAdSetId || claimedSlot?.adSetId || null,
        metaAdSetName: order.metaAdSetName || claimedSlot?.adSetName || null,
        metaAdId: effectiveAdId || claimedSlot?.adId || null,
        metaAdName: order.metaAdName || claimedSlot?.adName || null,
        isNewCustomer: order.customerOrderCountAtPurchase === 1, isNewToMeta: order.customerOrderCountAtPurchase === 1,
        matchMethod: "utm",
        metaConversionValue: metaPerConv,
      },
    });
    layer1Written++;

    // If we consumed a slot, any stale phantom placeholder for it should clear.
    if (claimedSlot && claimedSlot.deltaConversions === 0) {
      await deleteUnmatchedPlaceholders(shopDomain, claimedSlot.adId, dayStr, claimedSlot.hourSlot);
    }
  }

  if (layer1Written > 0) {
    console.log(`[Layer1] ${dayStr}: ${layer1Written} UTM attributions written, ${slotCreditsConsumed} slot credits consumed`);
  }
  return { layer1Written, slotCreditsConsumed };
}

/**
 * Upsert one unmatched placeholder row per missing Meta conversion for an ad-hour.
 * These rows surface in Order Explorer as "unmatched" entries. They use a deterministic
 * key so re-runs don't duplicate them, and they're cleaned up by deleteUnmatchedPlaceholders
 * as soon as a later cycle successfully matches the slot.
 */
async function upsertUnmatchedPlaceholders(shopDomain, conv, dateStr) {
  const perConvValue = conv.deltaConversions > 0
    ? Math.round((conv.deltaValue / conv.deltaConversions) * 100) / 100
    : conv.deltaValue;
  for (let ui = 0; ui < Math.max(conv.deltaConversions, 1); ui++) {
    const unmatchedKey = `unmatched_${conv.adId}_${dateStr}_${conv.hourSlot}_c${ui + 1}`;
    await db.attribution.upsert({
      where: { shopDomain_shopifyOrderId: { shopDomain, shopifyOrderId: unmatchedKey } },
      create: {
        shopDomain, shopifyOrderId: unmatchedKey,
        layer: 2, confidence: 0,
        metaCampaignId: conv.campaignId, metaCampaignName: conv.campaignName,
        metaAdSetId: conv.adSetId, metaAdSetName: conv.adSetName,
        metaAdId: conv.adId, metaAdName: conv.adName, matchMethod: "none",
        metaConversionValue: perConvValue,
      },
      update: {},
    });
  }
}

/**
 * Delete stale unmatched placeholder rows for a given ad-hour. Called after a
 * successful match lands, so a slot that was temporarily unmatched at cycle N
 * doesn't leave phantom rows in Order Explorer after being matched at cycle N+1.
 */
async function deleteUnmatchedPlaceholders(shopDomain, adId, dateStr, hourSlot) {
  await db.attribution.deleteMany({
    where: {
      shopDomain,
      confidence: 0,
      shopifyOrderId: { startsWith: `unmatched_${adId}_${dateStr}_${hourSlot}_` },
    },
  });
}

/**
 * Post-match confidence recalculation.
 *
 * After all conversions for a sync run are matched, some orders may have
 * rivalCount > 0 because at the time of matching, another candidate existed
 * for the same slot. But if that rival was subsequently matched to a DIFFERENT
 * conversion, the rivalry is resolved and confidence should be 100%.
 *
 * This function loads all newly matched attributions, checks each rival, and
 * upgrades confidence where all rivals were independently matched.
 */
async function recalculateConfidence(shopDomain, matchedOrderIds) {
  if (matchedOrderIds.length === 0) return 0;

  // Load all attributions that have rivals
  const attrsWithRivals = await db.attribution.findMany({
    where: {
      shopDomain,
      shopifyOrderId: { in: matchedOrderIds },
      rivalCount: { gt: 0 },
      confidence: { gt: 0 },
    },
  });

  if (attrsWithRivals.length === 0) return 0;

  // Build the set of ALL matched order IDs (not just from this run - globally)
  const allMatchedAttrs = await db.attribution.findMany({
    where: { shopDomain, confidence: { gt: 0 } },
    select: { shopifyOrderId: true },
  });
  const globallyMatched = new Set(allMatchedAttrs.map(a => a.shopifyOrderId));

  // For each attribution with rivals, load the orders that were candidates
  // and check if they're now matched. We need the original candidate info.
  // Since we don't store the full candidate list, we use a simpler heuristic:
  // if rivalCount > 0 but ALL orders for the same ad+day are now matched,
  // then all rivals were resolved → confidence = 100%.
  let upgraded = 0;
  for (const attr of attrsWithRivals) {
    // Find all attributions for the same ad on the same day
    const sameAdAttrs = await db.attribution.findMany({
      where: {
        shopDomain,
        metaAdId: attr.metaAdId,
        confidence: { gt: 0 },
        // Same day: match by matchedAt date (approximate)
        matchedAt: {
          gte: new Date(attr.matchedAt.getTime() - 86400000),
          lte: new Date(attr.matchedAt.getTime() + 86400000),
        },
      },
      select: { shopifyOrderId: true },
    });
    const matchedForThisAd = sameAdAttrs.length;

    // Get how many conversions Meta reported for this ad on this day
    // (we stored this as the total from the insight rows)
    // Simplified check: if the number of matched orders for this ad >= the
    // number of rivals + 1 (the pick itself), all candidates were absorbed.
    if (matchedForThisAd > attr.rivalCount) {
      // All rivals were matched to something → confidence = 100%
      await db.attribution.update({
        where: { id: attr.id },
        data: { confidence: 100, rivalCount: 0 },
      });
      upgraded++;
    }
  }

  if (upgraded > 0) {
    console.log(`[ConfidenceRecalc] Upgraded ${upgraded} attributions to 100% confidence (rivals resolved)`);
  }
  return upgraded;
}

async function matchSingleConversion(shopDomain, conv, todayOrders, revenueField, metaOffsetMinutes = 0, metaSpendCountries = null, adCountryDeltas = null) {
  // UTM Layer 1 attributions remain eligible candidates so the statistical
  // matcher can overwrite them when its pick is unambiguous (rivalCount=0).
  // Non-UTM confident attributions are excluded as before.
  const allAttrs = await db.attribution.findMany({
    where: { shopDomain, confidence: { gt: 0 }, NOT: { matchMethod: "utm" } },
    select: { shopifyOrderId: true },
  });
  const allMatchedIds = new Set(allAttrs.map(a => a.shopifyOrderId));

  // Build candidates with a given backward-padding window.
  const buildCandidates = (paddingMinutes) => {
    const { start, end } = hourToMinuteRange(conv.hourSlot, metaOffsetMinutes, paddingMinutes);
    const candidates = [];
    for (const order of todayOrders) {
      if (allMatchedIds.has(order.shopifyOrderId)) continue;
      const orderMinute = dateToMinute(order.createdAt);
      if (minuteInRange(orderMinute, start, end) === false) continue;
      const orderTotal = revenueField === "subtotal_price" ? order.frozenSubtotalPrice : order.frozenTotalPrice;
      // Country rank:
      //   2 = order country appears in this ad's per-cycle country delta (deterministic)
      //   1 = order country is one this ad had day-level spend in (soft signal) - also when no data
      //   0 = order country is NOT in the ad's day-level spend set
      const orderCountry = (order.countryCode || "").toUpperCase();
      const deltaSet = adCountryDeltas ? adCountryDeltas[conv.adId] : null;
      let countryRank;
      if (deltaSet && deltaSet.size > 0 && orderCountry && deltaSet.has(orderCountry)) {
        countryRank = 2;
      } else if (!metaSpendCountries || metaSpendCountries.size === 0 || !orderCountry || metaSpendCountries.has(orderCountry)) {
        countryRank = 1;
      } else {
        countryRank = 0;
      }
      candidates.push({
        id: order.id, orderId: order.shopifyOrderId, total: orderTotal,
        isNew: order.customerOrderCountAtPurchase === 1, time: order.createdAt,
        customerId: order.shopifyCustomerId, countryRank,
        // Keep boolean for any legacy consumers
        countryMatch: countryRank >= 1,
      });
    }
    return candidates;
  };

  // Try the tight window first. Only widen when there is genuinely nothing
  // to match, so the common case keeps its small candidate set.
  let paddingUsed = PADDING_MINUTES;
  let allCandidates = buildCandidates(PADDING_MINUTES);
  if (allCandidates.length === 0) {
    const wide = buildCandidates(WIDE_PADDING_MINUTES);
    if (wide.length > 0) {
      paddingUsed = WIDE_PADDING_MINUTES;
      allCandidates = wide;
      console.log(`[DeltaMatch] Widened window to ${WIDE_PADDING_MINUTES}min for ad ${conv.adId} hour ${conv.hourSlot}: ${wide.length} candidates`);
    }
  }
  // Time window actually used by the candidate set - referenced by the
  // zero-value £0 non-online fallback path below.
  const { start, end } = hourToMinuteRange(conv.hourSlot, metaOffsetMinutes, paddingUsed);

  if (allCandidates.length === 0) {
    console.log(`[DeltaMatch] REJECT ad=${conv.adId} hour=${conv.hourSlot} R=${conv.deltaConversions} value=${conv.deltaValue?.toFixed?.(2)}: no orders in time window (even after widening to ${WIDE_PADDING_MINUTES}min)`);
    return [];
  }

  // Zero-value conversions: Meta reports a purchase count but no monetary value.
  // Match against £0 Shopify orders (replacement orders) by time window only.
  // These are often placed via Shopify admin (isOnlineStore=false), so we also
  // fetch non-online £0 orders that aren't already matched.
  if (conv.deltaValue === 0 && conv.deltaConversions > 0) {
    // Start with any £0 candidates from the online orders
    let zeroCandidates = allCandidates.filter(c => c.total === 0);

    // Also fetch non-online £0 orders for the same day + time window.
    // Use shop-local day bounds so a 00:20 BST order is not excluded as "yesterday UTC".
    const shopTzLocal = (await db.shop.findUnique({ where: { shopDomain }, select: { shopifyTimezone: true } }))?.shopifyTimezone || "UTC";
    const dayBounds = shopDayBounds(shopTzLocal, shopLocalToday(shopTzLocal));
    const nonOnlineZero = await db.order.findMany({
      where: {
        shopDomain,
        isOnlineStore: false,
        frozenTotalPrice: 0,
        createdAt: { gte: dayBounds.gte, lte: dayBounds.lte },
      },
    });
    for (const order of nonOnlineZero) {
      if (allMatchedIds.has(order.shopifyOrderId)) continue;
      const orderMinute = dateToMinute(order.createdAt);
      if (!minuteInRange(orderMinute, start, end)) continue;
      zeroCandidates.push({
        id: order.id, orderId: order.shopifyOrderId, total: 0,
        isNew: order.customerOrderCountAtPurchase === 1, time: order.createdAt,
        customerId: order.shopifyCustomerId,
      });
    }

    // Prefer higher country rank when selecting zero-value candidates
    zeroCandidates.sort((a, b) => (b.countryRank || 0) - (a.countryRank || 0));
    const matched = zeroCandidates.slice(0, conv.deltaConversions);
    if (matched.length === 0) {
      console.log(`[DeltaMatch] REJECT ad=${conv.adId} hour=${conv.hourSlot} R=${conv.deltaConversions} value=0: zero-value conv but no £0 orders in window`);
      return [];
    }
    const pickedIds = new Set(matched.map(p => p.id));
    return matched.map(pick => {
      let rivalCount = 0;
      for (const cand of zeroCandidates) {
        if (pickedIds.has(cand.id)) continue;
        // Country disqualification: if the pick matches Meta's reported
        // country (rank > 0) and the candidate doesn't, the candidate
        // isn't a viable substitute.
        if ((pick.countryRank || 0) > 0 && (cand.countryRank || 0) < (pick.countryRank || 0)) continue;
        rivalCount++;
      }
      return {
        ...pick,
        confidence: Math.max(1, Math.round(100 / (1 + rivalCount))),
        rivalCount,
        diffPct: 0,
      };
    });
  }

  // R=1 fast path - the common case with hourly data. Meta reports one conversion
  // in one hour slot → we look for the single Shopify order whose total is closest
  // to the Meta value within tolerance. No combinatorial search, no averaging.
  const target = conv.deltaValue;
  // Tighter tolerance for R=1: 2% default (was 5%). Currency conversion drift
  // between Meta and Shopify is typically 0.3-0.8%; 2% gives comfortable room
  // without allowing false positives.
  const tolerance = 0.02;
  if (conv.deltaConversions === 1) {
    let best = null, bestDiff = Infinity, bestCountryRank = -1, bestIsNew = -1;
    for (const cand of allCandidates) {
      if (cand.total <= 0) continue;
      const diff = Math.abs(cand.total - target);
      if (diff > target * tolerance) continue;
      const rank = cand.countryRank || 0;
      const isNew = cand.isNew ? 1 : 0;
      // Priority: closer value → higher country rank → new customer
      const EPS = 1e-9;
      if (
        diff < bestDiff - EPS ||
        (Math.abs(diff - bestDiff) <= EPS && rank > bestCountryRank) ||
        (Math.abs(diff - bestDiff) <= EPS && rank === bestCountryRank && isNew > bestIsNew)
      ) {
        best = cand; bestDiff = diff; bestCountryRank = rank; bestIsNew = isNew;
      }
    }
    if (!best) {
      // Surface the closest near-miss so we can tell whether the issue is
      // tolerance, currency drift, refunds/edits, or just wrong ad. Helps
      // diagnose cases where Meta reported a conversion but the matcher
      // couldn't find a Shopify order at the expected value.
      let closest = null, closestDiff = Infinity;
      for (const cand of allCandidates) {
        if (cand.total <= 0) continue;
        const d = Math.abs(cand.total - target);
        if (d < closestDiff) { closest = cand; closestDiff = d; }
      }
      const closestStr = closest
        ? `closest=${closest.total?.toFixed?.(2)} (diff=${(closestDiff / (target || 1) * 100).toFixed(2)}%, tol=${(tolerance * 100).toFixed(1)}%)`
        : "no positive-value candidates";
      console.log(`[DeltaMatch] REJECT ad=${conv.adId} hour=${conv.hourSlot} R=1 value=${target?.toFixed?.(2)}: ${allCandidates.length} time-window candidates but none within tolerance; ${closestStr}`);
      return [];
    }
    // Rivals: other viable candidates within RIVAL_VALUE_TOLERANCE of the
    // pick's value AND at least as country-compatible as the pick. Meta's
    // per-cycle country breakdown already tells us which country this
    // conversion came from; a candidate from a different country isn't a
    // realistic substitute even if the order total happens to match.
    const pickRank = best.countryRank || 0;
    let rivalCount = 0;
    for (const cand of allCandidates) {
      if (cand.id === best.id) continue;
      if (best.total > 0) {
        const vd = Math.abs(cand.total - best.total) / best.total;
        if (vd > RIVAL_VALUE_TOLERANCE) continue;
      }
      if (pickRank > 0 && (cand.countryRank || 0) < pickRank) continue;
      rivalCount++;
    }
    return [{
      ...best,
      confidence: Math.max(1, Math.round(100 / (1 + rivalCount))),
      rivalCount,
      diffPct: bestDiff / (target || 1),
    }];
  }

  // R>1 path - multiple conversions in the same ad-hour slot. Backtracking finds
  // the combination of R orders whose totals sum to Meta's deltaValue within a
  // DYNAMIC tolerance derived from observed per-conversion drift on the same day.
  const R = Math.min(conv.deltaConversions, allCandidates.length);

  // Dynamic tolerance: use per-conversion baseline × R × padding.
  // perConvTolerance is passed in from the caller (computed from today's R=1 matches).
  // Default: 0.5% per conversion if no baseline available.
  const baseTolerancePerConv = conv._perConvTolerance || 0.005;
  const groupTolerance = Math.min(baseTolerancePerConv * R * 1.5, 0.03); // cap at 3%

  let bestPicks = null, bestDiff = Infinity, bestCountryScore = -1, bestNewCount = -1;
  let validComboCount = 0; // Count ALL valid combos for group confidence
  let iterations = 0;
  const deadline = Date.now() + 5000; // 5s budget per conversion event

  function countNew(picks) { let n = 0; for (const p of picks) if (p.isNew) n++; return n; }
  function scoreCountry(picks) { let n = 0; for (const p of picks) n += (p.countryRank || 0); return n; }

  function backtrack(i, start, sum, picks) {
    if (iterations++ % 5000 === 0 && Date.now() >= deadline) return;
    if (i === R) {
      const diff = Math.abs(sum - target);
      if (diff <= target * groupTolerance) {
        validComboCount++;
        const cScore = scoreCountry(picks);
        const newCount = countNew(picks);
        const EPS = 1e-9;
        if (diff < bestDiff - EPS ||
            (Math.abs(diff - bestDiff) <= EPS && cScore > bestCountryScore) ||
            (Math.abs(diff - bestDiff) <= EPS && cScore === bestCountryScore && newCount > bestNewCount)) {
          bestPicks = picks.slice();
          bestDiff = diff;
          bestCountryScore = cScore;
          bestNewCount = newCount;
        }
      }
      return;
    }
    for (let k = start; k < allCandidates.length; k++) {
      if (Date.now() >= deadline) return;
      picks.push(allCandidates[k]);
      backtrack(i + 1, k + 1, sum + allCandidates[k].total, picks);
      picks.pop();
    }
  }

  backtrack(0, 0, 0, []);
  const matched = bestPicks || [];

  if (matched.length === 0) {
    const totals = allCandidates.slice(0, 8).map(c => c.total?.toFixed?.(2)).join(", ");
    console.log(`[DeltaMatch] REJECT ad=${conv.adId} hour=${conv.hourSlot} R=${R} value=${target?.toFixed?.(2)}: backtracking found no combination summing within ${(groupTolerance * 100).toFixed(1)}% tolerance across ${allCandidates.length} candidates [${totals}${allCandidates.length > 8 ? ", ..." : ""}]`);
    return [];
  }
  const matchedSum = matched.reduce((s, p) => s + p.total, 0);
  const diffPct = Math.abs(matchedSum - target) / (target || 1);

  // Group confidence: based on how many alternative valid combos exist.
  // If only 1 combo qualifies → 100%. If 6 combos → ~17%. If 31 → ~3%.
  const groupConfidence = Math.max(1, Math.round(100 / validComboCount));

  if (validComboCount > 1) {
    console.log(`[DeltaMatch] R=${R} group: ${validComboCount} valid combos within ${(groupTolerance * 100).toFixed(1)}% tolerance, confidence=${groupConfidence}%`);
  }

  return matched.map(pick => ({
    ...pick,
    confidence: groupConfidence,
    rivalCount: validComboCount - 1,
    diffPct,
  }));
}

async function syncTodayBreakdowns(shopDomain, metaAccessToken, metaAdAccountId, today, rate = 1.0) {
  let totalRows = 0;
  // Per-cycle deltas keyed by adId. Each entry: { date, breakdownType,
  // breakdownValue, deltaConv, deltaValue }. Used by enrichFromDelta to assign
  // ground-truth demographic tags to attributions matched in this cycle.
  const deltaMap = new Map();

  for (const config of BREAKDOWN_CONFIGS) {
    try {
      const rows = await fetchBreakdown(metaAccessToken, metaAdAccountId, today, config);
      for (const row of rows) {
        convertMetaFields(row, rate);

        // Read previous-cycle observation BEFORE we overwrite. If there's no
        // existing row, `prev` defaults to 0 — but the migration seeded prev =
        // current for all rows present at migration time, so post-migration
        // the first delta we observe is genuinely "what changed this cycle".
        const existing = await db.metaBreakdown.findUnique({
          where: {
            shopDomain_date_adId_breakdownType_breakdownValue: {
              shopDomain, date: row.date, adId: row.adId,
              breakdownType: row.breakdownType, breakdownValue: row.breakdownValue,
            },
          },
          select: { conversions: true, conversionValue: true, importedAt: true },
        });

        const prevConv = existing?.conversions || 0;
        const prevVal = existing?.conversionValue || 0;
        const curConv = row.conversions || 0;
        const curVal = row.conversionValue || 0;
        const deltaConv = Math.max(0, curConv - prevConv);
        const deltaValue = Math.max(0, curVal - prevVal);

        // Only stash deltas we'll actually use — positive conversion deltas
        // on demographic / placement breakdowns. Country/region breakdowns
        // aren't used for per-order tag assignment.
        if (deltaConv > 0 &&
            (row.breakdownType === "age" || row.breakdownType === "gender" ||
             row.breakdownType === "publisher_platform" || row.breakdownType === "platform_position")) {
          if (row.adId) {
            const arr = deltaMap.get(row.adId) || [];
            arr.push({
              date: row.date,
              breakdownType: row.breakdownType,
              breakdownValue: row.breakdownValue,
              deltaConv,
              deltaValue,
            });
            deltaMap.set(row.adId, arr);
          }
        }

        const insightData = {
          campaignName: row.campaignName, adSetName: row.adSetName, adName: row.adName,
          impressions: row.impressions, clicks: row.clicks, spend: row.spend, reach: row.reach,
          conversions: curConv, conversionValue: curVal,
          linkClicks: row.linkClicks, landingPageViews: row.landingPageViews,
          addToCart: row.addToCart, initiateCheckout: row.initiateCheckout,
          viewContent: row.viewContent, outboundClicks: row.outboundClicks,
          // Roll the previous observation forward so the NEXT cycle can
          // compute its delta against this one.
          prevConversions: prevConv,
          prevConversionValue: prevVal,
          prevObservedAt: existing?.importedAt || null,
        };
        await db.metaBreakdown.upsert({
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
        totalRows++;
      }
    } catch (err) {
      console.error(`[IncrementalSync] Breakdown ${config.type} failed:`, err.message);
    }
  }
  console.log(`[IncrementalSync] Synced ${totalRows} breakdown rows for today (${deltaMap.size} ads with positive deltas)`);
  return { totalRows, deltaMap };
}

/**
 * Match new conversion deltas for a specific historical day.
 * Reads from MetaInsight (already synced by syncMetaAll), compares against
 * MetaSnapshot, and only matches NEW deltas - never touches existing attributions.
 * Used by the nightly scheduler for the 7-day lookback.
 */
export async function matchDayDeltas(shopDomain, dayStr) {
  const shop = await db.shop.findUnique({ where: { shopDomain } });
  if (!shop) return { newConversions: 0, matched: 0, unmatched: 0, skippedIncremental: 0 };

  const revenueField = shop.revenueDefinition || "total_price";
  const metaOffsetMinutes = getTimezoneOffsetMinutes(shop.metaAccountTimezone, dayStr);

  // Read current state from MetaInsight (already updated by syncMetaAll)
  const dayDate = new Date(dayStr + "T00:00:00.000Z");
  const insights = await db.metaInsight.findMany({
    where: { shopDomain, date: dayDate },
  });

  // Build current data in the same format as fetchTodayMeta returns
  const currentData = insights.map(ins => ({
    adId: ins.adId, adName: ins.adName, campaignId: ins.campaignId,
    campaignName: ins.campaignName, adSetId: ins.adSetId, adSetName: ins.adSetName,
    impressions: ins.impressions, clicks: ins.clicks, spend: ins.spend,
    hourSlot: ins.hourSlot, conversions: ins.conversions, conversionValue: ins.conversionValue,
  }));

  // Find deltas against snapshots
  const newConversions = await findNewConversions(shopDomain, dayStr, currentData);
  if (newConversions.length === 0) {
    return { newConversions: 0, matched: 0, unmatched: 0, skippedIncremental: 0 };
  }

  console.log(`[DeltaMatch] ${dayStr}: ${newConversions.length} new conversions to match`);

  // Load Meta spend countries for this date (soft preference signal)
  const countryRows = await db.metaBreakdown.findMany({
    where: { shopDomain, breakdownType: "country", spend: { gt: 0 }, date: new Date(dayStr + "T00:00:00.000Z") },
    select: { breakdownValue: true },
  });
  const metaSpendCountries = new Set(countryRows.map(r => r.breakdownValue.toUpperCase()));

  // Load existing incremental attributions for this day - these are PROTECTED.
  // Incremental matches are more accurate (captured original order values in real-time
  // before Shopify edits/refunds). The daily sweep must never overwrite them.
  const dayStart = new Date(dayStr + "T00:00:00.000Z");
  const dayEnd = new Date(dayStr + "T23:59:59.999Z");
  // Include orders from the last PADDING_MINUTES of the previous day.
  // Without this, an order at 23:55 on day N-1 can't match a Meta conversion
  // for hour 0 on day N (the 6-minute backward padding wraps across midnight).
  // Account for timezone offset: BST (+60) means Meta hour 0 = 23:00 UTC prev day.
  // Need to go back PADDING + offset minutes, not just PADDING.
  const paddedStart = new Date(dayStart.getTime() - (PADDING_MINUTES + Math.max(0, metaOffsetMinutes)) * 60 * 1000);

  // Get all orders for this day + padding window (needed for both protection check and matching)
  const dayOrders = await db.order.findMany({
    where: {
      shopDomain, isOnlineStore: true,
      createdAt: { gte: paddedStart, lt: dayEnd },
    },
    orderBy: { createdAt: "asc" },
  });

  // Meta-first: statistical matcher runs first against ALL new conversions
  // with full Meta-reported caps. UTM is consulted only as a fallback below
  // for utmConfirmedMeta orders the stat matcher couldn't bind.
  const remainingNewConversions = newConversions;

  const dayOrderIds = new Set(dayOrders.map(o => o.shopifyOrderId));

  // Find incremental attributions linked to orders on this day - these are PROTECTED.
  // Count per ad+hour so h23 deltas aren't consumed by earlier-hour matches.
  // We derive the Meta hour slot from the order's createdAt + timezone offset.
  const existingIncrementals = await db.attribution.findMany({
    where: {
      shopDomain,
      matchMethod: { in: ["incremental", "utm + incremental"] },
      confidence: { gt: 0 },
      shopifyOrderId: { in: [...dayOrderIds] },
    },
    select: { metaAdId: true, shopifyOrderId: true },
  });
  // Build a map of orderId → createdAt for hour derivation
  const orderCreatedAtMap = {};
  for (const o of dayOrders) orderCreatedAtMap[o.shopifyOrderId] = o.createdAt;

  const incrementalCountByAdHour = {};
  for (const a of existingIncrementals) {
    const orderTime = orderCreatedAtMap[a.shopifyOrderId];
    if (!orderTime) continue;
    // Derive the Meta hour slot this order falls in (applying timezone offset)
    const orderDate = new Date(orderTime);
    // Add Meta offset to get Meta-local hour (Meta reports in ad account tz)
    const metaLocalMs = orderDate.getTime() + metaOffsetMinutes * 60000;
    const metaHour = new Date(metaLocalMs).getUTCHours();
    const key = `${a.metaAdId}|${metaHour}`;
    incrementalCountByAdHour[key] = (incrementalCountByAdHour[key] || 0) + 1;
  }

  // Also count existing unmatched records per ad+hour so we don't double-create
  const existingUnmatched = await db.attribution.findMany({
    where: {
      shopDomain, confidence: 0,
      shopifyOrderId: { contains: dayStr },
    },
    select: { metaAdId: true, shopifyOrderId: true },
  });
  const unmatchedHourKeys = new Set(existingUnmatched.map(a => a.shopifyOrderId));

  let totalMatched = 0, totalUnmatched = 0, skippedIncremental = 0;
  const dailyMatchedOrderIds = [];

  for (const conv of remainingNewConversions) {
    // PRIORITY RULE: skip conversions already covered by incremental matches.
    // Now tracked per ad+hour - h23 deltas are only consumed by h23 incrementals.
    const adHourKey = `${conv.adId}|${conv.hourSlot}`;
    const incrementalCount = incrementalCountByAdHour[adHourKey] || 0;
    if (incrementalCount > 0) {
      const alreadyCovered = Math.min(conv.deltaConversions, incrementalCount);
      incrementalCountByAdHour[adHourKey] -= alreadyCovered;
      conv.deltaConversions -= alreadyCovered;
      if (conv.deltaConversions > 0) {
        const originalPerConv = conv.deltaValue / (conv.deltaConversions + alreadyCovered);
        conv.deltaValue = Math.round(originalPerConv * conv.deltaConversions * 100) / 100;
        console.log(`[DeltaMatch] ${conv.adId} hour ${conv.hourSlot}: ${alreadyCovered} covered by incremental, ${conv.deltaConversions} remaining`);
      } else {
        console.log(`[DeltaMatch] Preserving incremental match for ${conv.adId} hour ${conv.hourSlot} on ${dayStr}`);
        skippedIncremental++;
        continue;
      }
    }

    // Historical/batch path: no per-cycle country delta available, fall back to day-level soft signal only.
    const matched = await matchSingleConversion(shopDomain, conv, dayOrders, revenueField, metaOffsetMinutes, metaSpendCountries, null);
    if (matched.length > 0) {
      for (const pick of matched) {
        // SAFETY: never overwrite an incremental or blended-incremental match.
        // Daily-sweep is last-resort only - fills gaps the incremental run missed.
        const existing = await db.attribution.findUnique({
          where: { shopDomain_shopifyOrderId: { shopDomain, shopifyOrderId: pick.orderId } },
          select: { matchMethod: true, confidence: true, metaAdId: true },
        });
        if (existing && existing.confidence > 0) {
          if (existing.matchMethod === "incremental" || existing.matchMethod === "utm + incremental") {
            skippedIncremental++;
            continue;
          }
          // Meta-first: a statistical pick ALWAYS overrides a UTM Layer 1
          // row. UTM is now a fallback, only written for orders the stat
          // matcher could not bind. If we get here, the stat matcher found
          // a value+time fit — that's authoritative.
        }
        // Blend label when UTM and daily-sweep agree on the ad.
        const finalMethod = (existing && existing.matchMethod === "utm" && existing.metaAdId === conv.adId)
          ? "utm + daily-sweep" : "daily-sweep";
        // Store the META-reported per-conversion value (not Shopify order value).
        // conv.deltaValue / deltaConversions gives the exact per-conversion Meta value
        // for this specific hour slot.
        const metaPerConv = conv.deltaConversions > 0
          ? Math.round((conv.deltaValue / conv.deltaConversions) * 100) / 100 : 0;
        await db.attribution.upsert({
          where: { shopDomain_shopifyOrderId: { shopDomain, shopifyOrderId: pick.orderId } },
          create: {
            shopDomain, shopifyOrderId: pick.orderId, layer: 2,
            confidence: pick.confidence, rivalCount: pick.rivalCount || 0,
            metaCampaignId: conv.campaignId, metaCampaignName: conv.campaignName,
            metaAdSetId: conv.adSetId, metaAdSetName: conv.adSetName,
            metaAdId: conv.adId, metaAdName: conv.adName,
            isNewCustomer: pick.isNew, isNewToMeta: pick.isNew, matchMethod: finalMethod,
            metaConversionValue: metaPerConv,
          },
          update: {
            confidence: pick.confidence, rivalCount: pick.rivalCount || 0,
            metaCampaignId: conv.campaignId, metaCampaignName: conv.campaignName,
            metaAdSetId: conv.adSetId, metaAdSetName: conv.adSetName,
            metaAdId: conv.adId, metaAdName: conv.adName,
            isNewCustomer: pick.isNew, isNewToMeta: pick.isNew, matchMethod: finalMethod,
            metaConversionValue: metaPerConv,
          },
        });
        totalMatched++;
        dailyMatchedOrderIds.push(pick.orderId);
      }
      // Clean up any stale unmatched placeholder rows for this ad-hour now that
      // we've successfully matched the conversion(s).
      await deleteUnmatchedPlaceholders(shopDomain, conv.adId, dayStr, conv.hourSlot);
    } else {
      await upsertUnmatchedPlaceholders(shopDomain, conv, dayStr);
      totalUnmatched += Math.max(conv.deltaConversions, 1);
    }
  }

  // ── Layer 1 fallback (UTM last-resort) ──
  // For each conv that still has residual deltaConversions after stat matching,
  // let UTM-confirmed orders claim the leftover slot capacity.
  const residualForFallback = newConversions.filter(c => c.deltaConversions > 0);
  const layer1 = residualForFallback.length > 0
    ? await runUtmLayer1Pass(shopDomain, dayStr, dayOrders, residualForFallback, metaOffsetMinutes)
    : { layer1Written: 0, slotCreditsConsumed: 0 };

  // Post-match: recalculate confidence for orders whose rivals were resolved
  await recalculateConfidence(shopDomain, dailyMatchedOrderIds);

  // Update snapshots for this day
  await saveSnapshot(shopDomain, dayStr, currentData);

  console.log(`[DeltaMatch] ${dayStr}: ${totalMatched} stat-matched, ${layer1.layer1Written} UTM-fallback, ${totalUnmatched} unmatched, ${skippedIncremental} preserved incremental`);
  return { newConversions: newConversions.length, layer1: layer1.layer1Written, matched: totalMatched, unmatched: totalUnmatched, skippedIncremental };
}

/**
 * Clear today's unmatched attributions and adjust snapshots so the incremental
 * sync re-processes only those specific conversions. Preserves snapshots for
 * already-matched conversions to prevent duplicate processing.
 */
export async function clearTodayForRematch(shopDomain) {
  const shopRow = await db.shop.findUnique({ where: { shopDomain }, select: { shopifyTimezone: true } });
  const today = shopLocalToday(shopRow?.shopifyTimezone || "UTC");

  // Find unmatched attributions to determine which snapshots to adjust
  const unmatched = await db.attribution.findMany({
    where: { shopDomain, confidence: 0, shopifyOrderId: { contains: today } },
    select: { shopifyOrderId: true, metaAdId: true },
  });

  if (unmatched.length === 0) {
    console.log(`[IncrementalSync] No unmatched attributions for ${today}, nothing to clear`);
    return;
  }

  // Parse ad+hour from unmatched IDs (format: unmatched_ADID_DATE_HOUR_cN)
  // and decrement snapshot conversion counts so findNewConversions rediscovers them
  const adjustments = {};
  for (const attr of unmatched) {
    const parts = attr.shopifyOrderId.split("_");
    // Hour slot is at index 3 (after "unmatched", adId, date)
    const hourSlot = parts.length >= 4 ? parseInt(parts[3], 10) : NaN;
    const adId = attr.metaAdId;
    if (adId && !isNaN(hourSlot)) {
      const key = `${adId}|${hourSlot}`;
      adjustments[key] = (adjustments[key] || 0) + 1;
    }
  }

  // Adjust snapshot counts down so those conversions are re-detected as "new"
  for (const [key, count] of Object.entries(adjustments)) {
    const [adId, hourSlotStr] = key.split("|");
    const hourSlot = parseInt(hourSlotStr, 10);
    const snapshot = await db.metaSnapshot.findUnique({
      where: { shopDomain_date_adId_hourSlot: { shopDomain, date: today, adId, hourSlot } },
    });
    if (snapshot) {
      const newCount = Math.max(0, snapshot.conversions - count);
      await db.metaSnapshot.update({
        where: { id: snapshot.id },
        data: { conversions: newCount },
      });
    }
  }

  // Delete unmatched attributions
  const deleted = await db.attribution.deleteMany({
    where: { shopDomain, confidence: 0, shopifyOrderId: { contains: today } },
  });
  console.log(`[IncrementalSync] Cleared ${deleted.count} unmatched attributions for ${today}, adjusted ${Object.keys(adjustments).length} snapshots`);
}

export async function runIncrementalSync(shopDomain) {
  console.log(`[IncrementalSync] Starting for ${shopDomain}`);

  const shop = await db.shop.findUnique({ where: { shopDomain } });
  if (!shop) throw new Error("Shop not found");
  if (!shop.metaAccessToken || !shop.metaAdAccountId) throw new Error("Meta Ads not connected");

  const revenueField = shop.revenueDefinition || "total_price";
  // "today" is resolved in the shop's Shopify timezone so every downstream
  // bucket / snapshot / rollup aligns with how the merchant sees their day.
  // For shops where Meta ad account tz differs from Shopify tz, we still use
  // the Shopify day - Meta will return that calendar date's data in its own tz,
  // which is close enough for incremental matching and auto-heals across cycles.
  const today = shopLocalToday(shop.shopifyTimezone || "UTC");

  // Get exchange rate for currency conversion (Meta → Shopify currency)
  const rate = await getExchangeRate(today, shop.metaCurrency, shop.shopifyCurrency);
  if (rate !== 1.0) console.log(`[IncrementalSync] Converting ${shop.metaCurrency}→${shop.shopifyCurrency} at ${rate}`);

  // Compute Meta timezone offset for hour slot → UTC conversion
  const metaOffsetMinutes = getTimezoneOffsetMinutes(shop.metaAccountTimezone, today);
  if (metaOffsetMinutes !== 0) console.log(`[IncrementalSync] Meta timezone offset: ${metaOffsetMinutes} minutes (${shop.metaAccountTimezone})`);

  setProgress(`incrementalSync:${shopDomain}`, {
    status: "running",
    message: "Fetching today's Meta data...",
  });
  console.log(`[IncrementalSync] Fetching Meta data for ${today}...`);
  const currentData = await fetchTodayMeta(shop.metaAccessToken, shop.metaAdAccountId, today);
  console.log(`[IncrementalSync] Got ${currentData.length} rows from Meta`);

  // Convert Meta monetary fields to Shopify currency
  for (const row of currentData) convertMetaFields(row, rate);

  await saveInsights(shopDomain, today, currentData);

  const newConversions = await findNewConversions(shopDomain, today, currentData);
  console.log(`[IncrementalSync] Found ${newConversions.length} new conversion events`);

  // Save snapshot IMMEDIATELY after finding deltas - before matching or breakdowns.
  // This prevents stale snapshots if a later step crashes (fetch failed, etc.).
  // The snapshot must reflect what Meta currently reports, regardless of whether
  // we successfully match or sync breakdowns.
  await saveSnapshot(shopDomain, today, currentData);

  // ── Previous-day lookback (once per day per process) ──
  // Meta is inconsistent about which day it assigns late-night conversions to.
  // Sometimes h23 conversions roll into the next day's h0, sometimes they stay
  // in h23. The incremental sync only processes "today", so if Meta kept them
  // in yesterday's h23, we never see them. On the first sync of each new day,
  // fetch yesterday's data and pick up any uncaptured conversions.
  const yesterday = new Date(new Date(today + "T12:00:00Z").getTime() - 86400000).toISOString().slice(0, 10);
  const prevDayKey = `${shopDomain}:${yesterday}`;
  let yesterdayNewConversions = [];
  if (!previousDayChecked.has(prevDayKey)) {
    previousDayChecked.add(prevDayKey);
    try {
      const yesterdayData = await fetchTodayMeta(shop.metaAccessToken, shop.metaAdAccountId, yesterday);
      for (const row of yesterdayData) convertMetaFields(row, rate);
      await saveInsights(shopDomain, yesterday, yesterdayData);
      yesterdayNewConversions = await findNewConversions(shopDomain, yesterday, yesterdayData);
      if (yesterdayNewConversions.length > 0) {
        console.log(`[IncrementalSync] Previous-day lookback: ${yesterdayNewConversions.length} uncaptured conversions on ${yesterday}`);
        await saveSnapshot(shopDomain, yesterday, yesterdayData);
      }
    } catch (err) {
      console.error(`[IncrementalSync] Previous-day lookback failed (non-fatal): ${err.message}`);
      previousDayChecked.delete(prevDayKey); // retry next cycle
    }
  }

  // Country-delta detection: fetch today's country breakdown, diff against the
  // previous MetaCountrySnapshot to learn which country each new conversion came
  // from. Fed into the matcher as a strong (but not hard) tiebreaker signal.
  let adCountryDeltas = {};
  try {
    const countryRows = await fetchTodayCountryBreakdown(shop.metaAccessToken, shop.metaAdAccountId, today);
    adCountryDeltas = await computeCountryDeltas(shopDomain, today, countryRows);
    await saveCountrySnapshot(shopDomain, today, countryRows);
    const adsWithDeltas = Object.keys(adCountryDeltas).length;
    if (adsWithDeltas > 0) {
      console.log(`[IncrementalSync] Country deltas for ${adsWithDeltas} ads`);
    }
  } catch (err) {
    console.error(`[IncrementalSync] Country delta fetch failed (non-fatal): ${err.message}`);
  }

  // Early-return only when BOTH today and yesterday are empty. Previously this
  // checked only `newConversions` (today), which silently skipped the entire
  // yesterday-matching block at line ~1408. Symptom: a conversion first
  // captured by the previous-day lookback at the midnight cycle (when "today"
  // has no convs yet) was snapshotted but never matched - and subsequent
  // cycles bypass the lookback (`previousDayChecked` flag), so the delta was
  // permanently lost. Caught via Vollebak VBK1142602 on 2026-05-03.
  if (newConversions.length === 0 && yesterdayNewConversions.length === 0) {
    setProgress(`incrementalSync:${shopDomain}`, { status: "running", message: "Syncing today's breakdowns..." });
    let breakdownRows = 0;
    try {
      breakdownRows = await syncTodayBreakdowns(shopDomain, shop.metaAccessToken, shop.metaAdAccountId, today, rate);
    } catch (err) {
      console.error(`[IncrementalSync] Breakdown sync failed (non-fatal): ${err.message}`);
    }
    // Mark sync complete IMMEDIATELY so the UI unblocks. Rollups are
    // housekeeping — they shouldn't hold the user hostage.
    await db.shop.update({ where: { shopDomain }, data: { lastMetaSync: new Date() } });
    completeProgress(`incrementalSync:${shopDomain}`, { newConversions: 0, matched: 0, unmatched: 0, breakdownRows });

    // No new conversions — throttle rollup rebuilds to once per day.
    // Run in background (fire-and-forget) so the scheduler isn't blocked.
    rebuildAllRollups(shopDomain, { force: false }).then(didRebuild => {
      if (didRebuild) {
        invalidateShop(shopDomain);
        import("./cacheWarmer.server.js").then(({ warmAllShops }) => {
          warmAllShops().catch(err => console.error("[IncrementalSync] post-sync warm failed:", err.message));
        }).catch(err => console.error("[IncrementalSync] warmer import failed:", err.message));
      }
    }).catch(err => console.error("[IncrementalSync] Background rollup failed:", err.message));

    return { newConversions: 0, matched: 0, unmatched: 0, breakdownRows };
  }

  // Only match against web orders. Include last PADDING_MINUTES of yesterday
  // so an order at 23:55 can match a Meta conversion for today's hour 0.
  // Bounds are computed in shop-local time so "today" means the merchant's day.
  const shopTodayBounds = shopDayBounds(shop.shopifyTimezone || "UTC", today);
  const todayStart = shopTodayBounds.gte;
  const paddedTodayStart = new Date(todayStart.getTime() - (PADDING_MINUTES + Math.max(0, metaOffsetMinutes)) * 60 * 1000);
  const todayOrders = await db.order.findMany({
    where: {
      shopDomain,
      isOnlineStore: true,
      createdAt: { gte: paddedTodayStart, lte: shopTodayBounds.lte },
    },
    orderBy: { createdAt: "asc" },
  });
  console.log(`[IncrementalSync] ${todayOrders.length} web orders today (incl ${PADDING_MINUTES}min prev-day padding)`);

  // Load Meta spend countries for today (soft preference signal)
  const countryRows = await db.metaBreakdown.findMany({
    where: { shopDomain, breakdownType: "country", spend: { gt: 0 }, date: new Date(today + "T00:00:00.000Z") },
    select: { breakdownValue: true },
  });
  const metaSpendCountries = new Set(countryRows.map(r => r.breakdownValue.toUpperCase()));

  // Meta-first: statistical matcher runs first against the full Meta-reported
  // slot capacity. UTM fallback runs AFTER stat matching for any residual
  // slot capacity left unfilled.
  const remainingConversions = newConversions;

  // Load existing attributions so the statistical write can decide whether to
  // overwrite a UTM Layer 1 row. Rule: overwrite only when the Layer 2 pick is
  // unambiguous (rivalCount=0). When UTM and stat agree on the ad, blend the
  // label to "utm + incremental" so both methods are visible in the UI.
  const todayOrderIds = todayOrders.map(o => o.shopifyOrderId);
  const existingAttrRows = await db.attribution.findMany({
    where: { shopDomain, shopifyOrderId: { in: todayOrderIds }, confidence: { gt: 0 } },
    select: { shopifyOrderId: true, matchMethod: true, metaAdId: true },
  });
  const existingAttrByOrderId = new Map(existingAttrRows.map(r => [r.shopifyOrderId, r]));

  let totalMatched = 0, totalUnmatched = 0;
  const matchedOrderIds = []; // Track for post-match confidence recalculation

  // Compute dynamic per-conversion tolerance from today's existing R=1 matches.
  // These are high-confidence 1:1 matches where we know the real drift between
  // Meta's reported value and the actual Shopify order. Use the median as baseline.
  const existingR1Attrs = await db.attribution.findMany({
    where: { shopDomain, confidence: 100, rivalCount: 0, matchMethod: "incremental", metaConversionValue: { gt: 0 } },
    select: { shopifyOrderId: true, metaConversionValue: true },
    take: 50, orderBy: { matchedAt: "desc" },
  });
  let perConvTolerance = 0.005; // default 0.5% if no data
  if (existingR1Attrs.length >= 3) {
    const orderMap = new Map();
    const r1OrderIds = existingR1Attrs.map(a => a.shopifyOrderId);
    const r1Orders = await db.order.findMany({
      where: { shopifyOrderId: { in: r1OrderIds } },
      select: { shopifyOrderId: true, frozenTotalPrice: true },
    });
    for (const o of r1Orders) orderMap.set(o.shopifyOrderId, o.frozenTotalPrice);
    const drifts = [];
    for (const a of existingR1Attrs) {
      const orderVal = orderMap.get(a.shopifyOrderId);
      if (orderVal && orderVal > 0 && a.metaConversionValue > 0) {
        drifts.push(Math.abs(orderVal - a.metaConversionValue) / a.metaConversionValue);
      }
    }
    if (drifts.length >= 3) {
      drifts.sort((a, b) => a - b);
      const median = drifts[Math.floor(drifts.length / 2)];
      perConvTolerance = Math.max(0.003, Math.min(median * 1.5, 0.01)); // clamp 0.3% - 1%
      console.log(`[IncrementalSync] Per-conv tolerance from ${drifts.length} R=1 matches: median drift ${(median * 100).toFixed(3)}%, using ${(perConvTolerance * 100).toFixed(3)}%`);
    }
  }
  // Annotate R>1 conversions with the dynamic tolerance
  for (const conv of remainingConversions) {
    if (conv.deltaConversions > 1) conv._perConvTolerance = perConvTolerance;
  }

  setProgress(`incrementalSync:${shopDomain}`, {
    status: "running",
    current: 0,
    total: remainingConversions.length,
    message: `Matching ${remainingConversions.length} new conversions...`,
  });

  for (let ci = 0; ci < remainingConversions.length; ci++) {
    const conv = remainingConversions[ci];
    setProgress(`incrementalSync:${shopDomain}`, {
      status: "running",
      current: ci + 1,
      total: remainingConversions.length,
      message: `Matching conversion ${ci + 1} of ${remainingConversions.length}`,
    });
    const matched = await matchSingleConversion(shopDomain, conv, todayOrders, revenueField, metaOffsetMinutes, metaSpendCountries, adCountryDeltas);
    if (matched.length > 0) {
      // Store the META-reported per-conversion value for this slot
      const metaPerConv = conv.deltaConversions > 0
        ? Math.round((conv.deltaValue / conv.deltaConversions) * 100) / 100 : 0;
      for (const pick of matched) {
        const existing = existingAttrByOrderId.get(pick.orderId);
        // Meta-first: a statistical pick ALWAYS overrides a prior UTM Layer 1
        // row (UTM is now fallback, used only when stat matcher can't bind).
        // Blend label when UTM and stat agree on the ad (preserves
        // attribution-method visibility in the UI).
        const finalMethod = (existing && existing.matchMethod === "utm" && existing.metaAdId === conv.adId)
          ? "utm + incremental" : "incremental";
        matchedOrderIds.push(pick.orderId);
        await db.attribution.upsert({
          where: { shopDomain_shopifyOrderId: { shopDomain, shopifyOrderId: pick.orderId } },
          create: {
            shopDomain, shopifyOrderId: pick.orderId, layer: 2,
            confidence: pick.confidence, rivalCount: pick.rivalCount || 0,
            metaCampaignId: conv.campaignId, metaCampaignName: conv.campaignName,
            metaAdSetId: conv.adSetId, metaAdSetName: conv.adSetName,
            metaAdId: conv.adId, metaAdName: conv.adName,
            isNewCustomer: pick.isNew, isNewToMeta: pick.isNew, matchMethod: finalMethod,
            metaConversionValue: metaPerConv,
          },
          update: {
            confidence: pick.confidence, rivalCount: pick.rivalCount || 0,
            metaCampaignId: conv.campaignId, metaCampaignName: conv.campaignName,
            metaAdSetId: conv.adSetId, metaAdSetName: conv.adSetName,
            metaAdId: conv.adId, metaAdName: conv.adName,
            isNewCustomer: pick.isNew, isNewToMeta: pick.isNew, matchMethod: finalMethod,
            metaConversionValue: metaPerConv,
          },
        });
        existingAttrByOrderId.set(pick.orderId, { shopifyOrderId: pick.orderId, matchMethod: finalMethod, metaAdId: conv.adId });
        totalMatched++;
      }
      // Clean up any stale unmatched placeholders for this ad-hour
      await deleteUnmatchedPlaceholders(shopDomain, conv.adId, today, conv.hourSlot);
    } else {
      await upsertUnmatchedPlaceholders(shopDomain, conv, today);
      totalUnmatched += Math.max(conv.deltaConversions, 1);
    }
  }

  // ── Post-stat UTM fallback (today) ──
  // Layer 2 has had first crack at all of today's Meta capacity. Any slot
  // capacity still unfilled (deltaConversions > 0) is offered to UTM Layer 1
  // as a fallback. Confidence stays at the level UTM provides; the key change
  // vs. the old flow is that statistical matching is no longer pre-empted by
  // UTM claims when UTM and stat disagree on which order belongs to a slot.
  const todayResidualForFallback = remainingConversions.filter(c => c.deltaConversions > 0);
  const layer1 = todayResidualForFallback.length > 0
    ? await runUtmLayer1Pass(shopDomain, today, todayOrders, todayResidualForFallback, metaOffsetMinutes)
    : { layer1Written: 0, slotCreditsConsumed: 0 };

  // ── Match yesterday's uncaptured conversions ──
  if (yesterdayNewConversions.length > 0) {
    setProgress(`incrementalSync:${shopDomain}`, {
      status: "running",
      message: `Matching ${yesterdayNewConversions.length} previous-day conversions...`,
    });

    const yesterdayMetaOffset = getTimezoneOffsetMinutes(shop.metaAccountTimezone, yesterday);
    const yBounds = shopDayBounds(shop.shopifyTimezone || "UTC", yesterday);
    const yPaddedStart = new Date(yBounds.gte.getTime() - (PADDING_MINUTES + Math.max(0, yesterdayMetaOffset)) * 60 * 1000);
    const yesterdayOrders = await db.order.findMany({
      where: { shopDomain, isOnlineStore: true, createdAt: { gte: yPaddedStart, lte: yBounds.lte } },
      orderBy: { createdAt: "asc" },
    });

    // Meta-first: yesterday's uncaptured conversions are matched statistically
    // first, then any residual slot capacity falls back to UTM Layer 1 below.
    const yRemaining = yesterdayNewConversions;

    const yCountryRows = await db.metaBreakdown.findMany({
      where: { shopDomain, breakdownType: "country", spend: { gt: 0 }, date: new Date(yesterday + "T00:00:00.000Z") },
      select: { breakdownValue: true },
    });
    const yMetaSpendCountries = new Set(yCountryRows.map(r => r.breakdownValue.toUpperCase()));

    for (const conv of yRemaining) {
      const matched = await matchSingleConversion(shopDomain, conv, yesterdayOrders, revenueField, yesterdayMetaOffset, yMetaSpendCountries, null);
      if (matched.length > 0) {
        const metaPerConv = conv.deltaConversions > 0
          ? Math.round((conv.deltaValue / conv.deltaConversions) * 100) / 100 : 0;
        for (const pick of matched) {
          const existing = existingAttrByOrderId.get(pick.orderId);
          // Meta-first: statistical pick overrides any prior UTM row. Blend
          // label when UTM and stat agree on the ad.
          const finalMethod = (existing && existing.matchMethod === "utm" && existing.metaAdId === conv.adId)
            ? "utm + incremental" : "incremental";
          matchedOrderIds.push(pick.orderId);
          await db.attribution.upsert({
            where: { shopDomain_shopifyOrderId: { shopDomain, shopifyOrderId: pick.orderId } },
            create: {
              shopDomain, shopifyOrderId: pick.orderId, layer: 2,
              confidence: pick.confidence, rivalCount: pick.rivalCount || 0,
              metaCampaignId: conv.campaignId, metaCampaignName: conv.campaignName,
              metaAdSetId: conv.adSetId, metaAdSetName: conv.adSetName,
              metaAdId: conv.adId, metaAdName: conv.adName,
              isNewCustomer: pick.isNew, isNewToMeta: pick.isNew, matchMethod: finalMethod,
              metaConversionValue: metaPerConv,
            },
            update: {
              confidence: pick.confidence, rivalCount: pick.rivalCount || 0,
              metaCampaignId: conv.campaignId, metaCampaignName: conv.campaignName,
              metaAdSetId: conv.adSetId, metaAdSetName: conv.adSetName,
              metaAdId: conv.adId, metaAdName: conv.adName,
              isNewCustomer: pick.isNew, isNewToMeta: pick.isNew, matchMethod: finalMethod,
              metaConversionValue: metaPerConv,
            },
          });
          existingAttrByOrderId.set(pick.orderId, { shopifyOrderId: pick.orderId, matchMethod: finalMethod, metaAdId: conv.adId });
          totalMatched++;
        }
        await deleteUnmatchedPlaceholders(shopDomain, conv.adId, yesterday, conv.hourSlot);
      } else {
        await upsertUnmatchedPlaceholders(shopDomain, conv, yesterday);
        totalUnmatched += Math.max(conv.deltaConversions, 1);
      }
    }
    // Post-stat UTM fallback for yesterday's residual slot capacity
    const yResidualForFallback = yRemaining.filter(c => c.deltaConversions > 0);
    const yLayer1 = yResidualForFallback.length > 0
      ? await runUtmLayer1Pass(shopDomain, yesterday, yesterdayOrders, yResidualForFallback, yesterdayMetaOffset)
      : { layer1Written: 0, slotCreditsConsumed: 0 };
    if (yLayer1.layer1Written > 0 || yRemaining.length > 0) {
      console.log(`[IncrementalSync] Previous-day: ${yRemaining.length} statistical matches attempted, ${yLayer1.layer1Written} UTM fallback`);
    }
  }

  // Post-match: recalculate confidence for orders whose rivals were resolved
  // (e.g., two orders in the same time slot both got matched to different conversions)
  await recalculateConfidence(shopDomain, matchedOrderIds);

  setProgress(`incrementalSync:${shopDomain}`, { status: "running", message: "Syncing today's breakdowns..." });
  let breakdownRows = 0;
  let breakdownDeltaMap = new Map();
  try {
    const r = await syncTodayBreakdowns(shopDomain, shop.metaAccessToken, shop.metaAdAccountId, today, rate);
    breakdownRows = r.totalRows;
    breakdownDeltaMap = r.deltaMap || new Map();
  } catch (err) {
    console.error(`[IncrementalSync] Breakdown sync failed (non-fatal): ${err.message}`);
  }

  // Delta-based per-order demographic assignment (ground truth, going-forward).
  // Uses the per-cycle deltas captured above to assign demographic tags to the
  // attributions matched in THIS cycle. Attribution gets demographicExact=true
  // when the delta unambiguously points at one bucket. After this pass, run
  // the catch-up enricher to backfill any attribution still NULL (e.g. Meta
  // breakdown data hasn't caught up yet — typical 1-3h lag).
  setProgress(`incrementalSync:${shopDomain}`, { status: "running", message: "Assigning demographics from cycle deltas..." });
  let enrichResult = { enriched: 0 };
  try {
    const { enrichFromDelta, enrichRecentUnenriched } = await import("./attributionEnrichment.server.js");
    if (matchedOrderIds.length > 0 && breakdownDeltaMap.size > 0) {
      const deltaRes = await enrichFromDelta(shopDomain, breakdownDeltaMap, matchedOrderIds);
      console.log(`[IncrementalSync] Delta enrichment: ${deltaRes.enriched} (${deltaRes.exact} exact, ${deltaRes.probabilistic} probabilistic)`);
    }
    // Catch-up: anything still NULL after delta pass.
    enrichResult = await enrichRecentUnenriched(shopDomain, 1);
  } catch (err) {
    console.error(`[IncrementalSync] Demographic enrichment failed (non-fatal): ${err.message}`);
  }

  // Self-heal pixel calibration. If the install-time run landed before enough
  // UTM-confirmed orders existed (samples < 5 minimum), the dashboard pill
  // stays stuck on "gathering signal (0/5)" indefinitely. Re-run when we now
  // have more candidates than last time. Cheap — the function bails fast if
  // candidate count hasn't grown past the minimum.
  try {
    if ((shop.metaValueCalibrationSamples ?? 0) < 5) {
      const { calibratePixel } = await import("./pixelCalibration.server.js");
      const calRes = await calibratePixel(shopDomain);
      if (calRes?.winner) {
        console.log(`[IncrementalSync] Pixel re-calibrated: winner=${calRes.winner} samples=${calRes.sampleSize}`);
      } else {
        console.log(`[IncrementalSync] Pixel re-calibration: still insufficient (${calRes?.sampleSize ?? 0}/5)`);
      }
    }
  } catch (err) {
    console.error(`[IncrementalSync] Pixel re-calibration failed (non-fatal): ${err.message}`);
  }

  // Mark sync complete IMMEDIATELY so the UI unblocks.
  await db.shop.update({ where: { shopDomain }, data: { lastMetaSync: new Date() } });
  completeProgress(`incrementalSync:${shopDomain}`, { newConversions: newConversions.length, layer1: layer1.layer1Written, matched: totalMatched, unmatched: totalUnmatched, breakdownRows, ...enrichResult });
  console.log(`[IncrementalSync] Complete: ${layer1.layer1Written} UTM layer1, ${totalMatched} matched, ${totalUnmatched} unmatched, ${breakdownRows} breakdowns, ${enrichResult.enriched} demographics enriched`);

  // Rollups + cache warm in background (fire-and-forget). New conversions
  // arrived so force rebuild, but don't block the user or the scheduler.
  // IMPORTANT: invalidateShop runs AFTER rebuildAllRollups completes - not
  // before. Invalidating beforehand means any user request that lands during
  // the rebuild reads partial data (mid delete+insert window inside each
  // rollup builder) and caches it for the full TTL, leaving tiles showing
  // zero/null until the next sync. The transaction wrapping in each rollup
  // builder removes the partial-read window even on cache miss; the order
  // here keeps the warmer working off post-rebuild data.
  rebuildAllRollups(shopDomain, { force: true }).then(() => {
    invalidateShop(shopDomain);
    import("./cacheWarmer.server.js").then(({ warmAllShops }) => {
      warmAllShops().catch(err => console.error("[IncrementalSync] post-sync warm failed:", err.message));
    }).catch(err => console.error("[IncrementalSync] warmer import failed:", err.message));
  }).catch(err => console.error("[IncrementalSync] Background rollup failed:", err.message));

  return { newConversions: newConversions.length, layer1: layer1.layer1Written, matched: totalMatched, unmatched: totalUnmatched, breakdownRows, ...enrichResult };
}
