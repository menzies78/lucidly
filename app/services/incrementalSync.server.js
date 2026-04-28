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
  const last = lastRollupRebuildAt.get(shopDomain) || 0;
  const ageMs = Date.now() - last;
  if (!force && last > 0 && ageMs < ROLLUP_REBUILD_MIN_INTERVAL_MS) {
    const minutes = Math.round(ageMs / 60000);
    console.log(`[IncrementalSync] Skipping rollup rebuild for ${shopDomain} — last rebuild ${minutes}m ago, no new conversions`);
    return false;
  }
  setProgress(`incrementalSync:${shopDomain}`, { status: "running", message: "Rebuilding product rollups..." });
  try {
    const { rebuildProductRollups } = await import("./productRollups.server.js");
    await rebuildProductRollups(shopDomain);
  } catch (err) {
    console.error(`[IncrementalSync] Product rollup rebuild failed (non-fatal): ${err.message}`);
  }
  if (global.gc) global.gc();
  setProgress(`incrementalSync:${shopDomain}`, { status: "running", message: "Rebuilding campaign rollups..." });
  try {
    const { rebuildCampaignRollups } = await import("./campaignRollups.server.js");
    await rebuildCampaignRollups(shopDomain);
  } catch (err) {
    console.error(`[IncrementalSync] Campaign rollup rebuild failed (non-fatal): ${err.message}`);
  }
  if (global.gc) global.gc();
  setProgress(`incrementalSync:${shopDomain}`, { status: "running", message: "Rebuilding customer rollups..." });
  try {
    const { rebuildCustomerSegments, rebuildCustomerRollups } = await import("./customerRollups.server.js");
    await rebuildCustomerSegments(shopDomain);
    await rebuildCustomerRollups(shopDomain);
  } catch (err) {
    console.error(`[IncrementalSync] Customer rollup rebuild failed (non-fatal): ${err.message}`);
  }
  if (global.gc) global.gc();
  lastRollupRebuildAt.set(shopDomain, Date.now());
  return true;
}
// Fallback padding — tried only when the 6-minute window returned no
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
 * Meta reports hourly_stats_aggregated_by_advertiser_time_zone — we need UTC
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
  // Backward padding only — order is placed before Meta logs the conversion
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
  for (const row of currentData) {
    if (row.conversions === 0) continue;
    await db.metaSnapshot.upsert({
      where: { shopDomain_date_adId_hourSlot: { shopDomain, date: today, adId: row.adId, hourSlot: row.hourSlot } },
      create: { shopDomain, date: today, adId: row.adId, hourSlot: row.hourSlot, conversions: row.conversions, conversionValue: row.conversionValue },
      update: { conversions: row.conversions, conversionValue: row.conversionValue },
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
 * country attribution. When several did, each is included — the matcher treats any
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
 * LAYER 1 — UTM ground truth pass.
 *
 * Runs BEFORE the Layer 2 statistical matcher. For each order with
 * utmConfirmedMeta=true, writes an Attribution row (layer=1, confidence=100,
 * matchMethod="utm") using the order's already-linked metaAdId/campaign hierarchy.
 *
 * If the order fits a Meta conversion slot reported in this cycle (same adId,
 * same hour window), we DECREMENT that conv's deltaConversions/deltaValue so
 * Layer 2 solves the residual. If no slot exists (Meta didn't report this
 * conversion — common: dropped pixel, iOS, ad-blocker) we still write the
 * Layer 1 row — it's a confirmed Meta order regardless.
 *
 * Existing confident attributions on the same orderId are UPGRADED to Layer 1
 * (UTM is authoritative over statistical guesses).
 *
 * @returns { layer1Written: number, slotCreditsConsumed: number }
 */
async function runUtmLayer1Pass(shopDomain, dayStr, dayOrders, newConversions, metaOffsetMinutes) {
  const utmOrders = dayOrders.filter(o =>
    o.utmConfirmedMeta === true && o.metaAdId && o.metaAdId.length > 0
  );
  if (utmOrders.length === 0) return { layer1Written: 0, slotCreditsConsumed: 0 };

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
  //       was unambiguous — re-running Layer 1 over those would undo the
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

    // Try to find a Meta conversion slot this cycle for the same ad whose
    // time window contains this order. Prefer slots with remaining capacity.
    const adConvs = convsByAd[order.metaAdId] || [];
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
        metaCampaignId: order.metaCampaignId, metaCampaignName: order.metaCampaignName,
        metaAdSetId: order.metaAdSetId, metaAdSetName: order.metaAdSetName,
        metaAdId: order.metaAdId, metaAdName: order.metaAdName,
        isNewCustomer: order.customerOrderCountAtPurchase === 1, isNewToMeta: order.customerOrderCountAtPurchase === 1,
        matchMethod: "utm",
        metaConversionValue: metaPerConv,
      },
      update: {
        layer: 1, confidence: 100, rivalCount: 0,
        metaCampaignId: order.metaCampaignId, metaCampaignName: order.metaCampaignName,
        metaAdSetId: order.metaAdSetId, metaAdSetName: order.metaAdSetName,
        metaAdId: order.metaAdId, metaAdName: order.metaAdName,
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

  // Build the set of ALL matched order IDs (not just from this run — globally)
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
      //   1 = order country is one this ad had day-level spend in (soft signal) — also when no data
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
  // Time window actually used by the candidate set — referenced by the
  // zero-value £0 non-online fallback path below.
  const { start, end } = hourToMinuteRange(conv.hourSlot, metaOffsetMinutes, paddingUsed);

  if (allCandidates.length === 0) return [];

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
    if (matched.length === 0) return [];
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

  // R=1 fast path — the common case with hourly data. Meta reports one conversion
  // in one hour slot → we look for the single Shopify order whose total is closest
  // to the Meta value within tolerance. No combinatorial search, no averaging.
  const target = conv.deltaValue;
  const tolerance = 0.05;
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
    if (!best) return [];
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

  // R>1 path — multiple conversions in the same ad-hour slot. Backtracking finds
  // the combination of orders whose totals sum to Meta's deltaValue within tolerance.
  // Each matched order is then stored with its ACTUAL frozenTotalPrice as the Meta
  // value (not an average of deltaValue/R — that was the bug).
  const R = Math.min(conv.deltaConversions, allCandidates.length);

  let bestPicks = null, bestDiff = Infinity, bestCountryScore = -1, bestNewCount = -1;
  let iterations = 0;
  const deadline = Date.now() + 5000; // 5s budget per conversion event

  function countNew(picks) { let n = 0; for (const p of picks) if (p.isNew) n++; return n; }
  // Sum of countryRank across picks — higher is better.
  // Rank 2 = deterministic per-cycle country match; Rank 1 = day-level soft match.
  function scoreCountry(picks) { let n = 0; for (const p of picks) n += (p.countryRank || 0); return n; }

  function backtrack(i, start, sum, picks) {
    if (iterations++ % 5000 === 0 && Date.now() >= deadline) return;
    if (i === R) {
      const diff = Math.abs(sum - target);
      if (diff <= target * tolerance) {
        const cScore = scoreCountry(picks);
        const newCount = countNew(picks);
        const EPS = 1e-9;
        // Priority: 1) closer to target, 2) higher country score, 3) more new customers
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

  if (matched.length === 0) return [];
  const matchedSum = matched.reduce((s, p) => s + p.total, 0);
  const diffPct = Math.abs(matchedSum - target) / (target || 1);

  // Calculate rival-based confidence for each pick
  const pickedIds = new Set(matched.map(p => p.id));
  return matched.map(pick => {
    const pickRank = pick.countryRank || 0;
    let rivalCount = 0;
    for (const cand of allCandidates) {
      if (pickedIds.has(cand.id)) continue;
      if (pick.total > 0) {
        const valueDiff = Math.abs(cand.total - pick.total) / pick.total;
        if (valueDiff > RIVAL_VALUE_TOLERANCE) continue;
      }
      // Country disqualification — see R=1 comment above.
      if (pickRank > 0 && (cand.countryRank || 0) < pickRank) continue;
      rivalCount++;
    }
    return {
      ...pick,
      confidence: Math.max(1, Math.round(100 / (1 + rivalCount))),
      rivalCount,
      diffPct,
    };
  });
}

async function syncTodayBreakdowns(shopDomain, metaAccessToken, metaAdAccountId, today, rate = 1.0) {
  let totalRows = 0;
  for (const config of BREAKDOWN_CONFIGS) {
    try {
      const rows = await fetchBreakdown(metaAccessToken, metaAdAccountId, today, config);
      for (const row of rows) {
        convertMetaFields(row, rate);
        const insightData = {
          campaignName: row.campaignName, adSetName: row.adSetName, adName: row.adName,
          impressions: row.impressions, clicks: row.clicks, spend: row.spend, reach: row.reach,
          conversions: row.conversions, conversionValue: row.conversionValue,
          linkClicks: row.linkClicks, landingPageViews: row.landingPageViews,
          addToCart: row.addToCart, initiateCheckout: row.initiateCheckout,
          viewContent: row.viewContent, outboundClicks: row.outboundClicks,
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
  console.log(`[IncrementalSync] Synced ${totalRows} breakdown rows for today`);
  return totalRows;
}

/**
 * Match new conversion deltas for a specific historical day.
 * Reads from MetaInsight (already synced by syncMetaAll), compares against
 * MetaSnapshot, and only matches NEW deltas — never touches existing attributions.
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

  // Load existing incremental attributions for this day — these are PROTECTED.
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

  // LAYER 1: UTM ground truth pass before the statistical matcher. Claims
  // utmConfirmedMeta orders and reduces each conv's deltaConversions/deltaValue.
  const layer1 = await runUtmLayer1Pass(shopDomain, dayStr, dayOrders, newConversions, metaOffsetMinutes);
  // Drop conversions fully consumed by Layer 1.
  const remainingNewConversions = newConversions.filter(c => c.deltaConversions > 0);

  const dayOrderIds = new Set(dayOrders.map(o => o.shopifyOrderId));

  // Find incremental attributions linked to orders on this day — these are PROTECTED.
  // Count per-ad how many conversions are already covered by incremental matches.
  const existingIncrementals = await db.attribution.findMany({
    where: {
      shopDomain,
      matchMethod: "incremental",
      confidence: { gt: 0 },
      shopifyOrderId: { in: [...dayOrderIds] },
    },
    select: { metaAdId: true },
  });
  const incrementalCountByAd = {};
  for (const a of existingIncrementals) {
    incrementalCountByAd[a.metaAdId] = (incrementalCountByAd[a.metaAdId] || 0) + 1;
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
    // Incremental matches captured the original order value before Shopify edits —
    // they are more accurate. Only process the EXTRA conversions the incremental sync missed.
    const incrementalCount = incrementalCountByAd[conv.adId] || 0;
    if (incrementalCount > 0) {
      // Reduce delta by the number already handled incrementally
      // The snapshot tracks cumulative conversions, so the delta includes ones already matched
      const alreadyCovered = Math.min(conv.deltaConversions, incrementalCount);
      incrementalCountByAd[conv.adId] -= alreadyCovered;
      conv.deltaConversions -= alreadyCovered;
      if (conv.deltaConversions > 0) {
        // Adjust delta value proportionally
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
        // Daily-sweep is last-resort only — fills gaps the incremental run missed.
        const existing = await db.attribution.findUnique({
          where: { shopDomain_shopifyOrderId: { shopDomain, shopifyOrderId: pick.orderId } },
          select: { matchMethod: true, confidence: true, metaAdId: true },
        });
        if (existing && existing.confidence > 0) {
          if (existing.matchMethod === "incremental" || existing.matchMethod === "utm + incremental") {
            skippedIncremental++;
            continue;
          }
          // Preserve UTM Layer 1 unless the daily-sweep pick is unambiguous.
          if (existing.matchMethod === "utm" && (pick.rivalCount || 0) > 0) continue;
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

  // Post-match: recalculate confidence for orders whose rivals were resolved
  await recalculateConfidence(shopDomain, dailyMatchedOrderIds);

  // Update snapshots for this day
  await saveSnapshot(shopDomain, dayStr, currentData);

  console.log(`[DeltaMatch] ${dayStr}: ${layer1.layer1Written} UTM layer1, ${totalMatched} matched, ${totalUnmatched} unmatched, ${skippedIncremental} preserved incremental`);
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
  // the Shopify day — Meta will return that calendar date's data in its own tz,
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

  // Save snapshot IMMEDIATELY after finding deltas — before matching or breakdowns.
  // This prevents stale snapshots if a later step crashes (fetch failed, etc.).
  // The snapshot must reflect what Meta currently reports, regardless of whether
  // we successfully match or sync breakdowns.
  await saveSnapshot(shopDomain, today, currentData);

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

  if (newConversions.length === 0) {
    setProgress(`incrementalSync:${shopDomain}`, { status: "running", message: "Syncing today's breakdowns..." });
    let breakdownRows = 0;
    try {
      breakdownRows = await syncTodayBreakdowns(shopDomain, shop.metaAccessToken, shop.metaAdAccountId, today, rate);
    } catch (err) {
      console.error(`[IncrementalSync] Breakdown sync failed (non-fatal): ${err.message}`);
    }
    // No new conversions — throttle rollup rebuilds to once per day so the
    // hourly scheduler doesn't spend ~13 min every cycle rewriting identical
    // rollup rows. A fresh boot still rebuilds once (lastRollupRebuildAt starts
    // empty), so deploys continue to pick up code changes promptly.
    const didRebuild = await rebuildAllRollups(shopDomain, { force: false });
    if (didRebuild) {
      invalidateShop(shopDomain);
      // Re-warm the query cache in background (fire-and-forget)
      import("./cacheWarmer.server.js").then(({ warmAllShops }) => {
        warmAllShops().catch(err => console.error("[IncrementalSync] post-sync warm failed:", err.message));
      }).catch(err => console.error("[IncrementalSync] warmer import failed:", err.message));
    }
    await db.shop.update({ where: { shopDomain }, data: { lastMetaSync: new Date() } });
    completeProgress(`incrementalSync:${shopDomain}`, { newConversions: 0, matched: 0, unmatched: 0, breakdownRows });
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

  // LAYER 1: UTM ground truth pass — claims utmConfirmedMeta orders before
  // the statistical matcher runs and consumes slot capacity from newConversions.
  const layer1 = await runUtmLayer1Pass(shopDomain, today, todayOrders, newConversions, metaOffsetMinutes);

  // Filter out conversions fully consumed by the Layer 1 pass.
  const remainingConversions = newConversions.filter(c => c.deltaConversions > 0);

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
        // Preserve UTM Layer 1 unless this pick is unambiguous.
        if (existing && existing.matchMethod === "utm" && (pick.rivalCount || 0) > 0) continue;
        // Blend label when UTM and stat agree on the ad.
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

  // Post-match: recalculate confidence for orders whose rivals were resolved
  // (e.g., two orders in the same time slot both got matched to different conversions)
  await recalculateConfidence(shopDomain, matchedOrderIds);

  setProgress(`incrementalSync:${shopDomain}`, { status: "running", message: "Syncing today's breakdowns..." });
  let breakdownRows = 0;
  try {
    breakdownRows = await syncTodayBreakdowns(shopDomain, shop.metaAccessToken, shop.metaAdAccountId, today, rate);
  } catch (err) {
    console.error(`[IncrementalSync] Breakdown sync failed (non-fatal): ${err.message}`);
  }

  // Enrich today's attributions with demographic data from breakdowns
  setProgress(`incrementalSync:${shopDomain}`, { status: "running", message: "Enriching attribution demographics..." });
  let enrichResult = { enriched: 0 };
  try {
    const { enrichForDate } = await import("./attributionEnrichment.server.js");
    enrichResult = await enrichForDate(shopDomain, today);
  } catch (err) {
    console.error(`[IncrementalSync] Demographic enrichment failed (non-fatal): ${err.message}`);
  }

  // New conversions arrived — rollups may be stale, force a full rebuild.
  await rebuildAllRollups(shopDomain, { force: true });

  invalidateShop(shopDomain);

  // Re-warm the query cache for this shop in the background.
  // Without this, the first user tab load after a sync would be slow
  // (the sync invalidated the cache; the warm queries repopulate it).
  import("./cacheWarmer.server.js").then(({ warmAllShops }) => {
    // warmAllShops() warms every shop; cheap on small installs.
    // For large installs we could call warmShop(shopDomain) if it was exported.
    warmAllShops().catch(err => console.error("[IncrementalSync] post-sync warm failed:", err.message));
  }).catch(err => console.error("[IncrementalSync] warmer import failed:", err.message));

  await db.shop.update({ where: { shopDomain }, data: { lastMetaSync: new Date() } });
  completeProgress(`incrementalSync:${shopDomain}`, { newConversions: newConversions.length, layer1: layer1.layer1Written, matched: totalMatched, unmatched: totalUnmatched, breakdownRows, ...enrichResult });
  console.log(`[IncrementalSync] Complete: ${layer1.layer1Written} UTM layer1, ${totalMatched} matched, ${totalUnmatched} unmatched, ${breakdownRows} breakdowns, ${enrichResult.enriched} demographics enriched`);
  return { newConversions: newConversions.length, layer1: layer1.layer1Written, matched: totalMatched, unmatched: totalUnmatched, breakdownRows, ...enrichResult };
}
