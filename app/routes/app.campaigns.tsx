import { json } from "@remix-run/node";
import { useLoaderData, useSearchParams, useSubmit, useActionData, useRevalidator } from "@remix-run/react";
import {
  Page, Layout, Card, Text, BlockStack, InlineStack, Button, Checkbox,
  Popover, ActionList,
} from "@shopify/polaris";
import InteractiveTable from "../components/InteractiveTable";
import ReportTabs from "../components/ReportTabs";
import { usePageTheme } from "../components/PageTheme";
import TileGrid from "../components/TileGrid";
import type { TileDef } from "../components/TileGrid";
import AiInsightsPanel from "../components/AiInsightsPanel";
import PageSummary, { type SummaryBullet } from "../components/PageSummary";
import SummaryTile from "../components/SummaryTile";
import ChangesAnnotationStrip from "../components/ChangesAnnotationStrip";
import EntityTimelineDrawer, { type EntityRef } from "../components/EntityTimelineDrawer";
import { useState, useMemo, useCallback, useRef, useEffect } from "react";
import { createPortal } from "react-dom";
import { type ColumnDef } from "@tanstack/react-table";
import { authenticate } from "../shopify.server";
import db from "../db.server";
import { parseDateRange } from "../utils/dateRange.server";
import { shopLocalDayKey, shopRangeBounds } from "../utils/shopTime.server";
import { currencySymbolFromCode } from "../utils/currency";
import { getCachedInsights, computeDataHash, generateInsights } from "../services/aiAnalysis.server";
import { setProgress, failProgress, completeProgress } from "../services/progress.server";
import { cached as queryCached, DEFAULT_TTL } from "../services/queryCache.server";
import { loadLtvSnapshot } from "../services/ltvSnapshot.server.js";
import { netPaidOf } from "../utils/orderRevenue";

// loadLtvSnapshot extracted to app/services/ltvSnapshot.server.js so the cache
// warmer and the loader can both use it.

function aggregateInsights(insights, attributions, orders, level) {
  const aggregated = {};

  for (const ins of insights) {
    let key, name, campaignId, campaignName, adSetId, adSetName;
    if (level === "campaign") {
      key = ins.campaignId; name = ins.campaignName || ins.campaignId;
    } else if (level === "adset") {
      key = ins.adSetId; name = ins.adSetName || ins.adSetId;
      campaignId = ins.campaignId; campaignName = ins.campaignName || ins.campaignId;
    } else {
      key = ins.adId; name = ins.adName || ins.adId;
      campaignId = ins.campaignId; campaignName = ins.campaignName || ins.campaignId;
      adSetId = ins.adSetId; adSetName = ins.adSetName || ins.adSetId;
    }

    if (!aggregated[key]) {
      aggregated[key] = {
        id: key, name,
        campaignId: campaignId || null,
        campaignName: campaignName || null,
        adSetId: adSetId || null,
        adSetName: adSetName || null,
        spend: 0, impressions: 0, clicks: 0,
        metaConversions: 0, metaConversionValue: 0,
        attributedOrders: 0, attributedRevenue: 0,
        newCustomerOrders: 0, existingCustomerOrders: 0,
        newCustomerRevenue: 0, existingCustomerRevenue: 0,
        unverifiedRevenue: 0,
        linkClicks: 0, landingPageViews: 0,
        viewContent: 0, addToCart: 0, initiateCheckout: 0,
        videoP25: 0, videoP50: 0, videoP75: 0, videoP100: 0,
        frequencySum: 0, frequencyCount: 0,
        utmOnlyOrders: 0, utmOnlyRevenue: 0,
      };
    }

    const row = aggregated[key];
    row.spend += ins.spend;
    row.impressions += ins.impressions;
    row.clicks += ins.clicks;
    row.metaConversions += ins.conversions;
    row.metaConversionValue += ins.conversionValue;
    row.linkClicks += ins.linkClicks;
    row.landingPageViews += ins.landingPageViews;
    row.viewContent += ins.viewContent;
    row.addToCart += ins.addToCart;
    row.initiateCheckout += ins.initiateCheckout;
    row.videoP25 += ins.videoP25;
    row.videoP50 += ins.videoP50;
    row.videoP75 += ins.videoP75;
    row.videoP100 += ins.videoP100;
    if (ins.frequency > 0) {
      row.frequencySum += ins.frequency;
      row.frequencyCount++;
    }
  }

  const orderMap = {};
  for (const o of orders) orderMap[o.shopifyOrderId] = o;

  const matchedAttrs = attributions.filter(a => a.confidence > 0);
  const unmatchedAttrs = attributions.filter(a => a.confidence === 0);

  for (const attr of matchedAttrs) {
    let key;
    if (level === "campaign") key = attr.metaCampaignId;
    else if (level === "adset") key = attr.metaAdSetId;
    else key = attr.metaAdId;

    const row = aggregated[key];
    if (!row) continue;

    const order = orderMap[attr.shopifyOrderId];
    if (order) {
      const rev = netPaidOf(order); // exchange-aware (was missing refund subtraction entirely)
      row.attributedOrders++;
      row.attributedRevenue += rev;
      if (attr.isNewCustomer) {
        row.newCustomerOrders++;
        row.newCustomerRevenue += rev;
      } else {
        row.existingCustomerOrders++;
        row.existingCustomerRevenue += rev;
      }
    }
  }

  for (const attr of unmatchedAttrs) {
    let key;
    if (level === "campaign") key = attr.metaCampaignId;
    else if (level === "adset") key = attr.metaAdSetId;
    else key = attr.metaAdId;

    const row = aggregated[key];
    if (!row) continue;
    row.unverifiedRevenue += attr.metaConversionValue || 0;
  }

  // UTM-only orders: utmConfirmedMeta=true but no Layer 2 attribution
  // These are real Meta-driven sales that the statistical matcher couldn't match.
  // Roll them up under their linked campaign/adset/ad.
  const matchedOrderIds = new Set(matchedAttrs.map(a => a.shopifyOrderId));
  for (const order of orders) {
    if (!order.utmConfirmedMeta) continue;
    if (matchedOrderIds.has(order.shopifyOrderId)) continue; // already counted via Layer 2

    let key;
    if (level === "campaign") key = order.metaCampaignId;
    else if (level === "adset") key = order.metaAdSetId;
    else key = order.metaAdId;

    if (!key) continue; // no campaign linkage, can't aggregate

    // Create row if it doesn't exist (campaign may have no Meta insights in this period)
    if (!aggregated[key]) {
      let name, campaignId, campaignName, adSetId, adSetName;
      if (level === "campaign") {
        name = order.metaCampaignName || key;
      } else if (level === "adset") {
        name = order.metaAdSetName || key;
        campaignId = order.metaCampaignId; campaignName = order.metaCampaignName;
      } else {
        name = order.metaAdName || key;
        campaignId = order.metaCampaignId; campaignName = order.metaCampaignName;
        adSetId = order.metaAdSetId; adSetName = order.metaAdSetName;
      }
      aggregated[key] = {
        id: key, name,
        campaignId: campaignId || null, campaignName: campaignName || null,
        adSetId: adSetId || null, adSetName: adSetName || null,
        spend: 0, impressions: 0, clicks: 0,
        metaConversions: 0, metaConversionValue: 0,
        attributedOrders: 0, attributedRevenue: 0,
        newCustomerOrders: 0, existingCustomerOrders: 0,
        newCustomerRevenue: 0, existingCustomerRevenue: 0,
        unverifiedRevenue: 0,
        linkClicks: 0, landingPageViews: 0,
        viewContent: 0, addToCart: 0, initiateCheckout: 0,
        videoP25: 0, videoP50: 0, videoP75: 0, videoP100: 0,
        frequencySum: 0, frequencyCount: 0,
        utmOnlyOrders: 0, utmOnlyRevenue: 0,
      };
    }

    const row = aggregated[key];
    const rev = netPaidOf(order); // exchange-aware (was missing refund subtraction entirely)
    row.attributedOrders++;
    row.attributedRevenue += rev;
    row.utmOnlyOrders++;
    row.utmOnlyRevenue += rev;
    if (order.customerOrderCountAtPurchase === 1) {
      row.newCustomerOrders++;
      row.newCustomerRevenue += rev;
    } else {
      row.existingCustomerOrders++;
      row.existingCustomerRevenue += rev;
    }
  }

  return aggregated;
}

/**
 * Rollup-based aggregator.
 * Input: DailyAdRollup rows (pre-summed per ad per day with attributions baked in).
 * Output: same shape as aggregateInsights() so computeRows and downstream
 * consumers are unchanged.
 */
// Single-pass aggregation for ALL 3 levels (campaign/adset/ad).
// Runs over the rollup rows ONCE instead of 3×, then returns all three.
function aggregateRollupAllLevels(rows) {
  const campaign = {};
  const adset = {};
  const ad = {};

  const emptyRow = (id, name, campaignId, campaignName, adSetId, adSetName) => ({
    id, name,
    campaignId: campaignId || null, campaignName: campaignName || null,
    adSetId: adSetId || null, adSetName: adSetName || null,
    spend: 0, impressions: 0, clicks: 0,
    metaConversions: 0, metaConversionValue: 0,
    attributedOrders: 0, attributedRevenue: 0,
    newCustomerOrders: 0, existingCustomerOrders: 0,
    newCustomerRevenue: 0, existingCustomerRevenue: 0,
    unverifiedRevenue: 0,
    linkClicks: 0, landingPageViews: 0,
    viewContent: 0, addToCart: 0, initiateCheckout: 0,
    videoP25: 0, videoP50: 0, videoP75: 0, videoP100: 0,
    frequencySum: 0, frequencyCount: 0,
    utmOnlyOrders: 0, utmOnlyRevenue: 0,
  });

  const addTo = (row, r) => {
    row.spend += r.spend;
    row.impressions += r.impressions;
    row.clicks += r.clicks;
    row.metaConversions += r.metaConversions;
    row.metaConversionValue += r.metaConversionValue;
    row.linkClicks += r.linkClicks;
    row.landingPageViews += r.landingPageViews;
    row.viewContent += r.viewContent;
    row.addToCart += r.addToCart;
    row.initiateCheckout += r.initiateCheckout;
    row.videoP25 += r.videoP25;
    row.videoP50 += r.videoP50;
    row.videoP75 += r.videoP75;
    row.videoP100 += r.videoP100;
    row.frequencySum += r.frequencySum;
    row.frequencyCount += r.frequencyCount;
    row.attributedOrders += r.attributedOrders;
    row.attributedRevenue += r.attributedRevenue;
    row.newCustomerOrders += r.newCustomerOrders;
    row.newCustomerRevenue += r.newCustomerRevenue;
    row.existingCustomerOrders += r.existingCustomerOrders;
    row.existingCustomerRevenue += r.existingCustomerRevenue;
    row.unverifiedRevenue += r.unverifiedRevenue;
    row.utmOnlyOrders += r.utmOnlyOrders || 0;
    row.utmOnlyRevenue += r.utmOnlyRevenue || 0;
  };

  for (const r of rows) {
    // Campaign-level
    if (r.campaignId) {
      if (!campaign[r.campaignId]) {
        campaign[r.campaignId] = emptyRow(r.campaignId, r.campaignName || r.campaignId, null, null, null, null);
      }
      addTo(campaign[r.campaignId], r);
    }
    // AdSet-level
    if (r.adSetId) {
      if (!adset[r.adSetId]) {
        adset[r.adSetId] = emptyRow(r.adSetId, r.adSetName || r.adSetId, r.campaignId, r.campaignName || r.campaignId, null, null);
      }
      addTo(adset[r.adSetId], r);
    }
    // Ad-level
    if (r.adId) {
      if (!ad[r.adId]) {
        ad[r.adId] = emptyRow(r.adId, r.adName || r.adId, r.campaignId, r.campaignName || r.campaignId, r.adSetId, r.adSetName || r.adSetId);
      }
      addTo(ad[r.adId], r);
    }
  }
  return { campaign, adset, ad };
}

// Backward-compat wrapper for the single-level version (used by comparison period path).
function aggregateRollup(rows, level) {
  const all = aggregateRollupAllLevels(rows);
  return all[level];
}

function computeRows(aggregated) {
  const r2 = (v) => Math.round(v * 100) / 100;
  const computeRow = (r) => ({
    ...r,
    spend: r2(r.spend),
    attributedRevenue: r2(r.attributedRevenue),
    newCustomerRevenue: r2(r.newCustomerRevenue),
    existingCustomerRevenue: r2(r.existingCustomerRevenue),
    metaConversionValue: r2(r.metaConversionValue),
    unverifiedRevenue: r2(r.unverifiedRevenue),
    utmOnlyOrders: r.utmOnlyOrders || 0,
    utmOnlyRevenue: r2(r.utmOnlyRevenue || 0),
    blendedROAS: r.spend > 0 ? r2((r.attributedRevenue + r.unverifiedRevenue) / r.spend) : 0,
    blendedCPA: r.attributedOrders > 0 ? r2(r.spend / r.attributedOrders) : null,
    ctr: r.impressions > 0 ? r2((r.clicks / r.impressions) * 100) : 0,
    cpa: r.attributedOrders > 0 ? r2(r.spend / r.attributedOrders) : 0,
    avgFrequency: (r.frequencyCount || 0) > 0 ? r2(r.frequencySum / r.frequencyCount) : 0,
    atcRate: r.viewContent > 0 ? r2((r.addToCart / r.viewContent) * 100) : 0,
    checkoutRate: r.addToCart > 0 ? r2((r.initiateCheckout / r.addToCart) * 100) : 0,
    purchaseRate: r.initiateCheckout > 0 ? r2((r.metaConversions / r.initiateCheckout) * 100) : 0,
  });

  return Object.values(aggregated)
    .sort((a, b) => b.spend - a.spend)
    .map(computeRow);
}

export const loader = async ({ request }) => {
  const { session } = await authenticate.admin(request);
  const shopDomain = session.shop;

  const url = new URL(request.url);
  const breakdown = url.searchParams.get("breakdown") || "none";

  // Load shop first so we can do every date-bucket decision in its timezone.
  const shop = await db.shop.findUnique({ where: { shopDomain } });
  const tz = shop?.shopifyTimezone || "UTC";

  const { fromDate, toDate, fromKey, toKey, preset, compareFrom, compareTo, compareFromKey, compareToKey, hasComparison, compareLabel } = parseDateRange(request, tz);

  const _t0 = Date.now();

  // Day arithmetic on shop-local day keys. Storage stays UTC;
  // bounds are derived from shop-local keys via shopRangeBounds.
  const addDaysKey = (key: string, delta: number): string => {
    const [y, m, d] = key.split("-").map(Number);
    const anchor = new Date(Date.UTC(y, m - 1, d, 12, 0, 0, 0));
    anchor.setUTCDate(anchor.getUTCDate() + delta);
    return shopLocalDayKey(tz, anchor);
  };
  const diffDays = (a: string, b: string): number => {
    const [ay, am, ad] = a.split("-").map(Number);
    const [by, bm, bd] = b.split("-").map(Number);
    return Math.round((Date.UTC(by, bm - 1, bd) - Date.UTC(ay, am - 1, ad)) / 86400000);
  };
  const _dayCount = diffDays(fromKey, toKey) + 1;
  const _prevToKey = addDaysKey(fromKey, -1);
  const _prevFromKey = addDaysKey(_prevToKey, -(_dayCount - 1));
  const _prevBounds = shopRangeBounds(tz, _prevFromKey, _prevToKey);
  const _prevFrom = _prevBounds.gte;
  const _prevTo = _prevBounds.lte;

  let windowStart = _prevFrom < fromDate ? _prevFrom : fromDate;
  let windowEnd = toDate > _prevTo ? toDate : _prevTo;
  let windowStartKey = _prevFromKey < fromKey ? _prevFromKey : fromKey;
  let windowEndKey = toKey > _prevToKey ? toKey : _prevToKey;
  if (hasComparison && compareFrom && compareTo && compareFromKey && compareToKey) {
    if (compareFrom < windowStart) windowStart = compareFrom;
    if (compareTo > windowEnd) windowEnd = compareTo;
    if (compareFromKey < windowStartKey) windowStartKey = compareFromKey;
    if (compareToKey > windowEndKey) windowEndKey = compareToKey;
  }

  // Use DailyAdRollup which has all the same fields (renamed conversions→metaConversions)
  const insightSelect = {
    date: true,
    campaignId: true, campaignName: true, adSetId: true, adSetName: true,
    adId: true, adName: true, spend: true, impressions: true, clicks: true,
    metaConversions: true, metaConversionValue: true, reach: true,
    frequencySum: true, frequencyCount: true,
    linkClicks: true, landingPageViews: true, viewContent: true,
    addToCart: true, initiateCheckout: true,
    videoP25: true, videoP50: true, videoP75: true, videoP100: true,
  };
  const orderSelect = {
    shopifyOrderId: true, createdAt: true, frozenTotalPrice: true, totalRefunded: true, netPaid: true,
    isNewCustomerOrder: true, customerOrderCountAtPurchase: true, isOnlineStore: true, shopifyCustomerId: true, utmConfirmedMeta: true,
    metaCampaignId: true, metaCampaignName: true,
    metaAdSetId: true, metaAdSetName: true,
    metaAdId: true, metaAdName: true,
  };

  // ── Scoped DB queries ──
  // - Orders: narrowed to the window (was: entire order table)
  // - Attributions: confident ones for window orders, plus all placeholders
  //   (placeholders are a small set)
  // - LTV snapshot: cached, runs at most once per 5 min per shop
  // Prev-period DB-fetch window (alias the earlier computation).
  const _prevFromRP = _prevFrom;
  const _prevToRP = _prevTo;
  const prevFromKey = _prevFromKey;
  const prevToKey = _prevToKey;
  const compareKey = (hasComparison && compareFromKey && compareToKey)
    ? `${compareFromKey}:${compareToKey}`
    : "none";

  // Loader helpers - fetch rollup then pre-aggregate at all 3 levels in a single pass.
  // The output (thousands of aggregated rows) is much smaller than the raw 31k+ rollup rows,
  // so caching the aggregate skips both the DB query AND the JS aggregation on repeat hits.
  const fetchAndAggregate = (from: Date, to: Date) => async () => {
    const rows = await db.dailyAdRollup.findMany({ where: { shopDomain, date: { gte: from, lte: to } } });
    return aggregateRollupAllLevels(rows);
  };

  const time = async <T,>(label: string, p: Promise<T>): Promise<T> => {
    const t = Date.now();
    const r = await p;
    const ms = Date.now() - t;
    if (ms > 100) console.log(`[campaigns]   ${label}: ${ms}ms`);
    return r;
  };

  // Per-ad demographic rollup for the current period. Read by AdExplorerTable
  // when the gender or age filters are active - we recompute the row metrics
  // from these rows so the filter actually takes effect on order/revenue numbers.
  // Spend stays from DailyAdRollup (no demographic split - Meta does not attach
  // customer-resolved demographics to spend).
  const fetchAdDemographics = async () => {
    return db.dailyAdDemographicRollup.findMany({
      where: { shopDomain, date: { gte: fromDate, lte: toDate } },
      select: {
        adId: true, gender: true, ageBracket: true,
        attributedOrders: true, attributedRevenue: true,
        newCustomerOrders: true, newCustomerRevenue: true,
        existingCustomerOrders: true, existingCustomerRevenue: true,
      },
    });
  };

  // NB: order in destructure matches Promise.all() order. dailyLiveAds runs
  // before windowOrders in the array; previously the two variable names were
  // swapped at positions 7/8, which gave windowOrdersRaw the rows of the live-
  // ads aggregate and broke every downstream Order lookup. Fixed to match.
  const [currentAggRaw, prevAggRaw, compareAggRaw, metaEntities, ltvSnapshot, dailyChart, dailyLiveAdsRaw, windowOrdersRaw, adDemographicsRaw] = await Promise.all([
    time("campAgg", queryCached(`${shopDomain}:campAgg:${fromKey}:${toKey}`, DEFAULT_TTL, fetchAndAggregate(fromDate, toDate))),
    time("campAggPrev", queryCached(`${shopDomain}:campAgg:${prevFromKey}:${prevToKey}`, DEFAULT_TTL, fetchAndAggregate(_prevFromRP, _prevToRP))),
    (hasComparison && compareFrom && compareTo)
      ? time("campAggComp", queryCached(`${shopDomain}:campAgg:${compareKey}`, DEFAULT_TTL, fetchAndAggregate(compareFrom, compareTo)))
      : Promise.resolve(null),
    time("metaEntities", queryCached(`${shopDomain}:metaEntities`, DEFAULT_TTL, () =>
      db.metaEntity.findMany({
        where: { shopDomain },
        select: {
          entityType: true, entityId: true, entityName: true,
          createdTime: true, funnelStage: true,
          // Needed by AdFunnelTreeTile: live status badge + targeting summary
          // for each ad set, so each ad inherits the right pool description.
          targetingSpec: true, currentStatus: true,
          // Ad creative thumbnails (entityType='ad' only) - rendered as tiles
          // in AdExplorerTable. productSetId is non-null for Dynamic Product
          // Ads / Advantage+ catalog so the explorer can render a "D" badge.
          // thumbnailFetchedAt is appended as a cache-busting query param to
          // the proxy URL so browsers don't keep serving stale cached bytes
          // (e.g. the 64x64 placeholder cached before /adimages full-res
          // resolution landed). Without this, max-age=86400 on the proxy
          // means a refresh isn't visible to merchants for up to a day.
          thumbnailUrl: true, imageUrl: true, productSetId: true,
          thumbnailFetchedAt: true,
        },
      }),
    )),
    time("ltvSnapshot", queryCached(`${shopDomain}:ltvSnapshot`, DEFAULT_TTL, () => loadLtvSnapshot(shopDomain))),
    time("dailyChart", queryCached(`${shopDomain}:campDailyChart:${fromKey}:${toKey}`, DEFAULT_TTL, () =>
      db.dailyAdRollup.groupBy({
        by: ["date"],
        where: { shopDomain, date: { gte: fromDate, lte: toDate } },
        _sum: { spend: true, impressions: true },
      }),
    )),
    // Daily distinct-ad count: how many ads actually delivered each day,
    // defined as spend > 0 OR impressions > 0 in DailyAdRollup. Used by the
    // Live Ads summary tile (its sparkline + headline number) - a behavioural
    // definition of "live" beats relying on Meta's effective_status field
    // because that field comes back as PAUSED/IN_PROCESS/CAMPAIGN_PAUSED/etc
    // and was leaving the Live Ads count stuck at 0 for accounts where no ad
    // had effective_status="ACTIVE".
    time("dailyLiveAds", queryCached(`${shopDomain}:campDailyLiveAds:${fromKey}:${toKey}`, DEFAULT_TTL, () =>
      db.$queryRaw`
        SELECT date, COUNT(DISTINCT adId) AS liveAds
        FROM DailyAdRollup
        WHERE shopDomain = ${shopDomain}
          AND date >= ${fromDate}
          AND date <= ${toDate}
          AND (spend > 0 OR impressions > 0)
        GROUP BY date
        ORDER BY date
      ` as Promise<Array<{ date: Date; liveAds: number | bigint }>>,
    )),
    time("windowOrders", queryCached(`${shopDomain}:campWindowOrders:${windowStartKey}:${windowEndKey}`, DEFAULT_TTL, () =>
      db.order.findMany({
        where: { shopDomain, isOnlineStore: true, createdAt: { gte: windowStart, lte: windowEnd } },
        select: orderSelect,
      }),
    )),
    time("adDemographics", queryCached(`${shopDomain}:campAdDemo:${fromKey}:${toKey}`, DEFAULT_TTL, fetchAdDemographics)),
  ]);
  // insights is now a per-day aggregation (groupBy result); rename for downstream clarity
  const insights = dailyChart as any[];

  // Alias kept to minimise diff in the rest of the loader - all downstream code
  // that references `allOrders` now operates on the window slice.
  const allOrders = windowOrdersRaw;
  const windowOrderIds = allOrders.map(o => o.shopifyOrderId);

  // Date-scope the confidence=0 placeholder attributions (was fetching all-time)
  const attributions = await queryCached(
    `${shopDomain}:campAttrs:${fromKey}:${toKey}`,
    DEFAULT_TTL,
    () => db.attribution.findMany({
      where: {
        shopDomain,
        OR: [
          { shopifyOrderId: { in: windowOrderIds } },
          { confidence: 0, matchedAt: { gte: fromDate, lte: toDate } },
        ],
      },
      select: {
        shopifyOrderId: true, confidence: true, isNewCustomer: true,
        metaCampaignId: true, metaCampaignName: true,
        metaAdSetId: true, metaAdSetName: true,
        metaAdId: true, metaAdName: true,
        metaConversionValue: true,
        metaPlatform: true, metaPlacement: true,
      },
    }),
  );

  const { customerAcq, ltvMaps } = ltvSnapshot;

  const currentAgg = currentAggRaw;
  const prevAgg = prevAggRaw;
  const compareAgg = compareAggRaw;
  console.log(`[campaigns] db ${Date.now() - _t0}ms (orders=${allOrders.length}, insights=${insights.length}, adLevel=${Object.keys(currentAgg.ad).length})`);

  const currencySymbol = currencySymbolFromCode(shop?.shopifyCurrency);

  const ordersInRange = allOrders.filter(o => o.createdAt >= fromDate && o.createdAt <= toDate);
  const orderIdsInRange = new Set(ordersInRange.map(o => o.shopifyOrderId));
  const attrsInRange = attributions.filter(a => {
    if (a.confidence > 0) return orderIdsInRange.has(a.shopifyOrderId);
    const match = a.shopifyOrderId.match(/(\d{4}-\d{2}-\d{2})/);
    if (!match) return false;
    return match[1] >= fromKey && match[1] <= toKey;
  });

  // Total store revenue in reporting period (all orders, not just Meta-attributed)
  // Net of refunds so it's comparable to attributed revenue on the same page.
  const totalStoreRevenue = ordersInRange.reduce(
    (sum, o) => sum + netPaidOf(o), // exchange-aware
    0,
  );

  // Build entity created_time map from pre-fetched metaEntities
  const entityCreatedMap = {};
  // Ad creative thumbnails keyed by ad ID. Thumbnails go through our proxy
  // (/api/ad-thumbnail/:adId — top-level path, not /app/*, so browser <img>
  // requests don't trip the embedded-app session-token auth) so the explorer
  // can serve cached bytes
  // from the Fly volume - the raw Meta CDN URLs rotate every few hours and
  // were leaving the explorer with empty tiles between nightly refreshes
  // and immediately after deploys. The proxy falls back to a 302 to the
  // current Meta URL when local bytes aren't yet cached. imageUrl (full-
  // size) keeps the direct Meta URL since we don't proxy it.
  const adThumbMap: Record<string, { thumbnailUrl: string | null; imageUrl: string | null; productSetId: string | null }> = {};
  for (const e of metaEntities) {
    if (e.createdTime) entityCreatedMap[`${e.entityType}:${e.entityId}`] = e.createdTime;
    if (e.entityType === "ad" && (e.thumbnailUrl || e.imageUrl || e.productSetId)) {
      // Both URLs route through the proxy. ?size=full asks for the large
      // image_url asset (Top Ads for New Customers cards render at ~250px
      // and pixelate on the small thumbnail). The proxy serves cached
      // bytes preferentially and falls back to a 302 to the live Meta URL
      // when nothing is on disk yet.
      //
      // ?v=<thumbnailFetchedAt> is a cache-buster: the proxy sets
      // Cache-Control: max-age=86400, so without a versioned URL the
      // browser keeps serving stale bytes (e.g. the 64x64 placeholder it
      // grabbed before /adimages full-res resolution shipped) for up to a
      // day after a refresh. Bumping fetchedAt on each refreshAdCreatives
      // run forces a fresh GET.
      const proxyBase = `/api/ad-thumbnail/${e.entityId}`;
      const hasAnyImage = !!(e.thumbnailUrl || e.imageUrl);
      const v = e.thumbnailFetchedAt ? e.thumbnailFetchedAt.getTime() : 0;
      adThumbMap[e.entityId] = {
        thumbnailUrl: hasAnyImage ? `${proxyBase}?v=${v}` : null,
        imageUrl: hasAnyImage ? `${proxyBase}?size=full&v=${v}` : null,
        productSetId: e.productSetId,
      };
    }
  }

  const now = new Date();
  const reportingPeriodDays = Math.max(1, Math.ceil((toDate.getTime() - fromDate.getTime()) / 86400000));
  const r2g = (v) => Math.round(v * 100) / 100;

  // `orderMap` is used downstream for the breakdown and daily-chart blocks.
  // It's scoped to the window (was: all-time) - every downstream consumer
  // already filters to window sub-slices so this is safe.
  const orderMap = {};
  for (const o of allOrders) orderMap[o.shopifyOrderId] = o;

  // Count unique new Meta customers acquired in the current period
  // (previous-period count is computed further down when prev window is known).
  let uniqueNewMetaCustomers = 0;
  let prevUniqueNewMetaCustomers = 0;
  for (const acq of Object.values(customerAcq) as any[]) {
    if (acq.acquisitionDate >= fromDate && acq.acquisitionDate <= toDate) {
      uniqueNewMetaCustomers++;
    }
  }

  // Count ALL new customers in the period using BOTH signals: the
  // order-row flag (isNewCustomerOrder) set at sync time, OR the count
  // field (customerOrderCountAtPurchase === 1). Either signal alone misses
  // edge cases - see attribution_isnew_two_signal_fix memory and the
  // 2026-05-12 empty-New-Meta-tiles regression for context.
  const totalNewCustomersInPeriodIds = new Set<string>();
  for (const o of allOrders) {
    if (!o.isOnlineStore) continue;
    if (o.createdAt < fromDate || o.createdAt > toDate) continue;
    if (!o.shopifyCustomerId) continue;
    const isNew = o.isNewCustomerOrder === true || o.customerOrderCountAtPurchase === 1;
    if (isNew) {
      totalNewCustomersInPeriodIds.add(o.shopifyCustomerId);
    }
  }
  const totalNewCustomersInPeriod = totalNewCustomersInPeriodIds.size;

  const addAdAge = (rows, entityType) => rows.map(r => {
    const created = entityCreatedMap[`${entityType}:${r.id}`];
    const adAgeDays = created ? Math.ceil((now.getTime() - new Date(created).getTime()) / 86400000) : null;
    const activeDays = Math.min(adAgeDays || reportingPeriodDays, reportingPeriodDays);
    const ltv = ltvMaps[entityType]?.[r.id] || {};
    return {
      ...r,
      adAgeDays,
      newCustomerCPA: r.newCustomerOrders > 0 ? r2g(r.spend / r.newCustomerOrders) : null,
      revenuePerNewCustomer: r.newCustomerOrders > 0 ? r2g(r.newCustomerRevenue / r.newCustomerOrders) : null,
      spendPerDay: activeDays > 0 && r.spend > 0 ? r2g(r.spend / activeDays) : null,
      newCustomersPerDay: activeDays > 0 && r.newCustomerOrders > 0 ? r2g(r.newCustomerOrders / activeDays) : null,
      newCustomerRevenuePerDay: activeDays > 0 && r.newCustomerRevenue > 0 ? r2g(r.newCustomerRevenue / activeDays) : null,
      newCustomerROAS: r.spend > 0 && r.newCustomerRevenue > 0 ? r2g(r.newCustomerRevenue / r.spend) : null,
      // LTV fields (all-time, independent of reporting window)
      ltvAcquiredCustomers: ltv.ltvAcquiredCustomers || 0,
      avgLtv30: ltv.avgLtv30 ?? null,
      avgLtv90: ltv.avgLtv90 ?? null,
      avgLtv365: ltv.avgLtv365 ?? null,
      avgLtvAll: ltv.avgLtvAll ?? null,
      totalLtvAll: ltv.totalLtvAll ?? null,
      repeatRate: ltv.repeatRate ?? null,
      avgOrders: ltv.avgOrders ?? null,
      ltvCac: (ltv.avgLtv90 != null && r.newCustomerOrders > 0) ? r2g(ltv.avgLtv90 / (r.spend / r.newCustomerOrders)) : null,
      // Ad creative thumbnail (ad rows only - empty for campaign/adset).
      // productSetId tells the explorer this ad is a Dynamic Product Ad
      // so it can render a "D" badge in the thumbnail spot instead of the
      // generic initial-letter placeholder.
      thumbnailUrl: entityType === "ad" ? (adThumbMap[r.id]?.thumbnailUrl ?? null) : null,
      imageUrl: entityType === "ad" ? (adThumbMap[r.id]?.imageUrl ?? null) : null,
      productSetId: entityType === "ad" ? (adThumbMap[r.id]?.productSetId ?? null) : null,
    };
  });

  // currentAgg is now pre-computed and cached - just compute rows on the small aggregated output.
  const campaignRows = addAdAge(computeRows(currentAgg.campaign), "campaign");
  const adsetRows = addAdAge(computeRows(currentAgg.adset), "adset");
  const adRows = addAdAge(computeRows(currentAgg.ad), "ad");

  // ── Comparison + Previous period queries (run in parallel) ──
  // Re-use the shop-tz previous-period bounds we already computed at the top of the loader.
  const prevFrom = _prevFromRP;
  const prevTo = _prevToRP;

  // prevInsights: only used for daily chart (spend/impressions per day). groupBy = days-in-window rows.
  const prevInsights = await queryCached(
    `${shopDomain}:campPrevDailyChart:${prevFromKey}:${prevToKey}`,
    DEFAULT_TTL,
    () => db.dailyAdRollup.groupBy({
      by: ["date"],
      where: { shopDomain, date: { gte: prevFrom, lte: prevTo } },
      _sum: { spend: true, impressions: true },
    }),
  );

  // Comparison period. compareAgg is already fetched at the top of the loader
  // when hasComparison is true - use it directly rather than re-deriving.
  let compareTotals = null;
  if (hasComparison && compareFrom && compareTo && compareAgg) {
    const compAggregated = compareAgg.campaign || {};
    const compRows = computeRows(compAggregated);
    compareTotals = computeClientTotalsServer(compRows);
  }

  // ── Previous period (auto-computed for Performance Shift) ──
  const prevOrdersInRange = allOrders.filter(o => o.createdAt >= prevFrom && o.createdAt <= prevTo);
  const prevOrderIds = new Set(prevOrdersInRange.map(o => o.shopifyOrderId));
  const prevAttrs = attributions.filter(a => {
    if (a.confidence > 0) return prevOrderIds.has(a.shopifyOrderId);
    const match = a.shopifyOrderId.match(/(\d{4}-\d{2}-\d{2})/);
    if (!match) return false;
    return match[1] >= prevFromKey && match[1] <= prevToKey;
  });
  // prevAgg is cached (same pattern as currentAgg) - just compute rows on the small aggregated output.
  const prevCampaignRows = computeRows(prevAgg.campaign);
  const prevAdsetRows = computeRows(prevAgg.adset);
  const prevAdRows = computeRows(prevAgg.ad);

  // Count unique new Meta customers acquired in the previous period
  for (const [custId, acq] of Object.entries(customerAcq)) {
    if (acq.acquisitionDate >= prevFrom && acq.acquisitionDate <= prevTo) {
      prevUniqueNewMetaCustomers++;
    }
  }

  // Breakdown maps - keyed by entity ID, value is array of computed sub-rows
  const breakdownMaps = { campaign: {}, adset: {}, ad: {} };
  let hasBreakdownData = false;
  if (breakdown !== "none") {
    const breakdownType = breakdown === "platform" ? "publisher_platform"
      : breakdown === "placement" ? "platform_position"
      : breakdown === "age_gender" ? "age_gender"
      : breakdown;

    const breakdownData = await db.metaBreakdown.findMany({
      where: { shopDomain, breakdownType, date: { gte: fromDate, lte: toDate } },
    });

    if (breakdownData.length > 0) {
      hasBreakdownData = true;
      const r2 = (v) => Math.round(v * 100) / 100;

      // Build breakdown maps for each level
      for (const bdLevel of ["campaign", "adset", "ad"]) {
        const bdAgg = {};
        for (const bd of breakdownData) {
          const levelKey = bdLevel === "campaign" ? bd.campaignId
            : bdLevel === "adset" ? bd.adSetId
            : bd.adId;
          if (!levelKey) continue;

          const compoundKey = `${levelKey}||${bd.breakdownValue}`;
          if (!bdAgg[compoundKey]) {
            bdAgg[compoundKey] = {
              id: compoundKey,
              breakdownValue: bd.breakdownValue,
              levelKey,
              spend: 0, impressions: 0, clicks: 0,
              metaConversions: 0, metaConversionValue: 0,
              attributedOrders: 0, attributedRevenue: 0,
              newCustomerOrders: 0, existingCustomerOrders: 0,
              newCustomerRevenue: 0, existingCustomerRevenue: 0,
              unverifiedRevenue: 0,
              linkClicks: 0, landingPageViews: 0,
              viewContent: 0, addToCart: 0, initiateCheckout: 0,
              videoP25: 0, videoP50: 0, videoP75: 0, videoP100: 0,
              frequencySum: 0, frequencyCount: 0,
            };
          }
          const row = bdAgg[compoundKey];
          row.spend += bd.spend;
          row.impressions += bd.impressions;
          row.clicks += bd.clicks;
          row.metaConversions += bd.conversions;
          row.metaConversionValue += bd.conversionValue;
          row.linkClicks += bd.linkClicks;
          row.landingPageViews += bd.landingPageViews;
          row.viewContent += bd.viewContent;
          row.addToCart += bd.addToCart;
          row.initiateCheckout += bd.initiateCheckout;
        }

        // Build lookup: entityId+date → breakdown values with conversion shares
        const bdLookup = {};
        for (const bd of breakdownData) {
          const entityId = bdLevel === "campaign" ? bd.campaignId
            : bdLevel === "adset" ? bd.adSetId
            : bd.adId;
          if (!entityId) continue;
          const lookupKey = `${entityId}||${shopLocalDayKey(tz, bd.date)}`;
          if (!bdLookup[lookupKey]) bdLookup[lookupKey] = [];
          bdLookup[lookupKey].push({
            breakdownValue: bd.breakdownValue,
            conversions: bd.conversions,
            conversionValue: bd.conversionValue,
          });
        }

        const getWeights = (entityId, dateStr) => {
          const entries = bdLookup[`${entityId}||${dateStr}`];
          if (!entries || entries.length === 0) return null;
          const totalConv = entries.reduce((s, e) => s + e.conversions, 0);
          if (totalConv === 0) {
            const weight = 1 / entries.length;
            return entries.map(e => ({ breakdownValue: e.breakdownValue, weight }));
          }
          return entries.map(e => ({ breakdownValue: e.breakdownValue, weight: e.conversions / totalConv }));
        };

        // Distribute matched attributions
        const orderMap = {};
        for (const o of ordersInRange) orderMap[o.shopifyOrderId] = o;

        for (const attr of attrsInRange.filter(a => a.confidence > 0)) {
          const entityId = bdLevel === "campaign" ? attr.metaCampaignId
            : bdLevel === "adset" ? attr.metaAdSetId
            : attr.metaAdId;
          const order = orderMap[attr.shopifyOrderId];
          if (!order || !entityId) continue;
          const dateStr = shopLocalDayKey(tz, order.createdAt);
          const weights = getWeights(entityId, dateStr);
          if (!weights) continue;

          const rev = netPaidOf(order); // exchange-aware (was missing refund subtraction entirely)
          for (const { breakdownValue, weight } of weights) {
            const compoundKey = `${entityId}||${breakdownValue}`;
            const row = bdAgg[compoundKey];
            if (!row) continue;
            row.attributedOrders += weight;
            row.attributedRevenue += rev * weight;
            if (attr.isNewCustomer) {
              row.newCustomerOrders += weight;
              row.newCustomerRevenue += rev * weight;
            } else {
              row.existingCustomerOrders += weight;
              row.existingCustomerRevenue += rev * weight;
            }
          }
        }

        // Distribute unmatched attributions
        for (const attr of attrsInRange.filter(a => a.confidence === 0)) {
          const entityId = bdLevel === "campaign" ? attr.metaCampaignId
            : bdLevel === "adset" ? attr.metaAdSetId
            : attr.metaAdId;
          if (!entityId) continue;
          const dateMatch = attr.shopifyOrderId.match(/(\d{4}-\d{2}-\d{2})/);
          if (!dateMatch) continue;
          const weights = getWeights(entityId, dateMatch[1]);
          if (!weights) continue;

          for (const { breakdownValue, weight } of weights) {
            const compoundKey = `${entityId}||${breakdownValue}`;
            const row = bdAgg[compoundKey];
            if (!row) continue;
            row.unverifiedRevenue += (attr.metaConversionValue || 0) * weight;
          }
        }

        // Group by entity ID and compute final values
        const entityMap = {};
        for (const r of Object.values(bdAgg)) {
          if (!entityMap[r.levelKey]) entityMap[r.levelKey] = [];
          const spend = r2(r.spend);
          const attOrders = Math.round(r.attributedOrders);
          const attRev = r2(r.attributedRevenue);
          const newCustOrders = Math.round(r.newCustomerOrders);
          const newCustRev = r2(r.newCustomerRevenue);
          const unverRev = r2(r.unverifiedRevenue);
          entityMap[r.levelKey].push({
            ...r,
            _isBreakdownRow: true,
            spend,
            attributedOrders: attOrders,
            attributedRevenue: attRev,
            newCustomerOrders: newCustOrders,
            newCustomerRevenue: newCustRev,
            existingCustomerOrders: Math.round(r.existingCustomerOrders),
            existingCustomerRevenue: r2(r.existingCustomerRevenue),
            metaConversionValue: r2(r.metaConversionValue),
            unverifiedRevenue: unverRev,
            blendedROAS: spend > 0 ? r2((attRev + unverRev) / spend) : 0,
            ctr: r.impressions > 0 ? r2((r.clicks / r.impressions) * 100) : 0,
            cpa: attOrders > 0 ? r2(spend / attOrders) : 0,
            avgFrequency: (r.frequencyCount || 0) > 0 ? r2(r.frequencySum / r.frequencyCount) : 0,
            atcRate: r.viewContent > 0 ? r2((r.addToCart / r.viewContent) * 100) : 0,
            checkoutRate: r.addToCart > 0 ? r2((r.initiateCheckout / r.addToCart) * 100) : 0,
            purchaseRate: r.initiateCheckout > 0 ? r2((r.metaConversions / r.initiateCheckout) * 100) : 0,
            // Derived columns (same formulas as addAdAge, but no ad age or LTV at breakdown level)
            newCustomerCPA: newCustOrders > 0 ? r2(spend / newCustOrders) : null,
            revenuePerNewCustomer: newCustOrders > 0 ? r2(newCustRev / newCustOrders) : null,
            newCustomerROAS: spend > 0 && newCustRev > 0 ? r2(newCustRev / spend) : null,
            spendPerDay: null, newCustomersPerDay: null, newCustomerRevenuePerDay: null, adAgeDays: null,
            // LTV doesn't apply at breakdown level - it's per-customer, not per-country/platform
            ltvAcquiredCustomers: null, avgLtv30: null, avgLtv90: null, avgLtv365: null,
            avgLtvAll: null, totalLtvAll: null, ltvCac: null, repeatRate: null, avgOrders: null,
          });
        }
        // Sort sub-rows by spend desc within each entity
        for (const key of Object.keys(entityMap)) {
          entityMap[key].sort((a, b) => b.spend - a.spend);
        }
        breakdownMaps[bdLevel] = entityMap;
      }
    }
  }

  // ── Daily aggregation for summary charts ──
  // Pre-populate with every day in range so charts always show all days
  const emptyDay = (date: string) => ({ date, spend: 0, impressions: 0, attributedRevenue: 0, unverifiedRevenue: 0, newCustomerOrders: 0, newCustomerRevenue: 0, attributedOrders: 0, liveAds: 0 });
  const dailyMap: Record<string, any> = {};
  {
    // Iterate shop-local day keys fromKey..toKey inclusive.
    let key = fromKey;
    while (key <= toKey) {
      dailyMap[key] = emptyDay(key);
      key = addDaysKey(key, 1);
    }
  }
  // insights is now a groupBy result: { date, _sum: { spend, impressions } }
  for (const ins of insights as any[]) {
    const day = shopLocalDayKey(tz, ins.date);
    if (!dailyMap[day]) dailyMap[day] = { date: day, spend: 0, impressions: 0, attributedRevenue: 0, unverifiedRevenue: 0, newCustomerOrders: 0, newCustomerRevenue: 0, attributedOrders: 0, liveAds: 0 };
    dailyMap[day].spend += (ins._sum?.spend || 0);
    dailyMap[day].impressions += (ins._sum?.impressions || 0);
  }
  // Daily distinct live-ad counts. SQLite returns the count column as
  // BigInt; the chart renderer wants a plain number.
  for (const row of (dailyLiveAdsRaw as Array<{ date: Date; liveAds: number | bigint }> | null) || []) {
    const day = shopLocalDayKey(tz, row.date);
    if (!dailyMap[day]) dailyMap[day] = { date: day, spend: 0, impressions: 0, attributedRevenue: 0, unverifiedRevenue: 0, newCustomerOrders: 0, newCustomerRevenue: 0, attributedOrders: 0, liveAds: 0 };
    dailyMap[day].liveAds = Number(row.liveAds || 0);
  }
  // Build order lookup by date
  const ordersByDate = {};
  for (const o of ordersInRange) {
    ordersByDate[o.shopifyOrderId] = shopLocalDayKey(tz, o.createdAt);
  }
  for (const attr of attrsInRange) {
    if (attr.confidence > 0) {
      const order = orderMap[attr.shopifyOrderId];
      if (!order) continue;
      const day = shopLocalDayKey(tz, order.createdAt);
      if (!dailyMap[day]) dailyMap[day] = { date: day, spend: 0, impressions: 0, attributedRevenue: 0, unverifiedRevenue: 0, newCustomerOrders: 0, newCustomerRevenue: 0, attributedOrders: 0, liveAds: 0 };
      const rev = netPaidOf(order); // exchange-aware (was missing refund subtraction entirely)
      dailyMap[day].attributedRevenue += rev;
      dailyMap[day].attributedOrders += 1;
      if (attr.isNewCustomer) {
        dailyMap[day].newCustomerOrders += 1;
        dailyMap[day].newCustomerRevenue += rev;
      }
    } else {
      const dateMatch = attr.shopifyOrderId.match(/(\d{4}-\d{2}-\d{2})/);
      if (!dateMatch) continue;
      const day = dateMatch[1];
      if (!dailyMap[day]) dailyMap[day] = { date: day, spend: 0, impressions: 0, attributedRevenue: 0, unverifiedRevenue: 0, newCustomerOrders: 0, newCustomerRevenue: 0, attributedOrders: 0, liveAds: 0 };
      dailyMap[day].unverifiedRevenue += attr.metaConversionValue || 0;
    }
  }
  const dailyData = Object.values(dailyMap).sort((a, b) => a.date.localeCompare(b.date)).map(d => ({
    ...d,
    spend: Math.round(d.spend * 100) / 100,
    attributedRevenue: Math.round(d.attributedRevenue * 100) / 100,
    unverifiedRevenue: Math.round(d.unverifiedRevenue * 100) / 100,
    newCustomerRevenue: Math.round(d.newCustomerRevenue * 100) / 100,
  }));

  // ── Previous period daily data for chart overlay ──
  const prevDailyMap: Record<string, any> = {};
  {
    let key = prevFromKey;
    while (key <= prevToKey) {
      prevDailyMap[key] = emptyDay(key);
      key = addDaysKey(key, 1);
    }
  }
  for (const ins of prevInsights as any[]) {
    const day = shopLocalDayKey(tz, ins.date);
    if (!prevDailyMap[day]) prevDailyMap[day] = { date: day, spend: 0, impressions: 0, attributedRevenue: 0, unverifiedRevenue: 0, newCustomerOrders: 0, newCustomerRevenue: 0, attributedOrders: 0 };
    prevDailyMap[day].spend += (ins._sum?.spend || 0);
    prevDailyMap[day].impressions += (ins._sum?.impressions || 0);
  }
  for (const attr of prevAttrs) {
    if (attr.confidence > 0) {
      const order = orderMap[attr.shopifyOrderId];
      if (!order) continue;
      const day = shopLocalDayKey(tz, order.createdAt);
      if (!prevDailyMap[day]) prevDailyMap[day] = { date: day, spend: 0, impressions: 0, attributedRevenue: 0, unverifiedRevenue: 0, newCustomerOrders: 0, newCustomerRevenue: 0, attributedOrders: 0 };
      const rev = netPaidOf(order); // exchange-aware (was missing refund subtraction entirely)
      prevDailyMap[day].attributedRevenue += rev;
      prevDailyMap[day].attributedOrders += 1;
      if (attr.isNewCustomer) {
        prevDailyMap[day].newCustomerOrders += 1;
        prevDailyMap[day].newCustomerRevenue += rev;
      }
    } else {
      const dateMatch = attr.shopifyOrderId.match(/(\d{4}-\d{2}-\d{2})/);
      if (!dateMatch) continue;
      const day = dateMatch[1];
      if (!prevDailyMap[day]) prevDailyMap[day] = { date: day, spend: 0, impressions: 0, attributedRevenue: 0, unverifiedRevenue: 0, newCustomerOrders: 0, newCustomerRevenue: 0, attributedOrders: 0 };
      prevDailyMap[day].unverifiedRevenue += attr.metaConversionValue || 0;
    }
  }
  const prevDailyData = Object.values(prevDailyMap).sort((a, b) => a.date.localeCompare(b.date)).map(d => ({
    ...d,
    spend: Math.round(d.spend * 100) / 100,
    attributedRevenue: Math.round(d.attributedRevenue * 100) / 100,
    unverifiedRevenue: Math.round(d.unverifiedRevenue * 100) / 100,
    newCustomerRevenue: Math.round(d.newCustomerRevenue * 100) / 100,
  }));

  // ── Platform & Placement performance tiles (per level) ──
  const aggregateBreakdownByLevel = async (type: string) => {
    // Aggregate at each level: campaign, adset, ad (and overall)
    const levels = [
      { key: "overall", groupBy: ["breakdownValue"] as const },
      { key: "campaign", groupBy: ["breakdownValue", "campaignId", "campaignName"] as const },
      { key: "adset", groupBy: ["breakdownValue", "campaignId", "campaignName", "adSetId", "adSetName"] as const },
      { key: "ad", groupBy: ["breakdownValue", "campaignId", "campaignName", "adSetId", "adSetName", "adId", "adName"] as const },
    ];
    const result: Record<string, any[]> = {};
    for (const level of levels) {
      const raw = await db.metaBreakdown.groupBy({
        by: level.groupBy as any,
        where: { shopDomain, breakdownType: type, date: { gte: fromDate, lte: toDate } },
        _sum: { spend: true, impressions: true, clicks: true, conversions: true, conversionValue: true },
      });
      result[level.key] = raw.map((r: any) => {
        const spend = r._sum?.spend || 0;
        const conversions = r._sum?.conversions || 0;
        const revenue = r._sum?.conversionValue || 0;
        const entityName = level.key === "ad" ? (r.adName || r.adId || "Unknown")
          : level.key === "adset" ? (r.adSetName || r.adSetId || "Unknown")
          : level.key === "campaign" ? (r.campaignName || r.campaignId || "Unknown")
          : null;
        return {
          breakdownValue: r.breakdownValue,
          campaignId: r.campaignId || null,
          adSetId: r.adSetId || null,
          adId: r.adId || null,
          entityName,
          name: entityName ? `${r.breakdownValue} · ${entityName}` : r.breakdownValue,
          spend: r2g(spend),
          impressions: r._sum?.impressions || 0,
          clicks: r._sum?.clicks || 0,
          conversions,
          revenue: r2g(revenue),
          roas: spend > 0 ? r2g(revenue / spend) : 0,
          cpa: conversions > 0 ? r2g(spend / conversions) : 0,
          ctr: (r._sum?.impressions || 0) > 0 ? r2g(((r._sum?.clicks || 0) / (r._sum?.impressions || 0)) * 100) : 0,
        };
      }).sort((a, b) => b.conversions - a.conversions || b.spend - a.spend);
    }
    return result;
  };

  const [platformPerfRaw, placementPerfRaw] = await Promise.all([
    queryCached(
      `${shopDomain}:campBd:publisher_platform:${fromKey}:${toKey}`,
      DEFAULT_TTL,
      () => aggregateBreakdownByLevel("publisher_platform"),
    ),
    queryCached(
      `${shopDomain}:campBd:platform_position:${fromKey}:${toKey}`,
      DEFAULT_TTL,
      () => aggregateBreakdownByLevel("platform_position"),
    ),
  ]);

  // Build exact new customer counts per platform/placement from attribution data.
  // Each attribution has metaPlatform (e.g. "facebook") and metaPlacement (e.g. "instagram|feed")
  // stored directly, so we can get precise counts - no proportional allocation needed.
  const matchedAttrsInRange = attrsInRange.filter(a => a.confidence > 0 && a.isNewCustomer);
  const orderMapForEnrich: Record<string, any> = {};
  for (const o of ordersInRange) orderMapForEnrich[o.shopifyOrderId] = o;

  // Build lookup: breakdownType → key → newCustomerOrders count
  // key format varies by level: "breakdownValue", "breakdownValue|campaignId", etc.
  const buildNewCustMap = (breakdownType: "publisher_platform" | "platform_position") => {
    const attrField = breakdownType === "publisher_platform" ? "metaPlatform" : "metaPlacement";
    const maps: Record<string, Record<string, number>> = {
      overall: {}, campaign: {}, adset: {}, ad: {},
    };
    for (const attr of matchedAttrsInRange) {
      const bdValue = (attr as any)[attrField];
      if (!bdValue) continue;
      // Overall: just by breakdownValue
      maps.overall[bdValue] = (maps.overall[bdValue] || 0) + 1;
      // Campaign level
      if (attr.metaCampaignId) {
        const ck = `${bdValue}|${attr.metaCampaignId}`;
        maps.campaign[ck] = (maps.campaign[ck] || 0) + 1;
      }
      // Ad Set level
      if (attr.metaAdSetId) {
        const ak = `${bdValue}|${attr.metaAdSetId}`;
        maps.adset[ak] = (maps.adset[ak] || 0) + 1;
      }
      // Ad level
      if (attr.metaAdId) {
        const dk = `${bdValue}|${attr.metaAdId}`;
        maps.ad[dk] = (maps.ad[dk] || 0) + 1;
      }
    }
    return maps;
  };

  const platformNewCustMaps = buildNewCustMap("publisher_platform");
  const placementNewCustMaps = buildNewCustMap("platform_position");

  const enrichBreakdownWithNewCustomers = (breakdownData: Record<string, any[]>, newCustMaps: Record<string, Record<string, number>>) => {
    const levelIdFields: Record<string, string> = { campaign: "campaignId", adset: "adSetId", ad: "adId" };
    for (const levelKey of ["overall", "campaign", "adset", "ad"]) {
      const items = breakdownData[levelKey];
      if (!items) continue;
      for (const item of items) {
        let lookupKey: string;
        if (levelKey === "overall") {
          lookupKey = item.breakdownValue;
        } else {
          const entityId = item[levelIdFields[levelKey]];
          lookupKey = `${item.breakdownValue}|${entityId}`;
        }
        item.newCustomerOrders = newCustMaps[levelKey]?.[lookupKey] || 0;
        item.newCustomerCPA = item.newCustomerOrders > 0 ? r2g(item.spend / item.newCustomerOrders) : 0;
      }
    }
    return breakdownData;
  };

  const platformPerf = enrichBreakdownWithNewCustomers(platformPerfRaw, platformNewCustMaps);
  const placementPerf = enrichBreakdownWithNewCustomers(placementPerfRaw, placementNewCustMaps);

  // ── AI Insights cache ──
  const dateFromStr = fromKey;
  const dateToStr = toKey;
  const cached = await getCachedInsights(shopDomain, "campaigns", dateFromStr, dateToStr);
  const currentHash = computeDataHash({ campaignRows, dailyData, totalStoreRevenue, platformPerf, placementPerf, compareTotals });
  const aiCachedInsights = cached?.insights || null;
  const aiGeneratedAt = cached?.generatedAt?.toISOString() || null;
  const aiIsStale = cached ? cached.dataHash !== currentHash : false;

  // Meta change log events in the period - cheap query (tight index), cached
  // by date range. Rendered above the tile grid as a per-day activity strip
  // and surfaced per-entity via the timeline drawer.
  const metaChanges = await queryCached(
    `${shopDomain}:metaChanges:${fromKey}:${toKey}`,
    DEFAULT_TTL,
    () => db.metaChange.findMany({
      where: { shopDomain, eventTime: { gte: fromDate, lte: toDate } },
      orderBy: { eventTime: "asc" },
      select: {
        id: true, eventTime: true, category: true, rawEventType: true,
        objectType: true, objectId: true, objectName: true,
        actorName: true, actorId: true, summary: true,
      },
    }),
  );
  const changeEventsForStrip = metaChanges.map((c) => ({
    id: c.id,
    eventTimeISO: c.eventTime.toISOString(),
    category: c.category,
    objectType: c.objectType,
    objectName: c.objectName || "",
    summary: c.summary,
    rawEventType: c.rawEventType,
    actor: c.actorName || c.actorId || "",
  }));
  // Per-entity change counts keyed by objectId - used for Campaigns table badges.
  const changeCountsByObjectId: Record<string, number> = {};
  for (const c of metaChanges) {
    changeCountsByObjectId[c.objectId] = (changeCountsByObjectId[c.objectId] || 0) + 1;
  }

  // Ad Funnel Tree: stage → ad sets → ads, with targeting + status metadata.
  // Powers AdFunnelTreeTile - a vertical-band tree where every individual ad
  // sits at the funnel stage (cold/warm/hot) inferred from its parent ad set's
  // targeting spec. See classifyFunnelStage() in metaEntitySync.server.js for
  // the rules. UI exposes the audience-pool definitions explicitly so the
  // merchant can sanity-check the bucket each ad is in.
  const adsetEntityById: Record<string, any> = {};
  const adEntityById: Record<string, any> = {};
  for (const e of metaEntities) {
    if (e.entityType === "adset") adsetEntityById[e.entityId] = e;
    else if (e.entityType === "ad") adEntityById[e.entityId] = e;
  }

  const STAGE_KEYS = ["cold", "warm", "hot"] as const;
  type StageKey = typeof STAGE_KEYS[number];
  const funnelTree: Record<StageKey, any[]> = { cold: [], warm: [], hot: [] };
  const adsetByIdForTree = new Map<string, any>();

  for (const row of adsetRows) {
    if ((row.spend || 0) === 0 && (row.attributedRevenue || 0) === 0 && (row.attributedOrders || 0) === 0) continue;
    const entity = adsetEntityById[row.id];
    const stage: StageKey = (entity?.funnelStage && (STAGE_KEYS as readonly string[]).includes(entity.funnelStage))
      ? entity.funnelStage as StageKey : "cold";

    let audiences: string[] = [];
    let excludedAudiences: string[] = [];
    let targetingSummary = "";
    if (entity?.targetingSpec) {
      try {
        const t = JSON.parse(entity.targetingSpec);
        audiences = (t.custom_audiences || []).map((a: any) => a.name).filter(Boolean);
        excludedAudiences = (t.excluded_custom_audiences || []).map((a: any) => a.name).filter(Boolean);
        const bits: string[] = [];
        if (t.geo_locations?.countries?.length) bits.push(`Geo: ${t.geo_locations.countries.join(", ")}`);
        if (t.age_min || t.age_max) bits.push(`Age ${t.age_min || 13}–${t.age_max || 65}`);
        if (t.genders?.length) bits.push(t.genders.includes(1) ? (t.genders.includes(2) ? "All genders" : "Male") : (t.genders.includes(2) ? "Female" : ""));
        if (Array.isArray(t.flexible_spec) && t.flexible_spec.length) bits.push("Detailed targeting");
        if (t.targeting_optimization === "expansion_all" || t.targeting_relaxation_types) bits.push("Advantage+ relaxed");
        targetingSummary = bits.join(" · ");
      } catch {}
    }

    const adset = {
      id: row.id,
      name: row.name || row.id,
      campaignId: row.campaignId,
      campaignName: row.campaignName,
      stage,
      spend: row.spend || 0,
      newCustomers: row.newCustomerOrders || 0,
      revenue: row.attributedRevenue || 0,
      orders: row.attributedOrders || 0,
      impressions: row.impressions || 0,
      roas: (row.spend || 0) > 0 ? Math.round((row.attributedRevenue / row.spend) * 100) / 100 : 0,
      newCustomerCPA: row.newCustomerCPA ?? null,
      status: entity?.currentStatus || null,
      audiences, excludedAudiences, targetingSummary,
      ads: [] as any[],
    };
    adsetByIdForTree.set(row.id, adset);
    funnelTree[stage].push(adset);
  }

  for (const row of adRows) {
    if ((row.spend || 0) === 0 && (row.attributedRevenue || 0) === 0 && (row.attributedOrders || 0) === 0) continue;
    const adset = adsetByIdForTree.get(row.adSetId);
    if (!adset) continue;
    const entity = adEntityById[row.id];
    adset.ads.push({
      id: row.id,
      name: row.name || row.id,
      spend: row.spend || 0,
      newCustomers: row.newCustomerOrders || 0,
      revenue: row.attributedRevenue || 0,
      orders: row.attributedOrders || 0,
      impressions: row.impressions || 0,
      ctr: row.ctr || 0,
      roas: (row.spend || 0) > 0 ? Math.round((row.attributedRevenue / row.spend) * 100) / 100 : 0,
      newCustomerCPA: row.newCustomerCPA ?? null,
      status: entity?.currentStatus || null,
      ageDays: row.adAgeDays ?? null,
    });
  }

  const stageTotals: Record<StageKey, any> = { cold: {}, warm: {}, hot: {} };
  for (const stage of STAGE_KEYS) {
    const adsets = funnelTree[stage];
    adsets.sort((a, b) => b.spend - a.spend);
    for (const adset of adsets) adset.ads.sort((a: any, b: any) => b.spend - a.spend);
    stageTotals[stage] = {
      spend: adsets.reduce((s, a) => s + a.spend, 0),
      newCustomers: adsets.reduce((s, a) => s + a.newCustomers, 0),
      revenue: adsets.reduce((s, a) => s + a.revenue, 0),
      orders: adsets.reduce((s, a) => s + a.orders, 0),
      adsetCount: adsets.length,
      adCount: adsets.reduce((s, a) => s + a.ads.length, 0),
    };
  }

  // ── Top-of-page summary tiles ──
  // Pre-compute the 4 headline tile values here so the loader payload
  // contains everything the page needs. We can't compute the "poorest ad"
  // statistically on the client without re-deriving percentiles, so we do
  // it once on the server.
  // Live ads = distinct ads that delivered (spend>0 OR impressions>0) on
  // the most recent day in the window. We previously gated on
  // currentStatus === "ACTIVE" but Meta's effective_status field returns
  // a long list of "live but qualified" values (CAMPAIGN_PAUSED,
  // ADSET_PAUSED, IN_PROCESS, WITH_ISSUES, etc) and the tile was stuck
  // at 0 for accounts where no ad came back as exactly "ACTIVE". The
  // behavioural definition is more honest and lines up with the
  // sparkline below.
  const sortedDailyData = (dailyData as any[]).slice().sort((a, b) => a.date.localeCompare(b.date));
  const lastDay = sortedDailyData[sortedDailyData.length - 1];
  const liveAdCount = lastDay ? Number(lastDay.liveAds || 0) : 0;
  const pickAdSummary = (row: any) => row && {
    id: row.id, name: row.name,
    thumbnailUrl: row.thumbnailUrl, imageUrl: row.imageUrl,
    productSetId: row.productSetId || null,
    spend: row.spend, revenue: (row.attributedRevenue || 0) + (row.unverifiedRevenue || 0),
    newCustomerOrders: row.newCustomerOrders || 0,
    newCustomerRevenue: row.newCustomerRevenue || 0,
    newCustomerCPA: row.newCustomerCPA, newCustomerROAS: row.newCustomerROAS,
    roas: row.spend > 0 ? ((row.attributedRevenue || 0) + (row.unverifiedRevenue || 0)) / row.spend : 0,
  };
  // Top revenue ad: blended (matched + unverified) - matches what
  // Total Ad Revenue tile reports, so the headline number is consistent.
  const topRevenueAdRow = (adRows as any[]).slice().sort((a, b) =>
    ((b.attributedRevenue || 0) + (b.unverifiedRevenue || 0)) -
    ((a.attributedRevenue || 0) + (a.unverifiedRevenue || 0))
  )[0] || null;
  const topNewCustAdRow = (adRows as any[]).slice().sort((a, b) =>
    (b.newCustomerOrders || 0) - (a.newCustomerOrders || 0)
  )[0] || null;
  // Worst-performing ad: must be (a) currently LIVE and (b) statistically
  // significant - require spend in the upper half of all live spending ads
  // (so a £5 dud doesn't top the list). Live = delivered (spend>0 OR
  // impressions>0) on the most recent day in the window. Same behavioural
  // definition as the Live Ads tile.
  const liveAdIds: Set<string> = new Set();
  if (lastDay?.date) {
    const lastDayDate = lastDay.date instanceof Date ? lastDay.date : new Date(lastDay.date);
    const lastDayStart = new Date(lastDayDate); lastDayStart.setUTCHours(0, 0, 0, 0);
    const lastDayEnd = new Date(lastDayDate); lastDayEnd.setUTCHours(23, 59, 59, 999);
    const liveAdRows = await queryCached(
      `${shopDomain}:campLiveAdIds:${lastDayDate.toISOString().slice(0, 10)}`,
      DEFAULT_TTL,
      () => db.$queryRaw<Array<{ adId: string | null }>>`
        SELECT DISTINCT adId FROM DailyAdRollup
        WHERE shopDomain = ${shopDomain}
          AND date >= ${lastDayStart} AND date <= ${lastDayEnd}
          AND (spend > 0 OR impressions > 0)
          AND adId IS NOT NULL
      `,
    );
    for (const r of liveAdRows) if (r.adId) liveAdIds.add(String(r.adId));
  }
  const spendingAds = (adRows as any[]).filter(r => (r.spend || 0) > 0 && liveAdIds.has(String(r.id)));
  const sortedSpend = spendingAds.map(r => r.spend).sort((a, b) => a - b);
  const spendThreshold = sortedSpend.length > 0
    ? sortedSpend[Math.floor(sortedSpend.length / 2)]
    : 0;
  const worstAdCandidates = spendingAds.filter(r => (r.spend || 0) >= spendThreshold);
  let worstAdRow: any = null;
  let worstAdScore = -Infinity;
  for (const r of worstAdCandidates) {
    // Higher score = worse. CPA dominates when there are orders, otherwise
    // we score by spend × (1 / max(roas, 0.01)) so high-spend zero-return
    // ads still rank.
    const cpa = r.newCustomerCPA;
    const roas = r.spend > 0 ? ((r.attributedRevenue || 0) + (r.unverifiedRevenue || 0)) / r.spend : 0;
    const score = cpa != null
      ? cpa * Math.log(1 + r.spend)
      : r.spend / Math.max(roas, 0.01);
    if (score > worstAdScore) {
      worstAdScore = score;
      worstAdRow = r;
    }
  }

  const topTiles = {
    liveAdCount,
    topRevenueAd: pickAdSummary(topRevenueAdRow),
    topNewCustomerAd: pickAdSummary(topNewCustAdRow),
    worstAd: pickAdSummary(worstAdRow),
  };

  // Roll up the per-day demographic rows into one slice per (adId, gender, age)
  // for the selected period. AdExplorerTable applies gender + age filters
  // against this map and recomputes the row's order/revenue numbers from the
  // matching demographic slices when at least one filter is active.
  const adDemographicsByAd: Record<string, Array<{
    gender: string; ageBracket: string;
    attributedOrders: number; attributedRevenue: number;
    newCustomerOrders: number; newCustomerRevenue: number;
    existingCustomerOrders: number; existingCustomerRevenue: number;
  }>> = {};
  {
    const byKey = new Map<string, any>();
    for (const r of (adDemographicsRaw as any[]) || []) {
      const key = `${r.adId}|${r.gender}|${r.ageBracket}`;
      let agg = byKey.get(key);
      if (!agg) {
        agg = {
          adId: r.adId, gender: r.gender, ageBracket: r.ageBracket,
          attributedOrders: 0, attributedRevenue: 0,
          newCustomerOrders: 0, newCustomerRevenue: 0,
          existingCustomerOrders: 0, existingCustomerRevenue: 0,
        };
        byKey.set(key, agg);
      }
      agg.attributedOrders += r.attributedOrders;
      agg.attributedRevenue += r.attributedRevenue;
      agg.newCustomerOrders += r.newCustomerOrders;
      agg.newCustomerRevenue += r.newCustomerRevenue;
      agg.existingCustomerOrders += r.existingCustomerOrders;
      agg.existingCustomerRevenue += r.existingCustomerRevenue;
    }
    for (const agg of byKey.values()) {
      const list = adDemographicsByAd[agg.adId] || (adDemographicsByAd[agg.adId] = []);
      list.push({
        gender: agg.gender, ageBracket: agg.ageBracket,
        attributedOrders: agg.attributedOrders, attributedRevenue: agg.attributedRevenue,
        newCustomerOrders: agg.newCustomerOrders, newCustomerRevenue: agg.newCustomerRevenue,
        existingCustomerOrders: agg.existingCustomerOrders, existingCustomerRevenue: agg.existingCustomerRevenue,
      });
    }
  }

  console.log(`[campaigns] total ${Date.now() - _t0}ms`);

  return json({
    breakdown, campaignRows, adsetRows, adRows,
    prevCampaignRows, prevAdsetRows, prevAdRows,
    breakdownMaps, hasBreakdownData,
    compareTotals, compareLabel, hasComparison, currencySymbol,
    reportingPeriodDays, dailyData, prevDailyData, totalStoreRevenue,
    platformPerf, placementPerf,
    aiCachedInsights, aiGeneratedAt, aiIsStale,
    uniqueNewMetaCustomers, prevUniqueNewMetaCustomers, totalNewCustomersInPeriod,
    shopDomain,
    fromKey, toKey, preset,
    changeEvents: changeEventsForStrip,
    changeCountsByObjectId,
    funnelTree,
    stageTotals,
    topTiles,
    adDemographicsByAd,
  });
};

export const action = async ({ request }) => {
  const { session } = await authenticate.admin(request);
  const shopDomain = session.shop;
  const formData = await request.formData();
  const actionType = formData.get("actionType");

  if (actionType === "generateInsights") {
    const pageKey = String(formData.get("pageKey"));
    const taskId = `ai:${pageKey}:${shopDomain}`;
    const customSystem = formData.get("customSystemPrompt");
    const customPage = formData.get("customPagePrompt");
    const promptOverrides = (customSystem || customPage) ? { system: customSystem ? String(customSystem) : null, page: customPage ? String(customPage) : null } : null;

    setProgress(taskId, { status: "running", message: "Generating AI insights..." });

    // Fire and forget - build data and generate in background
    (async () => {
      try {
        const shop = await db.shop.findUnique({ where: { shopDomain } });
        const tz = shop?.shopifyTimezone || "UTC";
        const { fromDate, toDate, fromKey: dateFromStr, toKey: dateToStr } = parseDateRange(request, tz);
        const cs = currencySymbolFromCode(shop?.shopifyCurrency);

        const reportingPeriodDays = Math.max(1, Math.ceil((toDate.getTime() - fromDate.getTime()) / 86400000));

        // Fetch raw data for AI analysis (select only needed fields)
        const insights = await db.metaInsight.findMany({
          where: { shopDomain, date: { gte: fromDate, lte: toDate } },
          select: {
            campaignId: true, campaignName: true, adSetId: true, adSetName: true,
            adId: true, adName: true, spend: true, impressions: true, clicks: true,
            conversions: true, conversionValue: true, reach: true, frequency: true,
            linkClicks: true, landingPageViews: true, viewContent: true,
            addToCart: true, initiateCheckout: true,
          },
        });
        const attributions = await db.attribution.findMany({ where: { shopDomain } });
        const orders = await db.order.findMany({
          where: { shopDomain, isOnlineStore: true },
          select: { shopifyOrderId: true, createdAt: true, frozenTotalPrice: true, totalRefunded: true, netPaid: true },
        });

        // Build order lookup
        const orderMap = {};
        for (const o of orders) orderMap[o.shopifyOrderId] = o;

        // Aggregate campaign rows (simplified version for AI prompt)
        const campaignAgg = {};
        for (const ins of insights) {
          const key = ins.campaignId;
          if (!campaignAgg[key]) {
            campaignAgg[key] = {
              entityName: ins.campaignName || key, spend: 0, impressions: 0, clicks: 0,
              attributedOrders: 0, attributedRevenue: 0,
              newCustomerOrders: 0, existingCustomerOrders: 0,
              newCustomerRevenue: 0, existingCustomerRevenue: 0,
              frequencySum: 0, frequencyCount: 0,
            };
          }
          const c = campaignAgg[key];
          c.spend += ins.spend;
          c.impressions += ins.impressions;
          c.clicks += ins.clicks;
          if (ins.frequency) { c.frequencySum += ins.frequency; c.frequencyCount++; }
        }

        // Enrich with attribution data
        for (const a of attributions) {
          if (a.confidence === 0) continue;
          const order = orderMap[a.shopifyOrderId];
          if (!order || order.createdAt < fromDate || order.createdAt > toDate) continue;
          const camp = campaignAgg[a.metaCampaignId];
          if (!camp) continue;
          const rev = netPaidOf(order); // exchange-aware
          camp.attributedOrders++;
          camp.attributedRevenue += rev;
          if (a.isNewCustomer) { camp.newCustomerOrders++; camp.newCustomerRevenue += rev; }
          else { camp.existingCustomerOrders++; camp.existingCustomerRevenue += rev; }
        }

        const r2 = (v) => Math.round(v * 100) / 100;
        const campaignRows = Object.values(campaignAgg)
          .sort((a, b) => b.spend - a.spend)
          .map(c => ({
            ...c,
            spend: r2(c.spend), attributedRevenue: r2(c.attributedRevenue),
            newCustomerRevenue: r2(c.newCustomerRevenue),
            roas: c.spend > 0 ? r2(c.attributedRevenue / c.spend) : 0,
            cpa: c.attributedOrders > 0 ? r2(c.spend / c.attributedOrders) : 0,
            newCustomerCPA: c.newCustomerOrders > 0 ? r2(c.spend / c.newCustomerOrders) : 0,
            newCustomerROAS: c.spend > 0 && c.newCustomerRevenue > 0 ? r2(c.newCustomerRevenue / c.spend) : 0,
            avgFrequency: c.frequencyCount > 0 ? r2(c.frequencySum / c.frequencyCount) : 0,
            ctr: c.impressions > 0 ? r2((c.clicks / c.impressions) * 100) : 0,
          }));

        // Daily data for trend.
        // MetaInsight.date is stored as UTC-midnight of a shop-local day
        // (see metaSync.server.js). Taking the day-portion directly yields
        // the correct shop-local key and avoids a wrong tz round-trip for
        // shops at negative UTC offsets (LA etc).
        const dailyMap = {};
        for (const ins of insights) {
          const d = typeof ins.date === "string" ? ins.date.slice(0, 10) : ins.date.toISOString().slice(0, 10);
          if (!dailyMap[d]) dailyMap[d] = { date: d, spend: 0, attributedRevenue: 0, attributedOrders: 0, newCustomerOrders: 0, newCustomerRevenue: 0 };
          dailyMap[d].spend += ins.spend;
        }
        const dailyData = Object.values(dailyMap).sort((a, b) => a.date.localeCompare(b.date));

        // Total store revenue - net of refunds so it's comparable to
        // attributed revenue on the same page.
        const ordersInRange = orders.filter(o => o.createdAt >= fromDate && o.createdAt <= toDate);
        const totalStoreRevenue = ordersInRange.reduce(
          (sum, o) => sum + netPaidOf(o), // exchange-aware
          0,
        );

        const pageData = {
          campaignRows,
          prevCampaignRows: [],
          compareTotals: null,
          platformPerf: {},
          placementPerf: {},
          dailyData,
          totalStoreRevenue,
          reportingPeriodDays,
        };

        await generateInsights(shopDomain, pageKey, pageData, dateFromStr, dateToStr, cs, promptOverrides);
        completeProgress(taskId, { success: true });
      } catch (err) {
        console.error("[AI] Campaign insights failed:", err);
        failProgress(taskId, err);
      }
    })();

    return json({ aiTaskId: taskId });
  }

  return json({});
};

function computeClientTotalsServer(rows) {
  return rows.reduce((acc, r) => ({
    spend: acc.spend + r.spend,
    impressions: acc.impressions + r.impressions,
    clicks: acc.clicks + r.clicks,
    metaConversions: acc.metaConversions + (r.metaConversions || 0),
    metaConversionValue: acc.metaConversionValue + (r.metaConversionValue || 0),
    attributedOrders: acc.attributedOrders + (r.attributedOrders || 0),
    attributedRevenue: acc.attributedRevenue + (r.attributedRevenue || 0),
    newCustomerRevenue: acc.newCustomerRevenue + (r.newCustomerRevenue || 0),
    existingCustomerRevenue: acc.existingCustomerRevenue + (r.existingCustomerRevenue || 0),
    unverifiedRevenue: acc.unverifiedRevenue + (r.unverifiedRevenue || 0),
    newCustomerOrders: acc.newCustomerOrders + (r.newCustomerOrders || 0),
    existingCustomerOrders: acc.existingCustomerOrders + (r.existingCustomerOrders || 0),
  }), {
    spend: 0, impressions: 0, clicks: 0,
    metaConversions: 0, metaConversionValue: 0,
    attributedOrders: 0, attributedRevenue: 0, unverifiedRevenue: 0,
    newCustomerRevenue: 0, existingCustomerRevenue: 0,
    newCustomerOrders: 0, existingCustomerOrders: 0,
  });
}

const BREAKDOWN_LABELS = {
  none: "Breakdown",
  country: "Country",
  platform: "Platform",
  placement: "Placement",
  age: "Age",
  gender: "Gender",
  age_gender: "Age + Gender",
};

const tileGridStyles = ``;


// ── Infographic Subcomponents ──

const LEVEL_OPTIONS = [
  { id: "campaign", label: "Campaigns" },
  { id: "adset", label: "Ad Sets" },
  { id: "ad", label: "Ads" },
];

function SmallToggle({ options, selected, onChange }: {
  options: { id: string; label: string }[];
  selected: string;
  onChange: (id: string) => void;
}) {
  return (
    <div style={{ display: "inline-flex", gap: "4px" }}>
      {options.map(o => (
        <button
          key={o.id}
          onClick={() => onChange(o.id)}
          className={`l-pill${selected === o.id ? " l-pill--active" : ""}`}
          style={{ padding: "4px 10px", fontSize: "11px" }}
        >{o.label}</button>
      ))}
    </div>
  );
}

// Bigger themed toggle for primary tab-style switches (Campaigns/Ad Sets/Ads).
// Matches the .toggle-group/.toggle-btn cyan accent used elsewhere in the app
// (Customers, Geo, etc.) so the visual language stays consistent across pages.
function BigLevelToggle({ options, selected, onChange }: {
  options: { id: string; label: string }[];
  selected: string;
  onChange: (id: string) => void;
}) {
  return (
    <div style={{ display: "inline-flex", gap: "4px" }}>
      {options.map(o => (
        <button
          key={o.id}
          onClick={() => onChange(o.id)}
          className={`l-pill${selected === o.id ? " l-pill--active" : ""}`}
          style={{ padding: "7px 16px", fontSize: "var(--l-font-base)" }}
        >{o.label}</button>
      ))}
    </div>
  );
}

function SmallLinks({ options, selected, onChange }: {
  options: { id: string; label: string }[];
  selected: string;
  onChange: (id: string) => void;
}) {
  return (
    <div style={{ display: "flex", gap: "10px" }}>
      {options.map(o => (
        <button
          key={o.id}
          onClick={() => onChange(o.id)}
          style={{
            background: "none", border: "none", padding: "0 0 1px 0", cursor: "pointer",
            fontSize: "11px", fontWeight: selected === o.id ? 600 : 400,
            color: selected === o.id ? "#6B21A8" : "#8c9196",
            borderBottom: selected === o.id ? "1.5px solid #6B21A8" : "none",
          }}
        >{o.label}</button>
      ))}
    </div>
  );
}

const RANKING_METRICS = [
  { id: "blendedROAS", label: "ROAS", higherIsBetter: true, description: "Return on ad spend - total confirmed revenue (matched + unmatched) divided by spend. Higher is better." },
  { id: "newCustomerCPA", label: "New Customer CPA", higherIsBetter: false, description: "Cost to acquire each new customer - total spend divided by number of first-time buyers. Lower is better." },
  { id: "avgLtvAll", label: "LTV", higherIsBetter: true, description: "Average all-time revenue per customer acquired by this ad, across every order they've ever placed. Higher means more valuable customers." },
  { id: "ltvCac", label: "LTV:CAC", higherIsBetter: true, description: "Lifetime value vs acquisition cost - how much lifetime revenue each acquired customer generates relative to what it cost to acquire them. Above 3x is strong." },
  { id: "ctr", label: "CTR", higherIsBetter: true, description: "Click-through rate - percentage of people who clicked after seeing the ad. Higher means more engaging creative." },
];

function formatMetricValue(metricId: string, value: number, cs: string): string {
  if (value == null) return "-";
  switch (metricId) {
    case "newCustomerOrders": case "attributedOrders": return value.toLocaleString();
    case "blendedROAS": case "ltvCac": case "newCustomerROAS": return `${value}x`;
    case "ctr": case "purchaseRate": case "atcRate": case "checkoutRate": return `${value}%`;
    case "avgFrequency": return `${value}x`;
    default: return `${cs}${value.toLocaleString()}`;
  }
}

// Smooth gradient color based on metric value
function metricGradientColor(metricId: string, value: number, allValues: number[], higherIsBetter: boolean): string {
  if (value <= 0) return "#9E9E9E"; // grey for no data

  // ROAS / LTV:CAC - absolute thresholds with smooth gradient
  if (metricId === "blendedROAS" || metricId === "newCustomerROAS" || metricId === "ltvCac") {
    // 0→2 = red zone, 2→3 = orange zone, 3→5+ = green zone
    // Map to 0-1 score then to hue 0°(red) → 120°(green)
    const clamped = Math.min(Math.max(value, 0), 6);
    let score: number;
    if (clamped <= 2) score = (clamped / 2) * 0.33; // 0-0.33
    else if (clamped <= 3) score = 0.33 + ((clamped - 2) / 1) * 0.34; // 0.33-0.67
    else score = 0.67 + ((clamped - 3) / 3) * 0.33; // 0.67-1.0
    const hue = score * 120;
    return `hsl(${Math.round(hue)}, 72%, ${33 + Math.round(score * 12)}%)`;
  }

  // CPA - absolute thresholds, lower is better
  if (metricId === "cpa" || metricId === "newCustomerCPA") {
    // <£30 = green, £30-80 = orange, >£80 = red
    const clamped = Math.min(Math.max(value, 0), 150);
    let score: number;
    if (clamped <= 30) score = 1 - (clamped / 30) * 0.33; // 1.0-0.67
    else if (clamped <= 80) score = 0.67 - ((clamped - 30) / 50) * 0.34; // 0.67-0.33
    else score = 0.33 - ((clamped - 80) / 70) * 0.33; // 0.33-0.0
    score = Math.max(0, Math.min(1, score));
    const hue = score * 120;
    return `hsl(${Math.round(hue)}, 72%, ${33 + Math.round(score * 12)}%)`;
  }

  // Relative metrics - use data range for gradient
  const valid = allValues.filter(v => v > 0);
  if (valid.length < 2) return "#E65100";
  const min = Math.min(...valid);
  const max = Math.max(...valid);
  const range = max - min || 1;
  let score = (value - min) / range;
  if (!higherIsBetter) score = 1 - score;
  score = Math.max(0, Math.min(1, score));
  const hue = score * 120;
  return `hsl(${Math.round(hue)}, 72%, ${33 + Math.round(score * 12)}%)`;
}

function SortableHeader({ col, label, width, sortCol, sortDir, onSort, tooltip }: {
  col: string; label: string; width: string; sortCol: string; sortDir: "asc" | "desc"; onSort: (col: string) => void;
  tooltip?: string;
}) {
  const active = sortCol === col;
  return (
    <span
      title={tooltip}
      style={{ fontSize: "10px", fontWeight: 600, textTransform: "uppercase", textAlign: "right", flexShrink: 0,
        width, color: active ? "#7c3aed" : "#8c9196", cursor: "pointer", userSelect: "none" }}
      onClick={() => onSort(col)}
    >
      {label}{active ? (sortDir === "desc" ? " ↓" : " ↑") : ""}
    </span>
  );
}

function BestToWorstList({ rows, cs, entityType, onEntityClick }: {
  rows: any[]; cs: string;
  entityType?: "campaign" | "adset" | "ad";
  onEntityClick?: (id: string, name: string) => void;
}) {
  const [sortCol, setSortCol] = useState("newCustomerOrders");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc");
  const toggleSort = (col: string) => {
    if (sortCol === col) setSortDir(d => d === "desc" ? "asc" : "desc");
    else { setSortCol(col); setSortDir(col === "blendedCPA" || col === "newCustomerCPA" ? "asc" : "desc"); }
  };

  // Hover sparkline state - same lazy-fetch pattern as the Ad Age tile.
  // Cache is keyed by entity id (refetches if entityType changes via the
  // SmallToggle since the underlying tile remounts the list).
  const [hoverId, setHoverId] = useState<string | null>(null);
  const [hoverAnchor, setHoverAnchor] = useState<{ x: number; y: number } | null>(null);
  const [seriesCache, setSeriesCache] = useState<Record<string, Array<{ date: string; spend: number; revenue: number; orders: number; newCustomerOrders: number }> | "loading" | "error">>({});
  const requestSeries = (id: string) => {
    if (!entityType) return;
    if (seriesCache[id]) return;
    setSeriesCache((prev) => ({ ...prev, [id]: "loading" }));
    fetch(`/app/api/entity-timeline?type=${entityType}&id=${encodeURIComponent(id)}`)
      .then((r) => r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`)))
      .then((d) => setSeriesCache((prev) => ({ ...prev, [id]: d.daily || [] })))
      .catch(() => setSeriesCache((prev) => ({ ...prev, [id]: "error" })));
  };
  // Map sortCol → daily metric extractor + label + colour.
  const metricFor = (sortCol: string) => {
    switch (sortCol) {
      case "spend":              return { label: "Spend",        get: (d: any) => d.spend, fmt: (v: number) => `${cs}${Math.round(v).toLocaleString()}`, color: "#5C6AC4", lower: false };
      case "attributedOrders":   return { label: "Orders",       get: (d: any) => d.orders, fmt: (v: number) => Math.round(v).toLocaleString(), color: "#0E7490", lower: false };
      case "newCustomerOrders":  return { label: "New customers", get: (d: any) => d.newCustomerOrders, fmt: (v: number) => Math.round(v).toLocaleString(), color: "#6366F1", lower: false };
      case "blendedROAS":        return { label: "ROAS",         get: (d: any) => d.spend > 0 ? d.revenue / d.spend : 0, fmt: (v: number) => `${v.toFixed(2)}x`, color: "#059669", lower: false };
      case "blendedCPA":         return { label: "CPA",          get: (d: any) => d.orders > 0 ? d.spend / d.orders : 0, fmt: (v: number) => `${cs}${Math.round(v).toLocaleString()}`, color: "#D97706", lower: true };
      case "newCustomerCPA":     return { label: "New cust CPA", get: (d: any) => d.newCustomerOrders > 0 ? d.spend / d.newCustomerOrders : 0, fmt: (v: number) => `${cs}${Math.round(v).toLocaleString()}`, color: "#B45309", lower: true };
      default:                   return { label: "Spend", get: (d: any) => d.spend, fmt: (v: number) => `${cs}${Math.round(v).toLocaleString()}`, color: "#5C6AC4", lower: false };
    }
  };

  const sorted = [...rows].sort((a, b) => {
    const aVal = a[sortCol] || 0, bVal = b[sortCol] || 0;
    if (aVal === 0 && bVal === 0) return (b.spend || 0) - (a.spend || 0);
    if (aVal === 0) return 1;
    if (bVal === 0) return -1;
    return sortDir === "desc" ? bVal - aVal : aVal - bVal;
  });

  if (sorted.length === 0) return <div style={{ color: "#999", fontSize: "13px", padding: "8px 0" }}>No data</div>;

  const LOWER_IS_BETTER = new Set(["blendedCPA", "newCustomerCPA"]);
  const higherIsBetter = !LOWER_IS_BETTER.has(sortCol);
  const barValues = sorted.map(r => r[sortCol] || 0);
  const maxBarVal = Math.max(...barValues.filter(v => v > 0), 0.01);
  const colStyle = { fontSize: "12.5px", textAlign: "right" as const, flexShrink: 0 };

  const COLS = [
    { key: "attributedOrders", label: "Orders", width: "52px" },
    { key: "newCustomerOrders", label: "New", width: "40px" },
    { key: "spend", label: "Spend", width: "68px" },
    { key: "blendedCPA", label: "CPA", width: "60px" },
    { key: "newCustomerCPA", label: "NC CPA", width: "64px" },
    { key: "blendedROAS", label: "ROAS", width: "56px" },
  ];

  return (
    <div>
      <div style={{ display: "flex", gap: "10px", alignItems: "center", padding: "0 0 8px 0", borderBottom: "1px solid #e4e5e7", marginBottom: "8px" }}>
        <span style={{ width: "22px", flexShrink: 0 }} />
        <span style={{ flex: 1, fontSize: "11px", color: "#8c9196", fontWeight: 600, textTransform: "uppercase" }}>Name</span>
        {COLS.map(c => <SortableHeader key={c.key} col={c.key} label={c.label} width={c.width} sortCol={sortCol} sortDir={sortDir} onSort={toggleSort} />)}
      </div>
      <div style={{ maxHeight: "264px", overflowY: "auto", display: "flex", flexDirection: "column", gap: "8px", paddingRight: "4px" }}>
        {sorted.map((item, i) => {
          const orders = item.attributedOrders || 0;
          const newCusts = item.newCustomerOrders || 0;
          const spend = item.spend || 0;
          const roas = item.blendedROAS || 0;
          const barVal = item[sortCol] || 0;
          const barW = barVal > 0 && maxBarVal > 0 ? (barVal / maxBarVal) * 100 : 0;
          const barColor = metricGradientColor(sortCol, barVal, barValues, higherIsBetter);
          const interactive = !!item.id && !!entityType;
          return (
            <div
              key={item.id || i}
              onMouseEnter={(e) => {
                if (!interactive) return;
                setHoverId(item.id);
                const rect = (e.currentTarget as HTMLDivElement).getBoundingClientRect();
                const POP_W = 240, GAP = 8;
                const wantRight = rect.right + GAP + POP_W;
                const x = wantRight <= window.innerWidth
                  ? rect.right + GAP
                  : Math.max(GAP, rect.left - GAP - POP_W);
                setHoverAnchor({ x, y: rect.top + rect.height / 2 });
                requestSeries(item.id);
              }}
              onMouseLeave={() => { setHoverId(null); setHoverAnchor(null); }}
              onClick={() => interactive && onEntityClick?.(item.id, item.name)}
              title={interactive ? "Click for full timeline" : undefined}
              style={{
                display: "flex", alignItems: "center", gap: "10px",
                cursor: interactive ? "pointer" : "default",
                padding: "2px 4px", borderRadius: 4,
                transition: "background 0.12s ease",
              }}
              onMouseOver={(e) => { if (interactive) (e.currentTarget as HTMLDivElement).style.background = "#f1f5f9"; }}
              onMouseOut={(e) => { (e.currentTarget as HTMLDivElement).style.background = "transparent"; }}
            >
              <span style={{ fontSize: "12px", color: "#999", width: "22px", textAlign: "right", flexShrink: 0 }}>{i + 1}</span>
              <div style={{ flex: 1, minWidth: 0 }}>
                <div style={{ display: "flex", alignItems: "center", gap: "8px", marginBottom: "4px" }}>
                  <span style={{ fontSize: "13px", fontWeight: 500, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", flex: 1, minWidth: 0 }} title={item.name}>{item.name}</span>
                </div>
                <div style={{ height: "5px", background: "#f1f2f4", borderRadius: "2.5px", overflow: "hidden" }}>
                  <div style={{ height: "100%", width: `${barW}%`, background: barColor, borderRadius: "2.5px", transition: "width 0.3s", opacity: 0.8 }} />
                </div>
              </div>
              <span style={{ ...colStyle, width: "52px", color: orders > 0 ? "#444" : "#bbb" }}>{orders}</span>
              <span style={{ ...colStyle, width: "40px", color: newCusts > 0 ? "#444" : "#bbb" }}>{newCusts}</span>
              <span style={{ ...colStyle, width: "68px", color: spend > 0 ? "#444" : "#bbb" }}>{cs}{Math.round(spend).toLocaleString()}</span>
              <span style={{ ...colStyle, width: "60px", color: item.blendedCPA > 0 ? "#444" : "#bbb" }}>{item.blendedCPA > 0 ? `${cs}${Math.round(item.blendedCPA).toLocaleString()}` : "-"}</span>
              <span style={{ ...colStyle, width: "64px", color: item.newCustomerCPA > 0 ? "#444" : "#bbb" }}>{item.newCustomerCPA > 0 ? `${cs}${Math.round(item.newCustomerCPA).toLocaleString()}` : "-"}</span>
              <span style={{ ...colStyle, width: "56px", fontWeight: 600, color: metricGradientColor("blendedROAS", roas, sorted.map(r => r.blendedROAS || 0), true) }}>{roas > 0 ? `${roas}x` : "-"}</span>
            </div>
          );
        })}
      </div>
      {hoverId && hoverAnchor && (
        <SortMetricSparklinePopover
          x={hoverAnchor.x}
          y={hoverAnchor.y}
          state={seriesCache[hoverId]}
          metric={metricFor(sortCol)}
        />
      )}
    </div>
  );
}

// Floating popover used by Best-to-Worst hover. Renders a 90-day sparkline
// of whichever metric the list is sorted by, with avg/last value callouts.
function SortMetricSparklinePopover({ x, y, state, metric }: {
  x: number; y: number;
  state: Array<{ date: string; spend: number; revenue: number; orders: number; newCustomerOrders: number }> | "loading" | "error" | undefined;
  metric: { label: string; get: (d: any) => number; fmt: (v: number) => string; color: string; lower: boolean };
}) {
  const w = 240, h = 80, padX = 6, padY = 8;
  let body: React.ReactNode;
  if (!state || state === "loading") {
    body = <div style={{ fontSize: 11, color: "#9CA3AF", padding: "12px 10px" }}>Loading…</div>;
  } else if (state === "error") {
    body = <div style={{ fontSize: 11, color: "#B91C1C", padding: "12px 10px" }}>Couldn't load chart</div>;
  } else {
    const series = state.map((d) => metric.get(d) || 0);
    const positive = series.filter((v) => v > 0);
    if (positive.length === 0) {
      body = <div style={{ fontSize: 11, color: "#9CA3AF", padding: "12px 10px" }}>No {metric.label.toLowerCase()} in last 90 days</div>;
    } else {
      const max = Math.max(...positive);
      const stepX = (w - padX * 2) / Math.max(1, series.length - 1);
      const path = series.map((v, i) => {
        const px = padX + i * stepX;
        const py = h - padY - (max > 0 ? (v / max) * (h - padY * 2) : 0);
        return `${i === 0 ? "M" : "L"}${px},${py}`;
      }).join(" ");
      const avg = positive.reduce((s, v) => s + v, 0) / positive.length;
      const last = series[series.length - 1] || 0;
      body = (
        <>
          <div style={{ fontSize: 10, color: "#9CA3AF", padding: "5px 10px 0", display: "flex", justifyContent: "space-between" }}>
            <span><strong style={{ color: "#374151" }}>{metric.label}</strong> · {series.length}d</span>
            <span>avg {metric.fmt(avg)}</span>
          </div>
          <svg width={w} height={h} preserveAspectRatio="none" viewBox={`0 0 ${w} ${h}`} style={{ display: "block" }}>
            <path d={path} stroke={metric.color} strokeWidth="1.5" fill="none" />
          </svg>
          <div style={{ fontSize: 10, color: "#6B7280", padding: "0 10px 5px", textAlign: "right" }}>last: {metric.fmt(last)}</div>
        </>
      );
    }
  }
  // Portal to document.body so the popover escapes any parent stacking
  // context (e.g. a sibling tile's Card) that would otherwise pin it
  // behind the next tile in the grid - that's why hover tooltips were
  // appearing under the Platform Performance tile.
  if (typeof document === "undefined") return null;
  return createPortal(
    <div style={{
      position: "fixed", left: x, top: y,
      transform: "translate(0, -50%)",
      background: "#fff", border: "1px solid #E5E7EB", borderRadius: 8,
      boxShadow: "0 6px 20px rgba(0,0,0,0.12)",
      zIndex: 99999, pointerEvents: "none",
      minWidth: w,
    }}>
      {body}
    </div>,
    document.body,
  );
}

function NewCustomerList({ rows, cs }: { rows: any[]; cs: string }) {
  const [sortCol, setSortCol] = useState("newCustomerOrders");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc");
  const toggleSort = (col: string) => {
    if (sortCol === col) setSortDir(d => d === "desc" ? "asc" : "desc");
    else { setSortCol(col); setSortDir(col === "newCustomerCPA" ? "asc" : "desc"); }
  };

  const filtered = rows.filter(r => (r.newCustomerOrders || 0) > 0);
  const getVal = (r: any) => {
    if (sortCol === "newPct") {
      const orders = r.attributedOrders || 0;
      return orders > 0 ? (r.newCustomerOrders || 0) / orders : 0;
    }
    return r[sortCol] || 0;
  };
  const sorted = [...filtered].sort((a, b) => {
    const aVal = getVal(a), bVal = getVal(b);
    if (aVal === 0 && bVal === 0) return 0;
    if (aVal === 0) return 1;
    if (bVal === 0) return -1;
    return sortDir === "desc" ? bVal - aVal : aVal - bVal;
  });

  if (sorted.length === 0) return <div style={{ color: "#999", fontSize: "13px", padding: "8px 0" }}>No data</div>;

  const colStyle = { fontSize: "11px", textAlign: "right" as const, flexShrink: 0 };
  const LOWER_IS_BETTER = new Set(["newCustomerCPA"]);
  const higherIsBetter = !LOWER_IS_BETTER.has(sortCol);
  const barValues = sorted.map(r => getVal(r));
  const maxBarVal = Math.max(...barValues.filter(v => v > 0), 0.01);

  const COLS = [
    { key: "newCustomerOrders", label: "New", width: "36px" },
    { key: "attributedOrders", label: "Orders", width: "44px" },
    { key: "newPct", label: "New%", width: "36px" },
    { key: "newCustomerCPA", label: "NC CPA", width: "56px" },
    { key: "newCustomerRevenue", label: "NC Rev", width: "56px" },
  ];

  return (
    <div>
      <div style={{ display: "flex", gap: "8px", alignItems: "center", padding: "0 0 6px 0", borderBottom: "1px solid #e4e5e7", marginBottom: "6px" }}>
        <span style={{ width: "16px", flexShrink: 0 }} />
        <span style={{ flex: 1, fontSize: "10px", color: "#8c9196", fontWeight: 600, textTransform: "uppercase" }}>Name</span>
        {COLS.map(c => <SortableHeader key={c.key} col={c.key} label={c.label} width={c.width} sortCol={sortCol} sortDir={sortDir} onSort={toggleSort} />)}
      </div>
      <div style={{ maxHeight: "340px", overflowY: "auto", display: "flex", flexDirection: "column", gap: "6px", paddingRight: "4px" }}>
        {sorted.map((item, i) => {
          const newCusts = item.newCustomerOrders || 0;
          const orders = item.attributedOrders || 0;
          const newPct = orders > 0 ? Math.round((newCusts / orders) * 100) : 0;
          const barVal = getVal(item);
          const barW = barVal > 0 && maxBarVal > 0 ? (barVal / maxBarVal) * 100 : 0;
          const barColor = metricGradientColor(sortCol === "newPct" ? "ctr" : sortCol, barVal, barValues, higherIsBetter);
          const ncCpa = item.newCustomerCPA || 0;
          const ncRev = item.newCustomerRevenue || 0;
          return (
            <div key={item.id || i} style={{ display: "flex", alignItems: "center", gap: "8px" }}>
              <span style={{ fontSize: "11px", color: "#999", width: "16px", textAlign: "right", flexShrink: 0 }}>{i + 1}</span>
              <div style={{ flex: 1, minWidth: 0 }}>
                <span style={{ fontSize: "12px", fontWeight: 500, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", display: "block" }} title={item.name}>{item.name}</span>
                <div style={{ height: "4px", background: "#f1f2f4", borderRadius: "2px", overflow: "hidden", marginTop: "2px" }}>
                  <div style={{ height: "100%", width: `${barW}%`, background: barColor, borderRadius: "2px", opacity: 0.7 }} />
                </div>
              </div>
              <span style={{ ...colStyle, width: "36px", fontWeight: 600, color: "#7c3aed" }}>{newCusts}</span>
              <span style={{ ...colStyle, width: "44px", color: "#444" }}>{orders}</span>
              <span style={{ ...colStyle, width: "36px", color: "#444" }}>{newPct}%</span>
              <span style={{ ...colStyle, width: "56px", color: ncCpa > 0 ? "#444" : "#bbb" }}>{ncCpa > 0 ? `${cs}${Math.round(ncCpa).toLocaleString()}` : "-"}</span>
              <span style={{ ...colStyle, width: "56px", color: ncRev > 0 ? "#444" : "#bbb" }}>{ncRev > 0 ? `${cs}${Math.round(ncRev).toLocaleString()}` : "-"}</span>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function DonutChart({ segments }: { segments: { label: string; value: number; color: string }[] }) {
  const total = segments.reduce((s, seg) => s + seg.value, 0);
  if (total === 0) return <div style={{ color: "#999", fontSize: "13px" }}>No data</div>;

  const size = 260;
  const cx = size / 2, cy = size / 2, r = 95, sw = 35;
  const circ = 2 * Math.PI * r;

  let offset = 0;
  return (
    <div style={{ display: "flex", alignItems: "center", gap: "24px", justifyContent: "center" }}>
      <svg width={size} height={size} viewBox={`0 0 ${size} ${size}`}>
        {segments.filter(s => s.value > 0).map((seg, i) => {
          const pct = seg.value / total;
          const dashLen = circ * pct;
          const dashOff = -offset;
          offset += dashLen;
          return (
            <circle
              key={i}
              cx={cx} cy={cy} r={r}
              fill="none" stroke={seg.color} strokeWidth={sw}
              strokeDasharray={`${dashLen} ${circ - dashLen}`}
              strokeDashoffset={dashOff}
              transform={`rotate(-90 ${cx} ${cy})`}
            />
          );
        })}
        <text x={cx} y={cy - 10} textAnchor="middle" dominantBaseline="central" fontSize="26" fontWeight="700" fill="#1a1a1a">
          {total.toLocaleString()}
        </text>
        <text x={cx} y={cy + 16} textAnchor="middle" dominantBaseline="central" fontSize="13" fill="#6d7175">
          total
        </text>
      </svg>
      <div style={{ display: "flex", flexDirection: "column", gap: "12px" }}>
        {segments.filter(s => s.value > 0).map((seg, i) => (
          <div key={i} style={{ display: "flex", alignItems: "center", gap: "10px" }}>
            <div style={{ width: 14, height: 14, borderRadius: "50%", background: seg.color, flexShrink: 0 }} />
            <div>
              <div style={{ fontSize: "16px", fontWeight: 600 }}>{typeof seg.value === "number" && seg.value % 1 !== 0 ? seg.value.toLocaleString(undefined, { maximumFractionDigits: 0 }) : seg.value.toLocaleString()}</div>
              <div style={{ fontSize: "12px", color: "#6d7175" }}>{seg.label} ({Math.round((seg.value / total) * 100)}%)</div>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

function platformLabel(platform: string): string {
  const p = platform.toLowerCase().replace(/[_\s-]/g, "");
  if (p.includes("facebook") || p === "feed" || p === "righthandcolumn" || p === "instantarticle" || p === "marketplace" || p === "videofeeds") return "Facebook";
  if (p.includes("instagram") || p === "explore" || p === "profilefeed" || p === "igstories" || p === "igreels") return "Instagram";
  if (p.includes("messenger")) return "Messenger";
  if (p.includes("audiencenetwork") || p === "anclassic" || p === "rewardedvideo") return "Audience Network";
  if (p.includes("threads")) return "Threads";
  return platform;
}

function PlatformLogo({ platform, size = 16 }: { platform: string; size?: number }) {
  const p = platform.toLowerCase().replace(/[_\s-]/g, "");
  const tooltip = platformLabel(platform);
  if (p.includes("facebook") || p === "feed" || p === "righthandcolumn" || p === "instantarticle" || p === "marketplace" || p === "videofeeds") {
    return <svg width={size} height={size} viewBox="0 0 24 24" style={{ flexShrink: 0 }}><title>{tooltip}</title><circle cx="12" cy="12" r="12" fill="#1877F2"/><path d="M16.67 15.47l.58-3.8h-3.64v-2.47c0-1.04.51-2.05 2.14-2.05h1.66V3.95s-1.5-.26-2.94-.26c-3 0-4.97 1.82-4.97 5.12v2.9H6.3v3.8h3.2V24h3.94V15.47h2.64z" fill="#fff" transform="scale(0.85) translate(2,2)"/></svg>;
  }
  if (p.includes("instagram") || p === "explore" || p === "profilefeed" || p === "igstories" || p === "igreels") {
    return <svg width={size} height={size} viewBox="0 0 24 24" style={{ flexShrink: 0 }}><title>{tooltip}</title><defs><linearGradient id="ig" x1="0" y1="1" x2="1" y2="0"><stop offset="0%" stopColor="#FFDC80"/><stop offset="25%" stopColor="#F77737"/><stop offset="50%" stopColor="#C13584"/><stop offset="75%" stopColor="#833AB4"/><stop offset="100%" stopColor="#405DE6"/></linearGradient></defs><rect width="24" height="24" rx="6" fill="url(#ig)"/><rect x="4" y="4" width="16" height="16" rx="4" fill="none" stroke="#fff" strokeWidth="1.8"/><circle cx="12" cy="12" r="3.5" fill="none" stroke="#fff" strokeWidth="1.8"/><circle cx="17.5" cy="6.5" r="1.2" fill="#fff"/></svg>;
  }
  if (p.includes("messenger")) {
    return <svg width={size} height={size} viewBox="0 0 24 24" style={{ flexShrink: 0 }}><title>{tooltip}</title><circle cx="12" cy="12" r="12" fill="#0084FF"/><path d="M12 4C7.58 4 4 7.13 4 11c0 2.2 1.1 4.16 2.8 5.44V20l3.36-1.84c.58.16 1.2.24 1.84.24 4.42 0 8-3.13 8-7s-3.58-7-8-7zm.8 9.4l-2.04-2.17-3.96 2.17 4.36-4.63 2.08 2.17 3.92-2.17-4.36 4.63z" fill="#fff" transform="scale(0.8) translate(3,3)"/></svg>;
  }
  if (p.includes("audiencenetwork") || p === "anclassic" || p === "rewardedvideo") {
    return <svg width={size} height={size} viewBox="0 0 24 24" style={{ flexShrink: 0 }}><title>{tooltip}</title><circle cx="12" cy="12" r="12" fill="#4B5563"/><circle cx="12" cy="8" r="2" fill="#fff"/><circle cx="7" cy="15" r="2" fill="#fff"/><circle cx="17" cy="15" r="2" fill="#fff"/><line x1="12" y1="10" x2="7" y2="13" stroke="#fff" strokeWidth="1.2"/><line x1="12" y1="10" x2="17" y2="13" stroke="#fff" strokeWidth="1.2"/><line x1="7" y1="15" x2="17" y2="15" stroke="#fff" strokeWidth="1.2"/></svg>;
  }
  if (p.includes("threads")) {
    return <svg width={size} height={size} viewBox="0 0 24 24" style={{ flexShrink: 0 }}><title>{tooltip}</title><circle cx="12" cy="12" r="12" fill="#000"/><text x="12" y="17" textAnchor="middle" fontSize="14" fontWeight="700" fill="#fff">@</text></svg>;
  }
  // Fallback
  return <svg width={size} height={size} viewBox="0 0 24 24" style={{ flexShrink: 0 }}><title>{tooltip}</title><circle cx="12" cy="12" r="12" fill="#9CA3AF"/><circle cx="12" cy="12" r="4" fill="#fff"/></svg>;
}

function stripChannelPrefix(placementName: string): string {
  // "instagram|feed" → "feed"
  const idx = placementName.indexOf("|");
  if (idx >= 0) return placementName.substring(idx + 1);
  // Strip channel names from values like "instagram_stories", "facebook_reels", "an_classic"
  const lower = placementName.toLowerCase();
  const prefixes = ["instagram_", "facebook_", "messenger_", "audience_network_", "threads_", "ig_", "fb_", "an_"];
  for (const pfx of prefixes) {
    if (lower.startsWith(pfx)) return placementName.substring(pfx.length);
  }
  return placementName;
}

function inferPlatform(breakdownValue: string): string {
  const v = breakdownValue.toLowerCase().replace(/[_\s-]/g, "");
  if (v.includes("instagram") || v === "explore" || v === "profilefeed" || v.startsWith("ig")) return "instagram";
  if (v.includes("facebook") || v === "righthandcolumn" || v === "instantarticle" || v === "marketplace" || v === "videofeeds") return "facebook";
  if (v.includes("messenger")) return "messenger";
  if (v.includes("audience") || v === "anclassic" || v === "rewardedvideo") return "audience_network";
  if (v.includes("threads")) return "threads";
  // Generic placements: feed/story/reels/search could be either - show as generic
  return breakdownValue;
}

function BreakdownPerfTile({ title, subtitle, data, cs, defaultLevel = "overall", defaultSort = "spend", type = "platform" }: {
  title: string; subtitle?: string; data: Record<string, any[]>; cs: string; defaultLevel?: string; defaultSort?: string; type?: "platform" | "placement";
}) {
  const [level, setLevel] = useState(defaultLevel);
  const [sortCol, setSortCol] = useState(defaultSort);
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc");
  const toggleSort = (col: string) => {
    if (sortCol === col) setSortDir(d => d === "desc" ? "asc" : "desc");
    else { setSortCol(col); setSortDir(col === "cpa" || col === "newCustomerCPA" ? "asc" : "desc"); }
  };
  const BREAKDOWN_LEVELS = [
    { id: "overall", label: "Overall" },
    { id: "campaign", label: "Campaign" },
    { id: "adset", label: "Ad Set" },
    { id: "ad", label: "Ad" },
  ];
  const rawItems = data?.[level] || [];
  if (!data || Object.values(data).every(arr => !arr || arr.length === 0)) return null;

  const items = [...rawItems].sort((a, b) => {
    const aVal = a[sortCol] || 0, bVal = b[sortCol] || 0;
    if (aVal === 0 && bVal === 0) return (b.spend || 0) - (a.spend || 0);
    if (aVal === 0) return 1;
    if (bVal === 0) return -1;
    return sortDir === "desc" ? bVal - aVal : aVal - bVal;
  });

  const LOWER_IS_BETTER_BD = new Set(["cpa", "newCustomerCPA"]);
  const higherIsBetter = !LOWER_IS_BETTER_BD.has(sortCol);
  const barValues = items.map(r => r[sortCol] || 0);
  const maxBarVal = Math.max(...barValues.filter(v => v > 0), 0.01);
  const colStyle = { fontSize: "12.5px", textAlign: "right" as const, flexShrink: 0 };

  const COLS = [
    { key: "conversions", label: "Orders", width: "52px" },
    { key: "newCustomerOrders", label: "New", width: "40px" },
    { key: "spend", label: "Spend", width: "68px" },
    { key: "cpa", label: "CPA", width: "60px" },
    { key: "newCustomerCPA", label: "NC CPA", width: "64px" },
    { key: "roas", label: "ROAS", width: "56px" },
  ];

  const hasEntity = level !== "overall";
  const nameColLabel = level === "adset" ? "Ad Set" : level === "ad" ? "Ad" : level === "campaign" ? "Campaign" : (type === "placement" ? "Placement" : "Channel");

  // 10 rows visible, rest scrollable (each row ~37px with gap)
  const ROW_HEIGHT = 37;
  const VISIBLE_ROWS = 10;

  return (
    <Card>
      <BlockStack gap="300">
        <InlineStack align="space-between" blockAlign="center">
          <BlockStack gap="050">
            <Text as="h3" variant="headingSm">{title}</Text>
            {subtitle && <Text as="p" variant="bodySm" tone="subdued">{subtitle}</Text>}
          </BlockStack>
          <div style={{ display: "inline-flex", gap: "4px" }}>
            {BREAKDOWN_LEVELS.map(o => (
              <button key={o.id} onClick={() => setLevel(o.id)} className={`l-pill${level === o.id ? " l-pill--active" : ""}`}>{o.label}</button>
            ))}
          </div>
        </InlineStack>
        {items.length === 0 ? (
          <div style={{ color: "#999", fontSize: "13px", padding: "8px 0" }}>No data</div>
        ) : (
          <>
            <div style={{ display: "flex", gap: "10px", alignItems: "center", padding: "0 0 8px 0", borderBottom: "1px solid #e4e5e7", marginBottom: "8px" }}>
              <span style={{ width: "22px", flexShrink: 0 }} />
              <span style={{ flex: 1, fontSize: "11px", color: "#8c9196", fontWeight: 600, textTransform: "uppercase" }}>
                {nameColLabel}
              </span>
              {COLS.map(c => <SortableHeader key={c.key} col={c.key} label={c.label} width={c.width} sortCol={sortCol} sortDir={sortDir} onSort={toggleSort} />)}
            </div>
            <div style={{ maxHeight: `${ROW_HEIGHT * VISIBLE_ROWS}px`, overflowY: "auto", display: "flex", flexDirection: "column", gap: "8px", paddingRight: "4px" }}>
              {items.map((r, i) => {
                const barVal = r[sortCol] || 0;
                const barW = barVal > 0 && maxBarVal > 0 ? (barVal / maxBarVal) * 100 : 0;
                const barColor = metricGradientColor(sortCol === "roas" ? "blendedROAS" : sortCol === "newCustomerCPA" ? "cpa" : sortCol, barVal, barValues, higherIsBetter);
                const platform = inferPlatform(r.breakdownValue);
                const placementLabel = type === "placement" ? stripChannelPrefix(r.breakdownValue) : "";
                const displayName = hasEntity
                  ? (type === "placement" ? `${placementLabel} - ${r.entityName}` : r.entityName)
                  : (type === "placement" ? placementLabel : r.breakdownValue);
                const spend = r.spend || 0;
                const conversions = r.conversions || 0;
                const newCusts = r.newCustomerOrders || 0;
                const roas = r.roas || 0;
                return (
                  <div key={i} style={{ display: "flex", alignItems: "center", gap: "10px" }}>
                    <span style={{ fontSize: "12px", color: "#999", width: "22px", textAlign: "right", flexShrink: 0 }}>{i + 1}</span>
                    <div style={{ flex: 1, minWidth: 0 }}>
                      <div style={{ display: "flex", alignItems: "center", gap: "8px", marginBottom: "4px" }}>
                        <span style={{ fontSize: "13px", fontWeight: 500, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", flex: 1, minWidth: 0, display: "inline-flex", alignItems: "center", gap: "5px" }} title={hasEntity ? `${r.breakdownValue} - ${r.entityName}` : r.breakdownValue}>
                          <PlatformLogo platform={type === "placement" ? platform : r.breakdownValue} size={14} />
                          {displayName}
                        </span>
                      </div>
                      <div style={{ height: "5px", background: "#f1f2f4", borderRadius: "2.5px", overflow: "hidden" }}>
                        <div style={{ height: "100%", width: `${barW}%`, background: barColor, borderRadius: "2.5px", transition: "width 0.3s", opacity: 0.8 }} />
                      </div>
                    </div>
                    <span style={{ ...colStyle, width: "52px", color: conversions > 0 ? "#444" : "#bbb" }}>{conversions}</span>
                    <span style={{ ...colStyle, width: "40px", color: newCusts > 0 ? "#444" : "#bbb" }}>{newCusts}</span>
                    <span style={{ ...colStyle, width: "68px", color: spend > 0 ? "#444" : "#bbb" }}>{cs}{Math.round(spend).toLocaleString()}</span>
                    <span style={{ ...colStyle, width: "60px", color: r.cpa > 0 ? "#444" : "#bbb" }}>{r.cpa > 0 ? `${cs}${Math.round(r.cpa).toLocaleString()}` : "-"}</span>
                    <span style={{ ...colStyle, width: "64px", color: r.newCustomerCPA > 0 ? "#444" : "#bbb" }}>{r.newCustomerCPA > 0 ? `${cs}${Math.round(r.newCustomerCPA).toLocaleString()}` : "-"}</span>
                    <span style={{ ...colStyle, width: "56px", fontWeight: 600, color: metricGradientColor("blendedROAS", roas, items.map(x => x.roas || 0), true) }}>{roas > 0 ? `${roas}x` : "-"}</span>
                  </div>
                );
              })}
            </div>
          </>
        )}
      </BlockStack>
    </Card>
  );
}

const FUNNEL_STEPS = [
  { key: "impressions", label: "Impressions" },
  { key: "linkClicks", label: "Link Clicks" },
  { key: "addToCart", label: "Add to Cart" },
  { key: "initiateCheckout", label: "Checkout" },
  { key: "metaConversions", label: "Purchase" },
];

function FunnelFlow({ totals, mode }: { totals: Record<string, number>; mode: "counts" | "rates" }) {
  const steps = FUNNEL_STEPS.map(s => ({ ...s, value: totals[s.key] || 0 }));

  // Impressions shown separately since it's orders of magnitude larger
  const impressions = steps[0].value;
  const funnelSteps = steps.slice(1);
  const funnelMax = Math.max(...funnelSteps.map(s => s.value), 1);
  // Width: proportional to value, but compressed with sqrt so small steps are still visible
  // Range: 30% (minimum for text) to 100%
  const minPct = 30;
  const maxPct = 100;

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "0px", alignItems: "center" }}>
      {/* Impressions shown as context above the funnel */}
      <div style={{ width: "100%", marginBottom: "6px", textAlign: "center" }}>
        <div style={{ fontSize: "11px", color: "#6d7175", marginBottom: "2px" }}>Impressions</div>
        <div style={{ fontSize: "16px", fontWeight: 700 }}>{impressions.toLocaleString()}</div>
        {funnelSteps[0]?.value > 0 && (
          <div style={{ fontSize: "10px", color: "#E65100", fontWeight: 600, marginTop: "2px" }}>
            {mode === "rates"
              ? `${((funnelSteps[0].value / impressions) * 100).toFixed(2)}% click rate`
              : `↓ ${(100 - (funnelSteps[0].value / impressions) * 100).toFixed(1)}% drop-off`
            }
          </div>
        )}
      </div>
      {/* Funnel steps - widths proportional via sqrt scaling */}
      {funnelSteps.map((step, i) => {
        const ratio = funnelMax > 0 ? step.value / funnelMax : 0;
        // sqrt compresses the range so small values still get reasonable width
        const widthPct = step.value > 0 ? minPct + (maxPct - minPct) * Math.sqrt(ratio) : minPct;
        const nextStep = funnelSteps[i + 1];
        const rateToNext = nextStep && step.value > 0 ? ((nextStep.value / step.value) * 100).toFixed(1) : null;
        const rateColor = rateToNext && parseFloat(rateToNext) < 20 ? "#C62828" : rateToNext && parseFloat(rateToNext) < 50 ? "#E65100" : "#2E7D32";
        const opacity = 1 - (i * 0.1);
        const gradient = `linear-gradient(90deg, rgba(92,106,196,${opacity}), rgba(124,58,237,${opacity}))`;

        return (
          <div key={step.key} style={{ width: "100%", display: "flex", flexDirection: "column", alignItems: "center" }}>
            <div style={{
              width: `${widthPct}%`,
              height: "32px",
              background: gradient,
              borderRadius: i === 0 ? "6px 6px 4px 4px" : i === funnelSteps.length - 1 ? "4px 4px 6px 6px" : "4px",
              display: "flex", alignItems: "center", justifyContent: "space-between",
              padding: "0 12px",
              transition: "width 0.3s",
            }}>
              <span style={{ fontSize: "11px", fontWeight: 500, color: "#fff", whiteSpace: "nowrap" }}>{step.label}</span>
              <span style={{ fontSize: "11px", fontWeight: 700, color: "#fff" }}>{step.value.toLocaleString()}</span>
            </div>
            {rateToNext && (
              <div style={{ fontSize: "10px", color: rateColor, fontWeight: 600, margin: "2px 0" }}>
                {mode === "rates" ? `${rateToNext}%` : `↓ ${(100 - parseFloat(rateToNext)).toFixed(1)}%`}
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}

// Scrollable list of every ad with its age, spend, ROAS, and creative-fatigue
// flag. Header shows a colour-coded "X days since last new ad launched"
// callout - green if recent, amber if drifting, red if it's been a while.
//
// Per-row signals:
//   • Frequency badge (avg in-period frequency) - highlighted ≥ 3 since
//     that's the textbook fatigue threshold.
//   • Hover → small popover with a 90-day ROAS sparkline pulled from
//     /app/api/entity-timeline. Cached so re-hover doesn't refetch.
//   • Click → opens the EntityTimelineDrawer for the ad.
function AdAgeTile({ adRows, cs, onAdClick }: { adRows: any[]; cs: string; onAdClick?: (adId: string, adName: string) => void }) {
  const rows = (adRows || []).slice().sort((a, b) => {
    const ax = a.adAgeDays == null ? Infinity : a.adAgeDays;
    const bx = b.adAgeDays == null ? Infinity : b.adAgeDays;
    return ax - bx;
  });

  const dated = rows.filter(r => r.adAgeDays != null);
  const newestAge = dated.length > 0 ? dated[0].adAgeDays : null;
  const fatigueOver30 = rows.filter(r => (r.adAgeDays ?? 0) >= 30).length;
  const fatigueOver60 = rows.filter(r => (r.adAgeDays ?? 0) >= 60).length;

  const ageColor = (days: number | null) => {
    if (days == null) return "#9CA3AF";
    if (days < 7) return "#059669";
    if (days < 21) return "#65A30D";
    if (days < 45) return "#D97706";
    return "#B91C1C";
  };
  const headerColor = newestAge == null ? "#6B7280"
    : newestAge < 7 ? "#059669"
    : newestAge < 14 ? "#D97706"
    : "#B91C1C";

  // Hover state + per-ad daily series cache. We fetch lazily on hover so
  // first paint stays cheap; once we have the data, the popover renders
  // instantly on subsequent hovers.
  const [hoverAdId, setHoverAdId] = useState<string | null>(null);
  const [hoverAnchor, setHoverAnchor] = useState<{ x: number; y: number } | null>(null);
  const [seriesCache, setSeriesCache] = useState<Record<string, Array<{ date: string; spend: number; revenue: number }> | "loading" | "error">>({});

  const requestSeries = (adId: string) => {
    if (seriesCache[adId]) return; // cached or in flight
    setSeriesCache((prev) => ({ ...prev, [adId]: "loading" }));
    fetch(`/app/api/entity-timeline?type=ad&id=${encodeURIComponent(adId)}`)
      .then((r) => r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`)))
      .then((d) => setSeriesCache((prev) => ({ ...prev, [adId]: d.daily || [] })))
      .catch(() => setSeriesCache((prev) => ({ ...prev, [adId]: "error" })));
  };

  return (
    <BlockStack gap="300">
      <style>{`
        .ad-age-row:hover .ad-age-chevron { color: #2563EB; transform: translateX(2px); }
      `}</style>
      <Text as="h3" variant="headingSm">Ad Age</Text>
      <div style={{
        display: "flex", alignItems: "center", gap: 10,
        padding: "8px 12px", borderRadius: 8,
        background: headerColor + "15", border: `1px solid ${headerColor}40`,
      }}>
        <div style={{ fontSize: 22 }}>{newestAge == null ? "·" : newestAge < 7 ? "🆕" : newestAge < 14 ? "⏳" : "⚠️"}</div>
        <div>
          <div style={{ fontSize: 13, fontWeight: 700, color: headerColor }}>
            {newestAge == null ? "No ads with known launch date" : `${newestAge} day${newestAge === 1 ? "" : "s"} since last new ad launched`}
          </div>
          <div style={{ fontSize: 11, color: "#6B7280" }}>
            {fatigueOver30} ad{fatigueOver30 === 1 ? "" : "s"} ≥ 30 days · {fatigueOver60} ≥ 60 days
          </div>
        </div>
      </div>
      {rows.length === 0 ? (
        <div style={{ color: "#999", fontSize: 13, padding: "8px 0" }}>No ads in this period.</div>
      ) : (
        <div style={{ maxHeight: 240, overflowY: "auto", display: "flex", flexDirection: "column", gap: 4, position: "relative" }}>
          {rows.map((r, idx) => {
            const days = r.adAgeDays;
            const colour = ageColor(days);
            const ageLabel = days == null ? "-" : days === 0 ? "Today" : `${days}d`;
            const adId = r.id;
            const interactive = !!adId;
            return (
              <div
                key={adId || idx}
                className="ad-age-row"
                onMouseEnter={(e) => {
                  if (!interactive) return;
                  setHoverAdId(adId);
                  const rect = (e.currentTarget as HTMLDivElement).getBoundingClientRect();
                  // Viewport-aware: prefer the row's right edge but flip to
                  // the left if it would overflow. 240px = popover width
                  // budget incl. padding/shadow.
                  const POP_W = 240, GAP = 8;
                  const wantRight = rect.right + GAP + POP_W;
                  const x = wantRight <= window.innerWidth
                    ? rect.right + GAP
                    : Math.max(GAP, rect.left - GAP - POP_W);
                  setHoverAnchor({ x, y: rect.top + rect.height / 2 });
                  requestSeries(adId);
                }}
                onMouseLeave={() => { setHoverAdId(null); setHoverAnchor(null); }}
                onClick={() => interactive && onAdClick?.(adId, r.name)}
                title={interactive ? "Click for full timeline" : undefined}
                style={{
                  display: "flex", alignItems: "center", gap: 10,
                  padding: "6px 8px", borderRadius: 6,
                  background: idx % 2 === 0 ? "#fafafa" : "#fff",
                  borderLeft: `3px solid ${colour}`,
                  cursor: interactive ? "pointer" : "default",
                  transition: "background 0.12s ease",
                }}
                onMouseOver={(e) => { if (interactive) (e.currentTarget as HTMLDivElement).style.background = "#f1f5f9"; }}
                onMouseOut={(e) => { (e.currentTarget as HTMLDivElement).style.background = idx % 2 === 0 ? "#fafafa" : "#fff"; }}
              >
                {/* Two-column row: name fills available space on the left,
                    age sits flush right. Spend / ROAS / new customers and
                    frequency badge intentionally removed - the table below
                    is the right place for stats. */}
                <div style={{ flex: 1, minWidth: 0, fontSize: 12, fontWeight: 500, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }} title={r.name}>{r.name}</div>
                <div style={{
                  fontSize: 11, fontWeight: 700, color: colour,
                  flexShrink: 0, fontVariantNumeric: "tabular-nums",
                }}>{ageLabel}</div>
              </div>
            );
          })}
        </div>
      )}
      {hoverAdId && hoverAnchor && (
        <RoasHoverPopover
          x={hoverAnchor.x}
          y={hoverAnchor.y}
          state={seriesCache[hoverAdId]}
        />
      )}
    </BlockStack>
  );
}

// Tiny floating popover with a 90-day ROAS sparkline. Renders one of:
//   • spinner-style "loading"
//   • "no data" if the entity has no spend in the lookback
//   • the line itself with an avg ROAS callout
function RoasHoverPopover({ x, y, state }: {
  x: number; y: number;
  state: Array<{ date: string; spend: number; revenue: number }> | "loading" | "error" | undefined;
}) {
  const w = 220, h = 70, padX = 6, padY = 6;
  let body: React.ReactNode;
  if (!state || state === "loading") {
    body = <div style={{ fontSize: 11, color: "#9CA3AF", padding: "12px 8px" }}>Loading…</div>;
  } else if (state === "error") {
    body = <div style={{ fontSize: 11, color: "#B91C1C", padding: "12px 8px" }}>Couldn't load chart</div>;
  } else {
    const points = state.map((d) => ({ date: d.date, roas: d.spend > 0 ? d.revenue / d.spend : 0 }));
    const positive = points.filter((p) => p.roas > 0);
    if (points.length === 0 || positive.length === 0) {
      body = <div style={{ fontSize: 11, color: "#9CA3AF", padding: "12px 8px" }}>No spend in last 90 days</div>;
    } else {
      const max = Math.max(...positive.map((p) => p.roas));
      const stepX = (w - padX * 2) / Math.max(1, points.length - 1);
      const path = points.map((p, i) => {
        const px = padX + i * stepX;
        const py = h - padY - (max > 0 ? (p.roas / max) * (h - padY * 2) : 0);
        return `${i === 0 ? "M" : "L"}${px},${py}`;
      }).join(" ");
      const avg = positive.reduce((s, p) => s + p.roas, 0) / positive.length;
      body = (
        <>
          <div style={{ fontSize: 10, color: "#9CA3AF", padding: "4px 8px 0" }}>
            ROAS · last {points.length} days · avg {avg.toFixed(2)}x
          </div>
          <svg width={w} height={h} preserveAspectRatio="none" viewBox={`0 0 ${w} ${h}`}>
            <path d={path} stroke="#2563EB" strokeWidth="1.5" fill="none" />
          </svg>
        </>
      );
    }
  }
  // Portal to document.body so the popover escapes any parent stacking
  // context (e.g. a sibling tile's Card) that would otherwise pin it
  // behind the next tile in the grid - that's why hover tooltips were
  // appearing under the Platform Performance tile.
  if (typeof document === "undefined") return null;
  return createPortal(
    <div style={{
      position: "fixed", left: x, top: y,
      transform: "translate(0, -50%)",
      background: "#fff", border: "1px solid #E5E7EB", borderRadius: 8,
      boxShadow: "0 6px 20px rgba(0,0,0,0.12)",
      zIndex: 99999, pointerEvents: "none",
      minWidth: w,
    }}>
      {body}
    </div>,
    document.body,
  );
}


// ═══════════════════════════════════════════════════════════════
// Ad Explorer - full-width table with filters, replacing Best to Worst
// ═══════════════════════════════════════════════════════════════

// Small 32x32 thumb for the Ad Explorer. Hover reveals the full image_url
// in a popover so a merchant can spot which creative is which without
// leaving the table.
function AdThumbTile({ thumbnailUrl, imageUrl, name, isDpa }: { thumbnailUrl: string | null; imageUrl: string | null; name: string; isDpa?: boolean }) {
  const [imgFailed, setImgFailed] = useState(false);
  const [hover, setHover] = useState(false);
  const initial = (name || "?").trim().charAt(0).toUpperCase();
  // DPA ads have no useful single creative thumbnail - Meta returns a 64x64
  // placeholder PNG that reads as a blank grey blob. We swap in a branded
  // "DPA" tile (public/dpa-thumbnail.jpg) so the explorer surfaces them
  // visibly instead of falling through to a generic initial letter.
  const dpaSrc = "/dpa-thumbnail.jpg";
  const showImg = (isDpa) || (thumbnailUrl && !imgFailed);
  const smallSrc = isDpa ? dpaSrc : thumbnailUrl;
  const fullImg = isDpa ? dpaSrc : (imageUrl || thumbnailUrl);

  return (
    <span
      onMouseEnter={() => setHover(true)}
      onMouseLeave={() => setHover(false)}
      title={isDpa ? "Dynamic Product Ad - thumbnails come from the product catalogue, not a single creative." : undefined}
      style={{
        position: "relative", width: "32px", height: "32px", flexShrink: 0,
        borderRadius: 4, overflow: "visible",
        background: showImg ? "#f3f4f6" : "#E5E7EB",
        display: "inline-flex", alignItems: "center", justifyContent: "center",
        fontSize: "12px", fontWeight: 700,
        color: "#6B7280",
        border: "1px solid #E5E7EB",
      }}
    >
      {showImg ? (
        <img
          src={smallSrc as string}
          alt=""
          loading="lazy"
          onError={() => setImgFailed(true)}
          style={{ width: "100%", height: "100%", objectFit: "cover", borderRadius: 3 }}
        />
      ) : (
        <span>{initial}</span>
      )}
      {hover && fullImg && !imgFailed && (
        <span style={{
          position: "absolute", left: "40px", top: "-50px", zIndex: 1000,
          width: "180px", height: "180px", background: "#fff",
          border: "1px solid #D1D5DB", borderRadius: 6,
          boxShadow: "0 8px 24px rgba(0,0,0,0.18)",
          padding: 4, pointerEvents: "none",
        }}>
          <img src={fullImg} alt="" style={{ width: "100%", height: "100%", objectFit: "contain" }} />
        </span>
      )}
    </span>
  );
}

// ── Ad Explorer (additive customer columns) ─────────────────────────────
// All Customers / New Customers / Existing Customers act as additive toggles.
// Selecting more than one segment widens the table by adding NC- and EC-
// prefixed columns alongside the All-Customers columns for each metric.
// Spend is segment-independent so it appears once.
type Segment = "all" | "new" | "existing";

const SEGMENT_NAMES: Record<Segment, string> = {
  all: "All Customers",
  new: "New Customers",
  existing: "Existing Customers",
};

const SEGMENT_PREFIX: Record<Segment, string> = { all: "", new: "NC ", existing: "EC " };

function segOrders(r: any, seg: Segment): number {
  if (seg === "new") return r.newCustomerOrders || 0;
  if (seg === "existing") return r.existingCustomerOrders || 0;
  return r.attributedOrders || 0;
}
function segRevenue(r: any, seg: Segment): number {
  if (seg === "new") return r.newCustomerRevenue || 0;
  if (seg === "existing") return r.existingCustomerRevenue || 0;
  return r.attributedRevenue || 0;
}

// Daily payload returned by /app/api/entity-timeline (inline expansion uses
// the same endpoint as the slide-out drawer - one cache hit, two consumers).
type TimelineDay = {
  date: string; spend: number; revenue: number; orders: number;
  newCustomerOrders?: number; newCustomerRevenue?: number;
  existingCustomerOrders?: number; existingCustomerRevenue?: number;
};
type TimelineEvent = {
  id: string; eventTimeISO: string; category: string; summary: string;
  actor: string | null; rawEventType: string;
};
type TimelinePayload = {
  entity: { objectName: string | null; currentStatus: string | null;
    createdTime: string | null; effectiveStartAt: string | null; effectiveEndAt: string | null; };
  events: TimelineEvent[];
  daily: TimelineDay[];
};

type AdDemographicSlice = {
  gender: string; ageBracket: string;
  attributedOrders: number; attributedRevenue: number;
  newCustomerOrders: number; newCustomerRevenue: number;
  existingCustomerOrders: number; existingCustomerRevenue: number;
};

function AdExplorerTable({ rows, cs, entityType, adDemographicsByAd, onEntityClick }: {
  rows: any[]; cs: string;
  entityType: "campaign" | "adset" | "ad";
  adDemographicsByAd?: Record<string, AdDemographicSlice[]>;
  onEntityClick?: (id: string, name: string) => void;
}) {
  // Default sort surfaces highest order count - the metric merchants ask
  // about first ("which ads are actually selling?"). Spend sort is one click
  // away via column header.
  const [sortCol, setSortCol] = useState("all_orders");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc");
  // Multi-select segments. Selection always resets to {all} on mount - persisting
  // it between visits caused confusion when the explorer first opened with a
  // stale cut applied. At least one segment must remain selected at all times.
  const [selectedSegments, setSelectedSegments] = useState<Set<Segment>>(() => new Set<Segment>(["all"]));
  const [searchQuery, setSearchQuery] = useState("");
  // Demographic filters. Only active when entityType === "ad" - the demographic
  // rollup is keyed by adId so we can't apply it at campaign/adset level without
  // an extra rollup. Spend stays unchanged regardless of filter (no demographic
  // split on spend); the order/revenue numbers are recomputed from the matching
  // demographic slices.
  const [genderFilter, setGenderFilter] = useState<"All" | "Female" | "Male">("All");
  const [ageFilter, setAgeFilter] = useState<string[]>([]);
  const demoActive = entityType === "ad" && (genderFilter !== "All" || ageFilter.length > 0);

  // Apply demographic filter to a row. If active, swap the order/revenue
  // numbers for sums from the matching demographic slices. Spend is left as-is
  // (Meta does not split spend by customer-resolved demographic).
  const applyDemoFilter = useCallback((row: any): any => {
    if (!demoActive) return row;
    const slices = adDemographicsByAd?.[row.id] || [];
    const wantGender = genderFilter === "All" ? null : (genderFilter === "Female" ? "female" : "male");
    const ageSet = ageFilter.length === 0 ? null : new Set(ageFilter);
    let attributedOrders = 0, attributedRevenue = 0;
    let newCustomerOrders = 0, newCustomerRevenue = 0;
    let existingCustomerOrders = 0, existingCustomerRevenue = 0;
    for (const s of slices) {
      if (wantGender && s.gender !== wantGender) continue;
      if (ageSet && !ageSet.has(s.ageBracket)) continue;
      attributedOrders += s.attributedOrders;
      attributedRevenue += s.attributedRevenue;
      newCustomerOrders += s.newCustomerOrders;
      newCustomerRevenue += s.newCustomerRevenue;
      existingCustomerOrders += s.existingCustomerOrders;
      existingCustomerRevenue += s.existingCustomerRevenue;
    }
    return {
      ...row,
      attributedOrders, attributedRevenue,
      newCustomerOrders, newCustomerRevenue,
      existingCustomerOrders, existingCustomerRevenue,
    };
  }, [demoActive, adDemographicsByAd, genderFilter, ageFilter]);

  const filteredRows = useMemo(() => {
    if (!demoActive) return rows;
    return rows.map(applyDemoFilter);
  }, [rows, demoActive, applyDemoFilter]);

  // Set of age brackets that actually appear in the loaded data, so we don't
  // render empty pills. Falls back to the canonical Meta brackets when there's
  // no demographic data yet (first sync, etc.).
  const META_AGE_BRACKETS = ["13-17", "18-24", "25-34", "35-44", "45-54", "55-64", "65+"];
  const availableAges = useMemo(() => {
    const set = new Set<string>();
    if (entityType === "ad" && adDemographicsByAd) {
      for (const row of rows) {
        const slices = adDemographicsByAd[row.id] || [];
        for (const s of slices) {
          if (s.ageBracket && s.ageBracket !== "unknown") set.add(s.ageBracket);
        }
      }
    }
    return set;
  }, [rows, entityType, adDemographicsByAd]);
  const ageBracketsToRender = META_AGE_BRACKETS.filter(b => availableAges.has(b) || ageFilter.includes(b));

  const toggleAge = (b: string) => {
    setAgeFilter(prev => prev.includes(b) ? prev.filter(x => x !== b) : [...prev, b]);
  };

  // Inline row expansion. Replaces the slide-out drawer for explorer rows -
  // the table never greys out, sort/filter still work freely while a row is
  // open, and the embedded sparkline updates in-place when the active sort
  // metric changes (no re-fetch - daily payload carries every metric).
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [timelineCache, setTimelineCache] = useState<Map<string, TimelinePayload>>(() => new Map());
  const [loadingIds, setLoadingIds] = useState<Set<string>>(() => new Set());
  const [errorIds, setErrorIds] = useState<Map<string, string>>(() => new Map());

  const toggleExpand = useCallback((id: string, type: "campaign" | "adset" | "ad") => {
    setExpandedId(prev => prev === id ? null : id);
    if (timelineCache.has(id)) return;
    setLoadingIds(prev => { const n = new Set(prev); n.add(id); return n; });
    fetch(`/app/api/entity-timeline?type=${type}&id=${encodeURIComponent(id)}`)
      .then(r => r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`)))
      .then((d: TimelinePayload) => {
        setTimelineCache(prev => { const n = new Map(prev); n.set(id, d); return n; });
      })
      .catch((e) => {
        setErrorIds(prev => { const n = new Map(prev); n.set(id, e.message); return n; });
      })
      .finally(() => {
        setLoadingIds(prev => { const n = new Set(prev); n.delete(id); return n; });
      });
  }, [timelineCache]);

  const toggleSegment = (s: Segment) => {
    setSelectedSegments(prev => {
      const next = new Set(prev);
      if (next.has(s)) {
        if (next.size === 1) return prev; // can't deselect the last one
        next.delete(s);
      } else {
        next.add(s);
      }
      return next;
    });
  };

  // Stable order: All → New → Existing, regardless of click order.
  const orderedSegments: Segment[] = useMemo(() => (
    (["all", "new", "existing"] as Segment[]).filter(s => selectedSegments.has(s))
  ), [selectedSegments]);

  // Lookup for the numeric value behind any sort key (segment_metric).
  const getValue = (r: any, key: string): number => {
    if (key === "spend") return r.spend || 0;
    const [seg, metric] = key.split("_") as [Segment, string];
    if (!seg || !metric) return r[key] || 0;
    const o = segOrders(r, seg);
    const rev = segRevenue(r, seg);
    if (metric === "orders") return o;
    if (metric === "revenue") return rev;
    if (metric === "roas") return o > 0 && (r.spend || 0) > 0 ? rev / r.spend : 0;
    if (metric === "cpa") return o > 0 && (r.spend || 0) > 0 ? r.spend / o : 0;
    if (metric === "aov") return o > 0 ? rev / o : 0;
    return 0;
  };

  const toggleSort = (col: string) => {
    if (sortCol === col) setSortDir(d => d === "desc" ? "asc" : "desc");
    else {
      setSortCol(col);
      // CPA is "lower is better" - invert the default direction so the worst
      // CPA isn't pinned at the top when the user clicks the header.
      setSortDir(col.endsWith("_cpa") ? "asc" : "desc");
    }
  };

  const sorted = useMemo(() => {
    const q = searchQuery.trim().toLowerCase();
    return [...filteredRows]
      .filter(r => (r.spend || 0) > 0 || (r.attributedOrders || 0) > 0)
      .filter(r => !q || (r.name || "").toLowerCase().includes(q))
      .sort((a, b) => {
        const aVal = getValue(a, sortCol), bVal = getValue(b, sortCol);
        if (aVal === 0 && bVal === 0) return (b.spend || 0) - (a.spend || 0);
        if (aVal === 0) return 1;
        if (bVal === 0) return -1;
        return sortDir === "desc" ? bVal - aVal : aVal - bVal;
      });
  }, [filteredRows, sortCol, sortDir, searchQuery]);

  // Bar visualisation under each ad name - length scales with the active
  // sort metric, like the Product Demographics Explorer. CPA is "lower is
  // better" so we invert the bar (long bar = low CPA = good); for every
  // other metric, longer bars = higher values.
  const sortMeta = useMemo(() => {
    const isCpa = sortCol.endsWith("_cpa");
    let maxVal = 0, minVal = Infinity;
    for (const r of sorted) {
      const v = getValue(r, sortCol);
      if (v > maxVal) maxVal = v;
      if (v > 0 && v < minVal) minVal = v;
    }
    if (!isFinite(minVal)) minVal = 0;
    return { isCpa, maxVal, minVal };
  }, [sorted, sortCol]);

  const widthPctFor = (item: any): number => {
    const v = getValue(item, sortCol);
    if (v <= 0) return 0;
    const { isCpa, maxVal, minVal } = sortMeta;
    if (maxVal <= 0) return 0;
    if (isCpa) {
      // Invert: lowest CPA = full bar, highest = shortest. Min 8% so the
      // bar stays visible for the worst rows too.
      if (maxVal === minVal) return 100;
      const t = (maxVal - v) / (maxVal - minVal);
      return Math.max(8, Math.round(t * 100));
    }
    return Math.max(4, Math.round((v / maxVal) * 100));
  };
  // Bar colour echoes the segment so the visual reinforces what's being
  // sorted: blue for All, violet for New, teal for Existing, slate for
  // plain Spend.
  const barColors = useMemo(() => {
    if (sortCol === "spend") return { from: "#475569", to: "#94A3B8" };
    const seg = sortCol.split("_")[0];
    if (seg === "new") return { from: "#7C3AED", to: "#C4B5FD" };
    if (seg === "existing") return { from: "#0D9488", to: "#99F6E4" };
    return { from: "#2563EB", to: "#93C5FD" };
  }, [sortCol]);

  type ColDef = {
    key: string; label: string; width: string;
    format: (row: any) => string;
    tooltip?: string;
    isRevenue?: boolean;
  };

  // When more than one segment is on, the table grows from 6 columns to as
  // many as 16. We tighten column widths + inter-column gap to keep the grid
  // dense rather than letting horizontal scroll dominate.
  const compact = orderedSegments.length > 1;
  const W = {
    orders: compact ? "60px" : "70px",
    revenue: compact ? "78px" : "90px",
    spend: compact ? "72px" : "80px",
    roas: compact ? "56px" : "65px",
    cpa: compact ? "62px" : "65px",
    aov: compact ? "62px" : "70px",
    name: compact ? "200px" : "240px",
  };
  const ROW_GAP = compact ? "5px" : "10px";

  const COLS: ColDef[] = useMemo(() => {
    const out: ColDef[] = [];
    const fmtMoney = (v: number) => `${cs}${Math.round(v).toLocaleString()}`;
    const fmtCount = (v: number) => v.toLocaleString();
    const fmtRoas = (v: number) => v > 0 ? `${v.toFixed(2)}x` : "\u2014";
    const fmtCpa = (v: number) => v > 0 ? fmtMoney(v) : "\u2014";

    // Orders cluster
    for (const seg of orderedSegments) {
      out.push({
        key: `${seg}_orders`,
        label: `${SEGMENT_PREFIX[seg]}Orders`,
        width: W.orders,
        format: (r) => fmtCount(segOrders(r, seg)),
        tooltip: seg === "all"
          ? "All Meta-attributed orders for this entity in the selected period."
          : seg === "new"
            ? "Orders placed by first-time customers (their first ever order in this period)."
            : "Orders from returning customers - everyone who had ordered before. = All − New.",
      });
    }
    // Revenue cluster
    for (const seg of orderedSegments) {
      out.push({
        key: `${seg}_revenue`,
        label: `${SEGMENT_PREFIX[seg]}Revenue`,
        width: W.revenue,
        format: (r) => fmtMoney(segRevenue(r, seg)),
        isRevenue: true,
        tooltip: seg === "all"
          ? "Total Meta-attributed revenue."
          : seg === "new"
            ? "Revenue from new customers acquired by this entity."
            : "Revenue from returning customers attributed to this entity.",
      });
    }
    // Spend (segment-independent - same ad spend whoever placed the order)
    out.push({
      key: "spend",
      label: "Spend",
      width: W.spend,
      format: (r) => fmtMoney(r.spend || 0),
      tooltip: "Meta ad spend for this entity in the selected period.",
    });
    // ROAS cluster
    for (const seg of orderedSegments) {
      out.push({
        key: `${seg}_roas`,
        label: `${SEGMENT_PREFIX[seg]}ROAS`,
        width: W.roas,
        format: (r) => fmtRoas(getValue(r, `${seg}_roas`)),
        tooltip: seg === "all"
          ? "Return on ad spend. All revenue ÷ spend."
          : seg === "new"
            ? "ROAS from new customers only. New revenue ÷ spend."
            : "ROAS from returning customers only. Existing revenue ÷ spend.",
      });
    }
    // Cost columns: only show the two that are conceptually meaningful.
    //   • All Customers → "Cost per Order" (CpO): blended cost per attributed
    //     order. Useful as a stable benchmark across the entity.
    //   • New Customers → "Cost per Acquisition" (CAC): the textbook customer
    //     acquisition cost - the genuinely actionable cost metric.
    // Existing-customer "CPA" was previously rendered too, but for an existing
    // customer we don't pay an acquisition cost per repeat order - the spend
    // bought the *first* order. Dividing total spend by repeat-order count
    // produces a number that's mathematically valid but interpretively
    // misleading, so we drop the column entirely.
    if (selectedSegments.has("all")) {
      out.push({
        key: `all_cpa`,
        label: `CpO`,
        width: W.cpa,
        format: (r) => fmtCpa(getValue(r, `all_cpa`)),
        tooltip: "Cost per Order. Total spend ÷ all attributed orders. The blended cost of producing any order, new or repeat.",
      });
    }
    if (selectedSegments.has("new")) {
      out.push({
        key: `new_cpa`,
        label: `CAC`,
        width: W.cpa,
        format: (r) => fmtCpa(getValue(r, `new_cpa`)),
        tooltip: "Cost per Acquisition (CAC). Total spend ÷ new-customer orders - what it costs to acquire one first-time customer.",
      });
    }
    // AOV cluster
    for (const seg of orderedSegments) {
      out.push({
        key: `${seg}_aov`,
        label: `${SEGMENT_PREFIX[seg]}AOV`,
        width: W.aov,
        format: (r) => {
          const v = getValue(r, `${seg}_aov`);
          return v > 0 ? fmtMoney(v) : "\u2014";
        },
        tooltip: seg === "all"
          ? "Average order value across all attributed orders. Revenue ÷ orders."
          : seg === "new"
            ? "Average order value of new customers' first order."
            : "Average order value of returning customers in this period.",
      });
    }
    return out;
  }, [orderedSegments, cs]);

  const entityNoun = entityType === "campaign" ? "campaigns" : entityType === "adset" ? "ad sets" : "ads";

  // Note: we don't early-return on sorted.length === 0. Doing so collapses the
  // entire filter bar, which means a user who typed a term that matches
  // nothing loses the search box itself and can't recover without clicking
  // away. Instead, render the filter UI normally and show an inline empty
  // state inside the table body below.
  const hasActiveFilter = !!searchQuery.trim() || demoActive || selectedSegments.size < 3;

  // Footer aggregates: orders/revenue sum directly; ROAS/CPA/AOV are recomputed
  // from group totals (correct weighted average, not arithmetic mean of rows).
  const aggregateFor = (col: ColDef): string => {
    const fmtMoney = (v: number) => `${cs}${Math.round(v).toLocaleString()}`;
    const totalSpend = sorted.reduce((s, r) => s + (r.spend || 0), 0);

    if (col.key === "spend") return fmtMoney(totalSpend);

    const [seg, metric] = col.key.split("_") as [Segment, string];
    const totalOrders = sorted.reduce((s, r) => s + segOrders(r, seg), 0);
    const totalRev = sorted.reduce((s, r) => s + segRevenue(r, seg), 0);

    if (metric === "orders") return totalOrders.toLocaleString();
    if (metric === "revenue") return fmtMoney(totalRev);
    if (metric === "roas") return totalSpend > 0 && totalRev > 0 ? `${(totalRev / totalSpend).toFixed(2)}x` : "\u2014";
    if (metric === "cpa") return totalOrders > 0 ? fmtMoney(totalSpend / totalOrders) : "\u2014";
    if (metric === "aov") return totalOrders > 0 ? fmtMoney(totalRev / totalOrders) : "\u2014";
    return "";
  };

  // Cluster boundaries (for thin separator lines between metric groups).
  const isClusterBoundary = (idx: number): boolean => {
    if (idx === 0) return false;
    const prev = COLS[idx - 1].key;
    const curr = COLS[idx].key;
    const prevMetric = prev === "spend" ? "spend" : prev.split("_")[1];
    const currMetric = curr === "spend" ? "spend" : curr.split("_")[1];
    return prevMetric !== currMetric;
  };

  // Visual style for the Show / Gender / Age filter rows. Pill buttons match
  // the Product Demographics Explorer aesthetic so the two filter UIs read as
  // one design language across the app.
  const labelStyle: React.CSSProperties = {
    fontSize: "var(--l-font-sm)", fontWeight: 600, color: "var(--l-text-secondary)",
    width: "70px", textTransform: "uppercase", letterSpacing: "0.5px",
  };
  const pillClass = (active: boolean) => `l-pill${active ? " l-pill--active" : ""}`;

  const showAgeRow = entityType === "ad" && (availableAges.size > 0 || ageFilter.length > 0);
  const showGenderRow = entityType === "ad";

  return (
    <div>
      {/* Filter bar - mirrors Product Demographics Explorer:
          row 1 = Show segments, row 2 = Gender, row 3 = Age, search aligned right. */}
      <div style={{ display: "flex", flexDirection: "column", gap: "10px", marginBottom: "12px" }}>
        {/* Show row + search */}
        <div style={{ display: "flex", alignItems: "center", gap: "10px", flexWrap: "wrap" }}>
          <span style={labelStyle}>Show</span>
          {(["all", "new", "existing"] as const).map(s => {
            const active = selectedSegments.has(s);
            return (
              <button
                key={s}
                onClick={() => toggleSegment(s)}
                title={active && selectedSegments.size === 1 ? "At least one segment must stay selected" : `${active ? "Hide" : "Show"} ${SEGMENT_NAMES[s]} columns`}
                className={pillClass(active)}
              >
                {SEGMENT_NAMES[s]}
              </button>
            );
          })}
          {!showAgeRow && (
            <>
              <span style={{ flex: 1 }} />
              <input
                type="search"
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                placeholder={`Search ${entityNoun}...`}
                style={{
                  fontSize: "12px", padding: "6px 12px", borderRadius: "6px",
                  border: "1px solid #E5E7EB", background: "#fff", color: "#374151",
                  outline: "none", minWidth: "200px", fontWeight: 500,
                }}
              />
            </>
          )}
        </div>

        {showGenderRow && (
          <div style={{ display: "flex", alignItems: "center", gap: "10px", flexWrap: "wrap" }}>
            <span style={labelStyle}>Gender</span>
            {(["All", "Female", "Male"] as const).map((g) => (
              <button
                key={g}
                onClick={() => setGenderFilter(g)}
                className={pillClass(genderFilter === g)}
              >
                {g}
              </button>
            ))}
          </div>
        )}

        {showAgeRow && (
          <div style={{ display: "flex", alignItems: "center", gap: "10px", flexWrap: "wrap" }}>
            <span style={labelStyle}>Age</span>
            <button
              onClick={() => setAgeFilter([])}
              className={pillClass(ageFilter.length === 0)}
            >
              All
            </button>
            {ageBracketsToRender.map(b => (
              <button
                key={b}
                onClick={() => toggleAge(b)}
                className={pillClass(ageFilter.includes(b))}
              >
                {b}
              </button>
            ))}
            {(genderFilter !== "All" || ageFilter.length > 0) && (
              <button
                onClick={() => { setGenderFilter("All"); setAgeFilter([]); }}
                style={{
                  padding: "6px 12px", fontSize: "12px", fontWeight: 500,
                  borderRadius: "6px", cursor: "pointer",
                  background: "transparent", color: "#6B7280",
                  border: "1px solid transparent", textDecoration: "underline",
                }}
              >
                Clear filters
              </button>
            )}
            <span style={{ width: "24px", flexShrink: 0 }} />
            <input
              type="search"
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              placeholder={`Search ${entityNoun}...`}
              style={{
                fontSize: "12px", padding: "6px 12px", borderRadius: "6px",
                border: "1px solid #E5E7EB", background: "#fff", color: "#374151",
                outline: "none", minWidth: "200px", fontWeight: 500,
              }}
            />
          </div>
        )}

        {demoActive && (
          <div style={{ fontSize: "11.5px", color: "#7C3AED", fontWeight: 500 }}>
            Showing orders/revenue for selected demographic only. Spend is unchanged - Meta does not split spend by customer demographic.
          </div>
        )}
      </div>

      {/* Table - horizontally scrollable when columns exceed container width
          (selecting all 3 segments produces 16 columns, more than fits at
          standard widths). */}
      <div style={{ overflowX: "auto" }}>
        <div style={{ minWidth: "fit-content" }}>
          {/* Table header */}
          <div style={{ display: "flex", gap: ROW_GAP, alignItems: "center", padding: "0 0 8px 0", borderBottom: "1px solid #e4e5e7", marginBottom: "4px" }}>
            <span style={{ width: "28px", flexShrink: 0 }} />
            {entityType === "ad" && <span style={{ width: "36px", flexShrink: 0 }} />}
            <span style={{ flex: 1, fontSize: "11px", color: "#8c9196", fontWeight: 600, textTransform: "uppercase", minWidth: W.name }}>Name</span>
            {COLS.map((c, idx) => (
              <span key={c.key} style={{ display: "flex", alignItems: "center", flexShrink: 0 }}>
                {isClusterBoundary(idx) && (
                  <span style={{ width: "1px", height: "14px", background: "#e4e5e7", marginRight: "6px", marginLeft: "-2px" }} />
                )}
                <SortableHeader col={c.key} label={c.label} width={c.width} sortCol={sortCol} sortDir={sortDir} onSort={toggleSort} tooltip={c.tooltip} />
              </span>
            ))}
          </div>

          {/* Table body */}
          <div style={{ maxHeight: "480px", overflowY: "auto", display: "flex", flexDirection: "column", gap: "2px", paddingRight: "4px" }}>
            {sorted.length === 0 && (
              <div style={{
                padding: "32px 16px", textAlign: "center", color: "#6B7280", fontSize: "13px",
              }}>
                {hasActiveFilter
                  ? `No ${entityNoun} match the current filters${searchQuery.trim() ? ` and search "${searchQuery.trim()}"` : ""}.`
                  : "No data for selected period."}
              </div>
            )}
            {sorted.map((item, i) => {
              const interactive = !!item.id && !!entityType;
              const isExpanded = expandedId === item.id;
              return (
                <div key={item.id || i}>
                  <div
                    onClick={() => {
                      if (!interactive) return;
                      toggleExpand(item.id, entityType);
                    }}
                    title={interactive ? (isExpanded ? "Click to collapse" : "Click to expand timeline") : undefined}
                    style={{
                      display: "flex", alignItems: "center", gap: ROW_GAP,
                      cursor: interactive ? "pointer" : "default",
                      padding: "6px 4px", borderRadius: 4,
                      background: isExpanded ? "#EEF2FF" : (i % 2 === 0 ? "#fff" : "#f9fafb"),
                      transition: "background 0.12s ease",
                    }}
                    onMouseOver={(e) => { if (interactive && !isExpanded) (e.currentTarget as HTMLDivElement).style.background = "#f1f5f9"; }}
                    onMouseOut={(e) => { if (!isExpanded) (e.currentTarget as HTMLDivElement).style.background = i % 2 === 0 ? "#fff" : "#f9fafb"; }}
                  >
                    <span style={{ fontSize: "11px", color: isExpanded ? "#4338CA" : "#9CA3AF", width: "28px", textAlign: "right", flexShrink: 0 }}>
                      {isExpanded ? "▾" : i + 1}
                    </span>
                    {entityType === "ad" && (
                      <AdThumbTile
                        thumbnailUrl={item.thumbnailUrl}
                        imageUrl={item.imageUrl}
                        name={item.name}
                        isDpa={!!item.productSetId}
                      />
                    )}
                    {/* Name + sort-driven bar stacked vertically. The bar
                        sits within the name's flex region so it never runs
                        behind the stat columns - those remain on the right
                        with their fixed widths. Bar length is a percentage
                        of the row's value relative to the visible max for
                        the active sort column. */}
                    <span style={{
                      flex: 1, minWidth: W.name,
                      display: "flex", flexDirection: "column", gap: "3px",
                      overflow: "hidden",
                    }}>
                      <span style={{
                        fontSize: "13px", fontWeight: 500,
                        overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
                      }} title={item.name}>
                        {item.name}
                      </span>
                      <span style={{
                        position: "relative", height: "5px",
                        background: "#F1F5F9", borderRadius: "3px",
                        overflow: "hidden",
                      }}>
                        <span style={{
                          display: "block", height: "100%",
                          width: `${widthPctFor(item)}%`,
                          background: `linear-gradient(90deg, ${barColors.from}, ${barColors.to})`,
                          borderRadius: "3px", transition: "width 0.4s ease",
                        }} />
                      </span>
                    </span>
                    {COLS.map((c, idx) => {
                      const numVal = getValue(item, c.key);
                      return (
                        <span key={c.key} style={{ display: "flex", alignItems: "center", flexShrink: 0 }}>
                          {isClusterBoundary(idx) && (
                            <span style={{ width: "1px", height: "14px", background: "#f1f2f4", marginRight: "6px", marginLeft: "-2px" }} />
                          )}
                          <span style={{
                            fontSize: "12.5px", textAlign: "right", flexShrink: 0,
                            width: c.width, color: numVal > 0 ? "#374151" : "#bbb",
                            fontWeight: c.isRevenue ? 600 : 400,
                          }}>
                            {c.format(item)}
                          </span>
                        </span>
                      );
                    })}
                  </div>
                  {isExpanded && (
                    <ExpandedRow
                      cs={cs}
                      sortCol={sortCol}
                      payload={timelineCache.get(item.id) || null}
                      loading={loadingIds.has(item.id)}
                      error={errorIds.get(item.id) || null}
                    />
                  )}
                </div>
              );
            })}
          </div>

          {/* Footer summary */}
          <div style={{
            display: "flex", gap: ROW_GAP, alignItems: "center", padding: "8px 4px 0",
            borderTop: "1px solid #e4e5e7", marginTop: "4px", fontWeight: 600, fontSize: "12.5px",
          }}>
            <span style={{ width: "28px", flexShrink: 0 }} />
            {entityType === "ad" && <span style={{ width: "36px", flexShrink: 0 }} />}
            <span style={{ flex: 1, color: "#374151", minWidth: W.name }}>{sorted.length} {entityNoun}</span>
            {COLS.map((c, idx) => (
              <span key={c.key} style={{ display: "flex", alignItems: "center", flexShrink: 0 }}>
                {isClusterBoundary(idx) && (
                  <span style={{ width: "1px", height: "14px", background: "#e4e5e7", marginRight: "6px", marginLeft: "-2px" }} />
                )}
                <span style={{ width: c.width, textAlign: "right", flexShrink: 0, color: "#374151" }}>
                  {aggregateFor(c)}
                </span>
              </span>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}

// ── ExpandedRow ─────────────────────────────────────────────────────────
// Inline expansion under a clicked Ad Explorer row. Replaces the slide-out
// drawer for explorer interactions: the table stays interactive, sort/search
// keep working while a row is open, and the embedded sparkline live-updates
// when the active sort metric changes (no re-fetch - daily payload carries
// every metric).
//
// Data shape mirrors /app/api/entity-timeline. Sparkline plots two series:
//   • Spend (always, light line so the user sees cost context)
//   • Active sort metric (bold line) - derived from sortCol on the parent
// Hovering the chart shows a vertical guide + tooltip with date/value.
function ExpandedRow({ cs, sortCol, payload, loading, error }: {
  cs: string;
  sortCol: string;
  payload: TimelinePayload | null;
  loading: boolean;
  error: string | null;
}) {
  if (loading) {
    return (
      <div style={{ padding: "16px 20px", background: "#F9FAFB", borderLeft: "3px solid #5C6AC4", color: "#6B7280", fontSize: 12 }}>
        Loading timeline…
      </div>
    );
  }
  if (error) {
    return (
      <div style={{ padding: "16px 20px", background: "#FEF2F2", borderLeft: "3px solid #B91C1C", color: "#B91C1C", fontSize: 12 }}>
        Couldn't load timeline: {error}
      </div>
    );
  }
  if (!payload) return null;

  const { entity, daily, events } = payload;
  // Derive the active metric series from sortCol. sortCol is one of:
  //   "spend"               → daily.spend
  //   "{seg}_{metric}"      → see mapping below
  // Falls back to spend when the sort key isn't representable per-day
  // (defensive - all current sort keys are representable).
  const metricSeries = useMemo<{ label: string; values: number[]; isMoney: boolean }>(() => {
    const seg = sortCol === "spend" ? null : (sortCol.split("_")[0] as Segment);
    const metric = sortCol === "spend" ? "spend" : sortCol.split("_")[1];

    const dayOrders = (d: TimelineDay): number => {
      if (seg === "new") return d.newCustomerOrders || 0;
      if (seg === "existing") return d.existingCustomerOrders || 0;
      return d.orders || 0;
    };
    const dayRevenue = (d: TimelineDay): number => {
      if (seg === "new") return d.newCustomerRevenue || 0;
      if (seg === "existing") return d.existingCustomerRevenue || 0;
      return d.revenue || 0;
    };

    const segLabel = seg === "new" ? "New " : seg === "existing" ? "Existing " : (seg === "all" ? "All " : "");

    if (metric === "spend") {
      return { label: "Spend", values: daily.map(d => d.spend), isMoney: true };
    }
    if (metric === "orders") {
      return { label: `${segLabel}Orders`, values: daily.map(dayOrders), isMoney: false };
    }
    if (metric === "revenue") {
      return { label: `${segLabel}Revenue`, values: daily.map(dayRevenue), isMoney: true };
    }
    if (metric === "roas") {
      return {
        label: `${segLabel}ROAS`, isMoney: false,
        values: daily.map(d => d.spend > 0 ? dayRevenue(d) / d.spend : 0),
      };
    }
    if (metric === "cpa") {
      return {
        label: `${segLabel}CPA`, isMoney: true,
        values: daily.map(d => dayOrders(d) > 0 ? d.spend / dayOrders(d) : 0),
      };
    }
    if (metric === "aov") {
      return {
        label: `${segLabel}AOV`, isMoney: true,
        values: daily.map(d => dayOrders(d) > 0 ? dayRevenue(d) / dayOrders(d) : 0),
      };
    }
    return { label: "Spend", values: daily.map(d => d.spend), isMoney: true };
  }, [sortCol, daily]);

  const fmtMoney = (v: number) => `${cs}${Math.round(v).toLocaleString()}`;
  const fmtNum = (v: number) => v >= 100 ? Math.round(v).toLocaleString() : v.toFixed(2);
  const fmtVal = (v: number, isMoney: boolean) => isMoney ? fmtMoney(v) : fmtNum(v);

  const fmtDate = (iso: string | null) => {
    if (!iso) return "—";
    return new Date(iso).toLocaleDateString("en-GB", { day: "2-digit", month: "short", year: "numeric" });
  };

  const recentEvents = events.slice(0, 5);

  return (
    <div style={{
      padding: "12px 16px 16px", background: "#F9FAFB",
      borderLeft: "3px solid #5C6AC4", borderBottomLeftRadius: 4, borderBottomRightRadius: 4,
      marginBottom: 4,
    }}>
      {/* Lifecycle strip */}
      <div style={{ display: "flex", gap: 18, fontSize: 12, color: "#374151", flexWrap: "wrap", marginBottom: 10 }}>
        <span><span style={{ color: "#6B7280" }}>Status: </span><b>{entity.currentStatus || "—"}</b></span>
        <span><span style={{ color: "#6B7280" }}>Created: </span>{fmtDate(entity.createdTime)}</span>
        <span><span style={{ color: "#6B7280" }}>First delivery: </span>{fmtDate(entity.effectiveStartAt)}</span>
        <span><span style={{ color: "#6B7280" }}>Last delivery: </span>{fmtDate(entity.effectiveEndAt)}</span>
      </div>

      {/* Sparkline */}
      <ExpandedSparkline
        daily={daily}
        spendSeries={daily.map(d => d.spend)}
        metricSeries={metricSeries.values}
        metricLabel={metricSeries.label}
        metricIsMoney={metricSeries.isMoney}
        fmtVal={fmtVal}
      />

      {/* Recent change events */}
      {recentEvents.length > 0 && (
        <div style={{ marginTop: 12 }}>
          <div style={{ fontSize: 11, textTransform: "uppercase", letterSpacing: 0.5, color: "#6B7280", marginBottom: 6 }}>
            Recent changes ({events.length})
          </div>
          <ul style={{ listStyle: "none", padding: 0, margin: 0, display: "flex", flexDirection: "column", gap: 4 }}>
            {recentEvents.map(ev => (
              <li key={ev.id} style={{ fontSize: 12, color: "#374151", display: "flex", gap: 8 }}>
                <span style={{ color: "#9CA3AF", fontFamily: "monospace", fontSize: 11, flexShrink: 0, width: 90 }}>
                  {new Date(ev.eventTimeISO).toLocaleDateString("en-GB", { day: "2-digit", month: "short", year: "2-digit" })}
                </span>
                <span style={{ flex: 1 }}>{ev.summary}</span>
              </li>
            ))}
          </ul>
        </div>
      )}
    </div>
  );
}

function ExpandedSparkline({
  daily, spendSeries, metricSeries, metricLabel, metricIsMoney, fmtVal,
}: {
  daily: TimelineDay[];
  spendSeries: number[];
  metricSeries: number[];
  metricLabel: string;
  metricIsMoney: boolean;
  fmtVal: (v: number, isMoney: boolean) => string;
}) {
  const [hoverIdx, setHoverIdx] = useState<number | null>(null);

  if (daily.length === 0) {
    return <div style={{ fontSize: 12, color: "#6B7280", padding: "8px 0" }}>No daily data in the last 90 days.</div>;
  }

  const w = 720, h = 110, padX = 8, padTop = 14, padBottom = 18;
  const innerH = h - padTop - padBottom;
  const stepX = (w - padX * 2) / Math.max(1, daily.length - 1);

  const maxSpend = Math.max(...spendSeries, 1);
  const maxMetric = Math.max(...metricSeries, 1e-9);

  const yFor = (v: number, max: number) => padTop + innerH - (v / max) * innerH;

  const spendPath = spendSeries.map((v, i) =>
    `${i === 0 ? "M" : "L"}${padX + i * stepX},${yFor(v, maxSpend)}`).join(" ");
  const metricPath = metricSeries.map((v, i) =>
    `${i === 0 ? "M" : "L"}${padX + i * stepX},${yFor(v, maxMetric)}`).join(" ");

  // Mouse → nearest data index. SVG is responsive (preserveAspectRatio=none),
  // so we work in viewBox units by mapping clientX through getBoundingClientRect.
  const onMove = (e: React.MouseEvent<SVGSVGElement>) => {
    const rect = (e.currentTarget as SVGSVGElement).getBoundingClientRect();
    const xVB = ((e.clientX - rect.left) / rect.width) * w;
    const idx = Math.max(0, Math.min(daily.length - 1, Math.round((xVB - padX) / stepX)));
    setHoverIdx(idx);
  };

  const hover = hoverIdx != null ? daily[hoverIdx] : null;
  const hoverX = hoverIdx != null ? padX + hoverIdx * stepX : 0;

  return (
    <div style={{ position: "relative" }}>
      <div style={{ display: "flex", gap: 16, fontSize: 11, color: "#6B7280", marginBottom: 4 }}>
        <span><span style={{ display: "inline-block", width: 10, height: 2, background: "#94A3B8", verticalAlign: "middle", marginRight: 4 }} />Spend</span>
        <span><span style={{ display: "inline-block", width: 10, height: 2, background: "#5C6AC4", verticalAlign: "middle", marginRight: 4 }} /><b style={{ color: "#374151" }}>{metricLabel}</b></span>
        <span style={{ marginLeft: "auto", color: "#9CA3AF" }}>last {daily.length} days</span>
      </div>
      <svg
        width="100%" height={h} viewBox={`0 0 ${w} ${h}`} preserveAspectRatio="none"
        onMouseMove={onMove}
        onMouseLeave={() => setHoverIdx(null)}
        style={{ display: "block", background: "#fff", border: "1px solid #E5E7EB", borderRadius: 4, cursor: "crosshair" }}
      >
        <path d={spendPath} stroke="#94A3B8" strokeWidth="1" fill="none" />
        <path d={metricPath} stroke="#5C6AC4" strokeWidth="1.6" fill="none" />
        {hoverIdx != null && (
          <>
            <line x1={hoverX} y1={padTop} x2={hoverX} y2={h - padBottom} stroke="#9CA3AF" strokeDasharray="2 3" />
            <circle cx={hoverX} cy={yFor(spendSeries[hoverIdx], maxSpend)} r={3} fill="#94A3B8" />
            <circle cx={hoverX} cy={yFor(metricSeries[hoverIdx], maxMetric)} r={3.5} fill="#5C6AC4" />
          </>
        )}
      </svg>
      {hover && (
        <div style={{
          position: "absolute", top: 22, left: `${Math.min(85, Math.max(2, (hoverX / w) * 100))}%`,
          transform: "translateX(-50%)",
          background: "#111827", color: "#fff", padding: "6px 8px", borderRadius: 4,
          fontSize: 11, lineHeight: 1.4, pointerEvents: "none", whiteSpace: "nowrap",
          boxShadow: "0 4px 12px rgba(0,0,0,0.15)",
        }}>
          <div style={{ fontWeight: 600 }}>{new Date(hover.date + "T12:00:00Z").toLocaleDateString("en-GB", { day: "2-digit", month: "short", year: "numeric" })}</div>
          <div>Spend: {fmtVal(spendSeries[hoverIdx!], true)}</div>
          <div>{metricLabel}: {fmtVal(metricSeries[hoverIdx!], metricIsMoney)}</div>
        </div>
      )}
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// Ad Funnel Tree - per-stage tree of ad sets with their individual ads.
// Each ad set sits in the cold/warm/hot band determined by its targeting
// spec (see classifyFunnelStage in metaEntitySync.server.js); ads are
// children of their parent ad set. Click an ad set or ad to drill in.
// ═══════════════════════════════════════════════════════════════════════

const FUNNEL_STAGES = [
  {
    key: "cold", label: "TOF · Cold", sublabel: "Prospecting",
    pool: "Lookalikes, interest/broad targeting, Advantage+ broad audience, ad sets that exclude existing customers, or no custom audiences at all.",
    color: "#1D4ED8", accent: "#3B82F6", bg: "#EFF6FF", surface: "#F8FAFC", border: "#BFDBFE",
  },
  {
    key: "warm", label: "MOF · Warm", sublabel: "Retargeting",
    pool: "Custom audiences of site visitors, video viewers, page/profile engagers, lead lists, abandoned-browsers - anyone who has interacted but not bought.",
    color: "#B45309", accent: "#D97706", bg: "#FFFBEB", surface: "#FEFAF1", border: "#FDE68A",
  },
  {
    key: "hot", label: "BOF · Hot", sublabel: "Conversion / Re-purchase",
    pool: "Past purchasers, ATC + Initiate-Checkout abandoners, VIP / repeat-customer lists, DPA targeting recent product viewers and existing-customer lookbacks.",
    color: "#B91C1C", accent: "#DC2626", bg: "#FEF2F2", surface: "#FFF7F7", border: "#FCA5A5",
  },
] as const;

function statusColor(status: string | null): string {
  switch (status) {
    case "ACTIVE": return "#10B981";
    case "PAUSED": return "#F59E0B";
    case "ARCHIVED": case "DELETED": return "#9CA3AF";
    case "SCHEDULED": return "#6366F1";
    default: return "#9CA3AF";
  }
}

type SortKey = "spend" | "roas" | "newCustomers";
type FilterKey = "all" | "active" | "spending";

// "Top Ads for New Customers" - Instagram-style 5×2 grid showing the 10 best
// ads by either New Customer orders or ROAS within the selected window.
//
// Why a min-orders gate on the ROAS tab: a single high-AOV order on £5 of
// spend produces a 50x+ ROAS that is statistical noise, not a top performer.
// Requiring ≥3 new customer orders filters those out so the ROAS view
// surfaces ads that converted *consistently*, not just luckily.
function TopAdsForNewCustomersTile({ adRows, cs, onAdClick }: {
  adRows: any[];
  cs: string;
  onAdClick?: (id: string, name: string) => void;
}) {
  const [sortMode, setSortMode] = useState<"orders" | "roas" | "revenue" | "cac">("orders");
  // Min-order gate for ratio metrics (ROAS / CAC). A single order at low spend
  // produces statistically meaningless extremes; the gate ensures the
  // leaderboard reflects ads that converted *consistently*, not just luckily.
  const MIN_ORDERS_FOR_RATIO = 3;

  const topAds = useMemo(() => {
    const eligible = adRows.filter(a => (a.newCustomerOrders || 0) > 0);
    if (sortMode === "orders") {
      return eligible
        .slice()
        .sort((a, b) => (b.newCustomerOrders || 0) - (a.newCustomerOrders || 0))
        .slice(0, 10);
    }
    if (sortMode === "revenue") {
      return eligible
        .slice()
        .sort((a, b) => (b.newCustomerRevenue || 0) - (a.newCustomerRevenue || 0))
        .slice(0, 10);
    }
    if (sortMode === "cac") {
      return eligible
        .filter(a => (a.newCustomerOrders || 0) >= MIN_ORDERS_FOR_RATIO && a.newCustomerCPA != null)
        .slice()
        // Lower CAC is better - ascending sort.
        .sort((a, b) => (a.newCustomerCPA || Infinity) - (b.newCustomerCPA || Infinity))
        .slice(0, 10);
    }
    // ROAS - gate by minimum order volume to exclude single-order outliers.
    return eligible
      .filter(a => (a.newCustomerOrders || 0) >= MIN_ORDERS_FOR_RATIO)
      .slice()
      .sort((a, b) => (b.newCustomerROAS || 0) - (a.newCustomerROAS || 0))
      .slice(0, 10);
  }, [adRows, sortMode]);

  const fmtPrice = (v: number) => `${cs}${v.toLocaleString(undefined, { maximumFractionDigits: 0 })}`;
  const fmtRoas = (v: number) => `${v.toFixed(2)}x`;

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "16px" }}>
      {/* Sort tab row - kept simple to match the "Instagram explore" feel. */}
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: "12px", flexWrap: "wrap" }}>
        <div style={{ display: "inline-flex", gap: "4px" }}>
          {([
            { id: "orders", label: "Orders" },
            { id: "roas", label: "ROAS" },
            { id: "revenue", label: "Revenue" },
            { id: "cac", label: "CAC" },
          ] as const).map(o => (
            <button key={o.id} onClick={() => setSortMode(o.id)} className={`l-pill${sortMode === o.id ? " l-pill--active" : ""}`}>{o.label}</button>
          ))}
        </div>
        <div style={{ fontSize: "11px", color: "#6B7280" }}>
          {sortMode === "roas" && `Top 10 by New Customer ROAS · min ${MIN_ORDERS_FOR_RATIO} orders to qualify`}
          {sortMode === "cac" && `Top 10 lowest New Customer CAC · min ${MIN_ORDERS_FOR_RATIO} orders to qualify`}
          {sortMode === "orders" && "Top 10 by New Customer Orders"}
          {sortMode === "revenue" && "Top 10 by New Customer Revenue"}
        </div>
      </div>

      {topAds.length === 0 ? (
        <div style={{ padding: "32px", textAlign: "center", color: "#6B7280", fontSize: "13px" }}>
          {(sortMode === "roas" || sortMode === "cac")
            ? `No ads in this window cleared the ${MIN_ORDERS_FOR_RATIO}-order threshold yet.`
            : "No new-customer orders attributed to ads in this window."}
        </div>
      ) : (
        <div style={{
          display: "grid", gap: "16px",
          gridTemplateColumns: "repeat(5, minmax(0, 1fr))",
        }}>
          {topAds.map((ad, i) => (
            <TopAdCard
              key={ad.id}
              rank={i + 1}
              ad={ad}
              fmtPrice={fmtPrice}
              fmtRoas={fmtRoas}
              onClick={onAdClick ? () => onAdClick(ad.id, ad.name) : undefined}
            />
          ))}
        </div>
      )}
    </div>
  );
}

function TopAdCard({ rank, ad, fmtPrice, fmtRoas, onClick }: {
  rank: number;
  ad: any;
  fmtPrice: (v: number) => string;
  fmtRoas: (v: number) => string;
  onClick?: () => void;
}) {
  const [imgFailed, setImgFailed] = useState(false);
  // DPA ads have no per-ad creative image - Meta hands back a 64x64 grey
  // placeholder PNG which reads as a blank tile at card size. Treat any ad
  // with productSetId as DPA regardless of whether thumbnailUrl is set, so
  // the branded DPA tile renders consistently.
  const isDpa = !!ad.productSetId;
  const imgSrc = isDpa ? "/dpa-thumbnail.jpg" : (ad.imageUrl || ad.thumbnailUrl);
  const showImg = imgSrc && !imgFailed;

  const cac = ad.newCustomerOrders > 0 ? ad.spend / ad.newCustomerOrders : null;
  const roas = ad.newCustomerROAS || 0;

  return (
    <div
      onClick={onClick}
      style={{
        border: "1px solid #E5E7EB",
        background: "#fff", overflow: "hidden",
        cursor: onClick ? "pointer" : "default",
        transition: "transform 0.15s, box-shadow 0.15s",
        display: "flex", flexDirection: "column",
      }}
      onMouseEnter={e => {
        if (!onClick) return;
        e.currentTarget.style.transform = "translateY(-2px)";
        e.currentTarget.style.boxShadow = "0 4px 12px rgba(0,0,0,0.08)";
      }}
      onMouseLeave={e => {
        if (!onClick) return;
        e.currentTarget.style.transform = "translateY(0)";
        e.currentTarget.style.boxShadow = "none";
      }}
    >
      {/* Square image area - dominant visual, mimics IG post aspect. */}
      <div style={{
        position: "relative", width: "100%", paddingTop: "100%",
        background: "#F3F4F6", overflow: "hidden",
      }}>
        {/* Rank badge - top-left, always visible. */}
        <div style={{
          position: "absolute", top: "8px", left: "8px", zIndex: 2,
          background: "rgba(0,0,0,0.75)", color: "#fff",
          width: "26px", height: "26px", borderRadius: "50%",
          display: "flex", alignItems: "center", justifyContent: "center",
          fontSize: "12px", fontWeight: 700,
        }}>{rank}</div>
        {/* DPA badge - top-right, only when applicable. */}
        {isDpa && (
          <div title="Dynamic Product Ad" style={{
            position: "absolute", top: "8px", right: "8px", zIndex: 2,
            background: "#7C3AED", color: "#fff",
            padding: "3px 8px", borderRadius: "10px",
            fontSize: "10px", fontWeight: 700, letterSpacing: "0.3px",
          }}>DPA</div>
        )}
        {showImg ? (
          <img
            src={imgSrc}
            alt={ad.name}
            onError={() => setImgFailed(true)}
            style={{
              position: "absolute", top: 0, left: 0,
              width: "100%", height: "100%", objectFit: "cover",
            }}
          />
        ) : (
          <div style={{
            position: "absolute", top: 0, left: 0,
            width: "100%", height: "100%",
            display: "flex", alignItems: "center", justifyContent: "center",
            background: "linear-gradient(135deg, #E0E7FF 0%, #F3F4F6 100%)",
            color: "#6B7280", fontSize: "32px", fontWeight: 700,
          }}>
            {(ad.name || "?").charAt(0).toUpperCase()}
          </div>
        )}
      </div>

      {/* Stats panel - 4 numbers + name caption underneath the image. */}
      <div style={{ padding: "10px 12px", display: "flex", flexDirection: "column", gap: "8px" }}>
        <div title={ad.name} style={{
          fontSize: "12px", fontWeight: 600, color: "#1F2937",
          overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
        }}>{ad.name}</div>
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "6px 10px" }}>
          <Stat label="Orders" value={String(ad.newCustomerOrders || 0)} />
          <Stat label="Revenue" value={fmtPrice(ad.newCustomerRevenue || 0)} />
          <Stat label="ROAS" value={fmtRoas(roas)} />
          <Stat label="CAC" value={cac != null ? fmtPrice(cac) : "—"} />
        </div>
      </div>
    </div>
  );
}

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "1px" }}>
      <span style={{ fontSize: "10px", color: "#6B7280", fontWeight: 500, textTransform: "uppercase", letterSpacing: "0.4px" }}>{label}</span>
      <span style={{ fontSize: "13px", fontWeight: 700, color: "#111827" }}>{value}</span>
    </div>
  );
}

function AdFunnelTreeTile({ funnelTree, stageTotals, cs, onAdSetClick, onAdClick }: {
  funnelTree: { cold: any[]; warm: any[]; hot: any[] };
  stageTotals: { cold: any; warm: any; hot: any };
  cs: string;
  onAdSetClick?: (id: string, name: string) => void;
  onAdClick?: (id: string, name: string) => void;
}) {
  const [sortKey, setSortKey] = useState<SortKey>("spend");
  const [filterKey, setFilterKey] = useState<FilterKey>("all");
  const [expandedAdsetId, setExpandedAdsetId] = useState<string | null>(null);

  const totalSpend = (stageTotals.cold?.spend || 0) + (stageTotals.warm?.spend || 0) + (stageTotals.hot?.spend || 0);
  const hasData = totalSpend > 0;
  if (!hasData) {
    return (
      <div style={{ padding: "24px", textAlign: "center", color: "#6B7280" }}>
        <div style={{ fontSize: "14px", fontWeight: 500 }}>No funnel data yet</div>
        <div style={{ fontSize: "12px", marginTop: "4px" }}>
          Targeting data is synced nightly. Run a sync from the Health tab to populate.
        </div>
      </div>
    );
  }

  const sortFn = (a: any, b: any) => {
    if (sortKey === "roas") return (b.roas || 0) - (a.roas || 0);
    if (sortKey === "newCustomers") return (b.newCustomers || 0) - (a.newCustomers || 0);
    return (b.spend || 0) - (a.spend || 0);
  };
  const filterFn = (entity: any) => {
    if (filterKey === "active") return entity.status === "ACTIVE";
    if (filterKey === "spending") return (entity.spend || 0) > 0;
    return true;
  };

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "16px" }}>
      {/* Toolbar */}
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", flexWrap: "wrap", gap: "12px", padding: "8px 0", borderBottom: "1px solid #E5E7EB" }}>
        <div style={{ display: "flex", gap: "16px", alignItems: "center", flexWrap: "wrap" }}>
          <ToolbarToggle label="Sort" value={sortKey} options={[
            { id: "spend", label: "Spend" }, { id: "roas", label: "ROAS" }, { id: "newCustomers", label: "New customers" },
          ]} onChange={(v) => setSortKey(v as SortKey)} />
          <ToolbarToggle label="Show" value={filterKey} options={[
            { id: "all", label: "All" }, { id: "spending", label: "Spending" }, { id: "active", label: "Active only" },
          ]} onChange={(v) => setFilterKey(v as FilterKey)} />
        </div>
        <div style={{ fontSize: "11px", color: "#6B7280" }}>
          Click an ad set or ad to drill in · Click stage header to expand audience pool definition
        </div>
      </div>

      {/* Stage bands */}
      {FUNNEL_STAGES.map((stage) => {
        const adsets = (funnelTree[stage.key] || []).filter(filterFn).slice().sort(sortFn);
        const totals = stageTotals[stage.key] || {};
        const spendPct = totalSpend > 0 ? Math.round(((totals.spend || 0) / totalSpend) * 100) : 0;
        const roas = (totals.spend || 0) > 0 ? (totals.revenue / totals.spend).toFixed(2) : "—";

        return (
          <div key={stage.key} style={{
            background: stage.surface, border: `1px solid ${stage.border}`, borderLeft: `5px solid ${stage.accent}`,
            borderRadius: "12px", overflow: "hidden",
          }}>
            <StageBandHeader stage={stage} totals={totals} spendPct={spendPct} roas={roas} cs={cs} />
            {adsets.length === 0 ? (
              <div style={{ padding: "16px 20px", color: "#6B7280", fontSize: "12px", fontStyle: "italic" }}>
                No ad sets at this stage match the current filter.
              </div>
            ) : (
              <div style={{
                display: "grid", gap: "12px", padding: "12px 16px 16px",
                gridTemplateColumns: "repeat(auto-fill, minmax(300px, 1fr))",
              }}>
                {adsets.map((adset: any) => (
                  <AdSetCard
                    key={adset.id}
                    adset={adset}
                    stage={stage}
                    cs={cs}
                    expanded={expandedAdsetId === adset.id}
                    onToggleExpand={() => setExpandedAdsetId(expandedAdsetId === adset.id ? null : adset.id)}
                    onAdSetClick={onAdSetClick}
                    onAdClick={onAdClick}
                    sortFn={sortFn}
                  />
                ))}
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}

function ToolbarToggle({ label, value, options, onChange }: {
  label: string; value: string; options: { id: string; label: string }[]; onChange: (v: string) => void;
}) {
  return (
    <div style={{ display: "flex", alignItems: "center", gap: "8px" }}>
      <span style={{ fontSize: "var(--l-font-sm)", color: "var(--l-text-secondary)", fontWeight: 500, textTransform: "uppercase", letterSpacing: "0.5px" }}>{label}</span>
      <div style={{ display: "flex", gap: "4px" }}>
        {options.map((o) => (
          <button
            key={o.id}
            onClick={() => onChange(o.id)}
            className={`l-pill${value === o.id ? " l-pill--active" : ""}`}
            style={{ padding: "4px 10px" }}
          >{o.label}</button>
        ))}
      </div>
    </div>
  );
}

function StageBandHeader({ stage, totals, spendPct, roas, cs }: {
  stage: typeof FUNNEL_STAGES[number]; totals: any; spendPct: number; roas: string; cs: string;
}) {
  const [showPool, setShowPool] = useState(false);
  return (
    <div
      style={{ padding: "12px 16px", background: stage.bg, borderBottom: `1px solid ${stage.border}`, cursor: "pointer", userSelect: "none" }}
      onClick={() => setShowPool(!showPool)}
    >
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", flexWrap: "wrap", gap: "12px" }}>
        <div style={{ display: "flex", alignItems: "baseline", gap: "12px", flexWrap: "wrap" }}>
          <span style={{ fontSize: "15px", fontWeight: 700, color: stage.color, letterSpacing: "0.3px" }}>{stage.label}</span>
          <span style={{ fontSize: "12px", color: "#4B5563" }}>{stage.sublabel}</span>
          <span style={{ fontSize: "11px", color: stage.color, opacity: 0.7 }}>{showPool ? "▾ hide audience pool" : "▸ what counts as this stage?"}</span>
        </div>
        <div style={{ display: "flex", gap: "20px", alignItems: "baseline", flexWrap: "wrap" }}>
          <Metric value={`${cs}${Math.round(totals.spend || 0).toLocaleString()}`} sub={`${spendPct}% of spend`} />
          <Metric value={`${roas}${roas !== "—" ? "x" : ""}`} sub="ROAS" />
          <Metric value={(totals.newCustomers || 0).toLocaleString()} sub="new customers" />
          <Metric value={`${cs}${Math.round(totals.revenue || 0).toLocaleString()}`} sub="revenue" />
          <Metric value={`${totals.adsetCount || 0}/${totals.adCount || 0}`} sub="ad sets / ads" />
        </div>
      </div>
      {showPool && (
        <div style={{ marginTop: "10px", padding: "10px 12px", background: "#fff", border: `1px dashed ${stage.border}`, borderRadius: "6px", fontSize: "12px", color: "#374151", lineHeight: 1.5 }}>
          <strong style={{ color: stage.color }}>Audience pool · </strong>{stage.pool}
        </div>
      )}
    </div>
  );
}

function Metric({ value, sub }: { value: string | number; sub: string }) {
  return (
    <div style={{ display: "flex", flexDirection: "column", alignItems: "flex-end", lineHeight: 1.2 }}>
      <span style={{ fontSize: "16px", fontWeight: 700, color: "#111827" }}>{value}</span>
      <span style={{ fontSize: "10px", color: "#6B7280", textTransform: "uppercase", letterSpacing: "0.4px" }}>{sub}</span>
    </div>
  );
}

function AdSetCard({ adset, stage, cs, expanded, onToggleExpand, onAdSetClick, onAdClick, sortFn }: {
  adset: any; stage: typeof FUNNEL_STAGES[number]; cs: string; expanded: boolean;
  onToggleExpand: () => void;
  onAdSetClick?: (id: string, name: string) => void;
  onAdClick?: (id: string, name: string) => void;
  sortFn: (a: any, b: any) => number;
}) {
  const visibleAudiences = adset.audiences.slice(0, 2);
  const hiddenAudienceCount = adset.audiences.length - visibleAudiences.length;
  const hasExclusions = adset.excludedAudiences.length > 0;
  const sortedAds = adset.ads.slice().sort(sortFn);
  return (
    <div style={{
      background: "#fff", border: `1px solid ${stage.border}`, borderRadius: "10px",
      overflow: "hidden", display: "flex", flexDirection: "column",
      boxShadow: "0 1px 2px rgba(0,0,0,0.04)",
    }}>
      {/* Ad set header */}
      <div style={{ padding: "10px 12px", borderBottom: `1px solid ${stage.border}`, background: stage.surface }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", gap: "8px" }}>
          <button
            onClick={() => onAdSetClick?.(adset.id, adset.name)}
            title={`Open ${adset.name}`}
            style={{
              flex: 1, minWidth: 0, background: "transparent", border: "none", padding: 0, textAlign: "left", cursor: "pointer",
            }}
          >
            <div style={{ display: "flex", alignItems: "center", gap: "6px", marginBottom: "2px" }}>
              <span title={adset.status || "unknown status"} style={{
                display: "inline-block", width: "8px", height: "8px", borderRadius: "50%",
                background: statusColor(adset.status), flexShrink: 0,
              }} />
              <span style={{ fontSize: "13px", fontWeight: 600, color: "#111827", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{adset.name}</span>
            </div>
            <div style={{ fontSize: "10px", color: "#6B7280", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
              {adset.campaignName}
            </div>
          </button>
          <button
            onClick={onToggleExpand}
            title={expanded ? "Hide targeting" : "Show targeting"}
            style={{
              background: "transparent", border: `1px solid ${stage.border}`, borderRadius: "4px",
              padding: "1px 6px", cursor: "pointer", color: stage.color, fontSize: "10px", fontWeight: 600, flexShrink: 0,
            }}
          >{expanded ? "−" : "i"}</button>
        </div>

        {/* Audience chips */}
        {(visibleAudiences.length > 0 || hasExclusions) && (
          <div style={{ display: "flex", gap: "4px", flexWrap: "wrap", marginTop: "6px" }}>
            {visibleAudiences.map((a: string, i: number) => (
              <span key={i} title={a} style={{
                fontSize: "10px", padding: "1px 6px", borderRadius: "8px",
                background: "#fff", border: `1px solid ${stage.border}`, color: stage.color,
                maxWidth: "160px", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
              }}>{a}</span>
            ))}
            {hiddenAudienceCount > 0 && (
              <span style={{ fontSize: "10px", padding: "1px 6px", borderRadius: "8px", background: "#F3F4F6", color: "#6B7280" }}>
                +{hiddenAudienceCount}
              </span>
            )}
            {hasExclusions && (
              <span title={adset.excludedAudiences.join(", ")} style={{
                fontSize: "10px", padding: "1px 6px", borderRadius: "8px",
                background: "#fff", border: "1px dashed #9CA3AF", color: "#6B7280",
              }}>excludes {adset.excludedAudiences.length}</span>
            )}
          </div>
        )}

        {/* Stats */}
        <div style={{ display: "flex", gap: "10px", marginTop: "8px", fontSize: "11px", color: "#374151" }}>
          <span><strong>{cs}{Math.round(adset.spend).toLocaleString()}</strong> spend</span>
          <span><strong>{adset.roas || 0}x</strong> ROAS</span>
          <span><strong>{adset.newCustomers}</strong> NC</span>
        </div>
      </div>

      {/* Expanded targeting detail */}
      {expanded && (
        <div style={{ padding: "8px 12px", background: "#FAFAFA", borderBottom: `1px solid ${stage.border}`, fontSize: "11px", color: "#374151" }}>
          {adset.targetingSummary && <div style={{ marginBottom: "4px" }}>{adset.targetingSummary}</div>}
          {adset.audiences.length > 0 && (
            <div style={{ marginBottom: "4px" }}>
              <strong>Includes:</strong> {adset.audiences.join(", ")}
            </div>
          )}
          {adset.excludedAudiences.length > 0 && (
            <div><strong>Excludes:</strong> {adset.excludedAudiences.join(", ")}</div>
          )}
          {!adset.targetingSummary && adset.audiences.length === 0 && adset.excludedAudiences.length === 0 && (
            <div style={{ fontStyle: "italic", color: "#6B7280" }}>No targeting metadata available — likely Advantage+ broad audience or unsynced.</div>
          )}
        </div>
      )}

      {/* Ads grid */}
      {sortedAds.length > 0 ? (
        <div style={{ padding: "8px", display: "grid", gap: "6px", gridTemplateColumns: "repeat(auto-fill, minmax(140px, 1fr))" }}>
          {sortedAds.map((ad: any) => (
            <AdMiniCard key={ad.id} ad={ad} stage={stage} cs={cs} onClick={() => onAdClick?.(ad.id, ad.name)} />
          ))}
        </div>
      ) : (
        <div style={{ padding: "10px 12px", fontSize: "11px", fontStyle: "italic", color: "#9CA3AF" }}>
          No ads with activity in this period.
        </div>
      )}
    </div>
  );
}

function AdMiniCard({ ad, stage, cs, onClick }: {
  ad: any; stage: typeof FUNNEL_STAGES[number]; cs: string; onClick?: () => void;
}) {
  return (
    <button
      onClick={onClick}
      title={`${ad.name}\nStatus: ${ad.status || "unknown"}\nSpend: ${cs}${Math.round(ad.spend).toLocaleString()}\nROAS: ${ad.roas || 0}x\nNew customers: ${ad.newCustomers}\nAge: ${ad.ageDays != null ? ad.ageDays + "d" : "—"}`}
      style={{
        background: "#fff",
        border: `1px solid ${stage.border}`,
        borderLeft: `3px solid ${statusColor(ad.status)}`,
        borderRadius: "6px",
        padding: "6px 8px",
        textAlign: "left",
        cursor: "pointer",
        display: "flex",
        flexDirection: "column",
        gap: "3px",
        transition: "transform 80ms ease, box-shadow 80ms ease",
      }}
      onMouseEnter={(e) => { e.currentTarget.style.transform = "translateY(-1px)"; e.currentTarget.style.boxShadow = "0 2px 6px rgba(0,0,0,0.08)"; }}
      onMouseLeave={(e) => { e.currentTarget.style.transform = "none"; e.currentTarget.style.boxShadow = "none"; }}
    >
      <div style={{ fontSize: "11px", fontWeight: 600, color: "#111827", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
        {ad.name}
      </div>
      <div style={{ display: "flex", justifyContent: "space-between", fontSize: "10px", color: "#374151" }}>
        <span>{cs}{Math.round(ad.spend).toLocaleString()}</span>
        <span style={{ fontWeight: 600, color: stage.color }}>{ad.roas || 0}x</span>
      </div>
      <div style={{ display: "flex", justifyContent: "space-between", fontSize: "10px", color: "#6B7280" }}>
        <span>{ad.newCustomers} NC</span>
        <span>{ad.ageDays != null ? `${ad.ageDays}d` : ""}</span>
      </div>
    </button>
  );
}

const TAB_LEVELS = ["campaign", "adset", "ad"];
const TAB_LABELS = ["Campaigns", "Ad Sets", "Ads"];
const NAME_HEADERS: Record<string, string> = { campaign: "Campaign", adset: "Ad Set", ad: "Ad" };

export default function Campaigns() {
  const {
    breakdown, campaignRows, adsetRows, adRows,
    prevCampaignRows, prevAdsetRows, prevAdRows,
    breakdownMaps, hasBreakdownData,
    compareTotals, compareLabel, hasComparison, currencySymbol,
    reportingPeriodDays, dailyData, prevDailyData, totalStoreRevenue,
    platformPerf, placementPerf,
    aiCachedInsights, aiGeneratedAt, aiIsStale,
    uniqueNewMetaCustomers, prevUniqueNewMetaCustomers, totalNewCustomersInPeriod,
    shopDomain, fromKey, toKey, preset,
    changeEvents, changeCountsByObjectId,
    funnelTree, stageTotals,
    topTiles,
    adDemographicsByAd,
  } = useLoaderData();
  const cs = currencySymbol || currencySymbolFromCode(null);
  const [searchParams, setSearchParams] = useSearchParams();

  // Change log integration - strip above the tiles + drawer triggered from
  // entity names in the Campaigns table.
  const [drawerEntity, setDrawerEntity] = useState<EntityRef | null>(null);
  const [showChanges, setShowChanges] = useState(() => {
    if (typeof window === "undefined") return true;
    const v = window.localStorage.getItem("lucidly.campaigns.showChanges");
    return v === null ? true : v === "1";
  });
  useEffect(() => {
    if (typeof window !== "undefined") {
      window.localStorage.setItem("lucidly.campaigns.showChanges", showChanges ? "1" : "0");
    }
  }, [showChanges]);
  const dayKeyForEvent = (iso: string) => iso.slice(0, 10);

  // Measure sticky tabs header height
  const perfTabsRef = useRef<HTMLDivElement>(null);
  const [perfTabsHeight, setPerfTabsHeight] = useState(70);
  useEffect(() => {
    if (!perfTabsRef.current) return;
    const measure = () => setPerfTabsHeight(perfTabsRef.current?.offsetHeight || 70);
    measure();
    const obs = new ResizeObserver(measure);
    obs.observe(perfTabsRef.current);
    return () => obs.disconnect();
  }, []);

  // Tab state (0=campaigns, 1=ad sets, 2=ads)
  const [selectedTab, setSelectedTab] = useState(0);
  const level = TAB_LEVELS[selectedTab];

  // Drill-down filters
  const [filterCampaignId, setFilterCampaignId] = useState<string | null>(null);
  const [filterCampaignName, setFilterCampaignName] = useState<string | null>(null);
  const [filterAdSetId, setFilterAdSetId] = useState<string | null>(null);
  const [filterAdSetName, setFilterAdSetName] = useState<string | null>(null);

  // Checkbox selection - persists across tab changes for filtering
  const [selectedCampaignIds, setSelectedCampaignIds] = useState<Set<string>>(new Set());
  const [selectedAdSetIds, setSelectedAdSetIds] = useState<Set<string>>(new Set());

  const handleTabChange = useCallback((index: number) => {
    setSelectedTab(index);
    // Reset drill-down filters when going back to a higher level
    if (index === 0) {
      setFilterCampaignId(null);
      setFilterCampaignName(null);
      setFilterAdSetId(null);
      setFilterAdSetName(null);
      setSelectedAdSetIds(new Set());
    } else if (index === 1) {
      setFilterAdSetId(null);
      setFilterAdSetName(null);
      setSelectedAdSetIds(new Set());
    }
  }, []);

  const [breakdownOpen, setBreakdownOpen] = useState(false);

  // ── Infographic state ──
  const [rankLevel, setRankLevel] = useState("ad");
  const [rankMetric, setRankMetric] = useState("blendedROAS");

  const [funnelMode, setFunnelMode] = useState<"counts" | "rates">("counts");
  const [newCustLevel, setNewCustLevel] = useState("ad");

  const rowsByLevel = useMemo(() => ({ campaign: campaignRows, adset: adsetRows, ad: adRows }), [campaignRows, adsetRows, adRows]);
  const prevRowsByLevel = useMemo(() => ({ campaign: prevCampaignRows || [], adset: prevAdsetRows || [], ad: prevAdRows || [] }), [prevCampaignRows, prevAdsetRows, prevAdRows]);

  const handleBreakdownChange = useCallback((value) => {
    const params = new URLSearchParams(searchParams);
    params.set("breakdown", value);
    setSearchParams(params);
    setBreakdownOpen(false);
  }, [searchParams, setSearchParams]);

  // Get rows for current tab, filtered by drill-down AND parent selections
  const allRowsForLevel = level === "campaign" ? campaignRows
    : level === "adset" ? adsetRows : adRows;

  const filteredRows = useMemo(() => {
    let rows = allRowsForLevel;

    if (level === "adset") {
      // Filter by drill-down OR by selected campaigns
      if (filterCampaignId) {
        rows = rows.filter(r => r.campaignId === filterCampaignId);
      } else if (selectedCampaignIds.size > 0) {
        rows = rows.filter(r => selectedCampaignIds.has(r.campaignId));
      }
    }

    if (level === "ad") {
      if (filterAdSetId) {
        rows = rows.filter(r => r.adSetId === filterAdSetId);
      } else if (selectedAdSetIds.size > 0) {
        rows = rows.filter(r => selectedAdSetIds.has(r.adSetId));
      } else if (filterCampaignId) {
        rows = rows.filter(r => r.campaignId === filterCampaignId);
      } else if (selectedCampaignIds.size > 0) {
        rows = rows.filter(r => selectedCampaignIds.has(r.campaignId));
      }
    }

    return rows;
  }, [allRowsForLevel, level, filterCampaignId, filterAdSetId, selectedCampaignIds, selectedAdSetIds]);

  // Use filtered rows, with breakdown sub-rows interleaved when active
  const showBreakdown = breakdown !== "none" && hasBreakdownData;
  const displayRows = useMemo(() => {
    if (!showBreakdown) return filteredRows;
    const bdMap = breakdownMaps[level] || {};
    const result = [];
    for (const row of filteredRows) {
      result.push({ ...row, _isParent: true });
      const subRows = bdMap[row.id] || [];
      for (const sub of subRows) {
        result.push(sub);
      }
    }
    return result;
  }, [filteredRows, showBreakdown, breakdownMaps, level]);

  const totals = computeClientTotals(filteredRows);

  // Footer totals row for the table (use parent rows only to avoid double-counting)
  const footerSourceRows = useMemo(() =>
    showBreakdown ? displayRows.filter(r => r._isParent) : displayRows,
    [displayRows, showBreakdown]
  );
  const footerRow = useMemo(() => {
    if (footerSourceRows.length === 0) return undefined;
    const r2 = (v: number) => Math.round(v * 100) / 100;
    const sum = (key: string) => footerSourceRows.reduce((s, r) => s + (r[key] || 0), 0);

    const spend = sum("spend");
    const impressions = sum("impressions");
    const clicks = sum("clicks");
    const linkClicks = sum("linkClicks");
    const landingPageViews = sum("landingPageViews");
    const viewContent = sum("viewContent");
    const addToCart = sum("addToCart");
    const initiateCheckout = sum("initiateCheckout");
    const metaConversions = sum("metaConversions");
    const attributedRevenue = sum("attributedRevenue");
    const unverifiedRevenue = sum("unverifiedRevenue");
    const attributedOrders = sum("attributedOrders");
    const newCustomerOrders = sum("newCustomerOrders");
    const newCustomerRevenue = sum("newCustomerRevenue");
    const existingCustomerOrders = sum("existingCustomerOrders");
    const existingCustomerRevenue = sum("existingCustomerRevenue");
    const videoP25 = sum("videoP25");
    const videoP50 = sum("videoP50");
    const videoP75 = sum("videoP75");
    const videoP100 = sum("videoP100");

    const ctr = impressions > 0 ? r2((clicks / impressions) * 100) : 0;
    const blendedROAS = spend > 0 ? r2((attributedRevenue + unverifiedRevenue) / spend) : 0;
    const cpa = attributedOrders > 0 ? r2(spend / attributedOrders) : 0;
    const atcRate = viewContent > 0 ? r2((addToCart / viewContent) * 100) : 0;
    const checkoutRate = addToCart > 0 ? r2((initiateCheckout / addToCart) * 100) : 0;
    const purchaseRate = initiateCheckout > 0 ? r2((metaConversions / initiateCheckout) * 100) : 0;

    const levelLabel = TAB_LABELS[selectedTab];
    const newPct = attributedOrders > 0 ? Math.round((newCustomerOrders / attributedOrders) * 100) : 0;
    const existPct = attributedOrders > 0 ? Math.round((existingCustomerOrders / attributedOrders) * 100) : 0;
    const newRevPct = attributedRevenue > 0 ? Math.round((newCustomerRevenue / attributedRevenue) * 100) : 0;
    const existRevPct = attributedRevenue > 0 ? Math.round((existingCustomerRevenue / attributedRevenue) * 100) : 0;

    const footer: Record<string, any> = {
      select: "",
      name: `Results from ${footerSourceRows.length} ${levelLabel}`,
      adAgeDays: "-",
      spend: `${cs}${r2(spend).toLocaleString()}`,
      impressions: impressions.toLocaleString(),
      clicks: clicks.toLocaleString(),
      ctr: `${ctr}%`,
      avgFrequency: "-",
      linkClicks: linkClicks.toLocaleString(),
      landingPageViews: landingPageViews.toLocaleString(),
      viewContent: viewContent.toLocaleString(),
      addToCart: addToCart.toLocaleString(),
      atcRate: viewContent > 0 ? `${atcRate}%` : "-",
      initiateCheckout: initiateCheckout.toLocaleString(),
      checkoutRate: addToCart > 0 ? `${checkoutRate}%` : "-",
      metaConversions: metaConversions.toLocaleString(),
      purchaseRate: initiateCheckout > 0 ? `${purchaseRate}%` : "-",
      attributedRevenue: `${cs}${r2(attributedRevenue).toLocaleString()}`,
      unverifiedRevenue: `${cs}${r2(unverifiedRevenue).toLocaleString()}`,
      blendedROAS: `${blendedROAS}x`,
      attributedOrders: attributedOrders.toLocaleString(),
      newCustomerOrders: <>{newCustomerOrders.toLocaleString()} <span style={{ color: "#999", fontWeight: 400 }}>({newPct}%)</span></>,
      newCustomerRevenue: <>{cs}{r2(newCustomerRevenue).toLocaleString()} <span style={{ color: "#999", fontWeight: 400 }}>({newRevPct}%)</span></>,
      existingCustomerOrders: <>{existingCustomerOrders.toLocaleString()} <span style={{ color: "#999", fontWeight: 400 }}>({existPct}%)</span></>,
      existingCustomerRevenue: <>{cs}{r2(existingCustomerRevenue).toLocaleString()} <span style={{ color: "#999", fontWeight: 400 }}>({existRevPct}%)</span></>,
      cpa: attributedOrders > 0 ? `${cs}${cpa.toLocaleString()}` : "-",
      newCustomerCPA: newCustomerOrders > 0 ? `${cs}${r2(spend / newCustomerOrders).toLocaleString()}` : "-",
      revenuePerNewCustomer: newCustomerOrders > 0 ? `${cs}${r2(newCustomerRevenue / newCustomerOrders).toLocaleString()}` : "-",
      spendPerDay: reportingPeriodDays > 0 ? `${cs}${r2(spend / reportingPeriodDays).toLocaleString()}` : "-",
      newCustomersPerDay: reportingPeriodDays > 0 && newCustomerOrders > 0 ? r2(newCustomerOrders / reportingPeriodDays).toLocaleString() : "-",
      newCustomerRevenuePerDay: reportingPeriodDays > 0 && newCustomerRevenue > 0 ? `${cs}${r2(newCustomerRevenue / reportingPeriodDays).toLocaleString()}` : "-",
      newCustomerROAS: spend > 0 && newCustomerRevenue > 0 ? `${r2(newCustomerRevenue / spend)}x` : "-",
      videoP25: videoP25.toLocaleString(),
      videoP50: videoP50.toLocaleString(),
      videoP75: videoP75.toLocaleString(),
      videoP100: videoP100.toLocaleString(),
      metaConversionValue: `${cs}${r2(sum("metaConversionValue")).toLocaleString()}`,
      // LTV footer: weighted averages based on acquired customers
      ...((() => {
        const totalLtvCusts = footerSourceRows.reduce((s, r) => s + (r.ltvAcquiredCustomers || 0), 0);
        if (totalLtvCusts === 0) return {
          avgLtv30: "-", avgLtv90: "-", avgLtv365: "-", avgLtvAll: "-",
          totalLtvAll: "-", ltvCac: "-", repeatRate: "-", avgOrders: "-", ltvAcquiredCustomers: "-",
        };
        const wAvg = (key: string) => r2(footerSourceRows.reduce((s, r) => s + (r[key] || 0) * (r.ltvAcquiredCustomers || 0), 0) / totalLtvCusts);
        const totalLtv = r2(footerSourceRows.reduce((s, r) => s + (r.totalLtvAll || 0), 0));
        const wRepeat = Math.round(footerSourceRows.reduce((s, r) => s + (r.repeatRate || 0) * (r.ltvAcquiredCustomers || 0), 0) / totalLtvCusts);
        const wOrders = r2(footerSourceRows.reduce((s, r) => s + (r.avgOrders || 0) * (r.ltvAcquiredCustomers || 0), 0) / totalLtvCusts);
        const overallCac = newCustomerOrders > 0 ? spend / newCustomerOrders : 0;
        const ltv90Overall = wAvg("avgLtv90");
        return {
          avgLtv30: `${cs}${wAvg("avgLtv30").toLocaleString()}`,
          avgLtv90: `${cs}${ltv90Overall.toLocaleString()}`,
          avgLtv365: `${cs}${wAvg("avgLtv365").toLocaleString()}`,
          avgLtvAll: `${cs}${wAvg("avgLtvAll").toLocaleString()}`,
          totalLtvAll: `${cs}${totalLtv.toLocaleString()}`,
          ltvCac: overallCac > 0 ? `${r2(ltv90Overall / overallCac)}x` : "-",
          repeatRate: `${wRepeat}%`,
          avgOrders: `${wOrders}`,
          ltvAcquiredCustomers: totalLtvCusts.toLocaleString(),
        };
      })()),
    };
    return footer;
  }, [footerSourceRows, cs, selectedTab, reportingPeriodDays]);

  // Drill-down handler
  const handleDrillDown = useCallback((row: any) => {
    if (level === "campaign") {
      setFilterCampaignId(row.id);
      setFilterCampaignName(row.name);
      setSelectedTab(1);
      setSelectedAdSetIds(new Set());
    } else if (level === "adset") {
      setFilterAdSetId(row.id);
      setFilterAdSetName(row.name);
      setSelectedTab(2);
    }
  }, [level]);

  // Current-level checkbox selection for summary cards
  const currentSelectedIds = level === "campaign" ? selectedCampaignIds : selectedAdSetIds;
  const setCurrentSelectedIds = level === "campaign" ? setSelectedCampaignIds : setSelectedAdSetIds;

  const toggleSelect = useCallback((id: string) => {
    setCurrentSelectedIds(prev => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }, [setCurrentSelectedIds]);

  const toggleSelectAll = useCallback(() => {
    if (currentSelectedIds.size === filteredRows.length) {
      setCurrentSelectedIds(new Set());
    } else {
      setCurrentSelectedIds(new Set(filteredRows.map(r => r.id)));
    }
  }, [currentSelectedIds, filteredRows, setCurrentSelectedIds]);

  // Effective totals (filtered by selection, always from parent rows)
  const effectiveRows = currentSelectedIds.size > 0
    ? filteredRows.filter(r => currentSelectedIds.has(r.id))
    : filteredRows;
  const effectiveTotals = currentSelectedIds.size > 0 ? computeClientTotals(effectiveRows) : totals;

  const num = (key: string, header: string, fmt?: (v: number, r: any) => string, tip?: { desc: string; calc?: string }) => ({
    accessorKey: key,
    header,
    meta: { align: "right", ...(tip ? { description: tip.desc, calc: tip.calc } : {}) },
    cell: ({ getValue, row }: any) => {
      const v = getValue();
      if (v == null) return "-";
      if (fmt) return fmt(v, row.original);
      return v.toLocaleString();
    },
  } as ColumnDef<any, any>);

  const nameHeader = NAME_HEADERS[level] || "Name";

  const columns = useMemo(() => {
    const cols: ColumnDef<any, any>[] = [
      {
        id: "select",
        header: () => (
          <Checkbox
            label=""
            labelHidden
            checked={filteredRows.length > 0 && currentSelectedIds.size === filteredRows.length}
            onChange={toggleSelectAll}
          />
        ),
        cell: ({ row }: any) => {
          if (row.original._isBreakdownRow) return null;
          return (
            <Checkbox
              label=""
              labelHidden
              checked={currentSelectedIds.has(row.original.id)}
              onChange={() => toggleSelect(row.original.id)}
            />
          );
        },
        enableSorting: false,
      },
      {
        accessorKey: "name",
        header: showBreakdown ? `${nameHeader} / ${BREAKDOWN_LABELS[breakdown] || breakdown}` : nameHeader,
        meta: { maxWidth: "280px", description: showBreakdown ? `${nameHeader} with ${BREAKDOWN_LABELS[breakdown] || breakdown} breakdown rows underneath` : (level !== "ad" ? `Click a name to drill down into its ${level === "campaign" ? "ad sets" : "ads"}` : undefined) },
        cell: ({ getValue, row }: any) => {
          const original = row.original;
          // Breakdown sub-row: show breakdown value indented
          if (original._isBreakdownRow) {
            let label = original.breakdownValue;
            if (breakdown === "country" && label) {
              try {
                label = new Intl.DisplayNames(["en"], { type: "region" }).of(label) || label;
              } catch {}
            }
            return (
              <span style={{ paddingLeft: "16px", color: "#555", fontSize: "12px" }}>
                {label}
              </span>
            );
          }
          const name = getValue();
          const canDrill = level === "campaign" || level === "adset";
          const entityId = original.id;
          const changeCount = (changeCountsByObjectId && changeCountsByObjectId[entityId]) || 0;
          const activityBadge = changeCount > 0 ? (
            <button
              onClick={(e) => {
                e.stopPropagation();
                setDrawerEntity({
                  objectType: level as "campaign" | "adset" | "ad",
                  objectId: entityId,
                  objectName: name,
                });
              }}
              title={`${changeCount} change${changeCount === 1 ? "" : "s"} in period - click for full timeline`}
              style={{
                marginLeft: 6, padding: "1px 6px",
                fontSize: 10, fontWeight: 600, lineHeight: "14px",
                background: "#EEF2FF", color: "#4338CA",
                border: "1px solid #C7D2FE", borderRadius: 10,
                cursor: "pointer", verticalAlign: "middle",
                whiteSpace: "nowrap",
              }}
            >🔔 {changeCount}</button>
          ) : null;
          if (canDrill && !showBreakdown) {
            return (
              <span style={{ display: "inline-flex", alignItems: "center", maxWidth: "260px" }}>
                <button
                  onClick={() => handleDrillDown(row.original)}
                  style={{
                    background: "none",
                    border: "none",
                    color: "#2c6ecb",
                    cursor: "pointer",
                    padding: 0,
                    textDecoration: "underline",
                    fontSize: "inherit",
                    textAlign: "left",
                    maxWidth: "250px",
                    overflow: "hidden",
                    textOverflow: "ellipsis",
                    whiteSpace: "nowrap",
                    display: "block",
                  }}
                  title={name}
                >
                  {name}
                </button>
                {activityBadge}
              </span>
            );
          }
          return (
            <span style={{ display: "inline-flex", alignItems: "center" }}>
              <span style={{ fontWeight: 600 }}>{name}</span>
              {activityBadge}
            </span>
          );
        },
      },
    ];
    if (!showBreakdown) {
      cols.push({
        accessorKey: "adAgeDays",
        header: level === "campaign" ? "Campaign Age" : level === "adset" ? "Ad Set Age" : "Ad Age",
        meta: { align: "right", description: `Number of days since this ${level === "campaign" ? "campaign" : level === "adset" ? "ad set" : "ad"} was created on Meta` },
        cell: ({ getValue }: any) => {
          const v = getValue();
          if (v == null) return "-";
          return `${v} days`;
        },
      } as ColumnDef<any, any>);
    }
    cols.push(
      num("spend", "Spend", (v) => `${cs}${v.toLocaleString()}`, { desc: "Total amount spent on Meta ads in the selected period" }),
      num("impressions", "Impressions", undefined, { desc: "Total number of times your ads were shown on screen" }),
      num("clicks", "Clicks", undefined, { desc: "Total clicks on your ads, including all click types (link clicks, likes, comments, shares)" }),
      num("ctr", "CTR", (v) => `${v}%`, { desc: "Click-through rate - how often people clicked after seeing your ad", calc: "Clicks ÷ Impressions × 100" }),
      num("avgFrequency", "Frequency", (v) => v > 0 ? `${v}x` : "-", { desc: "Average number of times each person saw your ad. Higher frequency can mean ad fatigue" }),
      num("linkClicks", "Link Clicks", undefined, { desc: "Clicks that directed people to your website or app" }),
      num("landingPageViews", "Landing Page Views", undefined, { desc: "Number of times your landing page fully loaded after someone clicked your ad" }),
      num("viewContent", "View Content", undefined, { desc: "Number of times someone viewed a product page on your site after seeing your ad" }),
      num("addToCart", "Add to Cart", undefined, { desc: "Number of times someone added a product to their cart after seeing your ad" }),
      num("atcRate", "ATC Rate", (v, r) => r.viewContent > 0 ? `${v}%` : "-", { desc: "Add-to-cart rate - of people who viewed a product, how many added to cart", calc: "Add to Cart ÷ View Content × 100" }),
      num("initiateCheckout", "Initiate Checkout", undefined, { desc: "Number of times someone started the checkout process after seeing your ad" }),
      num("checkoutRate", "Checkout Rate", (v, r) => r.addToCart > 0 ? `${v}%` : "-", { desc: "Checkout rate - of people who added to cart, how many started checkout", calc: "Initiate Checkout ÷ Add to Cart × 100" }),
      num("metaConversions", "Purchases", undefined, { desc: "Total purchases reported by Meta, including both matched and unmatched orders" }),
      num("purchaseRate", "Purchase Rate", (v, r) => r.initiateCheckout > 0 ? `${v}%` : "-", { desc: "Purchase rate - of people who started checkout, how many completed a purchase", calc: "Purchases ÷ Initiate Checkout × 100" }),
      num("attributedRevenue", "Matched Revenue", (v) => `${cs}${v.toLocaleString()}`, { desc: "Revenue from orders we've verified and matched to specific Meta ads at order level" }),
      num("unverifiedRevenue", "Unmatched Revenue", (v) => `${cs}${v.toLocaleString()}`, { desc: "The gap between what Meta reported and what we could verify. Typically caused by order edits, refunds, or currency differences after purchase" }),
      num("blendedROAS", "Confirmed ROAS", (v) => `${v}x`, { desc: "Return on ad spend from confirmed Meta-attributed revenue (matched + unmatched)", calc: "(Matched Revenue + Unmatched Revenue) ÷ Spend" }),
      num("attributedOrders", "Attributed Orders", undefined, { desc: "Number of Shopify orders matched to Meta ads via statistical attribution" }),
      num("metaConversionValue", "Meta Revenue", (v) => `${cs}${v.toLocaleString()}`, { desc: "Total conversion value as reported by Meta. May differ from Shopify revenue due to order edits, refunds, and currency differences" }),
      {
        accessorKey: "newCustomerOrders",
        header: "New Customers",
        meta: { align: "right", description: "First-time customers acquired via this ad. Shows count and percentage of total attributed orders" },
        cell: ({ getValue, row }: any) => {
          const v = getValue();
          if (v == null) return "-";
          const total = row.original.attributedOrders;
          const pct = total > 0 ? Math.round((v / total) * 100) : 0;
          return <>{v.toLocaleString()} <span style={{ color: "#999" }}>({pct}%)</span></>;
        },
      } as ColumnDef<any, any>,
      {
        accessorKey: "newCustomerRevenue",
        header: "New Customer Revenue",
        meta: { align: "right", description: "Revenue from first-time customers acquired via this ad. Shows amount and percentage of total matched revenue" },
        cell: ({ getValue, row }: any) => {
          const v = getValue();
          if (v == null) return "-";
          const total = row.original.attributedRevenue;
          const pct = total > 0 ? Math.round((v / total) * 100) : 0;
          return <>{cs}{v.toLocaleString()} <span style={{ color: "#999" }}>({pct}%)</span></>;
        },
      } as ColumnDef<any, any>,
      {
        accessorKey: "existingCustomerOrders",
        header: "Existing Customers",
        meta: { align: "right", description: "Returning customers who purchased via this ad. Shows count and percentage of total attributed orders" },
        cell: ({ getValue, row }: any) => {
          const v = getValue();
          if (v == null) return "-";
          const total = row.original.attributedOrders;
          const pct = total > 0 ? Math.round((v / total) * 100) : 0;
          return <>{v.toLocaleString()} <span style={{ color: "#999" }}>({pct}%)</span></>;
        },
      } as ColumnDef<any, any>,
      {
        accessorKey: "existingCustomerRevenue",
        header: "Existing Customer Revenue",
        meta: { align: "right", description: "Revenue from returning customers via this ad. Shows amount and percentage of total matched revenue" },
        cell: ({ getValue, row }: any) => {
          const v = getValue();
          if (v == null) return "-";
          const total = row.original.attributedRevenue;
          const pct = total > 0 ? Math.round((v / total) * 100) : 0;
          return <>{cs}{v.toLocaleString()} <span style={{ color: "#999" }}>({pct}%)</span></>;
        },
      } as ColumnDef<any, any>,
      num("cpa", "CPA", (v, r) => r.attributedOrders > 0 ? `${cs}${v.toLocaleString()}` : "-", { desc: "Cost per acquisition - how much you spent to get each order", calc: "Spend ÷ Attributed Orders" }),
      num("newCustomerCPA", "New Customer CPA", (v) => v != null ? `${cs}${v.toLocaleString()}` : "-", { desc: "Cost to acquire each new customer", calc: "Spend ÷ New Customers" }),
      num("revenuePerNewCustomer", "Rev per New Customer", (v) => v != null ? `${cs}${v.toLocaleString()}` : "-", { desc: "Average first-order revenue per new customer", calc: "New Customer Revenue ÷ New Customers" }),
      num("spendPerDay", "Spend/Day", (v) => v != null ? `${cs}${v.toLocaleString()}` : "-", { desc: "Average daily spend, based on how long the ad has been active within the reporting period", calc: "Spend ÷ active days (shorter of ad age or reporting period)" }),
      num("newCustomersPerDay", "New Customers/Day", (v) => v != null ? `${v}` : "-", { desc: "Average new customers acquired per day", calc: "New Customers ÷ active days" }),
      num("newCustomerRevenuePerDay", "New Customer Rev/Day", (v) => v != null ? `${cs}${v.toLocaleString()}` : "-", { desc: "Average daily revenue from new customers", calc: "New Customer Revenue ÷ active days" }),
      num("newCustomerROAS", "New Customer ROAS", (v) => v != null ? `${v}x` : "-", { desc: "Return on ad spend from new customers only - excludes returning customer revenue", calc: "New Customer Revenue ÷ Spend" }),
      num("videoP25", "Video 25%", undefined, { desc: "Number of times your video was watched to 25% of its length" }),
      num("videoP50", "Video 50%", undefined, { desc: "Number of times your video was watched to 50% of its length" }),
      num("videoP75", "Video 75%", undefined, { desc: "Number of times your video was watched to 75% of its length" }),
      num("videoP100", "Video 100%", undefined, { desc: "Number of times your video was watched all the way through" }),
      // LTV columns (all-time, independent of reporting window)
      num("avgLtv30", "Avg LTV 30d", (v) => v != null ? `${cs}${v.toLocaleString()}` : "-", { desc: "Average revenue per acquired customer within 30 days of their first purchase. Unaffected by the reporting period - always uses all-time data", calc: "Total 30-day revenue ÷ acquired customers" }),
      num("avgLtv90", "Avg LTV 90d", (v) => v != null ? `${cs}${v.toLocaleString()}` : "-", { desc: "Average revenue per acquired customer within 90 days of their first purchase. Unaffected by the reporting period - always uses all-time data", calc: "Total 90-day revenue ÷ acquired customers" }),
      num("avgLtv365", "Avg LTV 1yr", (v) => v != null ? `${cs}${v.toLocaleString()}` : "-", { desc: "Average revenue per acquired customer within 1 year of their first purchase. Unaffected by the reporting period - always uses all-time data", calc: "Total 1-year revenue ÷ acquired customers" }),
      num("avgLtvAll", "Avg LTV All", (v) => v != null ? `${cs}${v.toLocaleString()}` : "-", { desc: "Average all-time revenue per acquired customer, across every order they've ever placed", calc: "Total all-time revenue ÷ acquired customers" }),
      num("totalLtvAll", "Total LTV", (v) => v != null ? `${cs}${v.toLocaleString()}` : "-", { desc: "Total lifetime revenue from every customer acquired via this ad, across all their orders" }),
      num("ltvCac", "LTV:CAC", (v) => v != null ? `${v}x` : "-", { desc: "How much lifetime revenue each acquired customer generates relative to what it cost to acquire them", calc: "Avg LTV All ÷ New Customer CPA" }),
      num("repeatRate", "Repeat Rate", (v) => v != null ? `${v}%` : "-", { desc: "Percentage of acquired customers who came back and purchased again", calc: "Repeat buyers ÷ total acquired customers × 100" }),
      num("avgOrders", "Avg Orders", (v) => v != null ? `${v}` : "-", { desc: "Average number of orders placed by each acquired customer", calc: "Total orders ÷ acquired customers" }),
      num("ltvAcquiredCustomers", "LTV Customers", (v) => v > 0 ? v.toLocaleString() : "-", { desc: "Number of unique customers acquired via this ad that we have lifetime value data for" }),
    );
    return cols;
  }, [cs, showBreakdown, breakdown, level, nameHeader, currentSelectedIds, filteredRows, toggleSelectAll, toggleSelect, handleDrillDown, changeCountsByObjectId]);

  // Show ALL columns by default — the table is now fit-content + horizontal
  // scroll, so the full set fits without truncation. Saved per-merchant
  // selection (via the "Save as Default" button in the column picker)
  // persists in localStorage and takes precedence over this default.
  const defaultVisibleColumns = useMemo(() => {
    return columns
      .map(c => (c as any).accessorKey || (c as any).id)
      .filter(Boolean) as string[];
  }, [columns]);

  const columnProfiles = useMemo(() => [
    {
      id: "overview", label: "Overview", icon: "📊",
      description: "Top-level performance snapshot - spend, purchases, revenue, and ROAS at a glance",
      columns: ["name", "spend", "metaConversions", "attributedRevenue", "blendedROAS", "cpa"],
      fullColumns: ["name", "spend", "impressions", "clicks", "ctr", "metaConversions", "attributedOrders", "attributedRevenue", "unverifiedRevenue", "blendedROAS", "cpa"],
    },
    {
      id: "newCustomers", label: "New Customers", icon: "👤",
      description: "How effectively each ad acquires new customers - acquisition cost, revenue, and long-term value",
      columns: ["name", "spend", "newCustomerOrders", "newCustomerCPA", "newCustomerROAS", "ltvCac", "repeatRate"],
      fullColumns: ["name", "spend", "newCustomerOrders", "newCustomerRevenue", "newCustomerCPA", "newCustomerROAS", "revenuePerNewCustomer", "newCustomersPerDay", "newCustomerRevenuePerDay", "avgLtv30", "avgLtv90", "avgLtvAll", "ltvCac", "repeatRate", "avgOrders"],
    },
    {
      id: "efficiency", label: "Efficiency", icon: "⚡",
      description: "Cost efficiency metrics - are you getting good value for your spend?",
      columns: ["name", "spend", "spendPerDay", "cpa", "blendedROAS", "ctr", "ltvCac"],
      fullColumns: ["name", "spend", "spendPerDay", "cpa", "newCustomerCPA", "blendedROAS", "newCustomerROAS", "ctr", "purchaseRate", "ltvCac", "newCustomersPerDay"],
    },
    {
      id: "funnel", label: "Funnel", icon: "🔽",
      description: "The customer journey from impression to purchase - where are people dropping off?",
      columns: ["name", "impressions", "linkClicks", "addToCart", "atcRate", "metaConversions", "purchaseRate"],
      fullColumns: ["name", "impressions", "linkClicks", "landingPageViews", "viewContent", "addToCart", "atcRate", "initiateCheckout", "checkoutRate", "metaConversions", "purchaseRate"],
    },
    {
      id: "revenue", label: "Revenue", icon: "💰",
      description: "Revenue breakdown - matched vs unmatched, new vs existing customer revenue",
      columns: ["name", "spend", "attributedRevenue", "blendedROAS", "newCustomerRevenue", "existingCustomerRevenue"],
      fullColumns: ["name", "spend", "attributedRevenue", "unverifiedRevenue", "blendedROAS", "newCustomerROAS", "newCustomerRevenue", "existingCustomerRevenue", "revenuePerNewCustomer", "totalLtvAll", "metaConversionValue"],
    },
    {
      id: "ltv", label: "Lifetime Value", icon: "🔄",
      description: "Long-term customer value - are the customers you're acquiring worth the investment?",
      columns: ["name", "newCustomerOrders", "avgLtv90", "avgLtvAll", "ltvCac", "repeatRate"],
      fullColumns: ["name", "newCustomerOrders", "ltvAcquiredCustomers", "avgLtv30", "avgLtv90", "avgLtv365", "avgLtvAll", "totalLtvAll", "ltvCac", "repeatRate", "avgOrders", "newCustomerCPA"],
    },
    {
      id: "creative", label: "Creative", icon: "🎬",
      description: "Creative performance - engagement, frequency, and video watch-through rates",
      columns: ["name", "impressions", "avgFrequency", "ctr", "videoP100"],
      fullColumns: ["name", "impressions", "avgFrequency", "ctr", "linkClicks", "videoP25", "videoP50", "videoP75", "videoP100"],
    },
    {
      id: "all", label: "All", icon: "📋",
      description: "Every available column",
      columns: columns.map(c => (c as any).accessorKey || (c as any).id).filter(Boolean),
    },
  ], [columns]);

  const blendedROAS = effectiveTotals.spend > 0
    ? ((effectiveTotals.attributedRevenue + effectiveTotals.unverifiedRevenue) / effectiveTotals.spend).toFixed(2) : "0";

  // Breadcrumb for drill-down
  const breadcrumbs: { label: string; onClick?: () => void }[] = [];
  if (filterCampaignId) {
    breadcrumbs.push({
      label: "All Campaigns",
      onClick: () => {
        setFilterCampaignId(null);
        setFilterCampaignName(null);
        setFilterAdSetId(null);
        setFilterAdSetName(null);
        setSelectedTab(0);
        setSelectedAdSetIds(new Set());
      },
    });
    breadcrumbs.push({
      label: filterCampaignName || filterCampaignId,
      onClick: filterAdSetId ? () => {
        setFilterAdSetId(null);
        setFilterAdSetName(null);
        setSelectedTab(1);
        setSelectedAdSetIds(new Set());
      } : undefined,
    });
  } else if (selectedCampaignIds.size > 0 && level !== "campaign") {
    breadcrumbs.push({
      label: `${selectedCampaignIds.size} campaign${selectedCampaignIds.size > 1 ? "s" : ""} selected`,
      onClick: () => {
        setSelectedTab(0);
        setSelectedAdSetIds(new Set());
      },
    });
  }
  if (filterAdSetId) {
    breadcrumbs.push({
      label: filterAdSetName || filterAdSetId,
    });
  }

  // Selection info for ad sets tab
  const parentFilterLabel = useMemo(() => {
    if (level === "adset" && selectedCampaignIds.size > 0 && !filterCampaignId) {
      return `Showing ad sets for ${selectedCampaignIds.size} selected campaign${selectedCampaignIds.size > 1 ? "s" : ""}`;
    }
    if (level === "ad" && selectedAdSetIds.size > 0 && !filterAdSetId) {
      return `Showing ads for ${selectedAdSetIds.size} selected ad set${selectedAdSetIds.size > 1 ? "s" : ""}`;
    }
    if (level === "ad" && selectedCampaignIds.size > 0 && !filterCampaignId && selectedAdSetIds.size === 0) {
      return `Showing ads for ${selectedCampaignIds.size} selected campaign${selectedCampaignIds.size > 1 ? "s" : ""}`;
    }
    return null;
  }, [level, selectedCampaignIds, selectedAdSetIds, filterCampaignId, filterAdSetId]);

  const tabStyle = (isActive: boolean): React.CSSProperties => ({
    padding: "10px 20px",
    fontSize: "14px",
    fontWeight: isActive ? 700 : 500,
    color: isActive ? "var(--l-accent-dark)" : "var(--l-text-secondary)",
    background: isActive ? "var(--l-bg)" : "var(--l-bg-subtle)",
    border: `1px solid ${isActive ? "var(--l-accent-40)" : "var(--l-border)"}`,
    borderBottom: isActive ? "1px solid var(--l-bg)" : "2px solid var(--l-accent-20)",
    borderRadius: "8px 8px 0 0",
    cursor: "pointer",
    marginRight: "-1px",
    marginBottom: "-1px",
    position: "relative" as const,
    zIndex: isActive ? 1 : 0,
  });

  // ── Page summary bullets ──
  // At-a-glance read-out of Meta spend efficiency for the selected range.
  // Always computed off campaignRows so the values stay in sync with the
  // Campaigns table footer, regardless of the active tab (campaign/adset/ad).
  const summaryBullets: SummaryBullet[] = useMemo(() => {
    const out: SummaryBullet[] = [];
    const rows: any[] = (campaignRows as any[]) || [];
    if (rows.length === 0) return out;

    const totals = rows.reduce(
      (acc, r) => {
        acc.spend += r.spend || 0;
        acc.attributedRevenue += r.attributedRevenue || 0;
        acc.unverifiedRevenue += r.unverifiedRevenue || 0;
        acc.attributedOrders += r.attributedOrders || 0;
        acc.newCustomerOrders += r.newCustomerOrders || 0;
        acc.newCustomerRevenue += r.newCustomerRevenue || 0;
        return acc;
      },
      { spend: 0, attributedRevenue: 0, unverifiedRevenue: 0, attributedOrders: 0, newCustomerOrders: 0, newCustomerRevenue: 0 },
    );

    const blendedROAS = totals.spend > 0
      ? Math.round(((totals.attributedRevenue + totals.unverifiedRevenue) / totals.spend) * 100) / 100
      : 0;

    if (totals.spend > 0) {
      out.push({
        tone: blendedROAS >= 1 ? "positive" : "negative",
        text: (
          <>
            <strong>Meta spend:</strong> {cs}{Math.round(totals.spend).toLocaleString()} at {blendedROAS}x blended ROAS ({cs}{Math.round(totals.attributedRevenue + totals.unverifiedRevenue).toLocaleString()} total revenue)
          </>
        ),
      });
    }

    if (totals.newCustomerOrders > 0) {
      const cac = totals.spend > 0 ? Math.round(totals.spend / totals.newCustomerOrders) : null;
      out.push({
        tone: "neutral",
        text: (
          <>
            <strong>New customers:</strong> {totals.newCustomerOrders.toLocaleString()} acquired via Meta{cac != null ? ` - ${cs}${cac} CAC` : ""}
          </>
        ),
      });
    }

    const topSpender = [...rows].sort((a, b) => (b.spend || 0) - (a.spend || 0))[0];
    if (topSpender) {
      const roas = topSpender.spend > 0
        ? Math.round(((topSpender.attributedRevenue + topSpender.unverifiedRevenue) / topSpender.spend) * 100) / 100
        : 0;
      out.push({
        tone: "neutral",
        text: (
          <>
            <strong>Biggest campaign:</strong> {topSpender.name || topSpender.campaignName || "-"} - {cs}{Math.round(topSpender.spend).toLocaleString()} at {roas}x ROAS
          </>
        ),
      });
    }

    const topROAS = [...rows]
      .filter((r) => (r.spend || 0) > 0 && (r.attributedOrders || 0) >= 5)
      .sort((a, b) => (b.blendedROAS || 0) - (a.blendedROAS || 0))[0];
    if (topROAS) {
      out.push({
        tone: "positive",
        text: (
          <>
            <strong>Best ROAS:</strong> {topROAS.name || topROAS.campaignName || "-"} - {topROAS.blendedROAS}x on {cs}{Math.round(topROAS.spend).toLocaleString()}
          </>
        ),
      });
    }

    const drains = rows
      .filter((r) => (r.spend || 0) > 0 && (r.blendedROAS || 0) > 0 && r.blendedROAS < 1)
      .sort((a, b) => (b.spend || 0) - (a.spend || 0));
    const topDrain = drains[0];
    if (topDrain) {
      out.push({
        tone: "negative",
        text: (
          <>
            <strong>Biggest drain:</strong> {topDrain.name || topDrain.campaignName || "-"} - {cs}{Math.round(topDrain.spend).toLocaleString()} spent at {topDrain.blendedROAS}x ROAS
          </>
        ),
      });
    }

    if (totals.attributedOrders > 0) {
      const aov = totals.attributedRevenue > 0 ? Math.round(totals.attributedRevenue / totals.attributedOrders) : 0;
      out.push({
        tone: "neutral",
        text: (
          <>
            <strong>Attributed orders:</strong> {totals.attributedOrders.toLocaleString()} matched via Meta - {cs}{aov} AOV
          </>
        ),
      });
    }

    return out;
  }, [campaignRows, cs]);

  return (
    <Page title="Ad Campaigns" fullWidth>
      <style dangerouslySetInnerHTML={{ __html: tileGridStyles }} />
      <ReportTabs>
      <BlockStack gap="500">
        {/* Hidden for V1 - bring back in V2. Loader wiring kept intact. */}
        {false && (
          <AiInsightsPanel
            pageKey="campaigns"
            cachedInsights={aiCachedInsights}
            generatedAt={aiGeneratedAt}
            isStale={aiIsStale}
            currencySymbol={cs}
          />
        )}
        <PageSummary bullets={summaryBullets} fromKey={fromKey} toKey={toKey} preset={preset} />
        {/* Breadcrumb */}
        {breadcrumbs.length > 0 && (
          <InlineStack gap="100" blockAlign="center">
            {breadcrumbs.map((bc, i) => (
              <InlineStack key={i} gap="100" blockAlign="center">
                {i > 0 && <Text as="span" variant="bodySm" tone="subdued">/</Text>}
                {bc.onClick ? (
                  <Button variant="plain" onClick={bc.onClick}>{bc.label}</Button>
                ) : (
                  <Text as="span" variant="bodyMd" fontWeight="semibold">{bc.label}</Text>
                )}
              </InlineStack>
            ))}
          </InlineStack>
        )}

        {currentSelectedIds.size > 0 && (
          <InlineStack gap="200" blockAlign="center">
            <Text as="p" variant="bodySm" tone="subdued">
              {currentSelectedIds.size} selected - summary cards show selected items only
            </Text>
            <Button size="slim" onClick={() => setCurrentSelectedIds(new Set())}>Clear selection</Button>
          </InlineStack>
        )}

        {/* ── All tiles (drag/drop, show/hide) ── */}
        {(() => {
          const rankRows = rowsByLevel[rankLevel] || [];

          // Funnel totals from effective rows
          const funnelSum = (key: string) => effectiveRows.reduce((s, r) => s + (r[key] || 0), 0);
          const funnelTotals = {
            impressions: funnelSum("impressions"),
            linkClicks: funnelSum("linkClicks"),
            addToCart: funnelSum("addToCart"),
            initiateCheckout: funnelSum("initiateCheckout"),
            metaConversions: funnelSum("metaConversions"),
          };

          const newCustRows = rowsByLevel[newCustLevel] || [];

          const fmtPrice = (v: number) => `${cs}${Math.round(v).toLocaleString()}`;
          const fmtRoas = (v: number) => `${v.toFixed(2)}x`;
          const fmtCount = (v: number) => Math.round(v).toLocaleString();
          const totalAdRev = effectiveTotals.attributedRevenue + effectiveTotals.unverifiedRevenue;
          const adRevPct = totalStoreRevenue > 0 ? Math.round((totalAdRev / totalStoreRevenue) * 100) : 0;
          const cpa = effectiveTotals.attributedOrders > 0 ? effectiveTotals.spend / effectiveTotals.attributedOrders : 0;
          const totalNewCust = effectiveTotals.newCustomerOrders || 0;
          const totalNewCustRev = effectiveTotals.newCustomerRevenue || 0;
          const newCustCPA = totalNewCust > 0 ? effectiveTotals.spend / totalNewCust : 0;
          const newCustROAS = effectiveTotals.spend > 0 && totalNewCustRev > 0 ? totalNewCustRev / effectiveTotals.spend : 0;
          const totalMetaOrders = effectiveTotals.attributedOrders || 0;
          const newCustPct = totalMetaOrders > 0 ? Math.round((totalNewCust / totalMetaOrders) * 100) : 0;

          // Previous period totals for delta badges
          const prevRows = prevCampaignRows || [];
          const prevT = computeClientTotals(prevRows);
          const prevAdRev = prevT.attributedRevenue + prevT.unverifiedRevenue;
          const prevCpa = prevT.attributedOrders > 0 ? prevT.spend / prevT.attributedOrders : 0;
          const prevNewCust = prevT.newCustomerOrders || 0;
          const prevNewCustRev = prevT.newCustomerRevenue || 0;
          const prevNewCustCPA = prevNewCust > 0 ? prevT.spend / prevNewCust : 0;
          const prevNewCustROAS = prevT.spend > 0 && prevNewCustRev > 0 ? prevNewCustRev / prevT.spend : 0;
          const prevBlendedROAS = prevT.spend > 0 ? prevAdRev / prevT.spend : 0;

          return (
            <>
            {/* Headline tiles: a glanceable scoreboard for the ad account.
                Live count + the standout ads in the period (best revenue,
                best new-customer engine, worst spender). Sparklines come
                from the aggregate dailyData since we don't preload per-ad
                daily series at the loader level (would multiply queries). */}
            <TileGrid pageId="campaigns-top" columns={4} tiles={[
              { id: "topLiveAds", label: "Live Ads", render: () => (
                <SummaryTile
                  label="Live Ads"
                  value={String(topTiles?.liveAdCount ?? 0)}
                  subtitle="Distinct ads that delivered on the most recent day"
                  tooltip={{ definition: "Number of distinct ads that delivered (spend > 0 or impressions > 0) on the most recent day in the selected period. Sparkline shows the daily count of distinct delivering ads, derived historically from MetaInsight." }}
                  chartData={dailyData} prevChartData={prevDailyData}
                  chartKey="liveAds" chartColor="#0E7490" chartFormat={(v) => `${Math.round(v)} ads`}
                />
              )},
              { id: "topRevenueAd", label: "Top Revenue Ad", render: () => topTiles?.topRevenueAd ? (
                <SummaryTile
                  label="Top Revenue Ad"
                  value={fmtPrice(topTiles.topRevenueAd.revenue || 0)}
                  subtitle={topTiles.topRevenueAd.name}
                  // For DPA ads Meta only returns a 64x64 grey placeholder
                  // PNG, which reads as a blank tile at this size - so we
                  // suppress the image and let SummaryTile paint a "D"
                  // badge via isDpa instead.
                  imageUrl={topTiles.topRevenueAd.productSetId
                    ? undefined
                    : (topTiles.topRevenueAd.imageUrl || topTiles.topRevenueAd.thumbnailUrl || undefined)}
                  isDpa={!!topTiles.topRevenueAd.productSetId}
                  tooltip={{ definition: `Highest revenue ad in the period: ${topTiles.topRevenueAd.name}. Sparkline shows total Meta-attributed revenue per day.` }}
                  chartData={dailyData} prevChartData={prevDailyData}
                  chartKey={(d) => (d.attributedRevenue || 0) + (d.unverifiedRevenue || 0)}
                  chartColor="#2E7D32" chartFormat={fmtPrice}
                  valueVariant="headingXl"
                />
              ) : (
                <SummaryTile label="Top Revenue Ad" value="—" subtitle="No ad revenue in period" />
              )},
              { id: "topNewCustAd", label: "Top New Customer Ad", render: () => topTiles?.topNewCustomerAd ? (
                <SummaryTile
                  label="Top New Customer Ad"
                  value={`${topTiles.topNewCustomerAd.newCustomerOrders} orders`}
                  subtitle={topTiles.topNewCustomerAd.name}
                  imageUrl={topTiles.topNewCustomerAd.productSetId
                    ? undefined
                    : (topTiles.topNewCustomerAd.imageUrl || topTiles.topNewCustomerAd.thumbnailUrl || undefined)}
                  isDpa={!!topTiles.topNewCustomerAd.productSetId}
                  tooltip={{ definition: `Ad that brought in the most new customers in the period: ${topTiles.topNewCustomerAd.name}. Sparkline shows daily new-customer orders.` }}
                  chartData={dailyData} prevChartData={prevDailyData}
                  chartKey="newCustomerOrders" chartColor="#7C3AED" chartFormat={(v) => `${Math.round(v)} orders`}
                  valueVariant="headingXl"
                />
              ) : (
                <SummaryTile label="Top New Customer Ad" value="—" subtitle="No new-customer orders attributed" />
              )},
              { id: "worstAd", label: "Poorest Performing Ad", render: () => topTiles?.worstAd ? (
                <SummaryTile
                  label="Poorest Performing Ad"
                  // Headline stacks two large values: the most damning ratio
                  // (CAC if there are new-customer orders, otherwise blended
                  // ROAS) on top, with the absolute spend in large directly
                  // below so "0.0x ROAS" is read against the cash being
                  // burned. Spend is the actionable number ("how much am I
                  // wasting?") and Andy wants it as a co-primary value, not
                  // a small subtitle aside.
                  value={
                    <span style={{ display: "flex", flexDirection: "column", lineHeight: 1.1 }}>
                      <span>
                        {topTiles.worstAd.newCustomerCPA != null
                          ? `${cs}${Math.round(topTiles.worstAd.newCustomerCPA).toLocaleString()} CAC`
                          : `${(topTiles.worstAd.roas || 0).toFixed(1)}x ROAS`}
                      </span>
                      <span style={{ fontSize: "22px", fontWeight: 700, color: "#DC2626", marginTop: "4px" }}>
                        {cs}{Math.round(topTiles.worstAd.spend).toLocaleString()} spent
                      </span>
                    </span>
                  }
                  subtitle={topTiles.worstAd.name}
                  imageUrl={topTiles.worstAd.productSetId
                    ? undefined
                    : (topTiles.worstAd.imageUrl || topTiles.worstAd.thumbnailUrl || undefined)}
                  isDpa={!!topTiles.worstAd.productSetId}
                  tooltip={{ definition: `Worst-performing ad among those spending in the upper half of the account. Ranked by CAC where new-customer orders exist, otherwise by spend ÷ ROAS so high-spend zero-return ads still surface. Spend in period: ${cs}${Math.round(topTiles.worstAd.spend).toLocaleString()}.` }}
                  valueVariant="headingXl"
                />
              ) : (
                <SummaryTile label="Poorest Performing Ad" value="—" subtitle="Not enough spend to identify" />
              )},
            ] as TileDef[]} />

            {/* Ad Explorer pinned above summary tiles - the primary entry point
                for browsing campaigns/ad sets/ads. Lives outside TileGrid so it
                cannot be reordered below the summary mini-tiles. */}
            <Card>
              <BlockStack gap="300">
                <InlineStack align="space-between" blockAlign="center">
                  <BlockStack gap="100">
                    <Text as="h2" variant="headingLg">Ad Explorer</Text>
                    <Text as="p" variant="bodySm" tone="subdued">
                      Every {rankLevel === "campaign" ? "campaign" : rankLevel === "adset" ? "ad set" : "ad"} that ran in the selected period - sortable by orders, revenue, spend, ROAS or CPA. Filter by customer type, search by name, click any row to see its full lifecycle and change history.
                    </Text>
                  </BlockStack>
                  <BigLevelToggle options={LEVEL_OPTIONS} selected={rankLevel} onChange={setRankLevel} />
                </InlineStack>
                <AdExplorerTable rows={rankRows} cs={cs} entityType={rankLevel as "campaign" | "adset" | "ad"} adDemographicsByAd={adDemographicsByAd} onEntityClick={(id, name) => setDrawerEntity({ objectType: rankLevel as any, objectId: id, objectName: name })} />
              </BlockStack>
            </Card>
            <TileGrid pageId="campaigns-v2" columns={4} tiles={[
              { id: "totalSpend", label: "Total Ad Spend", render: () => (
                <SummaryTile label="Meta Ad Spend" value={fmtPrice(effectiveTotals.spend)}
                  tooltip={{ definition: "Total amount spent on Meta ads within the selected date range" }}
                  currentValue={effectiveTotals.spend} previousValue={prevT.spend}
                  chartData={dailyData} prevChartData={prevDailyData} chartKey="spend" chartColor="#5C6AC4" chartFormat={fmtPrice} />
              )},
              { id: "totalRevenue", label: "Meta Ad Revenue", render: () => (
                <SummaryTile label="Meta Ad Revenue" value={fmtPrice(totalAdRev)}
                  subtitle={`${adRevPct}% of total store revenue (${fmtPrice(totalStoreRevenue)})`}
                  tooltip={{ definition: "Revenue from orders attributed to Meta ads within the selected date range, combining verified matches and Meta-reported conversions" }}
                  currentValue={totalAdRev} previousValue={prevAdRev}
                  chartData={dailyData} prevChartData={prevDailyData} chartKey={(d) => d.attributedRevenue + d.unverifiedRevenue}
                  chartColor="#2E7D32" chartFormat={fmtPrice} />
              )},
              { id: "totalRoas", label: "Meta ROAS", render: () => (
                <SummaryTile label="Meta ROAS" value={`${blendedROAS}x`}
                  tooltip={{ definition: "Return on Meta ad spend within the selected date range", calc: "(Matched revenue + unverified revenue) ÷ spend" }}
                  currentValue={parseFloat(blendedROAS)} previousValue={prevBlendedROAS}
                  chartData={dailyData} prevChartData={prevDailyData} chartKey={(d) => d.spend > 0 ? (d.attributedRevenue + d.unverifiedRevenue) / d.spend : 0}
                  chartColor="#008060" chartFormat={fmtRoas} />
              )},
              { id: "costPerOrder", label: "Meta Cost per Order", render: () => (
                <SummaryTile label="Meta Cost per Order" value={cpa > 0 ? fmtPrice(cpa) : "\u2014"}
                  tooltip={{ definition: "Average Meta ad cost per attributed order within the selected date range", calc: "Meta spend ÷ attributed orders" }}
                  currentValue={cpa} previousValue={prevCpa} lowerIsBetter
                  chartData={dailyData} prevChartData={prevDailyData} chartKey={(d) => d.attributedOrders > 0 ? d.spend / d.attributedOrders : 0}
                  chartColor="#D4760A" chartFormat={fmtPrice} />
              )},
              { id: "newCustomers", label: "New Meta Customers", render: () => (
                <SummaryTile label="New Meta Customers" value={(uniqueNewMetaCustomers || 0).toLocaleString()}
                  subtitle={totalNewCustomersInPeriod > 0 ? `${Math.round((uniqueNewMetaCustomers / totalNewCustomersInPeriod) * 100)}% of all new customers` : "No new customers in period"}
                  tooltip={{ definition: "Unique first-time customers acquired through Meta ads within the selected date range (deduplicated - a customer placing multiple orders on their first day counts once)" }}
                  currentValue={uniqueNewMetaCustomers || 0} previousValue={prevUniqueNewMetaCustomers || 0}
                  chartData={dailyData} prevChartData={prevDailyData} chartKey="newCustomerOrders" chartColor="#6366F1" chartFormat={(v) => v.toLocaleString()} />
              )},
              { id: "newCustRevenue", label: "New Meta Customer Revenue", render: () => (
                <SummaryTile label="New Meta Customer Revenue" value={fmtPrice(totalNewCustRev)}
                  tooltip={{ definition: "Revenue from first-time Meta-acquired customers within the selected date range" }}
                  currentValue={totalNewCustRev} previousValue={prevNewCustRev}
                  chartData={dailyData} prevChartData={prevDailyData} chartKey="newCustomerRevenue" chartColor="#0E7490" chartFormat={fmtPrice} />
              )},
              { id: "newCustRoas", label: "New Meta Customer ROAS", render: () => (
                <SummaryTile label="New Meta Customer ROAS" value={newCustROAS > 0 ? `${newCustROAS.toFixed(2)}x` : "\u2014"}
                  tooltip={{ definition: "Return on Meta ad spend from new customer revenue only, within the selected date range", calc: "New customer revenue ÷ Meta spend" }}
                  currentValue={newCustROAS} previousValue={prevNewCustROAS}
                  chartData={dailyData} prevChartData={prevDailyData} chartKey={(d) => d.spend > 0 && d.newCustomerRevenue > 0 ? d.newCustomerRevenue / d.spend : 0}
                  chartColor="#0891B2" chartFormat={fmtRoas} />
              )},
              { id: "newCustCostPerOrder", label: "Meta New Customer CPA", render: () => (
                <SummaryTile label="Meta New Customer CPA" value={totalNewCust > 0 ? fmtPrice(newCustCPA) : "\u2014"}
                  tooltip={{ definition: "Average Meta ad cost to acquire one new customer within the selected date range", calc: "Meta spend ÷ new customer orders" }}
                  currentValue={newCustCPA} previousValue={prevNewCustCPA} lowerIsBetter
                  chartData={dailyData} prevChartData={prevDailyData} chartKey={(d) => d.newCustomerOrders > 0 ? d.spend / d.newCustomerOrders : 0}
                  chartColor="#B45309" chartFormat={fmtPrice} />
              )},
              { id: "adFunnel", label: "Top Ads for New Customers", span: 4, render: () => (
                <Card>
                  <BlockStack gap="300">
                    <Text as="h2" variant="headingLg">Top Ads for New Customers</Text>
                    <Text as="p" variant="bodySm" tone="subdued">
                      The 10 ads driving the most new customers in the selected period. Sort by orders to see your biggest acquisition engines, or by ROAS to see your most efficient. Click any ad to open its lifecycle.
                    </Text>
                    <TopAdsForNewCustomersTile
                      adRows={adRows}
                      cs={cs}
                      onAdClick={(id, name) => setDrawerEntity({ objectType: "ad", objectId: id, objectName: name })}
                    />
                  </BlockStack>
                </Card>
              )},
              { id: "platformPerf", label: "Platform Performance", span: 4, render: () => (
                <BreakdownPerfTile
                  title="Platform Performance"
                  subtitle="Which platforms (Instagram, Facebook, Messenger, Audience Network) are pulling their weight - spot the platforms to scale and the ones to cut."
                  data={platformPerf}
                  cs={cs}
                  defaultLevel="campaign"
                  defaultSort="roas"
                />
              )},
              { id: "placementPerf", label: "Placement Performance", span: 4, render: () => (
                <BreakdownPerfTile
                  title="Placement Performance"
                  subtitle="Where your ads are actually showing up - feeds, stories, reels, marketplace - so you can see which placements convert and which are wasting spend."
                  data={placementPerf}
                  cs={cs}
                  type="placement"
                />
              )},
              { id: "adAge", label: "Ad Age", span: 2, render: () => (
                <Card>
                  <AdAgeTile adRows={adRows} cs={cs} onAdClick={(id, name) => setDrawerEntity({ objectType: "ad", objectId: id, objectName: name })} />
                </Card>
              )},
            ] as TileDef[]} />
            </>
          );
        })()}

        {/* Performance table with folder tabs */}
        <div>
          <div ref={perfTabsRef} style={{
            position: "sticky",
            top: 0,
            zIndex: 11,
            backgroundColor: "#fff",
            paddingTop: "4px",
          }}>
            <Text as="h2" variant="headingLg">Ad Performance</Text>
            <div style={{ display: "flex", alignItems: "flex-end", marginTop: "12px" }}>
              {TAB_LABELS.map((label, i) => (
                <button
                  key={TAB_LEVELS[i]}
                  onClick={() => handleTabChange(i)}
                  style={tabStyle(selectedTab === i)}
                >
                  {label}
                </button>
              ))}
              <div style={{ flex: 1, borderBottom: "1px solid var(--l-accent-40)" }} />
            </div>
          </div>
          <div style={{
            background: "var(--l-bg)",
            border: "1px solid var(--l-accent-40)",
            borderTop: "none",
            borderRadius: "0 0 12px 12px",
            padding: "16px",
          }}>
            <BlockStack gap="300">
              {parentFilterLabel && (
                <Text as="p" variant="bodySm" tone="subdued">{parentFilterLabel}</Text>
              )}
              <InteractiveTable
                columns={columns}
                data={displayRows}
                footerRow={footerRow}
                defaultVisibleColumns={defaultVisibleColumns}
                tableId="campaigns"
                fitContentColumns
                enableDownload
                downloadFilename="ad-performance"
                stickyTopOffset={perfTabsHeight}
                rowBackgroundFn={showBreakdown ? (original) => {
                  if (original._isBreakdownRow) return "#fff";
                  return "#f7f8fa";
                } : undefined}
                toolbarExtra={
                  <Popover
                    active={breakdownOpen}
                    activator={
                      <Button
                        size="slim"
                        onClick={() => setBreakdownOpen(v => !v)}
                        disclosure
                      >
                        {breakdown === "none" ? "Breakdown" : BREAKDOWN_LABELS[breakdown] || breakdown}
                      </Button>
                    }
                    onClose={() => setBreakdownOpen(false)}
                    preferredAlignment="left"
                  >
                    <div style={{ padding: "8px 12px", fontWeight: 600, fontSize: "12px", color: "#6d7175", borderBottom: "1px solid #e1e3e5" }}>
                      Breakdown
                    </div>
                    <ActionList
                      items={[
                        { content: "None", active: breakdown === "none", onAction: () => handleBreakdownChange("none") },
                        ...Object.entries(BREAKDOWN_LABELS)
                          .filter(([k]) => k !== "none")
                          .map(([value, label]) => ({
                            content: label,
                            active: breakdown === value,
                            onAction: () => handleBreakdownChange(value),
                          })),
                      ]}
                    />
                  </Popover>
                }
              />
            </BlockStack>
          </div>
        </div>

        {breakdown !== "none" && !hasBreakdownData && (
          <Card>
            <BlockStack gap="200">
              <Text as="p" variant="bodyMd" tone="subdued">
                No breakdown data yet. Run "Sync Meta (7d)" from the dashboard to pull breakdown data.
              </Text>
            </BlockStack>
          </Card>
        )}
      </BlockStack>
      </ReportTabs>
      <EntityTimelineDrawer
        shopDomain={shopDomain}
        open={!!drawerEntity}
        entity={drawerEntity}
        onClose={() => setDrawerEntity(null)}
      />
    </Page>
  );
}

function computeClientTotals(rows) {
  return rows.reduce((acc, r) => ({
    spend: acc.spend + r.spend,
    impressions: acc.impressions + r.impressions,
    clicks: acc.clicks + r.clicks,
    metaConversions: acc.metaConversions + (r.metaConversions || 0),
    metaConversionValue: acc.metaConversionValue + (r.metaConversionValue || 0),
    attributedOrders: acc.attributedOrders + (r.attributedOrders || 0),
    attributedRevenue: acc.attributedRevenue + (r.attributedRevenue || 0),
    newCustomerRevenue: acc.newCustomerRevenue + (r.newCustomerRevenue || 0),
    existingCustomerRevenue: acc.existingCustomerRevenue + (r.existingCustomerRevenue || 0),
    unverifiedRevenue: acc.unverifiedRevenue + (r.unverifiedRevenue || 0),
    newCustomerOrders: acc.newCustomerOrders + (r.newCustomerOrders || 0),
    existingCustomerOrders: acc.existingCustomerOrders + (r.existingCustomerOrders || 0),
  }), {
    spend: 0, impressions: 0, clicks: 0,
    metaConversions: 0, metaConversionValue: 0,
    attributedOrders: 0, attributedRevenue: 0, unverifiedRevenue: 0,
    newCustomerRevenue: 0, existingCustomerRevenue: 0,
    newCustomerOrders: 0, existingCustomerOrders: 0,
  });
}
