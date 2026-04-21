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
import SummaryTile from "../components/SummaryTile";
import ChangesAnnotationStrip from "../components/ChangesAnnotationStrip";
import EntityTimelineDrawer, { type EntityRef } from "../components/EntityTimelineDrawer";
import { useState, useMemo, useCallback, useRef, useEffect } from "react";
import { type ColumnDef } from "@tanstack/react-table";
import { authenticate } from "../shopify.server";
import db from "../db.server";
import { parseDateRange } from "../utils/dateRange.server";
import { shopLocalDayKey, shopRangeBounds } from "../utils/shopTime.server";
import { getCachedInsights, computeDataHash, generateInsights } from "../services/aiAnalysis.server";
import { setProgress, failProgress, completeProgress } from "../services/progress.server";
import { cached as queryCached, DEFAULT_TTL } from "../services/queryCache.server";
import { loadLtvSnapshot } from "../services/ltvSnapshot.server.js";

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
      const rev = order.frozenTotalPrice || 0;
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
    const rev = order.frozenTotalPrice || 0;
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

  const { fromDate, toDate, fromKey, toKey, compareFrom, compareTo, compareFromKey, compareToKey, hasComparison, compareLabel } = parseDateRange(request, tz);

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
    shopifyOrderId: true, createdAt: true, frozenTotalPrice: true, totalRefunded: true,
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

  // Loader helpers — fetch rollup then pre-aggregate at all 3 levels in a single pass.
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

  const [currentAggRaw, prevAggRaw, compareAggRaw, metaEntities, ltvSnapshot, dailyChart, windowOrdersRaw] = await Promise.all([
    time("campAgg", queryCached(`${shopDomain}:campAgg:${fromKey}:${toKey}`, DEFAULT_TTL, fetchAndAggregate(fromDate, toDate))),
    time("campAggPrev", queryCached(`${shopDomain}:campAgg:${prevFromKey}:${prevToKey}`, DEFAULT_TTL, fetchAndAggregate(_prevFromRP, _prevToRP))),
    (hasComparison && compareFrom && compareTo)
      ? time("campAggComp", queryCached(`${shopDomain}:campAgg:${compareKey}`, DEFAULT_TTL, fetchAndAggregate(compareFrom, compareTo)))
      : Promise.resolve(null),
    time("metaEntities", queryCached(`${shopDomain}:metaEntities`, DEFAULT_TTL, () =>
      db.metaEntity.findMany({
        where: { shopDomain },
        select: { entityType: true, entityId: true, createdTime: true },
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
    time("windowOrders", queryCached(`${shopDomain}:campWindowOrders:${windowStartKey}:${windowEndKey}`, DEFAULT_TTL, () =>
      db.order.findMany({
        where: { shopDomain, isOnlineStore: true, createdAt: { gte: windowStart, lte: windowEnd } },
        select: orderSelect,
      }),
    )),
  ]);
  // insights is now a per-day aggregation (groupBy result); rename for downstream clarity
  const insights = dailyChart as any[];

  // Alias kept to minimise diff in the rest of the loader — all downstream code
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

  const currencySymbol = (shop?.shopifyCurrency || "GBP") === "GBP" ? "£"
    : (shop?.shopifyCurrency || "GBP") === "EUR" ? "€" : "$";

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
    (sum, o) => sum + Math.max(0, (o.frozenTotalPrice || 0) - (o.totalRefunded || 0)),
    0,
  );

  // Build entity created_time map from pre-fetched metaEntities
  const entityCreatedMap = {};
  for (const e of metaEntities) {
    if (e.createdTime) entityCreatedMap[`${e.entityType}:${e.entityId}`] = e.createdTime;
  }

  const now = new Date();
  const reportingPeriodDays = Math.max(1, Math.ceil((toDate.getTime() - fromDate.getTime()) / 86400000));
  const r2g = (v) => Math.round(v * 100) / 100;

  // `orderMap` is used downstream for the breakdown and daily-chart blocks.
  // It's scoped to the window (was: all-time) — every downstream consumer
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

  // Count ALL new customers in the period using the order row's own
  // isNewCustomerOrder flag (set at sync time from customerOrderCountAtPurchase).
  // Previously this required loading the full customer table; now it's free.
  const totalNewCustomersInPeriodIds = new Set<string>();
  for (const o of allOrders) {
    if (!o.isOnlineStore) continue;
    if (o.createdAt < fromDate || o.createdAt > toDate) continue;
    if (o.customerOrderCountAtPurchase === 1 && o.shopifyCustomerId) {
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
    };
  });

  // currentAgg is now pre-computed and cached — just compute rows on the small aggregated output.
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
  // when hasComparison is true — use it directly rather than re-deriving.
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
  // prevAgg is cached (same pattern as currentAgg) — just compute rows on the small aggregated output.
  const prevCampaignRows = computeRows(prevAgg.campaign);
  const prevAdsetRows = computeRows(prevAgg.adset);
  const prevAdRows = computeRows(prevAgg.ad);

  // Count unique new Meta customers acquired in the previous period
  for (const [custId, acq] of Object.entries(customerAcq)) {
    if (acq.acquisitionDate >= prevFrom && acq.acquisitionDate <= prevTo) {
      prevUniqueNewMetaCustomers++;
    }
  }

  // Breakdown maps — keyed by entity ID, value is array of computed sub-rows
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

          const rev = order.frozenTotalPrice || 0;
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
            // LTV doesn't apply at breakdown level — it's per-customer, not per-country/platform
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
  const emptyDay = (date: string) => ({ date, spend: 0, impressions: 0, attributedRevenue: 0, unverifiedRevenue: 0, newCustomerOrders: 0, newCustomerRevenue: 0, attributedOrders: 0 });
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
    if (!dailyMap[day]) dailyMap[day] = { date: day, spend: 0, impressions: 0, attributedRevenue: 0, unverifiedRevenue: 0, newCustomerOrders: 0, newCustomerRevenue: 0, attributedOrders: 0 };
    dailyMap[day].spend += (ins._sum?.spend || 0);
    dailyMap[day].impressions += (ins._sum?.impressions || 0);
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
      if (!dailyMap[day]) dailyMap[day] = { date: day, spend: 0, impressions: 0, attributedRevenue: 0, unverifiedRevenue: 0, newCustomerOrders: 0, newCustomerRevenue: 0, attributedOrders: 0 };
      const rev = order.frozenTotalPrice || 0;
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
      if (!dailyMap[day]) dailyMap[day] = { date: day, spend: 0, impressions: 0, attributedRevenue: 0, unverifiedRevenue: 0, newCustomerOrders: 0, newCustomerRevenue: 0, attributedOrders: 0 };
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
      const rev = order.frozenTotalPrice || 0;
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
  // stored directly, so we can get precise counts — no proportional allocation needed.
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

  // Meta change log events in the period — cheap query (tight index), cached
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
  // Per-entity change counts keyed by objectId — used for Campaigns table badges.
  const changeCountsByObjectId: Record<string, number> = {};
  for (const c of metaChanges) {
    changeCountsByObjectId[c.objectId] = (changeCountsByObjectId[c.objectId] || 0) + 1;
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
    fromKey, toKey,
    changeEvents: changeEventsForStrip,
    changeCountsByObjectId,
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

    // Fire and forget — build data and generate in background
    (async () => {
      try {
        const shop = await db.shop.findUnique({ where: { shopDomain } });
        const tz = shop?.shopifyTimezone || "UTC";
        const { fromDate, toDate, fromKey: dateFromStr, toKey: dateToStr } = parseDateRange(request, tz);
        const cs = (shop?.shopifyCurrency || "GBP") === "GBP" ? "£"
          : (shop?.shopifyCurrency || "GBP") === "EUR" ? "€" : "$";

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
          select: { shopifyOrderId: true, createdAt: true, frozenTotalPrice: true, totalRefunded: true },
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
          const rev = (order.frozenTotalPrice || 0) - (order.totalRefunded || 0);
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

        // Total store revenue — net of refunds so it's comparable to
        // attributed revenue on the same page.
        const ordersInRange = orders.filter(o => o.createdAt >= fromDate && o.createdAt <= toDate);
        const totalStoreRevenue = ordersInRange.reduce(
          (sum, o) => sum + Math.max(0, (o.frozenTotalPrice || 0) - (o.totalRefunded || 0)),
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
    <div style={{ display: "inline-flex", gap: "1px", background: "#e4e5e7", borderRadius: "6px", padding: "2px" }}>
      {options.map(o => (
        <button
          key={o.id}
          onClick={() => onChange(o.id)}
          style={{
            padding: "3px 8px", fontSize: "11px", fontWeight: selected === o.id ? 600 : 400,
            background: selected === o.id ? "#fff" : "transparent",
            border: "none", borderRadius: "4px", cursor: "pointer",
            color: selected === o.id ? "#1a1a1a" : "#6d7175",
            boxShadow: selected === o.id ? "0 1px 2px rgba(0,0,0,0.08)" : "none",
          }}
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
  { id: "blendedROAS", label: "ROAS", higherIsBetter: true, description: "Return on ad spend — total confirmed revenue (matched + unmatched) divided by spend. Higher is better." },
  { id: "newCustomerCPA", label: "New Customer CPA", higherIsBetter: false, description: "Cost to acquire each new customer — total spend divided by number of first-time buyers. Lower is better." },
  { id: "avgLtvAll", label: "LTV", higherIsBetter: true, description: "Average all-time revenue per customer acquired by this ad, across every order they've ever placed. Higher means more valuable customers." },
  { id: "ltvCac", label: "LTV:CAC", higherIsBetter: true, description: "Lifetime value vs acquisition cost — how much lifetime revenue each acquired customer generates relative to what it cost to acquire them. Above 3x is strong." },
  { id: "ctr", label: "CTR", higherIsBetter: true, description: "Click-through rate — percentage of people who clicked after seeing the ad. Higher means more engaging creative." },
];

function formatMetricValue(metricId: string, value: number, cs: string): string {
  if (value == null) return "—";
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

  // ROAS / LTV:CAC — absolute thresholds with smooth gradient
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

  // CPA — absolute thresholds, lower is better
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

  // Relative metrics — use data range for gradient
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

function SortableHeader({ col, label, width, sortCol, sortDir, onSort }: {
  col: string; label: string; width: string; sortCol: string; sortDir: "asc" | "desc"; onSort: (col: string) => void;
}) {
  const active = sortCol === col;
  return (
    <span
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

  // Hover sparkline state — same lazy-fetch pattern as the Ad Age tile.
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
              <span style={{ ...colStyle, width: "60px", color: item.blendedCPA > 0 ? "#444" : "#bbb" }}>{item.blendedCPA > 0 ? `${cs}${Math.round(item.blendedCPA).toLocaleString()}` : "—"}</span>
              <span style={{ ...colStyle, width: "64px", color: item.newCustomerCPA > 0 ? "#444" : "#bbb" }}>{item.newCustomerCPA > 0 ? `${cs}${Math.round(item.newCustomerCPA).toLocaleString()}` : "—"}</span>
              <span style={{ ...colStyle, width: "56px", fontWeight: 600, color: metricGradientColor("blendedROAS", roas, sorted.map(r => r.blendedROAS || 0), true) }}>{roas > 0 ? `${roas}x` : "—"}</span>
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
  return (
    <div style={{
      position: "fixed", left: x, top: y,
      transform: "translate(0, -50%)",
      background: "#fff", border: "1px solid #E5E7EB", borderRadius: 8,
      boxShadow: "0 6px 20px rgba(0,0,0,0.12)",
      zIndex: 9999, pointerEvents: "none",
      minWidth: w,
    }}>
      {body}
    </div>
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
              <span style={{ ...colStyle, width: "56px", color: ncCpa > 0 ? "#444" : "#bbb" }}>{ncCpa > 0 ? `${cs}${Math.round(ncCpa).toLocaleString()}` : "—"}</span>
              <span style={{ ...colStyle, width: "56px", color: ncRev > 0 ? "#444" : "#bbb" }}>{ncRev > 0 ? `${cs}${Math.round(ncRev).toLocaleString()}` : "—"}</span>
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
  // Generic placements: feed/story/reels/search could be either — show as generic
  return breakdownValue;
}

function BreakdownPerfTile({ title, data, cs, defaultLevel = "overall", defaultSort = "spend", type = "platform" }: {
  title: string; data: Record<string, any[]>; cs: string; defaultLevel?: string; defaultSort?: string; type?: "platform" | "placement";
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
  const VISIBLE_ROWS = 6;

  return (
    <Card>
      <BlockStack gap="300">
        <InlineStack align="space-between" blockAlign="center">
          <Text as="h3" variant="headingSm">{title}</Text>
          <div style={{ display: "inline-flex", border: "1px solid #D1D5DB", borderRadius: "8px", overflow: "hidden" }}>
            {BREAKDOWN_LEVELS.map(o => (
              <button
                key={o.id}
                onClick={() => setLevel(o.id)}
                style={{
                  padding: "6px 14px", fontSize: "13px", fontWeight: 600, border: "none", cursor: "pointer",
                  transition: "all 0.15s",
                  background: level === o.id ? "#7C3AED" : "#fff",
                  color: level === o.id ? "#fff" : "#6B7280",
                }}
                onMouseEnter={(e) => { if (level !== o.id) (e.target as HTMLElement).style.background = "#F3F4F6"; }}
                onMouseLeave={(e) => { if (level !== o.id) (e.target as HTMLElement).style.background = "#fff"; }}
              >{o.label}</button>
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
                  ? (type === "placement" ? `${placementLabel} — ${r.entityName}` : r.entityName)
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
                        <span style={{ fontSize: "13px", fontWeight: 500, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", flex: 1, minWidth: 0, display: "inline-flex", alignItems: "center", gap: "5px" }} title={hasEntity ? `${r.breakdownValue} — ${r.entityName}` : r.breakdownValue}>
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
                    <span style={{ ...colStyle, width: "60px", color: r.cpa > 0 ? "#444" : "#bbb" }}>{r.cpa > 0 ? `${cs}${Math.round(r.cpa).toLocaleString()}` : "—"}</span>
                    <span style={{ ...colStyle, width: "64px", color: r.newCustomerCPA > 0 ? "#444" : "#bbb" }}>{r.newCustomerCPA > 0 ? `${cs}${Math.round(r.newCustomerCPA).toLocaleString()}` : "—"}</span>
                    <span style={{ ...colStyle, width: "56px", fontWeight: 600, color: metricGradientColor("blendedROAS", roas, items.map(x => x.roas || 0), true) }}>{roas > 0 ? `${roas}x` : "—"}</span>
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
      {/* Funnel steps — widths proportional via sqrt scaling */}
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

function WastedSpendList({ items, cs }: { items: { name: string; spend: number; orders: number; roas: number }[]; cs: string }) {
  if (items.length === 0) return <div style={{ color: "#999", fontSize: "13px", padding: "8px 0" }}>No wasted spend detected</div>;
  return (
    <div style={{ maxHeight: "240px", overflowY: "auto", display: "flex", flexDirection: "column", gap: "6px" }}>
      {items.map((item, i) => (
        <div key={i} style={{ display: "flex", alignItems: "center", gap: "8px", padding: "6px 8px", background: "#fef2f2", borderRadius: "6px", borderLeft: "3px solid #C62828" }}>
          <div style={{ flex: 1, minWidth: 0 }}>
            <div style={{ fontSize: "12px", fontWeight: 500, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }} title={item.name}>{item.name}</div>
            <div style={{ display: "flex", gap: "12px", marginTop: "2px" }}>
              <span style={{ fontSize: "11px", color: "#C62828" }}>Spend: <strong>{cs}{Math.round(item.spend).toLocaleString()}</strong></span>
              <span style={{ fontSize: "11px", color: "#6d7175" }}>Orders: <strong>{item.orders}</strong></span>
              <span style={{ fontSize: "11px", color: "#6d7175" }}>ROAS: <strong>{item.roas > 0 ? `${item.roas}x` : "0x"}</strong></span>
            </div>
          </div>
        </div>
      ))}
    </div>
  );
}

// Scrollable list of every ad with its age, spend, ROAS, and creative-fatigue
// flag. Header shows a colour-coded "X days since last new ad launched"
// callout — green if recent, amber if drifting, red if it's been a while.
//
// Per-row signals:
//   • Frequency badge (avg in-period frequency) — highlighted ≥ 3 since
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
            const ageLabel = days == null ? "—" : days === 0 ? "Today" : `${days}d`;
            const roas = r.spend > 0 ? ((r.attributedRevenue + (r.unverifiedRevenue || 0)) / r.spend) : 0;
            const newCust = r.newCustomerOrders || 0;
            const freq = r.avgFrequency || 0;
            const freqHigh = freq >= 3;
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
                <div style={{
                  fontSize: 11, fontWeight: 700, color: colour,
                  minWidth: 44, textAlign: "right", fontVariantNumeric: "tabular-nums",
                }}>{ageLabel}</div>
                {/* Title takes whatever room the metrics don't claim */}
                <div style={{ flex: 1, minWidth: 0, fontSize: 12, fontWeight: 500, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }} title={r.name}>{r.name}</div>
                {/* All metrics inline-right of the title — no second line. */}
                <div style={{ display: "flex", gap: 12, alignItems: "center", flexShrink: 0, fontVariantNumeric: "tabular-nums" }}>
                  <span style={{ fontSize: 11, color: "#6B7280" }}>{cs}{Math.round(r.spend || 0).toLocaleString()}</span>
                  <span style={{ fontSize: 11, color: roas >= 2 ? "#059669" : roas > 0 ? "#6B7280" : "#9CA3AF", fontWeight: 600 }}>{roas > 0 ? `${roas.toFixed(2)}x` : "0x"}</span>
                  <span style={{ fontSize: 11, color: "#6B7280" }} title="New customers in period">+{newCust}</span>
                  {freq > 0 && (
                    <span style={{
                      fontSize: 10, fontWeight: 700,
                      padding: "1px 6px", borderRadius: 8,
                      background: freqHigh ? "#FEE2E2" : "#E5E7EB",
                      color: freqHigh ? "#B91C1C" : "#374151",
                    }} title={freqHigh ? "Average frequency ≥ 3 — fatigue likely" : "Average impressions per unique reach in period"}>
                      ƒ {freq.toFixed(1)}{freqHigh ? "⚠" : ""}
                    </span>
                  )}
                  {/* Click affordance — chevron, brightens on row hover */}
                  {interactive && (
                    <span aria-hidden="true" style={{
                      fontSize: 14, color: "#94A3B8",
                      transition: "color 0.12s ease, transform 0.12s ease",
                      width: 12, textAlign: "center",
                    }} className="ad-age-chevron">›</span>
                  )}
                </div>
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
  return (
    <div style={{
      position: "fixed", left: x, top: y,
      transform: "translate(0, -50%)",
      background: "#fff", border: "1px solid #E5E7EB", borderRadius: 8,
      boxShadow: "0 6px 20px rgba(0,0,0,0.12)",
      zIndex: 9999, pointerEvents: "none",
      minWidth: w,
    }}>
      {body}
    </div>
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
    shopDomain, fromKey, toKey,
    changeEvents, changeCountsByObjectId,
  } = useLoaderData();
  const cs = currencySymbol || "£";
  const [searchParams, setSearchParams] = useSearchParams();

  // Change log integration — strip above the tiles + drawer triggered from
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

  // Checkbox selection — persists across tab changes for filtering
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
      adAgeDays: "—",
      spend: `${cs}${r2(spend).toLocaleString()}`,
      impressions: impressions.toLocaleString(),
      clicks: clicks.toLocaleString(),
      ctr: `${ctr}%`,
      avgFrequency: "—",
      linkClicks: linkClicks.toLocaleString(),
      landingPageViews: landingPageViews.toLocaleString(),
      viewContent: viewContent.toLocaleString(),
      addToCart: addToCart.toLocaleString(),
      atcRate: viewContent > 0 ? `${atcRate}%` : "—",
      initiateCheckout: initiateCheckout.toLocaleString(),
      checkoutRate: addToCart > 0 ? `${checkoutRate}%` : "—",
      metaConversions: metaConversions.toLocaleString(),
      purchaseRate: initiateCheckout > 0 ? `${purchaseRate}%` : "—",
      attributedRevenue: `${cs}${r2(attributedRevenue).toLocaleString()}`,
      unverifiedRevenue: `${cs}${r2(unverifiedRevenue).toLocaleString()}`,
      blendedROAS: `${blendedROAS}x`,
      attributedOrders: attributedOrders.toLocaleString(),
      newCustomerOrders: <>{newCustomerOrders.toLocaleString()} <span style={{ color: "#999", fontWeight: 400 }}>({newPct}%)</span></>,
      newCustomerRevenue: <>{cs}{r2(newCustomerRevenue).toLocaleString()} <span style={{ color: "#999", fontWeight: 400 }}>({newRevPct}%)</span></>,
      existingCustomerOrders: <>{existingCustomerOrders.toLocaleString()} <span style={{ color: "#999", fontWeight: 400 }}>({existPct}%)</span></>,
      existingCustomerRevenue: <>{cs}{r2(existingCustomerRevenue).toLocaleString()} <span style={{ color: "#999", fontWeight: 400 }}>({existRevPct}%)</span></>,
      cpa: attributedOrders > 0 ? `${cs}${cpa.toLocaleString()}` : "—",
      newCustomerCPA: newCustomerOrders > 0 ? `${cs}${r2(spend / newCustomerOrders).toLocaleString()}` : "—",
      revenuePerNewCustomer: newCustomerOrders > 0 ? `${cs}${r2(newCustomerRevenue / newCustomerOrders).toLocaleString()}` : "—",
      spendPerDay: reportingPeriodDays > 0 ? `${cs}${r2(spend / reportingPeriodDays).toLocaleString()}` : "—",
      newCustomersPerDay: reportingPeriodDays > 0 && newCustomerOrders > 0 ? r2(newCustomerOrders / reportingPeriodDays).toLocaleString() : "—",
      newCustomerRevenuePerDay: reportingPeriodDays > 0 && newCustomerRevenue > 0 ? `${cs}${r2(newCustomerRevenue / reportingPeriodDays).toLocaleString()}` : "—",
      newCustomerROAS: spend > 0 && newCustomerRevenue > 0 ? `${r2(newCustomerRevenue / spend)}x` : "—",
      videoP25: videoP25.toLocaleString(),
      videoP50: videoP50.toLocaleString(),
      videoP75: videoP75.toLocaleString(),
      videoP100: videoP100.toLocaleString(),
      metaConversionValue: `${cs}${r2(sum("metaConversionValue")).toLocaleString()}`,
      // LTV footer: weighted averages based on acquired customers
      ...((() => {
        const totalLtvCusts = footerSourceRows.reduce((s, r) => s + (r.ltvAcquiredCustomers || 0), 0);
        if (totalLtvCusts === 0) return {
          avgLtv30: "—", avgLtv90: "—", avgLtv365: "—", avgLtvAll: "—",
          totalLtvAll: "—", ltvCac: "—", repeatRate: "—", avgOrders: "—", ltvAcquiredCustomers: "—",
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
          ltvCac: overallCac > 0 ? `${r2(ltv90Overall / overallCac)}x` : "—",
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
      if (v == null) return "—";
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
              title={`${changeCount} change${changeCount === 1 ? "" : "s"} in period — click for full timeline`}
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
          if (v == null) return "—";
          return `${v} days`;
        },
      } as ColumnDef<any, any>);
    }
    cols.push(
      num("spend", "Spend", (v) => `${cs}${v.toLocaleString()}`, { desc: "Total amount spent on Meta ads in the selected period" }),
      num("impressions", "Impressions", undefined, { desc: "Total number of times your ads were shown on screen" }),
      num("clicks", "Clicks", undefined, { desc: "Total clicks on your ads, including all click types (link clicks, likes, comments, shares)" }),
      num("ctr", "CTR", (v) => `${v}%`, { desc: "Click-through rate — how often people clicked after seeing your ad", calc: "Clicks ÷ Impressions × 100" }),
      num("avgFrequency", "Frequency", (v) => v > 0 ? `${v}x` : "—", { desc: "Average number of times each person saw your ad. Higher frequency can mean ad fatigue" }),
      num("linkClicks", "Link Clicks", undefined, { desc: "Clicks that directed people to your website or app" }),
      num("landingPageViews", "Landing Page Views", undefined, { desc: "Number of times your landing page fully loaded after someone clicked your ad" }),
      num("viewContent", "View Content", undefined, { desc: "Number of times someone viewed a product page on your site after seeing your ad" }),
      num("addToCart", "Add to Cart", undefined, { desc: "Number of times someone added a product to their cart after seeing your ad" }),
      num("atcRate", "ATC Rate", (v, r) => r.viewContent > 0 ? `${v}%` : "—", { desc: "Add-to-cart rate — of people who viewed a product, how many added to cart", calc: "Add to Cart ÷ View Content × 100" }),
      num("initiateCheckout", "Initiate Checkout", undefined, { desc: "Number of times someone started the checkout process after seeing your ad" }),
      num("checkoutRate", "Checkout Rate", (v, r) => r.addToCart > 0 ? `${v}%` : "—", { desc: "Checkout rate — of people who added to cart, how many started checkout", calc: "Initiate Checkout ÷ Add to Cart × 100" }),
      num("metaConversions", "Purchases", undefined, { desc: "Total purchases reported by Meta, including both matched and unmatched orders" }),
      num("purchaseRate", "Purchase Rate", (v, r) => r.initiateCheckout > 0 ? `${v}%` : "—", { desc: "Purchase rate — of people who started checkout, how many completed a purchase", calc: "Purchases ÷ Initiate Checkout × 100" }),
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
          if (v == null) return "—";
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
          if (v == null) return "—";
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
          if (v == null) return "—";
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
          if (v == null) return "—";
          const total = row.original.attributedRevenue;
          const pct = total > 0 ? Math.round((v / total) * 100) : 0;
          return <>{cs}{v.toLocaleString()} <span style={{ color: "#999" }}>({pct}%)</span></>;
        },
      } as ColumnDef<any, any>,
      num("cpa", "CPA", (v, r) => r.attributedOrders > 0 ? `${cs}${v.toLocaleString()}` : "—", { desc: "Cost per acquisition — how much you spent to get each order", calc: "Spend ÷ Attributed Orders" }),
      num("newCustomerCPA", "New Customer CPA", (v) => v != null ? `${cs}${v.toLocaleString()}` : "—", { desc: "Cost to acquire each new customer", calc: "Spend ÷ New Customers" }),
      num("revenuePerNewCustomer", "Rev per New Customer", (v) => v != null ? `${cs}${v.toLocaleString()}` : "—", { desc: "Average first-order revenue per new customer", calc: "New Customer Revenue ÷ New Customers" }),
      num("spendPerDay", "Spend/Day", (v) => v != null ? `${cs}${v.toLocaleString()}` : "—", { desc: "Average daily spend, based on how long the ad has been active within the reporting period", calc: "Spend ÷ active days (shorter of ad age or reporting period)" }),
      num("newCustomersPerDay", "New Customers/Day", (v) => v != null ? `${v}` : "—", { desc: "Average new customers acquired per day", calc: "New Customers ÷ active days" }),
      num("newCustomerRevenuePerDay", "New Customer Rev/Day", (v) => v != null ? `${cs}${v.toLocaleString()}` : "—", { desc: "Average daily revenue from new customers", calc: "New Customer Revenue ÷ active days" }),
      num("newCustomerROAS", "New Customer ROAS", (v) => v != null ? `${v}x` : "—", { desc: "Return on ad spend from new customers only — excludes returning customer revenue", calc: "New Customer Revenue ÷ Spend" }),
      num("videoP25", "Video 25%", undefined, { desc: "Number of times your video was watched to 25% of its length" }),
      num("videoP50", "Video 50%", undefined, { desc: "Number of times your video was watched to 50% of its length" }),
      num("videoP75", "Video 75%", undefined, { desc: "Number of times your video was watched to 75% of its length" }),
      num("videoP100", "Video 100%", undefined, { desc: "Number of times your video was watched all the way through" }),
      // LTV columns (all-time, independent of reporting window)
      num("avgLtv30", "Avg LTV 30d", (v) => v != null ? `${cs}${v.toLocaleString()}` : "—", { desc: "Average revenue per acquired customer within 30 days of their first purchase. Unaffected by the reporting period — always uses all-time data", calc: "Total 30-day revenue ÷ acquired customers" }),
      num("avgLtv90", "Avg LTV 90d", (v) => v != null ? `${cs}${v.toLocaleString()}` : "—", { desc: "Average revenue per acquired customer within 90 days of their first purchase. Unaffected by the reporting period — always uses all-time data", calc: "Total 90-day revenue ÷ acquired customers" }),
      num("avgLtv365", "Avg LTV 1yr", (v) => v != null ? `${cs}${v.toLocaleString()}` : "—", { desc: "Average revenue per acquired customer within 1 year of their first purchase. Unaffected by the reporting period — always uses all-time data", calc: "Total 1-year revenue ÷ acquired customers" }),
      num("avgLtvAll", "Avg LTV All", (v) => v != null ? `${cs}${v.toLocaleString()}` : "—", { desc: "Average all-time revenue per acquired customer, across every order they've ever placed", calc: "Total all-time revenue ÷ acquired customers" }),
      num("totalLtvAll", "Total LTV", (v) => v != null ? `${cs}${v.toLocaleString()}` : "—", { desc: "Total lifetime revenue from every customer acquired via this ad, across all their orders" }),
      num("ltvCac", "LTV:CAC", (v) => v != null ? `${v}x` : "—", { desc: "How much lifetime revenue each acquired customer generates relative to what it cost to acquire them", calc: "Avg LTV All ÷ New Customer CPA" }),
      num("repeatRate", "Repeat Rate", (v) => v != null ? `${v}%` : "—", { desc: "Percentage of acquired customers who came back and purchased again", calc: "Repeat buyers ÷ total acquired customers × 100" }),
      num("avgOrders", "Avg Orders", (v) => v != null ? `${v}` : "—", { desc: "Average number of orders placed by each acquired customer", calc: "Total orders ÷ acquired customers" }),
      num("ltvAcquiredCustomers", "LTV Customers", (v) => v > 0 ? v.toLocaleString() : "—", { desc: "Number of unique customers acquired via this ad that we have lifetime value data for" }),
    );
    return cols;
  }, [cs, showBreakdown, breakdown, level, nameHeader, currentSelectedIds, filteredRows, toggleSelectAll, toggleSelect, handleDrillDown, changeCountsByObjectId]);

  // Default view = Overview profile
  const defaultVisibleColumns = useMemo(() => {
    return ["select", "name", "adAgeDays", "spend", "impressions", "clicks", "ctr",
      "metaConversions", "attributedRevenue", "blendedROAS", "cpa"];
  }, []);

  const columnProfiles = useMemo(() => [
    {
      id: "overview", label: "Overview", icon: "📊",
      description: "Top-level performance snapshot — spend, purchases, revenue, and ROAS at a glance",
      columns: ["name", "spend", "metaConversions", "attributedRevenue", "blendedROAS", "cpa"],
      fullColumns: ["name", "spend", "impressions", "clicks", "ctr", "metaConversions", "attributedOrders", "attributedRevenue", "unverifiedRevenue", "blendedROAS", "cpa"],
    },
    {
      id: "newCustomers", label: "New Customers", icon: "👤",
      description: "How effectively each ad acquires new customers — acquisition cost, revenue, and long-term value",
      columns: ["name", "spend", "newCustomerOrders", "newCustomerCPA", "newCustomerROAS", "ltvCac", "repeatRate"],
      fullColumns: ["name", "spend", "newCustomerOrders", "newCustomerRevenue", "newCustomerCPA", "newCustomerROAS", "revenuePerNewCustomer", "newCustomersPerDay", "newCustomerRevenuePerDay", "avgLtv30", "avgLtv90", "avgLtvAll", "ltvCac", "repeatRate", "avgOrders"],
    },
    {
      id: "efficiency", label: "Efficiency", icon: "⚡",
      description: "Cost efficiency metrics — are you getting good value for your spend?",
      columns: ["name", "spend", "spendPerDay", "cpa", "blendedROAS", "ctr", "ltvCac"],
      fullColumns: ["name", "spend", "spendPerDay", "cpa", "newCustomerCPA", "blendedROAS", "newCustomerROAS", "ctr", "purchaseRate", "ltvCac", "newCustomersPerDay"],
    },
    {
      id: "funnel", label: "Funnel", icon: "🔽",
      description: "The customer journey from impression to purchase — where are people dropping off?",
      columns: ["name", "impressions", "linkClicks", "addToCart", "atcRate", "metaConversions", "purchaseRate"],
      fullColumns: ["name", "impressions", "linkClicks", "landingPageViews", "viewContent", "addToCart", "atcRate", "initiateCheckout", "checkoutRate", "metaConversions", "purchaseRate"],
    },
    {
      id: "revenue", label: "Revenue", icon: "💰",
      description: "Revenue breakdown — matched vs unmatched, new vs existing customer revenue",
      columns: ["name", "spend", "attributedRevenue", "blendedROAS", "newCustomerRevenue", "existingCustomerRevenue"],
      fullColumns: ["name", "spend", "attributedRevenue", "unverifiedRevenue", "blendedROAS", "newCustomerROAS", "newCustomerRevenue", "existingCustomerRevenue", "revenuePerNewCustomer", "totalLtvAll", "metaConversionValue"],
    },
    {
      id: "ltv", label: "Lifetime Value", icon: "🔄",
      description: "Long-term customer value — are the customers you're acquiring worth the investment?",
      columns: ["name", "newCustomerOrders", "avgLtv90", "avgLtvAll", "ltvCac", "repeatRate"],
      fullColumns: ["name", "newCustomerOrders", "ltvAcquiredCustomers", "avgLtv30", "avgLtv90", "avgLtv365", "avgLtvAll", "totalLtvAll", "ltvCac", "repeatRate", "avgOrders", "newCustomerCPA"],
    },
    {
      id: "creative", label: "Creative", icon: "🎬",
      description: "Creative performance — engagement, frequency, and video watch-through rates",
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

  const theme = usePageTheme();
  const tabStyle = (isActive: boolean): React.CSSProperties => ({
    padding: "10px 20px",
    fontSize: "14px",
    fontWeight: isActive ? 700 : 500,
    color: isActive ? theme.accentDark : "#6d7175",
    background: isActive ? "#fff" : "#f6f6f7",
    border: `1px solid ${isActive ? theme.accent + "44" : "#c9cccf"}`,
    borderBottom: isActive ? "1px solid #fff" : `2px solid ${theme.accent}33`,
    borderRadius: "8px 8px 0 0",
    cursor: "pointer",
    marginRight: "-1px",
    marginBottom: "-1px",
    position: "relative" as const,
    zIndex: isActive ? 1 : 0,
  });

  return (
    <Page title="Ad Campaigns" fullWidth>
      <style dangerouslySetInnerHTML={{ __html: tileGridStyles }} />
      <ReportTabs>
      <BlockStack gap="500">
        <AiInsightsPanel
          pageKey="campaigns"
          cachedInsights={aiCachedInsights}
          generatedAt={aiGeneratedAt}
          isStale={aiIsStale}
          currencySymbol={cs}
        />
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
              {currentSelectedIds.size} selected — summary cards show selected items only
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

          // Wasted Spend: entities with spend but ROAS below 2.5
          const wastedItems = [...campaignRows, ...adsetRows, ...adRows]
            .filter(r => r.spend > 20 && r.blendedROAS < 2.5)
            .sort((a, b) => b.spend - a.spend)
            .slice(0, 8)
            .map(r => ({ name: r.name, spend: r.spend, orders: r.attributedOrders, roas: r.blendedROAS }));

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
                  tooltip={{ definition: "Unique first-time customers acquired through Meta ads within the selected date range (deduplicated — a customer placing multiple orders on their first day counts once)" }}
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
              { id: "bestToWorst", label: "Best to Worst Performing", span: 2, render: () => (
                <Card>
                  <BlockStack gap="300">
                    <InlineStack align="space-between" blockAlign="center">
                      <Text as="h3" variant="headingSm">Best to Worst Performing</Text>
                      <SmallToggle options={LEVEL_OPTIONS} selected={rankLevel} onChange={setRankLevel} />
                    </InlineStack>
                    <BestToWorstList rows={rankRows} cs={cs} entityType={rankLevel as "campaign" | "adset" | "ad"} onEntityClick={(id, name) => setDrawerEntity({ objectType: rankLevel as any, objectId: id, objectName: name })} />
                  </BlockStack>
                </Card>
              )},
              { id: "adAge", label: "Ad Age", span: 2, render: () => (
                <Card>
                  <AdAgeTile adRows={adRows} cs={cs} onAdClick={(id, name) => setDrawerEntity({ objectType: "ad", objectId: id, objectName: name })} />
                </Card>
              )},
              { id: "platformPerf", label: "Platform Performance", span: 2, render: () => (
                <BreakdownPerfTile title="Platform Performance" data={platformPerf} cs={cs} defaultLevel="campaign" defaultSort="roas" />
              )},
              { id: "placementPerf", label: "Placement Performance", span: 2, render: () => (
                <BreakdownPerfTile title="Placement Performance" data={placementPerf} cs={cs} type="placement" />
              )},
              { id: "funnelHealth", label: "Funnel Health", span: 2, render: () => (
                <Card>
                  <BlockStack gap="300">
                    <InlineStack align="space-between" blockAlign="center">
                      <Text as="h3" variant="headingSm">Funnel Health</Text>
                      <SmallToggle
                        options={[{ id: "counts", label: "Drop-off" }, { id: "rates", label: "Conversion" }]}
                        selected={funnelMode}
                        onChange={(v) => setFunnelMode(v as "counts" | "rates")}
                      />
                    </InlineStack>
                    <FunnelFlow totals={funnelTotals} mode={funnelMode} />
                  </BlockStack>
                </Card>
              )},
              { id: "wastedSpend", label: "Wasted Spend?", span: 2, render: () => (
                <Card>
                  <BlockStack gap="300">
                    <Text as="h3" variant="headingSm">Wasted Spend?</Text>
                    <Text as="p" variant="bodySm" tone="subdued">Ads with ROAS below 2.5x — sorted by spend</Text>
                    <WastedSpendList items={wastedItems} cs={cs} />
                  </BlockStack>
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
            <Text as="h2" variant="headingMd">Performance</Text>
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
              <div style={{ flex: 1, borderBottom: `1px solid ${theme.accent}44` }} />
            </div>
          </div>
          <div style={{
            background: "#fff",
            border: `1px solid ${theme.accent}44`,
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
                defaultColumnWidths={{
                  name: 260,
                  spend: 90,
                  impressions: 95,
                  clicks: 70,
                  ctr: 60,
                  cpa: 75,
                }}
                tableId="campaigns"
                stickyTopOffset={perfTabsHeight}
                columnProfiles={columnProfiles}
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
