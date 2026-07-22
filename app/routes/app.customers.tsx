import { json } from "@remix-run/node";
import { useLoaderData, useSearchParams, useSubmit, useActionData, useRevalidator, useFetcher } from "@remix-run/react";
import { Page, Card, BlockStack, Text, Select } from "@shopify/polaris";
import React, { useState, useMemo, useRef, useEffect } from "react";
import ReportTabs from "../components/ReportTabs";
// import InteractiveTable from "../components/InteractiveTable"; // table removed
import TileGrid from "../components/TileGrid";
import type { TileDef } from "../components/TileGrid";
import { authenticate } from "../shopify.server";
import db from "../db.server";
import { cached as queryCached, DEFAULT_TTL } from "../services/queryCache.server";
import { parseDateRange } from "../utils/dateRange.server";
import { shopLocalDayKey, shopRangeBounds } from "../utils/shopTime.server";
import { currencySymbolFromCode } from "../utils/currency";
import { getCachedInsights, computeDataHash, generateInsights } from "../services/aiAnalysis.server";
import { setProgress, failProgress, completeProgress } from "../services/progress.server";
import { buildOrderExplorerData } from "../services/orderExplorerData.server";
import AiInsightsPanel from "../components/AiInsightsPanel";
import OrderExplorerSection from "../components/OrderExplorerSection";
import AwaitingDataTile, { JourneyTimelinePreview, AcquisitionPathsPreview } from "../components/AwaitingDataTile";
import PageSummary from "../components/PageSummary";
import type { SummaryBullet, SummaryTone } from "../components/PageSummary";
import SummaryTile from "../components/SummaryTile";
import StackedBarChart from "../components/StackedBarChart";
import { TipButton } from "../components/TipButton";
import { SEGMENT_TIPS } from "../components/segmentTips";
import type { ColumnDef } from "@tanstack/react-table";

// ═══════════════════════════════════════════════════════════════
// LOADER
// ═══════════════════════════════════════════════════════════════

export const loader = async ({ request }) => {
  const { session } = await authenticate.admin(request);
  const shopDomain = session.shop;
  // Web-pixel journey tiles are validated on the internal stores (HM +
  // Vollebak) before public release. Gated by a per-APP env flag set in the
  // VB + HM fly configs; absent (→ false) on the public app, so public
  // merchants/reviewers never see them. Deliberately NOT tied to isInternalShop
  // (that also unlocks ops levers we don't want on merchant stores).
  const journeyReportsEnabled = process.env.JOURNEY_REPORTS_ENABLED === "true";
  const shopForTz = await db.shop.findUnique({ where: { shopDomain } });
  const tz = shopForTz?.shopifyTimezone || "UTC";
  const { fromDate, toDate, fromKey, toKey, preset } = parseDateRange(request, tz);
  const _t0 = Date.now();

  const r2 = (v: number) => Math.round(v * 100) / 100;
  const DAY_MS = 86400000;
  const now = Date.now();

  // Shop-local key arithmetic
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

  // Previous period dates (shop-local)
  const dayCount = diffDays(fromKey, toKey) + 1;
  const prevToKey = addDaysKey(fromKey, -1);
  const prevFromKey = addDaysKey(prevToKey, -(dayCount - 1));
  const prevBounds = shopRangeBounds(tz, prevFromKey, prevToKey);
  const prevFromDate = prevBounds.gte;
  const prevToDate = prevBounds.lte;

  // ── Single-batch parallel reads (all from pre-computed sources) ──
  const dateFromStr = fromKey;
  const dateToStr = toKey;
  const loadAllCustomers = () => db.customer.findMany({
    where: { shopDomain },
    select: {
      shopifyCustomerId: true, firstOrderDate: true, lastOrderDate: true,
      secondOrderDate: true, firstOrderValue: true,
      totalOrders: true, totalSpent: true, totalRefunded: true,
      metaOrders: true, discountOrdersCount: true, topProducts: true,
      avgConfidence: true, metaSegment: true,
      acquisitionCampaign: true, acquisitionAdSet: true, acquisitionAd: true,
      country: true, city: true,
    },
  });

  // Load all 3 customers:* analysis blobs in a single query, then parse + cache
  const loadAnalysisBlobs = async () => {
    const rows = await db.shopAnalysisCache.findMany({
      where: { shopDomain, cacheKey: { in: ["customers:ltv", "customers:journey", "customers:geo"] } },
    });
    const out: { ltv: any; journey: any; geo: any } = { ltv: null, journey: null, geo: null };
    for (const r of rows) {
      if (r.cacheKey === "customers:ltv") out.ltv = JSON.parse(r.payload);
      else if (r.cacheKey === "customers:journey") out.journey = JSON.parse(r.payload);
      else if (r.cacheKey === "customers:geo") out.geo = JSON.parse(r.payload);
    }
    return out;
  };

  // Cache the all-time spend too (only changes on sync)
  const loadAllTimeSpend = () => db.dailyAdRollup.aggregate({
    where: { shopDomain }, _sum: { spend: true },
  });

  const time = async <T,>(label: string, p: Promise<T>): Promise<T> => {
    const t = Date.now();
    const r = await p;
    const ms = Date.now() - t;
    if (ms > 200) console.log(`[customers]   ${label}: ${ms}ms`);
    return r;
  };

  const _qStart = Date.now();
  const [
    customers, dailyRollups, prevDailyRollups,
    insights, prevInsights, allTimeSpendResult, allTimeSpendByDayResult,
    ageRaw, genderRaw,
    genderDailyBlob,
    blobs,
    aiCached,
    unmatchedAttrs,
    // Attribution-based new customer count (deduplicated by customer ID)
    // Matches the same source Ad Campaigns uses — ensures tiles agree.
    attrNewCustomerOrdersRaw,
  ] = await Promise.all([
    time("customers", queryCached(`${shopDomain}:customersAll`, DEFAULT_TTL, loadAllCustomers)),
    time("dailyRollups", db.dailyCustomerRollup.findMany({
      where: { shopDomain, date: { gte: fromDate, lte: toDate } },
    })),
    time("prevDailyRollups", db.dailyCustomerRollup.findMany({
      where: { shopDomain, date: { gte: prevFromDate, lte: prevToDate } },
    })),
    // groupBy at DB level: returns one row per day instead of row-per-ad-per-day
    // (365 rows for a year range instead of ~30k rows on vollebak)
    time("insights", queryCached(
      `${shopDomain}:insightsDailyV2:${dateFromStr}:${dateToStr}`, DEFAULT_TTL,
      () => db.dailyAdRollup.groupBy({
        by: ["date"],
        where: { shopDomain, date: { gte: fromDate, lte: toDate } },
        _sum: { spend: true, metaConversions: true, metaConversionValue: true, newCustomerOrders: true, newCustomerRevenue: true },
      }),
    )),
    time("prevInsights", queryCached(
      `${shopDomain}:prevInsightsDaily:${prevFromKey}:${prevToKey}`, DEFAULT_TTL,
      () => db.dailyAdRollup.groupBy({
        by: ["date"],
        where: { shopDomain, date: { gte: prevFromDate, lte: prevToDate } },
        _sum: { spend: true },
      }),
    )),
    time("allTimeSpend", queryCached(`${shopDomain}:allTimeAdSpend`, DEFAULT_TTL, loadAllTimeSpend)),
    time("allTimeSpendByDay", queryCached(
      `${shopDomain}:allTimeAdSpendByDay`, DEFAULT_TTL,
      () => db.dailyAdRollup.groupBy({
        by: ["date"],
        where: { shopDomain },
        _sum: { spend: true },
      }),
    )),
    time("ageBreakdown", queryCached(
      `${shopDomain}:mbAge:${dateFromStr}:${dateToStr}`, DEFAULT_TTL,
      () => db.metaBreakdown.groupBy({
        by: ["breakdownValue"],
        where: { shopDomain, breakdownType: "age", date: { gte: fromDate, lte: toDate } },
        _sum: { conversions: true, conversionValue: true, spend: true, impressions: true },
      }),
    )),
    time("genderBreakdown", queryCached(
      `${shopDomain}:mbGender:${dateFromStr}:${dateToStr}`, DEFAULT_TTL,
      () => db.metaBreakdown.groupBy({
        by: ["breakdownValue"],
        where: { shopDomain, breakdownType: "gender", date: { gte: fromDate, lte: toDate } },
        _sum: { conversions: true, conversionValue: true, spend: true, impressions: true },
      }),
    )),
    // Customer Demographics gender breakdowns - read from precomputed blob
    // written by customerRollups.rebuildCustomerGenderDaily after each sync.
    // The blob stores per-day gender buckets for the 3 tile sources
    // (all-Meta combined, new-Meta combined, all-customer inferred); we
    // sum across the date range below. Replaces 3 per-request raw $queryRaw
    // joins that previously bypassed the rollup architecture.
    time("genderDailyBlob", db.shopAnalysisCache.findUnique({
      where: { shopDomain_cacheKey: { shopDomain, cacheKey: "customers:genderDaily" } },
      select: { payload: true },
    })),
    time("blobs", queryCached(`${shopDomain}:customersBlobs`, DEFAULT_TTL, loadAnalysisBlobs)),
    time("aiCache", getCachedInsights(shopDomain, "customers", dateFromStr, dateToStr)),
    // Unmatched attribution rows (confidence=0) - Meta conversions the matcher
    // couldn't tie to a Shopify order. shopifyOrderId is a synthetic key of the
    // form `unmatched_<adId>_<YYYY-MM-DD>...`, so the date extracts by regex.
    // Pulled for the union of current + previous ranges; bucketed per-day below.
    time("unmatchedAttrs", db.attribution.findMany({
      where: { shopDomain, confidence: 0, shopifyOrderId: { startsWith: "unmatched_" } },
      select: { shopifyOrderId: true, metaConversionValue: true },
    })),
    // Attribution-based new customers: deduplicated by shopifyCustomerId via Order join.
    // Same logic as Ad Campaigns tab — guarantees matching numbers.
    time("attrNewCustomerOrders", (async () => {
      const newOrders = await db.order.findMany({
        where: { shopDomain, isOnlineStore: true, isNewCustomerOrder: true, createdAt: { gte: fromDate, lte: toDate } },
        select: { shopifyOrderId: true, shopifyCustomerId: true, frozenTotalPrice: true, totalRefunded: true },
      });
      const attrIds = newOrders.map(o => o.shopifyOrderId);
      const attrs = attrIds.length > 0 ? await db.attribution.findMany({
        where: { shopDomain, shopifyOrderId: { in: attrIds }, confidence: { gt: 0 } },
        select: { shopifyOrderId: true },
      }) : [];
      const matchedSet = new Set(attrs.map(a => a.shopifyOrderId));
      // Also include UTM-confirmed orders (Layer 1)
      const metaNewOrders = newOrders.filter(o => matchedSet.has(o.shopifyOrderId) || false);
      // Deduplicate by customer ID
      const custIds = new Set();
      let uniqueCount = 0;
      let totalRev = 0;
      for (const o of metaNewOrders) {
        if (o.shopifyCustomerId && !custIds.has(o.shopifyCustomerId)) {
          custIds.add(o.shopifyCustomerId);
          uniqueCount++;
        }
        totalRev += (o.frozenTotalPrice || 0) - (o.totalRefunded || 0);
      }
      return { uniqueCount, totalRev, orderCount: metaNewOrders.length };
    })()),
  ]);
  console.log(`[customers] db ${Date.now() - _qStart}ms (customers=${customers.length}, dailyRollups=${dailyRollups.length})`);

  const shop = shopForTz;
  const currencySymbol = currencySymbolFromCode(shop?.shopifyCurrency);

  const ltvBlob = blobs.ltv;
  const journeyBlob = blobs.journey;
  const geoBlob = blobs.geo;

  // ── Build per-customer table rows from Customer model (cached, expensive at 15k+ rows) ──
  // The output depends only on `customers` (which is itself cached) and current time
  // for daysSince calculations. The 2-hour TTL is fine - those values change daily.
  const rows = await queryCached(`${shopDomain}:customerRows`, DEFAULT_TTL, async () => {
    const out = customers.map(c => {
      const tag = c.metaSegment === "metaNew" ? "Meta New"
        : c.metaSegment === "metaRetargeted" ? "Meta Retargeted" : "Organic";
      const totalRevenue = c.totalSpent || 0;
      const totalRefunded = c.totalRefunded || 0;
      const netRevenue = totalRevenue - totalRefunded;
      const orderCount = c.totalOrders || 0;
      const avgOrderValue = orderCount > 0 ? r2(totalRevenue / orderCount) : 0;
      const firstOrderValue = c.firstOrderValue || 0;
      const ltvMultiplier = firstOrderValue > 0 ? r2(totalRevenue / firstOrderValue) : null;
      const lastOrderTime = c.lastOrderDate?.getTime() || 0;
      const firstOrderTime = c.firstOrderDate?.getTime() || 0;
      const daysSinceLastOrder = lastOrderTime > 0 ? Math.floor((now - lastOrderTime) / DAY_MS) : 0;
      const daysSinceAcquisition = firstOrderTime > 0 ? Math.floor((now - firstOrderTime) / DAY_MS) : 0;
      const timeTo2ndOrder = c.secondOrderDate && c.firstOrderDate
        ? Math.floor((c.secondOrderDate.getTime() - c.firstOrderDate.getTime()) / DAY_MS) : null;
      const refundRate = totalRevenue > 0 ? Math.round((totalRefunded / totalRevenue) * 100) : 0;
      return {
        customerId: c.shopifyCustomerId, tag,
        acquisitionDate: c.firstOrderDate ? shopLocalDayKey(tz, c.firstOrderDate) : "",
        acquisitionCampaign: c.acquisitionCampaign || "",
        acquisitionAdSet: c.acquisitionAdSet || "",
        totalOrders: orderCount, metaOrders: c.metaOrders || 0,
        organicOrders: orderCount - (c.metaOrders || 0),
        grossRevenue: r2(totalRevenue), totalRefunded: r2(totalRefunded), netRevenue: r2(netRevenue),
        avgOrderValue, firstOrderValue: r2(firstOrderValue),
        lastOrderDate: c.lastOrderDate ? shopLocalDayKey(tz, c.lastOrderDate) : "",
        daysSinceLastOrder, daysSinceAcquisition, timeTo2ndOrder,
        country: c.country || "", city: c.city || "",
        ltvMultiplier, avgConfidence: c.avgConfidence,
        topProducts: c.topProducts || "",
        discountOrders: c.discountOrdersCount || 0, refundRate,
        orderNumAtAcq: 1,
      };
    });
    out.sort((a, b) => b.netRevenue - a.netRevenue);
    return out;
  });

  // ── Tile aggregates from DailyCustomerRollup ──
  // Sum across segments to get period totals
  const sumRollups = (rollups: any[]) => {
    const bySegment: Record<string, any> = { metaNew: { newCustomers: 0, orders: 0, revenue: 0, firstOrderRevenue: 0, repeatCustomers: 0 },
      metaRepeat: { newCustomers: 0, orders: 0, revenue: 0, firstOrderRevenue: 0, repeatCustomers: 0 },
      metaRetargeted: { newCustomers: 0, orders: 0, revenue: 0, firstOrderRevenue: 0, repeatCustomers: 0 },
      organic: { newCustomers: 0, orders: 0, revenue: 0, firstOrderRevenue: 0, repeatCustomers: 0 } };
    for (const r of rollups) {
      const b = bySegment[r.segment];
      if (!b) continue;
      b.newCustomers += r.newCustomers;
      b.orders += r.orders;
      b.revenue += r.revenue;
      b.firstOrderRevenue += r.firstOrderRevenue;
      b.repeatCustomers += r.repeatCustomers;
    }
    return bySegment;
  };
  const cur = sumRollups(dailyRollups);
  const prv = sumRollups(prevDailyRollups);

  // Headline tile aggregates
  const metaCount = cur.metaNew.newCustomers; // new customers acquired via Meta in range
  const metaRevenue = cur.metaNew.revenue + cur.metaRepeat.revenue + cur.metaRetargeted.revenue;
  const metaOrders = cur.metaNew.orders + cur.metaRepeat.orders + cur.metaRetargeted.orders;
  const metaFirstOrderTotal = cur.metaNew.firstOrderRevenue;
  const organicCount = cur.organic.newCustomers;
  const organicRevenue = cur.organic.revenue;
  const organicOrders = cur.organic.orders;
  const organicFirstOrderTotal = cur.organic.firstOrderRevenue;
  const metaRepeatCount = cur.metaRepeat.repeatCustomers;
  const organicRepeatCount = cur.organic.repeatCustomers;
  const totalAllRevenue = metaRevenue + organicRevenue;

  // insights is a groupBy result: one row per day with _sum field
  const totalMetaSpend = insights.reduce((s, i: any) => s + (i._sum?.spend || 0), 0);
  const totalMetaConversions = insights.reduce((s, i: any) => s + (i._sum?.metaConversions || 0), 0);
  const totalMetaConversionValue = insights.reduce((s, i: any) => s + (i._sum?.metaConversionValue || 0), 0);
  // Attribution-based new customer revenue from DailyAdRollup (same source as Ad Campaigns)
  const attrNewCustomerRevenue = insights.reduce((s, i: any) => s + (i._sum?.newCustomerRevenue || 0), 0);
  const attrNewCustomerOrders = insights.reduce((s, i: any) => s + (i._sum?.newCustomerOrders || 0), 0);
  // Deduplicated new customer count (attribution-based, same as Ad Campaigns)
  const attrUniqueNewCustomers = attrNewCustomerOrdersRaw.uniqueCount;
  const allTimeMetaSpend = allTimeSpendResult._sum?.spend || 0;

  // All-time Meta spend bucketed by acquisition month (YYYY-MM).
  // Powers the LTV chart's per-cohort CAC: cohort_CAC averages
  // (monthSpend / metaNewInMonth) weighted by how many of the cohort's
  // customers were acquired in each month. Without this the displayed
  // CAC was a single all-time average that didn't move when toggling
  // 12m / 6m / 3m / 1m.
  const metaSpendByAcqMonth: Record<string, number> = {};
  for (const r of allTimeSpendByDayResult as any[]) {
    const ym = shopLocalDayKey(tz, r.date).slice(0, 7);
    metaSpendByAcqMonth[ym] = (metaSpendByAcqMonth[ym] || 0) + (r._sum?.spend || 0);
  }

  // Daily spend map
  const dailySpendMap: Record<string, number> = {};
  for (const i of insights as any[]) {
    const d = shopLocalDayKey(tz, i.date);
    dailySpendMap[d] = (dailySpendMap[d] || 0) + (i._sum?.spend || 0);
  }

  // Bucket unmatched attributions (Meta conversions with no matched Shopify
  // order) into the current + previous ranges by shop-local day key extracted
  // from shopifyOrderId. Same approach as the Weekly Report so the two pages
  // agree on the unmatched number.
  const unmatchedByDay: Record<string, { count: number; revenue: number }> = {};
  const prevUnmatchedByDay: Record<string, { count: number; revenue: number }> = {};
  // Value>0 counters power the Total Meta Orders tile - £0 unmatched
  // conversions (typically customer-service/replacement) should not count
  // as a Meta order. prev* variants power the tile's delta badge + the
  // Meta Order Revenue tile's previousValue (was buggy previously).
  let unmatchedConversionsWithValue = 0;
  let prevUnmatchedConversions = 0;
  let prevUnmatchedRevenue = 0;
  let prevUnmatchedConversionsWithValue = 0;
  for (const a of unmatchedAttrs) {
    const m = a.shopifyOrderId.match(/(\d{4}-\d{2}-\d{2})/);
    if (!m) continue;
    const key = m[1];
    const rev = a.metaConversionValue || 0;
    if (key >= fromKey && key <= toKey) {
      if (!unmatchedByDay[key]) unmatchedByDay[key] = { count: 0, revenue: 0 };
      unmatchedByDay[key].count++;
      unmatchedByDay[key].revenue += rev;
      if (rev > 0) unmatchedConversionsWithValue++;
    } else if (key >= prevFromKey && key <= prevToKey) {
      if (!prevUnmatchedByDay[key]) prevUnmatchedByDay[key] = { count: 0, revenue: 0 };
      prevUnmatchedByDay[key].count++;
      prevUnmatchedByDay[key].revenue += rev;
      prevUnmatchedConversions++;
      prevUnmatchedRevenue += rev;
      if (rev > 0) prevUnmatchedConversionsWithValue++;
    }
  }

  // Build dailyData chart from rollups
  const dailyMap: Record<string, any> = {};
  {
    let key = fromKey;
    while (key <= toKey) {
      dailyMap[key] = { date: key, metaCustomers: 0, organicCustomers: 0, metaRevenue: 0, organicRevenue: 0, spend: 0, newMetaCustomers: 0, newMetaRevenue: 0, metaRepeatCustomers: 0,
        // Per-cohort daily series for the "Meta Customer Breakdown by Day"
        // stacked bar chart. Populated below from donutBreakdown.byDay — the
        // SAME per-order Meta-attribution tagging the Summary donut uses — so
        // the two tiles reconcile (the rollup's segment buckets over-include a
        // retargeted customer's non-Meta first order, which is what made the
        // Retargeted series disagree with the donut).
        mnCust: 0, mrCust: 0, mrtCust: 0, mnRev: 0, mrRev: 0, mrtRev: 0,
        // Meta Unidentified = matched-as-Meta-conversion but unmatched to a
        // specific Shopify order. Sourced from unmatchedByDay below.
        muCust: 0, muRev: 0 };
      key = addDaysKey(key, 1);
    }
  }
  for (const r of dailyRollups) {
    const key = shopLocalDayKey(tz, r.date);
    if (!dailyMap[key]) continue;
    if (r.segment === "metaNew") {
      dailyMap[key].newMetaCustomers += r.newCustomers;
      dailyMap[key].newMetaRevenue += r.firstOrderRevenue;
      dailyMap[key].metaCustomers += r.newCustomers + r.repeatCustomers;
      dailyMap[key].metaRevenue += r.revenue;
    } else if (r.segment === "metaRepeat") {
      dailyMap[key].metaRepeatCustomers += r.repeatCustomers;
      dailyMap[key].metaCustomers += r.repeatCustomers;
      dailyMap[key].metaRevenue += r.revenue;
    } else if (r.segment === "metaRetargeted") {
      dailyMap[key].metaCustomers += r.newCustomers + r.repeatCustomers;
      dailyMap[key].metaRevenue += r.revenue;
    } else {
      dailyMap[key].organicCustomers += r.newCustomers + r.repeatCustomers;
      dailyMap[key].organicRevenue += r.revenue;
    }
  }
  for (const [d, spend] of Object.entries(dailySpendMap)) {
    if (dailyMap[d]) dailyMap[d].spend += spend;
  }
  // Fold unmatched Meta conversions into the per-day totals so the chart hover
  // and "Total Meta Customers" tile agree (tile = Σ matched + Σ unmatched).
  for (const [d, v] of Object.entries(unmatchedByDay)) {
    if (!dailyMap[d]) continue;
    dailyMap[d].metaCustomers += v.count;
    dailyMap[d].metaRevenue += v.revenue;
    dailyMap[d].muCust += v.count;
    dailyMap[d].muRev += v.revenue;
  }
  const dailyData = Object.values(dailyMap).sort((a: any, b: any) => a.date.localeCompare(b.date));

  // Previous daily data
  const prevDailyMap: Record<string, any> = {};
  {
    let key = prevFromKey;
    while (key <= prevToKey) {
      prevDailyMap[key] = { date: key, metaCustomers: 0, organicCustomers: 0, metaRevenue: 0, organicRevenue: 0, spend: 0, newMetaCustomers: 0, newMetaRevenue: 0, metaRepeatCustomers: 0 };
      key = addDaysKey(key, 1);
    }
  }
  for (const r of prevDailyRollups) {
    const key = shopLocalDayKey(tz, r.date);
    if (!prevDailyMap[key]) continue;
    if (r.segment === "metaNew") {
      prevDailyMap[key].newMetaCustomers += r.newCustomers;
      prevDailyMap[key].newMetaRevenue += r.firstOrderRevenue;
      prevDailyMap[key].metaCustomers += r.newCustomers + r.repeatCustomers;
      prevDailyMap[key].metaRevenue += r.revenue;
    } else if (r.segment === "metaRepeat") {
      prevDailyMap[key].metaRepeatCustomers += r.repeatCustomers;
      prevDailyMap[key].metaCustomers += r.repeatCustomers;
      prevDailyMap[key].metaRevenue += r.revenue;
    } else if (r.segment === "metaRetargeted") {
      prevDailyMap[key].metaCustomers += r.newCustomers + r.repeatCustomers;
      prevDailyMap[key].metaRevenue += r.revenue;
    } else {
      prevDailyMap[key].organicCustomers += r.newCustomers + r.repeatCustomers;
      prevDailyMap[key].organicRevenue += r.revenue;
    }
  }
  for (const i of prevInsights as any[]) {
    const d = shopLocalDayKey(tz, i.date);
    if (prevDailyMap[d]) prevDailyMap[d].spend += (i._sum?.spend || 0);
  }
  for (const [d, v] of Object.entries(prevUnmatchedByDay)) {
    if (!prevDailyMap[d]) continue;
    prevDailyMap[d].metaCustomers += v.count;
    prevDailyMap[d].metaRevenue += v.revenue;
  }
  const prevDailyData = Object.values(prevDailyMap).sort((a: any, b: any) => a.date.localeCompare(b.date));

  // Computed tile values
  const metaRepeatTotal = dailyData.reduce((s: number, d: any) => s + d.metaRepeatCustomers, 0);
  const prevMetaRepeatTotal = prevDailyData.reduce((s: number, d: any) => s + d.metaRepeatCustomers, 0);
  const metaRepeatRate = metaCount > 0 ? Math.round((metaRepeatCount / metaCount) * 100) : 0;
  const organicRepeatRate = organicCount > 0 ? Math.round((organicRepeatCount / organicCount) * 100) : 0;
  const metaAvgLtv = metaCount > 0 ? r2(metaRevenue / metaCount) : 0;
  const organicAvgLtv = organicCount > 0 ? r2(organicRevenue / organicCount) : 0;
  const metaAvgOrders = metaCount > 0 ? r2(metaOrders / metaCount) : 0;
  const organicAvgOrders = organicCount > 0 ? r2(organicOrders / organicCount) : 0;
  // Use DailyAdRollup newCustomerOrders for CPA — same source as Ad Campaigns tab.
  // This counts new-customer ORDERS (not deduplicated customers) which matches
  // effectiveTotals.newCustomerOrders on Campaigns exactly.
  const newInPeriod = attrNewCustomerOrders;
  const newCustomerCPA = newInPeriod > 0 ? r2(totalMetaSpend / newInPeriod) : 0;
  const metaAvgFirstOrder = newInPeriod > 0 ? r2(attrNewCustomerRevenue / newInPeriod) : 0;
  const organicAvgFirstOrder = organicCount > 0 ? r2(organicFirstOrderTotal / organicCount) : 0;
  const aovCpaRatio = newCustomerCPA > 0 ? r2(metaAvgFirstOrder / newCustomerCPA) : 0;
  const paybackOrders = metaAvgFirstOrder > 0 ? r2(newCustomerCPA / metaAvgFirstOrder) : 0;
  const metaRevPct = totalAllRevenue > 0 ? Math.round((metaRevenue / totalAllRevenue) * 100) : 0;
  const metaAvgAov = metaOrders > 0 ? r2(metaRevenue / metaOrders) : 0;

  // Previous tiles
  const prevMetaCount = prv.metaNew.newCustomers;
  const prevOrganicCount = prv.organic.newCustomers;
  const prevMetaRevenue = prv.metaNew.revenue + prv.metaRepeat.revenue + prv.metaRetargeted.revenue;
  const prevMetaFirstOrderTotal = prv.metaNew.firstOrderRevenue;
  const prevMetaAvgFirstOrder = prevMetaCount > 0 ? r2(prevMetaFirstOrderTotal / prevMetaCount) : 0;
  const prevTotalSpend = (prevInsights as any[]).reduce((s, i) => s + (i._sum?.spend || 0), 0);
  const prevNewCustomerCPA = prevMetaCount > 0 ? r2(prevTotalSpend / prevMetaCount) : 0;
  const prevAovCpaRatio = prevNewCustomerCPA > 0 ? r2(prevMetaAvgFirstOrder / prevNewCustomerCPA) : 0;
  const prevMetaRepeatCount = prv.metaRepeat.repeatCustomers;
  const prevMetaRepeatRate = prevMetaCount > 0 ? Math.round((prevMetaRepeatCount / prevMetaCount) * 100) : 0;

  // Order-level in-range counts
  const metaNewOrdersInRange = cur.metaNew.orders;
  const metaNewRevenueInRange = r2(cur.metaNew.revenue);
  const metaNewCustomersInRange = cur.metaNew.newCustomers;
  const metaRepeatOrdersInRange = cur.metaRepeat.orders;
  const metaRepeatRevenueInRange = r2(cur.metaRepeat.revenue);
  const metaRepeatCustomersInRange = cur.metaRepeat.repeatCustomers;
  const metaRetargetedOrdersInRange = cur.metaRetargeted.orders;
  const metaRetargetedRevenueInRange = r2(cur.metaRetargeted.revenue);
  const metaRetargetedCustomersInRange = cur.metaRetargeted.newCustomers + cur.metaRetargeted.repeatCustomers;
  const totalCustomersInRange = metaNewCustomersInRange + metaRepeatCustomersInRange + metaRetargetedCustomersInRange + cur.organic.newCustomers + cur.organic.repeatCustomers;
  const totalRevenueInRange = metaRevenue + organicRevenue;
  const prevMetaNewCustomersInRange = prv.metaNew.newCustomers;
  const prevMetaNewRevenueInRange = r2(prv.metaNew.revenue);
  const prevMetaRepeatCustomersInRange = prv.metaRepeat.repeatCustomers;
  const prevMetaRepeatRevenueInRange = r2(prv.metaRepeat.revenue);
  const prevMetaRetargetedCustomersInRange = prv.metaRetargeted.newCustomers + prv.metaRetargeted.repeatCustomers;
  const prevMetaRetargetedRevenueInRange = r2(prv.metaRetargeted.revenue);
  const prevTotalCustomersInRange = prevMetaNewCustomersInRange + prevMetaRepeatCustomersInRange + prevMetaRetargetedCustomersInRange + prv.organic.newCustomers + prv.organic.repeatCustomers;
  const prevTotalRevenueInRange = prevMetaRevenue + prv.organic.revenue;
  // Order-count aggregates for Total Meta Orders tile.
  // Segment .orders already excludes £0 orders (rollup writer, commit a02b4f8).
  const totalOrdersInRange = cur.metaNew.orders + cur.metaRepeat.orders + cur.metaRetargeted.orders + cur.organic.orders;
  const prevMatchedMetaOrdersInRange = prv.metaNew.orders + prv.metaRepeat.orders + prv.metaRetargeted.orders;
  const prevTotalOrdersInRange = prevMatchedMetaOrdersInRange + prv.organic.orders;
  const matchedMetaOrdersInRange = cur.metaNew.orders + cur.metaRepeat.orders + cur.metaRetargeted.orders;

  // Demographics
  const AGE_ORDER = ["13-17", "18-24", "25-34", "35-44", "45-54", "55-64", "65+"];
  const ageBreakdown = AGE_ORDER
    .map(age => {
      const row = ageRaw.find(r => r.breakdownValue === age);
      return { label: age, conversions: row?._sum?.conversions || 0, spend: row?._sum?.spend || 0, revenue: row?._sum?.conversionValue || 0 };
    })
    .filter(a => a.conversions > 0 || a.spend > 0);
  // Per-gender ad-spend lookup, sourced from MetaBreakdown (the only place
  // where Meta gives us a spend split by gender). When a gender bucket
  // exists here we use it for the CPA chip; when it doesn't we fall back
  // to proportional period spend (avgCpa × conversions) so the chip stays
  // populated for date ranges where Meta's breakdown is empty.
  const metaBreakdownGenderSpend: Record<string, number> = {};
  let metaBreakdownGenderConversions = 0;
  for (const r of genderRaw) {
    const label = r.breakdownValue === "male" ? "Male" : r.breakdownValue === "female" ? "Female" : "Unknown";
    metaBreakdownGenderSpend[label] = (metaBreakdownGenderSpend[label] || 0) + (r._sum?.spend || 0);
    metaBreakdownGenderConversions += (r._sum?.conversions || 0);
  }

  // Materialize the 3 gender arrays out of the precomputed daily blob.
  // The blob holds per-day buckets for the last 400 days; we sum the
  // entries falling inside [fromKey..toKey]. Shape matches what the
  // downstream code expected from the now-removed $queryRaw calls.
  const genderDay: Record<string, {
    allMeta: Record<string, { c: number; r: number }>;
    newMeta: Record<string, { c: number; r: number }>;
    allCustomer: Record<string, { o: number; r: number }>;
  }> = (() => {
    if (!genderDailyBlob?.payload) return {};
    try { return JSON.parse(genderDailyBlob.payload).days || {}; }
    catch { return {}; }
  })();
  const sumGenderBucket = (kind: "allMeta" | "newMeta" | "allCustomer", valueKey: "c" | "o") => {
    const totals: Record<string, { count: number; revenue: number }> = {};
    for (const day in genderDay) {
      if (day < dateFromStr || day > dateToStr) continue;
      const sub = (genderDay[day] as any)[kind] || {};
      for (const g in sub) {
        const row = sub[g];
        if (!totals[g]) totals[g] = { count: 0, revenue: 0 };
        totals[g].count += row[valueKey] || 0;
        totals[g].revenue += row.r || 0;
      }
    }
    return totals;
  };
  const allMetaSum = sumGenderBucket("allMeta", "c");
  const newMetaSum = sumGenderBucket("newMeta", "c");
  const allCustomerSum = sumGenderBucket("allCustomer", "o");
  const allMetaCombinedGenderRaw = Object.entries(allMetaSum).map(([gender, v]) => ({
    gender: gender === "unknown" ? null : gender, conversions: v.count, revenue: v.revenue,
  }));
  const newMetaCombinedGenderRaw = Object.entries(newMetaSum).map(([gender, v]) => ({
    gender: gender === "unknown" ? null : gender, conversions: v.count, revenue: v.revenue,
  }));
  const allCustomerGenderRaw = Object.entries(allCustomerSum).map(([gender, v]) => ({
    gender: gender === "unknown" ? null : gender, orders: v.count, revenue: v.revenue,
  }));

  // genderBreakdown (All Meta) - now per-attribution + COALESCE so it
  // populates whenever there are matched orders, not just when Meta's
  // audience-level breakdown happens to cover the range.
  const allMetaConversionsTotal = (allMetaCombinedGenderRaw as Array<{ conversions: bigint | number }>)
    .reduce((s, r) => s + Number(r.conversions || 0), 0);
  const genderBreakdown = (allMetaCombinedGenderRaw as Array<{ gender: string | null; conversions: bigint | number; revenue: number | null }>)
    .map(r => {
      const label = r.gender === "male" ? "Male" : r.gender === "female" ? "Female" : "Unknown";
      const conversions = Number(r.conversions) || 0;
      // Prefer Meta's per-gender spend if present; otherwise proportional.
      let spend = metaBreakdownGenderSpend[label] || 0;
      if (spend === 0 && allMetaConversionsTotal > 0 && totalMetaSpend > 0) {
        spend = totalMetaSpend * (conversions / allMetaConversionsTotal);
      }
      return { label, conversions, spend, revenue: Number(r.revenue) || 0 };
    })
    .filter(g => g.conversions > 0 && g.label !== "Unknown")
    .sort((a, b) => b.conversions - a.conversions);
  const totalDemoConversions = ageBreakdown.reduce((s, a) => s + a.conversions, 0);

  // All-Customers gender breakdown - name-inferred only, every order in range.
  // No spend column (no per-order ad-spend signal); chip switches to AOV at
  // render time.
  const allCustomerGenderBreakdown = (allCustomerGenderRaw as Array<{ gender: string | null; orders: bigint | number; revenue: number | null }>)
    .map(r => ({
      label: r.gender === "male" ? "Male" : r.gender === "female" ? "Female" : "Unknown",
      conversions: Number(r.orders) || 0,
      spend: 0,
      revenue: Number(r.revenue) || 0,
    }))
    .filter(g => g.conversions > 0 && g.label !== "Unknown")
    .sort((a, b) => b.conversions - a.conversions);

  // New-Meta demographics - pulled from Attribution rows (which carry
  // per-order metaAge/metaGender from the breakdown enrichment step),
  // filtered to isNewCustomer=true within the date range. More precise
  // than Meta's aggregate breakdown because it's per-matched-order.
  //
  // Date pivot: Order.createdAt, NOT Attribution.matchedAt. matchedAt is
  // when the matcher ran; the user-visible date is when the order was
  // placed. Pivoting on matchedAt caused the AGE chart to show identical
  // results for "yesterday" and "30 days" (every backfilled match has the
  // same matchedAt cluster). See attribution_matchedat_gotcha memory.
  const newMetaAttrsWithDemo = await queryCached(
    `${shopDomain}:newMetaDemoAttrs:${dateFromStr}:${dateToStr}`,
    DEFAULT_TTL,
    () => db.$queryRaw<Array<{ metaAge: string | null; metaGender: string | null; metaConversionValue: number | null }>>`
      SELECT a.metaAge, a.metaGender, a.metaConversionValue
      FROM Attribution a
      JOIN "Order" o
        ON o.shopDomain = a.shopDomain AND o.shopifyOrderId = a.shopifyOrderId
      WHERE a.shopDomain = ${shopDomain}
        AND a.confidence > 0
        AND a.isNewCustomer = 1
        AND a.metaAge IS NOT NULL
        AND o.createdAt >= ${fromDate}
        AND o.createdAt <= ${toDate}
    `,
  );
  const newAgeAgg: Record<string, { conversions: number; value: number; spend: number; impressions: number }> = {};
  // Gender no longer aggregated here - moved to newMetaCombinedGenderRaw
  // (per-attribution + COALESCE with Customer.inferredGender). Age stays
  // here since there's no name-based age inference.
  for (const a of newMetaAttrsWithDemo) {
    if (a.metaAge) {
      if (!newAgeAgg[a.metaAge]) newAgeAgg[a.metaAge] = { conversions: 0, value: 0, spend: 0, impressions: 0 };
      newAgeAgg[a.metaAge].conversions++;
      newAgeAgg[a.metaAge].value += a.metaConversionValue || 0;
    }
  }
  // New-customer spend per age = the FULL Meta spend allocated to that age
  // bucket. Dividing by new-only conversions in the chip (spend / conv)
  // correctly yields a higher CPA than the All Meta scope, because the
  // same bucket spend is amortised across fewer (new) acquisitions. Using
  // the all-Meta CPA × new-conv formulation here would make New CPA == All
  // CPA, which is the bug Andy spotted on 2026-05-05.
  const newAgeBreakdown = AGE_ORDER
    .map(age => {
      const s = newAgeAgg[age];
      const conversions = s?.conversions || 0;
      const allMetaAge = ageBreakdown.find(a => a.label === age);
      const spend = allMetaAge?.spend || 0;
      return { label: age, conversions, spend, revenue: s?.value || 0 };
    })
    .filter(a => a.conversions > 0);
  // newGenderBreakdown - uses the per-attribution + COALESCE query so the
  // gender bars populate even when Meta's breakdown enrichment hadn't run
  // for these orders. Spend per gender = the FULL Meta spend allocated to
  // that gender bucket (from genderBreakdown, which already encapsulates
  // Meta's per-gender split with a proportional fallback). Dividing by
  // new-only conversions in the chip gives a higher CPA than All Meta -
  // same bucket spend, fewer new acquisitions in the denominator.
  const newMetaCombinedConversionsTotal = (newMetaCombinedGenderRaw as Array<{ conversions: bigint | number }>)
    .reduce((s, r) => s + Number(r.conversions || 0), 0);
  const newGenderBreakdown = (newMetaCombinedGenderRaw as Array<{ gender: string | null; conversions: bigint | number; revenue: number | null }>)
    .map(r => {
      const label = r.gender === "male" ? "Male" : r.gender === "female" ? "Female" : "Unknown";
      const conversions = Number(r.conversions) || 0;
      const allMetaG = genderBreakdown.find(g => g.label === label);
      let spend = 0;
      if (allMetaG && allMetaG.spend > 0) {
        spend = allMetaG.spend;
      } else if (newMetaCombinedConversionsTotal > 0 && totalMetaSpend > 0) {
        // No per-gender split available. Fall back to proportional spend
        // by new-conv share; CPA will be uniform across genders at
        // totalMetaSpend / totalNew.
        spend = totalMetaSpend * (conversions / newMetaCombinedConversionsTotal);
      }
      return { label, conversions, spend, revenue: Number(r.revenue) || 0 };
    })
    .filter(g => g.conversions > 0 && g.label !== "Unknown")
    .sort((a, b) => b.conversions - a.conversions);
  const newDemoConversions = newMetaCombinedConversionsTotal;
  const newDemoExactCount = 0;

  // Date-scoped geography - computed at loader time from orders placed in
  // the selected range, then joined to Attribution to classify each order
  // as organic / allMeta / metaNew. Replaces the all-time geoBlob that
  // previously fed this section (blob is left in place for other consumers
  // but bypassed here so the tile + summary respond to the date picker).
  const geoInRange = await queryCached(
    `${shopDomain}:customerGeoInRange:${dateFromStr}:${dateToStr}`,
    DEFAULT_TTL,
    async () => {
      const orders = await db.order.findMany({
        where: {
          shopDomain,
          isOnlineStore: true,
          createdAt: { gte: fromDate, lte: toDate },
          // Exclude £0 orders from geo aggregation. These are typically
          // in-house/staff test orders that would otherwise bloat city/
          // country counts (e.g. London) without representing real customers.
          // Matches the rollup convention set in commit a02b4f8.
          frozenTotalPrice: { gt: 0 },
        },
        select: {
          shopifyOrderId: true, shopifyCustomerId: true,
          country: true, city: true,
          frozenTotalPrice: true, totalRefunded: true,
          // First-order signal taken from the Order row itself (Shopify
          // ground truth) rather than the matched Attribution. Attribution
          // .isNewCustomer can lag if a re-match hasn't run since the order
          // count was populated — see matcherCore.server.js isNewOrder fix.
          isNewCustomerOrder: true,
        },
      });
      if (orders.length === 0) {
        return {
          all: { countries: [], cities: [], count: 0 },
          allMeta: { countries: [], cities: [], count: 0 },
          metaNew: { countries: [], cities: [], count: 0 },
        };
      }
      const orderIds = orders.map(o => o.shopifyOrderId);
      const attrs = await db.attribution.findMany({
        where: { shopDomain, shopifyOrderId: { in: orderIds }, confidence: { gt: 0 } },
        select: { shopifyOrderId: true },
      });
      const metaOrderIdSet = new Set(attrs.map(a => a.shopifyOrderId));

      type Bucket = { customers: Set<string>; revenue: number; orders: number };
      const mkAgg = () => ({} as Record<string, Bucket>);
      const add = (agg: Record<string, Bucket>, label: string | null, cust: string | null, net: number) => {
        if (!label) return;
        if (!agg[label]) agg[label] = { customers: new Set(), revenue: 0, orders: 0 };
        if (cust) agg[label].customers.add(cust);
        agg[label].revenue += net;
        agg[label].orders += 1;
      };

      const aCountry = mkAgg(), aCity = mkAgg();
      const mCountry = mkAgg(), mCity = mkAgg();
      const nCountry = mkAgg(), nCity = mkAgg();
      const allCusts = new Set<string>();
      const metaCusts = new Set<string>();
      const newMetaCusts = new Set<string>();

      for (const o of orders) {
        const net = Math.max(0, (o.frozenTotalPrice || 0) - (o.totalRefunded || 0));
        const cust = o.shopifyCustomerId || null;
        if (cust) allCusts.add(cust);
        add(aCountry, o.country || null, cust, net);
        add(aCity, o.city || null, cust, net);
        const isMeta = metaOrderIdSet.has(o.shopifyOrderId);
        if (isMeta) {
          if (cust) metaCusts.add(cust);
          add(mCountry, o.country || null, cust, net);
          add(mCity, o.city || null, cust, net);
          // metaNew = matched Meta order AND order is a first-order by
          // Shopify's own customer.orders signal. Decoupled from
          // Attribution.isNewCustomer (which can be stale).
          if (o.isNewCustomerOrder === true) {
            if (cust) newMetaCusts.add(cust);
            add(nCountry, o.country || null, cust, net);
            add(nCity, o.city || null, cust, net);
          }
        }
      }

      const toList = (agg: Record<string, Bucket>) => Object.entries(agg)
        .map(([label, v]) => ({
          label,
          customers: v.customers.size,
          revenue: Math.round(v.revenue * 100) / 100,
          orders: v.orders,
          spend: 0,
        }))
        .sort((a, b) => b.customers - a.customers)
        .slice(0, 50);

      return {
        all: { countries: toList(aCountry), cities: toList(aCity), count: allCusts.size },
        allMeta: { countries: toList(mCountry), cities: toList(mCity), count: metaCusts.size },
        metaNew: { countries: toList(nCountry), cities: toList(nCity), count: newMetaCusts.size },
      };
    },
  );

  const normalizeGeo = (arr: any[]) => (arr || []).map((item: any) => ({
    label: item.label || item.name || "",
    customers: item.customers || 0,
    revenue: item.revenue || 0,
    orders: item.orders || 0,
    spend: item.spend || 0,
    countryCode: item.countryCode,
  })).filter((x: any) => x.label).slice(0, 6);
  const topCountries = normalizeGeo(geoInRange.all.countries);
  const topCities = normalizeGeo(geoInRange.all.cities);
  const allMetaTopCountries = normalizeGeo(geoInRange.allMeta.countries);
  const allMetaTopCities = normalizeGeo(geoInRange.allMeta.cities);
  const metaNewTopCountries = normalizeGeo(geoInRange.metaNew.countries);
  const metaNewTopCities = normalizeGeo(geoInRange.metaNew.cities);
  const allGeoCount = geoInRange.all.count;
  const allMetaGeoCount = geoInRange.allMeta.count;
  const metaNewGeoCount = geoInRange.metaNew.count;

  // Date-scoped customer journey - customers whose FIRST order fell in the
  // selected period. Fetch their first three orders (online store only) and
  // compute mean AOV + median gap stats per scope (meta-new vs all). Replaces the
  // all-time journeyBlob that previously fed this tile so the "New Customer
  // Journey" responds to the date picker.
  const journeyInRange = await queryCached(
    `${shopDomain}:customerJourneyInRange:${dateFromStr}:${dateToStr}`,
    DEFAULT_TTL,
    async () => {
      const emptyScope = {
        firstAOV: 0, secondAOV: 0, thirdAOV: 0,
        gap1to2Days: null as number | null, gap2to3Days: null as number | null,
        firstOrderCount: 0, secondOrderCount: 0, thirdOrderCount: 0,
      };
      const custs = await db.customer.findMany({
        where: {
          shopDomain,
          firstOrderDate: { gte: fromDate, lte: toDate },
        },
        select: { shopifyCustomerId: true, metaSegment: true },
      });
      if (custs.length === 0) return { meta: emptyScope, all: emptyScope };

      const metaNewIds = new Set(
        custs.filter(c => c.metaSegment === "metaNew").map(c => c.shopifyCustomerId)
      );
      const allIds = custs.map(c => c.shopifyCustomerId);

      const orders = await db.order.findMany({
        where: {
          shopDomain,
          isOnlineStore: true,
          shopifyCustomerId: { in: allIds },
          customerOrderCountAtPurchase: { in: [1, 2, 3] },
          // Exclude £0 orders (free-gift / giveaway redemptions). They aren't
          // genuine repeat purchases — counting them both drags down the AOV
          // and inflates the "came back" rate. Matches DailyCustomerRollup,
          // which also skips £0 orders.
          frozenTotalPrice: { gt: 0 },
        },
        select: {
          shopifyCustomerId: true,
          customerOrderCountAtPurchase: true,
          frozenTotalPrice: true,
          totalRefunded: true,
          createdAt: true,
        },
      });

      type Slot = { val: number; date: Date };
      type Triple = { first?: Slot; second?: Slot; third?: Slot };
      const byCust = new Map<string, Triple>();
      for (const o of orders) {
        if (!o.shopifyCustomerId) continue;
        let t = byCust.get(o.shopifyCustomerId);
        if (!t) { t = {}; byCust.set(o.shopifyCustomerId, t); }
        const val = Math.max(0, (o.frozenTotalPrice || 0) - (o.totalRefunded || 0));
        const slot: Slot = { val, date: o.createdAt };
        if (o.customerOrderCountAtPurchase === 1) t.first = slot;
        else if (o.customerOrderCountAtPurchase === 2) t.second = slot;
        else if (o.customerOrderCountAtPurchase === 3) t.third = slot;
      }

      // AOV per cohort uses the MEAN, not the median: large orders are part of
      // the merchant's order DNA (e.g. high-ticket items) and must be counted
      // in full. Median understated first-order value badly for skewed
      // catalogues. This also makes the Journey "1st Order" box agree with the
      // mean-based "New Meta Customer AOV" tile.
      const mean = (arr: number[]): number => {
        if (arr.length === 0) return 0;
        return r2(arr.reduce((s, v) => s + v, 0) / arr.length);
      };
      const medianDays = (arr: number[]): number | null => {
        if (arr.length === 0) return null;
        const s = [...arr].sort((a, b) => a - b);
        const m = Math.floor(s.length / 2);
        return Math.round(s.length % 2 === 0 ? (s[m - 1] + s[m]) / 2 : s[m]);
      };

      const compute = (ids: Iterable<string>) => {
        const firsts: number[] = [], seconds: number[] = [], thirds: number[] = [];
        const g12: number[] = [], g23: number[] = [];
        let fc = 0, sc = 0, tc = 0;
        for (const id of ids) {
          const t = byCust.get(id);
          if (!t) continue;
          if (t.first) { firsts.push(t.first.val); fc++; }
          if (t.second) {
            seconds.push(t.second.val); sc++;
            if (t.first) g12.push((t.second.date.getTime() - t.first.date.getTime()) / DAY_MS);
          }
          if (t.third) {
            thirds.push(t.third.val); tc++;
            if (t.second) g23.push((t.third.date.getTime() - t.second.date.getTime()) / DAY_MS);
          }
        }
        return {
          firstAOV: mean(firsts),
          secondAOV: mean(seconds),
          thirdAOV: mean(thirds),
          gap1to2Days: medianDays(g12),
          gap2to3Days: medianDays(g23),
          firstOrderCount: fc,
          secondOrderCount: sc,
          thirdOrderCount: tc,
        };
      };

      return {
        meta: compute(metaNewIds),
        all: compute(allIds),
      };
    },
  );

  // Date-scoped Customer Breakdown - replicates Order Explorer's exact
  // tagging so the donut matches the "All Meta" filter. Key differences
  // vs the DailyCustomerRollup-based counts above:
  //   • Includes POS orders (rollup is online-store only).
  //   • Includes £0 orders (rollup skips them).
  //   • Includes UTM-only (utmConfirmedMeta) orders not statistically matched.
  //   • Tags per-order using Order Explorer's 2a/2b-ii/2c logic, then
  //     counts distinct customers + sums net revenue per bucket.
  const donutBreakdown = await queryCached(
    `${shopDomain}:customerDonutBreakdown:${dateFromStr}:${dateToStr}`,
    DEFAULT_TTL,
    async () => {
      const empty = { metaNew: { customers: 0, revenue: 0 }, metaRepeat: { customers: 0, revenue: 0 }, metaRetargeted: { customers: 0, revenue: 0 }, byDay: {} as Record<string, { mnCust: number; mrCust: number; mrtCust: number; mnRev: number; mrRev: number; mrtRev: number }> };
      const orders = await db.order.findMany({
        // Exclude £0 orders (staff / replacement / warranty) - same rule
        // Order Explorer + the rollup apply so customer counts agree.
        where: {
          shopDomain,
          createdAt: { gte: fromDate, lte: toDate },
          frozenTotalPrice: { gt: 0 },
        },
        select: {
          shopifyOrderId: true, shopifyCustomerId: true, createdAt: true,
          frozenTotalPrice: true, totalRefunded: true,
          customerOrderCountAtPurchase: true,
          utmConfirmedMeta: true,
        },
      });
      if (orders.length === 0) return empty;

      const orderIds = orders.map(o => o.shopifyOrderId);
      const attrs = await db.attribution.findMany({
        where: { shopDomain, shopifyOrderId: { in: orderIds }, confidence: { gt: 0 } },
        select: { shopifyOrderId: true },
      });
      const matchedOrderIds = new Set(attrs.map(a => a.shopifyOrderId));

      const custIds = Array.from(new Set(orders.map(o => o.shopifyCustomerId).filter(Boolean) as string[]));
      const custs = custIds.length > 0
        ? await db.customer.findMany({
            where: { shopDomain, shopifyCustomerId: { in: custIds } },
            select: { shopifyCustomerId: true, metaSegment: true },
          })
        : [];
      const segmentByCust = new Map(custs.map(c => [c.shopifyCustomerId, c.metaSegment || "organic"]));

      const newCusts = new Set<string>();
      const repeatCusts = new Set<string>();
      const retargetedCusts = new Set<string>();
      let newRev = 0, repeatRev = 0, retargetedRev = 0;

      // Per-day breakdown for the "Meta Customer Breakdown by Day" stacked
      // chart, built from the SAME per-order tagging as the donut totals so
      // the two tiles reconcile by construction. Customers are counted once
      // per day per cohort (distinct sets keyed by day); revenue is summed.
      const byDay: Record<string, {
        mn: Set<string>; mr: Set<string>; mrt: Set<string>;
        mnRev: number; mrRev: number; mrtRev: number;
      }> = {};
      const ensureDay = (k: string) => {
        let d = byDay[k];
        if (!d) { d = { mn: new Set(), mr: new Set(), mrt: new Set(), mnRev: 0, mrRev: 0, mrtRev: 0 }; byDay[k] = d; }
        return d;
      };

      for (const o of orders) {
        const custId = o.shopifyCustomerId;
        if (!custId) continue;
        const segment = segmentByCust.get(custId) || "organic";
        const isMatched = matchedOrderIds.has(o.shopifyOrderId);
        const isUtm = !!o.utmConfirmedMeta;
        const isFirst = o.customerOrderCountAtPurchase === 1;
        const net = Math.max(0, (o.frozenTotalPrice || 0) - (o.totalRefunded || 0));

        let tag: "metaNew" | "metaRepeat" | "metaRetargeted" | null = null;
        if (isMatched || isUtm) {
          // 2a / 2b-ii: order itself is Meta-attributed
          if (segment === "metaNew") {
            tag = isFirst ? "metaNew" : "metaRepeat";
          } else {
            tag = "metaRetargeted";
          }
        } else if (segment === "metaNew" && !isFirst) {
          // 2c: Meta-acquired customer returning via non-Meta channel
          tag = "metaRepeat";
        }

        if (!tag) continue;
        const dk = shopLocalDayKey(tz, o.createdAt);
        const day = ensureDay(dk);
        if (tag === "metaNew") { newCusts.add(custId); newRev += net; day.mn.add(custId); day.mnRev += net; }
        else if (tag === "metaRepeat") { repeatCusts.add(custId); repeatRev += net; day.mr.add(custId); day.mrRev += net; }
        else if (tag === "metaRetargeted") { retargetedCusts.add(custId); retargetedRev += net; day.mrt.add(custId); day.mrtRev += net; }
      }

      const byDayCounts: Record<string, { mnCust: number; mrCust: number; mrtCust: number; mnRev: number; mrRev: number; mrtRev: number }> = {};
      for (const [k, d] of Object.entries(byDay)) {
        byDayCounts[k] = { mnCust: d.mn.size, mrCust: d.mr.size, mrtCust: d.mrt.size, mnRev: r2(d.mnRev), mrRev: r2(d.mrRev), mrtRev: r2(d.mrtRev) };
      }

      return {
        metaNew: { customers: newCusts.size, revenue: r2(newRev) },
        metaRepeat: { customers: repeatCusts.size, revenue: r2(repeatRev) },
        metaRetargeted: { customers: retargetedCusts.size, revenue: r2(retargetedRev) },
        byDay: byDayCounts,
      };
    },
  );

  // Overlay the per-cohort daily series onto dailyMap from the donut's
  // per-order tagging, so "Meta Customer Breakdown by Day" reconciles with the
  // "Meta Customer Breakdown Summary" donut (single source of truth).
  for (const [k, d] of Object.entries(donutBreakdown.byDay || {})) {
    const row = dailyMap[k];
    if (!row) continue;
    row.mnCust = d.mnCust; row.mrCust = d.mrCust; row.mrtCust = d.mrtCust;
    row.mnRev = d.mnRev; row.mrRev = d.mrRev; row.mrtRev = d.mrtRev;
  }

  // Single/Repeat splits - derive from customers
  let metaNewSingleCount = 0, metaNewSingleOrders = 0, metaNewSingleRevenue = 0;
  let metaNewRepeatCount2 = 0, metaNewRepeatOrders = 0, metaNewRepeatRevenue = 0;
  let metaRetargetedCountTile = 0, metaRetargetedOrdersTile = 0, metaRetargetedRevenueTile = 0;
  for (const c of customers) {
    const acq = c.firstOrderDate;
    if (!acq || acq < fromDate || acq > toDate) continue;
    const net = (c.totalSpent || 0) - (c.totalRefunded || 0);
    if (c.metaSegment === "metaNew") {
      if ((c.totalOrders || 0) === 1) {
        metaNewSingleCount++; metaNewSingleOrders += 1; metaNewSingleRevenue += net;
      } else {
        metaNewRepeatCount2++; metaNewRepeatOrders += (c.totalOrders || 0); metaNewRepeatRevenue += net;
      }
    } else if (c.metaSegment === "metaRetargeted") {
      metaRetargetedCountTile++;
      metaRetargetedOrdersTile += (c.totalOrders || 0);
      metaRetargetedRevenueTile += net;
    }
  }

  // Weekly cohort revenue - one bucket per ISO week the customer was
  // acquired (keyed on the Monday), separately for all customers and for
  // Meta-acquired customers only. Bottom of each bar = first-order revenue,
  // top = repeat revenue from the same customers (totalSpent − first-order
  // value), floored at zero so refunds can't push the stack below zero.
  //
  // CRITICAL: Customer.firstOrderDate is "earliest order we've imported",
  // not "true first-ever purchase". Without filtering, a customer with 128
  // lifetime orders whose earliest-imported order happens to sit in, say,
  // Feb 2026 would be counted as a Feb-2026 acquisition and their
  // totalSpent from years of shopping would pollute the repeat stack. We
  // lean on Shopify's own customerOrderCountAtPurchase = 1 (set on the
  // order row at sync time) as ground truth for "genuinely new" -
  // metaSegment "metaNew" already bakes this in for the Meta lane; for the
  // All-customers lane we pull a set of genuinely-new customer IDs from
  // the Order table.
  const mondayKey = (d: Date): string => {
    const x = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));
    const day = x.getUTCDay();
    const monOffset = day === 0 ? 6 : day - 1;
    x.setUTCDate(x.getUTCDate() - monOffset);
    return `${x.getUTCFullYear()}-${String(x.getUTCMonth() + 1).padStart(2, "0")}-${String(x.getUTCDate()).padStart(2, "0")}`;
  };
  const genuinelyNewRows = await queryCached(
    `${shopDomain}:genuinelyNewCustomerIds`, DEFAULT_TTL,
    () => db.order.findMany({
      where: { shopDomain, customerOrderCountAtPurchase: 1, isOnlineStore: true },
      select: { shopifyCustomerId: true },
    }),
  );
  const genuinelyNewIds = new Set<string>();
  for (const r of genuinelyNewRows) if (r.shopifyCustomerId) genuinelyNewIds.add(r.shopifyCustomerId);

  type CohortBucket = { weekStart: string; firstRev: number; repeatRev: number; customers: number };
  const allMap = new Map<string, CohortBucket>();
  const metaMap = new Map<string, CohortBucket>();
  const ensure = (m: Map<string, CohortBucket>, week: string): CohortBucket => {
    let b = m.get(week);
    if (!b) { b = { weekStart: week, firstRev: 0, repeatRev: 0, customers: 0 }; m.set(week, b); }
    return b;
  };
  for (const c of customers) {
    if (!c.firstOrderDate) continue;
    // Filter out customers whose earliest imported order wasn't their true
    // first-ever purchase - their totalSpent reflects years of prior
    // shopping and would hugely inflate the repeat stack.
    if (!c.shopifyCustomerId || !genuinelyNewIds.has(c.shopifyCustomerId)) continue;
    const firstRev = Math.max(0, c.firstOrderValue || 0);
    const totalSpent = Math.max(0, c.totalSpent || 0);
    const repeatRev = Math.max(0, totalSpent - firstRev);
    const week = mondayKey(c.firstOrderDate);
    const all = ensure(allMap, week);
    all.firstRev += firstRev; all.repeatRev += repeatRev; all.customers += 1;
    if (c.metaSegment === "metaNew") {
      const m = ensure(metaMap, week);
      m.firstRev += firstRev; m.repeatRev += repeatRev; m.customers += 1;
    }
  }
  const toSeries = (m: Map<string, CohortBucket>) =>
    [...m.values()]
      .sort((a, b) => a.weekStart.localeCompare(b.weekStart))
      .map((w) => ({
        weekStart: w.weekStart,
        firstRev: Math.round(w.firstRev * 100) / 100,
        repeatRev: Math.round(w.repeatRev * 100) / 100,
        customers: w.customers,
      }));
  const weeklyCohortSeries = { all: toSeries(allMap), meta: toSeries(metaMap) };

  // Journey from cache blob
  const journeyMeta = journeyBlob?.meta || {};
  const journeyAll = journeyBlob?.all || {};

  // LTV from cache blob - also synthesize tile.cpa + tile.paybackOrders for the component
  const ltvTileRaw = ltvBlob?.ltvTile || { meta: {}, all: {} };
  const ltvMetaCount = ltvTileRaw.meta?.count || 0;
  const ltvMetaAvgFirst = ltvTileRaw.meta?.avgFirstOrder || 0;
  const tileCpa = ltvMetaCount > 0 ? r2(allTimeMetaSpend / ltvMetaCount) : 0;
  const tilePayback = ltvMetaAvgFirst > 0 ? r2(tileCpa / ltvMetaAvgFirst) : 0;
  const ltvTile = {
    meta: { ...ltvTileRaw.meta, cpa: tileCpa, paybackOrders: tilePayback },
    all: ltvTileRaw.all || {},
  };
  const ltvBenchmark = ltvBlob?.ltvBenchmark || { meta: { maxWindow: 0, windows: [] }, all: { maxWindow: 0, windows: [] } };
  const ltvRecent = ltvBlob?.ltvRecent || { meta: [], all: [] };
  const ltvMonthly = ltvBlob?.ltvMonthly || { meta: { rows: [], maxMonth: 0 }, all: { rows: [], maxMonth: 0 } };
  // Per-metaNew-customer records for the filterable LTV tile exploration.
  const ltvCustomers: Array<{
    gender: string | null; age: string | null; country: string | null;
    ltv: number; firstOrder: number; orders: number;
    timeTo2nd: number | null; tenureDays: number | null; acqMonth: string;
    acqDaysAgo?: number;
    ltvByWindow: Record<string, number>;
    ltvByMonth?: number[];
  }> = ltvBlob?.ltvCustomers || [];

  // LTV tile component fields (mn = Meta New, all = all customers)
  const mnAvgLtv = ltvTile.meta?.avgLtv || 0;
  const mnAvgOrders = ltvTile.meta?.avgOrders || 0;
  const mnRepeatRate = ltvTile.meta?.repeatRate || 0;
  const mnAvgAov = ltvTile.meta?.avgAov || 0;
  const mnMedianTimeTo2nd = ltvTile.meta?.medianTimeTo2nd || null;
  const ltvMetaNewCount = ltvTile.meta?.count || 0;
  const ltvAllMetaSpend = allTimeMetaSpend;
  const mnCPA = ltvMetaNewCount > 0 ? r2(ltvAllMetaSpend / ltvMetaNewCount) : 0;
  const mnLtvCac = mnCPA > 0 && mnAvgLtv > 0 ? r2(mnAvgLtv / mnCPA) : 0;
  const mnAvgFirstOrder = ltvTile.meta?.avgFirstOrder || 0;
  const mnPaybackOrders = mnAvgFirstOrder > 0 ? r2(mnCPA / mnAvgFirstOrder) : 0;
  const mnReorderWithin90 = 0; // not currently in cache; would need addition
  const allCount = ltvTile.all?.count || 0;
  const allAvgLtv = ltvTile.all?.avgLtv || 0;
  const allAvgOrders = ltvTile.all?.avgOrders || 0;
  const allRepeatRate = ltvTile.all?.repeatRate || 0;
  const allAvgAov = ltvTile.all?.avgAov || 0;
  const allMedianTimeTo2nd = ltvTile.all?.medianTimeTo2nd || null;
  const allReorderWithin90 = 0;

  const medianTimeTo2nd = mnMedianTimeTo2nd;
  const reorderWithin90 = 0;
  const prevMedianTimeTo2nd = null;

  // Unmatched conversions - count actual Attribution rows with confidence=0
  // whose synthetic shopifyOrderId date falls in the range. This matches the
  // Weekly Report's approach exactly, so the two pages always agree.
  // (Previously computed as `max(0, totalMetaConversions - metaCount)`, which
  // subtracted new-customer count from total Meta-reported conversions - two
  // different populations - and inflated whenever Meta reported more total
  // conversions than we matched to NEW customers, regardless of why.)
  let unmatchedConversions = 0;
  let unmatchedRevenue = 0;
  for (const v of Object.values(unmatchedByDay)) {
    unmatchedConversions += v.count;
    unmatchedRevenue += v.revenue;
  }

  // AI cache
  const aiCurrentHash = computeDataHash({
    metaCount, organicCount, metaAvgLtv, organicAvgLtv, aovCpaRatio, newCustomerCPA,
    metaRepeatRate, organicRepeatRate, metaRevenue, metaRevPct,
    ageBreakdown, genderBreakdown, topCountries,
  });
  const aiCachedInsights = aiCached?.insights || null;
  const aiGeneratedAt = aiCached?.generatedAt?.toISOString() || null;
  const aiIsStale = aiCached ? aiCached.dataHash !== aiCurrentHash : false;

  // Order Explorer (table now lives at the bottom of this tab — Order
  // Explorer no longer has its own route). Filter state is read from URL
  // search params and pushed back via useSubmit on change.
  const url = new URL(request.url);
  const orderTagFilter = url.searchParams.get("orderTag") || "meta";
  const orderCampaignFilter = url.searchParams.get("orderCampaign") || "all";
  const orderExplorer = await buildOrderExplorerData({
    shopDomain, fromDate, toDate, fromKey, toKey, tz,
    shopifyCurrency: shopForTz?.shopifyCurrency,
    tagFilter: orderTagFilter,
    campaignFilter: orderCampaignFilter,
  });

  console.log(`[customers] loader DONE ${Date.now() - _t0}ms`);
  return json({
    aiCachedInsights, aiGeneratedAt, aiIsStale,
    rows: [], dailyData, prevDailyData,
    metaCount, organicCount, metaAvgLtv, organicAvgLtv,
    newCustomerCPA, metaAvgFirstOrder, organicAvgFirstOrder, aovCpaRatio,
    metaRepeatRate, organicRepeatRate, metaRepeatTotal, prevMetaRepeatTotal,
    metaRevenue, metaRevPct, totalAllRevenue,
    metaAvgOrders, organicAvgOrders,
    medianTimeTo2nd, reorderWithin90,
    paybackOrders, newInPeriod, attrNewCustomerRevenue,
    currencySymbol,
    defaultMarginPct: shopForTz?.defaultMarginPct ?? null,
    prevMetaCount, prevMetaAvgFirstOrder, prevAovCpaRatio, prevNewCustomerCPA,
    prevMetaRepeatRate, prevMetaRevenue,
    prevMedianTimeTo2nd,
    totalCustomersInRange,
    prevMetaNewCustomersInRange, prevMetaRepeatCustomersInRange, prevMetaRetargetedCustomersInRange,
    prevMetaNewRevenueInRange, prevMetaRepeatRevenueInRange, prevMetaRetargetedRevenueInRange,
    prevTotalCustomersInRange,
    totalRevenueInRange, prevTotalRevenueInRange,
    matchedMetaOrdersInRange, totalOrdersInRange, prevMatchedMetaOrdersInRange, prevTotalOrdersInRange,
    unmatchedConversionsWithValue, prevUnmatchedConversions, prevUnmatchedRevenue, prevUnmatchedConversionsWithValue,
    ageBreakdown, genderBreakdown, newAgeBreakdown, newGenderBreakdown, allCustomerGenderBreakdown,
    newDemoConversions, newDemoExactCount, topCountries, topCities,
    allMetaTopCountries, allMetaTopCities, metaNewTopCountries, metaNewTopCities,
    allGeoCount, allMetaGeoCount, metaNewGeoCount,
    metaNewSingleCount, metaNewSingleOrders, metaNewSingleRevenue,
    metaNewRepeatCount: metaNewRepeatCount2, metaNewRepeatOrders, metaNewRepeatRevenue,
    metaRetargetedCount: metaRetargetedCountTile, metaRetargetedOrders: metaRetargetedOrdersTile, metaRetargetedRevenue: metaRetargetedRevenueTile,
    journeyFirstAOV: journeyInRange.meta.firstAOV || 0,
    journeySecondAOV: journeyInRange.meta.secondAOV || 0,
    journeyThirdAOV: journeyInRange.meta.thirdAOV || 0,
    journeyGapDays: journeyInRange.meta.gap1to2Days,
    journeyGap2to3Days: journeyInRange.meta.gap2to3Days,
    journeyCustomerCount: journeyInRange.meta.firstOrderCount || 0,
    totalCustomerCount: journeyInRange.all.firstOrderCount || 0,
    totalMetaCustomerCount: journeyInRange.meta.firstOrderCount || 0,
    allJourneyFirstAOV: journeyInRange.all.firstAOV || 0,
    allJourneySecondAOV: journeyInRange.all.secondAOV || 0,
    allJourneyThirdAOV: journeyInRange.all.thirdAOV || 0,
    allJourneyGapDays: journeyInRange.all.gap1to2Days,
    allJourneyGap2to3Days: journeyInRange.all.gap2to3Days,
    allJourneyCustomerCount: journeyInRange.all.firstOrderCount || 0,
    metaFirstOrderCount: journeyInRange.meta.firstOrderCount || 0,
    metaSecondOrderCount: journeyInRange.meta.secondOrderCount || 0,
    metaThirdOrderCount: journeyInRange.meta.thirdOrderCount || 0,
    allFirstOrderCount: journeyInRange.all.firstOrderCount || 0,
    allSecondOrderCount: journeyInRange.all.secondOrderCount || 0,
    allThirdOrderCount: journeyInRange.all.thirdOrderCount || 0,
    totalDemoConversions, totalMetaConversions, totalMetaConversionValue,
    metaNewOrdersInRange, metaNewRevenueInRange, metaNewCustomersInRange,
    metaRepeatOrdersInRange, metaRepeatRevenueInRange, metaRepeatCustomersInRange,
    metaRetargetedOrdersInRange, metaRetargetedRevenueInRange, metaRetargetedCustomersInRange,
    donutMetaNewCustomers: donutBreakdown.metaNew.customers,
    donutMetaNewRevenue: donutBreakdown.metaNew.revenue,
    donutMetaRepeatCustomers: donutBreakdown.metaRepeat.customers,
    donutMetaRepeatRevenue: donutBreakdown.metaRepeat.revenue,
    donutMetaRetargetedCustomers: donutBreakdown.metaRetargeted.customers,
    donutMetaRetargetedRevenue: donutBreakdown.metaRetargeted.revenue,
    metaNewCount: ltvMetaNewCount, mnAvgLtv, mnAvgOrders, mnRepeatRate, mnAvgAov,
    mnCPA, mnLtvCac, mnPaybackOrders, mnMedianTimeTo2nd, mnReorderWithin90,
    allCount, allAvgLtv, allAvgOrders, allRepeatRate, allAvgAov,
    allMedianTimeTo2nd, allReorderWithin90,
    metaAvgAov, totalMetaSpend, ltvAllMetaSpend, metaSpendByAcqMonth,
    unmatchedConversions, unmatchedRevenue,
    ltvBenchmark, ltvTile, ltvRecent, ltvMonthly, ltvCustomers,
    weeklyCohortSeries,
    fromKey, toKey, preset,
    orderExplorer,
    journeyReportsEnabled,
  });
};

// ═══════════════════════════════════════════════════════════════
// ACTION
// ═══════════════════════════════════════════════════════════════

export const action = async ({ request }) => {
  const { session } = await authenticate.admin(request);
  const shopDomain = session.shop;
  const formData = await request.formData();
  const actionType = formData.get("actionType");

  if (actionType === "generateInsights") {
    // AI Insights are hidden for V1 — the panel is gated behind `false` in the
    // render, and this server path is disabled too so the shipped build has no
    // route to Anthropic. Re-enable alongside the V2 UI.
    return new Response("AI insights are not available.", { status: 404 });
    const pageKey = String(formData.get("pageKey"));
    const taskId = `ai:${pageKey}:${shopDomain}`;
    const customSystem = formData.get("customSystemPrompt");
    const customPage = formData.get("customPagePrompt");
    const promptOverrides = (customSystem || customPage) ? { system: customSystem ? String(customSystem) : null, page: customPage ? String(customPage) : null } : null;

    setProgress(taskId, { status: "running", message: "Generating AI insights..." });

    (async () => {
      try {
        const shop = await db.shop.findUnique({ where: { shopDomain } });
        const tz = shop?.shopifyTimezone || "UTC";
        const { fromDate, toDate, fromKey: dateFromStr, toKey: dateToStr } = parseDateRange(request, tz);
        const cs = currencySymbolFromCode(shop?.shopifyCurrency);

        const orders = await db.order.findMany({ where: { shopDomain, isOnlineStore: true } });
        const attributions = await db.attribution.findMany({ where: { shopDomain, confidence: { gt: 0 } } });
        const customers = await db.customer.findMany({ where: { shopDomain } });
        const metaInsights = await db.metaInsight.findMany({
          where: { shopDomain, date: { gte: fromDate, lte: toDate } },
          select: { spend: true },
        });
        const totalMetaSpend = metaInsights.reduce((s, i) => s + i.spend, 0);

        // Build simplified customer data for AI
        const attrOrderIds = new Set(attributions.map(a => a.shopifyOrderId));
        const metaOrders = orders.filter(o => attrOrderIds.has(o.shopifyOrderId));
        const organicOrders = orders.filter(o => !attrOrderIds.has(o.shopifyOrderId));

        const metaCustomerIds = new Set(metaOrders.map(o => o.shopifyCustomerId).filter(Boolean));
        const organicCustomerIds = new Set(organicOrders.map(o => o.shopifyCustomerId).filter(Boolean));

        const metaCount = metaCustomerIds.size;
        const organicCount = organicCustomerIds.size;
        const metaRevenue = metaOrders.reduce((s, o) => s + (o.frozenTotalPrice - (o.totalRefunded || 0)), 0);
        const organicRevenue = organicOrders.reduce((s, o) => s + (o.frozenTotalPrice - (o.totalRefunded || 0)), 0);

        const metaAvgLtv = metaCount > 0 ? Math.round((metaRevenue / metaCount) * 100) / 100 : 0;
        const organicAvgLtv = organicCount > 0 ? Math.round((organicRevenue / organicCount) * 100) / 100 : 0;
        const newCustomerCPA = metaCount > 0 ? Math.round((totalMetaSpend / metaCount) * 100) / 100 : 0;
        const ltvCac = newCustomerCPA > 0 ? Math.round((metaAvgLtv / newCustomerCPA) * 100) / 100 : 0;

        // Count repeat customers
        const metaCustOrderCounts = {};
        for (const o of metaOrders) {
          if (o.shopifyCustomerId) metaCustOrderCounts[o.shopifyCustomerId] = (metaCustOrderCounts[o.shopifyCustomerId] || 0) + 1;
        }
        const metaRepeatRate = metaCount > 0 ? Math.round((Object.values(metaCustOrderCounts).filter((c: any) => c > 1).length / metaCount) * 100) : 0;

        const organicCustOrderCounts = {};
        for (const o of organicOrders) {
          if (o.shopifyCustomerId) organicCustOrderCounts[o.shopifyCustomerId] = (organicCustOrderCounts[o.shopifyCustomerId] || 0) + 1;
        }
        const organicRepeatRate = organicCount > 0 ? Math.round((Object.values(organicCustOrderCounts).filter((c: any) => c > 1).length / organicCount) * 100) : 0;

        const pageData = {
          metaCount, organicCount, metaAvgLtv, organicAvgLtv, ltvCac, newCustomerCPA,
          metaRepeatRate, organicRepeatRate,
          metaRevenue, metaRevPct: metaCount > 0 ? Math.round((metaRevenue / (metaRevenue + organicRevenue)) * 100) : 0,
        };

        await generateInsights(shopDomain, pageKey, pageData, dateFromStr, dateToStr, cs, promptOverrides);
        completeProgress(taskId, { success: true });
      } catch (err) {
        console.error("[AI] Customer insights failed:", err);
        failProgress(taskId, err);
      }
    })();

    return json({ aiTaskId: taskId });
  }

  if (actionType === "saveMargin") {
    const raw = parseInt(String(formData.get("marginPct")), 10);
    if (Number.isFinite(raw)) {
      const clamped = Math.max(0, Math.min(100, raw));
      await db.shop.update({
        where: { shopDomain },
        data: { defaultMarginPct: clamped },
      });
    }
    return json({ ok: true });
  }

  return json({});
};

// ═══════════════════════════════════════════════════════════════
// INFOGRAPHIC COMPONENTS
// ═══════════════════════════════════════════════════════════════

// ── Donut Chart ──
// Each segment is rendered as a discrete SVG <path> arc (rather than a
// dasharray-on-circle). Path arcs have explicit start/end points, so
// adjacent segments share an exact pixel-aligned boundary — no sub-pixel
// stroke bleed at the junction. `hovered` / `onHoverChange` link legend-row
// hover to segment hover; if omitted, internal state is used so drop-in
// callers still work.
function DonutChart({ segments, size = 180, thickness = 28, centerLabel, centerValue, hovered: hoveredProp, onHoverChange, formatValue }: {
  segments: { label: string; value: number; color: string }[];
  size?: number;
  thickness?: number;
  centerLabel?: string;
  centerValue?: string;
  hovered?: number | null;
  onHoverChange?: (i: number | null) => void;
  formatValue?: (v: number) => string;
}) {
  const [internalHovered, setInternalHovered] = useState<number | null>(null);
  const hovered = hoveredProp !== undefined ? hoveredProp : internalHovered;
  const setHovered = (i: number | null) => {
    if (onHoverChange) onHoverChange(i);
    else setInternalHovered(i);
  };
  const fmt = formatValue || ((v: number) => v.toLocaleString());
  const total = segments.reduce((s, seg) => s + seg.value, 0);
  if (total === 0) return <div style={{ width: size, height: size, display: "flex", alignItems: "center", justifyContent: "center", color: "#9CA3AF" }}>No data</div>;

  const radius = (size - thickness) / 2;
  const center = size / 2;
  const pad = 8;
  const svgSize = size + pad * 2;
  const cx = center + pad;
  const cy = center + pad;

  let cumulativeFraction = 0;
  const arcs = segments.filter(s => s.value > 0).map((seg, i) => {
    const startFrac = cumulativeFraction;
    const fraction = seg.value / total;
    cumulativeFraction += fraction;
    const endFrac = cumulativeFraction;
    return { ...seg, startFrac, endFrac, fraction, index: i };
  });

  // Polar offset: -90° so the path starts at 12 o'clock.
  const arcPath = (startFrac: number, endFrac: number, r: number) => {
    const f = endFrac - startFrac;
    // A single-segment full circle can't be expressed as one SVG arc, so
    // split into two halves.
    if (f >= 0.999) {
      return `M ${cx} ${cy - r} A ${r} ${r} 0 1 1 ${cx} ${cy + r} A ${r} ${r} 0 1 1 ${cx} ${cy - r}`;
    }
    const a0 = (startFrac * 2 - 0.5) * Math.PI;
    const a1 = (endFrac * 2 - 0.5) * Math.PI;
    const x0 = cx + r * Math.cos(a0);
    const y0 = cy + r * Math.sin(a0);
    const x1 = cx + r * Math.cos(a1);
    const y1 = cy + r * Math.sin(a1);
    const largeArc = f > 0.5 ? 1 : 0;
    return `M ${x0} ${y0} A ${r} ${r} 0 ${largeArc} 1 ${x1} ${y1}`;
  };

  // Auto-shrink the centre value when it's long (e.g. "£260,940") so the
  // text doesn't overflow the donut hole. Inner hole diameter is
  // `size - 2*thickness`; for the default 170/26 that's 118px.
  const innerD = size - 2 * thickness;
  const sizeFontFor = (text: string | undefined, base: number) => {
    if (!text) return base;
    // Rough cap: keep the rendered text under ~85% of the inner diameter.
    const maxPx = innerD * 0.85;
    const charW = base * 0.55; // bold sans-serif average
    const estW = text.length * charW;
    if (estW <= maxPx) return base;
    return Math.max(14, Math.floor(base * (maxPx / estW)));
  };
  const centerFontSize = sizeFontFor(centerValue, 26);
  const hoverFontSize = hovered !== null && arcs[hovered] ? sizeFontFor(fmt(arcs[hovered].value), 22) : 22;

  return (
    <div style={{ position: "relative", width: size, height: size }}>
      <svg
        width={svgSize}
        height={svgSize}
        viewBox={`0 0 ${svgSize} ${svgSize}`}
        style={{ position: "absolute", top: -pad, left: -pad, overflow: "visible" }}
        shapeRendering="geometricPrecision"
      >
        {arcs.map((arc) => (
          <path
            key={arc.label}
            d={arcPath(arc.startFrac, arc.endFrac, radius)}
            fill="none"
            stroke={arc.color}
            strokeWidth={hovered === arc.index ? thickness + 6 : thickness}
            strokeLinecap="butt"
            style={{ transition: "stroke-width 0.2s", cursor: "pointer", opacity: hovered !== null && hovered !== arc.index ? 0.5 : 1 }}
            onMouseEnter={() => setHovered(arc.index)}
            onMouseLeave={() => setHovered(null)}
          />
        ))}
      </svg>
      <div style={{
        position: "absolute", top: 0, left: 0, right: 0, bottom: 0,
        display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center",
        pointerEvents: "none",
      }}>
        {hovered !== null && arcs[hovered] ? (
          <>
            <div style={{ fontSize: `${hoverFontSize}px`, fontWeight: 800, color: arcs[hovered].color }}>{fmt(arcs[hovered].value)}</div>
            <div style={{ fontSize: "11px", color: "#6B7280", fontWeight: 500 }}>{arcs[hovered].label}</div>
            <div style={{ fontSize: "11px", color: "#9CA3AF" }}>{Math.round(arcs[hovered].fraction * 100)}%</div>
          </>
        ) : (
          <>
            <div style={{ fontSize: `${centerFontSize}px`, fontWeight: 800, color: "#1F2937" }}>{centerValue}</div>
            <div style={{ fontSize: "11px", color: "#6B7280", fontWeight: 500 }}>{centerLabel}</div>
          </>
        )}
      </div>
    </div>
  );
}

// ── Horizontal Bar Chart ──
// `maxVisible` caps how many rows show without scroll; remainder is
// scroll-revealed inside a fixed-height container. `maxItems` still caps
// the underlying dataset. `sharedMax` lets callers align two charts to
// the same x-axis scale (top-countries vs top-cities).
function HBarChart({ items, colorFn, formatValue, maxItems = 6, total, maxVisible, sharedMax }: {
  items: { label: string; value: number; subValue?: string }[];
  colorFn: (i: number) => string;
  formatValue: (v: number) => string;
  maxItems?: number;
  total?: number;
  maxVisible?: number;
  sharedMax?: number;
}) {
  const visible = items.slice(0, maxItems);
  const maxVal = sharedMax ?? Math.max(...visible.map(i => i.value), 1);
  const sumTotal = total ?? visible.reduce((s, i) => s + i.value, 0);
  const cap = maxVisible ?? maxItems;
  // Outer row height is pinned so that long labels which wrap onto two lines
  // (e.g. "United Arab Emirates") don't grow the row and break the consistent
  // spacing between bars. Bar height stays at barHeight; the row slot has
  // enough vertical room (rowSlotHeight) to fit a two-line label at lineHeight 1.1.
  const barHeight = 22;
  const rowSlotHeight = 30;
  const rowGap = 6;
  const needsScroll = visible.length > cap;
  const scrollHeight = cap * rowSlotHeight + (cap - 1) * rowGap;

  return (
    <div style={{
      display: "flex", flexDirection: "column", gap: `${rowGap}px`, width: "100%",
      ...(needsScroll ? { maxHeight: `${scrollHeight}px`, overflowY: "auto", paddingRight: "4px" } : {}),
    }}>
      {visible.map((item, i) => {
        const pct = sumTotal > 0 ? Math.round((item.value / sumTotal) * 100) : 0;
        return (
        <div key={item.label} style={{ display: "flex", alignItems: "center", gap: "8px", height: `${rowSlotHeight}px` }}>
          <div style={{ width: "70px", fontSize: "12px", lineHeight: "1.1", color: "#4B5563", fontWeight: 500, textAlign: "right", flexShrink: 0 }}>
            {item.label}
          </div>
          <div style={{ flex: 1, height: `${barHeight}px`, background: "#F3F4F6", borderRadius: "4px", overflow: "hidden", position: "relative" }}>
            <div style={{
              height: "100%", width: `${Math.max((item.value / maxVal) * 100, 2)}%`,
              background: colorFn(i), borderRadius: "4px",
              transition: "width 0.5s ease",
            }} />
            <div style={{
              position: "absolute", right: "6px", top: "50%", transform: "translateY(-50%)",
              fontSize: "11px", color: "#1F2937", display: "grid",
              gridTemplateColumns: "56px 36px 68px", alignItems: "baseline",
              gap: "6px", textAlign: "right",
            }}>
              <span style={{ fontWeight: 700 }}>{formatValue(item.value)}</span>
              <span style={{ fontWeight: 400 }}>{pct}%</span>
              <span style={{ fontWeight: 400 }}>{item.subValue || ""}</span>
            </div>
          </div>
        </div>
        );
      })}
    </div>
  );
}

// ── Journey Flow ──
function JourneyFlow({ firstAOV, gapDays, secondAOV, thirdAOV, gap2to3Days, customerCount, firstOrderCount, secondOrderCount, thirdOrderCount, cs }: {
  firstAOV: number; gapDays: number | null; secondAOV: number;
  thirdAOV: number; gap2to3Days: number | null;
  customerCount: number;
  firstOrderCount: number; secondOrderCount: number; thirdOrderCount: number; cs: string;
}) {
  if (firstOrderCount === 0) {
    return (
      <div style={{ display: "flex", alignItems: "center", justifyContent: "center", height: "100%", color: "#9CA3AF", fontSize: "14px" }}>
        No new customers acquired in this period
      </div>
    );
  }

  // Show the avg AOV as soon as there's at least one order in the cohort -
  // Andy wants the data surfaced even for a 1-2 customer sample rather than a
  // "too few to show" placeholder.
  const MIN_SAMPLE = 1;
  const aov2Change = firstAOV > 0 ? Math.round(((secondAOV - firstAOV) / firstAOV) * 100) : 0;
  const aov3Change = secondAOV > 0 ? Math.round(((thirdAOV - secondAOV) / secondAOV) * 100) : 0;
  const repeatRate = firstOrderCount > 0 ? Math.round((secondOrderCount / firstOrderCount) * 100) : 0;
  const thirdRate = secondOrderCount > 0 ? Math.round((thirdOrderCount / secondOrderCount) * 100) : 0;

  const orderBox = (label: string, aov: number, aovChange: number, count: number, countLabel: string, gradient: string, shadow: string, hasData: boolean) => (
    <div style={{ display: "flex", flexDirection: "column", alignItems: "center", flex: "1 1 0", minWidth: 0 }}>
      <div style={{
        background: hasData ? gradient : "#D1D5DB",
        borderRadius: "13px", padding: "18px clamp(10px, 2vw, 28px)", color: "#fff", textAlign: "center",
        width: "100%", minWidth: 0, boxShadow: hasData ? shadow : "none",
      }}>
        <div style={{ fontSize: "11px", fontWeight: 500, opacity: 0.8, textTransform: "uppercase", letterSpacing: "0.5px" }}>{label}</div>
        <div style={{ fontSize: "clamp(20px, 3.4vw, 28px)", fontWeight: 800, marginTop: "5px" }}>
          {hasData ? `${cs}${Math.round(aov)}` : "-"}
        </div>
        <div style={{ fontSize: "11px", opacity: 0.7, marginTop: "2px" }}>
          {hasData ? (
            <>avg AOV{aovChange !== 0 && (
              <span style={{ color: aovChange > 0 ? "#86EFAC" : "#FCA5A5" }}>
                {" "}({aovChange > 0 ? "+" : ""}{aovChange}%)
              </span>
            )}</>
          ) : (count > 0 ? "too few to show" : "no data yet")}
        </div>
      </div>
      <div style={{ fontSize: "12px", color: "#6B7280", fontWeight: 500, marginTop: "8px" }}>
        {count.toLocaleString()} {countLabel}
      </div>
    </div>
  );

  const arrow = (days: number | null, rate: number, rateLabel: string, gradId: string, colorFrom: string, colorTo: string) => (
    <div style={{ display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", padding: "0 4px", minWidth: 0, flex: "1 1 0" }}>
      <div style={{ display: "flex", flexDirection: "column", alignItems: "center", width: "100%" }}>
        <svg width="100%" height="32" viewBox="0 0 110 32" preserveAspectRatio="xMidYMid meet">
          <defs>
            <linearGradient id={gradId} x1="0" y1="0" x2="1" y2="0">
              <stop offset="0%" stopColor={colorFrom} />
              <stop offset="100%" stopColor={colorTo} />
            </linearGradient>
          </defs>
          <line x1="0" y1="16" x2="95" y2="16" stroke={`url(#${gradId})`} strokeWidth="2" strokeDasharray="6 3" />
          <polygon points="95,10 110,16 95,22" fill={colorTo} />
          {/* Lozenge background */}
          <rect x="20" y="3" width="66" height="24" rx="12" fill="#F9FAFB" stroke="#E5E7EB" strokeWidth="1" />
          <text x="53" y="20" textAnchor="middle" fontSize="11" fontWeight="700" fill="#374151">
            {days != null ? `${days} days` : "-"}
          </text>
        </svg>
      </div>
      <div style={{ fontSize: "11px", color: "#6B7280", fontWeight: 500, marginTop: "3px" }}>
        {rate}% {rateLabel}
      </div>
    </div>
  );

  // Outer wrapper handles overflow so the journey scrolls horizontally on
  // narrow laptops instead of getting cropped at both edges (Andy hit this
  // on a MacBook). Inner row keeps the original centered layout when there's
  // room, but allows the items to fall back to natural width if the
  // viewport is narrower than their combined min width.
  return (
    <div style={{ overflowX: "auto", width: "100%" }}>
      <div style={{
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        gap: "0",
        padding: "22px 8px",
        minWidth: 0,
        margin: "0 auto",
      }}>
        {orderBox("1st Order", firstAOV, 0, firstOrderCount, "acquired",
          "linear-gradient(135deg, #7C3AED 0%, #6D28D9 100%)", "0 4px 12px rgba(124,58,237,0.3)", true)}
        {arrow(gapDays, repeatRate, "came back", "arrowGrad1", "#7C3AED", "#0891B2")}
        {orderBox("2nd Order", secondAOV, aov2Change, secondOrderCount, "repeated",
          "linear-gradient(135deg, #0891B2 0%, #0E7490 100%)", "0 4px 12px rgba(8,145,178,0.3)", secondOrderCount >= MIN_SAMPLE)}
        {arrow(gap2to3Days, thirdRate, "came back", "arrowGrad2", "#0891B2", "#2E7D32")}
        {orderBox("3rd Order", thirdAOV, aov3Change, thirdOrderCount, "repeated",
          "linear-gradient(135deg, #2E7D32 0%, #1B5E20 100%)", "0 4px 12px rgba(46,125,50,0.3)", thirdOrderCount >= MIN_SAMPLE)}
      </div>
    </div>
  );
}


// ═══════════════════════════════════════════════════════════════
// Revenue by Weekly Cohort - stacked bars, first-order revenue at the
// bottom and repeat-order revenue stacked on top. Hover shows both
// actual figures; toggle selects the last 52 weeks or the whole history.
// ═══════════════════════════════════════════════════════════════

interface WeeklyCohortPoint {
  weekStart: string;
  firstRev: number;
  repeatRev: number;
  customers: number;
}

function WeeklyCohortRevenue({ weekly, cs }: { weekly: { all: WeeklyCohortPoint[]; meta: WeeklyCohortPoint[] }; cs: string }) {
  const [scope, setScope] = useState<"365" | "all">("365");
  const [segment, setSegment] = useState<"meta" | "all">("meta");
  const [hoverIdx, setHoverIdx] = useState<number | null>(null);
  const wrapRef = useRef<HTMLDivElement>(null);

  const series = segment === "meta" ? (weekly?.meta || []) : (weekly?.all || []);
  const sorted = useMemo(() => series.slice().sort((a, b) => a.weekStart.localeCompare(b.weekStart)), [series]);
  const windowed = useMemo(() => {
    if (scope === "all" || sorted.length === 0) return sorted;
    // 365 days back from the latest cohort in the dataset - 52-ish weeks.
    const lastKey = sorted[sorted.length - 1].weekStart;
    const [y, m, d] = lastKey.split("-").map(Number);
    const cutoff = new Date(Date.UTC(y, m - 1, d - 365));
    return sorted.filter((w) => {
      const [wy, wm, wd] = w.weekStart.split("-").map(Number);
      return new Date(Date.UTC(wy, wm - 1, wd)) >= cutoff;
    });
  }, [sorted, scope]);

  const maxVal = useMemo(() => {
    let m = 0;
    for (const p of windowed) m = Math.max(m, (p.firstRev || 0) + (p.repeatRev || 0));
    return m || 1;
  }, [windowed]);

  const fmtMoney = (v: number) => {
    if (v >= 1000000) return `${cs}${(v / 1000000).toFixed(2)}M`;
    if (v >= 1000) return `${cs}${(v / 1000).toFixed(v >= 10000 ? 0 : 1)}k`;
    return `${cs}${Math.round(v).toLocaleString()}`;
  };
  const fmtWeek = (key: string) => {
    const [y, m, d] = key.split("-").map(Number);
    return new Date(Date.UTC(y, m - 1, d)).toLocaleDateString("en-GB", { day: "2-digit", month: "short", year: "2-digit" });
  };

  return (
    <BlockStack gap="300">
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: 8, flexWrap: "wrap" }}>
        <Text as="h2" variant="headingLg">Revenue by Weekly Cohort</Text>
        <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
          <div className="toggle-group">
            <TipButton tip={SEGMENT_TIPS.metaCustomers} className={`toggle-btn ${segment === "meta" ? "active" : ""}`} onClick={() => setSegment("meta")}>Meta Customers</TipButton>
            <TipButton tip={SEGMENT_TIPS.allCustomers} className={`toggle-btn ${segment === "all" ? "active" : ""}`} onClick={() => setSegment("all")}>All Customers</TipButton>
          </div>
          <div className="toggle-group">
            <button className={`toggle-btn ${scope === "365" ? "active" : ""}`} onClick={() => setScope("365")}>Previous 365 days</button>
            <button className={`toggle-btn ${scope === "all" ? "active" : ""}`} onClick={() => setScope("all")}>All time</button>
          </div>
        </div>
      </div>
      <Text as="p" variant="bodySm" tone="subdued">
        {segment === "meta"
          ? "Meta-acquired customers grouped by the week of their first order. Base = first-order revenue; top = repeat orders those same customers have placed since."
          : "All new customers grouped by the week of their first order. Base = first-order revenue; top = repeat orders those same customers have placed since."}
      </Text>
      {windowed.length === 0 ? (
        <div style={{ padding: 20, textAlign: "center", color: "#9CA3AF", fontSize: 13 }}>No cohort data in this window.</div>
      ) : (() => {
        // Overlay a repeat-% line on a second Y-axis (0–100% right-side).
        // Repeat % = repeatRev / (firstRev + repeatRev). It tells the
        // merchant how much of that week's cohort revenue is coming from
        // customers who came back - the real LTV signal.
        const PLOT_H = 320;
        const X_AXIS_H = 20;            // reserved for the week labels under the bars
        const OUTER_H = PLOT_H + X_AXIS_H;
        const lineColor = "#F59E0B"; // amber - contrasts indigo bars
        return (
          <div ref={wrapRef} style={{ position: "relative" }}>
          <div style={{ display: "flex", gap: 8 }}>
            {/* Left Y-axis: £ revenue */}
            <div style={{ width: 44, flexShrink: 0, display: "flex", flexDirection: "column", justifyContent: "space-between", fontSize: 10, color: "#9CA3AF", paddingTop: 2, paddingBottom: X_AXIS_H - 2 }}>
              <span>{fmtMoney(maxVal)}</span>
              <span>{fmtMoney(maxVal / 2)}</span>
              <span>{cs}0</span>
            </div>
            <div style={{ flex: 1, position: "relative", height: OUTER_H }}>
              {/* Gridlines */}
              {[0, 0.5, 1].map((f) => (
                <div key={f} style={{
                  position: "absolute", left: 0, right: 0, top: `${(1 - f) * 100 * (PLOT_H / OUTER_H)}%`,
                  borderTop: "1px dashed #F3F4F6",
                }} />
              ))}
              {/* Bars */}
              <div style={{ display: "flex", alignItems: "flex-end", gap: 2, height: PLOT_H, paddingTop: 0, position: "relative" }}>
                {windowed.map((p, i) => {
                  const total = (p.firstRev || 0) + (p.repeatRev || 0);
                  const totalH = maxVal > 0 ? (total / maxVal) * PLOT_H : 0;
                  const firstH = total > 0 ? (p.firstRev / total) * totalH : 0;
                  const repeatH = Math.max(0, totalH - firstH);
                  const isHover = hoverIdx === i;
                  return (
                    <div
                      key={p.weekStart}
                      onMouseEnter={() => setHoverIdx(i)}
                      onMouseLeave={() => setHoverIdx(null)}
                      style={{
                        flex: 1, minWidth: 3, display: "flex", flexDirection: "column", justifyContent: "flex-end",
                        cursor: "default", position: "relative",
                      }}
                    >
                      <div style={{
                        height: repeatH, background: isHover ? "#DB2777" : "#EC4899",
                        borderTopLeftRadius: 2, borderTopRightRadius: 2,
                        transition: "background 0.15s",
                      }} />
                      <div style={{
                        height: firstH, background: isHover ? "#4338CA" : "#4F46E5",
                        transition: "background 0.15s",
                      }} />
                    </div>
                  );
                })}
                {/* Repeat-% line: SVG polyline stretched to fill. Dots are
                    rendered as HTML siblings below so (a) they stay round
                    regardless of bar width and (b) each is individually
                    hoverable without getting eaten by the <svg>. */}
                <svg
                  width="100%" height={PLOT_H}
                  preserveAspectRatio="none"
                  style={{ position: "absolute", inset: 0, pointerEvents: "none" }}
                  viewBox={`0 0 ${Math.max(1, windowed.length - 1)} 100`}
                >
                  <polyline
                    fill="none"
                    stroke={lineColor}
                    strokeOpacity={0.55}
                    strokeWidth={1.25}
                    vectorEffect="non-scaling-stroke"
                    points={windowed.map((p, i) => {
                      const total = (p.firstRev || 0) + (p.repeatRev || 0);
                      const pct = total > 0 ? (p.repeatRev / total) * 100 : 0;
                      return `${i},${100 - pct}`;
                    }).join(" ")}
                  />
                </svg>
                {/* Round, individually-hoverable dots */}
                <div style={{ position: "absolute", inset: 0, pointerEvents: "none" }}>
                  {windowed.map((p, i) => {
                    const total = (p.firstRev || 0) + (p.repeatRev || 0);
                    const pct = total > 0 ? (p.repeatRev / total) * 100 : 0;
                    const xPct = windowed.length > 1 ? (i / (windowed.length - 1)) * 100 : 50;
                    const topPx = (1 - pct / 100) * PLOT_H;
                    const isHover = hoverIdx === i;
                    return (
                      <div
                        key={p.weekStart}
                        onMouseEnter={() => setHoverIdx(i)}
                        onMouseLeave={() => setHoverIdx(null)}
                        style={{
                          position: "absolute",
                          left: `calc(${xPct}% - ${isHover ? 4 : 3}px)`,
                          top: topPx - (isHover ? 4 : 3),
                          width: isHover ? 8 : 6,
                          height: isHover ? 8 : 6,
                          borderRadius: "50%",
                          background: lineColor,
                          opacity: isHover ? 1 : 0.75,
                          boxShadow: isHover ? `0 0 0 2px ${lineColor}33` : "none",
                          pointerEvents: "auto",
                          cursor: "default",
                          transition: "width 0.1s, height 0.1s, opacity 0.1s",
                        }}
                      />
                    );
                  })}
                </div>
              </div>
              {/* X-axis labels (first / middle / last) */}
              <div style={{ display: "flex", justifyContent: "space-between", fontSize: 10, color: "#9CA3AF", paddingTop: 6 }}>
                <span>{fmtWeek(windowed[0].weekStart)}</span>
                {windowed.length > 2 && <span>{fmtWeek(windowed[Math.floor(windowed.length / 2)].weekStart)}</span>}
                <span>{fmtWeek(windowed[windowed.length - 1].weekStart)}</span>
              </div>
            </div>
            {/* Right Y-axis: repeat % */}
            <div style={{ width: 36, flexShrink: 0, display: "flex", flexDirection: "column", justifyContent: "space-between", fontSize: 10, color: lineColor, fontWeight: 600, paddingTop: 2, paddingBottom: X_AXIS_H - 2, textAlign: "right" }}>
              <span>100%</span>
              <span>50%</span>
              <span>0%</span>
            </div>
          </div>

          {/* Hover popover */}
          {hoverIdx != null && windowed[hoverIdx] && (
            <div style={{
              position: "absolute", top: 6, right: 6,
              background: "#111827", color: "#fff", borderRadius: 6,
              padding: "8px 12px", fontSize: 12, minWidth: 180,
              boxShadow: "0 6px 18px rgba(0,0,0,0.18)", pointerEvents: "none",
            }}>
              <div style={{ fontWeight: 700, marginBottom: 4 }}>Week of {fmtWeek(windowed[hoverIdx].weekStart)}</div>
              <div style={{ display: "flex", justifyContent: "space-between", gap: 12 }}>
                <span style={{ opacity: 0.8 }}>First order</span>
                <strong>{fmtMoney(windowed[hoverIdx].firstRev)}</strong>
              </div>
              <div style={{ display: "flex", justifyContent: "space-between", gap: 12 }}>
                <span style={{ opacity: 0.8 }}>Repeat orders</span>
                <strong>{fmtMoney(windowed[hoverIdx].repeatRev)}</strong>
              </div>
              <div style={{ display: "flex", justifyContent: "space-between", gap: 12, borderTop: "1px solid #374151", marginTop: 4, paddingTop: 4 }}>
                <span style={{ opacity: 0.8 }}>Total</span>
                <strong>{fmtMoney(windowed[hoverIdx].firstRev + windowed[hoverIdx].repeatRev)}</strong>
              </div>
              <div style={{ display: "flex", justifyContent: "space-between", gap: 12 }}>
                <span style={{ opacity: 0.8 }}>Customers</span>
                <strong>{windowed[hoverIdx].customers.toLocaleString()}</strong>
              </div>
              {(() => {
                const total = windowed[hoverIdx].firstRev + windowed[hoverIdx].repeatRev;
                const pct = total > 0 ? Math.round((windowed[hoverIdx].repeatRev / total) * 100) : 0;
                return (
                  <div style={{ display: "flex", justifyContent: "space-between", gap: 12, color: lineColor }}>
                    <span style={{ opacity: 0.9 }}>Repeat share</span>
                    <strong>{pct}%</strong>
                  </div>
                );
              })()}
            </div>
          )}

          {/* Legend */}
          <div style={{ display: "flex", justifyContent: "center", gap: 16, marginTop: 10, fontSize: 11, color: "#6B7280" }}>
            <span style={{ display: "inline-flex", alignItems: "center", gap: 4 }}>
              <span style={{ width: 10, height: 10, background: "#4F46E5", borderRadius: 2 }} /> First order
            </span>
            <span style={{ display: "inline-flex", alignItems: "center", gap: 4 }}>
              <span style={{ width: 10, height: 10, background: "#EC4899", borderRadius: 2 }} /> Repeat revenue
            </span>
            <span style={{ display: "inline-flex", alignItems: "center", gap: 4 }}>
              <span style={{ width: 14, height: 2, background: lineColor }} /> Repeat %
            </span>
          </div>
        </div>
        );
      })()}
    </BlockStack>
  );
}

// ═══════════════════════════════════════════════════════════════
// STYLES
// ═══════════════════════════════════════════════════════════════

const layoutStyles = `

.demo-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
@media (max-width: 700px) { .demo-grid { grid-template-columns: 1fr; } }

.segment-legend { display: flex; gap: 20px; flex-wrap: wrap; justify-content: center; margin-top: 12px; }
.segment-legend-item { display: flex; align-items: center; gap: 6px; font-size: 12px; color: #4B5563; }
.segment-legend-dot { width: 10px; height: 10px; border-radius: 50%; flex-shrink: 0; }

.toggle-group { display: inline-flex; gap: 4px; }
.toggle-btn { padding: 5px 14px; font-size: 12px; font-weight: 500; border: 1px solid var(--l-border); border-radius: var(--l-radius-pill); cursor: pointer; transition: all 0.15s; white-space: nowrap; background: var(--l-bg); color: var(--l-text); }
.toggle-btn.active { background: var(--l-accent); color: #fff; border-color: var(--l-accent); font-weight: 600; }
.toggle-btn:not(.active):hover { border-color: var(--l-accent); color: var(--l-accent-dark); }

.metric-selector { display: flex; gap: 16px; justify-content: flex-end; padding-top: 6px; }
.metric-link { font-size: 11px; font-weight: 500; color: #9CA3AF; cursor: pointer; border: none; background: none; padding: 0 0 3px 0; border-bottom: 2px solid transparent; transition: all 0.15s; letter-spacing: 0.3px; text-transform: uppercase; }
.metric-link:hover { color: #6B7280; }
.metric-link.active { color: #7C3AED; border-bottom-color: #7C3AED; font-weight: 700; }

/* Force paired tiles in the same row to render at the same height */
[data-tile-id="customerBreakdown"],
[data-tile-id="demographics"],
[data-tile-id="geography"],
[data-tile-id="customerJourney"] {
  display: flex;
  flex-direction: column;
  height: 100%;
}
[data-tile-id="customerBreakdown"] > :last-child,
[data-tile-id="demographics"] > :last-child,
[data-tile-id="geography"] > :last-child,
[data-tile-id="customerJourney"] > :last-child {
  flex: 1 1 auto;
  height: 100%;
  box-sizing: border-box;
}
`;

// ═══════════════════════════════════════════════════════════════
// MAIN COMPONENT
// ═══════════════════════════════════════════════════════════════

export default function Customers() {
  const data = useLoaderData<typeof loader>();
  const {
    rows, dailyData, prevDailyData,
    metaCount, organicCount, metaAvgLtv, organicAvgLtv,
    newCustomerCPA, metaAvgFirstOrder, organicAvgFirstOrder, aovCpaRatio,
    metaRepeatRate, organicRepeatRate, metaRepeatTotal, prevMetaRepeatTotal,
    metaRevenue, metaRevPct, totalAllRevenue,
    metaAvgOrders, organicAvgOrders,
    medianTimeTo2nd, reorderWithin90,
    paybackOrders, newInPeriod, attrNewCustomerRevenue,
    currencySymbol,
    ageBreakdown, genderBreakdown, newAgeBreakdown, newGenderBreakdown, allCustomerGenderBreakdown,
    newDemoConversions, newDemoExactCount, topCountries, topCities,
    allMetaTopCountries, allMetaTopCities, metaNewTopCountries, metaNewTopCities,
    allGeoCount, allMetaGeoCount, metaNewGeoCount,
    metaNewSingleCount, metaNewSingleOrders, metaNewSingleRevenue,
    metaNewRepeatCount, metaNewRepeatOrders, metaNewRepeatRevenue,
    metaRetargetedCount, metaRetargetedOrders, metaRetargetedRevenue,
    journeyFirstAOV, journeySecondAOV, journeyThirdAOV, journeyGapDays, journeyGap2to3Days, journeyCustomerCount,
    totalCustomerCount, totalMetaCustomerCount,
    allJourneyFirstAOV, allJourneySecondAOV, allJourneyThirdAOV, allJourneyGapDays, allJourneyGap2to3Days, allJourneyCustomerCount,
    metaFirstOrderCount, metaSecondOrderCount, metaThirdOrderCount, allFirstOrderCount, allSecondOrderCount, allThirdOrderCount,
    totalDemoConversions, totalMetaConversions, totalMetaConversionValue,
    metaNewOrdersInRange, metaNewRevenueInRange, metaNewCustomersInRange,
    metaRepeatOrdersInRange, metaRepeatRevenueInRange, metaRepeatCustomersInRange,
    metaRetargetedOrdersInRange, metaRetargetedRevenueInRange, metaRetargetedCustomersInRange,
    donutMetaNewCustomers, donutMetaNewRevenue,
    donutMetaRepeatCustomers, donutMetaRepeatRevenue,
    donutMetaRetargetedCustomers, donutMetaRetargetedRevenue,
    metaNewCount, mnAvgLtv, mnAvgOrders, mnRepeatRate, mnAvgAov,
    mnCPA, mnLtvCac, mnPaybackOrders, mnMedianTimeTo2nd, mnReorderWithin90,
    allCount, allAvgLtv, allAvgOrders, allRepeatRate, allAvgAov,
    allMedianTimeTo2nd, allReorderWithin90,
    metaAvgAov, totalMetaSpend, ltvAllMetaSpend, metaSpendByAcqMonth,
    unmatchedConversions, unmatchedRevenue,
    ltvBenchmark, ltvTile, ltvRecent, ltvMonthly, ltvCustomers,
    weeklyCohortSeries,
    prevMetaCount, prevMetaAvgFirstOrder, prevAovCpaRatio, prevNewCustomerCPA,
    prevMetaRepeatRate, prevMetaRevenue,
    prevMedianTimeTo2nd,
    totalCustomersInRange,
    prevMetaNewCustomersInRange, prevMetaRepeatCustomersInRange, prevMetaRetargetedCustomersInRange,
    prevMetaNewRevenueInRange, prevMetaRepeatRevenueInRange, prevMetaRetargetedRevenueInRange,
    prevTotalCustomersInRange,
    totalRevenueInRange, prevTotalRevenueInRange,
    matchedMetaOrdersInRange, totalOrdersInRange, prevMatchedMetaOrdersInRange, prevTotalOrdersInRange,
    unmatchedConversionsWithValue, prevUnmatchedConversions, prevUnmatchedRevenue, prevUnmatchedConversionsWithValue,
  } = data;
  const cs = currencySymbol || currencySymbolFromCode(null);
  const { aiCachedInsights, aiGeneratedAt, aiIsStale, orderExplorer, journeyReportsEnabled } = data;
  const [searchParams, setSearchParams] = useSearchParams();
  const [acqMode, setAcqMode] = useState<"customers" | "revenue">("customers");
  // Independent toggle for the "Meta Customer Breakdown by Day" stacked chart.
  const [dayMode, setDayMode] = useState<"customers" | "revenue">("customers");
  const [donutHover, setDonutHover] = useState<number | null>(null);
  const [demoScope, setDemoScope] = useState<"new" | "allMeta" | "all">("new");
  const [demoMetric, setDemoMetric] = useState<"cpa" | "roas" | "aov">("cpa");
  const [geoScope, setGeoScope] = useState<"new" | "allMeta" | "all">("new");
  const [geoMetric, setGeoMetric] = useState<"rev" | "cpa" | "roas" | "aov">("rev");
  const [journeyScope, setJourneyScope] = useState<"meta" | "all">("meta");
  const [ltvTab, setLtvTab] = useState<"meta" | "all">("meta");

  // ── LTV exploration state ─────────────────────────────────────────
  // Filters target the metaNew cohort (ltvCustomers) to answer
  // "which segments have the highest LTV?". Reset on every page load -
  // remembering filters between visits caused confusion when the page
  // first opened with stale cuts applied.
  const [ltvFilterGender, setLtvFilterGender] = useState<"All" | "male" | "female">("All");
  const [ltvFilterAges, setLtvFilterAges] = useState<string[]>([]); // empty = all
  const [ltvFilterCountry, setLtvFilterCountry] = useState<string>("All");
  const [ltvWindowPreset, setLtvWindowPreset] = useState<"lifetime" | 30 | 60 | 90 | 180 | 365>(365);
  // Gross margin % for the profit-payback calc. Initialised from the
  // merchant's saved Shop.defaultMarginPct (persisted below via a debounced
  // fetcher), falling back to 60 - a reasonable midpoint for DTC/fashion.
  // Revenue-based payback was misleading ("1 order = payback" sounds great
  // but ROAS=1 doesn't cover product cost, fulfilment, fees).
  const [marginPct, setMarginPct] = useState<number>(data.defaultMarginPct ?? 60);
  const marginFetcher = useFetcher();
  const marginSaveTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const handleMarginChange = (v: number) => {
    setMarginPct(v);
    if (marginSaveTimer.current) clearTimeout(marginSaveTimer.current);
    marginSaveTimer.current = setTimeout(() => {
      marginFetcher.submit({ actionType: "saveMargin", marginPct: String(v) }, { method: "post" });
    }, 800);
  };
  const [chartHover, setChartHover] = useState<{ month: number } | null>(null);
  // Chart horizon selector. 12m = anchor only (long-term average across all
  // fully-observed Meta-acquired customers). 6/3/1m = anchor PLUS a recent
  // overlay of customers acquired in the last N months, with projection to
  // month 12 using the anchor's historical ratio.
  const [ltvChartWindow, setLtvChartWindow] = useState<1 | 3 | 6 | 12>(12);
  // Chart sizing - track the actual rendered width of the chart wrapper so
  // the SVG viewBox can be set to match in pixel terms. This keeps internal
  // elements (text, lines, dots) at their native pixel size regardless of
  // viewport width - a fixed-size viewBox stretched into a wide container
  // would magnify everything proportionally.
  const ltvChartRef = useRef<HTMLDivElement>(null);
  const [ltvChartW, setLtvChartW] = useState<number>(900);
  useEffect(() => {
    const el = ltvChartRef.current;
    if (!el || typeof ResizeObserver === "undefined") return;
    const ro = new ResizeObserver((entries) => {
      for (const e of entries) {
        const w = Math.round(e.contentRect.width);
        if (w > 0) setLtvChartW(w);
      }
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  // Unique age brackets + countries pulled from the per-customer dataset.
  const ltvAgeOptions = useMemo(() => {
    const s = new Set<string>();
    for (const c of ltvCustomers) { if (c.age) s.add(c.age); }
    return [...s].sort();
  }, [ltvCustomers]);
  const ltvCountryOptions = useMemo(() => {
    const tally: Record<string, number> = {};
    for (const c of ltvCustomers) { if (c.country) tally[c.country] = (tally[c.country] || 0) + 1; }
    return Object.entries(tally).sort((a, b) => b[1] - a[1]).map(([k]) => k);
  }, [ltvCustomers]);

  // Filtered view + derived stats. When filters are inactive the result
  // matches the unfiltered metaNew cohort powering the existing tile.
  //
  // Chart strategy: pick a single FIXED cohort (the customers mature enough
  // to have completed the longest still-populated window) and average their
  // ltvByWindow at every window. Same customers at every point => curve is
  // guaranteed monotonically non-decreasing. This is the standard approach
  // used by Lifetimely / Polar / Triple Whale for cohort LTV curves.
  //
  // CAC strategy: when a gender or age filter narrows the cohort we
  // proportionally re-allocate Meta-New spend using the per-segment CPA
  // approximations from newGenderBreakdown / newAgeBreakdown. Country-only
  // filtering falls back to the base blended CAC (no country breakdown).
  const ltvFiltered = useMemo(() => {
    const filterActive = ltvFilterGender !== "All" || ltvFilterAges.length > 0 || ltvFilterCountry !== "All";
    let subset = ltvCustomers;
    if (filterActive) {
      subset = ltvCustomers.filter((c) => {
        if (ltvFilterGender !== "All" && c.gender !== ltvFilterGender) return false;
        if (ltvFilterAges.length > 0 && (!c.age || !ltvFilterAges.includes(c.age))) return false;
        if (ltvFilterCountry !== "All" && c.country !== ltvFilterCountry) return false;
        return true;
      });
    }
    const count = subset.length;
    const totalLtv = subset.reduce((s, c) => s + c.ltv, 0);
    const totalFirst = subset.reduce((s, c) => s + c.firstOrder, 0);
    const totalOrders = subset.reduce((s, c) => s + c.orders, 0);
    const avgLtv = count > 0 ? Math.round((totalLtv / count) * 100) / 100 : 0;
    const avgFirst = count > 0 ? Math.round((totalFirst / count) * 100) / 100 : 0;
    const repeatCount = subset.filter((c) => c.orders > 1).length;
    const repeatRate = count > 0 ? Math.round((repeatCount / count) * 100) : 0;
    const t2 = subset.map((c) => c.timeTo2nd).filter((v): v is number => v != null).sort((a, b) => a - b);
    const medT2 = t2.length > 0 ? (t2.length % 2 ? t2[(t2.length - 1) / 2] : (t2[t2.length / 2 - 1] + t2[t2.length / 2]) / 2) : null;
    // Avg customer lifetime - only customers with >1 order (single-order
    // customers have no observed tenure) AND only those acquired >=12mo
    // ago. Without that maturity filter the mean is biased low: a repeat
    // customer acquired last month with a 2nd order 3 weeks later has
    // tenureDays=21, even though their actual lifetime might be years.
    // Restricting to >=12mo-mature customers gives an honest "first year
    // observed lifetime" - the most we can defensibly call lifetime
    // without projection.
    const NOW_MS_T = Date.now();
    const TWELVE_MO_MS = 365 * 86400000;
    const matureForTenure = subset.filter((c) => {
      if (c.tenureDays == null || c.tenureDays <= 0) return false;
      if (!c.acqMonth) return false;
      const [ay, am] = c.acqMonth.split("-").map(Number);
      // End of acq month (latest possible acq day in that month)
      const acqMonthEnd = new Date(ay, am, 0).getTime();
      return (NOW_MS_T - acqMonthEnd) >= TWELVE_MO_MS;
    });
    const tenures = matureForTenure.map((c) => c.tenureDays).filter((v): v is number => v != null);
    const avgTenureDays = tenures.length > 0 ? Math.round(tenures.reduce((s, v) => s + v, 0) / tenures.length) : null;

    // Fixed-cohort chart. Walk windows long-to-short and pick the longest
    // window where ≥5 of the filtered customers are mature. That defines
    // the cohort. Then average their cumulative spend at every window
    // ≤ pivot. The same customers contribute to every point => monotonic.
    const windows = [30, 60, 90, 180, 365];
    let pivotIdx = -1;
    let fixedCohort: typeof subset = [];
    for (let i = windows.length - 1; i >= 0; i--) {
      const w = String(windows[i]);
      const mature = subset.filter((c) => c.ltvByWindow[w] !== undefined);
      if (mature.length >= 5) { pivotIdx = i; fixedCohort = mature; break; }
    }
    const byWindow: Array<{ window: number; count: number; avgLtv: number }> = [];
    if (pivotIdx >= 0 && fixedCohort.length > 0) {
      for (let i = 0; i <= pivotIdx; i++) {
        const w = windows[i];
        const wStr = String(w);
        const sum = fixedCohort.reduce((s, c) => s + (c.ltvByWindow[wStr] || 0), 0);
        byWindow.push({ window: w, count: fixedCohort.length, avgLtv: Math.round((sum / fixedCohort.length) * 100) / 100 });
      }
    }

    // Filter-aware CAC. Allocate Meta-New spend by selected segment(s) using
    // the per-segment CPA approximations from the loader. When multiple
    // segment-axes are filtered we average their CPAs (loader doesn't cross
    // gender × age, so a true cross-segment CPA isn't available).
    const baseCpa = newCustomerCPA || 0;
    let filteredCpa = baseCpa;
    if (filterActive) {
      const segCpas: number[] = [];
      if (ltvFilterGender !== "All") {
        const label = ltvFilterGender === "female" ? "Female" : ltvFilterGender === "male" ? "Male" : "Unknown";
        const seg = newGenderBreakdown.find((g: any) => g.label === label);
        if (seg && seg.conversions > 0) segCpas.push(seg.spend / seg.conversions);
      }
      if (ltvFilterAges.length > 0) {
        const segs = newAgeBreakdown.filter((a: any) => ltvFilterAges.includes(a.label));
        const sSpend = segs.reduce((s: number, a: any) => s + a.spend, 0);
        const sConv = segs.reduce((s: number, a: any) => s + a.conversions, 0);
        if (sConv > 0) segCpas.push(sSpend / sConv);
      }
      if (segCpas.length > 0) {
        filteredCpa = segCpas.reduce((s, c) => s + c, 0) / segCpas.length;
      }
      // country-only filter: keep baseCpa
    }

    return {
      filterActive, count, avgLtv, avgFirst, avgOrders: count > 0 ? Math.round((totalOrders / count) * 100) / 100 : 0,
      repeatRate, medianTimeTo2nd: medT2 != null ? Math.round(medT2) : null,
      avgTenureDays, tenuresCount: tenures.length,
      benchmarkWindows: byWindow,
      cac: Math.round(filteredCpa * 100) / 100,
    };
  }, [ltvCustomers, ltvFilterGender, ltvFilterAges, ltvFilterCountry, newCustomerCPA, newGenderBreakdown, newAgeBreakdown]);

  const tagFilter = searchParams.get("tag") || "all";
  const handleTagChange = (value: string) => {
    const params = new URLSearchParams(searchParams);
    params.set("tag", value);
    setSearchParams(params);
  };

  const filteredRows = useMemo(() => {
    if (tagFilter === "all") return rows;
    if (tagFilter === "meta") return rows.filter(r => r.tag === "Meta New" || r.tag === "Meta Retargeted");
    return rows.filter(r => r.tag === tagFilter);
  }, [rows, tagFilter]);

  const tagOptions = [
    { label: "All Customers", value: "all" },
    { label: "Meta-Acquired", value: "meta" },
    { label: "Meta New", value: "Meta New" },
    { label: "Meta Retargeted", value: "Meta Retargeted" },
    { label: "Organic", value: "Organic" },
  ];

  // Customer breakdown donut - segments sized by unique customers (default)
  // or net revenue. Matches what's rendered in the legend to the right.
  const acqSegments = useMemo(() => {
    if (acqMode === "customers") {
      return [
        { label: "Meta New", value: donutMetaNewCustomers, color: "#7C3AED" },
        { label: "Meta Repeat", value: donutMetaRepeatCustomers, color: "#0891B2" },
        { label: "Meta Retargeted", value: donutMetaRetargetedCustomers, color: "#B45309" },
      ];
    }
    return [
      { label: "Meta New", value: Math.round(donutMetaNewRevenue), color: "#7C3AED" },
      { label: "Meta Repeat", value: Math.round(donutMetaRepeatRevenue), color: "#0891B2" },
      { label: "Meta Retargeted", value: Math.round(donutMetaRetargetedRevenue), color: "#B45309" },
    ];
  }, [acqMode, donutMetaNewCustomers, donutMetaRepeatCustomers, donutMetaRetargetedCustomers, donutMetaNewRevenue, donutMetaRepeatRevenue, donutMetaRetargetedRevenue]);

  const acqTotal = acqSegments.reduce((s, seg) => s + seg.value, 0);

  const fmtOrd = (v: number | null) => {
    if (v == null) return "-";
    return v === 1 ? "1st" : v === 2 ? "2nd" : v === 3 ? "3rd" : `${v}th`;
  };

  const columns = useMemo<ColumnDef<any, any>[]>(() => [
    { accessorKey: "tag", header: "Type",
      meta: { filterType: "multi-select", description: "How this customer was acquired. Meta New = first purchased via Meta ads. Meta Retargeted = existing customer converted by Meta. Organic = no Meta attribution" },
      filterFn: "multiSelect" },
    { accessorKey: "acquisitionDate", header: "Acquired",
      meta: { description: "Date of the customer's first order" } },
    { accessorKey: "acquisitionCampaign", header: "Acquisition Campaign",
      meta: { maxWidth: "200px", filterType: "multi-select", description: "The Meta campaign that first brought this customer" },
      filterFn: "multiSelect",
      cell: ({ getValue }) => getValue() || "-" },
    { accessorKey: "acquisitionAdSet", header: "Acquisition Ad Set",
      meta: { maxWidth: "180px", description: "The Meta ad set that first brought this customer" },
      cell: ({ getValue }) => getValue() || "-" },
    { accessorKey: "totalOrders", header: "Orders",
      meta: { align: "right", description: "Total number of orders from this customer" } },
    { accessorKey: "metaOrders", header: "Meta Orders",
      meta: { align: "right", description: "Orders attributed to Meta ads" } },
    { accessorKey: "organicOrders", header: "Organic Orders",
      meta: { align: "right", description: "Orders with no Meta attribution" } },
    { accessorKey: "orderNumAtAcq", header: "Order # at Acq.",
      meta: { align: "right", description: "Which order this was for the customer at time of first purchase (e.g. 1st = brand new, 5th = long-time buyer)" },
      cell: ({ getValue }) => fmtOrd(getValue()) },
    { accessorKey: "grossRevenue", header: "Gross Revenue",
      meta: { align: "right", description: "Total revenue from all orders (before refunds)" },
      cell: ({ getValue }) => getValue() ? `${cs}${Math.round(getValue()).toLocaleString()}` : "-" },
    { accessorKey: "totalRefunded", header: "Refunded",
      meta: { align: "right", description: "Total refund amount across all orders" },
      cell: ({ getValue }) => getValue() > 0 ? `${cs}${Math.round(getValue()).toLocaleString()}` : "-" },
    { accessorKey: "netRevenue", header: "Net Revenue",
      meta: { align: "right", description: "Revenue after all refunds - the customer's true lifetime value", calc: "Gross Revenue - Refunded" },
      cell: ({ getValue }) => `${cs}${Math.round(getValue()).toLocaleString()}` },
    { accessorKey: "avgOrderValue", header: "AOV",
      meta: { align: "right", description: "Average order value for this customer", calc: "Gross Revenue / Orders" },
      cell: ({ getValue }) => getValue() ? `${cs}${Math.round(getValue()).toLocaleString()}` : "-" },
    { accessorKey: "firstOrderValue", header: "1st Order",
      meta: { align: "right", description: "Value of the customer's first order" },
      cell: ({ getValue }) => getValue() ? `${cs}${Math.round(getValue()).toLocaleString()}` : "-" },
    { accessorKey: "ltvMultiplier", header: "LTV Multiplier",
      meta: { align: "right", description: "How much more the customer has spent beyond their first order", calc: "Gross Revenue / First Order Value" },
      cell: ({ getValue }) => getValue() != null ? `${getValue()}x` : "-" },
    { accessorKey: "lastOrderDate", header: "Last Order",
      meta: { description: "Date of the customer's most recent order" } },
    { accessorKey: "daysSinceLastOrder", header: "Days Since Last",
      meta: { align: "right", description: "Days since their most recent order - high numbers may indicate churn" } },
    { accessorKey: "daysSinceAcquisition", header: "Customer Age",
      meta: { align: "right", description: "Days since first order - how long they've been a customer" },
      cell: ({ getValue }) => `${getValue()}d` },
    { accessorKey: "timeTo2ndOrder", header: "Days to 2nd Order",
      meta: { align: "right", description: "Days between their first and second purchase. Key retention indicator - shorter is better" },
      cell: ({ getValue }) => getValue() != null ? `${getValue()}d` : "-" },
    { accessorKey: "country", header: "Country",
      meta: { filterType: "multi-select", description: "Customer's billing country (from first order)" },
      filterFn: "multiSelect",
      cell: ({ getValue }) => getValue() || "-" },
    { accessorKey: "city", header: "City",
      meta: { description: "Customer's billing city (from first order)" },
      cell: ({ getValue }) => getValue() || "-" },
    { accessorKey: "topProducts", header: "Top Products",
      meta: { maxWidth: "200px", description: "Most frequently purchased products by this customer" },
      cell: ({ getValue }) => getValue() || "-" },
    { accessorKey: "discountOrders", header: "Discount Orders",
      meta: { align: "right", description: "Number of orders where a discount code was used" } },
    { accessorKey: "refundRate", header: "Refund Rate",
      meta: { align: "right", description: "Percentage of gross revenue that was refunded", calc: "Refunded / Gross Revenue x 100" },
      cell: ({ getValue }) => getValue() > 0 ? `${getValue()}%` : "-" },
    { accessorKey: "avgConfidence", header: "Avg Confidence",
      meta: { align: "right", description: "Average attribution confidence across all Meta-matched orders for this customer" },
      cell: ({ getValue }) => getValue() != null ? `${getValue()}%` : "-" },
  ], [cs]);

  const defaultVisibleColumns = useMemo(() => [
    "tag", "acquisitionDate", "totalOrders", "netRevenue",
    "avgOrderValue", "ltvMultiplier", "lastOrderDate", "daysSinceLastOrder",
  ], []);

  const columnProfiles = useMemo(() => [
    {
      id: "overview", label: "Overview", icon: "📊",
      description: "Key customer details - acquisition type, orders, revenue and recency",
      columns: ["tag", "acquisitionDate", "totalOrders", "netRevenue", "lastOrderDate", "daysSinceLastOrder"],
      fullColumns: ["tag", "acquisitionDate", "acquisitionCampaign", "totalOrders", "metaOrders", "grossRevenue", "totalRefunded", "netRevenue", "lastOrderDate", "daysSinceLastOrder"],
    },
    {
      id: "ltv", label: "Lifetime Value", icon: "💎",
      description: "Deep dive into customer value - LTV, multipliers, order frequency and retention signals",
      columns: ["tag", "totalOrders", "netRevenue", "avgOrderValue", "ltvMultiplier", "timeTo2ndOrder"],
      fullColumns: ["tag", "totalOrders", "grossRevenue", "totalRefunded", "netRevenue", "avgOrderValue", "firstOrderValue", "ltvMultiplier", "daysSinceAcquisition", "timeTo2ndOrder", "refundRate"],
    },
    {
      id: "acquisition", label: "Acquisition", icon: "🎯",
      description: "How each customer was acquired - which campaigns and ads brought them in",
      columns: ["tag", "acquisitionCampaign", "acquisitionDate", "netRevenue", "avgConfidence"],
      fullColumns: ["tag", "acquisitionCampaign", "acquisitionAdSet", "acquisitionDate", "orderNumAtAcq", "metaOrders", "organicOrders", "netRevenue", "avgConfidence"],
    },
    {
      id: "geography", label: "Geography", icon: "🌍",
      description: "Where your customers are located",
      columns: ["tag", "country", "totalOrders", "netRevenue"],
      fullColumns: ["tag", "country", "city", "totalOrders", "netRevenue", "avgOrderValue", "acquisitionCampaign"],
    },
    {
      id: "all", label: "All", icon: "📋",
      description: "Every available column",
      columns: columns.map(c => (c as any).accessorKey || (c as any).id).filter(Boolean),
    },
  ], [columns]);

  const fmtPrice = (v: number) => `${cs}${Math.round(v).toLocaleString()}`;
  const fmtCount = (v: number) => Math.round(v).toLocaleString();
  const fmtRatio = (v: number) => `${v.toFixed(1)}x`;

  // Age bar colors - gradient from light to deep purple
  const ageColors = ["#C4B5FD", "#A78BFA", "#8B5CF6", "#7C3AED", "#6D28D9", "#5B21B6", "#4C1D95"];

  // Metric computation for demographics bars
  const demoSubValue = (item: { conversions: number; spend: number; revenue: number }) => {
    if (demoMetric === "cpa") return item.conversions > 0 ? `${cs}${Math.round(item.spend / item.conversions)} CPA` : "";
    if (demoMetric === "roas") return item.spend > 0 ? `${(item.revenue / item.spend).toFixed(1)}x ROAS` : "";
    if (demoMetric === "aov") return item.conversions > 0 ? `${cs}${Math.round(item.revenue / item.conversions)} AOV` : "";
    return "";
  };

  // Metric computation for geography bars
  const geoSubValue = (item: { customers: number; revenue: number; orders: number; spend: number }) => {
    if (geoMetric === "rev") return `${cs}${Math.round(item.revenue / 1000)}k`;
    if (geoMetric === "cpa") return item.customers > 0 ? `${cs}${Math.round(item.spend / item.customers)} CPA` : "";
    if (geoMetric === "roas") return item.spend > 0 ? `${(item.revenue / item.spend).toFixed(1)}x ROAS` : "";
    if (geoMetric === "aov") return item.orders > 0 ? `${cs}${Math.round(item.revenue / item.orders)} AOV` : "";
    return "";
  };

  // Metric selector component - underlined text links
  const MetricSelector = ({ options, active, onChange }: {
    options: { value: string; label: string }[];
    active: string;
    onChange: (v: any) => void;
  }) => (
    <div className="metric-selector">
      {options.map(opt => (
        <button
          key={opt.value}
          className={`metric-link ${active === opt.value ? "active" : ""}`}
          onClick={() => onChange(opt.value)}
        >
          {opt.label}
        </button>
      ))}
    </div>
  );

  // Demographics: active data based on toggle. Three scopes:
  //   • "new"     - Meta-acquired new customers (per-Attribution metaGender)
  //   • "allMeta" - Meta's audience-level breakdown (MetaBreakdown rows)
  //   • "all"     - every customer who ordered in range, gender from
  //                 Customer.inferredGender (name-based). Only this scope
  //                 surfaces organic customers + historical ranges where
  //                 Meta has no data.
  // For "all" (All Customers) we have no age data for non-Meta customers, so
  // fall back to the Meta-only age breakdown and surface a caveat next to the
  // "AGE" header. Better to show what we have than render a blank chart.
  const activeAgeBreakdown = demoScope === "new" ? newAgeBreakdown
    : demoScope === "all" ? ageBreakdown
    : ageBreakdown;
  const activeGenderBreakdown = demoScope === "new" ? newGenderBreakdown
    : demoScope === "all" ? allCustomerGenderBreakdown
    : genderBreakdown;
  const activeDemoConversions = demoScope === "new" ? newDemoConversions
    : demoScope === "all" ? allCustomerGenderBreakdown.reduce((s, g) => s + g.conversions, 0)
    : totalDemoConversions;
  const genderTotal = activeGenderBreakdown.reduce((s: number, g: any) => s + g.conversions, 0);

  // Geography: active data based on 3-way toggle
  const activeGeoCountries = geoScope === "all" ? topCountries
    : geoScope === "new" ? metaNewTopCountries : allMetaTopCountries;
  const activeGeoCities = geoScope === "all" ? topCities
    : geoScope === "new" ? metaNewTopCities : allMetaTopCities;
  const activeGeoTotal = geoScope === "all" ? allGeoCount
    : geoScope === "new" ? metaNewGeoCount : allMetaGeoCount;
  const geoIsMeta = geoScope === "new" || geoScope === "allMeta";

  const footerRow = useMemo(() => {
    if (filteredRows.length === 0) return undefined;
    const sum = (key: string) => filteredRows.reduce((s, r) => s + (r[key] || 0), 0);
    const gross = sum("grossRevenue");
    const refunded = sum("totalRefunded");
    const net = sum("netRevenue");
    const orders = sum("totalOrders");
    const metaOrd = sum("metaOrders");
    return {
      tag: `${filteredRows.length} customers`,
      acquisitionDate: "", acquisitionCampaign: "", acquisitionAdSet: "",
      totalOrders: orders.toLocaleString(),
      metaOrders: metaOrd.toLocaleString(),
      organicOrders: (orders - metaOrd).toLocaleString(),
      orderNumAtAcq: "",
      grossRevenue: fmtPrice(gross),
      totalRefunded: refunded > 0 ? fmtPrice(refunded) : "-",
      netRevenue: fmtPrice(net),
      avgOrderValue: filteredRows.length > 0 ? fmtPrice(Math.round(gross / filteredRows.length)) : "-",
      firstOrderValue: "", ltvMultiplier: "",
      lastOrderDate: "", daysSinceLastOrder: "",
      daysSinceAcquisition: "", timeTo2ndOrder: "",
      country: "", city: "",
      topProducts: "", discountOrders: "",
      refundRate: "", avgConfidence: "",
    };
  }, [filteredRows, cs]);

  // ── Page summary bullets ──
  // Seven at-a-glance lines, tied to the currently selected date range.
  // Computed from the same pre-aggregated loader data the tiles below use -
  // no AI, no caching, no round-trips. Order is deliberate: who they are,
  // where they are, how many + how efficient, do they pay back, do they
  // come back, are they worth it, are we measuring honestly.
  const summaryBullets: SummaryBullet[] = useMemo(() => {
    const out: SummaryBullet[] = [];

    // 1) Avg age (midpoint-weighted) + gender split, new Meta only
    const AGE_MIDPOINT: Record<string, number> = {
      "13-17": 15, "18-24": 21, "25-34": 29.5, "35-44": 39.5,
      "45-54": 49.5, "55-64": 59.5, "65+": 70,
    };
    const ageConvTotal = (newAgeBreakdown || []).reduce((s: number, a: any) => s + (a.conversions || 0), 0);
    const ageWeighted = (newAgeBreakdown || []).reduce(
      (s: number, a: any) => s + (AGE_MIDPOINT[a.label] || 0) * (a.conversions || 0), 0,
    );
    const avgAge = ageConvTotal > 0 ? Math.round(ageWeighted / ageConvTotal) : null;

    const genderTot = (newGenderBreakdown || []).reduce((s: number, g: any) => s + (g.conversions || 0), 0);
    const female = (newGenderBreakdown || []).find((g: any) => g.label === "Female");
    const male = (newGenderBreakdown || []).find((g: any) => g.label === "Male");
    const femalePct = genderTot > 0 && female ? Math.round((female.conversions / genderTot) * 100) : null;
    const malePct = genderTot > 0 && male ? Math.round((male.conversions / genderTot) * 100) : null;

    if (avgAge != null || femalePct != null || malePct != null) {
      const parts: string[] = [];
      if (avgAge != null) parts.push(`Avg age ${avgAge}`);
      // Gender split - biggest first so the dominant audience leads the line.
      if (femalePct != null && malePct != null) {
        parts.push(femalePct >= malePct
          ? `${femalePct}% female / ${malePct}% male`
          : `${malePct}% male / ${femalePct}% female`);
      } else if (femalePct != null) {
        parts.push(`${femalePct}% female`);
      } else if (malePct != null) {
        parts.push(`${malePct}% male`);
      }
      out.push({
        tone: "neutral",
        text: <><strong>New Meta customers:</strong> {parts.join(" · ")}</>,
      });
    }

    // 1b) CAC split by gender. Uses newGenderBreakdown.spend, which is
    // (all-Meta avg CPA) × new conversions - same basis as the CPA chips
    // under the Gender split on the Customer Demographics tile, so numbers
    // tie out between the summary and the chart.
    {
      const femSpend = female?.spend || 0;
      const maleSpend = male?.spend || 0;
      const femConv = female?.conversions || 0;
      const maleConv = male?.conversions || 0;
      const femCpa = femConv > 0 ? Math.round(femSpend / femConv) : null;
      const maleCpa = maleConv > 0 ? Math.round(maleSpend / maleConv) : null;
      if (femCpa != null || maleCpa != null) {
        const parts: string[] = [];
        if (femCpa != null) parts.push(`${cs}${femCpa.toLocaleString()} female`);
        if (maleCpa != null) parts.push(`${cs}${maleCpa.toLocaleString()} male`);
        out.push({
          tone: "neutral",
          text: <><strong>CAC by gender:</strong> {parts.join(" · ")}</>,
        });
      }
    }

    // 2) Top country + top city - identical data source and denominator
    // as the Customer Geography tile below (metaNewTopCountries/Cities +
    // metaNewGeoCount from geoBlob, customer-count based, all-time).
    const topCountry = (metaNewTopCountries || [])[0];
    const topCity = (metaNewTopCities || [])[0];
    const geoDenom = metaNewGeoCount || 0;
    const countryPct = topCountry && geoDenom > 0 ? Math.round((topCountry.customers / geoDenom) * 100) : null;
    const cityPct = topCity && geoDenom > 0 ? Math.round((topCity.customers / geoDenom) * 100) : null;

    if (topCountry || topCity) {
      const parts: React.ReactNode[] = [];
      if (topCountry) {
        parts.push(
          <span key="country">
            Top country <strong>{topCountry.label}</strong>
            {countryPct != null ? ` (${countryPct}% of new Meta customers)` : ""}
          </span>,
        );
      }
      if (topCity) {
        parts.push(
          <span key="city">
            Top city <strong>{topCity.label}</strong>
            {cityPct != null ? ` (${cityPct}% of new Meta customers)` : ""}
          </span>,
        );
      }
      out.push({
        tone: "neutral",
        text: (
          <>
            {parts.map((p, i) => (
              <React.Fragment key={i}>
                {i > 0 ? " · " : null}
                {p}
              </React.Fragment>
            ))}
          </>
        ),
      });
    }

    // 3) Acquisition: count + WoW change + CAC direction
    if (metaCount > 0 || prevMetaCount > 0) {
      const countDelta = prevMetaCount > 0
        ? Math.round(((metaCount - prevMetaCount) / prevMetaCount) * 100)
        : null;
      const cpaDelta = prevNewCustomerCPA > 0 && newCustomerCPA > 0
        ? Math.round(((newCustomerCPA - prevNewCustomerCPA) / prevNewCustomerCPA) * 100)
        : null;
      const cpaImproved = cpaDelta != null && cpaDelta < 0;
      const cpaWorsened = cpaDelta != null && cpaDelta > 0;
      const countStr = countDelta != null
        ? `${countDelta > 0 ? "+" : ""}${countDelta}% vs previous period`
        : "first period with data";
      const cpaStr = newCustomerCPA > 0
        ? ` at ${cs}${newCustomerCPA.toLocaleString()} CAC${cpaDelta != null ? ` (${cpaDelta > 0 ? "+" : ""}${cpaDelta}%)` : ""}`
        : "";
      const tone: SummaryTone = cpaImproved && (countDelta ?? 0) >= 0 ? "positive"
        : cpaWorsened && (countDelta ?? 0) <= 0 ? "negative"
        : "neutral";
      out.push({
        tone,
        text: <><strong>Acquired {metaCount.toLocaleString()} new Meta customer{metaCount === 1 ? "" : "s"}</strong> - {countStr}{cpaStr}.</>,
      });
    }

    // 4) Payback: first-order AOV vs CAC. Fall back to all-time new-Meta
    // cohort values when the selected range has no Meta spend logged, so
    // short date ranges (that legitimately have 0 spend) still surface
    // this bullet rather than silently dropping it.
    {
      const inRangeOk = newCustomerCPA > 0 && paybackOrders > 0;
      const payback = inRangeOk ? paybackOrders : mnPaybackOrders;
      const effectiveRatio = inRangeOk
        ? aovCpaRatio
        : (mnCPA > 0 && mnPaybackOrders > 0 ? 1 / mnPaybackOrders : 0);
      const scope = inRangeOk ? "" : " (all-time)";
      if (payback > 0) {
        let tone: SummaryTone = "neutral";
        let msg: React.ReactNode;
        if (payback <= 1) {
          tone = "positive";
          msg = <>First order covers CAC ({(effectiveRatio || 0).toFixed(2)}× AOV:CAC ratio{scope}).</>;
        } else if (payback <= 2) {
          tone = "neutral";
          msg = <>Pays back in ~{payback.toFixed(1)} orders (AOV:CAC {(effectiveRatio || 0).toFixed(2)}×{scope}).</>;
        } else {
          tone = "warning";
          msg = <>Pays back in ~{payback.toFixed(1)} orders{scope} - CAC is outpacing first-order AOV.</>;
        }
        out.push({ tone, text: <><strong>Payback:</strong> {msg}</> });
      }
    }

    // 5) Returning Meta customers - absolute count of Meta-attributed
    // customers who made a repeat purchase in this period. Prior framing
    // as "repeat rate" divided returning customers by newly-acquired in
    // the same window (different cohorts) which produced nonsense
    // percentages >100% and bogus "lagging organic by N pts" tails.
    if (metaRepeatTotal > 0) {
      let tone: SummaryTone = "neutral";
      let delta: React.ReactNode = null;
      if (prevMetaRepeatTotal > 0) {
        const diff = metaRepeatTotal - prevMetaRepeatTotal;
        const pct = Math.round((diff / prevMetaRepeatTotal) * 100);
        if (pct >= 10) { tone = "positive"; delta = <> - up {pct}% vs previous period.</>; }
        else if (pct <= -10) { tone = "warning"; delta = <> - down {Math.abs(pct)}% vs previous period.</>; }
        else { delta = <> - roughly flat vs previous period.</>; }
      }
      out.push({
        tone,
        text: <><strong>Returning Meta customers:</strong> {metaRepeatTotal.toLocaleString()} came back to buy again in this period{delta}</>,
      });
    }

    // 6) LTV:CAC at the longest mature benchmark window. Falls back to
    // all-time new-Meta CAC (mnCPA) when the range's CPA is 0.
    {
      const windows = (ltvBenchmark?.meta?.windows || []) as any[];
      const hero = windows.length > 0 ? windows[windows.length - 1] : null;
      const heroLtv = hero?.avgLtv || 0;
      const heroWindow = hero?.window || 0;
      const cac = newCustomerCPA > 0 ? newCustomerCPA : mnCPA;
      const scope = newCustomerCPA > 0 ? "" : " all-time";
      const ratio = cac > 0 && heroLtv > 0
        ? Math.round((heroLtv / cac) * 100) / 100
        : 0;
      if (ratio > 0) {
        const label = heroWindow >= 365 ? `${Math.round(heroWindow / 365)}yr` : `${heroWindow}d`;
        let tone: SummaryTone = "neutral";
        let tail: React.ReactNode = "";
        if (ratio >= 3) { tone = "positive"; tail = " - healthy."; }
        else if (ratio < 2) { tone = "warning"; tail = " - below 2× threshold."; }
        out.push({
          tone,
          text: <><strong>LTV:CAC {ratio.toFixed(2)}×</strong> at {label} ({cs}{Math.round(heroLtv).toLocaleString()} LTV vs {cs}{Math.round(cac).toLocaleString()}{scope} CAC){tail}</>,
        });
      }
    }

    // 7) Attribution health - surface only if unmatched share is material
    {
      const matched = matchedMetaOrdersInRange || 0;
      const unmatched = unmatchedConversionsWithValue || 0;
      const totalMeta = matched + unmatched;
      if (totalMeta > 0 && unmatched > 0) {
        const pct = Math.round((unmatched / totalMeta) * 100);
        if (pct >= 5) {
          const tone: SummaryTone = pct >= 20 ? "warning" : "neutral";
          out.push({
            tone,
            text: <><strong>Attribution:</strong> {pct}% of Meta conversions ({unmatched.toLocaleString()}) couldn&apos;t be matched to a Shopify order - likely edited orders or refunds after purchase.</>,
          });
        }
      }
    }

    return out;
  }, [
    newAgeBreakdown, newGenderBreakdown,
    metaNewTopCountries, metaNewTopCities, metaNewGeoCount,
    metaCount, prevMetaCount, newCustomerCPA, prevNewCustomerCPA,
    paybackOrders, aovCpaRatio,
    mnCPA, mnPaybackOrders,
    metaRepeatRate, organicRepeatRate,
    ltvBenchmark,
    matchedMetaOrdersInRange, unmatchedConversionsWithValue,
    cs,
  ]);

  return (
    <Page title="Customer Intelligence" fullWidth>
      <style dangerouslySetInnerHTML={{ __html: layoutStyles }} />
      <ReportTabs>
      <BlockStack gap="500">
        {/* Hidden for V1 - bring back in V2. Loader wiring kept intact. */}
        {false && (
          <AiInsightsPanel
            pageKey="customers"
            cachedInsights={aiCachedInsights}
            generatedAt={aiGeneratedAt}
            isStale={aiIsStale}
            currencySymbol={cs}
          />
        )}
        <PageSummary scope="Customer" bullets={summaryBullets} fromKey={data.fromKey} toKey={data.toKey} preset={data.preset} />

        {/* ═══ ALL TILES (drag/drop, show/hide) ═══ */}
        <TileGrid pageId="customers-v8" columns={4} tiles={[
          { id: "customerBreakdown", label: "Meta Customer Breakdown Summary", span: 2, render: () => {
            // Plain-language read of the actual split (no invented benchmarks).
            // "Existing" = repeat (returning Meta-acquired) + retargeted.
            const newCust = donutMetaNewCustomers;
            const existingCust = donutMetaRepeatCustomers + donutMetaRetargetedCustomers;
            const newRev = donutMetaNewRevenue;
            const existingRev = donutMetaRepeatRevenue + donutMetaRetargetedRevenue;
            const totalCust = newCust + existingCust;
            const totalRev = newRev + existingRev;
            const avgNew = newCust > 0 ? newRev / newCust : 0;
            const avgExisting = existingCust > 0 ? existingRev / existingCust : 0;
            const money = (v: number) => `${cs}${Math.round(v).toLocaleString()}`;
            let analysis = "";
            if (totalCust === 0) {
              analysis = "No Meta customer activity in the selected period.";
            } else if (acqMode === "customers") {
              // Customers mode: describe the head-count split.
              const newPct = Math.round((newCust / totalCust) * 100);
              const existingPct = 100 - newPct;
              const lead = `${newPct}% of your Meta customers are new, ${existingPct}% are existing.`;
              let take = "";
              if (newPct >= 60) take = "Most of your Meta customers are first-time buyers — your spend is doing the acquisition job.";
              else if (newPct <= 35) take = "Most are existing customers being re-engaged — worth checking whether more spend could go toward winning new ones.";
              else take = "A fairly even split between winning new customers and re-engaging existing ones.";
              analysis = `${lead} ${take}`;
            } else {
              // Revenue mode: describe the revenue split AND explain it via AOV,
              // so a customer/revenue skew (e.g. 56% of buyers but 66% of revenue)
              // is surfaced rather than mislabelled as "even".
              const newPctRev = totalRev > 0 ? Math.round((newRev / totalRev) * 100) : 0;
              const existingPctRev = 100 - newPctRev;
              const lead = `${newPctRev}% of your Meta revenue comes from new customers, ${existingPctRev}% from existing ones.`;
              let take = "";
              if (avgNew > 0 && avgExisting > 0) {
                if (avgExisting >= avgNew * 1.15) take = `Your existing Meta customers spend more per head (${money(avgExisting)} vs ${money(avgNew)}), so returning buyers carry more of the revenue than their numbers suggest.`;
                else if (avgNew >= avgExisting * 1.15) take = `Your new Meta customers spend more per head (${money(avgNew)} vs ${money(avgExisting)}) — strong first-order value.`;
                else take = `New and existing Meta customers spend a similar amount per head (about ${money(avgNew)}).`;
              }
              analysis = `${lead} ${take}`.trim();
            }
            return (
          <Card>
            <BlockStack gap="300">
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start" }}>
                <div style={{ flex: 1, textAlign: "center" }}>
                  <Text as="h2" variant="headingLg">Meta Customer Breakdown Summary</Text>
                  <Text as="p" variant="bodySm" tone="subdued">
                    New, repeat, and retargeted Meta customers for the selected period
                  </Text>
                </div>
                <div className="toggle-group">
                  <button className={`toggle-btn ${acqMode === "customers" ? "active" : ""}`} onClick={() => setAcqMode("customers")}>Customers</button>
                  <button className={`toggle-btn ${acqMode === "revenue" ? "active" : ""}`} onClick={() => setAcqMode("revenue")}>Revenue</button>
                </div>
              </div>

              <div style={{ display: "flex", alignItems: "center", justifyContent: "center", gap: "32px", padding: "8px 0" }}>
                <DonutChart
                  segments={acqSegments}
                  centerValue={acqMode === "customers" ? acqTotal.toLocaleString() : `${cs}${Math.round(acqTotal).toLocaleString()}`}
                  centerLabel={acqMode === "customers" ? "Customers" : "Revenue"}
                  size={170}
                  thickness={26}
                  hovered={donutHover}
                  onHoverChange={setDonutHover}
                  formatValue={acqMode === "revenue" ? (v) => `${cs}${Math.round(v).toLocaleString()}` : undefined}
                />
                <div style={{ display: "flex", flexDirection: "column", gap: "12px" }}>
                  {[
                    { label: "Meta New", desc: "First-ever order, acquired by Meta", count: donutMetaNewCustomers, value: acqMode === "customers" ? donutMetaNewCustomers : donutMetaNewRevenue, color: "#7C3AED" },
                    { label: "Meta Repeat", desc: "Returning Meta-acquired customer (via ad or other channel)", count: donutMetaRepeatCustomers, value: acqMode === "customers" ? donutMetaRepeatCustomers : donutMetaRepeatRevenue, color: "#0891B2" },
                    { label: "Meta Retargeted", desc: "Existing customer converted by Meta", count: donutMetaRetargetedCustomers, value: acqMode === "customers" ? donutMetaRetargetedCustomers : donutMetaRetargetedRevenue, color: "#B45309" },
                  ].map((seg, i) => {
                    const isEmpty = seg.value === 0;
                    const isHovered = donutHover === i;
                    return (
                      <div
                        key={seg.label}
                        onMouseEnter={() => !isEmpty && setDonutHover(i)}
                        onMouseLeave={() => setDonutHover(null)}
                        style={{
                          display: "flex", alignItems: "center", gap: "10px",
                          opacity: isEmpty ? 0.45 : (donutHover !== null && !isHovered ? 0.55 : 1),
                          padding: "4px 6px", borderRadius: "4px",
                          background: isHovered ? "#F3F4F6" : "transparent",
                          cursor: isEmpty ? "default" : "pointer",
                          transition: "background 0.15s, opacity 0.15s",
                        }}
                      >
                        <div style={{ width: "12px", height: "12px", borderRadius: "3px", background: isEmpty ? "#D1D5DB" : seg.color, flexShrink: 0 }} />
                        <div>
                          <div style={{ fontSize: "13px", color: isEmpty ? "#9CA3AF" : "#1F2937" }}>
                            <strong>{seg.label}:</strong>{" "}
                            {acqMode === "customers"
                              ? `${seg.count.toLocaleString()} customer${seg.count !== 1 ? "s" : ""}`
                              : `${cs}${Math.round(seg.value).toLocaleString()}`}
                          </div>
                          <div style={{ fontSize: "11px", color: isEmpty ? "#D1D5DB" : "#9CA3AF" }}>
                            {seg.desc}
                          </div>
                        </div>
                      </div>
                    );
                  })}
                </div>
              </div>
              <div style={{ borderTop: "1px solid #F3F4F6", paddingTop: "12px", fontSize: "13px", color: "#4B5563", lineHeight: 1.5 }}>
                {analysis}
              </div>
            </BlockStack>
          </Card>
            );
          }},
          { id: "demographics", label: "Customer Demographics", span: 2, render: () => (
          <Card>
            <BlockStack gap="400">
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start" }}>
                <div style={{ flex: 1, textAlign: "center" }}>
                  <Text as="h2" variant="headingLg">Customer Demographics</Text>
                  <Text as="p" variant="bodySm" tone="subdued">
                    {demoScope === "new"
                      ? "All New customer Meta-reported conversions by age & gender"
                      : demoScope === "allMeta"
                        ? "All Meta-reported conversions by age & gender"
                        : "All customers (Meta + organic) - gender from name-based inference; age shown for the Meta subset only"}
                  </Text>
                </div>
                <div className="toggle-group">
                  <TipButton tip={SEGMENT_TIPS.newMetaConversions} className={`toggle-btn ${demoScope === "new" ? "active" : ""}`} onClick={() => setDemoScope("new")}>New from Meta</TipButton>
                  <TipButton tip={SEGMENT_TIPS.allMetaConversions} className={`toggle-btn ${demoScope === "allMeta" ? "active" : ""}`} onClick={() => setDemoScope("allMeta")}>All Meta</TipButton>
                  <TipButton tip={SEGMENT_TIPS.allCustomers} className={`toggle-btn ${demoScope === "all" ? "active" : ""}`} onClick={() => setDemoScope("all")}>All Customers</TipButton>
                </div>
              </div>
              <div className="demo-grid">
                {/* Age distribution */}
                <div>
                  <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: "8px", minHeight: "28px", marginBottom: "8px" }}>
                    <div style={{ display: "flex", alignItems: "baseline", gap: "8px" }}>
                      <div style={{ fontSize: "12px", fontWeight: 600, color: "#6B7280", textTransform: "uppercase", letterSpacing: "0.5px" }}>
                        Age
                      </div>
                      {demoScope === "all" && (
                        <span style={{ fontSize: "11px", color: "#DC2626", fontWeight: 500 }}>
                          (age data only available for Meta customers)
                        </span>
                      )}
                    </div>
                    {activeAgeBreakdown.length > 0 && (
                      <MetricSelector
                        options={[{ value: "cpa", label: "CPA" }, { value: "roas", label: "ROAS" }, { value: "aov", label: "AOV" }]}
                        active={demoMetric}
                        onChange={setDemoMetric}
                      />
                    )}
                  </div>
                  {activeAgeBreakdown.length > 0 ? (
                    <HBarChart
                      items={activeAgeBreakdown.map((a: any) => ({
                        label: a.label,
                        value: a.conversions,
                        subValue: demoSubValue(a),
                      }))}
                      colorFn={(i) => ageColors[i % ageColors.length]}
                      formatValue={(v) => v.toLocaleString()}
                      maxItems={10}
                    />
                  ) : (
                    <div style={{ color: "#9CA3AF", fontSize: "13px", padding: "12px 0" }}>No age data available</div>
                  )}
                </div>

                {/* Gender split */}
                <div>
                  <div style={{ display: "flex", alignItems: "center", minHeight: "28px", marginBottom: "8px" }}>
                    <div style={{ fontSize: "12px", fontWeight: 600, color: "#6B7280", textTransform: "uppercase", letterSpacing: "0.5px" }}>
                      Gender
                    </div>
                  </div>
                  {activeGenderBreakdown.length > 0 ? (
                    <div style={{ display: "flex", flexDirection: "column", gap: "10px" }}>
                      {/* Gender bar */}
                      <div style={{ display: "flex", height: "32px", borderRadius: "8px", overflow: "hidden" }}>
                        {activeGenderBreakdown.map((g: any) => {
                          const pct = genderTotal > 0 ? (g.conversions / genderTotal) * 100 : 0;
                          const color = g.label === "Female" ? "#EC4899" : g.label === "Male" ? "#3B82F6" : "#9CA3AF";
                          return (
                            <div
                              key={g.label}
                              style={{
                                width: `${pct}%`, background: color, minWidth: pct > 0 ? "2px" : 0,
                                display: "flex", alignItems: "center", justifyContent: "center",
                                transition: "width 0.5s ease",
                              }}
                            >
                              {pct > 15 && (
                                <span style={{ fontSize: "12px", fontWeight: 700, color: "#fff" }}>
                                  {Math.round(pct)}%
                                </span>
                              )}
                            </div>
                          );
                        })}
                      </div>
                      {/* Gender labels */}
                      <div style={{ display: "flex", gap: "16px", justifyContent: "center" }}>
                        {activeGenderBreakdown.map((g: any) => {
                          const color = g.label === "Female" ? "#EC4899" : g.label === "Male" ? "#3B82F6" : "#9CA3AF";
                          const pct = genderTotal > 0 ? Math.round((g.conversions / genderTotal) * 100) : 0;
                          return (
                            <div key={g.label} style={{ display: "flex", alignItems: "center", gap: "6px" }}>
                              <div style={{ width: "10px", height: "10px", borderRadius: "50%", background: color }} />
                              <span style={{ fontSize: "12px", color: "#4B5563" }}>
                                {g.label} <strong>{g.conversions.toLocaleString()}</strong> ({pct}%)
                              </span>
                            </div>
                          );
                        })}
                      </div>
                      {/* Spend comparison - CPA only meaningful when we have
                          spend, which the "all" (name-inferred) scope does not.
                          For "all" we show AOV instead so the chip stays useful. */}
                      <div style={{ display: "flex", gap: "12px", justifyContent: "center", marginTop: "4px" }}>
                        {activeGenderBreakdown.map((g: any) => {
                          const color = g.label === "Female" ? "#EC4899" : g.label === "Male" ? "#3B82F6" : "#9CA3AF";
                          const isAllScope = demoScope === "all";
                          const value = isAllScope
                            ? (g.conversions > 0 ? g.revenue / g.conversions : 0)
                            : (g.conversions > 0 ? g.spend / g.conversions : 0);
                          const chipLabel = isAllScope ? "AOV" : "CPA";
                          return (
                            <div key={g.label} style={{ textAlign: "center" }}>
                              <div style={{ fontSize: "18px", fontWeight: 700, color }}>{cs}{Math.round(value)}</div>
                              <div style={{ fontSize: "10px", color: "#9CA3AF" }}>{chipLabel} ({g.label})</div>
                            </div>
                          );
                        })}
                      </div>
                    </div>
                  ) : (
                    <div style={{ color: "#9CA3AF", fontSize: "13px", padding: "12px 0" }}>No gender data available</div>
                  )}
                </div>
              </div>
            </BlockStack>
          </Card>
          )},
          { id: "geography", label: "Customer Geography", span: 2, render: () => {
            // Independent scales: the country chart and city chart are
            // separate visualisations of different magnitudes (country totals
            // dwarf any individual city), so each one normalises against its
            // own top entry. Sharing the scale made the #1 city look tiny
            // because the top country drove the max.
            const countriesMax = Math.max(
              ...activeGeoCountries.slice(0, 10).map((c: any) => c.customers),
              1,
            );
            const citiesMax = Math.max(
              ...activeGeoCities.slice(0, 10).map((c: any) => c.customers),
              1,
            );
            return (
          <Card>
            <BlockStack gap="400">
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start" }}>
                <div style={{ flex: 1, textAlign: "center" }}>
                  <Text as="h2" variant="headingLg">Customer Geography</Text>
                  <Text as="p" variant="bodySm" tone="subdued">
                    {geoScope === "new"
                      ? "Billing address of new customers acquired via Meta ads"
                      : geoScope === "allMeta"
                        ? "Billing address of all Meta-attributed customers (acquired + retargeted)"
                        : "Billing address of all customers in this period (Meta + organic)"}
                  </Text>
                </div>
                <div className="toggle-group">
                  <TipButton tip={SEGMENT_TIPS.newFromMeta} className={`toggle-btn ${geoScope === "new" ? "active" : ""}`} onClick={() => setGeoScope("new")}>New from Meta</TipButton>
                  <TipButton tip={SEGMENT_TIPS.allMeta} className={`toggle-btn ${geoScope === "allMeta" ? "active" : ""}`} onClick={() => setGeoScope("allMeta")}>All Meta</TipButton>
                  <TipButton tip={SEGMENT_TIPS.allCustomers} className={`toggle-btn ${geoScope === "all" ? "active" : ""}`} onClick={() => { setGeoScope("all"); if (geoMetric === "cpa" || geoMetric === "roas") setGeoMetric("rev"); }}>All Customers</TipButton>
                </div>
              </div>
              <div className="demo-grid">
                {/* Countries */}
                <div>
                  <div style={{ display: "flex", alignItems: "center", minHeight: "28px", marginBottom: "8px" }}>
                    <div style={{ fontSize: "12px", fontWeight: 600, color: "#6B7280", textTransform: "uppercase", letterSpacing: "0.5px" }}>
                      Top Countries
                    </div>
                  </div>
                  {activeGeoCountries.length > 0 ? (
                    <HBarChart
                      items={activeGeoCountries.map((c: any) => ({
                        label: c.label, value: c.customers,
                        subValue: geoSubValue(c),
                      }))}
                      total={activeGeoTotal}
                      sharedMax={countriesMax}
                      maxItems={10}
                      maxVisible={6}
                      colorFn={(i) => ["#10B981", "#34D399", "#6EE7B7", "#A7F3D0", "#D1FAE5", "#ECFDF5", "#10B981", "#34D399", "#6EE7B7", "#A7F3D0"][i] || "#D1FAE5"}
                      formatValue={(v) => v.toLocaleString()}
                    />
                  ) : (
                    <div style={{ color: "#9CA3AF", fontSize: "13px", padding: "12px 0" }}>No country data</div>
                  )}
                </div>

                {/* Cities */}
                <div>
                  <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: "8px", minHeight: "28px", marginBottom: "8px" }}>
                    <div style={{ fontSize: "12px", fontWeight: 600, color: "#6B7280", textTransform: "uppercase", letterSpacing: "0.5px" }}>
                      Top Cities
                    </div>
                    <MetricSelector
                      options={[
                        { value: "rev", label: "Revenue" },
                        { value: "aov", label: "AOV" },
                        ...(geoIsMeta ? [{ value: "cpa", label: "CPA" }, { value: "roas", label: "ROAS" }] : []),
                      ]}
                      active={geoMetric}
                      onChange={setGeoMetric}
                    />
                  </div>
                  {activeGeoCities.length > 0 ? (
                    <HBarChart
                      items={activeGeoCities.map((c: any) => ({
                        label: c.label.length > 12 ? c.label.slice(0, 11) + "…" : c.label,
                        value: c.customers,
                        subValue: geoMetric === "aov" && c.orders > 0 ? `${cs}${Math.round(c.revenue / c.orders)} AOV` : `${cs}${Math.round(c.revenue / 1000)}k`,
                      }))}
                      total={activeGeoTotal}
                      sharedMax={citiesMax}
                      maxItems={10}
                      maxVisible={6}
                      colorFn={(i) => ["#0EA5E9", "#38BDF8", "#7DD3FC", "#BAE6FD", "#E0F2FE", "#F0F9FF", "#0EA5E9", "#38BDF8", "#7DD3FC", "#BAE6FD"][i] || "#BAE6FD"}
                      formatValue={(v) => v.toLocaleString()}
                    />
                  ) : (
                    <div style={{ color: "#9CA3AF", fontSize: "13px", padding: "12px 0" }}>No city data</div>
                  )}
                </div>
              </div>
            </BlockStack>
          </Card>
            );
          }},
          { id: "customerJourney", label: "New Customer Journey", span: 2, render: () => (
          <Card>
            <BlockStack gap="300">
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                <Text as="h2" variant="headingLg">New Customer Journey</Text>
                <div className="toggle-group">
                  <TipButton tip={SEGMENT_TIPS.metaCustomers} className={`toggle-btn ${journeyScope === "meta" ? "active" : ""}`} onClick={() => setJourneyScope("meta")}>Meta Customers</TipButton>
                  <TipButton tip={SEGMENT_TIPS.allCustomers} className={`toggle-btn ${journeyScope === "all" ? "active" : ""}`} onClick={() => setJourneyScope("all")}>All Customers</TipButton>
                </div>
              </div>
              <Text as="p" variant="bodySm" tone="subdued">
                {journeyScope === "meta"
                  ? "New customers acquired via Meta in this period - did they come back?"
                  : "All new customers acquired in this period - did they come back?"}
              </Text>
              <JourneyFlow
                firstAOV={journeyScope === "meta" ? journeyFirstAOV : allJourneyFirstAOV}
                gapDays={journeyScope === "meta" ? journeyGapDays : allJourneyGapDays}
                secondAOV={journeyScope === "meta" ? journeySecondAOV : allJourneySecondAOV}
                thirdAOV={journeyScope === "meta" ? journeyThirdAOV : allJourneyThirdAOV}
                gap2to3Days={journeyScope === "meta" ? journeyGap2to3Days : allJourneyGap2to3Days}
                customerCount={journeyScope === "meta" ? journeyCustomerCount : allJourneyCustomerCount}
                firstOrderCount={journeyScope === "meta" ? metaFirstOrderCount : allFirstOrderCount}
                secondOrderCount={journeyScope === "meta" ? metaSecondOrderCount : allSecondOrderCount}
                thirdOrderCount={journeyScope === "meta" ? metaThirdOrderCount : allThirdOrderCount}
                cs={cs}
              />
              {/* LONG-TERM REPEAT RATE - the mature benchmark. The flow
                  above is "repeats SO FAR" for this period's new customers
                  (young cohorts drag it down); this is the settled rate for
                  customers with a full year of history. From the rollup's
                  365-day maturity window (ltvBenchmark), both scopes. */}
              {(() => {
                const wins = (journeyScope === "meta" ? ltvBenchmark?.meta?.windows : ltvBenchmark?.all?.windows) || [];
                const w365 = (wins as any[]).find((w) => w.window === 365);
                if (w365 && w365.count >= 20) {
                  return (
                    <div style={{ padding: "12px 16px", background: "#F9FAFB", border: "1px solid #E5E7EB", borderRadius: 10, display: "flex", alignItems: "center", gap: 12, flexWrap: "wrap" }}>
                      <div style={{ fontSize: 11, fontWeight: 700, color: "#6366F1", textTransform: "uppercase", letterSpacing: 0.5 }}>Long-term repeat rate</div>
                      <div style={{ fontSize: 22, fontWeight: 800, color: "#1F2937", lineHeight: 1 }}>{w365.repeatRate}%</div>
                      <div style={{ fontSize: 12, color: "#4B5563", lineHeight: 1.5, flex: "1 1 260px" }}>
                        of {journeyScope === "meta" ? "Meta" : "all"} customers acquired 12+ months ago placed a 2nd order within their first year ({w365.count.toLocaleString()} customers).
                        The flow above counts repeats <em>so far</em> for this period&apos;s new customers - recent joiners haven&apos;t had time yet, so this settled rate is the benchmark to aim for.
                      </div>
                    </div>
                  );
                }
                return (
                  <div style={{ padding: "12px 16px", background: "#F9FAFB", border: "1px solid #E5E7EB", borderRadius: 10, opacity: 0.6 }}>
                    <div style={{ fontSize: 11, fontWeight: 700, color: "#6B7280", textTransform: "uppercase", letterSpacing: 0.5, marginBottom: 4 }}>Long-term repeat rate</div>
                    <div style={{ fontSize: 12, color: "#6B7280", lineHeight: 1.5 }}>
                      Not enough {journeyScope === "meta" ? "Meta" : ""} customers with 12+ months of history yet. This benchmark lights up as your data matures{journeyScope === "meta" ? " - the All Customers view usually gets there first, since Meta history starts at install" : ""}.
                    </div>
                  </div>
                );
              })()}
            </BlockStack>
          </Card>
          )},
          { id: "metaBreakdownByDay", label: "Meta Customer Breakdown by Day", span: 4, render: () => {
            const dayColors = { mn: "#7C3AED", mr: "#0891B2", mrt: "#B45309", mu: "#6B7280" };
            const dayTips = {
              mn: "Meta New — customers whose first-ever order was attributed to a Meta ad (matched, or confirmed via UTM).",
              mr: "Meta Repeat — later orders placed by customers we originally acquired through Meta, on any channel.",
              mrt: "Meta Retargeted — existing customers re-engaged by a Meta ad before ordering again.",
              mu: "Meta Unidentified — Meta-reported conversions we couldn't tie to a specific Shopify order (e.g. the order value shifted after purchase). Counted in your Meta totals but not matched to one order.",
            };
            const daySeries = dayMode === "customers"
              ? [
                  { key: "mnCust", label: "Meta New", color: dayColors.mn, tip: dayTips.mn },
                  { key: "mrCust", label: "Meta Repeat", color: dayColors.mr, tip: dayTips.mr },
                  { key: "mrtCust", label: "Meta Retargeted", color: dayColors.mrt, tip: dayTips.mrt },
                  { key: "muCust", label: "Meta Unidentified", color: dayColors.mu, tip: dayTips.mu },
                ]
              : [
                  { key: "mnRev", label: "Meta New", color: dayColors.mn, tip: dayTips.mn },
                  { key: "mrRev", label: "Meta Repeat", color: dayColors.mr, tip: dayTips.mr },
                  { key: "mrtRev", label: "Meta Retargeted", color: dayColors.mrt, tip: dayTips.mrt },
                  { key: "muRev", label: "Meta Unidentified", color: dayColors.mu, tip: dayTips.mu },
                ];
            return (
            <Card>
              <BlockStack gap="300">
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", gap: 8, flexWrap: "wrap" }}>
                  <div>
                    <Text as="h2" variant="headingLg">Meta Customer Breakdown by Day</Text>
                    <Text as="p" variant="bodySm" tone="subdued">
                      New, repeat, retargeted, and unidentified Meta customers per day for the selected period
                    </Text>
                  </div>
                  <div className="toggle-group">
                    <button className={`toggle-btn ${dayMode === "customers" ? "active" : ""}`} onClick={() => setDayMode("customers")}>Customers</button>
                    <button className={`toggle-btn ${dayMode === "revenue" ? "active" : ""}`} onClick={() => setDayMode("revenue")}>Revenue</button>
                  </div>
                </div>
                <StackedBarChart
                  data={dailyData}
                  series={daySeries}
                  formatValue={dayMode === "revenue" ? (v) => `${cs}${Math.round(v).toLocaleString()}` : (v) => Math.round(v).toLocaleString()}
                  formatAxis={dayMode === "revenue"
                    ? (v) => v >= 1000 ? `${cs}${(v / 1000).toFixed(v % 1000 === 0 ? 0 : 1)}k` : `${cs}${Math.round(v)}`
                    : (v) => Math.round(v).toLocaleString()}
                />
              </BlockStack>
            </Card>
            );
          }},
          { id: "totalMetaCustomers", label: "Total Meta Orders", render: () => {
            // Total Meta Orders = matched Shopify orders attributed to Meta
            // (rollup .orders already excludes £0) + unmatched Meta conversions
            // that had a non-zero value (£0 unmatched are typically replacement
            // / customer-service events, not real sales).
            const totalMetaOrders = matchedMetaOrdersInRange + unmatchedConversionsWithValue;
            const prevTotalMetaOrders = prevMatchedMetaOrdersInRange + prevUnmatchedConversionsWithValue;
            const pct = totalOrdersInRange > 0 ? Math.round((totalMetaOrders / totalOrdersInRange) * 100) : 0;
            return (
              <SummaryTile label="Total Meta Orders" value={totalMetaOrders.toLocaleString()}
                tooltip={{ definition: "Meta-attributed orders with a non-zero value in the selected period: matched Shopify orders plus unmatched Meta conversions" }}
                subtitle={`${pct}% of total website orders (${totalOrdersInRange.toLocaleString()})`}
                currentValue={totalMetaOrders} previousValue={prevTotalMetaOrders}
                chartData={dailyData} prevChartData={prevDailyData} chartKey="metaCustomers" chartColor="#7C3AED" chartFormat={fmtCount} />
            );
          }},
          { id: "totalMetaRevenue", label: "Revenue from Meta Customers", render: () => {
            const matchedRevenue = metaNewRevenueInRange + metaRepeatRevenueInRange + metaRetargetedRevenueInRange;
            const totalMetaRevenue = matchedRevenue + unmatchedRevenue;
            const prevTotalMetaRevenue = prevMetaNewRevenueInRange + prevMetaRepeatRevenueInRange + prevMetaRetargetedRevenueInRange + prevUnmatchedRevenue;
            const pct = totalRevenueInRange > 0 ? Math.round((totalMetaRevenue / totalRevenueInRange) * 100) : 0;
            return (
              <SummaryTile label="Revenue from Meta Customers"
                tooltip={{ definition: "Total revenue from all customers originally acquired via Meta ads (includes their repeat orders on any channel, not just the ad-attributed order). Compare to Ad Campaigns 'Meta Ad Revenue' which counts only orders directly matched to an ad." }}
                value={fmtPrice(totalMetaRevenue)}
                subtitle={`${pct}% of all website revenue (${fmtPrice(totalRevenueInRange)})`}
                currentValue={totalMetaRevenue} previousValue={prevTotalMetaRevenue}
                chartData={dailyData} prevChartData={prevDailyData} chartKey="metaRevenue" chartColor="#5C6AC4" chartFormat={fmtPrice} />
            );
          }},
          { id: "newMetaCustomers", label: "New Meta Customers", render: () => (
            <SummaryTile label="New Meta Customers" value={newInPeriod.toLocaleString()}
              tooltip={{ definition: "New customer orders attributed to Meta ads in the selected period (same source as Ad Campaigns tab: DailyAdRollup newCustomerOrders)" }}
              subtitle={`${newInPeriod + organicCount > 0 ? Math.round((newInPeriod / (newInPeriod + organicCount)) * 100) : 0}% of all new customers in period`}
              currentValue={newInPeriod} previousValue={prevMetaCount}
              chartData={dailyData} prevChartData={prevDailyData} chartKey="newMetaCustomers" chartColor="#2E7D32" chartFormat={fmtCount} />
          )},
          { id: "newMetaRevenue", label: "New Meta Customer Revenue", render: () => {
            const matchedRevenue = metaNewRevenueInRange + metaRepeatRevenueInRange + metaRetargetedRevenueInRange;
            const totalMetaRevenue = matchedRevenue + unmatchedRevenue;
            const newRevPct = totalMetaRevenue > 0 ? Math.round((attrNewCustomerRevenue / totalMetaRevenue) * 100) : 0;
            return (
              <SummaryTile label="New Meta Customer Revenue"
                tooltip={{ definition: "Net revenue from first-time Meta-acquired customers in the selected period (attribution-based, same source as Ad Campaigns tab)" }}
                value={fmtPrice(attrNewCustomerRevenue)}
                subtitle={`${newRevPct}% of all Meta revenue (${fmtPrice(totalMetaRevenue)})`}
                currentValue={attrNewCustomerRevenue} previousValue={prevMetaNewRevenueInRange}
                chartData={dailyData} prevChartData={prevDailyData} chartKey="newMetaRevenue" chartColor="#D4760A" chartFormat={fmtPrice} />
            );
          }},
          { id: "metaAov", label: "New Meta Customer AOV", render: () => (
            <SummaryTile label="New Meta Customer AOV"
              tooltip={{ definition: "Average first order value for customers acquired through Meta ads within the selected date range" }}
              value={metaAvgFirstOrder > 0 ? fmtPrice(metaAvgFirstOrder) : "\u2014"}
              subtitle={`vs ${fmtPrice(allAvgAov)} all website customers`}
              currentValue={metaAvgFirstOrder} previousValue={prevMetaAvgFirstOrder}
              chartData={dailyData} prevChartData={prevDailyData} chartKey={(d) => d.newMetaCustomers > 0 ? d.newMetaRevenue / d.newMetaCustomers : 0}
              chartColor="#2E7D32" chartFormat={fmtPrice} />
          )},
          { id: "aovCpa", label: "First Order ROI", render: () => (
            <SummaryTile label="First Order ROI"
              tooltip={{ definition: "Does the first order cover the cost of acquiring the customer? Above 1x = break even on first purchase. Compare to Ad Campaigns 'New Customer ROAS' which uses total new customer revenue (may include same-day repeat orders).", calc: "First order AOV ÷ CPA" }}
              value={aovCpaRatio > 0 ? `${aovCpaRatio}x` : "\u2014"}
              subtitle={aovCpaRatio > 0 ? undefined : "Need spend + customer data"}
              currentValue={aovCpaRatio} previousValue={prevAovCpaRatio}
              chartData={dailyData} prevChartData={prevDailyData} chartKey={(d) => {
                if (d.newMetaCustomers === 0 || d.spend === 0) return 0;
                const aov = d.newMetaRevenue / d.newMetaCustomers;
                const cpa = d.spend / d.newMetaCustomers;
                return cpa > 0 ? aov / cpa : 0;
              }} chartColor="#D4760A" chartFormat={fmtRatio} />
          )},
          { id: "repeatCustomers", label: "Meta Repeat Customers", render: () => (
            <SummaryTile label="Meta Repeat Customers" value={metaRepeatTotal.toLocaleString()}
              tooltip={{ definition: "Meta-acquired customers who placed a repeat order (any channel) in this period. A repeat order is any order after their first-ever purchase." }}
              subtitle={`${totalCustomersInRange > 0 ? Math.round((metaRepeatTotal / totalCustomersInRange) * 100) : 0}% of total website customers`}
              currentValue={metaRepeatTotal} previousValue={prevMetaRepeatTotal}
              chartData={dailyData} prevChartData={prevDailyData} chartKey="metaRepeatCustomers"
              chartColor="#0891B2" chartFormat={fmtCount} />
          )},
          { id: "newCustCpa", label: "Meta CPA", render: () => (
            <SummaryTile label="Meta CPA"
              tooltip={{ definition: "Cost to acquire one new customer through Meta within the selected date range", calc: "Meta spend in period ÷ new Meta customers in period" }}
              value={newCustomerCPA > 0 ? fmtPrice(newCustomerCPA) : "\u2014"}
              lowerIsBetter
              currentValue={newCustomerCPA} previousValue={prevNewCustomerCPA}
              subtitle={paybackOrders > 0 ? `Payback in ${paybackOrders} orders` : undefined}
              chartData={dailyData} prevChartData={prevDailyData} chartKey={(d) => d.newMetaCustomers > 0 && d.spend > 0 ? d.spend / d.newMetaCustomers : 0}
              chartColor="#DC2626" chartFormat={fmtPrice} />
          )},
          { id: "ltvOverview", label: "Meta Customer Lifetime Value Explorer", span: 4, render: () => (
          <Card>
            <BlockStack gap="400">
              {/* 3-col header: spacer | centered title | toggle pinned right.
                  Spacer keeps the title visually centered in the card while
                  the toggle stays right-aligned. */}
              <div style={{ display: "flex", alignItems: "center" }}>
                <div style={{ flex: 1 }} />
                <div style={{ flex: 1, textAlign: "center" }}>
                  <Text as="h2" variant="headingLg">{ltvTab === "meta" ? "Meta Customer Lifetime Value Explorer" : "All Customer Lifetime Value Explorer"}</Text>
                  <Text as="p" variant="bodySm" tone="subdued">
                    Cohort analysis across all matured customers - independent of the date range selector at the top of the page.
                  </Text>
                </div>
                <div style={{ flex: 1, display: "flex", justifyContent: "flex-end" }}>
                  <div className="toggle-group">
                    <TipButton tip={SEGMENT_TIPS.metaCustomers} className={`toggle-btn ${ltvTab === "meta" ? "active" : ""}`} onClick={() => setLtvTab("meta")}>Meta Customers</TipButton>
                    <TipButton tip={SEGMENT_TIPS.allCustomers} className={`toggle-btn ${ltvTab === "all" ? "active" : ""}`} onClick={() => setLtvTab("all")}>All Customers</TipButton>
                  </div>
                </div>
              </div>
              {(() => {
                const isMeta = ltvTab === "meta";
                const tile = isMeta ? ltvTile?.meta : ltvTile?.all;
                const baseCount = tile?.count || 0;
                const baseAvgAov = tile?.avgAov || 0;
                const baseAvgOrds = tile?.avgOrders || 0;
                const baseRepeatRate = tile?.repeatRate || 0;
                const baseMedT2 = tile?.medianTimeTo2nd ?? null;
                // Filter-aware CAC on the Meta tab. When the user narrows
                // the cohort by gender/age, ltvFiltered.cac re-allocates
                // Meta-New spend so Payback and LTV:CAC actually move.
                const cac = isMeta
                  ? (ltvFiltered.filterActive ? ltvFiltered.cac : ((tile as any)?.cpa || 0))
                  : 0;
                const baseBenchmark = isMeta ? ltvBenchmark?.meta : ltvBenchmark?.all;
                const baseBenchmarkWindows = baseBenchmark?.windows || [];

                // When filters are active on the Meta tab, source all stats
                // from the filtered subset. Otherwise use the pre-computed
                // cohort stats from the blob (faster, no recompute).
                const useFiltered = isMeta && ltvFiltered.filterActive;
                const count = useFiltered ? ltvFiltered.count : baseCount;
                const avgAov = useFiltered && ltvFiltered.avgFirst > 0 ? ltvFiltered.avgFirst : baseAvgAov;
                const avgOrds = useFiltered ? ltvFiltered.avgOrders : baseAvgOrds;
                const repeatRate = useFiltered ? ltvFiltered.repeatRate : baseRepeatRate;
                const medT2 = useFiltered ? ltvFiltered.medianTimeTo2nd : baseMedT2;
                // Always use ltvFiltered.benchmarkWindows - it computes a
                // FIXED COHORT (same customers at every window) so the curve
                // is monotonic and the 30d/180d/365d points are directly
                // comparable. The legacy baseBenchmarkWindows used a
                // different cohort per window (newer customers in 30d, only
                // the oldest in 365d), which produced misleading jumps like
                // 180d→365d going £895→£1,348. Fall back to base if the
                // fixed cohort is too small (<5 mature).
                const benchmarkWindows = ltvFiltered.benchmarkWindows.length > 0
                  ? ltvFiltered.benchmarkWindows
                  : baseBenchmarkWindows;

                // Hero LTV respects the window preset. "Lifetime" = true
                // realised cumulative spend per customer (sum of c.ltv over
                // the cohort) - uncapped by any maturity window. Numeric
                // presets pick that specific window from benchmarkWindows.
                const preset = ltvWindowPreset;
                const isLifetime = preset === "lifetime";
                const heroEntry = isLifetime
                  ? null
                  : (benchmarkWindows.length > 0
                      ? (benchmarkWindows.find((w: any) => w.window === preset) || benchmarkWindows[benchmarkWindows.length - 1])
                      : null);
                const lifetimeAvg = useFiltered ? ltvFiltered.avgLtv : (tile?.avgLtv || 0);
                const lifetimeCount = useFiltered ? ltvFiltered.count : (tile?.count || 0);
                const heroLtv = isLifetime
                  ? lifetimeAvg
                  : (heroEntry ? heroEntry.avgLtv : lifetimeAvg);
                const heroLabel = isLifetime
                  ? "Lifetime"
                  : (heroEntry ? `${heroEntry.window >= 365 ? "1yr" : heroEntry.window + "d"} LTV` : "Avg LTV");
                const heroCount = isLifetime
                  ? lifetimeCount
                  : (heroEntry ? heroEntry.count : count);
                const ltvCacRatio = cac > 0 ? Math.round(heroLtv / cac * 100) / 100 : 0;
                const windowLabel = (d: number) => d >= 365 ? `${Math.round(d / 365)}yr` : `${d}d`;

                // Profit-payback: CAC recovered through first-order gross
                // profit (AOV × margin%), not first-order revenue. Falls
                // back to revenue-based payback if margin is 0.
                const marginFrac = marginPct / 100;
                const grossPerOrder = avgAov * (marginFrac > 0 ? marginFrac : 1);
                const profitPaybackOrders = grossPerOrder > 0 && cac > 0
                  ? Math.round((cac / grossPerOrder) * 100) / 100 : 0;
                const payback = profitPaybackOrders;
                const paybackDays = payback > 0 && medT2 != null
                  ? (payback <= 1 ? 0 : Math.round(medT2 * (payback - 1)))
                  : null;
                const ratioColor = ltvCacRatio >= 3 ? "#059669" : ltvCacRatio >= 2 ? "#1F2937" : ltvCacRatio >= 1 ? "#D97706" : "#DC2626";
                const ratioBlurb = ltvCacRatio >= 3 ? `Healthy - every ${cs}1 of ad spend returns ${cs}${ltvCacRatio.toFixed(2)} over ${heroLabel.replace(" LTV", "")}`
                  : ltvCacRatio >= 2 ? `On track - ${cs}1 spent returns ${cs}${ltvCacRatio.toFixed(2)}`
                  : ltvCacRatio >= 1 ? "Thin margin - lift repeat rate or lower CAC"
                  : ltvCacRatio > 0 ? "Unprofitable - CAC is outpacing LTV"
                  : "Not enough mature customers yet";

                return (
                  <div>
                    {/* Hero tiles, secondary 5-tile strip, and the
                        Window/Gender/Age/Country/Margin filter row used to
                        live here. They've all been pulled INSIDE the chart
                        block so the chart owns its own controls and stat
                        tiles, and the per-cohort CAC/Payback figures move
                        coherently with the 12/6/3/1 selection. */}
                    {(benchmarkWindows.length > 0 || (isMeta ? ltvMonthly?.meta : ltvMonthly?.all)?.rows?.length > 0) && (() => {
                      const monthlyDataObj = isMeta ? ltvMonthly?.meta : ltvMonthly?.all;
                      const allMonthlyRows = monthlyDataObj?.rows || [];
                      // Cap the cohort view to the most recent 12 cohort rows
                      // and 12 month columns - anything older/longer is
                      // diminishing-returns noise for merchant decisions.
                      const monthlyRows = allMonthlyRows.slice(-12);
                      const maxMonthCol = Math.min(monthlyDataObj?.maxMonth || 0, 12);
                      return (
                        <div style={{ borderTop: "1px solid #E5E7EB", paddingTop: 20 }}>
                          {(() => {
                            // Anchor + recent-overlay LTV progression.
                            //
                            // ANCHOR: long-term average across every Meta-New
                            // customer with at least 12 fully-observed months
                            // (acqDaysAgo >= 365). At each x=m we average
                            // their cumulative net spend at end-of-month-m.
                            // Always rendered as the muted base line.
                            //
                            // OVERLAY (when window != 12): customers acquired
                            // in the last N months (window) plotted over
                            // months 0..N. At each x=m we average across
                            // customers who reached month m, so the overlay
                            // tapers gracefully where sample size shrinks.
                            //
                            // PROJECTION: extends the overlay's last point
                            // to month 12 using the anchor's historical
                            // multiplier (anchor[12] / anchor[N]). Dotted.
                            //
                            // x=0 is acquisition (£0). x=m corresponds to
                            // ltvByMonth[m-1] (cumulative through end of
                            // 30-day month m). Server caps at 13 months.
                            //
                            // "all" tab: ltvCustomers is metaNew-only, so
                            // we fall back to the pre-existing rollup-based
                            // chart for non-meta. Most decision value here
                            // is on the Meta tab.
                            const targetM: number = ltvChartWindow;
                            const MAX_MONTHS = 12;
                            // Margin slider (0–90%) drives the payback
                            // calculation. Payback fires when cumulative
                            // gross profit per customer (LTV × margin)
                            // crosses CAC, projected onto the day axis.
                            const marginFrac = Math.max(0, Math.min(1, marginPct / 100));

                            // ── Build anchor + overlay from per-customer ──
                            // ltvByMonth (Meta tab only). For "all" tab we
                            // gracefully degrade to a single-line chart
                            // built from cohort rollups.
                            type Series = Array<{ month: number; avgLtv: number; n: number }>;
                            const buildSeriesFromCusts = (
                              custs: Array<{ ltvByMonth?: number[] }>,
                              upTo: number
                            ): Series => {
                              const out: Series = [{ month: 0, avgLtv: 0, n: custs.length }];
                              for (let m = 1; m <= upTo; m++) {
                                let sum = 0, n = 0;
                                for (const c of custs) {
                                  const v = c.ltvByMonth?.[m - 1];
                                  if (v != null) { sum += v; n++; }
                                }
                                if (n > 0) out.push({ month: m, avgLtv: sum / n, n });
                              }
                              return out;
                            };

                            const DAY_MS_C = 86400000;

                            // Per-cohort CAC. Each customer's effective CAC
                            // is the CPA of their acquisition month
                            // (monthSpend ÷ Meta-new acquired that month,
                            // taken across the FULL Meta-new population
                            // since spend isn't gendered). The cohort CAC
                            // is the mean of those per-customer CPAs - so
                            // 12m / 6m / 3m / 1m display different CACs
                            // when the underlying acquisition months differ.
                            const _allMetaCusts = (ltvCustomers || []) as Array<{ acqMonth?: string | null }>;
                            const _allMetaByMonth: Record<string, number> = {};
                            for (const c of _allMetaCusts) {
                              if (!c.acqMonth) continue;
                              _allMetaByMonth[c.acqMonth] = (_allMetaByMonth[c.acqMonth] || 0) + 1;
                            }
                            const cohortCAC = (cohort: Array<{ acqMonth?: string | null }>): number => {
                              if (!cohort || cohort.length === 0) return 0;
                              let weighted = 0, total = 0;
                              for (const c of cohort) {
                                if (!c.acqMonth) continue;
                                const allCount = _allMetaByMonth[c.acqMonth] || 0;
                                if (allCount === 0) continue;
                                const spend = (metaSpendByAcqMonth || {})[c.acqMonth] || 0;
                                if (spend <= 0) continue;
                                weighted += spend / allCount;
                                total += 1;
                              }
                              return total > 0 ? weighted / total : 0;
                            };

                            // Anchor: rolling 12-24m slice (365 <= daysSince
                            // < 730). Bounded above so a 3-year-old shop's
                            // ancient customers don't dilute the late-stage
                            // growth shape used to project recent cohorts.
                            // BOOSTED with 10-11m customers whose missing m11/
                            // m12 is projected from the core's late-stage
                            // ratio - the smallest projection in the system
                            // and the only one that closes the "new store has
                            // no anchor" gap. Boost auto-retires as those
                            // customers cross day 365.
                            let anchorSeries: Series = [];
                            let anchorN = 0;
                            let anchorCoreN = 0;
                            let anchorProjectedN = 0;
                            // Overlay: customers acquired within last
                            // (window) months. Plotted 0..window.
                            let overlaySeries: Series = [];
                            let overlayN = 0;
                            // For "all" tab fallback only:
                            let fallbackSeries: Series = [];
                            // Per-cohort CAC (overrides outer-scope `cac`
                            // for the chart). Anchor CAC in 12m view;
                            // recent-cohort CAC in 6/3/1.
                            let chartCAC = 0;

                            // Meta path: per-customer metaNew records with
                            // gender filter. Always built so the Y-axis
                            // ceiling stays stable when toggling Meta ↔
                            // All Customers (the larger of the two anchor
                            // values wins regardless of which tab is shown).
                            const sourceCustsRaw = ltvCustomers as Array<{ ltvByMonth?: number[]; acqDaysAgo?: number; acqMonth: string; gender?: string | null }>;
                            const sourceCusts = ltvFilterGender !== "All"
                              ? sourceCustsRaw.filter((c) => c.gender === ltvFilterGender)
                              : sourceCustsRaw;
                            const NOW_DAYS = Date.now();
                            // Resolve days-since-acquisition. Prefer the
                            // exact acqDaysAgo from the rollup; fall back
                            // to deriving from acqMonth (start of that
                            // month) so the chart still functions on
                            // pre-existing blobs not yet refreshed.
                            const daysSince = (c: { acqDaysAgo?: number; acqMonth: string }): number => {
                              if (c.acqDaysAgo != null) return c.acqDaysAgo;
                              if (!c.acqMonth) return -1;
                              const [y, mo] = c.acqMonth.split("-").map(Number);
                              const acqStart = new Date(y, mo - 1, 1).getTime();
                              return Math.floor((NOW_DAYS - acqStart) / DAY_MS_C);
                            };
                            // Core anchor: customers acquired 12-24m ago.
                            // Bounded above to keep the growth shape fresh as
                            // the app's installed lifetime grows.
                            const coreAnchorCusts = sourceCusts.filter((c) => {
                              const d = daysSince(c);
                              return d >= 365 && d < 730;
                            });
                            const coreAnchorSeries = buildSeriesFromCusts(coreAnchorCusts, MAX_MONTHS);
                            const coreAnchorByMonth: Record<number, number> = {};
                            for (const p of coreAnchorSeries) {
                              if (p.month >= 1) coreAnchorByMonth[p.month] = p.avgLtv;
                            }

                            // Near-mature: 10-12m observable behaviour
                            // (daysSince in [300, 365)). Each one's missing
                            // m_k+1..m12 is projected from the core's
                            // observed_m_k -> m12 ratio. Boost only activates
                            // when the core has enough seed (>=30) to derive
                            // a stable ratio - otherwise the projection is
                            // too noisy and we fall back to core-only.
                            const CORE_ANCHOR_MIN_N = 30;
                            const nearMatureCusts = sourceCusts.filter((c) => {
                              const d = daysSince(c);
                              return d >= 300 && d < 365;
                            });

                            type ProjCust = { acqMonth?: string | null; ltvByMonth: number[] };
                            const projectedCusts: ProjCust[] = [];
                            const boostActive = coreAnchorCusts.length >= CORE_ANCHOR_MIN_N
                              && nearMatureCusts.length > 0;
                            if (boostActive) {
                              for (const c of nearMatureCusts) {
                                const lm = c.ltvByMonth || [];
                                // Largest observed month for this customer.
                                let kObs = 0;
                                for (let m = 1; m <= MAX_MONTHS; m++) {
                                  if (lm[m - 1] != null) kObs = m;
                                }
                                if (kObs < 1) continue;
                                const base = coreAnchorByMonth[kObs];
                                if (!base || base <= 0) continue;
                                const baseVal = lm[kObs - 1] as number;
                                const proj: number[] = [];
                                for (let m = 1; m <= MAX_MONTHS; m++) {
                                  if (m <= kObs) {
                                    const v = lm[m - 1];
                                    if (v != null) proj[m - 1] = v as number;
                                  } else {
                                    const r = coreAnchorByMonth[m];
                                    if (r == null) continue;
                                    proj[m - 1] = baseVal * (r / base);
                                  }
                                }
                                projectedCusts.push({ acqMonth: c.acqMonth, ltvByMonth: proj });
                              }
                            }

                            // anchorCusts is the population used by cohortCAC
                            // (combined core + projected acqMonths) and the
                            // basis for anchorSeries. Projected customers'
                            // ltvByMonth has actuals 1..k and projections
                            // k+1..12 - buildSeriesFromCusts treats them
                            // uniformly when computing month-by-month means.
                            const anchorCusts = projectedCusts.length > 0
                              ? ([...coreAnchorCusts, ...projectedCusts] as typeof sourceCusts)
                              : coreAnchorCusts;
                            anchorCoreN = coreAnchorCusts.length;
                            anchorProjectedN = projectedCusts.length;
                            anchorN = anchorCusts.length;
                            anchorSeries = buildSeriesFromCusts(anchorCusts, MAX_MONTHS);

                            let overlayCusts: typeof sourceCusts = [];
                            if (targetM !== 12) {
                              // Fixed cohort: customers that have FULLY
                              // observed all targetM months (so every point
                              // 0..targetM is the same set of customers -
                              // no composition shift, no dip at the end).
                              // Bounded ABOVE by the NEXT-larger cohort's
                              // floor so each cohort is a non-overlapping
                              // slice: 1m=30-90d, 3m=90-180d, 6m=180-365d.
                              // Previously bounded only by 365d, which
                              // diluted the 1m cohort with 11-month-old
                              // customers whose m1 LTV reflects stale
                              // acquisition quality.
                              const upper = targetM === 1 ? 90 : targetM === 3 ? 180 : 365;
                              overlayCusts = sourceCusts.filter((c) => {
                                const d = daysSince(c);
                                return d >= targetM * 30 && d < upper;
                              });
                              overlayN = overlayCusts.length;
                              overlaySeries = buildSeriesFromCusts(overlayCusts, targetM);
                            }
                            chartCAC = isMeta
                              ? cohortCAC(targetM === 12 ? anchorCusts : overlayCusts)
                              : 0;

                            // All-customers path: aggregate cohort rollup.
                            // Always built so the Meta tab's Y-axis includes
                            // the All-Customers ceiling. Uses ltvMonthly.all
                            // unconditionally (NOT monthlyDataObj which
                            // tracks the active tab) so that toggling tabs
                            // doesn't shift the Y-axis.
                            const NOW_C = Date.now();
                            const MS_PER_MONTH = 30 * DAY_MS_C;
                            const strictMaxMonth = (acqMonth: string): number => {
                              if (!acqMonth) return -1;
                              const [y, mo] = acqMonth.split("-").map(Number);
                              const acqEnd = new Date(y, mo, 1).getTime();
                              const elapsed = (NOW_C - acqEnd) / MS_PER_MONTH;
                              return Math.max(-1, Math.floor(elapsed) - 1);
                            };
                            // Pick the per-gender slice when a gender filter
                            // is active. byGender is keyed female/male/unknown
                            // and falls back to the parent rows[] if missing
                            // (older blobs pre-byGender, or empty bucket).
                            const allByGender = (ltvMonthly?.all as any)?.byGender;
                            const genderSlice = ltvFilterGender !== "All" && allByGender?.[ltvFilterGender]?.rows
                              ? allByGender[ltvFilterGender].rows
                              : null;
                            const allCohortRows = genderSlice || ltvMonthly?.all?.rows || [];
                            const allMatured = allCohortRows.filter((r: any) => strictMaxMonth(r.month) >= targetM);
                            const allTotalN = allMatured.reduce((s: number, r: any) => s + r.count, 0);
                            fallbackSeries = [{ month: 0, avgLtv: 0, n: allTotalN }];
                            if (allTotalN > 0) {
                              for (let m = 1; m <= targetM; m++) {
                                let totalRev = 0, totalCust = 0;
                                for (const row of allMatured) {
                                  const md = row.months[m];
                                  if (md?.avgLtv != null) {
                                    totalRev += md.avgLtv * row.count;
                                    totalCust += row.count;
                                  }
                                }
                                if (totalCust > 0) fallbackSeries.push({ month: m, avgLtv: totalRev / totalCust, n: totalCust });
                              }
                            }

                            // Shadow outer `cac` with the per-cohort value
                            // so every `cac` reference below picks up the
                            // 12/6/3/1-aware figure (not the all-time avg).
                            const cac = chartCAC;

                            // Projection: walk the anchor's per-month curve
                            // from window-end to month 12, scaled to the
                            // overlay's terminal value. This produces a
                            // CURVE that follows the anchor's growth shape
                            // (not a straight line - LTV typically slows
                            // over time, and the anchor reflects that).
                            //   projected[m] = overlay[N] × (anchor[m] / anchor[N])
                            // for m in (N+1)..12.
                            let projectionSeries: Series = [];
                            let projection: { from: { month: number; avgLtv: number }; to: { month: number; avgLtv: number }; multiplier: number } | null = null;
                            if (isMeta && targetM !== 12 && overlaySeries.length > 0 && anchorSeries.length > 0) {
                              const overlayLast = overlaySeries[overlaySeries.length - 1];
                              const anchorAtN = anchorSeries.find((p) => p.month === overlayLast.month);
                              if (anchorAtN && anchorAtN.avgLtv > 0) {
                                projectionSeries = [{ month: overlayLast.month, avgLtv: overlayLast.avgLtv, n: overlayLast.n }];
                                for (let m = overlayLast.month + 1; m <= MAX_MONTHS; m++) {
                                  const ap = anchorSeries.find((p) => p.month === m);
                                  if (!ap) continue;
                                  projectionSeries.push({
                                    month: m,
                                    avgLtv: overlayLast.avgLtv * (ap.avgLtv / anchorAtN.avgLtv),
                                    n: 0,
                                  });
                                }
                                if (projectionSeries.length >= 2) {
                                  const term = projectionSeries[projectionSeries.length - 1];
                                  projection = {
                                    from: { month: overlayLast.month, avgLtv: overlayLast.avgLtv },
                                    to: { month: term.month, avgLtv: term.avgLtv },
                                    multiplier: term.avgLtv / overlayLast.avgLtv,
                                  };
                                }
                              }
                            }

                            // Determine the active "headline" curve and the
                            // value at month-12 (or window end).
                            const primarySeries = isMeta && targetM !== 12 && overlaySeries.length > 1
                              ? overlaySeries
                              : (isMeta ? anchorSeries : fallbackSeries);
                            const primaryLast = primarySeries[primarySeries.length - 1];

                            // Empty state: no anchor data AND no overlay/
                            // fallback. Nothing meaningful to show.
                            const hasAnything = (isMeta
                              ? (anchorSeries.length > 1 || overlaySeries.length > 1)
                              : fallbackSeries.length > 1);
                            const xMaxAxis = isMeta ? MAX_MONTHS : targetM;

                            // Chart geometry. viewBox is sized to match the
                            // measured wrapper width in pixels (via the
                            // ResizeObserver up top), so SVG content renders
                            // 1:1 - text/lines/dots stay at native pixel
                            // size regardless of how wide the wrapper grows.
                            const chartWidth = ltvChartW;
                            const chartHeight = 340;
                            const padL = 64, padR = 24, padT = 20, padB = 42;
                            const innerW = chartWidth - padL - padR;
                            const innerH = chartHeight - padT - padB;
                            // Y-axis is locked to the long-term curve (+ CAC
                            // headroom + projection target if higher) and
                            // rounded to a "nice" multiple so values don't
                            // jiggle when the user toggles 6/3/1/12. Keeps
                            // grid lines on round numbers (e.g. 0/250/500…).
                            const niceCeil = (raw: number, ticks = 4): number => {
                              if (!isFinite(raw) || raw <= 0) return 1;
                              const rough = raw / ticks;
                              const mag = Math.pow(10, Math.floor(Math.log10(rough)));
                              const norm = rough / mag;
                              let stepMult: number;
                              if (norm <= 1) stepMult = 1;
                              else if (norm <= 2) stepMult = 2;
                              else if (norm <= 2.5) stepMult = 2.5;
                              else if (norm <= 5) stepMult = 5;
                              else stepMult = 10;
                              const step = stepMult * mag;
                              return Math.ceil(raw / step) * step;
                            };
                            const stablePoints: number[] = [
                              ...anchorSeries.map((p) => p.avgLtv),
                              ...fallbackSeries.map((p) => p.avgLtv),
                              projection?.to?.avgLtv ?? 0,
                              isMeta && cac > 0 ? cac * 1.15 : 0,
                              1,
                            ];
                            const ltvMaxRaw = Math.max(...stablePoints) * 1.08;
                            const ltvMax = niceCeil(ltvMaxRaw, 4);
                            const xPos = (m: number) => padL + (m / Math.max(xMaxAxis, 1)) * innerW;
                            const yPos = (v: number) => padT + innerH - (v / ltvMax) * innerH;

                            // Fritsch-Carlson monotone cubic Hermite. Same
                            // smoother as before - prevents the overshoot
                            // that Catmull-Rom produced.
                            const buildSmoothPath = (points: Series) => {
                              const n = points.length;
                              if (n < 2) return "";
                              const xs = points.map((p) => p.month);
                              const ys = points.map((p) => p.avgLtv);
                              const dx: number[] = [];
                              const slopes: number[] = [];
                              for (let i = 0; i < n - 1; i++) {
                                dx.push(xs[i + 1] - xs[i]);
                                slopes.push((ys[i + 1] - ys[i]) / dx[i]);
                              }
                              const t = new Array<number>(n);
                              t[0] = slopes[0];
                              t[n - 1] = slopes[n - 2];
                              for (let i = 1; i < n - 1; i++) t[i] = (slopes[i - 1] + slopes[i]) / 2;
                              for (let i = 0; i < n - 1; i++) {
                                if (slopes[i] === 0) {
                                  t[i] = 0; t[i + 1] = 0;
                                } else {
                                  const a = t[i] / slopes[i];
                                  const b = t[i + 1] / slopes[i];
                                  if (a < 0) t[i] = 0;
                                  if (b < 0) t[i + 1] = 0;
                                  const h = a * a + b * b;
                                  if (h > 9) {
                                    const scale = 3 / Math.sqrt(h);
                                    t[i] = scale * a * slopes[i];
                                    t[i + 1] = scale * b * slopes[i];
                                  }
                                }
                              }
                              let d = `M ${xPos(xs[0]).toFixed(1)},${yPos(ys[0]).toFixed(1)}`;
                              for (let i = 0; i < n - 1; i++) {
                                const c1xData = xs[i] + dx[i] / 3;
                                const c1yData = ys[i] + (t[i] * dx[i]) / 3;
                                const c2xData = xs[i + 1] - dx[i] / 3;
                                const c2yData = ys[i + 1] - (t[i + 1] * dx[i]) / 3;
                                d += ` C ${xPos(c1xData).toFixed(1)},${yPos(c1yData).toFixed(1)} ${xPos(c2xData).toFixed(1)},${yPos(c2yData).toFixed(1)} ${xPos(xs[i + 1]).toFixed(1)},${yPos(ys[i + 1]).toFixed(1)}`;
                              }
                              return d;
                            };

                            const anchorPath = buildSmoothPath(anchorSeries);
                            const overlayPath = buildSmoothPath(overlaySeries);
                            const fallbackPath = buildSmoothPath(fallbackSeries);
                            const projectionPath = buildSmoothPath(projectionSeries);
                            // Area fill under the primary headline curve
                            const areaSeries = primarySeries;
                            const areaPath = areaSeries.length >= 2
                              ? `${buildSmoothPath(areaSeries)} L ${xPos(areaSeries[areaSeries.length - 1].month).toFixed(1)} ${(padT + innerH).toFixed(1)} L ${xPos(0).toFixed(1)} ${(padT + innerH).toFixed(1)} Z`
                              : "";

                            // Payback: cumulative gross profit per customer
                            // (avgLtv × margin) crossing CAC. Search the
                            // primary curve first, then continue into the
                            // projected continuation if needed - that lets
                            // us still surface a payback estimate when the
                            // 6/3/1 cohort hasn't matured to recoup yet.
                            let paybackMonth: number | null = null;
                            if (isMeta && cac > 0 && marginFrac > 0) {
                              const search = (s: Series): number | null => {
                                for (let i = 1; i < s.length; i++) {
                                  const a = s[i - 1], b = s[i];
                                  const ag = a.avgLtv * marginFrac;
                                  const bg = b.avgLtv * marginFrac;
                                  if (ag <= cac && bg >= cac && bg !== ag) {
                                    return a.month + (b.month - a.month) * (cac - ag) / (bg - ag);
                                  }
                                }
                                return null;
                              };
                              paybackMonth = search(primarySeries);
                              if (paybackMonth == null && projectionSeries.length >= 2) {
                                paybackMonth = search(projectionSeries);
                              }
                            }
                            // Display payback in DAYS (1 month = 30 days)
                            // so the chart marker matches the hero tile.
                            const paybackDays = paybackMonth != null ? Math.max(1, Math.round(paybackMonth * 30)) : null;
                            const paybackOnProjection = paybackMonth != null && projectionSeries.length >= 2 && paybackMonth > primarySeries[primarySeries.length - 1].month;
                            const ltvCacRatioCurve = isMeta && cac > 0 && primaryLast ? primaryLast.avgLtv / cac : 0;
                            const gridVals = [0, ltvMax * 0.25, ltvMax * 0.5, ltvMax * 0.75, ltvMax];

                            // ── Scaling economics (Meta tab) ──────────────
                            // Two horizons: month 6 and month 12. LTV at each
                            // comes from the anchor (long-term average
                            // behaviour); when the anchor is too young, fall
                            // back to the projection curve. Break-even CAC =
                            // gross profit generated by that horizon: the
                            // most you could pay per customer and still fully
                            // recoup within it at the set margin.
                            const anchorLtvAt = (m: number) => anchorSeries.find((p) => p.month === m)?.avgLtv ?? 0;
                            const projLtvAt = (m: number) => projectionSeries.find((p) => p.month === m)?.avgLtv ?? 0;
                            const ltvH6 = anchorLtvAt(6) || projLtvAt(6);
                            const ltvH12 = anchorLtvAt(12) || projLtvAt(12) || (projection?.to?.avgLtv ?? 0);
                            const breakevenCac6 = ltvH6 * marginFrac;
                            const breakevenCac12 = ltvH12 * marginFrac;
                            const headroom6 = cac > 0 && breakevenCac6 > 0 ? breakevenCac6 / cac : 0;
                            const headroom12 = cac > 0 && breakevenCac12 > 0 ? breakevenCac12 / cac : 0;
                            // Cash float: what you're out of pocket per new
                            // customer after their first order's gross
                            // profit, and roughly how much cash that ties up
                            // per month at the current acquisition rate.
                            const firstOrderGross = avgAov * marginFrac;
                            const floatPerCust = Math.max(0, cac - firstOrderGross);
                            const nowYm = new Date().toISOString().slice(0, 7);
                            const completeMonths = Object.keys(_allMetaByMonth).filter((m) => m < nowYm).sort();
                            const last3Months = completeMonths.slice(-3);
                            const monthlyNew = last3Months.length > 0
                              ? last3Months.reduce((s, m) => s + (_allMetaByMonth[m] || 0), 0) / last3Months.length
                              : 0;
                            const monthlyFloat = floatPerCust * monthlyNew;

                            // ── Early scaling signals (Meta tab) ──────────
                            // The LTV curve is history; these judge the
                            // NEWEST monthly cohorts against the long-term
                            // customer at the same age. Per recent month:
                            // CAC vs trailing baseline, first-order AOV vs
                            // the anchor's, and cumulative LTV at the
                            // cohort's current age vs the anchor at the same
                            // age. That last delta is the earliest trustable
                            // "is scaling working" read.
                            type SignalRow = {
                              month: string; n: number;
                              spend: number; spendDelta: number | null;
                              cohortCac: number; cacDelta: number | null;
                              avgFirst: number; firstDelta: number | null;
                              ageMonths: number; trajDelta: number | null;
                            };
                            const signalRows: SignalRow[] = [];
                            // Trailing-baseline CAC: the 6 complete months
                            // preceding the 3 being judged.
                            const baseMonths = completeMonths.slice(-9, -3);
                            let baseSpend = 0, baseNew = 0;
                            for (const m of baseMonths) {
                              baseSpend += (metaSpendByAcqMonth || {})[m] || 0;
                              baseNew += _allMetaByMonth[m] || 0;
                            }
                            const baselineCac = baseNew > 0 ? baseSpend / baseNew : 0;
                            // Spend comparison is month-over-month (vs the
                            // immediately preceding month), NOT a long
                            // baseline - a 6-month average hides whether the
                            // merchant is upping spend RIGHT NOW (e.g. a peak
                            // inside the window masks a fresh increase).
                            const spendOfMonth = (m: string | undefined) => (m ? (metaSpendByAcqMonth || {})[m] || 0 : 0);
                            // Anchor first-order benchmark (real customers
                            // only - projected boost records carry no
                            // firstOrder field).
                            const anchorFirstVals = (coreAnchorCusts as any[]).map((c) => c.firstOrder).filter((v) => v > 0);
                            const anchorAvgFirst = anchorFirstVals.length > 0
                              ? anchorFirstVals.reduce((s: number, v: number) => s + v, 0) / anchorFirstVals.length
                              : 0;
                            if (isMeta) {
                              for (const m of completeMonths.slice(-3).reverse()) {
                                const cohort = (sourceCusts as any[]).filter((c) => c.acqMonth === m);
                                if (cohort.length === 0) continue;
                                const spendM = (metaSpendByAcqMonth || {})[m] || 0;
                                const allN = _allMetaByMonth[m] || 0;
                                const cohortCac = allN > 0 && spendM > 0 ? spendM / allN : 0;
                                const firstVals = cohort.map((c) => c.firstOrder).filter((v: number) => v > 0);
                                const avgFirst = firstVals.length > 0 ? firstVals.reduce((s: number, v: number) => s + v, 0) / firstVals.length : 0;
                                // Cohort age in complete 30-day months, from
                                // the median customer's acqDaysAgo.
                                const ages = cohort.map((c) => c.acqDaysAgo).filter((d: number) => d != null).sort((a: number, b: number) => a - b);
                                const medAge = ages.length > 0 ? ages[Math.floor(ages.length / 2)] : 0;
                                const ageMonths = Math.min(12, Math.floor(medAge / 30));
                                let trajDelta: number | null = null;
                                if (ageMonths >= 1) {
                                  let sum = 0, n = 0;
                                  for (const c of cohort) {
                                    const v = c.ltvByMonth?.[ageMonths - 1];
                                    if (v != null) { sum += v; n++; }
                                  }
                                  const anchorAt = anchorLtvAt(ageMonths);
                                  if (n >= 5 && anchorAt > 0) trajDelta = (sum / n) / anchorAt - 1;
                                }
                                const prevSpend = spendOfMonth(completeMonths[completeMonths.indexOf(m) - 1]);
                                signalRows.push({
                                  month: m, n: cohort.length,
                                  spend: spendM,
                                  spendDelta: prevSpend > 0 && spendM > 0 ? spendM / prevSpend - 1 : null,
                                  cohortCac,
                                  cacDelta: baselineCac > 0 && cohortCac > 0 ? cohortCac / baselineCac - 1 : null,
                                  avgFirst,
                                  firstDelta: anchorAvgFirst > 0 && avgFirst > 0 ? avgFirst / anchorAvgFirst - 1 : null,
                                  ageMonths, trajDelta,
                                });
                              }
                            }
                            // Is the merchant actually scaling? Judge only
                            // the LATEST complete month vs the month before
                            // it - a short, immediate read. No prior data
                            // (young store) = can't judge, keep panel live.
                            const latestSpendDelta = signalRows[0]?.spendDelta ?? null;
                            const spendRising = latestSpendDelta == null || latestSpendDelta >= 0.1;
                            const prevMonthSpend = spendOfMonth(completeMonths[completeMonths.length - 2]);

                            const windowOptions: Array<{ value: 1 | 3 | 6 | 12; label: string }> = [
                              { value: 12, label: "12m" }, { value: 6, label: "6m" }, { value: 3, label: "3m" }, { value: 1, label: "1m" },
                            ];
                            // Big, central button group - this is the
                            // headline interaction.
                            const windowSelector = (
                              <div style={{ display: "inline-flex", gap: "4px" }}>
                                {windowOptions.map((opt) => (
                                  <button
                                    key={opt.value}
                                    onClick={() => setLtvChartWindow(opt.value)}
                                    className={`l-pill${ltvChartWindow === opt.value ? " l-pill--active" : ""}`}
                                    style={{ padding: "7px 16px", fontSize: "13px" }}
                                  >{opt.label}</button>
                                ))}
                              </div>
                            );

                            // Empty state.
                            if (!hasAnything) {
                              return (
                                <div style={{ background: "linear-gradient(180deg, #FAFAFF 0%, #FFFFFF 60%)", border: "1px solid #ECECF5", borderRadius: 12, padding: "16px 18px" }}>
                                  <div style={{ display: "flex", justifyContent: "center", marginBottom: 16 }}>{windowSelector}</div>
                                  <div style={{ padding: "20px 12px", textAlign: "center", color: "#9CA3AF", fontSize: 13 }}>
                                    {isMeta
                                      ? "Not enough Meta-acquired customers with 12+ months of history yet to build the long-term average."
                                      : `No cohort has fully observed ${targetM} month${targetM === 1 ? "" : "s"} yet. Try a shorter horizon.`}
                                  </div>
                                </div>
                              );
                            }

                            // Hover lookup helpers.
                            const lookup = (s: Series, m: number) => s.find((p) => p.month === m) ?? null;
                            const hoverM = chartHover?.month ?? null;
                            const hoverAnchor = hoverM != null ? lookup(anchorSeries, hoverM) : null;
                            const hoverOverlay = hoverM != null ? lookup(overlaySeries, hoverM) : null;
                            const hoverFallback = hoverM != null ? lookup(fallbackSeries, hoverM) : null;

                            // Set of x positions that have at least one
                            // active series datapoint - powers the hit
                            // targets and dot rendering.
                            const xTickSet = new Set<number>();
                            for (const p of anchorSeries) xTickSet.add(p.month);
                            for (const p of overlaySeries) xTickSet.add(p.month);
                            for (const p of fallbackSeries) xTickSet.add(p.month);
                            const xTicks = Array.from({ length: xMaxAxis + 1 }, (_, i) => i);

                            return (
                              <div style={{ background: "linear-gradient(180deg, #FAFAFF 0%, #FFFFFF 60%)", border: "1px solid #ECECF5", borderRadius: 12, padding: "14px 16px 12px", boxShadow: "0 1px 2px rgba(15, 23, 42, 0.04)" }}>
                                {/* HERO TILES - ABOVE the selector. Lifetime
                                    Value · LTV:CAC · Payback. All three are
                                    cohort-aware: 12m shows realised, 6/3/1
                                    shows projected (LTV) / current+projected
                                    (LTV:CAC) / margin-aware days (Payback). */}
                                {(() => {
                                  // On All Customers, all hero values come from
                                  // fallbackSeries (the cohort-rollup path) - the
                                  // Meta-only anchor/overlay/projection don't apply.
                                  const fallbackAt = (m: number) => fallbackSeries.find((p) => p.month === m)?.avgLtv ?? 0;
                                  const heroLtvVal = isMeta
                                    ? (targetM === 12
                                        ? (anchorSeries.find((p) => p.month === MAX_MONTHS)?.avgLtv ?? 0)
                                        : (projection?.to?.avgLtv ?? primaryLast?.avgLtv ?? 0))
                                    : (fallbackAt(targetM) || primaryLast?.avgLtv || 0);
                                  const heroLtvSub = isMeta
                                    ? (targetM === 12 ? "realised by month 12" : "projected at month 12")
                                    : `realised by month ${targetM}`;
                                  const allCohortN = !isMeta ? (primaryLast?.n ?? 0) : 0;
                                  const projectedLtvCac = cac > 0 ? heroLtvVal / cac : 0;
                                  const ratioColor = projectedLtvCac >= 3 ? "#059669" : projectedLtvCac >= 2 ? "#1F2937" : projectedLtvCac >= 1 ? "#D97706" : "#DC2626";
                                  const ratioBlurb = projectedLtvCac >= 3 ? `Healthy - every ${cs}1 returns ${cs}${projectedLtvCac.toFixed(2)} by month 12`
                                    : projectedLtvCac >= 2 ? `On track - ${cs}1 returns ${cs}${projectedLtvCac.toFixed(2)} by month 12`
                                    : projectedLtvCac >= 1 ? "Thin margin - lift repeat rate or lower CAC"
                                    : projectedLtvCac > 0 ? "Unprofitable - CAC outpaces 12m LTV"
                                    : "Not enough data yet";
                                  const headroomColor = headroom12 >= 2 ? "#059669" : headroom12 >= 1.2 ? "#1F2937" : headroom12 >= 1 ? "#D97706" : "#DC2626";
                                  return (
                                    <div style={{ display: "grid", gridTemplateColumns: "1.2fr 1fr 1fr 1fr", gap: 14, marginBottom: 16 }}>
                                      <div style={{ padding: "20px 24px", background: "linear-gradient(135deg, #EEF2FF 0%, #F5F3FF 100%)", borderRadius: 12, border: "1px solid #E0E7FF" }}>
                                        <div style={{ fontSize: 11, fontWeight: 700, color: "#6366F1", textTransform: "uppercase", letterSpacing: 0.5, marginBottom: 6 }}>Lifetime Value</div>
                                        <div style={{ fontSize: 38, fontWeight: 800, color: "#1F2937", lineHeight: 1.05 }}>
                                          {heroLtvVal > 0 ? `${cs}${Math.round(heroLtvVal).toLocaleString()}` : "-"}
                                        </div>
                                        <div style={{ fontSize: 12, color: "#4B5563", marginTop: 6 }}>{heroLtvSub}</div>
                                        <div style={{ fontSize: 11, color: "#9CA3AF", marginTop: 2 }}>
                                          {isMeta
                                            ? (targetM === 12
                                                ? (anchorProjectedN > 0
                                                    ? `${anchorCoreN.toLocaleString()} matured + ${anchorProjectedN.toLocaleString()} projected (10-11m, m12 estimated)`
                                                    : `${anchorN.toLocaleString()} long-term customer${anchorN === 1 ? "" : "s"}`)
                                                : (anchorProjectedN > 0
                                                    ? `${overlayN.toLocaleString()} recent (last ${targetM}m) + ${anchorCoreN.toLocaleString()} matured + ${anchorProjectedN.toLocaleString()} projected anchor`
                                                    : `${overlayN.toLocaleString()} recent (last ${targetM}m) + ${anchorN.toLocaleString()}-cohort projection`))
                                            : `${allCohortN.toLocaleString()} mature cohort (>=${targetM}mo since acquisition)`}
                                        </div>
                                      </div>
                                      <div style={{ padding: "20px 24px", background: "#fff", borderRadius: 12, border: "1px solid #E5E7EB" }}>
                                        <div style={{ fontSize: 11, fontWeight: 700, color: "#6366F1", textTransform: "uppercase", letterSpacing: 0.5, marginBottom: 6 }}>LTV : CAC</div>
                                        <div style={{ fontSize: 38, fontWeight: 800, color: isMeta ? ratioColor : "#9CA3AF", lineHeight: 1.05 }}>
                                          {isMeta && projectedLtvCac > 0 ? `${projectedLtvCac.toFixed(2)}×` : "-"}
                                        </div>
                                        <div style={{ fontSize: 12, color: "#4B5563", marginTop: 6 }}>
                                          {isMeta ? ratioBlurb : "Meta-only metric"}
                                        </div>
                                        {isMeta && cac > 0 && (
                                          <div style={{ fontSize: 11, color: "#9CA3AF", marginTop: 2 }}>
                                            {cs}{Math.round(heroLtvVal).toLocaleString()} 12m LTV vs {cs}{Math.round(cac).toLocaleString()} CAC
                                          </div>
                                        )}
                                      </div>
                                      <div style={{ padding: "20px 24px", background: "#fff", borderRadius: 12, border: "1px solid #E5E7EB" }}>
                                        <div style={{ fontSize: 11, fontWeight: 700, color: "#6366F1", textTransform: "uppercase", letterSpacing: 0.5, marginBottom: 6 }}>Payback</div>
                                        <div style={{ fontSize: 38, fontWeight: 800, color: "#1F2937", lineHeight: 1.05 }}>
                                          {isMeta && paybackDays != null ? `${paybackDays}d` : "-"}
                                        </div>
                                        <div style={{ fontSize: 12, color: "#4B5563", marginTop: 6 }}>
                                          {!isMeta
                                            ? "Meta-only metric"
                                            : (paybackDays != null
                                                ? (paybackOnProjection
                                                    ? `Projected to clear ${cs}${Math.round(cac).toLocaleString()} CAC at ${Math.round(marginFrac * 100)}% margin`
                                                    : `${Math.round(marginFrac * 100)}% margin recoups ${cs}${Math.round(cac).toLocaleString()} CAC`)
                                                : (cac > 0 ? "Lift margin or lower CAC to recoup" : "Needs CAC"))}
                                        </div>
                                      </div>
                                      <div style={{ padding: "20px 24px", background: "#fff", borderRadius: 12, border: "1px solid #E5E7EB" }}>
                                        <div style={{ fontSize: 11, fontWeight: 700, color: "#6366F1", textTransform: "uppercase", letterSpacing: 0.5, marginBottom: 6 }}>Scaling Headroom</div>
                                        <div style={{ fontSize: 38, fontWeight: 800, color: isMeta && headroom12 > 0 ? headroomColor : "#9CA3AF", lineHeight: 1.05 }}>
                                          {isMeta && headroom12 > 0 ? `${headroom12.toFixed(1)}×` : "-"}
                                        </div>
                                        <div style={{ fontSize: 12, color: "#4B5563", marginTop: 6 }}>
                                          {!isMeta
                                            ? "Meta-only metric"
                                            : (headroom12 > 0
                                                ? `Break-even CAC ${cs}${Math.round(breakevenCac12).toLocaleString()} by month 12 vs ${cs}${Math.round(cac).toLocaleString()} today`
                                                : "Needs CAC + margin")}
                                        </div>
                                        {isMeta && headroom6 > 0 && (
                                          <div style={{ fontSize: 11, color: "#9CA3AF", marginTop: 2 }}>
                                            6-month horizon: {cs}{Math.round(breakevenCac6).toLocaleString()} break-even ({headroom6.toFixed(1)}×)
                                          </div>
                                        )}
                                      </div>
                                    </div>
                                  );
                                })()}
                                {/* CONTROLS - three distinct tools, each
                                    titled with its description directly
                                    underneath. All reshape the chart. */}
                                <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))", gap: 28, alignItems: "start", maxWidth: 940, margin: "0 auto 18px" }}>
                                  <div style={{ textAlign: "center" }}>
                                    <div style={{ fontSize: 11, fontWeight: 700, color: "#6366F1", textTransform: "uppercase", letterSpacing: 0.5, marginBottom: 8 }}>Cohort window</div>
                                    <div style={{ display: "flex", justifyContent: "center" }}>{windowSelector}</div>
                                    <div style={{ fontSize: 11.5, color: "#6B7280", lineHeight: 1.5, marginTop: 8 }}>
                                      Overlays customers acquired in the last {targetM === 12 ? "12" : targetM} month{targetM === 1 ? "" : "s"} on the long-term curve
                                      {isMeta && (
                                        <>
                                          {" · "}
                                          {targetM === 12
                                            ? `${anchorN.toLocaleString()} long-term customer${anchorN === 1 ? "" : "s"}`
                                            : `${anchorN.toLocaleString()} long-term + ${overlayN.toLocaleString()} recent`}
                                        </>
                                      )}
                                    </div>
                                  </div>
                                  <div style={{ textAlign: "center" }}>
                                    <div style={{ fontSize: 11, fontWeight: 700, color: "#6366F1", textTransform: "uppercase", letterSpacing: 0.5, marginBottom: 8 }}>Customers</div>
                                    <div style={{ display: "inline-flex", border: "1px solid #E5E7EB", borderRadius: 10, padding: 3, background: "#F9FAFB", gap: 2 }}>
                                      {([
                                        { value: "All" as const, label: "All" },
                                        { value: "female" as const, label: "Women" },
                                        { value: "male" as const, label: "Men" },
                                      ]).map((opt) => {
                                        const active = ltvFilterGender === opt.value;
                                        // Gender filter is fully active on both
                                        // tabs. On Meta it filters ltvCustomers;
                                        // on All it picks ltvMonthly.all.byGender
                                        // (populated by name-inferredGender for
                                        // every customer in customerRollups).
                                        return (
                                          <button
                                            key={opt.value}
                                            onClick={() => setLtvFilterGender(opt.value)}
                                            className={`l-pill${active ? " l-pill--active" : ""}`}
                                            style={{ padding: "7px 16px", fontSize: "13px" }}
                                          >{opt.label}</button>
                                        );
                                      })}
                                    </div>
                                    <div style={{ fontSize: 11.5, color: "#6B7280", lineHeight: 1.5, marginTop: 8 }}>
                                      Filters every figure on this page to male or female
                                    </div>
                                  </div>
                                  <div style={{ textAlign: "center" }}>
                                    <div style={{ fontSize: 11, fontWeight: 700, color: "#6366F1", textTransform: "uppercase", letterSpacing: 0.5, marginBottom: 8 }}>Your gross margin</div>
                                    <div style={{ display: "inline-flex", alignItems: "center", gap: 10, padding: "8px 14px", border: "1px solid #E5E7EB", borderRadius: 10, background: "#F9FAFB" }}>
                                      <input type="range" min={0} max={100} step={5} value={marginPct} onChange={(e) => handleMarginChange(parseInt(e.target.value, 10))} style={{ width: 130 }} />
                                      <span style={{ fontSize: 14, fontWeight: 700, color: "#1F2937", minWidth: 38 }}>{marginPct}%</span>
                                    </div>
                                    <div style={{ fontSize: 11.5, color: "#6B7280", lineHeight: 1.5, marginTop: 8 }}>
                                      Gross margin is the % of each order left after product, shipping and fulfilment costs. Lucidly can&apos;t see your costs - set it yourself. Every profit figure here depends on it. Saves automatically.
                                    </div>
                                  </div>
                                </div>
                                {/* Chart */}
                                <div ref={ltvChartRef} style={{ position: "relative", width: "70vw", margin: "0 auto" }}>
                                  <svg width={chartWidth} height={chartHeight} viewBox={`0 0 ${chartWidth} ${chartHeight}`} style={{ display: "block" }}>
                                    <defs>
                                      <linearGradient id="ltvAreaGradient" x1="0" y1="0" x2="0" y2="1">
                                        <stop offset="0%" stopColor="#A78BFA" stopOpacity="0.32" />
                                        <stop offset="60%" stopColor="#7C3AED" stopOpacity="0.10" />
                                        <stop offset="100%" stopColor="#7C3AED" stopOpacity="0" />
                                      </linearGradient>
                                      <linearGradient id="ltvLineGradient" x1="0" y1="0" x2="1" y2="0">
                                        <stop offset="0%" stopColor="#7C3AED" />
                                        <stop offset="100%" stopColor="#EC4899" />
                                      </linearGradient>
                                    </defs>
                                    {/* Y grid */}
                                    {gridVals.map((v, i) => (
                                      <g key={i}>
                                        <line x1={padL} x2={chartWidth - padR} y1={yPos(v)} y2={yPos(v)} stroke="#EEF0F7" strokeWidth="1" />
                                        <text x={padL - 8} y={yPos(v) + 4} textAnchor="end" fontSize="12" fill="#9CA3AF">{cs}{Math.round(v).toLocaleString()}</text>
                                      </g>
                                    ))}
                                    {/* X-axis baseline */}
                                    <line x1={padL} x2={chartWidth - padR} y1={padT + innerH} y2={padT + innerH} stroke="#D1D5DB" strokeWidth="1" />
                                    {/* X ticks 0..xMaxAxis */}
                                    {xTicks.map((m) => (
                                      <g key={m}>
                                        <line x1={xPos(m)} x2={xPos(m)} y1={padT + innerH} y2={padT + innerH + 4} stroke="#9CA3AF" strokeWidth="1" />
                                        <text x={xPos(m)} y={chartHeight - padB + 18} textAnchor="middle" fontSize="13" fontWeight="600" fill="#6B7280">{m}</text>
                                      </g>
                                    ))}
                                    <text x={(padL + chartWidth - padR) / 2} y={chartHeight - 8} textAnchor="middle" fontSize="11" fontWeight="600" fill="#9CA3AF" letterSpacing="0.5">MONTHS SINCE FIRST ORDER</text>
                                    {/* Area fill (under primary curve) */}
                                    <path d={areaPath} fill="url(#ltvAreaGradient)" stroke="none" />
                                    {/* CAC reference */}
                                    {isMeta && cac > 0 && cac < ltvMax && (
                                      <g>
                                        <line x1={padL} x2={chartWidth - padR} y1={yPos(cac)} y2={yPos(cac)} stroke="#DC2626" strokeWidth="1.5" strokeDasharray="4 4" />
                                        <text x={chartWidth - padR - 4} y={yPos(cac) - 6} textAnchor="end" fontSize="12" fontWeight="700" fill="#DC2626">CAC {cs}{Math.round(cac).toLocaleString()}</text>
                                      </g>
                                    )}
                                    {/* Anchor line - muted base when overlay present, primary when 12m */}
                                    {isMeta && anchorPath && (
                                      <path
                                        d={anchorPath}
                                        fill="none"
                                        stroke={targetM === 12 ? "url(#ltvLineGradient)" : "#94A3B8"}
                                        strokeWidth={targetM === 12 ? 3 : 2}
                                        strokeLinecap="round"
                                        strokeLinejoin="round"
                                        opacity={targetM === 12 ? 1 : 0.55}
                                      />
                                    )}
                                    {/* "all" tab fallback - single curve */}
                                    {!isMeta && fallbackPath && (
                                      <path d={fallbackPath} fill="none" stroke="url(#ltvLineGradient)" strokeWidth="3" strokeLinecap="round" strokeLinejoin="round" />
                                    )}
                                    {/* Recent overlay - on top, primary purple */}
                                    {isMeta && targetM !== 12 && overlayPath && (
                                      <path d={overlayPath} fill="none" stroke="url(#ltvLineGradient)" strokeWidth="3.25" strokeLinecap="round" strokeLinejoin="round" />
                                    )}
                                    {/* Projection - dotted CURVE following
                                        the anchor's growth shape, with a
                                        dot per projected month so the
                                        slowing of LTV is visible (and each
                                        month is hoverable). */}
                                    {projectionSeries.length >= 2 && projectionPath && (
                                      <g>
                                        <path d={projectionPath} fill="none" stroke="#7C3AED" strokeWidth="2.5" strokeDasharray="4 4" opacity="0.85" strokeLinecap="round" strokeLinejoin="round" />
                                        {projectionSeries.slice(1).map((p) => {
                                          const isHover = hoverM === p.month;
                                          const isLast = p.month === projectionSeries[projectionSeries.length - 1].month;
                                          return (
                                            <g key={`proj-${p.month}`} pointerEvents="none">
                                              {(isHover || isLast) && <circle cx={xPos(p.month)} cy={yPos(p.avgLtv)} r="9" fill="#7C3AED" opacity="0.18" />}
                                              <circle cx={xPos(p.month)} cy={yPos(p.avgLtv)} r={isLast ? 6 : isHover ? 5 : 4} fill="#fff" stroke="#7C3AED" strokeWidth="2.25" />
                                            </g>
                                          );
                                        })}
                                      </g>
                                    )}
                                    {/* Payback marker - hoverable. Drops a
                                        dashed vertical to the day axis and
                                        labels the day count. The hit zone
                                        below the marker toggles a tooltip
                                        explaining the calculation. */}
                                    {paybackMonth != null && (
                                      <g>
                                        <line x1={xPos(paybackMonth)} x2={xPos(paybackMonth)} y1={yPos(cac)} y2={padT + innerH} stroke="#DC2626" strokeWidth="1.5" strokeDasharray="3 3" />
                                        <circle
                                          cx={xPos(paybackMonth)} cy={yPos(cac)} r="9"
                                          fill="#fff" stroke="#DC2626" strokeWidth="2.25"
                                          style={{ cursor: "pointer" }}
                                          onMouseEnter={() => setChartHover({ month: -1 })}
                                          onMouseLeave={() => setChartHover(null)}
                                        />
                                        <text x={xPos(paybackMonth)} y={yPos(cac) - 14} textAnchor="middle" fontSize="12" fontWeight="700" fill="#DC2626">
                                          Payback {paybackDays != null ? `${paybackDays}d` : "-"}
                                        </text>
                                      </g>
                                    )}
                                    {/* Hit targets across all xTicks */}
                                    {xTicks.map((m) => (
                                      <g key={`hit-${m}`} style={{ cursor: "pointer" }} onMouseEnter={() => setChartHover({ month: m })} onMouseLeave={() => setChartHover(null)}>
                                        <rect x={xPos(m) - 22} y={padT} width={44} height={innerH} fill="transparent" />
                                      </g>
                                    ))}
                                    {/* Long-term-avg dots: purple in 12m view, grey when overlay is active */}
                                    {isMeta && targetM === 12 && anchorSeries.map((p) => {
                                      const isHover = hoverM === p.month;
                                      const isLast = p.month === anchorSeries[anchorSeries.length - 1].month;
                                      return (
                                        <g key={`a-${p.month}`} pointerEvents="none">
                                          {(isHover || isLast) && <circle cx={xPos(p.month)} cy={yPos(p.avgLtv)} r="10" fill="#7C3AED" opacity="0.18" />}
                                          <circle cx={xPos(p.month)} cy={yPos(p.avgLtv)} r={isLast ? 6 : isHover ? 5 : 4} fill="#7C3AED" stroke="#fff" strokeWidth="2" />
                                        </g>
                                      );
                                    })}
                                    {isMeta && targetM !== 12 && anchorSeries.map((p) => {
                                      const isHover = hoverM === p.month;
                                      const isLast = p.month === anchorSeries[anchorSeries.length - 1].month;
                                      return (
                                        <g key={`a-grey-${p.month}`} pointerEvents="none">
                                          {isHover && <circle cx={xPos(p.month)} cy={yPos(p.avgLtv)} r="9" fill="#94A3B8" opacity="0.18" />}
                                          <circle cx={xPos(p.month)} cy={yPos(p.avgLtv)} r={isLast ? 4.5 : isHover ? 4 : 3} fill="#94A3B8" stroke="#fff" strokeWidth="1.75" />
                                        </g>
                                      );
                                    })}
                                    {isMeta && targetM !== 12 && overlaySeries.map((p) => {
                                      const isHover = hoverM === p.month;
                                      const isLast = p.month === overlaySeries[overlaySeries.length - 1].month;
                                      return (
                                        <g key={`o-${p.month}`} pointerEvents="none">
                                          {(isHover || isLast) && <circle cx={xPos(p.month)} cy={yPos(p.avgLtv)} r="10" fill="#7C3AED" opacity="0.18" />}
                                          <circle cx={xPos(p.month)} cy={yPos(p.avgLtv)} r={isLast ? 6 : isHover ? 5 : 4} fill="#7C3AED" stroke="#fff" strokeWidth="2" />
                                        </g>
                                      );
                                    })}
                                    {!isMeta && fallbackSeries.map((p) => {
                                      const isHover = hoverM === p.month;
                                      const isLast = p.month === fallbackSeries[fallbackSeries.length - 1].month;
                                      return (
                                        <g key={`f-${p.month}`} pointerEvents="none">
                                          {(isHover || isLast) && <circle cx={xPos(p.month)} cy={yPos(p.avgLtv)} r="10" fill="#7C3AED" opacity="0.18" />}
                                          <circle cx={xPos(p.month)} cy={yPos(p.avgLtv)} r={isLast ? 6 : isHover ? 5 : 4} fill="#7C3AED" stroke="#fff" strokeWidth="2" />
                                        </g>
                                      );
                                    })}
                                    {/* Final value pill on primary */}
                                    {primaryLast && (() => {
                                      const x = xPos(primaryLast.month);
                                      const y = yPos(primaryLast.avgLtv);
                                      const labelText = `${cs}${Math.round(primaryLast.avgLtv).toLocaleString()}`;
                                      const labelW = 16 + labelText.length * 7;
                                      const lx = x - labelW - 10;
                                      const ly = y - 12;
                                      return (
                                        <g pointerEvents="none">
                                          <rect x={lx} y={ly} width={labelW} height={24} rx="12" fill="#7C3AED" />
                                          <text x={lx + labelW / 2} y={ly + 16} textAnchor="middle" fontSize="13" fontWeight="700" fill="#fff">{labelText}</text>
                                        </g>
                                      );
                                    })()}
                                    {/* Hover tooltip. month === -1 is the
                                        synthetic payback hover. Otherwise
                                        show the recent / long-term /
                                        projected value at the hovered month
                                        (n=xxx noise removed - it bloated
                                        the tooltip without adding signal). */}
                                    {chartHover && chartHover.month === -1 && paybackMonth != null && (() => {
                                      const hx = xPos(paybackMonth);
                                      const tipW = 260;
                                      const tipH = 78;
                                      const leftSide = hx > chartWidth / 2;
                                      const tx = leftSide ? hx - tipW - 14 : hx + 14;
                                      const ty = Math.max(padT + 6, yPos(cac) - tipH - 12);
                                      return (
                                        <g pointerEvents="none">
                                          <rect x={tx} y={ty} width={tipW} height={tipH} rx="8" fill="#0F172A" opacity="0.96" />
                                          <text x={tx + 12} y={ty + 20} fontSize="13" fontWeight="700" fill="#FCA5A5">Payback {paybackDays}d</text>
                                          <text x={tx + 12} y={ty + 40} fontSize="11" fill="#E5E7EB">{cs}{Math.round(cac).toLocaleString()} CAC recouped via</text>
                                          <text x={tx + 12} y={ty + 56} fontSize="11" fill="#E5E7EB">{Math.round(marginFrac * 100)}% margin {paybackOnProjection ? "(projected)" : "(observed)"}</text>
                                        </g>
                                      );
                                    })()}
                                    {chartHover && chartHover.month >= 0 && (() => {
                                      const hx = xPos(chartHover.month);
                                      const showAnchor = isMeta && hoverAnchor != null;
                                      const showOverlay = isMeta && targetM !== 12 && hoverOverlay != null;
                                      const showFallback = !isMeta && hoverFallback != null;
                                      const projAtMonth = isMeta && targetM !== 12 && projectionSeries.length >= 2
                                        ? projectionSeries.find((p) => p.month === chartHover.month && p.month > (overlaySeries[overlaySeries.length - 1]?.month ?? -1))
                                        : undefined;
                                      type Row = { kind: "overlay" | "anchor" | "fallback" | "projected"; avgLtv: number };
                                      const rows: Row[] = [];
                                      if (showOverlay) rows.push({ kind: "overlay", avgLtv: hoverOverlay!.avgLtv });
                                      if (projAtMonth) rows.push({ kind: "projected", avgLtv: projAtMonth.avgLtv });
                                      if (showAnchor) rows.push({ kind: "anchor", avgLtv: hoverAnchor!.avgLtv });
                                      if (showFallback) rows.push({ kind: "fallback", avgLtv: hoverFallback!.avgLtv });
                                      if (rows.length === 0) return null;
                                      const tipW = 230;
                                      const tipH = 30 + rows.length * 18 + 14;
                                      const leftSide = hx > chartWidth / 2;
                                      const tx = leftSide ? hx - tipW - 12 : hx + 12;
                                      const ty = padT + 6;
                                      return (
                                        <g pointerEvents="none">
                                          <line x1={hx} x2={hx} y1={padT} y2={padT + innerH} stroke="#9CA3AF" strokeWidth="1.25" strokeDasharray="3 4" />
                                          <rect x={tx} y={ty} width={tipW} height={tipH} rx="8" fill="#0F172A" opacity="0.96" />
                                          <text x={tx + 12} y={ty + 20} fontSize="13" fontWeight="700" fill="#fff">Month {chartHover.month}</text>
                                          {rows.map((r, idx) => {
                                            const y = ty + 38 + idx * 18;
                                            if (r.kind === "overlay") {
                                              return <text key={idx} x={tx + 12} y={y} fontSize="12" fill="#C4B5FD">Recent {targetM}m: <tspan fontWeight="700" fill="#fff">{cs}{Math.round(r.avgLtv).toLocaleString()}</tspan></text>;
                                            }
                                            if (r.kind === "anchor") {
                                              return <text key={idx} x={tx + 12} y={y} fontSize="12" fill="#94A3B8">Long-term avg: <tspan fontWeight="700" fill="#E5E7EB">{cs}{Math.round(r.avgLtv).toLocaleString()}</tspan></text>;
                                            }
                                            if (r.kind === "projected") {
                                              return <text key={idx} x={tx + 12} y={y} fontSize="12" fill="#DDD6FE">Projected: <tspan fontWeight="700" fill="#fff">{cs}{Math.round(r.avgLtv).toLocaleString()}</tspan></text>;
                                            }
                                            return <text key={idx} x={tx + 12} y={y} fontSize="12" fill="#C4B5FD">Cumulative: <tspan fontWeight="700" fill="#fff">{cs}{Math.round(r.avgLtv).toLocaleString()}</tspan></text>;
                                          })}
                                        </g>
                                      );
                                    })()}
                                  </svg>
                                </div>
                                {/* Footnotes / payback callouts */}
                                {isMeta && targetM !== 12 && projection && (
                                  <div style={{ marginTop: 10, padding: "8px 12px", background: "#F5F3FF", border: "1px solid #DDD6FE", borderRadius: 8, fontSize: 12, color: "#5B21B6" }}>
                                    <strong>Projection:</strong> recent {targetM}-month cohort is at {cs}{Math.round(projection.from.avgLtv).toLocaleString()} by month {projection.from.month}. The long-term average grows {projection.multiplier.toFixed(2)}× from there to month 12, suggesting <strong>{cs}{Math.round(projection.to.avgLtv).toLocaleString()}</strong> projected 12m LTV. Holds only if recent customers follow the same trajectory.
                                  </div>
                                )}
                                {/* WHAT THIS MEANS - headline interpretation
                                    of payback + the two-horizon scaling
                                    economics. Replaces the old small-print
                                    payback footnotes. */}
                                {isMeta && cac > 0 && (breakevenCac6 > 0 || breakevenCac12 > 0) && (() => {
                                  const observed = paybackDays != null && !paybackOnProjection;
                                  const marginLbl = Math.round(marginFrac * 100);
                                  const riseFullPct = Math.max(0, Math.round((headroom12 - 1) * 100));
                                  // Rule of half: break-even means working a
                                  // year for zero profit, so only ~half the
                                  // gap to break-even is a sensible buffer.
                                  const riseUsablePct = Math.max(0, Math.round(((headroom12 - 1) / 2) * 100));
                                  const accent = headroom12 >= 2 && observed ? "#059669"
                                    : headroom12 >= 2 ? "#7C3AED"
                                    : headroom12 >= 1.25 ? "#1F2937"
                                    : headroom12 >= 1 ? "#D97706" : "#DC2626";
                                  // Verdict tier for the headroom read
                                  const tier = headroom12 >= 2 && observed ? "scale"
                                    : headroom12 >= 2 ? "scaleProjected"
                                    : headroom12 >= 1.25 ? "moderate"
                                    : headroom12 >= 1 ? "tight" : "fix";
                                  const verdict = tier === "scale" ? "Room to scale"
                                    : tier === "scaleProjected" ? "Scale with caution"
                                    : tier === "moderate" ? "Moderate headroom"
                                    : tier === "tight" ? "Tight headroom"
                                    : "No headroom";
                                  // ── Prioritised actions across ALL levers -
                                  // ad spend is only one of them. Each
                                  // candidate is scored by how pressing it is
                                  // for THIS merchant's numbers; the weakest
                                  // area floats to the top.
                                  const ltv12Full = marginFrac > 0 ? breakevenCac12 / marginFrac : 0;
                                  const anchorArr = coreAnchorCusts as any[];
                                  const repeatRate = anchorArr.length >= 20
                                    ? anchorArr.filter((c) => (c.orders || 0) >= 2).length / anchorArr.length
                                    : null;
                                  type Rec = { area: string; title: string; body: React.ReactNode; score: number };
                                  const recs: Rec[] = [];
                                  // Meta spend - always present, but its
                                  // priority depends on the headroom tier.
                                  if (tier === "scale") {
                                    recs.push({ area: "Meta spend", title: "Scale in steps", score: 90,
                                      body: <>Raise budget 20-30% at a time. CAC can rise ~{riseFullPct}% before 12-month break-even - treat ~{riseUsablePct}% as the usable buffer. Judge each step by the Early Signals below, not this month&apos;s ROAS.</> });
                                  } else if (tier === "scaleProjected") {
                                    recs.push({ area: "Meta spend", title: "Scale with caution", score: 85,
                                      body: <>The numbers suggest room, but payback is projected rather than observed. Increase 10-20% at a time and confirm the Early Signals hold before each further step.</> });
                                  } else if (tier === "moderate") {
                                    recs.push({ area: "Meta spend", title: "Small steps only", score: 60,
                                      body: <>CAC can rise ~{riseFullPct}% before break-even disappears, and it climbs fastest at the top of scale - treat ~{riseUsablePct}% as usable. Move 10-20% at a time and stop when the Early Signals turn.</> });
                                  } else if (tier === "tight") {
                                    recs.push({ area: "Meta spend", title: "Hold spend", score: 35,
                                      body: <>Only ~{riseFullPct}% CAC rise erases 12-month break-even - no safe buffer. Changing ad spend is not the priority right now; the levers above will move the needle more.</> });
                                  } else {
                                    recs.push({ area: "Meta spend", title: "Hold or reduce spend", score: 32,
                                      body: <>At {marginLbl}% margin, customers don&apos;t repay their CAC within 12 months - more spend deepens the loss until the economics improve.</> });
                                  }
                                  // Gross margin - the biggest lever when low.
                                  if (marginLbl > 0 && marginLbl < 55 && ltv12Full > 0) {
                                    recs.push({ area: "Margin", title: "Rebuild gross margin", score: (marginLbl < 45 ? 82 : 62) + (headroom12 < 1.25 ? 10 : 0),
                                      body: <>At {marginLbl}%, margin may be your biggest lever: every 5 points adds ~<strong>{cs}{Math.round(ltv12Full * 0.05).toLocaleString()}</strong> to your break-even CAC. Review product cost, shipping and discounting before touching ad budget.</> });
                                  }
                                  // Repeat rate - the whole LTV engine.
                                  if (repeatRate != null && repeatRate < 0.3) {
                                    recs.push({ area: "Repeat rate", title: "Get the second order", score: repeatRate < 0.2 ? 78 : 58,
                                      body: <>Only <strong>{Math.round(repeatRate * 100)}%</strong> of your long-term Meta customers (acquired 12-24 months ago) ever order again. Post-purchase email/SMS flows and a second-order offer raise LTV - and break-even CAC - at zero acquisition cost.</> });
                                  }
                                  // First-order AOV - closes the day-one gap.
                                  if (floatPerCust > 0) {
                                    recs.push({ area: "First-order value", title: "Close the day-one gap", score: floatPerCust > cac * 0.5 ? 66 : 46,
                                      body: <>The first order returns {cs}{Math.round(firstOrderGross).toLocaleString()} of a {cs}{Math.round(cac).toLocaleString()} CAC, leaving {cs}{Math.round(floatPerCust).toLocaleString()} exposed{paybackDays != null ? <> until day {paybackDays}</> : null}. Bundles and free-shipping thresholds shrink that gap on day one.</> });
                                  }
                                  // CAC itself - cheaper acquisition beats
                                  // more budget when headroom is thin.
                                  if (headroom12 > 0 && headroom12 < 1.5) {
                                    recs.push({ area: "CAC", title: "Lower CAC itself", score: headroom12 < 1.25 ? 80 : 55,
                                      body: <>Before budget moves, work CAC down: refresh fatigued creative, tighten targeting, cut the worst ad sets. A 10% CAC drop alone lifts headroom from {headroom12.toFixed(1)}× to {(breakevenCac12 / (cac * 0.9)).toFixed(1)}×.</> });
                                  }
                                  // Cash float - only relevant if scaling is
                                  // actually on the table.
                                  if (floatPerCust > 0 && monthlyNew >= 1 && (tier === "scale" || tier === "scaleProjected" || tier === "moderate")) {
                                    recs.push({ area: "Cash flow", title: "Budget for the float", score: 44,
                                      body: <>At ~{Math.round(monthlyNew).toLocaleString()} new Meta customers/month, roughly <strong>{cs}{Math.round(monthlyFloat).toLocaleString()}/month</strong> sits in customers who haven&apos;t paid back yet. Raise spend and this cash-in-flight grows <em>before</em> profit does.</> });
                                  }
                                  recs.sort((a, b) => b.score - a.score);
                                  const topRecs = recs.slice(0, 4);
                                  const stepCard = (n: number, label: string, value: React.ReactNode, sub: React.ReactNode, small?: React.ReactNode, footer?: React.ReactNode) => (
                                    <div style={{ background: "#FFFFFF", border: "1px solid #E5E7EB", borderRadius: 12, padding: "20px 24px" }}>
                                      <div style={{ fontSize: 11, fontWeight: 700, color: "#6366F1", textTransform: "uppercase", letterSpacing: 0.5, marginBottom: 6 }}>
                                        Step {n} · {label}
                                      </div>
                                      <div style={{ fontSize: 38, fontWeight: 800, color: "#1F2937", lineHeight: 1.05 }}>{value}</div>
                                      <div style={{ fontSize: 12, color: "#4B5563", marginTop: 6 }}>{sub}</div>
                                      {small != null && <div style={{ fontSize: 11, color: "#9CA3AF", marginTop: 2 }}>{small}</div>}
                                      {footer != null && <div style={{ marginTop: 10 }}>{footer}</div>}
                                    </div>
                                  );
                                  return (
                                    <div style={{ marginTop: 16 }}>
                                      <div style={{ fontSize: 15, fontWeight: 800, color: "#1F2937", marginBottom: 10 }}>What this means</div>
                                      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))", gap: 14 }}>
                                        {stepCard(1, "You pay",
                                          <>{cs}{Math.round(cac).toLocaleString()}</>,
                                          <>to acquire one new Meta customer</>,
                                          <>Your blended CAC over this window</>)}
                                        {stepCard(2, "First order gross profit",
                                          <>{cs}{Math.round(firstOrderGross).toLocaleString()}</>,
                                          <>based on {marginLbl}% margin</>,
                                          floatPerCust > 0
                                            ? <>Leaves {cs}{Math.round(floatPerCust).toLocaleString()} still to recover</>
                                            : <>Covers the CAC on day one</>)}
                                        {stepCard(3, "Paid back",
                                          paybackDays != null ? <>Day {paybackDays}</> : (floatPerCust <= 0 ? <>Day 0</> : <>-</>),
                                          <>when cumulative profit crosses CAC</>)}
                                        {stepCard(4, "They generate",
                                          <>{cs}{Math.round(breakevenCac12).toLocaleString()}</>,
                                          <>total gross profit per customer by month 12{breakevenCac6 > 0 ? <> ({cs}{Math.round(breakevenCac6).toLocaleString()} by month 6)</> : null}</>,
                                          <>which makes it your break-even CAC: pay more than this per customer and 12 months of profit won&apos;t cover it</>)}
                                        {stepCard(5, "Headroom",
                                          <span style={{ color: accent }}>{headroom12 > 0 ? `${headroom12.toFixed(1)}×` : "-"}</span>,
                                          <>CAC could rise ~{riseFullPct}% before 12-month break-even</>,
                                          headroom6 > 0 ? <>6-month view: {headroom6.toFixed(1)}×</> : null,
                                          <span style={{ display: "inline-block", fontSize: 11, fontWeight: 700, color: "#FFFFFF", background: accent, borderRadius: 999, padding: "3px 10px", textTransform: "uppercase", letterSpacing: 0.5 }}>{verdict}</span>)}
                                      </div>
                                      {/* SUGGESTED ACTIONS - prioritised
                                          across ALL levers, not just spend */}
                                      <div style={{ marginTop: 20, marginBottom: 10 }}>
                                        <div style={{ fontSize: 15, fontWeight: 800, color: "#1F2937" }}>Suggested actions</div>
                                        <div style={{ fontSize: 12, color: "#6B7280", marginTop: 2 }}>
                                          Ranked by what&apos;s most pressing in your numbers - changing ad spend isn&apos;t always the top lever.
                                        </div>
                                      </div>
                                      <div style={{ display: "grid", gridTemplateColumns: `repeat(auto-fit, minmax(200px, 1fr))`, gap: 14 }}>
                                        {topRecs.map((s, i) => (
                                          <div key={i} style={{ background: "#FFFFFF", border: "1px solid #E5E7EB", borderRadius: 12, padding: "20px 24px" }}>
                                            <div style={{ display: "flex", alignItems: "flex-start", gap: 10, marginBottom: 8 }}>
                                              <span style={{ display: "inline-flex", alignItems: "center", justifyContent: "center", width: 24, height: 24, borderRadius: "50%", background: accent, color: "#FFFFFF", fontSize: 12, fontWeight: 800, flexShrink: 0 }}>{i + 1}</span>
                                              <span>
                                                <span style={{ display: "block", fontSize: 10, fontWeight: 700, color: "#9CA3AF", textTransform: "uppercase", letterSpacing: 0.5 }}>{s.area}</span>
                                                <span style={{ fontSize: 13, fontWeight: 700, color: "#1F2937" }}>{s.title}</span>
                                              </span>
                                            </div>
                                            <div style={{ fontSize: 12, color: "#4B5563", lineHeight: 1.55 }}>{s.body}</div>
                                          </div>
                                        ))}
                                      </div>
                                      <div style={{ fontSize: 11, color: "#9CA3AF", marginTop: 8 }}>
                                        Assumes new customers behave like your historic ones - at much higher spend they may not. That&apos;s what the Early Signals below are for. All figures use your gross margin setting ({marginLbl}%).
                                      </div>
                                    </div>
                                  );
                                })()}
                                {/* EARLY SIGNALS - leading indicators for
                                    judging a spend increase before the LTV
                                    curve can. Newest monthly cohorts vs the
                                    long-term customer at the same age. */}
                                {isMeta && signalRows.length > 0 && (() => {
                                  const fmtYm = (ym: string) => {
                                    const [y, mo] = ym.split("-").map(Number);
                                    return new Date(y, mo - 1, 1).toLocaleDateString("en-GB", { month: "short", year: "numeric" });
                                  };
                                  const pct = (v: number | null, flipGood = false) => {
                                    if (v == null) return { label: "-", color: "#9CA3AF" };
                                    const good = flipGood ? v <= 0 : v >= 0;
                                    return {
                                      label: `${v >= 0 ? "+" : ""}${Math.round(v * 100)}%`,
                                      color: Math.abs(v) < 0.05 ? "#6B7280" : good ? "#059669" : "#DC2626",
                                    };
                                  };
                                  const statusOf = (r: SignalRow) => {
                                    const basis = r.trajDelta ?? r.firstDelta;
                                    if (basis == null) return { label: "Too early", bg: "#F3F4F6", color: "#6B7280" };
                                    if (basis >= 0.1) return { label: "Ahead", bg: "#ECFDF5", color: "#059669" };
                                    if (basis >= -0.1) return { label: "On track", bg: "#EFF6FF", color: "#2563EB" };
                                    return { label: "Behind", bg: "#FEF2F2", color: "#DC2626" };
                                  };
                                  const latest = signalRows[0];
                                  return (
                                    <div style={{ marginTop: 14, padding: "16px 18px", background: "#FFFFFF", border: "1px solid #E5E7EB", borderRadius: 10 }}>
                                      <div style={{ fontSize: 15, fontWeight: 800, color: "#1F2937", marginBottom: 4 }}>Early signals - is increased spend working?</div>
                                      <div style={{ fontSize: 12, color: "#6B7280", lineHeight: 1.5, marginBottom: 12 }}>
                                        The LTV curve above is historic - it can&apos;t tell you quickly whether extra spend is working. These leading indicators judge your newest monthly cohorts against your long-term customer at the same age.
                                        If CAC jumps <em>and</em> new cohorts run behind the curve, scaling is buying worse customers. If cohorts hold the curve while volume grows, it&apos;s working - even while ROAS dips.
                                      </div>
                                      {!spendRising && (
                                        <div style={{ marginBottom: 12, padding: "10px 14px", background: "#F9FAFB", border: "1px solid #E5E7EB", borderRadius: 8, fontSize: 12.5, color: "#4B5563", lineHeight: 1.5 }}>
                                          <strong>Spend is steady</strong>{latest && latest.spend > 0 && prevMonthSpend > 0 ? <> - last month {cs}{Math.round(latest.spend).toLocaleString()} vs {cs}{Math.round(prevMonthSpend).toLocaleString()} the month before</> : null}. These signals matter most after a budget change, so they&apos;re shown greyed for reference. They&apos;ll light up automatically when a month&apos;s spend moves ~10% or more above the month before it.
                                        </div>
                                      )}
                                      <div style={{ overflowX: "auto", opacity: spendRising ? 1 : 0.45, transition: "opacity 0.2s" }}>
                                        <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12.5 }}>
                                          <thead>
                                            <tr style={{ color: "#6B7280", textAlign: "left" }}>
                                              <th style={{ padding: "6px 10px", fontWeight: 600 }}>Cohort</th>
                                              <th style={{ padding: "6px 10px", fontWeight: 600 }}>Spend (vs prior month)</th>
                                              <th style={{ padding: "6px 10px", fontWeight: 600 }}>New customers</th>
                                              <th style={{ padding: "6px 10px", fontWeight: 600 }}>CAC (vs 6-mo baseline)</th>
                                              <th style={{ padding: "6px 10px", fontWeight: 600 }}>First order (vs long-term)</th>
                                              <th style={{ padding: "6px 10px", fontWeight: 600 }}>LTV pace at same age</th>
                                              <th style={{ padding: "6px 10px", fontWeight: 600 }}>Status</th>
                                            </tr>
                                          </thead>
                                          <tbody>
                                            {signalRows.map((r) => {
                                              const cacP = pct(r.cacDelta, true);
                                              const firstP = pct(r.firstDelta);
                                              const trajP = pct(r.trajDelta);
                                              const st = statusOf(r);
                                              // Spend delta is context, not good/bad -
                                              // rendered neutral grey either way.
                                              const spendLbl = r.spendDelta != null ? `${r.spendDelta >= 0 ? "+" : ""}${Math.round(r.spendDelta * 100)}%` : null;
                                              return (
                                                <tr key={r.month} style={{ borderTop: "1px solid #F3F4F6", color: "#1F2937" }}>
                                                  <td style={{ padding: "7px 10px", fontWeight: 700 }}>{fmtYm(r.month)}</td>
                                                  <td style={{ padding: "7px 10px" }}>
                                                    {r.spend > 0 ? `${cs}${Math.round(r.spend).toLocaleString()}` : "-"}
                                                    {spendLbl && <span style={{ color: "#6B7280", fontWeight: 700, marginLeft: 6 }}>{spendLbl}</span>}
                                                  </td>
                                                  <td style={{ padding: "7px 10px" }}>{r.n.toLocaleString()}</td>
                                                  <td style={{ padding: "7px 10px" }}>
                                                    {r.cohortCac > 0 ? `${cs}${Math.round(r.cohortCac).toLocaleString()}` : "-"}
                                                    {r.cacDelta != null && <span style={{ color: cacP.color, fontWeight: 700, marginLeft: 6 }}>{cacP.label}</span>}
                                                  </td>
                                                  <td style={{ padding: "7px 10px" }}>
                                                    {r.avgFirst > 0 ? `${cs}${Math.round(r.avgFirst).toLocaleString()}` : "-"}
                                                    {r.firstDelta != null && <span style={{ color: firstP.color, fontWeight: 700, marginLeft: 6 }}>{firstP.label}</span>}
                                                  </td>
                                                  <td style={{ padding: "7px 10px" }}>
                                                    {r.trajDelta != null
                                                      ? <><span style={{ color: trajP.color, fontWeight: 700 }}>{trajP.label}</span><span style={{ color: "#9CA3AF", marginLeft: 6 }}>at month {r.ageMonths}</span></>
                                                      : <span style={{ color: "#9CA3AF" }}>too early ({r.ageMonths < 1 ? "<1 month old" : "small sample"})</span>}
                                                  </td>
                                                  <td style={{ padding: "7px 10px" }}>
                                                    <span style={{ display: "inline-block", padding: "2px 10px", borderRadius: 999, fontSize: 11.5, fontWeight: 700, background: st.bg, color: st.color }}>{st.label}</span>
                                                  </td>
                                                </tr>
                                              );
                                            })}
                                          </tbody>
                                        </table>
                                      </div>
                                      <div style={{ fontSize: 11, color: "#9CA3AF", marginTop: 10, lineHeight: 1.5 }}>
                                        <strong>How to read this while scaling:</strong> expect CAC to drift up as you raise spend - that&apos;s normal. The signal that matters is <em>LTV pace</em>: whether new cohorts track your long-term curve at the same age.
                                        First-order value is the earliest proxy (visible within days); LTV pace firms up from ~30 days; the curve above only settles after months. Judge a budget change over 2-4 weeks of cohort data, not a single day&apos;s ROAS.
                                      </div>
                                    </div>
                                  );
                                })()}
                              </div>
                            );
                          })()}
                        </div>
                      );
                    })()}
                    {benchmarkWindows.length === 0 && count > 0 && (
                      <div style={{ borderTop: "1px solid #E5E7EB", paddingTop: "16px", textAlign: "center", color: "#9CA3AF", fontSize: "13px" }}>
                        Not enough mature customers yet for LTV progression (need at least 5 customers acquired 30+ days ago).
                      </div>
                    )}
                  </div>
                );
              })()}
            </BlockStack>
          </Card>
          )},
          { id: "weeklyCohortRevenue", label: "Revenue by Weekly Cohort", span: 4, render: () => (
            <Card>
              <WeeklyCohortRevenue weekly={weeklyCohortSeries} cs={cs} />
            </Card>
          )},
        ] as TileDef[]} />

        {/* Journey-dependent views (web pixel). Greyed until touches arrive.
            Shown only on the HM + Vollebak apps while being validated (per-app
            JOURNEY_REPORTS_ENABLED flag); hidden on the public app. */}
        {journeyReportsEnabled && (
          <>
            <AwaitingDataTile
              title="Customer journey timeline"
              message="Click-by-click journeys are being collected from your storefront. Each customer's path from Meta ad to checkout will appear here automatically as soon as the first journeys arrive."
              preview={<JourneyTimelinePreview />}
            />
            <AwaitingDataTile
              title="Acquisition paths"
              message="The routes Meta-acquired customers take before they buy are being collected from your storefront. This breakdown will appear automatically once enough journeys have been captured."
              preview={<AcquisitionPathsPreview />}
            />
          </>
        )}

        {/* Order Explorer (moved here from /app/orders) — every Shopify
            order in the selected period, tagged by Meta attribution. */}
        <OrderExplorerSection
          rows={orderExplorer.rows}
          campaigns={orderExplorer.campaigns}
          currencySymbol={orderExplorer.currencySymbol}
          tagFilter={searchParams.get("orderTag") || "meta"}
          campaignFilter={searchParams.get("orderCampaign") || "all"}
          onTagChange={(v) => {
            const next = new URLSearchParams(searchParams);
            next.set("orderTag", v);
            setSearchParams(next, { replace: true });
          }}
          onCampaignChange={(v) => {
            const next = new URLSearchParams(searchParams);
            next.set("orderCampaign", v);
            setSearchParams(next, { replace: true });
          }}
        />
      </BlockStack>
      </ReportTabs>
    </Page>
  );
}
