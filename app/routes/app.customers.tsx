import { json } from "@remix-run/node";
import { useLoaderData, useSearchParams, useSubmit, useActionData, useRevalidator } from "@remix-run/react";
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
import AiInsightsPanel from "../components/AiInsightsPanel";
import PageSummary from "../components/PageSummary";
import type { SummaryBullet, SummaryTone } from "../components/PageSummary";
import SummaryTile from "../components/SummaryTile";
import type { ColumnDef } from "@tanstack/react-table";

// ═══════════════════════════════════════════════════════════════
// LOADER
// ═══════════════════════════════════════════════════════════════

export const loader = async ({ request }) => {
  const { session } = await authenticate.admin(request);
  const shopDomain = session.shop;
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
    insights, prevInsights, allTimeSpendResult,
    ageRaw, genderRaw,
    blobs,
    aiCached,
    unmatchedAttrs,
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
      `${shopDomain}:insightsDaily:${dateFromStr}:${dateToStr}`, DEFAULT_TTL,
      () => db.dailyAdRollup.groupBy({
        by: ["date"],
        where: { shopDomain, date: { gte: fromDate, lte: toDate } },
        _sum: { spend: true, metaConversions: true, metaConversionValue: true },
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
    time("blobs", queryCached(`${shopDomain}:customersBlobs`, DEFAULT_TTL, loadAnalysisBlobs)),
    time("aiCache", getCachedInsights(shopDomain, "customers", dateFromStr, dateToStr)),
    // Unmatched attribution rows (confidence=0) — Meta conversions the matcher
    // couldn't tie to a Shopify order. shopifyOrderId is a synthetic key of the
    // form `unmatched_<adId>_<YYYY-MM-DD>...`, so the date extracts by regex.
    // Pulled for the union of current + previous ranges; bucketed per-day below.
    time("unmatchedAttrs", db.attribution.findMany({
      where: { shopDomain, confidence: 0, shopifyOrderId: { startsWith: "unmatched_" } },
      select: { shopifyOrderId: true, metaConversionValue: true },
    })),
  ]);
  console.log(`[customers] db ${Date.now() - _qStart}ms (customers=${customers.length}, dailyRollups=${dailyRollups.length})`);

  const shop = shopForTz;
  const currencySymbol = currencySymbolFromCode(shop?.shopifyCurrency);

  const ltvBlob = blobs.ltv;
  const journeyBlob = blobs.journey;
  const geoBlob = blobs.geo;

  // ── Build per-customer table rows from Customer model (cached, expensive at 15k+ rows) ──
  // The output depends only on `customers` (which is itself cached) and current time
  // for daysSince calculations. The 2-hour TTL is fine — those values change daily.
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
  const allTimeMetaSpend = allTimeSpendResult._sum?.spend || 0;

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
  // Value>0 counters power the Total Meta Orders tile — £0 unmatched
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
      dailyMap[key] = { date: key, metaCustomers: 0, organicCustomers: 0, metaRevenue: 0, organicRevenue: 0, spend: 0, newMetaCustomers: 0, newMetaRevenue: 0, metaRepeatCustomers: 0 };
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
  const newInPeriod = metaCount;
  const newCustomerCPA = newInPeriod > 0 ? r2(totalMetaSpend / newInPeriod) : 0;
  const metaAvgFirstOrder = metaCount > 0 ? r2(metaFirstOrderTotal / metaCount) : 0;
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
  const genderBreakdown = genderRaw
    .map(r => ({
      label: r.breakdownValue === "male" ? "Male" : r.breakdownValue === "female" ? "Female" : "Unknown",
      conversions: r._sum?.conversions || 0,
      spend: r._sum?.spend || 0,
      revenue: r._sum?.conversionValue || 0,
    }))
    .filter(g => g.conversions > 0)
    .sort((a, b) => b.conversions - a.conversions);
  const totalDemoConversions = ageBreakdown.reduce((s, a) => s + a.conversions, 0);

  // New-Meta demographics — pulled from Attribution rows (which carry
  // per-order metaAge/metaGender from the breakdown enrichment step),
  // filtered to isNewCustomer=true within the date range. More precise
  // than Meta's aggregate breakdown because it's per-matched-order.
  const newMetaAttrsWithDemo = await queryCached(
    `${shopDomain}:newMetaDemoAttrs:${dateFromStr}:${dateToStr}`,
    DEFAULT_TTL,
    () => db.attribution.findMany({
      where: {
        shopDomain,
        confidence: { gt: 0 },
        isNewCustomer: true,
        metaAge: { not: null },
        matchedAt: { gte: fromDate, lte: toDate },
      },
      select: { metaAge: true, metaGender: true, metaConversionValue: true },
    }),
  );
  const newAgeAgg: Record<string, { conversions: number; value: number; spend: number; impressions: number }> = {};
  const newGenderAgg: Record<string, { conversions: number; value: number; spend: number; impressions: number }> = {};
  for (const a of newMetaAttrsWithDemo) {
    if (a.metaAge) {
      if (!newAgeAgg[a.metaAge]) newAgeAgg[a.metaAge] = { conversions: 0, value: 0, spend: 0, impressions: 0 };
      newAgeAgg[a.metaAge].conversions++;
      newAgeAgg[a.metaAge].value += a.metaConversionValue || 0;
    }
    if (a.metaGender) {
      if (!newGenderAgg[a.metaGender]) newGenderAgg[a.metaGender] = { conversions: 0, value: 0, spend: 0, impressions: 0 };
      newGenderAgg[a.metaGender].conversions++;
      newGenderAgg[a.metaGender].value += a.metaConversionValue || 0;
    }
  }
  // Approximate new-customer spend per age/gender via the all-Meta aggregate
  // breakdown's average CPA × new conversions. We don't have per-attribution
  // spend (spend lives on MetaBreakdown, not Attribution), so this is the
  // best signal available until spend is plumbed through the matcher.
  const newAgeBreakdown = AGE_ORDER
    .map(age => {
      const s = newAgeAgg[age];
      const conversions = s?.conversions || 0;
      const allMetaAge = ageBreakdown.find(a => a.label === age);
      const avgCpa = allMetaAge && allMetaAge.conversions > 0 ? allMetaAge.spend / allMetaAge.conversions : 0;
      return { label: age, conversions, spend: avgCpa * conversions, revenue: s?.value || 0 };
    })
    .filter(a => a.conversions > 0);
  const newGenderBreakdown = Object.entries(newGenderAgg)
    .map(([raw, s]) => {
      const label = raw === "male" ? "Male" : raw === "female" ? "Female" : "Unknown";
      const allMetaG = genderBreakdown.find(g => g.label === label);
      const avgCpa = allMetaG && allMetaG.conversions > 0 ? allMetaG.spend / allMetaG.conversions : 0;
      return {
        label,
        conversions: s.conversions,
        spend: avgCpa * s.conversions,
        revenue: s.value,
      };
    })
    .filter(g => g.conversions > 0)
    .sort((a, b) => b.conversions - a.conversions);
  const newDemoConversions = newMetaAttrsWithDemo.length;
  const newDemoExactCount = 0;

  // Date-scoped geography — computed at loader time from orders placed in
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
        select: { shopifyOrderId: true, isNewCustomer: true },
      });
      const metaByOrderId = new Map<string, { isNew: boolean }>();
      for (const a of attrs) metaByOrderId.set(a.shopifyOrderId, { isNew: !!a.isNewCustomer });

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
        const meta = metaByOrderId.get(o.shopifyOrderId);
        if (meta) {
          if (cust) metaCusts.add(cust);
          add(mCountry, o.country || null, cust, net);
          add(mCity, o.city || null, cust, net);
          if (meta.isNew) {
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

  // Date-scoped customer journey — customers whose FIRST order fell in the
  // selected period. Fetch their first three orders (online store only) and
  // compute median AOV + gap stats per scope (meta-new vs all). Replaces the
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

      const median = (arr: number[]): number => {
        if (arr.length === 0) return 0;
        const s = [...arr].sort((a, b) => a - b);
        const m = Math.floor(s.length / 2);
        return r2(s.length % 2 === 0 ? (s[m - 1] + s[m]) / 2 : s[m]);
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
          firstAOV: median(firsts),
          secondAOV: median(seconds),
          thirdAOV: median(thirds),
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

  // Date-scoped Customer Breakdown — replicates Order Explorer's exact
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
      const empty = { metaNew: { customers: 0, revenue: 0 }, metaRepeat: { customers: 0, revenue: 0 }, metaRetargeted: { customers: 0, revenue: 0 } };
      const orders = await db.order.findMany({
        // Exclude £0 orders (staff / replacement / warranty) — same rule
        // Order Explorer + the rollup apply so customer counts agree.
        where: {
          shopDomain,
          createdAt: { gte: fromDate, lte: toDate },
          frozenTotalPrice: { gt: 0 },
        },
        select: {
          shopifyOrderId: true, shopifyCustomerId: true,
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

        if (tag === "metaNew") { newCusts.add(custId); newRev += net; }
        else if (tag === "metaRepeat") { repeatCusts.add(custId); repeatRev += net; }
        else if (tag === "metaRetargeted") { retargetedCusts.add(custId); retargetedRev += net; }
      }

      return {
        metaNew: { customers: newCusts.size, revenue: r2(newRev) },
        metaRepeat: { customers: repeatCusts.size, revenue: r2(repeatRev) },
        metaRetargeted: { customers: retargetedCusts.size, revenue: r2(retargetedRev) },
      };
    },
  );

  // Single/Repeat splits — derive from customers
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

  // Weekly cohort revenue — one bucket per ISO week the customer was
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
  // order row at sync time) as ground truth for "genuinely new" —
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
    // first-ever purchase — their totalSpent reflects years of prior
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

  // LTV from cache blob — also synthesize tile.cpa + tile.paybackOrders for the component
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
    timeTo2nd: number | null; acqMonth: string;
    ltvByWindow: Record<string, number>;
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

  // Unmatched conversions — count actual Attribution rows with confidence=0
  // whose synthetic shopifyOrderId date falls in the range. This matches the
  // Weekly Report's approach exactly, so the two pages always agree.
  // (Previously computed as `max(0, totalMetaConversions - metaCount)`, which
  // subtracted new-customer count from total Meta-reported conversions — two
  // different populations — and inflated whenever Meta reported more total
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
    paybackOrders, newInPeriod,
    currencySymbol,
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
    ageBreakdown, genderBreakdown, newAgeBreakdown, newGenderBreakdown,
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
    metaAvgAov, totalMetaSpend,
    unmatchedConversions, unmatchedRevenue,
    ltvBenchmark, ltvTile, ltvRecent, ltvMonthly, ltvCustomers,
    weeklyCohortSeries,
    fromKey, toKey, preset,
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

  return json({});
};

// ═══════════════════════════════════════════════════════════════
// INFOGRAPHIC COMPONENTS
// ═══════════════════════════════════════════════════════════════

// ── Donut Chart ──
// `pathLength="1"` makes each arc render in 0–1 space rather than
// `fraction * circumference`, eliminating the floating-point rounding that
// previously caused adjacent segments to overlap by a fraction of a pixel.
// `hovered` / `onHoverChange` allow the parent to link legend-row hover to
// segment hover bidirectionally; if omitted, the component falls back to
// internal hover state so drop-in callers still work.
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

  let cumulativeFraction = 0;
  const arcs = segments.filter(s => s.value > 0).map((seg, i) => {
    const fraction = seg.value / total;
    const offset = -cumulativeFraction;
    cumulativeFraction += fraction;
    return { ...seg, offset, fraction, index: i };
  });

  // Pad the SVG so the hover-expanded stroke (thickness + 6) isn't cropped
  // by the viewBox. The outer box grows by `pad` on every side; the wrapper
  // div keeps its nominal `size` so layout (and the centred label overlay)
  // stay put, and we offset the SVG with negative top/left to re-centre it.
  const pad = 8;
  const svgSize = size + pad * 2;
  return (
    <div style={{ position: "relative", width: size, height: size }}>
      <svg
        width={svgSize}
        height={svgSize}
        viewBox={`0 0 ${svgSize} ${svgSize}`}
        style={{ position: "absolute", top: -pad, left: -pad, overflow: "visible" }}
      >
        {arcs.map((arc) => (
          <circle
            key={arc.label}
            cx={center + pad} cy={center + pad} r={radius}
            fill="none"
            stroke={arc.color}
            strokeWidth={hovered === arc.index ? thickness + 6 : thickness}
            pathLength={1}
            strokeDasharray={`${arc.fraction} ${1 - arc.fraction}`}
            strokeDashoffset={arc.offset}
            transform={`rotate(-90 ${center + pad} ${center + pad})`}
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
            <div style={{ fontSize: "22px", fontWeight: 800, color: arcs[hovered].color }}>{fmt(arcs[hovered].value)}</div>
            <div style={{ fontSize: "11px", color: "#6B7280", fontWeight: 500 }}>{arcs[hovered].label}</div>
            <div style={{ fontSize: "11px", color: "#9CA3AF" }}>{Math.round(arcs[hovered].fraction * 100)}%</div>
          </>
        ) : (
          <>
            <div style={{ fontSize: "26px", fontWeight: 800, color: "#1F2937" }}>{centerValue}</div>
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
  const rowHeight = 22;
  const rowGap = 6;
  const needsScroll = visible.length > cap;
  const scrollHeight = cap * rowHeight + (cap - 1) * rowGap;

  return (
    <div style={{
      display: "flex", flexDirection: "column", gap: `${rowGap}px`, width: "100%",
      ...(needsScroll ? { maxHeight: `${scrollHeight}px`, overflowY: "auto", paddingRight: "4px" } : {}),
    }}>
      {visible.map((item, i) => {
        const pct = sumTotal > 0 ? Math.round((item.value / sumTotal) * 100) : 0;
        return (
        <div key={item.label} style={{ display: "flex", alignItems: "center", gap: "8px" }}>
          <div style={{ width: "70px", fontSize: "12px", color: "#4B5563", fontWeight: 500, textAlign: "right", flexShrink: 0 }}>
            {item.label}
          </div>
          <div style={{ flex: 1, height: `${rowHeight}px`, background: "#F3F4F6", borderRadius: "4px", overflow: "hidden", position: "relative" }}>
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

  const aov2Change = firstAOV > 0 ? Math.round(((secondAOV - firstAOV) / firstAOV) * 100) : 0;
  const aov3Change = secondAOV > 0 ? Math.round(((thirdAOV - secondAOV) / secondAOV) * 100) : 0;
  const repeatRate = firstOrderCount > 0 ? Math.round((secondOrderCount / firstOrderCount) * 100) : 0;
  const thirdRate = secondOrderCount > 0 ? Math.round((thirdOrderCount / secondOrderCount) * 100) : 0;

  const orderBox = (label: string, aov: number, aovChange: number, count: number, countLabel: string, gradient: string, shadow: string, hasData: boolean) => (
    <div style={{ display: "flex", flexDirection: "column", alignItems: "center", flex: "0 0 auto" }}>
      <div style={{
        background: hasData ? gradient : "#D1D5DB",
        borderRadius: "13px", padding: "20px 28px", color: "#fff", textAlign: "center",
        minWidth: "130px", boxShadow: hasData ? shadow : "none",
      }}>
        <div style={{ fontSize: "11px", fontWeight: 500, opacity: 0.8, textTransform: "uppercase", letterSpacing: "0.5px" }}>{label}</div>
        <div style={{ fontSize: "28px", fontWeight: 800, marginTop: "5px" }}>
          {hasData ? `${cs}${Math.round(aov)}` : "—"}
        </div>
        <div style={{ fontSize: "11px", opacity: 0.7, marginTop: "2px" }}>
          {hasData ? (
            <>avg AOV{aovChange !== 0 && (
              <span style={{ color: aovChange > 0 ? "#86EFAC" : "#FCA5A5" }}>
                {" "}({aovChange > 0 ? "+" : ""}{aovChange}%)
              </span>
            )}</>
          ) : "no data yet"}
        </div>
      </div>
      <div style={{ fontSize: "12px", color: "#6B7280", fontWeight: 500, marginTop: "8px" }}>
        {count.toLocaleString()} {countLabel}
      </div>
    </div>
  );

  const arrow = (days: number | null, rate: number, rateLabel: string, gradId: string, colorFrom: string, colorTo: string) => (
    <div style={{ display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", padding: "0 8px", minWidth: "110px", flex: "0 0 auto" }}>
      <div style={{ display: "flex", flexDirection: "column", alignItems: "center" }}>
        <svg width="110" height="32" viewBox="0 0 110 32">
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
            {days != null ? `${days} days` : "—"}
          </text>
        </svg>
      </div>
      <div style={{ fontSize: "11px", color: "#6B7280", fontWeight: 500, marginTop: "3px" }}>
        {rate}% {rateLabel}
      </div>
    </div>
  );

  return (
    <div style={{ display: "flex", alignItems: "center", justifyContent: "center", gap: "0", padding: "22px 12px" }}>
      {orderBox("1st Order", firstAOV, 0, firstOrderCount, "acquired",
        "linear-gradient(135deg, #7C3AED 0%, #6D28D9 100%)", "0 4px 12px rgba(124,58,237,0.3)", true)}
      {arrow(gapDays, repeatRate, "came back", "arrowGrad1", "#7C3AED", "#0891B2")}
      {orderBox("2nd Order", secondAOV, aov2Change, secondOrderCount, "repeated",
        "linear-gradient(135deg, #0891B2 0%, #0E7490 100%)", "0 4px 12px rgba(8,145,178,0.3)", secondOrderCount > 0)}
      {arrow(gap2to3Days, thirdRate, "came back", "arrowGrad2", "#0891B2", "#2E7D32")}
      {orderBox("3rd Order", thirdAOV, aov3Change, thirdOrderCount, "repeated",
        "linear-gradient(135deg, #2E7D32 0%, #1B5E20 100%)", "0 4px 12px rgba(46,125,50,0.3)", thirdOrderCount > 0)}
    </div>
  );
}


// ═══════════════════════════════════════════════════════════════
// Revenue by Weekly Cohort — stacked bars, first-order revenue at the
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
    // 365 days back from the latest cohort in the dataset — 52-ish weeks.
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
            <button className={`toggle-btn ${segment === "meta" ? "active" : ""}`} onClick={() => setSegment("meta")}>Meta Customers</button>
            <button className={`toggle-btn ${segment === "all" ? "active" : ""}`} onClick={() => setSegment("all")}>All Customers</button>
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
        // customers who came back — the real LTV signal.
        const PLOT_H = 200;
        const lineColor = "#F59E0B"; // amber — contrasts indigo bars
        return (
          <div ref={wrapRef} style={{ position: "relative" }}>
          <div style={{ display: "flex", gap: 8 }}>
            {/* Left Y-axis: £ revenue */}
            <div style={{ width: 44, flexShrink: 0, display: "flex", flexDirection: "column", justifyContent: "space-between", fontSize: 10, color: "#9CA3AF", paddingTop: 2, paddingBottom: 18 }}>
              <span>{fmtMoney(maxVal)}</span>
              <span>{fmtMoney(maxVal / 2)}</span>
              <span>{cs}0</span>
            </div>
            <div style={{ flex: 1, position: "relative", height: 220 }}>
              {/* Gridlines */}
              {[0, 0.5, 1].map((f) => (
                <div key={f} style={{
                  position: "absolute", left: 0, right: 0, top: `${(1 - f) * 100 * (PLOT_H / 220)}%`,
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
            <div style={{ width: 36, flexShrink: 0, display: "flex", flexDirection: "column", justifyContent: "space-between", fontSize: 10, color: lineColor, fontWeight: 600, paddingTop: 2, paddingBottom: 18, textAlign: "right" }}>
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

.toggle-group { display: inline-flex; border: 1px solid #D1D5DB; border-radius: 5px; overflow: hidden; }
.toggle-btn { padding: 4px 10px; font-size: 11px; font-weight: 500; border: none; cursor: pointer; transition: all 0.15s; white-space: nowrap; }
.toggle-btn.active { background: #0E7490; color: #fff; }
.toggle-btn:not(.active) { background: #fff; color: #374151; }
.toggle-btn:not(.active):hover { background: #F3F4F6; }

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
    paybackOrders, newInPeriod,
    currencySymbol,
    ageBreakdown, genderBreakdown, newAgeBreakdown, newGenderBreakdown,
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
    metaAvgAov, totalMetaSpend,
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
  const { aiCachedInsights, aiGeneratedAt, aiIsStale } = data;
  const [searchParams, setSearchParams] = useSearchParams();
  const [acqMode, setAcqMode] = useState<"customers" | "revenue">("customers");
  const [donutHover, setDonutHover] = useState<number | null>(null);
  const [demoScope, setDemoScope] = useState<"new" | "allMeta">("new");
  const [demoMetric, setDemoMetric] = useState<"cpa" | "roas" | "aov">("cpa");
  const [geoScope, setGeoScope] = useState<"new" | "allMeta" | "all">("new");
  const [geoMetric, setGeoMetric] = useState<"rev" | "cpa" | "roas" | "aov">("rev");
  const [journeyScope, setJourneyScope] = useState<"meta" | "all">("meta");
  const [ltvTab, setLtvTab] = useState<"meta" | "all">("meta");
  const [ltvView, setLtvView] = useState<"progression" | "cohorts">("progression");
  const [cohortMetric, setCohortMetric] = useState<"ltv" | "retention">("ltv");

  // ── LTV exploration state ─────────────────────────────────────────
  // Filters target the metaNew cohort (ltvCustomers) to answer
  // "which segments have the highest LTV?". Persistent via localStorage
  // so the explorer remembers the user's last cut between sessions.
  const [ltvFilterGender, setLtvFilterGender] = useState<"All" | "male" | "female">("All");
  const [ltvFilterAges, setLtvFilterAges] = useState<string[]>([]); // empty = all
  const [ltvFilterCountry, setLtvFilterCountry] = useState<string>("All");
  const [ltvWindowPreset, setLtvWindowPreset] = useState<"lifetime" | 30 | 60 | 90 | 180 | 365>("lifetime");
  // Gross margin % for the profit-payback calc. Default 60 — reasonable
  // midpoint for DTC/fashion. Revenue-based payback was misleading
  // ("1 order = payback" sounds great but ROAS=1 doesn't cover product
  // cost, fulfilment, fees). Persist per-browser — eventually migrate
  // to Shop.defaultMargin.
  const [marginPct, setMarginPct] = useState<number>(60);
  const [chartHover, setChartHover] = useState<{ window: number; bench: number; recent: number | null } | null>(null);

  // Load persisted prefs on mount
  useEffect(() => {
    try {
      const raw = localStorage.getItem("lucidly.ltvExplorer");
      if (raw) {
        const p = JSON.parse(raw);
        if (p.gender) setLtvFilterGender(p.gender);
        if (Array.isArray(p.ages)) setLtvFilterAges(p.ages);
        if (p.country) setLtvFilterCountry(p.country);
        if (p.window) setLtvWindowPreset(p.window);
        if (typeof p.margin === "number" && p.margin >= 0 && p.margin <= 100) setMarginPct(p.margin);
      }
    } catch {}
  }, []);
  useEffect(() => {
    try {
      localStorage.setItem("lucidly.ltvExplorer", JSON.stringify({
        gender: ltvFilterGender, ages: ltvFilterAges, country: ltvFilterCountry,
        window: ltvWindowPreset, margin: marginPct,
      }));
    } catch {}
  }, [ltvFilterGender, ltvFilterAges, ltvFilterCountry, ltvWindowPreset, marginPct]);

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
    // Per-window avg LTV over the filtered subset (mirrors ltvBenchmark
    // shape so the chart can consume it interchangeably).
    const windows = [30, 60, 90, 180, 365];
    const byWindow = windows.map((w) => {
      const wStr = String(w);
      const mature = subset.filter((c) => c.ltvByWindow[wStr] !== undefined);
      if (mature.length < 3) return null; // under-powered, don't show
      const sum = mature.reduce((s, c) => s + (c.ltvByWindow[wStr] || 0), 0);
      return { window: w, count: mature.length, avgLtv: Math.round((sum / mature.length) * 100) / 100 };
    }).filter(Boolean) as Array<{ window: number; count: number; avgLtv: number }>;
    return {
      filterActive, count, avgLtv, avgFirst, avgOrders: count > 0 ? Math.round((totalOrders / count) * 100) / 100 : 0,
      repeatRate, medianTimeTo2nd: medT2 != null ? Math.round(medT2) : null,
      benchmarkWindows: byWindow,
    };
  }, [ltvCustomers, ltvFilterGender, ltvFilterAges, ltvFilterCountry]);

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

  // Customer breakdown donut — segments sized by unique customers (default)
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
    if (v == null) return "—";
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
      cell: ({ getValue }) => getValue() || "—" },
    { accessorKey: "acquisitionAdSet", header: "Acquisition Ad Set",
      meta: { maxWidth: "180px", description: "The Meta ad set that first brought this customer" },
      cell: ({ getValue }) => getValue() || "—" },
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
      cell: ({ getValue }) => getValue() ? `${cs}${Math.round(getValue()).toLocaleString()}` : "—" },
    { accessorKey: "totalRefunded", header: "Refunded",
      meta: { align: "right", description: "Total refund amount across all orders" },
      cell: ({ getValue }) => getValue() > 0 ? `${cs}${Math.round(getValue()).toLocaleString()}` : "—" },
    { accessorKey: "netRevenue", header: "Net Revenue",
      meta: { align: "right", description: "Revenue after all refunds — the customer's true lifetime value", calc: "Gross Revenue - Refunded" },
      cell: ({ getValue }) => `${cs}${Math.round(getValue()).toLocaleString()}` },
    { accessorKey: "avgOrderValue", header: "AOV",
      meta: { align: "right", description: "Average order value for this customer", calc: "Gross Revenue / Orders" },
      cell: ({ getValue }) => getValue() ? `${cs}${Math.round(getValue()).toLocaleString()}` : "—" },
    { accessorKey: "firstOrderValue", header: "1st Order",
      meta: { align: "right", description: "Value of the customer's first order" },
      cell: ({ getValue }) => getValue() ? `${cs}${Math.round(getValue()).toLocaleString()}` : "—" },
    { accessorKey: "ltvMultiplier", header: "LTV Multiplier",
      meta: { align: "right", description: "How much more the customer has spent beyond their first order", calc: "Gross Revenue / First Order Value" },
      cell: ({ getValue }) => getValue() != null ? `${getValue()}x` : "—" },
    { accessorKey: "lastOrderDate", header: "Last Order",
      meta: { description: "Date of the customer's most recent order" } },
    { accessorKey: "daysSinceLastOrder", header: "Days Since Last",
      meta: { align: "right", description: "Days since their most recent order — high numbers may indicate churn" } },
    { accessorKey: "daysSinceAcquisition", header: "Customer Age",
      meta: { align: "right", description: "Days since first order — how long they've been a customer" },
      cell: ({ getValue }) => `${getValue()}d` },
    { accessorKey: "timeTo2ndOrder", header: "Days to 2nd Order",
      meta: { align: "right", description: "Days between their first and second purchase. Key retention indicator — shorter is better" },
      cell: ({ getValue }) => getValue() != null ? `${getValue()}d` : "—" },
    { accessorKey: "country", header: "Country",
      meta: { filterType: "multi-select", description: "Customer's billing country (from first order)" },
      filterFn: "multiSelect",
      cell: ({ getValue }) => getValue() || "—" },
    { accessorKey: "city", header: "City",
      meta: { description: "Customer's billing city (from first order)" },
      cell: ({ getValue }) => getValue() || "—" },
    { accessorKey: "topProducts", header: "Top Products",
      meta: { maxWidth: "200px", description: "Most frequently purchased products by this customer" },
      cell: ({ getValue }) => getValue() || "—" },
    { accessorKey: "discountOrders", header: "Discount Orders",
      meta: { align: "right", description: "Number of orders where a discount code was used" } },
    { accessorKey: "refundRate", header: "Refund Rate",
      meta: { align: "right", description: "Percentage of gross revenue that was refunded", calc: "Refunded / Gross Revenue x 100" },
      cell: ({ getValue }) => getValue() > 0 ? `${getValue()}%` : "—" },
    { accessorKey: "avgConfidence", header: "Avg Confidence",
      meta: { align: "right", description: "Average attribution confidence across all Meta-matched orders for this customer" },
      cell: ({ getValue }) => getValue() != null ? `${getValue()}%` : "—" },
  ], [cs]);

  const defaultVisibleColumns = useMemo(() => [
    "tag", "acquisitionDate", "totalOrders", "netRevenue",
    "avgOrderValue", "ltvMultiplier", "lastOrderDate", "daysSinceLastOrder",
  ], []);

  const columnProfiles = useMemo(() => [
    {
      id: "overview", label: "Overview", icon: "📊",
      description: "Key customer details — acquisition type, orders, revenue and recency",
      columns: ["tag", "acquisitionDate", "totalOrders", "netRevenue", "lastOrderDate", "daysSinceLastOrder"],
      fullColumns: ["tag", "acquisitionDate", "acquisitionCampaign", "totalOrders", "metaOrders", "grossRevenue", "totalRefunded", "netRevenue", "lastOrderDate", "daysSinceLastOrder"],
    },
    {
      id: "ltv", label: "Lifetime Value", icon: "💎",
      description: "Deep dive into customer value — LTV, multipliers, order frequency and retention signals",
      columns: ["tag", "totalOrders", "netRevenue", "avgOrderValue", "ltvMultiplier", "timeTo2ndOrder"],
      fullColumns: ["tag", "totalOrders", "grossRevenue", "totalRefunded", "netRevenue", "avgOrderValue", "firstOrderValue", "ltvMultiplier", "daysSinceAcquisition", "timeTo2ndOrder", "refundRate"],
    },
    {
      id: "acquisition", label: "Acquisition", icon: "🎯",
      description: "How each customer was acquired — which campaigns and ads brought them in",
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

  // Age bar colors — gradient from light to deep purple
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

  // Metric selector component — underlined text links
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

  // Demographics: active data based on toggle
  const activeAgeBreakdown = demoScope === "new" ? newAgeBreakdown : ageBreakdown;
  const activeGenderBreakdown = demoScope === "new" ? newGenderBreakdown : genderBreakdown;
  const activeDemoConversions = demoScope === "new" ? newDemoConversions : totalDemoConversions;
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
      totalRefunded: refunded > 0 ? fmtPrice(refunded) : "—",
      netRevenue: fmtPrice(net),
      avgOrderValue: filteredRows.length > 0 ? fmtPrice(Math.round(gross / filteredRows.length)) : "—",
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
  // Computed from the same pre-aggregated loader data the tiles below use —
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
      // Gender split — biggest first so the dominant audience leads the line.
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
    // (all-Meta avg CPA) × new conversions — same basis as the CPA chips
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

    // 2) Top country + top city — identical data source and denominator
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
        text: <><strong>Acquired {metaCount.toLocaleString()} new Meta customer{metaCount === 1 ? "" : "s"}</strong> — {countStr}{cpaStr}.</>,
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
          msg = <>Pays back in ~{payback.toFixed(1)} orders{scope} — CAC is outpacing first-order AOV.</>;
        }
        out.push({ tone, text: <><strong>Payback:</strong> {msg}</> });
      }
    }

    // 5) Returning Meta customers — absolute count of Meta-attributed
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
        if (pct >= 10) { tone = "positive"; delta = <> — up {pct}% vs previous period.</>; }
        else if (pct <= -10) { tone = "warning"; delta = <> — down {Math.abs(pct)}% vs previous period.</>; }
        else { delta = <> — roughly flat vs previous period.</>; }
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
        if (ratio >= 3) { tone = "positive"; tail = " — healthy."; }
        else if (ratio < 2) { tone = "warning"; tail = " — below 2× threshold."; }
        out.push({
          tone,
          text: <><strong>LTV:CAC {ratio.toFixed(2)}×</strong> at {label} ({cs}{Math.round(heroLtv).toLocaleString()} LTV vs {cs}{Math.round(cac).toLocaleString()}{scope} CAC){tail}</>,
        });
      }
    }

    // 7) Attribution health — surface only if unmatched share is material
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
            text: <><strong>Attribution:</strong> {pct}% of Meta conversions ({unmatched.toLocaleString()}) couldn&apos;t be matched to a Shopify order — likely edited orders or refunds after purchase.</>,
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
        {/* Hidden for V1 — bring back in V2. Loader wiring kept intact. */}
        {false && (
          <AiInsightsPanel
            pageKey="customers"
            cachedInsights={aiCachedInsights}
            generatedAt={aiGeneratedAt}
            isStale={aiIsStale}
            currencySymbol={cs}
          />
        )}
        <PageSummary bullets={summaryBullets} fromKey={data.fromKey} toKey={data.toKey} preset={data.preset} />

        {/* ═══ ALL TILES (drag/drop, show/hide) ═══ */}
        <TileGrid pageId="customers-v8" columns={4} tiles={[
          { id: "customerBreakdown", label: "Customer Breakdown", span: 2, render: () => (
          <Card>
            <BlockStack gap="300">
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start" }}>
                <div style={{ flex: 1, textAlign: "center" }}>
                  <Text as="h2" variant="headingLg">Customer Breakdown</Text>
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
            </BlockStack>
          </Card>
          )},
          { id: "demographics", label: "Customer Demographics", span: 2, render: () => (
          <Card>
            <BlockStack gap="400">
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start" }}>
                <div style={{ flex: 1, textAlign: "center" }}>
                  <Text as="h2" variant="headingLg">Customer Demographics</Text>
                  <Text as="p" variant="bodySm" tone="subdued">
                    {demoScope === "new"
                      ? "All New customer Meta-reported conversions by age & gender"
                      : "All Meta-reported conversions by age & gender"}
                  </Text>
                </div>
                <div className="toggle-group">
                  <button className={`toggle-btn ${demoScope === "new" ? "active" : ""}`} onClick={() => setDemoScope("new")}>New Meta</button>
                  <button className={`toggle-btn ${demoScope === "allMeta" ? "active" : ""}`} onClick={() => setDemoScope("allMeta")}>All Meta</button>
                </div>
              </div>
              <div className="demo-grid">
                {/* Age distribution */}
                <div>
                  <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: "8px", minHeight: "28px", marginBottom: "8px" }}>
                    <div style={{ fontSize: "12px", fontWeight: 600, color: "#6B7280", textTransform: "uppercase", letterSpacing: "0.5px" }}>
                      Age
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
                      {/* Spend comparison */}
                      <div style={{ display: "flex", gap: "12px", justifyContent: "center", marginTop: "4px" }}>
                        {activeGenderBreakdown.map((g: any) => {
                          const color = g.label === "Female" ? "#EC4899" : g.label === "Male" ? "#3B82F6" : "#9CA3AF";
                          const cpa = g.conversions > 0 ? g.spend / g.conversions : 0;
                          return (
                            <div key={g.label} style={{ textAlign: "center" }}>
                              <div style={{ fontSize: "18px", fontWeight: 700, color }}>{cs}{Math.round(cpa)}</div>
                              <div style={{ fontSize: "10px", color: "#9CA3AF" }}>CPA ({g.label})</div>
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
            // Shared x-axis scale so country bars and city bars are directly
            // comparable, and so the two charts' bar widths line up visually.
            const sharedGeoMax = Math.max(
              ...activeGeoCountries.slice(0, 10).map((c: any) => c.customers),
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
                        ? "Billing address of all Meta-attributed customers (new + retargeted)"
                        : "Billing address of all new customers acquired in this period"}
                  </Text>
                </div>
                <div className="toggle-group">
                  <button className={`toggle-btn ${geoScope === "new" ? "active" : ""}`} onClick={() => setGeoScope("new")}>New Meta</button>
                  <button className={`toggle-btn ${geoScope === "allMeta" ? "active" : ""}`} onClick={() => setGeoScope("allMeta")}>All Meta</button>
                  <button className={`toggle-btn ${geoScope === "all" ? "active" : ""}`} onClick={() => { setGeoScope("all"); if (geoMetric === "cpa" || geoMetric === "roas") setGeoMetric("rev"); }}>All Customers</button>
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
                      sharedMax={sharedGeoMax}
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
                      sharedMax={sharedGeoMax}
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
                  <button className={`toggle-btn ${journeyScope === "meta" ? "active" : ""}`} onClick={() => setJourneyScope("meta")}>Meta Customers</button>
                  <button className={`toggle-btn ${journeyScope === "all" ? "active" : ""}`} onClick={() => setJourneyScope("all")}>All Customers</button>
                </div>
              </div>
              <Text as="p" variant="bodySm" tone="subdued">
                {journeyScope === "meta"
                  ? "New customers acquired via Meta in this period — did they come back?"
                  : "All new customers acquired in this period — did they come back?"}
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
            </BlockStack>
          </Card>
          )},
          { id: "weeklyCohortRevenue", label: "Revenue by Weekly Cohort", span: 4, render: () => (
            <Card>
              <WeeklyCohortRevenue weekly={weeklyCohortSeries} cs={cs} />
            </Card>
          )},
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
          { id: "totalMetaRevenue", label: "Meta Order Revenue", render: () => {
            const matchedRevenue = metaNewRevenueInRange + metaRepeatRevenueInRange + metaRetargetedRevenueInRange;
            const totalMetaRevenue = matchedRevenue + unmatchedRevenue;
            // Fix for the +206.8% delta bug: previousValue previously used
            // prevMetaNewRevenueInRange (new-customer only) but currentValue
            // sums all segments + unmatched, so deltas were inflated ~3x.
            const prevTotalMetaRevenue = prevMetaNewRevenueInRange + prevMetaRepeatRevenueInRange + prevMetaRetargetedRevenueInRange + prevUnmatchedRevenue;
            const pct = totalRevenueInRange > 0 ? Math.round((totalMetaRevenue / totalRevenueInRange) * 100) : 0;
            return (
              <SummaryTile label="Meta Order Revenue"
                tooltip={{ definition: "Revenue from Meta-attributed orders: Shopify net revenue for matched orders plus Meta-reported conversion values for unmatched" }}
                value={fmtPrice(totalMetaRevenue)}
                subtitle={`${pct}% of all website revenue (${fmtPrice(totalRevenueInRange)})`}
                currentValue={totalMetaRevenue} previousValue={prevTotalMetaRevenue}
                chartData={dailyData} prevChartData={prevDailyData} chartKey="metaRevenue" chartColor="#5C6AC4" chartFormat={fmtPrice} />
            );
          }},
          { id: "newMetaCustomers", label: "New Meta Customers", render: () => (
            <SummaryTile label="New Meta Customers" value={metaCount.toLocaleString()}
              tooltip={{ definition: "Customers whose first-ever order was attributed to a Meta ad within the selected date range" }}
              subtitle={`${metaCount + organicCount > 0 ? Math.round((metaCount / (metaCount + organicCount)) * 100) : 0}% of all new customers in period`}
              currentValue={metaCount} previousValue={prevMetaCount}
              chartData={dailyData} prevChartData={prevDailyData} chartKey="newMetaCustomers" chartColor="#2E7D32" chartFormat={fmtCount} />
          )},
          { id: "newMetaRevenue", label: "New Meta Customer Revenue", render: () => {
            const matchedRevenue = metaNewRevenueInRange + metaRepeatRevenueInRange + metaRetargetedRevenueInRange;
            const totalMetaRevenue = matchedRevenue + unmatchedRevenue;
            const newRevPct = totalMetaRevenue > 0 ? Math.round((metaNewRevenueInRange / totalMetaRevenue) * 100) : 0;
            return (
              <SummaryTile label="New Meta Customer Revenue"
                tooltip={{ definition: "Net revenue from first orders placed by newly acquired Meta customers in the selected period" }}
                value={fmtPrice(metaNewRevenueInRange)}
                subtitle={`${newRevPct}% of all Meta revenue (${fmtPrice(totalMetaRevenue)})`}
                currentValue={metaNewRevenueInRange} previousValue={prevMetaNewRevenueInRange}
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
          { id: "aovCpa", label: "Meta AOV : CPA", render: () => (
            <SummaryTile label="Meta AOV : CPA"
              tooltip={{ definition: "First order value vs acquisition cost. Above 1x means you break even on the first order", calc: "New customer AOV ÷ CPA" }}
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
          { id: "ltvOverview", label: "Meta Customer Lifetime Value", span: 4, render: () => (
          <Card>
            <BlockStack gap="400">
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                <Text as="h2" variant="headingLg">{ltvTab === "meta" ? "Meta Customer Lifetime Value" : "All Customer Lifetime Value"}</Text>
                <div className="toggle-group">
                  <button className={`toggle-btn ${ltvTab === "meta" ? "active" : ""}`} onClick={() => setLtvTab("meta")}>Meta Customers</button>
                  <button className={`toggle-btn ${ltvTab === "all" ? "active" : ""}`} onClick={() => setLtvTab("all")}>All Customers</button>
                </div>
              </div>
              {/* LTV explorer controls — only shown on the Meta tab. Filters
                  cut the per-customer dataset by gender/age/country and
                  recompute the three headline stats + the maturation chart.
                  Window tabs affect which benchmark window drives the "LTV by
                  X" hero. Margin slider flips Payback from revenue-based to
                  profit-based (CAC ÷ first-order gross profit). */}
              {ltvTab === "meta" && (
                <div style={{ display: "flex", flexWrap: "wrap", gap: 12, alignItems: "center", padding: "10px 12px", background: "#F9FAFB", border: "1px solid #E5E7EB", borderRadius: 8 }}>
                  <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
                    <span style={{ fontSize: 11, fontWeight: 600, color: "#6B7280", textTransform: "uppercase", letterSpacing: 0.5 }}>Window</span>
                    <div className="toggle-group">
                      {(["lifetime", 365, 180, 90, 60, 30] as const).map((w) => (
                        <button key={String(w)} className={`toggle-btn ${ltvWindowPreset === w ? "active" : ""}`} onClick={() => setLtvWindowPreset(w)}>
                          {w === "lifetime" ? "Lifetime" : w === 365 ? "1yr" : `${w}d`}
                        </button>
                      ))}
                    </div>
                  </div>
                  <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
                    <span style={{ fontSize: 11, fontWeight: 600, color: "#6B7280", textTransform: "uppercase", letterSpacing: 0.5 }}>Gender</span>
                    <div className="toggle-group">
                      {(["All", "female", "male"] as const).map((g) => (
                        <button key={g} className={`toggle-btn ${ltvFilterGender === g ? "active" : ""}`} onClick={() => setLtvFilterGender(g)}>
                          {g === "All" ? "All" : g === "female" ? "Women" : "Men"}
                        </button>
                      ))}
                    </div>
                  </div>
                  {ltvAgeOptions.length > 0 && (
                    <div style={{ display: "flex", gap: 6, alignItems: "center", flexWrap: "wrap" }}>
                      <span style={{ fontSize: 11, fontWeight: 600, color: "#6B7280", textTransform: "uppercase", letterSpacing: 0.5 }}>Age</span>
                      <div className="toggle-group">
                        <button className={`toggle-btn ${ltvFilterAges.length === 0 ? "active" : ""}`} onClick={() => setLtvFilterAges([])}>All</button>
                        {ltvAgeOptions.map((a) => (
                          <button key={a} className={`toggle-btn ${ltvFilterAges.includes(a) ? "active" : ""}`} onClick={() => setLtvFilterAges((prev) => prev.includes(a) ? prev.filter((x) => x !== a) : [...prev, a])}>
                            {a}
                          </button>
                        ))}
                      </div>
                    </div>
                  )}
                  {ltvCountryOptions.length > 0 && (
                    <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
                      <span style={{ fontSize: 11, fontWeight: 600, color: "#6B7280", textTransform: "uppercase", letterSpacing: 0.5 }}>Country</span>
                      <select value={ltvFilterCountry} onChange={(e) => setLtvFilterCountry(e.target.value)} style={{ fontSize: 12, padding: "4px 8px", border: "1px solid #D1D5DB", borderRadius: 6, background: "#fff" }}>
                        <option value="All">All</option>
                        {ltvCountryOptions.slice(0, 30).map((c) => <option key={c} value={c}>{c}</option>)}
                      </select>
                    </div>
                  )}
                  <div style={{ display: "flex", gap: 8, alignItems: "center", marginLeft: "auto" }}>
                    <span style={{ fontSize: 11, fontWeight: 600, color: "#6B7280", textTransform: "uppercase", letterSpacing: 0.5 }}>Margin</span>
                    <input type="range" min={0} max={90} step={5} value={marginPct} onChange={(e) => setMarginPct(parseInt(e.target.value, 10))} style={{ width: 120 }} />
                    <span style={{ fontSize: 13, fontWeight: 600, color: "#1F2937", minWidth: 36 }}>{marginPct}%</span>
                  </div>
                  {ltvFiltered.filterActive && (
                    <button onClick={() => { setLtvFilterGender("All"); setLtvFilterAges([]); setLtvFilterCountry("All"); }} style={{ fontSize: 11, padding: "4px 8px", border: "1px solid #D1D5DB", borderRadius: 6, background: "#fff", cursor: "pointer", color: "#6B7280" }}>
                      Clear filters
                    </button>
                  )}
                </div>
              )}
              {(() => {
                const isMeta = ltvTab === "meta";
                const tile = isMeta ? ltvTile?.meta : ltvTile?.all;
                const baseCount = tile?.count || 0;
                const baseAvgAov = tile?.avgAov || 0;
                const baseAvgOrds = tile?.avgOrders || 0;
                const baseRepeatRate = tile?.repeatRate || 0;
                const baseMedT2 = tile?.medianTimeTo2nd ?? null;
                const cac = isMeta ? (tile as any)?.cpa || 0 : 0;
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
                const benchmarkWindows = useFiltered ? ltvFiltered.benchmarkWindows : baseBenchmarkWindows;
                const benchmarkMaxWindow = benchmarkWindows.length > 0 ? benchmarkWindows[benchmarkWindows.length - 1].window : 0;

                // Hero LTV respects the window preset. "Lifetime" uses the
                // longest matured window (same as before); any other preset
                // picks that specific window if present.
                const preset = ltvWindowPreset;
                const heroEntry = benchmarkWindows.length > 0
                  ? (preset === "lifetime"
                      ? benchmarkWindows[benchmarkWindows.length - 1]
                      : benchmarkWindows.find((w: any) => w.window === preset) || benchmarkWindows[benchmarkWindows.length - 1])
                  : null;
                const heroLtv = heroEntry ? heroEntry.avgLtv : (useFiltered ? ltvFiltered.avgLtv : (tile?.avgLtv || 0));
                const heroLabel = heroEntry ? `${heroEntry.window >= 365 ? "1yr" : heroEntry.window + "d"} LTV` : "Avg LTV";
                const heroCount = heroEntry ? heroEntry.count : count;
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
                const ratioBlurb = ltvCacRatio >= 3 ? `Healthy — every ${cs}1 of ad spend returns ${cs}${ltvCacRatio.toFixed(2)} over ${heroLabel.replace(" LTV", "")}`
                  : ltvCacRatio >= 2 ? `On track — ${cs}1 spent returns ${cs}${ltvCacRatio.toFixed(2)}`
                  : ltvCacRatio >= 1 ? "Thin margin — lift repeat rate or lower CAC"
                  : ltvCacRatio > 0 ? "Unprofitable — CAC is outpacing LTV"
                  : "Not enough mature customers yet";

                return (
                  <div>
                    {/* Three headline stats: LTV · LTV:CAC · Payback. Non-Meta
                        tab drops the two CAC-dependent cards. */}
                    <div style={{ display: "grid", gridTemplateColumns: isMeta ? "1.2fr 1fr 1fr" : "1fr", gap: "14px", marginBottom: "14px" }}>
                      <div style={{ padding: "18px 22px", background: "linear-gradient(135deg, #EEF2FF 0%, #F5F3FF 100%)", borderRadius: "10px", border: "1px solid #E0E7FF" }}>
                        <div style={{ fontSize: "11px", fontWeight: 700, color: "#6366F1", textTransform: "uppercase", letterSpacing: "0.5px", marginBottom: "6px" }}>Lifetime Value</div>
                        <div style={{ fontSize: "34px", fontWeight: 800, color: "#1F2937", lineHeight: 1.05 }}>
                          {heroLtv > 0 ? `${cs}${Math.round(heroLtv).toLocaleString()}` : "—"}
                        </div>
                        <div style={{ fontSize: "12px", color: "#4B5563", marginTop: "6px" }}>
                          per customer by {heroLabel.replace(" LTV", "")}
                        </div>
                        <div style={{ fontSize: "11px", color: "#9CA3AF", marginTop: "2px" }}>
                          {heroCount.toLocaleString()} mature customer{heroCount !== 1 ? "s" : ""}
                        </div>
                      </div>
                      {isMeta && (
                        <div style={{ padding: "18px 22px", background: "#fff", borderRadius: "10px", border: "1px solid #E5E7EB" }}>
                          <div style={{ fontSize: "11px", fontWeight: 700, color: "#6366F1", textTransform: "uppercase", letterSpacing: "0.5px", marginBottom: "6px" }}>LTV : CAC</div>
                          <div style={{ fontSize: "34px", fontWeight: 800, color: ratioColor, lineHeight: 1.05 }}>
                            {ltvCacRatio > 0 ? `${ltvCacRatio.toFixed(2)}×` : "—"}
                          </div>
                          <div style={{ fontSize: "12px", color: "#4B5563", marginTop: "6px" }}>{ratioBlurb}</div>
                          {cac > 0 && heroLtv > 0 && (
                            <div style={{ fontSize: "11px", color: "#9CA3AF", marginTop: "2px" }}>
                              {cs}{Math.round(heroLtv).toLocaleString()} LTV vs {cs}{Math.round(cac).toLocaleString()} CAC
                            </div>
                          )}
                        </div>
                      )}
                      {isMeta && (
                        <div style={{ padding: "18px 22px", background: "#fff", borderRadius: "10px", border: "1px solid #E5E7EB" }}>
                          <div style={{ fontSize: "11px", fontWeight: 700, color: "#6366F1", textTransform: "uppercase", letterSpacing: "0.5px", marginBottom: "6px" }}>Payback</div>
                          <div style={{ fontSize: "34px", fontWeight: 800, color: "#1F2937", lineHeight: 1.05 }}>
                            {payback > 0
                              ? (paybackDays != null ? (paybackDays === 0 ? "Day 1" : `${paybackDays}d`) : `${payback.toFixed(1)} orders`)
                              : "—"}
                          </div>
                          <div style={{ fontSize: "12px", color: "#4B5563", marginTop: "6px" }}>
                            {payback > 0
                              ? (payback <= 1
                                  ? `First order (${cs}${Math.round(avgAov)} AOV) clears ${cs}${Math.round(cac)} CAC`
                                  : `Recoups ${cs}${Math.round(cac)} CAC over ~${payback.toFixed(1)} orders`)
                              : cac > 0 ? "Not enough orders to calculate" : ""}
                          </div>
                          {payback > 1 && medT2 != null && (
                            <div style={{ fontSize: "11px", color: "#9CA3AF", marginTop: "2px" }}>
                              median {medT2}d between 1st and 2nd order
                            </div>
                          )}
                        </div>
                      )}
                    </div>
                    {/* Secondary row: the three previously-hero-ish stats, now
                        de-emphasised so the narrative tiles above lead. */}
                    <div style={{ display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: "0", marginBottom: "20px", padding: "12px 16px", background: "#FAFAFA", borderRadius: "8px", border: "1px solid #F3F4F6" }}>
                      <div style={{ borderRight: "1px solid #E5E7EB", paddingRight: "16px" }}>
                        <div style={{ fontSize: "10px", fontWeight: 600, color: "#9CA3AF", textTransform: "uppercase", letterSpacing: "0.5px" }}>Avg Order</div>
                        <div style={{ fontSize: "18px", fontWeight: 700, color: "#1F2937", marginTop: "2px" }}>{avgAov > 0 ? `${cs}${Math.round(avgAov).toLocaleString()}` : "—"}</div>
                        <div style={{ fontSize: "11px", color: "#9CA3AF" }}>{avgOrds > 0 ? `${avgOrds.toFixed(1)} orders / customer` : "no data"}</div>
                      </div>
                      <div style={{ borderRight: "1px solid #E5E7EB", padding: "0 16px" }}>
                        <div style={{ fontSize: "10px", fontWeight: 600, color: "#9CA3AF", textTransform: "uppercase", letterSpacing: "0.5px" }}>Days to 2nd Order</div>
                        <div style={{ fontSize: "18px", fontWeight: 700, color: "#1F2937", marginTop: "2px" }}>{medT2 != null ? `${medT2}d` : "—"}</div>
                        <div style={{ fontSize: "11px", color: "#9CA3AF" }}>median across cohort</div>
                      </div>
                      <div style={{ paddingLeft: "16px" }}>
                        <div style={{ fontSize: "10px", fontWeight: 600, color: "#9CA3AF", textTransform: "uppercase", letterSpacing: "0.5px" }}>Repeat Rate</div>
                        <div style={{ fontSize: "18px", fontWeight: 700, color: "#1F2937", marginTop: "2px" }}>{repeatRate}%</div>
                        <div style={{ fontSize: "11px", color: "#9CA3AF" }}>{count.toLocaleString()} customer{count !== 1 ? "s" : ""}</div>
                      </div>
                    </div>
                    {(benchmarkWindows.length > 0 || (isMeta ? ltvMonthly?.meta : ltvMonthly?.all)?.rows?.length > 0) && (() => {
                      const recentData = useFiltered ? [] : (isMeta ? ltvRecent?.meta : ltvRecent?.all);
                      const recentByWindow: Record<number, any> = {};
                      for (const r of (recentData || [])) recentByWindow[r.window] = r;
                      const monthlyDataObj = isMeta ? ltvMonthly?.meta : ltvMonthly?.all;
                      const allMonthlyRows = monthlyDataObj?.rows || [];
                      // Cap the cohort view to the most recent 12 cohort rows
                      // and 12 month columns — anything older/longer is
                      // diminishing-returns noise for merchant decisions.
                      const monthlyRows = allMonthlyRows.slice(-12);
                      const maxMonthCol = Math.min(monthlyDataObj?.maxMonth || 0, 12);
                      return (
                        <div style={{ borderTop: "1px solid #E5E7EB", paddingTop: "16px" }}>
                          <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: "12px" }}>
                            <div>
                              <Text as="p" variant="headingSm">{ltvView === "progression" ? "LTV Progression" : "Monthly Cohort Table"}</Text>
                              <Text as="p" variant="bodySm" tone="subdued">
                                {ltvView === "progression"
                                  ? "Benchmark (all matured cohorts): average cumulative revenue per customer at each window. Recent cohort: the latest group that has completed each window (e.g. 30d = acquired 30–60 days ago)."
                                  : cohortMetric === "ltv"
                                    ? "Each row is an acquisition month. Values show cumulative revenue per customer through each 30-day period."
                                    : "Each row is an acquisition month. Values show % of customers who placed at least one order in each 30-day period."}
                              </Text>
                            </div>
                            <div className="toggle-group">
                              <button className={`toggle-btn ${ltvView === "progression" ? "active" : ""}`} onClick={() => setLtvView("progression")}>Progression</button>
                              <button className={`toggle-btn ${ltvView === "cohorts" ? "active" : ""}`} onClick={() => setLtvView("cohorts")}>Cohort Table</button>
                            </div>
                          </div>
                          {ltvView === "progression" && benchmarkWindows.length > 0 && (() => {
                            // Maturation chart. X-axis: days since acquisition
                            // (0 → last benchmark window). Y-axis: cumulative
                            // £/customer. Solid purple = benchmark, dashed
                            // green = latest completed cohort, dashed red =
                            // CAC. Payback day = where benchmark crosses CAC,
                            // computed via linear interp between windows.
                            // Recent cohort overlay is suppressed when filters
                            // are active (recent data isn't filter-aware in v1).
                            const recentCap = Math.floor(benchmarkMaxWindow / 2);
                            const chartWidth = 960;
                            const chartHeight = 360;
                            const padL = 64, padR = 28, padT = 24, padB = 48;
                            const innerW = chartWidth - padL - padR;
                            const innerH = chartHeight - padT - padB;
                            const lastWindow = benchmarkWindows[benchmarkWindows.length - 1].window;
                            const recentCut = useFiltered ? [] : (recentData || []).filter((r: any) => r.window <= recentCap);
                            const ltvMaxRaw = Math.max(
                              ...benchmarkWindows.map((w: any) => w.avgLtv),
                              ...recentCut.map((r: any) => r.avgLtv),
                              isMeta && cac > 0 ? cac * 1.15 : 0,
                              1,
                            );
                            const ltvMax = ltvMaxRaw * 1.08;
                            const xPos = (day: number) => padL + (day / lastWindow) * innerW;
                            const yPos = (val: number) => padT + innerH - (val / ltvMax) * innerH;
                            const benchPts = [{ window: 0, avgLtv: 0 }, ...benchmarkWindows];
                            const benchPath = benchPts.map((p: any, i: number) => `${i === 0 ? "M" : "L"} ${xPos(p.window).toFixed(1)} ${yPos(p.avgLtv).toFixed(1)}`).join(" ");
                            const recentPts = recentCut.length > 0 ? [{ window: 0, avgLtv: 0 }, ...recentCut] : [];
                            const recentPath = recentPts.length > 1 ? recentPts.map((p: any, i: number) => `${i === 0 ? "M" : "L"} ${xPos(p.window).toFixed(1)} ${yPos(p.avgLtv).toFixed(1)}`).join(" ") : "";
                            let paybackDay: number | null = null;
                            if (isMeta && cac > 0) {
                              for (let i = 1; i < benchPts.length; i++) {
                                const a = benchPts[i - 1], b = benchPts[i];
                                if (a.avgLtv <= cac && b.avgLtv >= cac && b.avgLtv !== a.avgLtv) {
                                  paybackDay = Math.round(a.window + (b.window - a.window) * (cac - a.avgLtv) / (b.avgLtv - a.avgLtv));
                                  break;
                                }
                              }
                            }
                            const gridVals = [0, ltvMax * 0.25, ltvMax * 0.5, ltvMax * 0.75, ltvMax];
                            const benchLast = benchmarkWindows[benchmarkWindows.length - 1];
                            const bench30 = benchmarkWindows.find((p: any) => p.window === 30);
                            const recent30 = recentByWindow[30];
                            const growthMultiple = bench30 && bench30.avgLtv > 0 ? benchLast.avgLtv / bench30.avgLtv : 0;
                            const projected = recent30 && growthMultiple > 0 ? Math.round(recent30.avgLtv * growthMultiple) : null;
                            return (
                              <div>
                                <div style={{ display: "flex", gap: "16px", marginBottom: "12px", fontSize: "11px", flexWrap: "wrap" }}>
                                  <div style={{ display: "flex", alignItems: "center", gap: "6px" }}>
                                    <svg width="22" height="8"><line x1="0" y1="4" x2="22" y2="4" stroke="#7C3AED" strokeWidth="2.5" strokeLinecap="round" /></svg>
                                    <span style={{ color: "#6B7280", fontWeight: 500 }}>Benchmark (all matured cohorts)</span>
                                  </div>
                                  {recentPath && (
                                    <div style={{ display: "flex", alignItems: "center", gap: "6px" }}>
                                      <svg width="22" height="8"><line x1="0" y1="4" x2="22" y2="4" stroke="#10B981" strokeWidth="2.5" strokeDasharray="5 3" strokeLinecap="round" /></svg>
                                      <span style={{ color: "#6B7280", fontWeight: 500 }}>Recent cohort (latest fully-matured)</span>
                                    </div>
                                  )}
                                  {isMeta && cac > 0 && (
                                    <div style={{ display: "flex", alignItems: "center", gap: "6px" }}>
                                      <svg width="22" height="8"><line x1="0" y1="4" x2="22" y2="4" stroke="#DC2626" strokeWidth="1.5" strokeDasharray="2 2" /></svg>
                                      <span style={{ color: "#6B7280", fontWeight: 500 }}>CAC ({cs}{Math.round(cac).toLocaleString()})</span>
                                    </div>
                                  )}
                                  {useFiltered && (
                                    <div style={{ color: "#9CA3AF", fontStyle: "italic" }}>Recent-cohort overlay hidden while filters are active.</div>
                                  )}
                                </div>
                                <div style={{ position: "relative", width: "100%" }}>
                                <svg viewBox={`0 0 ${chartWidth} ${chartHeight}`} preserveAspectRatio="none" style={{ width: "100%", height: "360px", display: "block" }}>
                                  <defs>
                                    <linearGradient id="benchGradient" x1="0" y1="0" x2="0" y2="1">
                                      <stop offset="0%" stopColor="#7C3AED" stopOpacity="0.18" />
                                      <stop offset="100%" stopColor="#7C3AED" stopOpacity="0" />
                                    </linearGradient>
                                  </defs>
                                  {gridVals.map((v, i) => (
                                    <g key={i}>
                                      <line x1={padL} x2={chartWidth - padR} y1={yPos(v)} y2={yPos(v)} stroke="#F3F4F6" strokeWidth="1" />
                                      <text x={padL - 8} y={yPos(v) + 3} textAnchor="end" fontSize="11" fill="#9CA3AF">{cs}{Math.round(v).toLocaleString()}</text>
                                    </g>
                                  ))}
                                  {benchmarkWindows.map((w: any) => (
                                    <g key={w.window}>
                                      <line x1={xPos(w.window)} x2={xPos(w.window)} y1={padT + innerH} y2={padT + innerH + 4} stroke="#9CA3AF" />
                                      <text x={xPos(w.window)} y={chartHeight - padB + 20} textAnchor="middle" fontSize="11" fill="#6B7280">{windowLabel(w.window)}</text>
                                    </g>
                                  ))}
                                  {/* Gradient fill under benchmark curve */}
                                  <path
                                    d={`${benchPath} L ${xPos(lastWindow).toFixed(1)} ${(padT + innerH).toFixed(1)} L ${xPos(0).toFixed(1)} ${(padT + innerH).toFixed(1)} Z`}
                                    fill="url(#benchGradient)"
                                    stroke="none"
                                  />
                                  {isMeta && cac > 0 && cac < ltvMax && (
                                    <line x1={padL} x2={chartWidth - padR} y1={yPos(cac)} y2={yPos(cac)} stroke="#DC2626" strokeWidth="1.5" strokeDasharray="2 2" />
                                  )}
                                  <path d={benchPath} fill="none" stroke="#7C3AED" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" />
                                  {recentPath && (
                                    <path d={recentPath} fill="none" stroke="#10B981" strokeWidth="2.5" strokeDasharray="5 3" strokeLinecap="round" strokeLinejoin="round" />
                                  )}
                                  {benchmarkWindows.map((p: any) => {
                                    const recent = recentByWindow[p.window];
                                    const isHover = chartHover?.window === p.window;
                                    return (
                                      <g key={p.window} style={{ cursor: "pointer" }} onMouseEnter={() => setChartHover({ window: p.window, bench: p.avgLtv, recent: recent?.avgLtv ?? null })} onMouseLeave={() => setChartHover(null)}>
                                        {/* invisible hit target */}
                                        <rect x={xPos(p.window) - 20} y={padT} width={40} height={innerH} fill="transparent" />
                                        <circle cx={xPos(p.window)} cy={yPos(p.avgLtv)} r={isHover ? 6 : 4} fill="#7C3AED" stroke="#fff" strokeWidth={isHover ? 2 : 0} />
                                        {recent && (
                                          <circle cx={xPos(p.window)} cy={yPos(recent.avgLtv)} r={isHover ? 5 : 3.5} fill="#10B981" stroke="#fff" strokeWidth={isHover ? 2 : 0} />
                                        )}
                                        {!isHover && (
                                          <text x={xPos(p.window)} y={yPos(p.avgLtv) - 10} textAnchor="middle" fontSize="11" fontWeight="600" fill="#5B21B6">{cs}{Math.round(p.avgLtv).toLocaleString()}</text>
                                        )}
                                      </g>
                                    );
                                  })}
                                  {paybackDay != null && (
                                    <g>
                                      <line x1={xPos(paybackDay)} x2={xPos(paybackDay)} y1={yPos(cac)} y2={padT + innerH} stroke="#DC2626" strokeWidth="1" strokeDasharray="2 2" />
                                      <circle cx={xPos(paybackDay)} cy={yPos(cac)} r="6" fill="#fff" stroke="#DC2626" strokeWidth="2" />
                                      <text x={xPos(paybackDay)} y={yPos(cac) - 12} textAnchor="middle" fontSize="11" fontWeight="700" fill="#DC2626">Payback ~{paybackDay}d</text>
                                    </g>
                                  )}
                                  {/* Hover tooltip */}
                                  {chartHover && (() => {
                                    const hx = xPos(chartHover.window);
                                    const tipW = 170;
                                    const tipH = chartHover.recent != null ? 68 : 50;
                                    const leftSide = hx > chartWidth / 2;
                                    const tx = leftSide ? hx - tipW - 12 : hx + 12;
                                    const ty = padT + 8;
                                    return (
                                      <g pointerEvents="none">
                                        <line x1={hx} x2={hx} y1={padT} y2={padT + innerH} stroke="#9CA3AF" strokeWidth="1" strokeDasharray="2 3" />
                                        <rect x={tx} y={ty} width={tipW} height={tipH} rx="6" fill="#111827" opacity="0.95" />
                                        <text x={tx + 10} y={ty + 18} fontSize="12" fontWeight="700" fill="#fff">{windowLabel(chartHover.window)}</text>
                                        <text x={tx + 10} y={ty + 36} fontSize="11" fill="#C4B5FD">Benchmark: {cs}{Math.round(chartHover.bench).toLocaleString()}</text>
                                        {chartHover.recent != null && (
                                          <text x={tx + 10} y={ty + 54} fontSize="11" fill="#6EE7B7">Recent: {cs}{Math.round(chartHover.recent).toLocaleString()}</text>
                                        )}
                                      </g>
                                    );
                                  })()}
                                </svg>
                                </div>
                                {isMeta && paybackDay != null && (
                                  <div style={{ marginTop: "10px", padding: "10px 14px", background: "#ECFDF5", border: "1px solid #A7F3D0", borderRadius: "8px", fontSize: "13px", color: "#065F46" }}>
                                    <strong>Payback:</strong> Meta customers recoup their {cs}{Math.round(cac).toLocaleString()} acquisition cost around day {paybackDay}. By {windowLabel(lastWindow)}, cumulative spend reaches {cs}{Math.round(benchLast.avgLtv).toLocaleString()} per customer ({ltvCacRatio.toFixed(2)}× CAC).
                                  </div>
                                )}
                                {isMeta && paybackDay == null && cac > 0 && benchLast.avgLtv > 0 && (
                                  <div style={{ marginTop: "10px", padding: "10px 14px", background: "#FFFBEB", border: "1px solid #FCD34D", borderRadius: "8px", fontSize: "13px", color: "#92400E" }}>
                                    <strong>Payback:</strong> benchmark LTV at {windowLabel(lastWindow)} ({cs}{Math.round(benchLast.avgLtv).toLocaleString()}) hasn&apos;t yet crossed CAC ({cs}{Math.round(cac).toLocaleString()}). Lift repeat rate or lower CAC to close the gap.
                                  </div>
                                )}
                                {projected != null && (
                                  <div style={{ marginTop: "8px", padding: "10px 14px", background: "#EEF2FF", border: "1px solid #C7D2FE", borderRadius: "8px", fontSize: "13px", color: "#3730A3" }}>
                                    <strong>Projected:</strong> If the latest cohort keeps the benchmark&apos;s growth curve ({growthMultiple.toFixed(2)}× from 30d → {windowLabel(benchLast.window)}), their {windowLabel(benchLast.window)} LTV lands at ~<strong>{cs}{projected.toLocaleString()}</strong>.
                                  </div>
                                )}
                              </div>
                            );
                          })()}
                          {ltvView === "cohorts" && monthlyRows.length > 0 && (() => {
                            const colAvgs: Record<number, number> = {};
                            for (let m = 0; m <= maxMonthCol; m++) {
                              const vals = monthlyRows.map((row: any) => { const md = row.months[m]; if (!md?.matured) return null; return cohortMetric === "ltv" ? md.avgLtv : md.retention; }).filter((v: any) => v !== null && v !== undefined) as number[];
                              colAvgs[m] = vals.length > 0 ? vals.reduce((s: number, v: number) => s + v, 0) / vals.length : 0;
                            }
                            const getRetentionColor = (val: number) => {
                              if (val >= 80) return { bg: "#DCFCE7", text: "#166534" };
                              if (val >= 50) return { bg: "#ECFDF5", text: "#059669" };
                              if (val >= 20) return { bg: "#FEF9C3", text: "#854D0E" };
                              if (val > 0) return { bg: "#FEF2F2", text: "#DC2626" };
                              return { bg: "transparent", text: "#D1D5DB" };
                            };
                            return (
                              <div>
                                <div style={{ display: "flex", alignItems: "center", gap: "8px", marginBottom: "10px" }}>
                                  <span style={{ fontSize: "12px", fontWeight: 500, color: "#6B7280" }}>Metric:</span>
                                  <div className="toggle-group">
                                    <button className={`toggle-btn ${cohortMetric === "ltv" ? "active" : ""}`} onClick={() => setCohortMetric("ltv")}>LTV</button>
                                    <button className={`toggle-btn ${cohortMetric === "retention" ? "active" : ""}`} onClick={() => setCohortMetric("retention")}>Retention Rate</button>
                                  </div>
                                </div>
                                <div style={{ overflowX: "auto" }}>
                                  <table style={{ borderCollapse: "collapse", fontSize: "12px", whiteSpace: "nowrap" }}>
                                    <thead>
                                      <tr style={{ borderBottom: "2px solid #E5E7EB" }}>
                                        <th style={{ padding: "6px 10px", textAlign: "left", fontWeight: 600, color: "#4B5563", fontSize: "11px", textTransform: "uppercase", letterSpacing: "0.5px", position: "sticky", left: 0, background: "#fff", zIndex: 1 }}>Cohort</th>
                                        <th style={{ padding: "6px 8px", textAlign: "right", fontWeight: 600, color: "#4B5563", fontSize: "11px", textTransform: "uppercase", letterSpacing: "0.5px" }}>N</th>
                                        {Array.from({ length: maxMonthCol + 1 }, (_, i) => (
                                          <th key={i} style={{ padding: "6px 8px", textAlign: "right", fontWeight: 600, color: "#4B5563", fontSize: "11px", textTransform: "uppercase", letterSpacing: "0.5px", minWidth: "58px" }}>M{i}</th>
                                        ))}
                                      </tr>
                                    </thead>
                                    <tbody>
                                      {monthlyRows.map((row: any, ri: number) => {
                                        const monthLabel = (() => { const [y, m] = row.month.split("-"); return new Date(parseInt(y), parseInt(m) - 1).toLocaleDateString("en-GB", { month: "short", year: "2-digit" }); })();
                                        return (
                                          <tr key={row.month} style={{ borderBottom: "1px solid #F3F4F6", background: ri % 2 === 0 ? "#fff" : "#FAFAFA" }}>
                                            <td style={{ padding: "6px 10px", fontWeight: 600, color: "#1F2937", position: "sticky", left: 0, background: ri % 2 === 0 ? "#fff" : "#FAFAFA", zIndex: 1 }}>{monthLabel}</td>
                                            <td style={{ padding: "6px 8px", textAlign: "right", color: "#6B7280", fontWeight: 500 }}>{row.count}</td>
                                            {Array.from({ length: maxMonthCol + 1 }, (_, mi) => {
                                              const md = row.months[mi];
                                              if (!md?.matured) return <td key={mi} style={{ padding: "6px 8px", textAlign: "right", color: "#D1D5DB" }}>—</td>;
                                              const val = cohortMetric === "ltv" ? md.avgLtv : md.retention;
                                              if (val === null || val === undefined) return <td key={mi} style={{ padding: "6px 8px", textAlign: "right", color: "#D1D5DB" }}>—</td>;
                                              let cellBg = "transparent", cellText = "#1F2937";
                                              if (cohortMetric === "retention") { const colors = getRetentionColor(val); cellBg = colors.bg; cellText = colors.text; }
                                              else { const avg = colAvgs[mi]; if (avg > 0) { const delta = ((val - avg) / avg) * 100; if (delta >= 10) { cellBg = "#ECFDF5"; cellText = "#059669"; } else if (delta <= -10) { cellBg = "#FEF2F2"; cellText = "#DC2626"; } } }
                                              return <td key={mi} style={{ padding: "6px 8px", textAlign: "right", background: cellBg, fontWeight: 600, color: cellText }}>{cohortMetric === "ltv" ? `${cs}${Math.round(val).toLocaleString()}` : `${val}%`}</td>;
                                            })}
                                          </tr>
                                        );
                                      })}
                                    </tbody>
                                  </table>
                                </div>
                                <div style={{ marginTop: "6px" }}>
                                  <Text as="p" variant="bodySm" tone="subdued">
                                    {cohortMetric === "ltv" ? "M0 = first 30 days. Values show cumulative spend per customer. Green/red = 10%+ above/below column average." : "M0 = first 30 days. Values show % of cohort who placed an order in that 30-day period. Blank = not yet matured."}
                                  </Text>
                                </div>
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
        ] as TileDef[]} />



        {/* Customer table removed — not needed at this stage */}
      </BlockStack>
      </ReportTabs>
    </Page>
  );
}
