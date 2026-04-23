import { json } from "@remix-run/node";
import { useLoaderData, useSubmit, useActionData, useRevalidator } from "@remix-run/react";
import { Page, Card, Text, BlockStack } from "@shopify/polaris";
import React, { useState, useMemo, useRef, useEffect } from "react";
import { createPortal } from "react-dom";
import ReportTabs from "../components/ReportTabs";
import InteractiveTable from "../components/InteractiveTable";
import TileGrid from "../components/TileGrid";
import SummaryTile from "../components/SummaryTile";
import type { TileDef } from "../components/TileGrid";
import { authenticate } from "../shopify.server";
import db from "../db.server";
import { parseDateRange } from "../utils/dateRange.server";
import { shopLocalDayKey, shopRangeBounds } from "../utils/shopTime.server";
import { cached as queryCached, DEFAULT_TTL } from "../services/queryCache.server";
import type { ColumnDef } from "@tanstack/react-table";
import { getCachedInsights, computeDataHash, generateInsights } from "../services/aiAnalysis.server";
import { setProgress, failProgress, completeProgress } from "../services/progress.server";
import AiInsightsPanel from "../components/AiInsightsPanel";
import PageSummary, { type SummaryBullet } from "../components/PageSummary";

// ── Variant stripping ──

const COLORS = new Set([
  "black", "cream", "grey", "blue", "white", "red", "oyster", "pink",
  "chartreuse", "multi", "rose", "camel", "navy", "lilac", "magenta",
  "natural", "ecru", "green", "brown", "khaki", "orange", "yellow",
  "teal", "coral", "ivory", "taupe", "beige", "stone", "tan", "nude",
  "gold", "silver", "burgundy", "terracotta", "olive",
]);

function toParentProduct(name: string): string {
  const parts = name.trim().split(" ");
  if (parts.length <= 1) return name.trim();
  if (parts.length >= 3 && parts[parts.length - 3]?.toLowerCase() === "acid" && parts[parts.length - 2]?.toLowerCase() === "wash") {
    if (COLORS.has(parts[parts.length - 1].toLowerCase())) return parts.slice(0, -3).join(" ");
    return parts.slice(0, -2).join(" ");
  }
  if (parts.length >= 2 && parts[parts.length - 2]?.toLowerCase() === "acid" && parts[parts.length - 1]?.toLowerCase() === "wash") {
    return parts.slice(0, -2).join(" ");
  }
  if (COLORS.has(parts[parts.length - 1].toLowerCase())) return parts.slice(0, -1).join(" ");
  return name.trim();
}

// ── Loader ──

export const loader = async ({ request }) => {
  const { admin, session } = await authenticate.admin(request);
  const shopDomain = session.shop;
  const shopForTz = await db.shop.findUnique({ where: { shopDomain } });
  const tz = shopForTz?.shopifyTimezone || "UTC";
  const { fromDate, toDate, fromKey, toKey } = parseDateRange(request, tz);

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

  // ── Compute previous period dates upfront (shop-local) ──
  const dayCount = diffDays(fromKey, toKey) + 1;
  const prevToKey = addDaysKey(fromKey, -1);
  const prevFromKey = addDaysKey(prevToKey, -(dayCount - 1));
  const prevBounds = shopRangeBounds(tz, prevFromKey, prevToKey);
  const prevFrom = prevBounds.gte;
  const prevTo = prevBounds.lte;

  // ── Fetch product images from Shopify (generic, per-shop via authenticated session) ──
  // 3-tier cache:
  //   1. In-process queryCache (1h TTL) — instant
  //   2. DB Shop.productImagesJson (24h refresh) — survives process restarts
  //   3. Shopify GraphQL fetch — last resort (slow, 4-5s)
  const fetchImages = async (): Promise<Record<string, string>> => {
    return queryCached(`${shopDomain}:productImages`, 60 * 60 * 1000, async () => {
      // Tier 2: try DB cache
      const cachedShop = await db.shop.findUnique({
        where: { shopDomain },
        select: { productImagesJson: true, productImagesUpdatedAt: true },
      });
      const dbCacheAgeMs = cachedShop?.productImagesUpdatedAt
        ? Date.now() - cachedShop.productImagesUpdatedAt.getTime()
        : Infinity;
      const DB_CACHE_TTL = 24 * 60 * 60 * 1000; // 24 hours
      if (cachedShop?.productImagesJson && dbCacheAgeMs < DB_CACHE_TTL) {
        try {
          const parsed = JSON.parse(cachedShop.productImagesJson);
          console.log(`[Products] Loaded ${Object.keys(parsed).length} product images for ${shopDomain} from DB cache (age ${Math.round(dbCacheAgeMs / 60000)}min)`);
          return parsed;
        } catch {}
      }

      // Tier 3: fetch from Shopify
      const imgMap: Record<string, string> = {};
      try {
        let hasNext = true;
        let cursor: string | null = null;
        while (hasNext) {
          const query = `#graphql
            query GetProductImages($cursor: String) {
              products(first: 250, after: $cursor) {
                edges {
                  node {
                    title
                    featuredImage { url }
                  }
                  cursor
                }
                pageInfo { hasNextPage }
              }
            }`;
          const resp = await admin.graphql(query, { variables: { cursor } });
          const data = await resp.json();
          const edges = data?.data?.products?.edges || [];
          for (const edge of edges) {
            const title = edge.node.title;
            const url = edge.node.featuredImage?.url;
            if (title && url) {
              imgMap[title] = url;
              const parent = toParentProduct(title);
              if (!imgMap[parent]) imgMap[parent] = url;
            }
          }
          hasNext = data?.data?.products?.pageInfo?.hasNextPage || false;
          cursor = edges.length > 0 ? edges[edges.length - 1].cursor : null;
        }
        // Persist to DB cache
        await db.shop.update({
          where: { shopDomain },
          data: {
            productImagesJson: JSON.stringify(imgMap),
            productImagesUpdatedAt: new Date(),
          },
        });
        console.log(`[Products] Fetched ${Object.keys(imgMap).length} product images for ${shopDomain} from Shopify (saved to DB cache)`);
      } catch (err: any) {
        console.error(`[Products] Failed to fetch product images for ${shopDomain}:`, err?.message || err);
      }
      return imgMap;
    });
  };

  // ── Rollup-based loader (Phase 1) ──
  // Reads DailyProductRollup + ShopAnalysisCache instead of iterating raw
  // orders on every request. Aggregations are precomputed during incremental
  // sync. Custom date ranges are served by summing daily rollup rows.
  const _t0 = Date.now();

  // Cache the per-window rollup queries (date-keyed) and the analysis blob (per-shop)
  const [currentRollup, prevRollup, analysisCacheRow, unmatchedAgg, imageMap, periodOrders] = await Promise.all([
    queryCached(`${shopDomain}:productRollup:${fromKey}:${toKey}`, DEFAULT_TTL, () =>
      db.dailyProductRollup.findMany({
        where: { shopDomain, date: { gte: fromDate, lte: toDate } },
      }),
    ),
    queryCached(`${shopDomain}:productRollup:${prevFromKey}:${prevToKey}`, DEFAULT_TTL, () =>
      db.dailyProductRollup.findMany({
        where: { shopDomain, date: { gte: prevFrom, lte: prevTo } },
      }),
    ),
    queryCached(`${shopDomain}:productsAnalysis`, DEFAULT_TTL, () =>
      db.shopAnalysisCache.findUnique({
        where: { shopDomain_cacheKey: { shopDomain, cacheKey: "products:analysis" } },
      }),
    ),
    db.attribution.aggregate({
      where: { shopDomain, confidence: 0 },
      _count: { _all: true },
      _sum: { metaConversionValue: true },
    }),
    fetchImages(),
    // Per-period orders, slim — needed to compute date-scoped add-on
    // appearances (cached blob is all-time and was confusing the user).
    queryCached(`${shopDomain}:periodOrdersForAddons:${fromKey}:${toKey}`, DEFAULT_TTL, () =>
      db.order.findMany({
        where: { shopDomain, isOnlineStore: true, createdAt: { gte: fromDate, lte: toDate } },
        select: { shopifyOrderId: true, lineItems: true, utmConfirmedMeta: true },
      }),
    ),
  ]);

  // Period attribution lookup — used to flag Meta orders for the Meta tab.
  const periodOrderIds = periodOrders.map((o) => o.shopifyOrderId);
  const periodMetaAttrs = periodOrderIds.length > 0
    ? await queryCached(
        `${shopDomain}:periodAttrsForAddons:${fromKey}:${toKey}`,
        DEFAULT_TTL,
        () => db.attribution.findMany({
          where: { shopDomain, shopifyOrderId: { in: periodOrderIds }, confidence: { gt: 0 } },
          select: { shopifyOrderId: true },
        }),
      )
    : [];
  const metaOrderIdSet = new Set(periodMetaAttrs.map((a) => a.shopifyOrderId));
  for (const o of periodOrders) if (o.utmConfirmedMeta) metaOrderIdSet.add(o.shopifyOrderId);
  console.log(`[products] db ${Date.now() - _t0}ms (rollup=${currentRollup.length}, prevRollup=${prevRollup.length})`);

  const shop = shopForTz;
  const cs = (shop?.shopifyCurrency || "GBP") === "GBP" ? "\u00a3"
    : (shop?.shopifyCurrency || "GBP") === "EUR" ? "\u20ac" : "$";

  const blob: any = analysisCacheRow ? JSON.parse(analysisCacheRow.payload) : {
    metaAcquiredCustomers: [], metaCombos: [], nonMetaCombos: [],
    topAddons: [], topAddonsMeta: [],
    metaFirstPurchaseList: [], nonMetaFirstPurchaseList: [],
    metaJourney: { topGateway: [], topSecond: [], flows: [] },
    allJourney: { topGateway: [], topSecond: [], flows: [] },
    dailyBasketStats: {}, dailyRefundByProduct: {},
  };

  // ── Build productData by aggregating rollup rows (per product across segments/days) ──
  type ProductAgg = {
    product: string; totalOrders: number; metaOrders: number;
    metaNewOrders: number; metaRepeatOrders: number; metaRetargetedOrders: number; organicOrders: number;
    totalRevenue: number; metaRevenue: number; metaNewRevenue: number;
    metaRepeatRevenue: number; metaRetargetedRevenue: number; organicRevenue: number;
    totalRefunded: number; refundedOrders: number;
    metaRefundedOrders: number; metaTotalRefunded: number;
    firstPurchaseCount: number; metaFirstPurchaseCount: number;
    collections: Set<string>; campaigns: Record<string, number>; adSets: Record<string, number>;
  };
  const productData: Record<string, ProductAgg> = {};
  const ensureProduct = (name: string): ProductAgg => {
    if (!productData[name]) {
      productData[name] = {
        product: name, totalOrders: 0, metaOrders: 0, metaNewOrders: 0,
        metaRepeatOrders: 0, metaRetargetedOrders: 0, organicOrders: 0,
        totalRevenue: 0, metaRevenue: 0, metaNewRevenue: 0,
        metaRepeatRevenue: 0, metaRetargetedRevenue: 0, organicRevenue: 0,
        totalRefunded: 0, refundedOrders: 0,
        metaRefundedOrders: 0, metaTotalRefunded: 0,
        firstPurchaseCount: 0, metaFirstPurchaseCount: 0,
        collections: new Set(), campaigns: {}, adSets: {},
      };
    }
    return productData[name];
  };

  // Daily sparkline data derived from rollup rows
  const dailyProductSales: Record<string, Record<string, { orders: number; revenue: number; refunds: number }>> = {};
  const dailyMetaProductSales: Record<string, Record<string, { orders: number; revenue: number; refunds: number }>> = {};

  for (const r of currentRollup) {
    const pd = ensureProduct(r.product);
    pd.totalOrders += r.orders;
    pd.totalRevenue += r.revenue;
    pd.refundedOrders += r.refundedOrders;
    pd.totalRefunded += r.refundedAmount;
    pd.firstPurchaseCount += r.firstPurchases;

    const isMeta = r.segment !== "organic";
    if (isMeta) {
      pd.metaOrders += r.orders;
      pd.metaRevenue += r.revenue;
      pd.metaRefundedOrders += r.refundedOrders;
      pd.metaTotalRefunded += r.refundedAmount;
      pd.metaFirstPurchaseCount += r.firstPurchases;
    }
    if (r.segment === "metaNew") { pd.metaNewOrders += r.orders; pd.metaNewRevenue += r.revenue; }
    else if (r.segment === "metaRepeat") { pd.metaRepeatOrders += r.orders; pd.metaRepeatRevenue += r.revenue; }
    else if (r.segment === "metaRetargeted") { pd.metaRetargetedOrders += r.orders; pd.metaRetargetedRevenue += r.revenue; }
    else if (r.segment === "organic") { pd.organicOrders += r.orders; pd.organicRevenue += r.revenue; }

    try {
      const camps = JSON.parse(r.topCampaignJson || "{}") as Record<string, number>;
      for (const k of Object.keys(camps)) pd.campaigns[k] = (pd.campaigns[k] || 0) + camps[k];
    } catch {}
    try {
      const asets = JSON.parse(r.topAdSetJson || "{}") as Record<string, number>;
      for (const k of Object.keys(asets)) pd.adSets[k] = (pd.adSets[k] || 0) + asets[k];
    } catch {}
    if (r.collections) {
      for (const c of r.collections.split(", ")) if (c) pd.collections.add(c);
    }

    const dateStr = shopLocalDayKey(tz, r.date);
    if (!dailyProductSales[r.product]) dailyProductSales[r.product] = {};
    if (!dailyProductSales[r.product][dateStr]) dailyProductSales[r.product][dateStr] = { orders: 0, revenue: 0, refunds: 0 };
    dailyProductSales[r.product][dateStr].orders += r.orders;
    dailyProductSales[r.product][dateStr].revenue += r.revenue;
    dailyProductSales[r.product][dateStr].refunds += r.refundedOrders;
    if (isMeta) {
      if (!dailyMetaProductSales[r.product]) dailyMetaProductSales[r.product] = {};
      if (!dailyMetaProductSales[r.product][dateStr]) dailyMetaProductSales[r.product][dateStr] = { orders: 0, revenue: 0, refunds: 0 };
      dailyMetaProductSales[r.product][dateStr].orders += r.orders;
      dailyMetaProductSales[r.product][dateStr].revenue += r.revenue;
      dailyMetaProductSales[r.product][dateStr].refunds += r.refundedOrders;
    }
  }

  // ── Basket stats from analysis blob (per-day distinct order counts) ──
  let totalOrderCount = 0, metaOrderCount = 0, totalItemCount = 0, metaItemCount = 0;
  const dailyMetaItems: Record<string, number> = {};
  for (const dateStr of Object.keys(blob.dailyBasketStats || {})) {
    if (dateStr < fromKey || dateStr > toKey) continue;
    const s = blob.dailyBasketStats[dateStr];
    totalOrderCount += s.totalOrders || 0;
    metaOrderCount += s.metaOrders || 0;
    totalItemCount += s.totalItems || 0;
    metaItemCount += s.metaItems || 0;
    dailyMetaItems[dateStr] = s.metaItems || 0;
  }

  // Unmatched conversions: cheap aggregate (all-time, not date-scoped).
  // TODO: once attribution has a date column, filter to range.
  const unmatchedConversions = unmatchedAgg._count._all;
  const unmatchedRevenue = unmatchedAgg._sum.metaConversionValue || 0;

  // ── First-purchase lists: derived from rollup (segment filter) ──
  const metaFpMap: Record<string, { qty: number; revenue: number }> = {};
  const nonMetaFpMap: Record<string, { qty: number; revenue: number }> = {};
  for (const r of currentRollup) {
    if (r.firstPurchases === 0) continue;
    const target = r.segment === "organic" ? nonMetaFpMap : metaFpMap;
    if (!target[r.product]) target[r.product] = { qty: 0, revenue: 0 };
    target[r.product].qty += r.firstPurchases;
    target[r.product].revenue += r.firstPurchaseRevenue;
  }
  const metaFirstPurchaseList = Object.entries(metaFpMap)
    .map(([product, data]) => ({ product, qty: data.qty, revenue: Math.round(data.revenue * 100) / 100 }))
    .sort((a, b) => b.qty - a.qty).slice(0, 20);
  const nonMetaFirstPurchaseList = Object.entries(nonMetaFpMap)
    .map(([product, data]) => ({ product, qty: data.qty, revenue: Math.round(data.revenue * 100) / 100 }))
    .sort((a, b) => b.qty - a.qty).slice(0, 20);

  // ── Combos, add-ons, journeys: read from precomputed blob ──
  // NOTE: these are all-time analyses, not date-scoped. Blob refreshes on each sync.
  const metaCombos = blob.metaCombos || [];
  const nonMetaCombos = blob.nonMetaCombos || [];
  // Date-scoped add-on counts. We rebuild from the period's raw orders
  // every loader call (cached per fromKey:toKey via queryCached above).
  // Each order's lineItems is comma-separated; multi-item orders are the
  // ones that count toward "baskets". A product's appearances = number of
  // multi-item baskets containing it. Rate = appearances ÷ basket total.
  const allBaskets: string[][] = [];
  const metaBaskets: string[][] = [];
  for (const o of periodOrders) {
    const items = (o.lineItems || "")
      .split(", ").map((s) => toParentProduct(s.trim())).filter(Boolean);
    const unique = [...new Set(items)];
    if (unique.length < 2) continue; // only multi-product baskets count
    allBaskets.push(unique);
    if (metaOrderIdSet.has(o.shopifyOrderId)) metaBaskets.push(unique);
  }
  const countAddons = (baskets: string[][]) => {
    const counts: Record<string, number> = {};
    for (const basket of baskets) {
      for (const p of basket) counts[p] = (counts[p] || 0) + 1;
    }
    return counts;
  };
  const allCounts = countAddons(allBaskets);
  const metaCounts = countAddons(metaBaskets);
  const buildAddonList = (counts: Record<string, number>, total: number) =>
    Object.entries(counts)
      .filter(([, n]) => n >= 2)
      .map(([product, appearances]) => ({
        product,
        appearances,
        addonRate: total > 0 ? Math.round((appearances / total) * 100) : null,
      }))
      .sort((a, b) => b.appearances - a.appearances)
      .slice(0, 20);
  const topAddonsAll = buildAddonList(allCounts, allBaskets.length);
  const topAddonsMeta = buildAddonList(metaCounts, metaBaskets.length);
  const topAddons = topAddonsAll.slice(0, 20);

  const topGateway = blob.metaJourney?.topGateway || [];
  const topSecond = blob.metaJourney?.topSecond || [];
  const flows = blob.metaJourney?.flows || [];
  const topGatewayAll = blob.allJourney?.topGateway || [];
  const topSecondAll = blob.allJourney?.topSecond || [];
  const flowsAll = blob.allJourney?.flows || [];

  // ── Chart dates (shop-local) ──
  const chartDates: string[] = [];
  {
    let key = fromKey;
    while (key <= toKey) {
      chartDates.push(key);
      key = addDaysKey(key, 1);
    }
  }
  const dailyMetaOrdersChart = chartDates.map(date => ({
    date, metaOrders: dailyMetaItems[date] || 0,
  }));

  // ── Table rows ──
  const rows = Object.values(productData).map(pd => {
    const topCampaign = Object.entries(pd.campaigns).sort((a, b) => b[1] - a[1])[0];
    const topAdSet = Object.entries(pd.adSets).sort((a, b) => b[1] - a[1])[0];
    return {
      product: pd.product,
      totalOrders: pd.totalOrders, metaOrders: pd.metaOrders,
      metaNewOrders: pd.metaNewOrders, metaRepeatOrders: pd.metaRepeatOrders,
      metaRetargetedOrders: pd.metaRetargetedOrders, organicOrders: pd.organicOrders,
      totalRevenue: Math.round(pd.totalRevenue * 100) / 100,
      metaRevenue: Math.round(pd.metaRevenue * 100) / 100,
      metaNewRevenue: Math.round(pd.metaNewRevenue * 100) / 100,
      metaRepeatRevenue: Math.round(pd.metaRepeatRevenue * 100) / 100,
      metaRetargetedRevenue: Math.round(pd.metaRetargetedRevenue * 100) / 100,
      organicRevenue: Math.round(pd.organicRevenue * 100) / 100,
      aov: pd.totalOrders > 0 ? Math.round((pd.totalRevenue / pd.totalOrders) * 100) / 100 : 0,
      metaAov: pd.metaOrders > 0 ? Math.round((pd.metaRevenue / pd.metaOrders) * 100) / 100 : 0,
      refundRate: pd.totalOrders > 0 ? Math.round((pd.refundedOrders / pd.totalOrders) * 100) : 0,
      metaRefundRate: pd.metaOrders > 0 ? Math.round((pd.metaRefundedOrders / pd.metaOrders) * 100) : 0,
      totalRefunded: Math.round(pd.totalRefunded * 100) / 100,
      metaTotalRefunded: Math.round(pd.metaTotalRefunded * 100) / 100,
      refundedOrders: pd.refundedOrders,
      metaRefundedOrders: pd.metaRefundedOrders,
      firstPurchaseCount: pd.firstPurchaseCount,
      metaFirstPurchaseCount: pd.metaFirstPurchaseCount,
      gatewayPct: pd.totalOrders > 0 ? Math.round((pd.firstPurchaseCount / pd.totalOrders) * 100) : 0,
      metaPct: pd.totalOrders > 0 ? Math.round((pd.metaOrders / pd.totalOrders) * 100) : 0,
      collections: Array.from(pd.collections).join(", "),
      topCampaign: topCampaign ? topCampaign[0] : "",
      topCampaignCount: topCampaign ? topCampaign[1] : 0,
      topAdSet: topAdSet ? topAdSet[0] : "",
      topAdSetCount: topAdSet ? topAdSet[1] : 0,
      imageUrl: imageMap[pd.product] || "",
    };
  }).sort((a, b) => b.metaRevenue - a.metaRevenue);

  // ── Previous period stats (from rollup + blob) ──
  let prevMetaOrderCount = 0;
  let prevMetaItemCount = 0;
  const prevDailyMetaItems: Record<string, number> = {};
  for (const dateStr of Object.keys(blob.dailyBasketStats || {})) {
    if (dateStr < prevFromKey || dateStr > prevToKey) continue;
    const s = blob.dailyBasketStats[dateStr];
    prevMetaOrderCount += s.metaOrders || 0;
    prevMetaItemCount += s.metaItems || 0;
    prevDailyMetaItems[dateStr] = s.metaItems || 0;
  }
  const prevRefundData: Record<string, { refunded: number; total: number }> = {};
  for (const dateStr of Object.keys(blob.dailyRefundByProduct || {})) {
    if (dateStr < prevFromKey || dateStr > prevToKey) continue;
    const prodMap = blob.dailyRefundByProduct[dateStr];
    for (const product of Object.keys(prodMap)) {
      const d = prodMap[product];
      if (!prevRefundData[product]) prevRefundData[product] = { refunded: 0, total: 0 };
      prevRefundData[product].refunded += d.refunded;
      prevRefundData[product].total += d.total;
    }
  }
  const prevChartDates: string[] = [];
  {
    let key = prevFromKey;
    while (key <= prevToKey) {
      prevChartDates.push(key);
      key = addDaysKey(key, 1);
    }
  }
  const prevDailyMetaOrdersChart = prevChartDates.map(date => ({
    date, metaOrders: prevDailyMetaItems[date] || 0,
  }));
  const prevHighestRefund = Object.entries(prevRefundData)
    .filter(([, d]) => d.total >= 3)
    .map(([, d]) => Math.round((d.refunded / d.total) * 100))
    .sort((a, b) => b - a)[0] || 0;

  console.log(`[products] total ${Date.now() - _t0}ms`);

  // ── Summary stats ──
  const totalProductCount = rows.length;
  const totalMetaOrders = rows.reduce((s, r) => s + r.metaOrders, 0);
  const totalOrganicOrders = rows.reduce((s, r) => s + r.organicOrders, 0);
  const totalMetaRevenue = rows.reduce((s, r) => s + r.metaRevenue, 0);
  const totalOrganicRevenue = rows.reduce((s, r) => s + r.organicRevenue, 0);

  // Gateway: sort by metaFirstPurchaseCount (Meta-only) — blends % with volume naturally
  // Exclude Gift Card, require 5+ Meta orders
  const topGatewayProduct = rows
    .filter(r => r.metaFirstPurchaseCount > 0 && r.metaOrders >= 5 && r.product.toLowerCase() !== "gift card")
    .sort((a, b) => b.metaFirstPurchaseCount - a.metaFirstPurchaseCount)[0];
  const topMetaProduct = rows.filter(r => r.metaOrders > 0).sort((a, b) => b.metaRevenue - a.metaRevenue)[0];
  // Headline tile uses the Wilson lower bound on refund rate — the textbook
  // way to surface "statistically concerning" rates that don't get tricked
  // by tiny samples (1 of 1 = 100% but meaningless) or dominated by a single
  // big-ticket refund. Min 5 orders to enter the ranking. The score is the
  // 95% lower confidence bound on the binomial proportion; whichever product
  // the matcher is *most confident* has a high refund rate wins.
  const wilsonLower = (refunds: number, orders: number) => {
    if (orders <= 0) return 0;
    const z = 1.96;
    const p = refunds / orders;
    const denom = 1 + (z * z) / orders;
    const centre = p + (z * z) / (2 * orders);
    const margin = z * Math.sqrt((p * (1 - p) + (z * z) / (4 * orders)) / orders);
    return (centre - margin) / denom;
  };
  const refundCandidates = rows
    .filter(r => (r.totalOrders || 0) >= 5 && (r.refundedOrders || 0) > 0)
    .map(r => ({ ...r, wilsonScore: wilsonLower(r.refundedOrders || 0, r.totalOrders || 0) }));
  const highestRefundProduct = refundCandidates
    .sort((a, b) => (b.wilsonScore || 0) - (a.wilsonScore || 0))[0];

  // Top Meta Product daily chart (Meta orders only)
  const topMetaProductChart = topMetaProduct ? chartDates.map(date => ({
    date,
    qty: dailyMetaProductSales[topMetaProduct.product]?.[date]?.orders || 0,
  })) : [];

  // Highest refund product daily chart (orders vs refunds)
  const highestRefundChart = highestRefundProduct ? chartDates.map(date => ({
    date,
    orders: dailyProductSales[highestRefundProduct.product]?.[date]?.orders || 0,
    refunds: dailyProductSales[highestRefundProduct.product]?.[date]?.refunds || 0,
  })) : [];

  // Top 20 products by refund rate — separate for meta and all (min 3 orders)
  const top20RefundRateAll = rows
    .filter(r => r.totalOrders >= 3 && r.refundRate > 0)
    .sort((a, b) => b.refundRate - a.refundRate)
    .slice(0, 20)
    .map(r => ({
      product: r.product, refundRate: r.refundRate,
      refundedOrders: r.refundedOrders, totalOrders: r.totalOrders,
      totalRefunded: r.totalRefunded, imageUrl: r.imageUrl,
    }));
  const top20RefundRateMeta = rows
    .filter(r => r.metaOrders >= 3 && r.metaRefundRate > 0)
    .sort((a, b) => b.metaRefundRate - a.metaRefundRate)
    .slice(0, 20)
    .map(r => ({
      product: r.product, refundRate: r.metaRefundRate,
      refundedOrders: r.metaRefundedOrders, totalOrders: r.metaOrders,
      totalRefunded: r.metaTotalRefunded, imageUrl: r.imageUrl,
    }));

  // Revenue by product for top 10 (interactive bars)
  const revenueBarData = rows.slice(0, 10).map(r => ({
    product: r.product,
    metaRevenue: r.metaRevenue,
    organicRevenue: r.organicRevenue,
    totalRevenue: r.totalRevenue,
    metaOrders: r.metaOrders,
    organicOrders: r.organicOrders,
    totalOrders: r.totalOrders,
    metaPct: r.metaPct,
    aov: r.aov,
    imageUrl: r.imageUrl,
  }));

  // Basket stats
  const avgItemsPerBasket = totalOrderCount > 0 ? Math.round((totalItemCount / totalOrderCount) * 10) / 10 : 0;
  const metaAvgItemsPerBasket = metaOrderCount > 0 ? Math.round((metaItemCount / metaOrderCount) * 10) / 10 : 0;

  // ── Product Demographics Explorer data ──
  // Flat per-line-item records for Meta-acquired customers (metaSegment =
  // "metaNew" — covers both their first purchase and subsequent Meta Repeat
  // orders; excludes Meta Retargeted). Powers filter-driven product ranking
  // + auto-detected statistically significant "gems" on the client tile.
  // Uses a single indexed JOIN via $queryRaw — building an IN clause with
  // tens of thousands of order IDs on large shops (Vollebak: ~30k orders
  // in 90d) overwhelms SQLite and blows up the response. We then cap the
  // flat records to the top 100 products by volume so payload stays
  // bounded (~10k records max on a big shop) without losing gem signal —
  // a product that never makes top-100 overall is unlikely to be a true
  // "absolute gem" anyway.
  const _demoT0 = Date.now();
  const demoData = await queryCached(
    `${shopDomain}:demoProductRecords:${fromKey}:${toKey}`,
    DEFAULT_TTL,
    async () => {
      try {
        const rows = await db.$queryRaw<Array<{
          metaAge: string; metaGender: string;
          country: string | null; lineItems: string | null; frozenTotalPrice: number;
        }>>`
          SELECT a.metaAge, a.metaGender, o.country, o.lineItems, o.frozenTotalPrice
          FROM Attribution a
          JOIN "Order" o
            ON a.shopDomain = o.shopDomain AND a.shopifyOrderId = o.shopifyOrderId
          JOIN Customer c
            ON c.shopDomain = o.shopDomain AND c.shopifyCustomerId = o.shopifyCustomerId
          WHERE a.shopDomain = ${shopDomain}
            AND a.confidence > 0
            AND a.metaAge IS NOT NULL
            AND a.metaGender IS NOT NULL
            AND o.isOnlineStore = 1
            AND o.frozenTotalPrice > 0
            AND o.createdAt >= ${fromDate}
            AND o.createdAt <= ${toDate}
            AND c.metaSegment = 'metaNew'
        `;

        if (rows.length === 0) return { records: [], countries: [] as string[] };

        // First pass: build flat record list + per-product volume tally
        type Rec = { product: string; age: string; gender: string; country: string; revenue: number };
        const rawRecords: Rec[] = [];
        const countrySet = new Set<string>();
        const productVolume = new Map<string, number>();
        for (const o of rows) {
          const items = (o.lineItems || "").split(", ").map((s) => toParentProduct(s.trim())).filter(Boolean);
          if (items.length === 0) continue;
          const perItem = (o.frozenTotalPrice || 0) / items.length;
          const country = (o.country || "Unknown").trim() || "Unknown";
          countrySet.add(country);
          const gender = o.metaGender === "male" ? "Male" : o.metaGender === "female" ? "Female" : "Unknown";
          for (const product of items) {
            rawRecords.push({ product, age: o.metaAge, gender, country, revenue: perItem });
            productVolume.set(product, (productVolume.get(product) || 0) + 1);
          }
        }

        // Cap to top 100 products by volume to keep the JSON payload bounded
        const topProducts = new Set(
          [...productVolume.entries()].sort((a, b) => b[1] - a[1]).slice(0, 100).map(([p]) => p),
        );
        const records = rawRecords.filter((r) => topProducts.has(r.product));
        return { records, countries: [...countrySet].sort() };
      } catch (err) {
        console.error("[products] demoProductRecords failed:", err);
        return { records: [], countries: [] as string[] };
      }
    },
  );
  const demoRecords = demoData.records;
  const demoCountries = demoData.countries;
  console.log(`[products] demo ${Date.now() - _demoT0}ms (records=${demoRecords.length}, countries=${demoCountries.length})`);

  // ── Gem detection ──
  // For each top product × demographic slice, compare the slice's share of
  // that product to its overall share. A binomial z-test gates significance
  // (z >= 1.96, ~95% CI) and a count floor (8) filters noise. Score by
  // lift * sqrt(count) so a 3× lift on 30 purchases beats 1.6× on 8.
  const demoGems = (() => {
    if (demoRecords.length < 30) return [] as any[];
    const total = demoRecords.length;
    // Product totals
    const productTotals = new Map<string, number>();
    for (const r of demoRecords) productTotals.set(r.product, (productTotals.get(r.product) || 0) + 1);
    // Only consider top 20 by volume
    const topProducts = [...productTotals.entries()].sort((a, b) => b[1] - a[1]).slice(0, 20).map(([p]) => p);
    const topProductSet = new Set(topProducts);

    // Build slice indexes: per slice key -> {total, perProduct Map}
    const slices = new Map<string, { total: number; perProduct: Map<string, number>; kind: string; value: string }>();
    const bumpSlice = (kind: string, value: string, product: string) => {
      const key = `${kind}:${value}`;
      let s = slices.get(key);
      if (!s) { s = { total: 0, perProduct: new Map(), kind, value }; slices.set(key, s); }
      s.total++;
      s.perProduct.set(product, (s.perProduct.get(product) || 0) + 1);
    };
    for (const r of demoRecords) {
      if (!topProductSet.has(r.product)) continue;
      // single-dimension
      bumpSlice("gender", r.gender, r.product);
      bumpSlice("age", r.age, r.product);
      bumpSlice("country", r.country, r.product);
      // pairs
      bumpSlice("gender+age", `${r.gender}|${r.age}`, r.product);
      bumpSlice("gender+country", `${r.gender}|${r.country}`, r.product);
      bumpSlice("age+country", `${r.age}|${r.country}`, r.product);
    }

    type Gem = { product: string; kind: string; value: string; lift: number; count: number; expected: number; z: number; score: number };
    const gems: Gem[] = [];
    for (const product of topProducts) {
      const productTotal = productTotals.get(product)!;
      const productShare = productTotal / total;
      for (const s of slices.values()) {
        const countInSlice = s.perProduct.get(product) || 0;
        if (countInSlice < 8) continue;
        const expected = s.total * productShare;
        if (expected <= 0) continue;
        const lift = countInSlice / expected;
        if (lift < 1.5) continue;
        const variance = productShare * (1 - productShare) * s.total;
        const std = Math.sqrt(variance);
        const z = std > 0 ? (countInSlice - expected) / std : 0;
        if (z < 1.96) continue;
        gems.push({
          product, kind: s.kind, value: s.value, lift, count: countInSlice,
          expected, z, score: lift * Math.sqrt(countInSlice),
        });
      }
    }
    // De-dup: keep strongest gem per product (avoid same product in multiple slices dominating)
    gems.sort((a, b) => b.score - a.score);
    const seen = new Set<string>();
    const unique: Gem[] = [];
    for (const g of gems) {
      if (seen.has(g.product)) continue;
      seen.add(g.product);
      unique.push(g);
      if (unique.length >= 5) break;
    }
    return unique;
  })();

  // ── AI Insights cache ──
  const dateFromStr = fromKey;
  const dateToStr = toKey;
  const aiCached = await getCachedInsights(shopDomain, "products", dateFromStr, dateToStr);
  const aiCurrentHash = computeDataHash({
    totalProductCount, totalMetaOrders, totalOrganicOrders, totalMetaRevenue, totalOrganicRevenue,
    avgItemsPerBasket, metaAvgItemsPerBasket, rows: rows.slice(0, 20).map(r => ({ title: r.product || r.title, metaOrders: r.metaOrders, metaRevenue: r.metaRevenue })),
  });
  const aiCachedInsights = aiCached?.insights || null;
  const aiGeneratedAt = aiCached?.generatedAt?.toISOString() || null;
  const aiIsStale = aiCached ? aiCached.dataHash !== aiCurrentHash : false;

  return json({
    rows, currencySymbol: cs, imageMap,
    metaFirstPurchaseList, nonMetaFirstPurchaseList, metaCombos, nonMetaCombos,
    topGateway, topSecond, flows,
    topGatewayAll, topSecondAll, flowsAll,
    totalProductCount, totalMetaOrders, totalOrganicOrders, totalMetaRevenue, totalOrganicRevenue,
    metaOrderCount, metaItemCount, totalOrderCount, totalItemCount,
    avgItemsPerBasket, metaAvgItemsPerBasket,
    topGatewayProduct: topGatewayProduct ? {
      product: topGatewayProduct.product, gatewayPct: topGatewayProduct.gatewayPct,
      firstPurchaseCount: topGatewayProduct.firstPurchaseCount,
      metaFirstPurchaseCount: topGatewayProduct.metaFirstPurchaseCount,
      totalOrders: topGatewayProduct.totalOrders, metaOrders: topGatewayProduct.metaOrders,
      imageUrl: topGatewayProduct.imageUrl,
    } : null,
    topMetaProduct: topMetaProduct ? {
      product: topMetaProduct.product, metaRevenue: topMetaProduct.metaRevenue,
      metaOrders: topMetaProduct.metaOrders, imageUrl: topMetaProduct.imageUrl,
    } : null,
    highestRefundProduct: highestRefundProduct ? {
      product: highestRefundProduct.product,
      totalRefunded: highestRefundProduct.totalRefunded,
      refundRate: highestRefundProduct.refundRate,
      refundedOrders: highestRefundProduct.refundedOrders,
      totalOrders: highestRefundProduct.totalOrders,
      wilsonRate: Math.round((highestRefundProduct.wilsonScore || 0) * 100),
      imageUrl: highestRefundProduct.imageUrl,
    } : null,
    revenueBarData,
    dailyMetaOrdersChart,
    topMetaProductChart,
    highestRefundChart,
    top20RefundRateAll,
    top20RefundRateMeta,
    aiCachedInsights, aiGeneratedAt, aiIsStale,
    prevMetaOrderCount, prevMetaItemCount, prevHighestRefund,
    prevDailyMetaOrdersChart,
    topAddonsAll, topAddonsMeta,
    unmatchedConversions, unmatchedRevenue,
    demoRecords, demoCountries, demoGems,
    fromKey, toKey,
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

    (async () => {
      try {
        const shop = await db.shop.findUnique({ where: { shopDomain } });
        const tz = shop?.shopifyTimezone || "UTC";
        const { fromDate, toDate, fromKey: dateFromStr, toKey: dateToStr } = parseDateRange(request, tz);
        const cs = (shop?.shopifyCurrency || "GBP") === "GBP" ? "\u00a3" : (shop?.shopifyCurrency || "GBP") === "EUR" ? "\u20ac" : "$";

        const orders = await db.order.findMany({ where: { shopDomain, isOnlineStore: true, frozenTotalPrice: { gt: 0 }, createdAt: { gte: fromDate, lte: toDate } } });
        const attributions = await db.attribution.findMany({ where: { shopDomain, confidence: { gt: 0 } } });
        const attrMap = {};
        for (const a of attributions) attrMap[a.shopifyOrderId] = a;

        // Build product data. `order.lineItems` is stored as a comma-separated
        // list of titles (see orderWebhook.server.js), NOT JSON. Revenue is
        // split equally across items since per-item price isn't persisted —
        // this matches what productRollups.server.js does and is imperfect
        // but non-zero. Net of refunds.
        const productAgg = {};
        for (const o of orders) {
          const gross = o.frozenTotalPrice || 0;
          if (gross === 0) continue;
          const titles = (o.lineItems || "").split(", ").map(s => s.trim()).filter(Boolean);
          if (titles.length === 0) continue;
          const net = gross - (o.totalRefunded || 0);
          const rev = net / titles.length;
          const attr = attrMap[o.shopifyOrderId];
          for (const title of titles) {
            if (!productAgg[title]) productAgg[title] = { title, totalOrders: 0, metaOrders: 0, organicOrders: 0, totalRevenue: 0, metaRevenue: 0, organicRevenue: 0, metaNewOrders: 0, metaRepeatOrders: 0, firstPurchaseCount: 0, metaFirstPurchaseCount: 0 };
            const p = productAgg[title];
            p.totalOrders++;
            p.totalRevenue += rev;
            if (attr) {
              p.metaOrders++;
              p.metaRevenue += rev;
              if (attr.isNewCustomer) p.metaNewOrders++;
              else p.metaRepeatOrders++;
            } else {
              p.organicOrders++;
              p.organicRevenue += rev;
            }
          }
        }

        const rows = Object.values(productAgg).sort((a, b) => b.metaRevenue - a.metaRevenue);
        const totalMetaOrders = rows.reduce((s, r) => s + r.metaOrders, 0);
        const totalOrganicOrders = rows.reduce((s, r) => s + r.organicOrders, 0);
        const totalMetaRevenue = rows.reduce((s, r) => s + r.metaRevenue, 0);
        const totalOrganicRevenue = rows.reduce((s, r) => s + r.organicRevenue, 0);

        const pageData = {
          rows, totalProductCount: rows.length, totalMetaOrders, totalOrganicOrders,
          totalMetaRevenue, totalOrganicRevenue,
          avgItemsPerBasket: 0, metaAvgItemsPerBasket: 0,
          topGatewayProduct: rows[0]?.title || null,
          topMetaProduct: rows[0]?.title || null,
          highestRefundProduct: null,
          metaFirstPurchaseList: [],
          flows: [],
          top20RefundRateMeta: [],
        };

        await generateInsights(shopDomain, pageKey, pageData, dateFromStr, dateToStr, cs, promptOverrides);
        completeProgress(taskId, { success: true });
      } catch (err) {
        console.error("[AI] Product insights failed:", err);
        failProgress(taskId, err);
      }
    })();

    return json({ aiTaskId: taskId });
  }

  return json({});
};

// ── Product Thumbnail ──

function ProductThumb({ url, size = 44 }: { url?: string; size?: number }) {
  if (!url) {
    return (
      <div style={{
        width: size, height: size, borderRadius: 6, backgroundColor: "#F3F4F6",
        display: "flex", alignItems: "center", justifyContent: "center", flexShrink: 0,
        fontSize: size * 0.4, color: "#9CA3AF",
      }}>?</div>
    );
  }
  return (
    <img src={url + "&width=96"} alt="" style={{
      width: size, height: size, borderRadius: 6, objectFit: "cover", flexShrink: 0,
    }} />
  );
}

// ── Revenue Bar Chart (interactive with hover stats) ──

function RevenueBarChart({ data, cs }: {
  data: { product: string; metaRevenue: number; organicRevenue: number; totalRevenue: number; metaOrders: number; organicOrders: number; totalOrders: number; metaPct: number; aov: number; imageUrl: string }[];
  cs: string;
}) {
  const [hovered, setHovered] = useState<number | null>(null);
  const maxRevenue = Math.max(...data.map(d => d.totalRevenue), 1);
  return (
    <div>
      {data.map((item, i) => {
        const metaPct = item.totalRevenue > 0 ? (item.metaRevenue / item.totalRevenue) * 100 : 0;
        const barWidth = (item.totalRevenue / maxRevenue) * 100;
        const isHovered = hovered === i;
        return (
          <div key={item.product}
            onMouseEnter={() => setHovered(i)}
            onMouseLeave={() => setHovered(null)}
            style={{ marginBottom: 4, borderRadius: 6, padding: "6px 8px", transition: "background 0.15s", background: isHovered ? "#F9FAFB" : "transparent" }}
          >
            <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
              <ProductThumb url={item.imageUrl} size={28} />
              <div style={{ width: 140, fontSize: 12, fontWeight: 500, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", flexShrink: 0 }}>
                {item.product}
              </div>
              <div style={{ flex: 1, height: 22, background: "#F3F4F6", borderRadius: 4, overflow: "hidden" }}>
                <div style={{ width: `${barWidth}%`, height: "100%", display: "flex", borderRadius: 4, overflow: "hidden" }}>
                  <div style={{ width: `${metaPct}%`, height: "100%", background: "#7C3AED" }} />
                  <div style={{ width: `${100 - metaPct}%`, height: "100%", background: "#D1D5DB" }} />
                </div>
              </div>
              <div style={{ width: 70, textAlign: "right", fontSize: 12, fontWeight: 600, fontVariantNumeric: "tabular-nums", flexShrink: 0 }}>
                {cs}{Math.round(item.totalRevenue).toLocaleString()}
              </div>
            </div>
            {isHovered && (
              <div style={{ display: "flex", gap: 16, marginTop: 4, marginLeft: 38, fontSize: 11, color: "#6B7280" }}>
                <span>Meta: {cs}{Math.round(item.metaRevenue).toLocaleString()} ({Math.round(metaPct)}%)</span>
                <span>Organic: {cs}{Math.round(item.organicRevenue).toLocaleString()} ({Math.round(100 - metaPct)}%)</span>
                <span>{item.totalOrders} orders</span>
                <span>AOV: {cs}{Math.round(item.aov).toLocaleString()}</span>
              </div>
            )}
          </div>
        );
      })}
      <div style={{ display: "flex", gap: 16, marginTop: 8, justifyContent: "flex-end" }}>
        <div style={{ display: "flex", alignItems: "center", gap: 4 }}>
          <div style={{ width: 10, height: 10, borderRadius: 2, background: "#7C3AED" }} />
          <span style={{ fontSize: 11, color: "#6d7175" }}>Meta</span>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 4 }}>
          <div style={{ width: 10, height: 10, borderRadius: 2, background: "#D1D5DB" }} />
          <span style={{ fontSize: 11, color: "#6d7175" }}>Organic</span>
        </div>
      </div>
    </div>
  );
}

// ── Product Journey Flow ──

function ProductJourneyFlow({ topGateway, topSecond, flows, imageMap }: {
  topGateway: [string, number][]; topSecond: [string, number][];
  flows: { from: string; to: string; count: number }[];
  imageMap: Record<string, string>;
}) {
  const [hoveredProduct, setHoveredProduct] = useState<string | null>(null);
  const [hoveredSide, setHoveredSide] = useState<"left" | "right" | null>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const [containerWidth, setContainerWidth] = useState(900);

  useEffect(() => {
    if (!containerRef.current) return;
    const obs = new ResizeObserver(entries => {
      for (const e of entries) setContainerWidth(e.contentRect.width);
    });
    obs.observe(containerRef.current);
    setContainerWidth(containerRef.current.offsetWidth);
    return () => obs.disconnect();
  }, []);

  if (topGateway.length === 0 || topSecond.length === 0) {
    return <Text as="p" variant="bodySm" tone="subdued">Not enough repeat purchase data with different products yet.</Text>;
  }

  const colWidth = Math.min(340, containerWidth * 0.38);
  const flowGap = 20;
  const flowAreaWidth = Math.max(containerWidth - colWidth * 2 - flowGap * 2, 80);
  const itemHeight = 44;
  const itemGap = 8;
  const svgHeight = Math.max(topGateway.length, topSecond.length) * (itemHeight + itemGap) + 20;
  const maxFlow = Math.max(...flows.map(f => f.count), 1);

  const leftY = (i: number) => 10 + i * (itemHeight + itemGap) + itemHeight / 2;
  const rightY = (i: number) => 10 + i * (itemHeight + itemGap) + itemHeight / 2;
  const leftMap: Record<string, number> = {};
  topGateway.forEach(([name], i) => { leftMap[name] = i; });
  const rightMap: Record<string, number> = {};
  topSecond.forEach(([name], i) => { rightMap[name] = i; });

  // Build connected product sets for highlighting
  const connectedFromLeft: Record<string, Set<string>> = {};
  const connectedFromRight: Record<string, Set<string>> = {};
  for (const f of flows) {
    if (!connectedFromLeft[f.from]) connectedFromLeft[f.from] = new Set();
    connectedFromLeft[f.from].add(f.to);
    if (!connectedFromRight[f.to]) connectedFromRight[f.to] = new Set();
    connectedFromRight[f.to].add(f.from);
  }

  // Determine which flows/products are highlighted
  const isFlowActive = (f: { from: string; to: string }) => {
    if (!hoveredProduct) return null; // no highlight state
    if (hoveredSide === "left") return f.from === hoveredProduct;
    if (hoveredSide === "right") return f.to === hoveredProduct;
    return false;
  };

  const isLeftActive = (name: string) => {
    if (!hoveredProduct) return true;
    if (hoveredSide === "left") return name === hoveredProduct;
    if (hoveredSide === "right") return connectedFromRight[hoveredProduct]?.has(name) || false;
    return true;
  };

  const isRightActive = (name: string) => {
    if (!hoveredProduct) return true;
    if (hoveredSide === "right") return name === hoveredProduct;
    if (hoveredSide === "left") return connectedFromLeft[hoveredProduct]?.has(name) || false;
    return true;
  };

  return (
    <div ref={containerRef}>
      <div style={{ display: "flex", alignItems: "flex-start" }}>
        <div style={{ width: colWidth, flexShrink: 0 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
            <Text as="p" variant="headingSm" tone="subdued">First Purchase</Text>
            <HeaderTip text="Number of customers whose first order included this product" />
          </div>
          <div style={{ marginTop: 8 }}>
            {topGateway.map(([name, count]) => (
              <div key={name}
                onMouseEnter={() => { setHoveredProduct(name); setHoveredSide("left"); }}
                onMouseLeave={() => { setHoveredProduct(null); setHoveredSide(null); }}
                style={{
                  height: itemHeight, marginBottom: itemGap,
                  display: "flex", alignItems: "center", gap: 8,
                  padding: "0 12px", borderRadius: 6,
                  backgroundColor: isLeftActive(name) ? "#F3F0FF" : "#F9FAFB",
                  border: `1px solid ${isLeftActive(name) ? "#DDD6FE" : "#E5E7EB"}`,
                  opacity: isLeftActive(name) ? 1 : 0.25,
                  transition: "opacity 0.2s, background-color 0.2s",
                  cursor: "pointer",
                }}>
                <ProductThumb url={imageMap[name]} size={28} />
                <span style={{ fontSize: 13, fontWeight: 500, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", flex: 1 }}>{name}</span>
                <span style={{ fontSize: 12, color: "#6B7280", flexShrink: 0, fontWeight: 600 }}>{count}</span>
              </div>
            ))}
          </div>
        </div>

        <div style={{ width: flowAreaWidth, flexShrink: 0, margin: `0 ${flowGap / 2}px`, paddingTop: 28 }}>
          <svg width={flowAreaWidth} height={svgHeight} style={{ display: "block" }}>
            {flows.map(f => {
              const li = leftMap[f.from];
              const ri = rightMap[f.to];
              if (li === undefined || ri === undefined) return null;
              const y1 = leftY(li);
              const y2 = rightY(ri);
              const flowKey = `${f.from}\u2192${f.to}`;
              const strokeW = Math.max(1.5, (f.count / maxFlow) * 8);
              const active = isFlowActive(f);
              const isHighlighted = active === true;
              const isFaded = active === false;
              const midX = flowAreaWidth * 0.5;
              const midY = (y1 + y2) / 2;
              return (
                <g key={flowKey}>
                  <path
                    d={`M0,${y1} C${flowAreaWidth * 0.4},${y1} ${flowAreaWidth * 0.6},${y2} ${flowAreaWidth},${y2}`}
                    fill="none" stroke={isHighlighted ? "#7C3AED" : "#A78BFA"}
                    strokeWidth={isHighlighted ? strokeW + 1 : strokeW}
                    opacity={isFaded ? 0.06 : isHighlighted ? 1 : 0.4}
                    style={{ cursor: "pointer", transition: "opacity 0.2s, stroke 0.2s" }}
                    onMouseEnter={() => { setHoveredProduct(f.from); setHoveredSide("left"); }}
                    onMouseLeave={() => { setHoveredProduct(null); setHoveredSide(null); }}
                  />
                  {isHighlighted && (
                    <g>
                      <rect x={midX - 14} y={midY - 9} width={28} height={18} rx={4} fill="#7C3AED" />
                      <text x={midX} y={midY + 4} textAnchor="middle" fill="#fff" fontSize={10} fontWeight={600}>{f.count}</text>
                    </g>
                  )}
                </g>
              );
            })}
          </svg>
        </div>

        <div style={{ width: colWidth, flexShrink: 0 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
            <Text as="p" variant="headingSm" tone="subdued">Second Purchase (Different Product)</Text>
            <HeaderTip text="Number of customers who bought a different product in their second order" />
          </div>
          <div style={{ marginTop: 8 }}>
            {topSecond.map(([name, count]) => (
              <div key={name}
                onMouseEnter={() => { setHoveredProduct(name); setHoveredSide("right"); }}
                onMouseLeave={() => { setHoveredProduct(null); setHoveredSide(null); }}
                style={{
                  height: itemHeight, marginBottom: itemGap,
                  display: "flex", alignItems: "center", gap: 8,
                  padding: "0 12px", borderRadius: 6,
                  backgroundColor: isRightActive(name) ? "#F0FDF4" : "#F9FAFB",
                  border: `1px solid ${isRightActive(name) ? "#BBF7D0" : "#E5E7EB"}`,
                  opacity: isRightActive(name) ? 1 : 0.25,
                  transition: "opacity 0.2s, background-color 0.2s",
                  cursor: "pointer",
                }}>
                <ProductThumb url={imageMap[name]} size={28} />
                <span style={{ fontSize: 13, fontWeight: 500, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", flex: 1 }}>{name}</span>
                <span style={{ fontSize: 12, color: "#6B7280", flexShrink: 0, fontWeight: 600 }}>{count}</span>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}

// ── Styles ──

const pageStyles = `
.product-list-scroll { max-height: 440px; overflow-y: auto; }
.product-list-row { display: flex; justify-content: space-between; align-items: center; padding: 8px 12px; border-bottom: 1px solid #F3F4F6; }
.product-list-row:hover { background: #F9FAFB; }
.product-list-row .rank { width: 28px; color: #9CA3AF; font-size: 12px; flex-shrink: 0; }
.product-list-row .name { flex: 1; font-size: 13px; font-weight: 500; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.product-list-row .stat { text-align: right; font-size: 13px; min-width: 50px; margin-left: 12px; }
.product-list-header { display: flex; justify-content: space-between; padding: 6px 12px; border-bottom: 2px solid #E5E7EB; }
.product-list-header span { font-size: 11px; font-weight: 600; color: #6B7280; text-transform: uppercase; }
.segment-toggle { display: inline-flex; gap: 0; border: 1px solid #D1D5DB; border-radius: 5px; overflow: hidden; }
.segment-toggle button { padding: 4px 10px; font-size: 11px; font-weight: 500; border: none; cursor: pointer; transition: background 0.15s, color 0.15s; text-align: center; white-space: nowrap; }
.segment-toggle button.active { background: #7C3AED; color: white; }
.segment-toggle button:not(.active) { background: white; color: #374151; }
.segment-toggle button:not(.active):hover { background: #F3F4F6; }
.tile-header-row { display: flex; justify-content: space-between; align-items: flex-start; }
.tile-header-row .segment-toggle { margin: 0; }
.combo-table { width: 100%; border-collapse: collapse; }
.combo-table th { text-align: left; font-size: 11px; font-weight: 600; color: #6B7280; text-transform: uppercase; padding: 6px 10px; border-bottom: 2px solid #E5E7EB; }
.combo-table td { font-size: 13px; padding: 8px 10px; border-bottom: 1px solid #F3F4F6; }
.combo-table tr:hover td { background: #F9FAFB; }
.combo-table .num { text-align: right; font-variant-numeric: tabular-nums; font-weight: 600; }
.refund-table { width: 100%; border-collapse: collapse; }
.refund-table th { text-align: left; font-size: 11px; font-weight: 600; color: #6B7280; text-transform: uppercase; padding: 8px 12px; border-bottom: 2px solid #E5E7EB; }
.refund-table td { font-size: 13px; padding: 8px 12px; border-bottom: 1px solid #F3F4F6; }
.refund-table tr:hover td { background: #F9FAFB; }
.refund-table .num { text-align: right; font-variant-numeric: tabular-nums; }
.scrollable-list { max-height: 440px; overflow-y: auto; }
`;

// ── Component ──

function HeaderTip({ text }: { text: string }) {
  const [show, setShow] = useState(false);
  const ref = useRef<HTMLSpanElement>(null);
  const [pos, setPos] = useState<{ top: number; left: number } | null>(null);

  useEffect(() => {
    if (show && ref.current) {
      const rect = ref.current.getBoundingClientRect();
      setPos({ top: rect.top - 6, left: rect.left });
    }
  }, [show]);

  return (
    <>
      <span
        ref={ref}
        onMouseEnter={() => setShow(true)}
        onMouseLeave={() => setShow(false)}
        style={{ cursor: "help", fontSize: 11, color: "#9CA3AF", fontWeight: 600, lineHeight: 1, marginLeft: 4 }}
      >?</span>
      {show && pos && createPortal(
        <div style={{
          position: "fixed", top: pos.top, left: pos.left,
          transform: "translateY(-100%)",
          background: "#1e1e1e", color: "#fff", padding: "8px 12px", borderRadius: 6,
          fontSize: 11.5, fontWeight: 400, lineHeight: 1.5, width: 260, zIndex: 99999,
          boxShadow: "0 2px 8px rgba(0,0,0,0.25)", whiteSpace: "normal",
          pointerEvents: "none",
        }}>
          {text}
        </div>,
        document.body,
      )}
    </>
  );
}

// ═══════════════════════════════════════════════════════════════
// Product Demographics Explorer
// ═══════════════════════════════════════════════════════════════
// One tile, three filters (Gender / Age / Country), one ranked bar
// chart. Gems auto-surface statistically-significant over-indexed
// combinations at the top — clicking a gem snaps the filters.

const AGE_BRACKETS = ["13-17", "18-24", "25-34", "35-44", "45-54", "55-64", "65+"];

type DemoRecord = { product: string; age: string; gender: string; country: string; revenue: number };
type DemoGem = {
  product: string; kind: string; value: string;
  lift: number; count: number; expected: number; z: number; score: number;
};

function gemSentence(g: DemoGem, currencySymbol: string): { text: string; filters: { gender: string; ages: string[]; country: string } } {
  const parts = g.value.split("|");
  let gender = "All"; const ages: string[] = []; let country = "All";
  if (g.kind === "gender") gender = parts[0];
  else if (g.kind === "age") ages.push(parts[0]);
  else if (g.kind === "country") country = parts[0];
  else if (g.kind === "gender+age") { gender = parts[0]; ages.push(parts[1]); }
  else if (g.kind === "gender+country") { gender = parts[0]; country = parts[1]; }
  else if (g.kind === "age+country") { ages.push(parts[0]); country = parts[1]; }

  // Friendly description of the slice
  const sliceLabel = (() => {
    if (g.kind === "gender") return gender === "Male" ? "men" : gender === "Female" ? "women" : "unknown-gender buyers";
    if (g.kind === "age") return `buyers aged ${ages[0]}`;
    if (g.kind === "country") return `buyers in ${country}`;
    if (g.kind === "gender+age") return `${gender === "Male" ? "men" : gender === "Female" ? "women" : "buyers"} aged ${ages[0]}`;
    if (g.kind === "gender+country") return `${gender === "Male" ? "men" : gender === "Female" ? "women" : "buyers"} in ${country}`;
    if (g.kind === "age+country") return `${ages[0]} buyers in ${country}`;
    return "this segment";
  })();
  const liftText = `${g.lift.toFixed(1)}×`;
  return {
    text: `${g.product} is ${liftText} more popular with ${sliceLabel} (${g.count} purchases vs ~${Math.round(g.expected)} expected)`,
    filters: { gender, ages, country },
  };
}

// Palette for the bar chart rows — a cohesive cool-tone family (indigo →
// teal), desaturated slightly so rows vary without feeling like a rainbow.
const BAR_COLOURS: Array<[string, string]> = [
  ["#6366F1", "#A5B4FC"], // indigo
  ["#7C3AED", "#C4B5FD"], // violet
  ["#0EA5E9", "#BAE6FD"], // sky
  ["#0D9488", "#99F6E4"], // teal
  ["#4F46E5", "#C7D2FE"], // deeper indigo
];

function ProductDemographicsExplorer({ records, countries, gems, currencySymbol, imageMap }: {
  records: DemoRecord[];
  countries: string[];
  gems: DemoGem[];
  currencySymbol: string;
  imageMap: Record<string, string>;
}) {
  const [gender, setGender] = useState<"All" | "Female" | "Male">("All");
  const [ages, setAges] = useState<string[]>([]); // empty = all
  const [country, setCountry] = useState<string>("All");
  const [sortBy, setSortBy] = useState<"units" | "revenue">("units");

  const toggleAge = (bracket: string) => {
    setAges((prev) => prev.includes(bracket) ? prev.filter((a) => a !== bracket) : [...prev, bracket]);
  };

  const applyGem = (g: DemoGem) => {
    const { filters } = gemSentence(g, currencySymbol);
    setGender(filters.gender as any);
    setAges(filters.ages);
    setCountry(filters.country);
  };

  const filtered = useMemo(() => {
    return records.filter((r) => {
      if (gender !== "All" && r.gender !== gender) return false;
      if (ages.length > 0 && !ages.includes(r.age)) return false;
      if (country !== "All" && r.country !== country) return false;
      return true;
    });
  }, [records, gender, ages, country]);

  const topProducts = useMemo(() => {
    const agg = new Map<string, { units: number; revenue: number }>();
    for (const r of filtered) {
      const e = agg.get(r.product) || { units: 0, revenue: 0 };
      e.units++;
      e.revenue += r.revenue;
      agg.set(r.product, e);
    }
    return [...agg.entries()]
      .map(([product, v]) => ({ product, ...v }))
      .sort((a, b) => sortBy === "units" ? b.units - a.units : b.revenue - a.revenue)
      .slice(0, 10);
  }, [filtered, sortBy]);

  const insight = useMemo(() => {
    if (filtered.length === 0) return "";
    // Compose a one-liner: dominant gender + dominant age
    const genderCount: Record<string, number> = {};
    const ageCount: Record<string, number> = {};
    const countryCount: Record<string, number> = {};
    for (const r of filtered) {
      genderCount[r.gender] = (genderCount[r.gender] || 0) + 1;
      ageCount[r.age] = (ageCount[r.age] || 0) + 1;
      countryCount[r.country] = (countryCount[r.country] || 0) + 1;
    }
    const total = filtered.length;
    const [topGen, topGenN] = Object.entries(genderCount).sort((a, b) => b[1] - a[1])[0] || [null, 0];
    const [topAge, topAgeN] = Object.entries(ageCount).sort((a, b) => b[1] - a[1])[0] || [null, 0];
    const [topCountry, topCountryN] = Object.entries(countryCount).sort((a, b) => b[1] - a[1])[0] || [null, 0];
    const bits: string[] = [];
    if (topGen && topGenN / total >= 0.55) bits.push(`${Math.round(topGenN / total * 100)}% ${topGen === "Male" ? "men" : topGen === "Female" ? "women" : "unknown-gender"}`);
    if (topAge && topAgeN / total >= 0.25) bits.push(`${Math.round(topAgeN / total * 100)}% aged ${topAge}`);
    if (topCountry && topCountryN / total >= 0.25) bits.push(`${Math.round(topCountryN / total * 100)}% in ${topCountry}`);
    if (bits.length === 0) return `${total.toLocaleString()} purchases across ${Object.keys(ageCount).length} age brackets and ${Object.keys(countryCount).length} countries.`;
    return `${bits.join(" · ")} — out of ${total.toLocaleString()} purchases.`;
  }, [filtered]);

  const maxValue = topProducts.length > 0
    ? (sortBy === "units" ? topProducts[0].units : topProducts[0].revenue)
    : 1;

  const activeFilterCount = (gender !== "All" ? 1 : 0) + ages.length + (country !== "All" ? 1 : 0);

  return (
    <Card>
      <BlockStack gap="400">
        <BlockStack gap="100">
          <Text as="h2" variant="headingMd">Product Demographics Explorer</Text>
          <Text as="p" variant="bodySm" tone="subdued">
            Top products among Meta-acquired customers (Meta New + Meta Repeat) — filter by gender, age, and country to see what each segment buys.
          </Text>
        </BlockStack>

        {/* Filter bar */}
        <div style={{ display: "flex", flexDirection: "column", gap: "10px" }}>
          {/* Gender */}
          <div style={{ display: "flex", alignItems: "center", gap: "10px", flexWrap: "wrap" }}>
            <span style={{ fontSize: "12px", fontWeight: 600, color: "#6B7280", width: "70px", textTransform: "uppercase", letterSpacing: "0.5px" }}>Gender</span>
            {(["All", "Female", "Male"] as const).map((g) => (
              <button
                key={g}
                onClick={() => setGender(g)}
                style={{
                  padding: "6px 12px", fontSize: "12px", fontWeight: 600,
                  borderRadius: "6px", cursor: "pointer",
                  background: gender === g ? "#7C3AED" : "#fff",
                  color: gender === g ? "#fff" : "#4B5563",
                  border: `1px solid ${gender === g ? "#7C3AED" : "#E5E7EB"}`,
                  transition: "all 0.15s",
                }}
              >
                {g}
              </button>
            ))}
          </div>

          {/* Age */}
          <div style={{ display: "flex", alignItems: "center", gap: "10px", flexWrap: "wrap" }}>
            <span style={{ fontSize: "12px", fontWeight: 600, color: "#6B7280", width: "70px", textTransform: "uppercase", letterSpacing: "0.5px" }}>Age</span>
            <button
              onClick={() => setAges([])}
              style={{
                padding: "6px 12px", fontSize: "12px", fontWeight: 600,
                borderRadius: "6px", cursor: "pointer",
                background: ages.length === 0 ? "#7C3AED" : "#fff",
                color: ages.length === 0 ? "#fff" : "#4B5563",
                border: `1px solid ${ages.length === 0 ? "#7C3AED" : "#E5E7EB"}`,
              }}
            >
              All
            </button>
            {AGE_BRACKETS.map((b) => {
              const selected = ages.includes(b);
              return (
                <button
                  key={b}
                  onClick={() => toggleAge(b)}
                  style={{
                    padding: "6px 12px", fontSize: "12px", fontWeight: 600,
                    borderRadius: "6px", cursor: "pointer",
                    background: selected ? "#7C3AED" : "#fff",
                    color: selected ? "#fff" : "#4B5563",
                    border: `1px solid ${selected ? "#7C3AED" : "#E5E7EB"}`,
                  }}
                >
                  {b}
                </button>
              );
            })}
          </div>

          {/* Country */}
          <div style={{ display: "flex", alignItems: "center", gap: "10px", flexWrap: "wrap" }}>
            <span style={{ fontSize: "12px", fontWeight: 600, color: "#6B7280", width: "70px", textTransform: "uppercase", letterSpacing: "0.5px" }}>Country</span>
            <select
              value={country}
              onChange={(e) => setCountry(e.target.value)}
              style={{
                padding: "6px 10px", fontSize: "12px", fontWeight: 600,
                borderRadius: "6px", border: "1px solid #E5E7EB",
                background: country !== "All" ? "#F5F3FF" : "#fff",
                color: "#4B5563", cursor: "pointer", minWidth: "200px",
              }}
            >
              <option value="All">All countries</option>
              {countries.map((c) => <option key={c} value={c}>{c}</option>)}
            </select>
            {activeFilterCount > 0 && (
              <button
                onClick={() => { setGender("All"); setAges([]); setCountry("All"); }}
                style={{
                  padding: "6px 12px", fontSize: "12px", fontWeight: 500,
                  borderRadius: "6px", cursor: "pointer",
                  background: "transparent", color: "#6B7280",
                  border: "1px solid transparent", textDecoration: "underline",
                }}
              >
                Clear all filters
              </button>
            )}
          </div>
        </div>

        {/* Sort toggle */}
        <div style={{ display: "flex", alignItems: "center", gap: "10px" }}>
          <span style={{ fontSize: "12px", fontWeight: 600, color: "#6B7280", textTransform: "uppercase", letterSpacing: "0.5px" }}>Sort by</span>
          <div style={{ display: "inline-flex", borderRadius: "6px", border: "1px solid #E5E7EB", overflow: "hidden" }}>
            {(["units", "revenue"] as const).map((s) => (
              <button
                key={s}
                onClick={() => setSortBy(s)}
                style={{
                  padding: "6px 14px", fontSize: "12px", fontWeight: 600,
                  background: sortBy === s ? "#7C3AED" : "#fff",
                  color: sortBy === s ? "#fff" : "#4B5563",
                  border: "none", cursor: "pointer",
                }}
              >
                {s === "units" ? "Units" : "Revenue"}
              </button>
            ))}
          </div>
        </div>

        {/* Bar chart */}
        {topProducts.length === 0 ? (
          <div style={{ color: "#9CA3AF", fontSize: "13px", padding: "24px 0", textAlign: "center" }}>
            No purchases match these filters.
          </div>
        ) : (
          <div style={{ display: "flex", flexDirection: "column", gap: "10px" }}>
            {topProducts.map((p, i) => {
              const value = sortBy === "units" ? p.units : p.revenue;
              const widthPct = Math.max((value / maxValue) * 100, 2);
              const valueLabel = sortBy === "units"
                ? `${p.units.toLocaleString()} units`
                : `${currencySymbol}${Math.round(p.revenue).toLocaleString()}`;
              const secondary = sortBy === "units"
                ? `${currencySymbol}${Math.round(p.revenue).toLocaleString()}`
                : `${p.units.toLocaleString()} units`;
              const [c1, c2] = BAR_COLOURS[i % BAR_COLOURS.length];
              return (
                <div key={p.product} style={{ display: "flex", alignItems: "center", gap: "10px" }}>
                  <div style={{ width: "22px", fontSize: "12px", color: "#9CA3AF", fontWeight: 600, textAlign: "right" }}>{i + 1}.</div>
                  <ProductThumb url={imageMap[p.product]} size={36} />
                  <div style={{ flex: 1, minWidth: 0 }}>
                    <div style={{ fontSize: "13px", fontWeight: 600, color: "#1F2937", marginBottom: "3px", whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>
                      {p.product}
                    </div>
                    <div style={{ position: "relative", height: "22px", background: "#F3F4F6", borderRadius: "4px", overflow: "hidden" }}>
                      <div style={{
                        height: "100%", width: `${widthPct}%`,
                        background: `linear-gradient(90deg, ${c1}, ${c2})`,
                        borderRadius: "4px", transition: "width 0.4s ease",
                      }} />
                      <div style={{
                        position: "absolute", right: "8px", top: "50%", transform: "translateY(-50%)",
                        fontSize: "11px", color: "#1F2937",
                      }}>
                        <strong>{valueLabel}</strong>
                        <span style={{ color: "#6B7280", marginLeft: "8px" }}>· {secondary}</span>
                      </div>
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        )}

        {/* Gems — auto-surfaced statistical anomalies. Placed at the bottom
            so the chart is the primary interaction; gems are a light,
            colourful "did you notice" footer, no heavy framing. */}
        {gems.length > 0 && (
          <BlockStack gap="200">
            <Text as="h3" variant="headingSm">
              <span style={{ marginRight: 6 }}>💎</span>Gems spotted in this period
            </Text>
            <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
              {gems.map((g, i) => {
                const { text } = gemSentence(g, currencySymbol);
                return (
                  <button
                    key={i}
                    onClick={() => applyGem(g)}
                    style={{
                      textAlign: "left", background: "#fff",
                      border: "1px solid #E5E7EB", borderRadius: 6,
                      padding: "8px 12px", fontSize: 13, color: "#1F2937",
                      cursor: "pointer", display: "flex", alignItems: "flex-start", gap: 8,
                      lineHeight: 1.45, transition: "border-color 0.15s, background 0.15s",
                    }}
                    onMouseEnter={(e) => {
                      (e.currentTarget as HTMLElement).style.borderColor = "#6366F1";
                      (e.currentTarget as HTMLElement).style.background = "#F5F3FF";
                    }}
                    onMouseLeave={(e) => {
                      (e.currentTarget as HTMLElement).style.borderColor = "#E5E7EB";
                      (e.currentTarget as HTMLElement).style.background = "#fff";
                    }}
                  >
                    <span style={{ flexShrink: 0 }}>💎</span>
                    <span style={{ flex: 1 }}>{text}</span>
                    <span style={{ flexShrink: 0, color: "#6366F1", fontWeight: 600, fontSize: 12 }}>Apply →</span>
                  </button>
                );
              })}
            </div>
          </BlockStack>
        )}

        {/* Dynamic insight */}
        {insight && (
          <Text as="p" variant="bodySm" tone="subdued">{insight}</Text>
        )}
      </BlockStack>
    </Card>
  );
}

export default function Products() {
  const {
    rows, currencySymbol: cs, imageMap,
    metaFirstPurchaseList, nonMetaFirstPurchaseList, metaCombos, nonMetaCombos,
    topGateway, topSecond, flows,
    topGatewayAll, topSecondAll, flowsAll,
    totalProductCount, totalMetaOrders, totalOrganicOrders, totalMetaRevenue, totalOrganicRevenue,
    metaOrderCount, metaItemCount, totalOrderCount, totalItemCount,
    avgItemsPerBasket, metaAvgItemsPerBasket,
    topGatewayProduct, topMetaProduct, highestRefundProduct, revenueBarData,
    dailyMetaOrdersChart, topMetaProductChart, highestRefundChart,
    top20RefundRateAll, top20RefundRateMeta,
    aiCachedInsights, aiGeneratedAt, aiIsStale,
    prevMetaOrderCount, prevMetaItemCount, prevHighestRefund,
    prevDailyMetaOrdersChart,
    topAddonsAll, topAddonsMeta,
    unmatchedConversions, unmatchedRevenue,
    demoRecords, demoCountries, demoGems,
    fromKey, toKey,
  } = useLoaderData<typeof loader>();

  const fmtPrice = (v: number) => `${cs}${Math.round(v).toLocaleString()}`;
  const [firstPurchaseMode, setFirstPurchaseMode] = useState<"meta" | "other">("meta");
  const [basketMode, setBasketMode] = useState<"meta" | "other">("meta");
  const [refundMode, setRefundMode] = useState<"meta" | "all">("all");
  const baseFirstPurchases = firstPurchaseMode === "meta" ? metaFirstPurchaseList : nonMetaFirstPurchaseList;
  const activeCombos = basketMode === "meta" ? metaCombos : nonMetaCombos;
  const baseRefundList = refundMode === "meta" ? top20RefundRateMeta : top20RefundRateAll;

  type SortDir = "asc" | "desc";
  const [refundSortKey, setRefundSortKey] = useState<"refundRate" | "refundedOrders" | "totalOrders" | "totalRefunded" | "product">("refundRate");
  const [refundSortDir, setRefundSortDir] = useState<SortDir>("desc");
  const [firstSortKey, setFirstSortKey] = useState<"qty" | "revenue" | "product">("qty");
  const [firstSortDir, setFirstSortDir] = useState<SortDir>("desc");
  const cycleSort = (
    key: string,
    setKey: (k: any) => void,
    dir: SortDir,
    setDir: (d: SortDir) => void,
    currentKey: string,
  ) => {
    if (currentKey === key) setDir(dir === "asc" ? "desc" : "asc");
    else { setKey(key); setDir("desc"); }
  };
  const arrow = (active: boolean, dir: SortDir) => active ? (dir === "desc" ? " ▼" : " ▲") : "";
  const sortedBy = <T,>(arr: T[], key: keyof T | ((r: T) => any), dir: SortDir): T[] => {
    const get = typeof key === "function" ? key : (r: T) => r[key];
    return arr.slice().sort((a, b) => {
      const av = get(a), bv = get(b);
      if (av == null && bv == null) return 0;
      if (av == null) return 1;
      if (bv == null) return -1;
      if (typeof av === "string" && typeof bv === "string") {
        return dir === "asc" ? av.localeCompare(bv) : bv.localeCompare(av);
      }
      return dir === "asc" ? (av as any) - (bv as any) : (bv as any) - (av as any);
    });
  };
  const activeRefundList = sortedBy(baseRefundList, refundSortKey as any, refundSortDir);
  const activeFirstPurchases = sortedBy(baseFirstPurchases, firstSortKey as any, firstSortDir);
  const [journeyMode, setJourneyMode] = useState<"all" | "meta">("all");
  const [addonMode, setAddonMode] = useState<"all" | "meta">("all");
  const [addonSort, setAddonSort] = useState<"addonRate" | "appearances">("appearances");
  const [addonSortDir, setAddonSortDir] = useState<"asc" | "desc">("desc");
  const toggleAddonSort = (col: "addonRate" | "appearances") => {
    if (addonSort === col) setAddonSortDir(d => d === "desc" ? "asc" : "desc");
    else { setAddonSort(col); setAddonSortDir("desc"); }
  };
  const activeAddons = useMemo(() => {
    const data = addonMode === "meta" ? topAddonsMeta : topAddonsAll;
    return [...data].sort((a, b) => addonSortDir === "desc" ? b[addonSort] - a[addonSort] : a[addonSort] - b[addonSort]);
  }, [addonMode, addonSort, addonSortDir, topAddonsAll, topAddonsMeta]);

  const columns: ColumnDef<any>[] = useMemo(() => [
    { accessorKey: "product", header: "Product",
      meta: { minWidth: "240px", filterType: "text", description: "Parent product name (variants grouped)" },
      cell: ({ row }) => (
        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          <ProductThumb url={row.original.imageUrl} size={32} />
          <span style={{ fontWeight: 500 }}>{row.original.product}</span>
        </div>
      ) },
    { accessorKey: "metaOrders", header: "Meta Orders",
      meta: { description: "Total orders containing this product attributed to Meta ads" } },
    { accessorKey: "metaRevenue", header: "Meta Revenue",
      meta: { description: "Revenue from Meta-attributed orders containing this product" },
      cell: ({ getValue }) => fmtPrice(getValue() as number) },
    { accessorKey: "metaNewOrders", header: "New Customer Orders",
      meta: { description: "Orders from first-time buyers acquired via Meta" } },
    { accessorKey: "metaNewRevenue", header: "New Customer Revenue",
      meta: { description: "Revenue from first-time Meta-acquired customers buying this product" },
      cell: ({ getValue }) => fmtPrice(getValue() as number) },
    { accessorKey: "metaRepeatOrders", header: "Returning Orders",
      meta: { description: "Orders from customers originally acquired via Meta, coming back to buy again" } },
    { accessorKey: "metaRepeatRevenue", header: "Returning Revenue",
      meta: { description: "Revenue from returning Meta-acquired customers" },
      cell: ({ getValue }) => fmtPrice(getValue() as number) },
    { accessorKey: "metaRetargetedOrders", header: "Retargeted Orders",
      meta: { description: "Orders from existing customers who were shown a Meta ad" } },
    { accessorKey: "metaRetargetedRevenue", header: "Retargeted Revenue",
      meta: { description: "Revenue from retargeted customers buying this product" },
      cell: ({ getValue }) => fmtPrice(getValue() as number) },
    { accessorKey: "organicOrders", header: "Organic Orders",
      meta: { description: "Orders not attributed to Meta ads" } },
    { accessorKey: "organicRevenue", header: "Organic Revenue",
      meta: { description: "Revenue from orders not attributed to Meta" },
      cell: ({ getValue }) => fmtPrice(getValue() as number) },
    { accessorKey: "totalOrders", header: "Total Orders",
      meta: { description: "All online orders containing this product" } },
    { accessorKey: "totalRevenue", header: "Total Revenue",
      meta: { description: "Total revenue from all online orders containing this product" },
      cell: ({ getValue }) => fmtPrice(getValue() as number) },
    { accessorKey: "metaPct", header: "Meta %",
      meta: { description: "Percentage of this product's orders that came via Meta ads" },
      cell: ({ getValue }) => `${getValue()}%` },
    { accessorKey: "metaAov", header: "Meta AOV",
      meta: { description: "Average order value for Meta-attributed orders containing this product" },
      cell: ({ getValue }) => fmtPrice(getValue() as number) },
    { accessorKey: "aov", header: "AOV",
      meta: { description: "Average order value across all orders containing this product" },
      cell: ({ getValue }) => fmtPrice(getValue() as number) },
    { accessorKey: "firstPurchaseCount", header: "First Purchases",
      meta: { description: "Times this product was in a customer's very first order" } },
    { accessorKey: "metaFirstPurchaseCount", header: "Meta First Purchases",
      meta: { description: "Times this product was in a Meta-acquired customer's first ever order" } },
    { accessorKey: "gatewayPct", header: "Gateway %",
      meta: { description: "Percentage of this product's orders that were a customer's first purchase \u2014 higher = better at acquiring new customers" },
      cell: ({ getValue }) => `${getValue()}%` },
    { accessorKey: "refundRate", header: "Refund Rate",
      meta: { description: "Percentage of orders containing this product that were partially or fully refunded" },
      cell: ({ getValue }) => `${getValue()}%` },
    { accessorKey: "totalRefunded", header: "Refunded",
      meta: { description: "Total refund amount for orders containing this product" },
      cell: ({ getValue }) => fmtPrice(getValue() as number) },
    { accessorKey: "topCampaign", header: "Top Campaign",
      meta: { maxWidth: "180px", filterType: "multi-select", description: "Meta campaign that drove the most purchases of this product" },
      cell: ({ getValue }) => (getValue() as string) || "\u2014" },
    { accessorKey: "topAdSet", header: "Top Ad Set",
      meta: { maxWidth: "180px", filterType: "multi-select", description: "Meta ad set that drove the most purchases of this product" },
      cell: ({ getValue }) => (getValue() as string) || "\u2014" },
    { accessorKey: "collections", header: "Collections",
      meta: { maxWidth: "200px", filterType: "text", description: "Shopify collections this product belongs to" },
      cell: ({ getValue }) => (getValue() as string) || "\u2014" },
  ], [cs]);

  const defaultVisibleColumns = useMemo(() => [
    "product", "metaOrders", "metaRevenue", "metaNewOrders", "metaRepeatOrders",
    "metaRetargetedOrders", "metaPct", "gatewayPct", "topCampaign",
  ], []);

  const columnProfiles = useMemo(() => [
    { id: "overview", label: "Overview", icon: "\ud83d\udcca", description: "Key product metrics at a glance",
      columns: ["product", "metaOrders", "metaRevenue", "metaNewOrders", "metaRepeatOrders", "metaRetargetedOrders", "organicOrders", "metaPct"] },
    { id: "gateway", label: "Gateway", icon: "\ud83d\udeaa", description: "Which products acquire new customers",
      columns: ["product", "metaFirstPurchaseCount", "firstPurchaseCount", "gatewayPct", "metaNewOrders", "metaNewRevenue", "topCampaign", "topAdSet"] },
    { id: "revenue", label: "Revenue", icon: "\ud83d\udcb0", description: "Revenue breakdown by customer type",
      columns: ["product", "metaRevenue", "metaNewRevenue", "metaRepeatRevenue", "metaRetargetedRevenue", "organicRevenue", "totalRevenue", "metaAov", "aov"] },
    { id: "campaigns", label: "Campaigns", icon: "\ud83d\udce3", description: "Which campaigns drive which products",
      columns: ["product", "metaOrders", "metaRevenue", "topCampaign", "topAdSet", "metaPct"] },
    { id: "quality", label: "Quality", icon: "\u2705", description: "Refund rates and product quality signals",
      columns: ["product", "totalOrders", "totalRevenue", "refundRate", "totalRefunded", "collections"] },
    { id: "all", label: "All", icon: "\ud83d\udccb", description: "Every available column",
      columns: columns.map(c => (c as any).accessorKey || (c as any).id).filter(Boolean) },
  ], [columns]);

  const footerRow = useMemo(() => ({
    product: `${rows.length} products`,
    metaOrders: rows.reduce((s, r) => s + r.metaOrders, 0),
    metaRevenue: fmtPrice(rows.reduce((s, r) => s + r.metaRevenue, 0)),
    metaNewOrders: rows.reduce((s, r) => s + r.metaNewOrders, 0),
    metaNewRevenue: fmtPrice(rows.reduce((s, r) => s + r.metaNewRevenue, 0)),
    metaRepeatOrders: rows.reduce((s, r) => s + r.metaRepeatOrders, 0),
    metaRepeatRevenue: fmtPrice(rows.reduce((s, r) => s + r.metaRepeatRevenue, 0)),
    metaRetargetedOrders: rows.reduce((s, r) => s + r.metaRetargetedOrders, 0),
    metaRetargetedRevenue: fmtPrice(rows.reduce((s, r) => s + r.metaRetargetedRevenue, 0)),
    organicOrders: rows.reduce((s, r) => s + r.organicOrders, 0),
    organicRevenue: fmtPrice(rows.reduce((s, r) => s + r.organicRevenue, 0)),
    totalOrders: rows.reduce((s, r) => s + r.totalOrders, 0),
    totalRevenue: fmtPrice(rows.reduce((s, r) => s + r.totalRevenue, 0)),
    totalRefunded: fmtPrice(rows.reduce((s, r) => s + r.totalRefunded, 0)),
    firstPurchaseCount: rows.reduce((s, r) => s + r.firstPurchaseCount, 0),
    metaFirstPurchaseCount: rows.reduce((s, r) => s + r.metaFirstPurchaseCount, 0),
  }), [rows, cs]);

  // ── Page summary bullets ──
  // At-a-glance product read-out for the selected range, pulling from the
  // same derived values that power the tiles below (topMetaProduct,
  // topGatewayProduct, highestRefundProduct, etc.).
  const summaryBullets: SummaryBullet[] = useMemo(() => {
    const out: SummaryBullet[] = [];

    if (topMetaProduct) {
      out.push({
        tone: "positive",
        text: (
          <>
            <strong>Top Meta product:</strong> {topMetaProduct.product} — {cs}{Math.round(topMetaProduct.metaRevenue).toLocaleString()} rev across {topMetaProduct.metaOrders} Meta orders
          </>
        ),
      });
    }

    if (topGatewayProduct) {
      out.push({
        tone: "positive",
        text: (
          <>
            <strong>Top gateway product:</strong> {topGatewayProduct.product} — acquired {topGatewayProduct.metaFirstPurchaseCount} new Meta customers
          </>
        ),
      });
    }

    if (highestRefundProduct) {
      out.push({
        tone: "negative",
        text: (
          <>
            <strong>Highest refund rate:</strong> {highestRefundProduct.product} — {highestRefundProduct.refundRate}% ({highestRefundProduct.refundedOrders} of {highestRefundProduct.totalOrders} orders)
          </>
        ),
      });
    }

    if (metaOrderCount > 0) {
      out.push({
        tone: "neutral",
        text: (
          <>
            <strong>Meta basket:</strong> {metaAvgItemsPerBasket} items per order on average ({metaItemCount.toLocaleString()} items across {metaOrderCount.toLocaleString()} Meta orders)
          </>
        ),
      });
    }

    const metaShare = (totalMetaRevenue + totalOrganicRevenue) > 0
      ? Math.round((totalMetaRevenue / (totalMetaRevenue + totalOrganicRevenue)) * 100) : null;
    if (metaShare != null) {
      out.push({
        tone: "neutral",
        text: (
          <>
            <strong>Meta revenue share:</strong> {metaShare}% of product revenue ({cs}{Math.round(totalMetaRevenue).toLocaleString()} Meta vs {cs}{Math.round(totalOrganicRevenue).toLocaleString()} organic)
          </>
        ),
      });
    }

    if (totalProductCount > 0) {
      out.push({
        tone: "neutral",
        text: (
          <>
            <strong>Catalogue:</strong> {totalProductCount} distinct products sold in range
          </>
        ),
      });
    }

    return out;
  }, [topMetaProduct, topGatewayProduct, highestRefundProduct, metaOrderCount, metaItemCount, metaAvgItemsPerBasket, totalMetaRevenue, totalOrganicRevenue, totalProductCount, cs]);

  return (
    <Page title="Product Intelligence" fullWidth>
      <style dangerouslySetInnerHTML={{ __html: pageStyles }} />
      <ReportTabs>
      <BlockStack gap="500">

        <AiInsightsPanel
          pageKey="products"
          cachedInsights={aiCachedInsights}
          generatedAt={aiGeneratedAt}
          isStale={aiIsStale}
          currencySymbol={cs}
        />
        <PageSummary bullets={summaryBullets} fromKey={fromKey} toKey={toKey} />

        {/* ── All tiles (drag/drop, show/hide) — everything except main table ── */}
        <TileGrid pageId="products" columns={4} tiles={[
          { id: "metaAdOrders", label: "Meta Product Purchases", render: () => (
            <SummaryTile
              label="Meta Product Purchases"
              value={String(metaItemCount)}
              subtitle={`${metaOrderCount} orders · ${metaAvgItemsPerBasket} items/basket`}
              tooltip={{ definition: "Total items sold via Meta-attributed orders within the selected date range", calc: "Sum of all line item quantities across Meta-attributed orders" }}
              currentValue={metaItemCount} previousValue={prevMetaItemCount}
              chartData={dailyMetaOrdersChart}
              prevChartData={prevDailyMetaOrdersChart}
              chartKey="metaOrders"
              chartColor="#7C3AED"
              chartFormat={(v) => `${v} products`}
            />
          )},
          { id: "topMetaProduct", label: "Top Meta Product", render: () => topMetaProduct ? (
            <SummaryTile
              label="Top Meta Product"
              value={fmtPrice(topMetaProduct.metaRevenue)}
              subtitle={`${topMetaProduct.product} · ${topMetaProduct.metaOrders} orders`}
              tooltip={{ definition: "Product generating the most Meta-attributed revenue within the selected date range" }}
              chartData={topMetaProductChart}
              chartKey="qty"
              chartColor="#7C3AED"
              chartFormat={(v) => `${v} sold`}
              imageUrl={topMetaProduct.imageUrl}
            />
          ) : (
            <SummaryTile label="Top Meta Product" value={"\u2014"} subtitle="No Meta orders in this period" tooltip={{ definition: "Product generating the most Meta-attributed revenue within the selected date range" }} />
          )},
          { id: "bestGateway", label: "Best Gateway Product", render: () => topGatewayProduct ? (
            <SummaryTile
              label="Best Gateway Product"
              value={topGatewayProduct.product}
              valueVariant="headingMd"
              subtitle={`${topGatewayProduct.metaFirstPurchaseCount} new Meta customers · ${topGatewayProduct.metaOrders} Meta orders`}
              imageUrl={topGatewayProduct.imageUrl}
              tooltip={{ definition: "Product most often bought as a first purchase by Meta-acquired customers", calc: "Ranked by Meta first purchase count (min 5 Meta orders)" }}
            />
          ) : (
            <SummaryTile label="Best Gateway Product" value={"\u2014"} subtitle="Not enough data (5+ Meta orders required)" tooltip={{ definition: "Product most often bought as a first purchase by Meta-acquired customers" }} />
          )},
          { id: "highestRefund", label: "Highest Refunded Item", render: () => highestRefundProduct && (highestRefundProduct.refundedOrders || 0) > 0 ? (
            <SummaryTile
              label="Highest Refunded Item"
              value={`${highestRefundProduct.refundRate}%`}
              subtitle={`${highestRefundProduct.product} · ${highestRefundProduct.refundedOrders} of ${highestRefundProduct.totalOrders} refunded · ${fmtPrice(highestRefundProduct.totalRefunded)} lost`}
              tooltip={{ definition: "Product the matcher is most statistically confident has a high refund rate. Filters out tiny samples (min 5 orders) and one-off cash losses by ranking on the Wilson lower bound of the binomial proportion.", calc: `Wilson 95% lower bound on refunds ÷ orders. This product's true rate is at least ${highestRefundProduct.wilsonRate ?? 0}% with 95% confidence.` }}
              lowerIsBetter
              chartData={highestRefundChart}
              chartKey="refunds"
              chartColor="#dc2626"
              chartFormat={(v) => `${v} refunds`}
              imageUrl={highestRefundProduct.imageUrl}
            />
          ) : (
            <SummaryTile label="Highest Refunded Item" value={"\u2014"} subtitle="Not enough refund signal yet (5+ orders required)" tooltip={{ definition: "Product the matcher is most statistically confident has a high refund rate. Min 5 orders to enter the ranking." }} />
          )},
          { id: "productDemographicsExplorer", label: "Product Demographics Explorer", span: 4, render: () => (
            <ProductDemographicsExplorer
              records={demoRecords || []}
              countries={demoCountries || []}
              gems={demoGems || []}
              currencySymbol={cs}
              imageMap={imageMap}
            />
          )},
          { id: "refundRate", label: "Refund Rate Table", span: 2, render: () => (
            <Card>
              <div style={{ height: 340, display: "flex", flexDirection: "column" }}>
              <BlockStack gap="300">
                <div className="tile-header-row">
                  <BlockStack gap="100">
                    <Text as="h2" variant="headingLg">Products with Highest Refund Rate</Text>
                    <Text as="p" variant="bodySm" tone="subdued">Products with 3+ orders, ranked by refund rate</Text>
                  </BlockStack>
                  <div className="segment-toggle">
                    <button className={refundMode === "all" ? "active" : ""} onClick={() => setRefundMode("all")}>All Customers</button>
                    <button className={refundMode === "meta" ? "active" : ""} onClick={() => setRefundMode("meta")}>Meta Customers</button>
                  </div>
                </div>
                <div className="scrollable-list" style={{ flex: 1, overflow: "auto" }}>
                  {activeRefundList.length === 0 ? (
                    <div style={{ padding: 20, textAlign: "center", color: "#9CA3AF" }}>No refund data for this segment</div>
                  ) : (
                    <table className="refund-table">
                      <thead>
                        <tr>
                          <th style={{ width: 24 }}>#</th>
                          <th style={{ width: 36 }}></th>
                          <th onClick={() => cycleSort("product", setRefundSortKey, refundSortDir, setRefundSortDir, refundSortKey)} style={{ cursor: "pointer", userSelect: "none" }}>Product{arrow(refundSortKey === "product", refundSortDir)}</th>
                          <th className="num" onClick={() => cycleSort("refundRate", setRefundSortKey, refundSortDir, setRefundSortDir, refundSortKey)} style={{ cursor: "pointer", userSelect: "none" }}>Rate{arrow(refundSortKey === "refundRate", refundSortDir)}</th>
                          <th className="num" onClick={() => cycleSort("refundedOrders", setRefundSortKey, refundSortDir, setRefundSortDir, refundSortKey)} style={{ cursor: "pointer", userSelect: "none" }}>Refunds{arrow(refundSortKey === "refundedOrders", refundSortDir)}</th>
                          <th className="num" onClick={() => cycleSort("totalOrders", setRefundSortKey, refundSortDir, setRefundSortDir, refundSortKey)} style={{ cursor: "pointer", userSelect: "none" }}>Orders{arrow(refundSortKey === "totalOrders", refundSortDir)}</th>
                          <th className="num" onClick={() => cycleSort("totalRefunded", setRefundSortKey, refundSortDir, setRefundSortDir, refundSortKey)} style={{ cursor: "pointer", userSelect: "none" }}>Value{arrow(refundSortKey === "totalRefunded", refundSortDir)}</th>
                        </tr>
                      </thead>
                      <tbody>
                        {activeRefundList.map((item, i) => (
                          <tr key={item.product}>
                            <td style={{ color: "#9CA3AF", fontSize: 12 }}>{i + 1}</td>
                            <td><ProductThumb url={item.imageUrl} size={28} /></td>
                            <td style={{ fontWeight: 500, maxWidth: 180, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{item.product}</td>
                            <td className="num" style={{ fontWeight: 600, color: item.refundRate >= 30 ? "#dc2626" : item.refundRate >= 15 ? "#d97706" : "#374151" }}>
                              {item.refundRate}%
                            </td>
                            <td className="num">{item.refundedOrders}</td>
                            <td className="num">{item.totalOrders}</td>
                            <td className="num" style={{ fontWeight: 600 }}>{fmtPrice(item.totalRefunded)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  )}
                </div>
              </BlockStack>
              </div>
            </Card>
          )},
          { id: "firstPurchases", label: "First Purchases", span: 2, render: () => (
            <Card>
              <div style={{ height: 340, display: "flex", flexDirection: "column" }}>
              <BlockStack gap="300">
                <div className="tile-header-row">
                  <BlockStack gap="100">
                    <Text as="h2" variant="headingLg">New Customer First Purchases</Text>
                    <Text as="p" variant="bodySm" tone="subdued">What customers buy on their very first order — your gateway products</Text>
                  </BlockStack>
                  <div className="segment-toggle">
                    <button className={firstPurchaseMode === "meta" ? "active" : ""} onClick={() => setFirstPurchaseMode("meta")}>Meta Customers</button>
                    <button className={firstPurchaseMode === "other" ? "active" : ""} onClick={() => setFirstPurchaseMode("other")}>All Other</button>
                  </div>
                </div>
                <div className="product-list-header">
                  <span style={{ width: 28 }}>#</span>
                  <span style={{ width: 36 }}></span>
                  <span style={{ flex: 1, cursor: "pointer", userSelect: "none" }} onClick={() => cycleSort("product", setFirstSortKey, firstSortDir, setFirstSortDir, firstSortKey)}>Product{arrow(firstSortKey === "product", firstSortDir)}</span>
                  <span style={{ minWidth: 40, textAlign: "right", cursor: "pointer", userSelect: "none" }} onClick={() => cycleSort("qty", setFirstSortKey, firstSortDir, setFirstSortDir, firstSortKey)}>Qty{arrow(firstSortKey === "qty", firstSortDir)}</span>
                  <span style={{ minWidth: 70, textAlign: "right", cursor: "pointer", userSelect: "none" }} onClick={() => cycleSort("revenue", setFirstSortKey, firstSortDir, setFirstSortDir, firstSortKey)}>Revenue{arrow(firstSortKey === "revenue", firstSortDir)}</span>
                </div>
                <div className="scrollable-list" style={{ flex: 1, overflow: "auto" }}>
                  {activeFirstPurchases.map((item, i) => (
                    <div key={item.product} className="product-list-row">
                      <span className="rank">{i + 1}</span>
                      <ProductThumb url={imageMap[item.product]} size={28} />
                      <span className="name" style={{ marginLeft: 8 }}>{item.product}</span>
                      <span className="stat">{item.qty}</span>
                      <span className="stat" style={{ minWidth: 70, fontWeight: 600 }}>{fmtPrice(item.revenue)}</span>
                    </div>
                  ))}
                  {activeFirstPurchases.length === 0 && (
                    <div style={{ padding: 20, textAlign: "center", color: "#9CA3AF" }}>No first purchase data in this period</div>
                  )}
                </div>
              </BlockStack>
              </div>
            </Card>
          )},
          { id: "revenueByProduct", label: "Revenue by Product", span: 2, render: () => (
            <Card>
              <div style={{ height: 340, display: "flex", flexDirection: "column" }}>
              <BlockStack gap="300">
                <BlockStack gap="100">
                  <Text as="h2" variant="headingLg">Revenue by Product</Text>
                  <Text as="p" variant="bodySm" tone="subdued">Top 10 products by total revenue. Hover for breakdown.</Text>
                </BlockStack>
                <div style={{ flex: 1, overflow: "auto" }}>
                  <RevenueBarChart data={revenueBarData} cs={cs} />
                </div>
              </BlockStack>
              </div>
            </Card>
          )},
          { id: "basketAnalysis", label: "Basket Analysis", span: 2, render: () => (
            <Card>
              <div style={{ height: 340, display: "flex", flexDirection: "column" }}>
              <BlockStack gap="300">
                <div className="tile-header-row">
                  <BlockStack gap="100">
                    <Text as="h2" variant="headingLg">Basket Analysis</Text>
                    <Text as="p" variant="bodySm" tone="subdued">Products most frequently purchased together</Text>
                  </BlockStack>
                  <div className="segment-toggle">
                    <button className={basketMode === "meta" ? "active" : ""} onClick={() => setBasketMode("meta")}>Meta Customers</button>
                    <button className={basketMode === "other" ? "active" : ""} onClick={() => setBasketMode("other")}>All Other</button>
                  </div>
                </div>
                {activeCombos.length === 0 ? (
                  <div style={{ padding: 20, textAlign: "center", color: "#9CA3AF" }}>No multi-item orders in this period</div>
                ) : (
                  <div className="scrollable-list" style={{ flex: 1, overflow: "auto" }}>
                    <table className="combo-table">
                      <thead>
                        <tr>
                          <th style={{ width: 24 }}>#</th>
                          <th>Product 1</th>
                          <th>Product 2</th>
                          <th className="num">Times</th>
                        </tr>
                      </thead>
                      <tbody>
                        {activeCombos.map((item, i) => (
                          <tr key={`${item.product1}-${item.product2}`}>
                            <td style={{ color: "#9CA3AF", fontSize: 12 }}>{i + 1}</td>
                            <td>
                              <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                                <ProductThumb url={imageMap[item.product1]} size={24} />
                                <span title={item.product1} style={{ fontSize: 11.5, fontWeight: 500, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", maxWidth: 160 }}>{item.product1}</span>
                              </div>
                            </td>
                            <td>
                              <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                                <ProductThumb url={imageMap[item.product2]} size={24} />
                                <span title={item.product2} style={{ fontSize: 11.5, fontWeight: 500, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", maxWidth: 160 }}>{item.product2}</span>
                              </div>
                            </td>
                            <td className="num">{item.count}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </BlockStack>
              </div>
            </Card>
          )},
          { id: "topAddons", label: "Top Add-on Products", span: 2, render: () => (
            <Card>
              <div style={{ height: 340, display: "flex", flexDirection: "column" }}>
              <BlockStack gap="300">
                <div className="tile-header-row">
                  <BlockStack gap="100">
                    <Text as="h2" variant="headingLg">Top Add-on Products</Text>
                    <Text as="p" variant="bodySm" tone="subdued">Products most frequently purchased alongside other items. Consider promoting these products throughout your website.</Text>
                  </BlockStack>
                  <div className="segment-toggle">
                    <button className={addonMode === "meta" ? "active" : ""} onClick={() => setAddonMode("meta")}>Meta Customers</button>
                    <button className={addonMode === "all" ? "active" : ""} onClick={() => setAddonMode("all")}>All Customers</button>
                  </div>
                </div>
                {activeAddons.length === 0 ? (
                  <div style={{ padding: 20, textAlign: "center", color: "#9CA3AF" }}>No multi-item orders in this period</div>
                ) : (
                  <div className="scrollable-list" style={{ flex: 1, overflow: "auto" }}>
                    <table className="combo-table">
                      <thead>
                        <tr>
                          <th style={{ width: 24 }}>#</th>
                          <th>Product</th>
                          <th className="num" style={{ cursor: "pointer", userSelect: "none" }} onClick={() => toggleAddonSort("appearances")}>
                            Baskets{addonSort === "appearances" ? (addonSortDir === "desc" ? " ↓" : " ↑") : ""}
                            <HeaderTip text="How many multi-item orders included this product" />
                          </th>
                          <th className="num" style={{ cursor: "pointer", userSelect: "none" }} onClick={() => toggleAddonSort("addonRate")}>
                            Paired %{addonSort === "addonRate" ? (addonSortDir === "desc" ? " ↓" : " ↑") : ""}
                            <HeaderTip text="Of all orders for this product, what % were bought alongside at least one other product (not purchased alone)" />
                          </th>
                        </tr>
                      </thead>
                      <tbody>
                        {activeAddons.map((item, i) => (
                          <tr key={item.product}>
                            <td style={{ color: "#9CA3AF", fontSize: 12 }}>{i + 1}</td>
                            <td>
                              <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                                <ProductThumb url={imageMap[item.product]} size={24} />
                                <span title={item.product} style={{ fontSize: 11.5, fontWeight: 500, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", maxWidth: 200 }}>{item.product}</span>
                              </div>
                            </td>
                            <td className="num">{item.appearances}</td>
                            <td className="num">{item.addonRate == null ? "—" : `${item.addonRate}%`}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </BlockStack>
              </div>
            </Card>
          )},
          { id: "productJourney", label: "Product Journey", span: 4, render: () => (
            <Card>
              <BlockStack gap="400">
                <div className="tile-header-row">
                  <BlockStack gap="100">
                    <Text as="h2" variant="headingLg">Customer Product Journey</Text>
                    <Text as="p" variant="bodySm" tone="subdued">What customers buy first, and what different product they buy next. Same-product repurchases excluded.</Text>
                  </BlockStack>
                  <div className="segment-toggle">
                    <button className={journeyMode === "meta" ? "active" : ""} onClick={() => setJourneyMode("meta")}>Meta Customers</button>
                    <button className={journeyMode === "all" ? "active" : ""} onClick={() => setJourneyMode("all")}>All Customers</button>
                  </div>
                </div>
                <div style={{ maxHeight: 560, overflow: "auto" }}>
                  <ProductJourneyFlow
                    topGateway={journeyMode === "meta" ? topGateway : topGatewayAll}
                    topSecond={journeyMode === "meta" ? topSecond : topSecondAll}
                    flows={journeyMode === "meta" ? flows : flowsAll}
                    imageMap={imageMap}
                  />
                </div>
              </BlockStack>
            </Card>
          )},
        ] as TileDef[]} />

        {/* ── Product Table ── */}
        <Card>
          <BlockStack gap="300">
            <BlockStack gap="100">
              <Text as="h2" variant="headingLg">Product Breakdown</Text>
              <Text as="p" variant="bodySm" tone="subdued">Every product sold online, with Meta attribution and customer type breakdown. Variants grouped by parent product.</Text>
            </BlockStack>
            <InteractiveTable
              columns={columns}
              data={rows}
              defaultVisibleColumns={defaultVisibleColumns}
              tableId="products"
              columnProfiles={columnProfiles}
              footerRow={footerRow}
            />
          </BlockStack>
        </Card>

      </BlockStack>
      </ReportTabs>
    </Page>
  );
}
