/**
 * Cache warmer.
 *
 * On server boot, pre-populates the in-process query cache and warms SQLite's
 * page cache for the common loader queries across the most-used date ranges.
 *
 * The first tab load after a deploy was taking 3-15 seconds because every
 * SQLite page had to be read from disk. With these queries pre-fired, the
 * pages are already in the OS page cache / SQLite mmap AND the in-process
 * queryCache is populated, so user loads hit <300 ms immediately.
 *
 * Runs 30 seconds after boot (lets the server stabilize first) and then
 * after each successful sync cycle (cache invalidation).
 */

import db from "../db.server.js";
import { cached as queryCached, DEFAULT_TTL } from "./queryCache.server.js";
import { loadLtvSnapshot } from "./ltvSnapshot.server.js";
import { shopLocalToday, shopLocalDayKey, shopRangeBounds } from "../utils/shopTime.server";

const TTL = DEFAULT_TTL;

// Compute the date ranges for every preset the date selector exposes,
// bucketed in the shop's local timezone so cache keys align with the keys
// produced by parseDateRange(request, tz) in loaders.
function computeRanges(tz) {
  const todayKey = shopLocalToday(tz);
  const addDaysKey = (key, delta) => {
    const [y, m, d] = key.split("-").map(Number);
    const anchor = new Date(Date.UTC(y, m - 1, d, 12, 0, 0, 0));
    anchor.setUTCDate(anchor.getUTCDate() + delta);
    return shopLocalDayKey(tz, anchor);
  };
  const yesterdayKey = addDaysKey(todayKey, -1);
  const toBounds = (fromKey, toKey) => {
    const b = shopRangeBounds(tz, fromKey, toKey);
    return { fromKey, toKey, from: b.gte, to: b.lte };
  };
  const dayBack = (n) => toBounds(addDaysKey(yesterdayKey, -(n - 1)), yesterdayKey);

  const ranges = [
    { name: "last7", ...dayBack(7) },
    { name: "last14", ...dayBack(14) },
    { name: "last30", ...dayBack(30) },
    { name: "last90", ...dayBack(90) },
    { name: "last365", ...dayBack(365) },
  ];

  const [ty, tm] = todayKey.split("-").map(Number);
  ranges.push({
    name: "thisMonth",
    ...toBounds(`${ty}-${String(tm).padStart(2, "0")}-01`, yesterdayKey),
  });
  {
    const prevAnchor = new Date(Date.UTC(ty, tm - 2, 1, 12, 0, 0, 0));
    const prevY = prevAnchor.getUTCFullYear();
    const prevM = prevAnchor.getUTCMonth() + 1;
    const fromKey = `${prevY}-${String(prevM).padStart(2, "0")}-01`;
    const lastDayAnchor = new Date(Date.UTC(ty, tm - 1, 0, 12, 0, 0, 0));
    const toKey = shopLocalDayKey(tz, lastDayAnchor);
    ranges.push({ name: "lastMonth", ...toBounds(fromKey, toKey) });
  }
  ranges.push({ name: "thisYear", ...toBounds(`${ty}-01-01`, yesterdayKey) });

  return ranges;
}

// Mirrors app.campaigns.tsx aggregateBreakdownByLevel() so the warmer can
// populate the same cache keys the loader will later read.
async function buildBreakdownAgg(shopDomain, breakdownType, from, to) {
  const r2 = v => Math.round(v * 100) / 100;
  const levels = [
    { key: "overall", groupBy: ["breakdownValue"] },
    { key: "campaign", groupBy: ["breakdownValue", "campaignId", "campaignName"] },
    { key: "adset", groupBy: ["breakdownValue", "campaignId", "campaignName", "adSetId", "adSetName"] },
    { key: "ad", groupBy: ["breakdownValue", "campaignId", "campaignName", "adSetId", "adSetName", "adId", "adName"] },
  ];
  const result = {};
  for (const level of levels) {
    const raw = await db.metaBreakdown.groupBy({
      by: level.groupBy,
      where: { shopDomain, breakdownType, date: { gte: from, lte: to } },
      _sum: { spend: true, impressions: true, clicks: true, conversions: true, conversionValue: true },
    });
    result[level.key] = raw.map(r => {
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
        spend: r2(spend),
        impressions: r._sum?.impressions || 0,
        clicks: r._sum?.clicks || 0,
        conversions,
        revenue: r2(revenue),
        roas: spend > 0 ? r2(revenue / spend) : 0,
        cpa: conversions > 0 ? r2(spend / conversions) : 0,
        ctr: (r._sum?.impressions || 0) > 0 ? r2(((r._sum?.clicks || 0) / (r._sum?.impressions || 0)) * 100) : 0,
      };
    }).sort((a, b) => b.conversions - a.conversions || b.spend - a.spend);
  }
  return result;
}

// Inline copy of aggregateRollupAllLevels — kept here to avoid importing from
// a .tsx route file. If the logic changes, update both places.
function aggregateRollupAllLevels(rows) {
  const campaign = {}, adset = {}, ad = {};
  const empty = (id, name, ci, cn, ai, an) => ({
    id, name, campaignId: ci || null, campaignName: cn || null,
    adSetId: ai || null, adSetName: an || null,
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
    row.spend += r.spend; row.impressions += r.impressions; row.clicks += r.clicks;
    row.metaConversions += r.metaConversions; row.metaConversionValue += r.metaConversionValue;
    row.linkClicks += r.linkClicks; row.landingPageViews += r.landingPageViews;
    row.viewContent += r.viewContent; row.addToCart += r.addToCart; row.initiateCheckout += r.initiateCheckout;
    row.videoP25 += r.videoP25; row.videoP50 += r.videoP50; row.videoP75 += r.videoP75; row.videoP100 += r.videoP100;
    row.frequencySum += r.frequencySum; row.frequencyCount += r.frequencyCount;
    row.attributedOrders += r.attributedOrders; row.attributedRevenue += r.attributedRevenue;
    row.newCustomerOrders += r.newCustomerOrders; row.newCustomerRevenue += r.newCustomerRevenue;
    row.existingCustomerOrders += r.existingCustomerOrders; row.existingCustomerRevenue += r.existingCustomerRevenue;
    row.unverifiedRevenue += r.unverifiedRevenue;
    row.utmOnlyOrders += r.utmOnlyOrders || 0; row.utmOnlyRevenue += r.utmOnlyRevenue || 0;
  };
  for (const r of rows) {
    if (r.campaignId) {
      if (!campaign[r.campaignId]) campaign[r.campaignId] = empty(r.campaignId, r.campaignName || r.campaignId, null, null, null, null);
      addTo(campaign[r.campaignId], r);
    }
    if (r.adSetId) {
      if (!adset[r.adSetId]) adset[r.adSetId] = empty(r.adSetId, r.adSetName || r.adSetId, r.campaignId, r.campaignName || r.campaignId, null, null);
      addTo(adset[r.adSetId], r);
    }
    if (r.adId) {
      if (!ad[r.adId]) ad[r.adId] = empty(r.adId, r.adName || r.adId, r.campaignId, r.campaignName || r.campaignId, r.adSetId, r.adSetName || r.adSetId);
      addTo(ad[r.adId], r);
    }
  }
  return { campaign, adset, ad };
}

async function warmShop(shopDomain) {
  const t0 = Date.now();
  const shopRow = await db.shop.findUnique({ where: { shopDomain }, select: { shopifyTimezone: true } });
  const tz = shopRow?.shopifyTimezone || "UTC";
  const ranges = computeRanges(tz);

  // 1. Customer-level caches (no date key)
  const tasks = [];

  tasks.push(async () => {
    const customers = await queryCached(`${shopDomain}:customersAll`, TTL, () =>
      db.customer.findMany({
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
      }),
    );
    // Pre-build the customer rows array (~80ms work) so the loader skips it
    const r2 = v => Math.round(v * 100) / 100;
    const DAY_MS = 86400000;
    const now = Date.now();
    await queryCached(`${shopDomain}:customerRows`, TTL, async () => {
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
  });

  tasks.push(() => queryCached(`${shopDomain}:customersBlobs`, TTL, async () => {
    const rows = await db.shopAnalysisCache.findMany({
      where: { shopDomain, cacheKey: { in: ["customers:ltv", "customers:journey", "customers:geo"] } },
    });
    const out = { ltv: null, journey: null, geo: null };
    for (const r of rows) {
      if (r.cacheKey === "customers:ltv") out.ltv = JSON.parse(r.payload);
      else if (r.cacheKey === "customers:journey") out.journey = JSON.parse(r.payload);
      else if (r.cacheKey === "customers:geo") out.geo = JSON.parse(r.payload);
    }
    return out;
  }));

  tasks.push(() => queryCached(`${shopDomain}:allTimeAdSpend`, TTL, () =>
    db.dailyAdRollup.aggregate({ where: { shopDomain }, _sum: { spend: true } }),
  ));

  tasks.push(() => queryCached(`${shopDomain}:metaEntities`, TTL, () =>
    db.metaEntity.findMany({
      where: { shopDomain },
      select: { entityType: true, entityId: true, createdTime: true },
    }),
  ));

  tasks.push(() => queryCached(`${shopDomain}:productsAnalysis`, TTL, () =>
    db.shopAnalysisCache.findUnique({
      where: { shopDomain_cacheKey: { shopDomain, cacheKey: "products:analysis" } },
    }),
  ));

  // Warm shop row (SQLite page cache) — not queryCached because it's a single row
  tasks.push(() => db.shop.findUnique({ where: { shopDomain } }));

  // Order Explorer: customers (shared across all date ranges)
  tasks.push(() => queryCached(`${shopDomain}:ordersCustomers`, TTL, () =>
    db.customer.findMany({
      where: { shopDomain },
      select: { shopifyCustomerId: true, firstOrderDate: true, metaSegment: true },
    }),
  ));

  // ltvSnapshot is the most expensive query in campaigns loader. Pre-warm it
  // so the first user click finds it cached.
  tasks.push(() => queryCached(`${shopDomain}:ltvSnapshot`, TTL, () => loadLtvSnapshot(shopDomain)));

  const addDaysKey = (key, delta) => {
    const [y, m, d] = key.split("-").map(Number);
    const anchor = new Date(Date.UTC(y, m - 1, d, 12, 0, 0, 0));
    anchor.setUTCDate(anchor.getUTCDate() + delta);
    return shopLocalDayKey(tz, anchor);
  };
  const diffDays = (a, b) => {
    const [ay, am, ad] = a.split("-").map(Number);
    const [by, bm, bd] = b.split("-").map(Number);
    return Math.round((Date.UTC(by, bm - 1, bd) - Date.UTC(ay, am - 1, ad)) / 86400000);
  };

  // 2. Per-range caches
  for (const { from, to, fromKey, toKey } of ranges) {
    // Prev period (same size, immediately before) in shop-local time
    const days = diffDays(fromKey, toKey) + 1;
    const prevToKey = addDaysKey(fromKey, -1);
    const prevFromKey = addDaysKey(prevToKey, -(days - 1));
    const prevBounds = shopRangeBounds(tz, prevFromKey, prevToKey);
    const prevFrom = prevBounds.gte;
    const prevTo = prevBounds.lte;

    // customers: age + gender breakdowns
    tasks.push(() => queryCached(`${shopDomain}:mbAge:${fromKey}:${toKey}`, TTL, () =>
      db.metaBreakdown.groupBy({
        by: ["breakdownValue"],
        where: { shopDomain, breakdownType: "age", date: { gte: from, lte: to } },
        _sum: { conversions: true, conversionValue: true, spend: true, impressions: true },
      }),
    ));
    tasks.push(() => queryCached(`${shopDomain}:mbGender:${fromKey}:${toKey}`, TTL, () =>
      db.metaBreakdown.groupBy({
        by: ["breakdownValue"],
        where: { shopDomain, breakdownType: "gender", date: { gte: from, lte: to } },
        _sum: { conversions: true, conversionValue: true, spend: true, impressions: true },
      }),
    ));

    // customers: insights per-day groupBy
    tasks.push(() => queryCached(`${shopDomain}:insightsDaily:${fromKey}:${toKey}`, TTL, () =>
      db.dailyAdRollup.groupBy({
        by: ["date"],
        where: { shopDomain, date: { gte: from, lte: to } },
        _sum: { spend: true, metaConversions: true, metaConversionValue: true },
      }),
    ));
    tasks.push(() => queryCached(`${shopDomain}:prevInsightsDaily:${prevFromKey}:${prevToKey}`, TTL, () =>
      db.dailyAdRollup.groupBy({
        by: ["date"],
        where: { shopDomain, date: { gte: prevFrom, lte: prevTo } },
        _sum: { spend: true },
      }),
    ));

    // campaigns: pre-aggregated rollup at all 3 levels (the single most expensive loader op)
    tasks.push(() => queryCached(`${shopDomain}:campAgg:${fromKey}:${toKey}`, TTL, async () => {
      const rows = await db.dailyAdRollup.findMany({ where: { shopDomain, date: { gte: from, lte: to } } });
      return aggregateRollupAllLevels(rows);
    }));
    tasks.push(() => queryCached(`${shopDomain}:campAgg:${prevFromKey}:${prevToKey}`, TTL, async () => {
      const rows = await db.dailyAdRollup.findMany({ where: { shopDomain, date: { gte: prevFrom, lte: prevTo } } });
      return aggregateRollupAllLevels(rows);
    }));

    // campaigns: daily chart (per-day spend + impressions)
    tasks.push(() => queryCached(`${shopDomain}:campDailyChart:${fromKey}:${toKey}`, TTL, () =>
      db.dailyAdRollup.groupBy({
        by: ["date"],
        where: { shopDomain, date: { gte: from, lte: to } },
        _sum: { spend: true, impressions: true },
      }),
    ));
    tasks.push(() => queryCached(`${shopDomain}:campPrevDailyChart:${prevFromKey}:${prevToKey}`, TTL, () =>
      db.dailyAdRollup.groupBy({
        by: ["date"],
        where: { shopDomain, date: { gte: prevFrom, lte: prevTo } },
        _sum: { spend: true, impressions: true },
      }),
    ));

    // campaigns: window orders span BOTH current + previous period (matches loader's windowStart/windowEnd).
    // windowStart = prevFrom (earlier of the two), windowEnd = to (later of the two).
    const windowStart = prevFrom < from ? prevFrom : from;
    const windowEnd = to > prevTo ? to : prevTo;
    const windowStartKey = prevFromKey < fromKey ? prevFromKey : fromKey;
    const windowEndKey = toKey > prevToKey ? toKey : prevToKey;
    // Sequential sub-task: orders first, then attributions using the fetched order IDs.
    // Wrapped so the outer Promise.allSettled still handles failure isolation.
    tasks.push(async () => {
      const windowOrders = await queryCached(`${shopDomain}:campWindowOrders:${windowStartKey}:${windowEndKey}`, TTL, () =>
        db.order.findMany({
          where: { shopDomain, createdAt: { gte: windowStart, lte: windowEnd } },
          select: {
            shopifyOrderId: true, createdAt: true, frozenTotalPrice: true, totalRefunded: true,
            isNewCustomerOrder: true, isOnlineStore: true, shopifyCustomerId: true, utmConfirmedMeta: true,
            metaCampaignId: true, metaCampaignName: true,
            metaAdSetId: true, metaAdSetName: true,
            metaAdId: true, metaAdName: true,
          },
        }),
      );
      const windowOrderIds = windowOrders.map(o => o.shopifyOrderId);
      await queryCached(`${shopDomain}:campAttrs:${fromKey}:${toKey}`, TTL, () =>
        db.attribution.findMany({
          where: {
            shopDomain,
            OR: [
              { shopifyOrderId: { in: windowOrderIds } },
              { confidence: 0, matchedAt: { gte: from, lte: to } },
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
    });

    // campaigns: platform + placement breakdown aggregates (8 groupBys combined into 2 cached entries)
    tasks.push(() => queryCached(`${shopDomain}:campBd:publisher_platform:${fromKey}:${toKey}`, TTL, async () => {
      return buildBreakdownAgg(shopDomain, "publisher_platform", from, to);
    }));
    tasks.push(() => queryCached(`${shopDomain}:campBd:platform_position:${fromKey}:${toKey}`, TTL, async () => {
      return buildBreakdownAgg(shopDomain, "platform_position", from, to);
    }));

    // products: rollup per-window
    tasks.push(() => queryCached(`${shopDomain}:productRollup:${fromKey}:${toKey}`, TTL, () =>
      db.dailyProductRollup.findMany({ where: { shopDomain, date: { gte: from, lte: to } } }),
    ));
    tasks.push(() => queryCached(`${shopDomain}:productRollup:${prevFromKey}:${prevToKey}`, TTL, () =>
      db.dailyProductRollup.findMany({ where: { shopDomain, date: { gte: prevFrom, lte: prevTo } } }),
    ));

    // ── Order Explorer: orders + attributions + insights for this date range ──
    tasks.push(async () => {
      const orders = await queryCached(`${shopDomain}:ordersExplorer:${fromKey}:${toKey}`, TTL, () =>
        db.order.findMany({
          where: { shopDomain, createdAt: { gte: from, lte: to } },
          orderBy: { createdAt: "desc" },
        }),
      );
      const orderIds = orders.map(o => o.shopifyOrderId);
      await queryCached(`${shopDomain}:ordersAttrs:${fromKey}:${toKey}`, TTL, () =>
        db.attribution.findMany({
          where: {
            shopDomain,
            OR: [
              { shopifyOrderId: { in: orderIds } },
              { confidence: 0, matchedAt: { gte: from, lte: to } },
            ],
          },
          orderBy: { matchedAt: "desc" },
        }),
      );
      await queryCached(`${shopDomain}:ordersInsights:${fromKey}:${toKey}`, TTL, () =>
        db.metaInsight.findMany({
          where: { shopDomain, conversions: { gt: 0 }, date: { gte: from, lte: to } },
        }),
      );
    });

    // ── Geo: breakdowns + orders + attributions for this date range ──
    tasks.push(async () => {
      const [, geoOrders] = await Promise.all([
        queryCached(`${shopDomain}:geoBreakdown:${fromKey}:${toKey}`, TTL, () =>
          db.metaBreakdown.findMany({
            where: { shopDomain, breakdownType: "country", date: { gte: from, lte: to } },
          }),
        ),
        queryCached(`${shopDomain}:geoOrders:${fromKey}:${toKey}`, TTL, () =>
          db.order.findMany({
            where: { shopDomain, isOnlineStore: true, createdAt: { gte: from, lte: to } },
          }),
        ),
      ]);
      const geoOrderIds = geoOrders.map(o => o.shopifyOrderId);
      await queryCached(`${shopDomain}:geoAttrs:${fromKey}:${toKey}`, TTL, () =>
        db.attribution.findMany({
          where: {
            shopDomain,
            OR: [
              { shopifyOrderId: { in: geoOrderIds } },
              { confidence: 0, matchedAt: { gte: from, lte: to } },
            ],
          },
        }),
      );
    });
  }

  // Run with limited concurrency (3 at a time). Fully parallel consumed all 8
  // DB connections and blocked user requests. Fully sequential took 2-3 minutes.
  // 3 concurrent = ~40s total, leaves 5 connections for user requests.
  const CONCURRENCY = 3;
  let succeeded = 0, failed = 0;
  for (let i = 0; i < tasks.length; i += CONCURRENCY) {
    const batch = tasks.slice(i, i + CONCURRENCY);
    const results = await Promise.allSettled(batch.map(fn => fn()));
    for (const r of results) {
      if (r.status === "fulfilled") succeeded++;
      else failed++;
    }
  }
  console.log(`[warmer] ${shopDomain}: warmed ${succeeded} cache entries in ${Date.now() - t0}ms${failed > 0 ? ` (${failed} failed)` : ""}`);
}

export async function warmAllShops() {
  try {
    const shops = await db.shop.findMany({ select: { shopDomain: true } });
    console.log(`[warmer] starting — ${shops.length} shops`);
    const t0 = Date.now();
    for (const { shopDomain } of shops) {
      await warmShop(shopDomain).catch(err => {
        console.error(`[warmer] ${shopDomain} failed:`, err.message);
      });
    }
    console.log(`[warmer] all shops done in ${Date.now() - t0}ms`);
  } catch (err) {
    console.error("[warmer] fatal error:", err);
  }
}
