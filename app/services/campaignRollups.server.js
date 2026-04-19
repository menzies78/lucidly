import db from "../db.server.js";
import { shopLocalDayKey } from "../utils/shopTime.server";

/**
 * Rebuild DailyAdRollup rows for a shop.
 *
 * Strategy:
 *  1. Load all MetaInsight rows for the shop (scoped to the relevant date
 *     window — currently full-history; Meta-side churn is low).
 *  2. Sum hour-slot rows into per-(date,adId) buckets with full entity names.
 *  3. Load all matched attributions + their orders within the same window.
 *     For each confident attribution, add the order revenue/count to the
 *     rollup row keyed by (orderDate, attr.metaAdId).
 *  4. Load all placeholder (confidence=0) attributions and apply their
 *     metaConversionValue to the unverifiedRevenue bucket keyed by
 *     (placeholderDate, attr.metaAdId).
 *  5. Delete existing rollup rows for shop and bulk insert new ones.
 *
 * The Campaigns loader then reads rollup rows for the requested window and
 * aggregates in JS (O(rows-in-window), no raw-table scan).
 */
export async function rebuildCampaignRollups(shopDomain) {
  const t0 = Date.now();

  const shopRow = await db.shop.findUnique({ where: { shopDomain }, select: { shopifyTimezone: true } });
  const tz = shopRow?.shopifyTimezone || "UTC";

  const [insights, attributions, orders] = await Promise.all([
    db.metaInsight.findMany({
      where: { shopDomain },
      select: {
        date: true, adId: true,
        campaignId: true, campaignName: true,
        adSetId: true, adSetName: true, adName: true,
        spend: true, impressions: true, clicks: true, reach: true,
        frequency: true,
        linkClicks: true, landingPageViews: true, viewContent: true,
        addToCart: true, initiateCheckout: true,
        conversions: true, conversionValue: true,
        videoP25: true, videoP50: true, videoP75: true, videoP100: true,
      },
    }),
    db.attribution.findMany({
      where: { shopDomain },
      select: {
        shopifyOrderId: true, confidence: true, isNewCustomer: true,
        metaAdId: true, metaConversionValue: true,
      },
    }),
    db.order.findMany({
      where: { shopDomain },
      select: {
        shopifyOrderId: true, createdAt: true, frozenTotalPrice: true,
        utmConfirmedMeta: true, isNewCustomerOrder: true, customerOrderCountAtPurchase: true,
        metaAdId: true, metaAdName: true,
        metaAdSetId: true, metaAdSetName: true,
        metaCampaignId: true, metaCampaignName: true,
      },
    }),
  ]);

  const orderMap = new Map();
  for (const o of orders) orderMap.set(o.shopifyOrderId, o);

  // bucket key: `${shopLocalDayKey}|${adId}` — every day bucket is a shop-local
  // calendar day. The stored .date is UTC-midnight of that calendar day
  // (canonical handle, matches MetaInsight.date convention).
  const buckets = new Map();

  const getBucket = (rawDate, adId, seed) => {
    const dayKey = shopLocalDayKey(tz, rawDate);
    const key = `${dayKey}|${adId}`;
    let b = buckets.get(key);
    if (!b) {
      b = {
        date: new Date(`${dayKey}T00:00:00.000Z`),
        adId,
        campaignId: seed?.campaignId || "",
        campaignName: seed?.campaignName || "",
        adSetId: seed?.adSetId || "",
        adSetName: seed?.adSetName || "",
        adName: seed?.adName || "",
        spend: 0, impressions: 0, clicks: 0, reach: 0,
        frequencySum: 0, frequencyCount: 0,
        linkClicks: 0, landingPageViews: 0, viewContent: 0,
        addToCart: 0, initiateCheckout: 0,
        metaConversions: 0, metaConversionValue: 0,
        videoP25: 0, videoP50: 0, videoP75: 0, videoP100: 0,
        attributedOrders: 0, attributedRevenue: 0,
        newCustomerOrders: 0, newCustomerRevenue: 0,
        existingCustomerOrders: 0, existingCustomerRevenue: 0,
        unverifiedRevenue: 0,
        utmOnlyOrders: 0,
        utmOnlyRevenue: 0,
      };
      buckets.set(key, b);
    }
    // Keep the most recently seen name/ids (insights are not guaranteed sorted)
    if (seed?.campaignId) b.campaignId = seed.campaignId;
    if (seed?.campaignName) b.campaignName = seed.campaignName;
    if (seed?.adSetId) b.adSetId = seed.adSetId;
    if (seed?.adSetName) b.adSetName = seed.adSetName;
    if (seed?.adName) b.adName = seed.adName;
    return b;
  };

  // 1. Insights → sum per (date, adId)
  for (const i of insights) {
    if (!i.adId) continue;
    const b = getBucket(i.date, i.adId, i);
    b.spend += i.spend || 0;
    b.impressions += i.impressions || 0;
    b.clicks += i.clicks || 0;
    b.reach += i.reach || 0;
    if (i.frequency) {
      b.frequencySum += i.frequency;
      b.frequencyCount += 1;
    }
    b.linkClicks += i.linkClicks || 0;
    b.landingPageViews += i.landingPageViews || 0;
    b.viewContent += i.viewContent || 0;
    b.addToCart += i.addToCart || 0;
    b.initiateCheckout += i.initiateCheckout || 0;
    b.metaConversions += i.conversions || 0;
    b.metaConversionValue += i.conversionValue || 0;
    b.videoP25 += i.videoP25 || 0;
    b.videoP50 += i.videoP50 || 0;
    b.videoP75 += i.videoP75 || 0;
    b.videoP100 += i.videoP100 || 0;
  }

  // 2. Matched attributions → join order, add to rollup at order date
  for (const a of attributions) {
    if (!a.metaAdId) continue;
    if (a.confidence > 0) {
      const order = orderMap.get(a.shopifyOrderId);
      if (!order) continue;
      const rev = order.frozenTotalPrice || 0;
      // Skip £0 orders (staff / replacement / warranty) from attributed
      // metrics so they don't inflate order counts and drag down AOV/CPA.
      if (rev === 0) continue;
      const b = getBucket(order.createdAt, a.metaAdId, null);
      b.attributedOrders += 1;
      b.attributedRevenue += rev;
      // Use Shopify ground truth for new customer check, not the attribution flag
      if (order.customerOrderCountAtPurchase === 1) {
        b.newCustomerOrders += 1;
        b.newCustomerRevenue += rev;
      } else {
        b.existingCustomerOrders += 1;
        b.existingCustomerRevenue += rev;
      }
    } else {
      // Placeholder attributions encode the date in shopifyOrderId like
      // "unmatched-{YYYY-MM-DD}-..." — extract and bucket unverified revenue.
      const m = a.shopifyOrderId.match(/(\d{4}-\d{2}-\d{2})/);
      if (!m) continue;
      const date = new Date(`${m[1]}T00:00:00.000Z`);
      const b = getBucket(date, a.metaAdId, null);
      b.unverifiedRevenue += a.metaConversionValue || 0;
    }
  }

  // 2b. UTM-only orders: utmConfirmedMeta=true but not in matchedOrderIds.
  // Counted as attributed revenue under their linked ad, mirroring the
  // behaviour of aggregateInsights in app.campaigns.tsx.
  const matchedOrderIds = new Set(
    attributions.filter(a => a.confidence > 0).map(a => a.shopifyOrderId)
  );
  for (const order of orders) {
    if (!order.utmConfirmedMeta) continue;
    if (matchedOrderIds.has(order.shopifyOrderId)) continue;
    if (!order.metaAdId) continue;
    const rev = order.frozenTotalPrice || 0;
    if (rev === 0) continue; // Same £0 exclusion as matched attributions above.
    const b = getBucket(order.createdAt, order.metaAdId, {
      campaignId: order.metaCampaignId,
      campaignName: order.metaCampaignName,
      adSetId: order.metaAdSetId,
      adSetName: order.metaAdSetName,
      adName: order.metaAdName,
    });
    b.attributedOrders += 1;
    b.attributedRevenue += rev;
    b.utmOnlyOrders += 1;
    b.utmOnlyRevenue += rev;
    if (order.customerOrderCountAtPurchase === 1) {
      b.newCustomerOrders += 1;
      b.newCustomerRevenue += rev;
    } else {
      b.existingCustomerOrders += 1;
      b.existingCustomerRevenue += rev;
    }
  }

  // 3. Delete + bulk insert
  await db.dailyAdRollup.deleteMany({ where: { shopDomain } });

  const rows = Array.from(buckets.values()).map(b => ({
    shopDomain,
    date: b.date,
    adId: b.adId,
    campaignId: b.campaignId,
    campaignName: b.campaignName,
    adSetId: b.adSetId,
    adSetName: b.adSetName,
    adName: b.adName,
    spend: b.spend,
    impressions: b.impressions,
    clicks: b.clicks,
    reach: b.reach,
    frequencySum: b.frequencySum,
    frequencyCount: b.frequencyCount,
    linkClicks: b.linkClicks,
    landingPageViews: b.landingPageViews,
    viewContent: b.viewContent,
    addToCart: b.addToCart,
    initiateCheckout: b.initiateCheckout,
    metaConversions: b.metaConversions,
    metaConversionValue: b.metaConversionValue,
    videoP25: b.videoP25,
    videoP50: b.videoP50,
    videoP75: b.videoP75,
    videoP100: b.videoP100,
    attributedOrders: b.attributedOrders,
    attributedRevenue: b.attributedRevenue,
    newCustomerOrders: b.newCustomerOrders,
    newCustomerRevenue: b.newCustomerRevenue,
    existingCustomerOrders: b.existingCustomerOrders,
    existingCustomerRevenue: b.existingCustomerRevenue,
    unverifiedRevenue: b.unverifiedRevenue,
    utmOnlyOrders: b.utmOnlyOrders,
    utmOnlyRevenue: b.utmOnlyRevenue,
  }));

  const CHUNK = 500;
  for (let i = 0; i < rows.length; i += CHUNK) {
    await db.dailyAdRollup.createMany({ data: rows.slice(i, i + CHUNK) });
  }

  console.log(`[campaignRollups] ${shopDomain} rebuilt ${rows.length} rows in ${Date.now() - t0}ms (insights=${insights.length}, attrs=${attributions.length})`);
  return { rows: rows.length, ms: Date.now() - t0 };
}
