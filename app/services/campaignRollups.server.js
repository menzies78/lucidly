import db from "../db.server.js";
import { shopLocalDayKey } from "../utils/shopTime.server";

/**
 * Rebuild DailyAdRollup rows for a shop.
 *
 * Strategy:
 *  1. Aggregate MetaInsight rows SQL-side (GROUP BY date, adId). The raw
 *     table is hourly and grows unbounded (620k+ rows for a mature shop);
 *     loading it into JS objects previously cost ~1GB of heap and OOM-killed
 *     the process. The grouped result is ~24x smaller (one row per ad-day).
 *  2. Merge the grouped rows into per-(shop-local-day, adId) buckets with
 *     full entity names.
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

  // SQLite stores DateTime as epoch-ms INTEGER; raw-query integers come back
  // as BigInt, so every Int-column aggregate below goes through num().
  const num = (v) => (typeof v === "bigint" ? Number(v) : v || 0);

  const [insightGroups, attributions, orders] = await Promise.all([
    db.$queryRaw`
      SELECT
        date, adId,
        MAX(COALESCE(campaignId, ''))   AS campaignId,
        MAX(COALESCE(campaignName, '')) AS campaignName,
        MAX(COALESCE(adSetId, ''))      AS adSetId,
        MAX(COALESCE(adSetName, ''))    AS adSetName,
        MAX(COALESCE(adName, ''))       AS adName,
        SUM(spend)            AS spend,
        SUM(impressions)      AS impressions,
        SUM(clicks)           AS clicks,
        SUM(reach)            AS reach,
        SUM(frequency)        AS frequencySum,
        SUM(CASE WHEN frequency IS NOT NULL AND frequency != 0 THEN 1 ELSE 0 END) AS frequencyCount,
        SUM(linkClicks)       AS linkClicks,
        SUM(landingPageViews) AS landingPageViews,
        SUM(viewContent)      AS viewContent,
        SUM(addToCart)        AS addToCart,
        SUM(initiateCheckout) AS initiateCheckout,
        SUM(conversions)      AS conversions,
        SUM(conversionValue)  AS conversionValue,
        SUM(videoP25)         AS videoP25,
        SUM(videoP50)         AS videoP50,
        SUM(videoP75)         AS videoP75,
        SUM(videoP100)        AS videoP100
      FROM MetaInsight
      WHERE shopDomain = ${shopDomain} AND adId IS NOT NULL AND adId != ''
      GROUP BY date, adId
    `,
    db.attribution.findMany({
      where: { shopDomain },
      select: {
        shopifyOrderId: true, confidence: true, isNewCustomer: true,
        metaAdId: true, metaConversionValue: true,
      },
    }),
    db.order.findMany({
      where: { shopDomain, isOnlineStore: true },
      select: {
        shopifyOrderId: true, createdAt: true, frozenTotalPrice: true,
        totalRefunded: true,
        utmConfirmedMeta: true, isNewCustomerOrder: true, customerOrderCountAtPurchase: true,
        metaAdId: true, metaAdName: true,
        metaAdSetId: true, metaAdSetName: true,
        metaCampaignId: true, metaCampaignName: true,
      },
    }),
  ]);

  const orderMap = new Map();
  for (const o of orders) orderMap.set(o.shopifyOrderId, o);

  // bucket key: `${shopLocalDayKey}|${adId}` - every day bucket is a shop-local
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
        unverifiedOrders: 0,
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

  // 1. Pre-grouped insights → merge per (shop-local day, adId). Buckets can
  // still merge across raw dates when the tz projection folds two UTC dates
  // into one local day, so this stays a += merge rather than an assignment.
  for (const g of insightGroups) {
    const b = getBucket(new Date(num(g.date)), g.adId, g);
    b.spend += g.spend || 0;
    b.impressions += num(g.impressions);
    b.clicks += num(g.clicks);
    b.reach += num(g.reach);
    b.frequencySum += g.frequencySum || 0;
    b.frequencyCount += num(g.frequencyCount);
    b.linkClicks += num(g.linkClicks);
    b.landingPageViews += num(g.landingPageViews);
    b.viewContent += num(g.viewContent);
    b.addToCart += num(g.addToCart);
    b.initiateCheckout += num(g.initiateCheckout);
    b.metaConversions += num(g.conversions);
    b.metaConversionValue += g.conversionValue || 0;
    b.videoP25 += num(g.videoP25);
    b.videoP50 += num(g.videoP50);
    b.videoP75 += num(g.videoP75);
    b.videoP100 += num(g.videoP100);
  }

  // 2. Matched attributions → join order, add to rollup at order date
  for (const a of attributions) {
    if (!a.metaAdId) continue;
    if (a.confidence > 0) {
      const order = orderMap.get(a.shopifyOrderId);
      if (!order) continue;
      const gross = order.frozenTotalPrice || 0;
      // Skip £0 orders (staff / replacement / warranty) from attributed
      // metrics so they don't inflate order counts and drag down AOV/CPA.
      if (gross === 0) continue;
      // Revenue is net of refunds. Clamp at 0 to defend against
      // over-refunded rows (rare; would otherwise go negative).
      const rev = Math.max(0, gross - (order.totalRefunded || 0));
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
      // "unmatched-{YYYY-MM-DD}-..." - extract and bucket unverified revenue.
      const m = a.shopifyOrderId.match(/(\d{4}-\d{2}-\d{2})/);
      if (!m) continue;
      const date = new Date(`${m[1]}T00:00:00.000Z`);
      const b = getBucket(date, a.metaAdId, null);
      b.unverifiedRevenue += a.metaConversionValue || 0;
      b.unverifiedOrders += 1;
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
    const gross = order.frozenTotalPrice || 0;
    if (gross === 0) continue; // Same £0 exclusion as matched attributions above.
    // Revenue net of refunds; clamp for over-refunded edge case.
    const rev = Math.max(0, gross - (order.totalRefunded || 0));
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

  // 3. Atomic delete + bulk insert. Wrapping in a transaction prevents
  // concurrent readers (loaders, cache warmer) from seeing a partially-empty
  // table mid-rebuild and caching bad zero-value data for the TTL window.
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
    unverifiedOrders: b.unverifiedOrders,
    utmOnlyOrders: b.utmOnlyOrders,
    utmOnlyRevenue: b.utmOnlyRevenue,
  }));

  // 10 min timeout — same defensive budget as geoRollups. Current Vollebak
  // (32k+ rows) completes well under 60s but we removed the lower bound to
  // future-proof against larger shops where the transaction would silently
  // roll back to 0 rows.
  const CHUNK = 500;
  await db.$transaction(async (tx) => {
    await tx.dailyAdRollup.deleteMany({ where: { shopDomain } });
    for (let i = 0; i < rows.length; i += CHUNK) {
      await tx.dailyAdRollup.createMany({ data: rows.slice(i, i + CHUNK) });
    }
  }, { timeout: 600000 });

  console.log(`[campaignRollups] ${shopDomain} rebuilt ${rows.length} rows in ${Date.now() - t0}ms (insightGroups=${insightGroups.length}, attrs=${attributions.length})`);
  return { rows: rows.length, ms: Date.now() - t0 };
}
