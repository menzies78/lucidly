/**
 * All-time LTV snapshot.
 *
 * Walks the full online-order history + customer table once and builds
 * per-entity LTV maps (at campaign / adset / ad level) plus the customerAcq
 * map used to count unique new Meta customers per period.
 *
 * Cached per shop via queryCache; invalidated at the end of each incremental
 * sync. Without this, every campaigns/dashboard load was re-walking 10k+ orders.
 *
 * Extracted to a shared service so the cache warmer can pre-populate it on
 * boot AND the campaigns route loader can read it.
 */

import db from "../db.server.js";

const DAY_MS = 86400000;
const r2g = (v) => Math.round(v * 100) / 100;

export async function loadLtvSnapshot(shopDomain) {
  const [allOrdersRaw, customers, attributions] = await Promise.all([
    db.order.findMany({
      where: { shopDomain, isOnlineStore: true },
      select: {
        shopifyOrderId: true, shopifyCustomerId: true, createdAt: true,
        frozenTotalPrice: true, utmConfirmedMeta: true,
        metaCampaignId: true, metaAdSetId: true, metaAdId: true,
        customerOrderCountAtPurchase: true,
      },
    }),
    db.customer.findMany({
      where: { shopDomain },
      select: { shopifyCustomerId: true, firstOrderDate: true, metaSegment: true },
    }),
    db.attribution.findMany({
      where: { shopDomain, confidence: { gt: 0 } },
      select: {
        shopifyOrderId: true,
        metaCampaignId: true, metaAdSetId: true, metaAdId: true,
      },
    }),
  ]);

  const attrByOrderId = {};
  for (const a of attributions) attrByOrderId[a.shopifyOrderId] = a;

  const custOnlineOrders = {};
  for (const o of allOrdersRaw) {
    if (!o.shopifyCustomerId) continue;
    (custOnlineOrders[o.shopifyCustomerId] ||= []).push(o);
  }

  const customerAcq = {};
  for (const c of customers) {
    if (c.metaSegment !== "metaNew" || !c.firstOrderDate) continue;
    const custOrds = custOnlineOrders[c.shopifyCustomerId];
    if (!custOrds || custOrds.length === 0) continue;
    const firstDateStr = c.firstOrderDate.toISOString().split("T")[0];
    const firstOrder = custOrds.find(o => o.createdAt.toISOString().split("T")[0] === firstDateStr);
    if (!firstOrder) continue;
    // Safety: double-check Shopify ground truth in case metaSegment is stale
    if (firstOrder.customerOrderCountAtPurchase !== 1) continue;
    const attr = attrByOrderId[firstOrder.shopifyOrderId];
    if (attr) {
      customerAcq[c.shopifyCustomerId] = {
        campaignId: attr.metaCampaignId, adSetId: attr.metaAdSetId, adId: attr.metaAdId,
        acquisitionDate: firstOrder.createdAt,
      };
    } else if (firstOrder.utmConfirmedMeta) {
      customerAcq[c.shopifyCustomerId] = {
        campaignId: firstOrder.metaCampaignId, adSetId: firstOrder.metaAdSetId, adId: firstOrder.metaAdId,
        acquisitionDate: firstOrder.createdAt,
      };
    }
  }

  const acquiredCustIds = new Set(Object.keys(customerAcq));
  const customerOrders = {};
  for (const o of allOrdersRaw) {
    if (o.shopifyCustomerId && acquiredCustIds.has(o.shopifyCustomerId)) {
      (customerOrders[o.shopifyCustomerId] ||= []).push(o);
    }
  }

  const computeLtvMap = (levelKey) => {
    const entityData = {};
    for (const [custId, acq] of Object.entries(customerAcq)) {
      const entityId = acq[levelKey];
      if (!entityId) continue;
      (entityData[entityId] ||= []);
      const orders = customerOrders[custId] || [];
      const acqTime = acq.acquisitionDate.getTime();
      let rev30 = 0, rev90 = 0, rev365 = 0, revAll = 0, orderCount = 0;
      for (const o of orders) {
        const rev = o.frozenTotalPrice || 0;
        const daysSinceAcq = (o.createdAt.getTime() - acqTime) / DAY_MS;
        revAll += rev;
        orderCount++;
        if (daysSinceAcq <= 30) rev30 += rev;
        if (daysSinceAcq <= 90) rev90 += rev;
        if (daysSinceAcq <= 365) rev365 += rev;
      }
      entityData[entityId].push({ rev30, rev90, rev365, revAll, orderCount });
    }
    const result = {};
    for (const [entityId, custs] of Object.entries(entityData)) {
      const n = custs.length;
      if (n === 0) continue;
      result[entityId] = {
        ltvAcquiredCustomers: n,
        avgLtv30: r2g(custs.reduce((s, c) => s + c.rev30, 0) / n),
        avgLtv90: r2g(custs.reduce((s, c) => s + c.rev90, 0) / n),
        avgLtv365: r2g(custs.reduce((s, c) => s + c.rev365, 0) / n),
        avgLtvAll: r2g(custs.reduce((s, c) => s + c.revAll, 0) / n),
        totalLtvAll: r2g(custs.reduce((s, c) => s + c.revAll, 0)),
        repeatRate: Math.round((custs.filter(c => c.orderCount > 1).length / n) * 100),
        avgOrders: r2g(custs.reduce((s, c) => s + c.orderCount, 0) / n),
      };
    }
    return result;
  };

  return {
    customerAcq,
    ltvMaps: {
      campaign: computeLtvMap("campaignId"),
      adset: computeLtvMap("adSetId"),
      ad: computeLtvMap("adId"),
    },
  };
}
