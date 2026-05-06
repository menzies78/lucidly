import db from "../db.server.js";
import { shopLocalDayKey } from "../utils/shopTime.server";

/**
 * Rebuild DailyAdDemographicRollup for a shop.
 *
 * Why a separate table from DailyAdRollup:
 *   - DailyAdRollup is keyed by (date, adId) and is what the Campaigns page
 *     reads for the per-ad rows. Spend lives there because Meta does not
 *     attach customer-resolved demographics to spend - it's an ad-level cost.
 *   - This table is keyed by (date, adId, gender, ageBracket) and only stores
 *     order/revenue counts. The Ad Explorer reads it when the user picks a
 *     gender/age filter, leaving spend pulled from DailyAdRollup unchanged.
 *
 * Demographic resolution (matches the Product Demographics Explorer pattern):
 *   gender     = COALESCE(Attribution.metaGender, Customer.inferredGender, "unknown")
 *   ageBracket = COALESCE(Attribution.metaAge, "unknown")
 *
 * Sources:
 *   1. Matched attributions (confidence > 0) joined to Order + Customer.
 *      Order revenue/count is bucketed under the resolved demographic.
 *   2. UTM-only orders (utmConfirmedMeta=true, not matched by Layer 2).
 *      Joined to Customer for inferredGender. metaAge is unknown for these
 *      because the attribution row doesn't exist.
 *
 * Same exclusions as campaignRollups: skip £0 orders, clamp revenue at 0.
 */
export async function rebuildAdDemographicRollups(shopDomain) {
  const t0 = Date.now();

  const shopRow = await db.shop.findUnique({ where: { shopDomain }, select: { shopifyTimezone: true } });
  const tz = shopRow?.shopifyTimezone || "UTC";

  const [attributions, orders, customers] = await Promise.all([
    db.attribution.findMany({
      where: { shopDomain, confidence: { gt: 0 } },
      select: {
        shopifyOrderId: true,
        metaAdId: true,
        metaGender: true,
        metaAge: true,
      },
    }),
    db.order.findMany({
      where: { shopDomain, isOnlineStore: true },
      select: {
        shopifyOrderId: true,
        shopifyCustomerId: true,
        createdAt: true,
        frozenTotalPrice: true,
        totalRefunded: true,
        utmConfirmedMeta: true,
        customerOrderCountAtPurchase: true,
        metaAdId: true,
      },
    }),
    db.customer.findMany({
      where: { shopDomain },
      select: { shopifyCustomerId: true, inferredGender: true },
    }),
  ]);

  const orderMap = new Map();
  for (const o of orders) orderMap.set(o.shopifyOrderId, o);

  const customerMap = new Map();
  for (const c of customers) customerMap.set(c.shopifyCustomerId, c);

  // bucket key: `${dayKey}|${adId}|${gender}|${age}`
  const buckets = new Map();

  const getBucket = (rawDate, adId, gender, ageBracket) => {
    const dayKey = shopLocalDayKey(tz, rawDate);
    const key = `${dayKey}|${adId}|${gender}|${ageBracket}`;
    let b = buckets.get(key);
    if (!b) {
      b = {
        date: new Date(`${dayKey}T00:00:00.000Z`),
        adId,
        gender,
        ageBracket,
        attributedOrders: 0, attributedRevenue: 0,
        newCustomerOrders: 0, newCustomerRevenue: 0,
        existingCustomerOrders: 0, existingCustomerRevenue: 0,
      };
      buckets.set(key, b);
    }
    return b;
  };

  // 1. Matched attributions
  for (const a of attributions) {
    if (!a.metaAdId) continue;
    const order = orderMap.get(a.shopifyOrderId);
    if (!order) continue;
    const gross = order.frozenTotalPrice || 0;
    if (gross === 0) continue;
    const rev = Math.max(0, gross - (order.totalRefunded || 0));

    const cust = order.shopifyCustomerId ? customerMap.get(order.shopifyCustomerId) : null;
    const gender = a.metaGender || cust?.inferredGender || "unknown";
    const ageBracket = a.metaAge || "unknown";

    const b = getBucket(order.createdAt, a.metaAdId, gender, ageBracket);
    b.attributedOrders += 1;
    b.attributedRevenue += rev;
    if (order.customerOrderCountAtPurchase === 1) {
      b.newCustomerOrders += 1;
      b.newCustomerRevenue += rev;
    } else {
      b.existingCustomerOrders += 1;
      b.existingCustomerRevenue += rev;
    }
  }

  // 2. UTM-only orders (utmConfirmedMeta but no matched attribution)
  const matchedOrderIds = new Set(attributions.map(a => a.shopifyOrderId));
  for (const order of orders) {
    if (!order.utmConfirmedMeta) continue;
    if (matchedOrderIds.has(order.shopifyOrderId)) continue;
    if (!order.metaAdId) continue;
    const gross = order.frozenTotalPrice || 0;
    if (gross === 0) continue;
    const rev = Math.max(0, gross - (order.totalRefunded || 0));

    const cust = order.shopifyCustomerId ? customerMap.get(order.shopifyCustomerId) : null;
    const gender = cust?.inferredGender || "unknown";
    const ageBracket = "unknown"; // No Attribution row → no Meta age signal.

    const b = getBucket(order.createdAt, order.metaAdId, gender, ageBracket);
    b.attributedOrders += 1;
    b.attributedRevenue += rev;
    if (order.customerOrderCountAtPurchase === 1) {
      b.newCustomerOrders += 1;
      b.newCustomerRevenue += rev;
    } else {
      b.existingCustomerOrders += 1;
      b.existingCustomerRevenue += rev;
    }
  }

  // 3. Delete + bulk insert
  await db.dailyAdDemographicRollup.deleteMany({ where: { shopDomain } });

  const rows = Array.from(buckets.values()).map(b => ({
    shopDomain,
    date: b.date,
    adId: b.adId,
    gender: b.gender,
    ageBracket: b.ageBracket,
    attributedOrders: b.attributedOrders,
    attributedRevenue: b.attributedRevenue,
    newCustomerOrders: b.newCustomerOrders,
    newCustomerRevenue: b.newCustomerRevenue,
    existingCustomerOrders: b.existingCustomerOrders,
    existingCustomerRevenue: b.existingCustomerRevenue,
  }));

  const CHUNK = 500;
  for (let i = 0; i < rows.length; i += CHUNK) {
    await db.dailyAdDemographicRollup.createMany({ data: rows.slice(i, i + CHUNK) });
  }

  console.log(`[adDemographicRollups] ${shopDomain} rebuilt ${rows.length} rows in ${Date.now() - t0}ms (attrs=${attributions.length})`);
  return { rows: rows.length, ms: Date.now() - t0 };
}
