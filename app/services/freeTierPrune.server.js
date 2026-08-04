// Free-tier data lifecycle: bounds storage for audit-tier shops.
//
// The loaders' clampRangeForPlan is the PAYWALL; this prune is only the
// COST CONTROL. It deletes raw rows older than the 90-day window (+10-day
// buffer) for shops that are on the free plan past their downgrade grace.
//
// Hard rules:
//  - Customer table is NEVER pruned: it's the shop's permanent memory
//    (firstOrderDate, lifetime counts, metaSegment, segmentSource) — the
//    new-vs-existing ground truth survives the rolling window, kilobytes
//    per shop.
//  - Demo shops are NEVER pruned: their dataset is a static seed; pruning
//    would eat the demo.
//  - 30-day downgrade grace: a shop that downgraded keeps full history
//    for 30 days (instant, backfill-free re-upgrade window). Grace is
//    measured from planChangedAt.
//  - Trial/paid shops are never touched.
//  - Attribution placeholders encode dates in shopifyOrderId strings, so
//    attribution rows are pruned via their matched order OR their
//    placeholder date prefix.

import db from "../db.server.js";

const WINDOW_DAYS = 100; // 90-day product window + 10-day buffer
const GRACE_DAYS = 30;

export async function pruneFreeShops() {
  const graceCutoff = new Date(Date.now() - GRACE_DAYS * 86400000);
  const shops = await db.shop.findMany({
    where: {
      plan: "free",
      demoMode: false,
      // Never prune inside the downgrade grace window. planChangedAt is
      // null only for pre-plan-era rows (all grandfathered paid, so they
      // never reach this query) — treat null as "in grace" defensively.
      planChangedAt: { lt: graceCutoff },
    },
    select: { shopDomain: true },
  });
  if (shops.length === 0) return { shops: 0 };

  const cutoff = new Date(Date.now() - WINDOW_DAYS * 86400000);
  const cutoffKey = cutoff.toISOString().slice(0, 10);
  let totals = { orders: 0, lineItems: 0, insights: 0, breakdowns: 0, attributions: 0 };

  for (const { shopDomain } of shops) {
    // Order line items go first (no FK, but keep referential tidiness).
    const oldOrderIds = await db.order.findMany({
      where: { shopDomain, createdAt: { lt: cutoff } },
      select: { shopifyOrderId: true },
    });
    if (oldOrderIds.length > 0) {
      const ids = oldOrderIds.map((o) => o.shopifyOrderId);
      const CHUNK = 800; // SQLite parameter limit headroom
      for (let i = 0; i < ids.length; i += CHUNK) {
        const slice = ids.slice(i, i + CHUNK);
        totals.lineItems += (await db.orderLineItem.deleteMany({
          where: { shopDomain, shopifyOrderId: { in: slice } },
        })).count;
        totals.attributions += (await db.attribution.deleteMany({
          where: { shopDomain, shopifyOrderId: { in: slice } },
        })).count;
      }
      totals.orders += (await db.order.deleteMany({
        where: { shopDomain, createdAt: { lt: cutoff } },
      })).count;
    }
    // Placeholder attributions: "unmatched_{adId}_{YYYY-MM-DD}_..." — date
    // is lexicographically comparable, so prune via raw LIKE + substring.
    totals.attributions += (await db.$executeRawUnsafe(
      `DELETE FROM Attribution WHERE shopDomain = ? AND confidence = 0
       AND shopifyOrderId LIKE 'unmatched%'
       AND substr(shopifyOrderId, instr(shopifyOrderId, '_2') + 1, 10) < ?`,
      shopDomain, cutoffKey,
    ));
    totals.insights += (await db.metaInsight.deleteMany({
      where: { shopDomain, date: { lt: cutoff } },
    })).count;
    totals.breakdowns += (await db.metaBreakdown.deleteMany({
      where: { shopDomain, date: { lt: cutoff } },
    })).count;
    // Rollups: cheap to rebuild, but pruning them directly avoids serving
    // out-of-window rows the clamp would hide anyway.
    await db.dailyAdRollup.deleteMany({ where: { shopDomain, date: { lt: cutoff } } });
    await db.dailyGeoRollup.deleteMany({ where: { shopDomain, date: { lt: cutoff } } });
    await db.dailyProductRollup.deleteMany({ where: { shopDomain, date: { lt: cutoff } } });
    console.log(`[FreeTierPrune] ${shopDomain}: pruned to ${WINDOW_DAYS}d window`);
  }
  console.log(`[FreeTierPrune] ${shops.length} shop(s): ${JSON.stringify(totals)}`);
  return { shops: shops.length, ...totals };
}
