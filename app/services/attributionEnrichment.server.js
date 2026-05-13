import db from "../db.server";

/**
 * Enriches Attribution records with demographic data from MetaBreakdown.
 *
 * For each matched attribution (confidence > 0), looks up MetaBreakdown
 * for the same adId + date across age, gender, publisher_platform, and
 * platform_position breakdown types.
 *
 * Allocation rules:
 * - 1 bucket for a breakdown type → exact assignment (demographicExact = true)
 * - Multiple buckets → assign the value with highest conversion count (probabilistic)
 * - If matched orders exceed conversions in top bucket → still assign top bucket
 *   but mark as probabilistic (demographicExact = false)
 *
 * Can run in two modes:
 * - enrichAll(shopDomain): enriches ALL attributions missing demographics (for full re-match)
 * - enrichForDate(shopDomain, date): enriches attributions for a specific date (for incremental)
 */

const BREAKDOWN_TYPES = ["age", "gender", "publisher_platform", "platform_position"];
const FIELD_MAP = {
  age: "metaAge",
  gender: "metaGender",
  publisher_platform: "metaPlatform",
  platform_position: "metaPlacement",
};

async function getOrderInfo(shopDomain, orderIds) {
  const orders = await db.order.findMany({
    where: { shopDomain, shopifyOrderId: { in: orderIds } },
    select: { shopifyOrderId: true, createdAt: true, frozenTotalPrice: true },
  });
  const map = {};
  for (const o of orders) {
    map[o.shopifyOrderId] = {
      date: o.createdAt.toISOString().split("T")[0],
      value: o.frozenTotalPrice || 0,
    };
  }
  return map;
}

// Tight match: bucket avg value within 2% of order value.
const VALUE_MATCH_TOLERANCE = 0.02;

async function enrichAttributions(attributions, shopDomain) {
  if (attributions.length === 0) return { enriched: 0, exact: 0, probabilistic: 0 };

  const orderIds = attributions.map(a => a.shopifyOrderId);
  const orderInfo = await getOrderInfo(shopDomain, orderIds);

  // Group attributions by adId + date. Carry the order value so we can do
  // per-order assignment when multiple breakdown buckets exist.
  const groups = {};
  for (const attr of attributions) {
    const info = orderInfo[attr.shopifyOrderId];
    if (!info || !info.date || !attr.metaAdId) continue;
    const key = `${attr.metaAdId}|${info.date}`;
    if (!groups[key]) groups[key] = { adId: attr.metaAdId, date: info.date, attrs: [] };
    groups[key].attrs.push({ id: attr.id, shopifyOrderId: attr.shopifyOrderId, value: info.value });
  }

  const allAdIds = [...new Set(attributions.map(a => a.metaAdId).filter(Boolean))];
  const allDates = [...new Set(Object.values(orderInfo).map(i => i.date).filter(Boolean))];
  if (allAdIds.length === 0 || allDates.length === 0) return { enriched: 0, exact: 0, probabilistic: 0 };

  const breakdowns = await db.metaBreakdown.findMany({
    where: {
      shopDomain,
      adId: { in: allAdIds },
      date: { in: allDates.map(d => new Date(d + "T00:00:00.000Z")) },
      breakdownType: { in: BREAKDOWN_TYPES },
      conversions: { gt: 0 },
    },
    select: { adId: true, date: true, breakdownType: true, breakdownValue: true, conversions: true, conversionValue: true },
  });

  // Index: adId|date|type → [{ value, conversions, conversionValue, avgValue }]
  const bdIndex = {};
  for (const bd of breakdowns) {
    const dateStr = bd.date.toISOString().split("T")[0];
    const key = `${bd.adId}|${dateStr}|${bd.breakdownType}`;
    if (!bdIndex[key]) bdIndex[key] = [];
    const conv = bd.conversions || 0;
    const totalVal = bd.conversionValue || 0;
    bdIndex[key].push({
      value: bd.breakdownValue,
      conversions: conv,
      conversionValue: totalVal,
      avgValue: conv > 0 ? totalVal / conv : 0,
    });
  }

  let enriched = 0, exact = 0, probabilistic = 0;
  const updates = [];

  for (const group of Object.values(groups)) {
    const { adId, date, attrs } = group;

    // Per-attribution field map: attrId → { fields: {fieldName: value}, exact: bool }
    const perAttr = new Map();
    for (const a of attrs) perAttr.set(a.id, { fields: {}, exact: true });

    for (const type of BREAKDOWN_TYPES) {
      const buckets = bdIndex[`${adId}|${date}|${type}`];
      if (!buckets || buckets.length === 0) continue;
      const field = FIELD_MAP[type];

      if (buckets.length === 1) {
        // Single bucket: all orders share this value, unambiguously.
        for (const a of attrs) perAttr.get(a.id).fields[field] = buckets[0].value;
        continue;
      }

      // Multiple buckets. Try value-based per-order assignment with capacity.
      // Sort orders desc by value, each picks the bucket with the closest avgValue
      // that still has remaining conversion capacity. This faithfully distributes
      // orders across buckets when bucket totals align with the matched order set.
      const totalCapacity = buckets.reduce((s, b) => s + b.conversions, 0);
      const allHaveValue = attrs.every(a => a.value > 0);

      if (totalCapacity >= attrs.length && allHaveValue) {
        const capacity = buckets.map(b => ({ ...b, remaining: b.conversions }));
        const ordered = [...attrs].sort((x, y) => y.value - x.value);
        for (const a of ordered) {
          let best = null;
          let bestDev = Infinity;
          for (const b of capacity) {
            if (b.remaining <= 0) continue;
            const dev = Math.abs(b.avgValue - a.value) / Math.max(a.value, 0.01);
            if (dev < bestDev) { bestDev = dev; best = b; }
          }
          if (!best) {
            // No capacity left — should not happen given the guard above.
            perAttr.get(a.id).exact = false;
            continue;
          }
          perAttr.get(a.id).fields[field] = best.value;
          best.remaining -= 1;
          if (bestDev > VALUE_MATCH_TOLERANCE) perAttr.get(a.id).exact = false;
        }
      } else {
        // Fallback: not enough capacity, or some orders have no value. Assign
        // top-conversions bucket to all attrs and mark probabilistic.
        const sorted = [...buckets].sort((a, b) => b.conversions - a.conversions);
        for (const a of attrs) {
          perAttr.get(a.id).fields[field] = sorted[0].value;
          perAttr.get(a.id).exact = false;
        }
      }
    }

    for (const a of attrs) {
      const ass = perAttr.get(a.id);
      if (Object.keys(ass.fields).length === 0) continue;
      updates.push({ id: a.id, data: { ...ass.fields, demographicExact: ass.exact } });
      enriched++;
      if (ass.exact) exact++;
      else probabilistic++;
    }
  }

  for (let i = 0; i < updates.length; i += 100) {
    const chunk = updates.slice(i, i + 100);
    await Promise.all(
      chunk.map(u => db.attribution.update({ where: { id: u.id }, data: u.data }))
    );
  }

  return { enriched, exact, probabilistic };
}

/**
 * Enrich all attributions that don't have demographics yet.
 * Used after a full re-match.
 */
export async function enrichAll(shopDomain) {
  console.log(`[AttrEnrichment] Enriching all attributions for ${shopDomain}`);

  const attrs = await db.attribution.findMany({
    where: {
      shopDomain,
      confidence: { gt: 0 },
      metaAdId: { not: null },
      metaAge: null, // only unenriched
    },
    select: { id: true, shopifyOrderId: true, metaAdId: true },
  });

  console.log(`[AttrEnrichment] Found ${attrs.length} unenriched attributions`);
  const result = await enrichAttributions(attrs, shopDomain);
  console.log(`[AttrEnrichment] Complete: ${result.enriched} enriched (${result.exact} exact, ${result.probabilistic} probabilistic)`);
  return result;
}

/**
 * Enrich attributions for a specific date.
 * Used after incremental sync (today's matches + today's breakdowns).
 */
export async function enrichForDate(shopDomain, date) {
  // Find attributions matched to orders on this date
  const dayStart = new Date(date + "T00:00:00.000Z");
  const dayEnd = new Date(date + "T23:59:59.999Z");

  const orders = await db.order.findMany({
    where: { shopDomain, createdAt: { gte: dayStart, lte: dayEnd } },
    select: { shopifyOrderId: true },
  });
  const orderIds = orders.map(o => o.shopifyOrderId);
  if (orderIds.length === 0) return { enriched: 0, exact: 0, probabilistic: 0 };

  const attrs = await db.attribution.findMany({
    where: {
      shopDomain,
      confidence: { gt: 0 },
      metaAdId: { not: null },
      shopifyOrderId: { in: orderIds },
    },
    select: { id: true, shopifyOrderId: true, metaAdId: true },
  });

  if (attrs.length === 0) return { enriched: 0, exact: 0, probabilistic: 0 };

  console.log(`[AttrEnrichment] Enriching ${attrs.length} attributions for ${date}`);
  const result = await enrichAttributions(attrs, shopDomain);
  console.log(`[AttrEnrichment] ${date}: ${result.enriched} enriched (${result.exact} exact, ${result.probabilistic} probabilistic)`);
  return result;
}

/**
 * Enrich any attribution in the recent window that is still missing demographics.
 * Self-heals when Meta breakdown data arrives late (typical 1-3h lag vs hourly
 * insights) — yesterday's matches get re-checked on today's cycle, etc.
 * Called every hourly cycle from incrementalSync.
 */
export async function enrichRecentUnenriched(shopDomain, days = 7) {
  const windowStart = new Date(Date.now() - days * 24 * 60 * 60 * 1000);

  const orders = await db.order.findMany({
    where: { shopDomain, createdAt: { gte: windowStart } },
    select: { shopifyOrderId: true },
  });
  const orderIds = orders.map(o => o.shopifyOrderId);
  if (orderIds.length === 0) return { enriched: 0, exact: 0, probabilistic: 0 };

  const attrs = await db.attribution.findMany({
    where: {
      shopDomain,
      confidence: { gt: 0 },
      metaAdId: { not: null },
      metaAge: null, // only unenriched
      shopifyOrderId: { in: orderIds },
    },
    select: { id: true, shopifyOrderId: true, metaAdId: true },
  });

  if (attrs.length === 0) return { enriched: 0, exact: 0, probabilistic: 0 };

  console.log(`[AttrEnrichment] Backfilling ${attrs.length} unenriched attributions in last ${days}d`);
  const result = await enrichAttributions(attrs, shopDomain);
  console.log(`[AttrEnrichment] Backfill complete: ${result.enriched} enriched (${result.exact} exact, ${result.probabilistic} probabilistic)`);
  return result;
}
