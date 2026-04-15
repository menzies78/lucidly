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

async function getOrderDates(shopDomain, orderIds) {
  const orders = await db.order.findMany({
    where: { shopDomain, shopifyOrderId: { in: orderIds } },
    select: { shopifyOrderId: true, createdAt: true },
  });
  const map = {};
  for (const o of orders) {
    map[o.shopifyOrderId] = o.createdAt.toISOString().split("T")[0];
  }
  return map;
}

async function enrichAttributions(attributions, shopDomain) {
  if (attributions.length === 0) return { enriched: 0, exact: 0, probabilistic: 0 };

  // Get order dates for all attributions
  const orderIds = attributions.map(a => a.shopifyOrderId);
  const orderDateMap = await getOrderDates(shopDomain, orderIds);

  // Group attributions by adId + date
  const groups = {};
  for (const attr of attributions) {
    const date = orderDateMap[attr.shopifyOrderId];
    if (!date || !attr.metaAdId) continue;
    const key = `${attr.metaAdId}|${date}`;
    if (!groups[key]) groups[key] = { adId: attr.metaAdId, date, attrs: [] };
    groups[key].attrs.push(attr);
  }

  // Fetch breakdown data for all relevant adId+date combos
  const allAdIds = [...new Set(attributions.map(a => a.metaAdId).filter(Boolean))];
  const allDates = [...new Set(Object.values(orderDateMap))];

  if (allAdIds.length === 0 || allDates.length === 0) return { enriched: 0, exact: 0, probabilistic: 0 };

  // Fetch all relevant breakdowns in one query
  const breakdowns = await db.metaBreakdown.findMany({
    where: {
      shopDomain,
      adId: { in: allAdIds },
      date: { in: allDates.map(d => new Date(d + "T00:00:00.000Z")) },
      breakdownType: { in: BREAKDOWN_TYPES },
      conversions: { gt: 0 },
    },
    select: { adId: true, date: true, breakdownType: true, breakdownValue: true, conversions: true },
  });

  // Index breakdowns: adId|date|type → [{ value, conversions }]
  const bdIndex = {};
  for (const bd of breakdowns) {
    const dateStr = bd.date.toISOString().split("T")[0];
    const key = `${bd.adId}|${dateStr}|${bd.breakdownType}`;
    if (!bdIndex[key]) bdIndex[key] = [];
    bdIndex[key].push({ value: bd.breakdownValue, conversions: bd.conversions });
  }

  let enriched = 0, exact = 0, probabilistic = 0;
  const updates = [];

  for (const group of Object.values(groups)) {
    const { adId, date, attrs } = group;

    // For each breakdown type, determine the best assignment
    const assignments = {};
    let allExact = true;

    for (const type of BREAKDOWN_TYPES) {
      const key = `${adId}|${date}|${type}`;
      const buckets = bdIndex[key];
      if (!buckets || buckets.length === 0) continue;

      if (buckets.length === 1) {
        // Unambiguous — all conversions from this ad/day share this value
        assignments[FIELD_MAP[type]] = buckets[0].value;
      } else {
        // Multiple buckets — assign by highest conversion count
        buckets.sort((a, b) => b.conversions - a.conversions);
        // If we have fewer matched orders than top bucket conversions, it's likely correct
        // If we have more, we still assign the top bucket but mark as probabilistic
        assignments[FIELD_MAP[type]] = buckets[0].value;
        allExact = false;
      }
    }

    if (Object.keys(assignments).length === 0) continue;

    // Apply assignments to all attributions in this group
    // If multiple matched orders share the same ad/day, they get the same demographics
    // (which is correct for unambiguous cases, and best-guess for probabilistic)
    for (const attr of attrs) {
      updates.push({
        id: attr.id,
        data: { ...assignments, demographicExact: allExact },
      });
      enriched++;
      if (allExact) exact++;
      else probabilistic++;
    }
  }

  // Batch update in chunks of 100
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
