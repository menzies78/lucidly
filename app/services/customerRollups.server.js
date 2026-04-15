/**
 * Customer rollup builder.
 *
 * Pre-computes EVERYTHING the Customers loader needs at sync time so the
 * loader can render in <500ms by reading only Customer + DailyCustomerRollup
 * + ShopAnalysisCache + MetaBreakdown.
 *
 * Three functions:
 *   1. rebuildCustomerSegments(shopDomain)
 *      - Loads orders + attributions + customers ONCE.
 *      - Assigns metaSegment + per-customer aggregates (lastOrderDate,
 *        firstOrderValue, secondOrderDate, topProducts, avgConfidence,
 *        totalOrders, totalSpent, totalRefunded, metaOrders, country, city,
 *        acquisitionCampaign/AdSet/Ad, discountOrdersCount).
 *      - Computes LTV/journey/geography blobs and writes them to
 *        ShopAnalysisCache.
 *
 *   2. rebuildCustomerRollups(shopDomain)
 *      - Builds DailyCustomerRollup rows from orders × pre-computed segments.
 *
 * Call sites:
 *   - scripts/backfillCustomerRollups.js (one-time)
 *   - incrementalSync.server.js (after each cycle)
 */

import db from "../db.server.js";
import { shopLocalDayKey } from "../utils/shopTime.server";

const DAY_MS = 86400000;
const MONTH_MS = 30 * DAY_MS;
const LTV_WINDOWS = [30, 60, 90, 180, 365];
const MAX_COHORT_MONTHS = 13;

function median(arr) {
  if (!arr.length) return null;
  const sorted = [...arr].sort((a, b) => a - b);
  const m = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[m] : (sorted[m - 1] + sorted[m]) / 2;
}

function r2(v) { return Math.round(v * 100) / 100; }

// ═══════════════════════════════════════════════════════════════
// rebuildCustomerSegments
// ═══════════════════════════════════════════════════════════════

export async function rebuildCustomerSegments(shopDomain) {
  const t0 = Date.now();
  console.log(`[customerRollups] rebuilding segments for ${shopDomain}…`);

  const shopRow = await db.shop.findUnique({ where: { shopDomain }, select: { shopifyTimezone: true } });
  const tz = shopRow?.shopifyTimezone || "UTC";

  // ── Single data load ──
  const [orders, attributions, customers] = await Promise.all([
    db.order.findMany({
      where: { shopDomain, isOnlineStore: true },
      orderBy: { createdAt: "asc" },
      select: {
        shopifyOrderId: true, shopifyCustomerId: true, createdAt: true,
        frozenTotalPrice: true, totalRefunded: true,
        utmConfirmedMeta: true, lineItems: true, discountCodes: true,
        metaCampaignName: true, metaAdSetName: true, metaAdName: true,
        utmCampaign: true, utmTerm: true, utmContent: true,
        country: true, city: true,
        customerOrderCountAtPurchase: true,
      },
    }),
    db.attribution.findMany({
      where: { shopDomain, confidence: { gt: 0 } },
      select: {
        shopifyOrderId: true, confidence: true,
        metaCampaignName: true, metaAdSetName: true, metaAdName: true,
      },
    }),
    db.customer.findMany({
      where: { shopDomain },
      select: { shopifyCustomerId: true, firstOrderDate: true },
    }),
  ]);

  // ── Index data ──
  const attrByOrderId = new Map();
  for (const a of attributions) attrByOrderId.set(a.shopifyOrderId, a);
  const matchedOrderIds = new Set(attributions.map(a => a.shopifyOrderId));
  const utmConfirmedOrderIds = new Set();
  for (const o of orders) {
    if (o.utmConfirmedMeta) utmConfirmedOrderIds.add(o.shopifyOrderId);
  }
  const metaAttributedOrderIds = new Set([...matchedOrderIds, ...utmConfirmedOrderIds]);

  // Customer first-order-date map (shop-local YYYY-MM-DD keys)
  const customerFirstOrderMap = new Map();
  for (const c of customers) {
    if (c.firstOrderDate) {
      customerFirstOrderMap.set(c.shopifyCustomerId, shopLocalDayKey(tz, c.firstOrderDate));
    }
  }

  // Group orders by customer (already sorted by createdAt asc)
  const ordersByCustomer = new Map();
  for (const o of orders) {
    if (!o.shopifyCustomerId) continue;
    let arr = ordersByCustomer.get(o.shopifyCustomerId);
    if (!arr) { arr = []; ordersByCustomer.set(o.shopifyCustomerId, arr); }
    arr.push(o);
  }

  // ── Determine meta-acquired customers ──
  // CRITICAL: Only "metaNew" if BOTH:
  //   1. Their first order in our DB was Meta-attributed AND
  //   2. customerOrderCountAtPurchase === 1 (Shopify confirms this is truly their first order)
  // Without check #2, customers who had previous orders outside our import window
  // would be falsely classified as "metaNew" when they're actually "metaRetargeted".
  const metaAcquiredCustomers = new Set();
  const customerFirstAttr = new Map();

  for (const custId of ordersByCustomer.keys()) {
    const firstDateStr = customerFirstOrderMap.get(custId);
    if (!firstDateStr) continue;
    const custOrders = ordersByCustomer.get(custId);
    for (const o of custOrders) {
      if (shopLocalDayKey(tz, o.createdAt) !== firstDateStr) break;
      // Ground truth: Shopify's customerOrderCountAtPurchase tells us if this
      // is genuinely the customer's first-ever order (not just first in our DB)
      const isGenuinelyNew = o.customerOrderCountAtPurchase === 1;
      if (!isGenuinelyNew) break; // Not a new customer — skip to retargeted detection
      if (matchedOrderIds.has(o.shopifyOrderId)) {
        metaAcquiredCustomers.add(custId);
        const attr = attrByOrderId.get(o.shopifyOrderId);
        customerFirstAttr.set(custId, {
          campaign: attr.metaCampaignName || "",
          adSet: attr.metaAdSetName || "",
          ad: attr.metaAdName || "",
        });
        break;
      }
      if (utmConfirmedOrderIds.has(o.shopifyOrderId)) {
        metaAcquiredCustomers.add(custId);
        customerFirstAttr.set(custId, {
          campaign: o.metaCampaignName || o.utmCampaign || "",
          adSet: o.metaAdSetName || o.utmTerm || "",
          ad: o.metaAdName || o.utmContent || "",
        });
        break;
      }
    }
  }

  // ── Determine retargeted customers (O(N) — uses orderById map) ──
  const orderById = new Map();
  for (const o of orders) orderById.set(o.shopifyOrderId, o);

  const retargetedCustomers = new Set();
  for (const a of attributions) {
    const order = orderById.get(a.shopifyOrderId);
    if (order?.shopifyCustomerId && !metaAcquiredCustomers.has(order.shopifyCustomerId)) {
      retargetedCustomers.add(order.shopifyCustomerId);
    }
  }
  for (const o of orders) {
    if (!o.utmConfirmedMeta || !o.shopifyCustomerId) continue;
    if (!metaAcquiredCustomers.has(o.shopifyCustomerId)) {
      retargetedCustomers.add(o.shopifyCustomerId);
    }
  }

  // ── Compute per-customer aggregates + LTV/journey/geo blobs ──
  // Single pass over customers, accumulating both DB updates and analysis blobs.

  const now = Date.now();

  // LTV accumulators (all-history)
  let ltvMetaNewCount = 0, ltvMetaNewRevenue = 0, ltvMetaNewOrders = 0, ltvMetaNewRepeatCt = 0, ltvMetaNewFirstTotal = 0;
  const ltvMetaNewTimeTo2nd = [];
  let ltvAllCount = 0, ltvAllRevenue = 0, ltvAllOrders = 0, ltvAllRepeatCt = 0;
  const ltvAllTimeTo2nd = [];

  // Maturity-windowed LTV
  const metaNewLtvByWindow = {};
  const allLtvByWindow = {};
  for (const w of LTV_WINDOWS) {
    metaNewLtvByWindow[w] = { count: 0, revenue: 0, orders: 0, repeatCount: 0 };
    allLtvByWindow[w] = { count: 0, revenue: 0, orders: 0, repeatCount: 0 };
  }

  // Recent cohort overlay
  const metaNewRecent = {};
  const allRecent = {};
  for (const w of LTV_WINDOWS) {
    metaNewRecent[w] = { count: 0, revenue: 0, orders: 0, repeatCount: 0 };
    allRecent[w] = { count: 0, revenue: 0, orders: 0, repeatCount: 0 };
  }

  // Monthly cohorts
  const metaNewMonthlyCohorts = {};
  const allMonthlyCohorts = {};

  // Journey accumulators
  const metaJourney = { firstAOV: [], secondAOV: [], thirdAOV: [], gap1to2: [], gap2to3: [] };
  const allJourney = { firstAOV: [], secondAOV: [], thirdAOV: [], gap1to2: [], gap2to3: [] };

  // Geo accumulators (overall, all-history)
  const metaNewCountryAgg = {};
  const allMetaCountryAgg = {};
  const allCountryAgg = {};
  const metaNewCityAgg = {};
  const allMetaCityAgg = {};
  const allCityAgg = {};

  // Per-customer update batches
  const CHUNK = 500;
  let customerUpdates = [];
  let totalUpdated = 0;

  async function flushUpdates() {
    if (customerUpdates.length === 0) return;
    await db.$transaction(customerUpdates);
    totalUpdated += customerUpdates.length;
    customerUpdates = [];
  }

  for (const c of customers) {
    const custId = c.shopifyCustomerId;
    const custOrders = ordersByCustomer.get(custId) || [];

    // Determine segment
    let segment = "organic";
    if (metaAcquiredCustomers.has(custId)) segment = "metaNew";
    else if (retargetedCustomers.has(custId)) segment = "metaRetargeted";

    // Per-customer aggregates (single pass over their orders)
    let totalRevenue = 0, totalRefunded = 0, metaOrdersCount = 0, discountOrdersCount = 0;
    const productCounts = {};
    const confidences = [];
    for (const o of custOrders) {
      totalRevenue += (o.frozenTotalPrice || 0);
      totalRefunded += (o.totalRefunded || 0);
      if (metaAttributedOrderIds.has(o.shopifyOrderId)) metaOrdersCount++;
      if (o.discountCodes) discountOrdersCount++;
      const items = (o.lineItems || "").split(", ").filter(Boolean);
      for (const item of items) productCounts[item] = (productCounts[item] || 0) + 1;
      const attr = attrByOrderId.get(o.shopifyOrderId);
      if (attr?.confidence != null) confidences.push(attr.confidence);
    }
    const topProducts = Object.entries(productCounts)
      .sort((a, b) => b[1] - a[1]).slice(0, 3).map(([n]) => n).join(", ");
    const avgConfidence = confidences.length > 0
      ? Math.round(confidences.reduce((s, c) => s + c, 0) / confidences.length) : null;

    const firstOrder = custOrders[0];
    const lastOrder = custOrders[custOrders.length - 1];
    const secondOrder = custOrders[1];
    const firstAttr = customerFirstAttr.get(custId);

    customerUpdates.push(db.customer.update({
      where: { shopDomain_shopifyCustomerId: { shopDomain, shopifyCustomerId: custId } },
      data: {
        metaSegment: segment,
        totalOrders: custOrders.length,
        totalSpent: r2(totalRevenue),
        totalRefunded: r2(totalRefunded),
        metaOrders: metaOrdersCount,
        firstOrderValue: firstOrder?.frozenTotalPrice || 0,
        lastOrderDate: lastOrder?.createdAt || null,
        secondOrderDate: secondOrder?.createdAt || null,
        topProducts: topProducts || null,
        avgConfidence,
        discountOrdersCount,
        acquisitionCampaign: firstAttr?.campaign || null,
        acquisitionAdSet: firstAttr?.adSet || null,
        acquisitionAd: firstAttr?.ad || null,
        country: firstOrder?.country || null,
        city: firstOrder?.city || null,
      },
    }));
    if (customerUpdates.length >= CHUNK) await flushUpdates();

    // ── Now accumulate analysis blobs (LTV/journey/geo) ──
    if (custOrders.length === 0 || !firstOrder?.createdAt) continue;

    const acqTime = firstOrder.createdAt.getTime();
    const daysSinceAcq = Math.floor((now - acqTime) / DAY_MS);
    const netRevenue = totalRevenue - totalRefunded;

    // LTV all-history tile stats
    ltvAllCount++;
    ltvAllRevenue += netRevenue;
    ltvAllOrders += custOrders.length;
    if (custOrders.length > 1) ltvAllRepeatCt++;
    let timeTo2nd = null;
    if (secondOrder) {
      timeTo2nd = Math.floor((secondOrder.createdAt.getTime() - acqTime) / DAY_MS);
      ltvAllTimeTo2nd.push(timeTo2nd);
    }
    if (segment === "metaNew") {
      ltvMetaNewCount++;
      ltvMetaNewRevenue += netRevenue;
      ltvMetaNewOrders += custOrders.length;
      ltvMetaNewFirstTotal += (firstOrder.frozenTotalPrice || 0);
      if (custOrders.length > 1) ltvMetaNewRepeatCt++;
      if (timeTo2nd != null) ltvMetaNewTimeTo2nd.push(timeTo2nd);
    }

    // Maturity-windowed LTV
    for (const w of LTV_WINDOWS) {
      if (daysSinceAcq < w) continue;
      const windowEndTime = acqTime + w * DAY_MS;
      let windowRev = 0, windowOrd = 0;
      for (const o of custOrders) {
        if (o.createdAt.getTime() > windowEndTime) break;
        windowRev += (o.frozenTotalPrice || 0) - (o.totalRefunded || 0);
        windowOrd++;
      }
      allLtvByWindow[w].count++;
      allLtvByWindow[w].revenue += windowRev;
      allLtvByWindow[w].orders += windowOrd;
      if (windowOrd > 1) allLtvByWindow[w].repeatCount++;
      if (segment === "metaNew") {
        metaNewLtvByWindow[w].count++;
        metaNewLtvByWindow[w].revenue += windowRev;
        metaNewLtvByWindow[w].orders += windowOrd;
        if (windowOrd > 1) metaNewLtvByWindow[w].repeatCount++;
      }

      // Recent overlay: customer matured past W and was acquired within last 2W days
      if (daysSinceAcq < w * 2) {
        allRecent[w].count++;
        allRecent[w].revenue += windowRev;
        allRecent[w].orders += windowOrd;
        if (windowOrd > 1) allRecent[w].repeatCount++;
        if (segment === "metaNew") {
          metaNewRecent[w].count++;
          metaNewRecent[w].revenue += windowRev;
          metaNewRecent[w].orders += windowOrd;
          if (windowOrd > 1) metaNewRecent[w].repeatCount++;
        }
      }
    }

    // Monthly cohort (shop-local)
    const acqMonth = shopLocalDayKey(tz, firstOrder.createdAt).slice(0, 7);
    const initRow = () => {
      const months = {};
      for (let m = 0; m < MAX_COHORT_MONTHS; m++) {
        months[m] = { revenue: 0, orders: 0, activeCustomers: 0, cumulativeRevenue: 0, cumulativeOrders: 0 };
      }
      return { count: 0, months };
    };
    if (!allMonthlyCohorts[acqMonth]) allMonthlyCohorts[acqMonth] = initRow();
    allMonthlyCohorts[acqMonth].count++;
    let cumRev = 0, cumOrd = 0;
    for (let m = 0; m < MAX_COHORT_MONTHS; m++) {
      if (daysSinceAcq < m * 30) break;
      const monthStart = acqTime + m * MONTH_MS;
      const monthEnd = acqTime + (m + 1) * MONTH_MS;
      let monthRev = 0, monthOrd = 0;
      for (const o of custOrders) {
        const t = o.createdAt.getTime();
        if (t >= monthStart && t < monthEnd) {
          monthRev += (o.frozenTotalPrice || 0) - (o.totalRefunded || 0);
          monthOrd++;
        }
      }
      cumRev += monthRev;
      cumOrd += monthOrd;
      const bucket = allMonthlyCohorts[acqMonth].months[m];
      bucket.revenue += monthRev;
      bucket.orders += monthOrd;
      bucket.cumulativeRevenue += cumRev;
      bucket.cumulativeOrders += cumOrd;
      if (monthOrd > 0) bucket.activeCustomers++;
    }
    if (segment === "metaNew") {
      if (!metaNewMonthlyCohorts[acqMonth]) metaNewMonthlyCohorts[acqMonth] = initRow();
      metaNewMonthlyCohorts[acqMonth].count++;
      let mCumRev = 0, mCumOrd = 0;
      for (let m = 0; m < MAX_COHORT_MONTHS; m++) {
        if (daysSinceAcq < m * 30) break;
        const monthStart = acqTime + m * MONTH_MS;
        const monthEnd = acqTime + (m + 1) * MONTH_MS;
        let monthRev = 0, monthOrd = 0;
        for (const o of custOrders) {
          const t = o.createdAt.getTime();
          if (t >= monthStart && t < monthEnd) {
            monthRev += (o.frozenTotalPrice || 0) - (o.totalRefunded || 0);
            monthOrd++;
          }
        }
        mCumRev += monthRev;
        mCumOrd += monthOrd;
        const bucket = metaNewMonthlyCohorts[acqMonth].months[m];
        bucket.revenue += monthRev;
        bucket.orders += monthOrd;
        bucket.cumulativeRevenue += mCumRev;
        bucket.cumulativeOrders += mCumOrd;
        if (monthOrd > 0) bucket.activeCustomers++;
      }
    }

    // Journey
    allJourney.firstAOV.push(firstOrder.frozenTotalPrice || 0);
    if (segment === "metaNew") metaJourney.firstAOV.push(firstOrder.frozenTotalPrice || 0);
    if (custOrders.length >= 2) {
      allJourney.secondAOV.push(secondOrder.frozenTotalPrice || 0);
      const gap = Math.floor((secondOrder.createdAt.getTime() - acqTime) / DAY_MS);
      allJourney.gap1to2.push(gap);
      if (segment === "metaNew") {
        metaJourney.secondAOV.push(secondOrder.frozenTotalPrice || 0);
        metaJourney.gap1to2.push(gap);
      }
    }
    if (custOrders.length >= 3) {
      const third = custOrders[2];
      allJourney.thirdAOV.push(third.frozenTotalPrice || 0);
      const gap2 = Math.floor((third.createdAt.getTime() - secondOrder.createdAt.getTime()) / DAY_MS);
      allJourney.gap2to3.push(gap2);
      if (segment === "metaNew") {
        metaJourney.thirdAOV.push(third.frozenTotalPrice || 0);
        metaJourney.gap2to3.push(gap2);
      }
    }

    // Geography (all-history aggregates by customer)
    const country = firstOrder.country;
    const city = firstOrder.city;
    if (country) {
      if (!allCountryAgg[country]) allCountryAgg[country] = { customers: 0, revenue: 0, orders: 0 };
      allCountryAgg[country].customers++;
      allCountryAgg[country].revenue += netRevenue;
      allCountryAgg[country].orders += custOrders.length;
      if (segment === "metaNew" || segment === "metaRetargeted") {
        if (!allMetaCountryAgg[country]) allMetaCountryAgg[country] = { customers: 0, revenue: 0, orders: 0 };
        allMetaCountryAgg[country].customers++;
        allMetaCountryAgg[country].revenue += netRevenue;
        allMetaCountryAgg[country].orders += custOrders.length;
      }
      if (segment === "metaNew") {
        if (!metaNewCountryAgg[country]) metaNewCountryAgg[country] = { customers: 0, revenue: 0, orders: 0 };
        metaNewCountryAgg[country].customers++;
        metaNewCountryAgg[country].revenue += netRevenue;
        metaNewCountryAgg[country].orders += custOrders.length;
      }
    }
    if (city) {
      if (!allCityAgg[city]) allCityAgg[city] = { customers: 0, revenue: 0, orders: 0 };
      allCityAgg[city].customers++;
      allCityAgg[city].revenue += netRevenue;
      allCityAgg[city].orders += custOrders.length;
      if (segment === "metaNew" || segment === "metaRetargeted") {
        if (!allMetaCityAgg[city]) allMetaCityAgg[city] = { customers: 0, revenue: 0, orders: 0 };
        allMetaCityAgg[city].customers++;
        allMetaCityAgg[city].revenue += netRevenue;
        allMetaCityAgg[city].orders += custOrders.length;
      }
      if (segment === "metaNew") {
        if (!metaNewCityAgg[city]) metaNewCityAgg[city] = { customers: 0, revenue: 0, orders: 0 };
        metaNewCityAgg[city].customers++;
        metaNewCityAgg[city].revenue += netRevenue;
        metaNewCityAgg[city].orders += custOrders.length;
      }
    }
  }
  await flushUpdates();

  // ── Build LTV blob ──
  const buildBenchmark = (bucketMap) => {
    const windows = LTV_WINDOWS
      .filter(w => bucketMap[w].count >= 5)
      .map(w => ({
        window: w,
        count: bucketMap[w].count,
        avgLtv: r2(bucketMap[w].revenue / bucketMap[w].count),
        avgOrders: r2(bucketMap[w].orders / bucketMap[w].count),
        repeatRate: Math.round((bucketMap[w].repeatCount / bucketMap[w].count) * 100),
        repeatCount: bucketMap[w].repeatCount,
      }));
    const maxWindow = windows.length > 0 ? windows[windows.length - 1].window : 0;
    return { maxWindow, windows };
  };

  const buildRecent = (bucketMap) => LTV_WINDOWS
    .filter(w => bucketMap[w].count > 0)
    .map(w => ({
      window: w,
      count: bucketMap[w].count,
      avgLtv: r2(bucketMap[w].revenue / bucketMap[w].count),
      avgOrders: r2(bucketMap[w].orders / bucketMap[w].count),
      repeatRate: Math.round((bucketMap[w].repeatCount / bucketMap[w].count) * 100),
      repeatCount: bucketMap[w].repeatCount,
    }));

  const buildMonthlyTable = (cohorts) => {
    const rows = Object.entries(cohorts)
      .sort((a, b) => a[0].localeCompare(b[0]))
      .map(([month, data]) => {
        const [y, m] = month.split("-").map(Number);
        const cohortStart = new Date(y, m - 1, 1);
        const monthsMatured = Math.floor((now - cohortStart.getTime()) / MONTH_MS);
        const maxMonth = Math.min(monthsMatured, MAX_COHORT_MONTHS - 1);
        return {
          month, count: data.count, maxMonth,
          months: Array.from({ length: MAX_COHORT_MONTHS }, (_, i) => {
            const bucket = data.months[i];
            const matured = i <= maxMonth;
            return {
              month: i,
              matured,
              avgLtv: matured && data.count > 0 ? r2(bucket.cumulativeRevenue / data.count) : null,
              retention: matured && data.count > 0 ? Math.round((bucket.activeCustomers / data.count) * 100) : null,
              orders: matured ? bucket.orders : null,
              activeCustomers: matured ? bucket.activeCustomers : null,
              cumulativeOrders: matured && data.count > 0 ? r2(bucket.cumulativeOrders / data.count) : null,
            };
          }),
        };
      });
    const maxMonthAcross = rows.length > 0 ? Math.max(...rows.map(r => r.maxMonth)) : 0;
    return { rows, maxMonth: maxMonthAcross };
  };

  const ltvBlob = {
    ltvTile: {
      meta: {
        count: ltvMetaNewCount,
        avgLtv: ltvMetaNewCount > 0 ? r2(ltvMetaNewRevenue / ltvMetaNewCount) : 0,
        avgOrders: ltvMetaNewCount > 0 ? r2(ltvMetaNewOrders / ltvMetaNewCount) : 0,
        repeatRate: ltvMetaNewCount > 0 ? Math.round((ltvMetaNewRepeatCt / ltvMetaNewCount) * 100) : 0,
        avgAov: ltvMetaNewOrders > 0 ? r2(ltvMetaNewRevenue / ltvMetaNewOrders) : 0,
        medianTimeTo2nd: median(ltvMetaNewTimeTo2nd),
        avgFirstOrder: ltvMetaNewCount > 0 ? r2(ltvMetaNewFirstTotal / ltvMetaNewCount) : 0,
      },
      all: {
        count: ltvAllCount,
        avgLtv: ltvAllCount > 0 ? r2(ltvAllRevenue / ltvAllCount) : 0,
        avgOrders: ltvAllCount > 0 ? r2(ltvAllOrders / ltvAllCount) : 0,
        repeatRate: ltvAllCount > 0 ? Math.round((ltvAllRepeatCt / ltvAllCount) * 100) : 0,
        avgAov: ltvAllOrders > 0 ? r2(ltvAllRevenue / ltvAllOrders) : 0,
        medianTimeTo2nd: median(ltvAllTimeTo2nd),
      },
    },
    ltvBenchmark: {
      meta: buildBenchmark(metaNewLtvByWindow),
      all: buildBenchmark(allLtvByWindow),
    },
    ltvRecent: {
      meta: buildRecent(metaNewRecent),
      all: buildRecent(allRecent),
    },
    ltvMonthly: {
      meta: buildMonthlyTable(metaNewMonthlyCohorts),
      all: buildMonthlyTable(allMonthlyCohorts),
    },
  };

  // ── Build journey blob ──
  const avg = (arr) => arr.length ? r2(arr.reduce((s, v) => s + v, 0) / arr.length) : 0;
  const journeyBlob = {
    meta: {
      firstOrderCount: metaJourney.firstAOV.length,
      secondOrderCount: metaJourney.secondAOV.length,
      thirdOrderCount: metaJourney.thirdAOV.length,
      firstAOV: avg(metaJourney.firstAOV),
      secondAOV: avg(metaJourney.secondAOV),
      thirdAOV: avg(metaJourney.thirdAOV),
      gap1to2Days: metaJourney.gap1to2.length ? Math.round(median(metaJourney.gap1to2)) : null,
      gap2to3Days: metaJourney.gap2to3.length ? Math.round(median(metaJourney.gap2to3)) : null,
    },
    all: {
      firstOrderCount: allJourney.firstAOV.length,
      secondOrderCount: allJourney.secondAOV.length,
      thirdOrderCount: allJourney.thirdAOV.length,
      firstAOV: avg(allJourney.firstAOV),
      secondAOV: avg(allJourney.secondAOV),
      thirdAOV: avg(allJourney.thirdAOV),
      gap1to2Days: allJourney.gap1to2.length ? Math.round(median(allJourney.gap1to2)) : null,
      gap2to3Days: allJourney.gap2to3.length ? Math.round(median(allJourney.gap2to3)) : null,
    },
  };

  // ── Build geo blob ──
  const topN = (agg, n = 50) => Object.entries(agg)
    .map(([name, v]) => ({ label: name, customers: v.customers, revenue: r2(v.revenue), orders: v.orders, spend: 0 }))
    .sort((a, b) => b.customers - a.customers).slice(0, n);

  const geoBlob = {
    countries: {
      all: topN(allCountryAgg),
      allMeta: topN(allMetaCountryAgg),
      metaNew: topN(metaNewCountryAgg),
    },
    cities: {
      all: topN(allCityAgg),
      allMeta: topN(allMetaCityAgg),
      metaNew: topN(metaNewCityAgg),
    },
    counts: {
      all: ltvAllCount,
      allMeta: Object.values(allMetaCountryAgg).reduce((s, c) => s + c.customers, 0),
      metaNew: Object.values(metaNewCountryAgg).reduce((s, c) => s + c.customers, 0),
    },
  };

  // ── Persist blobs to ShopAnalysisCache ──
  await db.shopAnalysisCache.upsert({
    where: { shopDomain_cacheKey: { shopDomain, cacheKey: "customers:ltv" } },
    create: { shopDomain, cacheKey: "customers:ltv", payload: JSON.stringify(ltvBlob) },
    update: { payload: JSON.stringify(ltvBlob), computedAt: new Date() },
  });
  await db.shopAnalysisCache.upsert({
    where: { shopDomain_cacheKey: { shopDomain, cacheKey: "customers:journey" } },
    create: { shopDomain, cacheKey: "customers:journey", payload: JSON.stringify(journeyBlob) },
    update: { payload: JSON.stringify(journeyBlob), computedAt: new Date() },
  });
  await db.shopAnalysisCache.upsert({
    where: { shopDomain_cacheKey: { shopDomain, cacheKey: "customers:geo" } },
    create: { shopDomain, cacheKey: "customers:geo", payload: JSON.stringify(geoBlob) },
    update: { payload: JSON.stringify(geoBlob), computedAt: new Date() },
  });

  console.log(`[customerRollups] segments: ${totalUpdated} customers + ltv/journey/geo blobs in ${Date.now() - t0}ms (metaNew=${metaAcquiredCustomers.size}, retargeted=${retargetedCustomers.size}, organic=${totalUpdated - metaAcquiredCustomers.size - retargetedCustomers.size})`);
  return { customers: totalUpdated, ms: Date.now() - t0 };
}

// ═══════════════════════════════════════════════════════════════
// rebuildCustomerRollups — daily per-segment aggregates
// ═══════════════════════════════════════════════════════════════

export async function rebuildCustomerRollups(shopDomain) {
  const t0 = Date.now();
  console.log(`[customerRollups] rebuilding daily rollups for ${shopDomain}…`);

  const shopRow = await db.shop.findUnique({ where: { shopDomain }, select: { shopifyTimezone: true } });
  const tz = shopRow?.shopifyTimezone || "UTC";

  const [orders, customers] = await Promise.all([
    db.order.findMany({
      where: { shopDomain, isOnlineStore: true },
      orderBy: { createdAt: "asc" },
      select: {
        shopifyOrderId: true, shopifyCustomerId: true, createdAt: true,
        frozenTotalPrice: true, totalRefunded: true,
        customerOrderCountAtPurchase: true,
      },
    }),
    db.customer.findMany({
      where: { shopDomain },
      select: { shopifyCustomerId: true, metaSegment: true },
    }),
  ]);

  const segmentMap = new Map();
  for (const c of customers) segmentMap.set(c.shopifyCustomerId, c.metaSegment || "organic");

  const buckets = new Map();
  function ensure(dateStr, segment) {
    const key = `${dateStr}|${segment}`;
    let b = buckets.get(key);
    if (!b) {
      b = {
        dateStr, segment,
        newCustomers: 0, orders: 0, revenue: 0,
        refundedAmount: 0, firstOrderRevenue: 0, repeatCustomers: 0,
        seenCustomers: new Set(),
      };
      buckets.set(key, b);
    }
    return b;
  }

  const countedNewCustomers = new Set();

  for (const order of orders) {
    const custId = order.shopifyCustomerId;
    if (!custId) continue;

    const custSegment = segmentMap.get(custId) || "organic";
    const dateStr = shopLocalDayKey(tz, order.createdAt);
    const isFirstPurchase = order.customerOrderCountAtPurchase === 1;
    const revenue = order.frozenTotalPrice || 0;
    const refunded = order.totalRefunded || 0;

    let orderSegment;
    if (custSegment === "metaNew") {
      orderSegment = isFirstPurchase ? "metaNew" : "metaRepeat";
    } else {
      orderSegment = custSegment;
    }

    const b = ensure(dateStr, orderSegment);
    b.orders++;
    b.revenue += revenue;
    b.refundedAmount += refunded;

    if (isFirstPurchase) {
      b.firstOrderRevenue += revenue;
      if (!countedNewCustomers.has(custId)) {
        b.newCustomers++;
        countedNewCustomers.add(custId);
      }
    }

    if (!b.seenCustomers.has(custId)) {
      b.seenCustomers.add(custId);
      if (!isFirstPurchase) b.repeatCustomers++;
    }
  }

  await db.dailyCustomerRollup.deleteMany({ where: { shopDomain } });

  const rows = [];
  for (const b of buckets.values()) {
    rows.push({
      shopDomain,
      date: new Date(`${b.dateStr}T00:00:00.000Z`),
      segment: b.segment,
      newCustomers: b.newCustomers,
      orders: b.orders,
      revenue: r2(b.revenue),
      refundedAmount: r2(b.refundedAmount),
      firstOrderRevenue: r2(b.firstOrderRevenue),
      repeatCustomers: b.repeatCustomers,
    });
  }

  const CHUNK = 500;
  for (let i = 0; i < rows.length; i += CHUNK) {
    await db.dailyCustomerRollup.createMany({ data: rows.slice(i, i + CHUNK) });
  }

  console.log(`[customerRollups] ${shopDomain}: ${rows.length} daily rollup rows in ${Date.now() - t0}ms`);
  return { rollupRows: rows.length, ms: Date.now() - t0 };
}
