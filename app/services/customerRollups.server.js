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
import { geocodeCity } from "./geo/geocoder.server.js";

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

// Some merchants (e.g. Vollebak) maintain near-duplicate product listings
// for technical reasons — "Sashiko Chore Jacket. Blue edition." and
// "Sashiko Chore Jacket. Blue edition" share a SKU but are two listings.
// We don't have SKU-level analytics yet (the `productSkus` field on Order
// is captured but unused downstream), so for now we normalize titles by
// stripping trailing periods + whitespace. Universally safe — there's no
// merchant where "Foo." and "Foo" should be different products. If we
// ever see non-trailing-period dup patterns we'll graduate to a real
// SKU → canonical-name mapping.
function normalizeProductName(s) {
  if (!s) return s;
  return s.replace(/[.\s]+$/u, "");
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
  // discountedOrderIds: orders that had at least one line item with
  // totalDiscount > 0. Used to flag "Discount" vs "Full price" customers
  // in the Customer Map Explorer. Captured at time of purchase via
  // originalUnitPrice − discountedUnitPrice in orderSync (so this is the
  // closest signal we have to the merchant's intended question of
  // "compareAtPrice > price at purchase" without snapshotting variants).
  const discountedRows = await db.$queryRaw`
    SELECT DISTINCT shopifyOrderId
    FROM OrderLineItem
    WHERE shopDomain = ${shopDomain} AND totalDiscount > 0
  `;
  const discountedOrderIds = new Set(discountedRows.map((r) => r.shopifyOrderId));

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
        country: true, countryCode: true, city: true,
        customerOrderCountAtPurchase: true,
      },
    }),
    db.attribution.findMany({
      where: { shopDomain, confidence: { gt: 0 } },
      select: {
        shopifyOrderId: true, confidence: true,
        metaCampaignName: true, metaAdSetName: true, metaAdName: true,
        metaAge: true, metaGender: true,
      },
    }),
    db.customer.findMany({
      where: { shopDomain },
      select: { shopifyCustomerId: true, firstOrderDate: true, inferredGender: true },
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
  // shopifyCustomerId -> name-derived gender, used as a fallback when
  // attribution-level metaGender is missing (which is most of the time
  // for orders older than the MetaBreakdown lookback window).
  const customerInferredGenderMap = new Map();
  for (const c of customers) {
    if (c.firstOrderDate) {
      customerFirstOrderMap.set(c.shopifyCustomerId, shopLocalDayKey(tz, c.firstOrderDate));
    }
    if (c.inferredGender) {
      customerInferredGenderMap.set(c.shopifyCustomerId, c.inferredGender);
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
          age: attr.metaAge || null,
          gender: attr.metaGender || null,
        });
        break;
      }
      if (utmConfirmedOrderIds.has(o.shopifyOrderId)) {
        metaAcquiredCustomers.add(custId);
        customerFirstAttr.set(custId, {
          campaign: o.metaCampaignName || o.utmCampaign || "",
          adSet: o.metaAdSetName || o.utmTerm || "",
          ad: o.metaAdName || o.utmContent || "",
          age: null,
          gender: null,
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

  // Per-customer LTV records (metaNew only) for filterable exploration.
  // Each record is the input to the Customers page LTV tile filters
  // (gender/age/country) and powers recompute of avgLtv, LTV:CAC, payback,
  // and the maturation line. Kept minimal to cap blob size: typical
  // merchants with 5–10k metaNew customers produce a ~400–800 KB blob.
  const ltvCustomers = [];

  // Geo accumulators (overall, all-history)
  const metaNewCountryAgg = {};
  const allMetaCountryAgg = {};
  const allCountryAgg = {};
  const metaNewCityAgg = {};
  const allMetaCityAgg = {};
  const allCityAgg = {};

  // Customer Map Explorer points. One row per geocoded customer with the
  // minimum data needed for client-side filtering + clustering. The full
  // blob is sent to the browser; supercluster handles aggregation visually.
  // Typical 30k-customer merchant = ~3 MB JSON / ~250 KB gzipped.
  const customerMapPoints = [];

  // Global product index for the map blob — strings deduped once, points
  // reference them by integer index. Lets the map filter by "buyers of X"
  // without bloating each point with full product names. Per customer we
  // include only DISTINCT products (not quantity), which is what the filter
  // needs.
  const productIndex = new Map(); // productName -> int index
  const getProductIdx = (name) => {
    let i = productIndex.get(name);
    if (i == null) { i = productIndex.size; productIndex.set(name, i); }
    return i;
  };

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
    let lineDiscountOrdersCount = 0;
    const productCounts = {};
    const confidences = [];
    for (const o of custOrders) {
      totalRevenue += (o.frozenTotalPrice || 0);
      totalRefunded += (o.totalRefunded || 0);
      if (metaAttributedOrderIds.has(o.shopifyOrderId)) metaOrdersCount++;
      if (o.discountCodes) discountOrdersCount++;
      if (discountedOrderIds.has(o.shopifyOrderId)) lineDiscountOrdersCount++;
      const items = (o.lineItems || "").split(", ").filter(Boolean);
      for (const item of items) {
        const name = normalizeProductName(item);
        if (!name) continue;
        productCounts[name] = (productCounts[name] || 0) + 1;
      }
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

    // Geocode the customer's billing address (taken from their first order's
    // billing address). geocodeCity returns the country centroid when the
    // city is missing or unmatched, so we still get a marker as long as the
    // country is known.
    const geo = firstOrder
      ? geocodeCity(firstOrder.countryCode, firstOrder.city)
      : null;
    const lat = geo ? geo[0] : null;
    const lng = geo ? geo[1] : null;

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
        lat, lng,
      },
    }));
    if (customerUpdates.length >= CHUNK) await flushUpdates();

    // Map Explorer point. Only push customers we could place geographically;
    // unplaceable customers (no country, exotic country code) are dropped
    // rather than rendered at lat 0,0 in the Atlantic.
    if (lat != null && lng != null && custOrders.length > 0) {
      // Compact point shape — single-letter keys + only fields the client
      // can't trivially derive. The blob is held in memory alongside every
      // other rollup data structure, so trimming each point ~60% (vs the
      // verbose object shape) keeps a 30k-customer merchant inside the
      // 3 GB heap. Client (CustomerMapExplorer) decodes the codes back to
      // human-readable values; net revenue and refundRate are computed on
      // the fly there.
      //   i  id            la lat            lo lng
      //   s  seg (m/r/o)   g  gender (f/m/null)
      //   a  age bracket   c  countryCode    t  city
      //   $  spent (£)     r  refunded (£)   n  num orders
      //   d  daysSinceLast x  approx (1=country centroid)
      //   p  discountEver (1=ever bought a discounted line item)
      //   v  vipBand (5/10/20, set in second pass)
      //   h  highestRefunds (1, set in second pass)
      //   pr distinct product indices into productList
      const distinctProductIdx = Object.keys(productCounts).map(getProductIdx);
      // Gender resolution mirrors ltvCustomers: prefer attribution gender
      // (Meta breakdown — only ~30% of orders carry it), fall back to the
      // name-based inferredGender on the Customer row. Without this fallback
      // the map filter only covered Meta-acquired customers; with it, every
      // segment (organic, retargeted, Meta-new) gets a M/F tag wherever the
      // first name is unambiguous.
      const mapGenderRaw = firstAttr?.gender || customerInferredGenderMap.get(custId) || null;
      customerMapPoints.push({
        i: custId,
        la: Math.round(lat * 10000) / 10000,   // 4dp ≈ 11m precision
        lo: Math.round(lng * 10000) / 10000,
        s: segment === "metaNew" ? "m" : segment === "metaRetargeted" ? "r" : "o",
        g: mapGenderRaw ? (mapGenderRaw[0] === "f" ? "f" : "m") : null,
        a: firstAttr?.age || null,
        c: firstOrder?.countryCode || null,
        t: firstOrder?.city || null,
        $: r2(totalRevenue),
        r: r2(totalRefunded),
        n: custOrders.length,
        d: lastOrder ? Math.floor((now - lastOrder.createdAt.getTime()) / DAY_MS) : null,
        x: !firstOrder?.city ? 1 : 0,
        p: lineDiscountOrdersCount > 0 ? 1 : 0,
        pr: distinctProductIdx,
      });
    }

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

    // Maturity-windowed LTV — plus per-customer window snapshots (metaNew
    // only) for the filterable LTV tile exploration.
    const customerLtvByWindow = {};
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
        customerLtvByWindow[w] = r2(windowRev);
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

    // Capture per-customer record for LTV exploration. Gender/age from
    // first attribution (captured at acquisition time); country from first
    // order shipping address. LTV is net of refunds.
    if (segment === "metaNew") {
      const firstAttr2 = customerFirstAttr.get(custId);
      // Gender resolution: prefer the observed attribution gender (Meta
      // breakdown enrichment), fall back to name-based inference, then
      // null. Coverage on attribution gender is sparse (only orders whose
      // ad+date intersect with MetaBreakdown rows), so the fallback fills
      // the long tail of historical customers.
      const resolvedGender = firstAttr2?.gender || customerInferredGenderMap.get(custId) || null;
      ltvCustomers.push({
        gender: resolvedGender,
        age: firstAttr2?.age || null,
        country: firstOrder?.country || null,
        ltv: r2(netRevenue),
        firstOrder: r2(firstOrder.frozenTotalPrice || 0),
        orders: custOrders.length,
        timeTo2nd,
        acqMonth: shopLocalDayKey(tz, firstOrder.createdAt).slice(0, 7),
        ltvByWindow: customerLtvByWindow,
      });
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
    // Per-metaNew-customer records powering the filterable LTV tile.
    // Consumers: app.customers.tsx MetaLtvTile — filters by gender/age/country
    // and recomputes avg LTV, LTV:CAC, payback, and maturation line from this
    // array.
    ltvCustomers,
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

  // ── Customer Map Explorer blob ──
  // VIP thresholds: top 5% / 10% / 20% by NET revenue (spent − refunded).
  // The "highestRefunds" band is the top 10% by refund rate among
  // customers who have any refunds at all. Both are computed once here so
  // the client doesn't have to re-sort 30k points to know which dot is a
  // VIP. The point shape uses single-letter keys ($/r = spent/refunded).
  const sortedNet = customerMapPoints.map((p) => p.$ - p.r).sort((a, b) => b - a);
  const pickThreshold = (pct) => {
    if (sortedNet.length === 0) return 0;
    const idx = Math.max(0, Math.floor(sortedNet.length * (pct / 100)) - 1);
    return sortedNet[idx] ?? 0;
  };
  const vipThresholds = {
    top5: pickThreshold(5),
    top10: pickThreshold(10),
    top20: pickThreshold(20),
  };
  const refundedNonZero = customerMapPoints
    .filter((p) => p.$ > 0 && p.r > 0)
    .map((p) => p.r / p.$)
    .sort((a, b) => b - a);
  const highestRefundThreshold = refundedNonZero.length > 0
    ? refundedNonZero[Math.max(0, Math.floor(refundedNonZero.length * 0.1) - 1)] ?? 0
    : 0;

  // Tag VIP band + highest-refunds on each point. Done in-place to avoid
  // doubling the array footprint.
  for (const p of customerMapPoints) {
    const net = p.$ - p.r;
    p.v = net >= vipThresholds.top5 && vipThresholds.top5 > 0
      ? 5
      : net >= vipThresholds.top10 && vipThresholds.top10 > 0
        ? 10
        : net >= vipThresholds.top20 && vipThresholds.top20 > 0
          ? 20
          : 0;
    const rate = p.$ > 0 ? p.r / p.$ : 0;
    p.h = highestRefundThreshold > 0 && rate >= highestRefundThreshold ? 1 : 0;
  }

  // ── Gem detection ──
  // Statistical anomalies in the geo distribution. We surface 5 high-signal
  // "did you notice" cards on the Map Explorer. Each gem is a country (or
  // country+product) where some feature over-indexes vs the global average,
  // with both a lift floor (1.5×) and a z-score floor (1.96 ≈ 95% CI) so we
  // don't fire on noise from tiny markets. Score = lift × sqrt(count) so
  // we prefer high-lift gems in countries with real volume.
  //
  // MEMORY: this runs at the peak of customerRollups — ltv/journey/geo
  // blobs are still in memory alongside customerMapPoints. We deliberately
  // avoid per-country product Maps (would be O(C × P) ≈ 500k entries) and
  // instead bound the product×country grid to top 5 products × top 10
  // countries. The Apr 28 OOM was traced to that O(C × P) Map.
  const productList = Array.from(productIndex.keys());
  productIndex.clear();   // release the Map — productList carries forward
  const gems = (() => {
    const N = customerMapPoints.length;
    if (N < 50) return [];

    // Single-pass: country aggregates + global counters + product totals.
    // Plain objects (Object.create(null)) instead of Maps trim per-entry
    // overhead; this matters when we have ~50 country buckets all alive
    // at the same time as the LTV/journey/geo blobs.
    const byCountry = Object.create(null);
    let gVip = 0, gDisc = 0, gRefund = 0, gRepeat = 0, gLapsed = 0;
    const gProduct = Object.create(null); // pIdx (string) -> count

    for (const p of customerMapPoints) {
      if (p.v === 5) gVip++;
      if (p.p === 1) gDisc++;
      if (p.h === 1) gRefund++;
      if (p.n >= 2) gRepeat++;
      if (p.d != null && p.d > 365) gLapsed++;
      for (let k = 0; k < p.pr.length; k++) {
        const idx = p.pr[k];
        gProduct[idx] = (gProduct[idx] || 0) + 1;
      }
      if (!p.c) continue;
      let ag = byCountry[p.c];
      if (!ag) {
        ag = byCountry[p.c] = { total: 0, vip: 0, disc: 0, refund: 0, repeat: 0, lapsed: 0 };
      }
      ag.total++;
      if (p.v === 5) ag.vip++;
      if (p.p === 1) ag.disc++;
      if (p.h === 1) ag.refund++;
      if (p.n >= 2) ag.repeat++;
      if (p.d != null && p.d > 365) ag.lapsed++;
    }

    const gVipShare = gVip / N, gDiscShare = gDisc / N, gRefundShare = gRefund / N;
    const gRepeatShare = gRepeat / N, gLapsedShare = gLapsed / N;

    const candidates = [];
    const testAnomaly = (kind, country, count, total, globalShare, filters) => {
      if (count < 5 || total < 20) return;
      const expected = total * globalShare;
      if (expected <= 0) return;
      const lift = count / expected;
      if (lift < 1.5) return;
      const std = Math.sqrt(globalShare * (1 - globalShare) * total);
      const z = std > 0 ? (count - expected) / std : 0;
      if (z < 1.96) return;
      candidates.push({
        kind, country, count, expected: Math.round(expected),
        lift: Math.round(lift * 10) / 10, filters,
        score: lift * Math.sqrt(count),
      });
    };

    for (const cc in byCountry) {
      const ag = byCountry[cc];
      testAnomaly("vip", cc, ag.vip, ag.total, gVipShare, { country: cc, vip: "top5" });
      testAnomaly("discount", cc, ag.disc, ag.total, gDiscShare, { country: cc, pricing: "discount" });
      testAnomaly("refund", cc, ag.refund, ag.total, gRefundShare, { country: cc, refundsTop: true });
      testAnomaly("repeat", cc, ag.repeat, ag.total, gRepeatShare, { country: cc, orderBand: "2-3" });
      testAnomaly("lapsed", cc, ag.lapsed, ag.total, gLapsedShare, { country: cc, recency: "lapsed" });
    }

    // Product × country anomalies — bounded grid so we never blow up on
    // catalogues with thousands of SKUs. Top 5 products globally × top 10
    // countries by total = max 50 cells, regardless of merchant size.
    const topProductIdx = Object.keys(gProduct)
      .map(Number)
      .sort((a, b) => gProduct[b] - gProduct[a])
      .slice(0, 5);
    const topCountries = Object.keys(byCountry)
      .filter((cc) => byCountry[cc].total >= 20)
      .sort((a, b) => byCountry[b].total - byCountry[a].total)
      .slice(0, 10);

    if (topProductIdx.length > 0 && topCountries.length > 0) {
      const topProdSet = new Set(topProductIdx);
      const topCountrySet = new Set(topCountries);
      // grid[cc][pIdx] = count. Allocated lazily, max 10 × 5 = 50 entries.
      const grid = Object.create(null);
      for (const cc of topCountries) grid[cc] = Object.create(null);

      for (const p of customerMapPoints) {
        if (!p.c || !topCountrySet.has(p.c)) continue;
        const row = grid[p.c];
        for (let k = 0; k < p.pr.length; k++) {
          const idx = p.pr[k];
          if (topProdSet.has(idx)) row[idx] = (row[idx] || 0) + 1;
        }
      }
      for (const pIdx of topProductIdx) {
        const productShare = gProduct[pIdx] / N;
        for (const cc of topCountries) {
          const cnt = grid[cc][pIdx] || 0;
          testAnomaly("product", cc, cnt, byCountry[cc].total, productShare,
            { country: cc, product: productList[pIdx] });
        }
      }
    }

    // Sort by score desc, dedupe so each country appears at most once
    // (we want geographic spread, not 5 gems all in the UK).
    candidates.sort((a, b) => b.score - a.score);
    const seenCountry = new Set();
    const out = [];
    for (const g of candidates) {
      if (seenCountry.has(g.country)) continue;
      seenCountry.add(g.country);
      out.push({
        kind: g.kind, country: g.country, count: g.count,
        expected: g.expected, lift: g.lift, filters: g.filters,
      });
      if (out.length >= 5) break;
    }
    return out;
  })();

  // ── Persist blobs to ShopAnalysisCache ──
  // Map blob FIRST so we can free the points array before stringify-ing
  // the LTV/journey/geo blobs. For Vollebak (~30k customers) this saved
  // ~50 MB of peak heap and prevented the OOM kill that caught the first
  // hourly cycle after the Customer Map deploy. (anon-rss 3.79 GB on a
  // 3 GB --max-old-space-size process — every byte matters.)
  let customerMapBlob = {
    points: customerMapPoints,
    thresholds: vipThresholds,
    highestRefundThreshold,
    productList,
    gems,
    computedAt: new Date().toISOString(),
  };
  await db.shopAnalysisCache.upsert({
    where: { shopDomain_cacheKey: { shopDomain, cacheKey: "customers:map" } },
    create: { shopDomain, cacheKey: "customers:map", payload: JSON.stringify(customerMapBlob) },
    update: { payload: JSON.stringify(customerMapBlob), computedAt: new Date() },
  });
  // Drop references and force GC before the other three big stringifies.
  customerMapBlob = null;
  customerMapPoints.length = 0;
  if (global.gc) global.gc();

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
    // Load both online-store AND POS orders. POS orders are only counted
    // below when a Meta-acquired customer is making a repeat purchase —
    // Order Explorer tags these as "Meta Repeat". Every other POS order
    // is dropped (no Meta relationship).
    db.order.findMany({
      where: { shopDomain },
      orderBy: { createdAt: "asc" },
      select: {
        shopifyOrderId: true, shopifyCustomerId: true, createdAt: true,
        frozenTotalPrice: true, totalRefunded: true,
        customerOrderCountAtPurchase: true,
        isOnlineStore: true,
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
    // Skip £0 orders (staff / replacement / warranty) from customer
    // rollup metrics so they don't inflate order counts or depress AOV.
    if ((order.frozenTotalPrice || 0) === 0) continue;

    const custSegment = segmentMap.get(custId) || "organic";
    const dateStr = shopLocalDayKey(tz, order.createdAt);
    const isFirstPurchase = order.customerOrderCountAtPurchase === 1;
    const revenue = order.frozenTotalPrice || 0;
    const refunded = order.totalRefunded || 0;

    let orderSegment;
    if (!order.isOnlineStore) {
      // POS / non-online orders only count when a Meta-acquired customer
      // is making a repeat purchase (Order Explorer's "Meta Repeat" tag).
      // Every other POS order has no Meta relationship — drop it.
      if (custSegment === "metaNew" && !isFirstPurchase) {
        orderSegment = "metaRepeat";
      } else {
        continue;
      }
    } else if (custSegment === "metaNew") {
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
