/**
 * Product rollup builder.
 *
 * Turns raw Order + Attribution rows into precomputed
 * (shop, date, product, segment) rows so the Products loader can
 * render any date range in <100ms instead of reparsing line items
 * on every request.
 *
 * Also builds the "analysis cache" blob containing basket combos,
 * journey flows, first-purchase lists, add-ons and the
 * metaAcquiredCustomers set — stuff that doesn't normalize cleanly
 * and needs to be precomputed per shop.
 *
 * Call sites:
 *   - scripts/backfillRollups.js (one-time full rebuild)
 *   - incrementalSync.server.js (incremental rebuild per touched date)
 */

import db from "../db.server";
import { shopLocalDayKey } from "../utils/shopTime.server";

// ── Variant stripping (mirror of app.products.tsx) ──
const COLORS = new Set([
  "black", "cream", "grey", "blue", "white", "red", "oyster", "pink",
  "chartreuse", "multi", "rose", "camel", "navy", "lilac", "magenta",
  "natural", "ecru", "green", "brown", "khaki", "orange", "yellow",
  "teal", "coral", "ivory", "taupe", "beige", "stone", "tan", "nude",
  "gold", "silver", "burgundy", "terracotta", "olive",
]);

export function toParentProduct(name) {
  const parts = (name || "").trim().split(" ");
  if (parts.length <= 1) return (name || "").trim();
  if (parts.length >= 3 && parts[parts.length - 3]?.toLowerCase() === "acid" && parts[parts.length - 2]?.toLowerCase() === "wash") {
    if (COLORS.has(parts[parts.length - 1].toLowerCase())) return parts.slice(0, -3).join(" ");
    return parts.slice(0, -2).join(" ");
  }
  if (parts.length >= 2 && parts[parts.length - 2]?.toLowerCase() === "acid" && parts[parts.length - 1]?.toLowerCase() === "wash") {
    return parts.slice(0, -2).join(" ");
  }
  if (COLORS.has(parts[parts.length - 1].toLowerCase())) return parts.slice(0, -1).join(" ");
  return name.trim();
}

/**
 * Build per-order "segment" tag. Mirrors app.products.tsx getOrderTag.
 * Requires: attrByOrderId map, metaAcquiredCustomers set, customer firstOrderDate map.
 */
function tagOrder(order, attrByOrderId, metaAcquiredCustomers, customerFirstOrderMap, tz) {
  const attr = attrByOrderId[order.shopifyOrderId];
  const custId = order.shopifyCustomerId;
  const isMetaOrder = !!attr || order.utmConfirmedMeta;
  if (!isMetaOrder) return "organic";
  if (custId && customerFirstOrderMap[custId]) {
    const isMetaAcquired = metaAcquiredCustomers.has(custId);
    if (isMetaAcquired) {
      const custFirstDate = customerFirstOrderMap[custId];
      const orderDate = shopLocalDayKey(tz, order.createdAt);
      return custFirstDate === orderDate ? "metaNew" : "metaRepeat";
    }
    return "metaRetargeted";
  }
  return "metaNew";
}

/**
 * Full rebuild of the product rollup table for a shop.
 *
 * Reads all online orders + attributions once, iterates line items,
 * groups into (date, product, segment) buckets, writes to DB.
 */
export async function rebuildProductRollups(shopDomain) {
  const t0 = Date.now();
  console.log(`[productRollups] rebuilding for ${shopDomain}…`);

  const shopRow = await db.shop.findUnique({ where: { shopDomain }, select: { shopifyTimezone: true } });
  const tz = shopRow?.shopifyTimezone || "UTC";

  // Pull everything we need
  const [orders, attributions, customers, lineItemRowsRaw] = await Promise.all([
    db.order.findMany({
      where: { shopDomain, isOnlineStore: true },
      orderBy: { createdAt: "asc" },
      select: {
        shopifyOrderId: true, shopifyCustomerId: true, createdAt: true,
        frozenTotalPrice: true, lineItems: true, refundLineItems: true,
        productCollections: true, customerOrderCountAtPurchase: true,
        utmConfirmedMeta: true,
      },
    }),
    db.attribution.findMany({
      where: { shopDomain, confidence: { gt: 0 } },
      select: {
        shopifyOrderId: true, metaCampaignName: true, metaAdSetName: true,
      },
    }),
    db.customer.findMany({
      where: { shopDomain },
      select: { shopifyCustomerId: true, firstOrderDate: true },
    }),
    // Structured per-line revenue rows. Populated by orderSync / orderWebhook.
    // Orders without rows (pre-migration, or still mid-backfill) fall back to
    // even-split revenue below so the rollup never goes dark during rollout.
    db.orderLineItem.findMany({
      where: { shopDomain },
      select: {
        shopifyOrderId: true, title: true, quantity: true,
        totalPrice: true, refundedAmount: true,
      },
    }),
  ]);

  // Group line-item rows by order. Each order has 0..N rows; an empty array
  // means "no structured data — use the legacy even-split path".
  const lineItemsByOrder = {};
  for (const r of lineItemRowsRaw) {
    (lineItemsByOrder[r.shopifyOrderId] ||= []).push(r);
  }

  // Index attributions by order id
  const attrByOrderId = {};
  for (const a of attributions) attrByOrderId[a.shopifyOrderId] = a;

  // Customer first-order-date map (shop-local)
  const customerFirstOrderMap = {};
  for (const c of customers) {
    if (c.firstOrderDate) {
      customerFirstOrderMap[c.shopifyCustomerId] = shopLocalDayKey(tz, c.firstOrderDate);
    }
  }

  // Meta-acquired customers: customer's first order was a Meta order.
  // O(customers) via customer first-order lookup against Meta orders.
  // (Previous implementation was O(customers × orders), hence slow.)
  const matchedOrderIds = new Set(attributions.map(a => a.shopifyOrderId));
  const utmConfirmedOrderIds = new Set(
    orders.filter(o => o.utmConfirmedMeta).map(o => o.shopifyOrderId)
  );
  // Group orders by customer for fast first-order lookup
  const ordersByCustomer = {};
  for (const o of orders) {
    if (!o.shopifyCustomerId) continue;
    (ordersByCustomer[o.shopifyCustomerId] ||= []).push(o);
  }
  const metaAcquiredCustomers = new Set();
  for (const custId of Object.keys(customerFirstOrderMap)) {
    const custOrders = ordersByCustomer[custId];
    if (!custOrders || custOrders.length === 0) continue;
    const firstDate = customerFirstOrderMap[custId];
    // Find the first order on that date
    const firstOrder = custOrders.find(
      o => shopLocalDayKey(tz, o.createdAt) === firstDate
    );
    if (!firstOrder) continue;
    // Ground truth: only count as "meta-acquired new customer" if Shopify confirms
    // this is genuinely their first-ever order (not just first in our DB)
    if (firstOrder.customerOrderCountAtPurchase !== 1) continue;
    if (matchedOrderIds.has(firstOrder.shopifyOrderId) || utmConfirmedOrderIds.has(firstOrder.shopifyOrderId)) {
      metaAcquiredCustomers.add(custId);
    }
  }

  // ── Build rollup buckets ──
  // Key: `${dateStr}|${product}|${segment}`
  const buckets = new Map();

  function ensure(dateStr, product, segment) {
    const key = `${dateStr}|${product}|${segment}`;
    let b = buckets.get(key);
    if (!b) {
      b = {
        dateStr, product, segment,
        orders: 0, items: 0, revenue: 0,
        refundedOrders: 0, refundedAmount: 0,
        firstPurchases: 0, firstPurchaseRevenue: 0,
        campaigns: {},  // campaignName → count
        adSets: {},     // adSetName → count
        collections: new Set(),
      };
      buckets.set(key, b);
    }
    return b;
  }

  for (const order of orders) {
    // Skip £0 orders (staff / replacement / warranty) from product metrics
    // so they don't inflate order counts and drag down per-product AOV.
    if ((order.frozenTotalPrice || 0) === 0) continue;

    const segment = tagOrder(order, attrByOrderId, metaAcquiredCustomers, customerFirstOrderMap, tz);
    const dateStr = shopLocalDayKey(tz, order.createdAt);
    const isFirstPurchase = order.customerOrderCountAtPurchase === 1;
    const attr = attrByOrderId[order.shopifyOrderId];
    const orderCollections = (order.productCollections || "").split(", ").map(s => s.trim()).filter(Boolean);

    // Build the list of (parentProduct, revenue, refundedAmount, qty) tuples
    // for this order. Prefer the structured OrderLineItem rows (real per-line
    // revenue). Fall back to even-split across the comma-separated titles for
    // orders that predate the OrderLineItem backfill.
    const structuredRows = lineItemsByOrder[order.shopifyOrderId];
    let perProduct;
    if (structuredRows && structuredRows.length > 0) {
      // Structured path: sum per parent-product across the row list so a
      // product that appears on multiple lines (e.g. different variants)
      // collapses into a single bucket entry for the rollup. Quantity counts
      // the distinct line-item rows that mention the parent product — this
      // matches the legacy semantics where `b.items` was the line count,
      // not the qty sold, so repeat-product tiles stay comparable.
      const byParent = {};
      for (const r of structuredRows) {
        const parent = toParentProduct(r.title);
        if (!parent) continue;
        if (!byParent[parent]) {
          byParent[parent] = { revenue: 0, refundedAmount: 0, lines: 0 };
        }
        byParent[parent].revenue += r.totalPrice || 0;
        byParent[parent].refundedAmount += r.refundedAmount || 0;
        byParent[parent].lines += 1;
      }
      perProduct = Object.entries(byParent).map(([parent, v]) => ({
        parent,
        revenue: v.revenue,
        refundedAmount: v.refundedAmount,
        lines: v.lines,
      }));
      if (perProduct.length === 0) continue;
    } else {
      // Legacy even-split fallback — kept so the rollup keeps working on
      // orders that haven't been re-synced into the new table yet.
      const rawItems = (order.lineItems || "").split(", ").map(s => s.trim()).filter(Boolean);
      if (rawItems.length === 0) continue;
      const parentItems = rawItems.map(toParentProduct);
      const revenuePerItem = (order.frozenTotalPrice || 0) / rawItems.length;
      // Per-product refund map from the legacy JSON blob.
      const refundMap = {};
      if (order.refundLineItems) {
        try {
          const parsed = JSON.parse(order.refundLineItems);
          for (const rli of parsed) {
            const pt = toParentProduct(rli.title);
            if (!refundMap[pt]) refundMap[pt] = { refundedAmount: 0, originalPrice: 0 };
            refundMap[pt].refundedAmount += rli.refundedAmount;
            refundMap[pt].originalPrice += rli.originalPrice;
          }
        } catch {}
      }
      perProduct = parentItems.map(parent => {
        const refund = refundMap[parent];
        const isFullRefund = refund ? refund.refundedAmount >= refund.originalPrice * 0.50 : false;
        return {
          parent,
          revenue: revenuePerItem,
          // In the legacy path `refundedAmount` was only counted when a full
          // (>=50%) refund was detected. Preserve that behaviour so numbers
          // on unbackfilled orders don't shift underneath the rollup.
          refundedAmount: isFullRefund ? refund.refundedAmount : 0,
          lines: 1,
          isFullRefund,
        };
      });
    }

    for (const p of perProduct) {
      const b = ensure(dateStr, p.parent, segment);
      b.orders++;
      b.items += p.lines;
      b.revenue += p.revenue;
      if (isFirstPurchase) {
        b.firstPurchases++;
        b.firstPurchaseRevenue += p.revenue;
      }
      for (const col of orderCollections) b.collections.add(col);
      if (attr?.metaCampaignName) b.campaigns[attr.metaCampaignName] = (b.campaigns[attr.metaCampaignName] || 0) + 1;
      if (attr?.metaAdSetName) b.adSets[attr.metaAdSetName] = (b.adSets[attr.metaAdSetName] || 0) + 1;
      // Structured path: any refundedAmount counts (fractional refunds too).
      // Legacy path: only tagged when isFullRefund. Matches the existing
      // >=50% threshold for the refundedOrders count.
      if (p.refundedAmount > 0) {
        if (p.isFullRefund === undefined || p.isFullRefund) b.refundedOrders++;
        b.refundedAmount += p.refundedAmount;
      }
    }
  }

  // ── Wipe & replace (simple, safe, ~thousands of rows) ──
  await db.dailyProductRollup.deleteMany({ where: { shopDomain } });

  const rows = [];
  for (const b of buckets.values()) {
    rows.push({
      shopDomain,
      date: new Date(`${b.dateStr}T00:00:00.000Z`),
      product: b.product,
      segment: b.segment,
      orders: b.orders,
      items: b.items,
      revenue: b.revenue,
      refundedOrders: b.refundedOrders,
      refundedAmount: b.refundedAmount,
      firstPurchases: b.firstPurchases,
      firstPurchaseRevenue: b.firstPurchaseRevenue,
      topCampaignJson: JSON.stringify(b.campaigns),
      topAdSetJson: JSON.stringify(b.adSets),
      collections: [...b.collections].join(", "),
    });
  }

  // Chunked createMany (SQLite has param limits)
  const CHUNK = 500;
  for (let i = 0; i < rows.length; i += CHUNK) {
    await db.dailyProductRollup.createMany({ data: rows.slice(i, i + CHUNK) });
  }

  // ── Build the analysis cache blob ──
  // Contains: journey flows, basket combos, add-ons, first-purchase lists,
  // metaAcquiredCustomers — all the stuff that doesn't fit a date rollup.
  // Computed once per shop, cached until the next sync.
  const analysisBlob = buildAnalysisBlob({
    orders, attrByOrderId, metaAcquiredCustomers, tz, lineItemsByOrder,
  });

  await db.shopAnalysisCache.upsert({
    where: { shopDomain_cacheKey: { shopDomain, cacheKey: "products:analysis" } },
    create: {
      shopDomain,
      cacheKey: "products:analysis",
      payload: JSON.stringify(analysisBlob),
    },
    update: {
      payload: JSON.stringify(analysisBlob),
      computedAt: new Date(),
    },
  });

  console.log(`[productRollups] ${shopDomain}: ${rows.length} rollup rows + analysis blob in ${Date.now() - t0}ms`);
  return { rollupRows: rows.length, ms: Date.now() - t0 };
}

/**
 * Build the analysis blob: journey flows, basket combos, first-purchase
 * lists, add-ons, metaAcquiredCustomers. All-time precomputation.
 */
function buildAnalysisBlob({ orders, attrByOrderId, metaAcquiredCustomers, tz, lineItemsByOrder = {} }) {
  const metaBaskets = [];
  const nonMetaBaskets = [];
  const metaFirstPurchaseProducts = {};
  const nonMetaFirstPurchaseProducts = {};
  const ordersByCustomer = {};
  // Per-date order-level stats (for basket metrics across any window)
  const dailyBasketStats = {}; // date → { totalOrders, metaOrders, totalItems, metaItems }
  // Per-date per-product refund data (for prev-period highestRefund)
  const dailyRefundByProduct = {}; // date → product → { total, refunded }

  for (const order of orders) {
    // Build parent-item list + per-parent revenue. Prefer the structured
    // OrderLineItem rows (real discounted-unit-price × qty per line); fall
    // back to the legacy even-split over the comma-separated titles for
    // orders that predate the line-item backfill.
    const structuredRows = lineItemsByOrder[order.shopifyOrderId];
    let parentItems; // array of parent names (may contain duplicates — basket semantics)
    let revenueByParent; // parent → allocated revenue for first-purchase totals
    if (structuredRows && structuredRows.length > 0) {
      parentItems = [];
      revenueByParent = {};
      for (const r of structuredRows) {
        const parent = toParentProduct(r.title);
        if (!parent) continue;
        parentItems.push(parent);
        revenueByParent[parent] = (revenueByParent[parent] || 0) + (r.totalPrice || 0);
      }
      if (parentItems.length === 0) continue;
    } else {
      const rawItems = (order.lineItems || "").split(", ").map(s => s.trim()).filter(Boolean);
      if (rawItems.length === 0) continue;
      parentItems = rawItems.map(toParentProduct);
      const revenuePerItem = (order.frozenTotalPrice || 0) / rawItems.length;
      revenueByParent = null; // signal legacy path below
      // legacy revenue allocation handled inline in firstPurchase loop
      // to preserve prior semantics (per-appearance share, not per-parent sum).
      parentItems.__legacyPerItem = revenuePerItem;
    }
    const uniqueParents = [...new Set(parentItems)];
    const attr = attrByOrderId[order.shopifyOrderId];
    const isMeta = !!attr || order.utmConfirmedMeta;
    const isFirstPurchase = order.customerOrderCountAtPurchase === 1;
    const dateStr = shopLocalDayKey(tz, order.createdAt);

    // Daily basket stats (distinct orders, not line items)
    if (!dailyBasketStats[dateStr]) dailyBasketStats[dateStr] = { totalOrders: 0, metaOrders: 0, totalItems: 0, metaItems: 0 };
    dailyBasketStats[dateStr].totalOrders++;
    dailyBasketStats[dateStr].totalItems += parentItems.length;
    if (isMeta) {
      dailyBasketStats[dateStr].metaOrders++;
      dailyBasketStats[dateStr].metaItems += parentItems.length;
    }

    // Daily refund data per product (for historical highestRefund comparison)
    const refundMap = {};
    if (order.refundLineItems) {
      try {
        const parsed = JSON.parse(order.refundLineItems);
        for (const rli of parsed) {
          const pt = toParentProduct(rli.title);
          if (!refundMap[pt]) refundMap[pt] = { refundedAmount: 0, originalPrice: 0 };
          refundMap[pt].refundedAmount += rli.refundedAmount;
          refundMap[pt].originalPrice += rli.originalPrice;
        }
      } catch {}
    }
    if (!dailyRefundByProduct[dateStr]) dailyRefundByProduct[dateStr] = {};
    for (const p of parentItems) {
      if (!dailyRefundByProduct[dateStr][p]) dailyRefundByProduct[dateStr][p] = { total: 0, refunded: 0 };
      dailyRefundByProduct[dateStr][p].total++;
      const rf = refundMap[p];
      if (rf && rf.refundedAmount >= rf.originalPrice * 0.50) dailyRefundByProduct[dateStr][p].refunded++;
    }

    if (uniqueParents.length >= 2) {
      if (isMeta) metaBaskets.push(uniqueParents);
      else nonMetaBaskets.push(uniqueParents);
    }

    if (isFirstPurchase) {
      const target = isMeta ? metaFirstPurchaseProducts : nonMetaFirstPurchaseProducts;
      if (revenueByParent) {
        // Structured path: count one qty per appearance (basket semantics),
        // but revenue is added once per unique parent to avoid double-counting
        // the summed line revenue when the same parent appears on multiple
        // variant lines.
        const seen = new Set();
        for (const p of parentItems) {
          if (!target[p]) target[p] = { qty: 0, revenue: 0 };
          target[p].qty++;
          if (!seen.has(p)) {
            target[p].revenue += revenueByParent[p] || 0;
            seen.add(p);
          }
        }
      } else {
        const revenuePerItem = parentItems.__legacyPerItem || 0;
        for (const p of parentItems) {
          if (!target[p]) target[p] = { qty: 0, revenue: 0 };
          target[p].qty++;
          target[p].revenue += revenuePerItem;
        }
      }
    }

    if (order.shopifyCustomerId) {
      (ordersByCustomer[order.shopifyCustomerId] ||= []).push({
        createdAt: order.createdAt.toISOString(),
        lineItems: order.lineItems || "",
      });
    }
  }

  // Basket combos
  const buildCombos = (baskets) => {
    const combos = {};
    for (const basket of baskets) {
      const sorted = [...basket].sort();
      for (let i = 0; i < sorted.length; i++) {
        for (let j = i + 1; j < sorted.length; j++) {
          const key = `${sorted[i]}|||${sorted[j]}`;
          if (!combos[key]) combos[key] = { product1: sorted[i], product2: sorted[j], count: 0 };
          combos[key].count++;
        }
      }
    }
    return Object.values(combos).sort((a, b) => b.count - a.count).slice(0, 15);
  };

  // Add-ons
  const buildAddons = (baskets) => {
    const counts = {};
    for (const basket of baskets) {
      for (const product of basket) {
        if (!counts[product]) counts[product] = { product, appearances: 0 };
        counts[product].appearances++;
      }
    }
    return Object.values(counts)
      .filter(a => a.appearances >= 2)
      .sort((a, b) => b.appearances - a.appearances)
      .slice(0, 20);
  };

  // Journey flows
  const buildJourney = (customerIds) => {
    const jFlows = {};
    const gProducts = {};
    const sProducts = {};
    for (const custId of customerIds) {
      const custOrders = ordersByCustomer[custId];
      if (!custOrders || custOrders.length < 2) continue;
      custOrders.sort((a, b) => a.createdAt.localeCompare(b.createdAt));
      const firstItems = (custOrders[0].lineItems || "").split(", ").map(s => toParentProduct(s.trim())).filter(Boolean);
      const secondItems = (custOrders[1].lineItems || "").split(", ").map(s => toParentProduct(s.trim())).filter(Boolean);
      const firstSet = new Set(firstItems);
      const differentSecondItems = secondItems.filter(item => !firstSet.has(item));
      if (differentSecondItems.length === 0) continue;
      for (const item of firstItems) gProducts[item] = (gProducts[item] || 0) + 1;
      for (const item of differentSecondItems) sProducts[item] = (sProducts[item] || 0) + 1;
      for (const first of firstItems) {
        if (!jFlows[first]) jFlows[first] = {};
        for (const second of differentSecondItems) {
          jFlows[first][second] = (jFlows[first][second] || 0) + 1;
        }
      }
    }
    const sortedG = Object.entries(gProducts).sort((a, b) => b[1] - a[1]);
    const sortedS = Object.entries(sProducts).sort((a, b) => b[1] - a[1]);
    const gThreshold = sortedG.length > 0 ? Math.max(2, Math.ceil(sortedG[0][1] * 0.1)) : 2;
    const sThreshold = sortedS.length > 0 ? Math.max(2, Math.ceil(sortedS[0][1] * 0.1)) : 2;
    const tGateway = sortedG.filter(([, c]) => c >= gThreshold).slice(0, 10);
    const tSecond = sortedS.filter(([, c]) => c >= sThreshold).slice(0, 10);
    const tGatewayNames = new Set(tGateway.map(g => g[0]));
    const tSecondNames = new Set(tSecond.map(g => g[0]));
    const fArr = [];
    for (const [from, tos] of Object.entries(jFlows)) {
      if (!tGatewayNames.has(from)) continue;
      for (const [to, count] of Object.entries(tos)) {
        if (!tSecondNames.has(to)) continue;
        fArr.push({ from, to, count });
      }
    }
    return { topGateway: tGateway, topSecond: tSecond, flows: fArr };
  };

  const metaFirstPurchaseList = Object.entries(metaFirstPurchaseProducts)
    .map(([product, data]) => ({ product, qty: data.qty, revenue: Math.round(data.revenue * 100) / 100 }))
    .sort((a, b) => b.qty - a.qty).slice(0, 20);
  const nonMetaFirstPurchaseList = Object.entries(nonMetaFirstPurchaseProducts)
    .map(([product, data]) => ({ product, qty: data.qty, revenue: Math.round(data.revenue * 100) / 100 }))
    .sort((a, b) => b.qty - a.qty).slice(0, 20);

  return {
    metaAcquiredCustomers: [...metaAcquiredCustomers],
    metaCombos: buildCombos(metaBaskets),
    nonMetaCombos: buildCombos(nonMetaBaskets),
    topAddons: buildAddons([...metaBaskets, ...nonMetaBaskets]),
    topAddonsMeta: buildAddons(metaBaskets),
    // Total multi-product baskets — denominators for the add-on share %
    // shown in the Top Add-ons tile. Without these the loader was dividing
    // all-time appearances by per-period order counts, producing values
    // like 5,000% on the UI.
    addonAllBaskets: metaBaskets.length + nonMetaBaskets.length,
    addonMetaBaskets: metaBaskets.length,
    metaFirstPurchaseList,
    nonMetaFirstPurchaseList,
    metaJourney: buildJourney(metaAcquiredCustomers),
    allJourney: buildJourney(Object.keys(ordersByCustomer)),
    dailyBasketStats,
    dailyRefundByProduct,
  };
}
