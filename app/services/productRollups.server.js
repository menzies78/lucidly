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
function tagOrder(order, attrByOrderId, metaAcquiredCustomers, customerFirstOrderMap) {
  const attr = attrByOrderId[order.shopifyOrderId];
  const custId = order.shopifyCustomerId;
  const isMetaOrder = !!attr || order.utmConfirmedMeta;
  if (!isMetaOrder) return "organic";
  if (custId && customerFirstOrderMap[custId]) {
    const isMetaAcquired = metaAcquiredCustomers.has(custId);
    if (isMetaAcquired) {
      const custFirstDate = customerFirstOrderMap[custId];
      const orderDate = order.createdAt.toISOString().split("T")[0];
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

  // Pull everything we need
  const [orders, attributions, customers] = await Promise.all([
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
  ]);

  // Index attributions by order id
  const attrByOrderId = {};
  for (const a of attributions) attrByOrderId[a.shopifyOrderId] = a;

  // Customer first-order-date map
  const customerFirstOrderMap = {};
  for (const c of customers) {
    if (c.firstOrderDate) {
      customerFirstOrderMap[c.shopifyCustomerId] = c.firstOrderDate.toISOString().split("T")[0];
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
      o => o.createdAt.toISOString().split("T")[0] === firstDate
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
    const rawItems = (order.lineItems || "").split(", ").map(s => s.trim()).filter(Boolean);
    if (rawItems.length === 0) continue;
    const parentItems = rawItems.map(toParentProduct);
    const revenuePerItem = (order.frozenTotalPrice || 0) / rawItems.length;
    const segment = tagOrder(order, attrByOrderId, metaAcquiredCustomers, customerFirstOrderMap);
    const dateStr = order.createdAt.toISOString().split("T")[0];
    const isFirstPurchase = order.customerOrderCountAtPurchase === 1;
    const attr = attrByOrderId[order.shopifyOrderId];
    const orderCollections = (order.productCollections || "").split(", ").map(s => s.trim()).filter(Boolean);

    // Per-product refund map
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

    for (const parentName of parentItems) {
      const b = ensure(dateStr, parentName, segment);
      b.orders++;
      b.items++;
      b.revenue += revenuePerItem;
      if (isFirstPurchase) {
        b.firstPurchases++;
        b.firstPurchaseRevenue += revenuePerItem;
      }
      for (const col of orderCollections) b.collections.add(col);
      if (attr?.metaCampaignName) b.campaigns[attr.metaCampaignName] = (b.campaigns[attr.metaCampaignName] || 0) + 1;
      if (attr?.metaAdSetName) b.adSets[attr.metaAdSetName] = (b.adSets[attr.metaAdSetName] || 0) + 1;
      const refund = refundMap[parentName];
      const isFullRefund = refund ? refund.refundedAmount >= refund.originalPrice * 0.50 : false;
      if (isFullRefund) {
        b.refundedOrders++;
        b.refundedAmount += refund.refundedAmount;
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
    orders, attrByOrderId, metaAcquiredCustomers,
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
function buildAnalysisBlob({ orders, attrByOrderId, metaAcquiredCustomers }) {
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
    const rawItems = (order.lineItems || "").split(", ").map(s => s.trim()).filter(Boolean);
    if (rawItems.length === 0) continue;
    const parentItems = rawItems.map(toParentProduct);
    const uniqueParents = [...new Set(parentItems)];
    const revenuePerItem = (order.frozenTotalPrice || 0) / rawItems.length;
    const attr = attrByOrderId[order.shopifyOrderId];
    const isMeta = !!attr || order.utmConfirmedMeta;
    const isFirstPurchase = order.customerOrderCountAtPurchase === 1;
    const dateStr = order.createdAt.toISOString().split("T")[0];

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
      for (const p of parentItems) {
        const target = isMeta ? metaFirstPurchaseProducts : nonMetaFirstPurchaseProducts;
        if (!target[p]) target[p] = { qty: 0, revenue: 0 };
        target[p].qty++;
        target[p].revenue += revenuePerItem;
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
    metaFirstPurchaseList,
    nonMetaFirstPurchaseList,
    metaJourney: buildJourney(metaAcquiredCustomers),
    allJourney: buildJourney(Object.keys(ordersByCustomer)),
    dailyBasketStats,
    dailyRefundByProduct,
  };
}
