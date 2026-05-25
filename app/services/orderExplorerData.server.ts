// Order Explorer data builder.
//
// Extracted from app/routes/app.orders.tsx so the same table can be embedded
// inside Customers tab (where Order Explorer now lives) without duplicating
// the row-tagging + summary logic. Backed by queryCache so calling this from
// multiple loaders does not double-fetch.

import db from "../db.server";
import { shopLocalDayKey } from "../utils/shopTime.server";
import { currencySymbolFromCode } from "../utils/currency";
import { cached as queryCached, DEFAULT_TTL } from "./queryCache.server";

export type OrderExplorerArgs = {
  shopDomain: string;
  fromDate: Date;
  toDate: Date;
  fromKey: string;
  toKey: string;
  tz: string;
  shopifyCurrency: string | null | undefined;
  tagFilter: string;
  campaignFilter: string;
};

function buildUtmString(o: any) {
  const parts: string[] = [];
  if (o.utmSource) parts.push(`utm_source=${o.utmSource}`);
  if (o.utmMedium) parts.push(`utm_medium=${o.utmMedium}`);
  if (o.utmCampaign) parts.push(`utm_campaign=${o.utmCampaign}`);
  if (o.utmContent) parts.push(`utm_content=${o.utmContent}`);
  if (o.utmTerm) parts.push(`utm_term=${o.utmTerm}`);
  if (o.utmId) parts.push(`utm_id=${o.utmId}`);
  return parts.join("&");
}

export async function buildOrderExplorerData(args: OrderExplorerArgs) {
  const { shopDomain, fromDate, toDate, fromKey, toKey, tz, shopifyCurrency, tagFilter, campaignFilter } = args;

  const [orders, customers] = await Promise.all([
    queryCached(`${shopDomain}:ordersExplorer:${fromKey}:${toKey}`, DEFAULT_TTL, () =>
      db.order.findMany({
        where: { shopDomain, createdAt: { gte: fromDate, lte: toDate } },
        orderBy: { createdAt: "desc" },
      }),
    ),
    queryCached(`${shopDomain}:ordersCustomers`, DEFAULT_TTL, () =>
      db.customer.findMany({
        where: { shopDomain },
        select: { shopifyCustomerId: true, firstOrderDate: true, metaSegment: true },
      }),
    ),
  ]);

  const orderIdsInRange = orders.map((o: any) => o.shopifyOrderId);
  const [attributions, metaInsights] = await Promise.all([
    queryCached(`${shopDomain}:ordersAttrs:${fromKey}:${toKey}`, DEFAULT_TTL, () =>
      db.attribution.findMany({
        where: {
          shopDomain,
          OR: [
            { shopifyOrderId: { in: orderIdsInRange } },
            { confidence: 0, shopifyOrderId: { startsWith: "unmatched_" } },
          ],
        },
        orderBy: { matchedAt: "desc" },
      }),
    ),
    queryCached(`${shopDomain}:ordersInsights:${fromKey}:${toKey}`, DEFAULT_TTL, () =>
      db.metaInsight.findMany({
        where: { shopDomain, conversions: { gt: 0 }, date: { gte: fromDate, lte: toDate } },
      }),
    ),
  ]);

  const orderMap: Record<string, any> = {};
  for (const o of orders) orderMap[o.shopifyOrderId] = o;

  const metaValueByAdDay: Record<string, number> = {};
  for (const ins of metaInsights) {
    const key = `${ins.adId}_${shopLocalDayKey(tz, ins.date)}`;
    metaValueByAdDay[key] = (metaValueByAdDay[key] || 0) + ins.conversionValue;
  }
  const shopifyValueByAdDay: Record<string, number> = {};
  const attrGroupKeys: Record<string, string> = {};
  for (const attr of attributions) {
    if (attr.confidence === 0 || !attr.metaAdId) continue;
    const order = orderMap[attr.shopifyOrderId];
    if (!order) continue;
    const orderDate = shopLocalDayKey(tz, order.createdAt);
    const key = `${attr.metaAdId}_${orderDate}`;
    shopifyValueByAdDay[key] = (shopifyValueByAdDay[key] || 0) + (order.frozenTotalPrice || 0);
    attrGroupKeys[attr.shopifyOrderId] = key;
  }
  const differenceByGroup: Record<string, number | null> = {};
  for (const key of Object.keys(shopifyValueByAdDay)) {
    const metaVal = metaValueByAdDay[key] || 0;
    const shopVal = shopifyValueByAdDay[key] || 0;
    differenceByGroup[key] = metaVal > 0 ? Math.round(((shopVal - metaVal) / metaVal) * 100) : null;
  }

  const customerMap: Record<string, any> = {};
  for (const c of customers) customerMap[c.shopifyCustomerId] = c;

  const metaAcquiredCustomers = new Set<string>();
  for (const c of customers) {
    if (c.metaSegment === "metaNew") metaAcquiredCustomers.add(c.shopifyCustomerId);
  }

  const rows: any[] = [];
  const processedOrderIds = new Set<string>();

  // 2a: Matched attributions
  for (const attr of attributions) {
    if (attr.confidence === 0) continue;
    const order = orderMap[attr.shopifyOrderId];
    if (!order) continue;
    processedOrderIds.add(order.shopifyOrderId);

    const custId = order.shopifyCustomerId;
    const customer = custId ? customerMap[custId] : null;
    let tag = "Meta New";

    if (custId && customer) {
      const isMetaAcquired = metaAcquiredCustomers.has(custId);
      if (isMetaAcquired) {
        const isFirst = order.customerOrderCountAtPurchase != null
          ? order.customerOrderCountAtPurchase === 1
          : shopLocalDayKey(tz, order.createdAt) === (customer.firstOrderDate ? shopLocalDayKey(tz, customer.firstOrderDate) : "");
        tag = isFirst ? "Meta New" : "Meta Repeat";
      } else {
        tag = "Meta Retargeted";
      }
    }

    if (tagFilter !== "all" && tagFilter !== "meta" && tag !== tagFilter) continue;
    if (campaignFilter !== "all" && attr.metaCampaignName !== campaignFilter) continue;

    const customerName = order.customerFirstName
      ? `${order.customerFirstName} ${order.customerLastInitial || ""}`.trim() : "";
    const groupKey = attrGroupKeys[attr.shopifyOrderId];
    const difference = groupKey ? (differenceByGroup[groupKey] ?? null) : null;
    const rev = order.frozenTotalPrice || 0;
    const refunded = order.totalRefunded || 0;

    rows.push({
      date: shopLocalDayKey(tz, order.createdAt),
      createdAtISO: order.createdAt.toISOString(),
      orderNumber: order.orderNumber || order.shopifyOrderId,
      country: order.country || "", city: order.city || "",
      customerName, orderCount: order.customerOrderCountAtPurchase,
      campaign: attr.metaCampaignName || "", adSet: attr.metaAdSetName || "",
      adName: attr.metaAdName || "",
      lineItems: order.lineItems || "", productSkus: order.productSkus || "",
      productCollections: order.productCollections || "",
      discountCodes: order.discountCodes || "",
      refundStatus: order.refundStatus || "none",
      totalRefunded: refunded,
      revenue: rev,
      netRevenue: Math.round((rev - refunded) * 100) / 100,
      difference, tag, confidence: attr.confidence, method: attr.matchMethod || "",
      attributionSource: order.utmConfirmedMeta ? "UTM & Lucidly" : "Lucidly",
      utm: buildUtmString(order),
    });
  }

  // 2b: Unattributed Meta conversions
  for (const attr of attributions) {
    if (attr.confidence !== 0) continue;
    if (tagFilter !== "all" && tagFilter !== "meta" && tagFilter !== "Unattributed") continue;
    if (campaignFilter !== "all" && attr.metaCampaignName !== campaignFilter) continue;
    const parts = attr.shopifyOrderId.split("_");
    const extractedDate = parts.length >= 3 ? parts[2] : shopLocalDayKey(tz, attr.matchedAt);
    if (extractedDate < fromKey || extractedDate > toKey) continue;
    rows.push({
      date: extractedDate, createdAtISO: "",
      orderNumber: "", country: "", city: "",
      customerName: "", orderCount: null,
      campaign: attr.metaCampaignName || "", adSet: attr.metaAdSetName || "",
      adName: attr.metaAdName || "",
      lineItems: "", productSkus: "", productCollections: "",
      discountCodes: "", refundStatus: "none", totalRefunded: 0,
      revenue: attr.metaConversionValue || 0,
      netRevenue: attr.metaConversionValue || 0,
      difference: null,
      tag: "Unattributed", confidence: 0, method: attr.matchMethod || "",
      attributionSource: "Unattributed", utm: "",
    });
  }

  // 2b-ii: UTM-only Meta orders
  for (const order of orders) {
    if (processedOrderIds.has(order.shopifyOrderId)) continue;
    if (!order.utmConfirmedMeta) continue;
    processedOrderIds.add(order.shopifyOrderId);

    const custId = order.shopifyCustomerId;
    const customer = custId ? customerMap[custId] : null;
    let tag = "Meta Unmatched New";

    if (custId && customer) {
      const isMetaAcquired = metaAcquiredCustomers.has(custId);
      if (isMetaAcquired) {
        const isFirst = order.customerOrderCountAtPurchase != null
          ? order.customerOrderCountAtPurchase === 1
          : shopLocalDayKey(tz, order.createdAt) === (customer.firstOrderDate ? shopLocalDayKey(tz, customer.firstOrderDate) : "");
        tag = isFirst ? "Meta Unmatched New" : "Meta Unmatched Repeat";
      } else {
        tag = "Meta Unmatched Retargeted";
      }
    }

    if (tagFilter !== "all" && tagFilter !== "meta") {
      if (tagFilter === "Meta Unmatched") {
        if (!tag.startsWith("Meta Unmatched")) continue;
      } else if (tag !== tagFilter) continue;
    }
    if (campaignFilter !== "all" && (order.metaCampaignName || "") !== campaignFilter) continue;

    const customerName = order.customerFirstName
      ? `${order.customerFirstName} ${order.customerLastInitial || ""}`.trim() : "";
    const rev = order.frozenTotalPrice || 0;
    const refunded = order.totalRefunded || 0;

    rows.push({
      date: shopLocalDayKey(tz, order.createdAt),
      createdAtISO: order.createdAt.toISOString(),
      orderNumber: order.orderNumber || order.shopifyOrderId,
      country: order.country || "", city: order.city || "",
      customerName, orderCount: order.customerOrderCountAtPurchase,
      campaign: order.metaCampaignName || order.utmCampaign || "",
      adSet: order.metaAdSetName || order.utmTerm || "",
      adName: order.metaAdName || order.utmContent || "",
      lineItems: order.lineItems || "", productSkus: order.productSkus || "",
      productCollections: order.productCollections || "",
      discountCodes: order.discountCodes || "",
      refundStatus: order.refundStatus || "none",
      totalRefunded: refunded,
      revenue: rev,
      netRevenue: Math.round((rev - refunded) * 100) / 100,
      difference: null, tag, confidence: null, method: "utm",
      attributionSource: "UTM", utm: buildUtmString(order),
    });
  }

  // 2c: Remaining orders - Meta Repeat, Non-Meta, or Non-Meta POS
  for (const order of orders) {
    if (processedOrderIds.has(order.shopifyOrderId)) continue;

    const custId = order.shopifyCustomerId;
    const customer = custId ? customerMap[custId] : null;
    const isPOS = !order.isOnlineStore;
    let tag = isPOS ? "Non-Meta POS" : "Non-Meta";

    if (custId && metaAcquiredCustomers.has(custId) && customer) {
      const isFirst = order.customerOrderCountAtPurchase != null
        ? order.customerOrderCountAtPurchase === 1
        : shopLocalDayKey(tz, order.createdAt) === (customer.firstOrderDate ? shopLocalDayKey(tz, customer.firstOrderDate) : "");
      if (!isFirst) tag = "Meta Repeat";
    }

    if (tagFilter === "meta" && tag !== "Meta Repeat") continue;
    if (tagFilter !== "all" && tagFilter !== "meta" && tag !== tagFilter) continue;
    if (campaignFilter !== "all" && (tag === "Non-Meta" || tag === "Non-Meta POS")) continue;
    if (campaignFilter !== "all" && tag === "Meta Repeat") continue;

    const customerName = order.customerFirstName
      ? `${order.customerFirstName} ${order.customerLastInitial || ""}`.trim() : "";
    const rev = order.frozenTotalPrice || 0;
    const refunded = order.totalRefunded || 0;

    rows.push({
      date: shopLocalDayKey(tz, order.createdAt),
      createdAtISO: order.createdAt.toISOString(),
      orderNumber: order.orderNumber || order.shopifyOrderId,
      country: order.country || "", city: order.city || "",
      customerName, orderCount: order.customerOrderCountAtPurchase,
      campaign: "", adSet: "",
      adName: tag === "Meta Repeat" && isPOS ? "(POS repeat)" : "",
      lineItems: order.lineItems || "", productSkus: order.productSkus || "",
      productCollections: order.productCollections || "",
      discountCodes: order.discountCodes || "",
      refundStatus: order.refundStatus || "none",
      totalRefunded: refunded,
      revenue: rev,
      netRevenue: Math.round((rev - refunded) * 100) / 100,
      difference: null, tag,
      confidence: null, method: "",
      attributionSource: "Unattributed", utm: buildUtmString(order),
    });
  }

  rows.sort((a, b) => b.date.localeCompare(a.date) || b.revenue - a.revenue);

  const attrCampaigns = attributions.map((a: any) => a.metaCampaignName).filter(Boolean);
  const utmCampaigns = orders.filter((o: any) => o.utmConfirmedMeta && o.metaCampaignName).map((o: any) => o.metaCampaignName);
  const campaignList = [...new Set([...attrCampaigns, ...utmCampaigns])].sort();

  const currencySymbol = currencySymbolFromCode(shopifyCurrency);

  return { rows, campaigns: campaignList, currencySymbol };
}
