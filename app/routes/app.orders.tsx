import { json } from "@remix-run/node";
import { useLoaderData, useSearchParams, useSubmit } from "@remix-run/react";
import { Page, Card, Text, BlockStack, InlineStack, Select } from "@shopify/polaris";
import ReportTabs from "../components/ReportTabs";
import SummaryTile from "../components/SummaryTile";
import React, { useState, useMemo, useRef, useEffect } from "react";
import { type ColumnDef } from "@tanstack/react-table";
import InteractiveTable from "../components/InteractiveTable";
import { authenticate } from "../shopify.server";
import db from "../db.server";
import { parseDateRange } from "../utils/dateRange.server";
import { shopLocalDayKey } from "../utils/shopTime.server";
import { cached as queryCached } from "../services/queryCache.server";

export const loader = async ({ request }) => {
  const { session } = await authenticate.admin(request);
  const shopDomain = session.shop;

  const url = new URL(request.url);
  const tagFilter = url.searchParams.get("tag") || "meta";
  const campaignFilter = url.searchParams.get("campaign") || "all";

  const shop = await db.shop.findUnique({ where: { shopDomain } });
  const tz = shop?.shopifyTimezone || "UTC";

  const { fromDate, toDate, fromKey, toKey } = parseDateRange(request, tz);
  const { DEFAULT_TTL } = await import("../services/queryCache.server");

  // Date-scoped queries with caching
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
  // Only fetch attributions for orders in the date window (not all-time)
  const orderIdsInRange = orders.map(o => o.shopifyOrderId);
  const [attributions, metaInsights] = await Promise.all([
    queryCached(`${shopDomain}:ordersAttrs:${fromKey}:${toKey}`, DEFAULT_TTL, () =>
      db.attribution.findMany({
        where: {
          shopDomain,
          OR: [
            { shopifyOrderId: { in: orderIdsInRange } },
            { confidence: 0, matchedAt: { gte: fromDate, lte: toDate } },
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

  const orderMap = {};
  for (const o of orders) orderMap[o.shopifyOrderId] = o;

  // Build attribution lookup by shopifyOrderId
  const attrByOrderId = {};
  for (const attr of attributions) {
    if (attr.confidence > 0) attrByOrderId[attr.shopifyOrderId] = attr;
  }

  // Build Difference % lookup: group matched attributions by metaAdId+date
  const metaValueByAdDay = {};
  for (const ins of metaInsights) {
    const key = `${ins.adId}_${shopLocalDayKey(tz, ins.date)}`;
    metaValueByAdDay[key] = (metaValueByAdDay[key] || 0) + ins.conversionValue;
  }
  const shopifyValueByAdDay = {};
  const attrGroupKeys = {};
  for (const attr of attributions) {
    if (attr.confidence === 0 || !attr.metaAdId) continue;
    const order = orderMap[attr.shopifyOrderId];
    if (!order) continue;
    const orderDate = shopLocalDayKey(tz, order.createdAt);
    const key = `${attr.metaAdId}_${orderDate}`;
    shopifyValueByAdDay[key] = (shopifyValueByAdDay[key] || 0) + (order.frozenTotalPrice || 0);
    attrGroupKeys[attr.shopifyOrderId] = key;
  }
  const differenceByGroup = {};
  for (const key of Object.keys(shopifyValueByAdDay)) {
    const metaVal = metaValueByAdDay[key] || 0;
    const shopVal = shopifyValueByAdDay[key] || 0;
    if (metaVal > 0) {
      differenceByGroup[key] = Math.round(((shopVal - metaVal) / metaVal) * 100);
    } else {
      differenceByGroup[key] = null;
    }
  }

  const customerMap = {};
  for (const c of customers) customerMap[c.shopifyCustomerId] = c;

  // Pre-computed customer segments (from Customer.metaSegment — set at sync time)
  const matchedAttrs = attributions.filter(a => a.confidence > 0);
  const matchedOrderIds = new Set(matchedAttrs.map(a => a.shopifyOrderId));
  const utmConfirmedOrderIds = new Set<string>();
  for (const o of orders) {
    if (o.utmConfirmedMeta) utmConfirmedOrderIds.add(o.shopifyOrderId);
  }
  const metaAcquiredCustomers = new Set<string>();
  for (const c of customers) {
    if (c.metaSegment === "metaNew") metaAcquiredCustomers.add(c.shopifyCustomerId);
  }

  // Helper: build raw UTM string from order fields
  function buildUtmString(o) {
    const parts = [];
    if (o.utmSource) parts.push(`utm_source=${o.utmSource}`);
    if (o.utmMedium) parts.push(`utm_medium=${o.utmMedium}`);
    if (o.utmCampaign) parts.push(`utm_campaign=${o.utmCampaign}`);
    if (o.utmContent) parts.push(`utm_content=${o.utmContent}`);
    if (o.utmTerm) parts.push(`utm_term=${o.utmTerm}`);
    if (o.utmId) parts.push(`utm_id=${o.utmId}`);
    return parts.join("&");
  }

  // Step 2: Build rows — start from ALL orders, tag each one
  const rows = [];
  const processedOrderIds = new Set();

  // 2a: Matched attributions (Meta New / Meta Repeat / Meta Retargeted)
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
        const custFirstDate = customer.firstOrderDate
          ? shopLocalDayKey(tz, customer.firstOrderDate) : "";
        const orderDate = shopLocalDayKey(tz, order.createdAt);
        tag = custFirstDate === orderDate ? "Meta New" : "Meta Repeat";
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

  // 2b: Unattributed Meta conversions (confidence = 0)
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

  // 2b-ii: UTM-only Meta orders — utmConfirmedMeta=true but no Layer 2 match.
  // Tagged as Meta Unmatched New / Repeat / Retargeted — same logic as matched tags.
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
        const custFirstDate = customer.firstOrderDate
          ? shopLocalDayKey(tz, customer.firstOrderDate) : "";
        const orderDate = shopLocalDayKey(tz, order.createdAt);
        tag = custFirstDate === orderDate ? "Meta Unmatched New" : "Meta Unmatched Repeat";
      } else {
        tag = "Meta Unmatched Retargeted";
      }
    }

    // Filter: "all" and "meta" show all unmatched; "Meta Unmatched" shows all 3 subtypes; specific subtypes filter exactly
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

  // 2c: Remaining orders — Meta Repeat, Non-Meta, or Non-Meta POS
  for (const order of orders) {
    if (processedOrderIds.has(order.shopifyOrderId)) continue;

    const custId = order.shopifyCustomerId;
    const customer = custId ? customerMap[custId] : null;
    const isPOS = !order.isOnlineStore;
    let tag = isPOS ? "Non-Meta POS" : "Non-Meta";

    if (custId && metaAcquiredCustomers.has(custId) && customer) {
      const custFirstDate = customer.firstOrderDate
        ? shopLocalDayKey(tz, customer.firstOrderDate) : "";
      const orderDate = shopLocalDayKey(tz, order.createdAt);
      if (orderDate !== custFirstDate) {
        tag = "Meta Repeat";
      }
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

  // Campaign list: include UTM-linked campaign names too
  const attrCampaigns = attributions.map(a => a.metaCampaignName).filter(Boolean);
  const utmCampaigns = orders.filter(o => o.utmConfirmedMeta && o.metaCampaignName).map(o => o.metaCampaignName);
  const campaignList = [...new Set([...attrCampaigns, ...utmCampaigns])].sort();

  // Summary counts
  const metaNew = rows.filter(r => r.tag === "Meta New").length;
  const metaRepeat = rows.filter(r => r.tag === "Meta Repeat").length;
  const metaRetargeted = rows.filter(r => r.tag === "Meta Retargeted").length;
  const metaUnmatchedNew = rows.filter(r => r.tag === "Meta Unmatched New").length;
  const metaUnmatchedRepeat = rows.filter(r => r.tag === "Meta Unmatched Repeat").length;
  const metaUnmatchedRetargeted = rows.filter(r => r.tag === "Meta Unmatched Retargeted").length;
  const metaUnmatched = metaUnmatchedNew + metaUnmatchedRepeat + metaUnmatchedRetargeted;
  const unattributed = rows.filter(r => r.tag === "Unattributed").length;
  const nonMeta = rows.filter(r => r.tag === "Non-Meta").length;
  const nonMetaPOS = rows.filter(r => r.tag === "Non-Meta POS").length;
  const totalOrders = rows.length;
  const totalRevenue = rows.reduce((s, r) => s + (r.revenue || 0), 0);
  const metaAttributed = metaNew + metaRepeat + metaRetargeted;
  const onlineStoreOrders = orders.filter(o => o.isOnlineStore);
  const onlineOrders = onlineStoreOrders.length;
  const onlineRevenue = onlineStoreOrders.reduce((s, o) => s + (o.frozenTotalPrice || 0), 0);
  const matchRate = onlineOrders > 0 ? Math.round((metaAttributed / onlineOrders) * 100) : 0;
  const metaRevenue = rows.filter(r => r.tag === "Meta New" || r.tag === "Meta Repeat" || r.tag === "Meta Retargeted").reduce((s, r) => s + (r.revenue || 0), 0);
  const aov = onlineOrders > 0 ? onlineRevenue / onlineOrders : 0;

  // Daily data for charts
  const dailyMap = {};
  for (const r of rows) {
    if (!dailyMap[r.date]) dailyMap[r.date] = { date: r.date, orders: 0, revenue: 0, metaNew: 0, metaRepeat: 0, metaRetargeted: 0, nonMeta: 0, nonMetaPOS: 0, unattributed: 0, metaNewRev: 0, metaRepeatRev: 0, metaRetargetedRev: 0 };
    dailyMap[r.date].orders++;
    dailyMap[r.date].revenue += r.revenue || 0;
    if (r.tag === "Meta New") { dailyMap[r.date].metaNew++; dailyMap[r.date].metaNewRev += r.revenue || 0; }
    else if (r.tag === "Meta Repeat") { dailyMap[r.date].metaRepeat++; dailyMap[r.date].metaRepeatRev += r.revenue || 0; }
    else if (r.tag === "Meta Retargeted") { dailyMap[r.date].metaRetargeted++; dailyMap[r.date].metaRetargetedRev += r.revenue || 0; }
    else if (r.tag === "Non-Meta") dailyMap[r.date].nonMeta++;
    else if (r.tag === "Non-Meta POS") dailyMap[r.date].nonMetaPOS++;
    else if (r.tag === "Unattributed") dailyMap[r.date].unattributed++;
  }
  const dailyData = Object.values(dailyMap).sort((a, b) => a.date.localeCompare(b.date));

  const currencySymbol = (shop?.shopifyCurrency || "GBP") === "GBP" ? "£"
    : (shop?.shopifyCurrency || "GBP") === "EUR" ? "€" : "$";

  // Attribution source counts
  const srcUtmAndLucidly = rows.filter(r => r.attributionSource === "UTM & Lucidly").length;
  const srcUtmOnly = rows.filter(r => r.attributionSource === "UTM").length;
  const srcLucidlyOnly = rows.filter(r => r.attributionSource === "Lucidly").length;
  const utmOnlyRevenue = rows.filter(r => r.attributionSource === "UTM").reduce((s, r) => s + (r.revenue || 0), 0);

  return json({
    rows, campaigns: campaignList,
    metaNew, metaRepeat, metaRetargeted, metaUnmatchedNew, metaUnmatchedRepeat, metaUnmatchedRetargeted, metaUnmatched, unattributed, nonMeta, nonMetaPOS,
    totalOrders, totalRevenue, metaAttributed, matchRate, aov, onlineOrders, onlineRevenue, metaRevenue,
    dailyData, currencySymbol,
    srcUtmAndLucidly, srcUtmOnly, srcLucidlyOnly, utmOnlyRevenue,
  });
};


const BREAKDOWN_SERIES = [
  { key: "metaNewRev", label: "New Customers", color: "#7C3AED" },
  { key: "metaRepeatRev", label: "Returning Customers", color: "#0891B2" },
  { key: "metaRetargetedRev", label: "Retargeted Customers", color: "#B45309" },
];

function RevenueBreakdownChart({ dailyData, formatPrice, highlight, onHighlightChange }: {
  dailyData: any[]; formatPrice: (v: number) => string;
  highlight: string | null; onHighlightChange: (key: string | null) => void;
}) {
  const [tooltip, setTooltip] = useState<{ x: number; y: number; content: string } | null>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const [containerWidth, setContainerWidth] = useState(800);

  useEffect(() => {
    if (!containerRef.current) return;
    const obs = new ResizeObserver((entries) => {
      for (const entry of entries) setContainerWidth(entry.contentRect.width);
    });
    obs.observe(containerRef.current);
    setContainerWidth(containerRef.current.offsetWidth);
    return () => obs.disconnect();
  }, []);

  // Collect all individual series values (not totals)
  const allVals = dailyData.flatMap(d => [d.metaNewRev || 0, d.metaRepeatRev || 0, d.metaRetargetedRev || 0]).filter(v => v > 0);
  if (allVals.length === 0) allVals.push(100);

  // Outlier-aware Y scale: use 90th percentile if top value is >2x the 90th
  allVals.sort((a, b) => a - b);
  const p90 = allVals[Math.floor(allVals.length * 0.9)];
  const rawMax = allVals[allVals.length - 1];
  const effectiveMax = (rawMax > p90 * 2.5 && allVals.length > 10) ? p90 * 1.3 : rawMax;

  // Nice rounding for Y axis
  const niceMax = (() => {
    if (effectiveMax <= 0) return 100;
    const mag = Math.pow(10, Math.floor(Math.log10(effectiveMax)));
    const norm = effectiveMax / mag;
    if (norm <= 1.2) return 1.2 * mag;
    if (norm <= 1.5) return 1.5 * mag;
    if (norm <= 2) return 2 * mag;
    if (norm <= 3) return 3 * mag;
    if (norm <= 5) return 5 * mag;
    if (norm <= 7.5) return 7.5 * mag;
    return 10 * mag;
  })();

  const yTickCount = 5;
  const yTicks = Array.from({ length: yTickCount + 1 }, (_, i) => Math.round((niceMax / yTickCount) * i));
  const fmtYTick = (v: number) => v >= 1000 ? `${(v / 1000).toFixed(v % 1000 === 0 ? 0 : 1)}k` : String(v);

  const chartHeight = 200;
  const yAxisWidth = 50;
  const xAxisHeight = 45;
  const days = dailyData.length;

  // Bar sizing: max bar width 14px, 3 bars per group, min gap = 1.5 * barWidth
  const maxBarW = 14;
  const plotWidth = containerWidth - yAxisWidth;
  // Work out bar width from available space: each day = 3*barW + gap, gap >= 1.5*barW → day = 4.5*barW min
  const barWFromSpace = days > 0 ? plotWidth / (days * 4.5) : maxBarW;
  const barWidth = Math.max(Math.min(Math.floor(barWFromSpace), maxBarW), 3);
  const barsWidth = barWidth * 3;
  const daySlotWidth = days > 0 ? plotWidth / days : barsWidth + barWidth * 1.5;
  const gapWidth = daySlotWidth - barsWidth;
  const barStartOffset = gapWidth / 2; // center bars within slot

  const totalWidth = containerWidth;
  const totalHeight = chartHeight + xAxisHeight;

  return (
    <div ref={containerRef}>
      <div style={{ paddingBottom: 4 }}>
        <svg width={totalWidth} height={totalHeight} style={{ display: "block", width: "100%" }}>
          {/* Horizontal gridlines + Y axis labels */}
          {yTicks.map((tick, ti) => {
            const y = chartHeight - (tick / niceMax) * chartHeight;
            return (
              <g key={ti}>
                <line x1={yAxisWidth} x2={yAxisWidth + plotWidth} y1={y} y2={y} stroke={ti === 0 ? "#6B7280" : "#D1D5DB"} strokeWidth={ti === 0 ? 1 : 0.5} />
                <text x={yAxisWidth - 6} y={y + 4} textAnchor="end" fontSize={10} fill="#6B7280">
                  {fmtYTick(tick)}
                </text>
              </g>
            );
          })}
          {/* Bars per day */}
          {dailyData.map((d, i) => {
            const slotX = yAxisWidth + i * daySlotWidth;
            return (
              <g key={d.date}>
                {BREAKDOWN_SERIES.map((s, si) => {
                  const val = d[s.key] || 0;
                  const clampedVal = Math.min(val, niceMax);
                  const barH = niceMax > 0 ? (clampedVal / niceMax) * chartHeight : 0;
                  const isActive = !highlight || highlight === s.key;
                  const isCapped = val > niceMax;
                  return (
                    <g key={s.key}>
                      <rect
                        x={slotX + barStartOffset + si * barWidth}
                        y={chartHeight - barH}
                        width={barWidth - 1}
                        height={Math.max(barH, 0)}
                        fill={s.color}
                        opacity={isActive ? 1 : 0.15}
                        rx={1}
                        style={{ cursor: "pointer", transition: "opacity 0.2s" }}
                        onMouseEnter={(e) => {
                          onHighlightChange(s.key);
                          setTooltip({ x: e.clientX, y: e.clientY, content: `${d.date}\n${s.label}: ${formatPrice(val)}` });
                        }}
                        onMouseLeave={() => {
                          onHighlightChange(null);
                          setTooltip(null);
                        }}
                      />
                      {/* Capped indicator — small triangle at top if bar exceeds scale */}
                      {isCapped && isActive && (
                        <polygon
                          points={`${slotX + barStartOffset + si * barWidth + (barWidth - 1) / 2},0 ${slotX + barStartOffset + si * barWidth},6 ${slotX + barStartOffset + si * barWidth + barWidth - 1},6`}
                          fill={s.color}
                          opacity={0.6}
                        />
                      )}
                    </g>
                  );
                })}
                {/* X axis date label */}
                <text
                  x={slotX + daySlotWidth / 2}
                  y={chartHeight + 12}
                  textAnchor="end"
                  fontSize={days > 45 ? 7 : days > 30 ? 8 : 9}
                  fill="#374151"
                  transform={`rotate(-45, ${slotX + daySlotWidth / 2}, ${chartHeight + 12})`}
                >
                  {d.date.slice(5)}
                </text>
              </g>
            );
          })}
        </svg>
      </div>
      {tooltip && (
        <div style={{
          position: "fixed", left: tooltip.x + 12, top: tooltip.y - 40,
          background: "#1F2937", color: "#fff", padding: "6px 10px", borderRadius: 6,
          fontSize: 12, whiteSpace: "pre-line", pointerEvents: "none", zIndex: 1000,
          boxShadow: "0 2px 8px rgba(0,0,0,0.2)",
        }}>
          {tooltip.content}
        </div>
      )}
      <div style={{ display: "flex", gap: 20, paddingTop: 10, flexWrap: "wrap", justifyContent: "center" }}>
        {BREAKDOWN_SERIES.map(s => {
          const isActive = !highlight || highlight === s.key;
          return (
            <div
              key={s.key}
              onMouseEnter={() => onHighlightChange(s.key)}
              onMouseLeave={() => onHighlightChange(null)}
              style={{ display: "flex", alignItems: "center", gap: 6, cursor: "pointer", opacity: isActive ? 1 : 0.4, transition: "opacity 0.2s" }}
            >
              <div style={{ width: 10, height: 10, borderRadius: 2, backgroundColor: s.color }} />
              <span style={{ fontSize: 12, color: "#374151" }}>{s.label}</span>
            </div>
          );
        })}
      </div>
    </div>
  );
}

const tileGridStyles = `
.tile-grid-top-row { display: grid; grid-template-columns: repeat(3, 1fr); gap: 16px; }
@media (max-width: 900px) { .tile-grid-top-row { grid-template-columns: 1fr; } }
.revenue-breakdown-segments { display: grid; grid-template-columns: repeat(3, 1fr); gap: 24px; }
@media (max-width: 900px) { .revenue-breakdown-segments { grid-template-columns: 1fr; } }
.revenue-segment { display: flex; flex-direction: column; gap: 4px; }
`;

export default function Orders() {
  const {
    rows, campaigns, metaNew, metaRepeat, metaRetargeted, unattributed, nonMeta, nonMetaPOS,
    totalOrders, totalRevenue, metaAttributed, matchRate, aov, onlineOrders, onlineRevenue, metaRevenue,
    dailyData, currencySymbol,
  } = useLoaderData();
  const cs = currencySymbol || "£";
  const [searchParams] = useSearchParams();
  const submit = useSubmit();

  const tagFilter = searchParams.get("tag") || "meta";
  const campaignFilter = searchParams.get("campaign") || "all";

  const navigateWithParams = (updates: Record<string, string>) => {
    const params = new URLSearchParams(searchParams);
    for (const [k, v] of Object.entries(updates)) params.set(k, v);
    submit(params, { method: "get", replace: true });
  };

  const handleTagChange = (value) => navigateWithParams({ tag: value });
  const handleCampaignChange = (value) => navigateWithParams({ campaign: value });

  const tagOptions = [
    { label: "All", value: "all" },
    { label: "All Meta", value: "meta" },
    { label: "Meta New", value: "Meta New" },
    { label: "Meta Repeat", value: "Meta Repeat" },
    { label: "Meta Retargeted", value: "Meta Retargeted" },
    { label: "Meta Unmatched (All)", value: "Meta Unmatched" },
    { label: "Meta Unmatched New", value: "Meta Unmatched New" },
    { label: "Meta Unmatched Repeat", value: "Meta Unmatched Repeat" },
    { label: "Meta Unmatched Retargeted", value: "Meta Unmatched Retargeted" },
    { label: "Unattributed", value: "Unattributed" },
    { label: "Non-Meta", value: "Non-Meta" },
    { label: "Non-Meta POS", value: "Non-Meta POS" },
  ];

  const campaignOptions = [
    { label: "All Campaigns", value: "all" },
    ...campaigns.map(c => ({ label: c, value: c })),
  ];

  const columns = useMemo<ColumnDef<any, any>[]>(() => [
    { accessorKey: "createdAtISO", header: "Date & Time",
      meta: { description: "When the order was placed" },
      cell: ({ getValue, row }) => {
        const iso = getValue() || (row.original.date ? row.original.date + "T12:00:00" : "");
        if (!iso) return "—";
        const d = new Date(iso);
        const date = d.toLocaleDateString("en-GB", { day: "numeric", month: "short", year: "numeric" });
        const time = d.toLocaleTimeString("en-GB", { hour: "2-digit", minute: "2-digit", hour12: false });
        return `${date} ${time}`;
      },
    },
    { accessorKey: "orderNumber", header: "Order",
      meta: { description: "Shopify order ID" },
      cell: ({ getValue }) => getValue() || "—" },
    { accessorKey: "country", header: "Country",
      meta: { filterType: "multi-select", description: "Customer's shipping country" },
      filterFn: "multiSelect",
      cell: ({ getValue }) => getValue() || "—" },
    { accessorKey: "city", header: "City",
      meta: { description: "Customer's shipping city" },
      cell: ({ getValue }) => getValue() || "—" },
    { accessorKey: "orderCount", header: "Order #",
      meta: { align: "right", description: "Which order this was for the customer (1st, 2nd, 3rd, etc.) at the time of purchase" },
      cell: ({ getValue }) => {
        const v = getValue();
        if (v == null) return "—";
        return v === 1 ? "1st" : v === 2 ? "2nd" : v === 3 ? "3rd" : `${v}th`;
      } },
    { accessorKey: "campaign", header: "Campaign",
      meta: { maxWidth: "200px", filterType: "multi-select", description: "Meta campaign that drove this order" },
      filterFn: "multiSelect",
      cell: ({ getValue }) => getValue() || "—" },
    { accessorKey: "adSet", header: "Ad Set",
      meta: { maxWidth: "180px", filterType: "multi-select", description: "Meta ad set that drove this order" },
      filterFn: "multiSelect",
      cell: ({ getValue }) => getValue() || "—" },
    { accessorKey: "adName", header: "Ad",
      meta: { maxWidth: "180px", filterType: "multi-select", description: "Specific Meta ad creative that drove this order" },
      filterFn: "multiSelect",
      cell: ({ getValue }) => getValue() || "—" },
    { accessorKey: "lineItems", header: "Products", meta: { maxWidth: "200px", description: "Products purchased in this order" },
      cell: ({ getValue }) => getValue() || "—" },
    { accessorKey: "productSkus", header: "SKUs", meta: { maxWidth: "160px", description: "Product SKU codes in this order" },
      cell: ({ getValue }) => getValue() || "—" },
    { accessorKey: "productCollections", header: "Collections",
      meta: { maxWidth: "160px", filterType: "multi-select", description: "Shopify collections the ordered products belong to" },
      filterFn: "multiSelect",
      cell: ({ getValue }) => getValue() || "—" },
    { accessorKey: "discountCodes", header: "Discount",
      meta: { filterType: "multi-select", description: "Discount or promo code applied to this order" },
      filterFn: "multiSelect",
      cell: ({ getValue }) => getValue() || "—" },
    { accessorKey: "revenue", header: "Revenue", meta: { align: "right", description: "Order total at time of purchase (frozen — unaffected by later edits)" },
      cell: ({ getValue }) => getValue() ? `${cs}${getValue().toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}` : "—" },
    { accessorKey: "totalRefunded", header: "Refunded", meta: { align: "right", description: "Amount refunded on this order" },
      cell: ({ getValue }) => getValue() > 0 ? `${cs}${getValue().toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}` : "—" },
    { accessorKey: "netRevenue", header: "Net Revenue", meta: { align: "right", description: "Revenue after refunds", calc: "Revenue − Refunded" },
      cell: ({ getValue }) => {
        const v = getValue();
        if (v == null) return "—";
        return `${cs}${v.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
      } },
    { accessorKey: "refundStatus", header: "Refund Status",
      meta: { filterType: "multi-select", description: "Current refund status of this order (none, partial, full)" },
      filterFn: "multiSelect",
      cell: ({ getValue }) => getValue() === "none" ? "—" : getValue() },
    { accessorKey: "tag", header: "Type",
      meta: { filterType: "multi-select", description: "How this order relates to Meta ads. Meta New = first-time customer via Meta. Meta Repeat = returning Meta-acquired customer. Meta Retargeted = existing customer converted by Meta. Meta Unmatched New/Repeat/Retargeted = UTM confirms Meta click but no statistical match. Non-Meta = online order with no Meta attribution. Non-Meta POS = in-store/POS order" },
      filterFn: "multiSelect" },
    { accessorKey: "difference", header: "Difference", meta: { align: "right", description: "Gap between Shopify order values and Meta-reported conversion values for the same ad+day group. Positive = Shopify higher", calc: "(Shopify value − Meta value) ÷ Meta value × 100" },
      cell: ({ getValue }) => {
        const v = getValue();
        if (v === null || v === undefined) return "—";
        return `${v > 0 ? "+" : ""}${v}%`;
      },
    },
    { id: "confidence", header: "Confidence",
      meta: { filterType: "multi-select", description: "How confident the attribution match is. 100% = only possible match. Lower % = multiple candidate orders could have matched" },
      filterFn: "multiSelect",
      accessorFn: (row) => {
        if (row.confidence === null || row.confidence === undefined) return "";
        if (row.confidence === 0) return "Unmatched";
        return `${row.confidence}%`;
      },
    },
    { accessorKey: "method", header: "Method",
      meta: { filterType: "multi-select", description: "Attribution method used. Primary = exhaustive backtracking matcher. FAST = greedy fallback. UTM = attributed via UTM parameters" },
      filterFn: "multiSelect",
      cell: ({ getValue }) => getValue() || "—" },
    { accessorKey: "attributionSource", header: "Source",
      meta: { filterType: "multi-select", description: "How this order was attributed. UTM & Lucidly = both UTM and statistical matcher agree. UTM = UTM confirms Meta ad but no statistical match. Lucidly = statistical match only. Unattributed = neither" },
      filterFn: "multiSelect",
      cell: ({ getValue }) => {
        const v = getValue();
        if (!v || v === "Unattributed") return "—";
        return v;
      },
    },
    { accessorKey: "utm", header: "UTM",
      meta: { maxWidth: "300px", description: "Raw UTM parameters from the landing page URL when this order was placed" },
      cell: ({ getValue }) => {
        const v = getValue();
        if (!v) return "—";
        return <span style={{ fontFamily: "monospace", fontSize: "11px" }}>{v}</span>;
      },
    },
  ], [cs]);

  const defaultVisibleColumns = useMemo(() => [
    "createdAtISO", "orderNumber", "orderCount", "campaign",
    "revenue", "netRevenue", "tag", "confidence",
  ], []);

  const columnProfiles = useMemo(() => [
    {
      id: "overview", label: "Overview", icon: "📊",
      description: "Key order details — who bought, what campaign, and how confident the match is",
      columns: ["createdAtISO", "orderCount", "campaign", "revenue", "tag", "confidence"],
      fullColumns: ["createdAtISO", "orderNumber", "orderCount", "campaign", "revenue", "netRevenue", "tag", "difference", "confidence"],
    },
    {
      id: "attribution", label: "Attribution", icon: "🎯",
      description: "Deep dive into how each order was matched to a Meta ad",
      columns: ["createdAtISO", "campaign", "adSet", "revenue", "attributionSource", "confidence", "method"],
      fullColumns: ["createdAtISO", "campaign", "adSet", "adName", "revenue", "netRevenue", "difference", "tag", "attributionSource", "confidence", "method", "utm"],
    },
    {
      id: "product", label: "Product", icon: "🛍️",
      description: "What products were purchased in each order",
      columns: ["createdAtISO", "orderCount", "lineItems", "revenue", "tag"],
      fullColumns: ["createdAtISO", "orderCount", "lineItems", "productSkus", "productCollections", "discountCodes", "revenue", "netRevenue", "tag"],
    },
    {
      id: "geography", label: "Geography", icon: "🌍",
      description: "Where orders are coming from geographically",
      columns: ["createdAtISO", "orderCount", "country", "revenue", "tag"],
      fullColumns: ["createdAtISO", "orderCount", "country", "city", "campaign", "revenue", "tag"],
    },
    {
      id: "all", label: "All", icon: "📋",
      description: "Every available column",
      columns: columns.map(c => (c as any).accessorKey || (c as any).id).filter(Boolean),
    },
  ], [columns]);

  const fmtPrice = (v: number) => `${cs}${v.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
  const fmtCount = (v: number) => Math.round(v).toLocaleString();

  const footerRow = useMemo(() => {
    if (rows.length === 0) return undefined;
    const sum = (key: string) => rows.reduce((s, r) => s + (r[key] || 0), 0);
    const revenue = sum("revenue");
    const refunded = sum("totalRefunded");
    const netRev = sum("netRevenue");
    const avgConf = rows.filter(r => r.confidence > 0).length > 0
      ? Math.round(rows.filter(r => r.confidence > 0).reduce((s, r) => s + r.confidence, 0) / rows.filter(r => r.confidence > 0).length)
      : 0;
    return {
      date: `${rows.length} orders`,
      createdAtISO: "", orderNumber: "", country: "", city: "",
      orderCount: "",
      campaign: "", adSet: "", adName: "",
      lineItems: "", productSkus: "", productCollections: "", discountCodes: "",
      revenue: fmtPrice(revenue),
      totalRefunded: refunded > 0 ? fmtPrice(refunded) : "—",
      netRevenue: fmtPrice(netRev),
      refundStatus: "",
      tag: "",
      difference: "",
      confidence: avgConf > 0 ? `${avgConf}% avg` : "",
      method: "",
    };
  }, [rows, cs]);

  return (
    <Page title="Order Explorer" fullWidth>
      <ReportTabs>
      <BlockStack gap="500">

        <Card>
          <BlockStack gap="300">
            <Text as="p" variant="bodySm" tone="subdued">
              Every Shopify order in the selected period, enriched with Meta attribution data.
              Each order is tagged — <strong>Meta New</strong> (first-ever purchase via Meta),
              {" "}<strong>Meta Repeat</strong> (returning Meta-acquired customer),
              {" "}<strong>Meta Retargeted</strong> (existing customer converted by a Meta ad),
              {" "}<strong>Meta Unmatched New/Repeat/Retargeted</strong> (UTM confirms Meta click but no statistical match),
              {" "}<strong>Non-Meta</strong> (online order with no Meta attribution),
              or <strong>Non-Meta POS</strong> (in-store/POS order).
              Use the filters below to drill into specific segments or campaigns.
            </Text>
            <InlineStack gap="400">
              <Select label="Customer Type" options={tagOptions} value={tagFilter} onChange={handleTagChange} />
              <Select label="Campaign" options={campaignOptions} value={campaignFilter} onChange={handleCampaignChange} />
            </InlineStack>
            <InteractiveTable
              columns={columns}
              data={rows}
              defaultVisibleColumns={defaultVisibleColumns}
              tableId="orders"
              columnProfiles={columnProfiles}
              footerRow={footerRow}
              initialSorting={[{ id: "createdAtISO", desc: true }]}
            />
          </BlockStack>
        </Card>
      </BlockStack>
      </ReportTabs>
    </Page>
  );
}
