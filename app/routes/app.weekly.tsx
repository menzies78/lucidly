import { json } from "@remix-run/node";
import { useLoaderData, useSearchParams, useNavigate } from "@remix-run/react";
import { Page, Text, InlineStack, Button } from "@shopify/polaris";
import ReportTabs from "../components/ReportTabs";
import { authenticate } from "../shopify.server";
import db from "../db.server";
import { cached as queryCached } from "../services/queryCache.server";

const SHORT_LABELS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];

function getWeekMonday(dateStr: string | null): Date {
  const d = dateStr ? new Date(dateStr + "T12:00:00Z") : new Date();
  const dow = d.getUTCDay();
  const offset = dow === 0 ? 6 : dow - 1;
  const monday = new Date(d);
  monday.setUTCDate(d.getUTCDate() - offset);
  monday.setUTCHours(0, 0, 0, 0);
  return monday;
}

// Latest fully-completed week: Monday of (this week's Monday - 7 days)
function getLatestCompleteWeekMonday(): Date {
  const thisMonday = getWeekMonday(null);
  const monday = new Date(thisMonday);
  monday.setUTCDate(thisMonday.getUTCDate() - 7);
  return monday;
}

function fmt(d: Date): string {
  return d.toISOString().split("T")[0];
}

function fmtDate(d: Date): string {
  return d.toLocaleDateString("en-GB", { day: "numeric", month: "short" });
}

function fmtCurrency(val: number, currency: string): string {
  const symbol = currency === "GBP" ? "£" : currency === "USD" ? "$" : "€";
  if (val === 0) return `${symbol}0`;
  return `${symbol}${Math.round(val).toLocaleString("en-GB")}`;
}

function fmtRoas(val: number): string {
  if (!isFinite(val) || isNaN(val)) return "—";
  return `${val.toFixed(1)}x`;
}

function wowPct(current: number, prev: number): number | null {
  if (prev === 0 && current === 0) return null;
  if (prev === 0) return 100;
  return Math.round(((current - prev) / prev) * 100);
}

export const loader = async ({ request }) => {
  const { session } = await authenticate.admin(request);
  const shopDomain = session.shop;

  const url = new URL(request.url);
  const weekParam = url.searchParams.get("week");

  // Weekly Report is NOT affected by the global nav date selector (from/to).
  // Only its own `week` param is honored; otherwise default to latest complete week.
  const monday = weekParam
    ? getWeekMonday(weekParam) // snap any chosen date to its Monday
    : getLatestCompleteWeekMonday();
  const sunday = new Date(monday);
  sunday.setUTCDate(monday.getUTCDate() + 6);
  sunday.setUTCHours(23, 59, 59, 999);

  // Previous week for WoW
  const prevMonday = new Date(monday);
  prevMonday.setUTCDate(monday.getUTCDate() - 7);
  const prevSunday = new Date(prevMonday);
  prevSunday.setUTCDate(prevMonday.getUTCDate() + 6);
  prevSunday.setUTCHours(23, 59, 59, 999);

  const shop = await db.shop.findUnique({ where: { shopDomain } });
  const currency = shop?.shopifyCurrency || "GBP";

  // ── Fetch data for BOTH weeks in parallel ──
  const [orders, prevOrders, insights, prevInsights, breakdowns, customers, allAttrs, metaEntities] = await Promise.all([
    db.order.findMany({
      where: { shopDomain, createdAt: { gte: monday, lte: sunday } },
      select: { shopifyOrderId: true, shopifyCustomerId: true, frozenTotalPrice: true, createdAt: true, country: true, countryCode: true, lineItems: true, utmConfirmedMeta: true },
    }),
    db.order.findMany({
      where: { shopDomain, createdAt: { gte: prevMonday, lte: prevSunday } },
      select: { shopifyOrderId: true, shopifyCustomerId: true, frozenTotalPrice: true, createdAt: true, country: true, countryCode: true, lineItems: true, utmConfirmedMeta: true },
    }),
    db.metaInsight.findMany({
      where: { shopDomain, date: { gte: monday, lte: sunday } },
      select: { date: true, spend: true, adId: true, adName: true, campaignName: true, adSetName: true },
    }),
    db.metaInsight.findMany({
      where: { shopDomain, date: { gte: prevMonday, lte: prevSunday } },
      select: { date: true, spend: true },
    }),
    db.metaBreakdown.findMany({
      where: { shopDomain, date: { gte: monday, lte: sunday }, breakdownType: "country" },
      select: { breakdownValue: true, spend: true, conversionValue: true, conversions: true },
    }),
    db.customer.findMany({
      where: { shopDomain },
      select: { shopifyCustomerId: true, firstOrderDate: true, metaSegment: true },
    }),
    // Date-scope attributions to the current+previous week (was loading ALL attributions)
    db.attribution.findMany({
      where: {
        shopDomain,
        OR: [
          { matchedAt: { gte: prevMonday, lte: sunday } },
          { confidence: { gt: 0 }, matchedAt: { gte: prevMonday, lte: sunday } },
        ],
      },
    }),
    db.metaEntity.findMany({
      where: { shopDomain, entityType: "ad" },
      select: { entityId: true, createdTime: true },
    }),
  ]);
  // allOrderRefs removed (segments pre-computed on Customer model)

  // Pre-computed customer segments (from Customer.metaSegment — set at sync time)
  const customerMap = new Map<string, any>();
  for (const c of customers) customerMap.set(c.shopifyCustomerId, c);

  const metaAcquiredCustomers = new Set<string>();
  for (const c of customers) {
    if (c.metaSegment === "metaNew") metaAcquiredCustomers.add(c.shopifyCustomerId);
  }

  // Build attribution lookup for current week orders
  const currentOrderIds = new Set(orders.map(o => o.shopifyOrderId));
  const weekAttrs = allAttrs.filter(a => a.confidence > 0 && currentOrderIds.has(a.shopifyOrderId));
  const attrMap = new Map<string, typeof weekAttrs[0]>();
  for (const a of weekAttrs) attrMap.set(a.shopifyOrderId, a);

  // Previous week attribution lookup
  const prevOrderIds = new Set(prevOrders.map(o => o.shopifyOrderId));
  const prevWeekAttrs = allAttrs.filter(a => a.confidence > 0 && prevOrderIds.has(a.shopifyOrderId));
  const prevAttrMap = new Map<string, typeof prevWeekAttrs[0]>();
  for (const a of prevWeekAttrs) prevAttrMap.set(a.shopifyOrderId, a);

  // Unmatched attributions for current + previous week
  const mondayStr = fmt(monday);
  const sundayStr = fmt(sunday);
  const prevMondayStr = fmt(prevMonday);
  const prevSundayStr = fmt(prevSunday);
  const unmatchedCurrent = allAttrs.filter(a => {
    if (a.confidence !== 0) return false;
    const m = a.shopifyOrderId.match(/(\d{4}-\d{2}-\d{2})/);
    return m && m[1] >= mondayStr && m[1] <= sundayStr;
  });
  const unmatchedPrev = allAttrs.filter(a => {
    if (a.confidence !== 0) return false;
    const m = a.shopifyOrderId.match(/(\d{4}-\d{2}-\d{2})/);
    return m && m[1] >= prevMondayStr && m[1] <= prevSundayStr;
  });

  // ── Helper: classify an order ──
  function classifyOrder(order: { shopifyCustomerId: string | null; createdAt: Date }, attr: { isNewCustomer?: boolean | null } | undefined): "new" | "repeat" | "retargeted" | "organic" {
    if (!attr) return "organic";
    const custId = order.shopifyCustomerId;
    if (!custId) return "new";
    const customer = customerMap.get(custId);
    if (!customer) return "new";
    const isMetaAcquired = metaAcquiredCustomers.has(custId);
    if (isMetaAcquired) {
      const custFirstDate = customer.firstOrderDate?.toISOString().split("T")[0] || "";
      const orderDate = order.createdAt.toISOString().split("T")[0];
      return custFirstDate === orderDate ? "new" : "repeat";
    }
    return "retargeted";
  }

  // ── Day-level aggregation (current + prev week) ──
  type DayBucket = {
    storeRevenue: number; adSpend: number; adOrders: number; adRevenue: number;
    newOrders: number; newRevenue: number; repeatOrders: number; repeatRevenue: number;
    retargetedOrders: number; retargetedRevenue: number;
    unmatchedConversions: number; unmatchedRevenue: number;
  };
  const emptyDay = (): DayBucket => ({ storeRevenue: 0, adSpend: 0, adOrders: 0, adRevenue: 0, newOrders: 0, newRevenue: 0, repeatOrders: 0, repeatRevenue: 0, retargetedOrders: 0, retargetedRevenue: 0, unmatchedConversions: 0, unmatchedRevenue: 0 });

  const days: DayBucket[] = Array.from({ length: 7 }, emptyDay);
  const prevDays: DayBucket[] = Array.from({ length: 7 }, emptyDay);

  function toDayIdx(d: Date): number { const dow = d.getUTCDay(); return dow === 0 ? 6 : dow - 1; }

  function processOrders(orderList: typeof orders, aMap: typeof attrMap, dayBuckets: DayBucket[]) {
    for (const order of orderList) {
      const idx = toDayIdx(new Date(order.createdAt));
      const rev = order.frozenTotalPrice || 0;
      dayBuckets[idx].storeRevenue += rev;

      const attr = aMap.get(order.shopifyOrderId);
      if (attr) {
        // Exclude £0 orders (staff orders etc) from ad metrics
        if (rev === 0) continue;
        dayBuckets[idx].adOrders++;
        dayBuckets[idx].adRevenue += rev;
        const cls = classifyOrder(order, attr);
        if (cls === "new") { dayBuckets[idx].newOrders++; dayBuckets[idx].newRevenue += rev; }
        else if (cls === "repeat") { dayBuckets[idx].repeatOrders++; dayBuckets[idx].repeatRevenue += rev; }
        else if (cls === "retargeted") { dayBuckets[idx].retargetedOrders++; dayBuckets[idx].retargetedRevenue += rev; }
      }
    }
  }

  processOrders(orders, attrMap, days);
  processOrders(prevOrders, prevAttrMap, prevDays);

  for (const ins of insights) { days[toDayIdx(new Date(ins.date))].adSpend += ins.spend || 0; }
  for (const ins of prevInsights) { prevDays[toDayIdx(new Date(ins.date))].adSpend += ins.spend || 0; }

  // Add unmatched conversions to day buckets
  for (const a of unmatchedCurrent) {
    const m = a.shopifyOrderId.match(/(\d{4}-\d{2}-\d{2})/);
    if (!m) continue;
    const idx = toDayIdx(new Date(m[1] + "T12:00:00Z"));
    days[idx].unmatchedConversions++;
    days[idx].unmatchedRevenue += a.metaConversionValue || 0;
  }
  for (const a of unmatchedPrev) {
    const m = a.shopifyOrderId.match(/(\d{4}-\d{2}-\d{2})/);
    if (!m) continue;
    const idx = toDayIdx(new Date(m[1] + "T12:00:00Z"));
    prevDays[idx].unmatchedConversions++;
    prevDays[idx].unmatchedRevenue += a.metaConversionValue || 0;
  }

  // ── Helper: build geo/product aggregates for a set of orders ──
  const buildGeo = (ords: any[], aMap: Map<string, any>) => {
    const agg: Record<string, { orders: number; revenue: number }> = {};
    for (const order of ords) {
      if ((order.frozenTotalPrice || 0) === 0) continue; // Exclude £0
      const attr = aMap.get(order.shopifyOrderId);
      if (!attr) continue;
      if (classifyOrder(order, attr) !== "new") continue;
      const country = order.country || "Unknown";
      if (!agg[country]) agg[country] = { orders: 0, revenue: 0 };
      agg[country].orders++;
      agg[country].revenue += order.frozenTotalPrice || 0;
    }
    return agg;
  };
  const buildProducts = (ords: any[], aMap: Map<string, any>) => {
    const agg: Record<string, { orders: number; revenue: number }> = {};
    for (const order of ords) {
      if ((order.frozenTotalPrice || 0) === 0) continue; // Exclude £0
      const attr = aMap.get(order.shopifyOrderId);
      if (!attr) continue;
      if (classifyOrder(order, attr) !== "new") continue;
      const items = (order.lineItems || "").split(", ").map((s: string) => s.trim()).filter(Boolean);
      if (items.length === 0) items.push("Unknown");
      const revShare = (order.frozenTotalPrice || 0) / items.length;
      for (const item of items) {
        if (!agg[item]) agg[item] = { orders: 0, revenue: 0 };
        agg[item].orders++;
        agg[item].revenue += revShare;
      }
    }
    return agg;
  };

  // Current + previous period
  const geoNewCur = buildGeo(orders, attrMap);
  const geoNewPrev = buildGeo(prevOrders, prevAttrMap);
  const geoNewSorted = Object.entries(geoNewCur).sort((a, b) => b[1].revenue - a[1].revenue)
    .map(([country, data]) => ({ country, ...data, prevRevenue: geoNewPrev[country]?.revenue || 0 }));

  const productNewCur = buildProducts(orders, attrMap);
  const productNewPrev = buildProducts(prevOrders, prevAttrMap);
  const productNewSorted = Object.entries(productNewCur).sort((a, b) => b[1].orders - a[1].orders)
    .map(([product, data]) => ({ product, ...data, prevRevenue: productNewPrev[product]?.revenue || 0 }));

  // ── Ad Spend vs Ad Revenue by Country ──
  const codeToName = (code: string) => {
    try {
      const name = new Intl.DisplayNames(["en"], { type: "region" }).of(code.toUpperCase());
      return name || code;
    } catch { return code; }
  };
  const countrySpend: Record<string, { spend: number; revenue: number; orders: number }> = {};
  for (const b of breakdowns) {
    // Exclude zero-value conversions (staff orders etc)
    if ((b.conversionValue || 0) === 0 && (b.conversions || 0) > 0) continue;
    const country = codeToName(b.breakdownValue || "Unknown");
    if (!countrySpend[country]) countrySpend[country] = { spend: 0, revenue: 0, orders: 0 };
    countrySpend[country].spend += b.spend || 0;
    countrySpend[country].revenue += b.conversionValue || 0;
    countrySpend[country].orders += b.conversions || 0;
  }
  const countrySpendSorted = Object.entries(countrySpend).sort((a, b) => b[1].spend - a[1].spend).slice(0, 10)
    .map(([country, data]) => ({ country, ...data }));

  // ── Top Performing Ads (split by customer type) ──
  type AdPerf = { adName: string; campaignName: string; adSetName: string; orders: number; revenue: number; spend: number };
  const adsNew: Record<string, AdPerf> = {};
  const adsExisting: Record<string, AdPerf> = {}; // Repeat + Retargeted combined

  // Build ad spend lookup from insights
  const adSpendMap: Record<string, number> = {};
  const adMetaMap: Record<string, { adName: string; campaignName: string; adSetName: string }> = {};
  for (const ins of insights) {
    if (!ins.adId) continue;
    adSpendMap[ins.adId] = (adSpendMap[ins.adId] || 0) + (ins.spend || 0);
    if (!adMetaMap[ins.adId]) adMetaMap[ins.adId] = { adName: ins.adName || ins.adId, campaignName: ins.campaignName || "", adSetName: ins.adSetName || "" };
  }

  for (const order of orders) {
    const attr = attrMap.get(order.shopifyOrderId);
    if (!attr || !attr.metaAdId) continue;
    const rev = order.frozenTotalPrice || 0;
    if (rev === 0) continue; // Exclude £0 orders from ad performance
    const cls = classifyOrder(order, attr);
    const adId = attr.metaAdId;
    const meta = adMetaMap[adId] || { adName: attr.metaAdName || adId, campaignName: attr.metaCampaignName || "", adSetName: attr.metaAdSetName || "" };

    let bucket: Record<string, AdPerf>;
    if (cls === "new") bucket = adsNew;
    else if (cls === "repeat" || cls === "retargeted") bucket = adsExisting;
    else continue;

    if (!bucket[adId]) bucket[adId] = { adName: meta.adName, campaignName: meta.campaignName, adSetName: meta.adSetName, orders: 0, revenue: 0, spend: adSpendMap[adId] || 0 };
    bucket[adId].orders++;
    bucket[adId].revenue += rev;
  }

  const sortAds = (map: Record<string, AdPerf>) => Object.values(map).sort((a, b) => b.revenue - a.revenue);

  // ── Newly launched ads this week (createdTime within monday–sunday) ──
  const entityCreatedMap = new Map<string, Date>();
  for (const e of metaEntities) {
    if (e.createdTime) entityCreatedMap.set(e.entityId, new Date(e.createdTime));
  }
  const newlyLaunchedAds: { adName: string; adId: string; orders: number; revenue: number; spend: number }[] = [];
  const allAdsThisWeek = { ...adsNew, ...adsExisting };
  for (const [adId, perf] of Object.entries(allAdsThisWeek)) {
    const created = entityCreatedMap.get(adId);
    if (created && created >= monday && created <= sunday) {
      newlyLaunchedAds.push({ adName: perf.adName, adId, orders: perf.orders, revenue: perf.revenue, spend: perf.spend });
    }
  }
  // Also check ads with spend but no orders
  for (const [adId, meta] of Object.entries(adMetaMap)) {
    if (allAdsThisWeek[adId]) continue; // already counted
    const created = entityCreatedMap.get(adId);
    if (created && created >= monday && created <= sunday && (adSpendMap[adId] || 0) > 0) {
      newlyLaunchedAds.push({ adName: meta.adName, adId, orders: 0, revenue: 0, spend: adSpendMap[adId] || 0 });
    }
  }
  newlyLaunchedAds.sort((a, b) => b.spend - a.spend);

  // ── Date labels ──
  const dateLabels = Array.from({ length: 7 }, (_, i) => {
    const d = new Date(monday);
    d.setUTCDate(monday.getUTCDate() + i);
    return fmt(d);
  });

  return json({
    monday: fmt(monday),
    sunday: fmt(sunday),
    days,
    prevDays,
    dateLabels,
    currency,
    geoNew: geoNewSorted,
    productNew: productNewSorted,
    countrySpend: countrySpendSorted,
    topAdsNew: sortAds(adsNew),
    topAdsExisting: sortAds(adsExisting),
    newlyLaunchedAds,
  });
};

// ── Types ──
interface DayData {
  storeRevenue: number; adSpend: number; adOrders: number; adRevenue: number;
  newOrders: number; newRevenue: number; repeatOrders: number; repeatRevenue: number;
  retargetedOrders: number; retargetedRevenue: number;
  unmatchedConversions: number; unmatchedRevenue: number;
}
interface GeoRow { country: string; orders: number; revenue: number }
interface ProductRow { product: string; orders: number; revenue: number }
interface CountrySpendRow { country: string; spend: number; revenue: number }
interface AdPerf { adName: string; campaignName: string; adSetName: string; orders: number; revenue: number; spend: number }

function sumDays(days: DayData[]): DayData {
  const t: DayData = { storeRevenue: 0, adSpend: 0, adOrders: 0, adRevenue: 0, newOrders: 0, newRevenue: 0, repeatOrders: 0, repeatRevenue: 0, retargetedOrders: 0, retargetedRevenue: 0, unmatchedConversions: 0, unmatchedRevenue: 0 };
  for (const d of days) { for (const k of Object.keys(t) as (keyof DayData)[]) t[k] += d[k]; }
  return t;
}

// ── WoW badge ──
function WowBadge({ current, prev }: { current: number; prev: number }) {
  const pct = wowPct(current, prev);
  if (pct === null) return null;
  const color = pct > 0 ? "#16a34a" : pct < 0 ? "#dc2626" : "#8c9196";
  const arrow = pct > 0 ? "▲" : pct < 0 ? "▼" : "–";
  return <span style={{ fontSize: "9px", fontWeight: 500, color }}>{arrow}{Math.abs(pct)}%</span>;
}

// ── Metric row ──
function MetricRow({ label, value, muted, current, prev }: { label: string; value: string; muted?: boolean; current?: number; prev?: number }) {
  return (
    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", padding: "1px 0" }}>
      <span style={{ fontSize: "13px", color: muted ? "#8c9196" : "#6d7175" }}>{label}</span>
      <div style={{ display: "flex", alignItems: "center", gap: "10px" }}>
        <span style={{ fontSize: "13px", fontWeight: 600, color: muted ? "#8c9196" : "#1a1a1a", fontVariantNumeric: "tabular-nums" }}>
          {value}
        </span>
        {current !== undefined && prev !== undefined && (
          <span style={{ minWidth: "32px", textAlign: "right" }}>
            <WowBadge current={current} prev={prev} />
          </span>
        )}
      </div>
    </div>
  );
}

// ── Day Tile ──
function DayTile({ title, subtitle, data, prevData, currency, highlight }: {
  title: string; subtitle: string; data: DayData; prevData: DayData; currency: string; highlight?: boolean;
}) {
  const aov = data.adOrders > 0 ? data.adRevenue / data.adOrders : 0;
  const blendedRevenue = data.adRevenue + data.unmatchedRevenue;
  const blendedRoas = data.adSpend > 0 ? blendedRevenue / data.adSpend : 0;
  const newAov = data.newOrders > 0 ? data.newRevenue / data.newOrders : 0;
  const newRoas = data.adSpend > 0 ? data.newRevenue / data.adSpend : 0;

  // Only show WoW comparison badges on the Weekly Total tile (highlight=true),
  // not on individual day tiles.
  const showCompare = !!highlight;
  const prevBlendedRevenue = prevData.adRevenue + prevData.unmatchedRevenue;
  const prevTotalOrders = prevData.adOrders + prevData.unmatchedConversions;
  const prevBlendedRoas = showCompare && prevData.adSpend > 0 ? prevBlendedRevenue / prevData.adSpend : undefined;
  const prevAov = showCompare && prevTotalOrders > 0 ? prevBlendedRevenue / prevTotalOrders : undefined;
  const prevNewAov = showCompare && prevData.newOrders > 0 ? prevData.newRevenue / prevData.newOrders : undefined;
  const prevNewRoas = showCompare && prevData.adSpend > 0 ? prevData.newRevenue / prevData.adSpend : undefined;

  return (
    <div style={{
      background: highlight ? "#faf8ff" : "#fff",
      border: highlight ? "2px solid #7c3aed" : "1px solid #e1e3e5",
      borderRadius: "10px",
      padding: "16px",
      flex: "1 1 0",
      minWidth: 0,
    }}>
      <div style={{ marginBottom: "8px" }}>
        <span style={{ fontSize: "14px", fontWeight: 700, color: highlight ? "#7c3aed" : "#1a1a1a" }}>{title}</span>
        <span style={{ fontSize: "11px", color: "#8c9196", marginLeft: "6px" }}>{subtitle}</span>
      </div>

      <MetricRow label="Store Revenue (Gross)" value={fmtCurrency(data.storeRevenue, currency)} current={data.storeRevenue} prev={showCompare ? prevData.storeRevenue : undefined} />
      <MetricRow label="Ad Spend" value={fmtCurrency(data.adSpend, currency)} current={data.adSpend} prev={showCompare ? prevData.adSpend : undefined} />

      <div style={{ height: "8px" }} />
      <div style={{ fontSize: "11px", fontWeight: 600, color: "#e67e22", textTransform: "uppercase", letterSpacing: "0.5px", marginBottom: "4px" }}>All Customers</div>
      <MetricRow label="Orders" value={String(data.adOrders + data.unmatchedConversions)} current={data.adOrders + data.unmatchedConversions} prev={showCompare ? prevData.adOrders + prevData.unmatchedConversions : undefined} />
      <MetricRow label="Revenue" value={fmtCurrency(blendedRevenue, currency)} current={blendedRevenue} prev={showCompare ? prevData.adRevenue + prevData.unmatchedRevenue : undefined} />
      <MetricRow label="AOV" value={(data.adOrders + data.unmatchedConversions) > 0 ? fmtCurrency(blendedRevenue / (data.adOrders + data.unmatchedConversions), currency) : "—"} current={(data.adOrders + data.unmatchedConversions) > 0 ? blendedRevenue / (data.adOrders + data.unmatchedConversions) : 0} prev={prevAov} />
      <MetricRow label="ROAS" value={data.adSpend > 0 ? fmtRoas(blendedRoas) : "—"} current={blendedRoas} prev={prevBlendedRoas} />

      <div style={{ height: "8px" }} />
      <div style={{ fontSize: "11px", fontWeight: 600, color: "#7c3aed", textTransform: "uppercase", letterSpacing: "0.5px", marginBottom: "4px" }}>New Customers</div>
      <MetricRow label="Orders" value={String(data.newOrders)} muted={data.newOrders === 0} current={data.newOrders} prev={showCompare ? prevData.newOrders : undefined} />
      <MetricRow label="Revenue" value={fmtCurrency(data.newRevenue, currency)} muted={data.newOrders === 0} current={data.newRevenue} prev={showCompare ? prevData.newRevenue : undefined} />
      <MetricRow label="AOV" value={data.newOrders > 0 ? fmtCurrency(newAov, currency) : "—"} muted={data.newOrders === 0} current={newAov} prev={prevNewAov} />
      <MetricRow label="ROAS" value={data.adSpend > 0 && data.newOrders > 0 ? fmtRoas(newRoas) : "—"} muted={data.newOrders === 0} current={newRoas} prev={prevNewRoas} />
    </div>
  );
}

// ── Section Tile (generic card) ──
function SectionTile({ title, titleColor, children }: { title: string; titleColor?: string; children: React.ReactNode }) {
  return (
    <div style={{ background: "#fff", border: "1px solid #e1e3e5", borderRadius: "10px", padding: "20px" }}>
      <div style={{ fontSize: "14px", fontWeight: 700, color: titleColor || "#1a1a1a", marginBottom: "12px", textTransform: "uppercase", letterSpacing: "0.5px" }}>{title}</div>
      {children}
    </div>
  );
}

// ── Mini table row ──
function TableRow({ cells, bold, wow }: { cells: React.ReactNode[]; bold?: boolean; wow?: React.ReactNode }) {
  return (
    <div style={{ display: "flex", padding: "3px 0", borderBottom: "1px solid #f0f0f0", alignItems: "center" }}>
      {cells.map((cell, i) => (
        <div key={i} style={{
          flex: i === 0 ? 2 : 1,
          minWidth: 0,
          fontSize: "12px",
          lineHeight: 1.3,
          fontWeight: bold ? 700 : 400,
          color: bold ? "#1a1a1a" : "#6d7175",
          textAlign: i === 0 ? "left" : "right",
          fontVariantNumeric: "tabular-nums",
          overflow: "hidden",
          textOverflow: "ellipsis",
          whiteSpace: "nowrap",
        }}>{cell}</div>
      ))}
      {/* Optional WoW badge in its own fixed-width column */}
      <div style={{ width: "44px", flexShrink: 0, textAlign: "right", paddingLeft: "8px" }}>
        {wow || null}
      </div>
    </div>
  );
}

// ── Ad Performance Table Section ──
function AdSection({ title, ads, currency, color }: { title: string; ads: AdPerf[]; currency: string; color: string }) {
  if (ads.length === 0) return (
    <div style={{ marginBottom: "16px" }}>
      <div style={{ fontSize: "12px", fontWeight: 600, color, textTransform: "uppercase", letterSpacing: "0.5px", marginBottom: "6px" }}>{title}</div>
      <div style={{ fontSize: "13px", color: "#8c9196", fontStyle: "italic" }}>No data this week</div>
    </div>
  );
  return (
    <div style={{ marginBottom: "16px" }}>
      <div style={{ fontSize: "12px", fontWeight: 600, color, textTransform: "uppercase", letterSpacing: "0.5px", marginBottom: "6px" }}>{title}</div>
      <TableRow cells={["Ad", "Orders", "Revenue", "Spend", "ROAS"]} bold />
      {ads.map((ad, i) => (
        <TableRow key={i} cells={[
          ad.adName,
          String(ad.orders),
          fmtCurrency(ad.revenue, currency),
          fmtCurrency(ad.spend, currency),
          ad.spend > 0 ? fmtRoas(ad.revenue / ad.spend) : "—",
        ]} />
      ))}
    </div>
  );
}

// ── Summary generator ──
type NewAd = { adName: string; adId: string; orders: number; revenue: number; spend: number };
function generateSummary(totals: DayData, prevTotals: DayData, currency: string, geoNew: GeoRow[], topAdsNew: AdPerf[], topAdsExisting: AdPerf[], newlyLaunchedAds: NewAd[]): string[] {
  const points: string[] = [];
  const sym = currency === "GBP" ? "£" : currency === "USD" ? "$" : "€";
  const f = (v: number) => `${sym}${Math.round(v).toLocaleString("en-GB")}`;

  // Revenue WoW
  const revPct = wowPct(totals.storeRevenue, prevTotals.storeRevenue);
  if (revPct !== null) {
    points.push(`Store revenue ${revPct >= 0 ? "up" : "down"} ${Math.abs(revPct)}% WoW (${f(totals.storeRevenue)} vs ${f(prevTotals.storeRevenue)} last week).`);
  }

  // Ad performance (blended: matched + unmatched combined)
  const totalAdOrders = totals.adOrders + totals.unmatchedConversions;
  const totalAdRevenue = totals.adRevenue + totals.unmatchedRevenue;
  if (totalAdOrders > 0) {
    const roas = totals.adSpend > 0 ? (totalAdRevenue / totals.adSpend).toFixed(1) : "N/A";
    points.push(`${totalAdOrders} ad orders generating ${f(totalAdRevenue)} revenue. ROAS: ${roas}x.`);
  }

  // New customers
  if (totals.newOrders > 0) {
    const newPct = wowPct(totals.newOrders, prevTotals.newOrders);
    const newNote = newPct !== null ? ` (${newPct >= 0 ? "+" : ""}${newPct}% WoW)` : "";
    const cpa = totals.adSpend > 0 ? f(Math.round(totals.adSpend / totals.newOrders)) : "N/A";
    points.push(`${totals.newOrders} new customers acquired via Meta${newNote}. CPA: ${cpa}.`);
  }

  // Repeat + retargeted (existing customers)
  const existingOrders = totals.repeatOrders + totals.retargetedOrders;
  const existingRevenue = totals.repeatRevenue + totals.retargetedRevenue;
  const prevExistingRevenue = prevTotals.repeatRevenue + prevTotals.retargetedRevenue;
  if (existingOrders > 0) {
    const existPct = wowPct(existingRevenue, prevExistingRevenue);
    const existNote = existPct !== null ? ` (${existPct >= 0 ? "+" : ""}${existPct}% WoW)` : "";
    points.push(`${existingOrders} repeat/existing Meta customers generated ${f(existingRevenue)}${existNote}.`);
  }

  // Spend WoW
  const spendPct = wowPct(totals.adSpend, prevTotals.adSpend);
  if (spendPct !== null && Math.abs(spendPct) >= 10) {
    points.push(`Ad spend ${spendPct >= 0 ? "increased" : "decreased"} ${Math.abs(spendPct)}% WoW (${f(totals.adSpend)}).`);
  }

  // Top geo
  if (geoNew.length > 0) {
    const totalNewRev = geoNew.reduce((s, r) => s + r.revenue, 0);
    const topGeo = geoNew[0];
    const pctRev = totalNewRev > 0 ? Math.round((topGeo.revenue / totalNewRev) * 100) : 0;
    points.push(`Top new customer market: ${topGeo.country} (${topGeo.orders} orders, ${f(topGeo.revenue)}, ${pctRev}% of total revenue).`);
  }

  // Best performing new customer ad
  if (topAdsNew.length > 0) {
    const best = topAdsNew[0];
    points.push(`Best new customer ad: "${best.adName.length > 40 ? best.adName.slice(0, 38) + "…" : best.adName}" — ${best.orders} orders, ${f(best.revenue)}.`);
  }

  // Best existing customer ad
  if (topAdsExisting.length > 0) {
    const best = topAdsExisting[0];
    points.push(`Best existing customer ad: "${best.adName.length > 40 ? best.adName.slice(0, 38) + "…" : best.adName}" — ${best.orders} orders, ${f(best.revenue)}.`);
  }

  // AOV comparison
  const totalOrders = totalAdOrders;
  if (totalOrders > 0) {
    const aov = totalAdRevenue / totalOrders;
    const prevTotalAdOrders = prevTotals.adOrders + prevTotals.unmatchedConversions;
    const prevTotalAdRevenue = prevTotals.adRevenue + prevTotals.unmatchedRevenue;
    const prevAov = prevTotalAdOrders > 0 ? prevTotalAdRevenue / prevTotalAdOrders : 0;
    const aovPct = wowPct(aov, prevAov);
    const aovNote = aovPct !== null ? ` (${aovPct >= 0 ? "+" : ""}${aovPct}% WoW)` : "";
    points.push(`Average order value: ${f(aov)}${aovNote}.`);
  }

  // New vs existing customer split
  if (totals.newOrders > 0 && existingOrders > 0) {
    const newPct = Math.round((totals.newOrders / totalAdOrders) * 100);
    points.push(`Customer split: ${newPct}% new, ${100 - newPct}% existing.`);
  }

  // Newly launched ads this week
  if (newlyLaunchedAds.length > 0) {
    for (const ad of newlyLaunchedAds) {
      const adLabel = ad.adName.length > 40 ? ad.adName.slice(0, 38) + "…" : ad.adName;
      if (ad.orders > 0) {
        const roas = ad.spend > 0 ? (ad.revenue / ad.spend).toFixed(1) : "N/A";
        points.push(`New ad launched: "${adLabel}" — ${ad.orders} orders, ${f(ad.revenue)}, ROAS: ${roas}x.`);
      } else {
        points.push(`New ad launched: "${adLabel}" — no orders yet, spend ${f(ad.spend)}.`);
      }
    }
  }

  if (points.length === 0) {
    points.push("No significant activity this week.");
  }

  return points;
}

// ══════════════════════════════════════════════════
// Component
// ══════════════════════════════════════════════════
export default function WeeklyReport() {
  const { monday, sunday, days, prevDays, dateLabels, currency, geoNew, productNew, countrySpend, topAdsNew, topAdsExisting, newlyLaunchedAds } = useLoaderData<typeof loader>();
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();

  const mondayDate = new Date(monday + "T12:00:00");
  const sundayDate = new Date(sunday + "T12:00:00");
  const weekLabel = `${fmtDate(mondayDate)} – ${fmtDate(sundayDate)} ${sundayDate.getFullYear()}`;

  const totals = sumDays(days);
  const prevTotals = sumDays(prevDays);

  const summaryPoints = generateSummary(totals, prevTotals, currency, geoNew, topAdsNew, topAdsExisting, newlyLaunchedAds);

  const navigateWeek = (direction: number) => {
    const m = new Date(monday + "T12:00:00");
    m.setDate(m.getDate() + direction * 7);
    navigate(`/app/weekly?week=${fmt(m)}`);
  };

  const jumpToWeek = (dateStr: string) => {
    if (!dateStr) return;
    navigate(`/app/weekly?week=${dateStr}`); // loader snaps any date to that week's Monday
  };

  return (
    <Page title="Weekly Report" fullWidth>
      <ReportTabs>
        {/* Print styles — 2 pages: summary+tiles on page 1, detail tables on page 2 */}
        <style>{`
          @media print {
            @page { size: landscape; margin: 10mm; }
            /* Hide Shopify chrome */
            .Polaris-Frame__Navigation, .Polaris-Frame__TopBar, .Polaris-TopBar,
            [class*="DateRange"], button, .Polaris-Button,
            .Polaris-Tabs, .Polaris-Frame__NavigationDismiss { display: none !important; }
            /* Full width */
            .Polaris-Frame__Content, .Polaris-Page, .Polaris-Page__Content,
            .Polaris-Frame__Main { max-width: 100% !important; padding: 0 !important; margin: 0 !important; }
            .Polaris-Frame { min-height: 0 !important; }
            /* Page break between page 1 and page 2 */
            .print-page-break { break-before: page; }
            /* Keep tiles together */
            [style*="border-radius: 10px"] { break-inside: avoid; }
            /* Show scrollable content fully */
            [style*="overflowY: auto"], [style*="overflow-y: auto"] { max-height: none !important; overflow: visible !important; }
          }
        `}</style>

        {/* Week navigation */}
        <div style={{ marginBottom: "20px" }}>
          <InlineStack align="center" blockAlign="center" gap="400">
            <Button onClick={() => navigateWeek(-1)} variant="plain">&larr; Prev Week</Button>
            <Text as="h2" variant="headingLg">{weekLabel}</Text>
            <Button onClick={() => navigateWeek(1)} variant="plain">Next Week &rarr;</Button>
            <input
              type="date"
              value={monday}
              max={fmt(new Date())}
              onChange={(e) => jumpToWeek(e.target.value)}
              aria-label="Jump to week"
              style={{
                padding: "6px 10px",
                border: "1px solid #d1d5db",
                borderRadius: "6px",
                fontSize: "13px",
                cursor: "pointer",
                background: "#fff",
              }}
            />
          </InlineStack>
        </div>

        {/* ── Weekly Summary (top of page) ── */}
        <div style={{ marginBottom: "16px" }}>
          <SectionTile title="Weekly Summary" titleColor="#16a34a">
            <ul style={{ margin: 0, paddingLeft: "18px" }}>
              {summaryPoints.map((point, i) => (
                <li key={i} style={{ fontSize: "13px", color: "#1a1a1a", lineHeight: 1.6, marginBottom: "4px" }}>{point}</li>
              ))}
            </ul>
          </SectionTile>
        </div>

        {/* ── Day Tiles: Top row Mon–Thu ── */}
        <div style={{ display: "flex", gap: "12px", marginBottom: "12px" }}>
          {[0, 1, 2, 3].map(i => (
            <DayTile
              key={i}
              title={SHORT_LABELS[i]}
              subtitle={fmtDate(new Date(dateLabels[i] + "T12:00:00"))}
              data={days[i]}
              prevData={prevDays[i]}
              currency={currency}
            />
          ))}
        </div>

        {/* ── Day Tiles: Bottom row Fri–Sun + Weekly Total ── */}
        <div style={{ display: "flex", gap: "12px", marginBottom: "24px" }}>
          {[4, 5, 6].map(i => (
            <DayTile
              key={i}
              title={SHORT_LABELS[i]}
              subtitle={fmtDate(new Date(dateLabels[i] + "T12:00:00"))}
              data={days[i]}
              prevData={prevDays[i]}
              currency={currency}
            />
          ))}
          <DayTile
            title="Weekly Total"
            subtitle={weekLabel}
            data={totals}
            prevData={prevTotals}
            currency={currency}
            highlight
          />
        </div>

        {/* ── PAGE 2 starts here when printing ── */}
        <div className="print-page-break" />

        {/* ── Row 1: New Customers by Geo + Ad Spend vs Revenue by Country ── */}
        <div style={{ display: "flex", gap: "12px", marginBottom: "12px", alignItems: "stretch" }}>
          <div style={{ flex: 1, display: "flex" }}>
            <div style={{ flex: 1 }}>
              <SectionTile title="New Customers by Geo" titleColor="#7c3aed">
                {geoNew.length === 0 ? (
                  <div style={{ fontSize: "12px", color: "#8c9196", fontStyle: "italic" }}>No new customer orders this week</div>
                ) : (() => {
                  const totalNewRev = geoNew.reduce((s, r) => s + r.revenue, 0);
                  return (
                    <>
                      <TableRow cells={["Country", "Orders", "Revenue", "% Rev"]} bold />
                      {geoNew.map((row: any, i: number) => (
                        <TableRow key={i} cells={[
                          row.country,
                          String(row.orders),
                          fmtCurrency(row.revenue, currency),
                          totalNewRev > 0 ? `${Math.round((row.revenue / totalNewRev) * 100)}%` : "—",
                        ]} wow={<WowBadge current={row.revenue} prev={row.prevRevenue} />} />
                      ))}
                      <TableRow cells={["Total", String(geoNew.reduce((s, r) => s + r.orders, 0)), fmtCurrency(totalNewRev, currency), "100%"]} bold />
                    </>
                  );
                })()}
              </SectionTile>
            </div>
          </div>

          <div style={{ flex: 1, display: "flex" }}>
            <div style={{ flex: 1 }}>
              <SectionTile title="Ad Spend vs Ad Revenue by Country">
                {countrySpend.length === 0 ? (
                  <div style={{ fontSize: "12px", color: "#8c9196", fontStyle: "italic" }}>No breakdown data this week</div>
                ) : (
                  <>
                    <TableRow cells={["Country", "Spend", "Orders", "Revenue", "ROAS"]} bold />
                    {countrySpend.map((row: any, i: number) => (
                      <TableRow key={i} cells={[
                        row.country,
                        fmtCurrency(row.spend, currency),
                        String(row.orders),
                        fmtCurrency(row.revenue, currency),
                        row.spend > 0 ? fmtRoas(row.revenue / row.spend) : "—",
                      ]} />
                    ))}
                  </>
                )}
              </SectionTile>
            </div>
          </div>
        </div>

        {/* ── Row 2: Product Purchases + Top Performing Ads ── */}
        <div style={{ display: "flex", gap: "12px", marginBottom: "12px", alignItems: "stretch" }}>
          <div style={{ flex: 1, display: "flex" }}>
            <div style={{ flex: 1, display: "flex", flexDirection: "column" }}>
              <SectionTile title="New Customer Product Purchases" titleColor="#7c3aed">
                <div style={{ maxHeight: "400px", overflowY: "auto" }}>
                  {productNew.length === 0 ? (
                    <div style={{ fontSize: "12px", color: "#8c9196", fontStyle: "italic" }}>No new customer orders this week</div>
                  ) : (
                    <>
                      <TableRow cells={["Product", "Qty", "Revenue"]} bold />
                      {productNew.map((row: any, i: number) => (
                        <TableRow key={i} cells={[
                          row.product,
                          String(row.orders),
                          fmtCurrency(row.revenue, currency),
                        ]} wow={<WowBadge current={row.revenue} prev={row.prevRevenue} />} />
                      ))}
                    </>
                  )}
                </div>
              </SectionTile>
            </div>
          </div>

          <div style={{ flex: 1, display: "flex" }}>
            <div style={{ flex: 1 }}>
              <SectionTile title="Top Performing Ads">
                <AdSection title="New Customers" ads={topAdsNew} currency={currency} color="#7c3aed" />
                <AdSection title="Existing Customers" ads={topAdsExisting} currency={currency} color="#e67e22" />
              </SectionTile>
            </div>
          </div>
        </div>

        {/* Weekly Summary moved to top of page */}

      </ReportTabs>
    </Page>
  );
}
