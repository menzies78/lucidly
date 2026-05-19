import { json } from "@remix-run/node";
import { useLoaderData, useSubmit, useActionData, useRevalidator } from "@remix-run/react";
import {
  Page, Card, Text, BlockStack,
  Popover, ActionList, Button,
} from "@shopify/polaris";
import ReportTabs from "../components/ReportTabs";
import TileGrid, { type TileDef } from "../components/TileGrid";
import SummaryTile from "../components/SummaryTile";
import CustomerMapExplorer, { type MapBlob } from "../components/CustomerMapExplorer";
import { useState, useMemo, useCallback } from "react";
import { authenticate } from "../shopify.server";
import db from "../db.server";
import { parseDateRange } from "../utils/dateRange.server";
import { currencySymbolFromCode } from "../utils/currency";
import { getCachedInsights, computeDataHash, generateInsights } from "../services/aiAnalysis.server";
import { setProgress, failProgress, completeProgress } from "../services/progress.server";
import AiInsightsPanel from "../components/AiInsightsPanel";
import PageSummary, { type SummaryBullet } from "../components/PageSummary";

// ═══════════════════════════════════════════════════════════════
// LOADER
// ═══════════════════════════════════════════════════════════════

export const loader = async ({ request }) => {
  const { session } = await authenticate.admin(request);
  const shopDomain = session.shop;

  const shop = await db.shop.findUnique({ where: { shopDomain } });
  const currencySymbol = currencySymbolFromCode(shop?.shopifyCurrency);

  const tz = shop?.shopifyTimezone || "UTC";
  const { fromDate, toDate, fromKey, toKey, preset } = parseDateRange(request, tz);

  // ── DailyGeoRollup is rebuilt at the end of every incremental sync. The
  // loader simply scans the window-of-interest and sums in JS. No raw
  // MetaBreakdown/Order/Attribution scans, no orderLineItem×customer joins.
  // Customer-map and top-products are pre-computed ShopAnalysisCache blobs
  // (all-time, page-level date filter intentionally doesn't apply per the
  // existing UX). ──
  const geoRows = await db.dailyGeoRollup.findMany({
    where: { shopDomain, date: { gte: fromDate, lte: toDate } },
  });

  const r2 = (v: number) => Math.round(v * 100) / 100;

  // ── Aggregate Meta spend by country (overall + per campaign/adset/ad) ──
  type GeoAgg = {
    country: string;
    spend: number; impressions: number; clicks: number; reach: number;
    metaConversions: number; metaConversionValue: number;
    linkClicks: number; landingPageViews: number;
    attributedOrders: number; attributedRevenue: number;
    newCustomerOrders: number; existingCustomerOrders: number;
    newCustomers: number;
    newCustomerRevenue: number; existingCustomerRevenue: number;
    unverifiedRevenue: number;
  };

  const makeAgg = (country: string): GeoAgg => ({
    country,
    spend: 0, impressions: 0, clicks: 0, reach: 0,
    metaConversions: 0, metaConversionValue: 0,
    linkClicks: 0, landingPageViews: 0,
    attributedOrders: 0, attributedRevenue: 0,
    newCustomerOrders: 0, existingCustomerOrders: 0,
    newCustomers: 0,
    newCustomerRevenue: 0, existingCustomerRevenue: 0,
    unverifiedRevenue: 0,
  });

  // Track unique new customers per country (overall) and per entity-country combo.
  // Sets are unioned across daily rollup rows from newCustomerIdsJson so that a
  // customer placing two new-orders in the window counts once.
  const newCustByCountry: Record<string, Set<string>> = {};
  const newCustByEntityCountry: Record<string, Set<string>> = {};

  // ── Overall country aggregation (Meta side) ──
  const overallByCountry: Record<string, GeoAgg> = {};

  // ── Per-entity country aggregation (Meta side) ──
  type EntityGeoAgg = GeoAgg & {
    entityId: string;
    entityName: string;
    campaignId?: string;
    campaignName?: string;
    adSetId?: string;
    adSetName?: string;
  };

  const entityGeo: Record<string, EntityGeoAgg> = {};

  const makeEntityKey = (level: string, entityId: string, country: string) =>
    `${level}:${entityId}:${country}`;

  // ── Sum daily rollup rows into the same overallByCountry/entityGeo maps the
  // legacy aggregation block produced. Net effect: identical downstream shape,
  // but the per-day partial-sums come pre-baked from the rollup rather than
  // re-computed from raw MetaBreakdown + Order + Attribution every page load. ──
  for (const r of geoRows) {
    const cc = r.country;
    if (r.level === "overall") {
      if (!overallByCountry[cc]) overallByCountry[cc] = makeAgg(cc);
      const row = overallByCountry[cc];
      row.spend += r.spend;
      row.impressions += r.impressions;
      row.clicks += r.clicks;
      row.reach += r.reach;
      row.metaConversions += r.metaConversions;
      row.metaConversionValue += r.metaConversionValue;
      row.linkClicks += r.linkClicks;
      row.landingPageViews += r.landingPageViews;
      row.attributedOrders += r.attributedOrders;
      row.attributedRevenue += r.attributedRevenue;
      row.newCustomerOrders += r.newCustomerOrders;
      row.newCustomerRevenue += r.newCustomerRevenue;
      row.existingCustomerOrders += r.existingCustomerOrders;
      row.existingCustomerRevenue += r.existingCustomerRevenue;
      row.unverifiedRevenue += r.unverifiedRevenue;
      try {
        const ids: string[] = JSON.parse(r.newCustomerIdsJson || "[]");
        if (ids.length) {
          if (!newCustByCountry[cc]) newCustByCountry[cc] = new Set();
          for (const id of ids) newCustByCountry[cc].add(id);
        }
      } catch { /* ignore malformed JSON */ }
    } else {
      const ek = makeEntityKey(r.level, r.entityId, cc);
      if (!entityGeo[ek]) {
        entityGeo[ek] = {
          ...makeAgg(cc),
          entityId: r.entityId,
          entityName: r.entityName || r.entityId,
          campaignId: r.campaignId || undefined,
          campaignName: r.campaignName || undefined,
          adSetId: r.adSetId || undefined,
          adSetName: r.adSetName || undefined,
        };
      }
      const row = entityGeo[ek];
      // Refresh denorm names from most-recent row (most-recent wins)
      if (r.entityName) row.entityName = r.entityName;
      if (r.campaignId) row.campaignId = r.campaignId;
      if (r.campaignName) row.campaignName = r.campaignName;
      if (r.adSetId) row.adSetId = r.adSetId;
      if (r.adSetName) row.adSetName = r.adSetName;
      row.spend += r.spend;
      row.impressions += r.impressions;
      row.clicks += r.clicks;
      row.reach += r.reach;
      row.metaConversions += r.metaConversions;
      row.metaConversionValue += r.metaConversionValue;
      row.linkClicks += r.linkClicks;
      row.landingPageViews += r.landingPageViews;
      row.attributedOrders += r.attributedOrders;
      row.attributedRevenue += r.attributedRevenue;
      row.newCustomerOrders += r.newCustomerOrders;
      row.newCustomerRevenue += r.newCustomerRevenue;
      row.existingCustomerOrders += r.existingCustomerOrders;
      row.existingCustomerRevenue += r.existingCustomerRevenue;
      row.unverifiedRevenue += r.unverifiedRevenue;
      try {
        const ids: string[] = JSON.parse(r.newCustomerIdsJson || "[]");
        if (ids.length) {
          if (!newCustByEntityCountry[ek]) newCustByEntityCountry[ek] = new Set();
          for (const id of ids) newCustByEntityCountry[ek].add(id);
        }
      } catch { /* ignore malformed JSON */ }
    }
  }

  // ── Stamp unique new customer counts onto aggregation objects ──
  for (const [cc, custSet] of Object.entries(newCustByCountry)) {
    if (overallByCountry[cc]) overallByCountry[cc].newCustomers = custSet.size;
  }
  for (const [ek, custSet] of Object.entries(newCustByEntityCountry)) {
    if (entityGeo[ek]) entityGeo[ek].newCustomers = custSet.size;
  }

  // ── Compute derived metrics and build output ──
  const computeRow = (agg: GeoAgg) => ({
    ...agg,
    spend: r2(agg.spend),
    attributedRevenue: r2(agg.attributedRevenue),
    newCustomerRevenue: r2(agg.newCustomerRevenue),
    existingCustomerRevenue: r2(agg.existingCustomerRevenue),
    metaConversionValue: r2(agg.metaConversionValue),
    unverifiedRevenue: r2(agg.unverifiedRevenue),
    blendedROAS: agg.spend > 0 ? r2((agg.attributedRevenue + agg.unverifiedRevenue) / agg.spend) : 0,
    ctr: agg.impressions > 0 ? r2((agg.clicks / agg.impressions) * 100) : 0,
    // CPA uses Meta-reported conversions (same breakdown row as the spend) so
    // numerator and denominator share semantics — both are "this audience
    // country, per Meta". Using Shopify attributedOrders here mixes audience
    // country (numerator) with shipping country (denominator), which on
    // HM-style shops produced artificially low UK CPA because UK shipping
    // dominated the denominator while only a fraction of spend got country-
    // tagged by Meta. metaConversions also lets the tile rank countries
    // consistently regardless of how matching is going on the Shopify side.
    cpa: agg.metaConversions > 0 ? r2(agg.spend / agg.metaConversions) : 0,
    newCustomerCPA: agg.newCustomers > 0 ? r2(agg.spend / agg.newCustomers) : null,
    aov: agg.attributedOrders > 0 ? r2(agg.attributedRevenue / agg.attributedOrders) : null,
    spendPct: 0,
  });

  const overallRows = Object.values(overallByCountry)
    .map(computeRow)
    .sort((a, b) => {
      // Push unknown/XX to the bottom
      const aUnk = !a.country || a.country === "XX" || a.country === "unknown" ? 1 : 0;
      const bUnk = !b.country || b.country === "XX" || b.country === "unknown" ? 1 : 0;
      if (aUnk !== bUnk) return aUnk - bUnk;
      return b.spend - a.spend;
    });

  const totalSpend = overallRows.reduce((s, r) => s + r.spend, 0);
  for (const r of overallRows) {
    r.spendPct = totalSpend > 0 ? r2((r.spend / totalSpend) * 100) : 0;
  }

  // ── Build entity-level data ──
  type EntityData = {
    entityId: string;
    entityName: string;
    campaignId?: string;
    campaignName?: string;
    adSetId?: string;
    adSetName?: string;
    countries: any[];
    totalSpend: number;
    totalAttributedRevenue: number;
    totalAttributedOrders: number;
    totalNewCustomerOrders: number;
    totalNewCustomers: number;
    totalNewCustomerRevenue: number;
    totalExistingCustomerOrders: number;
    totalExistingCustomerRevenue: number;
    totalUnverifiedRevenue: number;
    totalImpressions: number;
    totalClicks: number;
  };

  const buildEntityLevel = (level: string) => {
    const entities: Record<string, EntityData> = {};
    const prefix = `${level}:`;

    for (const [key, agg] of Object.entries(entityGeo)) {
      if (!key.startsWith(prefix)) continue;
      const eid = agg.entityId;
      if (!entities[eid]) {
        entities[eid] = {
          entityId: eid,
          entityName: agg.entityName,
          campaignId: agg.campaignId,
          campaignName: agg.campaignName,
          adSetId: agg.adSetId,
          adSetName: agg.adSetName,
          countries: [],
          totalSpend: 0, totalAttributedRevenue: 0, totalAttributedOrders: 0,
          totalNewCustomerOrders: 0, totalNewCustomers: 0, totalNewCustomerRevenue: 0,
          totalExistingCustomerOrders: 0, totalExistingCustomerRevenue: 0,
          totalUnverifiedRevenue: 0, totalImpressions: 0, totalClicks: 0,
        };
      }
      const row = computeRow(agg);
      entities[eid].countries.push(row);
      entities[eid].totalSpend += agg.spend;
      entities[eid].totalAttributedRevenue += agg.attributedRevenue;
      entities[eid].totalAttributedOrders += agg.attributedOrders;
      entities[eid].totalNewCustomerOrders += agg.newCustomerOrders;
      entities[eid].totalNewCustomers += agg.newCustomers;
      entities[eid].totalNewCustomerRevenue += agg.newCustomerRevenue;
      entities[eid].totalExistingCustomerOrders += agg.existingCustomerOrders;
      entities[eid].totalExistingCustomerRevenue += agg.existingCustomerRevenue;
      entities[eid].totalUnverifiedRevenue += agg.unverifiedRevenue;
      entities[eid].totalImpressions += agg.impressions;
      entities[eid].totalClicks += agg.clicks;
    }

    for (const ent of Object.values(entities)) {
      ent.countries.sort((a, b) => b.spend - a.spend);
      for (const c of ent.countries) {
        c.spendPct = ent.totalSpend > 0 ? r2((c.spend / ent.totalSpend) * 100) : 0;
      }
    }

    return Object.values(entities).sort((a, b) => b.totalSpend - a.totalSpend);
  };

  const campaignEntities = buildEntityLevel("campaign");
  const adsetEntities = buildEntityLevel("adset");
  const adEntities = buildEntityLevel("ad");

  // ── All Shopify orders by country (for map "All Customers" scope) ──
  // Sum the per-day overall rollup rows. These are date-range scoped so the
  // page-level date selector applies here (unlike the topProductsByCountry
  // and customerMapBlob, which are all-time by design).
  const shopifyByCountry: Record<string, { orders: number; revenue: number }> = {};
  for (const r of geoRows) {
    if (r.level !== "overall") continue;
    const cc = r.country;
    if (!cc) continue;
    if (!shopifyByCountry[cc]) shopifyByCountry[cc] = { orders: 0, revenue: 0 };
    shopifyByCountry[cc].orders += r.shopifyOrders;
    shopifyByCountry[cc].revenue += r.shopifyRevenue;
  }

  // ── Top Products per Country (pre-computed all-time blob) ──
  // Page-level date filter intentionally NOT applied; the tile shows the
  // merchant's lifetime best sellers per country. See geoRollups.server.js
  // for the cube definition.
  const topProductsRow = await db.shopAnalysisCache.findUnique({
    where: { shopDomain_cacheKey: { shopDomain, cacheKey: "geo:topProducts" } },
    select: { payload: true },
  });
  const topProductsByCountry = (() => {
    if (!topProductsRow?.payload) return [];
    try {
      const parsed = JSON.parse(topProductsRow.payload);
      return parsed?.topProductsByCountry || [];
    } catch { return []; }
  })();

  // ── Customer Map Explorer blob (computed at rollup time) ──
  // The blob is all-time. The tile has its own time-window control (default
  // All time - page-level date filter intentionally NOT applied here so the
  // first paint shows full historic depth). If the rollup hasn't run yet,
  // the component renders an empty state.
  const customerMapRow = await db.shopAnalysisCache.findUnique({
    where: { shopDomain_cacheKey: { shopDomain, cacheKey: "customers:map" } },
    select: { payload: true },
  });
  const customerMapBlob = customerMapRow?.payload ? JSON.parse(customerMapRow.payload) : null;

  // ── AI Insights cache ──
  const dateFromStr = fromKey;
  const dateToStr = toKey;
  const aiCached = await getCachedInsights(shopDomain, "geo", dateFromStr, dateToStr);
  const aiCurrentHash = computeDataHash({ overallRows, shopifyByCountry });
  const aiCachedInsights = aiCached?.insights || null;
  const aiGeneratedAt = aiCached?.generatedAt?.toISOString() || null;
  const aiIsStale = aiCached ? aiCached.dataHash !== aiCurrentHash : false;

  // Protomaps API key - public referrer-restricted key, safe to send to the
  // browser. Falls back to null so the client uses the CARTO basemap in dev
  // before the secret is set. App Store launch requires this to be present.
  const protomapsKey = process.env.PROTOMAPS_API_KEY || null;

  return json({
    overallRows,
    campaignEntities,
    adsetEntities,
    adEntities,
    shopifyByCountry,
    customerMapBlob,
    topProductsByCountry,
    currencySymbol,
    protomapsKey,
    hasData: overallRows.some(r => r.spend > 0 || r.attributedOrders > 0),
    aiCachedInsights,
    aiGeneratedAt,
    aiIsStale,
    fromKey, toKey, preset,
  });
};

export const action = async ({ request }) => {
  const { session } = await authenticate.admin(request);
  const shopDomain = session.shop;
  const formData = await request.formData();
  const actionType = formData.get("actionType");

  if (actionType === "generateInsights") {
    const pageKey = String(formData.get("pageKey"));
    const taskId = `ai:${pageKey}:${shopDomain}`;
    const customSystem = formData.get("customSystemPrompt");
    const customPage = formData.get("customPagePrompt");
    const promptOverrides = (customSystem || customPage) ? { system: customSystem ? String(customSystem) : null, page: customPage ? String(customPage) : null } : null;

    setProgress(taskId, { status: "running", message: "Generating AI insights..." });

    (async () => {
      try {
        const shop = await db.shop.findUnique({ where: { shopDomain } });
        const tz = shop?.shopifyTimezone || "UTC";
        const { fromDate, toDate, fromKey: dateFromStr, toKey: dateToStr } = parseDateRange(request, tz);
        const cs = currencySymbolFromCode(shop?.shopifyCurrency);

        // Fetch geo data for AI
        const breakdownData = await db.metaBreakdown.findMany({
          where: { shopDomain, breakdownType: "country", date: { gte: fromDate, lte: toDate } },
        });
        const attributions = await db.attribution.findMany({ where: { shopDomain } });
        const allOrders = await db.order.findMany({ where: { shopDomain, isOnlineStore: true } });
        const orderMap = {};
        for (const o of allOrders) orderMap[o.shopifyOrderId] = o;

        // Aggregate by country
        const countryAgg = {};
        const aiNewCustByCountry: Record<string, Set<string>> = {};
        for (const b of breakdownData) {
          const cc = b.breakdownValue;
          if (!countryAgg[cc]) countryAgg[cc] = { country: cc, spend: 0, impressions: 0, clicks: 0, reach: 0, metaConversions: 0, attributedOrders: 0, attributedRevenue: 0, newCustomerOrders: 0, newCustomers: 0, newCustomerRevenue: 0, existingCustomerOrders: 0, existingCustomerRevenue: 0 };
          countryAgg[cc].spend += b.spend;
          countryAgg[cc].impressions += b.impressions;
          countryAgg[cc].clicks += b.clicks;
          countryAgg[cc].reach += b.reach;
          countryAgg[cc].metaConversions += b.conversions || 0;
        }

        // Add attribution revenue by country
        const ordersInRange = allOrders.filter(o => o.createdAt >= fromDate && o.createdAt <= toDate);
        const orderIdsInRange = new Set(ordersInRange.map(o => o.shopifyOrderId));
        for (const a of attributions) {
          if (a.confidence === 0 || !orderIdsInRange.has(a.shopifyOrderId)) continue;
          const order = orderMap[a.shopifyOrderId];
          if (!order) continue;
          const cc = order.countryCode || "XX";
          if (!countryAgg[cc]) continue;
          const rev = order.frozenTotalPrice - (order.totalRefunded || 0);
          countryAgg[cc].attributedOrders++;
          countryAgg[cc].attributedRevenue += rev;
          if (a.isNewCustomer) {
            countryAgg[cc].newCustomerOrders++;
            countryAgg[cc].newCustomerRevenue += rev;
            const custId = order.shopifyCustomerId;
            if (custId) {
              if (!aiNewCustByCountry[cc]) aiNewCustByCountry[cc] = new Set();
              aiNewCustByCountry[cc].add(custId);
            }
          } else {
            countryAgg[cc].existingCustomerOrders++;
            countryAgg[cc].existingCustomerRevenue += rev;
          }
        }

        // Stamp unique new customer counts
        for (const [cc, custSet] of Object.entries(aiNewCustByCountry)) {
          if (countryAgg[cc]) countryAgg[cc].newCustomers = custSet.size;
        }

        const overallRows = Object.values(countryAgg).map(c => ({
          ...c,
          blendedROAS: c.spend > 0 ? Math.round((c.attributedRevenue / c.spend) * 100) / 100 : 0,
          // CPA uses Meta-reported conversions (audience country, same
          // breakdown row as the spend) — see Part D fix above for why
          // attributedOrders (shipping country) is the wrong denominator.
          cpa: c.metaConversions > 0 ? Math.round((c.spend / c.metaConversions) * 100) / 100 : 0,
          newCustomerCPA: c.newCustomers > 0 ? Math.round((c.spend / c.newCustomers) * 100) / 100 : 0,
        })).sort((a, b) => b.spend - a.spend);

        // Shopify orders by country (for untapped markets)
        const shopifyByCountry = {};
        for (const o of ordersInRange) {
          const cc = o.countryCode || "XX";
          if (!shopifyByCountry[cc]) shopifyByCountry[cc] = { orders: 0, revenue: 0 };
          shopifyByCountry[cc].orders++;
          shopifyByCountry[cc].revenue += o.frozenTotalPrice - (o.totalRefunded || 0);
        }

        const pageData = { overallRows, shopifyByCountry, campaignEntities: [] };

        await generateInsights(shopDomain, pageKey, pageData, dateFromStr, dateToStr, cs, promptOverrides);
        completeProgress(taskId, { success: true });
      } catch (err) {
        console.error("[AI] Geo insights failed:", err);
        failProgress(taskId, err);
      }
    })();

    return json({ aiTaskId: taskId });
  }

  return json({});
};

// ═══════════════════════════════════════════════════════════════
// CONSTANTS & HELPERS
// ═══════════════════════════════════════════════════════════════

function countryName(code: string): string {
  if (!code || code === "XX") return "Unknown";
  try {
    return new Intl.DisplayNames(["en"], { type: "region" }).of(code) || code;
  } catch {
    return code;
  }
}

function countryFlag(code: string): string {
  if (!code || code.length !== 2) return "";
  try {
    return String.fromCodePoint(
      ...code.toUpperCase().split("").map(c => 0x1F1E6 + c.charCodeAt(0) - 65)
    );
  } catch {
    return "";
  }
}

function fmtCompact(v: number, cs: string): string {
  if (v >= 1000000) return `${cs}${(v / 1000000).toFixed(1)}M`;
  if (v >= 10000) return `${cs}${(v / 1000).toFixed(1)}k`;
  return `${cs}${Math.round(v).toLocaleString()}`;
}

function roasStyle(roas: number): { bg: string; text: string } {
  if (roas >= 5) return { bg: "#DCFCE7", text: "#166534" };
  if (roas >= 3) return { bg: "#ECFDF5", text: "#059669" };
  if (roas >= 1) return { bg: "#FEF9C3", text: "#854D0E" };
  return { bg: "#FEF2F2", text: "#DC2626" };
}

const TAB_LEVELS = ["all", "campaign", "adset", "ad"] as const;
const TAB_LABELS = ["All", "Campaigns", "Ad Sets", "Ads"];

const CUSTOMER_FILTERS = [
  { label: "All Customers", value: "all" },
  { label: "New Customers", value: "new" },
  { label: "Existing Customers", value: "existing" },
];

// ═══════════════════════════════════════════════════════════════
// CSS
// ═══════════════════════════════════════════════════════════════

const PAGE_STYLES = `
.geo-grid-3 { display: grid; grid-template-columns: repeat(3, 1fr); gap: 16px; }
@media (max-width: 900px) { .geo-grid-3 { grid-template-columns: 1fr; } }
.geo-tab { padding: 4px 10px; font-size: 11px; font-weight: 500; border: none; cursor: pointer; border-radius: 5px; transition: all 0.15s; white-space: nowrap; }
.geo-tab-active { background: #059669; color: #fff; }
.geo-tab-inactive { background: #fff; color: #374151; border: 1px solid #D1D5DB; }
.geo-tab-inactive:hover { background: #F3F4F6; }
.geo-toggle { padding: 4px 10px; font-size: 11px; font-weight: 500; border: none; cursor: pointer; border-radius: 5px; transition: all 0.15s; white-space: nowrap; }
.geo-toggle-active { background: #059669; color: #fff; }
.geo-toggle-inactive { background: #fff; color: #374151; border: 1px solid #D1D5DB; }
.geo-toggle-inactive:hover { background: #F3F4F6; }
`;

// ═══════════════════════════════════════════════════════════════
// VIPs per Country tile
//
// Surfaces which countries punch above their weight for high-spending
// customers. Reuses the customerMapBlob already loaded for the map
// explorer - every point carries a lifetime VIP band (5/10/20/none),
// a country code, and days-since-last-order. No server work needed.
//
// Headline metric is the over-index ratio:
//   actual_VIPs / expected_VIPs   (where expected = country_customers × global_VIP_rate)
// A 2.4× over-index reads "this country has 2.4× the share of VIPs you'd
// expect given its customer base" - the UAE-style geo opportunity signal.
//
// Time window mirrors CME: filters customers by recency-of-last-order
// (p.d ≤ window). VIP band itself stays lifetime (the tier is computed
// over a customer's full history at rollup time), which matches CME's
// existing semantics so band thresholds are consistent across tiles.
// ═══════════════════════════════════════════════════════════════

type VipBand = "top5" | "top10" | "top20";
type VipWindow = "all" | 30 | 90 | 180 | 365;

const VIP_BANDS: { value: VipBand; label: string }[] = [
  { value: "top5", label: "Top 5%" },
  { value: "top10", label: "Top 10%" },
  { value: "top20", label: "Top 20%" },
];

const VIP_WINDOWS: { key: VipWindow; label: string }[] = [
  { key: "all", label: "All time" },
  { key: 365, label: "365d" },
  { key: 180, label: "180d" },
  { key: 90, label: "90d" },
  { key: 30, label: "30d" },
];

// Lifetime VIP-band membership rule. Mirrors CME: top10 includes top5,
// top20 includes top10/top5. Anything in tier 0 is not a VIP at any band.
function pointInBand(v: 0 | 5 | 10 | 20, band: VipBand): boolean {
  if (v === 0) return false;
  if (band === "top5") return v === 5;
  if (band === "top10") return v === 5 || v === 10;
  return true; // top20 = any non-zero band
}

type CountryAgg = {
  cc: string;
  customers: number;
  vips: number;
  vipRevenue: number;     // sum of (net) lifetime spend across this country's VIPs
  expected: number;
  overIndex: number;
};

type VipScope = "metaAcquired" | "allMeta" | "all";

const VIP_SCOPES: { value: VipScope; label: string }[] = [
  { value: "metaAcquired", label: "Meta Acquired" },
  { value: "allMeta", label: "All Meta" },
  { value: "all", label: "All Customers" },
];

// The three headline tiles double as bar-chart selectors. Each "view"
// controls (a) which metric the bar length encodes, (b) sort order,
// (c) colour, and (d) the axis treatment - see viewData below.
type VipView = "overIndex" | "mostVips" | "highestAvg";

// Round a positive number up to a "nice" axis bound (1, 2, 5, 10, 20, 50, …).
// Keeps tick labels reading as round numbers regardless of the dataset.
function niceCeil(v: number): number {
  if (v <= 0) return 1;
  const exp = Math.floor(Math.log10(v));
  const base = Math.pow(10, exp);
  const mantissa = v / base;
  const niceMantissa = mantissa <= 1 ? 1 : mantissa <= 2 ? 2 : mantissa <= 5 ? 5 : 10;
  return niceMantissa * base;
}

// Compact number for axis labels: 1.2k, 12k, 1.5M.
function formatCompactNumber(v: number): string {
  if (v >= 1000000) return `${(v / 1000000).toFixed(v >= 10000000 ? 0 : 1)}M`;
  if (v >= 1000) return `${(v / 1000).toFixed(v >= 10000 ? 0 : 1)}k`;
  return Math.round(v).toLocaleString();
}

function VipsByCountryTile({ blob, cs }: { blob: MapBlob | null; cs: string }) {
  const [band, setBand] = useState<VipBand>("top5");
  const [vwindow, setVwindow] = useState<VipWindow>("all");
  // Scope mirrors CME: metaAcquired = "m" only, allMeta = "m"+"r",
  // all = "m"+"r"+"o". Default to "all" because this report's question
  // ("which geos have high spenders?") is broader than Meta-only -
  // organic VIPs are still relevant signal for where to lean creative.
  const [scope, setScope] = useState<VipScope>("all");
  // Default view = over-index (the headline insight). Clicking another
  // tile re-sorts the bar chart and switches what the bar length encodes.
  const [view, setView] = useState<VipView>("overIndex");

  // Statistical-significance floor on EXPECTED VIPs, not raw customer
  // count. The earlier customer-count floor still let through Romania
  // (4 vs 2) and Estonia (3 vs 2) - 4-on-2 reads "2× over-indexed" but
  // is well within Poisson noise. The chi-square rule of thumb requires
  // expected ≥ 5 for the test to be valid, so we use that as the cutoff.
  // This mirrors how a statistician would flag "this isn't enough data
  // to claim a real lift". Countries below the threshold are simply
  // not shown - we'd rather hide a real signal than promote a false one.
  const MIN_EXPECTED_VIPS = 5;

  const { rows, totals } = useMemo(() => {
    const points = blob?.points || [];
    const cutoff = vwindow === "all" ? null : (vwindow as number);
    const inWindow = (d: number | null) => cutoff == null ? true : (d != null && d <= cutoff);
    const inScope = (s: "m" | "r" | "o") => {
      if (scope === "metaAcquired") return s === "m";
      if (scope === "allMeta") return s === "m" || s === "r";
      return true;
    };

    const byCountry: Record<string, CountryAgg> = {};
    let totalCustomers = 0;
    let totalVips = 0;
    let totalVipRevenue = 0;

    for (const p of points) {
      if (!inWindow(p.d)) continue;
      if (!inScope(p.s)) continue;
      if (!p.c) continue; // can't attribute to a country
      totalCustomers++;
      const isVip = pointInBand(p.v, band);
      if (isVip) totalVips++;
      let row = byCountry[p.c];
      if (!row) {
        row = byCountry[p.c] = {
          cc: p.c, customers: 0, vips: 0, vipRevenue: 0, expected: 0, overIndex: 0,
        };
      }
      row.customers++;
      if (isVip) {
        row.vips++;
        const net = p.$ - p.r;
        row.vipRevenue += net;
        totalVipRevenue += net;
      }
    }

    const globalVipRate = totalCustomers > 0 ? totalVips / totalCustomers : 0;
    const list: CountryAgg[] = [];
    let countrySuppressedBelowFloor = 0;
    for (const r of Object.values(byCountry)) {
      r.expected = r.customers * globalVipRate;
      r.overIndex = r.expected > 0 ? r.vips / r.expected : 0;
      if (r.expected < MIN_EXPECTED_VIPS) {
        countrySuppressedBelowFloor++;
        continue;
      }
      list.push(r);
    }
    list.sort((a, b) => b.overIndex - a.overIndex);

    return {
      rows: list,
      totals: {
        customers: totalCustomers,
        vips: totalVips,
        vipRevenue: totalVipRevenue,
        rate: globalVipRate,
        suppressed: countrySuppressedBelowFloor,
      },
    };
  }, [blob, band, vwindow, scope]);

  // Headline tiles: most over-indexed country, country with most VIPs
  // (raw count), and highest avg VIP lifetime spend.
  const highlights = useMemo(() => {
    if (rows.length === 0) return null;
    const overIndexed = [...rows].filter(r => r.vips > 0).sort((a, b) => b.overIndex - a.overIndex)[0];
    const mostVips = [...rows].sort((a, b) => b.vips - a.vips)[0];
    const highestAvg = [...rows]
      .filter(r => r.vips >= 5)
      .map(r => ({ ...r, avg: r.vipRevenue / r.vips }))
      .sort((a, b) => b.avg - a.avg)[0];
    return { overIndexed, mostVips, highestAvg };
  }, [rows]);

  // Bar chart axis for the over-index view: clamp to a sensible upper
  // bound. 3× covers most real geo signals; anything over gets a label.
  const AXIS_MAX_OVER_INDEX = 3;

  // Per-view rendering data: which rows to show, what the bar value is,
  // axis max, primary/secondary right-side text. Computed once per (view,
  // rows) change so the bar render below stays declarative.
  const viewData = useMemo(() => {
    if (rows.length === 0) return null;

    if (view === "overIndex") {
      const sorted = [...rows].sort((a, b) => b.overIndex - a.overIndex);
      return {
        items: sorted,
        axisMax: AXIS_MAX_OVER_INDEX,
        valueOf: (r: CountryAgg) => Math.min(r.overIndex, AXIS_MAX_OVER_INDEX),
        rawValueOf: (r: CountryAgg) => r.overIndex,
        baselinePct: (1 / AXIS_MAX_OVER_INDEX) * 100,
        ticks: [0, 1, 2, 3].map(t => ({ pos: (t / AXIS_MAX_OVER_INDEX) * 100, label: `${t}×`, isBaseline: t === 1 })),
        primaryFor: (r: CountryAgg) => `${r.overIndex.toFixed(2)}×`,
        secondaryFor: (r: CountryAgg) => `${r.vips} vs ~${Math.round(r.expected)}`,
        colorFor: (r: CountryAgg) =>
          r.overIndex < 1 ? "#9CA3AF"
          : r.overIndex >= 2 ? "#10B981"
          : r.overIndex >= 1.3 ? "#34D399"
          : "#A7F3D0",
        primaryColorFor: (r: CountryAgg) => r.overIndex >= 1 ? "#065F46" : "#6B7280",
        clampLabel: (r: CountryAgg) =>
          r.overIndex > AXIS_MAX_OVER_INDEX ? `${r.overIndex.toFixed(1)}×` : null,
      };
    }

    if (view === "mostVips") {
      const sorted = [...rows].filter(r => r.vips > 0).sort((a, b) => b.vips - a.vips);
      const maxVal = sorted[0]?.vips || 1;
      // Round axis to a nice round number so ticks read cleanly.
      const axisMax = niceCeil(maxVal);
      const tickCount = 4;
      return {
        items: sorted,
        axisMax,
        valueOf: (r: CountryAgg) => r.vips,
        rawValueOf: (r: CountryAgg) => r.vips,
        baselinePct: null,
        ticks: Array.from({ length: tickCount + 1 }, (_, i) => {
          const v = (i / tickCount) * axisMax;
          return { pos: (v / axisMax) * 100, label: Math.round(v).toLocaleString(), isBaseline: false };
        }),
        primaryFor: (r: CountryAgg) => `${r.vips.toLocaleString()}`,
        secondaryFor: (r: CountryAgg) => `of ${r.customers.toLocaleString()} customers`,
        colorFor: (): string => "#7C3AED",
        primaryColorFor: (): string => "#5B21B6",
        clampLabel: (): string | null => null,
      };
    }

    // highestAvg
    const withAvg = rows
      // Match the chi-square sample-size floor used elsewhere in this tile:
      // averaging across <5 VIPs is too noisy (a single high-net spender
      // skews the result). Mirrors the statistical-significance floor on
      // expected VIPs - same standard, applied to actuals.
      .filter(r => r.vips >= 5)
      .map(r => ({ ...r, avg: r.vipRevenue / r.vips }));
    const sorted = withAvg.sort((a, b) => b.avg - a.avg);
    if (sorted.length === 0) return {
      items: [], axisMax: 1,
      valueOf: (): number => 0, rawValueOf: (): number => 0,
      baselinePct: null, ticks: [],
      primaryFor: (): string => "-", secondaryFor: (): string => "",
      colorFor: (): string => "#F59E0B", primaryColorFor: (): string => "#92400E",
      clampLabel: (): string | null => null,
      empty: "Need a country with at least 5 VIPs to compare averages.",
    };
    const maxVal = sorted[0].avg;
    const axisMax = niceCeil(maxVal);
    const tickCount = 4;
    return {
      items: sorted,
      axisMax,
      valueOf: (r: any) => r.avg as number,
      rawValueOf: (r: any) => r.avg as number,
      baselinePct: null,
      ticks: Array.from({ length: tickCount + 1 }, (_, i) => {
        const v = (i / tickCount) * axisMax;
        return { pos: (v / axisMax) * 100, label: `${cs}${formatCompactNumber(v)}`, isBaseline: false };
      }),
      primaryFor: (r: any) => `${cs}${Math.round(r.avg).toLocaleString()}`,
      secondaryFor: (r: any) => `${r.vips} VIPs`,
      colorFor: (): string => "#F59E0B",
      primaryColorFor: (): string => "#92400E",
      clampLabel: (): string | null => null,
    };
  }, [rows, view, cs]);

  if (!blob) {
    return (
      <Card>
        <BlockStack gap="200">
          <Text as="h2" variant="headingMd">VIPs per Country</Text>
          <Text as="p" variant="bodySm" tone="subdued">
            VIP analysis becomes available once the customer map blob has finished building.
          </Text>
        </BlockStack>
      </Card>
    );
  }

  return (
    <Card>
      <BlockStack gap="300">
        <BlockStack gap="050">
          <Text as="h2" variant="headingMd">VIPs per Country</Text>
          <Text as="p" variant="bodySm" tone="subdued">
            Where your highest-spending customers come from. Over-index of 1.0× means a country has its fair share of VIPs;
            anything above means it punches above its weight - a strong signal for where to lean in.
          </Text>
        </BlockStack>

        {/* Filter row: scope first (matches CME's left-to-right ordering -
            scope is the dominant filter because it changes the population),
            then time window, then VIP band. */}
        <div style={{ display: "flex", flexWrap: "wrap", gap: 14, alignItems: "center" }}>
          <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
            <span style={{ fontSize: 11, color: "#6B7280", fontWeight: 600, textTransform: "uppercase", letterSpacing: 0.4 }}>Customers</span>
            {VIP_SCOPES.map(s => (
              <button key={s.value} onClick={() => setScope(s.value)} className={`l-pill${scope === s.value ? " l-pill--active" : ""}`}>
                {s.label}
              </button>
            ))}
          </div>
          <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
            <span style={{ fontSize: 11, color: "#6B7280", fontWeight: 600, textTransform: "uppercase", letterSpacing: 0.4 }}>Window</span>
            {VIP_WINDOWS.map(w => (
              <button key={String(w.key)} onClick={() => setVwindow(w.key)} className={`l-pill${vwindow === w.key ? " l-pill--active" : ""}`}>
                {w.label}
              </button>
            ))}
          </div>
          <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
            <span style={{ fontSize: 11, color: "#6B7280", fontWeight: 600, textTransform: "uppercase", letterSpacing: 0.4 }}>VIPs</span>
            {VIP_BANDS.map(b => (
              <button key={b.value} onClick={() => setBand(b.value)} className={`l-pill${band === b.value ? " l-pill--active" : ""}`}>
                {b.label}
              </button>
            ))}
          </div>
          {totals.customers > 0 && (
            <span style={{ fontSize: 11, color: "#9CA3AF", marginLeft: "auto" }}>
              {totals.vips.toLocaleString()} VIPs across {totals.customers.toLocaleString()} customers ({(totals.rate * 100).toFixed(1)}%)
              {totals.suppressed > 0 ? ` · ${totals.suppressed} small countr${totals.suppressed === 1 ? "y" : "ies"} hidden` : ""}
            </span>
          )}
        </div>

        {/* Headline strip - 3 mini-tiles, each acts as a chart-view selector.
            Default selection = Most over-indexed. Clicking a tile re-sorts
            the bars and switches the metric the bar length encodes.
            Hover on non-selected tiles semi-highlights to show clickability. */}
        {highlights ? (
          <div style={{ display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: 12 }}>
            <HighlightTile
              label="Most over-indexed"
              cc={highlights.overIndexed?.cc}
              value={highlights.overIndexed ? `${highlights.overIndexed.overIndex.toFixed(1)}×` : "-"}
              sub={highlights.overIndexed ? `${highlights.overIndexed.vips} VIPs · ~${Math.round(highlights.overIndexed.expected)} expected` : ""}
              accent="#10B981"
              selected={view === "overIndex"}
              onClick={() => setView("overIndex")}
            />
            <HighlightTile
              label="Most VIPs"
              cc={highlights.mostVips?.cc}
              value={highlights.mostVips ? highlights.mostVips.vips.toLocaleString() : "-"}
              sub={highlights.mostVips ? `${highlights.mostVips.customers.toLocaleString()} customers in country` : ""}
              accent="#7C3AED"
              selected={view === "mostVips"}
              onClick={() => setView("mostVips")}
            />
            <HighlightTile
              label="Highest avg VIP spend"
              cc={highlights.highestAvg?.cc}
              value={highlights.highestAvg ? `${cs}${Math.round(highlights.highestAvg.vipRevenue / highlights.highestAvg.vips).toLocaleString()}` : "-"}
              sub={highlights.highestAvg ? `${highlights.highestAvg.vips} VIPs in country` : "Need 5+ VIPs in a country"}
              accent="#F59E0B"
              selected={view === "highestAvg"}
              onClick={() => setView("highestAvg")}
            />
          </div>
        ) : null}

        {/* Bar chart - rendering driven by the selected view (over-index /
            most VIPs / highest avg spend). Each row: flag + name on the
            left, bar in the middle, primary value + secondary detail on
            the right. The over-index view also draws a 1.0× baseline. */}
        {rows.length === 0 || !viewData ? (
          <div style={{ padding: 28, textAlign: "center", color: "#6B7280", fontSize: 13, background: "#F9FAFB", borderRadius: 8 }}>
            No country has enough customers for a statistically meaningful comparison (need at least ~{MIN_EXPECTED_VIPS / Math.max(totals.rate, 0.001) | 0} customers per country at this VIP band). Try a wider window or a broader VIP band.
          </div>
        ) : (viewData.items.length === 0) ? (
          <div style={{ padding: 28, textAlign: "center", color: "#6B7280", fontSize: 13, background: "#F9FAFB", borderRadius: 8 }}>
            {(viewData as any).empty || "No data for this view."}
          </div>
        ) : (
          <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
            {/* Axis ticks - rendered via positioned absolute elements over
                a track-width spacer so they line up with the bar tracks. */}
            <div style={{ position: "relative", height: 16, marginLeft: 200, marginRight: 110 }}>
              {viewData.ticks.map((t, i) => (
                <div
                  key={i}
                  style={{
                    position: "absolute",
                    left: `${t.pos}%`,
                    top: 0,
                    transform: "translateX(-50%)",
                    fontSize: 10,
                    color: t.isBaseline ? "#6B7280" : "#9CA3AF",
                    fontWeight: t.isBaseline ? 700 : 500,
                  }}
                >
                  {t.label}
                </div>
              ))}
            </div>
            {viewData.items.map((r: any) => {
              const value = viewData.valueOf(r);
              const widthPct = (value / viewData.axisMax) * 100;
              const barColor = viewData.colorFor(r);
              const primaryColor = viewData.primaryColorFor(r);
              const clamp = viewData.clampLabel(r);
              return (
                <div key={r.cc} style={{ display: "flex", alignItems: "center", gap: 10, fontSize: 12 }}>
                  {/* Left: flag + name. Fixed width so bars align across rows. */}
                  <div style={{
                    width: 190, flexShrink: 0,
                    display: "flex", alignItems: "center", gap: 8,
                    overflow: "hidden", whiteSpace: "nowrap", textOverflow: "ellipsis",
                  }}>
                    <span style={{ fontSize: 18, lineHeight: 1, flexShrink: 0 }}>{countryFlag(r.cc)}</span>
                    <span style={{ fontWeight: 600, color: "#1F2937", overflow: "hidden", textOverflow: "ellipsis" }} title={countryName(r.cc)}>
                      {countryName(r.cc)}
                    </span>
                  </div>

                  {/* Middle: bar track. Baseline marker shown only on
                      views that have a meaningful baseline (over-index). */}
                  <div style={{ flex: 1, position: "relative", height: 22, background: "#F3F4F6", borderRadius: 4, overflow: "hidden" }}>
                    <div style={{
                      position: "absolute", left: 0, top: 0, bottom: 0,
                      width: `${widthPct}%`,
                      background: barColor,
                      transition: "width 0.3s ease, background 0.2s ease",
                    }} />
                    {viewData.baselinePct != null && (
                      <div style={{
                        position: "absolute",
                        left: `${viewData.baselinePct}%`, top: 0, bottom: 0,
                        width: 1, background: "#6B7280",
                      }} />
                    )}
                    {clamp && (
                      <div style={{
                        position: "absolute", right: 4, top: "50%", transform: "translateY(-50%)",
                        fontSize: 10, fontWeight: 700, color: "#fff",
                      }}>
                        {clamp}
                      </div>
                    )}
                  </div>

                  {/* Right: primary value + secondary detail. */}
                  <div style={{
                    width: 100, flexShrink: 0, textAlign: "right",
                    display: "flex", flexDirection: "column", gap: 1,
                  }}>
                    <span style={{ fontWeight: 700, color: primaryColor, fontSize: 13 }}>
                      {viewData.primaryFor(r)}
                    </span>
                    <span style={{ fontSize: 10, color: "#9CA3AF" }}>
                      {viewData.secondaryFor(r)}
                    </span>
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </BlockStack>
    </Card>
  );
}

// Headline mini-tile used in the VIPs-per-Country strip. Flag on the left,
// big number on the right, supporting count below. Doubles as a chart-view
// selector: when selected, the tile uses its accent colour for the border
// and a tinted background; when not selected, hover reveals a softer
// border + lift to signal the tile is clickable.
function HighlightTile({
  label, cc, value, sub, accent, selected, onClick,
}: {
  label: string; cc: string | undefined; value: string; sub: string; accent: string;
  selected: boolean; onClick: () => void;
}) {
  const [hovered, setHovered] = useState(false);

  // Border colour: solid accent when selected; mid-emerald when hovered
  // (echoing the page palette); default neutral grey otherwise.
  const borderColor = selected
    ? accent
    : hovered
    ? "#9CA3AF"
    : "#E5E7EB";
  const borderWidth = selected ? 2 : 1;
  const padding = selected ? 13 : 14; // compensate for the 2px selected border so layout doesn't jump
  const bg = selected
    // 12% accent tint - light enough to read text against, distinct enough
    // to broadcast "this view is active" at a glance.
    ? `${accent}1F`
    : "#fff";
  const boxShadow = selected
    ? `0 0 0 1px ${accent}33`
    : hovered
    ? "0 1px 3px rgba(0,0,0,0.08)"
    : "none";

  return (
    <button
      type="button"
      onClick={onClick}
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
      aria-pressed={selected}
      style={{
        border: `${borderWidth}px solid ${borderColor}`,
        borderRadius: 12,
        padding,
        background: bg,
        boxShadow,
        cursor: "pointer",
        textAlign: "left",
        display: "flex", alignItems: "center", gap: 12, minHeight: 78,
        width: "100%",
        transition: "border-color 0.15s ease, background 0.15s ease, box-shadow 0.15s ease",
        font: "inherit",
      }}
    >
      {cc ? (
        <span style={{ fontSize: 40, lineHeight: 1, flexShrink: 0 }}>{countryFlag(cc)}</span>
      ) : (
        <span style={{ fontSize: 40, lineHeight: 1, flexShrink: 0, color: "#E5E7EB" }}>·</span>
      )}
      <div style={{ minWidth: 0, flex: 1 }}>
        <div style={{ fontSize: 10, fontWeight: 600, textTransform: "uppercase", letterSpacing: 0.4, color: "#6B7280" }}>{label}</div>
        <div style={{ fontSize: 22, fontWeight: 700, color: accent, lineHeight: 1.15, marginTop: 2 }}>{value}</div>
        {cc && <div style={{ fontSize: 11, color: "#374151", marginTop: 1, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }} title={countryName(cc)}>{countryName(cc)}</div>}
        {sub && <div style={{ fontSize: 10, color: "#9CA3AF", marginTop: 1 }}>{sub}</div>}
      </div>
    </button>
  );
}

// ═══════════════════════════════════════════════════════════════
// Top Products per Country tile
// Visual grid - flag header + ranked product thumbnails. Filters by
// customer segment (Meta New / Meta Returning / All) and gender.
// Loader emits a (cc x parent product) crosstab with segment×gender
// cells; this tile re-aggregates client-side using the active filters
// so toggling pills doesn't go back to the server.
// ═══════════════════════════════════════════════════════════════

type ProductCell = {
  title: string;
  image: string | null;
  mn_F: number; mn_M: number; mn_U: number;
  mr_F: number; mr_M: number; mr_U: number;
  o_F: number;  o_M: number;  o_U: number;
  totalUnits: number; totalRevenue: number;
};
type CountryProducts = { cc: string; products: ProductCell[]; totalCountryUnits: number };

const SEGMENT_PILLS = [
  { value: "metaNew", label: "Meta New" },
  { value: "metaRet", label: "Meta Returning" },
  { value: "all",     label: "All Customers" },
] as const;

const GENDER_PILLS = [
  { value: "all", label: "All" },
  { value: "F",   label: "Female" },
  { value: "M",   label: "Male" },
] as const;

function geoPillClassName(active: boolean) {
  return `l-pill${active ? " l-pill--active" : ""}`;
}

function ProductInitial({ title, size }: { title: string; size: number }) {
  // Stable hue from title hash so each product reads as its own colour
  // when the catalogue image cache hasn't reached it yet.
  let h = 0;
  for (let i = 0; i < title.length; i++) h = (h * 31 + title.charCodeAt(i)) >>> 0;
  const hue = h % 360;
  return (
    <div style={{
      width: size, height: size, borderRadius: 8,
      background: `linear-gradient(135deg, hsl(${hue} 60% 70%), hsl(${(hue + 40) % 360} 60% 55%))`,
      display: "flex", alignItems: "center", justifyContent: "center",
      color: "#fff", fontWeight: 700, fontSize: Math.round(size * 0.42),
      flexShrink: 0,
    }}>
      {(title[0] || "?").toUpperCase()}
    </div>
  );
}

function unitsForFilters(p: ProductCell, seg: string, gen: string): number {
  const segs = seg === "all" ? ["mn", "mr", "o"] : seg === "metaNew" ? ["mn"] : ["mr"];
  const gens = gen === "all" ? ["F", "M", "U"] : [gen];
  let n = 0;
  for (const s of segs) for (const g of gens) n += (p as any)[`${s}_${g}`] as number;
  return n;
}

function TopProductsByCountryTile({
  data, cs,
}: {
  data: CountryProducts[]; cs: string;
}) {
  const [seg, setSeg] = useState<string>("all");
  const [gen, setGen] = useState<string>("all");

  const filtered = useMemo(() => {
    return data
      .map(c => {
        const products = c.products
          .map(p => ({ ...p, filteredUnits: unitsForFilters(p, seg, gen) }))
          .filter(p => p.filteredUnits > 0)
          .sort((a, b) => b.filteredUnits - a.filteredUnits)
          .slice(0, 5);
        const totalFiltered = products.reduce((s, p) => s + p.filteredUnits, 0);
        return { cc: c.cc, products, totalFiltered };
      })
      .filter(c => c.products.length > 0)
      .sort((a, b) => b.totalFiltered - a.totalFiltered)
      .slice(0, 12); // 12 countries max - keeps the grid tidy on a 1440 viewport
  }, [data, seg, gen]);

  return (
    <Card>
      <BlockStack gap="300">
        <BlockStack gap="050">
          <Text as="h2" variant="headingMd">Top Products per Country</Text>
          <Text as="p" variant="bodySm" tone="subdued">
            Best-selling products by country in this period. Filter by customer segment and gender to see what each audience is buying where.
          </Text>
        </BlockStack>
        <div style={{ display: "flex", flexWrap: "wrap", gap: 14, alignItems: "center" }}>
          <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
            <span style={{ fontSize: 11, color: "#6B7280", fontWeight: 600, textTransform: "uppercase", letterSpacing: 0.4 }}>Customers</span>
            {SEGMENT_PILLS.map(p => (
              <button key={p.value} onClick={() => setSeg(p.value)} className={geoPillClassName(seg === p.value)}>{p.label}</button>
            ))}
          </div>
          <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
            <span style={{ fontSize: 11, color: "#6B7280", fontWeight: 600, textTransform: "uppercase", letterSpacing: 0.4 }}>Gender</span>
            {GENDER_PILLS.map(p => (
              <button key={p.value} onClick={() => setGen(p.value)} className={geoPillClassName(gen === p.value)}>{p.label}</button>
            ))}
          </div>
        </div>

        {filtered.length === 0 ? (
          <div style={{ padding: 28, textAlign: "center", color: "#6B7280", fontSize: 13, background: "#F9FAFB", borderRadius: 8 }}>
            No product orders match these filters in this period.
          </div>
        ) : (
          // Single horizontal row, sideways scroll. Card width sized so 4.5
          // cards fit in the viewport - the half-card peeking on the right
          // is the affordance that says "scroll for more".
          <div style={{
            display: "flex", gap: 14, overflowX: "auto", overflowY: "hidden",
            paddingBottom: 4, scrollSnapType: "x proximity",
          }}>
            {filtered.map(c => (
              <div key={c.cc} style={{
                flex: "0 0 calc((100% - 4 * 14px) / 4.5)",
                minWidth: 0, scrollSnapAlign: "start",
                border: "1px solid #E5E7EB", borderRadius: 12, padding: 14, background: "#fff",
              }}>
                <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 12, paddingBottom: 10, borderBottom: "1px solid #F3F4F6" }}>
                  <span style={{ fontSize: 32, lineHeight: 1 }}>{countryFlag(c.cc)}</span>
                  <BlockStack gap="050">
                    <Text as="p" variant="headingSm">{countryName(c.cc)}</Text>
                    <Text as="p" variant="bodySm" tone="subdued">{c.totalFiltered.toLocaleString()} units sold</Text>
                  </BlockStack>
                </div>
                <BlockStack gap="200">
                  {c.products.map((p, idx) => (
                    <div key={p.title} style={{ display: "flex", alignItems: "center", gap: 10 }}>
                      <span style={{
                        width: 18, fontSize: 13, fontWeight: 700, color: "#9CA3AF", flexShrink: 0, textAlign: "center" as const,
                      }}>{idx + 1}</span>
                      {p.image ? (
                        <img
                          src={p.image} alt=""
                          style={{ width: 44, height: 44, borderRadius: 8, objectFit: "cover", border: "1px solid #E5E7EB", flexShrink: 0 }}
                        />
                      ) : (
                        <ProductInitial title={p.title} size={44} />
                      )}
                      <div style={{ flex: 1, minWidth: 0 }}>
                        <div style={{ fontSize: 13, fontWeight: 600, color: "#1F2937", whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }} title={p.title}>{p.title}</div>
                        <div style={{ fontSize: 11, color: "#6B7280", marginTop: 1 }}>
                          {p.filteredUnits.toLocaleString()} {p.filteredUnits === 1 ? "unit" : "units"}
                        </div>
                      </div>
                    </div>
                  ))}
                </BlockStack>
              </div>
            ))}
          </div>
        )}
      </BlockStack>
    </Card>
  );
}

// ═══════════════════════════════════════════════════════════════
// COMPONENT
// ═══════════════════════════════════════════════════════════════

export default function GeoPerformance() {
  const {
    overallRows, campaignEntities, adsetEntities, adEntities,
    shopifyByCountry, customerMapBlob, topProductsByCountry, currencySymbol, protomapsKey, hasData,
    aiCachedInsights, aiGeneratedAt, aiIsStale,
    fromKey, toKey, preset,
  } = useLoaderData<typeof loader>();
  const cs = currencySymbol || "\u00a3";

  const [selectedTab, setSelectedTab] = useState(0);
  const [customerFilter, setCustomerFilter] = useState("all");
  const [customerFilterOpen, setCustomerFilterOpen] = useState(false);
  const [hoveredCountry, setHoveredCountry] = useState<string | null>(null);
  const [expandedEntities, setExpandedEntities] = useState<Set<string>>(new Set());

  const toggleEntity = useCallback((id: string) => {
    setExpandedEntities(prev => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id); else next.add(id);
      return next;
    });
  }, []);

  const level = TAB_LEVELS[selectedTab];
  const entities = level === "campaign" ? campaignEntities
    : level === "adset" ? adsetEntities
    : level === "ad" ? adEntities : [];

  const r2 = (v: number) => Math.round(v * 100) / 100;

  // Apply customer filter
  const applyFilter = useCallback((row: any) => {
    if (customerFilter === "all") return row;
    if (customerFilter === "new") {
      return {
        ...row,
        attributedOrders: row.newCustomerOrders,
        attributedRevenue: row.newCustomerRevenue,
        blendedROAS: row.spend > 0 && row.newCustomerRevenue > 0 ? r2(row.newCustomerRevenue / row.spend) : 0,
        cpa: row.newCustomerOrders > 0 ? r2(row.spend / row.newCustomerOrders) : 0,
        aov: row.newCustomerOrders > 0 ? r2(row.newCustomerRevenue / row.newCustomerOrders) : null,
      };
    }
    return {
      ...row,
      attributedOrders: row.existingCustomerOrders,
      attributedRevenue: row.existingCustomerRevenue,
      blendedROAS: row.spend > 0 && row.existingCustomerRevenue > 0 ? r2(row.existingCustomerRevenue / row.spend) : 0,
      cpa: row.existingCustomerOrders > 0 ? r2(row.spend / row.existingCustomerOrders) : 0,
      aov: row.existingCustomerOrders > 0 ? r2(row.existingCustomerRevenue / row.existingCustomerOrders) : null,
    };
  }, [customerFilter]);

  const filteredRows = useMemo(() => {
    return overallRows.map(applyFilter).filter((r: any) => r.spend > 0 || r.attributedOrders > 0);
  }, [overallRows, applyFilter]);

  const totalSpend = filteredRows.reduce((s: number, r: any) => s + r.spend, 0);
  const totalRevenue = filteredRows.reduce((s: number, r: any) => s + r.attributedRevenue + r.unverifiedRevenue, 0);
  const totalOrders = filteredRows.reduce((s: number, r: any) => s + r.attributedOrders, 0);

  // Untapped markets: countries with Shopify revenue but zero Meta spend
  const untappedMarkets = useMemo(() => {
    const spendCountries = new Set(overallRows.filter((r: any) => r.spend > 0).map((r: any) => r.country));
    return Object.entries(shopifyByCountry as Record<string, { orders: number; revenue: number }>)
      .filter(([cc]) => !spendCountries.has(cc))
      .map(([cc, d]) => ({ cc, ...d, name: countryName(cc) }))
      .sort((a, b) => b.revenue - a.revenue)
      .slice(0, 8);
  }, [overallRows, shopifyByCountry]);

  // Concentration: what % of spend in top N countries
  const concentration = useMemo(() => {
    if (totalSpend === 0) return { top1: 0, top2: 0, top3: 0, top1Name: "", top2Name: "", top3Name: "" };
    const sorted = [...filteredRows].sort((a: any, b: any) => b.spend - a.spend);
    const top1 = sorted[0] ? Math.round((sorted[0].spend / totalSpend) * 100) : 0;
    const top2 = sorted[1] ? Math.round(((sorted[0].spend + sorted[1].spend) / totalSpend) * 100) : top1;
    const top3 = sorted[2] ? Math.round(((sorted[0].spend + sorted[1].spend + sorted[2].spend) / totalSpend) * 100) : top2;
    return {
      top1, top2, top3,
      top1Name: sorted[0] ? countryName(sorted[0].country) : "",
      top2Name: sorted[1] ? countryName(sorted[1].country) : "",
      top3Name: sorted[2] ? countryName(sorted[2].country) : "",
    };
  }, [filteredRows, totalSpend]);

  // Spend vs Revenue comparison bars
  const spendVsRevBars = useMemo(() => {
    const top = [...filteredRows].sort((a: any, b: any) => b.spend - a.spend).slice(0, 5);
    const totalRev = filteredRows.reduce((s: number, r: any) => s + r.attributedRevenue, 0);
    return top.map((r: any) => ({
      cc: r.country,
      name: countryName(r.country),
      spendPct: totalSpend > 0 ? (r.spend / totalSpend) * 100 : 0,
      revPct: totalRev > 0 ? (r.attributedRevenue / totalRev) * 100 : 0,
      spend: r.spend,
      revenue: r.attributedRevenue,
    }));
  }, [filteredRows, totalSpend]);

  // ── Quick-stat tiles data ──
  // Four headline stats per country, all gated on a minimum order count
  // so a single fluke doesn't crown a country. AOV uses attributed orders
  // (Meta-driven) so it stays consistent with the rest of the page.
  const quickStats = useMemo(() => {
    const MIN_ORDERS = 5;
    const eligible = [...overallRows].filter((r: any) => r.attributedOrders >= MIN_ORDERS);

    // 1. Highest new-customer revenue (Meta-driven).
    const highestNewCustRev = [...overallRows]
      .filter((r: any) => r.newCustomerRevenue > 0)
      .sort((a: any, b: any) => b.newCustomerRevenue - a.newCustomerRevenue)[0] || null;

    // 2. Highest blended ROAS (gated by min orders to avoid £5/£100 noise).
    const highestROAS = eligible
      .filter((r: any) => r.blendedROAS > 0)
      .sort((a: any, b: any) => b.blendedROAS - a.blendedROAS)[0] || null;

    // 3. Highest AOV among Meta-attributed orders.
    const highestAOV = eligible
      .map((r: any) => ({ ...r, aov: r.attributedOrders > 0 ? r2(r.attributedRevenue / r.attributedOrders) : 0 }))
      .filter((r: any) => r.aov > 0)
      .sort((a: any, b: any) => b.aov - a.aov)[0] || null;

    // 4. Lowest Meta CPA (cheapest cost per Meta-reported conversion,
    //    per audience country). Eligibility tightened to require
    //    metaConversions >= MIN_ORDERS rather than attributedOrders —
    //    the CPA denominator switched to metaConversions (Part D fix),
    //    so we filter on the same axis to avoid surfacing countries that
    //    only crossed the floor on the shipping-country side.
    const lowestCPA = overallRows
      .filter((r: any) => (r.metaConversions || 0) >= MIN_ORDERS && r.cpa > 0)
      .sort((a: any, b: any) => a.cpa - b.cpa)[0] || null;

    return { highestNewCustRev, highestROAS, highestAOV, lowestCPA, MIN_ORDERS };
  }, [overallRows]);

  // ── Page summary bullets ──
  // At-a-glance country-level read-out for the selected range. All values
  // come from the same overallRows / quickStats / customerMapBlob that
  // power the tiles below, so the summary and tiles stay in lock-step.
  // Bullets ordered headline-first: market snapshot, then top performers,
  // then over-index / repeat / waste / opportunity signals.
  const summaryBullets: SummaryBullet[] = useMemo(() => {
    const out: SummaryBullet[] = [];

    // 0. Market snapshot - sets the scale before any specific country call-out.
    const activeMarketCount = overallRows.filter((r: any) => r.spend > 0).length;
    const blended = totalSpend > 0 ? totalRevenue / totalSpend : 0;
    if (activeMarketCount > 0 && totalSpend > 0) {
      out.push({
        tone: "neutral",
        text: (
          <>
            <strong>Market snapshot:</strong> {activeMarketCount} active {activeMarketCount === 1 ? "market" : "markets"}, {fmtCompact(totalSpend, cs)} spend → {fmtCompact(totalRevenue, cs)} revenue ({blended.toFixed(2)}x blended ROAS)
          </>
        ),
      });
    }

    if (quickStats.highestNewCustRev) {
      const c = quickStats.highestNewCustRev;
      out.push({
        tone: "positive",
        text: (
          <>
            <strong>Highest new-customer revenue:</strong> {countryName(c.country)} - {fmtCompact(c.newCustomerRevenue, cs)} ({c.newCustomers} new customers)
          </>
        ),
      });
    }

    if (quickStats.highestROAS) {
      out.push({
        tone: "positive",
        text: (
          <>
            <strong>Highest ROAS:</strong> {countryName(quickStats.highestROAS.country)} - {quickStats.highestROAS.blendedROAS}x ({quickStats.highestROAS.attributedOrders} orders)
          </>
        ),
      });
    }

    if (quickStats.highestAOV) {
      out.push({
        tone: "positive",
        text: (
          <>
            <strong>Highest AOV:</strong> {countryName(quickStats.highestAOV.country)} - {cs}{Math.round(quickStats.highestAOV.aov).toLocaleString()} per order
          </>
        ),
      });
    }

    if (quickStats.lowestCPA) {
      out.push({
        tone: "positive",
        text: (
          <>
            <strong>Lowest CPA:</strong> {countryName(quickStats.lowestCPA.country)} - {cs}{Math.round(quickStats.lowestCPA.cpa)} per order
          </>
        ),
      });
    }

    // Largest repeat / existing-customer market. Different signal from
    // "highest new-customer revenue" - flags where retention/retargeting
    // is the dominant driver.
    const MIN_EXISTING_ORDERS = quickStats.MIN_ORDERS;
    const topExisting = [...overallRows]
      .filter((r: any) => r.existingCustomerOrders >= MIN_EXISTING_ORDERS && r.existingCustomerRevenue > 0)
      .sort((a: any, b: any) => b.existingCustomerRevenue - a.existingCustomerRevenue)[0];
    if (topExisting) {
      out.push({
        tone: "positive",
        text: (
          <>
            <strong>Largest repeat market:</strong> {countryName(topExisting.country)} - {fmtCompact(topExisting.existingCustomerRevenue, cs)} from {topExisting.existingCustomerOrders} existing-customer orders
          </>
        ),
      });
    }

    // VIP over-index: which country punches above its weight on lifetime
    // VIPs. Reuses the same chi-square floor (expected ≥ 5) as the
    // VipsByCountryTile to avoid promoting tail noise. All-time, all-scope
    // (matches the tile's defaults).
    if (customerMapBlob && Array.isArray((customerMapBlob as any).points)) {
      const points = (customerMapBlob as MapBlob).points;
      const byCountry: Record<string, { customers: number; vips: number; vipRevenue: number }> = {};
      let totalCustomers = 0;
      let totalVips = 0;
      for (const p of points) {
        if (!p.c) continue;
        totalCustomers++;
        const isVip = p.v !== 0;
        if (isVip) totalVips++;
        if (!byCountry[p.c]) byCountry[p.c] = { customers: 0, vips: 0, vipRevenue: 0 };
        byCountry[p.c].customers++;
        if (isVip) {
          byCountry[p.c].vips++;
          byCountry[p.c].vipRevenue += p.$ - p.r;
        }
      }
      const globalVipRate = totalCustomers > 0 ? totalVips / totalCustomers : 0;
      const MIN_EXPECTED = 5;
      let bestCc: string | null = null;
      let bestLift = 0;
      let bestVips = 0;
      for (const [cc, agg] of Object.entries(byCountry)) {
        const expected = agg.customers * globalVipRate;
        if (expected < MIN_EXPECTED) continue;
        const lift = expected > 0 ? agg.vips / expected : 0;
        if (lift > bestLift) { bestLift = lift; bestCc = cc; bestVips = agg.vips; }
      }
      if (bestCc && bestLift >= 1.2) {
        out.push({
          tone: "neutral",
          text: (
            <>
              <strong>Most VIP-dense market:</strong> {countryName(bestCc)} - {bestLift.toFixed(2)}x over-indexed on lifetime VIPs ({bestVips} VIPs)
            </>
          ),
        });
      }
    }

    // Spend concentration warning - flags single-market dependence.
    if (concentration.top1 >= 50 && concentration.top1Name) {
      out.push({
        tone: "warning",
        text: (
          <>
            <strong>Spend concentrated in one market:</strong> {concentration.top1}% of ad spend goes to {concentration.top1Name}
          </>
        ),
      });
    }

    // Wasted spend: a top-3 spender with sub-1x ROAS is a meaningful
    // financial leak, worth surfacing distinctly from "highest ROAS".
    const topSpenders = [...overallRows].sort((a: any, b: any) => b.spend - a.spend).slice(0, 3);
    const worstTopSpender = topSpenders
      .filter((r: any) => r.spend > 0 && r.attributedOrders >= quickStats.MIN_ORDERS && r.blendedROAS > 0 && r.blendedROAS < 1)
      .sort((a: any, b: any) => a.blendedROAS - b.blendedROAS)[0];
    if (worstTopSpender) {
      out.push({
        tone: "negative",
        text: (
          <>
            <strong>Underperforming spend:</strong> {countryName(worstTopSpender.country)} burns {fmtCompact(worstTopSpender.spend, cs)} at {worstTopSpender.blendedROAS}x ROAS
          </>
        ),
      });
    }

    // Largest untapped market - Shopify revenue with zero Meta spend.
    // Concrete opportunity, not noise.
    if (untappedMarkets.length > 0 && untappedMarkets[0].revenue > 0) {
      const u = untappedMarkets[0];
      out.push({
        tone: "warning",
        text: (
          <>
            <strong>Largest untapped market:</strong> {u.name} - {fmtCompact(u.revenue, cs)} organic revenue, no Meta spend
          </>
        ),
      });
    }

    return out;
  }, [quickStats, cs, overallRows, totalSpend, totalRevenue, customerMapBlob, concentration, untappedMarkets]);

  const customerFilterToolbar = (
    <Popover
      active={customerFilterOpen}
      activator={
        <Button size="slim" onClick={() => setCustomerFilterOpen(v => !v)} disclosure>
          {CUSTOMER_FILTERS.find(f => f.value === customerFilter)?.label || "All Customers"}
        </Button>
      }
      onClose={() => setCustomerFilterOpen(false)}
      preferredAlignment="left"
    >
      <ActionList
        items={CUSTOMER_FILTERS.map(f => ({
          content: f.label,
          active: customerFilter === f.value,
          onAction: () => { setCustomerFilter(f.value); setCustomerFilterOpen(false); },
        }))}
      />
    </Popover>
  );

  if (!hasData) {
    return (
      <Page title="Countries" fullWidth>
        <ReportTabs>
          <Card>
            <BlockStack gap="200">
              <Text as="p" variant="bodyMd" tone="subdued">
                No country breakdown data yet. Run "Sync Meta (7d)" from the dashboard to pull breakdown data.
              </Text>
            </BlockStack>
          </Card>
        </ReportTabs>
      </Page>
    );
  }

  return (
    <Page title="Countries" fullWidth>
      <style dangerouslySetInnerHTML={{ __html: PAGE_STYLES }} />
      <ReportTabs>
        <BlockStack gap="500">

          {/* Hidden for V1 - bring back in V2. Loader wiring kept intact. */}
          {false && (
            <AiInsightsPanel
              pageKey="geo"
              cachedInsights={aiCachedInsights}
              generatedAt={aiGeneratedAt}
              isStale={aiIsStale}
              currencySymbol={cs}
            />
          )}
          <PageSummary bullets={summaryBullets} fromKey={fromKey} toKey={toKey} preset={preset} />

          {/* ═══ CUSTOMER MAP EXPLORER ═══ */}
          <CustomerMapExplorer blob={customerMapBlob} cs={cs} protomapsKey={protomapsKey} />

          {/* ═══ 0. QUICK-STAT TILES ═══ */}
          {/* Flag is the hero - rendered at 80px (poster-sized) on its own line
              above the metric, with everything centred so the tile reads as
              "country, then number". This is a country-level page; the flag
              is the takeaway, not a decoration. */}
          <TileGrid pageId="geo-v3" columns={4} tiles={[
            { id: "highestNewCustRev", label: "Highest New Customer Revenue", render: () => (
              <SummaryTile
                label="Highest New Customer Revenue"
                centered
                value={quickStats.highestNewCustRev ? (
                  <span style={{ display: "flex", flexDirection: "column", alignItems: "center", gap: "10px" }}>
                    <span style={{ fontSize: "80px", lineHeight: 1 }}>{countryFlag(quickStats.highestNewCustRev.country)}</span>
                    <span>{fmtCompact(quickStats.highestNewCustRev.newCustomerRevenue, cs)}</span>
                  </span>
                ) : "-"}
                subtitle={quickStats.highestNewCustRev ? `${countryName(quickStats.highestNewCustRev.country)} · ${quickStats.highestNewCustRev.newCustomers} new customers` : "No data"}
                tooltip={{ definition: "Country generating the most revenue from first-time Meta-acquired customers within the selected date range" }}
              />
            )},
            { id: "highestROAS", label: "Highest ROAS", render: () => (
              <SummaryTile
                label="Highest ROAS"
                centered
                value={quickStats.highestROAS ? (
                  <span style={{ display: "flex", flexDirection: "column", alignItems: "center", gap: "10px" }}>
                    <span style={{ fontSize: "80px", lineHeight: 1 }}>{countryFlag(quickStats.highestROAS.country)}</span>
                    <span>{quickStats.highestROAS.blendedROAS}x</span>
                  </span>
                ) : "-"}
                subtitle={quickStats.highestROAS ? `${countryName(quickStats.highestROAS.country)} · ${quickStats.highestROAS.attributedOrders} orders` : `Min ${quickStats.MIN_ORDERS} orders needed`}
                tooltip={{ definition: `Country with the highest Meta blended ROAS within the selected date range (min ${quickStats.MIN_ORDERS} attributed orders)`, calc: "(Matched + unverified revenue) ÷ Meta spend per country" }}
              />
            )},
            { id: "highestAOV", label: "Highest AOV", render: () => (
              <SummaryTile
                label="Highest AOV"
                centered
                value={quickStats.highestAOV ? (
                  <span style={{ display: "flex", flexDirection: "column", alignItems: "center", gap: "10px" }}>
                    <span style={{ fontSize: "80px", lineHeight: 1 }}>{countryFlag(quickStats.highestAOV.country)}</span>
                    <span>{cs}{Math.round(quickStats.highestAOV.aov).toLocaleString()}</span>
                  </span>
                ) : "-"}
                subtitle={quickStats.highestAOV ? `${countryName(quickStats.highestAOV.country)} · ${quickStats.highestAOV.attributedOrders} orders` : `Min ${quickStats.MIN_ORDERS} orders needed`}
                tooltip={{ definition: `Country with the highest average order value among Meta-attributed orders (min ${quickStats.MIN_ORDERS} orders)`, calc: "Attributed revenue ÷ attributed orders per country" }}
              />
            )},
            { id: "lowestCPA", label: "Lowest CPA", render: () => (
              <SummaryTile
                label="Lowest CPA"
                centered
                value={quickStats.lowestCPA ? (
                  <span style={{ display: "flex", flexDirection: "column", alignItems: "center", gap: "10px" }}>
                    <span style={{ fontSize: "80px", lineHeight: 1 }}>{countryFlag(quickStats.lowestCPA.country)}</span>
                    <span>{cs}{Math.round(quickStats.lowestCPA.cpa)}</span>
                  </span>
                ) : "-"}
                subtitle={quickStats.lowestCPA ? `${countryName(quickStats.lowestCPA.country)} · ${quickStats.lowestCPA.attributedOrders} orders` : `Min ${quickStats.MIN_ORDERS} orders needed`}
                tooltip={{ definition: `Country with the lowest Meta cost per attributed order within the selected date range (min ${quickStats.MIN_ORDERS} orders)` }}
              />
            )},
          ] as TileDef[]} />

          {/* ═══ VIPs per Country ═══ */}
          <VipsByCountryTile blob={customerMapBlob} cs={cs} />

          {/* ═══ Top Products per Country ═══ */}
          <TopProductsByCountryTile data={topProductsByCountry} cs={cs} />

          {/* ═══ 2. VISUAL TILES (50/50 row) ═══ */}
          {/* Spend vs Revenue and Untapped Markets read together - one shows
              where Meta money goes, the other shows where Shopify revenue is
              already coming from without any Meta investment. Side-by-side
              makes the "we're not advertising in country X but we're already
              selling there" comparison obvious. */}
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "16px", alignItems: "start" }}>

            {/* ── Spend vs Revenue ── */}
            <Card>
              <BlockStack gap="300">
                <Text as="h2" variant="headingSm">Spend vs Revenue</Text>
                <Text as="p" variant="bodySm" tone="subdued">Where money goes vs where it comes from</Text>
                <div style={{ display: "flex", flexDirection: "column", gap: "10px" }}>
                  {spendVsRevBars.map(b => (
                    <div key={b.cc} style={{ fontSize: "12px" }}>
                      <div style={{ display: "flex", justifyContent: "space-between", marginBottom: "3px", alignItems: "center" }}>
                        <span style={{ fontWeight: 600, color: "#1F2937", display: "inline-flex", alignItems: "center", gap: "8px" }}>
                          <span style={{ fontSize: "22px", lineHeight: 1 }}>{countryFlag(b.cc)}</span>
                          {b.name}
                        </span>
                      </div>
                      {/* Spend bar */}
                      <div style={{ display: "flex", alignItems: "center", gap: "6px", marginBottom: "2px" }}>
                        <span style={{ fontSize: "10px", color: "#9CA3AF", width: "38px", flexShrink: 0 }}>Spend</span>
                        <div style={{ flex: 1, height: "6px", background: "#F3F4F6", borderRadius: "3px", overflow: "hidden" }}>
                          <div style={{ height: "100%", width: `${b.spendPct}%`, background: "#7C3AED", borderRadius: "3px" }} />
                        </div>
                        <span style={{ fontSize: "10px", color: "#6B7280", width: "36px", textAlign: "right" }}>{Math.round(b.spendPct)}%</span>
                      </div>
                      {/* Revenue bar */}
                      <div style={{ display: "flex", alignItems: "center", gap: "6px" }}>
                        <span style={{ fontSize: "10px", color: "#9CA3AF", width: "38px", flexShrink: 0 }}>Rev</span>
                        <div style={{ flex: 1, height: "6px", background: "#F3F4F6", borderRadius: "3px", overflow: "hidden" }}>
                          <div style={{ height: "100%", width: `${b.revPct}%`, background: "#10B981", borderRadius: "3px" }} />
                        </div>
                        <span style={{ fontSize: "10px", color: "#6B7280", width: "36px", textAlign: "right" }}>{Math.round(b.revPct)}%</span>
                      </div>
                    </div>
                  ))}
                </div>
              </BlockStack>
            </Card>

            {/* ── Untapped Markets ── */}
            <Card>
              <BlockStack gap="300">
                <Text as="h2" variant="headingSm">Untapped Markets</Text>
                <Text as="p" variant="bodySm" tone="subdued">Countries with Shopify sales but zero Meta ad spend -- where you could expand next</Text>
                {untappedMarkets.length > 0 ? (
                  <div style={{ display: "flex", flexDirection: "column", gap: "6px" }}>
                    {untappedMarkets.map(m => (
                      <div key={m.cc} style={{
                        display: "flex", alignItems: "center", justifyContent: "space-between",
                        padding: "8px 10px", borderRadius: "8px", background: "#F0FDF4",
                      }}>
                        <div style={{ display: "flex", alignItems: "center", gap: "10px" }}>
                          <span style={{ fontSize: "26px", lineHeight: 1 }}>{countryFlag(m.cc)}</span>
                          <div>
                            <div style={{ fontSize: "12px", fontWeight: 600, color: "#1F2937" }}>{m.name}</div>
                            <div style={{ fontSize: "10px", color: "#6B7280" }}>{m.orders} orders &middot; {cs}0 ad spend</div>
                          </div>
                        </div>
                        <div style={{ textAlign: "right" }}>
                          <div style={{ fontSize: "14px", fontWeight: 700, color: "#059669" }}>{fmtCompact(m.revenue, cs)}</div>
                        </div>
                      </div>
                    ))}
                  </div>
                ) : (
                  <div style={{ color: "#9CA3AF", fontSize: "13px", padding: "16px 0", textAlign: "center" }}>
                    All revenue countries have ad spend allocated
                  </div>
                )}
              </BlockStack>
            </Card>

          </div>{/* close visual tiles 50/50 row */}

        </BlockStack>
      </ReportTabs>
    </Page>
  );
}
