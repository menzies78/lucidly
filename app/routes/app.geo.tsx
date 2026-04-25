import { json } from "@remix-run/node";
import { useLoaderData, useSubmit, useActionData, useRevalidator } from "@remix-run/react";
import {
  Page, Card, Text, BlockStack,
  Popover, ActionList, Button,
} from "@shopify/polaris";
import ReportTabs from "../components/ReportTabs";
import TileGrid, { type TileDef } from "../components/TileGrid";
import SummaryTile from "../components/SummaryTile";
import CustomerMapExplorer from "../components/CustomerMapExplorer";
import { useState, useMemo, useCallback } from "react";
import { authenticate } from "../shopify.server";
import db from "../db.server";
import { parseDateRange } from "../utils/dateRange.server";
import { currencySymbolFromCode } from "../utils/currency";
import { cached as queryCached } from "../services/queryCache.server";
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
  const { DEFAULT_TTL } = await import("../services/queryCache.server");

  // ── All queries in parallel, with caching ──
  const [breakdownData, ordersInRange] = await Promise.all([
    queryCached(`${shopDomain}:geoBreakdown:${fromKey}:${toKey}`, DEFAULT_TTL, () =>
      db.metaBreakdown.findMany({
        where: { shopDomain, breakdownType: "country", date: { gte: fromDate, lte: toDate } },
      }),
    ),
    queryCached(`${shopDomain}:geoOrders:${fromKey}:${toKey}`, DEFAULT_TTL, () =>
      db.order.findMany({
        where: { shopDomain, isOnlineStore: true, createdAt: { gte: fromDate, lte: toDate } },
      }),
    ),
  ]);
  const orderIdsForAttr = ordersInRange.map(o => o.shopifyOrderId);
  const attributions = await queryCached(`${shopDomain}:geoAttrs:${fromKey}:${toKey}`, DEFAULT_TTL, () =>
    db.attribution.findMany({
      where: {
        shopDomain,
        OR: [
          { shopifyOrderId: { in: orderIdsForAttr } },
          { confidence: 0, matchedAt: { gte: fromDate, lte: toDate } },
        ],
      },
    }),
  );
  const allOrders = ordersInRange;
  const orderIdsInRange = new Set(ordersInRange.map(o => o.shopifyOrderId));
  const attrsInRange = attributions.filter(a => {
    if (a.confidence > 0) return orderIdsInRange.has(a.shopifyOrderId);
    const match = a.shopifyOrderId.match(/(\d{4}-\d{2}-\d{2})/);
    if (!match) return false;
    return match[1] >= fromKey && match[1] <= toKey;
  });

  const orderMap: Record<string, any> = {};
  for (const o of allOrders) orderMap[o.shopifyOrderId] = o;

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

  // Track unique new customers per country (overall) and per entity-country combo
  const newCustByCountry: Record<string, Set<string>> = {};
  const newCustByEntityCountry: Record<string, Set<string>> = {};

  // ── Overall country aggregation (Meta side) ──
  const overallByCountry: Record<string, GeoAgg> = {};

  for (const bd of breakdownData) {
    const cc = bd.breakdownValue;
    if (!overallByCountry[cc]) overallByCountry[cc] = makeAgg(cc);
    const row = overallByCountry[cc];
    row.spend += bd.spend;
    row.impressions += bd.impressions;
    row.clicks += bd.clicks;
    row.reach += bd.reach;
    row.metaConversions += bd.conversions;
    row.metaConversionValue += bd.conversionValue;
    row.linkClicks += bd.linkClicks;
    row.landingPageViews += bd.landingPageViews;
  }

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

  for (const bd of breakdownData) {
    const cc = bd.breakdownValue;

    // Campaign level
    const ck = makeEntityKey("campaign", bd.campaignId, cc);
    if (!entityGeo[ck]) {
      entityGeo[ck] = {
        ...makeAgg(cc),
        entityId: bd.campaignId,
        entityName: bd.campaignName || bd.campaignId,
      };
    }
    const cr = entityGeo[ck];
    cr.spend += bd.spend; cr.impressions += bd.impressions; cr.clicks += bd.clicks;
    cr.reach += bd.reach; cr.metaConversions += bd.conversions;
    cr.metaConversionValue += bd.conversionValue;
    cr.linkClicks += bd.linkClicks; cr.landingPageViews += bd.landingPageViews;

    // Ad Set level
    if (bd.adSetId) {
      const ak = makeEntityKey("adset", bd.adSetId, cc);
      if (!entityGeo[ak]) {
        entityGeo[ak] = {
          ...makeAgg(cc),
          entityId: bd.adSetId,
          entityName: bd.adSetName || bd.adSetId,
          campaignId: bd.campaignId,
          campaignName: bd.campaignName || bd.campaignId,
        };
      }
      const ar = entityGeo[ak];
      ar.spend += bd.spend; ar.impressions += bd.impressions; ar.clicks += bd.clicks;
      ar.reach += bd.reach; ar.metaConversions += bd.conversions;
      ar.metaConversionValue += bd.conversionValue;
      ar.linkClicks += bd.linkClicks; ar.landingPageViews += bd.landingPageViews;
    }

    // Ad level
    if (bd.adId) {
      const dk = makeEntityKey("ad", bd.adId, cc);
      if (!entityGeo[dk]) {
        entityGeo[dk] = {
          ...makeAgg(cc),
          entityId: bd.adId,
          entityName: bd.adName || bd.adId,
          campaignId: bd.campaignId,
          campaignName: bd.campaignName || bd.campaignId,
          adSetId: bd.adSetId || undefined,
          adSetName: bd.adSetName || bd.adSetId || undefined,
        };
      }
      const dr = entityGeo[dk];
      dr.spend += bd.spend; dr.impressions += bd.impressions; dr.clicks += bd.clicks;
      dr.reach += bd.reach; dr.metaConversions += bd.conversions;
      dr.metaConversionValue += bd.conversionValue;
      dr.linkClicks += bd.linkClicks; dr.landingPageViews += bd.landingPageViews;
    }
  }

  // ── Revenue by country (from Shopify order billing address + attribution) ──
  const matchedAttrs = attrsInRange.filter(a => a.confidence > 0);
  const unmatchedAttrs = attrsInRange.filter(a => a.confidence === 0);

  for (const attr of matchedAttrs) {
    const order = orderMap[attr.shopifyOrderId];
    if (!order) continue;
    const gross = order.frozenTotalPrice || 0;
    if (gross === 0) continue; // Skip £0 orders from geo metrics
    // Net of refunds for revenue aggregates; clamp for over-refund edge.
    const rev = Math.max(0, gross - (order.totalRefunded || 0));
    const cc = order.countryCode || "XX";
    const custId = order.shopifyCustomerId || null;

    if (!overallByCountry[cc]) overallByCountry[cc] = makeAgg(cc);
    overallByCountry[cc].attributedOrders++;
    overallByCountry[cc].attributedRevenue += rev;
    if (attr.isNewCustomer) {
      overallByCountry[cc].newCustomerOrders++;
      overallByCountry[cc].newCustomerRevenue += rev;
      if (custId) {
        if (!newCustByCountry[cc]) newCustByCountry[cc] = new Set();
        newCustByCountry[cc].add(custId);
      }
    } else {
      overallByCountry[cc].existingCustomerOrders++;
      overallByCountry[cc].existingCustomerRevenue += rev;
    }

    for (const level of ["campaign", "adset", "ad"]) {
      const entityId = level === "campaign" ? attr.metaCampaignId
        : level === "adset" ? attr.metaAdSetId
        : attr.metaAdId;
      if (!entityId) continue;
      const ek = makeEntityKey(level, entityId, cc);
      if (!entityGeo[ek]) {
        entityGeo[ek] = {
          ...makeAgg(cc),
          entityId,
          entityName: level === "campaign" ? (attr.metaCampaignName || entityId)
            : level === "adset" ? (attr.metaAdSetName || entityId)
            : (attr.metaAdName || entityId),
          campaignId: attr.metaCampaignId || undefined,
          campaignName: attr.metaCampaignName || undefined,
          adSetId: attr.metaAdSetId || undefined,
          adSetName: attr.metaAdSetName || undefined,
        };
      }
      const er = entityGeo[ek];
      er.attributedOrders++;
      er.attributedRevenue += rev;
      if (attr.isNewCustomer) {
        er.newCustomerOrders++;
        er.newCustomerRevenue += rev;
        if (custId) {
          if (!newCustByEntityCountry[ek]) newCustByEntityCountry[ek] = new Set();
          newCustByEntityCountry[ek].add(custId);
        }
      } else {
        er.existingCustomerOrders++;
        er.existingCustomerRevenue += rev;
      }
    }
  }

  // UTM-only orders: utmConfirmedMeta=true but no Layer 2 match
  const matchedOrderIdSet = new Set(matchedAttrs.map(a => a.shopifyOrderId));
  for (const order of ordersInRange) {
    if (!order.utmConfirmedMeta) continue;
    if (matchedOrderIdSet.has(order.shopifyOrderId)) continue;
    const gross = order.frozenTotalPrice || 0;
    if (gross === 0) continue; // Same £0 exclusion as matched orders
    // Net of refunds for revenue aggregates; clamp for over-refund edge.
    const rev = Math.max(0, gross - (order.totalRefunded || 0));
    const cc = order.countryCode || "XX";
    const custId = order.shopifyCustomerId || null;

    if (!overallByCountry[cc]) overallByCountry[cc] = makeAgg(cc);
    overallByCountry[cc].attributedOrders++;
    overallByCountry[cc].attributedRevenue += rev;
    if (order.isNewCustomerOrder) {
      overallByCountry[cc].newCustomerOrders++;
      overallByCountry[cc].newCustomerRevenue += rev;
      if (custId) {
        if (!newCustByCountry[cc]) newCustByCountry[cc] = new Set();
        newCustByCountry[cc].add(custId);
      }
    } else {
      overallByCountry[cc].existingCustomerOrders++;
      overallByCountry[cc].existingCustomerRevenue += rev;
    }

    for (const level of ["campaign", "adset", "ad"]) {
      const entityId = level === "campaign" ? order.metaCampaignId
        : level === "adset" ? order.metaAdSetId
        : order.metaAdId;
      if (!entityId) continue;
      const ek = makeEntityKey(level, entityId, cc);
      if (!entityGeo[ek]) {
        entityGeo[ek] = {
          ...makeAgg(cc),
          entityId,
          entityName: level === "campaign" ? (order.metaCampaignName || entityId)
            : level === "adset" ? (order.metaAdSetName || entityId)
            : (order.metaAdName || entityId),
          campaignId: order.metaCampaignId || undefined,
          campaignName: order.metaCampaignName || undefined,
          adSetId: order.metaAdSetId || undefined,
          adSetName: order.metaAdSetName || undefined,
        };
      }
      const er = entityGeo[ek];
      er.attributedOrders++;
      er.attributedRevenue += rev;
      if (order.isNewCustomerOrder) {
        er.newCustomerOrders++;
        er.newCustomerRevenue += rev;
        if (custId) {
          if (!newCustByEntityCountry[ek]) newCustByEntityCountry[ek] = new Set();
          newCustByEntityCountry[ek].add(custId);
        }
      } else {
        er.existingCustomerOrders++;
        er.existingCustomerRevenue += rev;
      }
    }
  }

  // Unmatched: distribute by country proportionally based on Meta conversion data
  for (const attr of unmatchedAttrs) {
    const val = attr.metaConversionValue || 0;
    if (val === 0) continue;

    for (const level of ["campaign", "adset", "ad"]) {
      const entityId = level === "campaign" ? attr.metaCampaignId
        : level === "adset" ? attr.metaAdSetId
        : attr.metaAdId;
      if (!entityId) continue;

      const entityBreakdowns = breakdownData.filter(bd => {
        if (level === "campaign") return bd.campaignId === entityId;
        if (level === "adset") return bd.adSetId === entityId;
        return bd.adId === entityId;
      });

      const countryConv: Record<string, number> = {};
      let totalConv = 0;
      for (const bd of entityBreakdowns) {
        countryConv[bd.breakdownValue] = (countryConv[bd.breakdownValue] || 0) + bd.conversions;
        totalConv += bd.conversions;
      }

      if (totalConv === 0) continue;

      for (const [cc, conv] of Object.entries(countryConv)) {
        const weight = conv / totalConv;
        const ek = makeEntityKey(level, entityId, cc);
        if (entityGeo[ek]) {
          entityGeo[ek].unverifiedRevenue += val * weight;
        }

        if (level === "campaign") {
          if (!overallByCountry[cc]) overallByCountry[cc] = makeAgg(cc);
          overallByCountry[cc].unverifiedRevenue += val * weight;
        }
      }
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
    cpa: agg.attributedOrders > 0 ? r2(agg.spend / agg.attributedOrders) : 0,
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
  const shopifyByCountry: Record<string, { orders: number; revenue: number }> = {};
  for (const o of ordersInRange) {
    const cc = o.countryCode;
    if (!cc) continue;
    if (!shopifyByCountry[cc]) shopifyByCountry[cc] = { orders: 0, revenue: 0 };
    shopifyByCountry[cc].orders++;
    shopifyByCountry[cc].revenue += (o.frozenTotalPrice || 0) - (o.totalRefunded || 0);
  }

  // ── Customer Map Explorer blob (computed at rollup time) ──
  // Independent of the date range — the explorer is an all-time geographic
  // view of every geocoded customer, with cohort filters that apply across
  // the customer's entire history. If the rollup hasn't run yet, the
  // component renders an empty state.
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

  return json({
    overallRows,
    campaignEntities,
    adsetEntities,
    adEntities,
    shopifyByCountry,
    customerMapBlob,
    currencySymbol,
    hasData: breakdownData.length > 0,
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
          if (!countryAgg[cc]) countryAgg[cc] = { country: cc, spend: 0, impressions: 0, clicks: 0, reach: 0, attributedOrders: 0, attributedRevenue: 0, newCustomerOrders: 0, newCustomers: 0, newCustomerRevenue: 0, existingCustomerOrders: 0, existingCustomerRevenue: 0 };
          countryAgg[cc].spend += b.spend;
          countryAgg[cc].impressions += b.impressions;
          countryAgg[cc].clicks += b.clicks;
          countryAgg[cc].reach += b.reach;
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
          cpa: c.attributedOrders > 0 ? Math.round((c.spend / c.attributedOrders) * 100) / 100 : 0,
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
// COMPONENT
// ═══════════════════════════════════════════════════════════════

export default function GeoPerformance() {
  const {
    overallRows, campaignEntities, adsetEntities, adEntities,
    shopifyByCountry, customerMapBlob, currencySymbol, hasData,
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
  const quickStats = useMemo(() => {
    // a. Top Country for New Customers (unique customers, not orders)
    const newCustSorted = [...overallRows].filter((r: any) => r.newCustomers > 0).sort((a: any, b: any) => b.newCustomers - a.newCustomers);
    const topNewCust = newCustSorted[0] || null;

    // b. Best ROAS Country (min 10 orders)
    const roasCandidates = [...overallRows].filter((r: any) => r.attributedOrders >= 10 && r.blendedROAS > 0);
    const bestROAS = roasCandidates.sort((a: any, b: any) => b.blendedROAS - a.blendedROAS)[0] || null;

    // c. Cheapest CPA Country (min 5 orders)
    const cpaCandidates = [...overallRows].filter((r: any) => r.attributedOrders >= 5 && r.cpa > 0);
    const cheapestCPA = cpaCandidates.sort((a: any, b: any) => a.cpa - b.cpa)[0] || null;

    // d. Spend Drains — campaigns with highest spend but ROAS < 1x
    const drainCampaigns = (campaignEntities as any[])
      .filter(e => {
        const rev = e.totalAttributedRevenue + e.totalUnverifiedRevenue;
        const roas = e.totalSpend > 0 ? rev / e.totalSpend : 0;
        return e.totalSpend > 0 && roas < 1;
      })
      .sort((a, b) => b.totalSpend - a.totalSpend);
    const topDrain = drainCampaigns[0] || null;
    const topDrainROAS = topDrain && topDrain.totalSpend > 0
      ? r2((topDrain.totalAttributedRevenue + topDrain.totalUnverifiedRevenue) / topDrain.totalSpend) : 0;

    // e. Geo Mismatch — countries with spend but zero conversions
    const mismatchCountries = overallRows.filter((r: any) => r.spend > 0 && r.attributedOrders === 0 && r.metaConversions === 0);
    const mismatchSpend = mismatchCountries.reduce((s: number, r: any) => s + r.spend, 0);

    return { topNewCust, bestROAS, cheapestCPA, topDrain, topDrainROAS, mismatchCountries, mismatchSpend };
  }, [overallRows, campaignEntities]);

  // ── Page summary bullets ──
  // At-a-glance country-level read-out for the selected range. All values
  // come from the same overallRows / quickStats that power the tiles below,
  // so the summary and tiles stay in lock-step.
  const summaryBullets: SummaryBullet[] = useMemo(() => {
    const out: SummaryBullet[] = [];

    if (quickStats.topNewCust) {
      out.push({
        tone: "positive",
        text: (
          <>
            <strong>Top new-customer country:</strong> {countryName(quickStats.topNewCust.country)} — {quickStats.topNewCust.newCustomers} new customers ({fmtCompact(quickStats.topNewCust.newCustomerRevenue, cs)} rev)
          </>
        ),
      });
    }

    if (quickStats.bestROAS) {
      out.push({
        tone: "positive",
        text: (
          <>
            <strong>Best Meta ROAS:</strong> {countryName(quickStats.bestROAS.country)} — {quickStats.bestROAS.blendedROAS}x ({quickStats.bestROAS.attributedOrders} orders)
          </>
        ),
      });
    }

    if (quickStats.cheapestCPA) {
      out.push({
        tone: "positive",
        text: (
          <>
            <strong>Cheapest Meta CPA:</strong> {countryName(quickStats.cheapestCPA.country)} — {cs}{Math.round(quickStats.cheapestCPA.cpa)} per order
          </>
        ),
      });
    }

    if (quickStats.topDrain) {
      out.push({
        tone: "negative",
        text: (
          <>
            <strong>Top spend drain:</strong> {quickStats.topDrain.entityName} — {fmtCompact(quickStats.topDrain.totalSpend, cs)} spent at {quickStats.topDrainROAS}x ROAS
          </>
        ),
      });
    }

    if (quickStats.mismatchCountries.length > 0) {
      out.push({
        tone: "warning",
        text: (
          <>
            <strong>Geo mismatch:</strong> {quickStats.mismatchCountries.length} {quickStats.mismatchCountries.length === 1 ? "country" : "countries"} with Meta spend but zero conversions — {fmtCompact(quickStats.mismatchSpend, cs)} at risk
          </>
        ),
      });
    }

    // Country concentration — how top-heavy is the Meta spend?
    const spendRows = [...overallRows].filter((r: any) => r.spend > 0).sort((a: any, b: any) => b.spend - a.spend);
    if (spendRows.length > 0) {
      const top = spendRows[0];
      out.push({
        tone: "neutral",
        text: (
          <>
            <strong>Spend concentration:</strong> {countryName(top.country)} receives {top.spendPct}% of Meta spend
          </>
        ),
      });
    }

    return out;
  }, [quickStats, overallRows, cs]);

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

          {/* Hidden for V1 — bring back in V2. Loader wiring kept intact. */}
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
          <CustomerMapExplorer blob={customerMapBlob} cs={cs} />

          {/* ═══ 0. QUICK-STAT TILES ═══ */}
          <TileGrid pageId="geo-v2" columns={4} tiles={[
            { id: "topNewCust", label: "Top New Customers", render: () => (
              <SummaryTile
                label="Top Meta New Customers"
                value={quickStats.topNewCust ? `${countryFlag(quickStats.topNewCust.country)} ${quickStats.topNewCust.newCustomers}` : "—"}
                subtitle={quickStats.topNewCust ? `${countryName(quickStats.topNewCust.country)} · ${fmtCompact(quickStats.topNewCust.newCustomerRevenue, cs)} rev` : "No data"}
                tooltip={{ definition: "Country with the most unique first-time Meta-acquired customers within the selected date range" }}
              />
            )},
            { id: "bestRoas", label: "Best Meta ROAS Country", render: () => (
              <SummaryTile
                label="Best Meta ROAS Country"
                value={quickStats.bestROAS ? `${countryFlag(quickStats.bestROAS.country)} ${quickStats.bestROAS.blendedROAS}x` : "—"}
                subtitle={quickStats.bestROAS ? `${countryName(quickStats.bestROAS.country)} · ${quickStats.bestROAS.attributedOrders} orders` : "Min 10 orders needed"}
                tooltip={{ definition: "Country with the highest Meta blended ROAS within the selected date range (min 10 attributed orders)", calc: "(Matched + unverified revenue) ÷ Meta spend per country" }}
              />
            )},
            { id: "cheapestCpa", label: "Cheapest Meta CPA", render: () => (
              <SummaryTile
                label="Cheapest Meta CPA"
                value={quickStats.cheapestCPA ? `${countryFlag(quickStats.cheapestCPA.country)} ${cs}${Math.round(quickStats.cheapestCPA.cpa)}` : "—"}
                subtitle={quickStats.cheapestCPA ? `${countryName(quickStats.cheapestCPA.country)} · ${quickStats.cheapestCPA.attributedOrders} orders` : "Min 5 orders needed"}
                tooltip={{ definition: "Country with the lowest Meta cost per attributed order within the selected date range (min 5 orders)" }}
              />
            )},
            { id: "spendDrains", label: "Meta Spend Drains", render: () => (
              <SummaryTile
                label="Meta Spend Drains"
                value={quickStats.topDrain ? fmtCompact(quickStats.topDrain.totalSpend, cs) : "No drains"}
                subtitle={quickStats.topDrain ? `${quickStats.topDrain.entityName} · ${quickStats.topDrainROAS}x ROAS` : "All campaigns above 1x ROAS"}
                tooltip={{ definition: "Meta campaign with highest spend that has ROAS below 1x within the selected date range", calc: "Campaigns where (revenue ÷ spend) < 1, sorted by spend" }}
              />
            )},
            { id: "geoMismatch", label: "Meta Geo Mismatch", render: () => (
              <SummaryTile
                label="Meta Geo Mismatch"
                value={quickStats.mismatchCountries.length > 0 ? `${quickStats.mismatchCountries.length} ${quickStats.mismatchCountries.length === 1 ? "country" : "countries"}` : "All clear"}
                subtitle={quickStats.mismatchCountries.length > 0 ? `${fmtCompact(quickStats.mismatchSpend, cs)} wasted Meta spend` : "No countries with Meta spend but zero conversions"}
                tooltip={{ definition: "Countries where you're spending on Meta ads but getting zero conversions within the selected date range" }}
              />
            )},
          ] as TileDef[]} />

          {/* ═══ 1. GEO PERFORMANCE TABLE ═══ */}
          <Card>
            <BlockStack gap="400">
              {/* Header: Title + Tabs + Customer Filter */}
              <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", flexWrap: "wrap", gap: "10px" }}>
                <div style={{ display: "flex", alignItems: "center", gap: "12px" }}>
                  <Text as="h2" variant="headingLg">Countries</Text>
                  <div style={{ display: "flex", gap: "4px" }}>
                    {TAB_LABELS.map((label, i) => (
                      <button
                        key={label}
                        className={`geo-tab ${selectedTab === i ? "geo-tab-active" : "geo-tab-inactive"}`}
                        onClick={() => {
                          setSelectedTab(i);
                          // Pre-expand all entities for entity tabs
                          const ents = i === 1 ? campaignEntities : i === 2 ? adsetEntities : i === 3 ? adEntities : [];
                          setExpandedEntities(new Set(ents.map((e: any) => e.entityId)));
                        }}
                      >
                        {label}
                      </button>
                    ))}
                  </div>
                </div>
                {customerFilterToolbar}
              </div>

              {/* Spend Concentration Strip */}
              {totalSpend > 0 && (
                <div>
                  <div style={{ display: "flex", justifyContent: "space-between", marginBottom: "4px" }}>
                    <span style={{ fontSize: "11px", fontWeight: 600, color: "#6B7280", textTransform: "uppercase", letterSpacing: "0.5px" }}>Spend Distribution</span>
                    <span style={{ fontSize: "11px", color: "#9CA3AF" }}>{fmtCompact(totalSpend, cs)} total</span>
                  </div>
                  <div style={{ height: "8px", borderRadius: "4px", overflow: "hidden", display: "flex", background: "#F3F4F6" }}>
                    {filteredRows.filter((r: any) => r.spend > 0).sort((a: any, b: any) => b.spend - a.spend).map((r: any, i: number) => {
                      const pct = (r.spend / totalSpend) * 100;
                      if (pct < 0.5) return null;
                      const colors = ["#7C3AED", "#A78BFA", "#C4B5FD", "#DDD6FE", "#EDE9FE", "#F5F3FF"];
                      return (
                        <div
                          key={r.country}
                          style={{ width: `${pct}%`, background: colors[Math.min(i, colors.length - 1)], height: "100%" }}
                          title={`${countryName(r.country)}: ${Math.round(pct)}%`}
                        />
                      );
                    })}
                  </div>
                  <div style={{ display: "flex", gap: "12px", marginTop: "4px", flexWrap: "wrap" }}>
                    {filteredRows.filter((r: any) => r.spendPct >= 1).sort((a: any, b: any) => b.spend - a.spend).slice(0, 6).map((r: any, i: number) => {
                      const colors = ["#7C3AED", "#A78BFA", "#C4B5FD", "#DDD6FE", "#EDE9FE", "#F5F3FF"];
                      return (
                        <div key={r.country} style={{ display: "flex", alignItems: "center", gap: "4px", fontSize: "11px", color: "#6B7280" }}>
                          <div style={{ width: "8px", height: "8px", borderRadius: "2px", background: colors[Math.min(i, colors.length - 1)] }} />
                          {countryName(r.country)} {Math.round(r.spendPct)}%
                        </div>
                      );
                    })}
                  </div>
                </div>
              )}

              {/* ── ALL TAB: Country rows ── */}
              {level === "all" && (
                <div style={{ display: "flex", flexDirection: "column", gap: "2px" }}>
                  {filteredRows.map((row: any, i: number) => {
                    const rs = roasStyle(row.blendedROAS);
                    const newPct = row.attributedOrders > 0 ? Math.round((row.newCustomerOrders / row.attributedOrders) * 100) : 0;
                    const isHovered = hoveredCountry === row.country;
                    const isUnknown = !row.country || row.country === "XX" || row.country === "unknown";

                    return (
                      <div
                        key={row.country}
                        style={{
                          display: "flex", alignItems: "center", gap: "14px", padding: "10px 14px",
                          opacity: isUnknown ? 0.45 : 1,
                          borderRadius: "10px", background: isHovered ? "#F5F3FF" : i % 2 === 0 ? "#FAFAFA" : "#fff",
                          transition: "background 0.15s", cursor: "default",
                        }}
                        onMouseEnter={() => setHoveredCountry(row.country)}
                        onMouseLeave={() => setHoveredCountry(null)}
                      >
                        {/* Rank */}
                        <div style={{
                          width: "26px", height: "26px", borderRadius: "50%", flexShrink: 0,
                          background: i < 3 ? "#7C3AED" : "#E5E7EB",
                          color: i < 3 ? "#fff" : "#6B7280",
                          display: "flex", alignItems: "center", justifyContent: "center",
                          fontSize: "11px", fontWeight: 700,
                        }}>
                          {i + 1}
                        </div>

                        {/* Flag + Name */}
                        <div style={{ width: "130px", flexShrink: 0 }}>
                          <div style={{ display: "flex", alignItems: "center", gap: "6px" }}>
                            <span style={{ fontSize: "18px", lineHeight: 1 }}>{countryFlag(row.country)}</span>
                            <span style={{ fontWeight: 700, fontSize: "13px", color: "#1F2937" }}>{countryName(row.country)}</span>
                          </div>
                        </div>

                        {/* Spend bar */}
                        <div style={{ flex: 1, minWidth: "110px" }}>
                          <div style={{ display: "flex", justifyContent: "space-between", fontSize: "11px", color: "#6B7280", marginBottom: "3px" }}>
                            <span style={{ fontWeight: 600 }}>{fmtCompact(row.spend, cs)}</span>
                            <span>{row.spendPct > 0 ? `${Math.round(row.spendPct)}%` : "\u2014"}</span>
                          </div>
                          <div style={{ height: "5px", background: "#EDE9FE", borderRadius: "3px", overflow: "hidden" }}>
                            <div style={{ height: "100%", width: `${Math.min(row.spendPct, 100)}%`, background: "#7C3AED", borderRadius: "3px", transition: "width 0.3s" }} />
                          </div>
                        </div>

                        {/* Revenue */}
                        <div style={{ textAlign: "right", minWidth: "80px" }}>
                          <div style={{ fontSize: "10px", color: "#9CA3AF", textTransform: "uppercase", letterSpacing: "0.3px" }}>Revenue</div>
                          <div style={{ fontWeight: 700, fontSize: "14px", color: "#1F2937" }}>{fmtCompact(row.attributedRevenue, cs)}</div>
                        </div>

                        {/* ROAS pill */}
                        <div style={{ minWidth: "58px", textAlign: "center" }}>
                          {row.blendedROAS > 0 ? (
                            <div style={{
                              display: "inline-block", padding: "3px 10px", borderRadius: "12px",
                              fontSize: "12px", fontWeight: 700, background: rs.bg, color: rs.text,
                            }}>
                              {row.blendedROAS}x
                            </div>
                          ) : <span style={{ color: "#D1D5DB", fontSize: "12px" }}>{"\u2014"}</span>}
                        </div>

                        {/* CPA */}
                        <div style={{ textAlign: "right", minWidth: "62px" }}>
                          <div style={{ fontSize: "10px", color: "#9CA3AF", textTransform: "uppercase", letterSpacing: "0.3px" }}>CPA</div>
                          <div style={{ fontWeight: 600, fontSize: "13px", color: "#1F2937" }}>
                            {row.cpa > 0 ? `${cs}${Math.round(row.cpa)}` : "\u2014"}
                          </div>
                        </div>

                        {/* Orders */}
                        <div style={{ textAlign: "right", minWidth: "55px" }}>
                          <div style={{ fontSize: "10px", color: "#9CA3AF", textTransform: "uppercase", letterSpacing: "0.3px" }}>Orders</div>
                          <div style={{ fontWeight: 700, fontSize: "14px", color: "#1F2937" }}>{row.attributedOrders.toLocaleString()}</div>
                        </div>

                        {/* New/Existing split */}
                        <div style={{ minWidth: "100px" }}>
                          <div style={{ display: "flex", justifyContent: "space-between", fontSize: "10px", color: "#9CA3AF", marginBottom: "3px" }}>
                            <span>{row.newCustomerOrders} new</span>
                            <span>{row.existingCustomerOrders} ret</span>
                          </div>
                          <div style={{ height: "5px", borderRadius: "3px", overflow: "hidden", display: "flex", background: "#F3F4F6" }}>
                            {row.attributedOrders > 0 && (
                              <>
                                <div style={{ height: "100%", width: `${newPct}%`, background: "#7C3AED" }} />
                                <div style={{ height: "100%", flex: 1, background: "#0891B2" }} />
                              </>
                            )}
                          </div>
                        </div>
                      </div>
                    );
                  })}

                  {/* Summary footer */}
                  {filteredRows.length > 0 && (
                    <div style={{
                      display: "flex", alignItems: "center", gap: "14px", padding: "10px 14px",
                      borderRadius: "10px", background: "#F5F3FF", marginTop: "4px",
                    }}>
                      <div style={{ width: "26px", height: "26px", flexShrink: 0 }} />
                      <div style={{ width: "130px", flexShrink: 0, fontWeight: 700, fontSize: "13px", color: "#7C3AED" }}>
                        {filteredRows.length} countries
                      </div>
                      <div style={{ flex: 1, minWidth: "110px", fontWeight: 700, fontSize: "12px", color: "#7C3AED" }}>
                        {fmtCompact(totalSpend, cs)} total
                      </div>
                      <div style={{ textAlign: "right", minWidth: "80px", fontWeight: 700, fontSize: "14px", color: "#7C3AED" }}>
                        {fmtCompact(totalRevenue, cs)}
                      </div>
                      <div style={{ minWidth: "58px", textAlign: "center" }}>
                        {totalSpend > 0 && (
                          <div style={{ display: "inline-block", padding: "3px 10px", borderRadius: "12px", fontSize: "12px", fontWeight: 700, background: "#EDE9FE", color: "#7C3AED" }}>
                            {r2(totalRevenue / totalSpend)}x
                          </div>
                        )}
                      </div>
                      <div style={{ textAlign: "right", minWidth: "62px", fontWeight: 700, fontSize: "13px", color: "#7C3AED" }}>
                        {totalOrders > 0 ? `${cs}${Math.round(totalSpend / totalOrders)}` : "\u2014"}
                      </div>
                      <div style={{ textAlign: "right", minWidth: "55px", fontWeight: 700, fontSize: "14px", color: "#7C3AED" }}>
                        {totalOrders.toLocaleString()}
                      </div>
                      <div style={{ minWidth: "100px" }} />
                    </div>
                  )}
                </div>
              )}

              {/* ── ENTITY TABS: Campaign / Ad Set / Ad ── */}
              {level !== "all" && (
                <div style={{ display: "flex", flexDirection: "column", gap: "8px" }}>
                  {entities.map((entity: any) => {
                    const isExpanded = expandedEntities.has(entity.entityId);
                    const totalRev = entity.totalAttributedRevenue + entity.totalUnverifiedRevenue;
                    const roas = entity.totalSpend > 0 ? r2(totalRev / entity.totalSpend) : 0;
                    const rs = roasStyle(roas);
                    const entityCPA = entity.totalAttributedOrders > 0 ? r2(entity.totalSpend / entity.totalAttributedOrders) : 0;

                    return (
                      <div key={entity.entityId} style={{ borderRadius: "10px", overflow: "hidden", border: "1px solid #E5E7EB" }}>
                        {/* Entity header */}
                        <div
                          onClick={() => toggleEntity(entity.entityId)}
                          style={{
                            display: "flex", alignItems: "center", gap: "0",
                            padding: "12px 14px", background: "#FAFAFA", cursor: "pointer",
                            borderBottom: isExpanded ? "1px solid #E5E7EB" : "none",
                          }}
                        >
                          <span style={{ fontSize: "12px", color: "#6B7280", transition: "transform 0.2s", transform: isExpanded ? "rotate(90deg)" : "rotate(0deg)", marginRight: "10px", flexShrink: 0 }}>
                            &#9654;
                          </span>
                          <div style={{ flex: 1, minWidth: 0, marginRight: "12px" }}>
                            <div style={{ fontWeight: 700, fontSize: "13px", color: "#1F2937", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                              {entity.entityName}
                            </div>
                            <div style={{ fontSize: "11px", color: "#9CA3AF" }}>
                              {entity.countries.length} countries
                            </div>
                          </div>

                          <div style={{ textAlign: "right", width: "90px", flexShrink: 0 }}>
                            <div style={{ fontWeight: 700, fontSize: "13px", color: "#1F2937" }}>{fmtCompact(entity.totalSpend, cs)}</div>
                          </div>
                          <div style={{ textAlign: "right", width: "90px", flexShrink: 0 }}>
                            <div style={{ fontWeight: 700, fontSize: "13px", color: "#1F2937" }}>{fmtCompact(entity.totalAttributedRevenue, cs)}</div>
                          </div>
                          <div style={{ width: "65px", flexShrink: 0, textAlign: "center" }}>
                            {roas > 0 ? (
                              <div style={{ display: "inline-block", padding: "3px 10px", borderRadius: "12px", fontSize: "12px", fontWeight: 700, background: rs.bg, color: rs.text }}>
                                {roas}x
                              </div>
                            ) : <span style={{ color: "#D1D5DB", fontSize: "12px" }}>{"\u2014"}</span>}
                          </div>
                          <div style={{ textAlign: "right", width: "70px", flexShrink: 0 }}>
                            <div style={{ fontWeight: 600, fontSize: "13px", color: "#1F2937" }}>
                              {entityCPA > 0 ? `${cs}${Math.round(entityCPA)}` : "\u2014"}
                            </div>
                          </div>
                          <div style={{ textAlign: "right", width: "60px", flexShrink: 0 }}>
                            <div style={{ fontWeight: 700, fontSize: "13px", color: "#1F2937" }}>{entity.totalAttributedOrders.toLocaleString()}</div>
                          </div>
                        </div>

                        {/* Column headers */}
                        {isExpanded && (
                          <div style={{
                            display: "flex", alignItems: "center", gap: "0",
                            padding: "6px 14px 6px 36px", background: "#F9FAFB",
                            borderBottom: "1px solid #F3F4F6",
                          }}>
                            <div style={{ flex: 1, minWidth: 0, fontSize: "10px", fontWeight: 600, color: "#9CA3AF", textTransform: "uppercase", letterSpacing: "0.5px" }}>Country</div>
                            <div style={{ textAlign: "right", width: "90px", flexShrink: 0, fontSize: "10px", fontWeight: 600, color: "#9CA3AF", textTransform: "uppercase", letterSpacing: "0.5px" }}>Spend</div>
                            <div style={{ textAlign: "right", width: "90px", flexShrink: 0, fontSize: "10px", fontWeight: 600, color: "#9CA3AF", textTransform: "uppercase", letterSpacing: "0.5px" }}>Revenue</div>
                            <div style={{ textAlign: "center", width: "65px", flexShrink: 0, fontSize: "10px", fontWeight: 600, color: "#9CA3AF", textTransform: "uppercase", letterSpacing: "0.5px" }}>ROAS</div>
                            <div style={{ textAlign: "right", width: "70px", flexShrink: 0, fontSize: "10px", fontWeight: 600, color: "#9CA3AF", textTransform: "uppercase", letterSpacing: "0.5px" }}>CPA</div>
                            <div style={{ textAlign: "right", width: "60px", flexShrink: 0, fontSize: "10px", fontWeight: 600, color: "#9CA3AF", textTransform: "uppercase", letterSpacing: "0.5px" }}>Orders</div>
                          </div>
                        )}

                        {/* Country sub-rows — aligned with parent columns */}
                        {isExpanded && entity.countries.map((c: any, ci: number) => {
                          const filtered = applyFilter(c);
                          const crs = roasStyle(filtered.blendedROAS);
                          const countryCPA = filtered.attributedOrders > 0 ? r2(filtered.spend / filtered.attributedOrders) : 0;
                          return (
                            <div key={c.country} style={{
                              display: "flex", alignItems: "center", gap: "0",
                              padding: "8px 14px 8px 36px",
                              background: ci % 2 === 0 ? "#fff" : "#FAFAFA",
                              borderBottom: ci < entity.countries.length - 1 ? "1px solid #F3F4F6" : "none",
                            }}>
                              <div style={{ flex: 1, minWidth: 0, display: "flex", alignItems: "center", gap: "5px" }}>
                                <span style={{ fontSize: "15px" }}>{countryFlag(c.country)}</span>
                                <span style={{ fontSize: "12px", fontWeight: 600, color: "#4B5563" }}>{countryName(c.country)}</span>
                                {filtered.spendPct > 0 && (
                                  <span style={{ fontSize: "10px", color: "#9CA3AF" }}>({Math.round(filtered.spendPct)}%)</span>
                                )}
                              </div>
                              <div style={{ textAlign: "right", width: "90px", flexShrink: 0, fontSize: "12px", fontWeight: 600, color: "#4B5563" }}>
                                {fmtCompact(filtered.spend, cs)}
                              </div>
                              <div style={{ textAlign: "right", width: "90px", flexShrink: 0, fontSize: "12px", fontWeight: 600, color: "#4B5563" }}>
                                {fmtCompact(filtered.attributedRevenue, cs)}
                              </div>
                              <div style={{ width: "65px", flexShrink: 0, textAlign: "center" }}>
                                {filtered.blendedROAS > 0 ? (
                                  <div style={{ display: "inline-block", padding: "2px 8px", borderRadius: "10px", fontSize: "11px", fontWeight: 700, background: crs.bg, color: crs.text }}>
                                    {filtered.blendedROAS}x
                                  </div>
                                ) : <span style={{ color: "#D1D5DB", fontSize: "11px" }}>{"\u2014"}</span>}
                              </div>
                              <div style={{ textAlign: "right", width: "70px", flexShrink: 0, fontSize: "12px", fontWeight: 600, color: "#4B5563" }}>
                                {countryCPA > 0 ? `${cs}${Math.round(countryCPA)}` : "\u2014"}
                              </div>
                              <div style={{ textAlign: "right", width: "60px", flexShrink: 0, fontSize: "12px", fontWeight: 600, color: "#4B5563" }}>
                                {filtered.attributedOrders > 0 ? filtered.attributedOrders.toLocaleString() : "\u2014"}
                              </div>
                            </div>
                          );
                        })}
                      </div>
                    );
                  })}
                </div>
              )}
            </BlockStack>
          </Card>

          {/* ═══ 2. VISUAL TILES (full width) ═══ */}
          <div style={{ display: "flex", flexDirection: "column", gap: "16px" }}>

            {/* ── Spend vs Revenue ── */}
            <Card>
              <BlockStack gap="300">
                <Text as="h2" variant="headingSm">Spend vs Revenue</Text>
                <Text as="p" variant="bodySm" tone="subdued">Where money goes vs where it comes from</Text>
                <div style={{ display: "flex", flexDirection: "column", gap: "10px" }}>
                  {spendVsRevBars.map(b => (
                    <div key={b.cc} style={{ fontSize: "12px" }}>
                      <div style={{ display: "flex", justifyContent: "space-between", marginBottom: "3px" }}>
                        <span style={{ fontWeight: 600, color: "#1F2937" }}>{countryFlag(b.cc)} {b.name}</span>
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
                <Text as="p" variant="bodySm" tone="subdued">Revenue with zero ad spend -- expansion opportunities</Text>
                {untappedMarkets.length > 0 ? (
                  <div style={{ display: "flex", flexDirection: "column", gap: "6px" }}>
                    {untappedMarkets.map(m => (
                      <div key={m.cc} style={{
                        display: "flex", alignItems: "center", justifyContent: "space-between",
                        padding: "8px 10px", borderRadius: "8px", background: "#F0FDF4",
                      }}>
                        <div style={{ display: "flex", alignItems: "center", gap: "6px" }}>
                          <span style={{ fontSize: "16px" }}>{countryFlag(m.cc)}</span>
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

            {/* ── Concentration Risk ── */}
            <Card>
              <BlockStack gap="300">
                <Text as="h2" variant="headingSm">Spend Concentration</Text>
                <Text as="p" variant="bodySm" tone="subdued">How concentrated is your ad spend?</Text>
                {totalSpend > 0 ? (
                  <div style={{ display: "flex", flexDirection: "column", gap: "14px" }}>
                    {/* Visual bar */}
                    <div>
                      <div style={{ height: "28px", borderRadius: "8px", overflow: "hidden", display: "flex", background: "#F3F4F6", position: "relative" }}>
                        <div style={{ width: `${concentration.top1}%`, background: "#7C3AED", display: "flex", alignItems: "center", justifyContent: "center" }}>
                          <span style={{ color: "#fff", fontSize: "10px", fontWeight: 700 }}>
                            {concentration.top1Name} {concentration.top1}%
                          </span>
                        </div>
                        {concentration.top2 > concentration.top1 && (
                          <div style={{ width: `${concentration.top2 - concentration.top1}%`, background: "#A78BFA", display: "flex", alignItems: "center", justifyContent: "center" }}>
                            <span style={{ color: "#fff", fontSize: "10px", fontWeight: 600 }}>
                              {concentration.top2Name.split(" ")[0]}
                            </span>
                          </div>
                        )}
                        {concentration.top3 > concentration.top2 && (
                          <div style={{ width: `${concentration.top3 - concentration.top2}%`, background: "#C4B5FD", display: "flex", alignItems: "center", justifyContent: "center" }}>
                            <span style={{ color: "#4C1D95", fontSize: "10px", fontWeight: 600 }}>
                              {concentration.top3Name.split(" ")[0]}
                            </span>
                          </div>
                        )}
                      </div>
                    </div>

                    {/* Stats */}
                    <div style={{ display: "flex", flexDirection: "column", gap: "8px" }}>
                      {[
                        { label: "Top 1 country", pct: concentration.top1, name: concentration.top1Name },
                        { label: "Top 2 countries", pct: concentration.top2, name: `${concentration.top1Name} + ${concentration.top2Name}` },
                        { label: "Top 3 countries", pct: concentration.top3, name: `+ ${concentration.top3Name}` },
                      ].filter(s => s.pct > 0).map(s => (
                        <div key={s.label} style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                          <div>
                            <div style={{ fontSize: "12px", fontWeight: 600, color: "#1F2937" }}>{s.label}</div>
                            <div style={{ fontSize: "10px", color: "#9CA3AF" }}>{s.name}</div>
                          </div>
                          <div style={{
                            fontSize: "18px", fontWeight: 700,
                            color: s.pct >= 90 ? "#DC2626" : s.pct >= 70 ? "#D97706" : "#059669",
                          }}>
                            {s.pct}%
                          </div>
                        </div>
                      ))}
                    </div>

                    {/* Verdict */}
                    <div style={{
                      padding: "8px 12px", borderRadius: "8px", fontSize: "12px", fontWeight: 600,
                      background: concentration.top1 >= 70 ? "#FEF2F2" : concentration.top1 >= 50 ? "#FEF9C3" : "#ECFDF5",
                      color: concentration.top1 >= 70 ? "#DC2626" : concentration.top1 >= 50 ? "#854D0E" : "#059669",
                    }}>
                      {concentration.top1 >= 70
                        ? `High concentration -- ${concentration.top1}% of spend in one country. Consider diversifying.`
                        : concentration.top1 >= 50
                          ? `Moderate concentration -- ${concentration.top1}% in top country.`
                          : `Well diversified -- no single country dominates spend.`}
                    </div>
                  </div>
                ) : (
                  <div style={{ color: "#9CA3AF", fontSize: "13px", padding: "16px 0", textAlign: "center" }}>
                    No spend data to analyze
                  </div>
                )}
              </BlockStack>
            </Card>
          </div>{/* close visual tiles column */}

        </BlockStack>
      </ReportTabs>
    </Page>
  );
}
