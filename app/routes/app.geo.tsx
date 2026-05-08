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

  // ── Top Products per Country ──
  // Cross-tab of (countryCode × parent product title × segment × gender),
  // limited to 8 products per country. Client filters live without going
  // back to the loader. Aggregating to the parent-product level (via
  // toParentProduct) collapses Vollebak's "Foo." / "Foo" duplicate listings
  // and Acid Wash colour variants - same logic productRollups uses.
  const { toParentProduct } = await import("../services/productRollups.server.js");

  const orderIdsList = ordersInRange.map(o => o.shopifyOrderId);
  const customerIdsList = [...new Set(ordersInRange.map(o => o.shopifyCustomerId).filter(Boolean) as string[])];

  const [lineItems, customers] = await Promise.all([
    queryCached(`${shopDomain}:geoLineItems:${fromKey}:${toKey}`, DEFAULT_TTL, () =>
      db.orderLineItem.findMany({
        where: { shopDomain, shopifyOrderId: { in: orderIdsList } },
        select: { shopifyOrderId: true, title: true, quantity: true, totalPrice: true, refundedQuantity: true, refundedAmount: true },
      })
    ),
    queryCached(`${shopDomain}:geoCustomers:${fromKey}:${toKey}`, DEFAULT_TTL, () =>
      db.customer.findMany({
        where: { shopDomain, shopifyCustomerId: { in: customerIdsList } },
        select: { shopifyCustomerId: true, metaSegment: true, inferredGender: true },
      })
    ),
  ]);

  const custBySid: Record<string, { seg: string | null; g: string | null }> = {};
  for (const c of customers) {
    custBySid[c.shopifyCustomerId] = {
      seg: c.metaSegment,
      g: c.inferredGender, // "male" | "female" | null
    };
  }

  // Attribution metaGender (more accurate than name inference) - sparse,
  // but where present overrides the name-inferred value.
  const attrGenderById: Record<string, string> = {};
  for (const a of attributions) {
    if ((a as any).metaGender && (a as any).metaGender !== "unknown") {
      attrGenderById[a.shopifyOrderId] = (a as any).metaGender;
    }
  }

  type ProductCell = {
    mn_F: number; mn_M: number; mn_U: number;     // metaNew × gender
    mr_F: number; mr_M: number; mr_U: number;     // metaRetargeted × gender
    o_F: number;  o_M: number;  o_U: number;      // organic × gender
    totalUnits: number; totalRevenue: number;
  };
  const emptyCell = (): ProductCell => ({
    mn_F: 0, mn_M: 0, mn_U: 0, mr_F: 0, mr_M: 0, mr_U: 0, o_F: 0, o_M: 0, o_U: 0,
    totalUnits: 0, totalRevenue: 0,
  });

  const productsByCountry: Record<string, Record<string, ProductCell>> = {};

  for (const li of lineItems) {
    const ord = orderMap[li.shopifyOrderId];
    if (!ord) continue;
    const cc = ord.countryCode;
    if (!cc) continue;

    const cust = ord.shopifyCustomerId ? custBySid[ord.shopifyCustomerId] : null;
    const segPrefix = cust?.seg === "metaNew" ? "mn"
                    : cust?.seg === "metaRetargeted" ? "mr"
                    : "o";

    const metaG = attrGenderById[li.shopifyOrderId];
    const g = metaG === "female" ? "F"
            : metaG === "male" ? "M"
            : cust?.g === "female" ? "F"
            : cust?.g === "male" ? "M"
            : "U";

    const cellKey = `${segPrefix}_${g}` as keyof ProductCell;
    const title = toParentProduct(li.title);
    if (!title) continue;

    const netUnits = (li.quantity || 0) - (li.refundedQuantity || 0);
    if (netUnits <= 0) continue;
    const netRev = (li.totalPrice || 0) - (li.refundedAmount || 0);

    if (!productsByCountry[cc]) productsByCountry[cc] = {};
    if (!productsByCountry[cc][title]) productsByCountry[cc][title] = emptyCell();

    const row = productsByCountry[cc][title];
    (row as any)[cellKey] += netUnits;
    row.totalUnits += netUnits;
    row.totalRevenue += netRev;
  }

  const productImagesMap: Record<string, string> = (() => {
    try { return shop?.productImagesJson ? JSON.parse(shop.productImagesJson) : {}; } catch { return {}; }
  })();

  const topProductsByCountry = Object.entries(productsByCountry)
    .map(([cc, products]) => {
      const sorted = Object.entries(products)
        .map(([title, cell]) => ({
          title,
          image: productImagesMap[title] || productImagesMap[toParentProduct(title)] || null,
          ...cell,
        }))
        .sort((a, b) => b.totalUnits - a.totalUnits)
        .slice(0, 8);
      const totalCountryUnits = sorted.reduce((s, p) => s + p.totalUnits, 0);
      return { cc, products: sorted, totalCountryUnits };
    })
    .filter(c => c.totalCountryUnits >= 3) // suppress noisy 1-2 order tail
    .sort((a, b) => b.totalCountryUnits - a.totalCountryUnits);

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

function pillClass(active: boolean) {
  return {
    padding: "5px 11px", fontSize: "12px", fontWeight: 500, borderRadius: "999px",
    cursor: "pointer", border: active ? "1px solid #059669" : "1px solid #D1D5DB",
    background: active ? "#059669" : "#fff", color: active ? "#fff" : "#374151",
    transition: "all 0.12s", whiteSpace: "nowrap" as const,
  };
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
              <button key={p.value} onClick={() => setSeg(p.value)} style={pillClass(seg === p.value)}>{p.label}</button>
            ))}
          </div>
          <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
            <span style={{ fontSize: 11, color: "#6B7280", fontWeight: 600, textTransform: "uppercase", letterSpacing: 0.4 }}>Gender</span>
            {GENDER_PILLS.map(p => (
              <button key={p.value} onClick={() => setGen(p.value)} style={pillClass(gen === p.value)}>{p.label}</button>
            ))}
          </div>
        </div>

        {filtered.length === 0 ? (
          <div style={{ padding: 28, textAlign: "center", color: "#6B7280", fontSize: 13, background: "#F9FAFB", borderRadius: 8 }}>
            No product orders match these filters in this period.
          </div>
        ) : (
          // maxHeight = ~2.5 country card rows. Each card runs ~340px (header
          // + 5 product rows). 2.5 * 340 = 850; round up so the half-row
          // peeking at the bottom is obvious.
          <div style={{
            display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(260px, 1fr))", gap: 14,
            maxHeight: 880, overflowY: "auto", paddingRight: 4,
          }}>
            {filtered.map(c => (
              <div key={c.cc} style={{ border: "1px solid #E5E7EB", borderRadius: 12, padding: 14, background: "#fff" }}>
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

    // 4. Lowest Meta CPA (cheapest cost per attributed order).
    const lowestCPA = eligible
      .filter((r: any) => r.cpa > 0)
      .sort((a: any, b: any) => a.cpa - b.cpa)[0] || null;

    return { highestNewCustRev, highestROAS, highestAOV, lowestCPA, MIN_ORDERS };
  }, [overallRows]);

  // ── Page summary bullets ──
  // At-a-glance country-level read-out for the selected range. All values
  // come from the same overallRows / quickStats that power the tiles below,
  // so the summary and tiles stay in lock-step.
  const summaryBullets: SummaryBullet[] = useMemo(() => {
    const out: SummaryBullet[] = [];

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

    return out;
  }, [quickStats, cs]);

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
