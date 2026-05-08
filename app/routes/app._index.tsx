import { json, redirect } from "@remix-run/node";
import { useLoaderData, useSubmit, useNavigate, useNavigation, useRevalidator, useSearchParams } from "@remix-run/react";
import { Page, Layout, Card, Text, Button, BlockStack, InlineStack, Banner, ProgressBar, Spinner } from "@shopify/polaris";
import ReportTabs from "../components/ReportTabs";
import OnboardingProgressCard from "../components/OnboardingProgressCard";
import { useState, useEffect, useRef, useCallback } from "react";
import { authenticate } from "../shopify.server";
import db from "../db.server";
import { syncOrders } from "../services/orderSync.server";
import { syncMetaAll } from "../services/metaSync.server";
import { runAttribution, runDateRangeRematch, runFillGaps } from "../services/matcher.server";
import { runIncrementalSync, clearTodayForRematch } from "../services/incrementalSync.server";
import { setProgress, failProgress, getProgress, completeProgress } from "../services/progress.server";
import { parseDateRange } from "../utils/dateRange.server";
import { shopLocalDayKey } from "../utils/shopTime.server";
import { currencySymbolFromCode } from "../utils/currency";
import { cached as queryCached } from "../services/queryCache.server";
import { isInternalShop } from "../utils/access.server";

// Cross-request guard: avoid kicking off the same auto-install order sync
// twice if the merchant rapid-refreshes the dashboard before the first sync
// has had a chance to write lastOrderSync. Memory-only, per-process.
const __autoInstallSyncStarted = (globalThis.__autoInstallSyncStarted ||= new Set());

export const loader = async ({ request }) => {
  const { admin, session } = await authenticate.admin(request);
  const shopDomain = session.shop;

  const shop = await db.shop.findUnique({ where: { shopDomain } });
  const tz = shop?.shopifyTimezone || "UTC";

  // ── Onboarding install gate ─────────────────────────────────────────
  // First-load auto-trigger of historical Shopify backfill. The Fit Report
  // can't run without orders, so we kick the sync the moment a merchant
  // lands on the dashboard with an empty DB. Fire-and-forget; the dashboard
  // shows the existing welcome banner until the import lands.
  if (shop && !shop.lastOrderSync && !__autoInstallSyncStarted.has(shopDomain)) {
    __autoInstallSyncStarted.add(shopDomain);
    console.log(`[onboarding] ${shopDomain}: auto-firing initial Shopify backfill`);
    (async () => {
      try {
        const { syncOrders } = await import("../services/orderSync.server.js");
        await syncOrders(admin, shopDomain);
      } catch (err) {
        console.error(`[onboarding] auto-backfill failed for ${shopDomain}: ${err.message}`);
      } finally {
        __autoInstallSyncStarted.delete(shopDomain);
      }
    })();
  }

  // Once the historical orders are in, the next gate is the Fit Report.
  // We redirect *once* - after the merchant has seen it, fitTestComputedAt
  // is set and the redirect stops firing. They can still hit Skip on the
  // Fit page to land on the dashboard, and re-visit /app/fit-test from the
  // Meta-connect CTA later.
  if (
    shop?.lastOrderSync
    && !shop.metaAccessToken
    && !shop.fitTestComputedAt
    && !shop.onboardingCompleted
  ) {
    return redirect("/app/fit-test");
  }
  // ────────────────────────────────────────────────────────────────────

  const { fromDate, toDate, fromKey, toKey } = parseDateRange(request, tz);
  const dateFilter = { gte: fromDate, lte: toDate };

  // 30 days ago for health charts (always, independent of date picker)
  const now = new Date();
  const thirtyDaysAgo = new Date(now.getTime() - 30 * 86400000);
  const sevenDaysAgo = new Date(now.getTime() - 7 * 86400000);
  const oneDayAgo = new Date(now.getTime() - 24 * 3600000);

  // Parallel batch 1: all independent DB queries
  const [
    orderCount,
    ordersInRange,
    newCustomerOrders,
    existingCustomerOrders,
    totalSpend,
    revenueAgg,
    orderIdsInRange,
    allAttrs,
    utmOnlyOrders,
    // Health-specific queries
    liveMetaInsights30d,
    liveMatchedOrders30d,
    recentlyStoppedCampaigns,
    activeCampaignCount,
  ] = await Promise.all([
    db.order.count({ where: { shopDomain, createdAt: dateFilter } }),
    db.order.findMany({
      where: { shopDomain, createdAt: dateFilter },
      select: { shopifyCustomerId: true },
      distinct: ["shopifyCustomerId"],
    }),
    db.order.count({ where: { shopDomain, isNewCustomerOrder: true, createdAt: dateFilter } }),
    db.order.count({ where: { shopDomain, isNewCustomerOrder: false, createdAt: dateFilter } }),
    db.dailyAdRollup.aggregate({ where: { shopDomain, date: dateFilter }, _sum: { spend: true } }),
    db.order.aggregate({
      where: { shopDomain, createdAt: dateFilter },
      _sum: { frozenTotalPrice: true, totalRefunded: true },
    }),
    db.order.findMany({
      where: { shopDomain, createdAt: dateFilter },
      select: { shopifyOrderId: true },
    }),
    (async () => {
      const bufferStart = new Date(fromDate.getTime() - 7 * 86400000);
      const bufferEnd = new Date(toDate.getTime() + 7 * 86400000);
      return db.attribution.findMany({
        where: {
          shopDomain,
          OR: [
            { confidence: { gt: 0 }, matchedAt: { gte: bufferStart, lte: bufferEnd } },
            { confidence: 0, matchedAt: { gte: bufferStart, lte: bufferEnd } },
          ],
        },
        select: { shopifyOrderId: true, confidence: true, metaConversionValue: true },
      });
    })(),
    db.order.findMany({
      where: { shopDomain, utmConfirmedMeta: true, isOnlineStore: true, createdAt: dateFilter },
      select: { shopifyOrderId: true, frozenTotalPrice: true, totalRefunded: true },
    }),
    // Match Accuracy chart sources - last 30 days, bucketed by the *day
    // the conversion / order actually happened*, not by `matchedAt`.
    //
    // Why we don't use Attribution.matchedAt:
    //   matchedAt is "the moment the matcher created/recreated this row".
    //   Full Re-matches stamp every row with `now`, and unmatched
    //   placeholders for old conversions get re-emitted on every cycle.
    //   Bucketing the chart by matchedAt therefore tells you "how busy
    //   the matcher was on that day", not the actual match rate. We hit
    //   this on 22/04 (chart said 50%, truth was 100%) and 05/04 (chart
    //   showed 4,025 rows from a Full Re-match; truth was 4 conversions).
    //
    // Authoritative shape:
    //   denominator = SUM(MetaInsight.conversions) on day D
    //   numerator   = COUNT(Attribution rows with confidence>0 whose
    //                       Order.createdAt falls on day D, shop-local)
    db.metaInsight.findMany({
      where: { shopDomain, date: { gte: thirtyDaysAgo } },
      select: { date: true, conversions: true },
    }),
    (async () => {
      // Order <-> Attribution have no Prisma relation, only a shopifyOrderId
      // string link. Two-step: pull recent order IDs first, then check which
      // ones have a confidence>0 Attribution row.
      const recentOrders = await db.order.findMany({
        where: { shopDomain, createdAt: { gte: thirtyDaysAgo } },
        select: { shopifyOrderId: true, createdAt: true },
      });
      if (recentOrders.length === 0) return [];
      const ids = recentOrders.map(o => o.shopifyOrderId);
      const matched = await db.attribution.findMany({
        where: {
          shopDomain,
          shopifyOrderId: { in: ids },
          confidence: { gt: 0 },
        },
        select: { shopifyOrderId: true },
      });
      const matchedSet = new Set(matched.map(a => a.shopifyOrderId));
      return recentOrders.filter(o => matchedSet.has(o.shopifyOrderId));
    })(),
    // Campaigns + ad sets that stopped delivering in last 7 days. Ad sets
    // are flagged separately so the merchant sees adset-level pauses even
    // when the parent campaign is still ACTIVE - common case where one
    // adset gets paused for budget reallocation while siblings keep running.
    db.metaEntity.findMany({
      where: {
        shopDomain,
        entityType: { in: ["campaign", "adset"] },
        currentStatus: { in: ["PAUSED", "ARCHIVED", "DELETED"] },
        effectiveEndAt: { gte: sevenDaysAgo },
      },
      select: { entityType: true, entityName: true, currentStatus: true, effectiveEndAt: true },
      orderBy: { effectiveEndAt: "desc" },
      take: 20,
    }),
    // Active campaign count
    db.metaEntity.count({
      where: { shopDomain, entityType: "campaign", currentStatus: "ACTIVE" },
    }),
  ]);

  // Count distinct customers who placed orders in this date range
  const customerCount = ordersInRange.filter(o => o.shopifyCustomerId).length;

  // Net revenue from ALL Shopify orders in period
  const netRevenue = (revenueAgg._sum.frozenTotalPrice || 0) - (revenueAgg._sum.totalRefunded || 0);

  // Filter attributions to orders within date range
  const orderIdSet = new Set(orderIdsInRange.map(o => o.shopifyOrderId));
  const fromStr = fromKey;
  const toStr = toKey;
  const attrsInRange = allAttrs.filter(a => {
    if (a.confidence > 0) return orderIdSet.has(a.shopifyOrderId);
    const match = a.shopifyOrderId.match(/(\d{4}-\d{2}-\d{2})/);
    if (!match) return false;
    return match[1] >= fromStr && match[1] <= toStr;
  });

  const attributionCount = attrsInRange.length;
  const matched = attrsInRange.filter(a => a.confidence > 0);
  const unmatchedAttrs = attrsInRange.filter(a => a.confidence === 0);
  const unmatched = unmatchedAttrs.length;
  const unmatchedRevenue = unmatchedAttrs.reduce((s, a) => s + (a.metaConversionValue || 0), 0);
  const avgConfidence = matched.length > 0
    ? Math.round(matched.reduce((s, a) => s + a.confidence, 0) / matched.length) : 0;

  // Net Meta revenue
  const matchedOrderIds = matched.map(a => a.shopifyOrderId);
  const matchedOrders = matchedOrderIds.length > 0
    ? await db.order.findMany({
        where: { shopDomain, isOnlineStore: true, shopifyOrderId: { in: matchedOrderIds } },
        select: { frozenTotalPrice: true, totalRefunded: true },
      })
    : [];
  const matchedMetaRevenue = matchedOrders.reduce((s, o) =>
    s + (o.frozenTotalPrice || 0) - (o.totalRefunded || 0), 0);

  const matchedOrderIdSet = new Set(matchedOrderIds);
  const utmOnlyNotMatched = utmOnlyOrders.filter(o => !matchedOrderIdSet.has(o.shopifyOrderId));
  const utmOnlyCount = utmOnlyNotMatched.length;
  const utmOnlyRevenue = utmOnlyNotMatched.reduce((s, o) =>
    s + (o.frozenTotalPrice || 0) - (o.totalRefunded || 0), 0);
  const utmAndLucidlyCount = utmOnlyOrders.length - utmOnlyCount;
  const netMetaRevenue = matchedMetaRevenue + utmOnlyRevenue;
  const currencySymbol = currencySymbolFromCode(shop?.shopifyCurrency);

  const isNewInstall = !shop?.lastOrderSync && orderCount === 0;

  // Build 30-day match accuracy chart from authoritative sources.
  // Denominator = SUM(MetaInsight.conversions) for day D
  // Numerator   = COUNT(matched orders whose Order.createdAt falls on D, shop-local)
  // We deliberately DO NOT bucket Attribution rows by matchedAt - that field
  // tracks "when the matcher created the row", not the conversion day.
  const metaConvByDay = new Map();
  for (const r of liveMetaInsights30d) {
    const day = r.date ? new Date(r.date).toISOString().slice(0, 10) : null;
    if (!day) continue;
    metaConvByDay.set(day, (metaConvByDay.get(day) || 0) + (r.conversions || 0));
  }
  const matchedByDay = new Map();
  for (const o of liveMatchedOrders30d) {
    if (!o.createdAt) continue;
    const day = shopLocalDayKey(tz, o.createdAt);
    if (!day) continue;
    matchedByDay.set(day, (matchedByDay.get(day) || 0) + 1);
  }
  const allDays = new Set([...metaConvByDay.keys(), ...matchedByDay.keys()]);
  const matchAccuracyChart = Array.from(allDays)
    .sort((a, b) => a.localeCompare(b))
    .map(day => {
      const total = Math.round(metaConvByDay.get(day) || 0);
      const matched = matchedByDay.get(day) || 0;
      const matchRate = total > 0
        ? Math.min(100, Math.round((matched / total) * 100))
        : null;
      return { date: day, matchRate, matched, total };
    });

  // 30-day average match accuracy (headline number)
  const allMatched30d = matchAccuracyChart.reduce((s, d) => s + d.matched, 0);
  const allTotal30d = matchAccuracyChart.reduce((s, d) => s + d.total, 0);
  const matchRate30d = allTotal30d > 0 ? Math.min(100, Math.round((allMatched30d / allTotal30d) * 100)) : null;

  // UTM health from shop record
  const utmHealth = {
    total: shop?.utmAdsTotal || 0,
    withTags: shop?.utmAdsWithTags || 0,
    missing: shop?.utmAdsMissing || 0,
    lastAudit: shop?.utmLastAudit || null,
    coveragePct: (shop?.utmAdsTotal || 0) > 0
      ? Math.round(((shop?.utmAdsWithTags || 0) / (shop?.utmAdsTotal || 1)) * 100) : null,
  };

  // Sync freshness
  const syncFreshness = {
    orderSyncAgo: shop?.lastOrderSync ? Math.round((now.getTime() - new Date(shop.lastOrderSync).getTime()) / 60000) : null,
    metaSyncAgo: shop?.lastMetaSync ? Math.round((now.getTime() - new Date(shop.lastMetaSync).getTime()) / 60000) : null,
  };

  // Check if any background task is currently running for this shop
  const taskNames = ["syncOrders", "syncMeta", "syncMetaHistorical", "runAttribution", "dateRangeRematch", "fillGaps", "incrementalSync", "startOngoingSync", "calibratePixel", "inferGender", "backfillFirstNames", "forceRollups", "refreshAdThumbnails"];
  let activeTaskFromServer = null;
  for (const t of taskNames) {
    const p = getProgress(`${t}:${shopDomain}`);
    if (p && p.status === "running") {
      activeTaskFromServer = t;
      break;
    }
  }

  return json({
    shopDomain, orderCount, customerCount, newCustomerOrders, existingCustomerOrders,
    totalSpend: totalSpend._sum.spend || 0, netRevenue, netMetaRevenue,
    lastSync: shop?.lastOrderSync || null, lastMetaSync: shop?.lastMetaSync || null,
    metaConnected: !!shop?.metaAccessToken, metaAdAccountId: shop?.metaAdAccountId || null,
    attribution: { total: attributionCount, matched: matched.length, avgConfidence, unmatched, unmatchedRevenue },
    currencySymbol, isNewInstall, activeTaskFromServer,
    utmOnlyCount, utmOnlyRevenue, utmAndLucidlyCount,
    onboardingCompleted: shop?.onboardingCompleted || false,
    onboardingPhase: shop?.onboardingPhase || "shopify",
    onboardingStartedAt: shop?.onboardingStartedAt || null,
    webhooksRegisteredAt: shop?.webhooksRegisteredAt || null,
    webhooksFirstFiredAt: shop?.webhooksFirstFiredAt || null,
    pixelCalibration: {
      calibratedAt: shop?.metaValueCalibratedAt || null,
      samples: shop?.metaValueCalibrationSamples || 0,
      results: shop?.metaValueCalibrationResults ? (() => { try { return JSON.parse(shop.metaValueCalibrationResults); } catch { return null; } })() : null,
    },
    // Health-specific data
    matchAccuracyChart,
    matchRate30d,
    matchRate30dDetail: { matched: allMatched30d, total: allTotal30d },
    utmHealth,
    syncFreshness,
    recentlyStoppedCampaigns: recentlyStoppedCampaigns.map(c => ({
      level: c.entityType,
      name: c.entityName,
      status: c.currentStatus,
      stoppedAt: c.effectiveEndAt?.toISOString() || null,
    })),
    activeCampaignCount,
    // Role gate: only LUCIDLY_INTERNAL_SHOPS see ops tooling. Production
    // merchants see a clean dashboard. Andy adds a shop to the env var
    // when he needs to debug it inside that merchant's store.
    isInternal: isInternalShop(shopDomain),
  });
};

// Action fires the task in the background and returns immediately.
export const action = async ({ request }) => {
  const { admin, session } = await authenticate.admin(request);
  const shopDomain = session.shop;
  const formData = await request.formData();
  const actionType = formData.get("action");

  const taskId = `${actionType}:${shopDomain}`;

  const runInBackground = (fn) => {
    setProgress(taskId, { status: "running", message: "Starting..." });
    fn().catch((err) => {
      console.error(`[${actionType}] Background task failed:`, err);
      failProgress(taskId, err);
    });
  };

  if (actionType === "syncOrders") {
    runInBackground(() => syncOrders(admin, shopDomain));
    return json({ started: true, task: "syncOrders" });
  }
  if (actionType === "syncMeta") {
    runInBackground(async () => {
      await syncMetaAll(shopDomain);
    });
    return json({ started: true, task: "syncMeta" });
  }
  if (actionType === "syncMetaHistorical") {
    runInBackground(async () => {
      await syncMetaAll(shopDomain, 730, taskId);
      const { linkUtmToCampaigns } = await import("../services/utmLinkage.server");
      await linkUtmToCampaigns(shopDomain);
    });
    return json({ started: true, task: "syncMetaHistorical" });
  }
  if (actionType === "runAttribution") {
    runInBackground(async () => {
      await runAttribution(shopDomain);
      const { linkUtmToCampaigns } = await import("../services/utmLinkage.server");
      await linkUtmToCampaigns(shopDomain);
    });
    return json({ started: true, task: "runAttribution" });
  }
  if (actionType === "dateRangeRematch") {
    const fromDate = formData.get("fromDate");
    const toDate = formData.get("toDate");
    if (!fromDate || !toDate) return json({ success: false, error: "Missing date range" });
    runInBackground(() => runDateRangeRematch(shopDomain, String(fromDate), String(toDate)));
    return json({ started: true, task: "dateRangeRematch" });
  }
  if (actionType === "fillGaps") {
    runInBackground(() => runFillGaps(shopDomain));
    return json({ started: true, task: "fillGaps" });
  }
  if (actionType === "startOngoingSync") {
    await db.shop.update({ where: { shopDomain }, data: { onboardingCompleted: true } });
    runInBackground(async () => {
      try {
        setProgress(taskId, { status: "running", message: "Calibrating Meta pixel..." });
        const { calibratePixel } = await import("../services/pixelCalibration.server.js");
        await calibratePixel(shopDomain);
      } catch (err) {
        console.error("[startOngoingSync] Pixel calibration failed (non-fatal):", err.message);
      }
      setProgress(taskId, { status: "running", message: "Running first incremental sync..." });
      await clearTodayForRematch(shopDomain);
      return runIncrementalSync(shopDomain);
    });
    return json({ started: true, task: "startOngoingSync" });
  }
  if (actionType === "calibratePixel") {
    runInBackground(async () => {
      setProgress(taskId, { status: "running", message: "Scanning historical data for clean pairs..." });
      const { calibratePixel } = await import("../services/pixelCalibration.server.js");
      const results = await calibratePixel(shopDomain);
      completeProgress(taskId, results);
    });
    return json({ started: true, task: "calibratePixel" });
  }
  if (actionType === "incrementalSync") {
    runInBackground(async () => {
      await clearTodayForRematch(shopDomain);
      return runIncrementalSync(shopDomain);
    });
    return json({ started: true, task: "incrementalSync" });
  }
  if (actionType === "forceRollups") {
    // Bypass the 24h rollup throttle in incrementalSync. Used when a deploy
    // changes the rollup output schema (e.g. new fields on ltvCustomers)
    // and the cached blob needs to refresh before the next forced rebuild.
    //
    // Order: campaign rollups FIRST (the heaviest - 622k MetaInsight rows for
    // Vollebak load into a bucket map and easily push heap past 3GB). Doing
    // it on a freshly-spawned action keeps headroom; running customer/product
    // first then campaign was OOM-killing the VM. global.gc() between phases
    // releases the bucket map before the next rebuild's working set lands.
    runInBackground(async () => {
      try {
        setProgress(taskId, { status: "running", message: "Rebuilding campaign rollups (this is the slow one - ~5min)..." });
        const { rebuildCampaignRollups } = await import("../services/campaignRollups.server.js");
        await rebuildCampaignRollups(shopDomain);
        if (global.gc) global.gc();

        setProgress(taskId, { status: "running", message: "Rebuilding ad demographic rollups..." });
        const { rebuildAdDemographicRollups } = await import("../services/adDemographicRollups.server.js");
        await rebuildAdDemographicRollups(shopDomain);
        if (global.gc) global.gc();

        setProgress(taskId, { status: "running", message: "Rebuilding customer rollups..." });
        const { rebuildCustomerSegments, rebuildCustomerRollups } = await import("../services/customerRollups.server.js");
        await rebuildCustomerSegments(shopDomain);
        await rebuildCustomerRollups(shopDomain);
        if (global.gc) global.gc();

        setProgress(taskId, { status: "running", message: "Rebuilding product rollups..." });
        const { rebuildProductRollups } = await import("../services/productRollups.server.js");
        await rebuildProductRollups(shopDomain);
        if (global.gc) global.gc();

        const { invalidateShop } = await import("../services/queryCache.server.js");
        invalidateShop(shopDomain);
        completeProgress(taskId, { ok: true });
      } catch (err: any) {
        console.error(`[forceRollups] ${shopDomain} failed: ${err?.message}`);
        failProgress(taskId, err?.message || String(err));
      }
    });
    return json({ started: true, task: "forceRollups" });
  }
  if (actionType === "inferGender") {
    runInBackground(async () => {
      const { backfillShopInferredGender } = await import("../services/nameGender.server.js");
      const result = await backfillShopInferredGender(db, shopDomain);
      completeProgress(taskId, result);
    });
    return json({ started: true, task: "inferGender" });
  }
  if (actionType === "backfillFirstNames") {
    runInBackground(async () => {
      const { backfillCustomerFirstNames } = await import("../services/orderSync.server.js");
      await backfillCustomerFirstNames(admin, shopDomain);
    });
    return json({ started: true, task: "backfillFirstNames" });
  }
  if (actionType === "refreshAdThumbnails") {
    runInBackground(async () => {
      setProgress(taskId, { status: "running", message: "Fetching ad creative URLs from Meta..." });
      const { refreshAdCreatives } = await import("../services/metaAdCreativeSync.server.js");
      const result = await refreshAdCreatives(shopDomain);
      const { invalidateShop } = await import("../services/queryCache.server.js");
      invalidateShop(shopDomain);
      completeProgress(taskId, result);
    });
    return json({ started: true, task: "refreshAdThumbnails" });
  }
  return json({ success: false });
};

// ═══════════════════════════════════════════════════════════════
// Match Accuracy Line Chart - hand-rolled SVG, 30 days
// ═══════════════════════════════════════════════════════════════

function MatchAccuracyChart({ data, accent }: { data: { date: string; matchRate: number | null; matched: number; total: number }[]; accent: string }) {
  const [hoverIdx, setHoverIdx] = useState<number | null>(null);

  // Filter to days with data
  const points = data.filter(d => d.matchRate !== null);
  if (points.length < 2) {
    return (
      <div style={{ padding: "20px", textAlign: "center", color: "#6B7280" }}>
        Not enough data for chart (need at least 2 days)
      </div>
    );
  }

  const W = 600;
  const H = 180;
  const padL = 40;
  const padR = 16;
  const padT = 16;
  const padB = 32;
  const chartW = W - padL - padR;
  const chartH = H - padT - padB;

  // Y-axis: 0-100%
  const yMin = 0;
  const yMax = 100;
  const xStep = chartW / (points.length - 1);

  const toX = (i: number) => padL + i * xStep;
  const toY = (v: number) => padT + chartH - ((v - yMin) / (yMax - yMin)) * chartH;

  // Build line path
  const linePath = points.map((p, i) => `${i === 0 ? "M" : "L"} ${toX(i).toFixed(1)} ${toY(p.matchRate!).toFixed(1)}`).join(" ");
  // Area fill
  const areaPath = `${linePath} L ${toX(points.length - 1).toFixed(1)} ${toY(0).toFixed(1)} L ${toX(0).toFixed(1)} ${toY(0).toFixed(1)} Z`;

  // 90% reference line
  const refY = toY(90);

  // X-axis labels: show ~5 evenly spaced dates
  const labelCount = Math.min(5, points.length);
  const labelStep = Math.max(1, Math.floor((points.length - 1) / (labelCount - 1)));
  const xLabels: { i: number; label: string }[] = [];
  for (let i = 0; i < points.length; i += labelStep) {
    const d = points[i].date;
    xLabels.push({ i, label: d.slice(5) }); // MM-DD
  }
  // Always include last point
  if (xLabels[xLabels.length - 1]?.i !== points.length - 1) {
    xLabels.push({ i: points.length - 1, label: points[points.length - 1].date.slice(5) });
  }

  return (
    <div style={{ position: "relative" }}>
      <svg
        width="100%"
        height={H}
        viewBox={`0 0 ${W} ${H}`}
        preserveAspectRatio="xMidYMid meet"
        style={{ display: "block" }}
      >
        {/* Grid lines */}
        {[0, 25, 50, 75, 100].map(v => (
          <g key={v}>
            <line x1={padL} y1={toY(v)} x2={W - padR} y2={toY(v)} stroke="#E5E7EB" strokeWidth={v === 0 ? 1 : 0.5} />
            <text x={padL - 6} y={toY(v) + 4} textAnchor="end" fontSize="10" fill="#9CA3AF">{v}%</text>
          </g>
        ))}

        {/* 90% target reference line */}
        <line x1={padL} y1={refY} x2={W - padR} y2={refY} stroke="#10B981" strokeWidth={1} strokeDasharray="4 3" opacity={0.6} />
        <text x={W - padR + 2} y={refY + 3} fontSize="9" fill="#10B981" fontWeight="600">target</text>

        {/* Area fill */}
        <path d={areaPath} fill={accent} opacity={0.08} />

        {/* Line */}
        <path d={linePath} fill="none" stroke={accent} strokeWidth={2} strokeLinejoin="round" />

        {/* Data points */}
        {points.map((p, i) => (
          <circle
            key={i}
            cx={toX(i)}
            cy={toY(p.matchRate!)}
            r={hoverIdx === i ? 5 : 3}
            fill={p.matchRate! >= 90 ? "#10B981" : p.matchRate! >= 70 ? "#F59E0B" : "#EF4444"}
            stroke="#fff"
            strokeWidth={1.5}
            style={{ cursor: "pointer", transition: "r 0.1s" }}
            onMouseEnter={() => setHoverIdx(i)}
            onMouseLeave={() => setHoverIdx(null)}
          />
        ))}

        {/* X-axis labels */}
        {xLabels.map(({ i, label }) => (
          <text key={i} x={toX(i)} y={H - 4} textAnchor="middle" fontSize="10" fill="#9CA3AF">{label}</text>
        ))}
      </svg>

      {/* Hover tooltip */}
      {hoverIdx !== null && points[hoverIdx] && (
        <div
          style={{
            position: "absolute",
            left: `${(toX(hoverIdx) / W) * 100}%`,
            top: `${toY(points[hoverIdx].matchRate!) - 8}px`,
            transform: "translate(-50%, -100%)",
            background: "#1F2937",
            color: "#fff",
            padding: "6px 10px",
            borderRadius: "6px",
            fontSize: "12px",
            lineHeight: "1.4",
            whiteSpace: "nowrap",
            pointerEvents: "none",
            zIndex: 10,
          }}
        >
          <div style={{ fontWeight: 700 }}>{points[hoverIdx].date}</div>
          <div>Match rate: {points[hoverIdx].matchRate}%</div>
          <div>{points[hoverIdx].matched} matched / {points[hoverIdx].total} total</div>
        </div>
      )}
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// Status Pill
// ═══════════════════════════════════════════════════════════════

function StatusPill({ label, ok, warning, detail }: { label: string; ok: boolean; warning?: boolean; detail?: string }) {
  const bg = ok ? "#ECFDF5" : warning ? "#FFFBEB" : "#FEF2F2";
  const border = ok ? "#10B981" : warning ? "#F59E0B" : "#EF4444";
  const color = ok ? "#065F46" : warning ? "#92400E" : "#991B1B";
  const icon = ok ? "\u2713" : warning ? "!" : "\u2717";

  return (
    <div style={{
      display: "inline-flex", alignItems: "center", gap: "6px",
      padding: "6px 14px", borderRadius: "20px",
      background: bg, border: `1px solid ${border}33`,
      fontSize: "13px", fontWeight: 600, color,
    }}>
      <span style={{
        display: "inline-flex", alignItems: "center", justifyContent: "center",
        width: "18px", height: "18px", borderRadius: "50%",
        background: border, color: "#fff", fontSize: "11px", fontWeight: 700,
      }}>{icon}</span>
      {label}
      {detail && <span style={{ fontWeight: 400, fontSize: "12px", opacity: 0.8 }}>{detail}</span>}
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// Format minutes to human-readable string
// ═══════════════════════════════════════════════════════════════

function formatMinutes(mins: number | null): string {
  if (mins === null) return "never";
  if (mins < 1) return "just now";
  if (mins < 60) return `${mins}m ago`;
  if (mins < 1440) return `${Math.round(mins / 60)}h ago`;
  return `${Math.round(mins / 1440)}d ago`;
}

function daysAgo(isoDate: string | null): string {
  if (!isoDate) return "";
  const diff = Math.round((Date.now() - new Date(isoDate).getTime()) / 86400000);
  if (diff === 0) return "today";
  if (diff === 1) return "yesterday";
  return `${diff}d ago`;
}

// ═══════════════════════════════════════════════════════════════
// Page Component
// ═══════════════════════════════════════════════════════════════

export default function Index() {
  const {
    shopDomain, orderCount, customerCount, newCustomerOrders,
    existingCustomerOrders, totalSpend, netRevenue, netMetaRevenue,
    lastSync, lastMetaSync, metaConnected, metaAdAccountId, attribution,
    currencySymbol, isNewInstall, activeTaskFromServer,
    utmOnlyCount, utmOnlyRevenue, utmAndLucidlyCount, onboardingCompleted,
    onboardingPhase, onboardingStartedAt,
    webhooksRegisteredAt, webhooksFirstFiredAt, pixelCalibration,
    matchAccuracyChart, matchRate30d, matchRate30dDetail, utmHealth, syncFreshness,
    recentlyStoppedCampaigns, activeCampaignCount, isInternal,
  } = useLoaderData();
  const submit = useSubmit();
  const navigate = useNavigate();
  const navigation = useNavigation();
  const { revalidate } = useRevalidator();
  const [searchParams] = useSearchParams();

  const dateQuery = () => {
    const params = new URLSearchParams();
    for (const key of ["from", "to", "preset", "compare"]) {
      const val = searchParams.get(key);
      if (val) params.set(key, val);
    }
    const str = params.toString();
    return str ? `?${str}` : "";
  };

  const [activeTask, setActiveTask] = useState(null);
  const [progress, setProgressState] = useState(null);
  const intervalRef = useRef(null);

  const stopPolling = useCallback(() => {
    if (intervalRef.current) {
      clearInterval(intervalRef.current);
      intervalRef.current = null;
    }
  }, []);

  const pendingTaskRef = useRef(null);

  const startTask = useCallback((actionName, extraData = {}) => {
    setActiveTask(actionName);
    setProgressState({ status: "running", message: "Starting..." });
    pendingTaskRef.current = actionName;
    submit({ action: actionName, ...extraData }, { method: "post" });
  }, [submit]);

  useEffect(() => {
    if (navigation.state === "idle" && pendingTaskRef.current) {
      const taskName = pendingTaskRef.current;
      pendingTaskRef.current = null;

      const poll = async () => {
        try {
          const res = await fetch(`/app/api/progress?task=${taskName}`);
          const data = await res.json();
          if (data.progress) {
            setProgressState(data.progress);
            if (data.progress.status === "complete") {
              stopPolling();
              setActiveTask(null);
              setProgressState(null);
              revalidate();
            } else if (data.progress.status === "error") {
              stopPolling();
              setActiveTask(null);
              setProgressState({ status: "error", error: data.progress.error });
              setTimeout(() => setProgressState(null), 8000);
            }
          } else {
            stopPolling();
            setActiveTask(null);
            setProgressState(null);
          }
        } catch (e) {
          // ignore fetch errors during polling
        }
      };

      poll();
      intervalRef.current = setInterval(poll, 2000);
    }
  }, [navigation.state, stopPolling, revalidate]);

  const resumedRef = useRef(false);
  useEffect(() => {
    if (activeTaskFromServer && !resumedRef.current && !activeTask) {
      resumedRef.current = true;
      const taskName = activeTaskFromServer;
      setActiveTask(taskName);
      setProgressState({ status: "running", message: "Resuming..." });

      const poll = async () => {
        try {
          const res = await fetch(`/app/api/progress?task=${taskName}`);
          const data = await res.json();
          if (data.progress) {
            setProgressState(data.progress);
            if (data.progress.status === "complete") {
              stopPolling();
              setActiveTask(null);
              setProgressState(null);
              revalidate();
            } else if (data.progress.status === "error") {
              stopPolling();
              setActiveTask(null);
              setProgressState({ status: "error", error: data.progress.error });
              setTimeout(() => setProgressState(null), 8000);
            }
          } else {
            stopPolling();
            setActiveTask(null);
            setProgressState(null);
          }
        } catch (e) {}
      };

      poll();
      intervalRef.current = setInterval(poll, 2000);
    }
  }, [activeTaskFromServer]);

  useEffect(() => stopPolling, [stopPolling]);

  const isRunning = !!activeTask;

  const matchedPct = attribution.total > 0
    ? Math.round((attribution.matched / attribution.total) * 100) : 0;

  const progressPct = progress?.total
    ? Math.round((progress.current / progress.total) * 100) : null;

  // Sync freshness status
  const syncOk = syncFreshness.metaSyncAgo !== null && syncFreshness.metaSyncAgo < 180; // < 3 hours
  const syncWarning = syncFreshness.metaSyncAgo !== null && syncFreshness.metaSyncAgo < 1440; // < 24 hours

  return (
    <Page title="Health" fullWidth>
      <ReportTabs>
      <BlockStack gap="500">

        {/* ═══ Status Pills ═══ */}
        <div style={{ display: "flex", flexWrap: "wrap", gap: "8px" }}>
          <StatusPill label="Shopify" ok={orderCount > 0} detail={orderCount > 0 ? `${orderCount.toLocaleString()} orders` : "no orders"} />
          <StatusPill label="Meta" ok={metaConnected} warning={!metaConnected} detail={metaConnected ? metaAdAccountId : "not connected"} />
          <StatusPill
            label="Webhooks"
            ok={!!webhooksFirstFiredAt}
            warning={!!webhooksRegisteredAt && !webhooksFirstFiredAt}
            detail={webhooksFirstFiredAt ? "active" : webhooksRegisteredAt ? "pending" : "not registered"}
          />
          <StatusPill
            label="Orders Sync"
            ok={syncFreshness.orderSyncAgo !== null && syncFreshness.orderSyncAgo < 180}
            warning={syncFreshness.orderSyncAgo !== null && syncFreshness.orderSyncAgo < 1440}
            detail={formatMinutes(syncFreshness.orderSyncAgo)}
          />
          <StatusPill
            label="Meta Sync"
            ok={syncOk}
            warning={!syncOk && syncWarning}
            detail={formatMinutes(syncFreshness.metaSyncAgo)}
          />
          <StatusPill
            label="Pixel"
            ok={!!pixelCalibration?.results?.winner}
            warning={!!pixelCalibration?.calibratedAt && !pixelCalibration?.results?.winner}
            detail={
              pixelCalibration?.results?.winner
                ? `${pixelCalibration.results.winner} (\u00B1${(pixelCalibration.results.winnerDeviation * 100).toFixed(1)}%)`
                : pixelCalibration?.calibratedAt ? "insufficient data" : "not calibrated"
            }
          />
        </div>

        {/* ═══ Match Accuracy + UTM Health ═══ */}
        <Layout>
          <Layout.Section variant="oneHalf">
            <Card>
              <BlockStack gap="300">
                <InlineStack align="space-between" blockAlign="center">
                  <Text as="h2" variant="headingLg">Match Accuracy</Text>
                  {matchRate30d !== null && (
                    <div style={{
                      fontSize: "28px", fontWeight: 800, letterSpacing: "-0.02em",
                      color: matchRate30d >= 90 ? "#059669" : matchRate30d >= 70 ? "#D97706" : "#DC2626",
                    }}>
                      {matchRate30d}%
                      <span style={{ fontSize: "13px", fontWeight: 500, color: "#6B7280", marginLeft: "6px" }}>
                        30 days ({matchRate30dDetail.matched}/{matchRate30dDetail.total})
                      </span>
                    </div>
                  )}
                </InlineStack>
                {matchRate30d === null && (
                  <Text as="p" variant="bodySm" tone="subdued">No conversion data in the last 30 days</Text>
                )}
                <MatchAccuracyChart data={matchAccuracyChart} accent="#5C6AC4" />
                <Text as="p" variant="bodySm" tone="subdued">
                  Matched attributions / total attributions per day (live data). Green dashed line = 90% target.
                </Text>
              </BlockStack>
            </Card>
          </Layout.Section>
          <Layout.Section variant="oneHalf">
            <Card>
              <BlockStack gap="300">
                <InlineStack align="space-between" blockAlign="center">
                  <Text as="h2" variant="headingLg">UTM Health</Text>
                  {utmHealth.coveragePct !== null && (
                    <div style={{
                      fontSize: "28px", fontWeight: 800, letterSpacing: "-0.02em",
                      color: utmHealth.coveragePct >= 90 ? "#059669" : utmHealth.coveragePct >= 70 ? "#D97706" : "#DC2626",
                    }}>
                      {utmHealth.coveragePct}%
                      <span style={{ fontSize: "13px", fontWeight: 500, color: "#6B7280", marginLeft: "6px" }}>coverage</span>
                    </div>
                  )}
                </InlineStack>

                {utmHealth.total > 0 ? (
                  <BlockStack gap="200">
                    <div style={{ display: "flex", gap: "16px", flexWrap: "wrap" }}>
                      <div style={{ flex: 1, minWidth: "120px", padding: "12px 16px", borderRadius: "8px", background: "#ECFDF5", textAlign: "center" }}>
                        <div style={{ fontSize: "22px", fontWeight: 700, color: "#065F46" }}>{utmHealth.withTags}</div>
                        <div style={{ fontSize: "12px", color: "#6B7280" }}>with UTMs</div>
                      </div>
                      <div style={{ flex: 1, minWidth: "120px", padding: "12px 16px", borderRadius: "8px", background: utmHealth.missing > 0 ? "#FEF2F2" : "#F9FAFB", textAlign: "center" }}>
                        <div style={{ fontSize: "22px", fontWeight: 700, color: utmHealth.missing > 0 ? "#991B1B" : "#374151" }}>{utmHealth.missing}</div>
                        <div style={{ fontSize: "12px", color: "#6B7280" }}>missing UTMs</div>
                      </div>
                    </div>
                    {utmHealth.lastAudit && (
                      <Text as="p" variant="bodySm" tone="subdued">
                        Last audit: {new Date(utmHealth.lastAudit).toLocaleDateString()} ({utmHealth.total} ads scanned)
                      </Text>
                    )}
                  </BlockStack>
                ) : (
                  <Text as="p" variant="bodySm" tone="subdued">No UTM audit run yet</Text>
                )}

                <Button onClick={() => navigate(`/app/utm${dateQuery()}`)}>
                  Open UTM Manager
                </Button>
              </BlockStack>
            </Card>
          </Layout.Section>
        </Layout>

        {/* ═══ Campaign Alerts + Data Quality ═══ */}
        <Layout>
          <Layout.Section variant="oneHalf">
            <Card>
              <BlockStack gap="300">
                <InlineStack align="space-between" blockAlign="center">
                  <Text as="h2" variant="headingLg">Campaign &amp; Ad Set Alerts</Text>
                  <div style={{
                    fontSize: "13px", fontWeight: 600, padding: "4px 12px",
                    borderRadius: "12px", background: "#EFF6FF", color: "#1D4ED8",
                  }}>
                    {activeCampaignCount} active
                  </div>
                </InlineStack>

                {recentlyStoppedCampaigns.length > 0 ? (
                  <BlockStack gap="100">
                    <Text as="p" variant="bodySm" tone="subdued">
                      Campaigns and ad sets that stopped delivering in the last 7 days:
                    </Text>
                    {recentlyStoppedCampaigns.map((c, i) => {
                      const isAdset = c.level === "adset";
                      return (
                        <div key={i} style={{
                          display: "flex", justifyContent: "space-between", alignItems: "center",
                          padding: "8px 12px", borderRadius: "6px", background: "#FEF2F2",
                          fontSize: "13px", gap: 8,
                        }}>
                          <span style={{ display: "inline-flex", alignItems: "center", gap: 8, minWidth: 0, flex: 1 }}>
                            <span style={{
                              fontSize: "10.5px", fontWeight: 700, padding: "2px 7px",
                              borderRadius: "10px", textTransform: "uppercase", letterSpacing: 0.3,
                              background: isAdset ? "#EDE9FE" : "#FEE2E2",
                              color: isAdset ? "#5B21B6" : "#991B1B",
                              flexShrink: 0,
                            }}>
                              {isAdset ? "Ad Set" : "Campaign"}
                            </span>
                            <span style={{ fontWeight: 600, color: "#374151", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                              {c.name || (isAdset ? "Unnamed ad set" : "Unnamed campaign")}
                            </span>
                          </span>
                          <span style={{ color: "#6B7280", fontSize: "12px", flexShrink: 0 }}>
                            {c.status.toLowerCase()} {daysAgo(c.stoppedAt)}
                          </span>
                        </div>
                      );
                    })}
                  </BlockStack>
                ) : (
                  <div style={{
                    padding: "20px", textAlign: "center", borderRadius: "8px",
                    background: "#ECFDF5", color: "#065F46", fontSize: "14px", fontWeight: 500,
                  }}>
                    No campaigns or ad sets stopped in the last 7 days
                  </div>
                )}

                <Button onClick={() => navigate(`/app/campaigns${dateQuery()}`)}>
                  View All Campaigns
                </Button>
              </BlockStack>
            </Card>
          </Layout.Section>
          <Layout.Section variant="oneHalf">
            <Card>
              <BlockStack gap="300">
                <Text as="h2" variant="headingLg">Data Quality</Text>
                <div style={{ display: "flex", gap: "12px", flexWrap: "wrap" }}>
                  <div style={{ flex: 1, minWidth: "120px", padding: "12px 16px", borderRadius: "8px", background: "#F0F1FF", textAlign: "center" }}>
                    <div style={{ fontSize: "22px", fontWeight: 700, color: "#4650A8" }}>
                      {attribution.avgConfidence}%
                    </div>
                    <div style={{ fontSize: "12px", color: "#6B7280" }}>avg confidence</div>
                  </div>
                  <div style={{ flex: 1, minWidth: "120px", padding: "12px 16px", borderRadius: "8px", background: "#F0F1FF", textAlign: "center" }}>
                    <div style={{ fontSize: "22px", fontWeight: 700, color: "#4650A8" }}>
                      {attribution.matched.toLocaleString()}
                    </div>
                    <div style={{ fontSize: "12px", color: "#6B7280" }}>matched attributions</div>
                  </div>
                  <div style={{ flex: 1, minWidth: "120px", padding: "12px 16px", borderRadius: "8px", background: attribution.unmatched > 0 ? "#FFFBEB" : "#F9FAFB", textAlign: "center" }}>
                    <div style={{ fontSize: "22px", fontWeight: 700, color: attribution.unmatched > 0 ? "#92400E" : "#374151" }}>
                      {attribution.unmatched.toLocaleString()}
                    </div>
                    <div style={{ fontSize: "12px", color: "#6B7280" }}>unmatched</div>
                  </div>
                </div>

                <div style={{ display: "flex", gap: "12px", flexWrap: "wrap" }}>
                  <div style={{ flex: 1, minWidth: "140px", padding: "12px 16px", borderRadius: "8px", background: attribution.unmatchedRevenue > 0 ? "#FFFBEB" : "#F9FAFB", textAlign: "center" }}>
                    <div style={{ fontSize: "18px", fontWeight: 700, color: attribution.unmatchedRevenue > 0 ? "#92400E" : "#374151" }}>
                      {currencySymbol}{Math.round(attribution.unmatchedRevenue).toLocaleString()}
                    </div>
                    <div style={{ fontSize: "12px", color: "#6B7280" }}>unmatched revenue</div>
                  </div>
                  <div style={{ flex: 1, minWidth: "140px", padding: "12px 16px", borderRadius: "8px", background: "#ECFDF5", textAlign: "center" }}>
                    <div style={{ fontSize: "18px", fontWeight: 700, color: "#065F46" }}>
                      {matchedPct}%
                    </div>
                    <div style={{ fontSize: "12px", color: "#6B7280" }}>match rate (selected period)</div>
                  </div>
                </div>

                <Text as="p" variant="bodySm" tone="subdued">
                  Match rate and Data Quality use live attribution data - always in sync with Order Explorer.
                  Unmatched = Meta conversions we couldn't verify against a Shopify order.
                </Text>
              </BlockStack>
            </Card>
          </Layout.Section>
        </Layout>

        {/* ═══ Phased ingest progress card (auto-driven, shown only while
            onboardingPhase=="ingesting"). The card hides itself when the
            backend reports onboardingCompleted=true and triggers a revalidate
            so the dashboard re-renders with the freshly-imported data. ═══ */}
        {!onboardingCompleted && onboardingPhase === "ingesting" && (
          <OnboardingProgressCard />
        )}

        {/* ═══ Manual Getting-Started buttons - kept for the pre-ingest
            phases (shopify/fit/meta) and for any merchant who skipped the
            auto-ingest path. Hidden once the orchestrator is running so we
            don't show two cards at once. ═══ */}
        {!onboardingCompleted && onboardingPhase !== "ingesting" && (
          <Card>
            <BlockStack gap="300">
              <Text as="h2" variant="headingLg">Getting Started</Text>
              <InlineStack gap="400" wrap>
                <BlockStack gap="100">
                  <Button variant={orderCount === 0 ? "primary" : undefined} onClick={() => startTask("syncOrders")} disabled={isRunning} loading={activeTask === "syncOrders"}>
                    1. Sync Shopify Orders
                  </Button>
                  <Text as="p" variant="bodySm" tone="subdued">
                    {lastSync ? `\u2713 ${orderCount.toLocaleString()} orders imported` : "Import 2 years of order history"}
                  </Text>
                </BlockStack>

                <BlockStack gap="100">
                  <Button
                    variant={!metaConnected && orderCount > 0 ? "primary" : undefined}
                    onClick={() => navigate("/app/meta-connect")}
                    disabled={isRunning}
                  >
                    {metaConnected ? "2. Connect Meta Ads \u2713" : "2. Connect Meta Ads"}
                  </Button>
                  <Text as="p" variant="bodySm" tone="subdued">
                    {metaConnected ? `Connected: ${metaAdAccountId}` : "Link your Meta ad account"}
                  </Text>
                </BlockStack>

                <BlockStack gap="100">
                  <Button
                    variant={metaConnected && !lastMetaSync ? "primary" : undefined}
                    onClick={() => startTask("syncMetaHistorical")}
                    disabled={isRunning || !metaConnected}
                    loading={activeTask === "syncMetaHistorical"}
                  >
                    {lastMetaSync ? "3. Sync Meta Ads Data \u2713" : "3. Sync Meta Ads Data"}
                  </Button>
                  <Text as="p" variant="bodySm" tone="subdued">
                    {lastMetaSync ? `Last: ${new Date(lastMetaSync).toLocaleString()}` : "2 years of ad performance data"}
                  </Text>
                </BlockStack>

                <BlockStack gap="100">
                  <Button
                    variant={lastMetaSync && orderCount > 0 && attribution.total === 0 ? "primary" : undefined}
                    onClick={() => startTask("runAttribution")}
                    disabled={isRunning || !metaConnected || orderCount === 0 || !lastMetaSync}
                    loading={activeTask === "runAttribution"}
                  >
                    {attribution.total > 0 ? "4. Run Customer Matcher \u2713" : "4. Run Customer Matcher"}
                  </Button>
                  <Text as="p" variant="bodySm" tone="subdued">
                    {attribution.total > 0 ? `${attribution.matched} matched, ${attribution.unmatched} unmatched` : "Match orders to Meta conversions"}
                  </Text>
                </BlockStack>

                <BlockStack gap="100">
                  <Button
                    tone="success"
                    variant={attribution.total > 0 ? "primary" : undefined}
                    onClick={() => startTask("startOngoingSync")}
                    disabled={isRunning || attribution.total === 0}
                    loading={activeTask === "startOngoingSync"}
                  >
                    5. Start Ongoing Syncing
                  </Button>
                  <Text as="p" variant="bodySm" tone="subdued">Enable automatic hourly sync</Text>
                </BlockStack>
              </InlineStack>

              {progress?.status === "running" && (
                <BlockStack gap="200">
                  <InlineStack gap="200" align="center">
                    <Spinner size="small" />
                    <Text as="p" variant="bodyMd">{progress.message || "Processing..."}</Text>
                  </InlineStack>
                  {progressPct !== null && (
                    <ProgressBar progress={progressPct} tone="highlight" size="small" />
                  )}
                </BlockStack>
              )}
              {progress?.status === "error" && (
                <Banner tone="critical"><p>Task failed: {progress.error}</p></Banner>
              )}
            </BlockStack>
          </Card>
        )}

        {/* ═══ Internal Tools (Lucidly admin only) ═══
            Gated behind isInternalShop() - controlled by LUCIDLY_INTERNAL_SHOPS
            env var (CSV of shop domains). Production merchants see a clean
            dashboard; Andy adds a shop here temporarily when debugging.
        */}
        {isInternal && (
        <Card>
          <BlockStack gap="100">
            <InlineStack gap="200" blockAlign="center">
              <Text as="h2" variant="headingLg">Internal Tools</Text>
              <span style={{
                fontSize: 10.5, fontWeight: 700, padding: "2px 8px", borderRadius: 10,
                background: "#FEF3C7", color: "#92400E", textTransform: "uppercase", letterSpacing: 0.4,
              }}>Lucidly admin only</span>
            </InlineStack>
            <Text as="p" variant="bodySm" tone="subdued">
              Operational levers - sync triggers, re-matchers, backfills. Hidden from merchants.
            </Text>
          </BlockStack>
          <div style={{ marginTop: 16 }} />
          <BlockStack gap="300">
            <InlineStack gap="300" wrap>
              <BlockStack gap="100">
                <Button variant="primary" onClick={() => startTask("syncOrders")} disabled={isRunning}
                  loading={activeTask === "syncOrders"}>
                  {activeTask === "syncOrders" ? "Syncing Orders..." : (orderCount > 0 ? "Sync Orders" : "Import Orders")}
                </Button>
                <Text as="p" variant="bodySm" tone="subdued">
                  {lastSync ? `Last: ${new Date(lastSync).toLocaleString()}` : "Never"}
                </Text>
              </BlockStack>
              {metaConnected && (
                <BlockStack gap="100">
                  <Button onClick={() => startTask("syncMeta")} disabled={isRunning}
                    loading={activeTask === "syncMeta"}>
                    {activeTask === "syncMeta" ? "Syncing Meta..." : "Sync Meta (7d)"}
                  </Button>
                  <Text as="p" variant="bodySm" tone="subdued">
                    {lastMetaSync ? `Last: ${new Date(lastMetaSync).toLocaleString()}` : "Never"}
                  </Text>
                </BlockStack>
              )}
              {metaConnected && orderCount > 0 && (
                <BlockStack gap="100">
                  <Button tone="success" onClick={() => startTask("incrementalSync")} disabled={isRunning}
                    loading={activeTask === "incrementalSync"}>
                    {activeTask === "incrementalSync" ? "Running..." : "Incremental Sync"}
                  </Button>
                  <Text as="p" variant="bodySm" tone="subdued">Pulls today's data + matches new conversions</Text>
                </BlockStack>
              )}
              {orderCount > 0 && (
                <BlockStack gap="100">
                  <Button onClick={() => startTask("forceRollups")} disabled={isRunning}
                    loading={activeTask === "forceRollups"}>
                    {activeTask === "forceRollups" ? "Rebuilding..." : "Force Rebuild Rollups"}
                  </Button>
                  <Text as="p" variant="bodySm" tone="subdued">Bypasses 24h throttle. Use after deploys that change rollup output.</Text>
                </BlockStack>
              )}
              {metaConnected && orderCount > 0 && attribution.total > 0 && (
                <BlockStack gap="100">
                  <Button onClick={() => startTask("runAttribution")} disabled={isRunning}
                    loading={activeTask === "runAttribution"}>
                    {activeTask === "runAttribution" ? "Running..." : "Full Re-match"}
                  </Button>
                  <Text as="p" variant="bodySm" tone="subdued">
                    {attribution.total > 0 ? `${attribution.total} attributions` : "Not run yet"}
                  </Text>
                </BlockStack>
              )}
              {metaConnected && orderCount > 0 && attribution.total > 0 && (
                <BlockStack gap="100">
                  <Button onClick={() => startTask("fillGaps")} disabled={isRunning}
                    loading={activeTask === "fillGaps"} tone="success">
                    {activeTask === "fillGaps" ? "Running..." : "Fill Gaps"}
                  </Button>
                  <Text as="p" variant="bodySm" tone="subdued">Auto-detects &amp; matches missing days</Text>
                </BlockStack>
              )}
              {metaConnected && orderCount > 0 && attribution.total > 0 && (
                <BlockStack gap="100">
                  <Button tone="critical" onClick={() => startTask("dateRangeRematch", { fromDate: "2025-10-18", toDate: "2026-01-30" })} disabled={isRunning}
                    loading={activeTask === "dateRangeRematch"}>
                    {activeTask === "dateRangeRematch" ? "Running..." : "Re-match Oct-Jan"}
                  </Button>
                  <Text as="p" variant="bodySm" tone="subdued">Re-matches Oct 18 - Jan 30 only</Text>
                </BlockStack>
              )}
              {orderCount > 0 && (
                <BlockStack gap="100">
                  <Button onClick={() => startTask("inferGender")} disabled={isRunning}
                    loading={activeTask === "inferGender"}>
                    {activeTask === "inferGender" ? "Running..." : "Infer Gender from Names"}
                  </Button>
                  <Text as="p" variant="bodySm" tone="subdued">Backfill demographics from billing names</Text>
                </BlockStack>
              )}
              {orderCount > 0 && (
                <BlockStack gap="100">
                  <Button onClick={() => startTask("backfillFirstNames")} disabled={isRunning}
                    loading={activeTask === "backfillFirstNames"}>
                    {activeTask === "backfillFirstNames" ? "Running..." : "Backfill First Names"}
                  </Button>
                  <Text as="p" variant="bodySm" tone="subdued">Pulls billing first names from Shopify, then re-runs gender inference</Text>
                </BlockStack>
              )}
              {metaConnected && (
                <BlockStack gap="100">
                  <Button onClick={() => startTask("refreshAdThumbnails")} disabled={isRunning}
                    loading={activeTask === "refreshAdThumbnails"}>
                    {activeTask === "refreshAdThumbnails" ? "Refreshing..." : "Refresh Ad Thumbnails"}
                  </Button>
                  <Text as="p" variant="bodySm" tone="subdued">Pulls Meta creative thumbnails for the Ad Explorer. Auto-runs nightly; use this to refresh now.</Text>
                </BlockStack>
              )}
            </InlineStack>

            {progress?.status === "error" && (
              <Banner tone="critical">
                <p>Task failed: {progress.error}</p>
              </Banner>
            )}

            {progress?.status === "running" && (
              <BlockStack gap="200">
                <InlineStack gap="200" align="center">
                  <Spinner size="small" />
                  <Text as="p" variant="bodyMd">
                    {progress.message || "Processing..."}
                  </Text>
                </InlineStack>
                {progressPct !== null && (
                  <ProgressBar progress={progressPct} tone="highlight" size="small" />
                )}
              </BlockStack>
            )}
          </BlockStack>
        </Card>
        )}
      </BlockStack>
      </ReportTabs>
    </Page>
  );
}
