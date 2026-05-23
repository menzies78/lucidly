import { json } from "@remix-run/node";
import { useLoaderData, useSubmit, useNavigate, useNavigation, useRevalidator, useSearchParams } from "@remix-run/react";
import { Page, Layout, Card, Text, Button, BlockStack, InlineStack, Banner, ProgressBar, Spinner } from "@shopify/polaris";
import ReportTabs from "../components/ReportTabs";
import OnboardingFlow from "../components/OnboardingFlow";
import { useState, useEffect, useRef, useCallback } from "react";
import { authenticate, unauthenticated } from "../shopify.server";
import db from "../db.server";
import { syncOrders } from "../services/orderSync.server";
import { syncMetaAll } from "../services/metaSync.server";
import { runAttribution, runDateRangeRematch, runFillGaps } from "../services/matcher.server";
import { runIncrementalSync, clearTodayForRematch } from "../services/incrementalSync.server";
import { setProgress, failProgress, getProgress, completeProgress } from "../services/progress.server";
import { parseDateRange } from "../utils/dateRange.server";
import { currencySymbolFromCode } from "../utils/currency";
import { cached as queryCached } from "../services/queryCache.server";
import { isInternalShop } from "../utils/access.server";

export const loader = async ({ request }) => {
  const { admin, session } = await authenticate.admin(request);
  const shopDomain = session.shop;

  const shop = await db.shop.findUnique({ where: { shopDomain } });
  const tz = shop?.shopifyTimezone || "UTC";

  // ── Onboarding install gate ─────────────────────────────────────────
  // The new flow is driven entirely by Shop.onboardingPhase (welcome →
  // fit-importing → fit-running → fit-ready → ingesting → complete).
  // The OnboardingFlow component handles the UI; we no longer auto-fire
  // a full backfill or redirect to /app/fit-test on first load. The full
  // 2-year Shopify backfill happens later, inside the orchestrator's
  // shopify track, which kicks off after the merchant connects Meta.
  //
  // Make sure a Shop row exists so OnboardingFlow has something to drive.
  if (!shop) {
    await db.shop.upsert({
      where: { shopDomain },
      create: { shopDomain, onboardingPhase: "welcome" },
      update: {},
    });
  }
  // ────────────────────────────────────────────────────────────────────

  const { fromDate, toDate, fromKey, toKey } = parseDateRange(request, tz);
  const dateFilter = { gte: fromDate, lte: toDate };

  // 7 days ago for "recently stopped campaigns" health alert.
  const now = new Date();
  const sevenDaysAgo = new Date(now.getTime() - 7 * 86400000);

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
    matchAccuracyBlob,
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
    // Match Accuracy chart - read from precomputed blob written by
    // dashboardRollups.server.js after each incremental sync. Blob holds
    // 90 days of daily {date, matched, total, matchRate} buckets plus
    // rolling 30d/7d rates. We slice last 30 below.
    //
    // Authoritative shape (computed in dashboardRollups):
    //   denominator = SUM(MetaInsight.conversions) on day D
    //   numerator   = COUNT(Attribution rows with confidence>0 whose
    //                       Order.createdAt falls on day D, shop-local)
    //
    // We deliberately do NOT bucket Attribution rows by matchedAt - that
    // field tracks "when the matcher created the row", not the conversion
    // day, and Full Re-matches would skew the chart heavily.
    db.shopAnalysisCache.findUnique({
      where: { shopDomain_cacheKey: { shopDomain, cacheKey: "dashboard:matchAccuracy" } },
      select: { payload: true },
    }),
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

  // Pull the precomputed match-accuracy blob. Blob stores up to 730 days of
  // daily {matched, total, confSum, confAvg} buckets plus rolling 7d / 30d /
  // lifetime totals for both match rate and match confidence. Client decides
  // which window to render via the toggle on the Match Rate / Match
  // Confidence tiles.
  type DayBucket = { date: string; matchRate: number | null; matched: number; total: number; confSum: number; confAvg: number | null };
  let matchAccuracyDays: DayBucket[] = [];
  let matchRate30d: number | null = null;
  let matchRateLifetime: number | null = null;
  let matchRate30dDetail = { matched: 0, total: 0 };
  let matchRateLifetimeDetail = { matched: 0, total: 0 };
  let matchConf30d: number | null = null;
  let matchConfLifetime: number | null = null;
  let matchConf30dDetail = { matched: 0, confSum: 0 };
  let matchConfLifetimeDetail = { matched: 0, confSum: 0 };
  if (matchAccuracyBlob?.payload) {
    try {
      const parsed = JSON.parse(matchAccuracyBlob.payload);
      matchAccuracyDays = Array.isArray(parsed.days) ? parsed.days : [];
      // Back-fill missing fields on old blobs so the UI doesn't NaN out.
      matchAccuracyDays = matchAccuracyDays.map((d: any) => ({
        date: d.date,
        matchRate: d.matchRate ?? null,
        matched: d.matched || 0,
        total: d.total || 0,
        confSum: d.confSum || 0,
        confAvg: d.confAvg ?? (d.matched > 0 && d.confSum ? Math.round(d.confSum / d.matched) : null),
      }));

      matchRate30d = parsed.rate30d ?? null;
      matchRate30dDetail = parsed.rate30dDetail || { matched: 0, total: 0 };
      matchRateLifetime = parsed.rateLifetime ?? null;
      matchRateLifetimeDetail = parsed.rateLifetimeDetail || { matched: 0, total: 0 };

      matchConf30d = parsed.conf30d ?? null;
      matchConf30dDetail = parsed.conf30dDetail || { matched: 0, confSum: 0 };
      matchConfLifetime = parsed.confLifetime ?? null;
      matchConfLifetimeDetail = parsed.confLifetimeDetail || { matched: 0, confSum: 0 };

      // Old blob (pre-confidence rollup) - derive rate fields from days if
      // they're missing so the tiles still render after a deploy but before
      // the next rollup cycle.
      if (matchRate30d === null && matchAccuracyDays.length > 0) {
        const tail = matchAccuracyDays.slice(Math.max(0, matchAccuracyDays.length - 30));
        matchRate30dDetail = {
          matched: tail.reduce((s, d) => s + d.matched, 0),
          total: tail.reduce((s, d) => s + d.total, 0),
        };
        matchRate30d = matchRate30dDetail.total > 0
          ? Math.min(100, Math.round((matchRate30dDetail.matched / matchRate30dDetail.total) * 100))
          : null;
      }
      if (matchRateLifetime === null && matchAccuracyDays.length > 0) {
        matchRateLifetimeDetail = {
          matched: matchAccuracyDays.reduce((s, d) => s + d.matched, 0),
          total: matchAccuracyDays.reduce((s, d) => s + d.total, 0),
        };
        matchRateLifetime = matchRateLifetimeDetail.total > 0
          ? Math.min(100, Math.round((matchRateLifetimeDetail.matched / matchRateLifetimeDetail.total) * 100))
          : null;
      }
    } catch (err) {
      console.error(`[app._index] failed to parse matchAccuracyBlob: ${(err as Error).message}`);
    }
  }

  // UTM health from shop record. Tile is cached — only the nightly audit
  // refreshes these counters, so the dashboard never blocks on a live Meta API
  // call. Consistency = how many ads use the dominant template vs how many
  // differ ("drifted"). Drift is usually a sign of an old template change or
  // manual edits that should be reconciled.
  const utmHealth = {
    total: shop?.utmAdsTotal || 0,
    withTags: shop?.utmAdsWithTags || 0,
    missing: shop?.utmAdsMissing || 0,
    consistent: shop?.utmAdsConsistent || 0,
    inconsistent: shop?.utmAdsInconsistent || 0,
    lastAudit: shop?.utmLastAudit || null,
    coveragePct: (shop?.utmAdsTotal || 0) > 0
      ? Math.round(((shop?.utmAdsWithTags || 0) / (shop?.utmAdsTotal || 1)) * 100) : null,
  };

  // Sync freshness
  const syncFreshness = {
    orderSyncAgo: shop?.lastOrderSync ? Math.round((now.getTime() - new Date(shop.lastOrderSync).getTime()) / 60000) : null,
    metaSyncAgo: shop?.lastMetaSync ? Math.round((now.getTime() - new Date(shop.lastMetaSync).getTime()) / 60000) : null,
  };

  // Internal-only: list of full-shop backups (newest first). Only loaded
  // when the viewer is an internal shop, so production merchants never
  // pay the disk-read cost.
  const isInternal = isInternalShop(shopDomain);
  let backups: Array<{ backupId: string; startedAt: string; completedAt?: string; totalRows?: number; verified?: boolean; sqliteBytes?: number; lastDownloadedAt?: string | null; downloadUrl?: string }> = [];
  if (isInternal) {
    try {
      const { listBackups } = await import("../services/shopBackup.server.js");
      const { signDownloadUrl } = await import("../utils/backupToken.server.js");
      const list = await listBackups(shopDomain);
      backups = list.slice(0, 20).map((b: any) => ({
        backupId: b.backupId,
        startedAt: b.startedAt,
        completedAt: b.completedAt,
        totalRows: b.totalRows,
        verified: b.verified || false,
        sqliteBytes: b.sqliteSnapshot?.bytes || 0,
        lastDownloadedAt: b.lastDownloadedAt || null,
        // Pre-sign the download URL so the button is a plain top-level
        // navigation - browser file downloads can't carry the App Bridge
        // session token, so /app/* auth would bounce to a login page.
        downloadUrl: signDownloadUrl(shopDomain, b.backupId),
      }));
    } catch (err: any) {
      console.error("[app._index] listBackups failed:", err.message);
    }
  }

  // Check if any background task is currently running for this shop
  const taskNames = ["syncOrders", "syncMeta", "syncMetaHistorical", "runAttribution", "dateRangeRematch", "fillGaps", "incrementalSync", "startOngoingSync", "calibratePixel", "inferGender", "backfillFirstNames", "forceRollups", "refreshAdThumbnails", "backupShop", "wipeShop", "restoreShop", "verifyBackup", "purgeData"];
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
    onboardingPhase: shop?.onboardingPhase || "welcome",
    onboardingStartedAt: shop?.onboardingStartedAt || null,
    fitTestScore: shop?.fitTestScore ?? null,
    fitTestComputedAt: shop?.fitTestComputedAt || null,
    webhooksRegisteredAt: shop?.webhooksRegisteredAt || null,
    webhooksFirstFiredAt: shop?.webhooksFirstFiredAt || null,
    pixelCalibration: {
      calibratedAt: shop?.metaValueCalibratedAt || null,
      samples: shop?.metaValueCalibrationSamples || 0,
      results: shop?.metaValueCalibrationResults ? (() => { try { return JSON.parse(shop.metaValueCalibrationResults); } catch { return null; } })() : null,
    },
    // Health-specific data
    matchAccuracyDays,
    matchRate30d,
    matchRate30dDetail,
    matchRateLifetime,
    matchRateLifetimeDetail,
    matchConf30d,
    matchConf30dDetail,
    matchConfLifetime,
    matchConfLifetimeDetail,
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
    isInternal,
    backups,
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
  // ── Onboarding: Begin Fit Test ──────────────────────────────────────
  // Triggered by the Welcome card's only button. Chains:
  //   1. set Shop.onboardingPhase = "fit-importing"
  //   2. syncOrdersForFitTest (90d minimal - just timestamps + values)
  //   3. set onboardingPhase = "fit-running"
  //   4. runFitTest (rival-density scoring, populates Shop.fitTestScore)
  //   5. set onboardingPhase = "fit-ready"
  //
  // Each phase transition is persisted to the DB so OnboardingFlow's poll
  // (which reads Shop.onboardingPhase) advances the UI in real time. If a
  // step throws, we leave the merchant on the failed phase rather than
  // silently rolling back - the next page load can then retry from where
  // they were.
  if (actionType === "begin-fit-test") {
    // Fire-and-forget. The form submit returns 303 immediately and
    // OnboardingFlow's polling does the rest.
    //
    // CRITICAL: do NOT capture the request-scoped `admin` GraphQL client in
    // the IIFE - by the time the IIFE actually runs, the request has ended
    // and the session reference is gone, so every GraphQL call throws
    // "Missing access token when creating GraphQL client". Build a fresh
    // admin client from the offline token via unauthenticated.admin() instead.
    (async () => {
      try {
        const { syncOrdersForFitTest } = await import("../services/orderSync.server.js");
        const { runFitTest } = await import("../services/fitTest.server.js");

        await db.shop.upsert({
          where: { shopDomain },
          create: { shopDomain, onboardingPhase: "fit-importing", onboardingStartedAt: new Date() },
          update: { onboardingPhase: "fit-importing", onboardingStartedAt: new Date() },
        });

        const { admin: bgAdmin } = await unauthenticated.admin(shopDomain);
        await syncOrdersForFitTest(bgAdmin, shopDomain);

        await db.shop.update({
          where: { shopDomain },
          data: { onboardingPhase: "fit-running" },
        });

        await runFitTest(shopDomain);

        await db.shop.update({
          where: { shopDomain },
          data: { onboardingPhase: "fit-ready" },
        });
      } catch (err) {
        console.error(`[begin-fit-test] failed for ${shopDomain}: ${err.message}`);
        // Leave phase where it is - UI can retry by re-clicking on next load.
      }
    })();
    return json({ started: true, task: "begin-fit-test" });
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

        // Geo + matchAccuracy + customerGenderDaily were previously missing
        // from this handler. When the orchestrator's geo step silently failed
        // during onboarding (Vollebak: transaction timeout on 200k+ rows), the
        // merchant had no recovery path — Countries tab stayed empty until
        // the next conversion-bearing incremental sync. Including them here
        // makes the button a true full rebuild.
        setProgress(taskId, { status: "running", message: "Rebuilding geo rollups..." });
        const { rebuildGeoRollups } = await import("../services/geoRollups.server.js");
        await rebuildGeoRollups(shopDomain);
        if (global.gc) global.gc();

        setProgress(taskId, { status: "running", message: "Rebuilding dashboard match accuracy..." });
        const { rebuildMatchAccuracy } = await import("../services/dashboardRollups.server.js");
        await rebuildMatchAccuracy(shopDomain);
        if (global.gc) global.gc();

        setProgress(taskId, { status: "running", message: "Rebuilding customer gender chart data..." });
        const { rebuildCustomerGenderDaily } = await import("../services/customerRollups.server.js");
        await rebuildCustomerGenderDaily(shopDomain);
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
  // ── Internal-only: full-shop backup, wipe, restore ──
  // Used to re-experience the new-merchant install flow on a real merchant
  // (typically Vollebak in dev). Wipe is gated behind a fresh-backup check
  // inside the service, so missing the backup step throws rather than
  // silently destroying data.
  if (actionType === "backupShop") {
    if (!isInternalShop(shopDomain)) return json({ success: false, error: "forbidden" });
    runInBackground(async () => {
      const { backupShop } = await import("../services/shopBackup.server.js");
      const manifest = await backupShop(shopDomain, (m) =>
        setProgress(taskId, { status: "running", message: m })
      );
      completeProgress(taskId, { backupId: manifest.backupId, totalRows: manifest.totalRows });
    });
    return json({ started: true, task: "backupShop" });
  }
  if (actionType === "wipeShop") {
    if (!isInternalShop(shopDomain)) return json({ success: false, error: "forbidden" });
    runInBackground(async () => {
      const { wipeShop } = await import("../services/shopBackup.server.js");
      const result = await wipeShop(shopDomain, (m) =>
        setProgress(taskId, { status: "running", message: m })
      );
      const { invalidateShop } = await import("../services/queryCache.server.js");
      invalidateShop(shopDomain);
      completeProgress(taskId, result);
    });
    return json({ started: true, task: "wipeShop" });
  }
  // Data-only purge: wipes orders, attribution, meta data, all rollups,
  // ingest jobs, AI insights - then resets onboarding state and kicks the
  // orchestrator. Crucially preserves Shop (with metaAccessToken /
  // metaAdAccountId) and Session (Shopify OAuth) so the merchant doesn't
  // need to reinstall. Internal-only. Designed for dev iteration where
  // we want to test the full ingest path against a clean slate without
  // losing the OAuth handshakes.
  if (actionType === "purgeData") {
    if (!isInternalShop(shopDomain)) return json({ success: false, error: "forbidden" });
    runInBackground(async () => {
      setProgress(taskId, { status: "running", message: "Purging data tables..." });
      // Delete in child-first order to satisfy FKs. Mirrors the wipeShop
      // child-first sequence but skips Shop + Session.
      const tables = [
        // Derived / rollup tables first
        "ingestJob",
        "dailyAdRollup",
        "dailyAdDemographicRollup",
        "dailyCustomerRollup",
        "dailyProductRollup",
        "dailyGeoRollup",
        "shopAnalysisCache",
        // Source data, child rows before parents
        "aiInsight",
        "attribution",
        "metaCountrySnapshot",
        "metaSnapshot",
        "metaChange",
        "metaEntity",
        "metaBreakdown",
        "metaInsight",
        "orderLineItem",
        "order",
        "customer",
      ];
      let totalDeleted = 0;
      for (let i = 0; i < tables.length; i++) {
        const t = tables[i];
        setProgress(taskId, { status: "running", message: `Wiping ${t} (${i + 1}/${tables.length})` });
        try {
          const r = await db[t].deleteMany({ where: { shopDomain } });
          totalDeleted += r.count;
        } catch (err) {
          console.warn(`[purgeData] Wipe failed for ${t}: ${err.message}`);
        }
      }
      // Reset onboarding + sync state so the orchestrator + dashboard
      // treat this as a fresh install. Keep metaAccessToken,
      // metaAdAccountId, shopifyTimezone - those are the OAuth artefacts
      // we're explicitly preserving.
      setProgress(taskId, { status: "running", message: "Resetting onboarding state..." });
      await db.shop.update({
        where: { shopDomain },
        data: {
          onboardingPhase: "ingesting",
          onboardingCompleted: false,
          onboardingStartedAt: new Date(),
          fitTestScore: null,
          fitTestComputedAt: null,
          fitTestData: null,
          lastOrderSync: null,
          lastMetaSync: null,
          lastRollupRebuild: null,
          webhooksRegisteredAt: null,
          webhooksFirstFiredAt: null,
          metaValueCalibratedAt: null,
          metaValueCalibrationSamples: 0,
          metaValueCalibrationResults: "",
        },
      });
      const { invalidateShop } = await import("../services/queryCache.server.js");
      invalidateShop(shopDomain);
      // Kick the orchestrator. startOnboardingIngest is fire-and-forget;
      // it rebuilds its own admin client from the preserved Session row.
      setProgress(taskId, { status: "running", message: `Purged ${totalDeleted} rows. Starting fresh ingest...` });
      const { startOnboardingIngest } = await import("../services/ingestOrchestrator.server.js");
      await startOnboardingIngest(shopDomain);
      completeProgress(taskId, { deleted: totalDeleted, ingestStarted: true });
    });
    return json({ started: true, task: "purgeData" });
  }
  if (actionType === "verifyBackup") {
    if (!isInternalShop(shopDomain)) return json({ success: false, error: "forbidden" });
    const backupId = formData.get("backupId");
    if (!backupId) return json({ success: false, error: "missing backupId" });
    runInBackground(async () => {
      const { verifyBackup } = await import("../services/shopBackup.server.js");
      const result = await verifyBackup(shopDomain, String(backupId));
      completeProgress(taskId, result);
    });
    return json({ started: true, task: "verifyBackup" });
  }
  if (actionType === "restoreShop") {
    if (!isInternalShop(shopDomain)) return json({ success: false, error: "forbidden" });
    const backupId = formData.get("backupId");
    runInBackground(async () => {
      const { restoreShop } = await import("../services/shopBackup.server.js");
      const result = await restoreShop(shopDomain, backupId ? String(backupId) : null, (m) =>
        setProgress(taskId, { status: "running", message: m })
      );
      const { invalidateShop } = await import("../services/queryCache.server.js");
      invalidateShop(shopDomain);
      completeProgress(taskId, result);
    });
    return json({ started: true, task: "restoreShop" });
  }
  return json({ success: false });
};

// ═══════════════════════════════════════════════════════════════
// Match Rate / Match Confidence tile - shared shell with toggle
// ═══════════════════════════════════════════════════════════════

type ChartDay = { date: string; matchRate: number | null; matched: number; total: number; confSum: number; confAvg: number | null };

function MatchTile({
  title, description, accent, metric, days,
  value30, valueLifetime, detail30, detailLifetime,
  detailLabel, detailTotalLabel,
}: {
  title: string;
  description: string;
  accent: string;
  metric: "rate" | "confidence";
  days: ChartDay[];
  value30: number | null;
  valueLifetime: number | null;
  detail30: { matched: number; total: number };
  detailLifetime: { matched: number; total: number };
  detailLabel: string;
  detailTotalLabel: string;
}) {
  const [range, setRange] = useState<"lifetime" | "30d">("30d");
  const headline = range === "lifetime" ? valueLifetime : value30;
  const detail = range === "lifetime" ? detailLifetime : detail30;
  const slice = range === "lifetime"
    ? days.slice(Math.max(0, days.length - 365))
    : days.slice(Math.max(0, days.length - 30));

  const headlineColor = headline === null
    ? "#6B7280"
    : headline >= 90 ? "#059669" : headline >= 70 ? "#D97706" : "#DC2626";

  const PillButton = ({ active, onClick, children }: { active: boolean; onClick: () => void; children: React.ReactNode }) => (
    <button
      onClick={onClick}
      style={{
        padding: "4px 10px",
        borderRadius: "12px",
        border: active ? `1px solid ${accent}` : "1px solid #E5E7EB",
        background: active ? `${accent}15` : "#fff",
        color: active ? accent : "#6B7280",
        fontSize: "12px",
        fontWeight: 600,
        cursor: "pointer",
      }}
    >
      {children}
    </button>
  );

  return (
    <Card>
      <BlockStack gap="300">
        <InlineStack align="space-between" blockAlign="center">
          <Text as="h2" variant="headingLg">{title}</Text>
          <div style={{ display: "inline-flex", gap: "6px" }}>
            <PillButton active={range === "lifetime"} onClick={() => setRange("lifetime")}>1 year</PillButton>
            <PillButton active={range === "30d"} onClick={() => setRange("30d")}>30 days</PillButton>
          </div>
        </InlineStack>

        {headline !== null ? (
          <div style={{
            fontSize: "32px", fontWeight: 800, letterSpacing: "-0.02em",
            color: headlineColor, lineHeight: 1.1,
          }}>
            {headline}%
            <span style={{ fontSize: "13px", fontWeight: 500, color: "#6B7280", marginLeft: "10px" }}>
              {metric === "rate"
                ? `${detail.matched} ${detailLabel} / ${detail.total} ${detailTotalLabel}`
                : `across ${detail.matched} ${detailTotalLabel}`}
            </span>
          </div>
        ) : (
          <Text as="p" variant="bodySm" tone="subdued">
            {range === "lifetime" ? "No data yet" : "No conversion data in the last 30 days"}
          </Text>
        )}

        <MatchAccuracyChart data={slice} accent={accent} metric={metric} />

        <Text as="p" variant="bodySm" tone="subdued">
          {description}
        </Text>
      </BlockStack>
    </Card>
  );
}

// ═══════════════════════════════════════════════════════════════
// Match Accuracy Line Chart - hand-rolled SVG
// ═══════════════════════════════════════════════════════════════

function MatchAccuracyChart({ data, accent, metric }: { data: ChartDay[]; accent: string; metric: "rate" | "confidence" }) {
  const [hoverIdx, setHoverIdx] = useState<number | null>(null);

  const valueOf = (d: ChartDay) => metric === "rate" ? d.matchRate : d.confAvg;

  // Daily bars - no bucketing. Days without data are skipped (filtered out
  // below) so the bar series only shows days where there's something to
  // measure. With up to ~730 daily points across 600px the bars become a
  // dense band you can scan for dips at-a-glance.
  const points = data.filter(d => valueOf(d) !== null);
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

  // Bar layout: each day claims an even slice of chartW. Gap is 15% of
  // slice width (capped at 1.5px so dense series stay readable).
  const slotW = chartW / points.length;
  const gap = Math.min(1.5, slotW * 0.15);
  const barW = Math.max(0.6, slotW - gap);

  const toX = (i: number) => padL + i * slotW + gap / 2;
  const toY = (v: number) => padT + chartH - ((v - yMin) / (yMax - yMin)) * chartH;

  // 90% reference line
  const refY = toY(90);

  // X-axis labels: show ~5 evenly spaced dates
  const labelCount = Math.min(5, points.length);
  const labelStep = Math.max(1, Math.floor((points.length - 1) / (labelCount - 1)));
  const xLabels: { i: number; label: string }[] = [];
  for (let i = 0; i < points.length; i += labelStep) {
    xLabels.push({ i, label: points[i].date.slice(5) }); // MM-DD
  }
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

        {/* Daily bars */}
        {points.map((p, i) => {
          const v = valueOf(p)!;
          const y = toY(v);
          const h = padT + chartH - y;
          const fill = v >= 90 ? "#10B981" : v >= 70 ? "#F59E0B" : "#EF4444";
          return (
            <rect
              key={i}
              x={toX(i)}
              y={y}
              width={barW}
              height={Math.max(1, h)}
              fill={fill}
              opacity={hoverIdx === i ? 1 : 0.85}
              style={{ cursor: "pointer", transition: "opacity 0.1s" }}
              onMouseEnter={() => setHoverIdx(i)}
              onMouseLeave={() => setHoverIdx(null)}
            />
          );
        })}

        {/* X-axis labels */}
        {xLabels.map(({ i, label }) => (
          <text key={i} x={toX(i) + barW / 2} y={H - 4} textAnchor="middle" fontSize="10" fill="#9CA3AF">{label}</text>
        ))}
      </svg>

      {/* Hover tooltip */}
      {hoverIdx !== null && points[hoverIdx] && (
        <div
          style={{
            position: "absolute",
            left: `${((toX(hoverIdx) + barW / 2) / W) * 100}%`,
            top: `${toY(valueOf(points[hoverIdx])!) - 8}px`,
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
          {metric === "rate" ? (
            <>
              <div>Match rate: {points[hoverIdx].matchRate}%</div>
              <div>{points[hoverIdx].matched} matched / {points[hoverIdx].total} total</div>
            </>
          ) : (
            <>
              <div>Avg confidence: {points[hoverIdx].confAvg}%</div>
              <div>{points[hoverIdx].matched} matched orders</div>
            </>
          )}
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
    onboardingPhase, onboardingStartedAt, fitTestScore, fitTestComputedAt,
    webhooksRegisteredAt, webhooksFirstFiredAt, pixelCalibration,
    matchAccuracyDays, matchRate30d, matchRate30dDetail,
    matchRateLifetime, matchRateLifetimeDetail,
    matchConf30d, matchConf30dDetail,
    matchConfLifetime, matchConfLifetimeDetail,
    utmHealth, syncFreshness,
    recentlyStoppedCampaigns, activeCampaignCount, isInternal, backups,
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

  // ── Onboarding gate ────────────────────────────────────────────────
  // Until onboardingCompleted flips to true the merchant sees ONLY the
  // OnboardingFlow component (welcome → fit-test → ingest). The rest of
  // the dashboard is hidden so the merchant has 100% focus on step 1.
  // Internal shops still see their tools because we (Andy) need them to
  // debug merchants mid-onboarding.
  if (!onboardingCompleted) {
    return (
      <Page title="Welcome to Lucidly" fullWidth>
        <BlockStack gap="500">
          <OnboardingFlow shopDomain={shopDomain} />
          {isInternal && (
            <Banner tone="warning">
              <Text as="p" variant="bodyMd">
                Internal admin: dashboard hidden until merchant completes onboarding.
                Phase: <strong>{onboardingPhase}</strong>
                {fitTestScore !== null && fitTestScore !== undefined ? ` · Fit Score: ${fitTestScore}` : ""}
              </Text>
            </Banner>
          )}
        </BlockStack>
      </Page>
    );
  }

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
            // Registered = healthy. Webhooks can sit "pending fire" for
            // days on a quiet store (no new orders) - that's not a
            // problem. Only flag red if Shopify never accepted our
            // registration request (the merchant needs to reinstall).
            ok={!!webhooksRegisteredAt}
            warning={false}
            detail={
              webhooksFirstFiredAt
                ? "active"
                : webhooksRegisteredAt
                ? "registered"
                : "reinstall to register"
            }
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
            // A winner is the gold state. Waiting for UTM samples is NOT a
            // problem - it's a "needs more data" state and a new install
            // can sit there for days before enough UTM-tagged orders come
            // in to triangulate. Only flag red if calibration has run with
            // a full sample but found no winner (a genuinely
            // mis-configured pixel that the merchant must investigate in
            // Meta Events Manager).
            ok={(() => {
              const r = pixelCalibration?.results;
              if (r?.winner) return true;
              // Not yet calibrated, or calibrated but still below the
              // sample threshold - both treated as "fine, still
              // gathering signal".
              const got = r?.sampleSize ?? 0;
              const need = r?.minimumRequired ?? 5;
              return !pixelCalibration?.calibratedAt || got < need;
            })()}
            warning={(() => {
              // Yellow = calibrated, hit the sample threshold, but no
              // clear winner. This is the actionable state.
              const r = pixelCalibration?.results;
              if (r?.winner) return false;
              if (!pixelCalibration?.calibratedAt) return false;
              const got = r?.sampleSize ?? 0;
              const need = r?.minimumRequired ?? 5;
              return got >= need;
            })()}
            detail={(() => {
              const r = pixelCalibration?.results;
              if (r?.winner) {
                return `${r.winner} (\u00B1${(r.winnerDeviation * 100).toFixed(1)}%)`;
              }
              const got = r?.sampleSize ?? 0;
              const need = r?.minimumRequired ?? 5;
              if (!pixelCalibration?.calibratedAt || got < need) {
                // Friendly waiting state - tell them we're still gathering
                // signal and what we need before we can decide.
                return `gathering signal (${got}/${need})`;
              }
              // Calibrated + enough samples + no winner = genuine problem.
              return "check pixel value field in Meta Events Manager";
            })()}
          />
        </div>

        {/* ═══ Match Rate + Match Confidence ═══ */}
        <Layout>
          <Layout.Section variant="oneHalf">
            <MatchTile
              title="Match Rate"
              description="Orders linked to a Meta-reported conversion (Layer 1 UTM-confirmed + Layer 2 statistical matcher) / Meta-reported conversions. Both layers reconcile against Meta's reported conversion count."
              accent="#5C6AC4"
              metric="rate"
              days={matchAccuracyDays}
              value30={matchRate30d}
              valueLifetime={matchRateLifetime}
              detail30={matchRate30dDetail}
              detailLifetime={matchRateLifetimeDetail}
              detailLabel="matched"
              detailTotalLabel="conversions"
            />
          </Layout.Section>
          <Layout.Section variant="oneHalf">
            <MatchTile
              title="Match Confidence"
              description="Average confidence across Layer 2 matched orders, weighted by match count. 100% = no rival candidates; lower means the matcher saw multiple compatible orders for the same Meta conversion."
              accent="#0D9488"
              metric="confidence"
              days={matchAccuracyDays}
              value30={matchConf30d}
              valueLifetime={matchConfLifetime}
              detail30={{ matched: matchConf30dDetail.matched, total: matchConf30dDetail.matched }}
              detailLifetime={{ matched: matchConfLifetimeDetail.matched, total: matchConfLifetimeDetail.matched }}
              detailLabel="matches"
              detailTotalLabel="orders"
            />
          </Layout.Section>
        </Layout>

        {/* ═══ UTM Health ═══ */}
        <Layout>
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

                    {/* Consistency section — shows how many tagged ads match the
                        dominant template vs differ. Drift usually indicates
                        either a template change that wasn't rolled out, or
                        manual edits to individual ads. */}
                    {(utmHealth.consistent + utmHealth.inconsistent) > 0 && (
                      <div style={{ display: "flex", gap: "16px", flexWrap: "wrap" }}>
                        <div style={{ flex: 1, minWidth: "120px", padding: "12px 16px", borderRadius: "8px", background: "#EFF6FF", textAlign: "center" }}>
                          <div style={{ fontSize: "22px", fontWeight: 700, color: "#1D4ED8" }}>{utmHealth.consistent}</div>
                          <div style={{ fontSize: "12px", color: "#6B7280" }}>consistent</div>
                        </div>
                        <div style={{ flex: 1, minWidth: "120px", padding: "12px 16px", borderRadius: "8px", background: utmHealth.inconsistent > 0 ? "#FFFBEB" : "#F9FAFB", textAlign: "center" }}>
                          <div style={{ fontSize: "22px", fontWeight: 700, color: utmHealth.inconsistent > 0 ? "#92400E" : "#374151" }}>{utmHealth.inconsistent}</div>
                          <div style={{ fontSize: "12px", color: "#6B7280" }}>inconsistent</div>
                        </div>
                      </div>
                    )}

                    {utmHealth.lastAudit && (
                      <Text as="p" variant="bodySm" tone="subdued">
                        Last audit: {new Date(utmHealth.lastAudit).toLocaleDateString()} ({utmHealth.total} ads scanned). Audits run nightly.
                      </Text>
                    )}
                  </BlockStack>
                ) : (
                  <Text as="p" variant="bodySm" tone="subdued">No UTM audit run yet. The next nightly audit will populate this tile.</Text>
                )}

                <Button onClick={() => navigate(`/app/utm${dateQuery()}`)}>
                  {utmHealth.missing > 0 || utmHealth.inconsistent > 0
                    ? "Audit & fix UTM issues"
                    : "Open UTM Manager"}
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

        {/* ═══ Onboarding cards removed - the !onboardingCompleted gate
            higher up renders <OnboardingFlow /> in place of the entire
            dashboard. The old "Getting Started" 5-step card is now dead
            code (replaced by the welcome → fit-test → ingest flow) and
            isn't rendered here anymore. Internal Tools below remain for
            staff debugging. ═══ */}

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
              {/* Backup / Verify / Download / Restore / Wipe - reset a test
                  merchant to walk through the new-install flow, then restore
                  the historical (incrementally-matched) data afterwards.
                  Four-layer safety: JSON dump, SQLite snapshot, verify pass,
                  off-Fly tarball download. */}
              <BlockStack gap="100">
                <Button onClick={() => startTask("backupShop")} disabled={isRunning}
                  loading={activeTask === "backupShop"}>
                  {activeTask === "backupShop" ? "Backing up..." : "Backup Shop"}
                </Button>
                <Text as="p" variant="bodySm" tone="subdued">
                  {backups && backups.length > 0
                    ? `Last: ${new Date(backups[0].startedAt).toLocaleString()} (${backups[0].totalRows ?? "?"} rows, ${backups[0].verified ? "✓ verified" : "✗ unverified"})`
                    : "No backups yet"}
                </Text>
              </BlockStack>
              <BlockStack gap="100">
                <Button onClick={() => {
                  if (!backups || backups.length === 0) return;
                  startTask("verifyBackup", { backupId: backups[0].backupId });
                }} disabled={isRunning || !backups || backups.length === 0}
                  loading={activeTask === "verifyBackup"}>
                  {activeTask === "verifyBackup" ? "Verifying..." : "Verify Latest"}
                </Button>
                <Text as="p" variant="bodySm" tone="subdued">Re-checks every JSON file's sha256 + row count.</Text>
              </BlockStack>
              <BlockStack gap="100">
                <Button onClick={() => {
                  if (!backups || backups.length === 0) return;
                  const url = backups[0].downloadUrl;
                  if (!url) return;
                  // Pre-signed URL from the loader: lives at /api/* so the
                  // browser can do a plain top-level navigation (App Bridge
                  // session tokens can't ride along on a file download).
                  // Open in a new tab so the embedded iframe state survives
                  // if the browser intercepts the download.
                  window.open(url, "_blank");
                }} disabled={!backups || backups.length === 0}>
                  Download Latest (.tar.gz)
                </Button>
                <Text as="p" variant="bodySm" tone="subdued">
                  {backups && backups.length > 0 && backups[0].lastDownloadedAt
                    ? `Last DL: ${new Date(backups[0].lastDownloadedAt).toLocaleString()}`
                    : "Get an off-Fly copy on your Mac before wiping."}
                </Text>
              </BlockStack>
              <BlockStack gap="100">
                <Button onClick={() => {
                  if (!backups || backups.length === 0) return;
                  if (!window.confirm(`Restore from backup ${backups[0].backupId}?\n\nThis re-inserts ${backups[0].totalRows ?? "?"} rows and rebuilds rollups.`)) return;
                  startTask("restoreShop", { backupId: backups[0].backupId });
                }} disabled={isRunning || !backups || backups.length === 0}
                  loading={activeTask === "restoreShop"}>
                  {activeTask === "restoreShop" ? "Restoring..." : "Restore Latest Backup"}
                </Button>
                <Text as="p" variant="bodySm" tone="subdued">
                  {backups && backups.length > 0
                    ? `Will restore: ${backups[0].backupId}`
                    : "No backups to restore"}
                </Text>
              </BlockStack>
              <BlockStack gap="100">
                <Button tone="critical" onClick={() => {
                  if (!backups || backups.length === 0 || !backups[0].verified) {
                    window.alert("Wipe is disabled until the newest backup is verified.");
                    return;
                  }
                  if (!window.confirm("Wipe ALL data for this shop AND uninstall from Shopify?\n\nThis:\n  • Deletes orders, attributions, Meta data, customers, rollups\n  • Revokes the Shopify access token (uninstalls the app)\n  • Forces a reinstall via the App Store link to come back\n\nRequires a verified backup younger than 24h.")) return;
                  if (!backups[0].lastDownloadedAt) {
                    if (!window.confirm("WARNING: this backup has not been downloaded to your Mac yet. The Fly volume could in theory be lost. Continue without an off-Fly copy?")) return;
                  }
                  if (!window.confirm("Final confirm: this will log you out and you'll need to reinstall via the App Store link. Continue?")) return;
                  startTask("wipeShop");
                }} disabled={isRunning || !backups || backups.length === 0 || !backups[0].verified}
                  loading={activeTask === "wipeShop"}>
                  {activeTask === "wipeShop" ? "Wiping..." : "Wipe Shop + Uninstall"}
                </Button>
                <Text as="p" variant="bodySm" tone="subdued">
                  {backups && backups.length > 0 && !backups[0].verified
                    ? "Disabled: latest backup is unverified."
                    : "Forces reinstall via App Store link."}
                </Text>
              </BlockStack>
              <BlockStack gap="100">
                <Button tone="critical" onClick={() => {
                  if (!window.confirm("Purge ALL data and restart from Fit Test?\n\nThis:\n  • Deletes orders, attributions, Meta data, customers, rollups, ingest jobs\n  • Resets onboarding state (Fit Test → fresh ingest)\n  • Preserves Shopify + Meta OAuth tokens (no reinstall)\n  • Kicks the orchestrator immediately\n\nNo backup required. Designed for dev iteration.")) return;
                  if (!window.confirm("Final confirm: all rollups + matches + Meta history for this shop will be deleted. Continue?")) return;
                  startTask("purgeData");
                }} disabled={isRunning}
                  loading={activeTask === "purgeData"}>
                  {activeTask === "purgeData" ? "Purging..." : "Purge Data + Restart"}
                </Button>
                <Text as="p" variant="bodySm" tone="subdued">
                  No backup needed. Keeps OAuth, restarts ingest.
                </Text>
              </BlockStack>
            </InlineStack>
            {backups && backups.length > 0 && (
              <BlockStack gap="100">
                <Text as="p" variant="bodySm" tone="subdued">
                  {backups.length} backup{backups.length === 1 ? "" : "s"} on disk -
                  newest: {new Date(backups[0].startedAt).toLocaleString()}
                  {backups[0].sqliteBytes ? ` · sqlite snapshot ${(backups[0].sqliteBytes / (1024 * 1024)).toFixed(1)} MB` : ""}
                  {backups[0].verified ? " · ✓ verified" : " · ✗ unverified"}
                  {backups[0].lastDownloadedAt ? ` · downloaded ${new Date(backups[0].lastDownloadedAt).toLocaleString()}` : " · not downloaded"}
                </Text>
              </BlockStack>
            )}

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
