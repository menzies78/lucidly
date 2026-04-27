import { json } from "@remix-run/node";
import { useLoaderData, useSubmit, useNavigate, useNavigation, useRevalidator, useSearchParams } from "@remix-run/react";
import { Page, Layout, Card, Text, Button, BlockStack, InlineStack, Banner, ProgressBar, Spinner } from "@shopify/polaris";
import ReportTabs from "../components/ReportTabs";
import { useState, useEffect, useRef, useCallback } from "react";
import { authenticate } from "../shopify.server";
import db from "../db.server";
import { syncOrders } from "../services/orderSync.server";
import { syncMetaAll } from "../services/metaSync.server";
import { runAttribution, runDateRangeRematch, runFillGaps } from "../services/matcher.server";
import { runIncrementalSync, clearTodayForRematch } from "../services/incrementalSync.server";
import { setProgress, failProgress, getProgress, completeProgress } from "../services/progress.server";
import { parseDateRange } from "../utils/dateRange.server";
import { currencySymbolFromCode } from "../utils/currency";
import { cached as queryCached } from "../services/queryCache.server";

export const loader = async ({ request }) => {
  const { admin, session } = await authenticate.admin(request);
  const shopDomain = session.shop;

  const shop = await db.shop.findUnique({ where: { shopDomain } });
  const tz = shop?.shopifyTimezone || "UTC";

  const { fromDate, toDate, fromKey, toKey } = parseDateRange(request, tz);
  const dateFilter = { gte: fromDate, lte: toDate };

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
  ] = await Promise.all([
    db.order.count({ where: { shopDomain, createdAt: dateFilter } }),
    db.order.findMany({
      where: { shopDomain, createdAt: dateFilter },
      select: { shopifyCustomerId: true },
      distinct: ["shopifyCustomerId"],
    }),
    db.order.count({ where: { shopDomain, isNewCustomerOrder: true, createdAt: dateFilter } }),
    db.order.count({ where: { shopDomain, isNewCustomerOrder: false, createdAt: dateFilter } }),
    // Use DailyAdRollup (~1k rows) instead of MetaInsight (~28k+ hourly rows)
    db.dailyAdRollup.aggregate({ where: { shopDomain, date: dateFilter }, _sum: { spend: true } }),
    db.order.aggregate({
      where: { shopDomain, createdAt: dateFilter },
      _sum: { frozenTotalPrice: true, totalRefunded: true },
    }),
    db.order.findMany({
      where: { shopDomain, createdAt: dateFilter },
      select: { shopifyOrderId: true },
    }),
    // Date-scoped: matchedAt with 7-day buffer (catches late-matched orders)
    // + placeholders (confidence=0) filtered later by date in shopifyOrderId
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
    // Unmatched: parse date from shopifyOrderId (format: unmatched_adId_YYYY-MM-DD_hour)
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

  // Net Meta revenue: sum frozenTotalPrice for matched attributed orders
  const matchedOrderIds = matched.map(a => a.shopifyOrderId);
  const matchedOrders = matchedOrderIds.length > 0
    ? await db.order.findMany({
        where: { shopDomain, isOnlineStore: true, shopifyOrderId: { in: matchedOrderIds } },
        select: { frozenTotalPrice: true, totalRefunded: true },
      })
    : [];
  const matchedMetaRevenue = matchedOrders.reduce((s, o) =>
    s + (o.frozenTotalPrice || 0) - (o.totalRefunded || 0), 0);

  // UTM-only Meta orders: utmConfirmedMeta=true but not matched by Layer 2
  const matchedOrderIdSet = new Set(matchedOrderIds);
  const utmOnlyNotMatched = utmOnlyOrders.filter(o => !matchedOrderIdSet.has(o.shopifyOrderId));
  const utmOnlyCount = utmOnlyNotMatched.length;
  const utmOnlyRevenue = utmOnlyNotMatched.reduce((s, o) =>
    s + (o.frozenTotalPrice || 0) - (o.totalRefunded || 0), 0);
  const utmAndLucidlyCount = utmOnlyOrders.length - utmOnlyCount;

  // Combined Net Meta Revenue = matched attribution + UTM-only orders.
  // Both represent Meta-attributed revenue; UTM-only lacks Layer 2 ad-level
  // granularity but is still Meta traffic per the UTM / Elevar signal.
  // Matches the treatment used across Campaigns, Customers, Products, Weekly.
  const netMetaRevenue = matchedMetaRevenue + utmOnlyRevenue;
  const currencySymbol = currencySymbolFromCode(shop?.shopifyCurrency);

  const isNewInstall = !shop?.lastOrderSync && orderCount === 0;

  // Check if any background task is currently running for this shop
  const taskNames = ["syncOrders", "syncMeta", "syncMetaHistorical", "runAttribution", "dateRangeRematch", "fillGaps", "incrementalSync", "startOngoingSync", "calibratePixel", "inferGender"];
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
    webhooksRegisteredAt: shop?.webhooksRegisteredAt || null,
    webhooksFirstFiredAt: shop?.webhooksFirstFiredAt || null,
    pixelCalibration: {
      calibratedAt: shop?.metaValueCalibratedAt || null,
      samples: shop?.metaValueCalibrationSamples || 0,
      results: shop?.metaValueCalibrationResults ? (() => { try { return JSON.parse(shop.metaValueCalibrationResults); } catch { return null; } })() : null,
    },
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
      // syncMetaAll handles all 3 steps: insights + breakdowns + entity sync
      await syncMetaAll(shopDomain);
    });
    return json({ started: true, task: "syncMeta" });
  }
  if (actionType === "syncMetaHistorical") {
    runInBackground(async () => {
      // syncMetaAll handles all 3 steps: insights + breakdowns + entity sync
      await syncMetaAll(shopDomain, 730, taskId);
      const { linkUtmToCampaigns } = await import("../services/utmLinkage.server");
      await linkUtmToCampaigns(shopDomain);
    });
    return json({ started: true, task: "syncMetaHistorical" });
  }
  if (actionType === "runAttribution") {
    runInBackground(async () => {
      await runAttribution(shopDomain);
      // Also link UTMs to campaigns after matching
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
    // Mark onboarding complete, calibrate the pixel, then trigger first incremental sync
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
  if (actionType === "inferGender") {
    runInBackground(async () => {
      const { backfillShopInferredGender } = await import("../services/nameGender.server.js");
      const result = await backfillShopInferredGender(db, shopDomain);
      completeProgress(taskId, result);
    });
    return json({ started: true, task: "inferGender" });
  }
  return json({ success: false });
};

export default function Index() {
  const {
    shopDomain, orderCount, customerCount, newCustomerOrders,
    existingCustomerOrders, totalSpend, netRevenue, netMetaRevenue,
    lastSync, lastMetaSync, metaConnected, metaAdAccountId, attribution,
    currencySymbol, isNewInstall, activeTaskFromServer,
    utmOnlyCount, utmOnlyRevenue, utmAndLucidlyCount, onboardingCompleted,
    webhooksRegisteredAt, webhooksFirstFiredAt, pixelCalibration,
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

  // When Remix finishes the form submission (action returned), start polling
  const pendingTaskRef = useRef(null);

  const startTask = useCallback((actionName, extraData = {}) => {
    setActiveTask(actionName);
    setProgressState({ status: "running", message: "Starting..." });
    pendingTaskRef.current = actionName;
    submit({ action: actionName, ...extraData }, { method: "post" });
  }, [submit]);

  // Watch for navigation to complete (action returned) → start polling
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
            // Server returned no progress — the task either finished (its
            // "complete" row was cleared) or the process restarted. Either
            // way, stop polling so the UI doesn't hammer the endpoint forever.
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

  // Resume polling if a task was already running when we loaded the page
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
            // Server has no record of this task — most likely the process
            // restarted while we were resuming. Stop polling instead of
            // hammering the endpoint every 2s forever.
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

  // Cleanup on unmount
  useEffect(() => stopPolling, [stopPolling]);

  const isRunning = !!activeTask;

  const matchedPct = attribution.total > 0
    ? Math.round((attribution.matched / attribution.total) * 100) : 0;

  const progressPct = progress?.total
    ? Math.round((progress.current / progress.total) * 100) : null;

  return (
    <Page title="Dashboard" fullWidth>
      <ReportTabs>
      <BlockStack gap="500">
        <Banner tone="info"><p>Connected to <strong>{shopDomain}</strong></p></Banner>

        <Layout>
          <Layout.Section variant="oneHalf">
            <Card><BlockStack gap="200">
              <Text as="h2" variant="headingLg">Shopify</Text>
              <Banner tone="success">
                <p>Connected — {orderCount.toLocaleString()} orders</p>
              </Banner>
              {webhooksFirstFiredAt ? (
                <Banner tone="success"><p>Webhooks — active</p></Banner>
              ) : webhooksRegisteredAt ? (
                <Banner tone="warning"><p>Webhooks — pending (awaiting first order)</p></Banner>
              ) : (
                <Banner tone="critical"><p>Webhooks — not registered</p></Banner>
              )}
              {pixelCalibration?.results?.winner ? (
                <Banner tone="success">
                  <p>
                    Pixel — calibrated, reports <strong>{pixelCalibration.results.winner}</strong>
                    {" (±"}{(pixelCalibration.results.winnerDeviation * 100).toFixed(2)}{"%, "}
                    {pixelCalibration.results.sampleSize} samples, {pixelCalibration.results.quality})
                  </p>
                </Banner>
              ) : pixelCalibration?.calibratedAt ? (
                <Banner tone="warning"><p>Pixel — insufficient data ({pixelCalibration.samples} pairs)</p></Banner>
              ) : (
                <Banner tone="warning"><p>Pixel — not yet calibrated</p></Banner>
              )}
            </BlockStack></Card>
          </Layout.Section>
          <Layout.Section variant="oneHalf">
            <Card><BlockStack gap="200">
              <Text as="h2" variant="headingLg">Meta Ads</Text>
              {metaConnected ? (
                <>
                  <Banner tone="success"><p>Connected — {metaAdAccountId}</p></Banner>
                  <Banner tone="success">
                    <p>
                      Last sync — {lastMetaSync ? new Date(lastMetaSync).toLocaleString() : "never"}
                    </p>
                  </Banner>
                  <Banner tone="success">
                    <p>Attribution — {attribution.total.toLocaleString()} matches ({attribution.avgConfidence}% avg confidence)</p>
                  </Banner>
                </>
              ) : (
                <Banner tone="warning"><p>Not connected</p></Banner>
              )}
            </BlockStack></Card>
          </Layout.Section>
        </Layout>

        {/* Onboarding steps 3-5 */}
        {!onboardingCompleted && (
          <Card>
            <BlockStack gap="300">
              <Text as="h2" variant="headingLg">Getting Started</Text>
              <InlineStack gap="400" wrap>
                <BlockStack gap="100">
                  <Button variant={orderCount === 0 ? "primary" : undefined} onClick={() => startTask("syncOrders")} disabled={isRunning} loading={activeTask === "syncOrders"}>
                    1. Sync Shopify Orders
                  </Button>
                  <Text as="p" variant="bodySm" tone="subdued">
                    {lastSync ? `✓ ${orderCount.toLocaleString()} orders imported` : "Import 2 years of order history"}
                  </Text>
                </BlockStack>

                <BlockStack gap="100">
                  <Button
                    variant={!metaConnected && orderCount > 0 ? "primary" : undefined}
                    onClick={() => navigate("/app/meta-connect")}
                    disabled={isRunning}
                  >
                    {metaConnected ? "2. Connect Meta Ads ✓" : "2. Connect Meta Ads"}
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
                    {lastMetaSync ? "3. Sync Meta Ads Data ✓" : "3. Sync Meta Ads Data"}
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
                    {attribution.total > 0 ? "4. Run Customer Matcher ✓" : "4. Run Customer Matcher"}
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

        <Card>
          <BlockStack gap="300">
            <Text as="h2" variant="headingLg">Data Pipeline</Text>
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
                  <Text as="p" variant="bodySm" tone="subdued">Re-matches Oct 18 – Jan 30 only</Text>
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
      </BlockStack>
      </ReportTabs>
    </Page>
  );
}
