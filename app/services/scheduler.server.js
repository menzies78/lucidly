import db from "../db.server";
import { runIncrementalSync, matchDayDeltas, clearTodayForRematch } from "./incrementalSync.server";
import { syncMetaAll } from "./metaSync.server";
import { syncMetaEntities } from "./metaEntitySync.server";
import { linkUtmToCampaigns } from "./utmLinkage.server";
import { getProgress } from "./progress.server";
import { syncOrders } from "./orderSync.server.js";
import { unauthenticated } from "../shopify.server";

const HOURLY_MS = 60 * 60 * 1000;
const DAILY_CHECK_MS = 15 * 60 * 1000; // check every 15 min if daily sync is due

let lastDailyRun = null;

async function getConnectedShops() {
  return db.shop.findMany({
    where: {
      metaAccessToken: { not: null },
      metaAdAccountId: { not: null },
    },
    select: { shopDomain: true },
  });
}

// Check if any manual Meta sync task is running for this shop
function isManualSyncRunning(shopDomain) {
  const manualTasks = ["syncMeta", "syncMetaHistorical", "runAttribution", "incrementalSync"];
  for (const t of manualTasks) {
    const p = getProgress(`${t}:${shopDomain}`);
    if (p && p.status === "running") {
      console.log(`[Scheduler] Skipping — manual task ${t} is running for ${shopDomain}`);
      return true;
    }
  }
  return false;
}

async function runHourlyCycle() {
  console.log(`[Scheduler] Hourly cycle starting at ${new Date().toISOString()}`);
  try {
    const shops = await getConnectedShops();
    if (shops.length === 0) {
      console.log("[Scheduler] No Meta-connected shops, skipping");
      return;
    }
    for (const shop of shops) {
      try {
        // Don't compete with manual syncs for Meta API rate limit
        if (isManualSyncRunning(shop.shopDomain)) continue;

        // 1. Pull any Shopify orders missed by webhooks (delta since lastOrderSync)
        try {
          const { admin } = await unauthenticated.admin(shop.shopDomain);
          const orderResult = await syncOrders(admin, shop.shopDomain);
          console.log(`[Scheduler] Shopify order sync for ${shop.shopDomain}: ${orderResult.totalImported} imported, ${orderResult.totalCustomers} customers`);
        } catch (err) {
          console.error(`[Scheduler] Shopify order sync failed for ${shop.shopDomain}:`, err.message);
        }

        // 2. Meta incremental sync + matcher
        const result = await runIncrementalSync(shop.shopDomain);
        console.log(`[Scheduler] Incremental sync for ${shop.shopDomain}: ${result.matched} matched, ${result.unmatched} unmatched, ${result.breakdownRows} breakdowns`);

        // 3. Meta change log delta (last ~36h) — small, quick, lets the
        // Changes page + campaign chart annotations stay current between
        // daily refreshes. Non-fatal on failure.
        try {
          const { syncMetaChanges } = await import("./metaChangeSync.server.js");
          const changeResult = await syncMetaChanges(shop.shopDomain);
          if (changeResult.added || changeResult.updated) {
            console.log(`[Scheduler] Change log delta for ${shop.shopDomain}: ${changeResult.added} new, ${changeResult.updated} updated (fetched ${changeResult.fetched})`);
          }
        } catch (err) {
          console.error(`[Scheduler] Change log delta failed for ${shop.shopDomain}:`, err.message);
        }
      } catch (err) {
        console.error(`[Scheduler] Incremental sync failed for ${shop.shopDomain}:`, err.message);
      }
    }
  } catch (err) {
    console.error("[Scheduler] Hourly cycle error:", err.message);
  }
}

async function runDailyCycle() {
  const now = new Date();
  const hour = now.getUTCHours();
  const today = now.toISOString().split("T")[0];

  // Run daily sync between 3:00-3:14 AM, once per day
  if (hour !== 3) return;
  if (lastDailyRun === today) return;
  lastDailyRun = today;

  console.log(`[Scheduler] Daily 7-day sync starting at ${now.toISOString()}`);
  try {
    const shops = await getConnectedShops();
    for (const shop of shops) {
      try {
        // Don't compete with manual syncs for Meta API rate limit
        if (isManualSyncRunning(shop.shopDomain)) {
          console.log(`[Scheduler] Deferring daily sync for ${shop.shopDomain} — manual task running`);
          lastDailyRun = null; // Reset so it retries next 15-min check
          continue;
        }

        await syncMetaAll(shop.shopDomain);
        console.log(`[Scheduler] Daily sync complete for ${shop.shopDomain}`);

        // Match only NEW conversion deltas for each of the last 7 days
        // Compares refreshed MetaInsight against snapshots — never touches existing attributions
        // PRIORITY: incremental matches are preserved — daily sweep only handles genuinely new deltas
        let totalNew = 0, totalMatched = 0, totalUnmatched = 0, totalPreserved = 0;
        for (let i = 7; i >= 1; i--) {
          const d = new Date();
          d.setUTCDate(d.getUTCDate() - i);
          const dayStr = d.toISOString().split("T")[0];
          const result = await matchDayDeltas(shop.shopDomain, dayStr);
          totalNew += result.newConversions;
          totalMatched += result.matched;
          totalUnmatched += result.unmatched;
          totalPreserved += result.skippedIncremental || 0;
        }
        console.log(`[Scheduler] 7-day delta match for ${shop.shopDomain}: ${totalNew} new, ${totalMatched} matched, ${totalUnmatched} unmatched, ${totalPreserved} incremental preserved`);

        // Sync campaign/adset/ad created_time metadata
        try {
          const entityResult = await syncMetaEntities(shop.shopDomain);
          console.log(`[Scheduler] Entity sync for ${shop.shopDomain}: ${entityResult.campaigns}c/${entityResult.adsets}as/${entityResult.ads}a`);
        } catch (err) {
          console.error(`[Scheduler] Entity sync failed for ${shop.shopDomain}:`, err.message);
        }

        // Refresh entity lifecycle (current status + scheduled start/end
        // from Graph, effective start/end from delivery) and top up any
        // change-log gaps from the last 7 days.
        try {
          const { refreshEntityLifecycle, recomputeEntityDeliveryWindows } = await import("./metaEntityLifecycle.server.js");
          const lifecycle = await refreshEntityLifecycle(shop.shopDomain);
          const windows = await recomputeEntityDeliveryWindows(shop.shopDomain);
          console.log(`[Scheduler] Entity lifecycle for ${shop.shopDomain}: ${lifecycle.updated} refreshed, delivery windows c=${windows.campaign} as=${windows.adset} a=${windows.ad}`);
        } catch (err) {
          console.error(`[Scheduler] Entity lifecycle failed for ${shop.shopDomain}:`, err.message);
        }
        try {
          const { syncMetaChanges } = await import("./metaChangeSync.server.js");
          const changeResult = await syncMetaChanges(shop.shopDomain, { backfillDays: 7 });
          console.log(`[Scheduler] Change log 7d reconcile for ${shop.shopDomain}: ${changeResult.added} new, ${changeResult.updated} updated`);
        } catch (err) {
          console.error(`[Scheduler] Change log reconcile failed for ${shop.shopDomain}:`, err.message);
        }

        // Link UTM data to Meta campaigns for any newly imported orders
        try {
          const linkResult = await linkUtmToCampaigns(shop.shopDomain);
          console.log(`[Scheduler] UTM linkage for ${shop.shopDomain}: ${linkResult.linked} linked, ${linkResult.noMatch} no match`);
        } catch (err) {
          console.error(`[Scheduler] UTM linkage failed for ${shop.shopDomain}:`, err.message);
        }
      } catch (err) {
        console.error(`[Scheduler] Daily sync failed for ${shop.shopDomain}:`, err.message);
      }
    }
  } catch (err) {
    console.error("[Scheduler] Daily cycle error:", err.message);
  }
}

export function startScheduler() {
  // Clear previous intervals on HMR restart — old callbacks reference stale modules
  if (global.__lucidlySchedulerHourly) clearInterval(global.__lucidlySchedulerHourly);
  if (global.__lucidlySchedulerDaily) clearInterval(global.__lucidlySchedulerDaily);
  if (global.__lucidlySchedulerBoot) clearTimeout(global.__lucidlySchedulerBoot);

  console.log("[Scheduler] Starting in-process scheduler");
  console.log("[Scheduler] Hourly: incremental sync (Meta insights + matching + breakdowns)");
  console.log("[Scheduler] Daily @ 3am: 7-day Meta lookback sync");

  // Warm caches 30 seconds after boot. Makes the user's first tab load fast
  // even on a fresh machine (SQLite page cache + in-process queryCache primed).
  if (global.__lucidlyWarmerBoot) clearTimeout(global.__lucidlyWarmerBoot);
  global.__lucidlyWarmerBoot = setTimeout(async () => {
    try {
      const { warmAllShops } = await import("./cacheWarmer.server.js");
      await warmAllShops();
    } catch (err) {
      console.error("[Scheduler] Cache warm failed (non-fatal):", err.message);
    }
  }, 30_000);

  // Run first hourly cycle after 5 minutes (let rate limits recover after deploys)
  global.__lucidlySchedulerBoot = setTimeout(() => {
    runHourlyCycle();
    // Then every hour
    global.__lucidlySchedulerHourly = setInterval(runHourlyCycle, HOURLY_MS);
  }, 5 * 60_000);

  // Check for daily sync every 15 minutes
  global.__lucidlySchedulerDaily = setInterval(runDailyCycle, DAILY_CHECK_MS);
}
