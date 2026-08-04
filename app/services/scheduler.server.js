import db from "../db.server";
import { runIncrementalSync, matchDayDeltas, clearTodayForRematch, rebuildAllRollups } from "./incrementalSync.server";
import { syncMetaAll } from "./metaSync.server";
import { invalidateShop } from "./queryCache.server";
import { syncMetaEntities } from "./metaEntitySync.server";
import { linkUtmToCampaigns } from "./utmLinkage.server";
import { getProgress } from "./progress.server";
import { syncOrders } from "./orderSync.server.js";
import { getOfflineAdmin } from "./offlineToken.server.js";
import { markSyncStart, markSyncEnd } from "./syncStatus.server.js";
import { isOnboardingIngestInFlight } from "./ingestOrchestrator.server.js";
import { startTokenWatchdog } from "./tokenWatchdog.server.js";

const HOURLY_MS = 60 * 60 * 1000;
const DAILY_CHECK_MS = 15 * 60 * 1000; // check every 15 min if daily sync is due

// A 'running' progress flag older than this is treated as dead. A hung/crashed
// manual task used to leave its flag set forever (running entries are never
// TTL-swept), which made isManualSyncRunning skip the shop indefinitely — the
// exact wedge that silently killed Meta sync for days after the Meta outage.
const RUNNING_STALE_MS = 15 * 60 * 1000;

// Meta-sync staleness gap that triggers a catch-up. A healthy shop syncs
// hourly, so anything past ~2h means at least one cycle was missed. Kept tight
// so even a short outage is caught within a cycle or two.
const CATCHUP_THRESHOLD_MS = 2 * 60 * 60 * 1000;
// The catch-up window is sized to the ACTUAL gap (gap in days + 1 day of
// slack for timezone/day-boundary edges), so a 1-day outage re-pulls ~2 days
// while a week-long outage re-pulls the whole week — same mechanism, work
// proportional to the outage. Floored at 2 days (matches the normal
// incremental's today+yesterday reach) and capped so a pathological gap (shop
// disconnected for months) can't request a 395-day pull on an hourly cycle.
const CATCHUP_MIN_DAYS = 2;
const CATCHUP_MAX_DAYS = 30;
// The nightly cycle always sweeps a fixed week to catch Meta's late-arriving
// conversion attributions, independent of any outage.
const DAILY_SWEEP_DAYS = 7;

// ── Per-shop sync stagger ────────────────────────────────────────────
// All shops share ONE SQLite writer, so firing every shop's sync at the
// same instant produces one long write burst that collides with page
// loads. Each shop instead gets a deterministic offset derived from its
// domain: hourly work starts hash%50 minutes into the hour, daily work
// runs in the 2-5am UTC hour picked by hash%4. Runs are still strictly
// serialized through a single promise chain, so two shops never write
// concurrently — the same guarantee the old sequential for-loop gave.
function shopHash(shopDomain) {
  let h = 0;
  for (let i = 0; i < shopDomain.length; i++) h = (h * 31 + shopDomain.charCodeAt(i)) >>> 0;
  return h;
}
const hourlyOffsetMs = (shopDomain) => (shopHash(shopDomain) % 50) * 60_000; // 0-49 min into the hour
const dailyHourFor = (shopDomain) => 2 + (shopHash(shopDomain) % 4); // 2, 3, 4 or 5am UTC

function enqueueSerialized(label, fn) {
  if (!global.__lucidlySyncChain) global.__lucidlySyncChain = Promise.resolve();
  global.__lucidlySyncChain = global.__lucidlySyncChain
    .then(fn)
    .catch((err) => console.error(`[Scheduler] ${label} failed:`, err.message));
  return global.__lucidlySyncChain;
}

// Daily-run bookkeeping is per shop now that each shop has its own slot.
const lastDailyRunByShop = new Map();

async function getConnectedShops() {
  // Only fully-onboarded shops. A shop mid-onboarding (welcome / fit-importing /
  // fit-running / fit-ready / ingesting) is being driven by either the Fit
  // Test action handler or the ingest orchestrator - both upsert orders and
  // Meta insights in tight loops. If the scheduler races them it (a) burns
  // Meta budget that the orchestrator needs for the historical backfill,
  // (b) writes phantom orders/insights with no IngestJob row so the
  // onboarding progress bars never advance, and (c) deadlocks the SQLite
  // pool. Skip until onboardingCompleted flips true.
  return db.shop.findMany({
    where: {
      metaAccessToken: { not: null },
      metaAdAccountId: { not: null },
      onboardingCompleted: true,
      // Demo shops carry a placeholder Meta token but no real ad account, so
      // never run live Meta syncs against them (they'd fail and churn the pool).
      demoMode: false,
    },
    select: { shopDomain: true, plan: true },
  });
}

// Check if any manual Meta sync task is running for this shop
function isManualSyncRunning(shopDomain) {
  const manualTasks = ["syncMeta", "syncMetaHistorical", "runAttribution", "incrementalSync"];
  for (const t of manualTasks) {
    const p = getProgress(`${t}:${shopDomain}`);
    if (p && p.status === "running") {
      // Self-heal: ignore a 'running' flag that hasn't advanced in
      // RUNNING_STALE_MS — its task has almost certainly died or hung, and
      // honouring it would wedge the scheduler forever.
      if (p.updatedAt && Date.now() - p.updatedAt > RUNNING_STALE_MS) {
        console.warn(`[Scheduler] Ignoring stale '${t}' running flag for ${shopDomain} (${Math.round((Date.now() - p.updatedAt) / 60000)}m stale) — treating as dead`);
        continue;
      }
      console.log(`[Scheduler] Skipping - manual task ${t} is running for ${shopDomain}`);
      return true;
    }
  }
  return false;
}

// The single proven Meta-recovery code path: re-pull `daysBack` days of
// insights + breakdowns, match each day's NEW conversion deltas, then rebuild
// every rollup the pages read. Used by BOTH the nightly daily cycle (fixed
// week) and the staleness catch-up (window sized to the outage) so there is
// exactly one tested path. No new matcher logic — matchDayDeltas is
// delta-based and idempotent (preserves existing matches, only fills
// genuinely-new conversions). Uses a 'recovery:' progress key so it never
// trips isManualSyncRunning.
async function runCatchUp(shopDomain, daysBack) {
  await syncMetaAll(shopDomain, daysBack, `recovery:${shopDomain}`);

  let totalNew = 0, totalMatched = 0, totalUnmatched = 0, totalPreserved = 0;
  for (let i = daysBack; i >= 1; i--) {
    const d = new Date();
    d.setUTCDate(d.getUTCDate() - i);
    const dayStr = d.toISOString().split("T")[0];
    const r = await matchDayDeltas(shopDomain, dayStr);
    totalNew += r.newConversions;
    totalMatched += r.matched;
    totalUnmatched += r.unmatched;
    totalPreserved += r.skippedIncremental || 0;
  }
  console.log(`[Recovery] ${shopDomain}: ${daysBack}-day delta match — ${totalNew} new, ${totalMatched} matched, ${totalUnmatched} unmatched, ${totalPreserved} preserved`);

  // Rebuild campaign / ad-demographic / geo / dashboard / customer / product
  // rollups (force bypasses the 24h throttle since we just wrote new data),
  // then drop stale query-cache entries so pages reflect the backfill at once.
  await rebuildAllRollups(shopDomain, { force: true });
  invalidateShop(shopDomain);
}

// Detect a Meta-sync staleness gap and, only once Meta is reachable again,
// repopulate every missed day (window sized to the actual gap). Gating is
// implicit: syncMetaAll only advances lastMetaSync on success, so if Meta is
// still down the bounded fetch throws fast, lastMetaSync stays stale, and we
// retry next cycle — repopulation completes ONLY after the issue is resolved.
// Returns true if a gap was detected (so the caller skips the normal
// incremental cycle, which the catch-up supersets).
async function runCatchUpIfStale(shopDomain) {
  const shop = await db.shop.findUnique({
    where: { shopDomain },
    select: { lastMetaSync: true },
  });
  const last = shop?.lastMetaSync ? new Date(shop.lastMetaSync).getTime() : 0;
  const gapMs = last === 0 ? Infinity : Date.now() - last;
  if (gapMs < CATCHUP_THRESHOLD_MS) return false; // healthy — run the normal cycle

  // Size the window to the gap: ceil(gap in days) + 1 slack day, clamped.
  const gapDays = gapMs === Infinity ? CATCHUP_MAX_DAYS : Math.ceil(gapMs / 86400000) + 1;
  const daysBack = Math.min(CATCHUP_MAX_DAYS, Math.max(CATCHUP_MIN_DAYS, gapDays));
  const gapHrs = last === 0 ? "never" : `${(gapMs / 3600000).toFixed(1)}h`;
  if (gapDays > CATCHUP_MAX_DAYS) {
    console.warn(`[Recovery] ${shopDomain}: gap ${gapHrs} exceeds ${CATCHUP_MAX_DAYS}-day cap — backfilling last ${CATCHUP_MAX_DAYS} days (older data needs a manual/onboarding backfill)`);
  }
  console.warn(`[Recovery] ${shopDomain}: Meta sync stale (last success: ${gapHrs} ago) — running ${daysBack}-day catch-up`);
  try {
    await runCatchUp(shopDomain, daysBack);
    console.log(`[Recovery] ${shopDomain}: catch-up complete — missing data repopulated (${daysBack} days)`);
  } catch (err) {
    // Meta still unreachable (the bounded fetch threw quickly). lastMetaSync
    // remains stale, so we attempt recovery again on the next cycle.
    console.error(`[Recovery] ${shopDomain}: catch-up failed — Meta likely still down, will retry next cycle: ${err.message}`);
  }
  return true;
}

async function runHourlyForShop(shopDomain) {
  markSyncStart("hourly");
  try {
    // Don't compete with manual syncs for Meta API rate limit
    if (isManualSyncRunning(shopDomain)) return;

    // Don't compete with the onboarding ingest for SQLite connections.
    // Both paths upsert orders + line items in tight loops; running them
    // concurrently triggers Prisma socket timeouts (seen 2026-05-10
    // when the hourly cycle fired mid-onboarding for vollebak).
    if (isOnboardingIngestInFlight(shopDomain)) {
      console.log(`[Scheduler] Skipping ${shopDomain} - onboarding ingest in progress`);
      return;
    }

    // 1. Pull any Shopify orders missed by webhooks (delta since lastOrderSync)
    try {
      const { admin } = await getOfflineAdmin(shopDomain);
      const orderResult = await syncOrders(admin, shopDomain);
      console.log(`[Scheduler] Shopify order sync for ${shopDomain}: ${orderResult.totalImported} imported, ${orderResult.totalCustomers} customers`);
    } catch (err) {
      console.error(`[Scheduler] Shopify order sync failed for ${shopDomain}:`, err.message);
    }

    // 2. Meta sync + matcher. If a staleness gap is detected (a previous
    // cycle was missed — e.g. a Meta outage), run the 7-day catch-up
    // instead of the normal today+yesterday incremental. The catch-up is a
    // superset, so we skip the incremental when it fires.
    const didCatchUp = await runCatchUpIfStale(shopDomain);
    if (!didCatchUp) {
      const result = await runIncrementalSync(shopDomain);
      console.log(`[Scheduler] Incremental sync for ${shopDomain}: ${result.matched} matched, ${result.unmatched} unmatched, ${result.breakdownRows} breakdowns`);
    }

    // 3. Meta change log delta (last ~36h) - small, quick, lets the
    // Changes page + campaign chart annotations stay current between
    // daily refreshes. Non-fatal on failure.
    try {
      const { syncMetaChanges } = await import("./metaChangeSync.server.js");
      const changeResult = await syncMetaChanges(shopDomain);
      if (changeResult.added || changeResult.updated) {
        console.log(`[Scheduler] Change log delta for ${shopDomain}: ${changeResult.added} new, ${changeResult.updated} updated (fetched ${changeResult.fetched})`);
      }
    } catch (err) {
      console.error(`[Scheduler] Change log delta failed for ${shopDomain}:`, err.message);
    }

    // 4. Fill any MetaEntity ad rows that still have no thumbnail. The
    // daily refreshAdCreatives walks every known ad, but entity
    // rows added between daily runs (new campaigns, new ads in existing
    // campaigns) end up with thumbnailFetchedAt=null and would
    // otherwise wait up to 24h to be resolved - which is why fresh
    // live ads were surfacing as letter placeholders on the
    // Campaigns tab. Bounded per run via the function's `limit` arg.
    try {
      const { fillBlankThumbnails } = await import("./metaAdCreativeSync.server.js");
      const blankResult = await fillBlankThumbnails(shopDomain);
      if (blankResult.attempted > 0) {
        console.log(`[Scheduler] Fill blank thumbnails for ${shopDomain}: ${blankResult.updated} updated, ${blankResult.missing} unresolved (attempted ${blankResult.attempted})`);
      }
    } catch (err) {
      console.error(`[Scheduler] Fill blank thumbnails failed for ${shopDomain}:`, err.message);
    }
  } catch (err) {
    console.error(`[Scheduler] Incremental sync failed for ${shopDomain}:`, err.message);
  } finally {
    markSyncEnd();
  }
}

async function runHourlyCycle() {
  console.log(`[Scheduler] Hourly cycle dispatch at ${new Date().toISOString()}`);
  try {
    const shops = await getConnectedShops();
    if (shops.length === 0) {
      console.log("[Scheduler] No Meta-connected shops, skipping");
      return;
    }
    global.__lucidlyShopTimers ??= [];
    for (const shop of shops) {
      // Free (audit) tier syncs once a day via the daily cycle — the hourly
      // loop is the paid plan's freshness. Keeps free-tier compute ~24x
      // cheaper and makes "hourly matching" a truthful upgrade lever.
      if (shop.plan === "free") {
        console.log(`[Scheduler] ${shop.shopDomain}: free plan — daily cycle only`);
        continue;
      }
      const delayMs = hourlyOffsetMs(shop.shopDomain);
      console.log(`[Scheduler] ${shop.shopDomain}: hourly sync scheduled +${Math.round(delayMs / 60000)}min`);
      global.__lucidlyShopTimers.push(setTimeout(() => {
        enqueueSerialized(`hourly:${shop.shopDomain}`, () => runHourlyForShop(shop.shopDomain));
      }, delayMs));
    }
  } catch (err) {
    console.error("[Scheduler] Hourly cycle error:", err.message);
  }
}

async function runDailyForShop(shopDomain) {
  console.log(`[Scheduler] Daily 7-day sync starting for ${shopDomain} at ${new Date().toISOString()}`);
  markSyncStart("daily");
  try {
    // Don't compete with manual syncs for Meta API rate limit
    if (isManualSyncRunning(shopDomain)) {
      console.log(`[Scheduler] Deferring daily sync for ${shopDomain} - manual task running`);
      lastDailyRunByShop.delete(shopDomain); // retry next 15-min check
      return;
    }

    // Same DB-contention reason as the hourly cycle - skip if the
    // onboarding ingest owns the SQLite pool right now.
    if (isOnboardingIngestInFlight(shopDomain)) {
      console.log(`[Scheduler] Deferring daily sync for ${shopDomain} - onboarding ingest in progress`);
      lastDailyRunByShop.delete(shopDomain);
      return;
    }

    // Re-pull 7 days, match each day's new deltas, and rebuild rollups.
    // Shared with the staleness catch-up so there is one tested path.
    // (Previously the daily cycle matched but did NOT rebuild rollups,
    // relying on the next hourly incremental — which left the Ad Campaigns
    // tiles stale after a backfill until that cycle ran.)
    await runCatchUp(shopDomain, DAILY_SWEEP_DAYS);
    console.log(`[Scheduler] Daily sync complete for ${shopDomain}`);

    // Sync campaign/adset/ad created_time metadata
    try {
      const entityResult = await syncMetaEntities(shopDomain);
      console.log(`[Scheduler] Entity sync for ${shopDomain}: ${entityResult.campaigns}c/${entityResult.adsets}as/${entityResult.ads}a`);
    } catch (err) {
      console.error(`[Scheduler] Entity sync failed for ${shopDomain}:`, err.message);
    }

    // Refresh entity lifecycle (current status + scheduled start/end
    // from Graph, effective start/end from delivery) and top up any
    // change-log gaps from the last 7 days.
    try {
      const { refreshEntityLifecycle, recomputeEntityDeliveryWindows } = await import("./metaEntityLifecycle.server.js");
      const lifecycle = await refreshEntityLifecycle(shopDomain);
      const windows = await recomputeEntityDeliveryWindows(shopDomain);
      console.log(`[Scheduler] Entity lifecycle for ${shopDomain}: ${lifecycle.updated} refreshed, delivery windows c=${windows.campaign} as=${windows.adset} a=${windows.ad}`);
    } catch (err) {
      console.error(`[Scheduler] Entity lifecycle failed for ${shopDomain}:`, err.message);
    }

    // Attribution-window history self-heal for RECENTLY-onboarded shops
    // only (the post-onboarding deferred task can be lost to a restart).
    // Bounded to onboardingStartedAt within 45 days so long-standing
    // shops (HM etc.) can never be surprise-backfilled; ensureWindowHistory
    // itself no-ops once history exists or the account has none.
    try {
      const db = (await import("../db.server")).default;
      const rec = await db.shop.findUnique({
        where: { shopDomain },
        select: { onboardingCompleted: true, onboardingStartedAt: true },
      });
      const recent = rec?.onboardingCompleted && rec?.onboardingStartedAt
        && (Date.now() - new Date(rec.onboardingStartedAt).getTime()) < 45 * 86400000;
      if (recent) {
        const { ensureWindowHistory } = await import("./metaAttributionWindowSync.server.js");
        const res = await ensureWindowHistory(shopDomain);
        if (res.status === "backfilled") {
          console.log(`[Scheduler] Window history self-heal for ${shopDomain}: ${res.rows} rows`);
        }
      }
    } catch (err) {
      console.error(`[Scheduler] Window history self-heal failed for ${shopDomain}:`, err.message);
    }
    try {
      const { syncMetaChanges } = await import("./metaChangeSync.server.js");
      const changeResult = await syncMetaChanges(shopDomain, { backfillDays: 7 });
      console.log(`[Scheduler] Change log 7d reconcile for ${shopDomain}: ${changeResult.added} new, ${changeResult.updated} updated`);
    } catch (err) {
      console.error(`[Scheduler] Change log reconcile failed for ${shopDomain}:`, err.message);
    }

    // Link UTM data to Meta campaigns for any newly imported orders
    try {
      const linkResult = await linkUtmToCampaigns(shopDomain);
      console.log(`[Scheduler] UTM linkage for ${shopDomain}: ${linkResult.linked} linked, ${linkResult.noMatch} no match`);
    } catch (err) {
      console.error(`[Scheduler] UTM linkage failed for ${shopDomain}:`, err.message);
    }

    // Refresh product images so new products (e.g. recent launches)
    // appear with thumbs on the Products page without waiting for the
    // 24 h DB cache to expire naturally.
    try {
      const { refreshProductImages } = await import("./productImageSync.server.js");
      const imgResult = await refreshProductImages(shopDomain);
      console.log(`[Scheduler] Product images for ${shopDomain}: ${imgResult.count} cached`);
    } catch (err) {
      console.error(`[Scheduler] Product image refresh failed for ${shopDomain}:`, err.message);
    }

    // Refresh Meta ad creative thumbnails. Meta CDN URLs are signed and
    // rotate, so this re-pulls every night to keep the Ad Explorer tiles
    // working.
    try {
      const { refreshAdCreatives } = await import("./metaAdCreativeSync.server.js");
      const creativeResult = await refreshAdCreatives(shopDomain);
      console.log(`[Scheduler] Ad creatives for ${shopDomain}: ${creativeResult.updated} updated, ${creativeResult.cached || 0} bytes cached, ${creativeResult.missing} unresolved`);
    } catch (err) {
      console.error(`[Scheduler] Ad creative refresh failed for ${shopDomain}:`, err.message);
    }
  } catch (err) {
    console.error(`[Scheduler] Daily sync failed for ${shopDomain}:`, err.message);
  } finally {
    markSyncEnd();
  }
}

async function runDailyCycle() {
  const now = new Date();
  const hour = now.getUTCHours();
  const today = now.toISOString().split("T")[0];
  try {
    const shops = await getConnectedShops();
    for (const shop of shops) {
      // Each shop runs once per day in its own 2-5am UTC slot.
      if (hour !== dailyHourFor(shop.shopDomain)) continue;
      if (lastDailyRunByShop.get(shop.shopDomain) === today) continue;
      lastDailyRunByShop.set(shop.shopDomain, today);
      console.log(`[Scheduler] ${shop.shopDomain}: daily 7-day sync dispatched (slot ${dailyHourFor(shop.shopDomain)}:00 UTC)`);
      enqueueSerialized(`daily:${shop.shopDomain}`, () => runDailyForShop(shop.shopDomain));
    }
  } catch (err) {
    console.error("[Scheduler] Daily cycle error:", err.message);
  }
}

/**
 * One-shot per shop: if the earliest MetaBreakdown row is younger than
 * 30 days, run the 400-day historical breakdown sync followed by
 * enrichAll (re-populates metaAge/metaGender/metaPlatform/metaPlacement
 * on attributions that previously failed to enrich because the breakdown
 * data didn't exist yet) and rebuild the affected rollups.
 *
 * Skips shops mid-onboarding (the new FINAL_PHASE_2 path handles them)
 * and shops with no breakdown rows at all (no Meta connection).
 *
 * Self-determining: after the backfill min(date) jumps to ~13 months
 * ago so the same check returns false on the next boot.
 */
async function runHistoricalDemographicsBackfillIfNeeded() {
  const shops = await db.shop.findMany({
    where: {
      metaAccessToken: { not: null },
      metaAdAccountId: { not: null },
      onboardingCompleted: true,
      // Demo shops carry a placeholder Meta token but no real ad account, so
      // never run live Meta syncs against them (they'd fail and churn the pool).
      demoMode: false,
    },
    select: { shopDomain: true },
  });
  if (shops.length === 0) return;

  const cutoff = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000);
  for (const { shopDomain } of shops) {
    if (isManualSyncRunning(shopDomain)) {
      console.log(`[HistoricalBackfill] ${shopDomain}: skipping (manual sync running)`);
      continue;
    }
    // Check coverage PER breakdown type, not globally. Vollebak had a
    // stray country-breakdown row from 2025-04 (initial onboarding sync)
    // which set the global min(date) > 30 days ago and made the check
    // conclude "already backfilled" — even though age / gender /
    // age_gender / platform_position / publisher_platform only covered
    // the last 14 days. Result: Product Demographics / Lowest CPA /
    // platform breakdowns silently empty across longer windows.
    //
    // Backfill if ANY required type lacks 30-day coverage.
    const perType = await db.metaBreakdown.groupBy({
      by: ["breakdownType"],
      where: { shopDomain },
      _min: { date: true },
    });
    if (perType.length === 0) {
      console.log(`[HistoricalBackfill] ${shopDomain}: no breakdown rows yet, skipping`);
      continue;
    }
    const REQUIRED_TYPES = [
      "country",
      "age",
      "gender",
      "age_gender",
      "platform_position",
      "publisher_platform",
    ];
    const earliestByType = Object.fromEntries(
      perType.map((p) => [p.breakdownType, p._min.date])
    );
    const stale = REQUIRED_TYPES.filter((t) => {
      const d = earliestByType[t];
      return !d || d >= cutoff;
    });
    if (stale.length === 0) {
      // Every required type has > 30 days of coverage — done.
      continue;
    }
    console.log(`[HistoricalBackfill] ${shopDomain}: per-type coverage gap: ${stale.join(", ")}`);

    console.log(`[HistoricalBackfill] ${shopDomain}: running 400-day backfill`);
    try {
      const { syncMetaBreakdowns } = await import("./metaBreakdownSync.server.js");
      const r = await syncMetaBreakdowns(shopDomain, `historical-backfill:${shopDomain}`, 400);
      console.log(`[HistoricalBackfill] ${shopDomain}: breakdown sync wrote ${r?.totalRows || 0} rows`);

      const { enrichAll } = await import("./attributionEnrichment.server.js");
      const enriched = await enrichAll(shopDomain);
      console.log(`[HistoricalBackfill] ${shopDomain}: enrichAll done - ${enriched.enriched} attributions enriched (${enriched.exact} exact / ${enriched.probabilistic} probabilistic)`);

      // Rebuild the rollups that depend on the now-populated demographic
      // fields. Product / customer rollups consume Attribution columns
      // directly; campaign rollups read MetaBreakdown for platform /
      // placement aggregates. Geo rollups read MetaBreakdown for the
      // country-breakdown spend.
      try {
        const { rebuildProductRollups } = await import("./productRollups.server.js");
        await rebuildProductRollups(shopDomain);
      } catch (err) {
        console.error(`[HistoricalBackfill] ${shopDomain}: product rollup rebuild failed: ${err.message}`);
      }
      try {
        const { rebuildCustomerSegments, rebuildCustomerRollups } = await import("./customerRollups.server.js");
        await rebuildCustomerSegments(shopDomain);
        await rebuildCustomerRollups(shopDomain);
      } catch (err) {
        console.error(`[HistoricalBackfill] ${shopDomain}: customer rollup rebuild failed: ${err.message}`);
      }
      try {
        const { rebuildAdDemographicRollups } = await import("./adDemographicRollups.server.js");
        await rebuildAdDemographicRollups(shopDomain);
      } catch (err) {
        console.error(`[HistoricalBackfill] ${shopDomain}: ad demographic rollup rebuild failed: ${err.message}`);
      }
      try {
        const { rebuildGeoRollups } = await import("./geoRollups.server.js");
        await rebuildGeoRollups(shopDomain);
      } catch (err) {
        console.error(`[HistoricalBackfill] ${shopDomain}: geo rollup rebuild failed: ${err.message}`);
      }
      try {
        const { invalidateShop } = await import("./queryCache.server.js");
        invalidateShop(shopDomain);
      } catch (err) {
        console.error(`[HistoricalBackfill] ${shopDomain}: cache invalidation failed: ${err.message}`);
      }
      console.log(`[HistoricalBackfill] ${shopDomain}: complete`);
    } catch (err) {
      console.error(`[HistoricalBackfill] ${shopDomain}: failed (will retry on next boot): ${err.message}`);
    }
  }
}

export function startScheduler() {
  // Clear previous intervals on HMR restart - old callbacks reference stale modules
  if (global.__lucidlySchedulerHourly) clearInterval(global.__lucidlySchedulerHourly);
  if (global.__lucidlySchedulerDaily) clearInterval(global.__lucidlySchedulerDaily);
  if (global.__lucidlySchedulerBackfill) clearInterval(global.__lucidlySchedulerBackfill);
  if (global.__lucidlySchedulerPrune) clearInterval(global.__lucidlySchedulerPrune);
  if (global.__lucidlySchedulerBoot) clearTimeout(global.__lucidlySchedulerBoot);

  console.log("[Scheduler] Starting in-process scheduler");
  console.log("[Scheduler] Hourly: incremental sync, per-shop stagger (hash%50 min into the hour)");
  console.log("[Scheduler] Daily: 7-day Meta lookback, per-shop slot 2-5am UTC");

  // Clear any pending per-shop stagger timers from a previous HMR cycle -
  // their callbacks reference stale modules.
  (global.__lucidlyShopTimers || []).forEach(clearTimeout);
  global.__lucidlyShopTimers = [];

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

  // One-shot attribution-window backfill trigger (env-gated, self-disabling).
  // Set ATTR_WINDOW_BACKFILL_SHOP=<shopDomain> via fly secrets to run the
  // 12-month windows backfill + retro-labeling without clicking the dashboard
  // button. Idempotent: skips when the shop already has window rows older
  // than 40 days (a backfill has already run), so a lingering secret across
  // restarts is harmless. Unset the secret once confirmed complete.
  if (global.__lucidlyAttrWindowBackfill) clearTimeout(global.__lucidlyAttrWindowBackfill);
  const attrBackfillShop = process.env.ATTR_WINDOW_BACKFILL_SHOP;
  if (attrBackfillShop) {
    global.__lucidlyAttrWindowBackfill = setTimeout(async () => {
      try {
        const { ensureWindowHistory } = await import("./metaAttributionWindowSync.server.js");
        const res = await ensureWindowHistory(attrBackfillShop);
        console.log(`[Scheduler] Env-triggered window history for ${attrBackfillShop}: ${res.status}${res.rows ? ` (${res.rows} rows)` : ""}`);
      } catch (err) {
        console.error(`[Scheduler] Attr-window backfill failed (non-fatal): ${err.message}`);
      }
    }, 90_000);
  }

  // Reap orphaned ingest jobs and resume any shop that was mid-onboarding
  // when the previous process died. Runs once on boot, slightly delayed so
  // the DB connection pool is fully warm.
  if (global.__lucidlyIngestResume) clearTimeout(global.__lucidlyIngestResume);
  global.__lucidlyIngestResume = setTimeout(async () => {
    try {
      const { reapOrphanedJobs, reapOrphanedFitPhases, resumePendingIngests } = await import("./ingestOrchestrator.server.js");
      await reapOrphanedJobs();
      await reapOrphanedFitPhases();
      const resumed = await resumePendingIngests();
      if (resumed > 0) console.log(`[Scheduler] Resumed ingest for ${resumed} shop(s)`);
    } catch (err) {
      console.error("[Scheduler] Ingest resume failed (non-fatal):", err.message);
    }
  }, 15_000);

  // Self-healing historical demographics backfill. Detects shops that
  // onboarded under the old 7-day MetaBreakdown window (so their Product
  // Demographics / Customer Demographics / Platform / Placement tiles
  // are empty for any order > ~7 days old at install time) and runs the
  // 400-day historical backfill + enrichAll catch-up exactly once.
  //
  // No schema flag needed: we infer "needs backfill" purely from
  // min(MetaBreakdown.date) — if the earliest breakdown row is younger
  // than 30 days the shop is stuck on the recent window. After the
  // backfill min(date) becomes ~13 months ago and the check naturally
  // returns false on subsequent boots.
  if (global.__lucidlyHistoricalBackfill) clearTimeout(global.__lucidlyHistoricalBackfill);
  global.__lucidlyHistoricalBackfill = setTimeout(async () => {
    try {
      await runHistoricalDemographicsBackfillIfNeeded();
    } catch (err) {
      console.error("[Scheduler] Historical demographics backfill check failed (non-fatal):", err.message);
    }
  }, 60_000);

  // Run first hourly cycle after 5 minutes (let rate limits recover after deploys)
  global.__lucidlySchedulerBoot = setTimeout(() => {
    runHourlyCycle();
    // Then every hour
    global.__lucidlySchedulerHourly = setInterval(runHourlyCycle, HOURLY_MS);
  }, 5 * 60_000);

  // Check for daily sync every 15 minutes
  global.__lucidlySchedulerDaily = setInterval(runDailyCycle, DAILY_CHECK_MS);

  // Upgrade history backfills: pick up pending/crashed runs every 15 min.
  // Serialized on the sync chain so a backfill never fights the hourly
  // sync or a rollup rebuild for the SQLite write lock; phase cursor makes
  // retries resume, not restart.
  global.__lucidlySchedulerBackfill = setInterval(async () => {
    try {
      const { shopsWithPendingBackfill, runHistoryBackfillIfPending } = await import("./historyBackfill.server.js");
      const pending = await shopsWithPendingBackfill();
      for (const { shopDomain } of pending) {
        enqueueSerialized(`backfill:${shopDomain}`, () => runHistoryBackfillIfPending(shopDomain));
      }
    } catch (err) {
      console.error("[Scheduler] backfill check failed:", err.message);
    }
  }, DAILY_CHECK_MS);

  // Free-tier prune: Sundays 04:xx UTC (after the daily sync slots).
  // Cost control only — the loaders' clamp is the paywall. Customer table
  // and demo shops are never touched; 30-day downgrade grace applies.
  global.__lucidlySchedulerPrune = setInterval(async () => {
    const now = new Date();
    if (now.getUTCDay() !== 0 || now.getUTCHours() !== 4) return;
    const today = now.toISOString().slice(0, 10);
    if (global.__lucidlyLastPruneDay === today) return;
    global.__lucidlyLastPruneDay = today;
    try {
      const { pruneFreeShops } = await import("./freeTierPrune.server.js");
      enqueueSerialized("freeTierPrune", () => pruneFreeShops());
    } catch (err) {
      console.error("[Scheduler] prune check failed:", err.message);
    }
  }, DAILY_CHECK_MS);

  // Token Health Watchdog: proactively probe every installed shop's offline
  // token so the "halt" class (non-expiring 403, revoked 401, refresh race) is
  // flagged by email before a merchant notices sync has stopped.
  startTokenWatchdog();
}
