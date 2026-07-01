// Phased ingest orchestrator. Run after Meta OAuth completes.
//
// Premise: a fresh install needs to pull a *lot* of data on day one - up to
// 2 years of Shopify orders + 13 months of Meta hourly insights + breakdowns
// + entity metadata + creative thumbnails - before the dashboards are useful.
// Naively chaining the existing sync entry points works, but:
//   1. It blocks the OAuth callback handler if called inline.
//   2. There's no resume on restart - a Fly redeploy mid-ingest loses work.
//   3. The merchant has no visibility into "where am I".
//
// This orchestrator wraps the existing per-service sync functions in
// IngestJob rows so we can:
//   - Fire-and-forget from the OAuth callback (HTTP returns immediately).
//   - Resume from the first incomplete phase on process restart.
//   - Surface phase-by-phase progress to the dashboard via a single
//     `/app/api/ingest-status` lookup.
//   - Run independent tracks in parallel (Shopify orders alongside Meta
//     insights) - they don't share state, so serialising them is wasted time.
//
// Track layout:
//   Track A (Shopify):  shopify-orders-Ny
//   Track B (Meta):     insights-13mo → breakdowns → entities → creatives
//   Final:              match (depends on BOTH tracks)
//
// Each phase declares a `progressKey` mapping to the in-memory progress map
// written by the underlying sync service. The status endpoint surfaces
// {current,total,message} per running phase so the UI can render a
// per-phase progress bar.
//
// We deliberately reuse the existing sync functions - this layer is *only*
// about scheduling and persistence. If a phase fails, we mark the IngestJob
// failed but keep going - partial data is much better than a blank dashboard.

import db from "../db.server.js";
import { syncOrders, syncOrdersSkeleton, backfillCustomerFirstNames } from "./orderSync.server.js";
import { syncMetaPass1, syncMetaPass2, syncMetaEntityPreflight } from "./metaSync.server.js";
import { syncMetaBreakdowns } from "./metaBreakdownSync.server.js";
import { syncMetaEntities } from "./metaEntitySync.server.js";
import { refreshAdCreatives } from "./metaAdCreativeSync.server.js";
import { runAttribution } from "./matcher.server.js";
import { sendOnboardingCompleteEmail } from "./email.server.js";
import { refreshProductImages } from "./productImageSync.server.js";
import { setProgress, completeProgress } from "./progress.server.js";
import { getOfflineAdmin } from "./offlineToken.server.js";
import { logIngestEvent, errInfo } from "./ingestEventLog.server.js";

// Retry policy for runPhase: how many times we re-run a failing phase before
// giving up and marking it failed. Most "failures" are transient (socket
// timeout under load, Meta rate-limit edge cases, brief network blip) so
// retrying with a backoff resolves them silently. A genuinely broken phase
// (bad token, missing scope) will still fail after MAX_PHASE_RETRIES.
const MAX_PHASE_RETRIES = 2;
const RETRY_BACKOFF_MS = [30_000, 60_000]; // 30s, then 60s

// How far back to pull Shopify orders on first install. Env-tunable so we
// can dial it up to 3 for hero accounts without redeploying.
const SHOPIFY_BACKFILL_YEARS = Number(process.env.SHOPIFY_BACKFILL_YEARS || 2);

// Per-track phase definitions. Within a track, phases run sequentially - they
// either depend on each other (entities before creatives) or share rate-limit
// budget (insights-90d then insights-13mo against the same Meta account).
// Between tracks, we Promise.all to overlap the wall-clock cost.
const TRACKS = {
  shopify: [
    {
      // Skeleton sweep: cheap id+name+createdAt walk at page-size 250 to
      // pre-build every Order row and reveal the EXACT count up-front.
      // Renders as its own progress row showing "X orders found...". Detail
      // phase below then walks the same window at page-size 50 with rich
      // fields, upserting into the skeleton rows already in place.
      key: "shopify-skeleton",
      label: "Counting your Shopify orders",
      requiresAdmin: true,
      progressKey: (shopDomain) => `syncOrdersSkeleton:${shopDomain}`,
      run: async (shopDomain, ctx) => {
        const r = await syncOrdersSkeleton(ctx.admin, shopDomain);
        return { rowsWritten: r?.totalCreated || 0 };
      },
    },
    {
      key: "shopify-orders",
      label: `${SHOPIFY_BACKFILL_YEARS} years of Shopify order details`,
      requiresAdmin: true,
      progressKey: (shopDomain) => `syncOrders:${shopDomain}`,
      run: async (shopDomain, ctx) => {
        const r = await syncOrders(ctx.admin, shopDomain);
        return { rowsWritten: r?.totalImported || 0 };
      },
    },
  ],
  meta: [
    {
      // Entity preflight: three tiny summary calls to surface campaign /
      // ad-set / ad totals before the slower daily-import bar starts moving.
      // Same UX idea as the Shopify skeleton row.
      key: "meta-preflight",
      label: "Discovering your ad account",
      progressKey: (shopDomain) => `ingest:${shopDomain}:meta-preflight`,
      run: async (shopDomain) => {
        await syncMetaEntityPreflight(shopDomain, `ingest:${shopDomain}:meta-preflight`);
        return { rowsWritten: 0 };
      },
    },
    {
      // Pass 1: daily aggregates across the whole 13-month window. Cheap,
      // low-row-count probe that tells us which days had conversions. Rendered
      // as its own progress row so the merchant sees a clean 0→400 days.
      key: "insights-pass1",
      label: "Daily ad totals (13 months)",
      progressKey: (shopDomain) => `ingest:${shopDomain}:insights-pass1`,
      run: async (shopDomain) => {
        const r = await syncMetaPass1(shopDomain, 400, `ingest:${shopDomain}:insights-pass1`);
        return { rowsWritten: r?.totalRows || 0 };
      },
    },
    {
      // Pass 2: hourly enrich for the conversion-day subset Pass 1 found.
      // Self-discovers from the DB (hourSlot=-1 rows with conversions>0), so
      // resume after crash is safe. Separate progress row means the bar shows
      // a clean 0→N where N is the actual count of conversion days, not the
      // confusing "789 = 400 + 389" combined total we used to show.
      key: "insights-pass2",
      label: "Hourly detail for conversion days",
      progressKey: (shopDomain) => `ingest:${shopDomain}:insights-pass2`,
      run: async (shopDomain) => {
        const r = await syncMetaPass2(shopDomain, 400, `ingest:${shopDomain}:insights-pass2`);
        return { rowsWritten: r?.totalRows || 0 };
      },
    },
    {
      // 13-month daily-aggregate backfill for all 6 breakdown types. Aggregate-
      // only — does NOT drive per-order metaAge/metaGender tags (going-forward
      // deltas in syncTodayBreakdowns handle that), so historical accuracy is
      // preserved without faking probabilistic tags. The full pull is a strict
      // superset of any "last N days" window, so no separate recent phase is
      // needed — the app is gated until onboardingCompleted, the merchant
      // never sees a partially populated Demographics tab.
      key: "breakdowns",
      label: "Demographic breakdowns (13 months, daily aggregates)",
      progressKey: (shopDomain) => `ingest:${shopDomain}:breakdowns`,
      run: async (shopDomain) => {
        const r = await syncMetaBreakdowns(shopDomain, `ingest:${shopDomain}:breakdowns`, 400);
        return { rowsWritten: r?.totalRows || 0 };
      },
    },
    {
      key: "entities",
      label: "Campaign, ad-set & ad metadata",
      progressKey: null,
      run: async (shopDomain) => {
        await syncMetaEntities(shopDomain);
        return { rowsWritten: 0 };
      },
    },
    {
      key: "creatives",
      label: "Ad creative thumbnails",
      progressKey: null,
      run: async (shopDomain) => {
        const r = await refreshAdCreatives(shopDomain);
        return { rowsWritten: r?.updated || 0 };
      },
    },
  ],
};

// Final phase - depends on BOTH tracks completing.
const FINAL_PHASE = {
  key: "match",
  label: "Matching Meta conversions to Shopify orders",
  progressKey: (shopDomain) => `runAttribution:${shopDomain}`,
  run: async (shopDomain) => {
    const r = await runAttribution(shopDomain);
    return { rowsWritten: r?.matched || 0 };
  },
};

// Tidying-up phase. Bakes EVERY rollup table BEFORE flipping
// onboardingCompleted. The merchant is about to land on a dashboard that
// reads from DailyCustomerRollup + DailyProductRollup + ShopAnalysisCache
// (Campaign Performance + Demographics + Customer Map). Skipping any of
// these means empty panels until the next hourly cycle.
//
// Previously this work ran silently after FINAL_PHASE. Now it's a visible
// phase row so the merchant sees "Tidying up & preparing dashboard" with
// live sub-step progress, instead of the dashboard hanging on FinalisingCard.
const FINAL_PHASE_2 = {
  key: "finalize",
  label: "Tidying up & preparing dashboard",
  progressKey: (shopDomain) => `ingest:${shopDomain}:finalize`,
  run: async (shopDomain, ctx) => {
    const pkey = `ingest:${shopDomain}:finalize`;
    const STEPS = [
      // Pulls billing first names from Shopify (the initial order sync stores
      // them only when present on the order at sync time — older customers
      // who placed orders before the billing-name field was reliably set
      // come back with empty firstName fields). Then runs inferGender against
      // every customer with a firstName, populating Customer.inferredGender
      // used by the demographics tile, Customer Map, and Product Demographics.
      // Without this, those views fall back to Meta-only gender (~30% coverage)
      // instead of Meta + inference (~85% coverage). The internal rollup
      // rebuild that follows is wasted work given the rebuilds below, but is
      // cheap (~15s) and the alternative is leaking the order of operations
      // into orderSync.server.js, so we accept the duplication.
      ["Inferring customer demographics", async () => {
        if (!ctx?.admin) {
          console.warn(`[ingestOrchestrator] ${shopDomain}: no admin client, skipping first-name backfill`);
          return;
        }
        await backfillCustomerFirstNames(ctx.admin, shopDomain);
      }],
      ["Building customer segments", async () => {
        const { rebuildCustomerSegments, rebuildCustomerRollups } = await import("./customerRollups.server.js");
        await rebuildCustomerSegments(shopDomain);
        await rebuildCustomerRollups(shopDomain);
      }],
      ["Building product rollups", async () => {
        const { rebuildProductRollups } = await import("./productRollups.server.js");
        await rebuildProductRollups(shopDomain);
      }],
      ["Building campaign rollups", async () => {
        const { rebuildCampaignRollups } = await import("./campaignRollups.server.js");
        await rebuildCampaignRollups(shopDomain);
      }],
      ["Building ad demographic rollups", async () => {
        const { rebuildAdDemographicRollups } = await import("./adDemographicRollups.server.js");
        await rebuildAdDemographicRollups(shopDomain);
      }],
      ["Building dashboard match accuracy", async () => {
        const { rebuildMatchAccuracy } = await import("./dashboardRollups.server.js");
        await rebuildMatchAccuracy(shopDomain);
      }],
      ["Building geographic rollups", async () => {
        const { rebuildGeoRollups } = await import("./geoRollups.server.js");
        await rebuildGeoRollups(shopDomain);
      }],
      ["Building customer gender chart data", async () => {
        const { rebuildCustomerGenderDaily } = await import("./customerRollups.server.js");
        await rebuildCustomerGenderDaily(shopDomain);
      }],
      ["Refreshing product images", async () => {
        await refreshProductImages(shopDomain);
      }],
      // Belt-and-braces re-run of refreshAdCreatives. The `creatives` phase
      // runs immediately after `entities`, which on large accounts (Vollebak
      // ~450+ ads, Advantage+ DPA) can finish before MetaEntity has settled
      // every row. The first creatives pass then operates on a partial set
      // and silently reports success while many ads still have no
      // imageUrl. Re-running here, after every other phase has completed,
      // ensures any newly-discovered ads get their thumbnails resolved
      // before the dashboard goes live. Cost: one extra bulk fetch +
      // hash-resolve cycle, governed by the Meta rate-limit governor.
      ["Filling in any missing ad creative images", async () => {
        const knownCount = await db.metaEntity.count({ where: { shopDomain, entityType: "ad" } });
        if (knownCount === 0) {
          console.warn(`[ingestOrchestrator] ${shopDomain}: skipping creatives-retry — no ad rows in MetaEntity`);
          return;
        }
        await refreshAdCreatives(shopDomain);
      }],
      ["Calibrating Meta pixel", async () => {
        const { calibratePixel } = await import("./pixelCalibration.server.js");
        await calibratePixel(shopDomain);
      }],
    ];
    const total = STEPS.length;
    for (let i = 0; i < STEPS.length; i++) {
      const [label, fn] = STEPS[i];
      setProgress(pkey, { status: "running", current: i, total, message: label });
      try {
        await fn();
        console.log(`[ingestOrchestrator] ${shopDomain}: ${label} done`);
      } catch (err) {
        // Non-fatal: a partial dashboard beats a blank one. Log + continue.
        console.error(`[ingestOrchestrator] ${shopDomain}: ${label} failed (non-fatal): ${err.message}`);
      }
    }
    // Persist rebuild timestamp so the hourly cycle's gated rebuildAllRollups
    // (24h throttle) skips a redundant run for the next 24h.
    try {
      await db.shop.update({ where: { shopDomain }, data: { lastRollupRebuild: new Date() } });
    } catch (err) {
      console.warn(`[ingestOrchestrator] ${shopDomain}: failed to update lastRollupRebuild: ${err.message}`);
    }
    completeProgress(pkey, { current: total, total, message: "Dashboard ready" });
    return { rowsWritten: 0 };
  },
};

// Flat list of every phase, in declaration order. Used by the status API to
// render the phase list deterministically.
export const ALL_PHASES = [
  ...TRACKS.shopify.map(p => ({ ...p, track: "shopify" })),
  ...TRACKS.meta.map(p => ({ ...p, track: "meta" })),
  { ...FINAL_PHASE, track: "final" },
  { ...FINAL_PHASE_2, track: "final" },
];

// Singleton guard - one ingest at a time per shop. If the OAuth callback
// fires twice (e.g. merchant double-clicks the connect button) we don't
// kick off two parallel ingests stomping on the same Meta budget.
const inFlight = (globalThis.__ingestInFlight ||= new Set());

// Exposed so the hourly scheduler can skip a shop while its onboarding
// ingest is running - they otherwise contend for the same SQLite
// connection pool and trigger socket timeouts mid-import.
export function isOnboardingIngestInFlight(shopDomain) {
  return inFlight.has(shopDomain);
}

/**
 * Public entry: triggered by the Meta OAuth callback after we've stored a
 * token + ad-account, OR by the Fit Test action once the merchant approves.
 * Fire-and-forget; HTTP returns immediately.
 *
 * The completion email recipient is resolved inside runIngest by querying
 * Shopify's { shop { email } } via the admin client we already need for the
 * Shopify track - so this works for both fresh OAuth callbacks and
 * resume-on-boot, no opt-args required.
 */
export async function startOnboardingIngest(shopDomain) {
  if (inFlight.has(shopDomain)) {
    console.log(`[ingestOrchestrator] ${shopDomain}: ingest already in flight, skipping`);
    return;
  }

  // If this store was exploring with sample data, a real Meta connect means
  // they're committing to real setup - wipe the demo store first so the real
  // ingest starts from a clean slate (demo data only ever exists on an
  // otherwise-empty store, so this can't touch real orders). wipeDemoData
  // preserves the just-stored Meta token/account; it only clears demo rows
  // and resets the demo/onboarding flags.
  try {
    const shop = await db.shop.findUnique({ where: { shopDomain }, select: { demoMode: true } });
    if (shop?.demoMode) {
      console.log(`[ingestOrchestrator] ${shopDomain}: real Meta connect on a demo store - wiping sample data first`);
      const { wipeDemoData } = await import("./demoData.server.js");
      await wipeDemoData(shopDomain);
    }
  } catch (err) {
    console.error(`[ingestOrchestrator] ${shopDomain}: demo wipe before ingest failed (non-fatal): ${err.message}`);
  }

  inFlight.add(shopDomain);

  // Flip the phase to "ingesting" up front so the UI can render the
  // progress card on the next poll. We don't swallow errors here - if the
  // DB is too overloaded to handle a single Shop update, the ingest is
  // doomed anyway and we want to know.
  try {
    await db.shop.update({
      where: { shopDomain },
      data: {
        onboardingPhase: "ingesting",
        onboardingStartedAt: new Date(),
      },
    });
  } catch (err) {
    console.error(`[ingestOrchestrator] ${shopDomain}: failed to set phase=ingesting: ${err.message}`);
    inFlight.delete(shopDomain);
    throw err;
  }

  // Run async, don't await - caller is the OAuth callback and must return.
  (async () => {
    try {
      await runIngest(shopDomain);
    } catch (err) {
      console.error(`[ingestOrchestrator] ${shopDomain}: orchestrator crashed: ${err.message}`);
    } finally {
      inFlight.delete(shopDomain);
    }
  })();
}

/**
 * Run all tracks for a shop, persisting an IngestJob per phase. Resumes
 * cleanly: a phase that already has a `completed` IngestJob is skipped.
 */
async function runIngest(shopDomain) {
  console.log(`[ingestOrchestrator] ${shopDomain}: starting phased ingest (parallel tracks)`);

  // Resolve an authenticated admin client for the Shopify track. We rebuild
  // it from the stored OAuth session rather than relying on the caller to
  // pass one - that way resume-on-boot still works.
  let admin = null;
  try {
    const result = await getOfflineAdmin(shopDomain);
    admin = result.admin;
  } catch (err) {
    console.warn(`[ingestOrchestrator] ${shopDomain}: no Shopify admin session available: ${err.message}`);
  }
  const ctx = { admin };

  // Run the two tracks in parallel. Each track is sequential internally,
  // but the wall-clock saving comes from overlapping Shopify import with
  // Meta import - they hit different APIs and write different tables.
  const trackResults = await Promise.allSettled([
    runTrack(shopDomain, "shopify", TRACKS.shopify, ctx),
    runTrack(shopDomain, "meta", TRACKS.meta, ctx),
  ]);

  for (const r of trackResults) {
    if (r.status === "rejected") {
      console.error(`[ingestOrchestrator] ${shopDomain}: track failed: ${r.reason?.message || r.reason}`);
    }
  }

  // Final phase: matcher needs both Shopify orders AND Meta insights present.
  await runPhase(shopDomain, FINAL_PHASE, ctx);

  // Tidying-up phase: bake every rollup table, refresh product images, and
  // calibrate the pixel BEFORE flipping onboardingCompleted. Runs as a
  // visible phase row in the OnboardingFlow checklist so the merchant sees
  // live progress instead of a static FinalisingCard.
  await runPhase(shopDomain, FINAL_PHASE_2, ctx);

  // Mark onboarding complete.
  await db.shop.update({
    where: { shopDomain },
    data: {
      onboardingPhase: "complete",
      onboardingCompleted: true,
    },
  });
  console.log(`[ingestOrchestrator] ${shopDomain}: phased ingest finished`);

  // Email the merchant. Fire-and-forget - email failures must not roll back
  // the "ingest complete" state. Resolve the recipient via Shopify GraphQL
  // (we have an admin client for the Shopify track) so resume-on-boot still
  // ends with a notification.
  if (admin) {
    try {
      const data = await admin.graphql(`{ shop { email myshopifyDomain } }`).then(r => r.json());
      const to = data?.data?.shop?.email;
      if (to) {
        sendOnboardingCompleteEmail({
          to,
          shopDomain,
          dashboardUrl: `https://${shopDomain}/admin/apps/lucidly`,
        }).catch((err) => console.warn(`[ingestOrchestrator] email failed: ${err?.message || err}`));
      } else {
        console.log(`[ingestOrchestrator] ${shopDomain}: no shop.email available, skipping completion email`);
      }
    } catch (err) {
      console.warn(`[ingestOrchestrator] failed to fetch shop.email: ${err?.message || err}`);
    }
  }
}

async function runTrack(shopDomain, trackName, phases, ctx) {
  for (const phase of phases) {
    await runPhase(shopDomain, phase, ctx);
  }
}

async function runPhase(shopDomain, phase, ctx) {
  // Skip phases we've already completed (resume support).
  const existing = await db.ingestJob.findFirst({
    where: { shopDomain, phase: phase.key, status: "completed" },
  });
  if (existing) {
    console.log(`[ingestOrchestrator] ${shopDomain}: phase ${phase.key} already complete, skipping`);
    return;
  }

  if (phase.requiresAdmin && !ctx.admin) {
    console.warn(`[ingestOrchestrator] ${shopDomain}: phase ${phase.key} requires admin client, skipping`);
    return;
  }

  const job = await db.ingestJob.create({
    data: {
      shopDomain,
      phase: phase.key,
      chunkLabel: phase.label,
      status: "running",
      startedAt: new Date(),
      attempts: 1,
    },
  });

  logIngestEvent({ shopDomain, phase: phase.key, type: "phase-start", label: phase.label });
  console.log(`[ingestOrchestrator] ${shopDomain}: starting phase ${phase.key}`);

  // Retry loop. Each attempt that throws is logged to the event log (with
  // full message + stack) so we have a durable trail. The IngestJob row only
  // holds the FINAL error if all retries exhausted. Until then we keep the
  // row in "running" status — the UI will show "retrying" rather than the
  // raw error, and the merchant sees a clean transition once it succeeds.
  let attempt = 0;
  let lastErr = null;
  while (attempt <= MAX_PHASE_RETRIES) {
    const attemptStartedAt = Date.now();
    try {
      const result = await phase.run(shopDomain, ctx);
      const elapsed = Math.round((Date.now() - attemptStartedAt) / 1000);
      console.log(`[ingestOrchestrator] ${shopDomain}: phase ${phase.key} done in ${elapsed}s (${result?.rowsWritten || 0} rows)`);
      logIngestEvent({
        shopDomain, phase: phase.key, type: "phase-complete",
        rowsWritten: result?.rowsWritten || 0, elapsedSec: elapsed, attempt: attempt + 1,
      });
      await db.ingestJob.update({
        where: { id: job.id },
        data: {
          status: "completed",
          completedAt: new Date(),
          rowsWritten: result?.rowsWritten || 0,
          // Clear any stale errorMessage from an earlier failed attempt.
          errorMessage: null,
        },
      });
      return;
    } catch (err) {
      lastErr = err;
      const isFinalAttempt = attempt >= MAX_PHASE_RETRIES;
      console.error(`[ingestOrchestrator] ${shopDomain}: phase ${phase.key} attempt ${attempt + 1} failed: ${err.message}`);
      logIngestEvent({
        shopDomain, phase: phase.key,
        type: isFinalAttempt ? "phase-failed" : "phase-retry",
        attempt: attempt + 1, maxAttempts: MAX_PHASE_RETRIES + 1,
        ...errInfo(err),
      });
      if (isFinalAttempt) break;
      // Bump attempts counter on the job row so the diagnostics page can see
      // how many tries we made. Keep status=running so the merchant UI shows
      // "retrying…" not "failed".
      await db.ingestJob.update({
        where: { id: job.id },
        data: { attempts: attempt + 2 },
      }).catch(() => {});
      const backoff = RETRY_BACKOFF_MS[attempt] || 60_000;
      await new Promise(r => setTimeout(r, backoff));
      attempt++;
    }
  }

  // Exhausted retries — mark the phase failed and move on. Independent
  // later phases still get a chance; partial data is much better than none.
  await db.ingestJob.update({
    where: { id: job.id },
    data: {
      status: "failed",
      completedAt: new Date(),
      errorMessage: lastErr?.message?.slice(0, 500) || String(lastErr).slice(0, 500),
    },
  });
}

/**
 * Snapshot of ingest progress for the dashboard card. Returns one row per
 * phase with status (pending/running/completed/failed) and timing/rows.
 */
export async function getIngestStatus(shopDomain) {
  const jobs = await db.ingestJob.findMany({
    where: { shopDomain },
    orderBy: { createdAt: "asc" },
  });

  // Group latest job per phase (we may have retries).
  const byPhase = new Map();
  for (const j of jobs) {
    byPhase.set(j.phase, j);
  }

  const phases = ALL_PHASES.map(p => {
    const j = byPhase.get(p.key);
    const progressKey = typeof p.progressKey === "function" ? p.progressKey(shopDomain) : null;
    if (!j) return { key: p.key, label: p.label, track: p.track, status: "pending", progressKey };
    return {
      key: p.key,
      label: p.label,
      track: p.track,
      status: j.status,
      rowsWritten: j.rowsWritten,
      errorMessage: j.errorMessage,
      startedAt: j.startedAt,
      completedAt: j.completedAt,
      progressKey,
    };
  });

  const shop = await db.shop.findUnique({
    where: { shopDomain },
    select: {
      onboardingPhase: true,
      onboardingStartedAt: true,
      onboardingCompleted: true,
      fitTestScore: true,
      fitTestData: true,
      fitTestComputedAt: true,
    },
  });

  return {
    onboardingPhase: shop?.onboardingPhase || "shopify",
    onboardingStartedAt: shop?.onboardingStartedAt,
    onboardingCompleted: shop?.onboardingCompleted || false,
    fitTestScore: shop?.fitTestScore ?? null,
    fitTestComputedAt: shop?.fitTestComputedAt || null,
    phases,
    inFlight: inFlight.has(shopDomain),
  };
}

/**
 * Recovery: on server boot, reset any Shop stuck in fit-importing or
 * fit-running back to "welcome". These phases are owned by the action
 * handler in app._index.tsx (NOT IngestJob rows), so a Fly redeploy mid-
 * import leaves them sticky in the DB while the in-memory progress map is
 * empty - merchant sees "Importing your last 90 days of orders" forever
 * with no live counter.
 *
 * Resetting to "welcome" surfaces the welcome card on next page load and
 * lets the merchant click Begin Fit Test again. The 90d sync is idempotent
 * (upsert keyed on shopDomain+shopifyOrderId), so retrying is safe.
 */
export async function reapOrphanedFitPhases() {
  const reset = await db.shop.updateMany({
    where: { onboardingPhase: { in: ["fit-importing", "fit-running"] } },
    data: { onboardingPhase: "welcome", onboardingStartedAt: null },
  });
  if (reset.count > 0) {
    console.log(`[ingestOrchestrator] Reset ${reset.count} shop(s) stuck in fit-importing/running back to welcome`);
  }
  return reset.count;
}

/**
 * Recovery: on server boot, reap orphaned `running` IngestJobs (their owning
 * process died mid-phase). Caller decides whether to immediately re-run.
 */
export async function reapOrphanedJobs() {
  const orphaned = await db.ingestJob.updateMany({
    where: { status: "running" },
    data: {
      status: "failed",
      errorMessage: "Process died before phase completed (orphaned on boot)",
      completedAt: new Date(),
    },
  });
  if (orphaned.count > 0) {
    console.log(`[ingestOrchestrator] Reaped ${orphaned.count} orphaned running jobs on boot`);
  }
  return orphaned.count;
}

/**
 * Resume any shops that were mid-ingest when the process died. Called from
 * server boot. Walks every shop with onboardingPhase=="ingesting" and
 * onboardingCompleted=false and re-kicks the orchestrator - completed
 * phases will be skipped via the existing-completed guard above.
 */
export async function resumePendingIngests() {
  const pending = await db.shop.findMany({
    where: {
      onboardingPhase: "ingesting",
      onboardingCompleted: false,
      metaAccessToken: { not: null },
      metaAdAccountId: { not: null },
    },
    select: { shopDomain: true },
  });
  for (const s of pending) {
    console.log(`[ingestOrchestrator] Resuming ingest for ${s.shopDomain}`);
    startOnboardingIngest(s.shopDomain);
  }
  return pending.length;
}
