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
import { syncOrders } from "./orderSync.server.js";
import { syncMetaInsights } from "./metaSync.server.js";
import { syncMetaBreakdowns } from "./metaBreakdownSync.server.js";
import { syncMetaEntities } from "./metaEntitySync.server.js";
import { refreshAdCreatives } from "./metaAdCreativeSync.server.js";
import { runAttribution } from "./matcher.server.js";
import { sendOnboardingCompleteEmail } from "./email.server.js";
import { unauthenticated } from "../shopify.server";

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
      key: "shopify-orders",
      label: `${SHOPIFY_BACKFILL_YEARS} years of Shopify orders`,
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
      key: "insights-13mo",
      label: "13 months of historical ad data",
      progressKey: (shopDomain) => `ingest:${shopDomain}:insights-13mo`,
      run: async (shopDomain) => {
        const r = await syncMetaInsights(shopDomain, 400, `ingest:${shopDomain}:insights-13mo`);
        return { rowsWritten: r?.totalRows || 0 };
      },
    },
    {
      key: "breakdowns",
      label: "Country, platform, age & gender breakdowns",
      progressKey: (shopDomain) => `ingest:${shopDomain}:breakdowns`,
      run: async (shopDomain) => {
        const r = await syncMetaBreakdowns(shopDomain, `ingest:${shopDomain}:breakdowns`, 7);
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

// Flat list of every phase, in declaration order. Used by the status API to
// render the phase list deterministically.
export const ALL_PHASES = [
  ...TRACKS.shopify.map(p => ({ ...p, track: "shopify" })),
  ...TRACKS.meta.map(p => ({ ...p, track: "meta" })),
  { ...FINAL_PHASE, track: "final" },
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
    const result = await unauthenticated.admin(shopDomain);
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

  console.log(`[ingestOrchestrator] ${shopDomain}: starting phase ${phase.key}`);
  const startedAt = Date.now();
  try {
    const result = await phase.run(shopDomain, ctx);
    const elapsed = Math.round((Date.now() - startedAt) / 1000);
    console.log(`[ingestOrchestrator] ${shopDomain}: phase ${phase.key} done in ${elapsed}s (${result?.rowsWritten || 0} rows)`);
    await db.ingestJob.update({
      where: { id: job.id },
      data: {
        status: "completed",
        completedAt: new Date(),
        rowsWritten: result?.rowsWritten || 0,
      },
    });
  } catch (err) {
    console.error(`[ingestOrchestrator] ${shopDomain}: phase ${phase.key} failed: ${err.message}`);
    await db.ingestJob.update({
      where: { id: job.id },
      data: {
        status: "failed",
        completedAt: new Date(),
        errorMessage: err.message?.slice(0, 500) || String(err).slice(0, 500),
      },
    });
    // Keep going. A failed phase shouldn't block independent later phases -
    // partial data is much better than no data.
  }
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
