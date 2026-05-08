// Phased ingest orchestrator. Run after Meta OAuth completes.
//
// Premise: a fresh install needs to pull a *lot* of data from Meta - up to 13
// months of ad-hourly insights, plus breakdowns, plus entity metadata, plus
// creative thumbnails - before the dashboards are useful. Naively chaining
// the existing sync entry points works, but:
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
//
// We deliberately reuse the existing sync functions (syncMetaInsights,
// syncMetaBreakdowns, etc.) - this layer is *only* about scheduling and
// persistence. If a phase fails, we mark the IngestJob failed but keep
// going - partial data is much better than a blank dashboard.

import db from "../db.server.js";
import { syncMetaInsights } from "./metaSync.server.js";
import { syncMetaBreakdowns } from "./metaBreakdownSync.server.js";
import { syncMetaEntities } from "./metaEntitySync.server.js";
import { refreshAdCreatives } from "./metaAdCreativeSync.server.js";
import { runAttribution } from "./matcher.server.js";

// Phase definitions. Order matters - later phases depend on earlier ones
// (e.g. matching needs insights present, creatives need entities). Each
// phase has a label (UI string) and a runner (async fn -> { rowsWritten? }).
//
// Day windows:
//   - Account-90d is the "fast feedback" phase: enough data to make the
//     dashboard useful within minutes. We use the existing syncMetaInsights
//     which switches to daily aggregates beyond HOURLY_LIMIT_DAYS - so 90d
//     pulls hourly for the recent slice + daily for the older slice.
//   - 13-month is the full historical pull. Same code path but a larger
//     daysBack. Most of the time is spent here.
const PHASES = [
  {
    key: "insights-90d",
    label: "Last 90 days of ad performance",
    daysBack: 90,
    run: async (shopDomain) => {
      const r = await syncMetaInsights(shopDomain, 90, `ingest:${shopDomain}:insights-90d`);
      return { rowsWritten: r?.totalRows || 0 };
    },
  },
  {
    key: "insights-13mo",
    label: "13 months of historical ad data",
    daysBack: 400,
    run: async (shopDomain) => {
      const r = await syncMetaInsights(shopDomain, 400, `ingest:${shopDomain}:insights-13mo`);
      return { rowsWritten: r?.totalRows || 0 };
    },
  },
  {
    key: "breakdowns",
    label: "Country, platform, age & gender breakdowns",
    daysBack: 7,
    run: async (shopDomain) => {
      const r = await syncMetaBreakdowns(shopDomain, `ingest:${shopDomain}:breakdowns`, 7);
      return { rowsWritten: r?.totalRows || 0 };
    },
  },
  {
    key: "entities",
    label: "Campaign, ad-set & ad metadata",
    run: async (shopDomain) => {
      await syncMetaEntities(shopDomain);
      return { rowsWritten: 0 };
    },
  },
  {
    key: "creatives",
    label: "Ad creative thumbnails",
    run: async (shopDomain) => {
      const r = await refreshAdCreatives(shopDomain);
      return { rowsWritten: r?.updated || 0 };
    },
  },
  {
    key: "match",
    label: "Matching Meta conversions to Shopify orders",
    run: async (shopDomain) => {
      const r = await runAttribution(shopDomain);
      return { rowsWritten: r?.matched || 0 };
    },
  },
];

// Singleton guard - one ingest at a time per shop. If the OAuth callback
// fires twice (e.g. merchant double-clicks the connect button) we don't
// kick off two parallel ingests stomping on the same Meta budget.
const inFlight = (globalThis.__ingestInFlight ||= new Set());

/**
 * Public entry: triggered by the Meta OAuth callback after we've stored a
 * token + ad-account. Fire-and-forget; HTTP returns immediately.
 */
export async function startOnboardingIngest(shopDomain) {
  if (inFlight.has(shopDomain)) {
    console.log(`[ingestOrchestrator] ${shopDomain}: ingest already in flight, skipping`);
    return;
  }
  inFlight.add(shopDomain);

  // Mark the shop as ingesting so the dashboard shows the progress card.
  await db.shop.update({
    where: { shopDomain },
    data: {
      onboardingPhase: "ingesting",
      onboardingStartedAt: new Date(),
    },
  }).catch(() => {});

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
 * Run all phases for a shop, persisting an IngestJob per phase. Resumes
 * cleanly: a phase that already has a `completed` IngestJob is skipped.
 */
async function runIngest(shopDomain) {
  console.log(`[ingestOrchestrator] ${shopDomain}: starting phased ingest`);

  for (const phase of PHASES) {
    // Skip phases we've already completed (resume support).
    const existing = await db.ingestJob.findFirst({
      where: { shopDomain, phase: phase.key, status: "completed" },
    });
    if (existing) {
      console.log(`[ingestOrchestrator] ${shopDomain}: phase ${phase.key} already complete, skipping`);
      continue;
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
      const result = await phase.run(shopDomain);
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
      // Keep going. A failed entities phase shouldn't block creatives or
      // the matcher - partial data is much better than no data.
    }
  }

  // Mark onboarding complete when the last phase has been attempted.
  await db.shop.update({
    where: { shopDomain },
    data: {
      onboardingPhase: "complete",
      onboardingCompleted: true,
    },
  });
  console.log(`[ingestOrchestrator] ${shopDomain}: phased ingest finished`);
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

  const phases = PHASES.map(p => {
    const j = byPhase.get(p.key);
    if (!j) return { key: p.key, label: p.label, status: "pending" };
    return {
      key: p.key,
      label: p.label,
      status: j.status,
      rowsWritten: j.rowsWritten,
      errorMessage: j.errorMessage,
      startedAt: j.startedAt,
      completedAt: j.completedAt,
    };
  });

  const shop = await db.shop.findUnique({
    where: { shopDomain },
    select: {
      onboardingPhase: true,
      onboardingStartedAt: true,
      onboardingCompleted: true,
    },
  });

  return {
    onboardingPhase: shop?.onboardingPhase || "shopify",
    onboardingStartedAt: shop?.onboardingStartedAt,
    onboardingCompleted: shop?.onboardingCompleted || false,
    phases,
    inFlight: inFlight.has(shopDomain),
  };
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
