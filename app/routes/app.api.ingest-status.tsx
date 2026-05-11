import { json } from "@remix-run/node";
import { authenticate } from "../shopify.server";
import { getIngestStatus } from "../services/ingestOrchestrator.server";
import { getProgress } from "../services/progress.server";
import { getFitTest } from "../services/fitTest.server.js";
import { getMetaAuthUrl } from "../services/metaAuth.server";
import { snapshot as metaGovernorSnapshot } from "../services/metaGovernor.server.js";

// Status endpoint for the onboarding flow. Polled every 3s by OnboardingFlow.
// Returns:
//   - ingest phase + per-phase status (from IngestJob rows)
//   - per-phase live progress (current/total/message) for any running phase,
//     so each row in the UI gets its own progress bar
//   - fit-test score + computedAt (from Shop)
//   - fitImportLive for the 90d minimal Shopify sync (separate progress key)
//
// All fields are optional/nullable - the UI is defensive and tolerates missing
// data on early polls.
export const loader = async ({ request }) => {
  const { session } = await authenticate.admin(request);
  const shopDomain = session.shop;

  const status = await getIngestStatus(shopDomain);

  // Decorate every phase with its live progress (current/total/message) from
  // the in-memory progress map. We do this for ALL phases, not just one
  // "winner" - the UI renders a per-phase progress bar so the merchant sees
  // both Shopify and Meta tracks ticking up in parallel.
  const phasesWithProgress = status.phases.map(phase => {
    if (!phase.progressKey) return phase;
    const p = getProgress(phase.progressKey);
    if (!p) return phase;
    return {
      ...phase,
      live: {
        current: typeof p.current === "number" ? p.current : null,
        total: typeof p.total === "number" ? p.total : null,
        totalIsApproximate: !!p.totalIsApproximate,
        unitLabel: p.unitLabel || null,
        detail: p.detail || p.message || null,
        rowsImported: typeof p.rowsImported === "number" ? p.rowsImported : null,
      },
    };
  });

  // Fit Test imports use a different progress key (no IngestJob row - the
  // 90d minimal sync doesn't go through the orchestrator). Surface it
  // separately so the FitImportingCard can show live order count.
  const fitImport = getProgress(`fit-test-import:${shopDomain}`);
  const fitImportLive = fitImport ? {
    current: fitImport.current || 0,
    message: fitImport.message || null,
    status: fitImport.status || "idle",
  } : null;

  // Pull the full Fit Test JSON snapshot so the FitReadyCard can show
  // dual scores (historic + projected ongoing), histogram, AOV, etc.
  // Cheap - it's a single Shop row read.
  const fitTestData = await getFitTest(shopDomain);

  // Meta OAuth URL so the FitReadyCard's "Connect Meta Ads" button can
  // open the Facebook auth popup directly - no intermediate page.
  const url = new URL(request.url);
  const metaAuthUrl = getMetaAuthUrl(shopDomain, `https://${url.host}`);

  // Meta API rate-limit state. Surfaces app-usage, per-account BUC util,
  // insights-throttle, and any "blocked for N seconds" cooldown so the UI
  // can show why the Meta track is sitting still. Without this, a frozen
  // counter looks like a bug when it's actually Meta parking us.
  const metaGovernor = metaGovernorSnapshot();
  const metaGovernorSummary = summariseGovernor(metaGovernor);

  return json({
    ...status,
    phases: phasesWithProgress,
    fitImportLive,
    fitTestData,
    metaAuthUrl,
    metaGovernor,
    metaGovernorSummary,
  });
};

// Collapse the per-account governor snapshot into a single human-readable
// line for the UI. Picks the worst account (highest util / longest block)
// so the merchant sees the most relevant signal without us shipping a
// per-account table during onboarding.
function summariseGovernor(snap) {
  const accounts = Object.entries(snap.accounts || {});
  let worstAcct = null;
  for (const [key, a] of accounts) {
    const score = Math.max(a.bucMaxPct || 0, a.insightsAccPct || 0) + (a.blockedFor || 0) * 2;
    if (!worstAcct || score > worstAcct.score) {
      worstAcct = { key, ...a, score };
    }
  }
  const app = snap.appUsage || {};
  const appMax = Math.max(app.call_count || 0, app.total_cputime || 0, app.total_time || 0);
  const acctUtil = worstAcct ? Math.max(worstAcct.bucMaxPct || 0, worstAcct.insightsAccPct || 0) : 0;
  const blockedFor = worstAcct?.blockedFor || 0;
  return {
    appUtilPct: appMax,
    acctUtilPct: acctUtil,
    blockedForSec: blockedFor,
    worstAccount: worstAcct?.key || null,
  };
}
