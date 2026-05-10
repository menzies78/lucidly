import { json } from "@remix-run/node";
import { authenticate } from "../shopify.server";
import { getIngestStatus } from "../services/ingestOrchestrator.server";
import { getProgress } from "../services/progress.server";

// Status endpoint for the onboarding flow. Polled every 3s by OnboardingFlow.
// Returns:
//   - ingest phase + per-phase status (from IngestJob rows)
//   - fit-test score + computedAt (from Shop)
//   - liveMessage for whichever phase is currently running
//   - livePhaseKey so the UI knows which phase row to attach the live msg to
//   - fitImportLive for the 90d minimal Shopify sync (separate progress key)
//
// All fields are optional/nullable - the UI is defensive and tolerates missing
// data on early polls.
export const loader = async ({ request }) => {
  const { session } = await authenticate.admin(request);
  const shopDomain = session.shop;

  const status = await getIngestStatus(shopDomain);

  // Decorate with live progress (the progress.server map keyed per phase) so
  // the UI can show "65% · 12,840 rows · 8m left" inside the running phase.
  // With parallel tracks there can be MULTIPLE running phases at once - we
  // pick the most recently started so the UI shows whatever's freshest.
  const runningPhases = status.phases.filter(p => p.status === "running");
  let liveMessage = null;
  let livePhaseKey = null;
  if (runningPhases.length > 0) {
    runningPhases.sort((a, b) => {
      const ta = a.startedAt ? new Date(a.startedAt).getTime() : 0;
      const tb = b.startedAt ? new Date(b.startedAt).getTime() : 0;
      return tb - ta;
    });
    const live = runningPhases[0];
    const p = getProgress(`ingest:${shopDomain}:${live.key}`);
    if (p?.message) {
      liveMessage = p.message;
      livePhaseKey = live.key;
    }
  }

  // Fit Test imports use a different progress key (no IngestJob row - the
  // 90d minimal sync doesn't go through the orchestrator). Surface it
  // separately so the FitImportingCard can show live order count.
  const fitImport = getProgress(`fit-test-import:${shopDomain}`);
  const fitImportLive = fitImport ? {
    current: fitImport.current || 0,
    message: fitImport.message || null,
    status: fitImport.status || "idle",
  } : null;

  return json({ ...status, liveMessage, livePhaseKey, fitImportLive });
};
