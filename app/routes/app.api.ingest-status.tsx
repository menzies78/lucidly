import { json } from "@remix-run/node";
import { authenticate } from "../shopify.server";
import { getIngestStatus } from "../services/ingestOrchestrator.server";
import { getProgress } from "../services/progress.server";
import { getFitTest } from "../services/fitTest.server.js";
import { getMetaAuthUrl } from "../services/metaAuth.server";

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
        message: p.message || null,
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

  return json({ ...status, phases: phasesWithProgress, fitImportLive, fitTestData, metaAuthUrl });
};
