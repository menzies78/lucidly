import { json } from "@remix-run/node";
import { authenticate } from "../shopify.server";
import { getIngestStatus } from "../services/ingestOrchestrator.server";
import { getProgress } from "../services/progress.server";

// Status endpoint for the onboarding progress card. Polled every few seconds
// while onboardingPhase=="ingesting"; the card auto-hides once
// onboardingCompleted flips true.
export const loader = async ({ request }) => {
  const { session } = await authenticate.admin(request);
  const shopDomain = session.shop;

  const status = await getIngestStatus(shopDomain);

  // Decorate with live progress (the progress.server map keyed per phase) so
  // the UI can show "65% · 12,840 rows · 8m left" inside the running phase.
  const livePhase = status.phases.find(p => p.status === "running");
  let liveMessage = null;
  if (livePhase) {
    const p = getProgress(`ingest:${shopDomain}:${livePhase.key}`);
    if (p?.message) liveMessage = p.message;
  }

  return json({ ...status, liveMessage });
};
