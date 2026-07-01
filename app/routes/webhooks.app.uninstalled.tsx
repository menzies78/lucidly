import type { ActionFunctionArgs } from "@remix-run/node";
import { authenticate } from "../shopify.server";
import db from "../db.server";

export const action = async ({ request }: ActionFunctionArgs) => {
  const { shop, session, topic } = await authenticate.webhook(request);

  console.log(`Received ${topic} webhook for ${shop}`);

  // Webhook requests can trigger multiple times and after an app has already been uninstalled.
  // If this webhook already ran, the session may have been deleted previously.
  if (session) {
    await db.session.deleteMany({ where: { shop } });
  }

  // Reset onboarding so a reinstall always lands on the standard Welcome screen.
  // The Shop row (and its imported orders + Meta token) intentionally survives an
  // uninstall, but the onboarding state machine must NOT: otherwise a merchant who
  // reinstalls resumes at their last phase (e.g. "fit-ready") and is shown a stale
  // Fit Report instead of Welcome. Clearing the fit-test artifacts also guarantees
  // the "not enough order history" empty-state can only appear as the result of an
  // actively-run Fit Test, never on first load after reinstall.
  await db.shop.updateMany({
    where: { shopDomain: shop },
    data: {
      onboardingPhase: "welcome",
      onboardingCompleted: false,
      onboardingStartedAt: null,
      fitTestScore: null,
      fitTestData: null,
      fitTestComputedAt: null,
    },
  });

  return new Response();
};
