// Lucidly Fit Report - the "will it work for my company?" page. Reads from the
// cached fitTestData JSON so it renders instantly; if the test hasn't run yet,
// the loader fires it on demand (single-flight guarded). Rich report markup
// lives in app/components/FitReport.tsx (shared with the onboarding demo).

import { json } from "@remix-run/node";
import { useLoaderData } from "@remix-run/react";
import { Page, Banner } from "@shopify/polaris";
import { authenticate } from "../shopify.server";
import db from "../db.server";
import { runFitTest, getFitTest } from "../services/fitTest.server.js";
import FitReport from "../components/FitReport";

export const loader = async ({ request }) => {
  const { session } = await authenticate.admin(request);
  const shopDomain = session.shop;

  const shop = await db.shop.findUnique({
    where: { shopDomain },
    select: {
      onboardingCompleted: true,
      lastOrderSync: true,
      fitTestComputedAt: true,
    },
  });

  // No order sync yet - tell the merchant to wait, the test needs orders.
  if (!shop?.lastOrderSync) {
    return json({ status: "waiting", shopDomain });
  }

  // Run on demand if we haven't yet, or if data is older than 7 days.
  const stale = !shop.fitTestComputedAt
    || (Date.now() - shop.fitTestComputedAt.getTime() > 7 * 24 * 3600 * 1000);
  let data = await getFitTest(shopDomain);
  // Also recompute if the snapshot predates the per-hour distribution.
  const missingHourly = data && data.score !== null && !data.hourly;
  if (!data || stale || missingHourly) {
    data = await runFitTest(shopDomain);
  }

  return json({ status: "ready", data, shopDomain });
};

export default function FitTest() {
  const data = useLoaderData<typeof loader>();

  if (data.status === "waiting") {
    return (
      <Page title="Lucidly Fit Report">
        <Banner tone="info">
          <p>
            We need your order history to predict how well our matcher will work
            for you. This usually takes a couple of minutes - refresh this page
            once your orders have synced.
          </p>
        </Banner>
      </Page>
    );
  }

  const d = (data as any).data;

  if (!d || d.score === null) {
    return (
      <Page title="Lucidly Fit Report">
        <Banner tone="info">
          <p>{d?.message || "Not enough order history yet to compute a Fit score."}</p>
        </Banner>
      </Page>
    );
  }

  return (
    <Page title="Lucidly Fit Report">
      <FitReport d={d} />
    </Page>
  );
}
