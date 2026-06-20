// UNLINKED demo of the full onboarding Fit Test flow, playable on a live store
// for screen-recording / iteration. Starts from the very first install screen
// (intro / expectation-setting) → importing → calculating → the live Fit
// Report computed from the store's REAL order history.
//
// Non-destructive: the loader reads the cached fit snapshot (getFitTest) and
// only computes if one doesn't exist yet. It never alters onboarding state,
// Meta, orders, or any production data. Not linked in any nav - reach by URL:
//   /app/fit-demo
//
// Once the flow + copy are signed off here, this gets wired into the real
// onboarding (OnboardingFlow.tsx) as the canonical sequence.

import { useEffect, useState } from "react";
import { json } from "@remix-run/node";
import { useLoaderData } from "@remix-run/react";
import {
  Page, Card, Box, BlockStack, InlineStack, Text, Button, Spinner, Banner,
} from "@shopify/polaris";
import { authenticate } from "../shopify.server";
import { runFitTest, getFitTest } from "../services/fitTest.server.js";
import FitReport from "../components/FitReport";

export const loader = async ({ request }) => {
  const { session } = await authenticate.admin(request);
  const shopDomain = session.shop;
  // Non-destructive: prefer the cached snapshot; only compute if missing.
  let data = await getFitTest(shopDomain);
  if (!data) data = await runFitTest(shopDomain);
  return json({ data, shopDomain });
};

const PURPLE = "#7C3AED";
const PURPLE_LIGHT = "#A78BFA";

function GradientPill({ children }: { children: React.ReactNode }) {
  return (
    <span style={{
      display: "inline-block",
      background: `linear-gradient(90deg, ${PURPLE}, ${PURPLE_LIGHT})`,
      color: "#fff", padding: "4px 12px", borderRadius: 999,
      fontSize: 12, fontWeight: 600, letterSpacing: 0.3,
    }}>{children}</span>
  );
}

function ProgressBar({ pct }: { pct: number }) {
  return (
    <div style={{ background: "#F3F4F6", borderRadius: 999, overflow: "hidden", height: 10 }}>
      <div style={{
        width: `${Math.max(2, Math.min(100, pct))}%`, height: "100%",
        background: `linear-gradient(90deg, ${PURPLE}, ${PURPLE_LIGHT})`,
        transition: "width 0.5s ease",
        boxShadow: "0 0 12px rgba(124,58,237,0.35)",
      }} />
    </div>
  );
}

function FeatureBullet({ children, tone = "good" }: { children: React.ReactNode; tone?: "good" | "challenge" }) {
  const isGood = tone === "good";
  return (
    <InlineStack gap="200" blockAlign="start" wrap={false}>
      <span style={{
        color: isGood ? PURPLE : "#B45309", fontSize: 14, fontWeight: 700, lineHeight: "20px",
      }}>{isGood ? "\u2713" : "\u0021"}</span>
      <Text as="span" variant="bodyMd">{children}</Text>
    </InlineStack>
  );
}

type Step = "intro" | "importing" | "running" | "result";

export default function FitDemo() {
  const { data } = useLoaderData<typeof loader>() as any;
  const [step, setStep] = useState<Step>("intro");
  const [imported, setImported] = useState(0);

  const totalOrders: number = data?.ordersAnalysed || 0;

  // Importing screen: count up toward the real order total, then advance.
  useEffect(() => {
    if (step !== "importing") return;
    const target = totalOrders > 0 ? totalOrders : 250;
    const start = Date.now();
    const DURATION = 3000;
    const id = setInterval(() => {
      const t = Math.min(1, (Date.now() - start) / DURATION);
      setImported(Math.round(t * target));
      if (t >= 1) {
        clearInterval(id);
        setStep("running");
      }
    }, 60);
    return () => clearInterval(id);
  }, [step, totalOrders]);

  // Calculating screen: brief pause, then reveal the real report.
  useEffect(() => {
    if (step !== "running") return;
    const id = setTimeout(() => setStep("result"), 2400);
    return () => clearTimeout(id);
  }, [step]);

  // ─── Intro / expectation-setting (the very first install screen) ──────
  if (step === "intro") {
    return (
      <Page>
        <Box paddingBlockEnd="600">
          <Card>
            <Box padding="600">
              <BlockStack gap="500">
                <BlockStack gap="200">
                  <GradientPill>Welcome to Lucidly</GradientPill>
                  <Text as="h1" variant="heading2xl">Will Lucidly work for your store?</Text>
                </BlockStack>

                <Text as="p" variant="bodyLg" tone="subdued">
                  Lucidly matches your Meta conversions to your Shopify orders
                  statistically - by <strong>when</strong> an order happened and{" "}
                  <strong>how much</strong> it was for. So it works best when your
                  orders are distinguishable from one another.
                </Text>

                <Box padding="400" background="bg-surface-secondary" borderRadius="300" borderColor="border" borderWidth="025">
                  <BlockStack gap="400">
                    <BlockStack gap="300">
                      <Text as="h3" variant="headingMd">Who it&apos;s a great fit for</Text>
                      <FeatureBullet>
                        <strong>Varied order values</strong> - fashion, homeware, and
                        considered-purchase brands. A spread of prices makes each order
                        easy to tell apart.
                      </FeatureBullet>
                      <FeatureBullet>
                        <strong>Mid-range to higher AOV</strong> - fewer orders landing on
                        the exact same price point in the same moment.
                      </FeatureBullet>
                      <FeatureBullet>
                        <strong>A broad catalogue</strong> - different products at different
                        prices naturally separate your orders.
                      </FeatureBullet>
                      <FeatureBullet>
                        <strong>Steady, spread-out order flow</strong> - orders arriving
                        across the day rather than all in the same few minutes.
                      </FeatureBullet>
                      <FeatureBullet>
                        <strong>Normal day-to-day trading</strong> - outside of a major
                        sale, the Fit Test reflects your real, ongoing match rate.
                      </FeatureBullet>
                    </BlockStack>

                    <BlockStack gap="300">
                      <Text as="h3" variant="headingMd">Where it&apos;s more challenging</Text>
                      <FeatureBullet tone="challenge">
                        <strong>Narrow price range at high volume</strong> - lots of orders
                        at the <strong>same price</strong> in the <strong>same hour</strong> are
                        hard to distinguish. Because Lucidly matches on time and value, even a
                        few same-priced orders in one hourly window become rivals - and each
                        rival roughly halves the confidence on that order.
                      </FeatureBullet>
                      <FeatureBullet tone="challenge">
                        <strong>Single-product or one-price promos</strong> - when most
                        orders share the exact same value, there&apos;s little to tell them
                        apart, so matching is very difficult.
                      </FeatureBullet>
                      <FeatureBullet tone="challenge">
                        <strong>Sale periods and spikes</strong> - when order volume surges,
                        more orders pile into each hourly slot, so match rate can dip. Run
                        the Fit Test mid-sale and the projected rate may understate your
                        normal trading.
                      </FeatureBullet>
                      <FeatureBullet tone="challenge">
                        <strong>Flash drops and launches</strong> - a burst of near-identical
                        orders in minutes is the hardest case for purely-statistical matching.
                      </FeatureBullet>
                      <Text as="p" variant="bodySm" tone="subdued">
                        Our cookie-based Layer 1 (coming) closes these gaps by attributing
                        orders directly, regardless of their timing or value.
                      </Text>
                    </BlockStack>
                  </BlockStack>
                </Box>

                <Text as="p" variant="bodyMd" tone="subdued">
                  The Fit Test checks your <strong>real</strong> last-90-days order history
                  and tells you honestly, up front - before you import anything or connect
                  Meta. If that window included a big sale, your score may read lower than
                  normal trading - re-run it any time for a fresh snapshot. No surprises,
                  no commitment.
                </Text>

                <Button variant="primary" size="large" fullWidth onClick={() => setStep("importing")}>
                  Begin Fit Test
                </Button>
              </BlockStack>
            </Box>
          </Card>
        </Box>
      </Page>
    );
  }

  // ─── Importing (90d minimal Shopify sync) ─────────────────────────────
  if (step === "importing") {
    const target = totalOrders > 0 ? totalOrders : 250;
    const pct = Math.min(80, 8 + (imported / target) * 72);
    return (
      <Page>
        <Box paddingBlockEnd="600">
          <Card>
            <Box padding="600">
              <BlockStack gap="500">
                <InlineStack gap="200" blockAlign="center">
                  <GradientPill>Step 1 of 1</GradientPill>
                  <Text as="span" variant="bodySm" tone="subdued">Fit Test</Text>
                </InlineStack>
                <BlockStack gap="200">
                  <Text as="h2" variant="headingLg">Importing your last 90 days of orders</Text>
                  <Text as="p" variant="bodyMd" tone="subdued">
                    Pulling just order timestamps and values - nothing else.
                  </Text>
                </BlockStack>
                <ProgressBar pct={pct} />
                <Text as="p" variant="bodySm" tone="subdued">
                  {imported.toLocaleString()} orders imported so far
                </Text>
              </BlockStack>
            </Box>
          </Card>
        </Box>
      </Page>
    );
  }

  // ─── Calculating ──────────────────────────────────────────────────────
  if (step === "running") {
    return (
      <Page>
        <Box paddingBlockEnd="600">
          <Card>
            <Box padding="600">
              <BlockStack gap="500">
                <InlineStack gap="300" blockAlign="center">
                  <Spinner size="small" />
                  <Text as="h2" variant="headingLg">Calculating your Fit Score</Text>
                </InlineStack>
                <Text as="p" variant="bodyMd" tone="subdued">
                  Analysing rival density across every hour - checking how many
                  orders Meta might confuse with each other.
                </Text>
                <ProgressBar pct={92} />
              </BlockStack>
            </Box>
          </Card>
        </Box>
      </Page>
    );
  }

  // ─── Result (the live Fit Report) ─────────────────────────────────────
  if (!data || data.score === null) {
    return (
      <Page title="Lucidly Fit Report">
        <Banner tone="info">
          <p>{data?.message || "Not enough order history yet to compute a Fit score on this store."}</p>
        </Banner>
      </Page>
    );
  }

  return (
    <Page title="Lucidly Fit Report">
      <FitReport d={data} />
    </Page>
  );
}
