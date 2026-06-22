// UNLINKED demo of the onboarding Fit Test flow, playable on a live store for
// screen-recording / iteration. Starts on a graphical intro (how Lucidly works +
// who it suits), then a single CTA kicks off importing → calculating → the live
// Fit Report computed from REAL order history.
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
  Page, Card, Box, BlockStack, InlineStack, Text, Spinner, Banner,
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

// "Who is Lucidly for?" answer: a large primary line + smaller supporting copy,
// led by an oversized purple tick.
function WhoBullet({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <InlineStack gap="300" blockAlign="start" wrap={false}>
      <span style={{ color: PURPLE, fontSize: 24, fontWeight: 800, lineHeight: "30px" }}>{"\u2713"}</span>
      <BlockStack gap="050">
        <Text as="span" variant="headingMd">{title}</Text>
        <Text as="span" variant="bodyMd" tone="subdued">{children}</Text>
      </BlockStack>
    </InlineStack>
  );
}

function PurpleButton({ children, onClick }: { children: React.ReactNode; onClick: () => void }) {
  return (
    <button type="button" onClick={onClick} style={{
      display: "inline-flex", alignItems: "center", justifyContent: "center",
      width: "100%", padding: "16px 24px", border: "none", borderRadius: 10,
      cursor: "pointer", color: "#fff", fontWeight: 700, fontSize: 16, letterSpacing: 0.5,
      background: `linear-gradient(90deg, ${PURPLE}, ${PURPLE_LIGHT})`,
      boxShadow: "0 4px 14px rgba(124,58,237,0.35)",
    }}>{children}</button>
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

  // ─── Intro: how Lucidly works + who it's for + CTA ────────────────────
  if (step === "intro") {
    return (
      <Page>
        <Box paddingBlockEnd="600">
          <BlockStack gap="400">
            {/* Graphical intro: how it works + who it suits */}
            <Card>
              <Box padding="600">
                <BlockStack gap="500">
                  <GradientPill>Welcome to Lucidly</GradientPill>
                  <BlockStack gap="300">
                    <Text as="h1" variant="heading2xl">How does Lucidly work?</Text>
                    <Text as="p" variant="bodyLg" tone="subdued">
                      Lucidly matches your Meta conversions to your Shopify orders statistically -
                      comparing the Meta-reported transaction amount and time slot with Shopify
                      orders of the same amount and time slot. So it works best when your orders
                      are distinguishable from one another.
                    </Text>
                  </BlockStack>
                  <div style={{ borderTop: "1px solid #E3E3E3" }} />
                  <BlockStack gap="400">
                    <Text as="h2" variant="heading2xl">Who is Lucidly for?</Text>
                    <BlockStack gap="400">
                      <WhoBullet title="Stores with varied order values">
                        Fashion, homeware, and considered-purchase brands. A spread of prices makes each order easy to tell apart.
                      </WhoBullet>
                      <WhoBullet title="Mid-range to higher AOV">
                        Fewer orders landing on the exact same price point in the same moment.
                      </WhoBullet>
                      <WhoBullet title="A broad catalogue">
                        Different products at different prices naturally separate your orders.
                      </WhoBullet>
                      <WhoBullet title="Steady, spread-out order flow">
                        Orders arriving across the day rather than all in the same few minutes.
                      </WhoBullet>
                      <WhoBullet title="Normal day-to-day trading">
                        Outside of a major sale, the Fit Test reflects your real, ongoing match rate.
                      </WhoBullet>
                    </BlockStack>
                  </BlockStack>
                </BlockStack>
              </Box>
            </Card>

            {/* The Fit Test CTA */}
            <Card>
              <Box padding="600">
                <BlockStack gap="400">
                  <BlockStack gap="200">
                    <Text as="h2" variant="heading2xl">Will Lucidly work for your store?</Text>
                    <Text as="p" variant="bodyLg" tone="subdued">
                      The Fit Test reads your last 90 days of real orders and predicts exactly how
                      accurately Lucidly will match your Meta conversions - no guessing, no commitment.
                    </Text>
                  </BlockStack>
                  <PurpleButton onClick={() => setStep("importing")}>
                    RUN THE 30 SECOND FIT TEST
                  </PurpleButton>
                </BlockStack>
              </Box>
            </Card>
          </BlockStack>
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
