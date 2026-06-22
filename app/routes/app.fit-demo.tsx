// UNLINKED demo of the full onboarding Fit Test flow, playable on a live store
// for screen-recording / iteration. Starts from the very first install screen:
// a quick 3-slider Q&A that gives an INSTANT primed verdict (never gates), then
// importing → calculating → the live Fit Report computed from REAL order history.
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
  Page, Card, Box, BlockStack, InlineStack, Text, Button, Spinner, Banner, RangeSlider,
} from "@shopify/polaris";
import { authenticate } from "../shopify.server";
import { runFitTest, getFitTest } from "../services/fitTest.server.js";
import FitReport, { VerdictBadge } from "../components/FitReport";

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

// The four value-variety options (slider index 0..3) and the per-option base
// probability that two orders in the same hour land within ±1% of each other.
// Re-anchored on real live match rates: Vollebak (~30/day, ~200 products, very
// mixed) runs ~100% live, and HM holds a 92% rolling-30d rate even at ~45/day
// THROUGH a sale (which compresses value variety). So "very mixed" must stay
// near-perfect across normal volumes; the lower options carry the volume drop-off.
const VARIETY_LABELS = ["All the same", "Very similar", "Some variety", "Very mixed"];
const VARIETY_BASE = [0.80, 0.35, 0.07, 0.008];

// Quick, client-side proxy for the real matcher, calibrated against real stores.
// The matcher's confidence is 100/(1+rivals), where rivals are orders sharing an
// hourly slot at a near-identical (±1%) value. We estimate:
//   peers   = orders competing in the same hour  ≈ ordersPerDay × 0.092
//             (anchored on Vollebak: 20.9 orders/day → 1.92 same-hour peers)
//   collide = chance a peer is within ±1% of value (driven by price variety,
//             diluted by a broad catalogue)
//   rivals  = peers × collide  →  confidence = 100/(1+rivals)
// Validated against live match rates: Vollebak (~30/day, ~200 products, very
// mixed) → ~98 (runs ~100% live); HM very-mixed at normal volume → ~98, and
// drops toward Good only when a sale both lifts volume and compresses variety.
// Maps to the SAME four verdict bands the real Fit Report uses, so the instant
// read rarely disagrees.
const PEERS_PER_OPD = 0.092;

function quickVerdict({ ordersPerDay, products, variety, saleNow }: {
  ordersPerDay: number; products: number; variety: number; saleNow: boolean;
}): { verdict: string; confidence: number; reasons: Array<{ tone: "good" | "challenge"; text: React.ReactNode }> } {
  const base = VARIETY_BASE[variety] ?? 0.12;
  // More products spread prices across more price points → fewer ±1% collisions.
  const productFactor = Math.min(1.8, Math.max(0.5, 1.6 - 0.35 * Math.log10(Math.max(1, products))));
  const collide = Math.min(0.98, Math.max(0.01, base * productFactor));
  let peers = ordersPerDay * PEERS_PER_OPD;
  if (saleNow) peers *= 1.7;                        // a spike crowds every hour
  const expectedRivals = peers * collide;
  const confidence = Math.round(Math.max(5, Math.min(99, 100 / (1 + expectedRivals))));

  let verdict: string;
  if (confidence >= 80) verdict = "excellent";
  else if (confidence >= 60) verdict = "good";
  else if (confidence >= 40) verdict = "marginal";
  else verdict = "challenging";

  // Personalised reasons - only the ones that actually apply to their answers.
  const reasons: Array<{ tone: "good" | "challenge"; text: React.ReactNode }> = [];
  if (variety >= 3) reasons.push({ tone: "good", text: <><strong>Varied order values.</strong> A wide spread of prices makes each order easy to tell apart.</> });
  else if (variety <= 1) reasons.push({ tone: "challenge", text: <><strong>Similar order values.</strong> When orders share a near-identical value, each one in the same hour roughly halves the confidence on its match.</> });
  if (products <= 3) reasons.push({ tone: "challenge", text: <><strong>Very few products.</strong> One-price or single-product stores produce near-identical orders that are hard to separate.</> });
  else if (products >= 100 && variety >= 2) reasons.push({ tone: "good", text: <><strong>A broad catalogue.</strong> Different products at different prices naturally separate your orders.</> });
  if (ordersPerDay >= 80 && variety <= 1) reasons.push({ tone: "challenge", text: <><strong>High volume, narrow prices.</strong> Lots of similarly-priced orders land in each hour, so they compete to match the same Meta conversion.</> });
  else if (ordersPerDay <= 30 && variety >= 2) reasons.push({ tone: "good", text: <><strong>Steady, spread-out flow.</strong> Orders arrive across the day rather than all at once.</> });
  if (saleNow) reasons.push({ tone: "challenge", text: <><strong>You&apos;re mid-sale.</strong> Spikes crowd every hour - today&apos;s score may read lower than normal trading. Re-run after the sale.</> });

  if (reasons.length === 0) {
    reasons.push({ tone: confidence >= 60 ? "good" : "challenge",
      text: <>This is roughly how distinguishable your orders look to our matcher, based on your volume and price spread.</> });
  }
  return { verdict, confidence, reasons: reasons.slice(0, 3) };
}

// Tick-scale row aligned to the slider thumb. A Polaris thumb is 16px, so its
// centre travels from 8px to (width - 8px) - i.e. within the track inset, not
// the full container width. We position each tick at the real thumb-centre with
// calc(f * (100% - 16px) + 8px), centring labels on their dot (edge ticks anchor
// inward so they don't clip). activeIndex highlights one tick (variety scale).
const THUMB_PX = 16;
function ScaleTicks({ ticks, min, max, activeIndex }: {
  ticks: Array<{ v: number; label: string }>; min: number; max: number; activeIndex?: number;
}) {
  return (
    <div style={{ position: "relative", height: 14, marginTop: 6 }}>
      {ticks.map((t, i) => {
        const f = (t.v - min) / (max - min);
        const tx = i === 0 ? "0" : i === ticks.length - 1 ? "-100%" : "-50%";
        const active = activeIndex === i;
        return (
          <span key={i} style={{
            position: "absolute",
            left: `calc(${f} * (100% - ${THUMB_PX}px) + ${THUMB_PX / 2}px)`,
            transform: `translateX(${tx})`,
            fontSize: 11, whiteSpace: "nowrap",
            fontWeight: active ? 700 : 400,
            color: active ? PURPLE : "#8C9196",
          }}>{t.label}</span>
        );
      })}
    </div>
  );
}

function SliderTile({ label, helper, value, min, max, step = 1, onChange, display, scale }: {
  label: string; helper: string; value: number; min: number; max: number;
  step?: number; onChange: (v: number) => void; display: React.ReactNode; scale?: React.ReactNode;
}) {
  return (
    <Box padding="400" background="bg-surface-secondary" borderRadius="300" borderColor="border" borderWidth="025">
      <BlockStack gap="100">
        <InlineStack align="space-between" blockAlign="center">
          <Text as="span" variant="headingSm">{label}</Text>
          <span style={{ color: PURPLE, fontWeight: 700, fontSize: 16 }}>{display}</span>
        </InlineStack>
        <Text as="p" variant="bodySm" tone="subdued">{helper}</Text>
        <Box paddingBlockStart="200">
          <RangeSlider
            label={label} labelHidden value={value} min={min} max={max} step={step}
            onChange={(v: number | [number, number]) => onChange(Array.isArray(v) ? v[0] : v)}
          />
        </Box>
        {scale}
      </BlockStack>
    </Box>
  );
}

function SegToggle({ value, onChange }: { value: boolean; onChange: (v: boolean) => void }) {
  const opt = (active: boolean, label: string, v: boolean) => (
    <button type="button" onClick={() => onChange(v)} style={{
      flex: 1, padding: "10px 0", border: "none", cursor: "pointer",
      background: active ? `linear-gradient(90deg, ${PURPLE}, ${PURPLE_LIGHT})` : "transparent",
      color: active ? "#fff" : "#6B7280", fontWeight: 600, fontSize: 14,
      transition: "all 0.2s ease",
    }}>{label}</button>
  );
  return (
    <div style={{ display: "flex", border: "1px solid #E3E3E3", borderRadius: 8, overflow: "hidden" }}>
      {opt(!value, "No", false)}
      {opt(value, "Yes", true)}
    </div>
  );
}

type Step = "quiz" | "importing" | "running" | "result";

export default function FitDemo() {
  const { data } = useLoaderData<typeof loader>() as any;
  const [step, setStep] = useState<Step>("quiz");
  const [imported, setImported] = useState(0);

  // Quick Q&A state (primes expectations - never gates).
  const [ordersPerDay, setOrdersPerDay] = useState(20);
  const [products, setProducts] = useState(50);
  const [variety, setVariety] = useState(2); // 0..3 index into VARIETY_LABELS
  const [saleNow, setSaleNow] = useState(false);
  const [showVerdict, setShowVerdict] = useState(false);

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

  // ─── Quick Q&A (primes expectations before the real test) ─────────────
  if (step === "quiz") {
    const verdict = quickVerdict({ ordersPerDay, products, variety, saleNow });
    return (
      <Page>
        <Box paddingBlockEnd="600">
          <Card>
            <Box padding="600">
              <BlockStack gap="500">
                <BlockStack gap="200">
                  <GradientPill>Welcome to Lucidly</GradientPill>
                  <Text as="h1" variant="heading2xl">Will Lucidly work for your store?</Text>
                  <Text as="p" variant="bodyLg" tone="subdued">
                    Lucidly reveals which customers came from Meta ads by matching conversions
                    to your orders on <strong>time</strong> and <strong>value</strong>. Answer
                    three quick questions for an instant read - then we&apos;ll check your{" "}
                    <strong>real</strong> order history to confirm.
                  </Text>
                </BlockStack>

                <BlockStack gap="300">
                  <SliderTile
                    label="Orders per day" helper="Roughly how many orders do you take on a normal day?"
                    value={ordersPerDay} min={0} max={100} step={5}
                    onChange={(v) => { setOrdersPerDay(v); setShowVerdict(false); }}
                    display={ordersPerDay >= 100 ? "100+" : ordersPerDay}
                    scale={<ScaleTicks min={0} max={100} ticks={[
                      { v: 0, label: "0" }, { v: 10, label: "10" }, { v: 20, label: "20" },
                      { v: 30, label: "30" }, { v: 40, label: "40" }, { v: 50, label: "50" },
                      { v: 60, label: "60" }, { v: 70, label: "70" }, { v: 80, label: "80" },
                      { v: 90, label: "90" }, { v: 100, label: "100+" },
                    ]} />}
                  />
                  <SliderTile
                    label="Number of products" helper="How many distinct products do you sell (parent products, not variants)?"
                    value={products} min={0} max={500} step={10}
                    onChange={(v) => { setProducts(v); setShowVerdict(false); }}
                    display={products >= 500 ? "500+" : products}
                    scale={<ScaleTicks min={0} max={500} ticks={[
                      { v: 0, label: "0" }, { v: 100, label: "100" }, { v: 200, label: "200" },
                      { v: 300, label: "300" }, { v: 400, label: "400" }, { v: 500, label: "500+" },
                    ]} />}
                  />
                  <SliderTile
                    label="How much do order values vary?" helper="Do most of your orders have a similar value, or different values?"
                    value={variety} min={0} max={3} step={1}
                    onChange={(v) => { setVariety(v); setShowVerdict(false); }}
                    display={VARIETY_LABELS[variety]}
                    scale={<ScaleTicks min={0} max={3} activeIndex={variety}
                      ticks={VARIETY_LABELS.map((label, i) => ({ v: i, label }))} />}
                  />

                  <Box padding="400" background="bg-surface-secondary" borderRadius="300" borderColor="border" borderWidth="025">
                    <BlockStack gap="200">
                      <Text as="span" variant="headingSm">Running a big sale or single-product promo right now?</Text>
                      <Text as="p" variant="bodySm" tone="subdued">Spikes crowd every hour and can lower today&apos;s score.</Text>
                      <Box paddingBlockStart="100">
                        <SegToggle value={saleNow} onChange={(v) => { setSaleNow(v); setShowVerdict(false); }} />
                      </Box>
                    </BlockStack>
                  </Box>
                </BlockStack>

                {!showVerdict && (
                  <Button variant="primary" size="large" fullWidth onClick={() => setShowVerdict(true)}>
                    See my instant verdict
                  </Button>
                )}

                {showVerdict && (
                  <div style={{
                    padding: 22, borderRadius: 14,
                    background: "linear-gradient(135deg, rgba(124,58,237,0.06), rgba(167,139,250,0.12))",
                    border: "1px solid rgba(124,58,237,0.20)",
                  }}>
                    <BlockStack gap="400">
                      <BlockStack gap="200" inlineAlign="start">
                        <Text as="span" variant="bodySm" tone="subdued">Your instant read</Text>
                        <VerdictBadge verdict={verdict.verdict} score={verdict.confidence} />
                      </BlockStack>
                      <BlockStack gap="200">
                        {verdict.reasons.map((r, i) => (
                          <FeatureBullet key={i} tone={r.tone}>{r.text}</FeatureBullet>
                        ))}
                      </BlockStack>
                      <Text as="p" variant="bodySm" tone="subdued">
                        This is a rough read from what you told us. The real Fit Test checks your{" "}
                        <strong>actual</strong> last-90-days orders - no guessing, no commitment.
                      </Text>
                      <Button variant="primary" size="large" fullWidth onClick={() => setStep("importing")}>
                        Run the Fit Test on my real orders
                      </Button>
                    </BlockStack>
                  </div>
                )}
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
