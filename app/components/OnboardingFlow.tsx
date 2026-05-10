// Onboarding state machine - the ONLY thing the merchant sees on /app until
// onboardingCompleted flips to true. Replaces the old "Getting Started" 5-button
// card and the standalone OnboardingProgressCard.
//
// State flow (driven by /app/api/ingest-status + Shop.onboardingPhase):
//   1. welcome          - explainer card, "Begin Fit Test" CTA
//   2. fit-importing    - 90d minimal Shopify import running
//   3. fit-running      - Fit Test calculation running
//   4. fit-ready        - score shown, "Connect Meta Ads" CTA
//   5. ingesting        - parallel Shopify + Meta full ingest, phase checklist
//   6. complete         - (component returns null; dashboard takes over)
//
// 100% focus design: the dashboard health tabs / date picker / getting-started
// card are hidden behind this flow until step 6.

import { useEffect, useState } from "react";
import {
  Card, BlockStack, Text, Button, Spinner, InlineStack, Banner, Box,
} from "@shopify/polaris";
import { useFetcher, useRevalidator, useNavigate } from "@remix-run/react";

type Phase = {
  key: string;
  label: string;
  track?: "shopify" | "meta" | "final";
  status: "pending" | "running" | "completed" | "failed";
  rowsWritten?: number;
  errorMessage?: string;
  startedAt?: string;
  completedAt?: string;
};

type Status = {
  onboardingPhase: string;
  onboardingStartedAt?: string;
  onboardingCompleted: boolean;
  fitTestScore?: number | null;
  fitTestComputedAt?: string | null;
  phases: Phase[];
  liveMessage: string | null;
  livePhaseKey?: string | null;
  fitImportLive?: { current?: number; message?: string } | null;
  inFlight: boolean;
};

const PURPLE = "#7C3AED";
const PURPLE_LIGHT = "#A78BFA";
const PURPLE_BG = "#F5F3FF";
const PURPLE_BORDER = "#DDD6FE";
const GREEN = "#059669";
const GREEN_BG = "#ECFDF5";
const RED = "#DC2626";
const TEXT_DIM = "#6B7280";

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
        transition: "width 0.6s ease",
        boxShadow: "0 0 12px rgba(124,58,237,0.35)",
      }} />
    </div>
  );
}

function PhaseRow({ phase, liveMessage, isLive }: {
  phase: Phase; liveMessage: string | null; isLive: boolean;
}) {
  const isRunning = phase.status === "running";
  const isDone = phase.status === "completed";
  const isFailed = phase.status === "failed";

  let icon: React.ReactNode;
  let color: string;
  if (isDone) { icon = <span style={{ fontWeight: 700, fontSize: 14 }}>{"\u2713"}</span>; color = GREEN; }
  else if (isFailed) { icon = <span style={{ fontWeight: 700, fontSize: 14 }}>{"\u2717"}</span>; color = RED; }
  else if (isRunning) { icon = <Spinner size="small" />; color = PURPLE; }
  else { icon = <span style={{ opacity: 0.4 }}>{"\u25CB"}</span>; color = "#9CA3AF"; }

  return (
    <div style={{
      display: "flex", alignItems: "flex-start", gap: 12, padding: "12px 14px",
      background: isRunning ? PURPLE_BG : (isDone ? GREEN_BG : "transparent"),
      borderRadius: 8,
      border: isRunning ? `1px solid ${PURPLE_BORDER}` : "1px solid transparent",
      transition: "background 0.2s",
    }}>
      <div style={{ width: 22, display: "flex", alignItems: "center", justifyContent: "center", color, marginTop: 2 }}>
        {icon}
      </div>
      <div style={{ flex: 1, minWidth: 0 }}>
        <div style={{ fontSize: 14, fontWeight: 600, color: isRunning ? "#5B21B6" : (isDone ? "#065F46" : "#1F2937") }}>
          {phase.label}
          {isDone && phase.rowsWritten ? (
            <span style={{ marginLeft: 8, fontWeight: 400, fontSize: 12, color: TEXT_DIM }}>
              {phase.rowsWritten.toLocaleString()} rows
            </span>
          ) : null}
          {phase.track && phase.track !== "final" && (
            <span style={{
              marginLeft: 8, fontWeight: 500, fontSize: 11, color: TEXT_DIM,
              textTransform: "uppercase", letterSpacing: 0.5,
            }}>
              {phase.track}
            </span>
          )}
        </div>
        {isRunning && isLive && liveMessage && (
          <div style={{ fontSize: 12, color: TEXT_DIM, marginTop: 4 }}>{liveMessage}</div>
        )}
        {isFailed && phase.errorMessage && (
          <div style={{ fontSize: 12, color: "#991B1B", marginTop: 4 }}>{phase.errorMessage}</div>
        )}
      </div>
    </div>
  );
}

export default function OnboardingFlow({ shopDomain }: { shopDomain: string }) {
  const [status, setStatus] = useState<Status | null>(null);
  const { revalidate } = useRevalidator();
  const fetcher = useFetcher();
  const navigate = useNavigate();

  // Poll the status endpoint every 3s. Stops once onboardingCompleted flips.
  useEffect(() => {
    let stopped = false;
    let lastCompleted = false;

    async function poll() {
      try {
        const res = await fetch("/app/api/ingest-status");
        if (!res.ok) return;
        const data: Status = await res.json();
        if (stopped) return;
        setStatus(data);
        if (data.onboardingCompleted && !lastCompleted) {
          lastCompleted = true;
          revalidate();
        }
      } catch {
        /* network blip */
      }
    }

    poll();
    const id = setInterval(poll, 3000);
    return () => { stopped = true; clearInterval(id); };
  }, [revalidate]);

  if (!status) {
    return (
      <Card>
        <BlockStack gap="200">
          <InlineStack gap="300" blockAlign="center"><Spinner size="small" /><Text as="span" variant="bodyMd">Loading...</Text></InlineStack>
        </BlockStack>
      </Card>
    );
  }

  if (status.onboardingCompleted) return null;

  const phase = status.onboardingPhase;
  const fitScore = status.fitTestScore;
  const fitDone = fitScore !== null && fitScore !== undefined;

  // ─── State 1: Welcome ──────────────────────────────────────────────
  if (phase === "shopify" || phase === "welcome") {
    return <WelcomeCard fetcher={fetcher} />;
  }

  // ─── State 2: Fit-importing (90d Shopify minimal sync) ────────────
  if (phase === "fit-importing") {
    return <FitImportingCard live={status.fitImportLive} />;
  }

  // ─── State 3: Fit-running (Fit Test calculation) ──────────────────
  if (phase === "fit-running") {
    return <FitRunningCard />;
  }

  // ─── State 4: Fit-ready (score + Connect Meta CTA) ────────────────
  if (phase === "fit-ready" || (phase === "fit" && fitDone)) {
    return <FitReadyCard score={fitScore!} navigate={navigate} />;
  }

  // ─── State 5: Ingesting (parallel Shopify + Meta) ─────────────────
  if (phase === "ingesting") {
    return <IngestingCard status={status} />;
  }

  // Fallback: unknown phase, show welcome to recover.
  return <WelcomeCard fetcher={fetcher} />;
}

// ─── Welcome card ────────────────────────────────────────────────────
function WelcomeCard({ fetcher }: { fetcher: ReturnType<typeof useFetcher> }) {
  const isSubmitting = fetcher.state !== "idle";
  return (
    <Box paddingBlockEnd="600">
      <Card>
        <Box padding="600">
          <BlockStack gap="500">
            <BlockStack gap="200">
              <GradientPill>Welcome to Lucidly</GradientPill>
              <Text as="h1" variant="heading2xl">Start with the Fit Test</Text>
            </BlockStack>

            <Text as="p" variant="bodyLg" tone="subdued">
              Lucidly works by matching your Meta Ads conversions to your Shopify
              orders with statistical certainty — so every campaign metric you
              see is grounded in real revenue.
            </Text>

            <Box
              padding="400"
              background="bg-surface-secondary"
              borderRadius="300"
              borderColor="border"
              borderWidth="025"
            >
              <BlockStack gap="300">
                <Text as="h3" variant="headingMd">What is the Fit Test?</Text>
                <Text as="p" variant="bodyMd" tone="subdued">
                  Before we import months of data, we run a quick check against
                  your last 90 days of Shopify orders. We look at how many orders
                  cluster within the same hour at similar values — the signal
                  Meta uses to attribute conversions.
                </Text>
                <Text as="p" variant="bodyMd" tone="subdued">
                  We&apos;ll calculate a projected matching accuracy score so you
                  know how confidently Lucidly can attribute your ads to revenue
                  before committing to the full import.
                </Text>
                <BlockStack gap="100">
                  <FeatureBullet>Imports just the order timestamps & values — nothing else</FeatureBullet>
                  <FeatureBullet>Takes about 30 seconds for most stores</FeatureBullet>
                  <FeatureBullet>No commitment — see your score before going further</FeatureBullet>
                </BlockStack>
              </BlockStack>
            </Box>

            <fetcher.Form method="post">
              <input type="hidden" name="action" value="begin-fit-test" />
              <Button
                variant="primary"
                size="large"
                submit
                loading={isSubmitting}
                fullWidth
              >
                Begin Fit Test
              </Button>
            </fetcher.Form>
          </BlockStack>
        </Box>
      </Card>
    </Box>
  );
}

function FeatureBullet({ children }: { children: React.ReactNode }) {
  return (
    <InlineStack gap="200" blockAlign="start" wrap={false}>
      <span style={{ color: PURPLE, fontSize: 14, fontWeight: 700, lineHeight: "20px" }}>{"\u2713"}</span>
      <Text as="span" variant="bodyMd">{children}</Text>
    </InlineStack>
  );
}

// ─── Fit-importing ───────────────────────────────────────────────────
function FitImportingCard({ live }: { live?: { current?: number; message?: string } | null }) {
  const current = live?.current || 0;
  const msg = live?.message || "Connecting to Shopify...";
  // Indeterminate-ish: we don't know total upfront. Cap visual at 80% until done.
  const pct = current === 0 ? 8 : Math.min(80, 8 + (current / 1000) * 30);
  return (
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
              <Text as="p" variant="bodyMd" tone="subdued">{msg}</Text>
            </BlockStack>
            <ProgressBar pct={pct} />
            <Text as="p" variant="bodySm" tone="subdued">
              {current.toLocaleString()} orders imported so far
            </Text>
          </BlockStack>
        </Box>
      </Card>
    </Box>
  );
}

// ─── Fit-running ─────────────────────────────────────────────────────
function FitRunningCard() {
  return (
    <Box paddingBlockEnd="600">
      <Card>
        <Box padding="600">
          <BlockStack gap="500">
            <InlineStack gap="300" blockAlign="center">
              <Spinner size="small" />
              <Text as="h2" variant="headingLg">Calculating your Fit Score</Text>
            </InlineStack>
            <Text as="p" variant="bodyMd" tone="subdued">
              Analysing rival density across every hour — checking how many
              orders Meta might confuse with each other.
            </Text>
            <ProgressBar pct={92} />
          </BlockStack>
        </Box>
      </Card>
    </Box>
  );
}

// ─── Fit-ready ───────────────────────────────────────────────────────
function FitReadyCard({ score, navigate }: { score: number; navigate: ReturnType<typeof useNavigate> }) {
  const verdict =
    score >= 85 ? { label: "Excellent", tone: "success", color: GREEN } :
    score >= 70 ? { label: "Good", tone: "success", color: GREEN } :
    score >= 50 ? { label: "Workable", tone: "warning", color: "#D97706" } :
                  { label: "Challenging", tone: "warning", color: RED };

  return (
    <Box paddingBlockEnd="600">
      <Card>
        <Box padding="600">
          <BlockStack gap="500">
            <BlockStack gap="200">
              <GradientPill>Step 1 complete</GradientPill>
              <Text as="h1" variant="heading2xl">Your Fit Score: {score}/100</Text>
              <Text as="p" variant="bodyLg" tone="subdued">
                Projected matching accuracy: <strong style={{ color: verdict.color }}>{verdict.label}</strong>
              </Text>
            </BlockStack>

            <Box padding="400" background="bg-surface-secondary" borderRadius="300">
              <BlockStack gap="200">
                <Text as="h3" variant="headingMd">What&apos;s next</Text>
                <Text as="p" variant="bodyMd" tone="subdued">
                  Connect your Meta Ads account and we&apos;ll import your full
                  Shopify order history alongside 13 months of Meta data — in
                  parallel. Then we match the two together and your dashboard
                  comes alive.
                </Text>
                <Text as="p" variant="bodyMd" tone="subdued">
                  This typically takes 10–30 minutes. You can close this tab —
                  we&apos;ll email you when your dashboard is ready.
                </Text>
              </BlockStack>
            </Box>

            <Button
              variant="primary"
              size="large"
              onClick={() => navigate("/app/meta-connect")}
              fullWidth
            >
              Connect Meta Ads
            </Button>
          </BlockStack>
        </Box>
      </Card>
    </Box>
  );
}

// ─── Ingesting ───────────────────────────────────────────────────────
function IngestingCard({ status }: { status: Status }) {
  const completedCount = status.phases.filter(p => p.status === "completed").length;
  const totalCount = status.phases.length;
  const overallPct = Math.round((completedCount / totalCount) * 100);

  let elapsedStr = "";
  if (status.onboardingStartedAt) {
    const elapsedMs = Date.now() - new Date(status.onboardingStartedAt).getTime();
    const min = Math.floor(elapsedMs / 60000);
    if (min < 60) elapsedStr = `${min}m elapsed`;
    else elapsedStr = `${Math.floor(min / 60)}h ${min % 60}m elapsed`;
  }

  // Group phases by track for visual clarity.
  const shopifyPhases = status.phases.filter(p => p.track === "shopify");
  const metaPhases = status.phases.filter(p => p.track === "meta");
  const finalPhases = status.phases.filter(p => p.track === "final");

  return (
    <Box paddingBlockEnd="600">
      <BlockStack gap="400">
        <Card>
          <Box padding="600">
            <BlockStack gap="500">
              <BlockStack gap="200">
                <GradientPill>Step 2 of 2 — Importing your data</GradientPill>
                <Text as="h1" variant="heading2xl">Building your dashboard</Text>
                <Text as="p" variant="bodyLg" tone="subdued">
                  Importing Shopify orders and Meta Ads data in parallel, then
                  matching them together. You can leave this tab — we&apos;ll
                  email you when it&apos;s ready.
                </Text>
              </BlockStack>

              <Banner tone="info">
                <Text as="p" variant="bodyMd">
                  <strong>{"\u2713"} We&apos;ll email you when this is ready.</strong>{" "}
                  This usually takes 10–30 minutes depending on your store size.
                </Text>
              </Banner>

              <div>
                <div style={{ display: "flex", justifyContent: "space-between", fontSize: 13, color: TEXT_DIM, marginBottom: 6 }}>
                  <span>{completedCount} of {totalCount} steps complete</span>
                  <span>{elapsedStr}</span>
                </div>
                <ProgressBar pct={overallPct} />
              </div>
            </BlockStack>
          </Box>
        </Card>

        <Card>
          <Box padding="500">
            <BlockStack gap="400">
              {shopifyPhases.length > 0 && (
                <BlockStack gap="200">
                  <Text as="h3" variant="headingSm">Shopify import</Text>
                  {shopifyPhases.map(p => (
                    <PhaseRow key={p.key} phase={p} liveMessage={status.liveMessage} isLive={status.livePhaseKey === p.key} />
                  ))}
                </BlockStack>
              )}
              {metaPhases.length > 0 && (
                <BlockStack gap="200">
                  <Text as="h3" variant="headingSm">Meta Ads import</Text>
                  {metaPhases.map(p => (
                    <PhaseRow key={p.key} phase={p} liveMessage={status.liveMessage} isLive={status.livePhaseKey === p.key} />
                  ))}
                </BlockStack>
              )}
              {finalPhases.length > 0 && (
                <BlockStack gap="200">
                  <Text as="h3" variant="headingSm">Attribution</Text>
                  {finalPhases.map(p => (
                    <PhaseRow key={p.key} phase={p} liveMessage={status.liveMessage} isLive={status.livePhaseKey === p.key} />
                  ))}
                </BlockStack>
              )}
            </BlockStack>
          </Box>
        </Card>
      </BlockStack>
    </Box>
  );
}
