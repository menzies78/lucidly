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
import { useFetcher, useRevalidator } from "@remix-run/react";
import FitReport, { type FitReportData } from "./FitReport";

type Phase = {
  key: string;
  label: string;
  track?: "shopify" | "meta" | "final";
  status: "pending" | "running" | "completed" | "failed";
  rowsWritten?: number;
  errorMessage?: string;
  startedAt?: string;
  completedAt?: string;
  live?: {
    current: number | null;
    total: number | null;
    totalIsApproximate?: boolean;
    unitLabel?: string | null;
    detail?: string | null;
    rowsImported?: number | null;
    // Legacy fallback - older payloads use `message`.
    message?: string | null;
  };
};

type MetaGovernorSummary = {
  appUtilPct: number;
  acctUtilPct: number;
  blockedForSec: number;
  worstAccount: string | null;
};

type Status = {
  onboardingPhase: string;
  onboardingStartedAt?: string;
  onboardingCompleted: boolean;
  fitTestScore?: number | null;
  fitTestComputedAt?: string | null;
  fitTestData?: FitReportData | null;
  phases: Phase[];
  liveMessage: string | null;
  livePhaseKey?: string | null;
  fitImportLive?: { current?: number; message?: string } | null;
  metaAuthUrl?: string | null;
  inFlight: boolean;
  metaGovernorSummary?: MetaGovernorSummary;
  // Legacy field name retained on the API but no longer used by the UI -
  // per-phase progress is now driven by phase.live.
  liveMessage?: string | null;
  livePhaseKey?: string | null;
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

// Self-animating bar for phases where we have no real progress signal (the
// demo seed + rollup build is a fire-and-forget background job). Creeps toward
// a ceiling on an easing curve so it always looks alive and never stalls at a
// fixed width, and never reaches 100% until the phase actually completes.
// `expectedSec` sets the pace: ~63% of the way to the ceiling by that mark.
function CreepingProgressBar({ ceiling = 95, expectedSec = 75 }: { ceiling?: number; expectedSec?: number }) {
  const [elapsedMs, setElapsedMs] = useState(0);
  useEffect(() => {
    const start = Date.now();
    const id = setInterval(() => setElapsedMs(Date.now() - start), 200);
    return () => clearInterval(id);
  }, []);
  const pct = ceiling * (1 - Math.exp(-elapsedMs / (expectedSec * 1000)));
  return <ProgressBar pct={pct} />;
}

function formatElapsed(ms: number): string {
  const sec = Math.floor(ms / 1000);
  if (sec < 60) return `${sec}s`;
  const min = Math.floor(sec / 60);
  if (min < 60) return `${min}m ${sec % 60}s`;
  return `${Math.floor(min / 60)}h ${min % 60}m`;
}

function phaseElapsed(phase: Phase): string | null {
  if (!phase.startedAt) return null;
  const start = new Date(phase.startedAt).getTime();
  const end = phase.completedAt ? new Date(phase.completedAt).getTime() : Date.now();
  return formatElapsed(end - start);
}

function MetaGovernorLine({ g }: { g: MetaGovernorSummary }) {
  // Only render when there's something worth showing. Util < 30% with no
  // block is "normal" and shouldn't clutter the row.
  const showUtil = (g.acctUtilPct || 0) >= 30 || (g.appUtilPct || 0) >= 30;
  const showBlock = (g.blockedForSec || 0) > 0;
  if (!showUtil && !showBlock) return null;
  const utilColor = (g.acctUtilPct || g.appUtilPct) >= 75 ? RED
    : (g.acctUtilPct || g.appUtilPct) >= 50 ? "#D97706"
    : TEXT_DIM;
  return (
    <div style={{ fontSize: 11, color: TEXT_DIM, marginTop: 6, fontVariantNumeric: "tabular-nums" }}>
      <span style={{ color: utilColor }}>
        Meta API: account {g.acctUtilPct || 0}% • app {g.appUtilPct || 0}%
      </span>
      {showBlock && (
        <span style={{ color: RED, marginLeft: 8 }}>
          {"\u2022"} paused for {g.blockedForSec}s (rate limit)
        </span>
      )}
    </div>
  );
}

function PhaseRow({ phase, metaGovernor }: { phase: Phase; metaGovernor?: MetaGovernorSummary }) {
  const isRunning = phase.status === "running";
  const isDone = phase.status === "completed";
  const isFailed = phase.status === "failed";

  let icon: React.ReactNode;
  let color: string;
  if (isDone) { icon = <span style={{ fontWeight: 700, fontSize: 14 }}>{"\u2713"}</span>; color = GREEN; }
  else if (isFailed) { icon = <span style={{ fontWeight: 700, fontSize: 14 }}>{"\u2717"}</span>; color = RED; }
  else if (isRunning) { icon = <Spinner size="small" />; color = PURPLE; }
  else { icon = <span style={{ opacity: 0.4 }}>{"\u25CB"}</span>; color = "#9CA3AF"; }

  const live = phase.live;
  const livePct = live && typeof live.current === "number" && typeof live.total === "number" && live.total > 0
    ? Math.round((live.current / live.total) * 100)
    : null;
  const elapsed = phaseElapsed(phase);

  // Client-side ETA: extrapolate remaining time from elapsed + completion %.
  // Only show once we have at least 5% done (any earlier and the rate is too
  // noisy to be useful). Display in human units: seconds < 60, then minutes,
  // then "Xm Ys" once we exceed a minute.
  let etaText: string | null = null;
  if (
    isRunning &&
    phase.startedAt &&
    live &&
    typeof live.current === "number" &&
    typeof live.total === "number" &&
    live.total > 0 &&
    live.current > 0 &&
    livePct !== null &&
    livePct >= 5 &&
    livePct < 100
  ) {
    const elapsedMs = Date.now() - new Date(phase.startedAt).getTime();
    const remainingMs = (elapsedMs / live.current) * (live.total - live.current);
    if (remainingMs > 0 && Number.isFinite(remainingMs)) {
      const sec = Math.round(remainingMs / 1000);
      if (sec < 60) etaText = `~${sec}s remaining`;
      else if (sec < 3600) {
        const m = Math.round(sec / 60);
        etaText = `~${m} min remaining`;
      } else {
        const h = Math.floor(sec / 3600);
        const m = Math.round((sec % 3600) / 60);
        etaText = `~${h}h ${m}m remaining`;
      }
    }
  }

  const detailText = live?.detail || live?.message || null;
  const unitLabel = live?.unitLabel || "rows";

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
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 8 }}>
          <div style={{ fontSize: 14, fontWeight: 600, color: isRunning ? "#5B21B6" : (isDone ? "#065F46" : "#1F2937") }}>
            {phase.label}
            {isDone && phase.rowsWritten ? (
              <span style={{ marginLeft: 8, fontWeight: 400, fontSize: 12, color: TEXT_DIM }}>
                {phase.rowsWritten.toLocaleString()} rows
              </span>
            ) : null}
          </div>
          {elapsed && (isRunning || isDone) && (
            <div style={{ fontSize: 12, color: TEXT_DIM, fontVariantNumeric: "tabular-nums", whiteSpace: "nowrap" }}>
              {elapsed}
            </div>
          )}
        </div>
        {isRunning && live && (live.current !== null || detailText) && (
          <div style={{ marginTop: 8 }}>
            {livePct !== null && (
              <>
                <ProgressBar pct={livePct} />
                <div style={{ display: "flex", justifyContent: "space-between", fontSize: 12, color: TEXT_DIM, marginTop: 4, fontVariantNumeric: "tabular-nums", gap: 8 }}>
                  <span>
                    {live.current !== null && live.total !== null
                      ? `${live.current.toLocaleString()} of ${live.totalIsApproximate ? "~" : ""}${live.total.toLocaleString()}${live.totalIsApproximate ? "+" : ""} ${unitLabel}`
                      : ""}
                  </span>
                  <span style={{ whiteSpace: "nowrap" }}>
                    {etaText || `${livePct}%`}
                  </span>
                </div>
              </>
            )}
            {livePct === null && live.current !== null && (
              <div style={{ fontSize: 12, color: TEXT_DIM, fontVariantNumeric: "tabular-nums" }}>
                {live.current.toLocaleString()} {unitLabel} so far
              </div>
            )}
            {detailText && (
              <div style={{ fontSize: 12, color: TEXT_DIM, marginTop: 6 }}>
                {detailText}
              </div>
            )}
          </div>
        )}
        {isRunning && phase.track === "meta" && metaGovernor && (
          <MetaGovernorLine g={metaGovernor} />
        )}
        {isFailed && (
          // Show a friendly, generic message — the raw Prisma / Meta error
          // (still stored on the IngestJob row) is unhelpful to merchants and
          // alarming when it's a transient socket timeout. Full detail lives
          // in the Diagnostics page for the dev team.
          <div style={{ fontSize: 12, color: "#991B1B", marginTop: 4 }}>
            This step couldn't complete. We'll retry it on the next sync cycle.
          </div>
        )}
      </div>
    </div>
  );
}

export default function OnboardingFlow({ shopDomain }: { shopDomain: string }) {
  const [status, setStatus] = useState<Status | null>(null);
  const { revalidate } = useRevalidator();
  const fetcher = useFetcher();

  // Poll the status endpoint every 3s. Once onboardingCompleted flips we
  // call revalidate() so the parent loader re-reads Shop.onboardingCompleted
  // and swaps the page wrapper from "Welcome to Lucidly" → "Health" with
  // the full dashboard. A short re-poll keeps trying revalidate in case the
  // first call lost the race with the DB write propagating through Prisma
  // (rare, but the cost of a second revalidate is one extra loader run).
  useEffect(() => {
    let stopped = false;
    let lastCompleted = false;
    let reloadTimer: ReturnType<typeof setTimeout> | null = null;

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
          // Belt-and-braces fallback: if the parent loader hasn't re-rendered
          // within 5s (e.g. revalidate raced the orchestrator's final commit
          // and saw stale data), force a hard reload so the merchant doesn't
          // sit on a half-blank screen.
          reloadTimer = setTimeout(() => {
            if (!stopped) window.location.reload();
          }, 5000);
        }
      } catch {
        /* network blip */
      }
    }

    poll();
    const id = setInterval(poll, 3000);
    return () => {
      stopped = true;
      clearInterval(id);
      if (reloadTimer) clearTimeout(reloadTimer);
    };
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

  // Pick the card for the current phase, then wrap every state with the
  // centred Lucidly logo so the brand sits at the top of every onboarding
  // screen (welcome → importing → fit → ingesting → finalising).
  let content: React.ReactNode;

  // ─── State 5: Ingesting (parallel Shopify + Meta) ─────────────────
  if (phase === "ingesting") {
    // Once every visible phase is marked completed the orchestrator is
    // still baking rollups (customer, product, campaign, ad-demographic,
    // pixel calibration) before flipping onboardingCompleted=true. Show a
    // dedicated "finalising" state so the merchant sees forward motion
    // rather than a static 100%/all-ticked checklist.
    const allPhasesDone =
      status.phases.length > 0 &&
      status.phases.every(p => p.status === "completed");
    content = allPhasesDone ? <FinalisingCard /> : <IngestingCard status={status} />;
  } else if (phase === "fit-importing") {
    // ─── State 2: Fit-importing (90d Shopify minimal sync) ──────────
    content = <FitImportingCard live={status.fitImportLive} />;
  } else if (phase === "fit-running") {
    // ─── State 3: Fit-running (Fit Test calculation) ───────────────
    content = <FitRunningCard />;
  } else if (phase === "fit-ready" || (phase === "fit" && fitDone)) {
    // ─── State 4: Fit-ready (score + Connect Meta CTA) ─────────────
    content = <FitReadyCard data={status.fitTestData ?? null} metaAuthUrl={status.metaAuthUrl ?? null} />;
  } else if (phase === "demo-seeding") {
    // ─── Explore with sample data: building the demo store ─────────
    content = <DemoSeedingCard />;
  } else if (phase === "complete") {
    // Phase is "complete" but the parent loader hasn't picked up the flag
    // yet (race between this poll and revalidate completing). Show the
    // finalising card so the merchant sees a friendly transition rather
    // than the fallback Welcome screen.
    content = <FinalisingCard />;
  } else {
    // ─── State 1: Welcome (also the fallback for unknown phases) ────
    content = <WelcomeCard fetcher={fetcher} />;
  }

  return (
    <>
      <OnboardingLogo />
      {content}
    </>
  );
}

// Centred Lucidly wordmark shown at the top of every onboarding screen.
// Medium size (~50px tall) - the wordmark SVG is ~3.37:1, so this reads as
// a clear brand header without dominating the card beneath it.
function OnboardingLogo() {
  return (
    <div style={{ display: "flex", justifyContent: "center", paddingTop: 8, paddingBottom: 24 }}>
      <img
        src="/lucidly-logo-brand.svg"
        alt="Lucidly"
        style={{ height: 50, width: "auto", display: "block" }}
      />
    </div>
  );
}

// ─── Finalising ──────────────────────────────────────────────────────
// Shown for the brief window between "all visible phases done" and
// onboardingCompleted=true (rollup builds + pixel calibration). Also
// shown when phase has already flipped to "complete" but the parent
// loader is mid-revalidate.
function FinalisingCard() {
  return (
    <Box paddingBlockEnd="600">
      <Card>
        <Box padding="600">
          <BlockStack gap="500">
            <InlineStack gap="300" blockAlign="center">
              <Spinner size="small" />
              <Text as="h2" variant="headingLg">Finalising your dashboard</Text>
            </InlineStack>
            <Text as="p" variant="bodyMd" tone="subdued">
              Baking rollup tables, calibrating your Meta pixel, and
              preparing tiles. This usually takes a minute or two.
            </Text>
            <ProgressBar pct={96} />
          </BlockStack>
        </Box>
      </Card>
    </Box>
  );
}

// ─── Demo-seeding ────────────────────────────────────────────────────
// Shown while seedDemoData synthesises 12 months of sample data + builds
// rollups. seedDemoData flips onboardingPhase to "complete" +
// onboardingCompleted when done, so the dashboard takes over automatically.
function DemoSeedingCard() {
  return (
    <Box paddingBlockEnd="600">
      <Card>
        <Box padding="600">
          <BlockStack gap="500">
            <InlineStack gap="300" blockAlign="center">
              <Spinner size="small" />
              <Text as="h2" variant="headingLg">Building your sample store</Text>
            </InlineStack>
            <Text as="p" variant="bodyMd" tone="subdued">
              Loading a year of demo orders, Meta campaigns and customers, then
              computing attribution, lifetime value and benchmarks - exactly as
              Lucidly would for your real store. This usually takes a minute or
              two - you don't need to do anything.
            </Text>
            <CreepingProgressBar />
            <Text as="p" variant="bodySm" tone="subdued">
              Building your dashboard - please keep this tab open.
            </Text>
          </BlockStack>
        </Box>
      </Card>
    </Box>
  );
}

// ─── Welcome card ────────────────────────────────────────────────────
function WelcomeCard({ fetcher }: { fetcher: ReturnType<typeof useFetcher> }) {
  // Two states drive button loading:
  //   1. fetcher.state - true while the POST is in flight
  //   2. hasSubmitted   - flips true on first click and never resets, so the
  //      button stays in the loading style through the 0-3s polling gap
  //      between the action returning 303 and /app/api/ingest-status reporting
  //      phase="fit-importing" (which unmounts this card entirely).
  // Without #2 the button reverts to its idle (black) look the instant the
  // action returns, even though the next screen is still loading - the user
  // can't tell their click registered.
  // Two states drive button loading - see the long-form note that previously
  // lived here: #1 fetcher.state (POST in flight), #2 hasSubmitted (latched on
  // first click so the button keeps its loading style through the 0-3s gap
  // before /app/api/ingest-status reports phase="fit-importing" and unmounts
  // this card). Without #2 the button reverts to idle the instant the action
  // returns, so the merchant can't tell their click registered.
  const [hasSubmitted, setHasSubmitted] = useState(false);
  const [demoSubmitted, setDemoSubmitted] = useState(false);
  const isSubmitting = fetcher.state !== "idle" || hasSubmitted;
  const anySubmitted = hasSubmitted || demoSubmitted;
  return (
    <Box paddingBlockEnd="600">
      <BlockStack gap="400">
        {/* Floating intro pill - the top-line hook before the explainer */}
        <div style={{
          background: "linear-gradient(135deg, #F5F3FF 0%, #FFFFFF 55%)",
          borderRadius: 28,
          border: `1px solid ${PURPLE_BORDER}`,
          boxShadow: "0 10px 34px rgba(124,58,237,0.14)",
          padding: "40px 36px",
        }}>
          <BlockStack gap="300">
            <Text as="h1" variant="heading3xl" alignment="center">Welcome to Lucidly</Text>
            <Text as="p" variant="bodyLg" alignment="center" tone="subdued">
              See which Meta ads bring you real, paying customers - and what
              those customers are worth over time. Clear attribution and lifetime
              value, matched straight to your Shopify orders.
            </Text>
          </BlockStack>
        </div>

        {/* Graphical intro: how it works + who it suits */}
        <Card>
          <Box padding="600">
            <BlockStack gap="500">
              <GradientPill>How it works</GradientPill>
              <BlockStack gap="300">
                <Text as="h1" variant="heading2xl">How does Lucidly work?</Text>
                <Text as="p" variant="bodyLg" tone="subdued">
                  Lucidly matches your Meta conversions to your Shopify orders
                  statistically - comparing the Meta-reported transaction amount
                  and time slot with Shopify orders of the same amount and time
                  slot. So it works best when your orders are distinguishable
                  from one another.
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

        {/* The Fit Test CTA - fires the real begin-fit-test action */}
        <Card>
          <Box padding="600">
            <BlockStack gap="400">
              <BlockStack gap="200">
                <Text as="h2" variant="heading2xl">Will Lucidly work for your store?</Text>
                <Text as="p" variant="bodyLg" tone="subdued">
                  The Fit Test reads your last 90 days of real orders and predicts
                  exactly how accurately Lucidly will match your Meta conversions -
                  no guessing, no commitment.
                </Text>
              </BlockStack>
              <fetcher.Form method="post" onSubmit={() => setHasSubmitted(true)}>
                <input type="hidden" name="action" value="begin-fit-test" />
                {/* Purple gradient CTA - exact treatment from the standalone
                    Fit Test page (FitReport "CONNECT META ADS"): uppercase,
                    fontWeight 700, fontSize 16, letterSpacing 0.5, gradient +
                    shadow. Native <button> because Polaris primary renders
                    black; we need the brand gradient. */}
                <button
                  type="submit"
                  disabled={anySubmitted}
                  style={{
                    display: "flex", alignItems: "center", justifyContent: "center",
                    width: "100%", padding: "16px 24px", borderRadius: 10,
                    color: "#fff", fontWeight: 700, fontSize: 16, letterSpacing: 0.5,
                    border: "none",
                    cursor: anySubmitted ? "default" : "pointer",
                    background: `linear-gradient(90deg, ${PURPLE}, ${PURPLE_LIGHT})`,
                    boxShadow: "0 4px 14px rgba(124,58,237,0.35)",
                    opacity: anySubmitted ? 0.7 : 1,
                  }}
                >
                  {isSubmitting && !demoSubmitted ? "RUNNING THE FIT TEST\u2026" : "RUN THE 10-SECOND FIT TEST"}
                </button>
              </fetcher.Form>
            </BlockStack>
          </Box>
        </Card>

        {/* Secondary path: explore with sample data. Its own bordered "Or
            explore first" card so it reads as a genuine parallel front-door
            option, not trailing fine print. Unconditionally available to
            everyone (and the route a reviewer with no Meta account takes -
            disclosed in submission notes). Installs a fully populated demo
            store the merchant can clear at any time. */}
        <div style={{
          border: `1px solid ${PURPLE_BORDER}`,
          borderRadius: 16,
          background: PURPLE_BG,
          padding: "28px 24px",
        }}>
          <BlockStack gap="300" inlineAlign="center">
            <Text as="h3" variant="headingLg" alignment="center">Or explore first</Text>
            <Text as="p" variant="bodyMd" tone="subdued" alignment="center">
              Loads a fully populated sample store onto your account so you can
              see the whole app working before connecting anything. Clear it and
              switch to your real data anytime.
            </Text>
            <fetcher.Form method="post" onSubmit={() => setDemoSubmitted(true)}>
              <input type="hidden" name="action" value="seed-demo" />
              <Button
                variant="secondary"
                size="large"
                submit
                loading={demoSubmitted}
                disabled={anySubmitted}
              >
                Explore Lucidly with sample data
              </Button>
            </fetcher.Form>
          </BlockStack>
        </div>
      </BlockStack>
    </Box>
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
              Analysing rival density across every hour - checking how many
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
// Renders the shared rich Fit Report (same component the standalone demo
// uses) followed by the onboarding-specific Meta-connect step. We can't use
// FitReport's built-in showConnectCta here because that links to
// /app/meta-connect, which the app.tsx onboarding gate bounces back to /app
// while onboarding is incomplete. So we keep the proven popup OAuth flow.
function FitReadyCard({ data, metaAuthUrl }: {
  data: FitReportData | null; metaAuthUrl: string | null;
}) {
  // fitTestData is read from the same Shop row that flips the phase to
  // fit-ready, so it's normally present - but guard against an early poll
  // racing ahead of the JSON write.
  if (!data) {
    return <FitRunningCard />;
  }

  return (
    <Box paddingBlockEnd="600">
      <BlockStack gap="500">
        <FitReport d={data} />

        <Card>
          <Box padding="600">
            <BlockStack gap="400">
              <GradientPill>Next step</GradientPill>
              <BlockStack gap="200">
                <Text as="h2" variant="heading2xl">Connect your Meta account</Text>
                <Text as="p" variant="bodyMd" tone="subdued">
                  Click the button below to connect your Meta Ads account to
                  Lucidly, and we&apos;ll begin importing up to 2 years of
                  Shopify data alongside 13 months of Meta Ads data (the
                  maximum period of time where Meta provides the granular data
                  we require to match your orders).
                </Text>
                <Text as="p" variant="bodyMd" tone="subdued">
                  This typically takes 4 - 6 hours, but could be more for
                  stores with more data.
                </Text>
                <Text as="p" variant="bodyMd" tone="subdued">
                  You can close this tab and come back later when the import
                  is complete.
                </Text>
              </BlockStack>

              <Button
                variant="primary"
                size="large"
                onClick={() => {
                  if (metaAuthUrl) window.open(metaAuthUrl, "meta_oauth", "width=600,height=700");
                }}
                disabled={!metaAuthUrl}
                fullWidth
              >
                Connect Meta Ads
              </Button>
            </BlockStack>
          </Box>
        </Card>
      </BlockStack>
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
                <GradientPill>Step 2 of 2 - Importing your data</GradientPill>
                <Text as="h1" variant="heading2xl">Building your dashboard</Text>
                <Text as="p" variant="bodyLg" tone="subdued">
                  Importing Shopify orders and Meta Ads data in parallel, then
                  matching them together. You can leave this tab - we&apos;ll
                  email you when it&apos;s ready.
                </Text>
              </BlockStack>

              <Banner tone="info">
                <Text as="p" variant="bodyMd">
                  <strong>{"\u2713"} We&apos;ll email you when this is ready.</strong>{" "}
                  This usually takes 4–6 hours depending on your store size.
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
                  {shopifyPhases.map(p => <PhaseRow key={p.key} phase={p} />)}
                </BlockStack>
              )}
              {metaPhases.length > 0 && (
                <BlockStack gap="200">
                  <Text as="h3" variant="headingSm">Meta Ads import</Text>
                  {metaPhases.map(p => (
                    <PhaseRow key={p.key} phase={p} metaGovernor={status.metaGovernorSummary} />
                  ))}
                </BlockStack>
              )}
              {finalPhases.length > 0 && (
                <BlockStack gap="200">
                  <Text as="h3" variant="headingSm">Attribution</Text>
                  {finalPhases.map(p => <PhaseRow key={p.key} phase={p} />)}
                </BlockStack>
              )}
            </BlockStack>
          </Box>
        </Card>
      </BlockStack>
    </Box>
  );
}
