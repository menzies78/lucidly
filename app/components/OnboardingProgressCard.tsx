// Onboarding progress card. Shown on the dashboard while the phased ingest
// is running (Shop.onboardingPhase === "ingesting"). Polls
// /app/api/ingest-status every 4 seconds and renders a phase-by-phase
// checklist with the live progress message inside the running phase.

import { useEffect, useState } from "react";
import { Card, BlockStack, Text, Spinner, InlineStack } from "@shopify/polaris";
import { useRevalidator } from "@remix-run/react";

type Phase = {
  key: string;
  label: string;
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
  phases: Phase[];
  liveMessage: string | null;
  inFlight: boolean;
};

function PhaseRow({ phase, liveMessage }: { phase: Phase; liveMessage: string | null }) {
  const isRunning = phase.status === "running";
  const isDone = phase.status === "completed";
  const isFailed = phase.status === "failed";

  let icon: React.ReactNode;
  let color: string;
  if (isDone) { icon = <span style={{ fontWeight: 700 }}>{"\u2713"}</span>; color = "#059669"; }
  else if (isFailed) { icon = <span style={{ fontWeight: 700 }}>{"\u2717"}</span>; color = "#DC2626"; }
  else if (isRunning) { icon = <Spinner size="small" />; color = "#7C3AED"; }
  else { icon = <span style={{ opacity: 0.4 }}>{"\u25CB"}</span>; color = "#9CA3AF"; }

  return (
    <div style={{
      display: "flex", alignItems: "flex-start", gap: 12, padding: "10px 14px",
      background: isRunning ? "#F5F3FF" : "transparent",
      borderRadius: 8,
      border: isRunning ? "1px solid #DDD6FE" : "1px solid transparent",
      transition: "background 0.2s",
    }}>
      <div style={{ width: 22, display: "flex", alignItems: "center", justifyContent: "center", color, marginTop: 2 }}>
        {icon}
      </div>
      <div style={{ flex: 1, minWidth: 0 }}>
        <div style={{ fontSize: 14, fontWeight: 600, color: isRunning ? "#5B21B6" : (isDone ? "#065F46" : "#1F2937") }}>
          {phase.label}
          {isDone && phase.rowsWritten ? (
            <span style={{ marginLeft: 8, fontWeight: 400, fontSize: 12, color: "#6B7280" }}>
              {phase.rowsWritten.toLocaleString()} rows
            </span>
          ) : null}
        </div>
        {isRunning && liveMessage && (
          <div style={{ fontSize: 12, color: "#6B7280", marginTop: 4 }}>{liveMessage}</div>
        )}
        {isFailed && phase.errorMessage && (
          <div style={{ fontSize: 12, color: "#991B1B", marginTop: 4 }}>{phase.errorMessage}</div>
        )}
      </div>
    </div>
  );
}

export default function OnboardingProgressCard() {
  const [status, setStatus] = useState<Status | null>(null);
  const { revalidate } = useRevalidator();

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
        // When the ingest just finished, force a dashboard reload so the
        // card disappears and the merchant sees their data.
        if (data.onboardingCompleted && !lastCompleted) {
          lastCompleted = true;
          revalidate();
        }
      } catch {
        // Network blip - try again next tick.
      }
    }

    poll();
    const id = setInterval(poll, 4000);
    return () => { stopped = true; clearInterval(id); };
  }, [revalidate]);

  if (!status) return null;
  if (status.onboardingCompleted) return null;
  // Only show the card while we're in the ingest phase. Earlier phases
  // (shopify/fit/meta) get the "Getting Started" buttons instead.
  if (status.onboardingPhase !== "ingesting") return null;

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

  return (
    <Card>
      <BlockStack gap="400">
        <InlineStack gap="300" blockAlign="center" wrap={false}>
          <Spinner size="small" />
          <BlockStack gap="100">
            <Text as="h2" variant="headingLg">Setting up your dashboard</Text>
            <Text as="p" variant="bodySm" tone="subdued">
              Importing your full Meta history. You can leave this tab — we&apos;ll keep
              working in the background. The dashboard will refresh automatically when ready.
            </Text>
          </BlockStack>
        </InlineStack>

        {/* Overall progress bar */}
        <div>
          <div style={{ display: "flex", justifyContent: "space-between", fontSize: 12, color: "#6B7280", marginBottom: 6 }}>
            <span>{completedCount} of {totalCount} steps complete</span>
            <span>{elapsedStr}</span>
          </div>
          <div style={{ background: "#F3F4F6", borderRadius: 999, overflow: "hidden", height: 8 }}>
            <div style={{
              width: `${overallPct}%`, height: "100%",
              background: "linear-gradient(90deg, #7C3AED, #A78BFA)",
              transition: "width 0.6s ease",
            }} />
          </div>
        </div>

        {/* Phase list */}
        <BlockStack gap="100">
          {status.phases.map(p => (
            <PhaseRow key={p.key} phase={p} liveMessage={status.liveMessage} />
          ))}
        </BlockStack>
      </BlockStack>
    </Card>
  );
}
