import { json, redirect } from "@remix-run/node";
import { useLoaderData } from "@remix-run/react";
import { Page, Card, Text, BlockStack, InlineStack, Badge, Button } from "@shopify/polaris";
import { authenticate } from "../shopify.server";
import db from "../db.server";
import { readIngestEvents } from "../services/ingestEventLog.server";
import { isInternalShop } from "../utils/access.server";

// Diagnostics page. Internal/dev-facing surface for reviewing what happened
// during an onboarding ingest — full error messages + stack traces, retry
// counts, phase timings. The merchant-facing OnboardingFlow only shows
// sanitised messages; this page is where we look when something needs
// investigating after the fact.
//
// Reads two sources:
//   1. /data/ingest-events.jsonl - structured event log written by every
//      runPhase attempt (start / complete / retry / fail) + governor warnings.
//      Survives container restarts (lives on the Fly volume).
//   2. IngestJob rows - per-phase summary state from the orchestrator. Useful
//      to see the latest status per phase, even for older runs whose JSONL
//      entries have rolled off.

export const loader = async ({ request }: { request: Request }) => {
  const { session } = await authenticate.admin(request);
  const shopDomain = session.shop;

  // Internal-only surface: exposes raw ingest event logs (stack traces, phase
  // names, governor warnings). Merchants don't need this; reviewers shouldn't
  // see it. Gated behind LUCIDLY_INTERNAL_SHOPS so a non-internal merchant
  // landing here by URL-guess gets bounced back to the dashboard.
  if (!isInternalShop(shopDomain)) {
    return redirect("/app");
  }

  const [events, jobs] = await Promise.all([
    readIngestEvents({ shopDomain, limit: 500 }),
    db.ingestJob.findMany({
      where: { shopDomain },
      orderBy: { createdAt: "desc" },
      take: 100,
    }),
  ]);

  return json({ shopDomain, events, jobs });
};

function statusBadge(status: string) {
  if (status === "completed") return <Badge tone="success">Completed</Badge>;
  if (status === "running") return <Badge tone="attention">Running</Badge>;
  if (status === "failed") return <Badge tone="critical">Failed</Badge>;
  return <Badge>{status as string}</Badge>;
}

function fmtTs(iso?: string | null) {
  if (!iso) return "—";
  try {
    return new Date(iso).toISOString().replace("T", " ").slice(0, 19) + "Z";
  } catch {
    return String(iso);
  }
}

function fmtElapsed(startedAt?: string | null, completedAt?: string | null) {
  if (!startedAt) return "—";
  const start = new Date(startedAt).getTime();
  const end = completedAt ? new Date(completedAt).getTime() : Date.now();
  const sec = Math.round((end - start) / 1000);
  if (sec < 60) return `${sec}s`;
  const m = Math.floor(sec / 60);
  const s = sec % 60;
  return `${m}m${s ? ` ${s}s` : ""}`;
}

function eventColor(type: string) {
  if (type === "phase-complete") return "#047857";
  if (type === "phase-failed") return "#991B1B";
  if (type === "phase-retry") return "#92400E";
  if (type === "phase-start") return "#1E40AF";
  return "#374151";
}

export default function DiagnosticsPage() {
  const { shopDomain, events, jobs } = useLoaderData<typeof loader>();

  return (
    <Page
      title="Diagnostics"
      subtitle={`Ingest event log + IngestJob history for ${shopDomain}`}
      primaryAction={{ content: "Refresh", onAction: () => window.location.reload() }}
    >
      <BlockStack gap="400">
        <Card>
          <BlockStack gap="200">
            <Text as="h2" variant="headingMd">Phase status (most recent attempt per phase)</Text>
            {jobs.length === 0 ? (
              <Text as="p" tone="subdued">No IngestJob rows yet.</Text>
            ) : (
              <div style={{ overflowX: "auto" }}>
                <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
                  <thead>
                    <tr style={{ textAlign: "left", borderBottom: "1px solid #E5E7EB" }}>
                      <th style={{ padding: "6px 8px" }}>Phase</th>
                      <th style={{ padding: "6px 8px" }}>Status</th>
                      <th style={{ padding: "6px 8px" }}>Attempts</th>
                      <th style={{ padding: "6px 8px" }}>Rows</th>
                      <th style={{ padding: "6px 8px" }}>Started</th>
                      <th style={{ padding: "6px 8px" }}>Elapsed</th>
                      <th style={{ padding: "6px 8px" }}>Error</th>
                    </tr>
                  </thead>
                  <tbody>
                    {jobs.map((j: any) => (
                      <tr key={j.id} style={{ borderBottom: "1px solid #F3F4F6" }}>
                        <td style={{ padding: "6px 8px", fontFamily: "monospace" }}>{j.phase}</td>
                        <td style={{ padding: "6px 8px" }}>{statusBadge(j.status)}</td>
                        <td style={{ padding: "6px 8px", textAlign: "right" }}>{j.attempts || 1}</td>
                        <td style={{ padding: "6px 8px", textAlign: "right" }}>{(j.rowsWritten || 0).toLocaleString()}</td>
                        <td style={{ padding: "6px 8px", fontFamily: "monospace", fontSize: 11 }}>{fmtTs(j.startedAt)}</td>
                        <td style={{ padding: "6px 8px" }}>{fmtElapsed(j.startedAt, j.completedAt)}</td>
                        <td style={{ padding: "6px 8px", color: "#991B1B", fontFamily: "monospace", fontSize: 11, maxWidth: 400, overflow: "hidden", textOverflow: "ellipsis" }}>
                          {j.errorMessage || ""}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </BlockStack>
        </Card>

        <Card>
          <BlockStack gap="200">
            <InlineStack align="space-between" blockAlign="center">
              <Text as="h2" variant="headingMd">Event log (most recent first, last {events.length})</Text>
              <Text as="span" tone="subdued" variant="bodySm">/data/ingest-events.jsonl</Text>
            </InlineStack>
            {events.length === 0 ? (
              <Text as="p" tone="subdued">No events logged yet. The event log starts populating from the next ingest run after this deploy.</Text>
            ) : (
              <div style={{ maxHeight: 600, overflowY: "auto", fontFamily: "monospace", fontSize: 12 }}>
                {events.map((e: any, i: number) => (
                  <div key={i} style={{ padding: "6px 8px", borderBottom: "1px solid #F3F4F6", color: eventColor(e.type) }}>
                    <div style={{ display: "flex", gap: 12 }}>
                      <span style={{ color: "#6B7280", minWidth: 170 }}>{e.ts}</span>
                      <span style={{ minWidth: 140 }}>{e.phase}</span>
                      <span style={{ fontWeight: 600 }}>{e.type}</span>
                      {e.attempt && <span>attempt {e.attempt}/{e.maxAttempts || "?"}</span>}
                      {typeof e.rowsWritten === "number" && <span>{e.rowsWritten.toLocaleString()} rows</span>}
                      {typeof e.elapsedSec === "number" && <span>{e.elapsedSec}s</span>}
                    </div>
                    {e.message && (
                      <div style={{ marginTop: 2, paddingLeft: 182, whiteSpace: "pre-wrap", wordBreak: "break-word" }}>{e.message}</div>
                    )}
                    {e.stack && (
                      <details style={{ marginTop: 2, paddingLeft: 182 }}>
                        <summary style={{ cursor: "pointer", color: "#6B7280" }}>stack trace</summary>
                        <pre style={{ whiteSpace: "pre-wrap", wordBreak: "break-word", fontSize: 11, color: "#374151" }}>{e.stack}</pre>
                      </details>
                    )}
                  </div>
                ))}
              </div>
            )}
          </BlockStack>
        </Card>
      </BlockStack>
    </Page>
  );
}
