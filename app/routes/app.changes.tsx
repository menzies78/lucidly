import { json } from "@remix-run/node";
import { useLoaderData, useSubmit, useRevalidator } from "@remix-run/react";
import { Page, Card, Text, BlockStack, InlineStack, Button, Banner, Spinner } from "@shopify/polaris";
import { useEffect, useMemo, useRef, useState } from "react";
import type { ColumnDef } from "@tanstack/react-table";

import ReportTabs from "../components/ReportTabs";
import InteractiveTable from "../components/InteractiveTable";
import SummaryTile from "../components/SummaryTile";
import ChangesAnnotationStrip from "../components/ChangesAnnotationStrip";
import EntityTimelineDrawer, { type EntityRef } from "../components/EntityTimelineDrawer";
import { authenticate } from "../shopify.server";
import db from "../db.server";
import { parseDateRange } from "../utils/dateRange.server";
import { shopLocalDayKey } from "../utils/shopTime.server";
import { currencySymbolFromCode } from "../utils/currency";
import { setProgress, failProgress, completeProgress } from "../services/progress.server";

const CATEGORY_META: Record<string, { label: string; icon: string; color: string }> = {
  launched:     { label: "Launched",     icon: "🚀", color: "#059669" },
  killed:       { label: "Killed",       icon: "🗑",  color: "#B91C1C" },
  paused:       { label: "Paused",       icon: "⏸",  color: "#6B7280" },
  resumed:      { label: "Resumed",      icon: "▶️",  color: "#0E7490" },
  budget:       { label: "Budget",       icon: "💰", color: "#D97706" },
  creative:     { label: "Creative",     icon: "🎨", color: "#7C3AED" },
  targeting:    { label: "Targeting",    icon: "🎯", color: "#2563EB" },
  optimisation: { label: "Optimisation", icon: "⚙️",  color: "#4338CA" },
  schedule:     { label: "Schedule",     icon: "📅", color: "#0891B2" },
  other:        { label: "Other",        icon: "·",  color: "#6B7280" },
};

// Patterns that mark an event as "routine" — billing, run-status tweaks that
// don't reflect operator intent, and anything whose whole content is "event"
// / "spec" noise from Meta's internal pipeline. Filtered out by default;
// toggleable via the "Hide routine events" checkbox. Patterns run against
// the rendered display summary (after prefix stripping + budget rewrite).
const NOISE_PATTERNS: Array<RegExp> = [
  /→ Pending process\b/i,         // any status transition landing in "Pending process"
  /Pending process → Pending review/i,
  /\bcharge\b/i,
  /\bevent\b/i,
  /\bspec\b/i,
];

// Meta's /activities extra_data for budget changes is an object keyed by
// old/new that itself contains the amount in minor units:
//   old: { type: "payment_amount", currency: "USD", old_value: 140000, ... }
//   new: { type: "payment_amount", currency: "USD", new_value: 70000, ... }
// Returns { minorUnits, currency } — currency is the ISO code from Meta.
function extractBudget(v: string | null, side: "old" | "new"): { minorUnits: number | null; currency: string | null } {
  if (v == null || v === "") return { minorUnits: null, currency: null };
  if (/^-?\d+(\.\d+)?$/.test(v)) return { minorUnits: Number(v), currency: null };
  try {
    const parsed = JSON.parse(v);
    if (parsed && typeof parsed === "object") {
      const direct =
        side === "old"
          ? (parsed.old_value ?? parsed.value ?? parsed.amount)
          : (parsed.new_value ?? parsed.value ?? parsed.amount);
      const currency = (typeof parsed.currency === "string" ? parsed.currency : null);
      let minorUnits: number | null = null;
      if (typeof direct === "number") minorUnits = direct;
      else if (typeof direct === "string" && /^-?\d+(\.\d+)?$/.test(direct)) minorUnits = Number(direct);
      return { minorUnits, currency };
    }
  } catch {}
  return { minorUnits: null, currency: null };
}

function symbolFor(currency: string | null): string {
  if (!currency) return "";
  return currencySymbolFromCode(currency);
}

function formatBudgetWithSymbol(minorUnits: number | null, currency: string | null): string {
  if (minorUnits == null) return "—";
  const display = Math.round(minorUnits / 100); // whole-currency, no decimals
  return `${symbolFor(currency)}${display.toLocaleString("en-GB")}`;
}

// Pull the "New → Old" transition if present in the raw summary or the
// stored old/new pair (run-status events). Used for both display and
// recategorising paused vs resumed.
function statusTransition(c: {
  summary: string; oldValue: string | null; newValue: string | null;
}): { from: string | null; to: string | null } {
  // Try to parse "Run status changed: X → Y" first (our classifier output).
  const m = c.summary?.match(/Run status changed:\s*(.+?)\s*→\s*(.+)/i);
  if (m) return { from: m[1].trim(), to: m[2].trim() };
  // Fall back to the raw extra_data old_value/new_value strings.
  const readStatus = (v: string | null): string | null => {
    if (!v) return null;
    if (/^[A-Z _-]+$/.test(v)) return v;
    try {
      const parsed = JSON.parse(v);
      if (typeof parsed === "string") return parsed;
      if (parsed?.old_value || parsed?.new_value) return parsed.old_value ?? parsed.new_value ?? null;
    } catch {}
    return null;
  };
  return { from: readStatus(c.oldValue), to: readStatus(c.newValue) };
}

function titleStatus(s: string | null): string {
  if (!s) return "—";
  return s.replace(/_/g, " ").toLowerCase().replace(/\b\w/g, (ch) => ch.toUpperCase());
}

// Infer the right category from raw event + values. Meta emits a single
// update_*_run_status type for both paused and resumed transitions, so
// we have to inspect the destination state.
function recategorise(
  storedCategory: string,
  c: { summary: string; oldValue: string | null; newValue: string | null; rawEventType?: string | null },
): string {
  if (storedCategory !== "paused" && storedCategory !== "resumed") return storedCategory;
  const t = statusTransition(c);
  const to = (t.to || "").toUpperCase();
  // Strip ACTIVE first because "INACTIVE" contains the substring ACTIVE.
  if (to.includes("INACTIVE")) return "paused";
  if (to.includes("ACTIVE")) return "resumed";
  if (to.includes("PAUSED") || to.includes("ARCHIVED") || to.includes("DELETED")) return "paused";
  if (to.includes("PENDING")) return "paused"; // inflight; treat as paused for chart colour
  return storedCategory;
}

function renderSummary(c: { category: string; summary: string; oldValue: string | null; newValue: string | null }): string {
  let s = c.summary || "";

  if (c.category === "budget") {
    const oldB = extractBudget(c.oldValue, "old");
    const newB = extractBudget(c.newValue, "new");
    // Prefer the newer side's currency if both are present; budgets don't
    // typically change currency mid-flight but the new side is "what now".
    const currency = newB.currency || oldB.currency;
    return `Budget changed: old ${formatBudgetWithSymbol(oldB.minorUnits, currency)} | new ${formatBudgetWithSymbol(newB.minorUnits, currency)}`;
  }

  // Strip "Run status changed: " prefix and render the transition with
  // tidy title-case on each side. E.g. "Active → Pending review".
  if (/^Run status changed:/i.test(s) || c.category === "paused" || c.category === "resumed") {
    const { from, to } = statusTransition({ summary: s, oldValue: c.oldValue, newValue: c.newValue });
    if (from || to) return `${titleStatus(from)} → ${titleStatus(to)}`;
    return s.replace(/^Run status changed:\s*/i, "");
  }

  return s;
}

// Column value the Summary filter sees. Groups volatile summaries (budgets,
// renames) under a single canonical label so the filter dropdown stays
// useable — without grouping, every unique amount/name pair shows up as
// its own filter option.
function filterSummaryKey(r: { category: string; summary: string }): string {
  if (r.category === "budget") return "Budget changed";
  if (r.category === "creative") return "Creative swapped";
  if (/^Renamed\b/i.test(r.summary)) return "Renamed";
  return r.summary;
}

function isNoiseSummary(summary: string): boolean {
  return NOISE_PATTERNS.some((p) => p.test(summary));
}

export const loader = async ({ request }: { request: Request }) => {
  const { session } = await authenticate.admin(request);
  const shopDomain = session.shop;
  const shop = await db.shop.findUnique({ where: { shopDomain } });
  const tz = shop?.shopifyTimezone || "UTC";
  const { fromDate, toDate, fromKey, toKey } = parseDateRange(request, tz);

  const [changes, totalEver] = await Promise.all([
    db.metaChange.findMany({
      where: { shopDomain, eventTime: { gte: fromDate, lte: toDate } },
      orderBy: { eventTime: "desc" },
      take: 2000,
    }),
    db.metaChange.count({ where: { shopDomain } }),
  ]);

  const rows = changes.map((c) => {
    // Fix up miscategorised run-status events (Meta emits one event type for
    // both paused and resumed; the stored category can't always tell which).
    const category = recategorise(c.category, {
      summary: c.summary, oldValue: c.oldValue, newValue: c.newValue,
      rawEventType: c.rawEventType,
    });
    const displaySummary = renderSummary({
      category, summary: c.summary,
      oldValue: c.oldValue, newValue: c.newValue,
    });
    const summaryFilterKey = filterSummaryKey({ category, summary: displaySummary });
    return {
      id: c.id,
      eventTimeISO: c.eventTime.toISOString(),
      category,
      categoryLabel: CATEGORY_META[category]?.label || category,
      objectType: c.objectType,
      objectTypeLabel:
        c.objectType === "campaign" ? "Campaign" :
        c.objectType === "adset" ? "Ad Set" :
        c.objectType === "ad" ? "Ad" : "Account",
      objectName: c.objectName || c.objectId,
      objectId: c.objectId,
      // `summary` carries the *grouped* filter key (e.g. "Budget changed")
      // because the multi-select dropdown reads row[columnId] directly.
      // `summaryDisplay` is the human-readable string the cell renders.
      summary: summaryFilterKey,
      summaryDisplay: displaySummary,
      isNoise: isNoiseSummary(displaySummary),
      actor: c.actorName || c.actorId || "",
      oldValue: c.oldValue || "",
      newValue: c.newValue || "",
      rawEventType: c.rawEventType,
    };
  });

  // Tile maths — count against the signal rows (excluding noise) so the
  // header numbers match what the user sees by default.
  const signalRows = rows.filter((r) => !r.isNoise);
  const byCategory: Record<string, number> = {};
  const daysWithActivity = new Set<string>();
  for (const r of signalRows) {
    byCategory[r.category] = (byCategory[r.category] || 0) + 1;
    daysWithActivity.add(r.eventTimeISO.slice(0, 10));
  }

  return json({
    shopDomain,
    rows,
    noiseCount: rows.length - signalRows.length,
    totalEver,
    fromKey,
    toKey,
    summary: {
      total: signalRows.length,
      launched: byCategory.launched || 0,
      killed: (byCategory.killed || 0) + (byCategory.paused || 0),
      resumed: byCategory.resumed || 0,
      budget: byCategory.budget || 0,
      creative: byCategory.creative || 0,
      daysWithActivity: daysWithActivity.size,
    },
  });
};

export const action = async ({ request }: { request: Request }) => {
  const { session } = await authenticate.admin(request);
  const shopDomain = session.shop;
  const form = await request.formData();
  const action = form.get("action");

  const taskId = `metaChangeBackfill:${shopDomain}`;

  if (action === "importHistory") {
    setProgress(taskId, { status: "running", message: "Starting Meta change history import..." });
    const backfillDays = Math.min(90, Math.max(1, parseInt(String(form.get("days") || "90"), 10)));
    (async () => {
      try {
        const { syncMetaChanges } = await import("../services/metaChangeSync.server.js");
        const result = await syncMetaChanges(shopDomain, { backfillDays, taskKey: taskId });
        completeProgress(taskId, result);
      } catch (err: any) {
        console.error("[ChangesBackfill] failed:", err);
        failProgress(taskId, err);
      }
    })();
    return json({ started: true, task: "metaChangeBackfill" });
  }

  return json({ ok: false });
};

export default function ChangeLog() {
  const { rows, noiseCount, totalEver, summary, fromKey, toKey, shopDomain } = useLoaderData<typeof loader>();
  const submit = useSubmit();
  const revalidator = useRevalidator();
  const [importState, setImportState] = useState<"idle" | "running" | "done" | "error">("idle");
  const [importMessage, setImportMessage] = useState<string>("");
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const [drawerEntity, setDrawerEntity] = useState<EntityRef | null>(null);
  const [hideRoutine, setHideRoutine] = useState(true);

  const visibleRows = useMemo(() => hideRoutine ? rows.filter(r => !r.isNoise) : rows, [rows, hideRoutine]);

  const dayKeyForEvent = (iso: string) => iso.slice(0, 10);
  const annotationChanges = visibleRows.map(r => ({
    id: r.id, eventTimeISO: r.eventTimeISO, category: r.category,
    objectType: r.objectType, objectName: r.objectName, summary: r.summaryDisplay,
    rawEventType: r.rawEventType, actor: r.actor,
  }));
  const openDrawerFor = (row: typeof rows[number]) => {
    if (row.objectType === "campaign" || row.objectType === "adset" || row.objectType === "ad") {
      setDrawerEntity({
        objectType: row.objectType,
        objectId: row.objectId,
        objectName: row.objectName,
      });
    }
  };

  useEffect(() => () => { if (pollRef.current) clearInterval(pollRef.current); }, []);

  const columns = useMemo<ColumnDef<any>[]>(() => [
    {
      accessorKey: "eventTimeISO",
      header: "Time",
      meta: { maxWidth: "110px", description: "When Meta recorded this change" },
      cell: ({ getValue }) => {
        const v = getValue() as string;
        if (!v) return "—";
        const d = new Date(v);
        return (
          <span style={{ whiteSpace: "nowrap", fontVariantNumeric: "tabular-nums" }}>
            {d.toLocaleString("en-GB", { day: "2-digit", month: "short", hour: "2-digit", minute: "2-digit" })}
          </span>
        );
      },
    },
    {
      accessorKey: "categoryLabel",
      header: "Category",
      meta: { maxWidth: "110px", filterType: "multi-select", description: "Normalised category for this change" },
      filterFn: "multiSelect" as any,
      cell: ({ row }) => {
        const cat = row.original.category as string;
        const meta = CATEGORY_META[cat] || CATEGORY_META.other;
        return (
          <span style={{
            display: "inline-flex", alignItems: "center", gap: 6,
            padding: "2px 8px", borderRadius: 12,
            background: meta.color + "22", color: meta.color,
            fontSize: 12, fontWeight: 600, whiteSpace: "nowrap",
          }}>
            <span>{meta.icon}</span>
            <span>{meta.label}</span>
          </span>
        );
      },
    },
    {
      accessorKey: "objectTypeLabel",
      header: "Level",
      meta: { maxWidth: "70px", filterType: "multi-select", description: "Whether the change targeted a campaign, ad set, or single ad" },
      filterFn: "multiSelect" as any,
    },
    {
      accessorKey: "objectName",
      header: "Object",
      meta: { maxWidth: "320px", description: "Name of the campaign/ad set/ad at the time the change was made. Click to open the full timeline." },
      cell: ({ row, getValue }) => {
        const value = getValue() || "—";
        const r = row.original;
        if (r.objectType === "account" || !r.objectId) return value;
        return (
          <button
            onClick={() => setDrawerEntity({
              objectType: r.objectType,
              objectId: r.objectId,
              objectName: r.objectName,
            })}
            style={{
              background: "transparent", border: "none", padding: 0,
              color: "#2563EB", textDecoration: "underline",
              cursor: "pointer", textAlign: "left", font: "inherit",
            }}
          >{value}</button>
        );
      },
    },
    {
      accessorKey: "summary",
      header: "Summary",
      meta: { maxWidth: "520px", filterType: "multi-select", description: "Human-readable description of the change. Filter groups volatile summaries (e.g. budget amounts, renames) under a single canonical label." },
      filterFn: "multiSelect" as any,
      cell: ({ row }) => row.original.summaryDisplay || "—",
    },
    {
      accessorKey: "actor",
      header: "By",
      meta: { maxWidth: "80px", description: "User who made the change. 'System' = Meta automation" },
      cell: ({ getValue }) => getValue() || "—",
    },
    {
      accessorKey: "oldValue",
      header: "Before",
      meta: { maxWidth: "120px", description: "Previous value before the change (where applicable)" },
      cell: ({ getValue }) => {
        const v = getValue();
        if (!v) return "—";
        return <span style={{ fontFamily: "monospace", fontSize: 11 }}>{String(v)}</span>;
      },
    },
    {
      accessorKey: "newValue",
      header: "After",
      meta: { maxWidth: "120px", description: "New value after the change (where applicable)" },
      cell: ({ getValue }) => {
        const v = getValue();
        if (!v) return "—";
        return <span style={{ fontFamily: "monospace", fontSize: 11 }}>{String(v)}</span>;
      },
    },
    {
      accessorKey: "rawEventType",
      header: "Event",
      meta: { maxWidth: "180px", filterType: "multi-select", description: "Meta's raw event_type — useful when filtering to a specific kind of change" },
      filterFn: "multiSelect" as any,
      cell: ({ getValue }) => <span style={{ fontFamily: "monospace", fontSize: 11 }}>{String(getValue() || "—")}</span>,
    },
  ], []);

  const defaultVisibleColumns = useMemo(() => [
    "eventTimeISO", "categoryLabel", "objectTypeLabel", "objectName", "summary", "actor",
  ], []);

  const pollProgress = () => {
    if (pollRef.current) clearInterval(pollRef.current);
    pollRef.current = setInterval(async () => {
      try {
        const res = await fetch(`/app/api/progress?task=metaChangeBackfill`);
        const data = await res.json();
        const p = data?.progress;
        if (!p) {
          if (pollRef.current) clearInterval(pollRef.current);
          pollRef.current = null;
          setImportState("idle");
          return;
        }
        if (p.status === "running") {
          setImportMessage(p.message || "Importing...");
          if (p.current && p.total) {
            setImportMessage(`Saving ${p.current} / ${p.total}...`);
          }
        } else if (p.status === "complete") {
          setImportState("done");
          setImportMessage(
            `Imported ${p.result?.added ?? 0} new, updated ${p.result?.updated ?? 0}, total ${p.result?.total ?? 0}`,
          );
          if (pollRef.current) clearInterval(pollRef.current);
          pollRef.current = null;
          revalidator.revalidate();
        } else if (p.status === "error") {
          setImportState("error");
          setImportMessage(p.error || "Import failed");
          if (pollRef.current) clearInterval(pollRef.current);
          pollRef.current = null;
        }
      } catch {
        // transient; poll again
      }
    }, 2000);
  };

  const startImport = () => {
    setImportState("running");
    setImportMessage("Starting...");
    const fd = new FormData();
    fd.set("action", "importHistory");
    fd.set("days", "90");
    submit(fd, { method: "post", replace: false });
    pollProgress();
  };

  return (
    <Page title="Change Log" fullWidth>
      <ReportTabs>
        <BlockStack gap="400">
          <Card>
            <BlockStack gap="300">
              <InlineStack align="space-between" blockAlign="center">
                <BlockStack gap="100">
                  <Text as="h2" variant="headingMd">Meta ad account changes</Text>
                  <Text as="p" variant="bodyMd" tone="subdued">
                    Every create / update / pause / delete event from Meta's activity log, normalised into categories.
                    Use the date selector to narrow the range.
                  </Text>
                </BlockStack>
                <InlineStack gap="200" blockAlign="center">
                  {importState === "running" && (
                    <InlineStack gap="200" blockAlign="center">
                      <Spinner size="small" />
                      <Text as="span" variant="bodyMd" tone="subdued">{importMessage}</Text>
                    </InlineStack>
                  )}
                  <Button
                    onClick={startImport}
                    disabled={importState === "running"}
                    variant={totalEver === 0 ? "primary" : "secondary"}
                  >
                    {totalEver === 0 ? "Import Meta change history (90 days)" : "Re-import last 90 days"}
                  </Button>
                </InlineStack>
              </InlineStack>
              {importState === "done" && (
                <Banner tone="success" onDismiss={() => setImportState("idle")}>
                  <Text as="p" variant="bodyMd">{importMessage}</Text>
                </Banner>
              )}
              {importState === "error" && (
                <Banner tone="critical" onDismiss={() => setImportState("idle")}>
                  <Text as="p" variant="bodyMd">Import failed: {importMessage}</Text>
                </Banner>
              )}
            </BlockStack>
          </Card>

          <div style={{ display: "grid", gridTemplateColumns: "repeat(6, 1fr)", gap: 12 }}>
            <SummaryTile label="Changes in period" value={String(summary.total)} />
            <SummaryTile label="Launches" value={String(summary.launched)} />
            <SummaryTile label="Paused / killed" value={String(summary.killed)} />
            <SummaryTile label="Resumed" value={String(summary.resumed)} />
            <SummaryTile label="Budget changes" value={String(summary.budget)} />
            <SummaryTile label="Creative swaps" value={String(summary.creative)} />
            <SummaryTile label="Days with activity" value={String(summary.daysWithActivity)} />
          </div>

          {totalEver === 0 ? (
            <Card>
              <BlockStack gap="200">
                <Text as="h3" variant="headingMd">No change history imported yet</Text>
                <Text as="p" variant="bodyMd" tone="subdued">
                  Click "Import Meta change history" above to pull the last 90 days of ad-account events.
                  After the first import, new events are picked up hourly.
                </Text>
              </BlockStack>
            </Card>
          ) : rows.length === 0 ? (
            <Card>
              <BlockStack gap="200">
                <Text as="h3" variant="headingMd">No changes in this period</Text>
                <Text as="p" variant="bodyMd" tone="subdued">
                  Widen the date range to see earlier events.
                </Text>
              </BlockStack>
            </Card>
          ) : (
            <>
              <Card>
                <BlockStack gap="200">
                  <InlineStack align="space-between" blockAlign="center">
                    <Text as="h3" variant="headingMd">Activity by day</Text>
                    {noiseCount > 0 && (
                      <label style={{ fontSize: 12, color: "#6b7280", cursor: "pointer" }}>
                        <input
                          type="checkbox"
                          checked={hideRoutine}
                          onChange={(e) => setHideRoutine(e.target.checked)}
                          style={{ marginRight: 6, verticalAlign: "middle" }}
                        />
                        Hide routine events ({noiseCount} hidden)
                      </label>
                    )}
                  </InlineStack>
                  <ChangesAnnotationStrip
                    changes={annotationChanges}
                    fromKey={fromKey}
                    toKey={toKey}
                    dayKeyForEvent={dayKeyForEvent}
                    onEventClick={(ev) => {
                      const row = visibleRows.find(r => r.id === ev.id);
                      if (row) openDrawerFor(row);
                    }}
                  />
                </BlockStack>
              </Card>
              <Card padding="0">
                <InteractiveTable
                  columns={columns}
                  data={visibleRows}
                  defaultVisibleColumns={defaultVisibleColumns}
                  tableId="changes-table"
                  initialSorting={[{ id: "eventTimeISO", desc: true }]}
                />
              </Card>
            </>
          )}
        </BlockStack>
      </ReportTabs>
      <EntityTimelineDrawer
        shopDomain={shopDomain}
        open={!!drawerEntity}
        entity={drawerEntity}
        onClose={() => setDrawerEntity(null)}
      />
    </Page>
  );
}
