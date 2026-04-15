import { useEffect, useMemo, useState } from "react";
import { Spinner, Text } from "@shopify/polaris";

// Full-lifecycle timeline for a single Meta entity (campaign / ad set / ad).
// Fetches changes + mini-metric series from /app/api/entity-timeline on open,
// then renders grouped-by-day events next to the scheduled/effective dates
// and status. Triggered by the Changes table, the annotation strip, and the
// Campaigns page entity names.

export interface EntityRef {
  objectType: "campaign" | "adset" | "ad";
  objectId: string;
  objectName: string;
}

interface Props {
  shopDomain: string;
  open: boolean;
  entity: EntityRef | null;
  onClose: () => void;
}

interface TimelinePayload {
  entity: {
    objectType: string;
    objectId: string;
    objectName: string | null;
    currentStatus: string | null;
    scheduledStartAt: string | null;
    scheduledEndAt: string | null;
    effectiveStartAt: string | null;
    effectiveEndAt: string | null;
    createdTime: string | null;
  };
  events: Array<{
    id: string;
    eventTimeISO: string;
    category: string;
    summary: string;
    actor: string | null;
    oldValue: string | null;
    newValue: string | null;
    rawEventType: string;
  }>;
  daily: Array<{ date: string; spend: number; revenue: number; orders: number }>;
}

const CATEGORY_COLOR: Record<string, string> = {
  launched: "#059669", killed: "#B91C1C", paused: "#6B7280", resumed: "#0E7490",
  budget: "#D97706", creative: "#7C3AED", targeting: "#2563EB",
  optimisation: "#4338CA", schedule: "#0891B2", other: "#94A3B8",
};
const CATEGORY_ICON: Record<string, string> = {
  launched: "🚀", killed: "🗑", paused: "⏸", resumed: "▶️",
  budget: "💰", creative: "🎨", targeting: "🎯",
  optimisation: "⚙️", schedule: "📅", other: "·",
};

function fmtDate(iso: string | null) {
  if (!iso) return "—";
  return new Date(iso).toLocaleString("en-GB", {
    day: "2-digit", month: "short", year: "numeric",
    hour: "2-digit", minute: "2-digit",
  });
}
function fmtDay(iso: string) {
  return new Date(iso).toLocaleDateString("en-GB", { day: "2-digit", month: "short", year: "numeric" });
}

export default function EntityTimelineDrawer({ shopDomain, open, entity, onClose }: Props) {
  const [data, setData] = useState<TimelinePayload | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!open || !entity) return;
    setLoading(true);
    setError(null);
    const url = `/app/api/entity-timeline?type=${entity.objectType}&id=${encodeURIComponent(entity.objectId)}`;
    fetch(url)
      .then((r) => r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`)))
      .then((d) => setData(d))
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, [open, entity, shopDomain]);

  // Group events by day key (descending).
  const groupedEvents = useMemo(() => {
    if (!data?.events) return [];
    const map = new Map<string, typeof data.events>();
    for (const ev of data.events) {
      const day = ev.eventTimeISO.slice(0, 10);
      if (!map.has(day)) map.set(day, []);
      map.get(day)!.push(ev);
    }
    return [...map.entries()].sort((a, b) => b[0].localeCompare(a[0]));
  }, [data]);

  if (!open || !entity) return null;

  return (
    <>
      {/* Backdrop */}
      <div
        onClick={onClose}
        style={{
          position: "fixed", inset: 0, background: "rgba(17,24,39,0.25)",
          zIndex: 9998,
        }}
      />
      {/* Drawer */}
      <aside
        style={{
          position: "fixed", top: 0, right: 0, bottom: 0,
          width: "min(520px, 100vw)", background: "#fff",
          boxShadow: "-8px 0 24px rgba(0,0,0,0.12)",
          zIndex: 9999, overflowY: "auto",
          display: "flex", flexDirection: "column",
        }}
      >
        <header style={{ padding: "16px 20px", borderBottom: "1px solid #e5e7eb", display: "flex", justifyContent: "space-between", alignItems: "flex-start", gap: 12 }}>
          <div style={{ flex: 1, minWidth: 0 }}>
            <div style={{ fontSize: 11, textTransform: "uppercase", letterSpacing: 0.5, color: "#6b7280" }}>
              {entity.objectType}
            </div>
            <div style={{ fontWeight: 700, fontSize: 16, marginTop: 2, overflow: "hidden", textOverflow: "ellipsis" }}>
              {entity.objectName || entity.objectId}
            </div>
          </div>
          <button onClick={onClose} aria-label="Close"
            style={{ background: "transparent", border: "none", cursor: "pointer", fontSize: 22, lineHeight: 1, color: "#6b7280" }}>×</button>
        </header>

        {loading && (
          <div style={{ padding: 32, display: "flex", justifyContent: "center" }}><Spinner /></div>
        )}
        {error && (
          <div style={{ padding: 20, color: "#B91C1C" }}>
            <Text as="p" variant="bodyMd">Failed to load timeline: {error}</Text>
          </div>
        )}

        {data && (
          <>
            <section style={{ padding: "14px 20px", borderBottom: "1px solid #e5e7eb", display: "grid", gridTemplateColumns: "auto 1fr", rowGap: 6, columnGap: 12, fontSize: 13 }}>
              <span style={{ color: "#6b7280" }}>Status</span>
              <span style={{ fontWeight: 600 }}>{data.entity.currentStatus || "—"}</span>
              <span style={{ color: "#6b7280" }}>Created</span>
              <span>{fmtDate(data.entity.createdTime)}</span>
              <span style={{ color: "#6b7280" }}>Scheduled start</span>
              <span>{fmtDate(data.entity.scheduledStartAt)}</span>
              <span style={{ color: "#6b7280" }}>Scheduled end</span>
              <span>{fmtDate(data.entity.scheduledEndAt)}</span>
              <span style={{ color: "#6b7280" }}>First delivery</span>
              <span>{fmtDate(data.entity.effectiveStartAt)}</span>
              <span style={{ color: "#6b7280" }}>Last delivery</span>
              <span>{fmtDate(data.entity.effectiveEndAt)}</span>
            </section>

            {data.daily.length > 0 && (
              <section style={{ padding: "14px 20px", borderBottom: "1px solid #e5e7eb" }}>
                <MiniSeries daily={data.daily} events={data.events.map(e => ({ day: e.eventTimeISO.slice(0, 10), category: e.category }))} />
              </section>
            )}

            <section style={{ padding: "14px 20px", flex: 1 }}>
              <Text as="h3" variant="headingMd">Changes</Text>
              {groupedEvents.length === 0 && (
                <Text as="p" variant="bodyMd" tone="subdued">No change log entries for this entity.</Text>
              )}
              {groupedEvents.map(([day, evs]) => (
                <div key={day} style={{ marginTop: 14 }}>
                  <div style={{ fontSize: 11, textTransform: "uppercase", letterSpacing: 0.5, color: "#6b7280" }}>{fmtDay(day + "T12:00:00Z")}</div>
                  <ul style={{ listStyle: "none", padding: 0, margin: "6px 0 0", display: "flex", flexDirection: "column", gap: 8 }}>
                    {evs.map(ev => (
                      <li key={ev.id} style={{ display: "flex", gap: 8 }}>
                        <span style={{
                          display: "inline-flex", alignItems: "center", justifyContent: "center",
                          width: 22, height: 22, borderRadius: "50%",
                          background: (CATEGORY_COLOR[ev.category] || CATEGORY_COLOR.other) + "22",
                          color: CATEGORY_COLOR[ev.category] || CATEGORY_COLOR.other,
                          fontSize: 12, flexShrink: 0,
                        }}>{CATEGORY_ICON[ev.category] || "·"}</span>
                        <div style={{ fontSize: 13, lineHeight: 1.4 }}>
                          <div>{ev.summary}</div>
                          <div style={{ color: "#6b7280", fontSize: 11 }}>
                            {new Date(ev.eventTimeISO).toLocaleTimeString("en-GB", { hour: "2-digit", minute: "2-digit" })}
                            {ev.actor ? ` · ${ev.actor}` : ""}
                            <span style={{ marginLeft: 6, fontFamily: "monospace" }}>{ev.rawEventType}</span>
                          </div>
                        </div>
                      </li>
                    ))}
                  </ul>
                </div>
              ))}
            </section>
          </>
        )}
      </aside>
    </>
  );
}

// Small inline sparkline for spend + annotation markers.
function MiniSeries({ daily, events }: {
  daily: Array<{ date: string; spend: number; revenue: number }>;
  events: Array<{ day: string; category: string }>;
}) {
  const w = 480, h = 70, padX = 4;
  if (!daily.length) return null;
  const maxSpend = Math.max(...daily.map(d => d.spend), 1);
  const stepX = (w - padX * 2) / Math.max(1, daily.length - 1);

  const spendPath = daily.map((d, i) => {
    const x = padX + i * stepX;
    const y = h - 8 - ((d.spend / maxSpend) * (h - 18));
    return `${i === 0 ? "M" : "L"}${x},${y}`;
  }).join(" ");

  const eventsByDay = new Map<string, string[]>();
  for (const e of events) {
    if (!eventsByDay.has(e.day)) eventsByDay.set(e.day, []);
    eventsByDay.get(e.day)!.push(e.category);
  }

  return (
    <svg width={w} height={h} style={{ display: "block" }} preserveAspectRatio="none" viewBox={`0 0 ${w} ${h}`}>
      <path d={spendPath} stroke="#5C6AC4" strokeWidth="1.5" fill="none" />
      {daily.map((d, i) => {
        const cats = eventsByDay.get(d.date);
        if (!cats) return null;
        const x = padX + i * stepX;
        return (
          <g key={d.date}>
            <line x1={x} y1={0} x2={x} y2={h - 2} stroke="#e5e7eb" strokeDasharray="2 3" />
            {cats.slice(0, 3).map((cat, j) => (
              <circle key={j}
                cx={x} cy={2 + j * 5} r={2.2}
                fill={CATEGORY_COLOR[cat] || CATEGORY_COLOR.other} />
            ))}
          </g>
        );
      })}
    </svg>
  );
}
