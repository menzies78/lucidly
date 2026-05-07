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
    productSetId: string | null;
    proxyImageUrl: string | null;
    proxyThumbUrl: string | null;
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
  daily: Array<{ date: string; spend: number; revenue: number; orders: number; newCustomerOrders?: number; newCustomerRevenue?: number; existingCustomerOrders?: number; existingCustomerRevenue?: number }>;
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
  if (!iso) return "-";
  return new Date(iso).toLocaleString("en-GB", {
    day: "2-digit", month: "short", year: "numeric",
    hour: "2-digit", minute: "2-digit",
  });
}
function fmtDay(iso: string) {
  return new Date(iso).toLocaleDateString("en-GB", { day: "2-digit", month: "short", year: "numeric" });
}
function fmtMoney(v: number) {
  if (!v && v !== 0) return "-";
  if (v >= 1000) return `${(v / 1000).toFixed(1)}k`;
  return Math.round(v).toLocaleString();
}

export default function EntityTimelineDrawer({ shopDomain, open, entity, onClose }: Props) {
  const [data, setData] = useState<TimelinePayload | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  // Changes Log starts collapsed - keeps the drawer scannable; expand on demand.
  const [changesOpen, setChangesOpen] = useState(false);

  useEffect(() => {
    if (!open || !entity) return;
    setLoading(true);
    setError(null);
    setChangesOpen(false); // reset collapse when opening a different entity
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

  // Image source for the hero panel. Prefer the full-res proxy URL the API
  // hands back; fall back to the small thumb proxy URL; if the entity is a
  // DPA (productSetId set) and there's no usable creative image, render the
  // branded DPA tile so the drawer always has a visual.
  const isDpa = !!data?.entity.productSetId;
  const heroImage = isDpa
    ? "/dpa-thumbnail.jpg"
    : (data?.entity.proxyImageUrl || data?.entity.proxyThumbUrl || null);

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
          width: "min(560px, 100vw)", background: "#fff",
          boxShadow: "-8px 0 24px rgba(0,0,0,0.12)",
          zIndex: 9999, overflowY: "auto",
          display: "flex", flexDirection: "column",
        }}
      >
        {/* Close button floats top-right above the hero image */}
        <button onClick={onClose} aria-label="Close"
          style={{
            position: "absolute", top: 10, right: 12, zIndex: 10,
            width: 32, height: 32, borderRadius: "50%",
            background: "rgba(255,255,255,0.92)", border: "1px solid #e5e7eb",
            cursor: "pointer", fontSize: 20, lineHeight: 1, color: "#374151",
            display: "inline-flex", alignItems: "center", justifyContent: "center",
            boxShadow: "0 2px 6px rgba(0,0,0,0.08)",
          }}>×</button>

        {/* Hero image - only for ads (campaigns/ad sets have no per-entity creative). */}
        {entity.objectType === "ad" && heroImage && (
          <div style={{
            width: "100%", aspectRatio: "1 / 1",
            background: "#F3F4F6",
            borderBottom: "1px solid #e5e7eb",
            position: "relative", overflow: "hidden",
          }}>
            <img
              src={heroImage}
              alt={entity.objectName}
              style={{ width: "100%", height: "100%", objectFit: "cover" }}
            />
          </div>
        )}

        <header style={{
          padding: "14px 20px", borderBottom: "1px solid #e5e7eb",
          display: "flex", flexDirection: "column", gap: 2,
        }}>
          <div style={{ fontSize: 11, textTransform: "uppercase", letterSpacing: 0.5, color: "#6b7280" }}>
            {entity.objectType}
          </div>
          <div style={{ fontWeight: 700, fontSize: 16, overflow: "hidden", textOverflow: "ellipsis" }}>
            {entity.objectName || entity.objectId}
          </div>
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
              <span style={{ fontWeight: 600 }}>{data.entity.currentStatus || "-"}</span>
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
              <section style={{ padding: "16px 20px", borderBottom: "1px solid #e5e7eb" }}>
                <MiniSeries daily={data.daily} events={data.events.map(e => ({ day: e.eventTimeISO.slice(0, 10), category: e.category }))} />
              </section>
            )}

            <section style={{ padding: "14px 20px", flex: 1 }}>
              <button
                type="button"
                onClick={() => setChangesOpen((v) => !v)}
                aria-expanded={changesOpen}
                style={{
                  display: "flex", alignItems: "center", gap: 8, width: "100%",
                  background: "transparent", border: "none", padding: 0, cursor: "pointer",
                  textAlign: "left",
                }}
              >
                <span style={{
                  display: "inline-block", width: 12, transition: "transform 0.15s ease",
                  transform: changesOpen ? "rotate(90deg)" : "rotate(0deg)",
                  color: "#6b7280", fontSize: 12, lineHeight: 1,
                }}>▶</span>
                <Text as="h3" variant="headingMd">Changes Log</Text>
                <span style={{ color: "#6b7280", fontSize: 12 }}>
                  {groupedEvents.length === 0 ? "(none)" : `(${data.events.length})`}
                </span>
              </button>
              {changesOpen && (
                <div style={{ marginTop: 8 }}>
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
                </div>
              )}
            </section>
          </>
        )}
      </aside>
    </>
  );
}

// Daily Spend & Revenue chart for the side drawer.
//
// Renders two stacked lines (spend, revenue) over the last 90 days with:
//   - section title + subtitle (so the user knows what they're looking at)
//   - left/right Y-axis labels (spend on the left, revenue on the right)
//   - hover crosshair + tooltip showing the date and both values
//   - vertical event markers in a dedicated strip above the chart so the
//     change-log dots don't sit on top of the data lines
function MiniSeries({ daily, events }: {
  daily: Array<{ date: string; spend: number; revenue: number }>;
  events: Array<{ day: string; category: string }>;
}) {
  const w = 520, padX = 36, padR = 36, padTop = 28, padBottom = 22, h = 160;
  const chartTop = padTop, chartBottom = h - padBottom;
  const chartH = chartBottom - chartTop;
  const [hoverIdx, setHoverIdx] = useState<number | null>(null);

  if (!daily.length) return null;

  const maxSpend = Math.max(...daily.map(d => d.spend), 1);
  const maxRev = Math.max(...daily.map(d => d.revenue), 1);
  const stepX = (w - padX - padR) / Math.max(1, daily.length - 1);

  const eventsByDay = new Map<string, string[]>();
  for (const e of events) {
    if (!eventsByDay.has(e.day)) eventsByDay.set(e.day, []);
    eventsByDay.get(e.day)!.push(e.category);
  }

  const toX = (i: number) => padX + i * stepX;
  const ySpend = (v: number) => chartBottom - (v / maxSpend) * chartH;
  const yRev = (v: number) => chartBottom - (v / maxRev) * chartH;

  const spendPath = daily.map((d, i) => `${i === 0 ? "M" : "L"}${toX(i)},${ySpend(d.spend)}`).join(" ");
  const revPath = daily.map((d, i) => `${i === 0 ? "M" : "L"}${toX(i)},${yRev(d.revenue)}`).join(" ");

  const totalSpend = daily.reduce((s, d) => s + d.spend, 0);
  const totalRev = daily.reduce((s, d) => s + d.revenue, 0);

  // First / mid / last date label for the X-axis (avoids visual clutter).
  const labelIdx = [0, Math.floor((daily.length - 1) / 2), daily.length - 1];
  function fmtAxisDay(iso: string) {
    return new Date(iso + "T12:00:00Z").toLocaleDateString("en-GB", { day: "2-digit", month: "short" });
  }

  const onMouseMove = (e: React.MouseEvent<SVGSVGElement>) => {
    const rect = e.currentTarget.getBoundingClientRect();
    const px = ((e.clientX - rect.left) / rect.width) * w;
    const i = Math.round((px - padX) / stepX);
    if (i >= 0 && i < daily.length) setHoverIdx(i);
    else setHoverIdx(null);
  };

  const hover = hoverIdx != null ? daily[hoverIdx] : null;
  const hoverEvents = hover ? eventsByDay.get(hover.date) : null;

  return (
    <div>
      {/* Title + 90-day totals legend */}
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", marginBottom: 4 }}>
        <Text as="h3" variant="headingSm">Daily Spend &amp; Revenue</Text>
        <span style={{ fontSize: 11, color: "#6b7280" }}>Last 90 days</span>
      </div>
      <div style={{ display: "flex", gap: 16, marginBottom: 8, fontSize: 12 }}>
        <span style={{ display: "inline-flex", alignItems: "center", gap: 6, color: "#6b7280" }}>
          <span style={{ display: "inline-block", width: 10, height: 2, background: "#5C6AC4" }} />
          Spend <strong style={{ color: "#111827" }}>{fmtMoney(totalSpend)}</strong>
        </span>
        <span style={{ display: "inline-flex", alignItems: "center", gap: 6, color: "#6b7280" }}>
          <span style={{ display: "inline-block", width: 10, height: 2, background: "#059669" }} />
          Revenue <strong style={{ color: "#111827" }}>{fmtMoney(totalRev)}</strong>
        </span>
      </div>

      <svg
        width="100%"
        height={h}
        viewBox={`0 0 ${w} ${h}`}
        preserveAspectRatio="none"
        style={{ display: "block", cursor: "crosshair" }}
        onMouseMove={onMouseMove}
        onMouseLeave={() => setHoverIdx(null)}
      >
        {/* Y-axis grid lines (3 horizontal rules) */}
        {[0.25, 0.5, 0.75].map((p, i) => {
          const y = chartTop + chartH * (1 - p);
          return <line key={i} x1={padX} y1={y} x2={w - padR} y2={y} stroke="#f1f5f9" strokeWidth="1" />;
        })}
        {/* Y-axis labels - left = spend max, right = revenue max */}
        <text x={padX - 6} y={chartTop + 4} fontSize="10" textAnchor="end" fill="#6b7280">{fmtMoney(maxSpend)}</text>
        <text x={padX - 6} y={chartBottom + 4} fontSize="10" textAnchor="end" fill="#6b7280">0</text>
        <text x={w - padR + 6} y={chartTop + 4} fontSize="10" textAnchor="start" fill="#6b7280">{fmtMoney(maxRev)}</text>
        <text x={w - padR + 6} y={chartBottom + 4} fontSize="10" textAnchor="start" fill="#6b7280">0</text>

        {/* X-axis baseline */}
        <line x1={padX} y1={chartBottom} x2={w - padR} y2={chartBottom} stroke="#e5e7eb" strokeWidth="1" />
        {/* X-axis labels */}
        {labelIdx.map(i => (
          <text key={i} x={toX(i)} y={h - 6} fontSize="10" textAnchor="middle" fill="#6b7280">
            {fmtAxisDay(daily[i].date)}
          </text>
        ))}

        {/* Event markers - vertical dotted lines above the chart in the
            event strip (rows 4-22 of the padTop) so they read as separate
            from the data lines. */}
        {daily.map((d, i) => {
          const cats = eventsByDay.get(d.date);
          if (!cats) return null;
          const x = toX(i);
          return (
            <g key={`ev-${d.date}`}>
              <line x1={x} y1={4} x2={x} y2={chartBottom} stroke="#e5e7eb" strokeDasharray="2 3" strokeWidth="1" />
              {cats.slice(0, 3).map((cat, j) => (
                <circle key={j}
                  cx={x} cy={6 + j * 6} r={2.5}
                  fill={CATEGORY_COLOR[cat] || CATEGORY_COLOR.other} />
              ))}
            </g>
          );
        })}

        {/* Data lines */}
        <path d={spendPath} stroke="#5C6AC4" strokeWidth="1.8" fill="none" />
        <path d={revPath} stroke="#059669" strokeWidth="1.8" fill="none" />

        {/* Hover crosshair + dots */}
        {hover && hoverIdx != null && (
          <g>
            <line x1={toX(hoverIdx)} y1={chartTop} x2={toX(hoverIdx)} y2={chartBottom} stroke="#6b7280" strokeWidth="1" strokeDasharray="2 2" />
            <circle cx={toX(hoverIdx)} cy={ySpend(hover.spend)} r={3.5} fill="#5C6AC4" stroke="#fff" strokeWidth="1.5" />
            <circle cx={toX(hoverIdx)} cy={yRev(hover.revenue)} r={3.5} fill="#059669" stroke="#fff" strokeWidth="1.5" />
          </g>
        )}
      </svg>

      {/* Tooltip card under the chart so it never overlaps cursor or clips
          out of the SVG viewport. Fixed slot keeps the layout stable. */}
      <div style={{
        marginTop: 6, minHeight: 38, fontSize: 12, color: "#374151",
        background: "#F9FAFB", border: "1px solid #E5E7EB", borderRadius: 6,
        padding: "6px 10px", display: "flex", alignItems: "center", gap: 14,
      }}>
        {hover ? (
          <>
            <span style={{ fontWeight: 600 }}>{fmtAxisDay(hover.date)}</span>
            <span style={{ color: "#5C6AC4" }}>Spend <strong>{fmtMoney(hover.spend)}</strong></span>
            <span style={{ color: "#059669" }}>Revenue <strong>{fmtMoney(hover.revenue)}</strong></span>
            {hoverEvents && hoverEvents.length > 0 && (
              <span style={{ color: "#6b7280" }}>{hoverEvents.length} change{hoverEvents.length === 1 ? "" : "s"}</span>
            )}
          </>
        ) : (
          <span style={{ color: "#9ca3af" }}>Hover the chart to inspect a day</span>
        )}
      </div>
    </div>
  );
}
