import { useMemo, useRef, useState } from "react";

// Lightweight horizontal strip showing every day in a date range, with a
// coloured dot per Meta change category that fired that day. Hover a day
// → popover lists the individual events. Click a day → opens the timeline
// drawer (optional; caller passes onDayClick).
//
// Render this above the Campaigns tile grid so the merchant can see "what
// was changed" and "what the numbers did" side by side without reading a
// second table.

export interface ChangeEvent {
  id: string;
  eventTimeISO: string;    // full event timestamp (UTC)
  category: string;        // canonical code from CATEGORIES
  objectType: string;
  objectName: string;
  summary: string;
  rawEventType: string;
  actor?: string | null;
}

interface Props {
  changes: ChangeEvent[];
  fromKey: string;          // shop-local YYYY-MM-DD
  toKey: string;            // shop-local YYYY-MM-DD
  dayKeyForEvent: (iso: string) => string; // maps event ISO to a shop-local YYYY-MM-DD
  onDayClick?: (dayKey: string, events: ChangeEvent[]) => void;
  onEventClick?: (event: ChangeEvent) => void;
}

const CATEGORY_COLOR: Record<string, string> = {
  launched:     "#059669",
  killed:       "#B91C1C",
  paused:       "#6B7280",
  resumed:      "#0E7490",
  budget:       "#D97706",
  creative:     "#7C3AED",
  targeting:    "#2563EB",
  optimisation: "#4338CA",
  schedule:     "#0891B2",
  other:        "#94A3B8",
};
const CATEGORY_ICON: Record<string, string> = {
  launched: "🚀", killed: "🗑", paused: "⏸", resumed: "▶️",
  budget: "💰", creative: "🎨", targeting: "🎯",
  optimisation: "⚙️", schedule: "📅", other: "·",
};
const CATEGORY_LABEL: Record<string, string> = {
  launched: "Launched", killed: "Killed", paused: "Paused", resumed: "Resumed",
  budget: "Budget", creative: "Creative", targeting: "Targeting",
  optimisation: "Optimisation", schedule: "Schedule", other: "Other",
};

function enumerateDays(fromKey: string, toKey: string): string[] {
  const [fy, fm, fd] = fromKey.split("-").map(Number);
  const [ty, tm, td] = toKey.split("-").map(Number);
  const out: string[] = [];
  const start = Date.UTC(fy, fm - 1, fd);
  const end = Date.UTC(ty, tm - 1, td);
  for (let t = start; t <= end; t += 86400000) {
    const d = new Date(t);
    out.push(
      `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, "0")}-${String(d.getUTCDate()).padStart(2, "0")}`,
    );
  }
  return out;
}

export default function ChangesAnnotationStrip({
  changes, fromKey, toKey, dayKeyForEvent, onDayClick, onEventClick,
}: Props) {
  const [hoverDay, setHoverDay] = useState<string | null>(null);
  const [hoverAnchor, setHoverAnchor] = useState<{ x: number; y: number } | null>(null);
  const stripRef = useRef<HTMLDivElement>(null);

  const days = useMemo(() => enumerateDays(fromKey, toKey), [fromKey, toKey]);

  // Group events by day + category.
  const byDay = useMemo(() => {
    const map: Record<string, Record<string, ChangeEvent[]>> = {};
    for (const e of changes) {
      const k = dayKeyForEvent(e.eventTimeISO);
      if (!map[k]) map[k] = {};
      if (!map[k][e.category]) map[k][e.category] = [];
      map[k][e.category].push(e);
    }
    return map;
  }, [changes, dayKeyForEvent]);

  const maxDotsShown = 4;
  const hovered = hoverDay ? byDay[hoverDay] : null;
  const hoveredEvents = hovered
    ? Object.values(hovered).flat().sort((a, b) => a.eventTimeISO.localeCompare(b.eventTimeISO))
    : [];

  return (
    <div
      ref={stripRef}
      style={{
        position: "relative",
        display: "grid",
        gridTemplateColumns: `repeat(${days.length}, minmax(10px, 1fr))`,
        gap: 1,
        padding: "6px 0 18px",
        background: "linear-gradient(180deg, #fafbfc 0%, #fff 100%)",
        borderRadius: 6,
        border: "1px solid #e5e7eb",
      }}
    >
      {days.map((day, idx) => {
        const bucket = byDay[day] || {};
        const categories = Object.keys(bucket);
        const total = Object.values(bucket).reduce((s, arr) => s + arr.length, 0);
        const isHovered = hoverDay === day;
        const hasEvents = total > 0;

        return (
          <div
            key={day}
            onMouseEnter={(e) => {
              setHoverDay(day);
              const rect = (e.currentTarget as HTMLDivElement).getBoundingClientRect();
              setHoverAnchor({ x: rect.left + rect.width / 2, y: rect.top });
            }}
            onMouseLeave={() => { setHoverDay(null); setHoverAnchor(null); }}
            onClick={() => hasEvents && onDayClick?.(day, Object.values(bucket).flat())}
            style={{
              position: "relative",
              minHeight: 32,
              cursor: hasEvents ? "pointer" : "default",
              borderLeft: idx === 0 ? "none" : "1px dashed #eef0f3",
              background: isHovered && hasEvents ? "#f3f4f6" : "transparent",
              display: "flex", flexDirection: "column",
              alignItems: "center", justifyContent: "flex-end",
              paddingBottom: 6, gap: 2,
            }}
            title={hasEvents ? `${total} change${total > 1 ? "s" : ""}` : ""}
          >
            {/* Day marker at very bottom */}
            {(idx === 0 || idx === days.length - 1 || day.endsWith("-01")) && (
              <span style={{
                position: "absolute", bottom: 2, left: 0, right: 0, textAlign: "center",
                fontSize: 9, color: "#9ca3af", pointerEvents: "none",
              }}>{day.slice(5)}</span>
            )}
            {/* Category dots, stacked */}
            <div style={{ display: "flex", flexDirection: "column-reverse", gap: 2, alignItems: "center" }}>
              {categories.slice(0, maxDotsShown).map(cat => (
                <div key={cat} style={{
                  width: 8, height: 8, borderRadius: "50%",
                  background: CATEGORY_COLOR[cat] || CATEGORY_COLOR.other,
                  boxShadow: isHovered ? "0 0 0 1.5px rgba(0,0,0,0.15)" : "none",
                  transition: "box-shadow 0.12s ease",
                }} />
              ))}
              {categories.length > maxDotsShown && (
                <span style={{
                  fontSize: 8, fontWeight: 600, color: "#6b7280",
                  background: "#e5e7eb", borderRadius: 6, padding: "0 4px",
                }}>+{categories.length - maxDotsShown}</span>
              )}
            </div>
          </div>
        );
      })}

      {/* Hover popover — floating, portal-free for simplicity */}
      {hoverDay && hoveredEvents.length > 0 && hoverAnchor && (
        <HoverPopover
          x={hoverAnchor.x}
          y={hoverAnchor.y}
          dayKey={hoverDay}
          events={hoveredEvents}
          onEventClick={onEventClick}
        />
      )}
    </div>
  );
}

function HoverPopover({ x, y, dayKey, events, onEventClick }: {
  x: number; y: number; dayKey: string; events: ChangeEvent[];
  onEventClick?: (e: ChangeEvent) => void;
}) {
  return (
    <div style={{
      position: "fixed",
      left: x,
      top: y - 8,
      transform: "translate(-50%, -100%)",
      background: "#111827",
      color: "#fff",
      padding: "8px 10px",
      borderRadius: 6,
      fontSize: 12,
      maxWidth: 320,
      zIndex: 9999,
      boxShadow: "0 10px 24px rgba(0,0,0,0.15)",
      pointerEvents: events.length > 8 ? "auto" : "none",
    }}>
      <div style={{ fontWeight: 700, marginBottom: 6 }}>{dayKey} · {events.length} change{events.length > 1 ? "s" : ""}</div>
      <div style={{ display: "flex", flexDirection: "column", gap: 4, maxHeight: 260, overflowY: "auto" }}>
        {events.map(ev => (
          <button
            key={ev.id}
            onClick={() => onEventClick?.(ev)}
            style={{
              display: "flex", gap: 6, alignItems: "flex-start",
              background: "transparent", border: "none", color: "#e5e7eb",
              textAlign: "left", padding: 0, cursor: onEventClick ? "pointer" : "default",
            }}
          >
            <span>{CATEGORY_ICON[ev.category] || "·"}</span>
            <span style={{ flex: 1 }}>
              <span style={{ opacity: 0.85 }}>{CATEGORY_LABEL[ev.category] || ev.category}</span>
              {" — "}{ev.summary}
              {ev.objectName && <span style={{ opacity: 0.65 }}> · {ev.objectName}</span>}
            </span>
          </button>
        ))}
      </div>
    </div>
  );
}
