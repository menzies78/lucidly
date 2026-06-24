import { useState } from "react";
import { TipButton } from "./TipButton";

// Vertical stacked bar chart for daily cohort breakdowns.
//
// X axis = days, Y axis = the chosen metric (customers or revenue). Each day is
// one bar; the series are stacked bottom-to-top in the order supplied (so
// series[0] sits at the base of the bar). Built as a single uniformly-scaled
// SVG so it stays crisp full-width, with an HTML tooltip overlaid on hover.
//
// The legend is interactive: hovering a key highlights that cohort's bars,
// clicking a key toggles it in/out so the chart can be filtered to just the
// cohorts the merchant cares about. The Y axis is cropped to the (currently
// selected) data max - no power-of-ten overshoot.
//
// Deliberately dependency-free (no chart lib) to match the other hand-rolled
// charts on this page (DonutChart / HBarChart / WeeklyCohortRevenue).

export interface StackedSeries {
  key: string;
  label: string;
  color: string;
  tip?: string; // optional hover explanation shown on the legend key
}

interface StackedBarChartProps {
  data: Array<Record<string, any>>; // each row: { date: "YYYY-MM-DD", ...seriesKeys }
  series: StackedSeries[];          // drawn base→top in array order
  formatValue?: (v: number) => string;
}

function fmtDayLabel(key: string): string {
  const [y, m, d] = key.split("-").map(Number);
  if (!y || !m || !d) return key;
  return new Date(Date.UTC(y, m - 1, d)).toLocaleDateString("en-GB", { day: "numeric", month: "short" });
}

export default function StackedBarChart({ data, series, formatValue }: StackedBarChartProps) {
  const [hover, setHover] = useState<number | null>(null);
  // Labels currently shown. Keyed by label (not key) so the selection survives
  // a Customers↔Revenue toggle, where the data keys change but labels don't.
  const [selected, setSelected] = useState<Set<string>>(() => new Set(series.map(s => s.label)));
  // Legend key being hovered - highlights its bars, dims the rest.
  const [legendHover, setLegendHover] = useState<string | null>(null);

  const fmt = formatValue || ((v: number) => Math.round(v).toLocaleString());

  // Only series the user has left selected are stacked / scaled / totalled.
  const activeSeries = series.filter(s => selected.has(s.label));

  function toggle(label: string) {
    setSelected(prev => {
      const next = new Set(prev);
      if (next.has(label)) next.delete(label);
      else next.add(label);
      return next;
    });
  }

  // Coordinate space. Uniformly scaled by the SVG, so text never distorts.
  const W = 960, H = 300;
  const padL = 52, padR = 12, padT = 10, padB = 26;
  const innerW = W - padL - padR;
  const innerH = H - padT - padB;

  const n = data.length;
  const totals = data.map(d => activeSeries.reduce((s, ser) => s + (Number(d[ser.key]) || 0), 0));
  const maxTotal = Math.max(0, ...totals);
  // Crop the axis just above the tallest bar (no nice-number overshoot).
  const yMax = maxTotal > 0 ? maxTotal : 1;

  const legend = (
    <div style={{ display: "flex", justifyContent: "center", gap: "10px", flexWrap: "wrap", marginTop: 4 }}>
      {series.map((ser) => {
        const isOn = selected.has(ser.label);
        return (
          <TipButton
            key={ser.key}
            tip={ser.tip || ser.label}
            onClick={() => toggle(ser.label)}
            style={{
              display: "inline-flex", alignItems: "center", gap: 7,
              padding: "6px 12px", borderRadius: 8, cursor: "pointer",
              border: isOn ? `1.5px solid ${ser.color}` : "1.5px solid #E5E7EB",
              background: isOn ? `${ser.color}14` : "#fff",
              color: isOn ? "#1F2937" : "#9CA3AF",
              fontSize: 13, fontWeight: 600,
              opacity: isOn ? 1 : 0.65,
              transition: "all 0.12s",
            }}
            onMouseEnter={() => setLegendHover(ser.key)}
            onMouseLeave={() => setLegendHover(null)}
          >
            <span style={{
              width: 12, height: 12, borderRadius: 3, flexShrink: 0,
              background: isOn ? ser.color : "transparent",
              border: isOn ? "none" : `2px solid ${ser.color}`,
            }} />
            {ser.label}
          </TipButton>
        );
      })}
    </div>
  );

  if (n === 0 || activeSeries.length === 0 || maxTotal === 0) {
    return (
      <div>
        <div style={{ height: 220, display: "flex", alignItems: "center", justifyContent: "center", color: "#9CA3AF", fontSize: 13 }}>
          {activeSeries.length === 0 ? "Select a cohort to display" : "No customer data for this period"}
        </div>
        {legend}
      </div>
    );
  }

  const slot = innerW / n;
  const barW = Math.max(1, slot * 0.8);
  const barGap = (slot - barW) / 2;
  const yOf = (v: number) => padT + innerH * (1 - v / yMax);

  // Y gridlines (max, half, 0) - mirrors WeeklyCohortRevenue for a clean,
  // cropped axis without ugly fractional ticks.
  const gridLines = [0, yMax / 2, yMax];

  // X labels: aim for ~8 evenly spaced ticks, always include first + last.
  const tickEvery = Math.max(1, Math.ceil(n / 8));
  const xTicks = data
    .map((d, i) => ({ d, i }))
    .filter(({ i }) => i % tickEvery === 0 || i === n - 1);

  return (
    <div style={{ position: "relative", width: "100%" }}>
      <svg viewBox={`0 0 ${W} ${H}`} width="100%" style={{ display: "block" }}>
        {/* Y gridlines + labels */}
        {gridLines.map((g, i) => (
          <g key={i}>
            <line x1={padL} y1={yOf(g)} x2={W - padR} y2={yOf(g)} stroke="#EEF0F3" strokeWidth={1} />
            <text x={padL - 8} y={yOf(g) + 3} textAnchor="end" fontSize={10} fill="#9CA3AF">
              {fmt(g)}
            </text>
          </g>
        ))}

        {/* Bars (stacked base→top) */}
        {data.map((d, i) => {
          const x = padL + i * slot + barGap;
          let acc = 0;
          const isDim = hover !== null && hover !== i;
          return (
            <g key={i} opacity={isDim ? 0.45 : 1}>
              {activeSeries.map((ser) => {
                const val = Number(d[ser.key]) || 0;
                if (val <= 0) return null;
                const segH = innerH * (val / yMax);
                const yTop = yOf(acc + val);
                acc += val;
                const segDim = legendHover !== null && legendHover !== ser.key;
                return (
                  <rect key={ser.key} x={x} y={yTop} width={barW} height={segH} fill={ser.color} opacity={segDim ? 0.2 : 1} />
                );
              })}
              {/* Invisible full-height hit area for hover */}
              <rect
                x={padL + i * slot} y={padT} width={slot} height={innerH}
                fill="transparent"
                onMouseEnter={() => setHover(i)}
                onMouseLeave={() => setHover(null)}
              />
            </g>
          );
        })}

        {/* X axis baseline */}
        <line x1={padL} y1={yOf(0)} x2={W - padR} y2={yOf(0)} stroke="#D1D5DB" strokeWidth={1} />

        {/* X tick labels */}
        {xTicks.map(({ d, i }) => (
          <text
            key={i}
            x={padL + i * slot + slot / 2}
            y={H - padB + 15}
            textAnchor="middle"
            fontSize={10}
            fill="#9CA3AF"
          >
            {fmtDayLabel(d.date)}
          </text>
        ))}
      </svg>

      {/* Hover tooltip */}
      {hover !== null && (() => {
        const d = data[hover];
        const total = totals[hover];
        const leftPct = ((padL + hover * slot + slot / 2) / W) * 100;
        const flip = leftPct > 60;
        return (
          <div
            style={{
              position: "absolute",
              top: 8,
              left: `${leftPct}%`,
              transform: flip ? "translateX(-100%) translateX(-8px)" : "translateX(8px)",
              background: "#1e1e1e",
              color: "#fff",
              padding: "8px 10px",
              borderRadius: 6,
              fontSize: 11.5,
              lineHeight: 1.5,
              boxShadow: "0 2px 8px rgba(0,0,0,0.25)",
              pointerEvents: "none",
              whiteSpace: "nowrap",
              zIndex: 5,
            }}
          >
            <div style={{ fontWeight: 600, marginBottom: 4 }}>{fmtDayLabel(d.date)}</div>
            {activeSeries.map((ser) => (
              <div key={ser.key} style={{ display: "flex", alignItems: "center", gap: 6 }}>
                <span style={{ width: 9, height: 9, borderRadius: 2, background: ser.color, flexShrink: 0 }} />
                <span style={{ color: "#D1D5DB" }}>{ser.label}:</span>
                <strong>{fmt(Number(d[ser.key]) || 0)}</strong>
              </div>
            ))}
            <div style={{ borderTop: "1px solid #3a3a3a", marginTop: 4, paddingTop: 4, display: "flex", justifyContent: "space-between", gap: 12 }}>
              <span style={{ color: "#D1D5DB" }}>Total</span>
              <strong>{fmt(total)}</strong>
            </div>
          </div>
        );
      })()}

      {legend}
    </div>
  );
}
