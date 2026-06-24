import { useState } from "react";

// Vertical stacked bar chart for daily cohort breakdowns.
//
// X axis = days, Y axis = the chosen metric (customers or revenue). Each day is
// one bar; the series are stacked bottom-to-top in the order supplied (so
// series[0] sits at the base of the bar). Built as a single uniformly-scaled
// SVG so it stays crisp full-width, with an HTML tooltip overlaid on hover.
//
// Deliberately dependency-free (no chart lib) to match the other hand-rolled
// charts on this page (DonutChart / HBarChart).

export interface StackedSeries {
  key: string;
  label: string;
  color: string;
}

interface StackedBarChartProps {
  data: Array<Record<string, any>>; // each row: { date: "YYYY-MM-DD", ...seriesKeys }
  series: StackedSeries[];          // drawn base→top in array order
  formatValue?: (v: number) => string;
}

// Round a positive number up to a "nice" axis bound (1, 2, 5, 10, 20, 50, …).
function niceCeil(v: number): number {
  if (v <= 0) return 1;
  const exp = Math.floor(Math.log10(v));
  const base = Math.pow(10, exp);
  const mantissa = v / base;
  const niceMantissa = mantissa <= 1 ? 1 : mantissa <= 2 ? 2 : mantissa <= 5 ? 5 : 10;
  return niceMantissa * base;
}

function fmtDayLabel(key: string): string {
  const [y, m, d] = key.split("-").map(Number);
  if (!y || !m || !d) return key;
  return new Date(Date.UTC(y, m - 1, d)).toLocaleDateString("en-GB", { day: "numeric", month: "short" });
}

export default function StackedBarChart({ data, series, formatValue }: StackedBarChartProps) {
  const [hover, setHover] = useState<number | null>(null);

  const fmt = formatValue || ((v: number) => Math.round(v).toLocaleString());

  // Coordinate space. Uniformly scaled by the SVG, so text never distorts.
  const W = 960, H = 320;
  const padL = 56, padR = 12, padT = 12, padB = 28;
  const innerW = W - padL - padR;
  const innerH = H - padT - padB;

  const n = data.length;
  const totals = data.map(d => series.reduce((s, ser) => s + (Number(d[ser.key]) || 0), 0));
  const maxTotal = Math.max(0, ...totals);
  const yMax = niceCeil(maxTotal);

  if (n === 0 || maxTotal === 0) {
    return (
      <div style={{ height: 220, display: "flex", alignItems: "center", justifyContent: "center", color: "#9CA3AF", fontSize: 13 }}>
        No customer data for this period
      </div>
    );
  }

  const slot = innerW / n;
  const barW = Math.max(1, slot * 0.8);
  const barGap = (slot - barW) / 2;
  const yOf = (v: number) => padT + innerH * (1 - v / yMax);

  // Y gridlines (5 bands).
  const gridLines = Array.from({ length: 5 }, (_, i) => (yMax / 4) * i);

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
            <text x={padL - 8} y={yOf(g) + 3} textAnchor="end" fontSize={11} fill="#9CA3AF">
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
              {series.map((ser) => {
                const val = Number(d[ser.key]) || 0;
                if (val <= 0) return null;
                const segH = innerH * (val / yMax);
                const yTop = yOf(acc + val);
                acc += val;
                return (
                  <rect key={ser.key} x={x} y={yTop} width={barW} height={segH} fill={ser.color} />
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
            y={H - padB + 16}
            textAnchor="middle"
            fontSize={11}
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
            {series.map((ser) => (
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
    </div>
  );
}
