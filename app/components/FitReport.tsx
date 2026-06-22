// Shared "Lucidly Fit Report" UI - the rich rendering of a computed Fit Test
// result. Used by the standalone route AND the onboarding demo/flow so they
// render identical markup. Presentational only: pass in a computed fit-data
// object. Visual language matches the pre-test intro screen (purple pill,
// heading2xl titles, app-style stat tiles).

import { useState } from "react";
import { Card, Text, BlockStack, InlineStack, Button, Banner, Box } from "@shopify/polaris";
import { Link } from "@remix-run/react";

const PURPLE = "#7C3AED";
const PURPLE_LIGHT = "#A78BFA";
const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

export type FitReportData = {
  score: number;
  verdict: string;
  verdictReason: string;
  ordersAnalysed: number;
  lookbackDays: number;
  ordersPerDay: number;
  histogramPct: Record<string, number>;
  aov: { mean: number; cv: number; spread: "narrow" | "moderate" | "wide"; currency?: string };
  worstHours?: Array<{ dow: number; hour: number; avgRivals: number; orderCount: number }>;
  hourly?: Array<{ hour: number; avgRivals: number; orderCount: number }>;
  daily?: Array<{ date: string; count: number }>;
  promo?: { start: string; end: string; days: number } | null;
};

function formatHour(h: number) {
  if (h === 0) return "12am";
  if (h === 12) return "12pm";
  return h < 12 ? `${h}am` : `${h - 12}pm`;
}

function shortDate(key: string) {
  const [, m, d] = key.split("-").map(Number);
  return `${MONTHS[(m || 1) - 1]} ${d}`;
}

function currencySymbol(code?: string) {
  const map: Record<string, string> = { GBP: "£", USD: "$", EUR: "€", AUD: "A$", CAD: "C$", NZD: "NZ$" };
  return map[code || "GBP"] || (code ? `${code} ` : "£");
}

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

export function VerdictBadge({ verdict, score, size = "default", fullWidth = false }: {
  verdict: string; score: number; size?: "default" | "large"; fullWidth?: boolean;
}) {
  const palette: Record<string, { bg: string; fg: string; label: string; icon: string }> = {
    excellent: { bg: "#D1FAE5", fg: "#065F46", label: "Excellent fit",  icon: "[OK]" },
    good:      { bg: "#DBEAFE", fg: "#1E40AF", label: "Good fit",       icon: "[+]" },
    passable:  { bg: "#FEF9C3", fg: "#854D0E", label: "Passable fit",   icon: "[~]" },
    below:     { bg: "#FFEDD5", fg: "#9A3412", label: "Below average",  icon: "[!]" },
    poor:      { bg: "#FEE2E2", fg: "#991B1B", label: "Not a good fit", icon: "[X]" },
    marginal:  { bg: "#FEF3C7", fg: "#92400E", label: "Marginal fit",   icon: "[!]" },
    challenging: { bg: "#FEE2E2", fg: "#991B1B", label: "Challenging fit", icon: "[X]" },
  };
  const p = palette[verdict] || palette.marginal;
  const lg = size === "large";
  return (
    <div style={{
      display: fullWidth ? "flex" : "inline-flex",
      width: fullWidth ? "100%" : undefined,
      alignItems: "center", justifyContent: "center", gap: lg ? 16 : 12,
      padding: lg ? "18px 30px" : "10px 18px", borderRadius: fullWidth ? 12 : 999,
      background: p.bg, color: p.fg,
      fontSize: lg ? 22 : 15, fontWeight: 700,
    }}>
      <span style={{ fontFamily: "monospace", fontSize: lg ? 17 : 13 }}>{p.icon}</span>
      <span>{p.label}</span>
      <span style={{ fontSize: lg ? 34 : 22, fontWeight: 800, marginLeft: lg ? 12 : 8 }}>{score}<span style={{ fontSize: lg ? 17 : 13, opacity: 0.7 }}>/100</span></span>
    </div>
  );
}

// App-style stat tile: subdued label pinned top, BIG value vertically centred,
// supporting line pinned bottom - so the three tiles align row-for-row.
function StatTile({ label, value, sub }: { label: string; value: React.ReactNode; sub?: React.ReactNode }) {
  return (
    <Card>
      <div style={{
        height: "100%", minHeight: 150, display: "flex", flexDirection: "column",
        alignItems: "center", textAlign: "center", padding: "4px 6px",
      }}>
        <div style={{ height: 24, display: "flex", alignItems: "center" }}>
          <Text as="p" variant="headingSm" tone="subdued">{label}</Text>
        </div>
        <div style={{ flex: 1, display: "flex", alignItems: "center" }}>
          <Text as="p" variant="heading2xl">{value}</Text>
        </div>
        <div style={{ minHeight: 40, display: "flex", alignItems: "flex-start", maxWidth: 250 }}>
          {sub && <Text as="p" variant="bodySm" tone="subdued">{sub}</Text>}
        </div>
      </div>
    </Card>
  );
}

type Bar = { value: number; color: string; xLabel?: string; tipTitle: string; tipSub: string };

// Interactive bar chart: left y-axis scale, faint gridlines, hover-highlight
// with a floating tooltip. Shared by the rival-per-hour and orders-per-day charts.
function InteractiveBars({ bars, height = 170, yFmt }: {
  bars: Bar[]; height?: number; yFmt: (v: number) => string;
}) {
  const [hover, setHover] = useState<number | null>(null);
  const max = Math.max(0.001, ...bars.map(b => b.value));
  const ticks = [1, 0.75, 0.5, 0.25, 0];
  const gap = bars.length > 40 ? 1 : 4;
  return (
    <div style={{ display: "flex", gap: 8 }}>
      {/* Y axis */}
      <div style={{ width: 36, height, position: "relative", flexShrink: 0 }}>
        {ticks.map((f, i) => (
          <span key={i} style={{
            position: "absolute", right: 0, top: `${(1 - f) * 100}%`,
            transform: i === 0 ? "translateY(0)" : i === ticks.length - 1 ? "translateY(-100%)" : "translateY(-50%)",
            fontSize: 10, color: "#8C9196", lineHeight: 1,
          }}>{yFmt(f * max)}</span>
        ))}
      </div>
      {/* Plot */}
      <div style={{ flex: 1 }}>
        <div style={{ position: "relative", height }}>
          {/* gridlines */}
          {ticks.map((f, i) => (
            <div key={i} style={{
              position: "absolute", left: 0, right: 0, top: `${(1 - f) * 100}%`,
              borderTop: "1px solid #F1F2F4",
            }} />
          ))}
          {/* bars */}
          <div style={{ display: "flex", alignItems: "flex-end", gap, height, position: "relative" }}>
            {bars.map((b, i) => (
              <div
                key={i}
                onMouseEnter={() => setHover(i)}
                onMouseLeave={() => setHover(h => (h === i ? null : h))}
                style={{ flex: 1, height: "100%", display: "flex", alignItems: "flex-end", cursor: "default" }}
              >
                <div style={{
                  width: "100%", height: `${Math.max(1, (b.value / max) * 100)}%`,
                  background: b.color, borderRadius: "3px 3px 0 0",
                  opacity: hover === null || hover === i ? 1 : 0.4,
                  outline: hover === i ? "2px solid rgba(124,58,237,0.5)" : "none",
                  transition: "opacity 0.12s ease",
                }} />
              </div>
            ))}
            {/* tooltip */}
            {hover !== null && (
              <div style={{
                position: "absolute", left: `${((hover + 0.5) / bars.length) * 100}%`,
                bottom: `calc(${(bars[hover].value / max) * 100}% + 8px)`,
                transform: "translateX(-50%)",
                background: "#1e1e1e", color: "#fff", padding: "6px 10px", borderRadius: 6,
                fontSize: 11.5, lineHeight: 1.45, whiteSpace: "nowrap", pointerEvents: "none",
                zIndex: 5, boxShadow: "0 2px 8px rgba(0,0,0,0.25)",
              }}>
                <div style={{ fontWeight: 700 }}>{bars[hover].tipTitle}</div>
                <div style={{ opacity: 0.85 }}>{bars[hover].tipSub}</div>
              </div>
            )}
          </div>
        </div>
        {/* x labels */}
        <div style={{ display: "flex", gap, marginTop: 6 }}>
          {bars.map((b, i) => (
            <div key={i} style={{ flex: 1, textAlign: "center", fontSize: 10, color: "#8C9196", whiteSpace: "nowrap" }}>
              {b.xLabel || ""}
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

function rivalColor(v: number) {
  if (v < 0.5) return "#10B981";
  if (v < 1) return "#34D399";
  if (v < 2) return "#FBBF24";
  if (v < 3) return "#F59E0B";
  return "#EF4444";
}

// Orders-per-day colour ramp: green up to ~30/day, through orange ~65, red at
// 100+. High sustained volume crowds hours and hurts match accuracy.
function volumeColor(count: number) {
  const stops: Array<{ at: number; c: [number, number, number] }> = [
    { at: 30, c: [16, 185, 129] },
    { at: 65, c: [245, 158, 11] },
    { at: 100, c: [239, 68, 68] },
  ];
  if (count <= stops[0].at) return `rgb(${stops[0].c.join(",")})`;
  if (count >= stops[stops.length - 1].at) return `rgb(${stops[stops.length - 1].c.join(",")})`;
  for (let i = 0; i < stops.length - 1; i++) {
    if (count >= stops[i].at && count <= stops[i + 1].at) {
      const t = (count - stops[i].at) / (stops[i + 1].at - stops[i].at);
      const c = stops[i].c.map((v, k) => Math.round(v + (stops[i + 1].c[k] - v) * t));
      return `rgb(${c.join(",")})`;
    }
  }
  return `rgb(${stops[0].c.join(",")})`;
}

function Histogram({ pct }: { pct: Record<string, number> }) {
  const buckets = [
    { key: "0",  label: "Zero rivals", color: "#10B981", desc: "matches uniquely" },
    { key: "1",  label: "1 rival",     color: "#34D399", desc: "50% confidence" },
    { key: "2",  label: "2 rivals",    color: "#FBBF24", desc: "33% confidence" },
    { key: "3",  label: "3 rivals",    color: "#F59E0B", desc: "25% confidence" },
    { key: "4+", label: "4+ rivals",   color: "#EF4444", desc: "<20% confidence" },
  ];
  return (
    <BlockStack gap="300">
      {buckets.map(b => (
        <div key={b.key}>
          <div style={{ display: "flex", justifyContent: "space-between", fontSize: 13, marginBottom: 5 }}>
            <span style={{ fontWeight: 700, color: "#1F2937" }}>{b.label}</span>
            <span style={{ color: "#6B7280" }}>
              <strong style={{ color: "#111827" }}>{pct[b.key] ?? 0}%</strong> of orders <span style={{ marginLeft: 8 }}>· {b.desc}</span>
            </span>
          </div>
          <div style={{ background: "#F3F4F6", borderRadius: 5, overflow: "hidden", height: 18 }}>
            <div style={{
              width: `${pct[b.key] ?? 0}%`, height: "100%",
              background: b.color, transition: "width 0.4s ease",
            }} />
          </div>
        </div>
      ))}
    </BlockStack>
  );
}

export default function FitReport({ d, showConnectCta = false }: {
  d: FitReportData;
  showConnectCta?: boolean;
}) {
  const sym = currencySymbol(d.aov.currency);
  const cvPct = Math.round((d.aov.cv ?? 0) * 100);
  const spreadWord = d.aov.spread.charAt(0).toUpperCase() + d.aov.spread.slice(1);
  const spreadGuidance =
    d.aov.spread === "wide" ? "Varied prices help unique matching."
    : d.aov.spread === "moderate" ? "Some value clustering, manageable."
    : "Narrow prices create many look-alikes.";

  const hourlyBars: Bar[] = (d.hourly || []).map(h => ({
    value: h.avgRivals,
    color: rivalColor(h.avgRivals),
    xLabel: h.hour % 6 === 0 ? formatHour(h.hour) : "",
    tipTitle: formatHour(h.hour),
    tipSub: `avg ${h.avgRivals.toFixed(1)} rivals · ${h.orderCount} orders`,
  }));

  const dailyBars: Bar[] = (d.daily || []).map((day, i, arr) => ({
    value: day.count,
    color: volumeColor(day.count),
    xLabel: i % 15 === 0 || i === arr.length - 1 ? shortDate(day.date) : "",
    tipTitle: shortDate(day.date),
    tipSub: `${day.count} order${day.count === 1 ? "" : "s"}`,
  }));

  return (
    <BlockStack gap="500">
      {/* Strong, honest warning for hard-to-match stores. We never block the
          merchant - they can still proceed - but we're upfront. */}
      {d.verdict === "challenging" && (
        <Banner tone="warning" title="Your orders will be hard to attribute accurately">
          <p>
            Lots of your orders share a similar value within the same hour, so our
            statistical matcher will struggle to tell them apart. Expect a meaningful
            share of your Meta revenue to show as &quot;unverified&quot; rather than matched to
            a specific order. You can still use Lucidly - but go in with eyes open. Our
            cookie-based Layer 1 (coming) will close this gap by attributing orders directly.
          </p>
        </Banner>
      )}

      {/* Headline - purple bar + "Your Results" + edge-to-edge verdict pill */}
      <Card>
        <Box padding="600">
          <BlockStack gap="400">
            <GradientPill>Lucidly Fit Report</GradientPill>
            <BlockStack gap="100">
              <Text as="h1" variant="heading2xl">Your Results</Text>
              <Text as="p" variant="bodyMd" tone="subdued">
                Predicted Meta attribution accuracy across {d.ordersAnalysed.toLocaleString()} orders from the last {d.lookbackDays} days
              </Text>
            </BlockStack>
            <VerdictBadge verdict={d.verdict} score={d.score} size="large" fullWidth />
            <Text as="p" variant="bodyLg">{d.verdictReason}</Text>
          </BlockStack>
        </Box>
      </Card>

      {/* Order pattern - three app-style stat tiles, row-aligned */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 16 }}>
        <StatTile
          label="Orders per day"
          value={d.ordersPerDay}
          sub="online-store orders, averaged over 90 days"
        />
        <StatTile
          label="Average order value"
          value={`${sym}${d.aov.mean.toLocaleString()}`}
          sub="typical online-store order"
        />
        <StatTile
          label="Order value spread"
          value={spreadWord}
          sub={<>Order values vary roughly <strong>{cvPct}%</strong> around your average. {spreadGuidance}</>}
        />
      </div>

      {/* Average rival orders per hour */}
      {hourlyBars.length === 24 && (
        <Card>
          <Box padding="600">
            <BlockStack gap="400">
              <BlockStack gap="100">
                <Text as="h2" variant="headingLg">Average rival orders per hour</Text>
                <Text as="p" variant="bodyMd" tone="subdued">
                  Across a typical day, how many similar-value orders share each hour. Hover a bar for the detail - taller, redder bars are the hours hardest to attribute.
                </Text>
              </BlockStack>
              <InteractiveBars bars={hourlyBars} yFmt={(v) => v.toFixed(1)} />
            </BlockStack>
          </Box>
        </Card>
      )}

      {/* Orders per day, last 90 days */}
      {dailyBars.length > 0 && (
        <Card>
          <Box padding="600">
            <BlockStack gap="400">
              <BlockStack gap="100">
                <Text as="h2" variant="headingLg">Orders per day, last 90 days</Text>
                <Text as="p" variant="bodyMd" tone="subdued">
                  Daily order volume. Bars shift from green to red as volume climbs - high-volume days crowd each hour and make matching harder.
                </Text>
              </BlockStack>
              <InteractiveBars bars={dailyBars} yFmt={(v) => `${Math.round(v)}`} />
              {d.promo && (
                <div style={{
                  padding: "12px 16px", background: "#FEF3C7", borderRadius: 8,
                  border: "1px solid #FDE68A", fontSize: 13, color: "#92400E",
                }}>
                  <strong>Looks like a promotion ran between {shortDate(d.promo.start)} and {shortDate(d.promo.end)}.</strong>{" "}
                  Sales spike volume and compress order values into similar amounts, which can temporarily lower the accuracy of the matcher. Your everyday match rate outside this window will read higher.
                </div>
              )}
            </BlockStack>
          </Box>
        </Card>
      )}

      {/* Order uniqueness distribution - moved to the bottom */}
      <Card>
        <Box padding="600">
          <BlockStack gap="400">
            <BlockStack gap="100">
              <Text as="h2" variant="headingLg">Order uniqueness distribution</Text>
              <Text as="p" variant="bodyMd" tone="subdued">
                For each order, how many other orders sit in the same hour at a near-identical value
              </Text>
            </BlockStack>
            <Histogram pct={d.histogramPct} />
          </BlockStack>
        </Box>
      </Card>

      {/* CTA - only on the real pre-Meta onboarding step. */}
      {showConnectCta && (
        <Card>
          <Box padding="600">
            <BlockStack gap="300">
              <Text as="h2" variant="headingLg">Ready to connect Meta?</Text>
              <Text as="p" variant="bodyMd">
                {d.score >= 60
                  ? "Your data shape works well with our matcher. Connect Meta and we'll start pulling your full ad history - this runs in the background and takes a few hours to complete."
                  : "Your order pattern is challenging for purely-statistical matching. We'll still pull your Meta history and surface what we can, but you'll see a meaningful 'unverified revenue' figure on the dashboard. Layer 1 (cookie-based) attribution will close that gap when it ships."}
              </Text>
              <InlineStack gap="200">
                <Link to="/app/meta-connect">
                  <Button variant="primary">Connect Meta Ads</Button>
                </Link>
                <Link to="/app">
                  <Button>Skip for now</Button>
                </Link>
              </InlineStack>
            </BlockStack>
          </Box>
        </Card>
      )}
    </BlockStack>
  );
}
