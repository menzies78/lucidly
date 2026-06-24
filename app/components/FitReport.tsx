// Shared "Lucidly Fit Report" UI - the rich rendering of a computed Fit Test
// result. Used by the standalone route AND the onboarding demo/flow so they
// render identical markup. Presentational only: pass in a computed fit-data
// object. Visual language matches the pre-test intro screen (purple pill,
// heading2xl titles, app-style stat tiles).

import { useState } from "react";
import { Card, Text, BlockStack, InlineStack, Banner, Box } from "@shopify/polaris";
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

// Rival-per-hour colour: purple is the calm baseline, ramping to red as the
// average climbs (more rivals per hour = harder to attribute). Purple holds
// until ~1 rival, then interpolates purple -> red, fully red by ~3.
function rivalColor(v: number) {
  const PURPLE_RGB: [number, number, number] = [124, 58, 237];
  const RED_RGB: [number, number, number] = [239, 68, 68];
  const lo = 1, hi = 3;
  const t = Math.max(0, Math.min(1, (v - lo) / (hi - lo)));
  const c = PURPLE_RGB.map((p, k) => Math.round(p + (RED_RGB[k] - p) * t));
  return `rgb(${c.join(",")})`;
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

// Purple tick + primary/secondary copy, used by the "Next step" checklist.
function TickBullet({ title, children }: { title: string; children?: React.ReactNode }) {
  return (
    <InlineStack gap="300" blockAlign="start" wrap={false}>
      <span style={{ color: PURPLE, fontSize: 20, fontWeight: 800, lineHeight: "24px" }}>{"\u2713"}</span>
      <BlockStack gap="050">
        <Text as="span" variant="bodyMd" fontWeight="semibold">{title}</Text>
        {children && <Text as="span" variant="bodySm" tone="subdued">{children}</Text>}
      </BlockStack>
    </InlineStack>
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
    tipSub: `avg ${h.avgRivals.toFixed(1)} similar · ${h.orderCount} orders`,
  }));

  // Next-step copy aligned to the four server verdict bands (excellent 80+,
  // good 60+, marginal 40+, challenging <40) so it sets the right expectation
  // against the badge shown at the top. Honest about blended/unverified
  // revenue rather than over-promising.
  const NEXT_STEP_COPY: Record<string, string> = {
    excellent:
      "Your orders are highly distinguishable, so Lucidly will tie the vast majority of your Meta-driven sales to a specific order. Connect Meta to start your full setup - everything below runs automatically in the background.",
    good:
      "Most of your orders match cleanly to a single order. A few crowded hours will show as blended rather than order-level attribution, but your verified coverage will be strong. Connect Meta to start your full setup - everything below runs automatically in the background.",
    marginal:
      "Roughly half your orders are distinguishable; the rest cluster too tightly to pin to one order, so expect a meaningful share of blended (unverified) revenue alongside your matched sales. You'll still get full spend, campaign, customer and LTV reporting. Connect Meta to start your setup.",
    challenging:
      "Your store is high-volume with closely-priced orders, so the matcher will tie only a minority of sales to a specific order - expect significant attribution gaps reported as blended ROAS. You'll still get full spend, campaign, customer and LTV reporting. Connect Meta to start your setup.",
  };
  const nextStepCopy = NEXT_STEP_COPY[d.verdict] || NEXT_STEP_COPY.marginal;

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
            a specific order. You can still use Lucidly - but go in with eyes open.
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
                <Text as="h2" variant="heading2xl">Average similar orders per hour</Text>
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
                <Text as="h2" variant="heading2xl">Orders per day, last 90 days</Text>
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

      {/* Next step - connect Meta. Shown on the onboarding fit report so the
          page reads as the start of setup, not a standalone report. */}
      {showConnectCta && (
        <Card>
          <Box padding="600">
            <BlockStack gap="400">
              <GradientPill>Next step</GradientPill>
              <BlockStack gap="100">
                <Text as="h2" variant="heading2xl">Connect your Meta account</Text>
                <Text as="p" variant="bodyMd" tone="subdued">{nextStepCopy}</Text>
              </BlockStack>
              <BlockStack gap="300">
                <TickBullet title="Import your Meta ad history">
                  Spend, campaigns, ad sets and conversions are pulled straight from your ad account.
                </TickBullet>
                <TickBullet title="Import your Shopify order history">
                  The last 12 months of orders - timestamps, values and customers - sync in the background.
                </TickBullet>
                <TickBullet title="Match conversions to orders">
                  Lucidly statistically links each Meta conversion to the Shopify order behind it, with a confidence score.
                </TickBullet>
                <TickBullet title="Build your benchmarks">
                  Your baseline acquisition, LTV and repeat metrics are established for future performance to be measured against.
                </TickBullet>
              </BlockStack>
              <Link to="/app/meta-connect" style={{
                display: "flex", alignItems: "center", justifyContent: "center",
                width: "100%", padding: "16px 24px", borderRadius: 10,
                color: "#fff", fontWeight: 700, fontSize: 16, letterSpacing: 0.5,
                textDecoration: "none",
                background: `linear-gradient(90deg, ${PURPLE}, ${PURPLE_LIGHT})`,
                boxShadow: "0 4px 14px rgba(124,58,237,0.35)",
              }}>CONNECT META ADS</Link>
            </BlockStack>
          </Box>
        </Card>
      )}
    </BlockStack>
  );
}
