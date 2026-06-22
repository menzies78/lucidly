// Shared "Lucidly Fit Report" UI - the rich, breakdown-rich rendering of a
// computed Fit Test result. Extracted from app.fit-test.tsx so the standalone
// route AND the onboarding demo/flow render identical markup (single source of
// truth). Presentational only: pass in a computed fit-data object.

import { Card, Text, BlockStack, InlineStack, Button, Banner } from "@shopify/polaris";
import { Link } from "@remix-run/react";

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
};

function formatHour(h: number) {
  if (h === 0) return "12am";
  if (h === 12) return "12pm";
  return h < 12 ? `${h}am` : `${h - 12}pm`;
}

function currencySymbol(code?: string) {
  const map: Record<string, string> = { GBP: "£", USD: "$", EUR: "€", AUD: "A$", CAD: "C$", NZD: "NZ$" };
  return map[code || "GBP"] || (code ? `${code} ` : "£");
}

export function VerdictBadge({ verdict, score, size = "default" }: { verdict: string; score: number; size?: "default" | "large" }) {
  const palette: Record<string, { bg: string; fg: string; label: string; icon: string }> = {
    // Five-band scale used by the quick Q&A instant read (90+/75+/65+/50+/<50).
    excellent: { bg: "#D1FAE5", fg: "#065F46", label: "Excellent fit",  icon: "[OK]" },
    good:      { bg: "#DBEAFE", fg: "#1E40AF", label: "Good fit",       icon: "[+]" },
    passable:  { bg: "#FEF9C3", fg: "#854D0E", label: "Passable fit",   icon: "[~]" },
    below:     { bg: "#FFEDD5", fg: "#9A3412", label: "Below average",  icon: "[!]" },
    poor:      { bg: "#FEE2E2", fg: "#991B1B", label: "Not a good fit", icon: "[X]" },
    // Legacy bands still emitted by the real fitTest.server.js algorithm.
    marginal:  { bg: "#FEF3C7", fg: "#92400E", label: "Marginal fit",   icon: "[!]" },
    challenging: { bg: "#FEE2E2", fg: "#991B1B", label: "Challenging fit", icon: "[X]" },
  };
  const p = palette[verdict] || palette.marginal;
  const lg = size === "large";
  return (
    <div style={{
      display: "inline-flex", alignItems: "center", gap: lg ? 16 : 12,
      padding: lg ? "16px 30px" : "10px 18px", borderRadius: 999,
      background: p.bg, color: p.fg,
      fontSize: lg ? 22 : 15, fontWeight: 700,
    }}>
      <span style={{ fontFamily: "monospace", fontSize: lg ? 17 : 13 }}>{p.icon}</span>
      <span>{p.label}</span>
      <span style={{ fontSize: lg ? 34 : 22, fontWeight: 800, marginLeft: lg ? 12 : 8 }}>{score}<span style={{ fontSize: lg ? 17 : 13, opacity: 0.7 }}>/100</span></span>
    </div>
  );
}

function Histogram({ pct }: { pct: Record<string, number> }) {
  const buckets = [
    { key: "0",  label: "Alone",      color: "#10B981", desc: "matches uniquely" },
    { key: "1",  label: "1 rival",    color: "#34D399", desc: "50% confidence" },
    { key: "2",  label: "2 rivals",   color: "#FBBF24", desc: "33% confidence" },
    { key: "3",  label: "3 rivals",   color: "#F59E0B", desc: "25% confidence" },
    { key: "4+", label: "4+ rivals",  color: "#EF4444", desc: "<20% confidence" },
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

// App-style stat tile: small subdued label up top, a BIG centred value, optional
// supporting line. Mirrors SummaryTile's typography (headingSm / heading2xl).
function StatTile({ label, value, sub }: { label: string; value: React.ReactNode; sub?: React.ReactNode }) {
  return (
    <Card>
      <div style={{
        minHeight: 150, height: "100%", display: "flex", flexDirection: "column",
        alignItems: "center", justifyContent: "center", textAlign: "center", gap: 8, padding: "10px 6px",
      }}>
        <Text as="p" variant="headingSm" tone="subdued">{label}</Text>
        <Text as="p" variant="heading2xl">{value}</Text>
        {sub && (
          <div style={{ maxWidth: 240 }}>
            <Text as="p" variant="bodySm" tone="subdued">{sub}</Text>
          </div>
        )}
      </div>
    </Card>
  );
}

function rivalColor(v: number) {
  if (v < 0.5) return "#10B981";
  if (v < 1) return "#34D399";
  if (v < 2) return "#FBBF24";
  if (v < 3) return "#F59E0B";
  return "#EF4444";
}

// 24-hour bar chart of the average rival count per hour-of-day, combined across
// the whole lookback window. Taller/redder bars = hours that are harder to match.
function RivalHourChart({ hourly }: { hourly: NonNullable<FitReportData["hourly"]> }) {
  const max = Math.max(0.1, ...hourly.map(h => h.avgRivals));
  return (
    <div style={{ display: "flex", alignItems: "flex-end", gap: 4, height: 156 }}>
      {hourly.map(h => {
        const pct = (h.avgRivals / max) * 100;
        return (
          <div key={h.hour} style={{ flex: 1, display: "flex", flexDirection: "column", alignItems: "center", gap: 6 }}>
            <div
              style={{ width: "100%", height: 124, display: "flex", alignItems: "flex-end" }}
              title={`${formatHour(h.hour)} - avg ${h.avgRivals.toFixed(1)} rivals across ${h.orderCount} orders`}
            >
              <div style={{
                width: "100%", height: `${Math.max(2, pct)}%`,
                background: rivalColor(h.avgRivals), borderRadius: "4px 4px 0 0",
                transition: "height 0.4s ease",
              }} />
            </div>
            <span style={{ fontSize: 10, color: "#8C9196", height: 12, lineHeight: "12px" }}>
              {h.hour % 6 === 0 ? formatHour(h.hour) : ""}
            </span>
          </div>
        );
      })}
    </div>
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

  return (
    <BlockStack gap="500">
      {/* Strong, honest warning for hard-to-match stores. We never block the
          merchant - they can still proceed - but we're upfront that a chunk of
          Meta revenue will read as 'unverified' on a challenging order pattern. */}
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

      {/* Headline */}
      <Card>
        <BlockStack gap="400">
          <BlockStack gap="200">
            <Text as="p" variant="bodyMd" tone="subdued">
              Predicted Meta attribution accuracy for {d.ordersAnalysed.toLocaleString()} orders over the last {d.lookbackDays} days
            </Text>
            <div style={{ marginTop: 6 }}>
              <VerdictBadge verdict={d.verdict} score={d.score} size="large" />
            </div>
          </BlockStack>
          <Text as="p" variant="bodyLg">{d.verdictReason}</Text>
          <div style={{
            padding: 16, background: "#F9FAFB", borderRadius: 8,
            border: "1px solid #E5E7EB", fontSize: 14, lineHeight: 1.55, color: "#374151",
          }}>
            <strong>How we calculate this.</strong> Lucidly&apos;s matcher correlates each Meta-reported conversion to a Shopify order by timestamp and order value. The more similar value orders placed during the same hour, the trickier attribution becomes. We measure that &quot;rival density&quot; across your real order history - no assumptions, just the signals the matcher actually uses.
          </div>
        </BlockStack>
      </Card>

      {/* Order uniqueness distribution */}
      <Card>
        <BlockStack gap="300">
          <Text as="h2" variant="headingLg">Order uniqueness distribution</Text>
          <Text as="p" variant="bodyMd" tone="subdued">
            For each order, how many other orders sit in the same hour at a near-identical value
          </Text>
          <Histogram pct={d.histogramPct} />
        </BlockStack>
      </Card>

      {/* Order pattern - three app-style stat tiles */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 16 }}>
        <StatTile
          label="Orders per day"
          value={d.ordersPerDay}
          sub="online-store orders, averaged over 90 days"
        />
        <StatTile
          label="Average order value"
          value={`${sym}${d.aov.mean.toLocaleString()}`}
        />
        <StatTile
          label="Order value spread"
          value={spreadWord}
          sub={<>Order values vary roughly <strong>{cvPct}%</strong> around your average. {spreadGuidance}</>}
        />
      </div>

      {/* 24-hour rival distribution */}
      {d.hourly && d.hourly.length === 24 && (
        <Card>
          <BlockStack gap="300">
            <Text as="h2" variant="headingLg">Average rival orders per hour</Text>
            <Text as="p" variant="bodyMd" tone="subdued">
              Across a typical day, how many similar-value orders share each hour. Taller, redder bars are the hours hardest to attribute uniquely.
            </Text>
            <RivalHourChart hourly={d.hourly} />
          </BlockStack>
        </Card>
      )}

      {/* CTA - only shown when explicitly requested (e.g. the real pre-Meta
          onboarding step). Hidden on the standalone/orphan view and the demo. */}
      {showConnectCta && (
        <Card>
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
        </Card>
      )}
    </BlockStack>
  );
}
