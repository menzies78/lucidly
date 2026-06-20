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
};

const DOW_LABELS = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];

function formatHour(h: number) {
  if (h === 0) return "12am";
  if (h === 12) return "12pm";
  return h < 12 ? `${h}am` : `${h - 12}pm`;
}

export function VerdictBadge({ verdict, score }: { verdict: string; score: number }) {
  const palette: Record<string, { bg: string; fg: string; label: string; icon: string }> = {
    excellent: { bg: "#D1FAE5", fg: "#065F46", label: "Excellent fit", icon: "[OK]" },
    good:      { bg: "#DBEAFE", fg: "#1E40AF", label: "Good fit",      icon: "[+]" },
    marginal:  { bg: "#FEF3C7", fg: "#92400E", label: "Marginal fit",  icon: "[!]" },
    challenging: { bg: "#FEE2E2", fg: "#991B1B", label: "Challenging fit", icon: "[X]" },
  };
  const p = palette[verdict] || palette.marginal;
  return (
    <div style={{
      display: "inline-flex", alignItems: "center", gap: 12,
      padding: "10px 18px", borderRadius: 999,
      background: p.bg, color: p.fg,
      fontSize: 15, fontWeight: 700,
    }}>
      <span style={{ fontFamily: "monospace", fontSize: 13 }}>{p.icon}</span>
      <span>{p.label}</span>
      <span style={{ fontSize: 22, fontWeight: 800, marginLeft: 8 }}>{score}<span style={{ fontSize: 13, opacity: 0.7 }}>/100</span></span>
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
    <BlockStack gap="200">
      {buckets.map(b => (
        <div key={b.key}>
          <div style={{ display: "flex", justifyContent: "space-between", fontSize: 12, marginBottom: 4 }}>
            <span style={{ fontWeight: 600, color: "#1F2937" }}>{b.label}</span>
            <span style={{ color: "#6B7280" }}>
              <strong>{pct[b.key] ?? 0}%</strong> of orders <span style={{ marginLeft: 8 }}>· {b.desc}</span>
            </span>
          </div>
          <div style={{ background: "#F3F4F6", borderRadius: 4, overflow: "hidden", height: 14 }}>
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

function WorstHours({ hours }: { hours: FitReportData["worstHours"] }) {
  if (!hours || hours.length === 0) {
    return (
      <Text as="p" variant="bodySm" tone="subdued">
        No crowded hours - your order pattern is well-spread across the week.
      </Text>
    );
  }
  return (
    <BlockStack gap="150">
      {hours.map((h, i) => (
        <div key={i} style={{
          display: "flex", justifyContent: "space-between", alignItems: "center",
          padding: "8px 12px", background: "#FEF3C7", borderRadius: 6, fontSize: 13,
        }}>
          <span style={{ fontWeight: 600, color: "#92400E" }}>
            {DOW_LABELS[h.dow]} {formatHour(h.hour)}
          </span>
          <span style={{ color: "#78350F", fontSize: 12 }}>
            avg <strong>{h.avgRivals.toFixed(1)}</strong> rivals · {h.orderCount} orders
          </span>
        </div>
      ))}
    </BlockStack>
  );
}

export default function FitReport({ d, showConnectCta = false }: {
  d: FitReportData;
  showConnectCta?: boolean;
}) {
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
          <BlockStack gap="100">
            <Text as="p" variant="bodySm" tone="subdued">
              Predicted Meta attribution accuracy for {d.ordersAnalysed.toLocaleString()} orders over the last {d.lookbackDays} days
            </Text>
            <div style={{ marginTop: 8 }}>
              <VerdictBadge verdict={d.verdict} score={d.score} />
            </div>
          </BlockStack>
          <Text as="p" variant="bodyMd">{d.verdictReason}</Text>
          <div style={{
            padding: 14, background: "#F9FAFB", borderRadius: 8,
            border: "1px solid #E5E7EB", fontSize: 13, color: "#374151",
          }}>
            <strong>How we calculate this.</strong> Lucidly&apos;s matcher correlates each Meta-reported conversion to a Shopify order by timestamp (±30 min) and order value (±2%). The more orders share the same hour at similar values, the harder unique attribution becomes. We measure that &quot;rival density&quot; across your real order history - no assumptions, just the math the matcher actually uses.
          </div>
        </BlockStack>
      </Card>

      {/* Histogram + AOV stats side by side */}
      <div style={{ display: "grid", gridTemplateColumns: "2fr 1fr", gap: 16 }}>
        <Card>
          <BlockStack gap="300">
            <Text as="h2" variant="headingMd">Order uniqueness distribution</Text>
            <Text as="p" variant="bodySm" tone="subdued">
              For each order, how many other orders sit within ±30 min and ±2% of its value
            </Text>
            <Histogram pct={d.histogramPct} />
          </BlockStack>
        </Card>
        <Card>
          <BlockStack gap="300">
            <Text as="h2" variant="headingMd">Order pattern</Text>
            <BlockStack gap="200">
              <div>
                <Text as="p" variant="bodySm" tone="subdued">Volume</Text>
                <Text as="p" variant="headingMd">{d.ordersPerDay} <span style={{ fontSize: 13, fontWeight: 400 }}>orders/day</span></Text>
              </div>
              <div>
                <Text as="p" variant="bodySm" tone="subdued">Average order value</Text>
                <Text as="p" variant="headingMd">£{d.aov.mean.toLocaleString()}</Text>
              </div>
              <div>
                <Text as="p" variant="bodySm" tone="subdued">AOV spread</Text>
                <Text as="p" variant="bodyMd">
                  <strong>{d.aov.spread}</strong> (CV {d.aov.cv})
                </Text>
                <Text as="p" variant="bodySm" tone="subdued">
                  {d.aov.spread === "wide"
                    ? "Varied prices help unique matching."
                    : d.aov.spread === "moderate"
                    ? "Some value clustering, manageable."
                    : "Narrow AOV creates many rivals - expect more attribution gaps."}
                </Text>
              </div>
            </BlockStack>
          </BlockStack>
        </Card>
      </div>

      {/* Worst hours */}
      <Card>
        <BlockStack gap="300">
          <Text as="h2" variant="headingMd">Most crowded hours</Text>
          <Text as="p" variant="bodySm" tone="subdued">
            These slots have the highest average rival count - orders here are statistically harder to attribute.
          </Text>
          <WorstHours hours={d.worstHours || []} />
        </BlockStack>
      </Card>

      {/* CTA - only shown when explicitly requested (e.g. the real pre-Meta
          onboarding step). Hidden on the standalone/orphan view and the demo. */}
      {showConnectCta && (
        <Card>
          <BlockStack gap="300">
            <Text as="h2" variant="headingMd">Ready to connect Meta?</Text>
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
