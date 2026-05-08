// Lucidly Fit Report - the "will it work for my company?" page shown
// after Shopify connects, before Meta connect. Reads from the cached
// fitTestData JSON so it renders instantly; if the test hasn't run yet,
// the loader fires it on demand (single-flight guarded).

import { json, redirect } from "@remix-run/node";
import { useLoaderData, Link } from "@remix-run/react";
import {
  Page, Card, Text, BlockStack, InlineStack, Button, Banner, Box,
} from "@shopify/polaris";
import { authenticate } from "../shopify.server";
import db from "../db.server";
import { runFitTest, getFitTest } from "../services/fitTest.server.js";

export const loader = async ({ request }) => {
  const { session } = await authenticate.admin(request);
  const shopDomain = session.shop;

  // If onboarding is already complete, this page has no business being
  // visible - bounce back to dashboard.
  const shop = await db.shop.findUnique({
    where: { shopDomain },
    select: {
      onboardingCompleted: true,
      lastOrderSync: true,
      fitTestComputedAt: true,
    },
  });

  // No order sync yet - tell the merchant to wait, the test needs orders.
  if (!shop?.lastOrderSync) {
    return json({ status: "waiting", shopDomain });
  }

  // Run on demand if we haven't yet, or if data is older than 7 days.
  const stale = !shop.fitTestComputedAt
    || (Date.now() - shop.fitTestComputedAt.getTime() > 7 * 24 * 3600 * 1000);
  let data = await getFitTest(shopDomain);
  if (!data || stale) {
    data = await runFitTest(shopDomain);
  }

  return json({ status: "ready", data, shopDomain });
};

const DOW_LABELS = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];

function formatHour(h: number) {
  if (h === 0) return "12am";
  if (h === 12) return "12pm";
  return h < 12 ? `${h}am` : `${h - 12}pm`;
}

function VerdictBadge({ verdict, score }: { verdict: string; score: number }) {
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

function WorstHours({ hours }: { hours: any[] }) {
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

export default function FitTest() {
  const data = useLoaderData<typeof loader>();

  if (data.status === "waiting") {
    return (
      <Page title="Lucidly Fit Report">
        <Card>
          <BlockStack gap="300">
            <Text as="h2" variant="headingLg">Importing your orders</Text>
            <Text as="p" variant="bodyMd">
              We need your order history to predict how well our matcher will work for you.
              This usually takes a couple of minutes. Refresh this page once your orders have synced.
            </Text>
          </BlockStack>
        </Card>
      </Page>
    );
  }

  const d = (data as any).data;

  if (!d || d.score === null) {
    return (
      <Page title="Lucidly Fit Report">
        <Banner tone="info">
          <p>{d?.message || "Not enough order history yet to compute a Fit score."}</p>
        </Banner>
      </Page>
    );
  }

  return (
    <Page title="Lucidly Fit Report">
      <BlockStack gap="500">
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
              <strong>How we calculate this.</strong> Lucidly's matcher correlates each Meta-reported conversion to a Shopify order by timestamp (±30 min) and order value (±2%). The more orders share the same hour at similar values, the harder unique attribution becomes. We measure that "rival density" across your real order history - no assumptions, just the math the matcher actually uses.
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

        {/* CTA */}
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
      </BlockStack>
    </Page>
  );
}
