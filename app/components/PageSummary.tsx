import React from "react";
import { Card, Text, BlockStack } from "@shopify/polaris";

// ── Types ──

export type SummaryTone = "positive" | "negative" | "warning" | "neutral";

export interface SummaryBullet {
  tone?: SummaryTone;
  text: React.ReactNode;
}

interface PageSummaryProps {
  title?: string;
  bullets: SummaryBullet[];
  fromKey?: string; // YYYY-MM-DD — when provided alongside toKey, title becomes "Summary for <range>"
  toKey?: string;
}

// Parse a YYYY-MM-DD date key as UTC so toLocaleDateString doesn't drift
// across timezones when rendering server-side vs browser-local.
function formatDateKey(key: string): string {
  const [y, m, d] = key.split("-").map(Number);
  const date = new Date(Date.UTC(y, m - 1, d));
  return date.toLocaleDateString("en-GB", { day: "numeric", month: "short", timeZone: "UTC" });
}

function rangeLabel(fromKey: string, toKey: string): string {
  const [yTo] = toKey.split("-").map(Number);
  if (fromKey === toKey) return `${formatDateKey(fromKey)} ${yTo}`;
  return `${formatDateKey(fromKey)} – ${formatDateKey(toKey)} ${yTo}`;
}

// ── Styles ──

const TONE_COLOR: Record<SummaryTone, string> = {
  positive: "#10B981",
  negative: "#EF4444",
  warning:  "#F59E0B",
  neutral:  "#7C3AED",
};

// ── Component ──
// Rule-based page summary. Lives side-by-side with AiInsightsPanel at the
// top of a page. Bullets are computed in the route loader from the same
// pre-aggregated data that feeds the tiles below — no AI, no caching,
// tied to the currently selected date range. Always single-column,
// left-aligned.

export default function PageSummary({ title, bullets, fromKey, toKey }: PageSummaryProps) {
  const resolvedTitle = title
    ?? (fromKey && toKey ? `Summary for ${rangeLabel(fromKey, toKey)}` : "Summary");
  return (
    <Card>
      <BlockStack gap="400">
        <Text as="h2" variant="headingLg">{resolvedTitle}</Text>
        {bullets.length === 0 ? (
          <Text as="p" tone="subdued" variant="bodySm">No data for this period.</Text>
        ) : (
          <ul style={{ margin: 0, paddingLeft: 0, listStyle: "none", display: "flex", flexDirection: "column", gap: 10 }}>
            {bullets.map((b, i) => {
              const color = TONE_COLOR[b.tone || "neutral"];
              return (
                <li key={i} style={{ display: "flex", gap: 10, alignItems: "flex-start", fontSize: 13, lineHeight: 1.5, color: "#1F2937" }}>
                  <span style={{ color, fontSize: 14, lineHeight: "22px", flexShrink: 0 }}>●</span>
                  <span>{b.text}</span>
                </li>
              );
            })}
          </ul>
        )}
      </BlockStack>
    </Card>
  );
}
