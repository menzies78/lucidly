import React from "react";
import { Card, Text, BlockStack } from "@shopify/polaris";

/**
 * A placeholder tile for journey/pixel-dependent visualisations that have no
 * data yet (the storefront web pixel only starts emitting touches once the
 * merchant re-grants the write_pixels scope, and stitched journeys take time
 * to accumulate). It renders a greyed-out preview of the eventual chart with
 * an overlay notification, so the layout reads as "coming soon" rather than
 * looking broken during onboarding.
 *
 * Keep these in NON-prominent positions (bottom of a page). Once real data
 * exists, swap the call site to render the live visualisation instead.
 */
export default function AwaitingDataTile({
  title,
  message,
  preview,
}: {
  title: string;
  message: string;
  preview: React.ReactNode;
}) {
  return (
    <Card>
      <BlockStack gap="300">
        <Text as="h3" variant="headingMd">
          {title}
        </Text>
        <div style={{ position: "relative" }}>
          {/* Greyed preview — desaturated, faded, non-interactive */}
          <div
            aria-hidden
            style={{
              filter: "grayscale(1)",
              opacity: 0.28,
              pointerEvents: "none",
              userSelect: "none",
            }}
          >
            {preview}
          </div>

          {/* Overlay notification */}
          <div
            style={{
              position: "absolute",
              inset: 0,
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              padding: 16,
            }}
          >
            <div
              style={{
                maxWidth: 420,
                textAlign: "center",
                background: "rgba(255,255,255,0.92)",
                border: "1px solid #E1E3E5",
                borderRadius: 12,
                padding: "18px 22px",
                boxShadow: "0 1px 3px rgba(0,0,0,0.08)",
                backdropFilter: "blur(1px)",
              }}
            >
              <div
                style={{
                  display: "inline-flex",
                  alignItems: "center",
                  gap: 8,
                  marginBottom: 8,
                  fontSize: 12,
                  fontWeight: 700,
                  letterSpacing: 0.3,
                  textTransform: "uppercase",
                  color: "#6D28D9",
                }}
              >
                <span
                  style={{
                    width: 8,
                    height: 8,
                    borderRadius: "50%",
                    background: "#7C3AED",
                    display: "inline-block",
                  }}
                />
                Collecting data
              </div>
              <div style={{ fontSize: 14, color: "#374151", lineHeight: 1.45 }}>
                {message}
              </div>
            </div>
          </div>
        </div>
      </BlockStack>
    </Card>
  );
}

/* ── Lightweight greyed preview illustrations (inline SVG, no deps) ── */

export function FirstLastClickPreview() {
  const rows = [
    { label: "First click", first: 62, last: 18 },
    { label: "Mid journey", first: 24, last: 30 },
    { label: "Last click", first: 14, last: 52 },
  ];
  return (
    <div style={{ padding: "8px 4px" }}>
      <BlockStack gap="200">
        {rows.map((r) => (
          <div key={r.label} style={{ display: "flex", alignItems: "center", gap: 12 }}>
            <div style={{ width: 90, fontSize: 12, color: "#6B7280" }}>{r.label}</div>
            <div style={{ flex: 1, display: "flex", gap: 6 }}>
              <div style={{ height: 16, width: `${r.first}%`, background: "#7C3AED", borderRadius: 3 }} />
              <div style={{ height: 16, width: `${r.last}%`, background: "#C4B5FD", borderRadius: 3 }} />
            </div>
          </div>
        ))}
        <div style={{ display: "flex", gap: 16, fontSize: 11, color: "#6B7280", marginTop: 4 }}>
          <span>● First-click credit</span>
          <span>● Last-click credit</span>
        </div>
      </BlockStack>
    </div>
  );
}

export function JourneyTimelinePreview() {
  const steps = ["Meta ad", "Landing", "Product", "Cart", "Checkout"];
  return (
    <div style={{ padding: "16px 8px" }}>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
        {steps.map((s, i) => (
          <React.Fragment key={s}>
            <div style={{ textAlign: "center" }}>
              <div
                style={{
                  width: 34,
                  height: 34,
                  borderRadius: "50%",
                  background: i === steps.length - 1 ? "#7C3AED" : "#DDD6FE",
                  margin: "0 auto 6px",
                }}
              />
              <div style={{ fontSize: 11, color: "#6B7280", maxWidth: 60 }}>{s}</div>
            </div>
            {i < steps.length - 1 && (
              <div style={{ flex: 1, height: 3, background: "#E5E7EB", margin: "0 4px", marginBottom: 18 }} />
            )}
          </React.Fragment>
        ))}
      </div>
    </div>
  );
}

export function AcquisitionPathsPreview() {
  const paths = [
    { label: "Meta → direct return → purchase", w: 80 },
    { label: "Meta → search → purchase", w: 58 },
    { label: "Meta → email → purchase", w: 40 },
    { label: "Meta → multi-touch → purchase", w: 26 },
  ];
  return (
    <div style={{ padding: "8px 4px" }}>
      <BlockStack gap="200">
        {paths.map((p) => (
          <div key={p.label}>
            <div style={{ fontSize: 12, color: "#6B7280", marginBottom: 3 }}>{p.label}</div>
            <div style={{ height: 14, width: `${p.w}%`, background: "#A78BFA", borderRadius: 3 }} />
          </div>
        ))}
      </BlockStack>
    </div>
  );
}
