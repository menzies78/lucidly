import { useState, useRef, useEffect } from "react";
import { createPortal } from "react-dom";
import { Card, Text, BlockStack } from "@shopify/polaris";
import MiniChart from "./MiniChart";

export interface SummaryTileProps {
  label: string;
  // ReactNode (not just string) so callers can inline emoji/flag glyphs at a
  // larger size than the value text - e.g. the Countries page renders the
  // country flag at 36px alongside the metric.
  value: React.ReactNode;
  subtitle?: string;
  tooltip?: { definition: string; calc?: string };
  previousValue?: number;
  currentValue?: number;
  lowerIsBetter?: boolean;
  chartData?: any[];
  prevChartData?: any[];
  chartKey?: string | ((d: any) => number | null);
  chartColor?: string;
  chartFormat?: (v: number) => string;
  imageUrl?: string;
  // For ad tiles backed by a Dynamic Product Ad (Advantage+ catalog) where
  // there is no single creative image - Meta hands back a 64x64 placeholder
  // for these and it reads as a blank grey blob at tile size. When isDpa
  // is true and there is no usable imageUrl, we paint a "D" badge instead.
  isDpa?: boolean;
  // Optional override for tiles whose "value" is a long product name
  // that would overflow the default 2xl heading (e.g. Best Gateway Product).
  valueVariant?: "headingXl" | "headingLg" | "headingMd" | "heading2xl";
  // When true, centres the value row + subtitle horizontally. Used by the
  // Countries quick-stat tiles where the country flag is the star and the
  // metric reads as a poster, not a left-aligned label.
  centered?: boolean;
  // When true, renders a shorter tile (~110px). For chartless count tiles
  // (e.g. Change Log summary row) the default 180px feels oversized.
  compact?: boolean;
}

function ProductThumb({ url, size = 44 }: { url: string; size?: number }) {
  return (
    <img
      src={url}
      alt=""
      style={{
        width: size, height: size, borderRadius: 6,
        objectFit: "cover", border: "1px solid #E5E7EB", flexShrink: 0,
      }}
    />
  );
}

// Visual stand-in for Dynamic Product Ads, which don't have a single
// creative thumbnail (Meta returns a 64x64 placeholder PNG that reads as a
// blank grey blob at tile size). Same colour treatment as the "D" badge
// in AdThumbTile so the visual language stays consistent across the page.
function DpaBadge({ size = 44 }: { size?: number }) {
  return (
    <img
      src="/dpa-thumbnail.jpg"
      alt="DPA"
      title="Dynamic Product Ad - thumbnails come from the product catalogue, not a single creative."
      style={{
        width: size, height: size, borderRadius: 6,
        objectFit: "cover", flexShrink: 0,
        border: "1px solid #C7D2FE",
      }}
    />
  );
}

function PortalTooltip({ anchorRef, children }: { anchorRef: React.RefObject<HTMLElement>; children: React.ReactNode }) {
  const [pos, setPos] = useState<{ top: number; left: number } | null>(null);

  useEffect(() => {
    if (!anchorRef.current) return;
    const rect = anchorRef.current.getBoundingClientRect();
    setPos({ top: rect.top - 6, left: rect.left });
  }, [anchorRef]);

  if (!pos) return null;

  return createPortal(
    <div style={{
      position: "fixed", top: pos.top, left: pos.left,
      transform: "translateY(-100%)",
      background: "#1e1e1e", color: "#fff", padding: "8px 12px", borderRadius: 6,
      fontSize: 11.5, fontWeight: 400, lineHeight: 1.5, width: 260, zIndex: 99999,
      boxShadow: "0 2px 8px rgba(0,0,0,0.25)", whiteSpace: "normal",
      pointerEvents: "none",
    }}>
      {children}
    </div>,
    document.body,
  );
}

function DeltaBadge({ currentValue, previousValue, lowerIsBetter, onHoverChange }: {
  currentValue: number; previousValue: number; lowerIsBetter?: boolean;
  onHoverChange?: (hovering: boolean) => void;
}) {
  const [showTip, setShowTip] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  if (!previousValue || previousValue === 0) return null;
  const pct = ((currentValue - previousValue) / Math.abs(previousValue)) * 100;
  const went = pct >= 0 ? "up" : "down";
  const isGood = lowerIsBetter ? went === "down" : went === "up";
  const color = isGood ? "#059669" : "#DC2626";
  const bg = isGood ? "#ECFDF5" : "#FEF2F2";
  const arrow = went === "up" ? "\u25B2" : "\u25BC";

  return (
    <div
      ref={ref}
      style={{ position: "relative", display: "inline-flex", alignItems: "center" }}
      onMouseEnter={() => { setShowTip(true); onHoverChange?.(true); }}
      onMouseLeave={() => { setShowTip(false); onHoverChange?.(false); }}
    >
      <span style={{
        fontSize: 11, fontWeight: 600, color, background: bg,
        borderRadius: 4, padding: "1px 6px",
        display: "inline-flex", alignItems: "center", gap: 2,
        cursor: "default", whiteSpace: "nowrap",
      }}>
        <span style={{ fontSize: 8 }}>{arrow}</span>
        {Math.abs(pct).toFixed(1)}%
      </span>
      {showTip && ref.current && (
        <PortalTooltip anchorRef={ref as React.RefObject<HTMLElement>}>
          <div style={{ textAlign: "center" }}>Compared to previous period</div>
        </PortalTooltip>
      )}
    </div>
  );
}

export default function SummaryTile({
  label, value, subtitle, tooltip, previousValue, currentValue, lowerIsBetter,
  chartData, prevChartData, chartKey, chartColor, chartFormat, imageUrl, isDpa,
  valueVariant = "heading2xl", centered = false, compact = false,
}: SummaryTileProps) {
  const [showTip, setShowTip] = useState(false);
  const [showPrevOverlay, setShowPrevOverlay] = useState(false);
  const labelRef = useRef<HTMLDivElement>(null);

  return (
    <Card>
      <div style={{ minHeight: compact ? 96 : 180, height: "100%", display: "flex", flexDirection: "column" }}>
        {/* Header: label + delta badge */}
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 6, marginBottom: 4 }}>
          <div
            ref={labelRef}
            style={{ display: "flex", alignItems: "center", gap: 4 }}
            onMouseEnter={() => tooltip && setShowTip(true)}
            onMouseLeave={() => setShowTip(false)}
          >
            <Text as="p" variant="headingSm" tone="subdued">{label}</Text>
            {tooltip && (
              <span style={{ cursor: "help", fontSize: 11, color: "#9CA3AF", fontWeight: 600, lineHeight: 1 }}>?</span>
            )}
          </div>
          {previousValue !== undefined && currentValue !== undefined && (
            <DeltaBadge
              currentValue={currentValue}
              previousValue={previousValue}
              lowerIsBetter={lowerIsBetter}
              onHoverChange={prevChartData ? setShowPrevOverlay : undefined}
            />
          )}
        </div>

        {/* Tooltip via portal */}
        {showTip && tooltip && labelRef.current && (
          <PortalTooltip anchorRef={labelRef as React.RefObject<HTMLElement>}>
            <div style={{ fontWeight: 600, marginBottom: 3 }}>{label}</div>
            <div>{tooltip.definition}</div>
            {tooltip.calc && (
              <div style={{ marginTop: 4, fontStyle: "italic", color: "#93C5FD" }}>
                {tooltip.calc}
              </div>
            )}
          </PortalTooltip>
        )}

        {/* Value + optional product image */}
        <div style={{
          display: "flex", alignItems: "center", gap: 10, marginBottom: 2,
          justifyContent: centered ? "center" : "flex-start",
          textAlign: centered ? "center" : "left",
        }}>
          {imageUrl ? <ProductThumb url={imageUrl} /> : isDpa ? <DpaBadge /> : null}
          <Text as="p" variant={valueVariant}>{value}</Text>
        </div>

        {/* Subtitle */}
        {subtitle && (
          <div style={{ marginBottom: 4, textAlign: centered ? "center" : "left" }}>
            <Text as="p" variant="bodySm" tone="subdued">{subtitle}</Text>
          </div>
        )}

        {/* Chart fills remaining space */}
        <div style={{ flex: 1, display: "flex", flexDirection: "column", justifyContent: "flex-end" }}>
          {chartData && chartKey && chartColor && chartFormat && (
            <MiniChart
              data={chartData}
              valueKey={chartKey}
              color={chartColor}
              formatValue={chartFormat}
              prevData={showPrevOverlay ? prevChartData : undefined}
              showPrev={showPrevOverlay}
            />
          )}
        </div>
      </div>
    </Card>
  );
}
