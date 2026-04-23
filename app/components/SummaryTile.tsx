import { useState, useRef, useEffect } from "react";
import { createPortal } from "react-dom";
import { Card, Text, BlockStack } from "@shopify/polaris";
import MiniChart from "./MiniChart";

export interface SummaryTileProps {
  label: string;
  value: string;
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
  // Optional override for tiles whose "value" is a long product name
  // that would overflow the default 2xl heading (e.g. Best Gateway Product).
  valueVariant?: "headingXl" | "headingLg" | "headingMd" | "heading2xl";
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
  chartData, prevChartData, chartKey, chartColor, chartFormat, imageUrl,
  valueVariant = "heading2xl",
}: SummaryTileProps) {
  const [showTip, setShowTip] = useState(false);
  const [showPrevOverlay, setShowPrevOverlay] = useState(false);
  const labelRef = useRef<HTMLDivElement>(null);

  return (
    <Card>
      <div style={{ minHeight: 180, height: "100%", display: "flex", flexDirection: "column" }}>
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
        <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 2 }}>
          {imageUrl && <ProductThumb url={imageUrl} />}
          <Text as="p" variant={valueVariant}>{value}</Text>
        </div>

        {/* Subtitle */}
        {subtitle && (
          <div style={{ marginBottom: 4 }}>
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
