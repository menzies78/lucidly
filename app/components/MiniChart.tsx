import { useState, useMemo, useRef, useEffect } from "react";
import { createPortal } from "react-dom";

function ChartTooltip({ containerRef, hover, points, prevPoints, showPrev, color, formatValue, getY, height, pointsLength }: {
  containerRef: React.RefObject<HTMLDivElement>;
  hover: number;
  points: { date: string; value: number | null }[];
  prevPoints: { date: string; value: number | null }[] | null;
  showPrev?: boolean;
  color: string;
  formatValue: (v: number) => string;
  getY: (v: number) => number;
  height: number;
  pointsLength: number;
}) {
  const [pos, setPos] = useState<{ left: number; top: number; containerTop: number; containerHeight: number } | null>(null);

  useEffect(() => {
    if (!containerRef.current) return;
    const rect = containerRef.current.getBoundingClientRect();
    const xPct = hover / (pointsLength - 1);
    const left = rect.left + xPct * rect.width;
    setPos({ left, top: rect.top, containerTop: rect.top, containerHeight: rect.height });
  }, [containerRef, hover, pointsLength]);

  if (!pos) return null;

  const p = points[hover];
  if (!p || p.value === null) return null;

  const yPct = getY(p.value) / height;
  const dotTop = pos.containerTop + yPct * pos.containerHeight;

  const hasPrev = showPrev && prevPoints && prevPoints[hover]?.value !== null && prevPoints[hover]?.value !== undefined && isFinite(prevPoints[hover].value!);
  const prevDotYPct = hasPrev ? getY(prevPoints![hover].value!) / height : 0;
  const prevDotTop = pos.containerTop + prevDotYPct * pos.containerHeight;

  return createPortal(
    <>
      {/* Vertical line */}
      <div style={{
        position: "fixed", left: pos.left, top: pos.containerTop,
        width: 1, height: pos.containerHeight,
        backgroundColor: color, opacity: 0.5, pointerEvents: "none", zIndex: 99998,
      }} />
      {/* Dot */}
      <div style={{
        position: "fixed", left: pos.left, top: dotTop,
        width: 7, height: 7, borderRadius: "50%",
        backgroundColor: color, border: "2px solid #fff",
        transform: "translate(-50%, -50%)", pointerEvents: "none",
        boxShadow: "0 1px 4px rgba(0,0,0,0.2)", zIndex: 99998,
      }} />
      {/* Previous period dot */}
      {hasPrev && (
        <div style={{
          position: "fixed", left: pos.left, top: prevDotTop,
          width: 7, height: 7, borderRadius: "50%",
          backgroundColor: color, border: "2px solid #fff",
          transform: "translate(-50%, -50%)", pointerEvents: "none",
          boxShadow: "0 1px 4px rgba(0,0,0,0.2)", opacity: 0.4, zIndex: 99998,
        }} />
      )}
      {/* Tooltip label */}
      <div style={{
        position: "fixed", left: pos.left, top: pos.containerTop - 6,
        transform: "translateX(-50%) translateY(-100%)",
        background: "#1e1e1e", color: "#fff",
        padding: "3px 8px", borderRadius: 5,
        fontSize: "11.5px", fontWeight: 600,
        whiteSpace: "nowrap", pointerEvents: "none",
        zIndex: 99999, boxShadow: "0 2px 6px rgba(0,0,0,0.2)",
      }}>
        <div>{formatValue(p.value)}</div>
        {hasPrev && (
          <div style={{ fontSize: "10px", fontWeight: 400, color: "#aaa" }}>
            prev: {formatValue(prevPoints![hover].value!)}
          </div>
        )}
        <div style={{ fontSize: "10px", fontWeight: 400, color: "#aaa" }}>
          {new Date(p.date + "T12:00:00").toLocaleDateString("en-GB", { day: "numeric", month: "short" })}
        </div>
      </div>
    </>,
    document.body,
  );
}

export default function MiniChart({ data, valueKey, color, formatValue, height = 60, prevData, showPrev }: {
  data: any[];
  valueKey: string | ((d: any) => number | null);
  color: string;
  formatValue: (v: number) => string;
  height?: number;
  prevData?: any[];
  showPrev?: boolean;
}) {
  const [hover, setHover] = useState<number | null>(null);
  const containerRef = useRef<HTMLDivElement>(null);

  const points = useMemo(() => {
    return data.map(d => {
      const v = typeof valueKey === "function" ? valueKey(d) : d[valueKey];
      return { date: d.date, value: v ?? null };
    });
  }, [data, valueKey]);

  const prevPoints = useMemo(() => {
    if (!prevData) return null;
    return prevData.map(d => {
      const v = typeof valueKey === "function" ? valueKey(d) : d[valueKey];
      return { date: d.date, value: v ?? null };
    });
  }, [prevData, valueKey]);

  const validPoints = points.filter(p => p.value !== null && isFinite(p.value));
  if (validPoints.length < 2) return null;

  const values = validPoints.map(p => p.value!);
  const prevValidValues = showPrev && prevPoints
    ? prevPoints.filter(p => p.value !== null && isFinite(p.value!)).map(p => p.value!)
    : [];
  const allValues = showPrev ? [...values, ...prevValidValues] : values;
  const minV = Math.min(...allValues);
  const maxV = Math.max(...allValues);
  const range = maxV - minV || 1;
  const w = 100;
  const pad = 4;
  const chartH = height - pad * 2;

  const getX = (i: number, len: number) => (i / (len - 1)) * w;
  const getY = (v: number) => pad + chartH - ((v - minV) / range) * chartH;

  // Build current period paths
  let linePath = "";
  let areaPath = "";
  let started = false;
  for (let i = 0; i < points.length; i++) {
    if (points[i].value === null || !isFinite(points[i].value!)) continue;
    const x = getX(i, points.length);
    const y = getY(points[i].value!);
    if (!started) {
      linePath += `M${x},${y}`;
      areaPath += `M${x},${height}L${x},${y}`;
      started = true;
    } else {
      linePath += `L${x},${y}`;
      areaPath += `L${x},${y}`;
    }
  }
  areaPath += `L${getX(points.length - 1, points.length)},${height}Z`;

  // Build previous period path (dashed overlay)
  let prevLinePath = "";
  if (showPrev && prevPoints && prevPoints.length >= 2) {
    let prevStarted = false;
    for (let i = 0; i < prevPoints.length; i++) {
      if (prevPoints[i].value === null || !isFinite(prevPoints[i].value!)) continue;
      const x = getX(i, prevPoints.length);
      const y = getY(prevPoints[i].value!);
      if (!prevStarted) {
        prevLinePath += `M${x},${y}`;
        prevStarted = true;
      } else {
        prevLinePath += `L${x},${y}`;
      }
    }
  }

  const gradientId = `grad-${color.replace("#", "")}`;

  return (
    <div ref={containerRef} style={{ position: "relative", width: "100%", height, marginTop: "6px" }}
      onMouseLeave={() => setHover(null)}
    >
      <svg viewBox={`0 0 ${w} ${height}`} preserveAspectRatio="none" style={{ width: "100%", height: "100%", display: "block" }}>
        <defs>
          <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor={color} stopOpacity={showPrev ? 0.12 : 0.25} />
            <stop offset="100%" stopColor={color} stopOpacity="0.03" />
          </linearGradient>
        </defs>
        <path d={areaPath} fill={`url(#${gradientId})`} />
        {showPrev && prevLinePath && (
          <path d={prevLinePath} fill="none" stroke={color} strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" vectorEffect="non-scaling-stroke" opacity="0.3" strokeDasharray="4 3" />
        )}
        <path d={linePath} fill="none" stroke={color} strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" vectorEffect="non-scaling-stroke" />
      </svg>
      <div style={{ position: "absolute", top: 0, left: 0, right: 0, bottom: 0, display: "flex" }}>
        {points.map((p, i) => (
          <div
            key={i}
            style={{ flex: 1, height: "100%", cursor: p.value !== null ? "crosshair" : "default" }}
            onMouseEnter={() => p.value !== null && isFinite(p.value) && setHover(i)}
          />
        ))}
      </div>
      {hover !== null && points[hover]?.value !== null && (
        <ChartTooltip
          containerRef={containerRef as React.RefObject<HTMLDivElement>}
          hover={hover}
          points={points}
          prevPoints={prevPoints}
          showPrev={showPrev}
          color={color}
          formatValue={formatValue}
          getY={getY}
          height={height}
          pointsLength={points.length}
        />
      )}
    </div>
  );
}
