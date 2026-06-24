import { useState, useRef, useEffect, type ReactNode, type CSSProperties } from "react";
import { createPortal } from "react-dom";

// Toggle/pill button with an explanatory hover tooltip. The tooltip visual
// mirrors HeaderTip (app.products.tsx) so the dark popover reads identically
// everywhere it's used: same #1e1e1e box, padding, radius, font, width.
//
// Renders a real <button> as a direct child (the tooltip is portaled to
// document.body), so existing toggle-group / segment-toggle / l-pill CSS that
// targets first/last/adjacent buttons keeps working untouched.
export function TipButton({
  tip,
  className,
  onClick,
  children,
  style,
  onMouseEnter,
  onMouseLeave,
}: {
  tip: string;
  className?: string;
  onClick?: () => void;
  children: ReactNode;
  style?: CSSProperties;
  onMouseEnter?: () => void;
  onMouseLeave?: () => void;
}) {
  const [show, setShow] = useState(false);
  const ref = useRef<HTMLButtonElement>(null);
  const [pos, setPos] = useState<{ top: number; left: number } | null>(null);

  useEffect(() => {
    if (show && ref.current) {
      const rect = ref.current.getBoundingClientRect();
      setPos({ top: rect.top - 8, left: rect.left + rect.width / 2 });
    }
  }, [show]);

  return (
    <>
      <button
        ref={ref}
        className={className}
        style={style}
        onClick={onClick}
        onMouseEnter={() => { setShow(true); onMouseEnter?.(); }}
        onMouseLeave={() => { setShow(false); onMouseLeave?.(); }}
      >
        {children}
      </button>
      {show && pos && createPortal(
        <div style={{
          position: "fixed", top: pos.top, left: pos.left,
          transform: "translate(-50%, -100%)",
          background: "#1e1e1e", color: "#fff", padding: "8px 12px", borderRadius: 6,
          fontSize: 11.5, fontWeight: 400, lineHeight: 1.5, width: 260, zIndex: 99999,
          boxShadow: "0 2px 8px rgba(0,0,0,0.25)", whiteSpace: "normal",
          pointerEvents: "none", textAlign: "left",
        }}>
          {tip}
        </div>,
        document.body,
      )}
    </>
  );
}
