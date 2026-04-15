import React, { useState, useCallback, useRef, useEffect, useLayoutEffect } from "react";

// ── Types ──

export interface TileDef {
  id: string;
  label: string;
  span?: number;
  render: () => React.ReactNode;
}

interface TileGridProps {
  pageId: string;
  tiles: TileDef[];
  columns?: number;
}

// ── Persistence ──

const STORAGE_PREFIX = "tileGrid_";

interface TileState {
  order: string[];
  hidden: string[];
}

function loadState(pageId: string): TileState | null {
  try {
    const raw = localStorage.getItem(STORAGE_PREFIX + pageId);
    return raw ? JSON.parse(raw) : null;
  } catch { return null; }
}

function saveState(pageId: string, state: TileState) {
  try { localStorage.setItem(STORAGE_PREFIX + pageId, JSON.stringify(state)); } catch {}
}

// ── Helpers ──

/** Get an element's layout rect, ignoring any CSS transform (critical during FLIP animations) */
function getLayoutRect(el: HTMLElement): DOMRect {
  const rect = el.getBoundingClientRect();
  const style = getComputedStyle(el);
  if (!style.transform || style.transform === "none") return rect;
  const matrix = new DOMMatrix(style.transform);
  return new DOMRect(
    rect.left - matrix.m41,
    rect.top - matrix.m42,
    rect.width,
    rect.height,
  );
}

// ── Component ──

export default function TileGrid({ pageId, tiles, columns = 4 }: TileGridProps) {
  const allIds = tiles.map(t => t.id);
  const allIdsKey = allIds.join(",");

  // Always use code-defined order (localStorage persistence disabled — drag/drop is WIP)
  const [order, setOrder] = useState<string[]>(allIds);
  const [hidden, setHidden] = useState<string[]>([]);

  // Sync when tile definitions change
  useEffect(() => {
    setOrder(prev => {
      const existing = prev.filter(id => allIds.includes(id));
      const newOnes = allIds.filter(id => !existing.includes(id));
      if (newOnes.length === 0 && existing.length === prev.length) return prev;
      return [...existing, ...newOnes];
    });
    setHidden(prev => prev.filter(id => allIds.includes(id)));
  }, [allIdsKey]);

  const [settingsOpen, setSettingsOpen] = useState(false);
  const settingsRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!settingsOpen) return;
    const handler = (e: MouseEvent) => {
      if (settingsRef.current && !settingsRef.current.contains(e.target as Node)) {
        setSettingsOpen(false);
      }
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [settingsOpen]);

  useEffect(() => {
    saveState(pageId, { order, hidden });
  }, [pageId, order, hidden]);

  // ── FLIP animation: snapshot positions before render, animate after ──
  const prevRectsRef = useRef<Map<string, DOMRect>>(new Map());
  const gridRef = useRef<HTMLDivElement>(null);

  // Snapshot current positions BEFORE DOM update
  const snapshotPositions = useCallback(() => {
    if (!gridRef.current) return;
    const map = new Map<string, DOMRect>();
    gridRef.current.querySelectorAll("[data-tile-id]").forEach(el => {
      const id = el.getAttribute("data-tile-id");
      if (id) map.set(id, el.getBoundingClientRect());
    });
    prevRectsRef.current = map;
  }, []);

  // After order changes, animate tiles from old position to new position (FLIP)
  useLayoutEffect(() => {
    if (!gridRef.current || prevRectsRef.current.size === 0) return;

    const children = gridRef.current.querySelectorAll("[data-tile-id]") as NodeListOf<HTMLElement>;
    children.forEach(el => {
      const id = el.getAttribute("data-tile-id");
      if (!id) return;
      const prevRect = prevRectsRef.current.get(id);
      if (!prevRect) return;
      const newRect = el.getBoundingClientRect();
      const dx = prevRect.left - newRect.left;
      const dy = prevRect.top - newRect.top;
      if (dx === 0 && dy === 0) return;

      // Start at old position
      el.style.transition = "none";
      el.style.transform = `translate(${dx}px, ${dy}px)`;

      // Force reflow, then animate to new position
      el.getBoundingClientRect();
      el.style.transition = "transform 0.25s cubic-bezier(0.2, 0, 0, 1)";
      el.style.transform = "";
    });

    prevRectsRef.current = new Map();
  }, [order]);

  // ── Pointer-based drag (document-level listeners for reliability) ──
  const [dragId, setDragId] = useState<string | null>(null);
  const dragRef = useRef<{
    id: string;
    pointerId: number;
    startX: number;
    startY: number;
    el: HTMLElement;
    rects: { id: string; rect: DOMRect }[];
  } | null>(null);

  const handlePointerDown = useCallback((e: React.PointerEvent, id: string) => {
    if (e.button !== 0) return;
    e.preventDefault();

    const tileEl = (e.target as HTMLElement).closest(`[data-tile-id="${id}"]`) as HTMLElement;
    if (!tileEl) return;

    // Snapshot all tile positions before drag starts
    snapshotPositions();

    const rects: { id: string; rect: DOMRect }[] = [];
    if (gridRef.current) {
      gridRef.current.querySelectorAll("[data-tile-id]").forEach(child => {
        const childId = child.getAttribute("data-tile-id");
        if (childId) rects.push({ id: childId, rect: getLayoutRect(child as HTMLElement) });
      });
    }

    dragRef.current = {
      id,
      pointerId: e.pointerId,
      startX: e.clientX,
      startY: e.clientY,
      el: tileEl,
      rects,
    };

    setDragId(id);
  }, [snapshotPositions]);

  // Use document-level listeners so drag never gets stuck
  useEffect(() => {
    if (!dragId) return;

    const onMove = (e: PointerEvent) => {
      if (!dragRef.current) return;
      e.preventDefault();

      const sourceId = dragRef.current.id;
      let targetId: string | null = null;

      for (const { id, rect } of dragRef.current.rects) {
        if (id === sourceId) continue;
        if (e.clientX >= rect.left && e.clientX <= rect.right &&
            e.clientY >= rect.top && e.clientY <= rect.bottom) {
          targetId = id;
          break;
        }
      }

      if (targetId) {
        // Snapshot before reorder for FLIP
        snapshotPositions();

        setOrder(prev => {
          const sourceIdx = prev.indexOf(sourceId);
          const targetIdx = prev.indexOf(targetId!);
          if (sourceIdx === -1 || targetIdx === -1 || sourceIdx === targetIdx) return prev;
          const newOrder = [...prev];
          newOrder.splice(sourceIdx, 1);
          newOrder.splice(targetIdx, 0, sourceId);
          return newOrder;
        });

        // Re-snapshot layout positions after DOM update (ignoring FLIP transforms)
        requestAnimationFrame(() => {
          if (!gridRef.current || !dragRef.current) return;
          const newRects: { id: string; rect: DOMRect }[] = [];
          gridRef.current.querySelectorAll("[data-tile-id]").forEach(child => {
            const childId = child.getAttribute("data-tile-id");
            if (childId) newRects.push({ id: childId, rect: getLayoutRect(child as HTMLElement) });
          });
          if (dragRef.current) dragRef.current.rects = newRects;
        });
      }
    };

    const onUp = () => {
      dragRef.current = null;
      setDragId(null);
    };

    document.addEventListener("pointermove", onMove);
    document.addEventListener("pointerup", onUp);
    document.addEventListener("pointercancel", onUp);

    return () => {
      document.removeEventListener("pointermove", onMove);
      document.removeEventListener("pointerup", onUp);
      document.removeEventListener("pointercancel", onUp);
    };
  }, [dragId, snapshotPositions]);

  // ── Toggle visibility ──
  const toggleTile = useCallback((id: string) => {
    setHidden(prev => prev.includes(id) ? prev.filter(h => h !== id) : [...prev, id]);
  }, []);

  const resetLayout = useCallback(() => {
    setOrder(allIds);
    setHidden([]);
    try { localStorage.removeItem(STORAGE_PREFIX + pageId); } catch {}
  }, [allIdsKey, pageId]);

  // ── Render ──
  const tileMap = new Map(tiles.map(t => [t.id, t]));
  const visibleTiles = order
    .filter(id => !hidden.includes(id) && tileMap.has(id))
    .map(id => tileMap.get(id)!);

  const gridClass = `tile-grid-${pageId}`;

  return (
    <div>
      <style dangerouslySetInnerHTML={{ __html: `
        .${gridClass} { display: grid; grid-template-columns: repeat(${columns}, 1fr); gap: 16px; }
        @media (max-width: 1100px) { .${gridClass} { grid-template-columns: repeat(2, 1fr); } }
        @media (max-width: 560px) { .${gridClass} { grid-template-columns: 1fr; } }
      `}} />

      {/* Settings bar */}
      <div style={{ display: "flex", justifyContent: "flex-end", marginBottom: 8, position: "relative" }} ref={settingsRef}>
        <button
          onClick={() => setSettingsOpen(v => !v)}
          style={{
            background: "none", border: "1px solid #D1D5DB", borderRadius: 6,
            padding: "4px 10px", fontSize: 12, fontWeight: 500, color: "#6B7280",
            cursor: "pointer", display: "flex", alignItems: "center", gap: 4,
          }}
          title="Customise tiles"
        >
          <svg width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round">
            <circle cx="8" cy="8" r="2.5" />
            <path d="M8 1v2M8 13v2M1 8h2M13 8h2M3.05 3.05l1.41 1.41M11.54 11.54l1.41 1.41M3.05 12.95l1.41-1.41M11.54 4.46l1.41-1.41" />
          </svg>
          Tiles
          {hidden.length > 0 && (
            <span style={{ background: "#7C3AED", color: "#fff", borderRadius: 8, fontSize: 10, padding: "1px 5px", fontWeight: 600 }}>
              {hidden.length} hidden
            </span>
          )}
        </button>

        {settingsOpen && (
          <div style={{
            position: "absolute", top: "100%", right: 0, marginTop: 4, zIndex: 30,
            background: "#fff", border: "1px solid #E5E7EB", borderRadius: 8,
            boxShadow: "0 4px 16px rgba(0,0,0,0.12)", width: 260, padding: "8px 0",
            maxHeight: 400, overflowY: "auto",
          }}>
            <div style={{ padding: "6px 14px 8px", borderBottom: "1px solid #F3F4F6", display: "flex", justifyContent: "space-between", alignItems: "center" }}>
              <span style={{ fontSize: 12, fontWeight: 600, color: "#374151" }}>Show / Hide Tiles</span>
              <button
                onClick={resetLayout}
                style={{ fontSize: 11, color: "#7C3AED", background: "none", border: "none", cursor: "pointer", fontWeight: 500 }}
              >
                Reset
              </button>
            </div>
            {order.map(id => {
              const tile = tileMap.get(id);
              if (!tile) return null;
              const isHidden = hidden.includes(id);
              return (
                <label key={id} style={{
                  display: "flex", alignItems: "center", gap: 8,
                  padding: "7px 14px", cursor: "pointer", fontSize: 13,
                  color: isHidden ? "#9CA3AF" : "#374151",
                  transition: "background 0.1s",
                }}
                  onMouseEnter={e => (e.currentTarget.style.background = "#F9FAFB")}
                  onMouseLeave={e => (e.currentTarget.style.background = "transparent")}
                >
                  <input
                    type="checkbox"
                    checked={!isHidden}
                    onChange={() => toggleTile(id)}
                    style={{ accentColor: "#7C3AED", width: 14, height: 14 }}
                  />
                  <span style={{ fontWeight: 500 }}>{tile.label}</span>
                </label>
              );
            })}
          </div>
        )}
      </div>

      {/* Tile grid */}
      <div className={gridClass} ref={gridRef}>
        {visibleTiles.map(tile => {
          const isDragging = dragId === tile.id;
          const span = tile.span || 1;
          return (
            <div
              key={tile.id}
              data-tile-id={tile.id}
              style={{
                position: "relative",
                opacity: isDragging ? 0.6 : 1,
                boxShadow: isDragging ? "0 8px 25px rgba(124, 58, 237, 0.25)" : "none",
                borderRadius: 12,
                gridColumn: span > 1 ? `span ${span}` : undefined,
                touchAction: "none",
                zIndex: isDragging ? 10 : 1,
              }}
            >
              {/* Drag handle — top-right grip icon */}
              <div
                onPointerDown={e => handlePointerDown(e, tile.id)}
                style={{
                  position: "absolute", top: 6, right: 8, zIndex: 2,
                  color: isDragging ? "#7C3AED" : "#D1D5DB", fontSize: 14, lineHeight: 1,
                  cursor: isDragging ? "grabbing" : "grab", padding: "2px 4px", borderRadius: 4,
                  userSelect: "none",
                  transition: "color 0.15s",
                }}
                title="Drag to reorder"
              >
                <svg width="14" height="14" viewBox="0 0 14 14" fill="currentColor">
                  <circle cx="4" cy="2" r="1.5" />
                  <circle cx="10" cy="2" r="1.5" />
                  <circle cx="4" cy="7" r="1.5" />
                  <circle cx="10" cy="7" r="1.5" />
                  <circle cx="4" cy="12" r="1.5" />
                  <circle cx="10" cy="12" r="1.5" />
                </svg>
              </div>
              {tile.render()}
            </div>
          );
        })}
      </div>
    </div>
  );
}
