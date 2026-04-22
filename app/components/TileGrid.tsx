import React from "react";

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

// ── Component ──
// Simple CSS-grid layout. Tiles render in the order supplied. No drag/drop,
// no show/hide, no persistence — the source-code order is the source of truth.

export default function TileGrid({ pageId, tiles, columns = 4 }: TileGridProps) {
  const gridClass = `tile-grid-${pageId}`;

  return (
    <div>
      <style dangerouslySetInnerHTML={{ __html: `
        .${gridClass} { display: grid; grid-template-columns: repeat(${columns}, 1fr); gap: 16px; }
        @media (max-width: 1100px) { .${gridClass} { grid-template-columns: repeat(2, 1fr); } }
        @media (max-width: 560px) { .${gridClass} { grid-template-columns: 1fr; } }
      `}} />

      <div className={gridClass}>
        {tiles.map(tile => {
          const span = tile.span || 1;
          return (
            <div
              key={tile.id}
              data-tile-id={tile.id}
              style={{
                position: "relative",
                borderRadius: 12,
                gridColumn: span > 1 ? `span ${span}` : undefined,
              }}
            >
              {tile.render()}
            </div>
          );
        })}
      </div>
    </div>
  );
}
