// Free-tier tile gate.
//
// Wrap any tile/section body. When `gated` is true the children are replaced
// by a blurred PLACEHOLDER rendering — never real data behind CSS blur (the
// paywall must hold in devtools). Titles/subtitles/section copy live outside
// the wrapper and stay readable by design: the free tier sells with labels,
// not numbers.
//
// Loader contract: when the shop plan gates a tile, the loader must NOT run
// that tile's queries — ship `null`/omitted data and `gated: true`. This
// component is only the visual half of the gate.

import { BlockStack, Box, Button, Card, InlineStack, Text } from "@shopify/polaris";
import type { TileDef } from "./TileGrid";

// Deterministic fake numbers so the blur has realistic shapes under it.
// Static — same for every shop, carries zero information.
const PLACEHOLDER_ROWS = [
  ["£24.3k", "1,204", "38%", "£61"],
  ["£18.7k", "982", "41%", "£54"],
  ["£11.2k", "647", "29%", "£72"],
];

/**
 * Gate a TileGrid's TileDef array for the free plan. Tiles whose id is in
 * `freeIds` render untouched; every other tile keeps its LABEL as a readable
 * heading (the free tier sells with labels) over a blurred placeholder body.
 * No-op when `gated` is false — paid rendering is byte-identical.
 */
export function gateTileDefs(tiles: TileDef[], gated: boolean, freeIds: Set<string>): TileDef[] {
  if (!gated) return tiles;
  return tiles.map((t) =>
    freeIds.has(t.id)
      ? t
      : {
          ...t,
          render: () => (
            <Card>
              <BlockStack gap="300">
                <Text as="h2" variant={(t.span || 1) >= 2 ? "headingLg" : "headingSm"}>
                  {t.label}
                </Text>
                <GatedTile gated minHeight={(t.span || 1) >= 2 ? 200 : 80}>
                  {null}
                </GatedTile>
              </BlockStack>
            </Card>
          ),
        },
  );
}

export function GatedTile({
  gated,
  children,
  minHeight = 120,
  cta = "Unlock with Lucidly",
  onUpgrade,
}: {
  gated: boolean;
  children: React.ReactNode;
  minHeight?: number;
  cta?: string;
  onUpgrade?: () => void;
}) {
  if (!gated) return <>{children}</>;
  return (
    <Box position="relative" minHeight={`${minHeight}px`}>
      {/* Placeholder content, blurred. aria-hidden: it's decorative noise. */}
      <div
        aria-hidden
        style={{
          filter: "blur(7px)",
          userSelect: "none",
          pointerEvents: "none",
          opacity: 0.55,
          minHeight,
          overflow: "hidden",
        }}
      >
        <BlockStack gap="300">
          {PLACEHOLDER_ROWS.map((row, i) => (
            <InlineStack key={i} gap="600" wrap={false}>
              {row.map((v, j) => (
                <Text key={j} as="span" variant="headingLg">
                  {v}
                </Text>
              ))}
            </InlineStack>
          ))}
        </BlockStack>
      </div>
      {/* Unlock overlay */}
      <div
        style={{
          position: "absolute",
          inset: 0,
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
        }}
      >
        <Button variant="primary" onClick={onUpgrade} url={onUpgrade ? undefined : "/app/plan"}>
          {cta}
        </Button>
      </div>
    </Box>
  );
}
