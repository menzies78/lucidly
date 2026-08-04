// Free-tier tile gate.
//
// Wrap any tile/section body. When `gated` is true the children are replaced
// by a pre-blurred SCREENSHOT (from /gated/*.jpg — sample-store data blurred
// at the pixel level before it ever entered the repo) or, where no capture
// exists, a static placeholder. Never real merchant data behind CSS blur —
// the paywall must hold in devtools. Titles/subtitles/section copy live
// outside the wrapper and stay readable by design: the free tier sells with
// labels, not numbers.
//
// Loader contract: when the shop plan gates a tile, the loader must NOT run
// that tile's queries — ship `null`/omitted data and `gated: true`. This
// component is only the visual half of the gate.

import { BlockStack, Box, Button, Card, InlineStack, Text } from "@shopify/polaris";
import type { TileDef } from "./TileGrid";

// tileId/section → pre-blurred sample screenshot under public/gated/.
// Captured from the internal demo dataset, Gaussian-blurred at the pixel
// level (radius 10-20) — carries layout and colour, zero readable data.
export const GATED_IMAGES: Record<string, string> = {
  // Customers
  metaBreakdownByDay: "/gated/customers-breakdown-by-day.jpg",
  ltvOverview: "/gated/customers-ltv-explorer.jpg",
  weeklyCohortRevenue: "/gated/customers-weekly-cohort.jpg",
  orderExplorer: "/gated/customers-order-explorer.jpg",
  totalMetaCustomers: "/gated/tile-totalMetaCustomers.jpg",
  totalMetaRevenue: "/gated/tile-totalMetaRevenue.jpg",
  newMetaCustomers: "/gated/tile-newMetaCustomers.jpg",
  newMetaRevenue: "/gated/tile-newMetaRevenue.jpg",
  metaAov: "/gated/tile-metaAov.jpg",
  aovCpa: "/gated/tile-aovCpa.jpg",
  repeatCustomers: "/gated/tile-repeatCustomers.jpg",
  newCustCpa: "/gated/tile-newCustCpa.jpg",
  // Products
  productDemographicsExplorer: "/gated/products-demographics.jpg",
  refundRate: "/gated/products-refund-rate.jpg",
  firstPurchases: "/gated/products-first-purchases.jpg",
  revenueByProduct: "/gated/products-revenue-by-product.jpg",
  basketAnalysis: "/gated/products-basket-analysis.jpg",
  topAddons: "/gated/products-top-addons.jpg",
  entryToLtv: "/gated/products-entry-to-ltv.jpg",
  productJourney: "/gated/products-journey.jpg",
  productBreakdown: "/gated/products-breakdown-table.jpg",
  // Ads
  newCustomers: "/gated/tile-newCustomers.jpg",
  newCustRevenue: "/gated/tile-newCustRevenue.jpg",
  newCustRoas: "/gated/tile-newCustRoas.jpg",
  newCustCostPerOrder: "/gated/tile-newCustCostPerOrder.jpg",
  topAdsNewCustomers: "/gated/ads-top-ads-new-customers.jpg",
  platformPerf: "/gated/ads-platform-performance.jpg",
  placementPerf: "/gated/ads-placement-performance.jpg",
  adPerformance: "/gated/ads-ad-performance-table.jpg",
  // Geo
  geoMapExplorer: "/gated/geo-map-explorer.jpg",
  geoVips: "/gated/geo-vips.jpg",
  // Weekly
  weeklySummary: "/gated/weekly-summary.jpg",
  weeklyDaily: "/gated/weekly-daily.jpg",
  // Changes
  changesActivity: "/gated/changes-activity.jpg",
};

// Deterministic fake numbers for tiles with no captured screenshot.
const PLACEHOLDER_ROWS = [
  ["£24.3k", "1,204", "38%", "£61"],
  ["£18.7k", "982", "41%", "£54"],
  ["£11.2k", "647", "29%", "£72"],
];

/**
 * Gate a TileGrid's TileDef array for the free plan. Tiles whose id is in
 * `freeIds` render untouched; every other tile keeps its LABEL as a readable
 * heading over a blurred sample screenshot (GATED_IMAGES[tile.id]) or
 * placeholder. No-op when `gated` is false — paid rendering is byte-identical.
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
                <GatedTile gated imageSrc={GATED_IMAGES[t.id]} minHeight={(t.span || 1) >= 2 ? 200 : 80}>
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
  imageSrc,
  minHeight = 120,
  cta = "Unlock with Lucidly",
  onUpgrade,
}: {
  gated: boolean;
  children: React.ReactNode;
  imageSrc?: string;
  minHeight?: number;
  cta?: string;
  onUpgrade?: () => void;
}) {
  if (!gated) return <>{children}</>;
  return (
    <Box position="relative" minHeight={`${minHeight}px`}>
      {imageSrc ? (
        // Pre-blurred sample screenshot — pixels carry no real data.
        <img
          src={imageSrc}
          alt=""
          aria-hidden
          draggable={false}
          style={{
            display: "block",
            width: "100%",
            minHeight,
            objectFit: "cover",
            objectPosition: "top",
            borderRadius: 8,
            userSelect: "none",
            pointerEvents: "none",
            opacity: 0.9,
          }}
        />
      ) : (
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
      )}
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
