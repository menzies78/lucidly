// productImageSync.server.js
// ─────────────────────────────────────────────────────────────────────
// Fetches product featured-image URLs from Shopify and persists them to
// Shop.productImagesJson for fast loader lookups.
//
// Also called from the daily scheduler so new products (e.g.
// "Indestructible T-shirt. Dark Grey Edition") are swept in automatically
// — otherwise the image map only refreshes when the Products page loads
// AND the existing DB cache has aged past 24 h, which meant new launches
// could render with broken thumbs for a day.
//
// Uses the same parent/variant resolution as app.products.tsx loader so
// the DB payload stays read-compatible.

import db from "../db.server";
import { unauthenticated } from "../shopify.server";
// Use the rollup's canonicaliser so image-map keys match rollup keys —
// otherwise Vollebak-style trailing-period titles ("Planet Earth Suit Jacket.")
// produce keys that diverge from the period-stripped rollup product names.
import { toParentProduct } from "./productRollups.server";

/**
 * Refresh the product image map for a shop from Shopify GraphQL.
 * Writes Shop.productImagesJson + Shop.productImagesUpdatedAt.
 * Returns { count, shopDomain } on success or throws.
 */
export async function refreshProductImages(shopDomain) {
  const { admin } = await unauthenticated.admin(shopDomain);
  const imgMap = {};
  let hasNext = true;
  let cursor = null;
  while (hasNext) {
    const query = `#graphql
      query GetProductImages($cursor: String) {
        products(first: 250, after: $cursor) {
          edges {
            node { title featuredImage { url } }
            cursor
          }
          pageInfo { hasNextPage }
        }
      }`;
    const resp = await admin.graphql(query, { variables: { cursor } });
    const data = await resp.json();
    const edges = data?.data?.products?.edges || [];
    for (const edge of edges) {
      const title = edge.node.title;
      const url = edge.node.featuredImage?.url;
      if (title && url) {
        imgMap[title] = url;
        const parent = toParentProduct(title);
        if (!imgMap[parent]) imgMap[parent] = url;
      }
    }
    hasNext = data?.data?.products?.pageInfo?.hasNextPage || false;
    cursor = edges.length > 0 ? edges[edges.length - 1].cursor : null;
  }
  await db.shop.update({
    where: { shopDomain },
    data: {
      productImagesJson: JSON.stringify(imgMap),
      productImagesUpdatedAt: new Date(),
    },
  });
  return { shopDomain, count: Object.keys(imgMap).length };
}
