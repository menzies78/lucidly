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

const COLORS = new Set([
  "black", "cream", "grey", "blue", "white", "red", "oyster", "pink",
  "chartreuse", "multi", "rose", "camel", "navy", "lilac", "magenta",
  "natural", "ecru", "green", "brown", "khaki", "orange", "yellow",
  "teal", "coral", "ivory", "taupe", "beige", "stone", "tan", "nude",
  "gold", "silver", "burgundy", "terracotta", "olive",
]);

function toParentProduct(name) {
  const parts = name.trim().split(" ");
  if (parts.length <= 1) return name.trim();
  if (parts.length >= 3 && parts[parts.length - 3]?.toLowerCase() === "acid" && parts[parts.length - 2]?.toLowerCase() === "wash") {
    if (COLORS.has(parts[parts.length - 1].toLowerCase())) return parts.slice(0, -3).join(" ");
    return parts.slice(0, -2).join(" ");
  }
  if (parts.length >= 2 && parts[parts.length - 2]?.toLowerCase() === "acid" && parts[parts.length - 1]?.toLowerCase() === "wash") {
    return parts.slice(0, -2).join(" ");
  }
  if (COLORS.has(parts[parts.length - 1].toLowerCase())) return parts.slice(0, -1).join(" ");
  return name.trim();
}

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
