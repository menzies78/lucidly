// Ensures the Lucidly Journey web pixel is registered + configured for a shop.
// Idempotent. Called on app load per process per shop (alongside ensureWebhooks).
//
// A shop can hold at most ONE web pixel per app, so we query the current one and
// either create it or update its settings to match. Settings carry everything
// the sandboxed pixel needs to authenticate its ingest posts: shop domain, the
// app's /api/journey URL, and a per-shop HMAC token (see pixelToken.server).
//
// The token is derived statelessly from the shop domain + app secret, so a
// settings drift (or a secret rotation) is self-healed on the next load: we
// always overwrite the stored settings with freshly-computed values.
import { pixelToken } from "../utils/pixelToken.server";

const done = new Set(); // in-memory cache: shops already reconciled this process lifetime

export async function ensurePixel(shopDomain, accessToken) {
  if (done.has(shopDomain)) return { skipped: true };

  const appUrl = process.env.SHOPIFY_APP_URL;
  if (!appUrl) {
    console.warn("[ensurePixel] SHOPIFY_APP_URL not set, skipping");
    return { skipped: true };
  }

  const ingestUrl = `${appUrl.replace(/\/$/, "")}/api/journey`;
  const settings = {
    shop: shopDomain,
    ingestUrl,
    token: pixelToken(shopDomain),
  };
  const settingsJson = JSON.stringify(settings);

  const gql = async (query, variables) => {
    const res = await fetch(`https://${shopDomain}/admin/api/2025-01/graphql.json`, {
      method: "POST",
      headers: { "X-Shopify-Access-Token": accessToken, "Content-Type": "application/json" },
      body: JSON.stringify({ query, variables }),
    });
    return res.json();
  };

  // Does this shop already have our web pixel?
  const cur = await gql(`{ webPixel { id settings } }`);
  const existing = cur?.data?.webPixel || null;

  if (existing) {
    // Only push an update if the stored settings actually differ - avoids a
    // needless mutation on every load.
    let same = false;
    try {
      same = existing.settings === settingsJson ||
        JSON.stringify(JSON.parse(existing.settings)) === settingsJson;
    } catch {
      same = false;
    }
    if (same) {
      done.add(shopDomain);
      return { unchanged: true };
    }
    const upd = await gql(
      `mutation($id: ID!, $webPixel: WebPixelInput!) {
        webPixelUpdate(id: $id, webPixel: $webPixel) {
          webPixel { id }
          userErrors { field message }
        }
      }`,
      { id: existing.id, webPixel: { settings: settingsJson } }
    );
    const errs = upd?.data?.webPixelUpdate?.userErrors || [];
    if (errs.length) {
      console.error(`[ensurePixel] ${shopDomain} update errors:`, errs);
      return { error: errs };
    }
    console.log(`[ensurePixel] ${shopDomain} settings updated`);
    done.add(shopDomain);
    return { updated: true };
  }

  // No pixel yet - create it.
  const create = await gql(
    `mutation($webPixel: WebPixelInput!) {
      webPixelCreate(webPixel: $webPixel) {
        webPixel { id }
        userErrors { field message }
      }
    }`,
    { webPixel: { settings: settingsJson } }
  );
  const errs = create?.data?.webPixelCreate?.userErrors || [];
  if (errs.length) {
    console.error(`[ensurePixel] ${shopDomain} create errors:`, errs);
    return { error: errs };
  }
  console.log(`[ensurePixel] ${shopDomain} created → ${ingestUrl}`);
  done.add(shopDomain);
  return { created: true };
}
