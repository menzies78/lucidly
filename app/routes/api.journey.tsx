import type { ActionFunctionArgs } from "@remix-run/node";
import db from "../db.server";
import { isPaidMetaUtm } from "../utils/utmClassification.js";
import { verifyPixelToken } from "../utils/pixelToken.server";

// Public ingest endpoint for the Lucidly web pixel (Layer 1 click-journey).
//
// Why /api/* with no authenticate.admin: the pixel runs in a storefront
// sandbox and has no Shopify admin session to present (same reasoning as
// api.ad-thumbnail). Instead every post carries a per-shop HMAC token minted
// by the server at webPixelCreate time and verified here - see pixelToken.server.
//
// The pixel sends the body as text/plain (not application/json) and uses no
// custom headers, which keeps each POST a CORS "simple request" - no OPTIONS
// preflight to handle. We still echo Access-Control-Allow-Origin so the pixel
// can read the {ok:true} ack.
//
// Two payload shapes (discriminated by `type`):
//   touch  - a Meta-tagged storefront landing (one JourneyTouch row)
//   order  - checkout_completed, linking a visitor (clientId) to an order
//
// Stitching touches → orders happens later (Phase 2); this route only captures.

const CORS = { "Access-Control-Allow-Origin": "*" } as const;

function ok(body: object, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json", ...CORS },
  });
}

// Defensive trim - pixel payloads come off the open internet. Cap field
// lengths so a malformed/abusive post can't bloat a row.
function s(v: unknown, max = 512): string {
  if (typeof v !== "string") return "";
  return v.slice(0, max);
}

function parseDate(v: unknown): Date {
  if (typeof v === "number" && Number.isFinite(v)) return new Date(v);
  if (typeof v === "string") {
    const d = new Date(v);
    if (!Number.isNaN(d.getTime())) return d;
  }
  return new Date();
}

export const action = async ({ request }: ActionFunctionArgs) => {
  if (request.method !== "POST") return ok({ ok: false, error: "method" }, 405);

  let payload: Record<string, unknown>;
  try {
    payload = JSON.parse(await request.text());
  } catch {
    return ok({ ok: false, error: "bad json" }, 400);
  }

  const shop = s(payload.shop, 255).toLowerCase().trim();
  if (!shop || !verifyPixelToken(shop, s(payload.token, 128))) {
    return ok({ ok: false, error: "unauthorized" }, 401);
  }

  const clientId = s(payload.clientId, 128);
  if (!clientId) return ok({ ok: false, error: "no clientId" }, 400);

  const type = s(payload.type, 16);

  if (type === "order") {
    await db.journeyOrderLink.create({
      data: {
        shopDomain: shop,
        clientId,
        shopifyOrderId: s(payload.orderId, 64) || null,
        checkoutToken: s(payload.checkoutToken, 128),
        occurredAt: parseDate(payload.occurredAt),
      },
    });
    return ok({ ok: true });
  }

  if (type === "touch") {
    const source = s(payload.source, 128);
    const medium = s(payload.medium, 128);
    const fbclid = s(payload.fbclid, 256);
    // "Paid Meta" if the UTM source/medium says so OR an fbclid is present
    // (fbclid only ever rides on a Meta ad click). The stitch step uses this
    // flag to find ad-driven touches without re-parsing UTMs.
    const isPaidMeta = isPaidMetaUtm(source, medium) || !!fbclid;
    await db.journeyTouch.create({
      data: {
        shopDomain: shop,
        clientId,
        occurredAt: parseDate(payload.occurredAt),
        source,
        medium,
        campaign: s(payload.campaign, 256),
        content: s(payload.content, 256),
        term: s(payload.term, 256),
        fbclid,
        landingPath: s(payload.landingPath, 512),
        isPaidMeta,
        rawUrl: s(payload.rawUrl, 1024),
      },
    });
    return ok({ ok: true });
  }

  return ok({ ok: false, error: "unknown type" }, 400);
};
