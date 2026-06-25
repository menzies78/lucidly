import { createHmac, timingSafeEqual } from "node:crypto";

// Per-shop token for the Lucidly web pixel's ingest calls.
//
// The pixel runs in a sandbox on the merchant storefront and POSTs touches to
// the public /api/journey route (no admin session available there). To stop
// anyone from spraying junk into a shop's JourneyTouch table, every post
// carries a token that only the server can mint.
//
// Stateless by design: token = HMAC-SHA256(shopDomain) keyed by the app's
// SHOPIFY_API_SECRET. The server hands it to the pixel inside webPixelCreate
// settings at registration time, and recomputes + compares it on each ingest
// call - no column, no token table, nothing to rotate or leak from the DB.
// Per-app secret means a token minted for one Lucidly app can't be replayed
// against another.
function secret(): string {
  const s = process.env.SHOPIFY_API_SECRET;
  if (!s) throw new Error("SHOPIFY_API_SECRET is not set");
  return s;
}

export function pixelToken(shopDomain: string): string {
  return createHmac("sha256", secret()).update(shopDomain.toLowerCase().trim()).digest("hex");
}

export function verifyPixelToken(shopDomain: string, token: string | undefined | null): boolean {
  if (!token) return false;
  const expected = pixelToken(shopDomain);
  // timingSafeEqual throws on length mismatch - guard first.
  if (token.length !== expected.length) return false;
  try {
    return timingSafeEqual(Buffer.from(token), Buffer.from(expected));
  } catch {
    return false;
  }
}
