// Short-lived signed URLs for backup tarball downloads.
//
// Why we need this: the dashboard download button does a top-level
// `window.location.href` navigation - that browser GET cannot carry a
// Shopify App Bridge session token, so any /app/* route bounces to the
// merchant login page. Solution: park the download route under /api/*
// (outside the embedded auth scope) and gate it with an HMAC signature
// produced by the already-authenticated dashboard loader.
//
// Token format: base64url(HMAC-SHA256(`${shop}|${backupId}|${exp}`, secret))
// passed alongside ?shop & ?exp. exp is a unix-ms expiry. Default 30 min
// gives Andy plenty of time to click Download but limits exposure if a
// link is accidentally shared.

import crypto from "node:crypto";

const DEFAULT_TTL_MS = 30 * 60 * 1000;

function getSecret() {
  const s = process.env.SHOPIFY_API_SECRET;
  if (!s) throw new Error("SHOPIFY_API_SECRET not set - backup download URLs cannot be signed");
  return s;
}

function hmac(payload) {
  return crypto.createHmac("sha256", getSecret()).update(payload).digest("base64url");
}

/**
 * Build a signed download URL for a backup. Caller must already have
 * verified the requester is authorised (e.g. isInternalShop in the
 * dashboard loader) - this just produces the URL.
 */
export function signDownloadUrl(shopDomain, backupId, ttlMs = DEFAULT_TTL_MS) {
  const exp = Date.now() + ttlMs;
  const sig = hmac(`${shopDomain}|${backupId}|${exp}`);
  const qs = new URLSearchParams({ shop: shopDomain, exp: String(exp), sig });
  return `/api/backup-download/${encodeURIComponent(backupId)}?${qs.toString()}`;
}

/**
 * Validate a signed URL. Returns { ok: true, shop } on success or
 * { ok: false, reason } on any failure. Constant-time signature compare
 * prevents timing oracles.
 */
export function verifyDownloadToken({ shop, backupId, exp, sig }) {
  if (!shop || !backupId || !exp || !sig) return { ok: false, reason: "missing params" };
  const expMs = Number(exp);
  if (!Number.isFinite(expMs)) return { ok: false, reason: "bad exp" };
  if (Date.now() > expMs) return { ok: false, reason: "expired" };

  const expected = hmac(`${shop}|${backupId}|${expMs}`);
  // base64url is fixed-length for sha256 (43 chars) - safe to compare directly.
  let a, b;
  try {
    a = Buffer.from(expected, "base64url");
    b = Buffer.from(sig, "base64url");
  } catch {
    return { ok: false, reason: "bad sig encoding" };
  }
  if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) {
    return { ok: false, reason: "bad sig" };
  }
  return { ok: true, shop };
}
