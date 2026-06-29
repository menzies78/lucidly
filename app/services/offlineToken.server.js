import { sessionStorage, unauthenticated } from "../shopify.server";

/**
 * Concurrency-safe offline access token refresh.
 *
 * Background context: when offline access tokens expire (mandatory for the
 * public App Store distribution), the shopify-app-remix library can refresh
 * them automatically inside request-path auth (authenticate.webhook,
 * unauthenticated.admin). But that auto-refresh has no per-shop locking: a
 * burst of concurrent webhooks each load the SAME stored (stale) session and
 * each fire their own refresh. Shopify rotates the refresh token on every use
 * and invalidates the prior one, so concurrent refreshes race — the first wins,
 * the rest get invalid_grant and the library throws a 500. Enough 500s and
 * Shopify removes the webhook subscription (the incident that started this).
 *
 * The fix: the library's auto-refresh is disabled (the
 * `expiringOfflineAccessTokens` future flag is OFF in shopify.server.ts), so
 * request-path auth never refreshes — webhook handlers don't need a live admin
 * token (pure DB work), so they can no longer 500 under concurrency. The ONLY
 * path that genuinely needs a live offline token is background Shopify Admin
 * API work, and it routes through here, where refresh happens under a per-shop
 * single-flight lock.
 *
 * On custom-distribution apps (Vollebak / HM) offline tokens never expire, so
 * `session.expires` is null and the refresh branch is simply never taken — the
 * same code is correct on all three apps.
 */

// Refresh when the token is within this window of expiry. Mirrors the 5-minute
// buffer the library itself used, so we refresh before a request-path call
// would have needed to.
const REFRESH_BUFFER_MS = 5 * 60 * 1000;

// Per-shop single-flight guard: concurrent refreshes for the same shop collapse
// onto one in-flight promise so we never invalidate our own refresh token.
const inflight = new Map(); // shopDomain -> Promise<void>

function offlineSessionId(shop) {
  return `offline_${shop}`;
}

function isExpiring(session) {
  // No `expires` => non-expiring (custom app) token => never refresh.
  if (!session?.expires) return false;
  return new Date(session.expires).getTime() - Date.now() <= REFRESH_BUFFER_MS;
}

async function doRefresh(shop, session) {
  if (!session?.refreshToken) {
    // Nothing to refresh with. On a custom app this never happens (no expiry);
    // on the public app a missing refresh token means the merchant must
    // re-auth, which the next embedded request's token exchange handles.
    return;
  }
  const res = await fetch(`https://${shop}/admin/oauth/access_token`, {
    method: "POST",
    headers: { "Content-Type": "application/json", Accept: "application/json" },
    body: JSON.stringify({
      client_id: process.env.SHOPIFY_API_KEY,
      client_secret: process.env.SHOPIFY_API_SECRET,
      refresh_token: session.refreshToken,
      grant_type: "refresh_token",
    }),
  });
  if (!res.ok) {
    const detail = await res.text().catch(() => "");
    throw new Error(
      `[offlineToken] refresh failed for ${shop}: ${res.status} ${detail.slice(0, 200)}`,
    );
  }
  // { access_token, scope, expires_in, refresh_token, refresh_token_expires_in }
  const data = await res.json();

  session.accessToken = data.access_token;
  if (data.scope) session.scope = data.scope;
  session.expires = data.expires_in
    ? new Date(Date.now() + data.expires_in * 1000)
    : undefined;
  if (data.refresh_token && data.refresh_token_expires_in) {
    session.refreshToken = data.refresh_token;
    session.refreshTokenExpires = new Date(
      Date.now() + data.refresh_token_expires_in * 1000,
    );
  }
  await sessionStorage.storeSession(session);
  console.log(
    `[offlineToken] refreshed offline token for ${shop} (expires ${session.expires?.toISOString?.() || "n/a"})`,
  );
}

/**
 * Ensure the stored offline session has a live access token, refreshing under a
 * per-shop single-flight lock when it's expiring (or when `force` is set, e.g.
 * after a 401 where the stored `expires` still looks valid but the token was
 * revoked/rotated elsewhere). No-op on custom apps whose tokens never expire.
 *
 * Returns the (possibly refreshed) session, or null if none is stored.
 */
export async function ensureValidOfflineSession(shop, { force = false } = {}) {
  const session = await sessionStorage.loadSession(offlineSessionId(shop));
  if (!session) return null;
  if (!force && !isExpiring(session)) return session;

  let p = inflight.get(shop);
  if (!p) {
    p = doRefresh(shop, session).finally(() => inflight.delete(shop));
    inflight.set(shop, p);
  }
  await p;
  // Re-load to pick up whatever the winning refresh persisted.
  return sessionStorage.loadSession(offlineSessionId(shop));
}

/**
 * Background Shopify Admin API client built from a guaranteed-fresh offline
 * token. Use this for ALL background / scheduled admin work instead of calling
 * unauthenticated.admin() directly — with library auto-refresh disabled, the
 * raw call hands back an expired token on the public app and 401s.
 */
export async function getOfflineAdmin(shop, opts) {
  await ensureValidOfflineSession(shop, opts);
  return unauthenticated.admin(shop);
}
