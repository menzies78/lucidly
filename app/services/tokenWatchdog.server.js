// Token Health Watchdog.
//
// Purpose: proactively detect the offline-token "halt" class BEFORE a merchant
// notices — the exact family of faults that silently took Vollebak's background
// sync (and later the public app's Fit Test) offline:
//
//   1. non-expiring token rejected — Shopify now 403s non-expiring offline
//      tokens on the Admin API ("Non-expiring access tokens are no longer
//      accepted"). Every background Admin call (order sync, Fit Test, ingest,
//      product images) dies. This is what broke the public app when the
//      expiring-tokens flag was misconfigured.
//   2. token revoked / invalid — a 401 from the Admin API means the stored
//      offline token no longer works (merchant revoked, scope change, rotation
//      lost the token). Background work 401s until re-auth.
//   3. refresh failed — the single-use refresh-token rotation raced (the
//      concurrency bug that removed Vollebak's orders/updated webhook), or the
//      refresh token expired. Surfaced in real time via the doRefresh hook in
//      offlineToken.server.js, and proactively here when a probe's refresh throws.
//
// Mechanism: every cycle, for each installed shop (offline session present), run
// a cheap synthetic Admin API call through the SAME path background jobs use
// (getOfflineAdmin → refresh-if-needed → Admin call). Classify the outcome and
// alert (deduped) on failure, resolve on recovery. Because it exercises the real
// path on a short cadence, a token that's about to halt sync gets flagged while
// it's still only affecting a synthetic probe — not the merchant's dashboard.

import db from "../db.server";
import { apiVersion } from "../shopify.server";
import { getOfflineAdmin } from "./offlineToken.server.js";
import { alertOps, resolveOps } from "./opsAlert.server.js";

// Cadence. Tight enough to catch the public app's hourly-TTL token entering
// trouble within a cycle or two, cheap enough (one `{ shop { name } }` query per
// shop) to run against every install without rate-limit concern.
const WATCHDOG_MS = 12 * 60 * 1000; // every 12 minutes

const PROBE_QUERY = "{ shop { name myshopifyDomain } }";

// The signature Shopify returns when it rejects a non-expiring token. Matched
// case-insensitively against the 403 body so we can tell this apart from an
// ordinary permission 403.
const NON_EXPIRING_MARKER = "non-expiring access tokens are no longer accepted";

/**
 * Probe one shop's offline token by making a real Admin API call through the
 * background path. Returns { ok, kind, detail } where kind is one of:
 * "healthy" | "refresh_failed" | "non_expiring_rejected" | "unauthorized" |
 * "forbidden" | "shop_gone" | "no_session" | "error".
 */
export async function probeShopToken(shopDomain) {
  let session;
  try {
    // Exercises the refresh path: on the public app a token within 5 min of
    // expiry refreshes here under the single-flight lock. A refresh failure
    // (invalid_grant / rotation race / expired refresh token) throws out of
    // getOfflineAdmin and is the highest-signal fault we can catch proactively.
    const res = await getOfflineAdmin(shopDomain);
    session = res?.session;
    if (!session?.accessToken) {
      return { ok: false, kind: "no_session", detail: "no offline session/token" };
    }
  } catch (err) {
    return { ok: false, kind: "refresh_failed", detail: err?.message?.slice(0, 300) || String(err) };
  }

  // Raw Admin GraphQL call so we can read the exact status + body (the library
  // client masks 401/403 as thrown errors and hides the non-expiring marker).
  let httpRes;
  try {
    httpRes = await fetch(`https://${shopDomain}/admin/api/${apiVersion}/graphql.json`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": session.accessToken,
      },
      body: JSON.stringify({ query: PROBE_QUERY }),
    });
  } catch (err) {
    // Network-level failure — not a token fault. Report as transient error so
    // it alerts (something is wrong) but is classified distinctly.
    return { ok: false, kind: "error", detail: `probe fetch failed: ${err?.message || err}` };
  }

  if (httpRes.ok) {
    // 200 doesn't guarantee no GraphQL errors, but for `{ shop { name } }` an OK
    // status with a data payload means the token is accepted and working.
    const body = await httpRes.json().catch(() => ({}));
    if (body?.data?.shop) return { ok: true, kind: "healthy", detail: body.data.shop.name };
    return { ok: false, kind: "error", detail: `200 but no shop in payload: ${JSON.stringify(body).slice(0, 200)}` };
  }

  const text = await httpRes.text().catch(() => "");
  const lower = text.toLowerCase();
  if (httpRes.status === 404) {
    // The whole Admin endpoint is Not Found — the store itself no longer exists
    // (deleted / deauthorized), not a token fault. Most common cause: an
    // ephemeral Shopify App Review sandbox store that Shopify tore down, leaving
    // a stale Session row we never got an uninstall webhook for. Distinct kind so
    // the caller can suppress the page (it's not actionable).
    return { ok: false, kind: "shop_gone", detail: `404: ${text.slice(0, 200)}` };
  }
  if (httpRes.status === 401) {
    return { ok: false, kind: "unauthorized", detail: `401: ${text.slice(0, 200)}` };
  }
  if (httpRes.status === 403) {
    if (lower.includes(NON_EXPIRING_MARKER)) {
      return { ok: false, kind: "non_expiring_rejected", detail: `403: ${text.slice(0, 200)}` };
    }
    return { ok: false, kind: "forbidden", detail: `403: ${text.slice(0, 200)}` };
  }
  return { ok: false, kind: "error", detail: `${httpRes.status}: ${text.slice(0, 200)}` };
}

const SEVERITY_BY_KIND = {
  non_expiring_rejected: "critical",
  refresh_failed: "critical",
  unauthorized: "warn",
  forbidden: "warn",
  no_session: "warn",
  error: "warn",
};

const SUMMARY_BY_KIND = {
  non_expiring_rejected:
    "Shopify is rejecting this shop's offline token as non-expiring. Every background Admin call (order sync, Fit Test, ingest, product images) will fail with 403. Check EXPIRING_OFFLINE_TOKENS on the app minting this shop's token, then delete the stale offline session so token-exchange re-mints an expiring one.",
  refresh_failed:
    "This shop's offline token refresh FAILED. Likely an invalid_grant from the single-use refresh-token rotation racing (the class that removed Vollebak's orders/updated webhook), or an expired refresh token. Background Admin work will 401 until a fresh token is minted.",
  unauthorized:
    "This shop's offline token was rejected with 401 (revoked, scope change, or lost rotation). Background Admin work is dead until the merchant re-auths (next embedded load token-exchanges a fresh token).",
  forbidden:
    "This shop's Admin API returned 403 (not the non-expiring marker). Possible scope/permission issue — investigate.",
  no_session:
    "No offline session/token is stored for this shop despite an install record. Background Admin work cannot run.",
  error:
    "A synthetic Admin probe for this shop failed with an unexpected error. Investigate.",
};

/**
 * Probe every installed shop's offline token and alert (deduped) on any fault,
 * resolving the alert when a previously-faulted shop comes back healthy.
 * Returns a summary array for logging/inspection.
 */
export async function checkAllTokens() {
  // Every installed shop has an offline session id "offline_{shop}". Uninstalled
  // shops have no session (deleted by the app/uninstalled webhook) so they're
  // correctly skipped — a token we can't use isn't a fault worth paging on.
  const sessions = await db.session.findMany({
    where: { id: { startsWith: "offline_" } },
    select: { shop: true },
  });

  const results = [];
  for (const { shop } of sessions) {
    const key = `token:${shop}`;
    let r;
    try {
      r = await probeShopToken(shop);
    } catch (err) {
      r = { ok: false, kind: "error", detail: err?.message || String(err) };
    }
    results.push({ shop, ...r });

    if (r.ok) {
      await resolveOps(key, {
        subject: `Offline token healthy again — ${shop}`,
        title: `Token recovered for ${shop}`,
        bodyHtml: `<p>A previously-faulting offline token for <strong>${shop}</strong> is accepted by the Admin API again. Background sync should be running normally.</p>`,
        bodyText: `Offline token healthy again for ${shop}.`,
      });
      continue;
    }

    if (r.kind === "shop_gone") {
      // Store is gone (404), not a token fault — don't page. Clear any prior
      // alert for this shop and log it. We deliberately DON'T auto-delete the
      // Session here (the watchdog stays read-only); a stale session for a dead
      // store is harmless and is cleaned up on the next uninstall/reinstall.
      await resolveOps(key, {
        subject: `Shop gone — ${shop}`,
        title: `Shop no longer exists — ${shop}`,
        bodyHtml: `<p><strong>${shop}</strong> now returns 404 from the Admin API — the store has been deleted or deauthorized (commonly an ephemeral App Review sandbox). No action needed; this is not a token fault.</p>`,
        bodyText: `Shop ${shop} returns 404 (store gone) — not a token fault, no action needed.`,
      });
      console.log(`[TokenWatchdog] ${shop}: 404 (shop gone) — not paging (stale session for a deleted store)`);
      continue;
    }

    const severity = SEVERITY_BY_KIND[r.kind] || "warn";
    const summary = SUMMARY_BY_KIND[r.kind] || "Offline token fault.";
    await alertOps(key, {
      severity,
      subject: `Offline token fault (${r.kind}) — ${shop}`,
      title: `Offline token fault for ${shop}: ${r.kind}`,
      bodyHtml: `<p>${summary}</p><p style="margin-top:12px;"><strong>Shop:</strong> ${shop}<br/><strong>Kind:</strong> ${r.kind}<br/><strong>Detail:</strong> <code style="font-size:12px;">${(r.detail || "").replace(/</g, "&lt;")}</code></p>`,
      bodyText: `${summary}\n\nShop: ${shop}\nKind: ${r.kind}\nDetail: ${r.detail}`,
    });
  }

  const faults = results.filter((r) => !r.ok);
  if (faults.length) {
    console.warn(`[TokenWatchdog] ${faults.length}/${results.length} shop(s) faulting: ${faults.map((f) => `${f.shop}=${f.kind}`).join(", ")}`);
  } else {
    console.log(`[TokenWatchdog] all ${results.length} shop(s) healthy`);
  }
  return results;
}

/**
 * Start the periodic watchdog. Singleton-guarded on globalThis so HMR / repeated
 * startScheduler calls don't stack intervals. First run is delayed a few minutes
 * so a fresh deploy's tokens have settled before we probe.
 */
export function startTokenWatchdog() {
  if (globalThis.__lucidlyTokenWatchdog) clearInterval(globalThis.__lucidlyTokenWatchdog);
  if (globalThis.__lucidlyTokenWatchdogBoot) clearTimeout(globalThis.__lucidlyTokenWatchdogBoot);

  console.log("[TokenWatchdog] Starting — synthetic Admin probe every 12 min");
  globalThis.__lucidlyTokenWatchdogBoot = setTimeout(() => {
    checkAllTokens().catch((err) => console.error("[TokenWatchdog] first run failed:", err.message));
    globalThis.__lucidlyTokenWatchdog = setInterval(() => {
      checkAllTokens().catch((err) => console.error("[TokenWatchdog] cycle failed:", err.message));
    }, WATCHDOG_MS);
  }, 3 * 60_000);
}
