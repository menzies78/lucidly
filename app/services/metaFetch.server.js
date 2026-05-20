// Shared Meta API fetch layer. Sits on top of metaGovernor.server.js
// (BUC-aware pacing) and accountLock.server.js (one-in-flight per account).
//
// Key principles:
//  1. Reads X-App-Usage, X-Business-Use-Case-Usage, X-FB-Ads-Insights-Throttle
//     via metaGovernor on every response.
//  2. ONE in-flight call per ad account at a time, app-wide. Different
//     accounts run in parallel. This prevents the thundering-herd loop we
//     hit when 10 parallel calls all tripped subcode 1504022, each parked
//     for 60s, all unparked together, all tripped again.
//  3. NEVER silently drops data on rate limits - retries indefinitely.
//  4. Error code 1 ("reduce data") throws ReduceDataError so callers can
//     split ranges.

import {
  reportHeaders,
  paceBeforeRequest,
  accountKeyFromUrl,
  markBlocked,
  getEffectiveUtil,
} from "./metaGovernor.server.js";
import { withAccountLock } from "./accountLock.server.js";

// Special error class for "reduce the amount of data" - callers can catch
// and retry with smaller range.
export class ReduceDataError extends Error {
  constructor(message) {
    super(message);
    this.name = "ReduceDataError";
  }
}

// Back-compat shim. Old callers asked for getMetaApiUsage() to display a
// single number; we surface the worst observed util across all accounts
// so the meaning is consistent.
export function getMetaApiUsage() {
  return getEffectiveUtil();
}

// Resilient fetch that NEVER silently drops data on rate limits.
//  - Rate limits (code 4/17): retries indefinitely with exponential backoff (cap 120s)
//  - Error subcode 1504022: parks account for max(60s, BUC.estimated × 1.5), retries
//  - Error code 1 ("reduce data"): throws ReduceDataError immediately
//  - Transient errors (code 2 "Service temporarily unavailable" — Meta's own
//    docs say retry): treated like a rate-limit, retried indefinitely with
//    exponential backoff (cap 120s). Killed HM's 2026-05-19 breakdowns-historical
//    phase after only 3 retries × 3s.
//  - Other real errors (bad token, invalid params): retries 8 times with
//    exponential backoff (cap 60s) then throws. Bumped from 3 retries × 3s
//    after the same HM failure — many "real" errors are actually transient
//    edge proxy hiccups and benefit from more aggressive retry.
//  - Network errors: retries 5 times then throws
//
// The accountKey argument lets callers explicitly bind a request to an
// ad account that wouldn't be parseable from the URL (rare). When omitted
// we infer from the URL (/act_xxx/...).
export async function fetchWithRetry(url, label = "MetaAPI", accountKey = null) {
  const MAX_REAL_ERROR_RETRIES = 8;
  const MAX_NETWORK_RETRIES = 5;
  const acctKey = accountKey || accountKeyFromUrl(url);

  let realErrorCount = 0;
  let networkErrorCount = 0;
  let rateLimitRetries = 0;

  // Account lock holds across the entire retry loop so backoffs don't release
  // and re-acquire (which would let other in-flight calls jump in front and
  // re-trigger the throttle we're trying to recover from).
  return withAccountLock(acctKey, async () => {
    while (true) {
      // Pace against current util (and any blockedUntil) before issuing.
      await paceBeforeRequest(acctKey);

      try {
        const res = await fetch(url);
        reportHeaders(res, acctKey);

        let data;
        try {
          data = await res.json();
        } catch {
          networkErrorCount++;
          console.error(`[${label}] JSON parse error (${networkErrorCount}/${MAX_NETWORK_RETRIES})`);
          if (networkErrorCount >= MAX_NETWORK_RETRIES) {
            throw new Error(`${label}: JSON parse failed after ${MAX_NETWORK_RETRIES} attempts`);
          }
          await new Promise(r => setTimeout(r, 3000));
          continue;
        }

        if (!data.error) return data;

        const errMsg = data.error.message || "";
        const errCode = data.error.code;
        const errSubcode = data.error.error_subcode;
        const isRateLimit = errCode === 4 || errCode === 17
          || errMsg.includes("request limit") || errMsg.includes("too many calls");
        const isReduceData = errCode === 1 && errMsg.includes("reduce the amount of data");
        const isAccountThrottled = errSubcode === 1504022;
        // Code 2 is Meta's "Service temporarily unavailable" — Meta's own docs
        // mark it retryable. Treating it like a rate-limit (indefinite retry
        // with exponential backoff) prevents a 3-retry-then-die failure mode
        // we saw on HM 2026-05-19.
        const isTransientService = errCode === 2
          || errMsg.includes("Service temporarily unavailable");

        if (isReduceData) {
          throw new ReduceDataError(`${label}: ${errMsg}`);
        }

        if (isAccountThrottled) {
          // Trust BUC.estimated_time_to_regain_access if present (already
          // captured by metaGovernor.parseBucHeader from the response).
          // Otherwise default to 60s. Pad by 50% (community-validated).
          // Floor at 60s, ceiling at 30 min so we don't sleep forever on a
          // bad estimate.
          const estimateSec = readBlockEstimateSec(acctKey);
          const cooldown = Math.min(30 * 60, Math.max(60, Math.round(estimateSec * 1.5)));
          markBlocked(acctKey, cooldown);
          rateLimitRetries++;
          console.warn(`[${label}] Account throttled (subcode 1504022) on ${acctKey}, parked ${cooldown}s (est=${estimateSec}s)`);
          continue;
        }

        if (isRateLimit || isTransientService) {
          rateLimitRetries++;
          const wait = Math.min(3000 * Math.pow(2, rateLimitRetries - 1), 120000);
          const reason = isTransientService ? "Transient service error" : "Rate limited";
          console.warn(`[${label}] ${reason} (retry #${rateLimitRetries}) on ${acctKey}, util=${getEffectiveUtil(acctKey)}%, code=${errCode}, waiting ${Math.round(wait/1000)}s`);
          await new Promise(r => setTimeout(r, wait));
          continue;
        }

        realErrorCount++;
        console.error(`[${label}] API error (${realErrorCount}/${MAX_REAL_ERROR_RETRIES}): [${errCode}] ${errMsg}`);
        if (realErrorCount >= MAX_REAL_ERROR_RETRIES) {
          throw new Error(`${label}: API error after ${MAX_REAL_ERROR_RETRIES} retries: [${errCode}] ${errMsg}`);
        }
        // Exponential backoff (3s, 6s, 12s, ..., cap 60s) so transient edge
        // proxy hiccups get a chance to recover instead of failing the phase.
        const realErrorWait = Math.min(3000 * Math.pow(2, realErrorCount - 1), 60000);
        await new Promise(r => setTimeout(r, realErrorWait));
      } catch (err) {
        if (err instanceof ReduceDataError) throw err;
        if (err.message && err.message.startsWith(`${label}:`)) throw err;
        networkErrorCount++;
        console.error(`[${label}] Network error (${networkErrorCount}/${MAX_NETWORK_RETRIES}): ${err.message}`);
        if (networkErrorCount >= MAX_NETWORK_RETRIES) {
          throw new Error(`${label}: Network error after ${MAX_NETWORK_RETRIES} retries: ${err.message}`);
        }
        await new Promise(r => setTimeout(r, 3000));
      }
    }
  });
}

// Pull the most recent block-estimate (seconds) for this account. Stored on
// the governor record by parseBucHeader when Meta returns
// estimated_time_to_regain_access. Returns 0 if not available.
function readBlockEstimateSec(acctKey) {
  // metaGovernor exports markBlocked which sets blockedUntil based on
  // a seconds value, but the estimate is parsed from the BUC header into
  // record.blockedUntil already. Read the residual time from blockedUntil
  // (set by parseBucHeader before this call returned an error).
  // If parseBucHeader didn't fire (no header), blockedUntil is 0.
  // We expose this indirectly via getEffectiveUtil's record.
  // To avoid circular imports we just compute from the governor's snapshot
  // surface - cheap.
  try {
    // Lazy import to avoid pulling the snapshot fn into hot paths above.
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const { snapshot } = require("./metaGovernor.server.js");
    const acct = snapshot().accounts[acctKey];
    return acct ? Math.max(0, acct.blockedFor) : 0;
  } catch {
    return 0;
  }
}

// Fetch all pages from a paginated Meta response. Each page goes through
// fetchWithRetry which handles lock + governor + retries.
export async function fetchAllPages(initialUrl, label = "MetaAPI", accountKey = null) {
  const allRows = [];
  let url = initialUrl;
  while (url) {
    const data = await fetchWithRetry(url, label, accountKey);
    if (!data.data) break;
    allRows.push(...(data.data || []));
    url = data.paging?.next || null;
  }
  return allRows;
}
