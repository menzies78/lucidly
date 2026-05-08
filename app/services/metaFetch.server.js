// Shared Meta API fetch layer. Sits on top of metaGovernor.server.js
// which does all per-(app, ad_account, BUC) tracking, pacing, and
// concurrency. This file owns retry semantics + error classification
// only.
//
// Key principles:
//  1. Reads X-App-Usage, X-Business-Use-Case-Usage, X-FB-Ads-Insights-Throttle
//     via metaGovernor on every response.
//  2. Proactively paces against the highest util % across all signals
//     for the targeted ad account.
//  3. NEVER silently drops data on rate limits - retries indefinitely.
//  4. Error code 1 ("reduce data") throws ReduceDataError so callers
//     can split ranges.

import {
  reportHeaders,
  paceBeforeRequest,
  accountKeyFromUrl,
  markBlocked,
  getEffectiveUtil,
  withSlot,
} from "./metaGovernor.server.js";

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
//  - Rate limits: retries indefinitely with exponential backoff (cap 120s)
//  - Error subcode 1504022: parks the account via the governor, then retries
//  - Error code 1 ("reduce data"): throws ReduceDataError immediately
//  - Real errors (bad token, invalid params): retries 3 times then throws
//  - Network errors: retries 5 times then throws
//
// The accountKey argument lets callers explicitly bind a request to an
// ad account that wouldn't be parseable from the URL (rare). When omitted
// we infer from the URL (/act_xxx/...).
export async function fetchWithRetry(url, label = "MetaAPI", accountKey = null) {
  const MAX_REAL_ERROR_RETRIES = 3;
  const MAX_NETWORK_RETRIES = 5;
  const acctKey = accountKey || accountKeyFromUrl(url);

  let realErrorCount = 0;
  let networkErrorCount = 0;
  let rateLimitRetries = 0;

  // Slot acquisition wraps the whole retry loop so we don't release the
  // slot just to immediately re-acquire it on backoff. The slot caps
  // *parallelism per account*, not total requests.
  return withSlot(acctKey, async () => {
    while (true) {
      // Pace against current util before issuing the request.
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

        if (isReduceData) {
          throw new ReduceDataError(`${label}: ${errMsg}`);
        }

        if (isAccountThrottled) {
          // Meta has explicitly blocked the account. Park it for 60s and
          // let the governor's blockedUntil gate hold off the next call.
          markBlocked(acctKey, 60);
          rateLimitRetries++;
          console.warn(`[${label}] Account throttled (subcode 1504022) on ${acctKey}, parked 60s`);
          continue;
        }

        if (isRateLimit) {
          rateLimitRetries++;
          const wait = Math.min(3000 * Math.pow(2, rateLimitRetries - 1), 120000);
          console.warn(`[${label}] Rate limited (retry #${rateLimitRetries}) on ${acctKey}, util=${getEffectiveUtil(acctKey)}%, waiting ${Math.round(wait/1000)}s`);
          await new Promise(r => setTimeout(r, wait));
          continue;
        }

        realErrorCount++;
        console.error(`[${label}] API error (${realErrorCount}/${MAX_REAL_ERROR_RETRIES}): [${errCode}] ${errMsg}`);
        if (realErrorCount >= MAX_REAL_ERROR_RETRIES) {
          throw new Error(`${label}: API error after ${MAX_REAL_ERROR_RETRIES} retries: [${errCode}] ${errMsg}`);
        }
        await new Promise(r => setTimeout(r, 3000));
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

// Fetch all pages from a paginated Meta response. Each page goes through
// fetchWithRetry which handles slot/governor on its own.
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
