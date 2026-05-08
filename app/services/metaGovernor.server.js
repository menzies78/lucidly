// Meta API rate-limit governor.
//
// Replaces the previous global currentUsage gauge in metaFetch.server.js.
// Reads three Meta response headers and tracks usage per ad-account so a
// heavy backfill on one shop doesn't throttle the whole fleet.
//
// Headers we read (per Meta Marketing API docs, May 2026):
//
//  X-App-Usage
//    {"call_count":28,"total_cputime":5,"total_time":12}
//    App-wide. % of the app's hourly budget. Single value across all
//    callers - if we burn this down on one shop, every shop suffers.
//
//  X-Business-Use-Case-Usage
//    {"act_123":[{"type":"ads_insights","call_count":42,"total_cputime":11,
//                 "total_time":8,"estimated_time_to_regain_access":0}]}
//    Per (ad-account, BUC type) - this is the lever Meta actually
//    throttles on. We log the highest util per account.
//
//  X-FB-Ads-Insights-Throttle
//    {"app_id_util_pct":7,"acc_id_util_pct":31,"ads_api_access_tier":"standard_access"}
//    Insights-specific. acc_id_util_pct is the cleanest single number for
//    ad-account-level throttle - we use this as the primary signal.
//
// Concurrency model:
// - Each ad-account has a soft slot pool. Default 5 concurrent.
// - As acc_id_util_pct climbs, the pool shrinks: at 60% we're at 3 slots,
//   at 80% we're at 1, at 95% we hold all new work for 30s.
// - Slots are cooperative - callers acquire/release via withSlot().
//
// On error subcode 1504022 ("ad-account temporarily blocked due to high
// load"), the governor reads estimated_time_to_regain_access and parks
// the entire account for that long.

const usageByAccount = new Map(); // ad_account_id -> { app, buc, insights, blockedUntil }
const APP_USAGE = { call_count: 0, total_cputime: 0, total_time: 0, updatedAt: 0 };

// In-flight slot tracking
const slotsInUse = new Map(); // ad_account_id -> number

const DEFAULT_KEY = "__app__";

function getRecord(key) {
  let r = usageByAccount.get(key);
  if (!r) {
    r = {
      bucMaxPct: 0,        // max % across all BUC types for this account
      insightsAccPct: 0,   // X-FB-Ads-Insights-Throttle.acc_id_util_pct
      blockedUntil: 0,     // epoch ms; 0 means not blocked
      updatedAt: 0,
    };
    usageByAccount.set(key, r);
  }
  return r;
}

/**
 * Parse X-Business-Use-Case-Usage header into max-pct-per-account.
 * Returns the max util % we observed (0-100+).
 */
function parseBucHeader(header) {
  if (!header) return 0;
  let parsed;
  try { parsed = JSON.parse(header); } catch { return 0; }
  let globalMax = 0;
  for (const [acctId, entries] of Object.entries(parsed)) {
    if (!Array.isArray(entries)) continue;
    let acctMax = 0;
    let estimatedRegain = 0;
    for (const e of entries) {
      const m = Math.max(
        Number(e.call_count) || 0,
        Number(e.total_cputime) || 0,
        Number(e.total_time) || 0,
      );
      if (m > acctMax) acctMax = m;
      if (e.estimated_time_to_regain_access > estimatedRegain) {
        estimatedRegain = e.estimated_time_to_regain_access;
      }
    }
    const r = getRecord(acctId);
    r.bucMaxPct = acctMax;
    r.updatedAt = Date.now();
    if (estimatedRegain > 0) {
      r.blockedUntil = Date.now() + estimatedRegain * 1000;
    }
    if (acctMax > globalMax) globalMax = acctMax;
  }
  return globalMax;
}

function parseInsightsThrottleHeader(header, accountKey) {
  if (!header) return;
  let parsed;
  try { parsed = JSON.parse(header); } catch { return; }
  const r = getRecord(accountKey);
  r.insightsAccPct = Number(parsed.acc_id_util_pct) || 0;
  // We could also surface app_id_util_pct here; X-App-Usage already
  // covers that channel so we don't double-track.
  r.updatedAt = Date.now();
}

function parseAppUsageHeader(header) {
  if (!header) return;
  let parsed;
  try { parsed = JSON.parse(header); } catch { return; }
  APP_USAGE.call_count = Number(parsed.call_count) || 0;
  APP_USAGE.total_cputime = Number(parsed.total_cputime) || 0;
  APP_USAGE.total_time = Number(parsed.total_time) || 0;
  APP_USAGE.updatedAt = Date.now();
}

/**
 * Public: read all rate-limit headers from a Meta response.
 * Call once per response. accountKey can be the ad-account-id (preferred)
 * or DEFAULT_KEY for non-account-scoped calls (e.g. /me/adaccounts).
 */
export function reportHeaders(res, accountKey = DEFAULT_KEY) {
  parseAppUsageHeader(res.headers.get("x-app-usage"));
  parseBucHeader(res.headers.get("x-business-use-case-usage"));
  parseInsightsThrottleHeader(res.headers.get("x-fb-ads-insights-throttle"), accountKey);
}

/**
 * Compute the effective utilisation % we should pace against for a given
 * account. Takes the max of: app-wide usage, this account's BUC usage,
 * and this account's insights throttle.
 */
export function getEffectiveUtil(accountKey = DEFAULT_KEY) {
  const r = getRecord(accountKey);
  const appMax = Math.max(APP_USAGE.call_count, APP_USAGE.total_cputime, APP_USAGE.total_time);
  return Math.max(appMax, r.bucMaxPct, r.insightsAccPct);
}

/**
 * Compute current concurrency budget for an ad-account.
 * 5 slots when util < 50%, sliding down to 1 at 80%, 0 over 95%.
 */
function concurrencyLimit(util) {
  if (util >= 95) return 0;
  if (util >= 80) return 1;
  if (util >= 65) return 2;
  if (util >= 50) return 3;
  return 5;
}

/**
 * How long to wait before the next request when we're hot. Different
 * pacing curve from concurrencyLimit because "wait" is per-call and
 * "concurrency" is per-account.
 */
export async function paceBeforeRequest(accountKey = DEFAULT_KEY) {
  const r = getRecord(accountKey);
  const now = Date.now();
  if (r.blockedUntil > now) {
    const wait = r.blockedUntil - now;
    console.warn(`[metaGovernor] ${accountKey}: account blocked until +${Math.round(wait/1000)}s, waiting...`);
    await new Promise(res => setTimeout(res, wait));
  }
  const util = getEffectiveUtil(accountKey);
  if (util >= 90) {
    console.warn(`[metaGovernor] ${accountKey}: util ${util}%, sleeping 30s`);
    await new Promise(res => setTimeout(res, 30000));
  } else if (util >= 75) {
    await new Promise(res => setTimeout(res, 8000));
  } else if (util >= 60) {
    await new Promise(res => setTimeout(res, 3000));
  }
  // < 60% — full speed.
}

/**
 * Acquire a concurrency slot for an account. Wraps an async fn; releases
 * the slot whether it succeeds or throws. This is what callers should
 * use to honour the per-account parallelism budget.
 *
 * Slots are cooperative: we don't queue new arrivals strictly, we just
 * loop with a 250ms backoff until we're under the limit. This keeps the
 * implementation simple at the cost of a tiny amount of throughput.
 */
export async function withSlot(accountKey, fn) {
  const key = accountKey || DEFAULT_KEY;
  while (true) {
    const inUse = slotsInUse.get(key) || 0;
    const util = getEffectiveUtil(key);
    const limit = concurrencyLimit(util);
    if (limit === 0) {
      // Hard hold: wait for budget to recover
      await new Promise(r => setTimeout(r, 5000));
      continue;
    }
    if (inUse < limit) {
      slotsInUse.set(key, inUse + 1);
      try {
        return await fn();
      } finally {
        slotsInUse.set(key, Math.max(0, (slotsInUse.get(key) || 1) - 1));
      }
    }
    await new Promise(r => setTimeout(r, 250));
  }
}

/**
 * Mark an account as temporarily blocked. Caller passes seconds to wait.
 * Used when Meta returns subcode 1504022 directly (rather than via the
 * estimated_time_to_regain_access in the BUC header).
 */
export function markBlocked(accountKey, seconds) {
  const r = getRecord(accountKey || DEFAULT_KEY);
  r.blockedUntil = Date.now() + seconds * 1000;
}

/**
 * Snapshot for diagnostics / dashboard surfaces.
 */
export function snapshot() {
  const accounts = {};
  for (const [k, v] of usageByAccount.entries()) {
    accounts[k] = {
      bucMaxPct: v.bucMaxPct,
      insightsAccPct: v.insightsAccPct,
      blockedFor: v.blockedUntil > Date.now()
        ? Math.round((v.blockedUntil - Date.now()) / 1000)
        : 0,
      slotsInUse: slotsInUse.get(k) || 0,
    };
  }
  return {
    appUsage: { ...APP_USAGE },
    accounts,
  };
}

/**
 * Best-effort parse of an ad-account ID from a Meta URL. Used as the
 * default account key when callers don't pass one. Returns DEFAULT_KEY
 * if the URL isn't ad-account-scoped (e.g. /me/adaccounts).
 */
export function accountKeyFromUrl(url) {
  if (!url) return DEFAULT_KEY;
  const m = String(url).match(/\/act_(\d+)/);
  if (!m) return DEFAULT_KEY;
  return `act_${m[1]}`;
}
