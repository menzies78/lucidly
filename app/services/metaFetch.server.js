// Shared Meta API fetch layer with rate limit awareness.
// Used by metaSync, metaBreakdownSync, and metaEntitySync.
//
// Key principles:
// 1. Read x-app-usage header to know current rate limit consumption
// 2. Proactively slow down when usage is high (don't wait for the wall)
// 3. NEVER silently drop data — rate limits retry indefinitely, only real errors bail
// 4. Error code 1 ("reduce data") throws ReduceDataError so callers can split ranges

// Tracks current API usage across all callers (singleton in-process)
let currentUsage = 0; // 0-100, from x-app-usage header

export function getMetaApiUsage() {
  return currentUsage;
}

// Special error class for "reduce the amount of data" — callers can catch and retry with smaller range
export class ReduceDataError extends Error {
  constructor(message) {
    super(message);
    this.name = "ReduceDataError";
  }
}

function updateUsageFromHeaders(res) {
  try {
    // Meta returns: x-app-usage: {"call_count":28,"total_cputime":5,"total_time":12}
    const header = res.headers.get("x-app-usage");
    if (header) {
      const usage = JSON.parse(header);
      // The highest of the three percentages is the effective usage
      currentUsage = Math.max(usage.call_count || 0, usage.total_cputime || 0, usage.total_time || 0);
    }
  } catch {
    // Ignore parse errors on usage header
  }
}

// Proactive throttle: if usage is high, wait before making the next call.
// This prevents us from slamming into the wall.
async function throttleIfNeeded(label) {
  if (currentUsage >= 90) {
    // Very close to limit — pause 30s to let window roll
    console.warn(`[${label}] API usage at ${currentUsage}%, pausing 30s to let window roll...`);
    await new Promise(r => setTimeout(r, 30000));
  } else if (currentUsage >= 75) {
    // Getting warm — slow down with 10s pause
    console.warn(`[${label}] API usage at ${currentUsage}%, slowing down (10s)...`);
    await new Promise(r => setTimeout(r, 10000));
  } else if (currentUsage >= 50) {
    // Moderate — small pause
    await new Promise(r => setTimeout(r, 2000));
  }
  // Under 50% — full speed
}

// Resilient fetch that NEVER silently drops data on rate limits.
// - Rate limits: retries indefinitely with exponential backoff (capped at 120s waits)
// - Error code 1 ("reduce data"): throws ReduceDataError immediately (caller should split range)
// - Real errors (bad token, invalid params): retries 3 times then throws
// - Network errors: retries 5 times then throws
export async function fetchWithRetry(url, label = "MetaAPI") {
  const MAX_REAL_ERROR_RETRIES = 3;
  const MAX_NETWORK_RETRIES = 5;
  let realErrorCount = 0;
  let networkErrorCount = 0;
  let rateLimitRetries = 0;

  while (true) {
    // Proactive throttle based on known usage
    await throttleIfNeeded(label);

    try {
      const res = await fetch(url);
      updateUsageFromHeaders(res);

      let data;
      try {
        data = await res.json();
      } catch (e) {
        networkErrorCount++;
        console.error(`[${label}] JSON parse error (attempt ${networkErrorCount}/${MAX_NETWORK_RETRIES})`);
        if (networkErrorCount >= MAX_NETWORK_RETRIES) {
          throw new Error(`${label}: JSON parse failed after ${MAX_NETWORK_RETRIES} attempts`);
        }
        await new Promise(r => setTimeout(r, 3000));
        continue;
      }

      if (!data.error) {
        return data; // Success
      }

      // Error response from Meta
      const errMsg = data.error.message || "";
      const errCode = data.error.code;
      const isRateLimit = errCode === 4 || errCode === 17 ||
        errMsg.includes("request limit") || errMsg.includes("too many calls");
      const isReduceData = errCode === 1 && errMsg.includes("reduce the amount of data");

      if (isReduceData) {
        // Don't retry — throw immediately so caller can split the range
        throw new ReduceDataError(`${label}: ${errMsg}`);
      }

      if (isRateLimit) {
        rateLimitRetries++;
        // Exponential backoff: 6s, 12s, 24s, 48s, 60s, 90s, 120s, 120s...
        const wait = Math.min(3000 * Math.pow(2, rateLimitRetries - 1), 120000);
        console.warn(`[${label}] Rate limited (retry #${rateLimitRetries}), usage=${currentUsage}%, waiting ${Math.round(wait/1000)}s...`);
        await new Promise(r => setTimeout(r, wait));
        continue; // NEVER give up on rate limits
      }

      // Real API error (bad token, invalid field, etc)
      realErrorCount++;
      console.error(`[${label}] API error (${realErrorCount}/${MAX_REAL_ERROR_RETRIES}): [${errCode}] ${errMsg}`);
      if (realErrorCount >= MAX_REAL_ERROR_RETRIES) {
        throw new Error(`${label}: API error after ${MAX_REAL_ERROR_RETRIES} retries: [${errCode}] ${errMsg}`);
      }
      await new Promise(r => setTimeout(r, 3000));

    } catch (err) {
      if (err instanceof ReduceDataError) throw err; // Pass through
      if (err.message.startsWith(`${label}:`)) throw err; // Re-throw our own errors
      networkErrorCount++;
      console.error(`[${label}] Network error (${networkErrorCount}/${MAX_NETWORK_RETRIES}): ${err.message}`);
      if (networkErrorCount >= MAX_NETWORK_RETRIES) {
        throw new Error(`${label}: Network error after ${MAX_NETWORK_RETRIES} retries: ${err.message}`);
      }
      await new Promise(r => setTimeout(r, 3000));
    }
  }
}

// Fetch all pages from a paginated Meta API response
export async function fetchAllPages(initialUrl, label = "MetaAPI") {
  const allRows = [];
  let url = initialUrl;
  while (url) {
    const data = await fetchWithRetry(url, label);
    if (!data.data) break;
    allRows.push(...(data.data || []));
    url = data.paging?.next || null;
  }
  return allRows;
}
