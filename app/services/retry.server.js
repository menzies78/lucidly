// Generic retry wrapper for non-Meta external calls (ECB, Shopify GraphQL, etc).
// Meta API calls have their own specialised retry in metaFetch.server.js.

const MAX_ATTEMPTS = 3;
const BASE_DELAY_MS = 1000;

/**
 * Retries an async operation up to 3 times with exponential backoff.
 * @param {Function} fn - async function to retry
 * @param {string} label - log label
 * @param {Object} opts - { maxAttempts, baseDelayMs, shouldRetry(err): bool }
 */
export async function withRetry(fn, label = "external", opts = {}) {
  const maxAttempts = opts.maxAttempts ?? MAX_ATTEMPTS;
  const baseDelayMs = opts.baseDelayMs ?? BASE_DELAY_MS;
  const shouldRetry = opts.shouldRetry ?? defaultShouldRetry;

  let lastErr;
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      return await fn();
    } catch (err) {
      lastErr = err;
      if (attempt >= maxAttempts || !shouldRetry(err)) {
        console.error(`[${label}] failed after ${attempt} attempt(s): ${err.message}`);
        throw err;
      }
      const delay = baseDelayMs * Math.pow(2, attempt - 1); // 1s, 2s, 4s
      console.warn(`[${label}] attempt ${attempt}/${maxAttempts} failed: ${err.message} — retrying in ${delay}ms`);
      await new Promise(r => setTimeout(r, delay));
    }
  }
  throw lastErr;
}

// Retry on network errors, 5xx, 429, and JSON parse errors.
// Don't retry on 4xx (bad request / auth / not found).
function defaultShouldRetry(err) {
  const msg = err.message || "";
  if (/status 5\d\d/.test(msg)) return true;
  if (/status 429/.test(msg)) return true;
  if (/network|ECONNRESET|ETIMEDOUT|ENOTFOUND|EAI_AGAIN|fetch failed/i.test(msg)) return true;
  if (/JSON|Unexpected token/i.test(msg)) return true;
  // 4xx: don't retry
  if (/status 4\d\d/.test(msg)) return false;
  // Unknown errors: retry (better to try than silently drop)
  return true;
}
