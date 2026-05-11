// Per-Meta-ad-account FIFO mutex.
//
// Why this exists:
//   Meta gates rate limits per ad account. Two parallel callers hitting the
//   same account each see a clean rate-limit budget at issue-time, but Meta
//   sees the combined load and starts returning subcode 1504022 ("ad-account
//   temporarily blocked due to high load"). The previous "soft slot pool"
//   (5 concurrent) bypassed this because each in-flight call independently
//   hit the wall and parked - thundering herd, no progress.
//
// What it does:
//   withAccountLock(accountKey, fn) guarantees that only one fn() runs per
//   accountKey at a time, app-wide. Different accounts run in parallel.
//   Calls queue in FIFO order so a long backfill on one account doesn't
//   starve the hourly cron for the same account - they interleave naturally.
//
// In-process only:
//   The lock is a Map in module memory. We run a single Fly machine, so
//   one process == one global mutex. If we ever scale to multiple machines
//   for the same shop's account, we'll need a Redis lock; until then this
//   is correct and zero-overhead.

// Use globalThis to survive Vite/Remix module reloads in dev. Same trick
// used by other in-memory singletons (progress map, syncStatus).
const GLOBAL_KEY = "__lucidly_meta_account_locks__";
if (!globalThis[GLOBAL_KEY]) globalThis[GLOBAL_KEY] = new Map();
const tails = globalThis[GLOBAL_KEY]; // accountKey -> Promise (queue tail)

/**
 * Run fn with the account lock held. fn is invoked exactly once and its
 * return value is forwarded. If fn throws, the lock is still released.
 *
 * Implementation: we keep one Promise per account that resolves when the
 * current holder releases. New arrivals chain onto it via .then(), then
 * replace the tail with their own promise. This is FIFO and safe under
 * concurrent acquisition because Map.set is atomic in single-threaded JS.
 */
export async function withAccountLock(accountKey, fn) {
  const key = accountKey || "__default__";
  const prev = tails.get(key) || Promise.resolve();

  let release;
  const next = new Promise((r) => { release = r; });
  // Tail must catch upstream rejections so subsequent callers don't see them.
  tails.set(key, prev.then(() => next, () => next));

  await prev.catch(() => {}); // wait our turn; ignore prior errors
  try {
    return await fn();
  } finally {
    release();
    // No explicit GC of the Map entry - it holds one Promise per active
    // account and the entry is overwritten by the next acquire(). Steady-state
    // size == number of accounts ever locked; no per-call leak.
  }
}

/**
 * Best-effort: return the queue depth for an account (1 = lock held, no
 * waiters). Useful for diagnostics endpoints.
 */
export function queueDepth(accountKey) {
  return tails.has(accountKey) ? 1 : 0; // we don't track waiters explicitly
}
