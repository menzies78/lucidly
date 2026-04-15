/**
 * Tiny in-memory LRU with TTL for loader results.
 *
 * The analytics loaders are expensive (multi-table aggregation) but the
 * underlying data only changes on sync ticks. Cache them for a short TTL so
 * tab-switching, profile toggles, and repeat dashboard views are instant.
 *
 * Invalidated per-shop at the end of each incremental sync so newly matched
 * conversions surface immediately after a cycle completes.
 */

type Entry = { value: any; expiresAt: number };

declare global {
  // eslint-disable-next-line no-var
  var __lucidlyQueryCache: Map<string, Entry> | undefined;
}

const MAX_ENTRIES = 500;
// Default TTL for loader caches. Sync cycles invalidate per-shop caches via
// invalidateShop(), so stale data never lives past the next sync anyway.
// A longer TTL means cache entries survive between user visits that span
// more than the 5 min default (e.g. user comes back after lunch).
export const DEFAULT_TTL = 2 * 60 * 60 * 1000; // 2 hours

// CRITICAL: use globalThis so the cache is shared across module instances.
// Vite/Remix bundles chunk this file into multiple output files, so a
// module-scoped `const cache = new Map()` creates SEPARATE instances for each
// importer. The warmer would then populate one copy while the loader reads a
// different (empty) copy. Storing the Map on globalThis forces all importers
// to share a single cache.
const cache: Map<string, Entry> =
  globalThis.__lucidlyQueryCache ?? (globalThis.__lucidlyQueryCache = new Map<string, Entry>());

export async function cached<T>(
  key: string,
  ttlMs: number,
  fn: () => Promise<T>
): Promise<T> {
  const now = Date.now();
  const hit = cache.get(key);
  if (hit && hit.expiresAt > now) {
    // Move to end for LRU ordering
    cache.delete(key);
    cache.set(key, hit);
    return hit.value as T;
  }
  const value = await fn();
  cache.set(key, { value, expiresAt: now + ttlMs });
  if (cache.size > MAX_ENTRIES) {
    // Evict oldest (Map preserves insertion order)
    const firstKey = cache.keys().next().value;
    if (firstKey !== undefined) cache.delete(firstKey);
  }
  return value;
}

export function invalidateShop(shopDomain: string) {
  const prefix = `${shopDomain}:`;
  let removed = 0;
  for (const k of cache.keys()) {
    if (k.startsWith(prefix)) {
      cache.delete(k);
      removed++;
    }
  }
  if (removed > 0) console.log(`[queryCache] invalidated ${removed} entries for ${shopDomain}`);
}

export function invalidateAll() {
  cache.clear();
}

export function cacheStats() {
  return { size: cache.size, max: MAX_ENTRIES };
}
