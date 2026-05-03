// Tracks whether the in-process scheduler is currently running a sync
// cycle (hourly or daily). The status is read by /app/api/sync-status and
// rendered by the LoadingIndicator pill so the merchant knows why a tab
// click feels sluggish during the cycle.
//
// Stored on globalThis so HMR doesn't lose the flag and so multiple
// imports share the same value (per perf_shared_cache_gotcha — module
// scope is not stable in Vite/Remix server builds).

const KEY = "__lucidlySyncStatus__";

function get() {
  return globalThis[KEY] || null;
}

function set(value) {
  globalThis[KEY] = value;
}

/** Mark a sync as running. Stack-aware so nested marks don't clobber. */
export function markSyncStart(label) {
  const current = get();
  // Keep a tiny stack so the daily cycle (which calls into hourly-style
  // helpers) doesn't lose its label when the inner call ends first.
  const stack = current?.stack ? [...current.stack, label] : [label];
  set({ running: true, label: stack[stack.length - 1], stack, startedAt: Date.now() });
}

export function markSyncEnd() {
  const current = get();
  if (!current) return;
  if (current.stack && current.stack.length > 1) {
    const stack = current.stack.slice(0, -1);
    set({ running: true, label: stack[stack.length - 1], stack, startedAt: current.startedAt });
    return;
  }
  set(null);
}

export function getSyncStatus() {
  const s = get();
  if (!s || !s.running) return { running: false };
  return { running: true, label: s.label, startedAt: s.startedAt };
}
