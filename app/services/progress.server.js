// In-memory progress tracking for long-running tasks.
// Works for single-process deployments (dev + single-instance prod).

const progress = new Map();

// How long to keep terminal (complete/error) progress entries around before
// dropping them. Must be long enough to survive React StrictMode double-poll
// + network latency + any poll-interval jitter, but short enough to not leak.
const TERMINAL_TTL_MS = 5 * 60 * 1000; // 5 minutes

export function setProgress(taskId, data) {
  progress.set(taskId, { ...data, updatedAt: Date.now() });
}

export function getProgress(taskId) {
  const v = progress.get(taskId);
  if (!v) return null;
  // Lazy TTL sweep — drop terminal entries older than TERMINAL_TTL_MS on read.
  if ((v.status === "complete" || v.status === "error") &&
      Date.now() - v.updatedAt > TERMINAL_TTL_MS) {
    progress.delete(taskId);
    return null;
  }
  return v;
}

export function clearProgress(taskId) {
  progress.delete(taskId);
}

// Mark task as complete with result data. Stays in map for TERMINAL_TTL_MS
// so repeated polls (StrictMode, re-render, slow consumer) all see it.
export function completeProgress(taskId, result) {
  progress.set(taskId, { status: "complete", result, updatedAt: Date.now() });
}

// Mark task as failed.
export function failProgress(taskId, error) {
  progress.set(taskId, { status: "error", error: error.message || String(error), updatedAt: Date.now() });
}
