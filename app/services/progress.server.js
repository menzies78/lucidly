// In-memory progress tracking for long-running tasks.
// Works for single-process deployments (dev + single-instance prod).

const progress = new Map();

export function setProgress(taskId, data) {
  progress.set(taskId, { ...data, updatedAt: Date.now() });
}

export function getProgress(taskId) {
  return progress.get(taskId) || null;
}

export function clearProgress(taskId) {
  progress.delete(taskId);
}

// Mark task as complete with result data. Stays in map until frontend picks it up.
export function completeProgress(taskId, result) {
  progress.set(taskId, { status: "complete", result, updatedAt: Date.now() });
}

// Mark task as failed.
export function failProgress(taskId, error) {
  progress.set(taskId, { status: "error", error: error.message || String(error), updatedAt: Date.now() });
}
