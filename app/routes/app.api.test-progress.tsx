import { json } from "@remix-run/node";
import { setProgress, completeProgress } from "../services/progress.server";

// Simulates a long-running task with progress updates.
// Hit /app/api/test-progress to start, then poll /app/api/progress?task=test
export const loader = async () => {
  const taskId = "test:test-shop";
  const total = 10;

  for (let i = 1; i <= total; i++) {
    setProgress(taskId, {
      status: "running",
      current: i,
      total,
      message: `Processing step ${i} of ${total}`,
    });
    await new Promise(r => setTimeout(r, 1000));
  }

  completeProgress(taskId, { done: true });
  return json({ done: true });
};
