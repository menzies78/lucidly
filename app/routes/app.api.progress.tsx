import { json } from "@remix-run/node";
import { authenticate } from "../shopify.server";
import { getProgress, clearProgress } from "../services/progress.server";

export const loader = async ({ request }) => {
  const { session } = await authenticate.admin(request);
  const shopDomain = session.shop;

  const url = new URL(request.url);
  const taskId = url.searchParams.get("task");
  if (!taskId) return json({ progress: null });

  const fullKey = `${taskId}:${shopDomain}`;
  const progress = getProgress(fullKey);

  // Clear terminal states after they've been read by the frontend
  if (progress?.status === "complete" || progress?.status === "error") {
    clearProgress(fullKey);
  }

  return json({ progress });
};
