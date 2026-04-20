import { json } from "@remix-run/node";
import { authenticate } from "../shopify.server";
import { getProgress } from "../services/progress.server";

export const loader = async ({ request }) => {
  const { session } = await authenticate.admin(request);
  const shopDomain = session.shop;

  const url = new URL(request.url);
  const taskId = url.searchParams.get("task");
  if (!taskId) return json({ progress: null });

  const fullKey = `${taskId}:${shopDomain}`;
  const progress = getProgress(fullKey);

  // Do NOT clear terminal state here — React StrictMode, re-renders, and
  // network jitter cause the same terminal state to be polled more than once.
  // progress.server.js TTL-sweeps terminal entries after 5 minutes.
  return json({ progress });
};
