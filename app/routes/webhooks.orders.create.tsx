import type { ActionFunctionArgs } from "@remix-run/node";
import { authenticate } from "../shopify.server";
import { enqueueOrderWebhook } from "../services/orderWebhookQueue.server";

// Ack immediately, process async. A synchronous processOrderWebhook here
// blocked on the SQLite writer whenever it was held (hourly rollup rebuild),
// which timed out deliveries and triggered Shopify's retry amplification -
// see orderWebhookQueue.server.js for the full 2026-07-08 incident story.
export const action = async ({ request }: ActionFunctionArgs) => {
  const { shop, topic, payload } = await authenticate.webhook(request);
  console.log(`Received ${topic} webhook for ${shop}`);
  enqueueOrderWebhook(shop, payload, true);
  return new Response();
};
