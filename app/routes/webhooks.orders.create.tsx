import type { ActionFunctionArgs } from "@remix-run/node";
import { authenticate } from "../shopify.server";
import { processOrderWebhook } from "../services/orderWebhook.server";

export const action = async ({ request }: ActionFunctionArgs) => {
  const { shop, topic, payload } = await authenticate.webhook(request);
  console.log(`Received ${topic} webhook for ${shop}`);

  try {
    await processOrderWebhook(shop, payload, true);
  } catch (err) {
    console.error(`[Webhook] orders/create failed for ${shop}:`, err);
  }

  return new Response();
};
