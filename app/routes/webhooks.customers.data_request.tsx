import type { ActionFunctionArgs } from "@remix-run/node";
import { authenticate } from "../shopify.server";

// GDPR / Shopify Privacy mandatory webhook.
//
// Triggered when a merchant (or customer via the merchant) requests a copy
// of the customer's data we hold. Shopify requires every public app to
// register this endpoint - the response itself is just an acknowledgement
// (we do not have to deliver the data here; the merchant compiles + sends
// it via their own channel). We log the request so we have an audit trail
// if a merchant ever asks "did Shopify reach you about customer X".
export const action = async ({ request }: ActionFunctionArgs) => {
  const { shop, topic, payload } = await authenticate.webhook(request);
  console.log(`[GDPR] ${topic} for ${shop}:`, JSON.stringify(payload));
  return new Response();
};
