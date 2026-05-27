import type { ActionFunctionArgs } from "@remix-run/node";
import { authenticate } from "../shopify.server";
import db from "../db.server";

// GDPR / Shopify Privacy mandatory webhook.
//
// Triggered when a customer's data must be deleted. Shopify sends this:
//   - 10 days after the merchant uninstalls the app (paired with shop/redact)
//   - whenever a customer formally requests erasure via the merchant
//
// Payload shape (Shopify spec):
//   {
//     shop_id, shop_domain,
//     customer: { id, email, phone },
//     orders_to_redact: [orderId, ...]
//   }
//
// We delete every record that identifies this customer, scoped to the
// shopDomain that sent the webhook so a redact request for one merchant
// can never spill into another's data. Order line items and attributions
// are tied to Order rows by (shopDomain, shopifyOrderId) so deleting the
// Order rows cascades correctly. Aggregated rollups (DailyProductRollup,
// DailyAdRollup, etc.) hold no per-customer identifiers so they survive
// unchanged - they're already de-personalised.
export const action = async ({ request }: ActionFunctionArgs) => {
  const { shop, topic, payload } = await authenticate.webhook(request);
  console.log(`[GDPR] ${topic} for ${shop}:`, JSON.stringify(payload));

  const customerId = (payload as any)?.customer?.id;
  const ordersToRedact: Array<number | string> = (payload as any)?.orders_to_redact || [];

  try {
    // Strict shopDomain scoping prevents cross-merchant data deletion even
    // if a payload was somehow spoofed onto the wrong endpoint.
    const where = { shopDomain: shop };

    // 1. Identify the customer's orders. Use both the customer-id linkage
    // and the explicit orders_to_redact list from the payload (covers
    // guest checkouts where Order has no customerId).
    const orderIdSet = new Set<string>();
    if (customerId != null) {
      const orders = await db.order.findMany({
        where: { ...where, shopifyCustomerId: String(customerId) },
        select: { shopifyOrderId: true },
      });
      for (const o of orders) orderIdSet.add(o.shopifyOrderId);
    }
    for (const oid of ordersToRedact) orderIdSet.add(String(oid));
    const orderIds = Array.from(orderIdSet);

    if (orderIds.length > 0) {
      // 2. Delete dependent rows first (line items + attributions are
      // keyed by shopifyOrderId, not the Customer table directly).
      await db.orderLineItem.deleteMany({
        where: { ...where, shopifyOrderId: { in: orderIds } },
      });
      await db.attribution.deleteMany({
        where: { ...where, shopifyOrderId: { in: orderIds } },
      });
      // 3. Delete the orders themselves.
      await db.order.deleteMany({
        where: { ...where, shopifyOrderId: { in: orderIds } },
      });
    }

    // 4. Delete the Customer row (if we have one).
    if (customerId != null) {
      await db.customer.deleteMany({
        where: { ...where, shopifyCustomerId: String(customerId) },
      });
    }

    console.log(`[GDPR] customers/redact ${shop} customer=${customerId} orders=${orderIds.length}`);
  } catch (err) {
    console.error(`[GDPR] customers/redact failed for ${shop}:`, err);
  }

  return new Response();
};
