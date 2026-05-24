// Canonical "net amount the customer actually paid" helpers.
//
// Why this exists: `Order.frozenTotalPrice − Order.totalRefunded` is the
// formula that was sprinkled throughout the app, and it under-corrects in
// Shopify exchanges. A pure exchange (refund line item A + replace with line
// item B of equal price) leaves `totalRefunded = 0` because no money moved,
// but `frozenTotalPrice` has summed both items — so the formula yields 2x
// the true net paid. The line-item table has per-row `refundedAmount` set
// correctly by the GraphQL / webhook ingest, so the canonical net is
//
//     Σ max(0, OrderLineItem.totalPrice − OrderLineItem.refundedAmount)
//
// We store that as `Order.netPaid` at write time (orderSync + orderWebhook)
// so every read site can use a single field with no joins.

export type NetPaidOrder = {
  netPaid?: number | null;
  frozenTotalPrice?: number | null;
  totalRefunded?: number | null;
};

// Reader: prefer the stored netPaid; fall back to the legacy formula for
// orders imported before the column existed, or orders with no line item
// rows (a few very-legacy imports).
export function netPaidOf(order: NetPaidOrder): number {
  if (order.netPaid != null) return order.netPaid;
  const gross = order.frozenTotalPrice || 0;
  const refunded = order.totalRefunded || 0;
  return Math.max(0, gross - refunded);
}

// Writer: compute net paid by summing the refund-adjusted line items.
// Used by orderSync.server.js + orderWebhook.server.js at the moment they
// build OrderLineItem rows, before the Order upsert.
export function computeNetPaidFromLineItems(
  lineItems: Array<{ totalPrice?: number | null; refundedAmount?: number | null }>,
): number {
  let net = 0;
  for (const li of lineItems) {
    const adj = (li.totalPrice || 0) - (li.refundedAmount || 0);
    if (adj > 0) net += adj;
  }
  return Math.round(net * 100) / 100;
}
