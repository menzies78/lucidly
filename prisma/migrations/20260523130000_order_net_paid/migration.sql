-- Order.netPaid — canonical "net amount customer actually paid". Sum across
-- OrderLineItem of max(0, totalPrice - refundedAmount). Correct for Shopify
-- exchanges where Order.totalRefunded = 0 but line items were refunded and
-- replaced, so frozenTotalPrice - totalRefunded over-states the payment.
ALTER TABLE "Order" ADD COLUMN "netPaid" REAL;

-- Backfill from existing OrderLineItem rows. SUM(CASE …) avoids SQLite's
-- scalar-vs-aggregate ambiguity that bites when you nest MAX(0, x) inside SUM.
-- Orders with no line item rows (very legacy imports) are left NULL — the
-- read-side helper netPaidOf() falls back to frozenTotalPrice - totalRefunded.
UPDATE "Order" SET "netPaid" = (
  SELECT COALESCE(SUM(
    CASE WHEN (oli.totalPrice - COALESCE(oli.refundedAmount, 0)) > 0
         THEN (oli.totalPrice - COALESCE(oli.refundedAmount, 0))
         ELSE 0 END
  ), 0)
  FROM OrderLineItem oli
  WHERE oli.shopDomain = "Order".shopDomain
    AND oli.shopifyOrderId = "Order".shopifyOrderId
)
WHERE EXISTS (
  SELECT 1 FROM OrderLineItem oli2
  WHERE oli2.shopDomain = "Order".shopDomain
    AND oli2.shopifyOrderId = "Order".shopifyOrderId
);
