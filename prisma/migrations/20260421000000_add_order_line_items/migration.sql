-- Structured line-item rows. Replaces the even-split revenue logic that
-- divided Order.frozenTotalPrice by the count of titles in the comma-
-- separated Order.lineItems string. `unitPrice` is the discounted unit
-- price; `totalPrice` = unitPrice * quantity. Sum across an order's rows
-- ≈ frozenSubtotalPrice (excludes shipping / tax).
--
-- Rows are replaced (deleteMany + createMany) whenever an order is upserted
-- by orderSync or orderWebhook, so refund changes propagate on the next
-- order update. The ON DELETE CASCADE removes rows when an Order is
-- deleted (not part of normal operation but keeps FK state clean).
CREATE TABLE "OrderLineItem" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "shopDomain" TEXT NOT NULL,
    "shopifyOrderId" TEXT NOT NULL,
    "shopifyLineItemId" TEXT,
    "title" TEXT NOT NULL DEFAULT '',
    "sku" TEXT NOT NULL DEFAULT '',
    "quantity" INTEGER NOT NULL DEFAULT 1,
    "unitPrice" REAL NOT NULL DEFAULT 0,
    "totalPrice" REAL NOT NULL DEFAULT 0,
    "totalDiscount" REAL NOT NULL DEFAULT 0,
    "refundedQuantity" INTEGER NOT NULL DEFAULT 0,
    "refundedAmount" REAL NOT NULL DEFAULT 0,
    CONSTRAINT "OrderLineItem_shopDomain_shopifyOrderId_fkey" FOREIGN KEY ("shopDomain", "shopifyOrderId") REFERENCES "Order" ("shopDomain", "shopifyOrderId") ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE INDEX "OrderLineItem_shopDomain_shopifyOrderId_idx" ON "OrderLineItem"("shopDomain", "shopifyOrderId");
CREATE INDEX "OrderLineItem_shopDomain_title_idx" ON "OrderLineItem"("shopDomain", "title");
CREATE INDEX "OrderLineItem_shopDomain_sku_idx" ON "OrderLineItem"("shopDomain", "sku");
