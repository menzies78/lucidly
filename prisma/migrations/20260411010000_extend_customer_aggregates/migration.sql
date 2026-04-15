-- Per-customer aggregates pre-computed at sync time so loaders can read
-- the customer table directly without joining/aggregating raw orders.
ALTER TABLE "Customer" ADD COLUMN "lastOrderDate" DATETIME;
ALTER TABLE "Customer" ADD COLUMN "secondOrderDate" DATETIME;
ALTER TABLE "Customer" ADD COLUMN "firstOrderValue" REAL NOT NULL DEFAULT 0;
ALTER TABLE "Customer" ADD COLUMN "discountOrdersCount" INTEGER NOT NULL DEFAULT 0;
ALTER TABLE "Customer" ADD COLUMN "topProducts" TEXT;
ALTER TABLE "Customer" ADD COLUMN "avgConfidence" INTEGER;
