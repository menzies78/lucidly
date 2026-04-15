-- Add pre-computed segment fields to Customer
ALTER TABLE "Customer" ADD COLUMN "metaSegment" TEXT;
ALTER TABLE "Customer" ADD COLUMN "acquisitionCampaign" TEXT;
ALTER TABLE "Customer" ADD COLUMN "acquisitionAdSet" TEXT;
ALTER TABLE "Customer" ADD COLUMN "acquisitionAd" TEXT;
ALTER TABLE "Customer" ADD COLUMN "totalRefunded" REAL NOT NULL DEFAULT 0;
ALTER TABLE "Customer" ADD COLUMN "metaOrders" INTEGER NOT NULL DEFAULT 0;
ALTER TABLE "Customer" ADD COLUMN "country" TEXT;
ALTER TABLE "Customer" ADD COLUMN "city" TEXT;

-- Index for fast segment lookups
CREATE INDEX "Customer_shopDomain_metaSegment_idx" ON "Customer"("shopDomain", "metaSegment");

-- Per-day per-segment aggregates for tile/chart loading
CREATE TABLE "DailyCustomerRollup" (
    "id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "shopDomain" TEXT NOT NULL,
    "date" DATETIME NOT NULL,
    "segment" TEXT NOT NULL,
    "newCustomers" INTEGER NOT NULL DEFAULT 0,
    "orders" INTEGER NOT NULL DEFAULT 0,
    "revenue" REAL NOT NULL DEFAULT 0,
    "refundedAmount" REAL NOT NULL DEFAULT 0,
    "firstOrderRevenue" REAL NOT NULL DEFAULT 0,
    "repeatCustomers" INTEGER NOT NULL DEFAULT 0,
    "metaSpend" REAL NOT NULL DEFAULT 0
);

CREATE UNIQUE INDEX "DailyCustomerRollup_shopDomain_date_segment_key" ON "DailyCustomerRollup"("shopDomain", "date", "segment");
CREATE INDEX "DailyCustomerRollup_shopDomain_date_idx" ON "DailyCustomerRollup"("shopDomain", "date");
