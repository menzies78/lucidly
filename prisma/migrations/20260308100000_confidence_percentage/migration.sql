-- Convert confidence from String (HIGH/MEDIUM/LOW/NONE) to Int (0-100)
-- Step 1: Add new columns
ALTER TABLE "Attribution" ADD COLUMN "confidence_new" INTEGER NOT NULL DEFAULT 0;
ALTER TABLE "Attribution" ADD COLUMN "rivalCount" INTEGER NOT NULL DEFAULT 0;

-- Step 2: Convert existing values
UPDATE "Attribution" SET "confidence_new" = CASE
  WHEN "confidence" = 'HIGH' THEN 85
  WHEN "confidence" = 'MEDIUM' THEN 50
  WHEN "confidence" = 'LOW' THEN 25
  WHEN "confidence" = 'NONE' THEN 0
  ELSE 0
END;

-- Step 3: Create new table with correct schema
CREATE TABLE "Attribution_new" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "shopDomain" TEXT NOT NULL,
    "shopifyOrderId" TEXT NOT NULL,
    "layer" INTEGER NOT NULL,
    "confidence" INTEGER NOT NULL DEFAULT 0,
    "metaCampaignId" TEXT,
    "metaCampaignName" TEXT,
    "metaAdSetId" TEXT,
    "metaAdSetName" TEXT,
    "metaAdId" TEXT,
    "metaAdName" TEXT,
    "isNewCustomer" BOOLEAN,
    "isNewToMeta" BOOLEAN,
    "matchedAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "matchMethod" TEXT,
    "metaConversionValue" REAL NOT NULL DEFAULT 0,
    "rivalCount" INTEGER NOT NULL DEFAULT 0
);

-- Step 4: Copy data with converted confidence
INSERT INTO "Attribution_new" SELECT
    "id", "shopDomain", "shopifyOrderId", "layer", "confidence_new",
    "metaCampaignId", "metaCampaignName", "metaAdSetId", "metaAdSetName",
    "metaAdId", "metaAdName", "isNewCustomer", "isNewToMeta", "matchedAt",
    "matchMethod", "metaConversionValue", "rivalCount"
FROM "Attribution";

-- Step 5: Replace table
DROP TABLE "Attribution";
ALTER TABLE "Attribution_new" RENAME TO "Attribution";

-- Step 6: Recreate indexes and unique constraint
CREATE UNIQUE INDEX "Attribution_shopDomain_shopifyOrderId_key" ON "Attribution"("shopDomain", "shopifyOrderId");
CREATE INDEX "Attribution_shopDomain_layer_idx" ON "Attribution"("shopDomain", "layer");
CREATE INDEX "Attribution_shopDomain_metaCampaignId_idx" ON "Attribution"("shopDomain", "metaCampaignId");

-- Step 7: Change Order.attributionConfidence from String to Int
CREATE TABLE "Order_new" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "shopDomain" TEXT NOT NULL,
    "shopifyOrderId" TEXT NOT NULL,
    "shopifyCustomerId" TEXT,
    "orderNumber" TEXT,
    "createdAt" DATETIME NOT NULL,
    "totalPrice" REAL NOT NULL,
    "subtotalPrice" REAL NOT NULL,
    "currency" TEXT NOT NULL,
    "financialStatus" TEXT,
    "channelName" TEXT,
    "isOnlineStore" BOOLEAN NOT NULL DEFAULT 1,
    "frozenTotalPrice" REAL NOT NULL,
    "frozenSubtotalPrice" REAL NOT NULL,
    "attributionLayer" INTEGER,
    "attributionConfidence" INTEGER,
    "metaCampaignId" TEXT,
    "metaCampaignName" TEXT,
    "metaAdSetId" TEXT,
    "metaAdSetName" TEXT,
    "metaAdId" TEXT,
    "metaAdName" TEXT,
    "isNewCustomerOrder" BOOLEAN,
    "pixelJourney" TEXT,
    "country" TEXT NOT NULL DEFAULT '',
    "countryCode" TEXT NOT NULL DEFAULT '',
    "city" TEXT NOT NULL DEFAULT '',
    "regionCode" TEXT NOT NULL DEFAULT '',
    "customerFirstName" TEXT NOT NULL DEFAULT '',
    "customerLastInitial" TEXT NOT NULL DEFAULT '',
    "customerOrderCountAtPurchase" INTEGER,
    "lineItems" TEXT NOT NULL DEFAULT '',
    "productSkus" TEXT NOT NULL DEFAULT '',
    "productCollections" TEXT NOT NULL DEFAULT '',
    "discountCodes" TEXT NOT NULL DEFAULT '',
    "refundStatus" TEXT NOT NULL DEFAULT 'none',
    "totalRefunded" REAL NOT NULL DEFAULT 0,
    "metaCustomerTag" TEXT NOT NULL DEFAULT '',
    "importedAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO "Order_new" SELECT
    "id", "shopDomain", "shopifyOrderId", "shopifyCustomerId", "orderNumber",
    "createdAt", "totalPrice", "subtotalPrice", "currency", "financialStatus",
    "channelName", "isOnlineStore", "frozenTotalPrice", "frozenSubtotalPrice",
    "attributionLayer",
    CASE WHEN "attributionConfidence" IS NULL THEN NULL
         WHEN "attributionConfidence" = 'HIGH' THEN 85
         WHEN "attributionConfidence" = 'MEDIUM' THEN 50
         WHEN "attributionConfidence" = 'LOW' THEN 25
         ELSE 0 END,
    "metaCampaignId", "metaCampaignName", "metaAdSetId", "metaAdSetName",
    "metaAdId", "metaAdName", "isNewCustomerOrder", "pixelJourney",
    "country", "countryCode", "city", "regionCode",
    "customerFirstName", "customerLastInitial", "customerOrderCountAtPurchase",
    "lineItems", "productSkus", "productCollections", "discountCodes",
    "refundStatus", "totalRefunded", "metaCustomerTag", "importedAt"
FROM "Order";

DROP TABLE "Order";
ALTER TABLE "Order_new" RENAME TO "Order";

CREATE UNIQUE INDEX "Order_shopDomain_shopifyOrderId_key" ON "Order"("shopDomain", "shopifyOrderId");
CREATE INDEX "Order_shopDomain_createdAt_idx" ON "Order"("shopDomain", "createdAt");
CREATE INDEX "Order_shopDomain_shopifyCustomerId_idx" ON "Order"("shopDomain", "shopifyCustomerId");
CREATE INDEX "Order_attributionLayer_idx" ON "Order"("attributionLayer");
