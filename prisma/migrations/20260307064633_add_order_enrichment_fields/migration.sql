-- RedefineTables
PRAGMA defer_foreign_keys=ON;
PRAGMA foreign_keys=OFF;
CREATE TABLE "new_Order" (
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
    "isOnlineStore" BOOLEAN NOT NULL DEFAULT true,
    "frozenTotalPrice" REAL NOT NULL,
    "frozenSubtotalPrice" REAL NOT NULL,
    "attributionLayer" INTEGER,
    "attributionConfidence" TEXT,
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
INSERT INTO "new_Order" ("attributionConfidence", "attributionLayer", "channelName", "country", "countryCode", "createdAt", "currency", "customerFirstName", "customerLastInitial", "financialStatus", "frozenSubtotalPrice", "frozenTotalPrice", "id", "importedAt", "isNewCustomerOrder", "isOnlineStore", "lineItems", "metaAdId", "metaAdName", "metaAdSetId", "metaAdSetName", "metaCampaignId", "metaCampaignName", "metaCustomerTag", "orderNumber", "pixelJourney", "shopDomain", "shopifyCustomerId", "shopifyOrderId", "subtotalPrice", "totalPrice") SELECT "attributionConfidence", "attributionLayer", "channelName", "country", "countryCode", "createdAt", "currency", "customerFirstName", "customerLastInitial", "financialStatus", "frozenSubtotalPrice", "frozenTotalPrice", "id", "importedAt", "isNewCustomerOrder", "isOnlineStore", "lineItems", "metaAdId", "metaAdName", "metaAdSetId", "metaAdSetName", "metaCampaignId", "metaCampaignName", "metaCustomerTag", "orderNumber", "pixelJourney", "shopDomain", "shopifyCustomerId", "shopifyOrderId", "subtotalPrice", "totalPrice" FROM "Order";
DROP TABLE "Order";
ALTER TABLE "new_Order" RENAME TO "Order";
CREATE INDEX "Order_shopDomain_createdAt_idx" ON "Order"("shopDomain", "createdAt");
CREATE INDEX "Order_shopDomain_shopifyCustomerId_idx" ON "Order"("shopDomain", "shopifyCustomerId");
CREATE INDEX "Order_attributionLayer_idx" ON "Order"("attributionLayer");
CREATE UNIQUE INDEX "Order_shopDomain_shopifyOrderId_key" ON "Order"("shopDomain", "shopifyOrderId");
PRAGMA foreign_keys=ON;
PRAGMA defer_foreign_keys=OFF;
