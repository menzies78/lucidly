-- CreateTable
CREATE TABLE "Shop" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "shopDomain" TEXT NOT NULL,
    "installedAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "metaAccessToken" TEXT,
    "metaAdAccountId" TEXT,
    "metaAccountTimezone" TEXT,
    "revenueDefinition" TEXT NOT NULL DEFAULT 'total_price',
    "matchingTolerance" REAL NOT NULL DEFAULT 0.02,
    "currencyCode" TEXT NOT NULL DEFAULT 'GBP',
    "lastOrderSync" DATETIME,
    "lastMetaSync" DATETIME,
    "onboardingCompleted" BOOLEAN NOT NULL DEFAULT false
);

-- CreateTable
CREATE TABLE "Customer" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "shopDomain" TEXT NOT NULL,
    "shopifyCustomerId" TEXT NOT NULL,
    "emailHash" TEXT,
    "firstOrderDate" DATETIME,
    "totalOrders" INTEGER NOT NULL DEFAULT 0,
    "totalSpent" REAL NOT NULL DEFAULT 0,
    "isNewCustomer" BOOLEAN NOT NULL DEFAULT true,
    "createdAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" DATETIME NOT NULL
);

-- CreateTable
CREATE TABLE "Order" (
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
    "importedAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- CreateTable
CREATE TABLE "MetaInsight" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "shopDomain" TEXT NOT NULL,
    "date" DATETIME NOT NULL,
    "hourSlot" INTEGER NOT NULL,
    "campaignId" TEXT NOT NULL,
    "campaignName" TEXT,
    "adSetId" TEXT,
    "adSetName" TEXT,
    "adId" TEXT,
    "adName" TEXT,
    "impressions" INTEGER NOT NULL DEFAULT 0,
    "clicks" INTEGER NOT NULL DEFAULT 0,
    "spend" REAL NOT NULL DEFAULT 0,
    "conversions" INTEGER NOT NULL DEFAULT 0,
    "conversionValue" REAL NOT NULL DEFAULT 0,
    "importedAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- CreateTable
CREATE TABLE "Attribution" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "shopDomain" TEXT NOT NULL,
    "shopifyOrderId" TEXT NOT NULL,
    "layer" INTEGER NOT NULL,
    "confidence" TEXT NOT NULL,
    "metaCampaignId" TEXT,
    "metaCampaignName" TEXT,
    "metaAdSetId" TEXT,
    "metaAdId" TEXT,
    "metaAdName" TEXT,
    "isNewCustomer" BOOLEAN,
    "isNewToMeta" BOOLEAN,
    "matchedAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "matchMethod" TEXT
);

-- CreateIndex
CREATE UNIQUE INDEX "Shop_shopDomain_key" ON "Shop"("shopDomain");

-- CreateIndex
CREATE INDEX "Customer_shopDomain_idx" ON "Customer"("shopDomain");

-- CreateIndex
CREATE INDEX "Customer_emailHash_idx" ON "Customer"("emailHash");

-- CreateIndex
CREATE UNIQUE INDEX "Customer_shopDomain_shopifyCustomerId_key" ON "Customer"("shopDomain", "shopifyCustomerId");

-- CreateIndex
CREATE INDEX "Order_shopDomain_createdAt_idx" ON "Order"("shopDomain", "createdAt");

-- CreateIndex
CREATE INDEX "Order_shopDomain_shopifyCustomerId_idx" ON "Order"("shopDomain", "shopifyCustomerId");

-- CreateIndex
CREATE INDEX "Order_attributionLayer_idx" ON "Order"("attributionLayer");

-- CreateIndex
CREATE UNIQUE INDEX "Order_shopDomain_shopifyOrderId_key" ON "Order"("shopDomain", "shopifyOrderId");

-- CreateIndex
CREATE INDEX "MetaInsight_shopDomain_date_idx" ON "MetaInsight"("shopDomain", "date");

-- CreateIndex
CREATE UNIQUE INDEX "MetaInsight_shopDomain_date_hourSlot_adId_key" ON "MetaInsight"("shopDomain", "date", "hourSlot", "adId");

-- CreateIndex
CREATE INDEX "Attribution_shopDomain_layer_idx" ON "Attribution"("shopDomain", "layer");

-- CreateIndex
CREATE INDEX "Attribution_shopDomain_metaCampaignId_idx" ON "Attribution"("shopDomain", "metaCampaignId");

-- CreateIndex
CREATE UNIQUE INDEX "Attribution_shopDomain_shopifyOrderId_key" ON "Attribution"("shopDomain", "shopifyOrderId");
