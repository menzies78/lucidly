-- CreateTable
CREATE TABLE "DailyProductRollup" (
    "id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "shopDomain" TEXT NOT NULL,
    "date" DATETIME NOT NULL,
    "product" TEXT NOT NULL,
    "segment" TEXT NOT NULL,
    "orders" INTEGER NOT NULL DEFAULT 0,
    "items" INTEGER NOT NULL DEFAULT 0,
    "revenue" REAL NOT NULL DEFAULT 0,
    "refundedOrders" INTEGER NOT NULL DEFAULT 0,
    "refundedAmount" REAL NOT NULL DEFAULT 0,
    "firstPurchases" INTEGER NOT NULL DEFAULT 0,
    "firstPurchaseRevenue" REAL NOT NULL DEFAULT 0,
    "topCampaignJson" TEXT NOT NULL DEFAULT '{}',
    "topAdSetJson" TEXT NOT NULL DEFAULT '{}',
    "collections" TEXT NOT NULL DEFAULT ''
);

-- CreateTable
CREATE TABLE "ShopAnalysisCache" (
    "id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "shopDomain" TEXT NOT NULL,
    "cacheKey" TEXT NOT NULL,
    "payload" TEXT NOT NULL,
    "computedAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- CreateIndex
CREATE INDEX "DailyProductRollup_shopDomain_date_idx" ON "DailyProductRollup"("shopDomain", "date");

-- CreateIndex
CREATE UNIQUE INDEX "DailyProductRollup_shopDomain_date_product_segment_key" ON "DailyProductRollup"("shopDomain", "date", "product", "segment");

-- CreateIndex
CREATE UNIQUE INDEX "ShopAnalysisCache_shopDomain_cacheKey_key" ON "ShopAnalysisCache"("shopDomain", "cacheKey");
