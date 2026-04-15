-- CreateTable
CREATE TABLE "DailyAdRollup" (
    "id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "shopDomain" TEXT NOT NULL,
    "date" DATETIME NOT NULL,
    "adId" TEXT NOT NULL,
    "campaignId" TEXT NOT NULL,
    "campaignName" TEXT NOT NULL,
    "adSetId" TEXT NOT NULL,
    "adSetName" TEXT NOT NULL,
    "adName" TEXT NOT NULL,
    "spend" REAL NOT NULL DEFAULT 0,
    "impressions" INTEGER NOT NULL DEFAULT 0,
    "clicks" INTEGER NOT NULL DEFAULT 0,
    "reach" INTEGER NOT NULL DEFAULT 0,
    "frequencySum" REAL NOT NULL DEFAULT 0,
    "frequencyCount" INTEGER NOT NULL DEFAULT 0,
    "linkClicks" INTEGER NOT NULL DEFAULT 0,
    "landingPageViews" INTEGER NOT NULL DEFAULT 0,
    "viewContent" INTEGER NOT NULL DEFAULT 0,
    "addToCart" INTEGER NOT NULL DEFAULT 0,
    "initiateCheckout" INTEGER NOT NULL DEFAULT 0,
    "metaConversions" INTEGER NOT NULL DEFAULT 0,
    "metaConversionValue" REAL NOT NULL DEFAULT 0,
    "videoP25" INTEGER NOT NULL DEFAULT 0,
    "videoP50" INTEGER NOT NULL DEFAULT 0,
    "videoP75" INTEGER NOT NULL DEFAULT 0,
    "videoP100" INTEGER NOT NULL DEFAULT 0,
    "attributedOrders" INTEGER NOT NULL DEFAULT 0,
    "attributedRevenue" REAL NOT NULL DEFAULT 0,
    "newCustomerOrders" INTEGER NOT NULL DEFAULT 0,
    "newCustomerRevenue" REAL NOT NULL DEFAULT 0,
    "existingCustomerOrders" INTEGER NOT NULL DEFAULT 0,
    "existingCustomerRevenue" REAL NOT NULL DEFAULT 0,
    "unverifiedRevenue" REAL NOT NULL DEFAULT 0,
    "utmOnlyOrders" INTEGER NOT NULL DEFAULT 0,
    "utmOnlyRevenue" REAL NOT NULL DEFAULT 0
);

-- CreateIndex
CREATE INDEX "DailyAdRollup_shopDomain_date_idx" ON "DailyAdRollup"("shopDomain", "date");

-- CreateIndex
CREATE UNIQUE INDEX "DailyAdRollup_shopDomain_date_adId_key" ON "DailyAdRollup"("shopDomain", "date", "adId");
