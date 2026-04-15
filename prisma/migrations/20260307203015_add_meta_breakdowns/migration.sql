-- CreateTable
CREATE TABLE "MetaBreakdown" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "shopDomain" TEXT NOT NULL,
    "date" DATETIME NOT NULL,
    "campaignId" TEXT NOT NULL,
    "campaignName" TEXT,
    "adSetId" TEXT,
    "adSetName" TEXT,
    "adId" TEXT,
    "adName" TEXT,
    "breakdownType" TEXT NOT NULL,
    "breakdownValue" TEXT NOT NULL,
    "impressions" INTEGER NOT NULL DEFAULT 0,
    "clicks" INTEGER NOT NULL DEFAULT 0,
    "spend" REAL NOT NULL DEFAULT 0,
    "reach" INTEGER NOT NULL DEFAULT 0,
    "conversions" INTEGER NOT NULL DEFAULT 0,
    "conversionValue" REAL NOT NULL DEFAULT 0,
    "linkClicks" INTEGER NOT NULL DEFAULT 0,
    "landingPageViews" INTEGER NOT NULL DEFAULT 0,
    "addToCart" INTEGER NOT NULL DEFAULT 0,
    "initiateCheckout" INTEGER NOT NULL DEFAULT 0,
    "viewContent" INTEGER NOT NULL DEFAULT 0,
    "outboundClicks" INTEGER NOT NULL DEFAULT 0,
    "importedAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- CreateIndex
CREATE INDEX "MetaBreakdown_shopDomain_breakdownType_idx" ON "MetaBreakdown"("shopDomain", "breakdownType");

-- CreateIndex
CREATE INDEX "MetaBreakdown_shopDomain_date_idx" ON "MetaBreakdown"("shopDomain", "date");

-- CreateIndex
CREATE UNIQUE INDEX "MetaBreakdown_shopDomain_date_adId_breakdownType_breakdownValue_key" ON "MetaBreakdown"("shopDomain", "date", "adId", "breakdownType", "breakdownValue");
