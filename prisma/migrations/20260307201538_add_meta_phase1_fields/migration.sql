-- RedefineTables
PRAGMA defer_foreign_keys=ON;
PRAGMA foreign_keys=OFF;
CREATE TABLE "new_MetaInsight" (
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
    "reach" INTEGER NOT NULL DEFAULT 0,
    "frequency" REAL NOT NULL DEFAULT 0,
    "outboundClicks" INTEGER NOT NULL DEFAULT 0,
    "cpc" REAL NOT NULL DEFAULT 0,
    "cpm" REAL NOT NULL DEFAULT 0,
    "linkClicks" INTEGER NOT NULL DEFAULT 0,
    "landingPageViews" INTEGER NOT NULL DEFAULT 0,
    "addToCart" INTEGER NOT NULL DEFAULT 0,
    "initiateCheckout" INTEGER NOT NULL DEFAULT 0,
    "viewContent" INTEGER NOT NULL DEFAULT 0,
    "videoP25" INTEGER NOT NULL DEFAULT 0,
    "videoP50" INTEGER NOT NULL DEFAULT 0,
    "videoP75" INTEGER NOT NULL DEFAULT 0,
    "videoP100" INTEGER NOT NULL DEFAULT 0,
    "importedAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO "new_MetaInsight" ("adId", "adName", "adSetId", "adSetName", "campaignId", "campaignName", "clicks", "conversionValue", "conversions", "date", "hourSlot", "id", "importedAt", "impressions", "shopDomain", "spend") SELECT "adId", "adName", "adSetId", "adSetName", "campaignId", "campaignName", "clicks", "conversionValue", "conversions", "date", "hourSlot", "id", "importedAt", "impressions", "shopDomain", "spend" FROM "MetaInsight";
DROP TABLE "MetaInsight";
ALTER TABLE "new_MetaInsight" RENAME TO "MetaInsight";
CREATE INDEX "MetaInsight_shopDomain_date_idx" ON "MetaInsight"("shopDomain", "date");
CREATE UNIQUE INDEX "MetaInsight_shopDomain_date_hourSlot_adId_key" ON "MetaInsight"("shopDomain", "date", "hourSlot", "adId");
PRAGMA foreign_keys=ON;
PRAGMA defer_foreign_keys=OFF;
