-- AlterTable: add unverifiedOrders to DailyAdRollup for weekly-report blended counts
ALTER TABLE "DailyAdRollup" ADD COLUMN "unverifiedOrders" INTEGER NOT NULL DEFAULT 0;

-- CreateTable: DailyGeoRollup powering the Geo report (per-day per-country slice
-- with overall + per-entity rows so the loader can reconstruct country share
-- without re-aggregating raw MetaBreakdown + Order + Attribution).
CREATE TABLE "DailyGeoRollup" (
    "id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "shopDomain" TEXT NOT NULL,
    "date" DATETIME NOT NULL,
    "level" TEXT NOT NULL,
    "entityId" TEXT NOT NULL,
    "country" TEXT NOT NULL,
    "entityName" TEXT,
    "campaignId" TEXT,
    "campaignName" TEXT,
    "adSetId" TEXT,
    "adSetName" TEXT,
    "spend" REAL NOT NULL DEFAULT 0,
    "impressions" INTEGER NOT NULL DEFAULT 0,
    "clicks" INTEGER NOT NULL DEFAULT 0,
    "reach" INTEGER NOT NULL DEFAULT 0,
    "metaConversions" INTEGER NOT NULL DEFAULT 0,
    "metaConversionValue" REAL NOT NULL DEFAULT 0,
    "linkClicks" INTEGER NOT NULL DEFAULT 0,
    "landingPageViews" INTEGER NOT NULL DEFAULT 0,
    "attributedOrders" INTEGER NOT NULL DEFAULT 0,
    "attributedRevenue" REAL NOT NULL DEFAULT 0,
    "newCustomerOrders" INTEGER NOT NULL DEFAULT 0,
    "newCustomerRevenue" REAL NOT NULL DEFAULT 0,
    "existingCustomerOrders" INTEGER NOT NULL DEFAULT 0,
    "existingCustomerRevenue" REAL NOT NULL DEFAULT 0,
    "newCustomerIdsJson" TEXT NOT NULL DEFAULT '[]',
    "utmOnlyOrders" INTEGER NOT NULL DEFAULT 0,
    "utmOnlyRevenue" REAL NOT NULL DEFAULT 0,
    "utmOnlyNewOrders" INTEGER NOT NULL DEFAULT 0,
    "utmOnlyNewRevenue" REAL NOT NULL DEFAULT 0,
    "utmOnlyNewCustomerIdsJson" TEXT NOT NULL DEFAULT '[]',
    "unverifiedRevenue" REAL NOT NULL DEFAULT 0,
    "shopifyOrders" INTEGER NOT NULL DEFAULT 0,
    "shopifyRevenue" REAL NOT NULL DEFAULT 0
);

-- CreateIndex
CREATE UNIQUE INDEX "DailyGeoRollup_shopDomain_date_level_entityId_country_key" ON "DailyGeoRollup"("shopDomain", "date", "level", "entityId", "country");
CREATE INDEX "DailyGeoRollup_shopDomain_date_idx" ON "DailyGeoRollup"("shopDomain", "date");
