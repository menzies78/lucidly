-- RedefineTables
PRAGMA defer_foreign_keys=ON;
PRAGMA foreign_keys=OFF;
CREATE TABLE "new_Customer" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "shopDomain" TEXT NOT NULL,
    "shopifyCustomerId" TEXT NOT NULL,
    "emailHash" TEXT,
    "customerEmail" TEXT,
    "firstOrderDate" DATETIME,
    "lastOrderDate" DATETIME,
    "secondOrderDate" DATETIME,
    "segmentSource" TEXT NOT NULL DEFAULT 'unknown',
    "firstOrderValue" REAL NOT NULL DEFAULT 0,
    "totalOrders" INTEGER NOT NULL DEFAULT 0,
    "totalSpent" REAL NOT NULL DEFAULT 0,
    "totalRefunded" REAL NOT NULL DEFAULT 0,
    "metaOrders" INTEGER NOT NULL DEFAULT 0,
    "discountOrdersCount" INTEGER NOT NULL DEFAULT 0,
    "topProducts" TEXT,
    "avgConfidence" INTEGER,
    "isNewCustomer" BOOLEAN NOT NULL DEFAULT true,
    "metaSegment" TEXT,
    "acquisitionCampaign" TEXT,
    "acquisitionAdSet" TEXT,
    "acquisitionAd" TEXT,
    "country" TEXT,
    "city" TEXT,
    "lat" REAL,
    "lng" REAL,
    "inferredGender" TEXT,
    "inferredGenderConfidence" REAL,
    "inferredGenderSource" TEXT,
    "createdAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" DATETIME NOT NULL
);
INSERT INTO "new_Customer" ("acquisitionAd", "acquisitionAdSet", "acquisitionCampaign", "avgConfidence", "city", "country", "createdAt", "customerEmail", "discountOrdersCount", "emailHash", "firstOrderDate", "firstOrderValue", "id", "inferredGender", "inferredGenderConfidence", "inferredGenderSource", "isNewCustomer", "lastOrderDate", "lat", "lng", "metaOrders", "metaSegment", "secondOrderDate", "shopDomain", "shopifyCustomerId", "topProducts", "totalOrders", "totalRefunded", "totalSpent", "updatedAt") SELECT "acquisitionAd", "acquisitionAdSet", "acquisitionCampaign", "avgConfidence", "city", "country", "createdAt", "customerEmail", "discountOrdersCount", "emailHash", "firstOrderDate", "firstOrderValue", "id", "inferredGender", "inferredGenderConfidence", "inferredGenderSource", "isNewCustomer", "lastOrderDate", "lat", "lng", "metaOrders", "metaSegment", "secondOrderDate", "shopDomain", "shopifyCustomerId", "topProducts", "totalOrders", "totalRefunded", "totalSpent", "updatedAt" FROM "Customer";
DROP TABLE "Customer";
ALTER TABLE "new_Customer" RENAME TO "Customer";
CREATE INDEX "Customer_shopDomain_idx" ON "Customer"("shopDomain");
CREATE INDEX "Customer_shopDomain_metaSegment_idx" ON "Customer"("shopDomain", "metaSegment");
CREATE INDEX "Customer_emailHash_idx" ON "Customer"("emailHash");
CREATE INDEX "Customer_shopDomain_inferredGender_idx" ON "Customer"("shopDomain", "inferredGender");
CREATE UNIQUE INDEX "Customer_shopDomain_shopifyCustomerId_key" ON "Customer"("shopDomain", "shopifyCustomerId");
CREATE TABLE "new_Shop" (
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
    "lastRollupRebuild" DATETIME,
    "shopifyTimezone" TEXT NOT NULL DEFAULT 'Europe/London',
    "shopifyCurrency" TEXT NOT NULL DEFAULT 'GBP',
    "metaCurrency" TEXT NOT NULL DEFAULT 'GBP',
    "metaAttributionWindow" TEXT NOT NULL DEFAULT '7d_click_1d_view',
    "utmTemplate" TEXT NOT NULL DEFAULT '',
    "utmLastAudit" DATETIME,
    "utmAdsTotal" INTEGER NOT NULL DEFAULT 0,
    "utmAdsWithTags" INTEGER NOT NULL DEFAULT 0,
    "utmAdsMissing" INTEGER NOT NULL DEFAULT 0,
    "utmAdsFixed" INTEGER NOT NULL DEFAULT 0,
    "utmDominantPattern" TEXT NOT NULL DEFAULT '',
    "utmAdsConsistent" INTEGER NOT NULL DEFAULT 0,
    "utmAdsInconsistent" INTEGER NOT NULL DEFAULT 0,
    "defaultMarginPct" INTEGER,
    "onboardingCompleted" BOOLEAN NOT NULL DEFAULT false,
    "webhooksRegisteredAt" DATETIME,
    "webhooksFirstFiredAt" DATETIME,
    "metaValueCalibratedAt" DATETIME,
    "metaValueCalibrationSamples" INTEGER NOT NULL DEFAULT 0,
    "metaValueCalibrationResults" TEXT NOT NULL DEFAULT '',
    "productImagesJson" TEXT,
    "productImagesUpdatedAt" DATETIME,
    "fitTestScore" INTEGER,
    "fitTestData" TEXT,
    "fitTestComputedAt" DATETIME,
    "onboardingPhase" TEXT NOT NULL DEFAULT 'shopify',
    "onboardingStartedAt" DATETIME,
    "demoMode" BOOLEAN NOT NULL DEFAULT false,
    "demoSeededAt" DATETIME,
    "plan" TEXT NOT NULL DEFAULT 'free',
    "planChangedAt" DATETIME,
    "dataWindowStart" DATETIME,
    "historyBackfillStatus" TEXT NOT NULL DEFAULT 'none',
    "historyBackfillCursor" TEXT
);
INSERT INTO "new_Shop" ("currencyCode", "defaultMarginPct", "demoMode", "demoSeededAt", "fitTestComputedAt", "fitTestData", "fitTestScore", "id", "installedAt", "lastMetaSync", "lastOrderSync", "lastRollupRebuild", "matchingTolerance", "metaAccessToken", "metaAccountTimezone", "metaAdAccountId", "metaAttributionWindow", "metaCurrency", "metaValueCalibratedAt", "metaValueCalibrationResults", "metaValueCalibrationSamples", "onboardingCompleted", "onboardingPhase", "onboardingStartedAt", "productImagesJson", "productImagesUpdatedAt", "revenueDefinition", "shopDomain", "shopifyCurrency", "shopifyTimezone", "utmAdsConsistent", "utmAdsFixed", "utmAdsInconsistent", "utmAdsMissing", "utmAdsTotal", "utmAdsWithTags", "utmDominantPattern", "utmLastAudit", "utmTemplate", "webhooksFirstFiredAt", "webhooksRegisteredAt") SELECT "currencyCode", "defaultMarginPct", "demoMode", "demoSeededAt", "fitTestComputedAt", "fitTestData", "fitTestScore", "id", "installedAt", "lastMetaSync", "lastOrderSync", "lastRollupRebuild", "matchingTolerance", "metaAccessToken", "metaAccountTimezone", "metaAdAccountId", "metaAttributionWindow", "metaCurrency", "metaValueCalibratedAt", "metaValueCalibrationResults", "metaValueCalibrationSamples", "onboardingCompleted", "onboardingPhase", "onboardingStartedAt", "productImagesJson", "productImagesUpdatedAt", "revenueDefinition", "shopDomain", "shopifyCurrency", "shopifyTimezone", "utmAdsConsistent", "utmAdsFixed", "utmAdsInconsistent", "utmAdsMissing", "utmAdsTotal", "utmAdsWithTags", "utmDominantPattern", "utmLastAudit", "utmTemplate", "webhooksFirstFiredAt", "webhooksRegisteredAt" FROM "Shop";
DROP TABLE "Shop";
ALTER TABLE "new_Shop" RENAME TO "Shop";
CREATE UNIQUE INDEX "Shop_shopDomain_key" ON "Shop"("shopDomain");
PRAGMA foreign_keys=ON;
PRAGMA defer_foreign_keys=OFF;

-- Grandfather: every shop that existed before the plan tier shipped keeps the
-- full product. New installs get the column default ('free') once
-- FREE_TIER_ENABLED is live.
UPDATE "Shop" SET "plan" = 'paid', "planChangedAt" = CURRENT_TIMESTAMP;

-- Existing customers were classified by the matcher over full history —
-- highest evidence grade. Free-tier ingest will stamp its own grades.
UPDATE "Customer" SET "segmentSource" = 'matched';
