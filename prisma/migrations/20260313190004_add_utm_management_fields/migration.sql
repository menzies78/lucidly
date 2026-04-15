-- RedefineTables
PRAGMA defer_foreign_keys=ON;
PRAGMA foreign_keys=OFF;
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
    "onboardingCompleted" BOOLEAN NOT NULL DEFAULT false
);
INSERT INTO "new_Shop" ("currencyCode", "id", "installedAt", "lastMetaSync", "lastOrderSync", "matchingTolerance", "metaAccessToken", "metaAccountTimezone", "metaAdAccountId", "metaAttributionWindow", "metaCurrency", "onboardingCompleted", "revenueDefinition", "shopDomain", "shopifyCurrency", "shopifyTimezone") SELECT "currencyCode", "id", "installedAt", "lastMetaSync", "lastOrderSync", "matchingTolerance", "metaAccessToken", "metaAccountTimezone", "metaAdAccountId", "metaAttributionWindow", "metaCurrency", "onboardingCompleted", "revenueDefinition", "shopDomain", "shopifyCurrency", "shopifyTimezone" FROM "Shop";
DROP TABLE "Shop";
ALTER TABLE "new_Shop" RENAME TO "Shop";
CREATE UNIQUE INDEX "Shop_shopDomain_key" ON "Shop"("shopDomain");
PRAGMA foreign_keys=ON;
PRAGMA defer_foreign_keys=OFF;
