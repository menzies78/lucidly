-- RedefineTables
PRAGMA defer_foreign_keys=ON;
PRAGMA foreign_keys=OFF;
CREATE TABLE "new_Attribution" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "shopDomain" TEXT NOT NULL,
    "shopifyOrderId" TEXT NOT NULL,
    "layer" INTEGER NOT NULL,
    "confidence" TEXT NOT NULL,
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
    "metaConversionValue" REAL NOT NULL DEFAULT 0
);
INSERT INTO "new_Attribution" ("confidence", "id", "isNewCustomer", "isNewToMeta", "layer", "matchMethod", "matchedAt", "metaAdId", "metaAdName", "metaAdSetId", "metaAdSetName", "metaCampaignId", "metaCampaignName", "shopDomain", "shopifyOrderId") SELECT "confidence", "id", "isNewCustomer", "isNewToMeta", "layer", "matchMethod", "matchedAt", "metaAdId", "metaAdName", "metaAdSetId", "metaAdSetName", "metaCampaignId", "metaCampaignName", "shopDomain", "shopifyOrderId" FROM "Attribution";
DROP TABLE "Attribution";
ALTER TABLE "new_Attribution" RENAME TO "Attribution";
CREATE INDEX "Attribution_shopDomain_layer_idx" ON "Attribution"("shopDomain", "layer");
CREATE INDEX "Attribution_shopDomain_metaCampaignId_idx" ON "Attribution"("shopDomain", "metaCampaignId");
CREATE UNIQUE INDEX "Attribution_shopDomain_shopifyOrderId_key" ON "Attribution"("shopDomain", "shopifyOrderId");
PRAGMA foreign_keys=ON;
PRAGMA defer_foreign_keys=OFF;
