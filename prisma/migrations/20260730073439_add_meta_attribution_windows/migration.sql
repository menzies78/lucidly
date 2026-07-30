-- AlterTable
ALTER TABLE "MetaEntity" ADD COLUMN "attributionSpec" TEXT;

-- CreateTable
CREATE TABLE "MetaAttributionWindow" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "shopDomain" TEXT NOT NULL,
    "date" DATETIME NOT NULL,
    "campaignId" TEXT NOT NULL,
    "campaignName" TEXT,
    "adSetId" TEXT,
    "adSetName" TEXT,
    "adId" TEXT,
    "adName" TEXT,
    "purchasesDefault" INTEGER NOT NULL DEFAULT 0,
    "purchases1dClick" INTEGER NOT NULL DEFAULT 0,
    "purchases7dClick" INTEGER NOT NULL DEFAULT 0,
    "purchases28dClick" INTEGER NOT NULL DEFAULT 0,
    "purchases1dView" INTEGER NOT NULL DEFAULT 0,
    "purchases1dEv" INTEGER NOT NULL DEFAULT 0,
    "valueDefault" REAL NOT NULL DEFAULT 0,
    "value1dClick" REAL NOT NULL DEFAULT 0,
    "value7dClick" REAL NOT NULL DEFAULT 0,
    "value28dClick" REAL NOT NULL DEFAULT 0,
    "value1dView" REAL NOT NULL DEFAULT 0,
    "value1dEv" REAL NOT NULL DEFAULT 0,
    "importedAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- CreateIndex
CREATE INDEX "MetaAttributionWindow_shopDomain_date_idx" ON "MetaAttributionWindow"("shopDomain", "date");

-- CreateIndex
CREATE INDEX "MetaAttributionWindow_shopDomain_campaignId_date_idx" ON "MetaAttributionWindow"("shopDomain", "campaignId", "date");

-- CreateIndex
CREATE UNIQUE INDEX "MetaAttributionWindow_shopDomain_date_adId_key" ON "MetaAttributionWindow"("shopDomain", "date", "adId");
