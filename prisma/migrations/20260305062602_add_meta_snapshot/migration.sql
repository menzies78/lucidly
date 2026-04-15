-- CreateTable
CREATE TABLE "MetaSnapshot" (
    "id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "shopDomain" TEXT NOT NULL,
    "date" TEXT NOT NULL,
    "adId" TEXT NOT NULL,
    "hourSlot" INTEGER NOT NULL,
    "conversions" INTEGER NOT NULL,
    "conversionValue" REAL NOT NULL,
    "snapshotAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- CreateIndex
CREATE INDEX "MetaSnapshot_shopDomain_date_idx" ON "MetaSnapshot"("shopDomain", "date");

-- CreateIndex
CREATE UNIQUE INDEX "MetaSnapshot_shopDomain_date_adId_hourSlot_key" ON "MetaSnapshot"("shopDomain", "date", "adId", "hourSlot");
