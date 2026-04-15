-- CreateTable
CREATE TABLE "MetaCountrySnapshot" (
    "id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "shopDomain" TEXT NOT NULL,
    "date" TEXT NOT NULL,
    "adId" TEXT NOT NULL,
    "country" TEXT NOT NULL,
    "conversions" INTEGER NOT NULL,
    "conversionValue" REAL NOT NULL,
    "snapshotAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- CreateIndex
CREATE INDEX "MetaCountrySnapshot_shopDomain_date_idx" ON "MetaCountrySnapshot"("shopDomain", "date");

-- CreateIndex
CREATE UNIQUE INDEX "MetaCountrySnapshot_shopDomain_date_adId_country_key" ON "MetaCountrySnapshot"("shopDomain", "date", "adId", "country");
