-- CreateTable
CREATE TABLE "DailyAdDemographicRollup" (
    "id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "shopDomain" TEXT NOT NULL,
    "date" DATETIME NOT NULL,
    "adId" TEXT NOT NULL,
    "gender" TEXT NOT NULL,
    "ageBracket" TEXT NOT NULL,
    "attributedOrders" INTEGER NOT NULL DEFAULT 0,
    "attributedRevenue" REAL NOT NULL DEFAULT 0,
    "newCustomerOrders" INTEGER NOT NULL DEFAULT 0,
    "newCustomerRevenue" REAL NOT NULL DEFAULT 0,
    "existingCustomerOrders" INTEGER NOT NULL DEFAULT 0,
    "existingCustomerRevenue" REAL NOT NULL DEFAULT 0
);

-- CreateIndex
CREATE INDEX "DailyAdDemographicRollup_shopDomain_date_idx" ON "DailyAdDemographicRollup"("shopDomain", "date");

-- CreateIndex
CREATE INDEX "DailyAdDemographicRollup_shopDomain_adId_date_idx" ON "DailyAdDemographicRollup"("shopDomain", "adId", "date");

-- CreateIndex
CREATE UNIQUE INDEX "DailyAdDemographicRollup_shopDomain_date_adId_gender_ageBracket_key" ON "DailyAdDemographicRollup"("shopDomain", "date", "adId", "gender", "ageBracket");
