-- CreateTable
CREATE TABLE "JourneyTouch" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "shopDomain" TEXT NOT NULL,
    "clientId" TEXT NOT NULL,
    "occurredAt" DATETIME NOT NULL,
    "source" TEXT NOT NULL DEFAULT '',
    "medium" TEXT NOT NULL DEFAULT '',
    "campaign" TEXT NOT NULL DEFAULT '',
    "content" TEXT NOT NULL DEFAULT '',
    "term" TEXT NOT NULL DEFAULT '',
    "fbclid" TEXT NOT NULL DEFAULT '',
    "landingPath" TEXT NOT NULL DEFAULT '',
    "isPaidMeta" BOOLEAN NOT NULL DEFAULT false,
    "rawUrl" TEXT NOT NULL DEFAULT '',
    "receivedAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- CreateTable
CREATE TABLE "JourneyOrderLink" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "shopDomain" TEXT NOT NULL,
    "clientId" TEXT NOT NULL,
    "shopifyOrderId" TEXT,
    "checkoutToken" TEXT NOT NULL DEFAULT '',
    "occurredAt" DATETIME NOT NULL,
    "stitched" BOOLEAN NOT NULL DEFAULT false,
    "receivedAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- CreateIndex
CREATE INDEX "JourneyTouch_shopDomain_clientId_idx" ON "JourneyTouch"("shopDomain", "clientId");

-- CreateIndex
CREATE INDEX "JourneyTouch_shopDomain_occurredAt_idx" ON "JourneyTouch"("shopDomain", "occurredAt");

-- CreateIndex
CREATE INDEX "JourneyOrderLink_shopDomain_clientId_idx" ON "JourneyOrderLink"("shopDomain", "clientId");

-- CreateIndex
CREATE INDEX "JourneyOrderLink_shopDomain_shopifyOrderId_idx" ON "JourneyOrderLink"("shopDomain", "shopifyOrderId");

-- CreateIndex
CREATE INDEX "JourneyOrderLink_shopDomain_stitched_idx" ON "JourneyOrderLink"("shopDomain", "stitched");
