-- CreateTable
CREATE TABLE "AiInsight" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "shopDomain" TEXT NOT NULL,
    "pageKey" TEXT NOT NULL,
    "dateFrom" TEXT NOT NULL,
    "dateTo" TEXT NOT NULL,
    "dataHash" TEXT NOT NULL,
    "insights" TEXT NOT NULL,
    "generatedAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "modelId" TEXT NOT NULL DEFAULT 'claude-sonnet-4-20250514',
    "tokenCost" INTEGER NOT NULL DEFAULT 0
);

-- CreateIndex
CREATE INDEX "AiInsight_shopDomain_pageKey_idx" ON "AiInsight"("shopDomain", "pageKey");

-- CreateIndex
CREATE UNIQUE INDEX "AiInsight_shopDomain_pageKey_dateFrom_dateTo_key" ON "AiInsight"("shopDomain", "pageKey", "dateFrom", "dateTo");
