-- AlterTable: Shop fit-test + onboarding phase fields
ALTER TABLE "Shop" ADD COLUMN "fitTestScore" INTEGER;
ALTER TABLE "Shop" ADD COLUMN "fitTestData" TEXT;
ALTER TABLE "Shop" ADD COLUMN "fitTestComputedAt" DATETIME;
ALTER TABLE "Shop" ADD COLUMN "onboardingPhase" TEXT NOT NULL DEFAULT 'shopify';
ALTER TABLE "Shop" ADD COLUMN "onboardingStartedAt" DATETIME;

-- CreateTable: IngestJob - one row per chunked ingest unit, persisted so we
-- survive restart and can show live progress to the merchant.
CREATE TABLE "IngestJob" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "shopDomain" TEXT NOT NULL,
    "phase" TEXT NOT NULL,
    "chunkLabel" TEXT NOT NULL,
    "fromDate" DATETIME,
    "toDate" DATETIME,
    "status" TEXT NOT NULL DEFAULT 'pending',
    "attempts" INTEGER NOT NULL DEFAULT 0,
    "rowsWritten" INTEGER NOT NULL DEFAULT 0,
    "errorMessage" TEXT,
    "reportRunId" TEXT,
    "createdAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "startedAt" DATETIME,
    "completedAt" DATETIME
);

-- CreateIndex
CREATE INDEX "IngestJob_shopDomain_phase_status_idx" ON "IngestJob"("shopDomain", "phase", "status");
CREATE INDEX "IngestJob_shopDomain_status_idx" ON "IngestJob"("shopDomain", "status");
