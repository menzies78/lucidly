-- Delta-based per-order demographic assignment.
--
-- Each MetaBreakdown row now tracks the previous cycle's cumulative counts so
-- we can compute (current - previous) = delta per hourly poll. Seed prev = current
-- on first run so the next post-migration cycle sees a true delta of 0 (avoids
-- a one-off "everything is new" false-positive).
ALTER TABLE "MetaBreakdown" ADD COLUMN "prevConversions" INTEGER NOT NULL DEFAULT 0;
ALTER TABLE "MetaBreakdown" ADD COLUMN "prevConversionValue" REAL NOT NULL DEFAULT 0;
ALTER TABLE "MetaBreakdown" ADD COLUMN "prevObservedAt" DATETIME;

UPDATE "MetaBreakdown"
SET "prevConversions"     = "conversions",
    "prevConversionValue" = "conversionValue",
    "prevObservedAt"      = "importedAt";
