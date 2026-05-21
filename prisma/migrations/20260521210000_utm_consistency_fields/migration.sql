-- UTM consistency tracking on Shop. Populated by the nightly UTM audit so the
-- dashboard's UTM Health tile can show "X consistent / Y inconsistent" without
-- calling the Meta API on every page load.
ALTER TABLE "Shop" ADD COLUMN "utmDominantPattern" TEXT NOT NULL DEFAULT '';
ALTER TABLE "Shop" ADD COLUMN "utmAdsConsistent" INTEGER NOT NULL DEFAULT 0;
ALTER TABLE "Shop" ADD COLUMN "utmAdsInconsistent" INTEGER NOT NULL DEFAULT 0;
