-- Add name-based gender inference fields to Customer.
-- Independent of Attribution.metaGender (which is sparse and Meta-only).
-- inferredGender:           "male" | "female" | null
-- inferredGenderConfidence: 0–1, only stored when threshold met (≥0.95)
-- inferredGenderSource:     "name" — reserved for future signals
ALTER TABLE "Customer" ADD COLUMN "inferredGender" TEXT;
ALTER TABLE "Customer" ADD COLUMN "inferredGenderConfidence" REAL;
ALTER TABLE "Customer" ADD COLUMN "inferredGenderSource" TEXT;

CREATE INDEX "Customer_shopDomain_inferredGender_idx" ON "Customer"("shopDomain", "inferredGender");
