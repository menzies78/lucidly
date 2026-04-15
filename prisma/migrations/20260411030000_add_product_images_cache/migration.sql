-- DB-backed cache for product image URLs.
-- The Products loader was making a 4-5s Shopify GraphQL call on every server
-- restart to fetch all product images. Caching the JSON map per shop with a
-- 24h refresh window eliminates that cost on every cold load.
ALTER TABLE "Shop" ADD COLUMN "productImagesJson" TEXT;
ALTER TABLE "Shop" ADD COLUMN "productImagesUpdatedAt" DATETIME;
