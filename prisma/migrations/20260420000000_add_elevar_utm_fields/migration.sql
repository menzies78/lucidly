-- Capture UTM / click data supplied by Elevar (and, later, our own Web Pixel)
-- via order.note_attributes. fbclid and metaAdIdFromUtm are populated when
-- available; both default to empty string so existing code paths that read
-- these columns don't need null-checks.
ALTER TABLE "Order" ADD COLUMN "fbclid" TEXT NOT NULL DEFAULT '';
ALTER TABLE "Order" ADD COLUMN "metaAdIdFromUtm" TEXT NOT NULL DEFAULT '';
