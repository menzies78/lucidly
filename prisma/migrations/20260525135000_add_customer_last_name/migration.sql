-- Add full last name capture to Order. Order Explorer surfaces First/Last
-- as separate columns; the legacy customerLastInitial field is retained for
-- backward compatibility but no longer the source of truth.
ALTER TABLE "Order" ADD COLUMN "customerLastName" TEXT NOT NULL DEFAULT '';
