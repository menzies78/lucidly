-- Add lat/lng to Customer for the Customer Map Explorer.
ALTER TABLE "Customer" ADD COLUMN "lat" REAL;
ALTER TABLE "Customer" ADD COLUMN "lng" REAL;
