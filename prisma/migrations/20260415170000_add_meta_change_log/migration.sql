-- Extend MetaEntity with declared + delivery lifecycle fields.
ALTER TABLE "MetaEntity" ADD COLUMN "entityName" TEXT;
ALTER TABLE "MetaEntity" ADD COLUMN "scheduledStartAt" DATETIME;
ALTER TABLE "MetaEntity" ADD COLUMN "scheduledEndAt" DATETIME;
ALTER TABLE "MetaEntity" ADD COLUMN "currentStatus" TEXT;
ALTER TABLE "MetaEntity" ADD COLUMN "lastStatusAt" DATETIME;
ALTER TABLE "MetaEntity" ADD COLUMN "effectiveStartAt" DATETIME;
ALTER TABLE "MetaEntity" ADD COLUMN "effectiveEndAt" DATETIME;

-- Meta ad-account activity log.
CREATE TABLE "MetaChange" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "shopDomain" TEXT NOT NULL,
    "eventTime" DATETIME NOT NULL,
    "category" TEXT NOT NULL,
    "rawEventType" TEXT NOT NULL,
    "objectType" TEXT NOT NULL,
    "objectId" TEXT NOT NULL,
    "objectName" TEXT NOT NULL,
    "actorId" TEXT,
    "actorName" TEXT,
    "oldValue" TEXT,
    "newValue" TEXT,
    "summary" TEXT NOT NULL,
    "rawPayload" TEXT NOT NULL,
    "createdAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX "MetaChange_shopDomain_eventTime_rawEventType_objectId_key" ON "MetaChange"("shopDomain", "eventTime", "rawEventType", "objectId");
CREATE INDEX "MetaChange_shopDomain_eventTime_idx" ON "MetaChange"("shopDomain", "eventTime");
CREATE INDEX "MetaChange_shopDomain_objectId_eventTime_idx" ON "MetaChange"("shopDomain", "objectId", "eventTime");
CREATE INDEX "MetaChange_shopDomain_category_eventTime_idx" ON "MetaChange"("shopDomain", "category", "eventTime");
