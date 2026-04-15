-- CreateTable
CREATE TABLE "MetaEntity" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "shopDomain" TEXT NOT NULL,
    "entityType" TEXT NOT NULL,
    "entityId" TEXT NOT NULL,
    "createdTime" DATETIME,
    "fetchedAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- CreateIndex
CREATE INDEX "MetaEntity_shopDomain_entityType_idx" ON "MetaEntity"("shopDomain", "entityType");

-- CreateIndex
CREATE UNIQUE INDEX "MetaEntity_shopDomain_entityType_entityId_key" ON "MetaEntity"("shopDomain", "entityType", "entityId");
