-- CreateIndex
CREATE INDEX "Attribution_shopDomain_confidence_idx" ON "Attribution"("shopDomain", "confidence");

-- CreateIndex
CREATE INDEX "MetaInsight_shopDomain_adId_date_idx" ON "MetaInsight"("shopDomain", "adId", "date");

-- CreateIndex
CREATE INDEX "Order_shopDomain_isOnlineStore_createdAt_idx" ON "Order"("shopDomain", "isOnlineStore", "createdAt");
