-- Composite index for the hottest MetaBreakdown query pattern:
-- loaders filter by shopDomain + breakdownType + date range and groupBy breakdownValue.
-- IF NOT EXISTS makes this safe to re-run if a previous attempt was interrupted.
CREATE INDEX IF NOT EXISTS "MetaBreakdown_shopDomain_breakdownType_date_idx"
  ON "MetaBreakdown"("shopDomain", "breakdownType", "date");
