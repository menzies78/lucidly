/**
 * Dashboard rollup builders.
 *
 * Pre-computes ShopAnalysisCache blobs consumed by the Dashboard loader so
 * the app._index.tsx loader can render in <100ms by reading the cached blob
 * instead of scanning MetaInsight + Order + Attribution at request time.
 *
 * Blobs written:
 *   - "dashboard:matchAccuracy"
 *       { days: [{ date, matched, total, matchRate }],
 *         rate30d, rate30dDetail: { matched, total },
 *         rate7d, rate7dDetail: { matched, total },
 *         computedAt }
 *
 * Called from incrementalSync.server.js after each cycle.
 */

import db from "../db.server.js";
import { shopLocalDayKey } from "../utils/shopTime.server";

const DAY_MS = 86400000;

// How far back to keep daily buckets. Dashboard chart shows 30 days, but we
// pre-compute 90 so future tile expansions (60d, 90d trend) don't need a
// rebuild.
const LOOKBACK_DAYS = 90;

export async function rebuildMatchAccuracy(shopDomain) {
  const t0 = Date.now();

  const shopRow = await db.shop.findUnique({
    where: { shopDomain },
    select: { shopifyTimezone: true },
  });
  const tz = shopRow?.shopifyTimezone || "UTC";

  const now = new Date();
  const lookbackStart = new Date(now.getTime() - LOOKBACK_DAYS * DAY_MS);

  // ── Sources ──
  // Denominator: SUM(MetaInsight.conversions) per UTC-day (matches the
  // original loader's bucketing - MetaInsight.date is already a UTC-midnight
  // canonical handle, so toISOString().slice(0,10) gives the right key).
  //
  // Numerator: orders in the last 90 days with a confidence>0 Attribution
  // row, bucketed by Order.createdAt in shop-local time.
  //
  // We deliberately do NOT use Attribution.matchedAt - that field tracks
  // when the matcher created the row, not the conversion day, and Full
  // Re-matches would skew the chart heavily.
  const [insights, recentOrders] = await Promise.all([
    db.metaInsight.findMany({
      where: { shopDomain, date: { gte: lookbackStart } },
      select: { date: true, conversions: true },
    }),
    db.order.findMany({
      where: { shopDomain, createdAt: { gte: lookbackStart } },
      select: { shopifyOrderId: true, createdAt: true },
    }),
  ]);

  let matchedSet = new Set();
  if (recentOrders.length > 0) {
    const ids = recentOrders.map((o) => o.shopifyOrderId);
    // Chunk the IN query - SQLite param limit is ~32k.
    const CHUNK = 5000;
    for (let i = 0; i < ids.length; i += CHUNK) {
      const slice = ids.slice(i, i + CHUNK);
      const rows = await db.attribution.findMany({
        where: {
          shopDomain,
          shopifyOrderId: { in: slice },
          confidence: { gt: 0 },
        },
        select: { shopifyOrderId: true },
      });
      for (const r of rows) matchedSet.add(r.shopifyOrderId);
    }
  }

  // ── Bucket by day ──
  const metaConvByDay = new Map();
  for (const r of insights) {
    const day = r.date ? new Date(r.date).toISOString().slice(0, 10) : null;
    if (!day) continue;
    metaConvByDay.set(day, (metaConvByDay.get(day) || 0) + (r.conversions || 0));
  }

  const matchedByDay = new Map();
  for (const o of recentOrders) {
    if (!matchedSet.has(o.shopifyOrderId)) continue;
    if (!o.createdAt) continue;
    const day = shopLocalDayKey(tz, o.createdAt);
    if (!day) continue;
    matchedByDay.set(day, (matchedByDay.get(day) || 0) + 1);
  }

  const allDays = new Set([...metaConvByDay.keys(), ...matchedByDay.keys()]);
  const days = Array.from(allDays)
    .sort((a, b) => a.localeCompare(b))
    .map((day) => {
      const total = Math.round(metaConvByDay.get(day) || 0);
      const matched = matchedByDay.get(day) || 0;
      const matchRate =
        total > 0 ? Math.min(100, Math.round((matched / total) * 100)) : null;
      return { date: day, matchRate, matched, total };
    });

  // ── Rolling rates ──
  // 30d / 7d windows over the last completed period. Slice from the end of
  // `days` rather than re-filtering by date so we get the same set the
  // loader would: trailing 30 buckets ordered ascending.
  const tail = (n) => days.slice(Math.max(0, days.length - n));
  const sumRate = (slice) => {
    const matched = slice.reduce((s, d) => s + d.matched, 0);
    const total = slice.reduce((s, d) => s + d.total, 0);
    const rate = total > 0 ? Math.min(100, Math.round((matched / total) * 100)) : null;
    return { rate, matched, total };
  };
  const r30 = sumRate(tail(30));
  const r7 = sumRate(tail(7));

  const blob = {
    days,
    rate30d: r30.rate,
    rate30dDetail: { matched: r30.matched, total: r30.total },
    rate7d: r7.rate,
    rate7dDetail: { matched: r7.matched, total: r7.total },
    computedAt: new Date().toISOString(),
  };

  await db.shopAnalysisCache.upsert({
    where: { shopDomain_cacheKey: { shopDomain, cacheKey: "dashboard:matchAccuracy" } },
    create: { shopDomain, cacheKey: "dashboard:matchAccuracy", payload: JSON.stringify(blob) },
    update: { payload: JSON.stringify(blob), computedAt: new Date() },
  });

  console.log(
    `[dashboardRollups] match accuracy for ${shopDomain}: ${days.length} days, 30d=${r30.rate}% (${r30.matched}/${r30.total}) in ${Date.now() - t0}ms`,
  );
  return { days: days.length, ms: Date.now() - t0 };
}
