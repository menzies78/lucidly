/**
 * Dashboard rollup builders.
 *
 * Pre-computes ShopAnalysisCache blobs consumed by the Dashboard loader so
 * the app._index.tsx loader can render in <100ms by reading the cached blob
 * instead of scanning MetaInsight + Order + Attribution at request time.
 *
 * Blobs written:
 *   - "dashboard:matchAccuracy"
 *       { days: [{ date, matched, total, matchRate, confSum, confAvg }],
 *         rate30d, rate30dDetail: { matched, total },
 *         rate7d,  rate7dDetail:  { matched, total },
 *         rateLifetime, rateLifetimeDetail: { matched, total },
 *         conf30d, conf30dDetail: { matched, confSum },
 *         conf7d,  conf7dDetail:  { matched, confSum },
 *         confLifetime, confLifetimeDetail: { matched, confSum },
 *         computedAt }
 *
 * Called from incrementalSync.server.js after each cycle.
 */

import db from "../db.server.js";
import { shopLocalDayKey } from "../utils/shopTime.server";

const DAY_MS = 86400000;

// How far back to keep daily buckets. Two-year window covers the "lifetime"
// view on the dashboard's Match Rate / Match Confidence tiles - any installs
// older than this still get correct headline numbers (rateLifetime is
// computed from the full attribution table, not the days slice), but the
// chart is capped at 2yrs of daily points for performance.
const LOOKBACK_DAYS = 730;

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
  // Numerator: orders in the last LOOKBACK_DAYS days with a confidence>0
  // Attribution row, bucketed by Order.createdAt in shop-local time.
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

  // Pull matched orderId -> confidence so we can build per-day avg
  // confidence alongside the matched count.
  //
  // Restricted to Layer 2 (statistical matcher) attributions. The Match
  // Rate tile compares matched orders against Meta-reported conversions;
  // Layer 1 UTM-only orders don't reconcile against Meta's reporting (they
  // exist precisely because Meta failed to log the conversion - iOS
  // opt-outs, pixel drop, ad blockers), so including them produces a
  // numerator that can exceed the denominator. The Match Confidence tile
  // has the same issue worse: L1 confidence is 100 by definition, which
  // would falsely inflate the average. L1 orders are still surfaced
  // separately on the UTM Health tile and Customer Demographics.
  const matchedConf = new Map();
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
          layer: 2,
        },
        select: { shopifyOrderId: true, confidence: true },
      });
      for (const r of rows) {
        const prev = matchedConf.get(r.shopifyOrderId) || 0;
        if (r.confidence > prev) matchedConf.set(r.shopifyOrderId, r.confidence);
      }
    }
  }

  // ── Bucket by day ──
  const metaConvByDay = new Map();
  for (const r of insights) {
    const day = r.date ? new Date(r.date).toISOString().slice(0, 10) : null;
    if (!day) continue;
    metaConvByDay.set(day, (metaConvByDay.get(day) || 0) + (r.conversions || 0));
  }

  const matchedByDay = new Map();   // day -> matched count
  const confSumByDay = new Map();   // day -> sum of confidence (for avg calc)
  for (const o of recentOrders) {
    const conf = matchedConf.get(o.shopifyOrderId);
    if (!conf) continue;
    if (!o.createdAt) continue;
    const day = shopLocalDayKey(tz, o.createdAt);
    if (!day) continue;
    matchedByDay.set(day, (matchedByDay.get(day) || 0) + 1);
    confSumByDay.set(day, (confSumByDay.get(day) || 0) + conf);
  }

  const allDays = new Set([...metaConvByDay.keys(), ...matchedByDay.keys()]);
  const days = Array.from(allDays)
    .sort((a, b) => a.localeCompare(b))
    .map((day) => {
      const total = Math.round(metaConvByDay.get(day) || 0);
      const matched = matchedByDay.get(day) || 0;
      const confSum = confSumByDay.get(day) || 0;
      const matchRate =
        total > 0 ? Math.min(100, Math.round((matched / total) * 100)) : null;
      const confAvg = matched > 0 ? Math.round(confSum / matched) : null;
      return { date: day, matchRate, matched, total, confSum, confAvg };
    });

  // ── Rolling windows ──
  // 30d / 7d / lifetime over the days array. Slice from the end rather than
  // re-filter by date so we get the same set the loader would.
  const tail = (n) => days.slice(Math.max(0, days.length - n));
  const sumRate = (slice) => {
    const matched = slice.reduce((s, d) => s + d.matched, 0);
    const total = slice.reduce((s, d) => s + d.total, 0);
    const rate = total > 0 ? Math.min(100, Math.round((matched / total) * 100)) : null;
    return { rate, matched, total };
  };
  const sumConf = (slice) => {
    const matched = slice.reduce((s, d) => s + d.matched, 0);
    const confSum = slice.reduce((s, d) => s + d.confSum, 0);
    const conf = matched > 0 ? Math.round(confSum / matched) : null;
    return { conf, matched, confSum };
  };

  const r30 = sumRate(tail(30));
  const r7  = sumRate(tail(7));
  const rL  = sumRate(days);
  const c30 = sumConf(tail(30));
  const c7  = sumConf(tail(7));
  const cL  = sumConf(days);

  const blob = {
    days,
    rate30d: r30.rate,
    rate30dDetail: { matched: r30.matched, total: r30.total },
    rate7d: r7.rate,
    rate7dDetail: { matched: r7.matched, total: r7.total },
    rateLifetime: rL.rate,
    rateLifetimeDetail: { matched: rL.matched, total: rL.total },
    conf30d: c30.conf,
    conf30dDetail: { matched: c30.matched, confSum: c30.confSum },
    conf7d: c7.conf,
    conf7dDetail: { matched: c7.matched, confSum: c7.confSum },
    confLifetime: cL.conf,
    confLifetimeDetail: { matched: cL.matched, confSum: cL.confSum },
    computedAt: new Date().toISOString(),
  };

  await db.shopAnalysisCache.upsert({
    where: { shopDomain_cacheKey: { shopDomain, cacheKey: "dashboard:matchAccuracy" } },
    create: { shopDomain, cacheKey: "dashboard:matchAccuracy", payload: JSON.stringify(blob) },
    update: { payload: JSON.stringify(blob), computedAt: new Date() },
  });

  console.log(
    `[dashboardRollups] match accuracy for ${shopDomain}: ${days.length} days, lifetime=${rL.rate}% rate / ${cL.conf}% conf (${rL.matched}/${rL.total}) in ${Date.now() - t0}ms`,
  );
  return { days: days.length, ms: Date.now() - t0 };
}
