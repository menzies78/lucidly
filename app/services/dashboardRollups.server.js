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

  // Pull matched orderId -> { confidence, layer } so we can build per-day
  // matched count + avg confidence.
  //
  // Match Rate numerator: BOTH Layer 1 (UTM-confirmed Meta) and Layer 2
  // (statistical matcher). Layer 1 attributions are created in
  // incrementalSync.server.js for orders with utmConfirmedMeta=true, and
  // they actively claim a Meta conversion slot (decrementing deltaConversions
  // for that ad - see incrementalSync.server.js:534). So they DO reconcile
  // against Meta-reported conversions. Excluding them caused the recent-day
  // dip on shops with high UTM coverage: the matched orders existed but were
  // attributed by Layer 1, so the tile's `layer = 2` filter dropped them.
  //
  // Match Confidence: only Layer 2 contributes to the confidence average.
  // Layer 1 confidence is 100 by definition (UTM is a deterministic signal),
  // which would falsely inflate the statistical matcher's average. We
  // capture the layer per order and gate the confidence sum on it.
  const matched = new Map(); // orderId -> { confidence, layer }
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
        select: { shopifyOrderId: true, confidence: true, layer: true },
      });
      for (const r of rows) {
        // One Attribution row per (shop, order) by unique constraint - so
        // no merging needed. The matcher gives Layer 1 priority and skips
        // L1-claimed orders in Layer 2.
        matched.set(r.shopifyOrderId, { confidence: r.confidence, layer: r.layer });
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

  const matchedByDay = new Map();   // day -> matched count (L1 + L2)
  const confSumByDay = new Map();   // day -> sum of L2 confidence (for avg calc)
  const confCountByDay = new Map(); // day -> count of L2 attributions (denominator for avg)
  for (const o of recentOrders) {
    const m = matched.get(o.shopifyOrderId);
    if (!m) continue;
    if (!o.createdAt) continue;
    const day = shopLocalDayKey(tz, o.createdAt);
    if (!day) continue;
    matchedByDay.set(day, (matchedByDay.get(day) || 0) + 1);
    if (m.layer === 2) {
      confSumByDay.set(day, (confSumByDay.get(day) || 0) + m.confidence);
      confCountByDay.set(day, (confCountByDay.get(day) || 0) + 1);
    }
  }

  const allDays = new Set([...metaConvByDay.keys(), ...matchedByDay.keys()]);
  const days = Array.from(allDays)
    .sort((a, b) => a.localeCompare(b))
    .map((day) => {
      const matched = matchedByDay.get(day) || 0;
      // Clamp the denominator to never fall below the numerator. Two sources
      // can legitimately diverge here:
      //   (a) Layer 1 (UTM) attributions exist for orders Meta's pixel never
      //       fired on (ad blockers, iOS ATT, slow page unload). Our UTM data
      //       proves a Meta-driven conversion that Meta itself never logged.
      //   (b) Bucketing edges — Meta's UTC-day vs shop-local-day can
      //       shift one hour of conversions across the boundary.
      // Both cases push `matched > total`. The tile reads `matched / total`
      // verbatim, so we expand total to keep the ratio coherent (rate ≤ 100%
      // and counts that don't visually contradict each other).
      const rawTotal = Math.round(metaConvByDay.get(day) || 0);
      const total = Math.max(matched, rawTotal);
      const confSum = confSumByDay.get(day) || 0;
      const confCount = confCountByDay.get(day) || 0;
      const matchRate =
        total > 0 ? Math.min(100, Math.round((matched / total) * 100)) : null;
      // Confidence average is over Layer 2 attributions only - Layer 1 is
      // 100 by definition and would mask the statistical matcher's accuracy.
      const confAvg = confCount > 0 ? Math.round(confSum / confCount) : null;
      return { date: day, matchRate, matched, total, confSum, confCount, confAvg };
    });

  // ── Rolling windows ──
  // 30d / 7d / lifetime over the days array. Slice from the end rather than
  // re-filter by date so we get the same set the loader would.
  const tail = (n) => days.slice(Math.max(0, days.length - n));
  const sumRate = (slice) => {
    const matched = slice.reduce((s, d) => s + d.matched, 0);
    // Each day already has total ≥ matched (clamp above), so the summed
    // total automatically dominates the summed matched. Re-clamp anyway for
    // defensive correctness — cheap and keeps the invariant explicit.
    const rawTotal = slice.reduce((s, d) => s + d.total, 0);
    const total = Math.max(matched, rawTotal);
    const rate = total > 0 ? Math.min(100, Math.round((matched / total) * 100)) : null;
    return { rate, matched, total };
  };
  const sumConf = (slice) => {
    // Confidence is averaged over Layer 2 attributions only (see day-level
    // confAvg comment above).
    const confCount = slice.reduce((s, d) => s + (d.confCount || 0), 0);
    const confSum = slice.reduce((s, d) => s + d.confSum, 0);
    const conf = confCount > 0 ? Math.round(confSum / confCount) : null;
    return { conf, matched: confCount, confSum };
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
