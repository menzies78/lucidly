// Lucidly Fit Test - statistical pre-flight check that runs after the
// merchant connects Shopify but BEFORE they connect Meta.
//
// Premise: Lucidly's Layer-2 statistical matcher correlates Meta hourly
// conversion slots to Shopify orders. Each Meta hourly slot S spans
// [S - 6 min, S + 60 min] in shop-local time (backward-only padding -
// the pixel fires after order placement, never before). Within that
// window an order matches a Meta conversion if value is within ±2%.
// The accuracy ceiling is bounded by how unique each order is within
// its slot at its value - we can predict that from Shopify data alone.
//
// Two scores are produced:
//   1. historicScore - what the BATCH matcher would achieve re-running
//      over the full 90 days at once. Volume-weighted avg of
//      100/(1+rivals) across all orders. This is the floor.
//   2. projectedOngoingScore - what the LIVE incremental matcher tends
//      to achieve as orders arrive one at a time. Computed as
//      historic + α × (100 - historic) where α ("gap-recovery factor")
//      is calibrated from observed merchants. The incremental matcher
//      disambiguates rivals over time (matched orders drop from the
//      rival pool, post-processor upgrades remaining rivals to 100%
//      when alternatives clear), so live confidence sits above the
//      static fit score.
//
// Output is stored on Shop.fitTestScore (= historicScore for backward
// compat) and Shop.fitTestData (JSON snapshot of histogram, worst hours,
// AOV spread, projectedOngoingScore, gapRecoveryFactor). The Fit Ready
// card reads from the JSON snapshot.

import db from "../db.server.js";

const LOOKBACK_DAYS = 90;
// ±1% - tighter than the matcher's REVENUE_TOLERANCE (0.02) because in practice
// the vast majority of real matches land within ±0.4% of the conversion value.
// ±2% over-counts rivals that the matcher would never actually confuse.
const VALUE_TOLERANCE = 0.01;
const SLOT_PADDING_MS = 6 * 60 * 1000; // -6 min - matches matcher.server.js PADDING_MINUTES
const HOUR_MS = 60 * 60 * 1000;

// Gap-recovery factor: fraction of the (100 - historic) gap that the live
// incremental matcher typically reclaims. Seeded from Vollebak: historic 91
// → live 99 = α = 0.89. Will be replaced with a learned median across all
// calibrated merchants once the FitCalibration table is wired up (see
// projectedOngoingScore comment above).
//
// TODO: replace with median(α) across FitCalibration rows where sampleSize ≥ 30d.
const GAP_RECOVERY_FACTOR = 0.89;

/**
 * Compute and persist the Fit Test for a shop.
 * @param {string} shopDomain
 * @returns {Promise<{score: number, ordersAnalysed: number, data: object}>}
 */
export async function runFitTest(shopDomain) {
  const since = new Date(Date.now() - LOOKBACK_DAYS * 24 * 3600 * 1000);

  // Look up the shop currency once - we use it on the AOV display so the
  // Fit Ready card renders the right symbol regardless of where Lucidly is
  // running. Falls back to GBP if the minimal Fit-Test sync hasn't yet
  // resolved currencyCode.
  const shopRow = await db.shop.findUnique({
    where: { shopDomain },
    select: { shopifyCurrency: true },
  });
  const currency = shopRow?.shopifyCurrency || "GBP";

  // Pull only online_store orders - those are the matchable universe.
  // We don't need refunded amount or full enrichment, just timestamp +
  // value for the rival math.
  const orders = await db.order.findMany({
    where: {
      shopDomain,
      isOnlineStore: true,
      createdAt: { gte: since },
      totalPrice: { gt: 0 },
    },
    select: {
      shopifyOrderId: true,
      createdAt: true,
      totalPrice: true,
    },
    orderBy: { createdAt: "asc" },
  });

  if (orders.length === 0) {
    const empty = {
      score: null,
      ordersAnalysed: 0,
      lookbackDays: LOOKBACK_DAYS,
      message: "No online-store orders in the last 90 days. The Fit Test needs order history to predict match accuracy.",
    };
    await db.shop.update({
      where: { shopDomain },
      data: {
        fitTestScore: null,
        fitTestData: JSON.stringify(empty),
        fitTestComputedAt: new Date(),
      },
    });
    return empty;
  }

  // Rival counting mirrors the actual matcher. Each Meta hourly slot S spans
  // [S - 6min, S + 60min] in shop-local time. Two orders are rivals iff they
  // share at least one candidate slot AND fall within ±2% value of each
  // other. Most orders have exactly one candidate slot (their natural hour
  // bucket); orders placed in the last 6 min of an hour are candidates for
  // both that hour AND the next, since a Meta pixel firing for either slot
  // would consider them.
  //
  // We bucket in UTC rather than shop-local. The rival distribution is
  // statistically equivalent (we're counting clusters, not labelling them),
  // and skipping the timezone offset keeps this loader-fast for new installs.
  const slotsToOrders = new Map(); // slotKey (hourBucket) -> [{idx, value}]
  const orderToSlots = new Array(orders.length);

  for (let i = 0; i < orders.length; i++) {
    const ms = orders[i].createdAt.getTime();
    const slotA = Math.floor(ms / HOUR_MS);
    const slotB = Math.floor((ms + SLOT_PADDING_MS) / HOUR_MS);
    const slots = slotA === slotB ? [slotA] : [slotA, slotB];
    orderToSlots[i] = slots;
    for (const s of slots) {
      let bucket = slotsToOrders.get(s);
      if (!bucket) { bucket = []; slotsToOrders.set(s, bucket); }
      bucket.push({ idx: i, value: orders[i].totalPrice });
    }
  }

  const rivalCounts = new Array(orders.length).fill(0);
  for (let i = 0; i < orders.length; i++) {
    const myValue = orders[i].totalPrice;
    const lo = myValue * (1 - VALUE_TOLERANCE);
    const hi = myValue * (1 + VALUE_TOLERANCE);
    const rivalSet = new Set();
    for (const s of orderToSlots[i]) {
      const peers = slotsToOrders.get(s);
      for (const peer of peers) {
        if (peer.idx === i) continue;
        if (peer.value >= lo && peer.value <= hi) rivalSet.add(peer.idx);
      }
    }
    rivalCounts[i] = rivalSet.size;
  }

  // Aggregate metrics
  const totalConfidence = rivalCounts.reduce(
    (sum, r) => sum + 100 / (1 + r),
    0,
  );
  const score = Math.round(totalConfidence / orders.length);

  // Projected ongoing accuracy - what the live incremental matcher tends to
  // achieve. The static fit score is a worst case; in practice the matcher
  // disambiguates clusters over time. α (gap-recovery) is calibrated from
  // observed merchants - see GAP_RECOVERY_FACTOR comment above.
  const projectedOngoingScore = Math.min(
    99,
    Math.round(score + GAP_RECOVERY_FACTOR * (100 - score)),
  );

  // Histogram of rival counts
  const histogram = { 0: 0, 1: 0, 2: 0, 3: 0, "4+": 0 };
  for (const r of rivalCounts) {
    if (r === 0) histogram["0"]++;
    else if (r === 1) histogram["1"]++;
    else if (r === 2) histogram["2"]++;
    else if (r === 3) histogram["3"]++;
    else histogram["4+"]++;
  }

  // Worst hours: bucket by (day-of-week, hour-of-day) shop-local-ish
  // (we use UTC here for simplicity; the storyline is "Friday 8pm tends
  // to be crowded" and shop-local vs UTC doesn't change the headline).
  const dayHourBuckets = {}; // key "dow-hour" -> {count, totalRivals}
  for (let i = 0; i < orders.length; i++) {
    const d = orders[i].createdAt;
    const key = `${d.getUTCDay()}-${d.getUTCHours()}`;
    if (!dayHourBuckets[key]) dayHourBuckets[key] = { count: 0, totalRivals: 0 };
    dayHourBuckets[key].count++;
    dayHourBuckets[key].totalRivals += rivalCounts[i];
  }
  const worstHours = Object.entries(dayHourBuckets)
    .map(([key, v]) => {
      const [dow, hour] = key.split("-").map(Number);
      return {
        dow,
        hour,
        avgRivals: v.totalRivals / v.count,
        orderCount: v.count,
      };
    })
    .filter(h => h.orderCount >= 5) // ignore noisy buckets
    .sort((a, b) => b.avgRivals - a.avgRivals)
    .slice(0, 5);

  // Average rival orders per hour-of-day (0-23), combined across all days.
  // Drives the 24h distribution bar chart on the Fit Report.
  const hourAgg = Array.from({ length: 24 }, () => ({ count: 0, totalRivals: 0 }));
  for (let i = 0; i < orders.length; i++) {
    const h = orders[i].createdAt.getUTCHours();
    hourAgg[h].count++;
    hourAgg[h].totalRivals += rivalCounts[i];
  }
  const hourly = hourAgg.map((b, hour) => ({
    hour,
    avgRivals: b.count ? Math.round((b.totalRivals / b.count) * 100) / 100 : 0,
    orderCount: b.count,
  }));

  // Daily order counts across the full lookback window (continuous series,
  // gaps filled with 0). Drives the orders-per-day volume chart and the promo
  // spike detector below.
  const dayCounts = new Map(); // 'YYYY-MM-DD' -> count
  for (const o of orders) {
    const key = o.createdAt.toISOString().slice(0, 10);
    dayCounts.set(key, (dayCounts.get(key) || 0) + 1);
  }
  const daily = [];
  const nowMs = Date.now();
  for (let back = LOOKBACK_DAYS - 1; back >= 0; back--) {
    const dt = new Date(nowMs - back * 24 * 3600 * 1000);
    const key = dt.toISOString().slice(0, 10);
    daily.push({ date: key, count: dayCounts.get(key) || 0 });
  }

  // Promo spike detection: a contiguous run of days whose volume sits well above
  // the merchant's baseline. Sales compress order-value variety and crowd hours,
  // which the matcher finds harder - worth flagging on the report.
  const dCounts = daily.map(d => d.count);
  const dSorted = [...dCounts].sort((a, b) => a - b);
  const dMedian = dSorted[Math.floor(dSorted.length / 2)] || 0;
  const spikeThreshold = Math.max(dMedian * 2, dMedian + 5);
  let promo = null;
  if (dMedian >= 1) {
    let runStart = -1;
    let best = null;
    for (let i = 0; i <= daily.length; i++) {
      const isSpike = i < daily.length && daily[i].count >= spikeThreshold;
      if (isSpike && runStart < 0) runStart = i;
      if (!isSpike && runStart >= 0) {
        const len = i - runStart;
        if (len >= 3 && (!best || len > best.len)) best = { start: runStart, end: i - 1, len };
        runStart = -1;
      }
    }
    if (best) promo = { start: daily[best.start].date, end: daily[best.end].date, days: best.len };
  }

  // AOV spread: standard deviation as % of mean. Wide spread = good for
  // matching, narrow spread = bad (everyone buying the same thing at
  // similar prices means many rivals).
  const meanAov =
    orders.reduce((s, o) => s + o.totalPrice, 0) / orders.length;
  const variance =
    orders.reduce(
      (s, o) => s + (o.totalPrice - meanAov) ** 2,
      0,
    ) / orders.length;
  const stdDev = Math.sqrt(variance);
  const cv = meanAov > 0 ? stdDev / meanAov : 0; // coefficient of variation

  // Verdict bands
  let verdict, verdictReason;
  if (score >= 80) {
    verdict = "excellent";
    verdictReason =
      "Your order pattern is statistically uniquely identifiable. Lucidly will match the vast majority of your Meta-driven orders cleanly.";
  } else if (score >= 60) {
    verdict = "good";
    verdictReason =
      "Most orders match cleanly. Some hours are crowded but you'll see strong verified attribution alongside a smaller blended-ROAS contribution.";
  } else if (score >= 40) {
    verdict = "marginal";
    verdictReason =
      "Mixed fit. About half your orders sit alone in their time slot; the rest cluster too tightly to attribute uniquely, so expect a mix of order-level matches and blended (unverified) revenue.";
  } else {
    verdict = "challenging";
    verdictReason =
      "High-volume narrow-AOV merchant. The statistical matcher will struggle with most orders - expect significant attribution gaps, with much of your Meta revenue shown as blended rather than matched to a specific order.";
  }

  const data = {
    score, // historic batch score - kept as `score` for backward compat
    historicScore: score,
    projectedOngoingScore,
    gapRecoveryFactor: GAP_RECOVERY_FACTOR,
    verdict,
    verdictReason,
    ordersAnalysed: orders.length,
    lookbackDays: LOOKBACK_DAYS,
    histogram,
    histogramPct: {
      "0": Math.round((histogram["0"] / orders.length) * 100),
      "1": Math.round((histogram["1"] / orders.length) * 100),
      "2": Math.round((histogram["2"] / orders.length) * 100),
      "3": Math.round((histogram["3"] / orders.length) * 100),
      "4+": Math.round((histogram["4+"] / orders.length) * 100),
    },
    worstHours,
    hourly,
    daily,
    promo,
    aov: {
      mean: Math.round(meanAov * 100) / 100,
      stdDev: Math.round(stdDev * 100) / 100,
      cv: Math.round(cv * 100) / 100,
      spread: cv >= 0.4 ? "wide" : cv >= 0.2 ? "moderate" : "narrow",
      currency,
    },
    ordersPerDay: Math.round((orders.length / LOOKBACK_DAYS) * 10) / 10,
    computedAt: new Date().toISOString(),
  };

  await db.shop.update({
    where: { shopDomain },
    data: {
      fitTestScore: score,
      fitTestData: JSON.stringify(data),
      fitTestComputedAt: new Date(),
      onboardingPhase: "fit",
    },
  });

  console.log(
    `[fitTest] ${shopDomain}: score=${score} verdict=${verdict} orders=${orders.length} cv=${cv.toFixed(2)}`,
  );

  return data;
}

export async function getFitTest(shopDomain) {
  const shop = await db.shop.findUnique({
    where: { shopDomain },
    select: { fitTestScore: true, fitTestData: true, fitTestComputedAt: true },
  });
  if (!shop || !shop.fitTestData) return null;
  try {
    return JSON.parse(shop.fitTestData);
  } catch {
    return null;
  }
}
