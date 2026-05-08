// Lucidly Fit Test - statistical pre-flight check that runs after the
// merchant connects Shopify but BEFORE they connect Meta.
//
// Premise: Lucidly's Layer-2 statistical matcher correlates Meta hourly
// conversion slots to Shopify orders by (timestamp ± 30 min, value ± 2%).
// The accuracy ceiling is bounded by how unique each order is within its
// time slot at its value. We can compute that uniqueness from Shopify
// alone - we don't need Meta to predict whether the matcher will work.
//
// For each online_store order in the last 90 days we count "rivals" -
// other online_store orders in the same hour within ±2% value. Per-order
// confidence = 100 / (1 + rivals). The volume-weighted average across
// all orders is the predicted match accuracy.
//
// Output is stored on Shop.fitTestScore (0-100) and Shop.fitTestData
// (JSON snapshot of the histogram, worst hours, AOV spread). The Fit
// Report UI reads from the JSON snapshot rather than recomputing.

import db from "../db.server.js";

const LOOKBACK_DAYS = 90;
const VALUE_TOLERANCE = 0.02; // ±2% - matches matcher.server.js
const TIME_PAD_MS = 30 * 60 * 1000; // ±30 min - matches matcher hourly slot

/**
 * Compute and persist the Fit Test for a shop.
 * @param {string} shopDomain
 * @returns {Promise<{score: number, ordersAnalysed: number, data: object}>}
 */
export async function runFitTest(shopDomain) {
  const since = new Date(Date.now() - LOOKBACK_DAYS * 24 * 3600 * 1000);

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

  // Time-window scan. Orders are sorted ascending; for each order we walk
  // forward until we leave the time pad. Within the window we count how
  // many other orders are within ±2% value. This is O(N * K) where K is
  // average orders per ±30 min window; for any normal merchant K is tiny
  // so this scales to 100k+ orders.
  const rivalCounts = new Array(orders.length).fill(0);
  for (let i = 0; i < orders.length; i++) {
    const a = orders[i];
    const aMs = a.createdAt.getTime();
    const aLow = a.totalPrice * (1 - VALUE_TOLERANCE);
    const aHigh = a.totalPrice * (1 + VALUE_TOLERANCE);

    // Walk backwards within window
    for (let j = i - 1; j >= 0; j--) {
      const b = orders[j];
      if (aMs - b.createdAt.getTime() > TIME_PAD_MS) break;
      if (b.totalPrice >= aLow && b.totalPrice <= aHigh) {
        rivalCounts[i]++;
      }
    }
    // Walk forwards within window
    for (let j = i + 1; j < orders.length; j++) {
      const b = orders[j];
      if (b.createdAt.getTime() - aMs > TIME_PAD_MS) break;
      if (b.totalPrice >= aLow && b.totalPrice <= aHigh) {
        rivalCounts[i]++;
      }
    }
  }

  // Aggregate metrics
  const totalConfidence = rivalCounts.reduce(
    (sum, r) => sum + 100 / (1 + r),
    0,
  );
  const score = Math.round(totalConfidence / orders.length);

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
      "Mixed fit. About half your orders sit alone in their time slot, the rest cluster too tightly to attribute uniquely. Layer 1 (cookie/UTM) attribution will help once it ships.";
  } else {
    verdict = "challenging";
    verdictReason =
      "High-volume narrow-AOV merchant. The statistical matcher will struggle with most orders - expect attribution gaps. Strongly recommend waiting for Layer 1.";
  }

  const data = {
    score,
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
    aov: {
      mean: Math.round(meanAov * 100) / 100,
      stdDev: Math.round(stdDev * 100) / 100,
      cv: Math.round(cv * 100) / 100,
      spread: cv >= 0.4 ? "wide" : cv >= 0.2 ? "moderate" : "narrow",
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
