import db from "../db.server";

/**
 * Pixel Calibration (v2 — UTM ground-truth)
 * -----------------------------------------
 * Determines which Shopify price field the merchant's Meta pixel reports as the
 * Purchase `value`. Uses UTM-confirmed orders as ground truth: when an order
 * carries Meta UTM tags AND Meta reported a conversion for that exact ad in
 * that exact hour, we KNOW the two refer to the same purchase event.
 *
 * Sample selection (strictest first):
 *   1. Order has `utmConfirmedMeta = true` (UTM ↔ ad link)
 *   2. Order has NOT been refunded or edited (`totalRefunded == 0`, `refundStatus == "none"`)
 *   3. A MetaInsight row exists for the order's adId on the order's Meta-TZ date+hour
 *      with EXACTLY 1 conversion in that slot (unambiguous value)
 *   4. Walk orders newest → oldest (most relevant, least likely to have changed
 *      since purchase). Stop after MAX_SAMPLES.
 *
 * NOTE: We only count Shopify orders that Meta ACTUALLY REPORTED as conversions.
 * Orders with Meta UTMs but no Meta report ("bonus" orders) are ignored — we
 * have nothing to compare against.
 */

const CANDIDATE_FIELDS = [
  { key: "total_price",    label: "Full total (items + shipping + tax − discounts)", getValue: (o) => o.frozenTotalPrice },
  { key: "subtotal_price", label: "Subtotal (items − discounts, no shipping/tax)",   getValue: (o) => o.frozenSubtotalPrice },
];

const MAX_SAMPLES = 300;
const MIN_SAMPLES = 5;

export async function calibratePixel(shopDomain) {
  const shop = await db.shop.findUnique({ where: { shopDomain } });
  if (!shop) throw new Error(`Shop not found: ${shopDomain}`);

  console.log(`[PixelCalibration] Starting for ${shopDomain}`);

  const pairs = await findGroundTruthPairs(shopDomain, shop);
  console.log(`[PixelCalibration] Found ${pairs.length} ground-truth pairs`);

  if (pairs.length < MIN_SAMPLES) {
    const results = {
      winner: null,
      reason: "insufficient_data",
      sampleSize: pairs.length,
      minimumRequired: MIN_SAMPLES,
      candidates: [],
    };
    await db.shop.update({
      where: { shopDomain },
      data: {
        metaValueCalibratedAt: new Date(),
        metaValueCalibrationSamples: pairs.length,
        metaValueCalibrationResults: JSON.stringify(results),
      },
    });
    return results;
  }

  // MetaInsight.conversionValue is already stored in shop currency (FX applied at sync time).
  // Do NOT re-convert here.
  const perCandidate = {};
  for (const cand of CANDIDATE_FIELDS) perCandidate[cand.key] = { key: cand.key, label: cand.label, deviations: [] };

  for (const pair of pairs) {
    const metaShopCurrency = pair.metaValue;
    if (metaShopCurrency <= 0) continue;
    for (const cand of CANDIDATE_FIELDS) {
      const shopVal = cand.getValue(pair.order) || 0;
      if (shopVal <= 0) continue;
      const dev = Math.abs(shopVal - metaShopCurrency) / metaShopCurrency;
      perCandidate[cand.key].deviations.push(dev);
    }
  }

  for (const key of Object.keys(perCandidate)) {
    const c = perCandidate[key];
    c.deviations.sort((a, b) => a - b);
    c.sampleSize = c.deviations.length;
    c.median = c.sampleSize ? c.deviations[Math.floor(c.sampleSize / 2)] : Infinity;
    c.p75    = c.sampleSize ? c.deviations[Math.floor(c.sampleSize * 0.75)] : Infinity;
    c.p95    = c.sampleSize ? c.deviations[Math.floor(c.sampleSize * 0.95)] : Infinity;
    c.mean   = c.sampleSize ? c.deviations.reduce((s, d) => s + d, 0) / c.sampleSize : Infinity;
  }

  const candidateList = Object.values(perCandidate).sort((a, b) => a.median - b.median);
  const winner = candidateList[0];

  let quality;
  if (winner.median < 0.005) quality = "excellent";
  else if (winner.median < 0.02) quality = "good";
  else if (winner.median < 0.05) quality = "fair";
  else quality = "poor";

  const tolerance = Math.min(0.05, Math.max(0.005, Math.ceil(winner.p75 * 1000) / 1000));

  const results = {
    winner: winner.key,
    winnerLabel: winner.label,
    winnerDeviation: winner.median,
    quality,
    tolerance,
    sampleSize: winner.sampleSize,
    method: "utm_ground_truth",
    candidates: candidateList.map(c => ({
      key: c.key, label: c.label, sampleSize: c.sampleSize,
      median: c.median, mean: c.mean, p75: c.p75, p95: c.p95,
    })),
  };

  await db.shop.update({
    where: { shopDomain },
    data: {
      revenueDefinition: winner.key,
      matchingTolerance: tolerance,
      metaValueCalibratedAt: new Date(),
      metaValueCalibrationSamples: winner.sampleSize,
      metaValueCalibrationResults: JSON.stringify(results),
    },
  });

  console.log(`[PixelCalibration] Winner=${winner.key} quality=${quality} medianDev=${(winner.median * 100).toFixed(2)}% tolerance=${(tolerance * 100).toFixed(2)}% samples=${winner.sampleSize}`);
  return results;
}

/**
 * Walks UTM-confirmed orders newest → oldest. For each, looks up the matching
 * MetaInsight slot. Keeps only pairs where Meta reported exactly 1 conversion
 * in that slot for that ad (so the Meta value is unambiguously this order's).
 */
async function findGroundTruthPairs(shopDomain, shop) {
  const pairs = [];
  const PAGE = 500;
  let skip = 0;

  while (pairs.length < MAX_SAMPLES) {
    const orders = await db.order.findMany({
      where: {
        shopDomain,
        utmConfirmedMeta: true,
        isOnlineStore: true,
        totalRefunded: 0,
        refundStatus: "none",
        metaAdId: { not: null },
      },
      orderBy: { createdAt: "desc" },
      skip,
      take: PAGE,
      select: {
        id: true, shopifyOrderId: true, createdAt: true,
        frozenTotalPrice: true, frozenSubtotalPrice: true,
        metaAdId: true,
      },
    });
    if (!orders.length) break;
    skip += orders.length;

    for (const o of orders) {
      if (!o.frozenTotalPrice || o.frozenTotalPrice <= 0) continue;

      // Convert order createdAt (UTC) → Meta-TZ date + hour
      const { dateStr, hourSlot } = utcToMetaSlot(o.createdAt, shop.metaAccountTimezone || "Europe/London");
      const slotDate = new Date(dateStr + "T00:00:00.000Z");

      const insight = await db.metaInsight.findUnique({
        where: {
          shopDomain_date_hourSlot_adId: {
            shopDomain, date: slotDate, hourSlot, adId: o.metaAdId,
          },
        },
        select: { conversions: true, conversionValue: true },
      });

      if (!insight) continue;
      if (insight.conversions !== 1) continue; // unambiguous slots only
      if (!insight.conversionValue || insight.conversionValue <= 0) continue;

      pairs.push({
        dateStr,
        metaValue: insight.conversionValue,
        order: o,
      });

      if (pairs.length >= MAX_SAMPLES) break;
    }
  }

  return pairs;
}

/**
 * Convert a UTC Date to the Meta ad account's local date string + hour slot.
 * Uses Intl.DateTimeFormat so DST is handled correctly per-date.
 */
function utcToMetaSlot(utcDate, tz) {
  try {
    const fmt = new Intl.DateTimeFormat("en-CA", {
      timeZone: tz,
      year: "numeric", month: "2-digit", day: "2-digit",
      hour: "2-digit", hour12: false,
    });
    const parts = fmt.formatToParts(utcDate);
    const get = (t) => parts.find(p => p.type === t)?.value;
    const dateStr = `${get("year")}-${get("month")}-${get("day")}`;
    let hourSlot = parseInt(get("hour"), 10);
    if (hourSlot === 24) hourSlot = 0;
    return { dateStr, hourSlot };
  } catch {
    const dateStr = utcDate.toISOString().split("T")[0];
    return { dateStr, hourSlot: utcDate.getUTCHours() };
  }
}
