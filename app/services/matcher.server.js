import db from "../db.server";
import { setProgress, completeProgress, failProgress } from "./progress.server";
import { shopLocalDayKey } from "../utils/shopTime.server";
import { invalidateShop } from "./queryCache.server";

/**
 * Lucidly Attribution Matcher
 *
 * Strategy:
 * 1. For each day, get Meta ads with purchase conversions (from MetaInsight)
 * 2. For each ad, find Shopify orders within the conversion time window
 * 3. Use exhaustive backtracking to find the combination of orders whose
 *    total revenue best matches Meta's reported conversion value
 * 4. Fall back to FAST greedy matcher if exhaustive times out
 * 5. Calculate confidence as percentage: 100 / (1 + rival_count)
 *    where rivals = other candidates with similar value in compatible time slots
 *
 * Layer system:
 * - Layer 1: Cookie/UTM based (future, 100% confidence)
 * - Layer 2: Statistical matcher (this file, variable confidence %)
 * Layer 1 matches take priority — orders already attributed by Layer 1 are excluded.
 *
 * Filters:
 * - Only isOnlineStore=true orders are matched (POS/wholesale excluded)
 * - Country preference: orders from countries where Meta spent on that date
 *   are preferred over orders from other countries (soft tiebreaker, not a hard filter)
 */

const PADDING_MINUTES = 6;
// Fallback window — used by runFillGaps to catch orders that fell outside
// the tight 6-minute window of the primary matcher. Starting tight keeps
// the candidate set small in the common case; widening to 10 rescues the
// occasional order where the Meta pixel fired ≥7 minutes after checkout.
const PADDING_MINUTES_WIDE = 10;
const REVENUE_TOLERANCE = 0.02;
const REVENUE_TOLERANCE_MEDIUM = 0.05;
const PER_AD_BUDGET_MS = 120000;
const FAST_FALLBACK_BUDGET_MS = 12000;
const MAX_CANDIDATES = 300;
const MAX_CANDIDATES_FAST = 900;
const RIVAL_VALUE_TOLERANCE = 0.02; // ±2% value = interchangeable

function sampleOffsetAtUtcHour(timezone, dateStr, utcHour) {
  const iso = `${dateStr}T${String(utcHour).padStart(2, "0")}:00:00Z`;
  const dt = new Date(iso);
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: timezone,
    hour: "numeric", minute: "numeric", hour12: false,
    year: "numeric", month: "numeric", day: "numeric",
  }).formatToParts(dt);
  const hour = parseInt(parts.find(p => p.type === "hour")?.value || String(utcHour), 10);
  const minute = parseInt(parts.find(p => p.type === "minute")?.value || "0", 10);
  return (hour * 60 + minute) - (utcHour * 60);
}

function getTimezoneOffsetMinutes(timezone, dateStr) {
  if (!timezone) return 0;
  try {
    // Sample at both 00:00 and 23:00 UTC to cover DST-transition days.
    // On non-transition days both samples agree; on fall-back / spring-forward
    // days we take the max so orders in the 1-hour ambiguous window bucket
    // to the post-transition Meta day. Zero behavior change on normal days.
    const a = sampleOffsetAtUtcHour(timezone, dateStr, 0);
    const b = sampleOffsetAtUtcHour(timezone, dateStr, 23);
    return Math.max(a, b);
  } catch {
    return 0;
  }
}

// Backward padding only: Shopify order is placed BEFORE Meta pixel fires.
// An order a few minutes before the Meta hour can have its conversion logged
// in that hour. An order after the hour would be in a later hour slot.
function hourToMinuteRange(hour, metaOffsetMinutes = 0, paddingMinutes = PADDING_MINUTES) {
  let utcStart = hour * 60 - metaOffsetMinutes;
  let utcEnd = utcStart + 59;
  utcStart -= paddingMinutes;
  if (utcStart < 0) utcStart += 1440;
  if (utcEnd < 0) utcEnd += 1440;
  if (utcStart >= 1440) utcStart -= 1440;
  if (utcEnd >= 1440) utcEnd -= 1440;
  return { start: utcStart, end: utcEnd };
}

function minuteInRange(minute, start, end) {
  if (start <= end) return minute >= start && minute <= end;
  return minute >= start || minute <= end;
}

function dateToMinute(date) {
  return date.getUTCHours() * 60 + date.getUTCMinutes();
}

/**
 * Calculate confidence % for a matched order based on rival count.
 * Rivals = unpicked candidates that could substitute for this pick
 * (same time slot + value within ±2% + at least as country-compatible).
 *
 * Country disqualification: when the pick matches Meta's day-level
 * country spend (pick.countryMatch === true) and a candidate doesn't,
 * that candidate isn't a realistic substitute even if the order total
 * coincides. Without this, two near-identical-value orders from
 * different countries always shared confidence, even when Meta's
 * own breakdown clearly pointed at one of them.
 */
function calculateConfidence(pick, allCandidates, allPicks) {
  const pickedIds = new Set(allPicks.map(p => p.id));
  let rivalCount = 0;

  for (const candidate of allCandidates) {
    if (pickedIds.has(candidate.id)) continue;
    // Value similarity check
    if (pick.total > 0) {
      const valueDiff = Math.abs(candidate.total - pick.total) / pick.total;
      if (valueDiff > RIVAL_VALUE_TOLERANCE) continue;
    }
    // Time slot compatibility — could this candidate go in the same slot?
    const hasCompatibleSlot = candidate.slots.some(s => s === pick.slot);
    if (!hasCompatibleSlot) continue;
    // Country disqualification
    if (pick.countryMatch && !candidate.countryMatch) continue;
    rivalCount++;
  }

  return {
    confidence: Math.max(1, Math.round(100 / (1 + rivalCount))),
    rivalCount,
  };
}

function countNew(picks) { let n = 0; for (const p of picks) if (p.isNew) n++; return n; }
function countCountryMatch(picks) { let n = 0; for (const p of picks) if (p.countryMatch) n++; return n; }

/**
 * Compare two candidate solutions. Returns true if the new solution is better.
 * Priority: 1) closer to target value, 2) more country matches, 3) more new customers
 */
function isBetterSolution(diff, newCount, countryCount, bestDiff, bestNewCount, bestCountryCount) {
  const EPS = 1e-9;
  if (diff < bestDiff - EPS) return true;
  if (Math.abs(diff - bestDiff) > EPS) return false;
  if (countryCount > bestCountryCount) return true;
  if (countryCount < bestCountryCount) return false;
  if (newCount > bestNewCount) return true;
  return false;
}

function chooseRSlotsFlexible(items, R, slotCaps, target, tolerance, deadline) {
  let best = null, bestDiff = Infinity, bestNewCount = -1, bestCountryCount = -1;
  let timedOut = false;
  let iterations = 0;
  const arr = items.slice().sort((a, b) => {
    const td = b.total - a.total;
    if (td !== 0) return td;
    if (b.countryMatch !== a.countryMatch) return b.countryMatch ? 1 : -1;
    if (b.isNew !== a.isNew) return b.isNew ? 1 : -1;
    return 0;
  });

  function backtrack(i, start, sum, caps, picks) {
    if (timedOut) return;
    if (++iterations % 10000 === 0 && Date.now() >= deadline) { timedOut = true; return; }
    if (i === R) {
      const diff = Math.abs(sum - target);
      if (diff <= target * tolerance) {
        const newCount = countNew(picks);
        const cCount = countCountryMatch(picks);
        if (isBetterSolution(diff, newCount, cCount, bestDiff, bestNewCount, bestCountryCount)) {
          best = picks.slice();
          bestDiff = diff;
          bestNewCount = newCount;
          bestCountryCount = cCount;
        }
      }
      return;
    }
    for (let k = start; k < arr.length; k++) {
      if (timedOut) return;
      const it = arr[k];
      for (const s of (it.slots || [])) {
        if ((caps[s] | 0) <= 0) continue;
        caps[s]--;
        picks.push({ id: it.id, total: it.total, slot: s, isNew: it.isNew, countryMatch: it.countryMatch, time: it.time, orderId: it.orderId, slots: it.slots });
        backtrack(i + 1, k + 1, sum + it.total, caps, picks);
        picks.pop();
        caps[s]++;
        if (timedOut) return;
      }
    }
  }

  backtrack(0, 0, 0, slotCaps.slice(), []);
  return { picks: best || [], timedOut };
}

function chooseRItemsIgnoreCaps(items, R, target, tolerance, deadline) {
  let best = null, bestDiff = Infinity, bestNewCount = -1, bestCountryCount = -1;
  let timedOut = false;
  let iterations = 0;
  const arr = items.slice().sort((a, b) => {
    const td = b.total - a.total;
    if (td !== 0) return td;
    if (b.countryMatch !== a.countryMatch) return b.countryMatch ? 1 : -1;
    if (b.isNew !== a.isNew) return b.isNew ? 1 : -1;
    return 0;
  });

  function backtrack(i, start, sum, picks) {
    if (timedOut) return;
    if (++iterations % 10000 === 0 && Date.now() >= deadline) { timedOut = true; return; }
    if (i === R) {
      const diff = Math.abs(sum - target);
      if (diff <= target * tolerance) {
        const newCount = countNew(picks);
        const cCount = countCountryMatch(picks);
        if (isBetterSolution(diff, newCount, cCount, bestDiff, bestNewCount, bestCountryCount)) {
          best = picks.slice();
          bestDiff = diff;
          bestNewCount = newCount;
          bestCountryCount = cCount;
        }
      }
      return;
    }
    for (let k = start; k < arr.length; k++) {
      if (timedOut) return;
      const it = arr[k];
      picks.push(it);
      backtrack(i + 1, k + 1, sum + it.total, picks);
      picks.pop();
      if (timedOut) return;
    }
  }

  backtrack(0, 0, 0, []);
  return { picks: best || [], timedOut };
}

function lowerBound(arr, x) {
  let lo = 0, hi = arr.length;
  while (lo < hi) { const mid = (lo + hi) >> 1; if (arr[mid].total < x) lo = mid + 1; else hi = mid; }
  return lo;
}

function fastGreedyMatch(pool, R, slotCaps, metaRevenue, budgetMs) {
  const startTs = Date.now();
  const slots = slotCaps.slice();
  // Sort pools: prefer new customers, then country matches, then by value
  const poolNew = pool.filter(x => x.isNew).sort((a, b) => a.total - b.total);
  const poolOld = pool.filter(x => !x.isNew).sort((a, b) => a.total - b.total);

  let bestPickSet = null, bestDiffPct = Infinity, bestSum = 0, attempt = 0;

  while (Date.now() - startTs < budgetMs) {
    attempt++;
    const caps = slots.slice();
    const picked = [];
    const pickedIds = new Set();
    let sum = 0;
    let needNew = Math.min(R, poolNew.length);
    const jitter = ((attempt % 9) - 4) * 0.0015;
    const target = metaRevenue * (1 + jitter);
    const W = R >= 25 ? 400 : 140;

    function pickOne(listSortedAsc) {
      const n = listSortedAsc.length;
      if (!n) return null;
      const remainingTarget = Math.max(0, target - sum);
      const idx = lowerBound(listSortedAsc, remainingTarget);
      let best = null, bestDelta = Infinity;
      for (let i = Math.max(0, idx - W); i <= Math.min(n - 1, idx + W); i++) {
        const cand = listSortedAsc[i];
        if (!cand || pickedIds.has(cand.id)) continue;
        let slotChosen = -1;
        for (const s of cand.slots) { if ((caps[s] | 0) > 0) { slotChosen = s; break; } }
        if (slotChosen < 0) continue;
        const delta = Math.abs((sum + cand.total) - target);
        // Prefer country-matching candidates at equal delta
        if (delta < bestDelta || (delta === bestDelta && cand.countryMatch && (!best || !best.cand.countryMatch))) {
          bestDelta = delta; best = { cand, slot: slotChosen };
        }
      }
      return best;
    }

    for (let i = 0; i < R; i++) {
      if (Date.now() - startTs > budgetMs) break;
      let choice = null;
      if (needNew > 0) choice = pickOne(poolNew);
      if (!choice) choice = pickOne(poolOld);
      if (!choice) break;
      caps[choice.slot]--;
      pickedIds.add(choice.cand.id);
      picked.push({ ...choice.cand, slot: choice.slot });
      sum += choice.cand.total;
      if (choice.cand.isNew && needNew > 0) needNew--;
    }

    if (picked.length < Math.ceil(R * 0.9)) continue;

    // 1-swap improvement
    const pickedById = new Set(picked.map(p => p.id));
    const unpicked = pool.filter(x => !pickedById.has(x.id));
    for (let pass = 0; pass < 100; pass++) {
      if (Date.now() - startTs > budgetMs) break;
      let improved = false;
      const currentDiffAbs = Math.abs(sum - metaRevenue);
      for (let pi = 0; pi < Math.min(picked.length, 50); pi++) {
        const p = picked[pi];
        caps[p.slot]++;
        for (let ui = 0; ui < Math.min(unpicked.length, 200); ui++) {
          const u = unpicked[ui];
          if (!u) continue;
          let uSlot = -1;
          for (const s of u.slots) { if ((caps[s] | 0) > 0) { uSlot = s; break; } }
          if (uSlot < 0) continue;
          const newDiff = Math.abs(sum - p.total + u.total - metaRevenue);
          // Accept swap if: closer to target, OR same diff but better country match
          if (newDiff < currentDiffAbs || (newDiff === currentDiffAbs && u.countryMatch && !p.countryMatch)) {
            caps[uSlot]--;
            sum = sum - p.total + u.total;
            picked[pi] = { ...u, slot: uSlot };
            unpicked[ui] = p;
            improved = true;
            break;
          }
        }
        caps[p.slot]--;
        if (improved) break;
      }
      if (!improved) break;
    }

    const diffPct = Math.abs(sum - metaRevenue) / (metaRevenue || 1);
    if (diffPct < bestDiffPct) { bestDiffPct = diffPct; bestPickSet = picked.slice(); bestSum = sum; }
    if (diffPct <= 0.002) break;
  }

  return { picks: bestPickSet || [], diffPct: bestDiffPct, sum: bestSum };
}

/**
 * Build a per-date lookup of countries where Meta spent money.
 * Used as a soft preference signal during matching — orders from countries
 * where Meta advertised on that date are preferred over orders from other countries.
 */
async function buildMetaSpendCountries(shopDomain) {
  const rows = await db.metaBreakdown.findMany({
    where: { shopDomain, breakdownType: "country", spend: { gt: 0 } },
    select: { date: true, breakdownValue: true },
  });
  const byDate = new Map();
  for (const row of rows) {
    const dateKey = row.date.toISOString().split("T")[0];
    if (!byDate.has(dateKey)) byDate.set(dateKey, new Set());
    byDate.get(dateKey).add(row.breakdownValue.toUpperCase());
  }
  return byDate;
}

export async function runAttribution(shopDomain) {
  console.log(`[Attribution] Starting full re-match for ${shopDomain}`);

  const shop = await db.shop.findUnique({ where: { shopDomain } });
  if (!shop) throw new Error("Shop not found");

  const revenueField = shop.revenueDefinition || "total_price";
  const tolerance = shop.matchingTolerance || REVENUE_TOLERANCE;

  const metaInsights = await db.metaInsight.findMany({
    where: { shopDomain, conversions: { gt: 0 }, hourSlot: { gte: 0 } },
    orderBy: [{ date: "asc" }, { adId: "asc" }],
  });

  if (!metaInsights.length) {
    console.log("[Attribution] No Meta insights with conversions");
    return { matched: 0, unmatched: 0 };
  }

  // CRITICAL: Only match against web orders — POS/wholesale/draft orders must never
  // enter the matching pool. They have nothing to do with Meta ad conversions.
  const allOrders = await db.order.findMany({
    where: { shopDomain, isOnlineStore: true },
    orderBy: { createdAt: "asc" },
  });

  if (!allOrders.length) {
    console.log("[Attribution] No Shopify web orders");
    return { matched: 0, unmatched: 0 };
  }

  console.log(`[Attribution] ${allOrders.length} web orders loaded (isOnlineStore=true)`);

  // Build country preference lookup from Meta breakdown data
  const metaSpendCountriesByDate = await buildMetaSpendCountries(shopDomain);
  console.log(`[Attribution] Country preference data for ${metaSpendCountriesByDate.size} dates`);

  // Full re-match: delete all Layer 2
  await db.attribution.deleteMany({ where: { shopDomain, layer: 2 } });

  // Pre-populate usedOrders with any Layer 1 matches (future-proofing)
  const layer1Attrs = await db.attribution.findMany({
    where: { shopDomain, layer: 1 },
    select: { shopifyOrderId: true },
  });
  const usedOrders = new Set();
  for (const a of layer1Attrs) {
    // Find the order DB id for this shopifyOrderId
    const order = allOrders.find(o => o.shopifyOrderId === a.shopifyOrderId);
    if (order) usedOrders.add(order.id);
  }
  if (layer1Attrs.length > 0) {
    console.log(`[Attribution] ${layer1Attrs.length} Layer 1 (cookie/UTM) matches preserved`);
  }

  const insightsByDate = new Map();
  for (const ins of metaInsights) {
    const dateKey = ins.date.toISOString().split("T")[0];
    if (!insightsByDate.has(dateKey)) insightsByDate.set(dateKey, []);
    insightsByDate.get(dateKey).push(ins);
  }

  const ordersByDate = new Map();
  for (const order of allOrders) {
    const dateKey = order.createdAt.toISOString().split("T")[0];
    if (!ordersByDate.has(dateKey)) ordersByDate.set(dateKey, []);
    ordersByDate.get(dateKey).push(order);
  }

  let totalMatched = 0, totalUnmatched = 0;
  const dates = Array.from(insightsByDate.keys()).sort();

  for (let di = 0; di < dates.length; di++) {
    const day = dates[di];
    const metaOffset = getTimezoneOffsetMinutes(shop.metaAccountTimezone, day);
    const dayCountries = metaSpendCountriesByDate.get(day) || new Set();

    setProgress(`runAttribution:${shopDomain}`, {
      status: "running",
      current: di + 1,
      total: dates.length,
      message: `Processing ${day} (${di + 1} of ${dates.length})`,
    });
    console.log(`[Attribution] Processing ${day} (${di + 1}/${dates.length})...`);
    const dayInsights = insightsByDate.get(day) || [];
    // Include orders from the last PADDING_MINUTES of the PREVIOUS day.
    // An order at 23:55 on day N-1 is eligible for Meta's hour 0 slot on day N
    // because the 6-minute backward padding wraps across midnight.
    const prevDay = new Date(new Date(day + "T00:00:00Z").getTime() - 86400000)
      .toISOString().split("T")[0];
    const prevDayOrders = ordersByDate.get(prevDay) || [];
    // Account for timezone: BST (+60) means hour 0 maps to 23:00 UTC prev day,
    // so padding starts at 22:54 UTC, not 23:54.
    const paddingCutoff = 1440 - PADDING_MINUTES - Math.max(0, metaOffset);
    const prevDayPaddingOrders = prevDayOrders.filter(o => {
      const m = o.createdAt.getUTCHours() * 60 + o.createdAt.getUTCMinutes();
      return m >= paddingCutoff;
    });
    const dayOrders = [...(ordersByDate.get(day) || []), ...prevDayPaddingOrders];

    const adTotals = new Map();
    for (const ins of dayInsights) {
      const existing = adTotals.get(ins.adId) || {
        adId: ins.adId, campaignId: ins.campaignId, campaignName: ins.campaignName || "",
        adSetId: ins.adSetId || "", adSetName: ins.adSetName || "",
        adName: ins.adName || "", totalConversions: 0, totalConversionValue: 0, slots: [],
      };
      existing.totalConversions += ins.conversions;
      existing.totalConversionValue += ins.conversionValue;
      const { start, end } = hourToMinuteRange(ins.hourSlot, metaOffset);
      existing.slots.push({ hour: ins.hourSlot, cap: ins.conversions, start, end, slotValue: ins.conversionValue });
      adTotals.set(ins.adId, existing);
    }

    const sortedAds = Array.from(adTotals.values())
      .sort((a, b) => a.totalConversions - b.totalConversions);

    for (const ad of sortedAds) {
      const metaRevenue = ad.totalConversionValue;
      const results = ad.totalConversions;

      if (!results) {
        continue;
      }

      const slots = ad.slots.sort((a, b) => a.hour - b.hour);
      const slotCaps = slots.map(s => Math.max(0, s.cap));

      const candidates = [];
      for (const order of dayOrders) {
        if (usedOrders.has(order.id)) continue;
        const orderMinute = dateToMinute(order.createdAt);
        const orderTotal = revenueField === "subtotal_price"
          ? order.frozenSubtotalPrice : order.frozenTotalPrice;

        const matchingSlots = [];
        for (let idx = 0; idx < slots.length; idx++) {
          if (minuteInRange(orderMinute, slots[idx].start, slots[idx].end)) {
            matchingSlots.push(idx);
          }
        }
        if (!matchingSlots.length) continue;

        // Country preference: true if Meta spent in this country on this date,
        // or if we have no country breakdown data for this date (don't penalise)
        const orderCountry = (order.countryCode || "").toUpperCase();
        const countryMatch = dayCountries.size === 0 || !orderCountry || dayCountries.has(orderCountry);

        candidates.push({
          id: order.id, orderId: order.shopifyOrderId, total: orderTotal,
          isNew: order.customerOrderCountAtPurchase === 1, slots: matchingSlots,
          time: order.createdAt, customerId: order.shopifyCustomerId,
          countryMatch,
        });
      }

      if (!candidates.length) {
        totalUnmatched += results;
        // Per-slot unmatched placeholders (no candidates matched any time slot)
        for (const slot of slots) {
          const slotPerConv = slot.cap > 0 ? Math.round((slot.slotValue / slot.cap) * 100) / 100 : 0;
          for (let ui = 0; ui < slot.cap; ui++) {
            await db.attribution.create({
              data: {
                shopDomain, shopifyOrderId: `unmatched_${ad.adId}_${day}_${slot.hour}_c${ui + 1}`,
                layer: 2, confidence: 0,
                metaCampaignId: ad.campaignId, metaCampaignName: ad.campaignName,
                metaAdSetId: ad.adSetId, metaAdSetName: ad.adSetName, metaAdId: ad.adId, metaAdName: ad.adName,
                matchMethod: "none", metaConversionValue: slotPerConv,
              },
            });
          }
        }
        continue;
      }

      // Zero-value conversions: Meta reports purchases but no monetary value.
      // Match against £0 Shopify orders (replacement orders) by time window only.
      if (!metaRevenue) {
        const zeroCandidates = candidates.filter(c => c.total === 0);
        // Prefer country-matching zero candidates
        zeroCandidates.sort((a, b) => (b.countryMatch ? 1 : 0) - (a.countryMatch ? 1 : 0));
        const picks = zeroCandidates.slice(0, results);
        if (picks.length > 0) {
          const pickedIds = new Set(picks.map(p => p.id));
          for (const pick of picks) {
            usedOrders.add(pick.id);
            totalMatched++;
            let rivalCount = 0;
            for (const cand of zeroCandidates) {
              if (pickedIds.has(cand.id)) continue;
              // Same country-disqualification rule as calculateConfidence above.
              if (pick.countryMatch && !cand.countryMatch) continue;
              rivalCount++;
            }
            const confidence = Math.max(1, Math.round(100 / (1 + rivalCount)));
            await db.attribution.create({
              data: {
                shopDomain, shopifyOrderId: pick.orderId || String(pick.id),
                layer: 2, confidence, rivalCount,
                metaCampaignId: ad.campaignId, metaCampaignName: ad.campaignName,
                metaAdSetId: ad.adSetId, metaAdSetName: ad.adSetName, metaAdId: ad.adId, metaAdName: ad.adName,
                isNewCustomer: pick.isNew, isNewToMeta: pick.isNew,
                matchMethod: "exhaustive", metaConversionValue: 0,
              },
            });
          }
          if (picks.length < results) {
            totalUnmatched += results - picks.length;
          }
        } else {
          totalUnmatched += results;
          for (let ui = 0; ui < results; ui++) {
            await db.attribution.create({
              data: {
                shopDomain, shopifyOrderId: `unmatched_${ad.adId}_${day}_c${ui + 1}`,
                layer: 2, confidence: 0,
                metaCampaignId: ad.campaignId, metaCampaignName: ad.campaignName,
                metaAdSetId: ad.adSetId, metaAdSetName: ad.adSetName, metaAdId: ad.adId, metaAdName: ad.adName,
                matchMethod: "none", metaConversionValue: 0,
              },
            });
          }
        }
        continue;
      }

      const totalCap = slotCaps.reduce((s, c) => s + c, 0);
      const R = Math.min(results, candidates.length, totalCap);

      const exhaustivePool = candidates.sort((a, b) => b.total - a.total).slice(0, MAX_CANDIDATES);
      const deadline = Date.now() + PER_AD_BUDGET_MS;

      let result = chooseRSlotsFlexible(exhaustivePool, R, slotCaps, metaRevenue, tolerance, deadline);
      let picks = result.picks;
      let exhaustiveTimedOut = result.timedOut;

      if (!picks.length && R > 0 && Date.now() < deadline) {
        result = chooseRItemsIgnoreCaps(exhaustivePool, R, metaRevenue, tolerance, deadline);
        picks = result.picks;
        exhaustiveTimedOut = exhaustiveTimedOut || result.timedOut;
      }

      let matchMethod = "exhaustive";

      if (!picks.length && R > 0) {
        console.log(`[Attribution] Exhaustive found no solution for ${ad.adId} on ${day}${exhaustiveTimedOut ? " (timed out)" : ""}, trying FAST...`);
        const fastPool = candidates
          .sort((a, b) => (((b.countryMatch ? 1 : 0) - (a.countryMatch ? 1 : 0)) || (b.isNew - a.isNew) || (b.total - a.total)))
          .slice(0, MAX_CANDIDATES_FAST);
        const fastResult = fastGreedyMatch(fastPool, R, slotCaps, metaRevenue, FAST_FALLBACK_BUDGET_MS);
        picks = fastResult.diffPct <= 0.05 ? fastResult.picks : [];
        matchMethod = "fast_greedy";
      }

      if (!picks.length) {
        totalUnmatched += results;
        // Per-slot unmatched placeholders: use actual slot value, not an average
        for (const slot of slots) {
          const slotPerConv = slot.cap > 0 ? Math.round((slot.slotValue / slot.cap) * 100) / 100 : 0;
          for (let ui = 0; ui < slot.cap; ui++) {
            await db.attribution.create({
              data: {
                shopDomain, shopifyOrderId: `unmatched_${ad.adId}_${day}_${slot.hour}_c${ui + 1}`,
                layer: 2, confidence: 0,
                metaCampaignId: ad.campaignId, metaCampaignName: ad.campaignName,
                metaAdSetId: ad.adSetId, metaAdSetName: ad.adSetName, metaAdId: ad.adId, metaAdName: ad.adName,
                matchMethod: "none", metaConversionValue: slotPerConv,
              },
            });
          }
        }
        continue;
      }

      // Per-slot Meta value — NOT total/picks average.
      // pick.slot tells us which slot the solver assigned this order to.
      for (const pick of picks) {
        const assignedSlot = slots[pick.slot];
        const perPickValue = assignedSlot
          ? Math.round((assignedSlot.slotValue / Math.max(1, assignedSlot.cap)) * 100) / 100
          : Math.round((metaRevenue / picks.length) * 100) / 100; // fallback

        usedOrders.add(pick.id);
        totalMatched++;

        const { confidence, rivalCount } = calculateConfidence(pick, candidates, picks);

        await db.attribution.create({
          data: {
            shopDomain, shopifyOrderId: pick.orderId || String(pick.id),
            layer: 2, confidence, rivalCount,
            metaCampaignId: ad.campaignId, metaCampaignName: ad.campaignName,
            metaAdSetId: ad.adSetId, metaAdSetName: ad.adSetName, metaAdId: ad.adId, metaAdName: ad.adName,
            isNewCustomer: pick.isNew, isNewToMeta: pick.isNew,
            matchMethod, metaConversionValue: perPickValue,
          },
        });
      }
    }
  }

  // Enrich all attributions with demographic data from MetaBreakdown
  setProgress(`runAttribution:${shopDomain}`, { status: "running", message: "Enriching attribution demographics..." });
  const { enrichAll } = await import("./attributionEnrichment.server.js");
  const enrichResult = await enrichAll(shopDomain);

  invalidateShop(shopDomain);
  completeProgress(`runAttribution:${shopDomain}`, { matched: totalMatched, unmatched: totalUnmatched, ...enrichResult });
  console.log(`[Attribution] Complete: ${totalMatched} matched, ${totalUnmatched} unmatched, ${enrichResult.enriched} demographics enriched`);
  return { matched: totalMatched, unmatched: totalUnmatched, ...enrichResult };
}

/**
 * Re-match attributions for a specific date range only.
 * Deletes existing Layer 2 attributions within the range, then re-runs matching.
 * Preserves all attributions outside the range.
 *
 * @param {string} shopDomain
 * @param {string} fromDate - YYYY-MM-DD inclusive
 * @param {string} toDate - YYYY-MM-DD inclusive
 */
export async function runDateRangeRematch(shopDomain, fromDate, toDate) {
  const taskKey = `dateRangeRematch:${shopDomain}`;
  console.log(`[Attribution] Starting date-range re-match for ${shopDomain}: ${fromDate} to ${toDate}`);

  const shop = await db.shop.findUnique({ where: { shopDomain } });
  if (!shop) throw new Error("Shop not found");

  const revenueField = shop.revenueDefinition || "total_price";
  const tolerance = shop.matchingTolerance || REVENUE_TOLERANCE;

  const rangeStart = new Date(fromDate + "T00:00:00.000Z");
  const rangeEnd = new Date(toDate + "T23:59:59.999Z");

  // Load Meta insights ONLY for the date range (hourly only — daily aggregates can't be time-matched)
  const metaInsights = await db.metaInsight.findMany({
    where: {
      shopDomain,
      conversions: { gt: 0 },
      hourSlot: { gte: 0 },
      date: { gte: rangeStart, lte: rangeEnd },
    },
    orderBy: [{ date: "asc" }, { adId: "asc" }],
  });

  if (!metaInsights.length) {
    console.log("[Attribution] No Meta insights with conversions in date range");
    completeProgress(taskKey, { matched: 0, unmatched: 0, deleted: 0 });
    return { matched: 0, unmatched: 0, deleted: 0 };
  }

  console.log(`[Attribution] ${metaInsights.length} insight rows in range`);

  // Load web orders for the date range + PADDING_MINUTES before the range start
  // (so an order at 23:55 on the day before rangeStart can match hour 0 of rangeStart).
  const paddedRangeStart = new Date(rangeStart.getTime() - PADDING_MINUTES * 60 * 1000);
  const rangeOrders = await db.order.findMany({
    where: {
      shopDomain,
      isOnlineStore: true,
      createdAt: { gte: paddedRangeStart, lte: rangeEnd },
    },
    orderBy: { createdAt: "asc" },
  });

  console.log(`[Attribution] ${rangeOrders.length} web orders in range`);

  // Build country preference lookup
  const metaSpendCountriesByDate = await buildMetaSpendCountries(shopDomain);

  // Delete existing Layer 2 attributions for this date range.
  // This includes both matched orders (by looking up order dates) and unmatched records.
  const rangeOrderIds = new Set(rangeOrders.map(o => o.shopifyOrderId));

  // Find all Layer 2 attributions linked to orders in this date range
  const existingAttrs = await db.attribution.findMany({
    where: { shopDomain, layer: 2 },
    select: { id: true, shopifyOrderId: true },
  });

  const idsToDelete = [];
  for (const attr of existingAttrs) {
    // Matched orders: check if the order falls in our date range
    if (rangeOrderIds.has(attr.shopifyOrderId)) {
      idsToDelete.push(attr.id);
      continue;
    }
    // Unmatched records: check if the date in the key falls in range
    if (attr.shopifyOrderId.startsWith("unmatched_")) {
      // Format: unmatched_ADID_YYYY-MM-DD_cN or unmatched_ADID_YYYY-MM-DD_HOUR_cN
      const parts = attr.shopifyOrderId.split("_");
      // Find the date part (YYYY-MM-DD format)
      for (const part of parts) {
        if (/^\d{4}-\d{2}-\d{2}$/.test(part)) {
          if (part >= fromDate && part <= toDate) {
            idsToDelete.push(attr.id);
          }
          break;
        }
      }
    }
  }

  if (idsToDelete.length > 0) {
    // Delete in batches to avoid SQLite limits
    const BATCH = 500;
    for (let i = 0; i < idsToDelete.length; i += BATCH) {
      await db.attribution.deleteMany({
        where: { id: { in: idsToDelete.slice(i, i + BATCH) } },
      });
    }
  }
  console.log(`[Attribution] Deleted ${idsToDelete.length} existing attributions in date range`);

  // Build usedOrders set from attributions OUTSIDE the date range
  // (orders matched for other dates should not be re-used)
  const remainingAttrs = await db.attribution.findMany({
    where: { shopDomain, confidence: { gt: 0 } },
    select: { shopifyOrderId: true },
  });
  const usedOrderIds = new Set(remainingAttrs.map(a => a.shopifyOrderId));
  const usedOrders = new Set();
  for (const order of rangeOrders) {
    if (usedOrderIds.has(order.shopifyOrderId)) usedOrders.add(order.id);
  }

  // Group insights and orders by date
  const insightsByDate = new Map();
  for (const ins of metaInsights) {
    const dateKey = ins.date.toISOString().split("T")[0];
    if (!insightsByDate.has(dateKey)) insightsByDate.set(dateKey, []);
    insightsByDate.get(dateKey).push(ins);
  }

  const ordersByDate = new Map();
  for (const order of rangeOrders) {
    const dateKey = order.createdAt.toISOString().split("T")[0];
    if (!ordersByDate.has(dateKey)) ordersByDate.set(dateKey, []);
    ordersByDate.get(dateKey).push(order);
  }

  let totalMatched = 0, totalUnmatched = 0;
  const dates = Array.from(insightsByDate.keys()).sort();

  for (let di = 0; di < dates.length; di++) {
    const day = dates[di];
    const metaOffset = getTimezoneOffsetMinutes(shop.metaAccountTimezone, day);
    const dayCountries = metaSpendCountriesByDate.get(day) || new Set();

    setProgress(taskKey, {
      status: "running",
      current: di + 1,
      total: dates.length,
      message: `Processing ${day} (${di + 1} of ${dates.length})`,
    });
    console.log(`[Attribution] Processing ${day} (${di + 1}/${dates.length})...`);
    const dayInsights = insightsByDate.get(day) || [];
    // Include previous-day padding orders (same cross-midnight fix as runAttribution)
    const prevDay2 = new Date(new Date(day + "T00:00:00Z").getTime() - 86400000)
      .toISOString().split("T")[0];
    const prevDayOrders2 = ordersByDate.get(prevDay2) || [];
    const paddingCutoff2 = 1440 - PADDING_MINUTES - Math.max(0, metaOffset);
    const prevDayPaddingOrders2 = prevDayOrders2.filter(o => {
      const m = o.createdAt.getUTCHours() * 60 + o.createdAt.getUTCMinutes();
      return m >= paddingCutoff2;
    });
    const dayOrders = [...(ordersByDate.get(day) || []), ...prevDayPaddingOrders2];

    const adTotals = new Map();
    for (const ins of dayInsights) {
      const existing = adTotals.get(ins.adId) || {
        adId: ins.adId, campaignId: ins.campaignId, campaignName: ins.campaignName || "",
        adSetId: ins.adSetId || "", adSetName: ins.adSetName || "",
        adName: ins.adName || "", totalConversions: 0, totalConversionValue: 0, slots: [],
      };
      existing.totalConversions += ins.conversions;
      existing.totalConversionValue += ins.conversionValue;
      const { start, end } = hourToMinuteRange(ins.hourSlot, metaOffset);
      existing.slots.push({ hour: ins.hourSlot, cap: ins.conversions, start, end, slotValue: ins.conversionValue });
      adTotals.set(ins.adId, existing);
    }

    const sortedAds = Array.from(adTotals.values())
      .sort((a, b) => a.totalConversions - b.totalConversions);

    for (const ad of sortedAds) {
      const metaRevenue = ad.totalConversionValue;
      const results = ad.totalConversions;

      if (!results) continue;

      const slots = ad.slots.sort((a, b) => a.hour - b.hour);
      const slotCaps = slots.map(s => Math.max(0, s.cap));

      const candidates = [];
      for (const order of dayOrders) {
        if (usedOrders.has(order.id)) continue;
        const orderMinute = dateToMinute(order.createdAt);
        const orderTotal = revenueField === "subtotal_price"
          ? order.frozenSubtotalPrice : order.frozenTotalPrice;

        const matchingSlots = [];
        for (let idx = 0; idx < slots.length; idx++) {
          if (minuteInRange(orderMinute, slots[idx].start, slots[idx].end)) {
            matchingSlots.push(idx);
          }
        }
        if (!matchingSlots.length) continue;

        const orderCountry = (order.countryCode || "").toUpperCase();
        const countryMatch = dayCountries.size === 0 || !orderCountry || dayCountries.has(orderCountry);

        candidates.push({
          id: order.id, orderId: order.shopifyOrderId, total: orderTotal,
          isNew: order.customerOrderCountAtPurchase === 1, slots: matchingSlots,
          time: order.createdAt, customerId: order.shopifyCustomerId,
          countryMatch,
        });
      }

      if (!candidates.length) {
        totalUnmatched += results;
        // Per-slot unmatched placeholders (no candidates matched any time slot)
        for (const slot of slots) {
          const slotPerConv = slot.cap > 0 ? Math.round((slot.slotValue / slot.cap) * 100) / 100 : 0;
          for (let ui = 0; ui < slot.cap; ui++) {
            await db.attribution.create({
              data: {
                shopDomain, shopifyOrderId: `unmatched_${ad.adId}_${day}_${slot.hour}_c${ui + 1}`,
                layer: 2, confidence: 0,
                metaCampaignId: ad.campaignId, metaCampaignName: ad.campaignName,
                metaAdSetId: ad.adSetId, metaAdSetName: ad.adSetName, metaAdId: ad.adId, metaAdName: ad.adName,
                matchMethod: "none", metaConversionValue: slotPerConv,
              },
            });
          }
        }
        continue;
      }

      // Zero-value conversions
      if (!metaRevenue) {
        const zeroCandidates = candidates.filter(c => c.total === 0);
        zeroCandidates.sort((a, b) => (b.countryMatch ? 1 : 0) - (a.countryMatch ? 1 : 0));
        const picks = zeroCandidates.slice(0, results);
        if (picks.length > 0) {
          const pickedIds = new Set(picks.map(p => p.id));
          for (const pick of picks) {
            usedOrders.add(pick.id);
            totalMatched++;
            let rivalCount = 0;
            for (const cand of zeroCandidates) {
              if (pickedIds.has(cand.id)) continue;
              // Same country-disqualification rule as calculateConfidence above.
              if (pick.countryMatch && !cand.countryMatch) continue;
              rivalCount++;
            }
            const confidence = Math.max(1, Math.round(100 / (1 + rivalCount)));
            await db.attribution.create({
              data: {
                shopDomain, shopifyOrderId: pick.orderId || String(pick.id),
                layer: 2, confidence, rivalCount,
                metaCampaignId: ad.campaignId, metaCampaignName: ad.campaignName,
                metaAdSetId: ad.adSetId, metaAdSetName: ad.adSetName, metaAdId: ad.adId, metaAdName: ad.adName,
                isNewCustomer: pick.isNew, isNewToMeta: pick.isNew,
                matchMethod: "exhaustive", metaConversionValue: 0,
              },
            });
          }
          if (picks.length < results) totalUnmatched += results - picks.length;
        } else {
          totalUnmatched += results;
          for (let ui = 0; ui < results; ui++) {
            await db.attribution.create({
              data: {
                shopDomain, shopifyOrderId: `unmatched_${ad.adId}_${day}_c${ui + 1}`,
                layer: 2, confidence: 0,
                metaCampaignId: ad.campaignId, metaCampaignName: ad.campaignName,
                metaAdSetId: ad.adSetId, metaAdSetName: ad.adSetName, metaAdId: ad.adId, metaAdName: ad.adName,
                matchMethod: "none", metaConversionValue: 0,
              },
            });
          }
        }
        continue;
      }

      const totalCap = slotCaps.reduce((s, c) => s + c, 0);
      const R = Math.min(results, candidates.length, totalCap);

      const exhaustivePool = candidates.sort((a, b) => b.total - a.total).slice(0, MAX_CANDIDATES);
      const deadline = Date.now() + PER_AD_BUDGET_MS;

      let result = chooseRSlotsFlexible(exhaustivePool, R, slotCaps, metaRevenue, tolerance, deadline);
      let picks = result.picks;
      let exhaustiveTimedOut = result.timedOut;

      if (!picks.length && R > 0 && Date.now() < deadline) {
        result = chooseRItemsIgnoreCaps(exhaustivePool, R, metaRevenue, tolerance, deadline);
        picks = result.picks;
        exhaustiveTimedOut = exhaustiveTimedOut || result.timedOut;
      }

      let matchMethod = "exhaustive";

      if (!picks.length && R > 0) {
        const fastPool = candidates
          .sort((a, b) => (((b.countryMatch ? 1 : 0) - (a.countryMatch ? 1 : 0)) || (b.isNew - a.isNew) || (b.total - a.total)))
          .slice(0, MAX_CANDIDATES_FAST);
        const fastResult = fastGreedyMatch(fastPool, R, slotCaps, metaRevenue, FAST_FALLBACK_BUDGET_MS);
        picks = fastResult.diffPct <= 0.05 ? fastResult.picks : [];
        matchMethod = "fast_greedy";
      }

      if (!picks.length) {
        totalUnmatched += results;
        // Per-slot unmatched placeholders: use actual slot value, not an average
        for (const slot of slots) {
          const slotPerConv = slot.cap > 0 ? Math.round((slot.slotValue / slot.cap) * 100) / 100 : 0;
          for (let ui = 0; ui < slot.cap; ui++) {
            await db.attribution.create({
              data: {
                shopDomain, shopifyOrderId: `unmatched_${ad.adId}_${day}_${slot.hour}_c${ui + 1}`,
                layer: 2, confidence: 0,
                metaCampaignId: ad.campaignId, metaCampaignName: ad.campaignName,
                metaAdSetId: ad.adSetId, metaAdSetName: ad.adSetName, metaAdId: ad.adId, metaAdName: ad.adName,
                matchMethod: "none", metaConversionValue: slotPerConv,
              },
            });
          }
        }
        continue;
      }

      // Per-slot Meta value — NOT total/picks average.
      // pick.slot tells us which slot the solver assigned this order to.
      for (const pick of picks) {
        const assignedSlot = slots[pick.slot];
        const perPickValue = assignedSlot
          ? Math.round((assignedSlot.slotValue / Math.max(1, assignedSlot.cap)) * 100) / 100
          : Math.round((metaRevenue / picks.length) * 100) / 100; // fallback

        usedOrders.add(pick.id);
        totalMatched++;

        const { confidence, rivalCount } = calculateConfidence(pick, candidates, picks);

        await db.attribution.create({
          data: {
            shopDomain, shopifyOrderId: pick.orderId || String(pick.id),
            layer: 2, confidence, rivalCount,
            metaCampaignId: ad.campaignId, metaCampaignName: ad.campaignName,
            metaAdSetId: ad.adSetId, metaAdSetName: ad.adSetName, metaAdId: ad.adId, metaAdName: ad.adName,
            isNewCustomer: pick.isNew, isNewToMeta: pick.isNew,
            matchMethod, metaConversionValue: perPickValue,
          },
        });
      }
    }
  }

  // Enrich attributions in the date range with demographic data
  setProgress(taskKey, { status: "running", message: "Enriching attribution demographics..." });
  const { enrichAll } = await import("./attributionEnrichment.server.js");
  const enrichResult = await enrichAll(shopDomain);

  invalidateShop(shopDomain);
  completeProgress(taskKey, { matched: totalMatched, unmatched: totalUnmatched, deleted: idsToDelete.length, ...enrichResult });
  console.log(`[Attribution] Date range re-match complete: ${totalMatched} matched, ${totalUnmatched} unmatched, ${idsToDelete.length} old attributions replaced, ${enrichResult.enriched} demographics enriched`);
  return { matched: totalMatched, unmatched: totalUnmatched, deleted: idsToDelete.length, ...enrichResult };
}

/**
 * Fill Gaps — auto-detects days with Meta conversions but no/missing attributions,
 * then matches ONLY those gaps without deleting any existing attributions.
 *
 * Scans the last `lookbackDays` days (default 30) for:
 *   - Days where Meta has conversions but zero matched attributions
 *   - Days where web orders exist with no attribution record at all
 *
 * Safe to run at any time — purely additive.
 */
export async function runFillGaps(shopDomain, lookbackDays = 30) {
  const taskKey = `fillGaps:${shopDomain}`;
  console.log(`[Attribution] Starting Fill Gaps for ${shopDomain} (${lookbackDays}-day lookback)`);

  const shop = await db.shop.findUnique({ where: { shopDomain } });
  if (!shop) throw new Error("Shop not found");

  const revenueField = shop.revenueDefinition || "total_price";
  const tolerance = shop.matchingTolerance || REVENUE_TOLERANCE;

  const now = new Date();
  const lookbackStart = new Date(now);
  lookbackStart.setUTCDate(now.getUTCDate() - lookbackDays);
  lookbackStart.setUTCHours(0, 0, 0, 0);
  const lookbackEnd = new Date(now);
  lookbackEnd.setUTCHours(23, 59, 59, 999);

  setProgress(taskKey, { status: "running", message: "Scanning for attribution gaps..." });

  // Load Meta conversions in the lookback window (hourly only — daily aggregates can't be time-matched)
  const metaInsights = await db.metaInsight.findMany({
    where: {
      shopDomain,
      conversions: { gt: 0 },
      hourSlot: { gte: 0 },
      date: { gte: lookbackStart, lte: lookbackEnd },
    },
    orderBy: [{ date: "asc" }, { adId: "asc" }],
  });

  // Load web orders in the lookback window + the wide padding before start.
  // Fill Gaps deliberately uses the 10-minute window — its job is to catch
  // orders the tight 6-minute primary matcher missed.
  const paddedLookbackStart = new Date(lookbackStart.getTime() - PADDING_MINUTES_WIDE * 60 * 1000);
  const allOrders = await db.order.findMany({
    where: {
      shopDomain,
      isOnlineStore: true,
      createdAt: { gte: paddedLookbackStart, lte: lookbackEnd },
    },
    orderBy: { createdAt: "asc" },
  });

  // Load all existing attributions for these orders. We also carry matchMethod
  // and metaAdId so the statistical pick can decide whether to overwrite a
  // UTM Layer 1 attribution (only allowed when the pick is unambiguous).
  const orderIds = allOrders.map(o => o.shopifyOrderId);
  const existingAttrs = await db.attribution.findMany({
    where: { shopDomain, shopifyOrderId: { in: orderIds } },
    select: { shopifyOrderId: true, confidence: true, matchMethod: true, metaAdId: true },
  });
  const attrByOrderId = new Map();
  for (const a of existingAttrs) attrByOrderId.set(a.shopifyOrderId, a);

  // Also load ALL Layer 2 attributions to know which orders are already used
  const allL2Attrs = await db.attribution.findMany({
    where: { shopDomain, layer: 2, confidence: { gt: 0 } },
    select: { shopifyOrderId: true },
  });
  const globalUsedOrderIds = new Set(allL2Attrs.map(a => a.shopifyOrderId));

  // Group Meta conversions by date
  const insightsByDate = new Map();
  for (const ins of metaInsights) {
    const dateKey = ins.date.toISOString().split("T")[0];
    if (!insightsByDate.has(dateKey)) insightsByDate.set(dateKey, []);
    insightsByDate.get(dateKey).push(ins);
  }

  // Group orders by shop-local day so Fill Gaps aligns with how MetaInsight
  // buckets data (ad-account-local day stored as UTC-midnight of that date).
  // Without this, an order placed 00:20 BST — which Meta logs under Apr 15
  // but which sits in the Apr 14 UTC range — gets mis-attributed to the
  // previous UTC day and its value pollutes the wrong day's remaining-revenue
  // math. Shop tz is used as a proxy for Meta tz; they're the same for
  // Vollebak (both Europe/London).
  const tz = shop.metaAccountTimezone || shop.shopifyTimezone || "UTC";
  const ordersByDate = new Map();
  for (const order of allOrders) {
    const dateKey = shopLocalDayKey(tz, order.createdAt);
    if (!ordersByDate.has(dateKey)) ordersByDate.set(dateKey, []);
    ordersByDate.get(dateKey).push(order);
  }

  // Any unmatched placeholder is a per-ad gap waiting to be filled, even if
  // the day's total matched-order count looks "complete" in aggregate. Fetch
  // the set of days with placeholders so we don't miss ad-level gaps hidden
  // behind spurious matches elsewhere on the day.
  const placeholderDaysRaw = await db.attribution.findMany({
    where: {
      shopDomain,
      confidence: 0,
      shopifyOrderId: { startsWith: "unmatched_" },
    },
    select: { shopifyOrderId: true },
  });
  const placeholderDays = new Set();
  for (const a of placeholderDaysRaw) {
    const m = a.shopifyOrderId.match(/^unmatched_[^_]+_(\d{4}-\d{2}-\d{2})/);
    if (m) placeholderDays.add(m[1]);
  }

  // Find gap days. A day qualifies if:
  //   (a) Meta reports more conversions than we've matched on that day, OR
  //   (b) at least one unmatched placeholder exists for an ad on that day.
  // Placeholder-gated detection catches the case where a spurious match on
  // some other ad inflates matchedCount above totalConversions and hides the
  // real per-ad gap.
  const gapDays = [];
  for (const [day, dayInsights] of insightsByDate) {
    const dayOrders = ordersByDate.get(day) || [];
    const totalConversions = dayInsights.reduce((s, i) => s + i.conversions, 0);
    const matchedCount = dayOrders.filter(o => {
      const a = attrByOrderId.get(o.shopifyOrderId);
      return a && a.confidence > 0;
    }).length;

    const hasPlaceholder = placeholderDays.has(day);
    if (totalConversions > matchedCount || hasPlaceholder) {
      const unattributedOrders = dayOrders.filter(o => !attrByOrderId.has(o.shopifyOrderId));
      gapDays.push({
        day,
        conversions: totalConversions,
        matched: matchedCount,
        unattributed: unattributedOrders.length,
        hasPlaceholder,
      });
    }
  }

  if (gapDays.length === 0) {
    console.log("[Attribution] Fill Gaps: No gaps found");
    completeProgress(taskKey, { matched: 0, unmatched: 0, gapDays: 0, message: "No gaps found — all days fully matched" });
    return { matched: 0, unmatched: 0, gapDays: 0 };
  }

  console.log(`[Attribution] Fill Gaps: Found ${gapDays.length} gap days:`);
  for (const g of gapDays) {
    console.log(`  ${g.day}: ${g.conversions} conversions, ${g.matched} matched, ${g.unattributed} unattributed orders`);
  }

  // Build country preference lookup
  const metaSpendCountriesByDate = await buildMetaSpendCountries(shopDomain);

  // Build usedOrders from globally matched attributions
  const usedOrders = new Set();
  for (const order of allOrders) {
    if (globalUsedOrderIds.has(order.shopifyOrderId)) usedOrders.add(order.id);
  }

  let totalMatched = 0, totalUnmatched = 0;

  for (let gi = 0; gi < gapDays.length; gi++) {
    const { day } = gapDays[gi];
    const metaOffset = getTimezoneOffsetMinutes(shop.metaAccountTimezone, day);
    const dayCountries = metaSpendCountriesByDate.get(day) || new Set();

    setProgress(taskKey, {
      status: "running",
      current: gi + 1,
      total: gapDays.length,
      message: `Filling gaps: ${day} (${gi + 1} of ${gapDays.length})`,
    });
    console.log(`[Attribution] Fill Gaps: Processing ${day} (${gi + 1}/${gapDays.length})...`);

    const dayInsights = insightsByDate.get(day) || [];
    // Include previous-day padding orders (cross-midnight fix)
    const prevDay3 = new Date(new Date(day + "T00:00:00Z").getTime() - 86400000)
      .toISOString().split("T")[0];
    const prevDayOrders3 = ordersByDate.get(prevDay3) || [];
    // Fill Gaps uses the wider padding so an order placed 7–10 minutes before
    // the Meta hour (which the tight 6-min primary matcher skipped) becomes
    // eligible here.
    const paddingCutoff3 = 1440 - PADDING_MINUTES_WIDE - Math.max(0, metaOffset);
    const prevDayPaddingOrders3 = prevDayOrders3.filter(o => {
      const m = o.createdAt.getUTCHours() * 60 + o.createdAt.getUTCMinutes();
      return m >= paddingCutoff3;
    });
    const dayOrders = [...(ordersByDate.get(day) || []), ...prevDayPaddingOrders3];

    const adTotals = new Map();
    for (const ins of dayInsights) {
      const existing = adTotals.get(ins.adId) || {
        adId: ins.adId, campaignId: ins.campaignId, campaignName: ins.campaignName || "",
        adSetId: ins.adSetId || "", adSetName: ins.adSetName || "",
        adName: ins.adName || "", totalConversions: 0, totalConversionValue: 0, slots: [],
      };
      existing.totalConversions += ins.conversions;
      existing.totalConversionValue += ins.conversionValue;
      const { start, end } = hourToMinuteRange(ins.hourSlot, metaOffset, PADDING_MINUTES_WIDE);
      existing.slots.push({ hour: ins.hourSlot, cap: ins.conversions, start, end, slotValue: ins.conversionValue });
      adTotals.set(ins.adId, existing);
    }

    const sortedAds = Array.from(adTotals.values())
      .sort((a, b) => a.totalConversions - b.totalConversions);

    for (const ad of sortedAds) {
      // Skip ads that already have attributions for this day
      const existingAdAttr = await db.attribution.findFirst({
        where: { shopDomain, metaAdId: ad.adId, confidence: { gt: 0 } },
        select: { shopifyOrderId: true },
      });
      // Check if there's already an attribution for this ad on this day
      const existingForAdDay = existingAttrs.filter(a =>
        a.shopifyOrderId && !a.shopifyOrderId.startsWith("unmatched_")
      );
      // Actually, we need a smarter check: does an attribution already exist for this ad+day combo?
      // The simplest safe approach: check if an unmatched record exists for this ad+day
      const unmatchedPrefix = `unmatched_${ad.adId}_${day}`;
      // Canonical id used when this sweep has to create a fresh placeholder
      // because no per-slot one exists yet. Single row; fine for Fill Gaps
      // which is working at ad-day granularity.
      const unmatchedKey = `${unmatchedPrefix}_c1`;
      const hasUnmatchedRecord = await db.attribution.findFirst({
        where: { shopDomain, shopifyOrderId: { startsWith: unmatchedPrefix } },
      });

      // Check if matched attributions already exist for this ad on this day
      // by seeing if any order on this day is already attributed to this ad
      const dayOrderIds = new Set(dayOrders.map(o => o.shopifyOrderId));
      const existingMatchedForAd = await db.attribution.findMany({
        where: {
          shopDomain,
          metaAdId: ad.adId,
          confidence: { gt: 0 },
          shopifyOrderId: { in: [...dayOrderIds] },
        },
      });

      // Count unmatched placeholders for this ad on this day — each one is a
      // confirmed gap Meta told us about that we haven't paired with an order.
      const placeholderCount = await db.attribution.count({
        where: {
          shopDomain,
          confidence: 0,
          shopifyOrderId: { startsWith: unmatchedPrefix },
        },
      });

      // Stop only when every Meta conversion for this ad-day is either (a)
      // paired with a real order or (b) explicitly held open by a placeholder
      // we *intend* to reclaim here. If placeholders exist we must not skip:
      // the placeholder is the gap. Previously this short-circuited whenever
      // existingMatchedForAd.length >= totalConversions, but a spurious match
      // on some other order could inflate that count and lock the real slot
      // out of Fill Gaps.
      if (existingMatchedForAd.length >= ad.totalConversions && placeholderCount === 0) {
        continue;
      }

      // How many NEW matches do we need? Cap by (totalConversions - matched)
      // but never below the number of placeholders so we always try to clear
      // them even if the day's arithmetic looks balanced.
      const alreadyMatched = existingMatchedForAd.length;
      const naiveNeeded = ad.totalConversions - alreadyMatched;
      let neededConversions = Math.max(placeholderCount, naiveNeeded);
      const metaRevenue = ad.totalConversionValue;
      // Compute remaining revenue as total minus already-matched — NOT a proportional
      // share. Different hour slots can have very different values, so averaging
      // produces a wrong target that the solver can't match.
      const matchedRevenue = existingMatchedForAd.reduce((s, a) => s + (a.metaConversionValue || 0), 0);
      let remainingRevenue = Math.max(0, metaRevenue - matchedRevenue);

      if (neededConversions <= 0) continue;

      const slots = ad.slots.sort((a, b) => a.hour - b.hour);
      const slotCaps = slots.map(s => Math.max(0, s.cap));

      const candidates = [];
      for (const order of dayOrders) {
        if (usedOrders.has(order.id)) continue;
        // Skip orders already claimed by a confident non-UTM attribution.
        // UTM Layer 1 attributions remain eligible — a Layer 2 pick with
        // rivalCount=0 is allowed to overwrite UTM last-click (handles
        // view-through vs click-through divergence).
        const existingAttr = attrByOrderId.get(order.shopifyOrderId);
        if (existingAttr && existingAttr.confidence > 0 && existingAttr.matchMethod !== "utm") continue;

        const orderMinute = dateToMinute(order.createdAt);
        const orderTotal = revenueField === "subtotal_price"
          ? order.frozenSubtotalPrice : order.frozenTotalPrice;

        const matchingSlots = [];
        for (let idx = 0; idx < slots.length; idx++) {
          if (minuteInRange(orderMinute, slots[idx].start, slots[idx].end)) {
            matchingSlots.push(idx);
          }
        }
        if (!matchingSlots.length) continue;

        const orderCountry = (order.countryCode || "").toUpperCase();
        const countryMatch = dayCountries.size === 0 || !orderCountry || dayCountries.has(orderCountry);

        candidates.push({
          id: order.id, orderId: order.shopifyOrderId, total: orderTotal,
          isNew: order.customerOrderCountAtPurchase === 1, slots: matchingSlots,
          time: order.createdAt, customerId: order.shopifyCustomerId,
          countryMatch,
          utmMatch: !!(order.utmConfirmedMeta && order.metaAdId === ad.adId),
        });
      }

      console.log(`[FillGaps] ${day} ad ${ad.adName}: ${dayOrders.length} dayOrders, ${candidates.length} candidates, need ${neededConversions}, remainingRev=£${Math.round(remainingRevenue * 100) / 100} (total=£${Math.round(metaRevenue * 100) / 100}, matched=£${Math.round(matchedRevenue * 100) / 100}), tol=${tolerance}`);
      if (candidates.length > 0) {
        console.log(`[FillGaps]   top candidates: ${candidates.slice(0, 3).map(c => `${c.orderId}=£${c.total} min=${dateToMinute(c.time)} utm=${c.utmMatch || false}`).join(', ')}`);
      }

      // ── UTM ground-truth pass (before statistical matching) ──
      // If a candidate has utmConfirmedMeta AND its metaAdId matches this ad,
      // it's a definitive match — no revenue tolerance needed. This handles
      // cross-midnight cases where the revenue residual drifts from the exact
      // per-slot value.
      {
        let utmMatchCount = 0;
        for (const cand of [...candidates]) {
          if (utmMatchCount >= neededConversions) break;
          const order = dayOrders.find(o => o.shopifyOrderId === cand.orderId);
          if (order?.utmConfirmedMeta && order?.metaAdId === ad.adId) {
            // Delete existing unmatched placeholder if present
            const unmatchedPrefix = `unmatched_${ad.adId}_${day}`;
            await db.attribution.deleteMany({
              where: { shopDomain, shopifyOrderId: { startsWith: unmatchedPrefix } },
            });
            // Use per-slot value from the candidate's matched slot
            const candSlot = cand.slots?.length > 0 ? ad.slots[cand.slots[0]] : null;
            const utmMetaValue = candSlot
              ? Math.round((candSlot.slotValue / Math.max(1, candSlot.cap)) * 100) / 100
              : (ad.totalConversions > 0 ? Math.round((ad.totalConversionValue / ad.totalConversions) * 100) / 100 : 0);
            // Upsert — the order may already have a Layer 1 UTM attribution
            // from the incremental pass; this overwrite agrees with it and
            // refines with per-slot Meta value.
            const utmPassAttrData = {
              shopDomain, shopifyOrderId: cand.orderId,
              layer: 2, confidence: 100, rivalCount: 0,
              metaCampaignId: ad.campaignId, metaCampaignName: ad.campaignName,
              metaAdSetId: ad.adSetId, metaAdSetName: ad.adSetName,
              metaAdId: ad.adId, metaAdName: ad.adName,
              isNewCustomer: cand.isNew, isNewToMeta: cand.isNew,
              matchMethod: "utm+fillgaps",
              metaConversionValue: utmMetaValue,
            };
            await db.attribution.upsert({
              where: { shopDomain_shopifyOrderId: { shopDomain, shopifyOrderId: cand.orderId } },
              create: utmPassAttrData,
              update: utmPassAttrData,
            });
            attrByOrderId.set(cand.orderId, { shopifyOrderId: cand.orderId, confidence: 100, matchMethod: "utm+fillgaps", metaAdId: ad.adId });
            usedOrders.add(cand.id);
            totalMatched++;
            utmMatchCount++;
            // Remove from candidates so statistical matcher doesn't re-process
            const idx = candidates.indexOf(cand);
            if (idx >= 0) candidates.splice(idx, 1);
            console.log(`[FillGaps]   UTM match: ${cand.orderId} → ${ad.adName} (100% confidence)`);
          }
        }
        if (utmMatchCount > 0) {
          neededConversions -= utmMatchCount;
          if (neededConversions <= 0) continue;
          // Recalculate remaining revenue
          const updatedMatchedForAd = await db.attribution.findMany({
            where: { shopDomain, metaAdId: ad.adId, confidence: { gt: 0 }, shopifyOrderId: { in: [...dayOrderIds] } },
          });
          const updatedMatchedRev = updatedMatchedForAd.reduce((s, a) => s + (a.metaConversionValue || 0), 0);
          remainingRevenue = Math.max(0, metaRevenue - updatedMatchedRev);
        }
      }

      if (!candidates.length) {
        // Only create an unmatched record if one doesn't already exist
        if (!hasUnmatchedRecord) {
          totalUnmatched += neededConversions;
          await db.attribution.create({
            data: {
              shopDomain, shopifyOrderId: unmatchedKey,
              layer: 2, confidence: 0,
              metaCampaignId: ad.campaignId, metaCampaignName: ad.campaignName,
              metaAdSetId: ad.adSetId, metaAdSetName: ad.adSetName, metaAdId: ad.adId, metaAdName: ad.adName,
              matchMethod: "none", metaConversionValue: remainingRevenue,
            },
          });
        }
        continue;
      }

      // Zero-value conversions
      if (!remainingRevenue) {
        const zeroCandidates = candidates.filter(c => c.total === 0);
        zeroCandidates.sort((a, b) => (b.countryMatch ? 1 : 0) - (a.countryMatch ? 1 : 0));
        const picks = zeroCandidates.slice(0, neededConversions);
        if (picks.length > 0) {
          const pickedIds = new Set(picks.map(p => p.id));
          for (const pick of picks) {
            let rivalCount = 0;
            for (const cand of zeroCandidates) {
              if (pickedIds.has(cand.id)) continue;
              // Same country-disqualification rule as calculateConfidence above.
              if (pick.countryMatch && !cand.countryMatch) continue;
              rivalCount++;
            }
            const confidence = Math.max(1, Math.round(100 / (1 + rivalCount)));
            const pickKey = pick.orderId || String(pick.id);
            const existingForPick = attrByOrderId.get(pickKey);
            // Preserve UTM Layer 1 unless this pick is unambiguous.
            if (existingForPick && existingForPick.matchMethod === "utm" && rivalCount > 0) continue;
            // If UTM agrees with this ad, blend labels so both methods show.
            const finalMethod = (existingForPick && existingForPick.matchMethod === "utm" && existingForPick.metaAdId === ad.adId)
              ? "utm + exhaustive" : "exhaustive";
            usedOrders.add(pick.id);
            totalMatched++;
            const zeroAttrData = {
              shopDomain, shopifyOrderId: pickKey,
              layer: 2, confidence, rivalCount,
              metaCampaignId: ad.campaignId, metaCampaignName: ad.campaignName,
              metaAdSetId: ad.adSetId, metaAdSetName: ad.adSetName, metaAdId: ad.adId, metaAdName: ad.adName,
              isNewCustomer: pick.isNew, isNewToMeta: pick.isNew,
              matchMethod: finalMethod, metaConversionValue: 0,
            };
            await db.attribution.upsert({
              where: { shopDomain_shopifyOrderId: { shopDomain, shopifyOrderId: pickKey } },
              create: zeroAttrData,
              update: zeroAttrData,
            });
            attrByOrderId.set(pickKey, { shopifyOrderId: pickKey, confidence, matchMethod: finalMethod, metaAdId: ad.adId });
          }
          if (picks.length < neededConversions && !hasUnmatchedRecord) {
            totalUnmatched += neededConversions - picks.length;
          }
        } else if (!hasUnmatchedRecord) {
          totalUnmatched += neededConversions;
          await db.attribution.create({
            data: {
              shopDomain, shopifyOrderId: unmatchedKey,
              layer: 2, confidence: 0,
              metaCampaignId: ad.campaignId, metaCampaignName: ad.campaignName,
              metaAdSetId: ad.adSetId, metaAdSetName: ad.adSetName, metaAdId: ad.adId, metaAdName: ad.adName,
              matchMethod: "none", metaConversionValue: 0,
            },
          });
        }
        continue;
      }

      const totalCap = slotCaps.reduce((s, c) => s + c, 0);
      const R = Math.min(neededConversions, candidates.length, totalCap);

      const exhaustivePool = candidates.sort((a, b) => b.total - a.total).slice(0, MAX_CANDIDATES);
      const deadline = Date.now() + PER_AD_BUDGET_MS;

      let result = chooseRSlotsFlexible(exhaustivePool, R, slotCaps, remainingRevenue, tolerance, deadline);
      let picks = result.picks;

      if (!picks.length && R > 0 && Date.now() < deadline) {
        result = chooseRItemsIgnoreCaps(exhaustivePool, R, remainingRevenue, tolerance, deadline);
        picks = result.picks;
      }

      let matchMethod = "exhaustive";

      if (!picks.length && R > 0) {
        const fastPool = candidates
          .sort((a, b) => (((b.countryMatch ? 1 : 0) - (a.countryMatch ? 1 : 0)) || (b.isNew - a.isNew) || (b.total - a.total)))
          .slice(0, MAX_CANDIDATES_FAST);
        const fastResult = fastGreedyMatch(fastPool, R, slotCaps, remainingRevenue, FAST_FALLBACK_BUDGET_MS);
        picks = fastResult.diffPct <= 0.05 ? fastResult.picks : [];
        matchMethod = "fast_greedy";
      }

      if (!picks.length) {
        if (!hasUnmatchedRecord) {
          totalUnmatched += neededConversions;
          await db.attribution.create({
            data: {
              shopDomain, shopifyOrderId: unmatchedKey,
              layer: 2, confidence: 0,
              metaCampaignId: ad.campaignId, metaCampaignName: ad.campaignName,
              metaAdSetId: ad.adSetId, metaAdSetName: ad.adSetName, metaAdId: ad.adId, metaAdName: ad.adName,
              matchMethod: "none", metaConversionValue: remainingRevenue,
            },
          });
        }
        continue;
      }

      // Delete the unmatched record if we now have matches
      if (hasUnmatchedRecord) {
        await db.attribution.delete({ where: { id: hasUnmatchedRecord.id } });
      }

      // Per-slot Meta value (same fix as runAttribution)
      for (const pick of picks) {
        const assignedSlot = slots[pick.slot];
        const perPickValue = assignedSlot
          ? Math.round((assignedSlot.slotValue / Math.max(1, assignedSlot.cap)) * 100) / 100
          : Math.round((remainingRevenue / picks.length) * 100) / 100;

        const { confidence, rivalCount } = calculateConfidence(pick, candidates, picks);
        const pickKey = pick.orderId || String(pick.id);
        const existingForPick = attrByOrderId.get(pickKey);
        // Preserve UTM Layer 1 unless this pick is unambiguous.
        if (existingForPick && existingForPick.matchMethod === "utm" && rivalCount > 0) continue;
        // If UTM agrees with this ad, blend labels so both methods show.
        const finalMethod = (existingForPick && existingForPick.matchMethod === "utm" && existingForPick.metaAdId === ad.adId)
          ? `utm + ${matchMethod}` : matchMethod;

        usedOrders.add(pick.id);
        totalMatched++;

        const statAttrData = {
          shopDomain, shopifyOrderId: pickKey,
          layer: 2, confidence, rivalCount,
          metaCampaignId: ad.campaignId, metaCampaignName: ad.campaignName,
          metaAdSetId: ad.adSetId, metaAdSetName: ad.adSetName, metaAdId: ad.adId, metaAdName: ad.adName,
          isNewCustomer: pick.isNew, isNewToMeta: pick.isNew,
          matchMethod: finalMethod, metaConversionValue: perPickValue,
        };
        await db.attribution.upsert({
          where: { shopDomain_shopifyOrderId: { shopDomain, shopifyOrderId: pickKey } },
          create: statAttrData,
          update: statAttrData,
        });
        attrByOrderId.set(pickKey, { shopifyOrderId: pickKey, confidence, matchMethod: finalMethod, metaAdId: ad.adId });
      }
    }
  }

  // Enrich new attributions with demographic data
  setProgress(taskKey, { status: "running", message: "Enriching attribution demographics..." });
  const { enrichAll } = await import("./attributionEnrichment.server.js");
  const enrichResult = await enrichAll(shopDomain);

  // Drop cached loader reads for this shop so the Orders / Campaigns pages
  // pick up the new attributions on the next render. Without this the UI
  // served stale "Meta Unmatched" rows for up to 2 hours after a successful
  // sweep.
  invalidateShop(shopDomain);

  const gapSummary = gapDays.map(g => g.day).join(", ");
  completeProgress(taskKey, { matched: totalMatched, unmatched: totalUnmatched, gapDays: gapDays.length, days: gapSummary, ...enrichResult });
  console.log(`[Attribution] Fill Gaps complete: ${gapDays.length} gap days (${gapSummary}), ${totalMatched} matched, ${totalUnmatched} unmatched, ${enrichResult.enriched} demographics enriched`);
  return { matched: totalMatched, unmatched: totalUnmatched, gapDays: gapDays.length, days: gapSummary };
}
