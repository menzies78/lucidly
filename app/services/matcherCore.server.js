// Pure matcher core. No DB access, no Prisma, no progress writes — everything
// here is a function of its inputs. The orchestrator in matcher.server.js
// loads data from SQLite, builds per-day contexts, and either calls matchDay
// directly (single-thread fallback path) or dispatches contexts to worker
// threads in matcherWorker.js.
//
// Splitting the pure core out lets us:
//   1. Run multiple days in parallel via worker_threads (the matcher is
//      CPU-bound — async parallelism in the same event loop gains nothing).
//   2. Keep workers light: they only import this file, no Prisma init.
//   3. Test the matching logic without spinning up the DB.
//
// matchDay() is the unit of work. It takes a fully-prepared context (insights
// for the day, candidate orders inc. prev-day padding, country preferences,
// snapshot of usedOrders at dispatch time) and returns a structured result
// the orchestrator can persist serially.

// ---- Tuning constants. Mirror the values from the prior matcher.server.js. ----

export const PADDING_MINUTES = 6;
export const PADDING_MINUTES_WIDE = 10;
export const REVENUE_TOLERANCE = 0.02;
export const REVENUE_TOLERANCE_MEDIUM = 0.02;
export const PER_AD_BUDGET_MS = 120000;
export const FAST_FALLBACK_BUDGET_MS = 12000;
export const MAX_CANDIDATES = 300;
export const MAX_CANDIDATES_FAST = 900;
export const RIVAL_VALUE_TOLERANCE = 0.02;

// ---- Time/range helpers ----

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

export function getTimezoneOffsetMinutes(timezone, dateStr) {
  if (!timezone) return 0;
  try {
    const a = sampleOffsetAtUtcHour(timezone, dateStr, 0);
    const b = sampleOffsetAtUtcHour(timezone, dateStr, 23);
    return Math.max(a, b);
  } catch {
    return 0;
  }
}

export function hourToMinuteRange(hour, metaOffsetMinutes = 0, paddingMinutes = PADDING_MINUTES) {
  let utcStart = hour * 60 - metaOffsetMinutes;
  let utcEnd = utcStart + 59;
  utcStart -= paddingMinutes;
  if (utcStart < 0) utcStart += 1440;
  if (utcEnd < 0) utcEnd += 1440;
  if (utcStart >= 1440) utcStart -= 1440;
  if (utcEnd >= 1440) utcEnd -= 1440;
  return { start: utcStart, end: utcEnd };
}

export function minuteInRange(minute, start, end) {
  if (start <= end) return minute >= start && minute <= end;
  return minute >= start || minute <= end;
}

export function dateToMinute(date) {
  return date.getUTCHours() * 60 + date.getUTCMinutes();
}

// ---- Confidence + tie-breaker helpers ----

function countNew(picks) { let n = 0; for (const p of picks) if (p.isNew) n++; return n; }
function countCountryMatch(picks) { let n = 0; for (const p of picks) if (p.countryMatch) n++; return n; }

function isBetterSolution(diff, newCount, countryCount, bestDiff, bestNewCount, bestCountryCount) {
  const EPS = 1e-9;
  if (diff < bestDiff - EPS) return true;
  if (Math.abs(diff - bestDiff) > EPS) return false;
  if (countryCount > bestCountryCount) return true;
  if (countryCount < bestCountryCount) return false;
  if (newCount > bestNewCount) return true;
  return false;
}

function calculateConfidence(pick, allCandidates, allPicks) {
  const pickedIds = new Set(allPicks.map(p => p.id));
  let rivalCount = 0;
  for (const candidate of allCandidates) {
    if (pickedIds.has(candidate.id)) continue;
    if (pick.total > 0) {
      const valueDiff = Math.abs(candidate.total - pick.total) / pick.total;
      if (valueDiff > RIVAL_VALUE_TOLERANCE) continue;
    }
    const hasCompatibleSlot = candidate.slots.some(s => s === pick.slot);
    if (!hasCompatibleSlot) continue;
    if (pick.countryMatch && !candidate.countryMatch) continue;
    rivalCount++;
  }
  return { confidence: Math.max(1, Math.round(100 / (1 + rivalCount))), rivalCount };
}

// ---- Exhaustive backtracking ----

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

// ---- FAST greedy fallback (last resort when exhaustive times out) ----

function lowerBound(arr, x) {
  let lo = 0, hi = arr.length;
  while (lo < hi) { const mid = (lo + hi) >> 1; if (arr[mid].total < x) lo = mid + 1; else hi = mid; }
  return lo;
}

function fastGreedyMatch(pool, R, slotCaps, metaRevenue, budgetMs) {
  const startTs = Date.now();
  const slots = slotCaps.slice();
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

// ---- The unit of parallel work: one day. ----
//
// Input: a fully-prepared day context, no DB access required from this fn.
//   {
//     shopDomain, day (YYYY-MM-DD),
//     metaAccountTimezone, revenueField, tolerance,
//     dayInsights:    [{ adId, campaignId, ..., hourSlot, conversions, conversionValue }, ...]
//     dayOrders:      [{ id, shopifyOrderId, createdAt, frozenTotalPrice, frozenSubtotalPrice,
//                        customerOrderCountAtPurchase, countryCode, shopifyCustomerId }, ...]
//     dayCountries:   Set<string> upper-case country codes Meta spent in on this day
//     usedOrderIds:   Set<number> Order.id values already attributed
//   }
//
// Output:
//   {
//     day,
//     attributions: [...rows ready to db.attribution.create({ data })],
//     pickedOrderIds: [Order.id, ...]   // to add to the global usedOrders set
//     matched: int, unmatched: int,
//     bucketStats: [{ adId, hour, metaConv, metaValue, matchedConv, matchedValue }, ...]
//       One entry per (ad, hourSlot) seen in dayInsights. Orchestrator writes
//       these into MetaSnapshot so the hourly/daily delta matcher knows how
//       many conversions the bulk pass already attributed for each bucket —
//       without this, the next sweep recomputes delta=meta_reported and
//       creates phantom orphans for every conversion the bulk pass matched.
//   }
export function matchDay(ctx) {
  const {
    shopDomain, day,
    metaAccountTimezone, revenueField, tolerance,
    dayInsights, dayOrders, dayCountries, usedOrderIds,
  } = ctx;

  const metaOffset = getTimezoneOffsetMinutes(metaAccountTimezone, day);
  const usedSet = usedOrderIds instanceof Set ? usedOrderIds : new Set(usedOrderIds || []);

  // Aggregate insights into per-ad totals + slot lists.
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

  const attributions = [];
  const pickedOrderIds = [];
  const bucketStats = [];
  let matched = 0, unmatched = 0;

  // Resolve which slot index a pick belongs to. Picks from the exhaustive
  // backtracker carry a numeric `slot`; picks from `chooseRItemsIgnoreCaps`
  // (the no-cap fallback) and from the zero-revenue branch don't, so we
  // fall back to matching by time window. Used to attribute matched counts
  // to the correct bucket for the snapshot write.
  function resolveSlotIdx(pick, slots) {
    if (typeof pick.slot === "number" && pick.slot >= 0 && pick.slot < slots.length) return pick.slot;
    if (Array.isArray(pick.slots) && pick.slots.length > 0) return pick.slots[0];
    if (pick.time && typeof pick.time.getUTCHours === "function") {
      const m = pick.time.getUTCHours() * 60 + pick.time.getUTCMinutes();
      for (let si = 0; si < slots.length; si++) {
        if (minuteInRange(m, slots[si].start, slots[si].end)) return si;
      }
    }
    return 0;
  }

  function pushBucketStats(ad, slots, matchedBySlot, matchedValueBySlot) {
    for (let si = 0; si < slots.length; si++) {
      bucketStats.push({
        adId: ad.adId,
        hour: slots[si].hour,
        metaConv: slots[si].cap,
        metaValue: Math.round((slots[si].slotValue || 0) * 100) / 100,
        matchedConv: matchedBySlot[si] || 0,
        matchedValue: Math.round((matchedValueBySlot[si] || 0) * 100) / 100,
      });
    }
  }

  for (const ad of sortedAds) {
    const metaRevenue = ad.totalConversionValue;
    const results = ad.totalConversions;
    if (!results) continue;

    const slots = ad.slots.sort((a, b) => a.hour - b.hour);
    const slotCaps = slots.map(s => Math.max(0, s.cap));
    const matchedBySlot = new Array(slots.length).fill(0);
    const matchedValueBySlot = new Array(slots.length).fill(0);

    const candidates = [];
    for (const order of dayOrders) {
      if (usedSet.has(order.id)) continue;
      const orderMinute = dateToMinute(order.createdAt);
      // For total-price matching, prefer Order.netPaid (Σ line-item totalPrice
      // minus refundedAmount). It's the only value that's correct for Shopify
      // exchanges, where Order.frozenTotalPrice = A + B but the customer paid
      // only the price of the kept item. Subtotal path stays on
      // frozenSubtotalPrice — different code path, different scaling.
      const orderTotal = revenueField === "subtotal_price"
        ? order.frozenSubtotalPrice
        : (order.netPaid != null ? order.netPaid : order.frozenTotalPrice);

      const matchingSlots = [];
      for (let idx = 0; idx < slots.length; idx++) {
        if (minuteInRange(orderMinute, slots[idx].start, slots[idx].end)) {
          matchingSlots.push(idx);
        }
      }
      if (!matchingSlots.length) continue;

      const orderCountry = (order.countryCode || "").toUpperCase();
      const countryMatch = dayCountries.size === 0 || !orderCountry || dayCountries.has(orderCountry);

      // isNew sourced from BOTH order signals so the matcher is resilient to
      // either field being null at match time. isNewCustomerOrder is written
      // directly from Shopify's `customer.orders.edges[0]` during the detail
      // walk and survives upserts. customerOrderCountAtPurchase is populated
      // by the post-import computeOrderCounts() step, but during a re-sync
      // the UPDATE branch may transiently null it before the recompute runs,
      // and any concurrent matcher invocation would otherwise read null and
      // mark every order as repeat (the bug that emptied the New Meta
      // demographics tile on 2026-05-12).
      const isNewOrder =
        order.isNewCustomerOrder === true
        || order.customerOrderCountAtPurchase === 1;
      candidates.push({
        id: order.id, orderId: order.shopifyOrderId, total: orderTotal,
        isNew: isNewOrder, slots: matchingSlots,
        time: order.createdAt, customerId: order.shopifyCustomerId,
        countryMatch,
      });
    }

    if (!candidates.length) {
      unmatched += results;
      for (const slot of slots) {
        const slotPerConv = slot.cap > 0 ? Math.round((slot.slotValue / slot.cap) * 100) / 100 : 0;
        for (let ui = 0; ui < slot.cap; ui++) {
          attributions.push({
            shopDomain, shopifyOrderId: `unmatched_${ad.adId}_${day}_${slot.hour}_c${ui + 1}`,
            layer: 2, confidence: 0,
            metaCampaignId: ad.campaignId, metaCampaignName: ad.campaignName,
            metaAdSetId: ad.adSetId, metaAdSetName: ad.adSetName, metaAdId: ad.adId, metaAdName: ad.adName,
            matchMethod: "none", metaConversionValue: slotPerConv,
          });
        }
      }
      pushBucketStats(ad, slots, matchedBySlot, matchedValueBySlot);
      continue;
    }

    // Zero-revenue Meta conversions (replacement orders etc.) — match by
    // time window only against £0 candidates.
    if (!metaRevenue) {
      const zeroCandidates = candidates.filter(c => c.total === 0);
      zeroCandidates.sort((a, b) => (b.countryMatch ? 1 : 0) - (a.countryMatch ? 1 : 0));
      const picks = zeroCandidates.slice(0, results);
      if (picks.length > 0) {
        const pickedIds = new Set(picks.map(p => p.id));
        for (const pick of picks) {
          usedSet.add(pick.id);
          pickedOrderIds.push(pick.id);
          matched++;
          const slotIdx = resolveSlotIdx(pick, slots);
          if (slotIdx >= 0 && slotIdx < slots.length) matchedBySlot[slotIdx]++;
          let rivalCount = 0;
          for (const cand of zeroCandidates) {
            if (pickedIds.has(cand.id)) continue;
            if (pick.countryMatch && !cand.countryMatch) continue;
            rivalCount++;
          }
          const confidence = Math.max(1, Math.round(100 / (1 + rivalCount)));
          attributions.push({
            shopDomain, shopifyOrderId: pick.orderId || String(pick.id),
            layer: 2, confidence, rivalCount,
            metaCampaignId: ad.campaignId, metaCampaignName: ad.campaignName,
            metaAdSetId: ad.adSetId, metaAdSetName: ad.adSetName, metaAdId: ad.adId, metaAdName: ad.adName,
            isNewCustomer: pick.isNew, isNewToMeta: pick.isNew,
            matchMethod: "exhaustive", metaConversionValue: 0,
          });
        }
        if (picks.length < results) unmatched += results - picks.length;
      } else {
        unmatched += results;
        for (let ui = 0; ui < results; ui++) {
          attributions.push({
            shopDomain, shopifyOrderId: `unmatched_${ad.adId}_${day}_c${ui + 1}`,
            layer: 2, confidence: 0,
            metaCampaignId: ad.campaignId, metaCampaignName: ad.campaignName,
            metaAdSetId: ad.adSetId, metaAdSetName: ad.adSetName, metaAdId: ad.adId, metaAdName: ad.adName,
            matchMethod: "none", metaConversionValue: 0,
          });
        }
      }
      pushBucketStats(ad, slots, matchedBySlot, matchedValueBySlot);
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
      picks = fastResult.diffPct <= REVENUE_TOLERANCE_MEDIUM ? fastResult.picks : [];
      matchMethod = "fast_greedy";
    }

    if (!picks.length) {
      unmatched += results;
      for (const slot of slots) {
        const slotPerConv = slot.cap > 0 ? Math.round((slot.slotValue / slot.cap) * 100) / 100 : 0;
        for (let ui = 0; ui < slot.cap; ui++) {
          attributions.push({
            shopDomain, shopifyOrderId: `unmatched_${ad.adId}_${day}_${slot.hour}_c${ui + 1}`,
            layer: 2, confidence: 0,
            metaCampaignId: ad.campaignId, metaCampaignName: ad.campaignName,
            metaAdSetId: ad.adSetId, metaAdSetName: ad.adSetName, metaAdId: ad.adId, metaAdName: ad.adName,
            matchMethod: "none", metaConversionValue: slotPerConv,
          });
        }
      }
      pushBucketStats(ad, slots, matchedBySlot, matchedValueBySlot);
      continue;
    }

    for (const pick of picks) {
      const slotIdx = resolveSlotIdx(pick, slots);
      const assignedSlot = slots[slotIdx];
      const perPickValue = assignedSlot
        ? Math.round((assignedSlot.slotValue / Math.max(1, assignedSlot.cap)) * 100) / 100
        : Math.round((metaRevenue / picks.length) * 100) / 100;

      usedSet.add(pick.id);
      pickedOrderIds.push(pick.id);
      matched++;
      if (slotIdx >= 0 && slotIdx < slots.length) {
        matchedBySlot[slotIdx]++;
        matchedValueBySlot[slotIdx] += perPickValue;
      }

      const { confidence, rivalCount } = calculateConfidence(pick, candidates, picks);

      attributions.push({
        shopDomain, shopifyOrderId: pick.orderId || String(pick.id),
        layer: 2, confidence, rivalCount,
        metaCampaignId: ad.campaignId, metaCampaignName: ad.campaignName,
        metaAdSetId: ad.adSetId, metaAdSetName: ad.adSetName, metaAdId: ad.adId, metaAdName: ad.adName,
        isNewCustomer: pick.isNew, isNewToMeta: pick.isNew,
        matchMethod, metaConversionValue: perPickValue,
      });
    }

    // Partial-pick orphans. Before this, the matcher would silently drop any
    // slot the exhaustive/fast picker couldn't fill — leaving the snapshot
    // accounting believing the bucket had `picks.length` matches while Meta
    // reported `totalCap`. The next hourly/daily delta sweep then resurfaced
    // those gaps as phantom orphans on top of the matched orders. By emitting
    // the unfilled placeholders here, the snapshot we write below stays
    // self-consistent: snapshot.conversions = matched count, orphan rows
    // cover the rest, and the daily sweep's idempotent upsert is a no-op.
    for (let si = 0; si < slots.length; si++) {
      const slot = slots[si];
      const unfilled = slot.cap - matchedBySlot[si];
      if (unfilled <= 0) continue;
      unmatched += unfilled;
      const slotPerConv = slot.cap > 0 ? Math.round((slot.slotValue / slot.cap) * 100) / 100 : 0;
      for (let ui = 0; ui < unfilled; ui++) {
        attributions.push({
          shopDomain, shopifyOrderId: `unmatched_${ad.adId}_${day}_${slot.hour}_c${ui + 1}`,
          layer: 2, confidence: 0,
          metaCampaignId: ad.campaignId, metaCampaignName: ad.campaignName,
          metaAdSetId: ad.adSetId, metaAdSetName: ad.adSetName, metaAdId: ad.adId, metaAdName: ad.adName,
          matchMethod: "none", metaConversionValue: slotPerConv,
        });
      }
    }

    pushBucketStats(ad, slots, matchedBySlot, matchedValueBySlot);
  }

  return { day, attributions, pickedOrderIds, matched, unmatched, bucketStats };
}
