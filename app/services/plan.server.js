// Plan-tier helpers: the single place that knows what "free" means.
//
// Tiers: "free" (audit tier: 90-day rolling window, daily sync, gated tiles),
// "trial" (full product, 30-day trial), "paid" (subscribed). Existing shops
// were grandfathered to "paid" in the add_plan_tier migration; new installs
// only land on "free" once FREE_TIER_ENABLED=true ships, so all of this
// deploys dark.
//
// Enforcement is two-layered by design: loaders CLAMP query ranges (the
// paywall) and the weekly prune bounds storage (the cost control). Never
// rely on data absence as the gate — and never ship real gated numbers to
// the client for CSS-blurring; gated tiles render from placeholders.

import db from "../db.server.js";

export const FREE_WINDOW_DAYS = 90;

export function isFreeTierEnabled() {
  return process.env.FREE_TIER_ENABLED === "true";
}

// Plan assigned to a brand-new install. Until the audit tier launches
// publicly, new installs keep getting the full product.
export function planForNewInstall() {
  return isFreeTierEnabled() ? "free" : "paid";
}

export function isFreePlan(shop) {
  return shop?.plan === "free";
}

export function hasFullDepth(shop) {
  return shop?.plan === "paid" || shop?.plan === "trial";
}

// Earliest date a free-plan query may reach back to.
export function freeWindowStart(now = new Date()) {
  return new Date(now.getTime() - FREE_WINDOW_DAYS * 24 * 60 * 60 * 1000);
}

/**
 * Clamp a requested {gte, lte} date range to what the shop's plan allows.
 * Paid/trial pass through untouched. Free plans are floored at the rolling
 * window (and at dataWindowStart, so a shop mid-import never queries days
 * that were deliberately not fetched).
 *
 * Returns { gte, lte, clamped } — `clamped` lets loaders tell the UI the
 * range was reduced (rendered as the upgrade nudge, not silently).
 */
export function clampRangeForPlan(shop, { gte, lte }) {
  if (!isFreePlan(shop)) return { gte, lte, clamped: false };
  const floorCandidates = [freeWindowStart()];
  if (shop?.dataWindowStart) floorCandidates.push(new Date(shop.dataWindowStart));
  const floor = new Date(Math.max(...floorCandidates.map((d) => d.getTime())));
  if (!gte || gte < floor) {
    return { gte: floor, lte, clamped: true };
  }
  return { gte, lte, clamped: false };
}

// Loader convenience: fetch just the plan-relevant Shop fields.
export async function getShopPlan(shopDomain) {
  return db.shop.findUnique({
    where: { shopDomain },
    select: { plan: true, dataWindowStart: true, historyBackfillStatus: true },
  });
}
