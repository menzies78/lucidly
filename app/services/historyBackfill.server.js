// Upgrade history backfill: extends a free-tier shop's 90-day window to
// full history after they subscribe (plan flip → historyBackfillStatus
// "pending" → the scheduler calls runHistoryBackfillIfPending inside the
// serialized sync chain).
//
// Design constraints (learned the hard way on 2026-08-03):
//  - Every phase is IDEMPOTENT (all writes are upserts on natural keys),
//    so a crash anywhere resumes safely by re-running the phase.
//  - The cursor (Shop.historyBackfillCursor) records the last COMPLETED
//    phase, not row-level progress — the phases themselves are internally
//    resumable (order sync via skeleton rows, Meta via upserts).
//  - Runs inside enqueueSerialized so it never fights the hourly sync or a
//    rollup rebuild for the SQLite write lock.
//  - Newest-first where possible: the recent year lands before the deep
//    tail (Meta tiers escalate 180 → 365 → 730 days).
//
// Phase order matters:
//  1. orders    — full 2-year Shopify import (skeleton + detail walk).
//                 Reuses the standard initial-backfill path by clearing
//                 lastOrderSync inside the serialized chain; syncOrders
//                 restores it on completion, so hourly incrementals
//                 continue normally afterwards.
//  2. customers — recompute customerOrderCountAtPurchase across ALL
//                 orders + reclassify pre-install acquisitions (some
//                 "new" customers become repeat once history arrives;
//                 skipping this poisons every cohort/LTV report).
//  3. meta      — insights + breakdowns in escalating windows. Each tier
//                 is one syncMetaAll call; the cursor records the last
//                 completed tier so a crash resumes at the failed tier.
//                 TODO(scale): move to IngestJob week-slicing (the
//                 onboarding orchestrator's machinery) before opening the
//                 free tier to VB-sized shops — a 730d pull in one call
//                 is hours of Meta API for 600k+ row accounts.
//  4. match     — FillGaps over the full lookback (additive; existing
//                 window attributions untouched).
//  5. rollups   — full rebuild of all four rollup families (SQL-side
//                 aggregation, so this is minutes not GBs).

import db from "../db.server.js";
import { getOfflineAdmin } from "./offlineToken.server.js";
import { syncOrdersSkeleton, syncOrders } from "./orderSync.server.js";
import { classifyPreInstallAcquisitions } from "./customerTruth.server.js";
import { syncMetaAll } from "./metaSync.server.js";
import { runFillGaps } from "./matcher.server.js";
import { rebuildCampaignRollups } from "./campaignRollups.server.js";
import { rebuildGeoRollups } from "./geoRollups.server.js";
import { rebuildProductRollups } from "./productRollups.server.js";
import { rebuildCustomerRollups } from "./customerRollups.server.js";
import { alertOps, resolveOps } from "./opsAlert.server.js";

const META_TIERS = [180, 365, 730];
const PHASES = ["orders", "customers", "meta-180", "meta-365", "meta-730", "match", "rollups"];

async function setStatus(shopDomain, status, cursor) {
  await db.shop.update({
    where: { shopDomain },
    data: {
      historyBackfillStatus: status,
      ...(cursor !== undefined ? { historyBackfillCursor: cursor } : {}),
    },
  });
}

/**
 * Entry point for the scheduler: runs the backfill for one shop if it's
 * pending (or resumes a crashed "running"). Never throws — failure is
 * recorded on the Shop row and alerted, and the next scheduler cycle
 * retries from the cursor.
 */
export async function runHistoryBackfillIfPending(shopDomain) {
  const shop = await db.shop.findUnique({
    where: { shopDomain },
    select: { plan: true, historyBackfillStatus: true, historyBackfillCursor: true, demoMode: true },
  });
  if (!shop || shop.demoMode) return { skipped: true };
  if (shop.plan !== "paid") return { skipped: true };
  if (!["pending", "running", "failed"].includes(shop.historyBackfillStatus)) return { skipped: true };

  const doneUpTo = shop.historyBackfillCursor
    ? PHASES.indexOf(shop.historyBackfillCursor) + 1
    : 0;
  const t0 = Date.now();
  console.log(`[HistoryBackfill] ${shopDomain}: starting at phase ${PHASES[doneUpTo] || "?"} (cursor=${shop.historyBackfillCursor || "none"})`);
  await setStatus(shopDomain, "running");

  try {
    for (let i = doneUpTo; i < PHASES.length; i++) {
      const phase = PHASES[i];
      const pt0 = Date.now();
      await runPhase(shopDomain, phase);
      await setStatus(shopDomain, "running", phase);
      console.log(`[HistoryBackfill] ${shopDomain}: phase ${phase} complete in ${Math.round((Date.now() - pt0) / 1000)}s`);
    }
    await setStatus(shopDomain, "complete");
    console.log(`[HistoryBackfill] ${shopDomain}: COMPLETE in ${Math.round((Date.now() - t0) / 1000)}s`);
    await resolveOps(`backfill:${shopDomain}`, {
      subject: `History backfill complete — ${shopDomain}`,
      title: `Full history unlocked for ${shopDomain}`,
      bodyHtml: `<p>The upgrade history backfill for <strong>${shopDomain}</strong> finished in ${Math.round((Date.now() - t0) / 60000)} min. All reports now cover full history.</p>`,
      bodyText: `History backfill complete for ${shopDomain}.`,
    });
    return { complete: true };
  } catch (err) {
    console.error(`[HistoryBackfill] ${shopDomain}: FAILED —`, err?.message || err);
    await setStatus(shopDomain, "failed").catch(() => {});
    await alertOps(`backfill:${shopDomain}`, {
      severity: "warn",
      subject: `History backfill failed — ${shopDomain}`,
      title: `Upgrade backfill failed for ${shopDomain}`,
      bodyHtml: `<p>The history backfill for <strong>${shopDomain}</strong> failed at phase <code>${(await db.shop.findUnique({ where: { shopDomain }, select: { historyBackfillCursor: true } }))?.historyBackfillCursor || "start"}</code>: <code>${String(err?.message || err).slice(0, 300).replace(/</g, "&lt;")}</code>. The scheduler retries from the last completed phase automatically.</p>`,
      bodyText: `History backfill failed for ${shopDomain}: ${err?.message}`,
    }).catch(() => {});
    return { failed: true };
  }
}

async function runPhase(shopDomain, phase) {
  if (phase === "orders") {
    const { admin } = await getOfflineAdmin(shopDomain);
    // Standard initial-backfill path: clearing lastOrderSync makes
    // resolveSyncWindow return the full 2-year window with skeleton-count
    // UX and per-page resume. syncOrders restores lastOrderSync at the end.
    await db.shop.update({ where: { shopDomain }, data: { lastOrderSync: null } });
    await syncOrdersSkeleton(admin, shopDomain);
    await syncOrders(admin, shopDomain); // includes computeOrderCounts full sweep
    return;
  }
  if (phase === "customers") {
    const { admin } = await getOfflineAdmin(shopDomain);
    await classifyPreInstallAcquisitions(shopDomain, admin.graphql.bind(admin));
    return;
  }
  if (phase.startsWith("meta-")) {
    const days = parseInt(phase.slice(5), 10);
    await syncMetaAll(shopDomain, days);
    return;
  }
  if (phase === "match") {
    await runFillGaps(shopDomain, 730);
    return;
  }
  if (phase === "rollups") {
    await rebuildCampaignRollups(shopDomain);
    await rebuildGeoRollups(shopDomain);
    await rebuildProductRollups(shopDomain);
    await rebuildCustomerRollups(shopDomain);
    return;
  }
  throw new Error(`unknown backfill phase: ${phase}`);
}

/** Any shop with a backfill waiting? (scheduler probe) */
export async function shopsWithPendingBackfill() {
  return db.shop.findMany({
    where: {
      plan: "paid",
      demoMode: false,
      historyBackfillStatus: { in: ["pending", "failed"] },
    },
    select: { shopDomain: true },
  });
}
