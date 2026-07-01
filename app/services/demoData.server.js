// demoData.server.js
// ─────────────────────────────────────────────────────────────────────
// Synthesises 12 months of relative-dated sample data for the "Explore
// with sample data" demo store, then runs the real rollup builders so
// every aggregate (campaign rollups, customer LTV, geo, demographics)
// is guaranteed-consistent with the seeded base tables.
//
// Design principles:
//   • Borrow STRUCTURE, invent CONTENT. Catalogue breadth, price tiers,
//     funnel geometry, geo mix and segment split mirror a real DTC brand;
//     names / products / campaigns / customers are all fictional (Norvik).
//   • Relative dates. Everything is anchored to "now" at seed time, so the
//     demo never ages out — re-seeding regenerates fresh 12-month data.
//   • Seed only the seven base tables (+ Shop). All rollups come from the
//     production builders via rebuildAllRollups(force:true) — same code
//     path a real merchant's data flows through, so the demo can never
//     drift from real behaviour.
//   • Derived truth. We do NOT hand-set Customer.metaSegment; we generate
//     orders + attributions whose shape implies the segment, and let
//     rebuildCustomerSegments derive it (metaNew = first order attributed
//     with customerOrderCountAtPurchase===1; metaRetargeted = a later
//     order attributed; organic = none).
//   • Idempotent. seedDemoData wipes any existing demo rows first.

import db from "../db.server";
import { rebuildAllRollups } from "./incrementalSync.server.js";
import {
  DEMO_PRODUCTS, DEMO_HERO_PRODUCTS, DEMO_CAMPAIGNS, DEMO_GEO,
  DEMO_FIRST_NAMES_M, DEMO_FIRST_NAMES_F, DEMO_LAST_NAMES,
  DEMO_SEED, DEMO_CURRENCY, DEMO_TIMEZONE, DEMO_META_CURRENCY,
} from "./demoCatalog.server.js";

const DAY_MS = 24 * 60 * 60 * 1000;
const TARGET_CUSTOMERS = 7500;

// Acquisition history depth. 18 months (not 12) so a healthy slice of
// customers have ≥12 months of history — that's what makes the LTV cohort
// table mature (long-term cohort = acquired 365–730 days ago) and gives the
// cumulative-LTV curve its natural fast-then-taper shape instead of a stub.
const WINDOW_DAYS = 540;

// Catalogue prices are borrowed from a premium brand; scale every monetary
// value (unit price → order value → conversion value → LTV) to a more typical
// mid-market AOV so the demo reads as believable for most merchants.
const PRICE_SCALE = 1 / 3;
const unitPriceOf = (prod, aff = 1) => Math.max(5, Math.round(prod.price * PRICE_SCALE * aff));

// Blended Meta ROAS the demo is calibrated to. Total ad spend is derived so
// that Σ(attributed conversion value) / Σ(spend) lands here. A deliberately
// modest ~2× headline that the LTV view then justifies via strong repeat rate
// (Meta customer LTV:CAC ≈ 3×).
const TARGET_ROAS = 2.2;

// Meta age distribution — shared by attribution stamping (drives the "New from
// Meta" age panel, sourced from Attribution.metaAge) and the MetaBreakdown age
// rows. A single source so the two always agree.
const AGE_BUCKETS = [
  { v: "18-24", w: 12 }, { v: "25-34", w: 34 }, { v: "35-44", w: 27 },
  { v: "45-54", w: 15 }, { v: "55-64", w: 8 }, { v: "65+", w: 4 },
];

// Product popularity by category. Real catalogues sell on a long tail: cheap
// everyday pieces (tees, accessories) move in volume while premium shells sell
// rarely. Weighting selection this way concentrates the Product Journey flows
// (so they read as real funnels, not an even symmetric web) and guarantees the
// top entry products clear the "≥5 customers" bar the Entry-to-LTV view needs.
const CATEGORY_POP = {
  accessory: 10, tee: 9, hoodie: 6, sweatshirt: 6, sweatpants: 6,
  vest: 5, fleece: 5, shorts: 5, pants: 4,
  jacket: 2.5, bomber: 2.5, blazer: 2.5, parka: 2.2, shell: 1,
};
const productWeight = (prod) => CATEGORY_POP[prod.category] ?? 3;

// A couple of pieces have a known sizing/returns problem, so they refund well
// above the baseline. Concentrating refunds on a few high-volume items is what
// makes the "Top Refund Warning" surface a real signal instead of noise spread
// one-per-product. Both are high-popularity so they clear the ≥5-orders gate.
const REFUND_PRONE = new Set([
  "Tundra Pants. Black edition",
  "Basalt Tech Tee. Navy edition",
]);

// ── Seeded RNG (mulberry32) — deterministic so re-seeds reproduce ──────
function makeRng(seed) {
  let a = seed >>> 0;
  return function () {
    a |= 0; a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

let SEQ = 0;
function genId(prefix) {
  SEQ += 1;
  return `${prefix}${(23800000000000 + SEQ).toString()}`;
}

// ── Tables we own for a demo shop. Demo mode is only ever entered on an
// empty store, so "the demo rows" == "every row for this shopDomain".
// Wiping is therefore a clean per-shop delete with no quarantine needed.
async function wipeShopData(shopDomain) {
  const where = { shopDomain };
  // Order matters only where FK cascade applies (OrderLineItem → Order).
  await db.orderLineItem.deleteMany({ where });
  await db.attribution.deleteMany({ where });
  await db.order.deleteMany({ where });
  await db.metaInsight.deleteMany({ where });
  await db.metaBreakdown.deleteMany({ where });
  await db.metaEntity.deleteMany({ where });
  await db.metaChange.deleteMany({ where });
  await db.customer.deleteMany({ where });
  // Derived rollup / cache tables. The builders delete+rewrite these on
  // seed, but wipeDemoData has no rebuild afterwards, so clear them here so
  // no stale demo aggregates survive a "set up with real data" wipe.
  await db.dailyProductRollup.deleteMany({ where });
  await db.dailyAdRollup.deleteMany({ where });
  await db.dailyGeoRollup.deleteMany({ where });
  await db.dailyAdDemographicRollup.deleteMany({ where });
  await db.dailyCustomerRollup.deleteMany({ where });
  await db.shopAnalysisCache.deleteMany({ where });
}

const pick = (rng, arr) => arr[Math.floor(rng() * arr.length)];
const randInt = (rng, min, max) => min + Math.floor(rng() * (max - min + 1));

function weightedPick(rng, items, weightFn) {
  const total = items.reduce((s, it) => s + weightFn(it), 0);
  let r = rng() * total;
  for (const it of items) {
    r -= weightFn(it);
    if (r <= 0) return it;
  }
  return items[items.length - 1];
}

// Resolve a concrete place for a customer: pick a real city within the chosen
// country (weighted), then jitter the coordinates a little so households don't
// all stack on the exact city centroid. Returns a flat place object that keeps
// the country identity (country/code) the rest of the generator relies on.
function pickPlace(rng, country) {
  const c = country.cities && country.cities.length
    ? weightedPick(rng, country.cities, (x) => x.w)
    : country;
  // ±~0.11° lat, ±~0.15° lng ≈ a metro-area spread, not the whole country.
  const jLat = (rng() - 0.5) * 0.22;
  const jLng = (rng() - 0.5) * 0.30;
  return {
    country: country.country, code: country.code,
    city: c.city, region: c.region,
    lat: +(c.lat + jLat).toFixed(4), lng: +(c.lng + jLng).toFixed(4),
    // Affluence multiplier (default 1). A few wealthy markets over-index on
    // basket value so VIPs-per-Country shows real range, not a flat ranking.
    aff: country.aff || 1,
  };
}

// Calendar-date @ UTC midnight (matches MetaInsight.date convention).
function dayUTC(d) {
  return new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));
}
function dayKey(d) {
  return dayUTC(d).toISOString().slice(0, 10);
}
// Stamp a realistic shopping-hour time onto the SAME calendar day as `d`
// (08:00–21:59 UTC + random minutes/seconds), so orders don't all share the
// seed-run clock. Staying inside a single UTC day keeps day-bucketed
// conversions/rollups on the intended date. Clamps to nowTs so today's partial
// day never lands in the future.
function withDayTime(d, rng, nowTs) {
  const base = dayUTC(d).getTime();
  let t = base + (8 + randInt(rng, 0, 13)) * 3600000 + randInt(rng, 0, 59) * 60000 + randInt(rng, 0, 59) * 1000;
  if (t > nowTs) t = nowTs;
  return new Date(t);
}
function isoWeekMonday(d) {
  const x = dayUTC(d);
  const dow = (x.getUTCDay() + 6) % 7; // 0 = Monday
  return new Date(x.getTime() - dow * DAY_MS);
}

// ─────────────────────────────────────────────────────────────────────
// Build the Meta entity tree (campaigns → adsets → ads) with active
// delivery windows. Returns { entityRows, adPool }.
function buildEntities(rng, now, windowStart) {
  const entityRows = [];
  const adPool = [];
  const adEntityById = new Map();

  for (const camp of DEMO_CAMPAIGNS) {
    const campaignId = genId("c");
    const campaignName = camp.name;
    entityRows.push({
      entityType: "campaign", entityId: campaignId, entityName: campaignName,
      currentStatus: camp.status,
      createdTime: new Date(windowStart.getTime() + randInt(rng, 0, 120) * DAY_MS),
    });

    for (const aset of camp.adsets) {
      const adSetId = genId("a");
      const adSetName = aset.name;
      entityRows.push({
        entityType: "adset", entityId: adSetId, entityName: adSetName,
        currentStatus: camp.status, funnelStage: aset.funnelStage,
        createdTime: new Date(windowStart.getTime() + randInt(rng, 0, 150) * DAY_MS),
      });

      const adCount = randInt(rng, 2, 4);
      for (let k = 0; k < adCount; k++) {
        const adId = genId("d");
        // Pick a product to feature; hero-biased adsets push flagship pieces.
        const product = aset.heroBias && rng() < 0.7
          ? pick(rng, DEMO_HERO_PRODUCTS)
          : pick(rng, DEMO_PRODUCTS).name;
        const geoTag = (aset.geo[0] || "WW");
        const adName = `${product.replace(/[.\s]+/g, "_").replace(/_+/g, "_").slice(0, 40)}_${geoTag}`;
        // Active window: starts anywhere across the history, runs 45–240 days,
        // clamped to now. ACTIVE campaigns keep their newest ad running.
        const startOffset = randInt(rng, 0, WINDOW_DAYS - 30);
        const start = new Date(windowStart.getTime() + startOffset * DAY_MS);
        const runDays = randInt(rng, 45, 240);
        let end = new Date(Math.min(start.getTime() + runDays * DAY_MS, now.getTime()));
        if (camp.status === "ACTIVE" && rng() < 0.5) end = now;
        const dailyBudget = aset.funnelStage === "cold"
          ? randInt(rng, 40, 160)
          : randInt(rng, 25, 90); // GBP/day
        const adEntity = {
          entityType: "ad", entityId: adId, entityName: adName,
          currentStatus: start <= now && end >= now && camp.status === "ACTIVE" ? "ACTIVE" : "PAUSED",
          createdTime: start, effectiveStartAt: start, effectiveEndAt: end,
        };
        entityRows.push(adEntity);
        adEntityById.set(adId, adEntity);
        adPool.push({
          adId, adName, adSetId, adSetName, campaignId, campaignName,
          funnelStage: aset.funnelStage, geo: aset.geo, heroBias: !!aset.heroBias,
          campStatus: camp.status, start, end, dailyBudget, product,
        });
      }
    }
  }

  // ── Weekly-report signal: guarantee a couple of freshly-launched ads and a
  // couple just switched off, so the "New ads launched" / "Ads switched off"
  // panels are never empty. Mutate both the adPool entry (drives insight
  // generation) and its entity row (drives the createdTime / status the
  // weekly route reads) in lockstep.
  const activeAds = adPool.filter((a) => a.campStatus === "ACTIVE");
  // Fresh launches: started 1–6 days ago, still running.
  for (let i = 0; i < 3 && activeAds.length; i++) {
    const a = activeAds[Math.floor(rng() * activeAds.length)];
    const start = new Date(now.getTime() - randInt(rng, 1, 6) * DAY_MS);
    a.start = start; a.end = now;
    const e = adEntityById.get(a.adId);
    e.createdTime = start; e.effectiveStartAt = start; e.effectiveEndAt = now;
    e.currentStatus = "ACTIVE";
  }
  // Just switched off: last delivered 8–12 days ago (spend lands in last
  // week's bucket, nothing this week).
  for (let i = 0; i < 2 && adPool.length; i++) {
    const a = adPool[Math.floor(rng() * adPool.length)];
    const end = new Date(now.getTime() - randInt(rng, 8, 12) * DAY_MS);
    if (a.start.getTime() >= end.getTime()) a.start = new Date(end.getTime() - randInt(rng, 60, 180) * DAY_MS);
    a.end = end;
    const e = adEntityById.get(a.adId);
    e.effectiveEndAt = end; e.currentStatus = "PAUSED";
  }

  // ── Always-on evergreen prospecting ad spanning the FULL window ──
  // Real accounts this size are never fully dark: there's Meta delivery every
  // single day. This guarantees at least one ACTIVE ad on every calendar day,
  // so (a) the account never has a day with no delivery and (b) the daily Meta
  // coverage backfill in seedDemoData always has an active ad to attribute a
  // gap-filling conversion to (an inactive ad wouldn't emit a MetaInsight row).
  const evProduct = pick(rng, DEMO_HERO_PRODUCTS);
  const evAdName = `${evProduct.replace(/[.\s]+/g, "_").replace(/_+/g, "_").slice(0, 40)}_WW`;
  const evCampId = genId("c");
  const evAdSetId = genId("a");
  const evAdId = genId("d");
  entityRows.push({
    entityType: "campaign", entityId: evCampId, entityName: "Evergreen Prospecting",
    currentStatus: "ACTIVE", createdTime: windowStart,
  });
  entityRows.push({
    entityType: "adset", entityId: evAdSetId, entityName: "Evergreen — Broad",
    currentStatus: "ACTIVE", funnelStage: "cold", createdTime: windowStart,
  });
  const evEntity = {
    entityType: "ad", entityId: evAdId, entityName: evAdName,
    currentStatus: "ACTIVE", createdTime: windowStart,
    effectiveStartAt: windowStart, effectiveEndAt: now,
  };
  entityRows.push(evEntity);
  adEntityById.set(evAdId, evEntity);
  adPool.push({
    adId: evAdId, adName: evAdName, adSetId: evAdSetId, adSetName: "Evergreen — Broad",
    campaignId: evCampId, campaignName: "Evergreen Prospecting",
    funnelStage: "cold", geo: DEMO_GEO.map((g) => g.code), heroBias: true,
    campStatus: "ACTIVE", start: windowStart, end: now,
    dailyBudget: randInt(rng, 60, 120), product: evProduct,
  });

  return { entityRows, adPool };
}

// Pick an ad active on `date` whose geo covers `countryCode` and whose
// funnel stage matches the intent ("acquire" → cold/warm, "retarget" →
// warm/hot). Falls back progressively so we always return something.
function pickAd(rng, adPool, date, countryCode, intent) {
  const t = date.getTime();
  const stageOk = intent === "retarget"
    ? (s) => s === "hot" || s === "warm"
    : (s) => s === "cold" || s === "warm";
  const active = adPool.filter((a) => a.start.getTime() <= t && a.end.getTime() >= t);
  let pool = active.filter((a) => stageOk(a.funnelStage) && a.geo.includes(countryCode));
  if (!pool.length) pool = active.filter((a) => stageOk(a.funnelStage));
  if (!pool.length) pool = active;
  if (!pool.length) pool = adPool; // last resort (date outside all windows)
  return pick(rng, pool);
}

// ─────────────────────────────────────────────────────────────────────
// Synthesise the Meta ad-account activity feed (Change Log). A spread of
// realistic budget / status / creative / targeting events over the last few
// weeks across the campaign tree, so the Change Log and the Weekly Report's
// "what changed" context render with believable history.
const CHANGE_ACTORS = [
  { id: "act_1001", name: "Mia Carter" },
  { id: "act_1002", name: "Tom Reeves" },
  { id: "act_1003", name: "Norvik Automated Rules" },
];
function buildMetaChanges(rng, now, entityRows) {
  const campaigns = entityRows.filter((e) => e.entityType === "campaign");
  const adsets = entityRows.filter((e) => e.entityType === "adset");
  const ads = entityRows.filter((e) => e.entityType === "ad");
  const rows = [];
  const push = (daysAgo, category, rawEventType, obj, objectType, summary, oldValue, newValue) => {
    const actor = pick(rng, CHANGE_ACTORS);
    const eventTime = new Date(now.getTime() - daysAgo * DAY_MS - randInt(rng, 0, 23) * 3600000);
    rows.push({
      eventTime, category, rawEventType,
      objectType, objectId: obj.entityId, objectName: obj.entityName || obj.entityId,
      actorId: actor.id, actorName: actor.name,
      oldValue: oldValue ?? null, newValue: newValue ?? null,
      summary, rawPayload: "{}",
    });
  };

  // ~6 budget tweaks over the last 3 weeks.
  for (let i = 0; i < 6; i++) {
    const obj = pick(rng, adsets);
    const oldB = randInt(rng, 30, 120);
    const up = rng() < 0.6;
    const newB = up ? oldB + randInt(rng, 10, 60) : Math.max(10, oldB - randInt(rng, 10, 40));
    push(randInt(rng, 0, 20), "budget", "update_ad_set_budget", obj, "adset",
      `Daily budget ${up ? "raised" : "lowered"} from £${oldB} to £${newB}`, `£${oldB}`, `£${newB}`);
  }
  // Launches (tie to the freshly-launched ads' window).
  for (let i = 0; i < 3 && ads.length; i++) {
    const obj = pick(rng, ads);
    push(randInt(rng, 0, 6), "launched", "create_ad", obj, "ad", `New ad launched: ${obj.entityName}`);
  }
  // Pauses / resumes.
  for (let i = 0; i < 3 && ads.length; i++) {
    const obj = pick(rng, ads);
    push(randInt(rng, 1, 14), "paused", "update_ad_run_status", obj, "ad", `Ad paused: ${obj.entityName}`, "ACTIVE", "PAUSED");
  }
  for (let i = 0; i < 2 && adsets.length; i++) {
    const obj = pick(rng, adsets);
    push(randInt(rng, 1, 16), "resumed", "update_ad_set_run_status", obj, "adset", `Ad set resumed: ${obj.entityName}`, "PAUSED", "ACTIVE");
  }
  // Creative swaps and targeting tweaks.
  for (let i = 0; i < 3 && ads.length; i++) {
    const obj = pick(rng, ads);
    push(randInt(rng, 0, 18), "creative", "update_ad_creative", obj, "ad", `Creative updated on ${obj.entityName}`);
  }
  for (let i = 0; i < 3 && adsets.length; i++) {
    const obj = pick(rng, adsets);
    push(randInt(rng, 2, 19), "targeting", "update_ad_set_targeting", obj, "adset", `Audience targeting adjusted on ${obj.entityName}`);
  }
  // An optimisation-goal change and a campaign budget bump.
  if (adsets.length) {
    const obj = pick(rng, adsets);
    push(randInt(rng, 3, 17), "optimisation", "update_optimization_goal", obj, "adset",
      `Optimisation goal changed to Purchases on ${obj.entityName}`, "LINK_CLICKS", "OFFSITE_CONVERSIONS");
  }
  if (campaigns.length) {
    const obj = pick(rng, campaigns);
    push(randInt(rng, 0, 12), "budget", "update_campaign_budget", obj, "campaign",
      `Campaign budget increased on ${obj.entityName}`, "£300", "£450");
  }

  // De-dupe on the unique key (eventTime, rawEventType, objectId).
  const seen = new Set();
  return rows.filter((r) => {
    const k = `${r.eventTime.toISOString()}|${r.rawEventType}|${r.objectId}`;
    if (seen.has(k)) return false;
    seen.add(k); return true;
  });
}

// ─────────────────────────────────────────────────────────────────────
export async function seedDemoData(shopDomain) {
  const t0 = Date.now();
  console.log(`[demoData] seeding demo store for ${shopDomain}…`);
  SEQ = 0;
  const rng = makeRng(DEMO_SEED);

  const now = new Date();
  const windowStart = new Date(now.getTime() - WINDOW_DAYS * DAY_MS);
  // Repeat/retarget mix, calibrated so the Meta Customer Breakdown donut reads a
  // healthy, New-led ~39% New / 31% Retargeted / 30% Repeat over the default
  // last-30 window while keeping repeat rate (~30%) + LTV realistic. Note: an
  // exact 45/35/20 last-30 split is not reachable without breaking realism — in
  // any trailing window the Repeat slice tracks the ~30% repeat rate (Retargeted
  // and Repeat orders accumulate into recent windows, New is bounded by recent
  // acquisitions), so pushing Repeat to 20% would require either an unrealistic
  // sub-20% repeat rate or making Meta-new customers repeat LESS than organic
  // (which would kill the LTV:CAC story). Over ALL time the split is New-heavier
  // (~61/21/18). (Acquisition timing is a gentle linear growth — see ageDays
  // below — NOT a recency skew, so daily order volume ramps ~22% across the
  // window with no terminal spike.)
  const NEW_CONTPROB = 0.30;
  const RT_LATE = 0.50;
  // Acquisition-timing slope: customers/day acquired at the recent end vs the old
  // end. Kept gentle — repeat orders accumulate on top of new acquisitions, so a
  // modest acquisition slope already yields ~25% growth in *total* daily order
  // volume across the window (repeats amplify the trend).
  const ACQ_SLOPE = 0.04;

  await wipeShopData(shopDomain);

  const { entityRows, adPool } = buildEntities(rng, now, windowStart);
  const metaChanges = buildMetaChanges(rng, now, entityRows);

  // Buckets that let MetaInsight conversions track real attributed orders.
  // key = `${adId}|${dayKey}` → { count, value }
  const convByAdDay = new Map();
  function recordConversion(adId, date, value) {
    const key = `${adId}|${dayKey(date)}`;
    const cur = convByAdDay.get(key) || { count: 0, value: 0 };
    cur.count += 1; cur.value += value;
    convByAdDay.set(key, cur);
  }

  const customers = [];
  const orders = [];
  const lineItems = [];
  const attributions = [];

  // Segment intent for a customer (drives attribution shape; final
  // metaSegment is derived by the rollup builder). A showcase mix where Meta
  // drives a healthy share of acquisition: ~27% new-from-Meta, ~8% retargeted.
  // Retargeted requires a SECOND attributed order, so only assign it to
  // customers acquired long enough ago to have plausibly returned — recent
  // acquisitions fall back to metaNew rather than being forced an instant
  // (unbelievable) repeat.
  // Target DERIVED segment shares (after the rollup builder classifies):
  // ~14% metaNew, ~10% metaRetargeted, ~76% organic — calibrated to real
  // Vollebak (14.2/8.7/77.1) and HM (13/11.6/75.4). Retargeted requires a
  // SECOND attributed order, so it's only assigned to customers acquired long
  // enough ago to have plausibly returned; recent acquisitions that can't yet
  // have completed a repeat fall back to organic (they simply haven't been
  // retargeted into a second purchase yet — realistic), NOT metaNew, so the
  // metaNew share stays pinned.
  function drawSegmentIntent(ageDays) {
    const r = rng();
    if (r < 0.20) return "metaNew";
    if (r < 0.28) return ageDays >= 18 ? "metaRetargeted" : "organic";
    return "organic";
  }

  for (let i = 0; i < TARGET_CUSTOMERS; i++) {
    const country = weightedPick(rng, DEMO_GEO, (g) => g.weight);
    const geo = pickPlace(rng, country);
    const isMale = rng() < 0.75; // skews male like the real brand
    const firstName = isMale ? pick(rng, DEMO_FIRST_NAMES_M) : pick(rng, DEMO_FIRST_NAMES_F);
    const lastName = pick(rng, DEMO_LAST_NAMES);
    const customerId = genId("u");
    // Acquisition date: gentle linear GROWTH across the window. Sample a
    // position p∈[0,1] (0 = oldest day, 1 = today) from a linear density that
    // is 25% higher at the recent end than the old end — a brand acquiring ~25%
    // more customers/day by year-end than at the start. Inverse-CDF of
    // f(p) ∝ (1 + 0.25p): p = (√(1 + 0.5625u) − 1) / 0.25. The first order lands
    // ON the acquisition date; repeats extend FORWARD and stop at "now", so
    // daily order volume ramps smoothly ~25% with NO terminal spike.
    const u = rng();
    const p = (Math.sqrt(1 + 2 * ACQ_SLOPE * (1 + ACQ_SLOPE / 2) * u) - 1) / ACQ_SLOPE;
    const ageDays = Math.floor((1 - p) * WINDOW_DAYS);
    const acqDate = new Date(now.getTime() - ageDays * DAY_MS);
    const intent = drawSegmentIntent(ageDays);

    // Repeat behaviour: decaying continuation with widening gaps. After each
    // order the customer may place another, but each is less likely and
    // further out — so cumulative LTV climbs fastest in the first weeks then
    // tapers, and any order that would fall after "now" simply hasn't happened
    // yet. Meta-acquired customers continue more readily (stronger repeat rate
    // → the LTV:CAC story that justifies the modest blended ROAS).
    let orderDate = new Date(acqDate);
    // Continuation probability after each order, modelled as a funnel: a
    // MODERATE chance of a second order (sets the headline repeat rate ≈ real
    // 30%), but the customers who DO return become loyalists — their
    // continuation probability is boosted once they cross into repeat
    // territory, so a believable minority keep buying across the whole year on
    // a multi-month cadence. That sustained tail is what makes the cumulative
    // LTV curve climb steadily for 12 months instead of dying after month one.
    let contProb = intent === "organic" ? 0.24 : intent === "metaRetargeted" ? 0.5 : NEW_CONTPROB;
    // First repeat gap: median ≈ 18 days (real Vollebak ≈ 19), tightened so the
    // second order surfaces inside short 30/90-day windows and the New-Customer
    // Journey reads as a quick first re-purchase. AFTER that first repeat the
    // cadence switches to a sustained multi-month band (set below) so the 3rd+
    // orders spread ACROSS the year rather than clustering in month one — that
    // spread is what keeps the cumulative-LTV curve climbing for 12 months.
    let gapMin = 8;
    let gapMax = 28;

    let totalSpent = 0;
    let firstOrderValue = 0;
    let secondOrderDate = null;
    let lastOrderDate = orderDate;
    let firstOrderDate = null;

    let n = 0;
    while (true) {
      if (orderDate.getTime() > now.getTime()) break;
      // Give this order a natural time-of-day on its calendar day (orders would
      // otherwise all inherit the seed-run clock). Order numbers are assigned
      // later in a single date-sorted pass, so leave orderNumber unset here.
      orderDate = withDayTime(orderDate, rng, now.getTime());
      const orderId = genId("o");
      const isFirst = n === 0;
      if (isFirst) firstOrderDate = orderDate;

      // Line items: 1–3 products. Meta-driven first orders are hero-biased;
      // everything else is drawn by popularity (a long-tail concentration),
      // and add-on items lean toward the first item's category so baskets look
      // like real outfits rather than a uniform random mix.
      const itemCount = randInt(rng, 1, 3);
      const chosen = [];
      for (let m = 0; m < itemCount; m++) {
        const wantHero = (intent !== "organic" && isFirst && rng() < 0.4);
        let prod;
        if (wantHero) {
          prod = DEMO_PRODUCTS.find((p) => p.name === pick(rng, DEMO_HERO_PRODUCTS)) || pick(rng, DEMO_PRODUCTS);
        } else if (m > 0 && rng() < 0.35) {
          // Add-on affinity: another piece from the first item's category.
          const cat = chosen[0].prod.category;
          const sameCat = DEMO_PRODUCTS.filter((p) => p.category === cat);
          prod = sameCat.length ? weightedPick(rng, sameCat, productWeight) : weightedPick(rng, DEMO_PRODUCTS, productWeight);
        } else {
          prod = weightedPick(rng, DEMO_PRODUCTS, productWeight);
        }
        const qty = rng() < 0.85 ? 1 : 2;
        chosen.push({ prod, qty, unit: unitPriceOf(prod, geo.aff) });
      }
      const subtotal = chosen.reduce((s, c) => s + c.unit * c.qty, 0);
      // Occasional discount; occasional partial refund. Refund-prone pieces
      // return well above the ~4% baseline so the Top Refund Warning has a
      // concentrated, believable signal to surface.
      const hasDiscount = rng() < 0.18;
      const discount = hasDiscount ? Math.round(subtotal * (0.1 + rng() * 0.15)) : 0;
      const total = subtotal - discount;
      // Refunds are modelled at line-item level (a returned piece) and written
      // into refundLineItems JSON, because the product-rollup builder only
      // counts a product refund when that title's refunded amount ≥ 50% of its
      // original price. Refund-prone pieces return well above baseline so the
      // Top Refund Warning surfaces a concentrated, believable signal.
      const refundProneItems = chosen.filter((c) => REFUND_PRONE.has(c.prod.name));
      const doRefund = rng() < (refundProneItems.length ? 0.26 : 0.04);
      let refunded = 0;
      let refundLineItemsJson = "";
      if (doRefund) {
        const target = refundProneItems.length
          ? refundProneItems[0]
          : chosen[Math.floor(rng() * chosen.length)];
        const lineTotal = target.unit * target.qty;
        refunded = Math.min(total, lineTotal);
        // Mark the returned line so OrderLineItem.refundedAmount carries it —
        // that's the field the product-rollup builder reads to count refunds.
        target.refundedAmount = refunded;
        refundLineItemsJson = JSON.stringify([
          { title: target.prod.name, refundedAmount: refunded, originalPrice: lineTotal },
        ]);
      }

      totalSpent += total - refunded;
      if (isFirst) firstOrderValue = total;
      if (n === 1) secondOrderDate = orderDate;
      lastOrderDate = orderDate;

      const lineItemTitles = chosen.map((c) => c.prod.name).join(", ");
      orders.push({
        shopifyOrderId: orderId, shopifyCustomerId: customerId,
        orderNumber: "", createdAt: new Date(orderDate),
        totalPrice: total, subtotalPrice: subtotal, currency: DEMO_CURRENCY,
        financialStatus: refunded > 0 ? "partially_refunded" : "paid",
        channelName: "Online Store", isOnlineStore: true,
        frozenTotalPrice: total, frozenSubtotalPrice: subtotal,
        isNewCustomerOrder: isFirst,
        country: geo.country, countryCode: geo.code, city: geo.city, regionCode: geo.region,
        customerFirstName: firstName, customerLastName: lastName,
        customerLastInitial: lastName.slice(0, 1),
        customerOrderCountAtPurchase: n + 1,
        lineItems: lineItemTitles,
        discountCodes: hasDiscount ? "WELCOME10" : "",
        refundStatus: refunded > 0 ? "partial" : "none",
        totalRefunded: refunded,
        refundLineItems: refundLineItemsJson,
        importedAt: now,
      });
      for (const c of chosen) {
        lineItems.push({
          shopifyOrderId: orderId, shopifyLineItemId: genId("li"),
          title: c.prod.name, sku: "", quantity: c.qty,
          unitPrice: c.unit,
          totalPrice: c.unit * c.qty,
          totalDiscount: hasDiscount ? Math.round((discount * (c.prod.price * c.qty)) / subtotal) : 0,
          refundedAmount: c.refundedAmount || 0,
          refundedQuantity: c.refundedAmount ? c.qty : 0,
        });
      }

      // ── Attribution shape drives derived segment ──
      // metaNew: attribute the FIRST order (count===1 already set).
      // metaRetargeted: attribute a LATER order (n>=1), leave first organic.
      // organic: never attributed.
      let attributeThis = false;
      let intentForAd = "acquire";
      if (intent === "metaNew" && isFirst) { attributeThis = true; intentForAd = "acquire"; }
      // Retargeted: leave the FIRST order organic, then attribute most repeat
      // orders to a retargeting ad. Spreading these across the customer's whole
      // lifetime (not just the 2nd order) keeps retargeted attributions landing
      // in recent windows, so the 90-day Summary donut shows a healthy slice —
      // not just the By-Day chart.
      else if (intent === "metaRetargeted" && !isFirst) { attributeThis = (n === 1 ? true : rng() < RT_LATE); intentForAd = "retarget"; }
      // A minority of metaNew customers also get a later retargeted order.
      else if (intent === "metaNew" && !isFirst && rng() < 0.14) { attributeThis = true; intentForAd = "retarget"; }

      if (attributeThis) {
        const ad = pickAd(rng, adPool, orderDate, geo.code, intentForAd);
        // ~26% land via UTM/cookie (Layer 1, certain). The statistical matcher
        // resolves the rest cleanly — most with no rival, so confidence stays
        // high (blended avg ≈ 90%).
        const isLayer1 = rng() < 0.26;
        // The matcher resolves the vast majority of orders cleanly with NO
        // rival candidate in the same hour/value slot (distinct order values
        // spread across the day), so confidence stays high. Only a small tail
        // sees one rival (50%) and a rare case two (33%) → blended avg ≈ 96–97%.
        // Kept rare so low-volume recent days don't get dragged to a lone 50%
        // bar on the 30-day dashboard chart.
        const rivals = isLayer1 ? 0 : (rng() < 0.98 ? 0 : (rng() < 0.995 ? 1 : 2));
        const confidence = isLayer1 ? 100 : Math.round(100 / (1 + rivals));
        // Stamp Meta demographics so the "New from Meta" age panel (sourced
        // from Attribution.metaAge) and gender CPA tiles populate.
        const metaAge = weightedPick(rng, AGE_BUCKETS, (x) => x.w).v;
        const metaGender = isMale ? "male" : "female";
        attributions.push({
          shopifyOrderId: orderId, layer: isLayer1 ? 1 : 2, confidence,
          metaCampaignId: ad.campaignId, metaCampaignName: ad.campaignName,
          metaAdSetId: ad.adSetId, metaAdSetName: ad.adSetName,
          metaAdId: ad.adId, metaAdName: ad.adName,
          metaAge, metaGender,
          isNewCustomer: isFirst, isNewToMeta: isFirst && intent === "metaNew",
          matchedAt: new Date(orderDate.getTime() + randInt(rng, 1, 30) * 60000),
          matchMethod: isLayer1 ? "utm" : "statistical",
          metaConversionValue: total,
          rivalCount: rivals,
        });
        // Mirror onto the order (matcher writes both in production).
        const o = orders[orders.length - 1];
        o.attributionLayer = isLayer1 ? 1 : 2;
        o.attributionConfidence = confidence;
        o.metaCampaignId = ad.campaignId; o.metaCampaignName = ad.campaignName;
        o.metaAdSetId = ad.adSetId; o.metaAdSetName = ad.adSetName;
        o.metaAdId = ad.adId; o.metaAdName = ad.adName;
        if (isLayer1) { o.utmConfirmedMeta = true; o.utmSource = "facebook"; o.utmMedium = "paid"; o.utmCampaign = ad.campaignName; }
        recordConversion(ad.adId, orderDate, total);
      }

      // Decide whether this customer returns. metaRetargeted needs a second
      // order for the segment to materialise, so force one when there's still
      // room before "now"; otherwise the decay/widening-gap model governs.
      n++;
      const forceSecond = intent === "metaRetargeted" && n < 2;
      if (!forceSecond && (n >= 9 || rng() > contProb)) break;
      const gap = randInt(rng, gapMin, gapMax);
      orderDate = new Date(orderDate.getTime() + gap * DAY_MS);
      if (orderDate.getTime() > now.getTime()) break;
      if (n === 1) {
        // Crossed into repeat territory → loyalist. Strongly boost continuation
        // and switch to a multi-month cadence so a repeater keeps coming back
        // (typically 4–7 lifetime orders) spread across months 2–12. Because
        // most customers are one-and-done, it's this DEPTH among the minority
        // who return — not a higher repeat rate — that lifts the cumulative-LTV
        // curve steadily for the full year.
        contProb = Math.min(0.82, contProb * 2.3);
        gapMin = 30; gapMax = 78;
      } else {
        contProb *= 0.98;        // loyalists stay loyal — near-flat decay keeps
        gapMin += 8; gapMax += 16; // …and gently further out
      }
    }
    const orderCount = n;

    // Customer row. metaSegment / acquisition* / LTV totals are RECOMPUTED
    // by the builders from orders+attributions; we seed identity + geo +
    // inferred gender (which the builder reads, not derives) and sensible
    // initial totals.
    customers.push({
      shopifyCustomerId: customerId,
      customerEmail: null, emailHash: null,
      firstOrderDate: new Date(firstOrderDate || acqDate),
      lastOrderDate, secondOrderDate, firstOrderValue,
      totalOrders: orderCount, totalSpent: Math.round(totalSpent),
      isNewCustomer: orderCount === 1,
      country: geo.country, city: geo.city, lat: geo.lat, lng: geo.lng,
      inferredGender: isMale ? "male" : "female",
      inferredGenderConfidence: 0.97, inferredGenderSource: "name",
    });
  }

  // ── Guarantee a few Meta orders on EVERY day ──
  // Stores this size take Meta orders every single day; a random gap day would
  // render as a grey "no conversions" bar and read as fake. Backfill any day in
  // the window that ended up with zero attributed Meta orders with a single
  // realistic metaNew acquisition on that day. Volume above already fills the
  // vast majority of days with several orders — this only patches the rare gap,
  // so it adds a negligible customer count and leaves the segment mix, AOV and
  // rates essentially unchanged. Each backfill order is a clean single-rival-
  // free match (confidence 100), matched to an ad active that day (the evergreen
  // ad guarantees one exists) so its conversion is emitted by the MetaInsight
  // pass below.
  const coveredDays = new Set();
  for (const key of convByAdDay.keys()) coveredDays.add(key.split("|")[1]);
  for (let off = 0; off < WINDOW_DAYS; off++) {
    const dayStart = new Date(now.getTime() - off * DAY_MS);
    const dk = dayKey(dayStart);
    if (coveredDays.has(dk)) continue;
    coveredDays.add(dk);
    // Anchor the order at UTC-midday of dk. dayKey()/metaConvByDay bucket by UTC,
    // so landing between 08:00–19:59 UTC guarantees dayKey(orderDate) === dk and
    // the emitted MetaInsight conversion lands on dk (never drifting into the
    // previous UTC day) — which is what makes this day non-grey on the chart.
    const orderDate = new Date(dayUTC(dayStart).getTime() + (8 + randInt(rng, 0, 11)) * 3600000 + randInt(rng, 0, 59) * 60000);
    const country = weightedPick(rng, DEMO_GEO, (g) => g.weight);
    const geo = pickPlace(rng, country);
    const isMale = rng() < 0.75;
    const firstName = isMale ? pick(rng, DEMO_FIRST_NAMES_M) : pick(rng, DEMO_FIRST_NAMES_F);
    const lastName = pick(rng, DEMO_LAST_NAMES);
    const customerId = genId("u");
    const orderId = genId("o");
    const prod = weightedPick(rng, DEMO_PRODUCTS, productWeight);
    const qty = rng() < 0.85 ? 1 : 2;
    const unit = unitPriceOf(prod, geo.aff);
    const total = unit * qty;
    const ad = pickAd(rng, adPool, orderDate, geo.code, "acquire");
    const isLayer1 = rng() < 0.26;
    const metaAge = weightedPick(rng, AGE_BUCKETS, (x) => x.w).v;
    orders.push({
      shopifyOrderId: orderId, shopifyCustomerId: customerId,
      orderNumber: "", createdAt: new Date(orderDate),
      totalPrice: total, subtotalPrice: total, currency: DEMO_CURRENCY,
      financialStatus: "paid",
      channelName: "Online Store", isOnlineStore: true,
      frozenTotalPrice: total, frozenSubtotalPrice: total,
      isNewCustomerOrder: true,
      country: geo.country, countryCode: geo.code, city: geo.city, regionCode: geo.region,
      customerFirstName: firstName, customerLastName: lastName,
      customerLastInitial: lastName.slice(0, 1),
      customerOrderCountAtPurchase: 1,
      lineItems: prod.name,
      discountCodes: "", refundStatus: "none", totalRefunded: 0, refundLineItems: "",
      importedAt: now,
      attributionLayer: isLayer1 ? 1 : 2,
      attributionConfidence: 100,
      metaCampaignId: ad.campaignId, metaCampaignName: ad.campaignName,
      metaAdSetId: ad.adSetId, metaAdSetName: ad.adSetName,
      metaAdId: ad.adId, metaAdName: ad.adName,
      ...(isLayer1 ? { utmConfirmedMeta: true, utmSource: "facebook", utmMedium: "paid", utmCampaign: ad.campaignName } : {}),
    });
    lineItems.push({
      shopifyOrderId: orderId, shopifyLineItemId: genId("li"),
      title: prod.name, sku: "", quantity: qty,
      unitPrice: unit, totalPrice: total, totalDiscount: 0,
      refundedAmount: 0, refundedQuantity: 0,
    });
    attributions.push({
      shopifyOrderId: orderId, layer: isLayer1 ? 1 : 2, confidence: 100,
      metaCampaignId: ad.campaignId, metaCampaignName: ad.campaignName,
      metaAdSetId: ad.adSetId, metaAdSetName: ad.adSetName,
      metaAdId: ad.adId, metaAdName: ad.adName,
      metaAge, metaGender: isMale ? "male" : "female",
      isNewCustomer: true, isNewToMeta: true,
      matchedAt: new Date(orderDate.getTime() + randInt(rng, 1, 30) * 60000),
      matchMethod: isLayer1 ? "utm" : "statistical",
      metaConversionValue: total,
      rivalCount: 0,
    });
    customers.push({
      shopifyCustomerId: customerId,
      customerEmail: null, emailHash: null,
      firstOrderDate: new Date(orderDate),
      lastOrderDate: orderDate, secondOrderDate: null, firstOrderValue: total,
      totalOrders: 1, totalSpent: total,
      isNewCustomer: true,
      country: geo.country, city: geo.city, lat: geo.lat, lng: geo.lng,
      inferredGender: isMale ? "male" : "female",
      inferredGenderConfidence: 0.97, inferredGenderSource: "name",
    });
    recordConversion(ad.adId, orderDate, total);
  }

  // ── MetaInsight: daily rows per ad over its active window ──
  // Spend/impressions/clicks are independent; conversions overlay the real
  // attributed orders PLUS a Meta-reported surplus (the matched-vs-
  // unverified gap the app surfaces honestly).
  // Two-pass so blended ROAS is pinned to TARGET_ROAS. Pass 1 fixes each
  // delivering ad-day's conversions (real attributed orders + a rare Meta-
  // reported surplus) and a relative spend weight. Pass 2 scales total spend
  // to Σ(conversionValue)/TARGET_ROAS and distributes it by weight, then
  // derives impressions/clicks from the resulting spend.
  const drafts = [];
  let totalConvValue = 0;
  let weightSum = 0;
  // Reference order value for Meta-reported conversions we can't tie to a
  // Shopify order (no matched order to copy a value from).
  const aovRef = orders.length
    ? orders.reduce((s, o) => s + o.totalPrice, 0) / orders.length
    : 200;
  // Per-day total matched conversions across ALL ads. The Meta-reported
  // "surplus" below is gated on this so a +1 unmatched conversion only lands on
  // a busy day (a shallow dip like 5→6 = 83%), never tanking a solo-conversion
  // day to 50%/0%. At most one surplus per day (surplusDays guard).
  const matchedTotalByDay = new Map();
  for (const [key, v] of convByAdDay) {
    const dk = key.split("|")[1];
    matchedTotalByDay.set(dk, (matchedTotalByDay.get(dk) || 0) + v.count);
  }
  const surplusDays = new Set();
  const surplusConsidered = new Set();
  for (const ad of adPool) {
    for (let t = ad.start.getTime(); t <= ad.end.getTime(); t += DAY_MS) {
      const d = new Date(t);
      const dk = dayKey(d);
      const matched = convByAdDay.get(`${ad.adId}|${dk}`) || { count: 0, value: 0 };
      // Skip ~25% of ad-days for delivery realism — but NEVER skip a day that
      // carries a real attributed conversion, otherwise that order would have
      // no matching Meta-reported conversion and the Lucidly match-rate ring
      // could exceed 100% in a window. Guarantees conversions ⊇ matched.
      if (matched.count === 0 && rng() < 0.25) continue;
      // Meta always reports more conversions than we can verify at order level
      // (edited orders, partial refunds, value drift, cross-device). Model that
      // honest gap as at most ONE surplus conversion per day, only on days that
      // already carry enough matched volume that the extra reads as a shallow
      // dip (5→6 = 83%) rather than a lone 0%/50% bar. This keeps the windowed
      // match rate a couple points below 100% (≈97–98%) — never over. Days with
      // no matched order carry 0 conversions and render as a grey
      // "no conversions" filler bar on the dashboard chart.
      // Decide surplus at most ONCE per day. The ad loop visits this dk once per
      // delivering ad, so rolling inside the loop would give a busy day N chances
      // (effective prob ≈ 1 − 0.6^N) and, at higher order volume, push the gap far
      // wider than intended (match rate sank to ~91%). Gate on a "considered" set so
      // each eligible day rolls exactly once — keeping lifetime rate ≈ 96–97%.
      let surplus = 0;
      if (
        matched.count > 0 &&
        !surplusConsidered.has(dk) &&
        (matchedTotalByDay.get(dk) || 0) >= 5
      ) {
        surplusConsidered.add(dk);
        if (rng() < 0.4) {
          surplus = 1;
          surplusDays.add(dk);
        }
      }
      const extra = surplus;
      const conversions = matched.count + extra;
      const refVal = matched.count ? matched.value / matched.count : aovRef * (0.7 + rng() * 0.6);
      const conversionValue = Math.round(matched.value + extra * refVal);
      // Spend weight blends a baseline budget term (so zero-conversion days
      // still carry realistic spend) with that day's conversion value (so
      // windowed ROAS — not just the blended headline — stays near
      // TARGET_ROAS; otherwise a recent 30/90-day window can read as
      // loss-making purely because spend was flat while conversions varied).
      const weight = ad.dailyBudget * (0.45 + rng() * 0.5) + conversionValue * 0.45;
      totalConvValue += conversionValue;
      weightSum += weight;
      drafts.push({ ad, d, conversions, conversionValue, weight, cpm: 8 + rng() * 14, ctr: 0.008 + rng() * 0.02 });
    }
  }
  const spendPerWeight = weightSum > 0 ? (totalConvValue / TARGET_ROAS) / weightSum : 0;

  const insights = [];
  for (const { ad, d, conversions, conversionValue, weight, cpm, ctr } of drafts) {
    const spend = Math.max(2, Math.round(weight * spendPerWeight));
    const impressions = Math.round((spend / cpm) * 1000);
    const clicks = Math.round(impressions * ctr);
    insights.push({
      date: dayUTC(d), hourSlot: 0,
      campaignId: ad.campaignId, campaignName: ad.campaignName,
      adSetId: ad.adSetId, adSetName: ad.adSetName,
      adId: ad.adId, adName: ad.adName,
      impressions, clicks, spend, conversions, conversionValue,
      reach: Math.round(impressions * (0.6 + rng() * 0.3)),
      frequency: 1 + rng() * 2.5,
      outboundClicks: Math.round(clicks * (0.7 + rng() * 0.25)),
      cpc: clicks ? +(spend / clicks).toFixed(2) : 0,
      cpm: +cpm.toFixed(2),
      linkClicks: Math.round(clicks * (0.8 + rng() * 0.15)),
      landingPageViews: Math.round(clicks * (0.5 + rng() * 0.3)),
      addToCart: Math.round(clicks * (0.08 + rng() * 0.08)),
      initiateCheckout: Math.round(clicks * (0.03 + rng() * 0.04)),
      viewContent: Math.round(clicks * (0.3 + rng() * 0.2)),
      videoP25: Math.round(impressions * 0.2), videoP50: Math.round(impressions * 0.12),
      videoP75: Math.round(impressions * 0.07), videoP100: Math.round(impressions * 0.04),
      importedAt: now,
    });
  }

  // ── MetaBreakdown: weekly per ad, split across demographic dimensions ──
  const AGES = AGE_BUCKETS;
  // Male ≈75% of conversions (gender split above) but ≈78% of spend, so female
  // CPA (spend÷conversions) lands ≈20/25 vs male ≈78/75 → female ≈23% cheaper.
  const GENDERS = [{ v: "male", w: 78 }, { v: "female", w: 20 }, { v: "unknown", w: 2 }];
  const PLATFORMS = [{ v: "instagram", w: 70 }, { v: "facebook", w: 30 }];
  const POSITIONS = [{ v: "feed", w: 50 }, { v: "story", w: 30 }, { v: "reels", w: 20 }];

  const breakdowns = [];
  // Aggregate insight spend/conv per ad per week first.
  const weekAgg = new Map(); // `${adId}|${weekKey}` → {spend,impr,clicks,conv,val,date}
  for (const ins of insights) {
    const wk = isoWeekMonday(ins.date);
    const key = `${ins.adId}|${wk.toISOString().slice(0, 10)}`;
    const cur = weekAgg.get(key) || {
      adId: ins.adId, adName: ins.adName, adSetId: ins.adSetId, adSetName: ins.adSetName,
      campaignId: ins.campaignId, campaignName: ins.campaignName, date: wk,
      spend: 0, impr: 0, clicks: 0, conv: 0, val: 0,
    };
    cur.spend += ins.spend; cur.impr += ins.impressions; cur.clicks += ins.clicks;
    cur.conv += ins.conversions; cur.val += ins.conversionValue;
    weekAgg.set(key, cur);
  }
  function emitBreakdown(w, type, valuesWithWeights, adGeo) {
    const values = type === "country"
      ? adGeo.map((code) => {
          const g = DEMO_GEO.find((x) => x.code === code);
          return { v: code, w: g ? g.weight : 1 };
        })
      : valuesWithWeights;
    if (type === "age_gender") {
      // cross age × gender, capped to the common buckets
      const cross = [];
      for (const a of AGES) for (const g of GENDERS.slice(0, 2)) cross.push({ v: `${a.v}|${g.v}`, w: a.w * g.w });
      values.length = 0; values.push(...cross);
    }
    const totW = values.reduce((s, x) => s + x.w, 0) || 1;
    for (const val of values) {
      const frac = val.w / totW;
      const spend = Math.round(w.spend * frac);
      if (spend <= 0 && w.conv === 0) continue;
      breakdowns.push({
        date: w.date, campaignId: w.campaignId, campaignName: w.campaignName,
        adSetId: w.adSetId, adSetName: w.adSetName, adId: w.adId, adName: w.adName,
        breakdownType: type, breakdownValue: val.v,
        impressions: Math.round(w.impr * frac), clicks: Math.round(w.clicks * frac),
        spend, conversions: Math.round(w.conv * frac),
        conversionValue: Math.round(w.val * frac),
        reach: Math.round(w.impr * frac * 0.7), linkClicks: Math.round(w.clicks * frac * 0.85),
        landingPageViews: Math.round(w.clicks * frac * 0.5),
        addToCart: Math.round(w.clicks * frac * 0.1), initiateCheckout: Math.round(w.clicks * frac * 0.05),
        viewContent: Math.round(w.clicks * frac * 0.35), outboundClicks: Math.round(w.clicks * frac * 0.8),
        importedAt: now,
      });
    }
  }
  const adGeoById = new Map(adPool.map((a) => [a.adId, a.geo]));
  for (const w of weekAgg.values()) {
    if (w.spend <= 0) continue;
    const adGeo = adGeoById.get(w.adId) || ["US"];
    emitBreakdown(w, "country", null, adGeo);
    emitBreakdown(w, "publisher_platform", PLATFORMS, adGeo);
    emitBreakdown(w, "platform_position", POSITIONS, adGeo);
    emitBreakdown(w, "age", AGES, adGeo);
    emitBreakdown(w, "gender", GENDERS, adGeo);
    emitBreakdown(w, "age_gender", [], adGeo);
  }

  // ── Sequential order numbers by date ──
  // Orders are generated per-customer (each customer's lifetime in a block), so
  // insertion order is NOT chronological. Real Shopify numbers climb with time,
  // so sort every order by createdAt and assign #1001, #1002, … in that order.
  // Joins are all by shopifyOrderId, so re-sorting the array is safe.
  orders.sort((a, b) => a.createdAt.getTime() - b.createdAt.getTime());
  orders.forEach((o, i) => { o.orderNumber = String(1001 + i); });

  // ── Bulk insert (chunked) ──
  const stamp = (rows) => rows.map((r) => ({ ...r, shopDomain }));
  await insertChunked(db.metaEntity, stamp(entityRows));
  await insertChunked(db.customer, stamp(customers));
  await insertChunked(db.order, stamp(orders));
  await insertChunked(db.orderLineItem, stamp(lineItems));
  await insertChunked(db.attribution, stamp(attributions));
  await insertChunked(db.metaInsight, stamp(insights));
  await insertChunked(db.metaBreakdown, stamp(breakdowns));
  await insertChunked(db.metaChange, stamp(metaChanges));

  // ── UTM Health: most ads carry the dominant tag pattern, a couple are
  // missing tags and one is inconsistent — the believable "mostly healthy,
  // small gap to fix" shape the UTM Health tile is built to surface.
  const adCount = adPool.length;
  const utmMissing = 2;
  const utmInconsistent = 1;
  const utmWithTags = Math.max(0, adCount - utmMissing);
  const utmConsistent = Math.max(0, utmWithTags - utmInconsistent);
  const UTM_PATTERN = "utm_source=facebook&utm_medium=paid&utm_campaign={{campaign.name}}&utm_content={{ad.name}}";

  // ── Shop config: mark demo, mirror real-brand currency/timezone ──
  await db.shop.update({
    where: { shopDomain },
    data: {
      demoMode: true, demoSeededAt: now,
      // NOTE: onboarding is NOT marked complete here. The dashboard reveal is
      // gated on onboardingPhase === "complete", and the tiles/charts read from
      // the rollup tables built by rebuildAllRollups() below. Flipping complete
      // before the rollups exist reveals a blank dashboard until the user
      // refreshes. We flip to complete only AFTER rebuildAllRollups() returns.
      currencyCode: DEMO_CURRENCY, shopifyCurrency: DEMO_CURRENCY, metaCurrency: DEMO_META_CURRENCY,
      shopifyTimezone: DEMO_TIMEZONE, metaAccountTimezone: DEMO_TIMEZONE,
      // Pretend Meta is connected: a real store couldn't show this data without
      // a live Meta connection, so the demo mirrors that. These are placeholder
      // credentials — the scheduler skips demoMode shops (see scheduler.server.js)
      // so no real Meta API call is ever made against this fake token.
      metaAccessToken: "demo-meta-token", metaAdAccountId: "act_000000000000000",
      lastOrderSync: now, lastMetaSync: now,
      utmTemplate: UTM_PATTERN, utmDominantPattern: UTM_PATTERN, utmLastAudit: now,
      utmAdsTotal: adCount, utmAdsWithTags: utmWithTags, utmAdsMissing: utmMissing,
      utmAdsConsistent: utmConsistent, utmAdsInconsistent: utmInconsistent,
    },
  });

  console.log(`[demoData] seeded base tables for ${shopDomain}: ` +
    `${customers.length} customers, ${orders.length} orders, ${lineItems.length} line items, ` +
    `${attributions.length} attributions, ${insights.length} insights, ${breakdowns.length} breakdowns, ` +
    `${entityRows.length} entities. Building rollups…`);

  // Rollups feed every tile/chart, so build them BEFORE releasing the spinner.
  // Completion is flipped in `finally` so a rollup failure still lets the user
  // into the app (blank, as before) rather than hanging on the spinner forever.
  try {
    await rebuildAllRollups(shopDomain, { force: true });
  } finally {
    await db.shop.update({
      where: { shopDomain },
      data: { onboardingCompleted: true, onboardingPhase: "complete" },
    });
  }

  console.log(`[demoData] demo store ready for ${shopDomain} in ${Math.round((Date.now() - t0) / 1000)}s`);
  return {
    customers: customers.length, orders: orders.length, attributions: attributions.length,
    insights: insights.length, breakdowns: breakdowns.length, entities: entityRows.length,
  };
}

async function insertChunked(model, rows, size = 1000) {
  for (let i = 0; i < rows.length; i += size) {
    await model.createMany({ data: rows.slice(i, i + size) });
  }
}

// ─────────────────────────────────────────────────────────────────────
// Wipe all demo data and clear the flag — called when a real merchant
// chooses "set up with my real data" (and again, defensively, the moment
// a real Meta account connects). The subsequent real Shopify backfill
// repopulates from scratch.
export async function wipeDemoData(shopDomain) {
  console.log(`[demoData] wiping demo store for ${shopDomain}…`);
  await wipeShopData(shopDomain);
  await db.shop.update({
    where: { shopDomain },
    data: {
      demoMode: false, demoSeededAt: null,
      onboardingCompleted: false, onboardingPhase: "shopify",
      lastOrderSync: null, lastMetaSync: null, lastRollupRebuild: null,
    },
  });
}
