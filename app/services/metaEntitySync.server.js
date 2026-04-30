import db from "../db.server";
import { fetchAllPages } from "./metaFetch.server";

/**
 * Fetches created_time (and targeting for ad sets) for Meta campaigns, ad sets, and ads.
 * Only fetches entities we don't already have in the MetaEntity table.
 * Called during the daily 3am sync.
 */

export async function syncMetaEntities(shopDomain) {
  const shop = await db.shop.findUnique({ where: { shopDomain } });
  if (!shop?.metaAccessToken || !shop?.metaAdAccountId) {
    console.log(`[MetaEntitySync] Skipping ${shopDomain} — not connected`);
    return { campaigns: 0, adsets: 0, ads: 0 };
  }

  const token = shop.metaAccessToken;
  const accountId = shop.metaAdAccountId;

  // Find entity IDs we already know about from MetaInsight data
  const knownCampaigns = await db.metaInsight.findMany({
    where: { shopDomain },
    select: { campaignId: true },
    distinct: ["campaignId"],
  });
  const knownAdSets = await db.metaInsight.findMany({
    where: { shopDomain },
    select: { adSetId: true },
    distinct: ["adSetId"],
  });
  const knownAds = await db.metaInsight.findMany({
    where: { shopDomain },
    select: { adId: true },
    distinct: ["adId"],
  });

  // Find which ones we already have in MetaEntity
  const existingEntities = await db.metaEntity.findMany({
    where: { shopDomain },
    select: { entityType: true, entityId: true },
  });
  const existingSet = new Set(existingEntities.map(e => `${e.entityType}:${e.entityId}`));

  const missingCampaigns = knownCampaigns.map(r => r.campaignId).filter(id => id && !existingSet.has(`campaign:${id}`));
  const missingAdSets = knownAdSets.map(r => r.adSetId).filter(id => id && !existingSet.has(`adset:${id}`));
  const missingAds = knownAds.map(r => r.adId).filter(id => id && !existingSet.has(`ad:${id}`));

  const totals = { campaigns: 0, adsets: 0, ads: 0 };

  // Fetch campaigns
  if (missingCampaigns.length > 0) {
    const campaigns = await fetchEntityBatch(accountId, token, "campaigns", missingCampaigns);
    for (const c of campaigns) {
      await db.metaEntity.upsert({
        where: { shopDomain_entityType_entityId: { shopDomain, entityType: "campaign", entityId: c.id } },
        create: { shopDomain, entityType: "campaign", entityId: c.id, createdTime: new Date(c.created_time) },
        update: { createdTime: new Date(c.created_time) },
      });
      totals.campaigns++;
    }
  }

  // Fetch ad sets — includes targeting for funnel classification
  if (missingAdSets.length > 0) {
    const adsets = await fetchEntityBatch(accountId, token, "adsets", missingAdSets, "id,created_time,targeting");
    for (const a of adsets) {
      const targeting = a.targeting || null;
      const stage = classifyFunnelStage(targeting);
      await db.metaEntity.upsert({
        where: { shopDomain_entityType_entityId: { shopDomain, entityType: "adset", entityId: a.id } },
        create: {
          shopDomain, entityType: "adset", entityId: a.id,
          createdTime: new Date(a.created_time),
          targetingSpec: targeting ? JSON.stringify(targeting) : null,
          funnelStage: stage,
        },
        update: {
          createdTime: new Date(a.created_time),
          targetingSpec: targeting ? JSON.stringify(targeting) : null,
          funnelStage: stage,
        },
      });
      totals.adsets++;
    }
  }

  // Fetch ads
  if (missingAds.length > 0) {
    const ads = await fetchEntityBatch(accountId, token, "ads", missingAds);
    for (const a of ads) {
      await db.metaEntity.upsert({
        where: { shopDomain_entityType_entityId: { shopDomain, entityType: "ad", entityId: a.id } },
        create: { shopDomain, entityType: "ad", entityId: a.id, createdTime: new Date(a.created_time) },
        update: { createdTime: new Date(a.created_time) },
      });
      totals.ads++;
    }
  }

  // Also refresh targeting for EXISTING ad sets that don't have it yet
  const adsetsWithoutTargeting = await db.metaEntity.findMany({
    where: { shopDomain, entityType: "adset", targetingSpec: null },
    select: { entityId: true },
  });
  if (adsetsWithoutTargeting.length > 0) {
    const ids = adsetsWithoutTargeting.map(e => e.entityId);
    console.log(`[MetaEntitySync] Backfilling targeting for ${ids.length} ad sets`);
    const adsets = await fetchEntityBatch(accountId, token, "adsets", ids, "id,created_time,targeting");
    let backfilled = 0;
    for (const a of adsets) {
      const targeting = a.targeting || null;
      const stage = classifyFunnelStage(targeting);
      await db.metaEntity.update({
        where: { shopDomain_entityType_entityId: { shopDomain, entityType: "adset", entityId: a.id } },
        data: {
          targetingSpec: targeting ? JSON.stringify(targeting) : null,
          funnelStage: stage,
        },
      });
      backfilled++;
    }
    console.log(`[MetaEntitySync] Backfilled targeting for ${backfilled} ad sets`);
  }

  console.log(`[MetaEntitySync] ${shopDomain}: ${totals.campaigns} campaigns, ${totals.adsets} adsets, ${totals.ads} ads synced`);

  // Nightly UTM audit — flag missing/inconsistent UTMs (no auto-push without consent)
  try {
    const { nightlyUtmAudit } = await import("./utmManager.server.js");
    await nightlyUtmAudit(shopDomain);
  } catch (err) {
    console.error(`[MetaEntitySync] UTM audit failed (non-fatal): ${err.message}`);
  }

  return totals;
}

/**
 * Classify an ad set's funnel position based on its targeting spec.
 *
 * Cold (Top): No custom audiences, OR only lookalike audiences, OR interest/broad targeting
 * Warm (Mid): Custom audiences with engagement signals (visitors, video viewers, engagers)
 * Hot (Bottom): Custom audiences with purchase intent (ATC, checkout, past purchasers)
 */
export function classifyFunnelStage(targeting) {
  if (!targeting) return "cold";
  const audiences = targeting.custom_audiences || [];
  if (audiences.length === 0) return "cold";

  // Check audience names for purchase-intent signals (hot)
  const hasHot = audiences.some(a =>
    /purchase|buyer|customer|checkout|add.to.cart|atc|converter|high.value|vip|repeat/i.test(a.name || "")
  );
  if (hasHot) return "hot";

  // Check for engagement/retargeting signals (warm)
  const hasWarm = audiences.some(a =>
    /visit|view|engag|video|page|profile|website|pixel|retarget|abandon|browse|interact|follower/i.test(a.name || "")
  );
  if (hasWarm) return "warm";

  // Lookalike audiences are cold (prospecting)
  const allLookalike = audiences.every(a =>
    /lookalike|lal|similar/i.test(a.name || "")
  );
  if (allLookalike) return "cold";

  // Default: has custom audiences but unclear type — treat as warm
  return "warm";
}

/**
 * Fetch created_time (and optionally targeting) for a batch of entity IDs.
 * Uses the /act_{id}/{type} endpoint with filtering to get only the ones we need.
 * Falls back to fetching ALL entities if filtering by IDs isn't practical.
 */
async function fetchEntityBatch(accountId, token, type, missingIds, fields = "id,created_time") {
  const limit = 500;
  const url = `https://graph.facebook.com/v21.0/${accountId}/${type}?fields=${fields}&limit=${limit}&access_token=${token}`;
  const allEntities = await fetchAllPages(url, "MetaEntitySync");

  const missingSet = new Set(missingIds);
  return allEntities.filter(e => missingSet.has(e.id));
}
