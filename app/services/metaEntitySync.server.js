import db from "../db.server";
import { fetchAllPages } from "./metaFetch.server";

/**
 * Fetches created_time for Meta campaigns, ad sets, and ads.
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

  // Fetch ad sets
  if (missingAdSets.length > 0) {
    const adsets = await fetchEntityBatch(accountId, token, "adsets", missingAdSets);
    for (const a of adsets) {
      await db.metaEntity.upsert({
        where: { shopDomain_entityType_entityId: { shopDomain, entityType: "adset", entityId: a.id } },
        create: { shopDomain, entityType: "adset", entityId: a.id, createdTime: new Date(a.created_time) },
        update: { createdTime: new Date(a.created_time) },
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
 * Fetch created_time for a batch of entity IDs.
 * Uses the /act_{id}/{type} endpoint with filtering to get only the ones we need.
 * Falls back to fetching ALL entities if filtering by IDs isn't practical.
 */
async function fetchEntityBatch(accountId, token, type, missingIds) {
  // Meta API supports filtering by IDs for campaigns/adsets/ads
  // But with large numbers, it's more efficient to fetch all and filter client-side
  const limit = 500;
  const url = `https://graph.facebook.com/v21.0/${accountId}/${type}?fields=id,created_time&limit=${limit}&access_token=${token}`;
  const allEntities = await fetchAllPages(url, "MetaEntitySync");

  const missingSet = new Set(missingIds);
  return allEntities.filter(e => missingSet.has(e.id));
}
