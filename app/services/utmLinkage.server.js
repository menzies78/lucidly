import db from "../db.server";

/**
 * UTM → Meta Campaign Linkage
 *
 * For orders with utmConfirmedMeta=true, resolves UTM parameters to current
 * Meta campaign/adset/ad IDs and names using MetaInsight data.
 *
 * Key: when an ad is resolved (by ID or name), we look up the CURRENT parent
 * campaign/adset from the most recent MetaInsight row for that ad — not the
 * UTM's campaign/adset values, which may be stale (renamed campaigns etc).
 *
 * Populates Order.metaCampaignId/Name, metaAdSetId/Name, metaAdId/Name.
 * Only sets fields that aren't already populated (preserves Layer 2 matcher data).
 */

function isNumericId(val) {
  return /^\d{5,}$/.test(val);
}

/**
 * Build lookup maps from MetaInsight for a given shop.
 * - ads.byId/byName → adId
 * - adHierarchy[adId] → { campaignId, campaignName, adSetId, adSetName, adName }
 *   from the MOST RECENT insight row (current names)
 */
async function buildLookupMaps(shopDomain) {
  // Get all insight rows ordered by date desc so first match per ad = most recent
  const insights = await db.metaInsight.findMany({
    where: { shopDomain },
    select: {
      campaignId: true, campaignName: true,
      adSetId: true, adSetName: true,
      adId: true, adName: true,
      date: true,
    },
    orderBy: { date: "desc" },
  });

  const ads = { byId: {}, byName: {} };
  const adHierarchy = {}; // adId → { campaignId, campaignName, adSetId, adSetName, adName }

  for (const row of insights) {
    if (!row.adId) continue;

    // First occurrence per adId = most recent (ordered by date desc)
    if (!adHierarchy[row.adId]) {
      adHierarchy[row.adId] = {
        campaignId: row.campaignId,
        campaignName: row.campaignName,
        adSetId: row.adSetId,
        adSetName: row.adSetName,
        adName: row.adName,
      };
    }

    // Build ad lookup (name → id, id → name)
    if (!ads.byId[row.adId]) ads.byId[row.adId] = row.adName || row.adId;
    if (row.adName && !ads.byName[row.adName]) ads.byName[row.adName] = row.adId;
  }

  return { ads, adHierarchy };
}

/**
 * Resolve a UTM value to an ad ID.
 * @returns {string|null} adId
 */
function resolveAdId(utmValue, adsLookup) {
  if (!utmValue) return null;
  if (isNumericId(utmValue)) return utmValue;
  return adsLookup.byName[utmValue] || null;
}

/**
 * Link UTM data to Meta campaign/adset/ad for all utmConfirmedMeta orders.
 * Uses the ad hierarchy from the most recent MetaInsight to get current names,
 * not the potentially stale UTM values.
 *
 * @param {string} shopDomain
 * @returns {{ linked: number, alreadyLinked: number, noMatch: number }}
 */
export async function linkUtmToCampaigns(shopDomain) {
  console.log(`[UTMLinkage] Starting for ${shopDomain}`);

  const { ads, adHierarchy } = await buildLookupMaps(shopDomain);
  console.log(`[UTMLinkage] Built lookup maps: ${Object.keys(ads.byId).length} ads, ${Object.keys(adHierarchy).length} hierarchies`);

  const orders = await db.order.findMany({
    where: { shopDomain, utmConfirmedMeta: true },
    select: {
      id: true, utmCampaign: true, utmContent: true, utmTerm: true,
      utmId: true,
      metaCampaignId: true, metaCampaignName: true,
      metaAdSetId: true, metaAdSetName: true,
      metaAdId: true, metaAdName: true,
    },
  });

  let linked = 0, alreadyLinked = 0, noMatch = 0;

  for (const order of orders) {
    // Skip if matcher already populated full campaign data
    if (order.metaCampaignId && order.metaAdId) {
      alreadyLinked++;
      continue;
    }

    // Resolve ad ID from utm_id (preferred), utm_content (ad name), or existing metaAdId
    const adId = resolveAdId(order.utmId, ads)
      || resolveAdId(order.utmContent, ads)
      || order.metaAdId
      || null;

    if (!adId) {
      noMatch++;
      continue;
    }

    // Look up the CURRENT hierarchy for this ad
    const hierarchy = adHierarchy[adId];
    if (!hierarchy) {
      // Ad ID found but no insight data — set what we can
      const updateData = {};
      if (!order.metaAdId) updateData.metaAdId = adId;
      if (!order.metaAdName && ads.byId[adId]) updateData.metaAdName = ads.byId[adId];
      if (Object.keys(updateData).length > 0) {
        await db.order.update({ where: { id: order.id }, data: updateData });
        linked++;
      } else {
        alreadyLinked++;
      }
      continue;
    }

    // Set all fields from the current hierarchy
    const updateData = {};
    if (!order.metaAdId) updateData.metaAdId = adId;
    if (!order.metaAdName) updateData.metaAdName = hierarchy.adName || ads.byId[adId] || adId;
    if (!order.metaCampaignId && hierarchy.campaignId) updateData.metaCampaignId = hierarchy.campaignId;
    if (!order.metaCampaignName && hierarchy.campaignName) updateData.metaCampaignName = hierarchy.campaignName;
    if (!order.metaAdSetId && hierarchy.adSetId) updateData.metaAdSetId = hierarchy.adSetId;
    if (!order.metaAdSetName && hierarchy.adSetName) updateData.metaAdSetName = hierarchy.adSetName;

    // Also FIX stale names from previous linkage runs
    if (order.metaCampaignName && hierarchy.campaignName && order.metaCampaignName !== hierarchy.campaignName) {
      updateData.metaCampaignName = hierarchy.campaignName;
      if (hierarchy.campaignId) updateData.metaCampaignId = hierarchy.campaignId;
    }
    if (order.metaAdSetName && hierarchy.adSetName && order.metaAdSetName !== hierarchy.adSetName) {
      updateData.metaAdSetName = hierarchy.adSetName;
      if (hierarchy.adSetId) updateData.metaAdSetId = hierarchy.adSetId;
    }

    if (Object.keys(updateData).length > 0) {
      await db.order.update({ where: { id: order.id }, data: updateData });
      linked++;
    } else {
      alreadyLinked++;
    }
  }

  console.log(`[UTMLinkage] Complete: ${linked} linked, ${alreadyLinked} already linked, ${noMatch} no match`);
  return { linked, alreadyLinked, noMatch };
}
