import db from "../db.server";
import { fetchAllPages, fetchWithRetry } from "./metaFetch.server";

/**
 * Refresh ad creative thumbnails from Meta.
 *
 * Meta serves creative thumbnails from signed CDN URLs at scontent-*.fbcdn.net.
 * Those URLs rotate periodically (signature expires), so we re-fetch every night
 * and store the latest URL on MetaEntity.thumbnailUrl / imageUrl.
 *
 * Strategy:
 *   1. Bulk fetch /act_{id}/ads?fields=id,creative{thumbnail_url,image_url} (paged).
 *      Single request gets every ad on the account - much cheaper than per-ad calls.
 *   2. For each ad we know about (entityType='ad' rows), upsert URLs.
 *   3. For ads the bulk fetch missed (e.g. archived old ads), fall back to a
 *      per-ad GET so the explorer still has a thumb.
 *
 * Called from the daily 3am scheduler after syncMetaEntities.
 */
export async function refreshAdCreatives(shopDomain) {
  const shop = await db.shop.findUnique({ where: { shopDomain } });
  if (!shop?.metaAccessToken || !shop?.metaAdAccountId) {
    console.log(`[MetaAdCreativeSync] Skipping ${shopDomain} - not connected`);
    return { fetched: 0, updated: 0, missing: 0 };
  }

  const token = shop.metaAccessToken;
  const accountId = shop.metaAdAccountId;

  // 1) Bulk pull all ads on the account with their creative thumbs in one go.
  // Meta's /ads edge doesn't always return archived ads - the per-ad fallback
  // below handles those.
  const fields = "id,creative{thumbnail_url,image_url,image_hash}";
  const url = `https://graph.facebook.com/v21.0/${accountId}/ads?fields=${fields}&limit=500&access_token=${token}`;
  let bulkAds = [];
  try {
    bulkAds = await fetchAllPages(url, "MetaAdCreativeSync");
  } catch (err) {
    console.error(`[MetaAdCreativeSync] Bulk fetch failed for ${shopDomain}: ${err.message}`);
    return { fetched: 0, updated: 0, missing: 0, error: err.message };
  }

  const bulkMap = new Map(); // adId -> { thumbnail_url, image_url }
  for (const a of bulkAds) {
    const c = a.creative || {};
    if (c.thumbnail_url || c.image_url) {
      bulkMap.set(a.id, {
        thumbnailUrl: c.thumbnail_url || null,
        imageUrl: c.image_url || null,
      });
    }
  }

  // 2) Look up ads we know about from MetaEntity. We only update ads that are
  // already tracked - syncMetaEntities is responsible for inserting new rows.
  const knownAds = await db.metaEntity.findMany({
    where: { shopDomain, entityType: "ad" },
    select: { entityId: true, thumbnailUrl: true, thumbnailFetchedAt: true },
  });

  let updated = 0;
  const missingFromBulk = [];
  const now = new Date();

  for (const ad of knownAds) {
    const hit = bulkMap.get(ad.entityId);
    if (hit) {
      await db.metaEntity.update({
        where: { shopDomain_entityType_entityId: { shopDomain, entityType: "ad", entityId: ad.entityId } },
        data: {
          thumbnailUrl: hit.thumbnailUrl,
          imageUrl: hit.imageUrl,
          thumbnailFetchedAt: now,
        },
      });
      updated++;
    } else if (!ad.thumbnailUrl) {
      // Only chase missing ones we've never resolved - avoids hammering the
      // API for permanently-deleted creative every night.
      missingFromBulk.push(ad.entityId);
    }
  }

  // 3) Fallback: per-ad fetch for ones missing from the bulk listing.
  // Cap to 200 per run so a brand new install doesn't spend its API budget here.
  let missing = 0;
  const fallbackTargets = missingFromBulk.slice(0, 200);
  for (const adId of fallbackTargets) {
    try {
      const data = await fetchWithRetry(
        `https://graph.facebook.com/v21.0/${adId}?fields=${fields}&access_token=${token}`,
        "MetaAdCreativeSync",
      );
      const c = data.creative || {};
      if (c.thumbnail_url || c.image_url) {
        await db.metaEntity.update({
          where: { shopDomain_entityType_entityId: { shopDomain, entityType: "ad", entityId: adId } },
          data: {
            thumbnailUrl: c.thumbnail_url || null,
            imageUrl: c.image_url || null,
            thumbnailFetchedAt: now,
          },
        });
        updated++;
      } else {
        missing++;
        // Stamp fetchedAt so we don't keep retrying every night for ads with
        // genuinely unrecoverable creative (deleted, etc).
        await db.metaEntity.update({
          where: { shopDomain_entityType_entityId: { shopDomain, entityType: "ad", entityId: adId } },
          data: { thumbnailFetchedAt: now },
        });
      }
    } catch (err) {
      missing++;
      console.warn(`[MetaAdCreativeSync] per-ad fetch failed for ${adId}: ${err.message}`);
    }
  }

  console.log(`[MetaAdCreativeSync] ${shopDomain}: ${updated} thumbnails refreshed, ${missing} unresolved (bulk=${bulkMap.size}, known=${knownAds.length})`);
  return { fetched: bulkMap.size, updated, missing };
}
