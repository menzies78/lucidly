import db from "../db.server";
import { fetchWithRetry, ReduceDataError } from "./metaFetch.server";

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
  //
  // Page size: Meta returns code 1 ("reduce the amount of data") on subsequent
  // pages once the creative{...} expansion gets too heavy at limit=500. Empirically
  // limit=100 paginates cleanly across thousands of ads. Don't raise without
  // testing - a single ReduceDataError used to nuke the whole bulk fetch.
  // product_set_id is set on Dynamic Product Ad creative (DPA / Advantage+
  // catalog) - the explorer renders a "D" badge instead of an empty thumb
  // when this is non-null.
  const fields = "id,creative{thumbnail_url,image_url,image_hash,product_set_id}";
  const initialUrl = `https://graph.facebook.com/v21.0/${accountId}/ads?fields=${fields}&limit=100&access_token=${token}`;
  // Inline paging so a mid-walk failure preserves whatever pages already
  // succeeded. fetchAllPages would discard the partial set on throw - which
  // had been silently breaking thumbnail refresh for this account.
  const bulkAds = [];
  let nextUrl = initialUrl;
  let pages = 0;
  let pageError = null;
  while (nextUrl) {
    try {
      const data = await fetchWithRetry(nextUrl, "MetaAdCreativeSync");
      if (!data?.data) break;
      bulkAds.push(...data.data);
      pages++;
      nextUrl = data.paging?.next || null;
    } catch (err) {
      pageError = err;
      console.warn(`[MetaAdCreativeSync] Bulk fetch stopped after page ${pages} for ${shopDomain}: ${err.message}`);
      // ReduceDataError or any other mid-walk failure: keep what we collected
      // and continue to the per-ad fallback for the rest. Hard-fail only when
      // page 1 itself failed (we have nothing to write).
      break;
    }
  }
  if (pages === 0 && pageError) {
    return { fetched: 0, updated: 0, missing: 0, error: pageError.message };
  }
  console.log(`[MetaAdCreativeSync] ${shopDomain}: bulk fetched ${bulkAds.length} ads across ${pages} pages${pageError ? ` (stopped: ${pageError.name})` : ""}`);

  const bulkMap = new Map(); // adId -> { thumbnail_url, image_url, productSetId }
  for (const a of bulkAds) {
    const c = a.creative || {};
    // Always record DPA ads even when they lack thumbnail / image_url - we
    // still want the explorer to know they're DPAs so it can render the
    // distinctive "D" badge.
    if (c.thumbnail_url || c.image_url || c.product_set_id) {
      bulkMap.set(a.id, {
        thumbnailUrl: c.thumbnail_url || null,
        imageUrl: c.image_url || null,
        productSetId: c.product_set_id || null,
      });
    }
  }

  // 2) Look up ads we know about from MetaEntity. We only update ads that are
  // already tracked - syncMetaEntities is responsible for inserting new rows.
  const knownAds = await db.metaEntity.findMany({
    where: { shopDomain, entityType: "ad" },
    select: { entityId: true, thumbnailUrl: true, thumbnailFetchedAt: true, productSetId: true },
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
          productSetId: hit.productSetId,
          thumbnailFetchedAt: now,
        },
      });
      updated++;
    } else if (!ad.thumbnailUrl && !ad.productSetId) {
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
      if (c.thumbnail_url || c.image_url || c.product_set_id) {
        await db.metaEntity.update({
          where: { shopDomain_entityType_entityId: { shopDomain, entityType: "ad", entityId: adId } },
          data: {
            thumbnailUrl: c.thumbnail_url || null,
            imageUrl: c.image_url || null,
            productSetId: c.product_set_id || null,
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
