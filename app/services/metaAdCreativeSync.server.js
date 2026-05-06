import db from "../db.server";
import { fetchWithRetry, ReduceDataError } from "./metaFetch.server";
import { promises as fs } from "node:fs";
import path from "node:path";

/**
 * Refresh ad creative thumbnails from Meta.
 *
 * Meta serves creative thumbnails from signed CDN URLs at scontent-*.fbcdn.net.
 * Those URLs rotate periodically (signature expires), so we re-fetch the URL
 * itself every night. To survive deploys and stop the explorer flickering empty
 * tiles when the latest URL signature has aged out before our next run, we
 * also download the bytes once and persist them to a Fly volume at THUMB_DIR.
 * The /app/api/ad-thumbnail/$adId proxy serves those bytes preferentially and
 * only falls back to a 302 to the freshest Meta URL when the local copy is
 * missing.
 *
 * Bytes are cached once - they don't drift the way URL signatures do. If a
 * merchant edits a creative, Meta returns a new asset path and we'll see a
 * fresh URL; we re-download in that case (path-key compare below).
 *
 * Strategy:
 *   1. Bulk fetch /act_{id}/ads?fields=id,creative{thumbnail_url,image_url} (paged).
 *      Single request gets every ad on the account - much cheaper than per-ad calls.
 *   2. For each ad we know about (entityType='ad' rows), upsert URLs and
 *      download the thumbnail bytes if not already on disk.
 *   3. For ads the bulk fetch missed (e.g. archived old ads), fall back to a
 *      per-ad GET so the explorer still has a thumb.
 *
 * Called from the daily 3am scheduler after syncMetaEntities.
 */

// Storage location for cached thumbnail bytes. Production runs on Fly with a
// 5 GB volume mounted at /data; dev falls back to a repo-local tmp dir so
// nightly tests don't need volume privileges.
const THUMB_DIR = process.env.AD_THUMBNAIL_DIR
  || (process.env.NODE_ENV === "production" ? "/data/ad-thumbnails" : path.join(process.cwd(), "tmp", "ad-thumbnails"));

let thumbDirReady = false;
async function ensureThumbDir() {
  if (thumbDirReady) return;
  await fs.mkdir(THUMB_DIR, { recursive: true });
  thumbDirReady = true;
}

// Path-key: the asset path without query/signature. When Meta rotates the
// URL signature for the same creative, the path stays the same - so we use
// it as the cache identity. If the path changes the creative content has
// changed and we re-download.
function pathKey(url) {
  try {
    const u = new URL(url);
    return u.pathname;
  } catch {
    return null;
  }
}

function thumbFilePath(adId) {
  return path.join(THUMB_DIR, `${adId}.bin`);
}

function thumbMetaPath(adId) {
  return path.join(THUMB_DIR, `${adId}.key`);
}

/**
 * Download thumbnail bytes for adId from `url` if not already cached for that
 * asset path. Stores the path-key alongside as `{adId}.key` so subsequent
 * runs can cheaply skip when the asset is unchanged. Failures are swallowed
 * (and logged) - missing local cache just means the proxy falls back to
 * redirecting to the live Meta URL.
 */
async function cacheThumbnailBytes(adId, url) {
  if (!url) return false;
  const key = pathKey(url);
  if (!key) return false;
  await ensureThumbDir();
  const filePath = thumbFilePath(adId);
  const metaPath = thumbMetaPath(adId);
  try {
    const existing = await fs.readFile(metaPath, "utf8").catch(() => null);
    if (existing === key) {
      // Same asset, already cached. Confirm the bin file is still there
      // (volume could have been wiped).
      const stat = await fs.stat(filePath).catch(() => null);
      if (stat && stat.size > 0) return false;
    }
    const res = await fetch(url);
    if (!res.ok) {
      console.warn(`[MetaAdCreativeSync] thumb download ${adId} HTTP ${res.status}`);
      return false;
    }
    const buf = Buffer.from(await res.arrayBuffer());
    if (buf.length === 0) return false;
    await fs.writeFile(filePath, buf);
    await fs.writeFile(metaPath, key);
    return true;
  } catch (err) {
    console.warn(`[MetaAdCreativeSync] thumb cache failed for ${adId}: ${err.message}`);
    return false;
  }
}
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
  let cached = 0;
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
      // Persist bytes to the Fly volume so deploys / URL-signature rotations
      // don't blank out the explorer.
      const wrote = await cacheThumbnailBytes(ad.entityId, hit.thumbnailUrl);
      if (wrote) cached++;
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
        const wrote = await cacheThumbnailBytes(adId, c.thumbnail_url);
        if (wrote) cached++;
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

  console.log(`[MetaAdCreativeSync] ${shopDomain}: ${updated} thumbnails refreshed, ${cached} bytes cached, ${missing} unresolved (bulk=${bulkMap.size}, known=${knownAds.length})`);
  return { fetched: bulkMap.size, updated, cached, missing };
}

/**
 * Resolve the on-disk path for a cached thumbnail, or null if not present.
 * Used by the proxy route to decide between streaming local bytes vs
 * redirecting to the live Meta CDN URL.
 */
export async function getCachedThumbnailPath(adId) {
  if (!adId) return null;
  const filePath = thumbFilePath(adId);
  try {
    const stat = await fs.stat(filePath);
    if (stat.size > 0) return filePath;
  } catch {
    // Not cached.
  }
  return null;
}
