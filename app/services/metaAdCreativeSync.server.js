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
 * The /api/ad-thumbnail/$adId proxy serves those bytes preferentially and
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

// Two cached variants per ad:
//   `${adId}.bin`      - small thumbnail_url (used by Ad Explorer + headline tiles)
//   `${adId}.full.bin` - full image_url (used by Top Ads for New Customers
//                        Instagram-style cards). The full asset is bigger
//                        (~50-200 KB) but visibly nicer when shown at card
//                        size. Each variant tracks its own path-key sidecar.
function thumbFilePath(adId, variant) {
  const suffix = variant === "full" ? "full." : "";
  return path.join(THUMB_DIR, `${adId}.${suffix}bin`);
}

function thumbMetaPath(adId, variant) {
  const suffix = variant === "full" ? "full." : "";
  return path.join(THUMB_DIR, `${adId}.${suffix}key`);
}

/**
 * Download bytes for adId from `url` if not already cached for that asset
 * path. `variant` is "thumb" (default) or "full". Stores the path-key
 * alongside as `{adId}.{variant}.key` so subsequent runs can cheaply skip
 * when the asset is unchanged. Failures are swallowed (and logged) -
 * missing local cache just means the proxy falls back to redirecting to
 * the live Meta URL.
 */
async function cacheBytes(adId, url, variant = "thumb") {
  if (!url) return false;
  const key = pathKey(url);
  if (!key) return false;
  await ensureThumbDir();
  const filePath = thumbFilePath(adId, variant);
  const metaPath = thumbMetaPath(adId, variant);
  try {
    const existing = await fs.readFile(metaPath, "utf8").catch(() => null);
    if (existing === key) {
      const stat = await fs.stat(filePath).catch(() => null);
      if (stat && stat.size > 0) return false;
    }
    const res = await fetch(url);
    if (!res.ok) {
      console.warn(`[MetaAdCreativeSync] ${variant} download ${adId} HTTP ${res.status}`);
      return false;
    }
    const buf = Buffer.from(await res.arrayBuffer());
    if (buf.length === 0) return false;
    await fs.writeFile(filePath, buf);
    await fs.writeFile(metaPath, key);
    return true;
  } catch (err) {
    console.warn(`[MetaAdCreativeSync] ${variant} cache failed for ${adId}: ${err.message}`);
    return false;
  }
}

// Convenience wrapper kept for the existing call sites.
async function cacheThumbnailBytes(adId, url) {
  return cacheBytes(adId, url, "thumb");
}
async function cacheFullImageBytes(adId, url) {
  return cacheBytes(adId, url, "full");
}

// Resolve image hashes to the originally-uploaded full-resolution asset URL.
//
// Meta's creative.thumbnail_url is a 64x64 PNG (visibly pixelated above
// ~80px) and creative.image_url is frequently empty or also a small
// CDN-resized variant. The /act_{id}/adimages?hashes=[...] endpoint, in
// contrast, returns the original upload URL (typically 1080x1080 or
// larger) keyed by image_hash. This is what powers the "Top Ads for New
// Customers" cards and the Ad Explorer hover-zoom — both render at
// 180-300px and pixelate badly off the 64x64 thumb.
//
// Batched up to 50 hashes per call (Meta's documented limit). Failures
// per chunk are logged and swallowed — the caller falls back to
// image_url / thumbnail_url, so a chunk failure just means lower-res
// images for the affected ads, not a hard failure.
async function resolveImageHashUrls(hashes, accountId, token) {
  const out = new Map();
  if (!hashes.size) return out;
  const arr = [...hashes];
  for (let i = 0; i < arr.length; i += 50) {
    const chunk = arr.slice(i, i + 50);
    const url = `https://graph.facebook.com/v21.0/${accountId}/adimages`
      + `?hashes=${encodeURIComponent(JSON.stringify(chunk))}`
      + `&fields=hash,url,permalink_url,width,height`
      + `&access_token=${token}`;
    try {
      const data = await fetchWithRetry(url, "MetaAdCreativeSync");
      for (const img of data?.data || []) {
        if (img.hash && img.url) out.set(img.hash, img.url);
      }
    } catch (err) {
      console.warn(`[MetaAdCreativeSync] adimages chunk failed (${chunk.length} hashes): ${err.message}`);
    }
  }
  return out;
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

  // Resolve image_hash -> full-resolution upload URL for every creative we
  // saw. This is what the "full" variant cache + the proxy fallback hand
  // back to the Top Ads / hover-zoom UI; without it those would render the
  // 64x64 thumbnail_url scaled up. DPA ads typically have no image_hash
  // (catalog-driven), so they fall through to thumbnail_url and the
  // explorer paints a "D" badge instead.
  const imageHashes = new Set();
  for (const a of bulkAds) {
    const h = a.creative?.image_hash;
    if (h) imageHashes.add(h);
  }
  const hashUrlMap = await resolveImageHashUrls(imageHashes, accountId, token);
  console.log(`[MetaAdCreativeSync] ${shopDomain}: resolved ${hashUrlMap.size}/${imageHashes.size} image hashes to full-res URLs`);

  const bulkMap = new Map(); // adId -> { thumbnail_url, image_url, productSetId }
  for (const a of bulkAds) {
    const c = a.creative || {};
    // Always record DPA ads even when they lack thumbnail / image_url - we
    // still want the explorer to know they're DPAs so it can render the
    // distinctive "D" badge.
    if (c.thumbnail_url || c.image_url || c.product_set_id) {
      // Prefer the resolved full-res URL (from /adimages) over the
      // CDN-resized image_url Meta returned with the creative bulk fetch.
      const fullResUrl = c.image_hash ? hashUrlMap.get(c.image_hash) : null;
      bulkMap.set(a.id, {
        thumbnailUrl: c.thumbnail_url || null,
        imageUrl: fullResUrl || c.image_url || null,
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
      // don't blank out the explorer. Cache both the small thumb (Ad
      // Explorer rows + headline tiles) and the full image (Top Ads for
      // New Customers cards render at ~250px so the small thumb pixelates).
      const wroteThumb = await cacheThumbnailBytes(ad.entityId, hit.thumbnailUrl);
      const wroteFull = await cacheFullImageBytes(ad.entityId, hit.imageUrl);
      if (wroteThumb || wroteFull) cached++;
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
        // Resolve this single hash inline. Per-ad fallback runs only for
        // ads missing from the bulk listing (small N) so a one-off lookup
        // here is cheaper than wiring it into the outer batch.
        let fullResUrl = c.image_url || null;
        if (c.image_hash) {
          const m = await resolveImageHashUrls(new Set([c.image_hash]), accountId, token);
          fullResUrl = m.get(c.image_hash) || fullResUrl;
        }
        await db.metaEntity.update({
          where: { shopDomain_entityType_entityId: { shopDomain, entityType: "ad", entityId: adId } },
          data: {
            thumbnailUrl: c.thumbnail_url || null,
            imageUrl: fullResUrl,
            productSetId: c.product_set_id || null,
            thumbnailFetchedAt: now,
          },
        });
        updated++;
        const wroteThumb = await cacheThumbnailBytes(adId, c.thumbnail_url);
        const wroteFull = await cacheFullImageBytes(adId, fullResUrl);
        if (wroteThumb || wroteFull) cached++;
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
 * Resolve the on-disk path for a cached image, or null if not present.
 * `variant` is "thumb" (default) or "full". Used by the proxy route to
 * decide between streaming local bytes vs redirecting to the live Meta
 * CDN URL.
 */
export async function getCachedThumbnailPath(adId, variant = "thumb") {
  if (!adId) return null;
  const filePath = thumbFilePath(adId, variant);
  try {
    const stat = await fs.stat(filePath);
    if (stat.size > 0) return filePath;
  } catch {
    // Not cached.
  }
  return null;
}
