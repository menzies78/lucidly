import type { LoaderFunctionArgs } from "@remix-run/node";
import { promises as fs } from "node:fs";
import { authenticate } from "../shopify.server";
import db from "../db.server";
import { getCachedThumbnailPath } from "../services/metaAdCreativeSync.server";

// Ad thumbnail proxy.
//
// Why a proxy instead of pointing <img src> directly at the Meta CDN URL:
//   1. Meta CDN URLs are signed and rotate every few hours/days. If the URL
//      stored in MetaEntity goes stale between nightly refreshes, the
//      explorer renders broken thumbs - a deploy that re-runs after the
//      signature ages out is the worst case.
//   2. Cached bytes survive deploys. metaAdCreativeSync writes the bytes
//      once to /data/ad-thumbnails on the Fly volume; this route streams
//      them back, falling through to a 302 to the freshest Meta URL only
//      when no local copy exists.
//
// Auth: lives under /app/* so it inherits the embedded-app session check.
// Cache-Control: long-immutable for the local path (bytes are content-keyed
//   to the ad ID and re-written when the creative changes), short cache for
//   the redirect path so a stale Meta URL gets re-fetched soon.
export const loader = async ({ request, params }: LoaderFunctionArgs) => {
  await authenticate.admin(request);
  const adId = params.adId;
  if (!adId) return new Response("missing adId", { status: 400 });

  const cached = await getCachedThumbnailPath(adId);
  if (cached) {
    const buf = await fs.readFile(cached);
    return new Response(buf, {
      status: 200,
      headers: {
        "Content-Type": "image/jpeg",
        // Bytes for a given adId effectively never change (we re-cache when
        // the path-key flips, which is rare). One day in browser is fine -
        // longer would be ideal but this matches the nightly refresh window.
        "Cache-Control": "private, max-age=86400",
      },
    });
  }

  // No local copy yet. Fall back to the most recent Meta URL we have on
  // file - the merchant still gets a thumb until the next nightly run
  // downloads bytes.
  const ent = await db.metaEntity.findFirst({
    where: { entityType: "ad", entityId: adId },
    select: { thumbnailUrl: true },
  });
  if (ent?.thumbnailUrl) {
    return new Response(null, {
      status: 302,
      headers: {
        Location: ent.thumbnailUrl,
        // Short cache - signature will age out, no point clinging to it.
        "Cache-Control": "private, max-age=300",
      },
    });
  }

  return new Response("not found", { status: 404 });
};
