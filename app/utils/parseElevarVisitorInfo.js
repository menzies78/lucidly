/**
 * parseElevarVisitorInfo
 *
 * Elevar (a marketing attribution app installed on some merchants' Shopify stores)
 * writes a JSON blob to order.note_attributes — keyed `_elevar_visitor_info` —
 * containing the original UTM / click context captured on the visitor's first
 * pageview. This is data Shopify's own session tracker (`customerJourneySummary`)
 * often misses when a consent banner gates the `_shopify_y` cookie, but Elevar
 * classifies its cookie as functional and captures it regardless.
 *
 * This parser accepts either:
 *   - webhook shape:  [{ name, value }, ...]   (payload.note_attributes)
 *   - GraphQL shape:  [{ key,  value }, ...]   (order.customAttributes)
 *
 * It returns an object whose keys line up with the Order model columns, so the
 * caller can spread the result straight into a Prisma write. All fields default
 * to "" when the attribute is missing or unparseable, matching the column defaults.
 *
 * Convention we rely on from Vollebak's Elevar install (and which is typical of
 * Elevar's Meta attribution setup): `utm_term` contains the Meta ad ID. We mirror
 * that into `metaAdIdFromUtm` only when it looks like a real numeric ad ID, so
 * merchants using utm_term for keywords aren't corrupted.
 */

const ELEVAR_KEY = "_elevar_visitor_info";

function findAttr(attrs, key) {
  if (!Array.isArray(attrs)) return null;
  for (const a of attrs) {
    const k = a?.name ?? a?.key;
    if (k === key) return a?.value ?? null;
  }
  return null;
}

function isLikelyMetaAdId(val) {
  // Meta ad IDs are purely numeric and typically 15+ digits.
  return /^\d{13,}$/.test(val || "");
}

export function parseElevarVisitorInfo(attrs) {
  const empty = {
    utmSource: "", utmMedium: "", utmCampaign: "",
    utmContent: "", utmTerm: "", utmId: "",
    fbclid: "", metaAdIdFromUtm: "",
    hasElevar: false,
  };

  const raw = findAttr(attrs, ELEVAR_KEY);
  if (!raw) return empty;

  let data;
  try { data = JSON.parse(raw); } catch { return empty; }
  if (!data || typeof data !== "object") return empty;

  const utmSource   = String(data.utm_source   || "").trim();
  const utmMedium   = String(data.utm_medium   || "").trim();
  const utmCampaign = String(data.utm_campaign || "").trim();
  const utmContent  = String(data.utm_content  || "").trim();
  const utmTerm     = String(data.utm_term     || "").trim();
  const fbclid      = String(data.fbclid       || "").trim();

  // Elevar doesn't expose utm_id as a top-level field, but its
  // `session_landing_page_url` usually carries the full querystring.
  let utmId = "";
  const landing = String(data.session_landing_page_url || "");
  if (landing.includes("utm_id=")) {
    try {
      const url = new URL(landing);
      utmId = url.searchParams.get("utm_id") || "";
    } catch {}
  }

  const metaAdIdFromUtm = isLikelyMetaAdId(utmTerm) ? utmTerm : "";

  return {
    utmSource, utmMedium, utmCampaign,
    utmContent, utmTerm, utmId,
    fbclid, metaAdIdFromUtm,
    hasElevar: true,
  };
}
