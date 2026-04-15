/**
 * UTM Classification — determines if an order's UTMs indicate a paid Meta ad click.
 *
 * Paid Meta = utm_source is a Meta platform AND utm_medium indicates paid traffic.
 * Organic Facebook/Instagram traffic (utm_medium = "social", "referral", or empty)
 * is explicitly excluded — we only attribute paid ads.
 */

const META_SOURCES = new Set([
  "facebook", "fb", "ig", "instagram", "meta", "facebook-sitelink",
]);

const PAID_MEDIUMS = new Set([
  "cpc", "paid_social", "paid", "paidsocial", "ppc",
]);

/**
 * Returns true if the UTM parameters indicate a paid Meta ad click.
 */
export function isPaidMetaUtm(utmSource, utmMedium) {
  if (!utmSource || !utmMedium) return false;
  const source = utmSource.toLowerCase().trim();
  const medium = utmMedium.toLowerCase().trim();
  return META_SOURCES.has(source) && PAID_MEDIUMS.has(medium);
}

/**
 * Derives the attribution label from UTM + Layer 2 match status.
 *
 * @param {boolean} utmConfirmedMeta - order has paid Meta UTMs
 * @param {boolean} hasLayer2Match - Layer 2 statistical matcher found a match (confidence > 0)
 * @returns {"UTM & Lucidly" | "UTM" | "Lucidly" | "Unattributed"}
 */
export function getAttributionLabel(utmConfirmedMeta, hasLayer2Match) {
  if (utmConfirmedMeta && hasLayer2Match) return "UTM & Lucidly";
  if (utmConfirmedMeta) return "UTM";
  if (hasLayer2Match) return "Lucidly";
  return "Unattributed";
}
