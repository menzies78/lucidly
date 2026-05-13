/**
 * Unified gender precedence used across rollups, route loaders, and demographic
 * tiles.
 *
 * Precedence (confirmed 2026-05-13):
 *   1. Customer.inferredGender when inferredGenderConfidence >= 0.95
 *      (name + country signal a merchant can sense-check from billing details).
 *   2. Attribution.metaGender (Meta's audience signal — directionally useful
 *      but can disagree with billing for the same buyer).
 *   3. Customer.inferredGender at any confidence (long-tail fallback for orders
 *      where Meta breakdowns weren't enriched).
 *   4. null.
 *
 * The 0.95 threshold matches DEFAULT_CONFIDENCE in nameGender.server.js — the
 * value the package emits for unambiguous name hits. Title-only fallbacks write
 * 0.85, which deliberately sits below the threshold so Meta still wins those.
 *
 * SQL equivalent (inlined where used to avoid Prisma.sql composition overhead):
 *   CASE
 *     WHEN c.inferredGenderConfidence >= 0.95 AND c.inferredGender IS NOT NULL
 *       THEN c.inferredGender
 *     WHEN a.metaGender IS NOT NULL THEN a.metaGender
 *     WHEN c.inferredGender IS NOT NULL THEN c.inferredGender
 *     ELSE NULL
 *   END
 */
export const HIGH_CONFIDENCE_GENDER_THRESHOLD = 0.95;

export function resolveGender(metaGender, inferredGender, inferredGenderConfidence) {
  if (
    inferredGender &&
    inferredGenderConfidence != null &&
    inferredGenderConfidence >= HIGH_CONFIDENCE_GENDER_THRESHOLD
  ) {
    return inferredGender;
  }
  if (metaGender) return metaGender;
  if (inferredGender) return inferredGender;
  return null;
}
