// Name-based gender inference. Independent of Meta breakdown enrichment;
// uses the customer's billing first name, country-aware where possible.
//
// Backed by `gender-detection-from-name` (curated per-language name lists).
// The package only emits "male"/"female"/"unknown"; "unknown" means the
// author judged the name too ambiguous to assign — which already enforces
// a high-confidence threshold inside the package.
//
// We layer one extra check on top: when a country code is available, we
// also run the global (no-language) lookup. If they DISAGREE we drop to
// null, because that signals the name is gendered differently across
// cultures (e.g. Andrea = female in EN, male in IT). Better to leave it
// blank than to mislabel a customer.
//
// Returns: { gender: "male" | "female" | null, confidence: number | null }

import { getGender } from "gender-detection-from-name";

// Country code → BCP-47-ish language hint accepted by the package.
// Add codes as we encounter merchants in new markets. Unknown/missing
// country falls through to the no-language ("global") path which still
// works but loses tie-break info.
const COUNTRY_TO_LANG = {
  // English-speaking
  US: "en", GB: "en", UK: "en", IE: "en", AU: "en", NZ: "en", CA: "en", ZA: "en",
  // Romance
  IT: "it",
  ES: "es", AR: "es", MX: "es", CL: "es", CO: "es", PE: "es", VE: "es",
  FR: "fr", BE: "fr", LU: "fr", CH: "fr",
  // Germanic
  DE: "de", AT: "de",
  // Other supported
  TR: "tr",
};

// Default confidence stamp when the package returns a non-"unknown" answer.
// The package's binary output already implies it cleared its internal
// threshold; we record 0.95 as a flat probability marker.
const DEFAULT_CONFIDENCE = 0.95;

// Strip whitespace, hyphens, apostrophes — try first token first (most
// people enter "Mary-Anne" or "John Paul" as a multi-part first name; the
// first token is usually the conventional gendered part).
function normalizeFirstName(raw) {
  if (!raw || typeof raw !== "string") return null;
  const cleaned = raw.trim().replace(/[^\p{L}\s\-']/gu, "");
  if (!cleaned) return null;
  // Split on whitespace, hyphen, apostrophe; keep tokens of length ≥2
  const tokens = cleaned.split(/[\s\-']/).map((t) => t.trim()).filter((t) => t.length >= 2);
  if (tokens.length === 0) return null;
  return tokens[0];
}

/**
 * Infer gender from a first name and optional ISO country code.
 *
 * @param {string|null|undefined} firstName  Raw billing first name (any case).
 * @param {string|null|undefined} countryCode  Two-letter ISO code, e.g. "GB".
 * @returns {{ gender: "male"|"female"|null, confidence: number|null }}
 */
export function inferGender(firstName, countryCode) {
  const name = normalizeFirstName(firstName);
  if (!name) return { gender: null, confidence: null };

  const lang = countryCode ? COUNTRY_TO_LANG[countryCode.toUpperCase()] : null;

  // Country-language lookup (or undefined → global) and global lookup.
  // We call both so we can detect cross-cultural disagreement.
  let countryAnswer;
  try {
    countryAnswer = lang ? getGender(name, lang) : null;
  } catch {
    countryAnswer = null;
  }
  let globalAnswer;
  try {
    globalAnswer = getGender(name);
  } catch {
    globalAnswer = "unknown";
  }

  // Normalize "unknown" → null for our purposes.
  const c = countryAnswer === "male" || countryAnswer === "female" ? countryAnswer : null;
  const g = globalAnswer === "male" || globalAnswer === "female" ? globalAnswer : null;

  // Decision matrix:
  // - country known + matches global → confident, return it
  // - country known + global unknown → trust country
  // - country known + disagrees with global → ambiguous, null
  // - country unknown + global known → return global (single source)
  // - both unknown → null
  if (c && g) {
    if (c === g) return { gender: c, confidence: DEFAULT_CONFIDENCE };
    return { gender: null, confidence: null }; // cross-cultural ambiguity
  }
  if (c && !g) return { gender: c, confidence: DEFAULT_CONFIDENCE };
  if (!c && g) return { gender: g, confidence: DEFAULT_CONFIDENCE };
  return { gender: null, confidence: null };
}

/**
 * Backfill inferred gender for every Customer in the shop. Pulls each
 * customer's first order to find a billing first name + country code.
 *
 * Idempotent: safe to re-run. Only writes when the inference actually
 * changes (or first-time populates) the stored value.
 *
 * @param {object} db  Prisma client.
 * @param {string} shopDomain
 * @returns {Promise<{scanned: number, inferred: number, ambiguous: number, noName: number}>}
 */
export async function backfillShopInferredGender(db, shopDomain) {
  const customers = await db.customer.findMany({
    where: { shopDomain },
    select: { id: true, shopifyCustomerId: true, inferredGender: true },
  });

  if (customers.length === 0) {
    return { scanned: 0, inferred: 0, ambiguous: 0, noName: 0 };
  }

  // For each customer, find their earliest order with a non-empty first
  // name. We only need (firstName, countryCode); a single query per
  // customer would be slow at 30k+ rows, so pull all-orders-with-name in
  // one go and pick the earliest per customer client-side.
  const orders = await db.order.findMany({
    where: {
      shopDomain,
      shopifyCustomerId: { in: customers.map((c) => c.shopifyCustomerId) },
      customerFirstName: { not: "" },
    },
    select: { shopifyCustomerId: true, customerFirstName: true, countryCode: true, createdAt: true },
    orderBy: { createdAt: "asc" },
  });

  const firstByCustomer = new Map();
  for (const o of orders) {
    if (!firstByCustomer.has(o.shopifyCustomerId)) {
      firstByCustomer.set(o.shopifyCustomerId, o);
    }
  }

  let scanned = 0, inferred = 0, ambiguous = 0, noName = 0;
  const updates = [];

  for (const c of customers) {
    scanned++;
    const o = firstByCustomer.get(c.shopifyCustomerId);
    if (!o || !o.customerFirstName) { noName++; continue; }
    const { gender, confidence } = inferGender(o.customerFirstName, o.countryCode);
    if (!gender) { ambiguous++; continue; }
    inferred++;
    updates.push({
      id: c.id,
      data: {
        inferredGender: gender,
        inferredGenderConfidence: confidence,
        inferredGenderSource: "name",
      },
    });
  }

  // Batched updates — chunks of 200 keep the SQLite write transaction
  // small enough to avoid locking the connection pool.
  for (let i = 0; i < updates.length; i += 200) {
    const chunk = updates.slice(i, i + 200);
    await Promise.all(chunk.map((u) => db.customer.update({ where: { id: u.id }, data: u.data })));
  }

  return { scanned, inferred, ambiguous, noName };
}

/**
 * Single-customer update path — used by webhooks and order sync when a
 * new order arrives. Only writes when we have a confident inference
 * AND the customer doesn't already have one (so we never re-infer on
 * later orders, which could bounce on multi-token names).
 *
 * @param {object} db
 * @param {string} shopDomain
 * @param {string} shopifyCustomerId
 * @param {string} firstName
 * @param {string} countryCode
 */
export async function updateCustomerInferredGenderIfMissing(db, shopDomain, shopifyCustomerId, firstName, countryCode) {
  const customer = await db.customer.findUnique({
    where: { shopDomain_shopifyCustomerId: { shopDomain, shopifyCustomerId } },
    select: { id: true, inferredGender: true },
  });
  if (!customer) return;
  if (customer.inferredGender) return; // already set, leave alone
  const { gender, confidence } = inferGender(firstName, countryCode);
  if (!gender) return;
  await db.customer.update({
    where: { id: customer.id },
    data: {
      inferredGender: gender,
      inferredGenderConfidence: confidence,
      inferredGenderSource: "name",
    },
  });
}
