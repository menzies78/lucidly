// Canonical tooltip copy for the customer-segment toggles used across the app
// (Products, Customers, Geo, Customer Map). Centralised so the same concept
// always reads the same way, and so the honest/precise wording can't drift
// page-to-page.
//
// Taxonomy reference (Customer.metaSegment, set in customerRollups.server.js):
//   metaNew         → acquired by a Meta ad (first purchase came from Meta)
//   metaRetargeted  → existing customer re-engaged by a Meta retargeting ad
//   organic         → no Meta attribution
// "metaRepeat" is an ORDER-level label only (a later order by a metaNew
// customer); it is NOT a customer segment, so "Meta-Acquired" already contains
// those repeat orders.
export const SEGMENT_TIPS = {
  // Cohort: all orders of customers acquired by Meta, repeats included.
  acquired:
    "Customers whose first purchase came from a Meta ad — counts all their orders, including later repeats.",
  // Event: the acquiring order only.
  newFromMeta:
    "New-customer purchases attributed to a Meta ad — the acquiring order only, not later repeats.",
  // Any Meta-attributed customer (metaNew + metaRetargeted).
  allMeta:
    "All customers acquired or retargeted by Meta — every Meta-attributed customer.",
  // Existing customers re-engaged by retargeting.
  retargeted:
    "Existing customers brought back by a Meta retargeting ad — their Meta touch was retargeting, not acquisition.",
  // Complement: no Meta attribution.
  nonMeta:
    "Customers with no Meta attribution — organic, direct, and other channels.",
  // Superset: everyone.
  allCustomers: "Every customer, Meta and non-Meta combined.",
  // Binary "Meta Customers" vs "All Customers" — any Meta touch.
  metaCustomers: "Customers attributed to a Meta ad — acquired or retargeted.",
  // Meta-reported conversion scopes (Demographics / Geography on Customers).
  newMetaConversions:
    "New-customer conversions reported by Meta — first-time buyers only.",
  allMetaConversions:
    "All conversions reported by Meta — new and returning customers.",
} as const;
