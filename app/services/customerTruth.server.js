// New-vs-existing ground truth for windowed (free-tier) imports.
//
// Problem: a 90-day import cannot see a customer's pre-window orders, so
// deriving customerOrderCountAtPurchase from imported rows alone would
// misclassify long-standing customers as "new". Shopify's Customer
// numberOfOrders field is a lifetime counter maintained server-side — it
// rides along on order queries at no extra API cost and is independent of
// what we imported.
//
// The arithmetic: for a customer with lifetime count N (as of fetch time)
// and W imported orders in the window, every order after the window start
// is IN the window (the window extends to now), so their k-th in-window
// order (k = 1..W, oldest first) had at-purchase count (N - W) + k.
// N === W ⟺ all their orders are in-window ⟺ the first one is their
// first ever.
//
// Trust contract: this is Shopify's own ledger, not an inference from a
// 90-day peephole. Known edge cases (report, never hide): guest checkouts
// (no customer id → "unidentified" bucket), and cancelled orders if
// Shopify's counter and our window count use different inclusion rules —
// quantified by the oracle test in _oracle_nve harness before launch.

import db from "../db.server.js";
import { isPaidMetaUtm } from "../utils/utmClassification.js";

/**
 * Compute at-purchase order counts for one customer's in-window orders.
 *
 * @param lifetimeCount  Shopify customer.numberOfOrders at fetch time (N)
 * @param windowOrders   that customer's imported orders, any order
 * @returns Map<shopifyOrderId, countAtPurchase>
 */
export function atPurchaseCountsFromLifetime(lifetimeCount, windowOrders) {
  const W = windowOrders.length;
  const sorted = [...windowOrders].sort(
    (a, b) => new Date(a.createdAt) - new Date(b.createdAt),
  );
  const out = new Map();
  sorted.forEach((o, i) => {
    // k = i+1; count = (N - W) + k. Floor at k: a lifetime counter that
    // lags behind our window count (rare webhook-timing artifact) must
    // never produce counts below what we directly observed.
    out.set(o.shopifyOrderId, Math.max(i + 1, lifetimeCount - W + (i + 1)));
  });
  return out;
}

/**
 * Classify a windowed order set for a whole shop.
 *
 * @param orders          in-window orders: { shopifyOrderId, shopifyCustomerId, createdAt }
 * @param lifetimeCounts  Map<shopifyCustomerId, numberOfOrders>
 * @returns { counts: Map<shopifyOrderId, number|null>, unidentified: number }
 *          null count = guest/no-customer order — report as its own bucket,
 *          never default into "new".
 */
export function classifyWindowOrders(orders, lifetimeCounts) {
  const byCustomer = new Map();
  let unidentified = 0;
  const counts = new Map();
  for (const o of orders) {
    if (!o.shopifyCustomerId) {
      counts.set(o.shopifyOrderId, null);
      unidentified++;
      continue;
    }
    let arr = byCustomer.get(o.shopifyCustomerId);
    if (!arr) { arr = []; byCustomer.set(o.shopifyCustomerId, arr); }
    arr.push(o);
  }
  for (const [custId, custOrders] of byCustomer) {
    const n = lifetimeCounts.get(custId);
    if (n == null) {
      // Counter unavailable (customer fetch failed): unknown, not "new".
      for (const o of custOrders) { counts.set(o.shopifyOrderId, null); unidentified++; }
      continue;
    }
    for (const [id, c] of atPurchaseCountsFromLifetime(n, custOrders)) counts.set(id, c);
  }
  return { counts, unidentified };
}

/**
 * Pre-install acquisition evidence (free tier).
 *
 * Customers whose first-ever order predates the imported window can't be
 * classified by the matcher (that history was never imported). Shopify still
 * remembers their FIRST order's UTM journey — fetch it and stamp the "utm"
 * evidence grade:
 *   - first order carried paid Meta UTMs  → metaSegment stays/becomes a
 *     Meta acquisition, segmentSource="utm" (lower-bound truth — the same
 *     weak instrument the audit demonstrates)
 *   - anything else → segmentSource stays "unknown" (organic-looking first
 *     visits can still be unmatched Meta; never guess)
 * Also backfills Customer.firstOrderDate with the true first-ever date —
 * without it, windowed LTV cohorts would date customers from their first
 * IN-WINDOW order.
 *
 * Only touches segmentSource="unknown" rows → idempotent, safe to re-run.
 * Batched to respect Admin API cost limits. The upgrade backfill's matcher
 * pass later overwrites "utm"/"unknown" with "matched" — evidence upgrades
 * only ever go up.
 *
 * @param shopDomain shop to classify
 * @param graphql    authenticated Admin GraphQL fn (from getOfflineAdmin)
 */
export async function classifyPreInstallAcquisitions(shopDomain, graphql) {
  const BATCH = 40; // customers per query — orders(first:1) subselection is cost-heavy
  const unknowns = await db.customer.findMany({
    where: { shopDomain, segmentSource: "unknown" },
    select: { shopifyCustomerId: true },
  });
  if (unknowns.length === 0) return { checked: 0, utmConfirmed: 0 };

  let checked = 0, utmConfirmed = 0;
  for (let i = 0; i < unknowns.length; i += BATCH) {
    const ids = unknowns.slice(i, i + BATCH).map(
      (c) => `gid://shopify/Customer/${c.shopifyCustomerId}`,
    );
    const res = await graphql(
      `#graphql
      query preInstallAcq($ids: [ID!]!) {
        nodes(ids: $ids) {
          ... on Customer {
            id
            numberOfOrders
            orders(first: 1, sortKey: CREATED_AT) {
              nodes {
                createdAt
                customerJourneySummary {
                  firstVisit { utmParameters { source medium } }
                }
              }
            }
          }
        }
      }`,
      { variables: { ids } },
    );
    const body = await res.json();
    for (const node of body?.data?.nodes || []) {
      if (!node?.id) continue;
      const custId = node.id.replace("gid://shopify/Customer/", "");
      const first = node.orders?.nodes?.[0];
      if (!first) continue;
      checked++;
      const utm = first.customerJourneySummary?.firstVisit?.utmParameters;
      const isMeta = isPaidMetaUtm(utm?.source, utm?.medium);
      const data = {
        firstOrderDate: new Date(first.createdAt),
        totalOrders: parseInt(node.numberOfOrders) || 0,
      };
      if (isMeta) {
        data.segmentSource = "utm";
        data.metaSegment = "metaNew";
        utmConfirmed++;
      }
      await db.customer.update({
        where: { shopDomain_shopifyCustomerId: { shopDomain, shopifyCustomerId: custId } },
        data,
      }).catch(() => {}); // row deleted mid-run: skip
    }
  }
  console.log(`[customerTruth] ${shopDomain}: pre-install acquisition pass — ${checked} checked, ${utmConfirmed} UTM-confirmed Meta`);
  return { checked, utmConfirmed };
}
