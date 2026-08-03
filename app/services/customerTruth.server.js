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
