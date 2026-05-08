// Role gating for Lucidly internal tooling.
//
// Some surfaces in the dashboard (Data Pipeline buttons, full re-matchers,
// historical backfill triggers) are not safe to expose to every merchant -
// they're operational levers we use ourselves while shaking down the app.
// Production merchants should see a clean dashboard without the dev plumbing.
//
// `LUCIDLY_INTERNAL_SHOPS` is a comma-separated list of shop domains that
// see internal tooling. Set it on Fly.io. Add a merchant's domain
// temporarily when you need to debug their account in their store, then
// remove it to revoke access.

export function isInternalShop(shopDomain: string | null | undefined): boolean {
  if (!shopDomain) return false;
  const raw = process.env.LUCIDLY_INTERNAL_SHOPS || "";
  if (!raw.trim()) return false;
  const allowList = raw.split(",").map(s => s.trim().toLowerCase()).filter(Boolean);
  return allowList.includes(shopDomain.toLowerCase());
}
