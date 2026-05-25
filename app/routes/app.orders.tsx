// Order Explorer was folded into the Customers tab. This route now just
// redirects so any old bookmarks land in the right place. The full table
// + filters live in app/components/OrderExplorerSection.tsx and are
// rendered from app/routes/app.customers.tsx.

import { redirect, type LoaderFunctionArgs } from "@remix-run/node";

export const loader = ({ request }: LoaderFunctionArgs) => {
  const url = new URL(request.url);
  // Preserve any incoming search params (date range, tag, campaign filters)
  // so the redirect target lands in the same filtered state. The tag and
  // campaign filter names changed (tag → orderTag, campaign → orderCampaign)
  // since the Customers tab has its own filter URL namespace.
  const next = new URL("/app/customers", url.origin);
  for (const [k, v] of url.searchParams.entries()) {
    if (k === "tag") next.searchParams.set("orderTag", v);
    else if (k === "campaign") next.searchParams.set("orderCampaign", v);
    else next.searchParams.set(k, v);
  }
  return redirect(next.pathname + next.search);
};
