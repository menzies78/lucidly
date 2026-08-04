import type { LoaderFunctionArgs } from "@remix-run/node";
import { redirect } from "@remix-run/node";
import { authenticate } from "../shopify.server";

/**
 * Upgrade entry point — every "Unlock with Lucidly" button lands here.
 *
 * With Shopify Managed Pricing the plan-selection page is HOSTED BY
 * SHOPIFY (defined in the Partner Dashboard); the app never renders
 * pricing UI or calls billing mutations. We just bounce the merchant to
 * their admin's pricing page for this app. Plan changes come back via
 * the app_subscriptions/update webhook.
 *
 * The redirect must break out of the embedded iframe (App Bridge treats
 * admin URLs correctly when thrown as a document-level redirect from a
 * loader with the embedded auth's exit-iframe handling).
 */
export const loader = async ({ request }: LoaderFunctionArgs) => {
  const { session } = await authenticate.admin(request);
  const storeHandle = session.shop.replace(".myshopify.com", "");
  // The public-distribution app's handle. Overridable per-app via env so
  // the legacy custom apps (no managed pricing) never build a dead link.
  const appHandle = process.env.LUCIDLY_APP_HANDLE || "lucidly-1";
  throw redirect(
    `https://admin.shopify.com/store/${storeHandle}/charges/${appHandle}/pricing_plans`,
    { headers: { "Content-Type": "text/html" } },
  );
};
