// Managed Pricing (Shopify App Pricing) enforcement.
//
// Billing itself is handled entirely by Shopify's Managed Pricing: the
// pricing page, plan selection, the 30-day trial, charges, upgrades and
// reinstalls all happen outside our code. What Shopify does NOT do is stop a
// merchant whose trial has expired (and who declined to pay) from loading the
// app - their subscription is simply gone. This helper closes that gap by
// reading the merchant's active subscription and letting the app loader bounce
// anyone without one to the pricing page.
//
// Enforcement is behind an env kill-switch so the gate can be deployed dark
// and only switched on once Managed Pricing plans are actually live in the
// Partner Dashboard. Until then merchants are never gated.
//   LUCIDLY_BILLING_ENFORCED = "true"  -> gate active
//   SHOPIFY_APP_HANDLE        = app handle used in the pricing-page URL
//                               (defaults to "lucidly")

import type { AdminApiContext } from "@shopify/shopify-app-remix/server";

export function isBillingEnforced(): boolean {
  return (process.env.LUCIDLY_BILLING_ENFORCED || "").trim().toLowerCase() === "true";
}

// True when the shop has at least one ACTIVE app subscription. A merchant on
// the free trial counts as ACTIVE (Managed Pricing creates the subscription up
// front), so this correctly lets trialling merchants through and only bounces
// those whose subscription has lapsed or was declined.
export async function hasActiveSubscription(admin: AdminApiContext): Promise<boolean> {
  const res = await admin.graphql(
    `#graphql
    query LucidlyActiveSubscriptions {
      currentAppInstallation {
        activeSubscriptions {
          id
          status
        }
      }
    }`,
  );
  const body = await res.json();
  const subs = body?.data?.currentAppInstallation?.activeSubscriptions ?? [];
  return subs.some((s: { status?: string }) => s?.status === "ACTIVE");
}

// The Managed Pricing plan-selection page for this app, for the given shop.
export function pricingPageUrl(shopDomain: string): string {
  const storeHandle = shopDomain.replace(/\.myshopify\.com$/, "");
  const appHandle = process.env.SHOPIFY_APP_HANDLE || "lucidly";
  return `https://admin.shopify.com/store/${storeHandle}/charges/${appHandle}/pricing_plans`;
}
