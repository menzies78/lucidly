import "@shopify/shopify-app-remix/adapters/node";
import {
  ApiVersion,
  AppDistribution,
  shopifyApp,
} from "@shopify/shopify-app-remix/server";
import { PrismaSessionStorage } from "@shopify/shopify-app-session-storage-prisma";
import prisma from "./db.server";
import { startScheduler } from "./services/scheduler.server";

startScheduler();

const shopify = shopifyApp({
  apiKey: process.env.SHOPIFY_API_KEY,
  apiSecretKey: process.env.SHOPIFY_API_SECRET || "",
  apiVersion: ApiVersion.January25,
  scopes: process.env.SCOPES?.split(","),
  appUrl: process.env.SHOPIFY_APP_URL || "",
  authPathPrefix: "/auth",
  sessionStorage: new PrismaSessionStorage(prisma),
  distribution: AppDistribution.AppStore,
  future: {
    unstable_newEmbeddedAuthStrategy: true,
    // Public App Store distribution MUST mint expiring offline tokens — Shopify
    // now rejects non-expiring tokens on the Admin API with a 403 ("Non-expiring
    // access tokens are no longer accepted"), which breaks every background
    // Admin call (order sync, Fit Test, ingest, product images).
    //
    // This is env-gated, NOT on unconditionally, on purpose. Custom-distribution
    // apps (Vollebak / HM) mint non-expiring tokens that Shopify still accepts,
    // and their webhook auth never refreshes — so they avoid the library's
    // UNLOCKED webhook-path refresh (ensureValidOfflineSession in
    // authenticate.webhook), which is the concurrency race that removed
    // Vollebak's orders/updated webhook on 2026-06-29. Enabling expiring tokens
    // re-arms that race, so we scope it to the public app (EXPIRING_OFFLINE_TOKENS
    // set only in fly.app.toml) where it's mandatory, and leave the two healthy
    // custom apps untouched.
    expiringOfflineAccessTokens: process.env.EXPIRING_OFFLINE_TOKENS === "true",
  },
  ...(process.env.SHOP_CUSTOM_DOMAIN
    ? { customShopDomains: [process.env.SHOP_CUSTOM_DOMAIN] }
    : {}),
});

export default shopify;
export const apiVersion = ApiVersion.January25;
export const addDocumentResponseHeaders = shopify.addDocumentResponseHeaders;
export const authenticate = shopify.authenticate;
export const unauthenticated = shopify.unauthenticated;
export const login = shopify.login;
export const registerWebhooks = shopify.registerWebhooks;
export const sessionStorage = shopify.sessionStorage;
