// Ensures required webhook subscriptions exist for a shop. Idempotent.
// Called on auth (install/reauth) and on first app load per process per shop.
import db from "../db.server";

const REQUIRED = [
  { topic: "ORDERS_CREATE", path: "/webhooks/orders/create" },
  { topic: "ORDERS_UPDATED", path: "/webhooks/orders/updated" },
];

const done = new Set(); // in-memory cache: shops already checked this process lifetime

export async function ensureWebhooks(shopDomain, accessToken) {
  if (done.has(shopDomain)) return { skipped: true };

  const appUrl = process.env.SHOPIFY_APP_URL;
  if (!appUrl) {
    console.warn("[ensureWebhooks] SHOPIFY_APP_URL not set, skipping");
    return { skipped: true };
  }

  const gql = async (query, variables) => {
    const res = await fetch(`https://${shopDomain}/admin/api/2025-01/graphql.json`, {
      method: "POST",
      headers: { "X-Shopify-Access-Token": accessToken, "Content-Type": "application/json" },
      body: JSON.stringify({ query, variables }),
    });
    return res.json();
  };

  // List existing subscriptions
  const listRes = await gql(`{
    webhookSubscriptions(first: 100) {
      edges { node { id topic endpoint { __typename ... on WebhookHttpEndpoint { callbackUrl } } } }
    }
  }`);
  const existing = listRes?.data?.webhookSubscriptions?.edges || [];

  const created = [];
  for (const { topic, path } of REQUIRED) {
    const callbackUrl = `${appUrl.replace(/\/$/, "")}${path}`;
    const alreadyExists = existing.some(e =>
      e.node.topic === topic && e.node.endpoint?.callbackUrl === callbackUrl
    );
    if (alreadyExists) continue;

    const mut = await gql(
      `mutation($topic: WebhookSubscriptionTopic!, $sub: WebhookSubscriptionInput!) {
        webhookSubscriptionCreate(topic: $topic, webhookSubscription: $sub) {
          webhookSubscription { id }
          userErrors { field message }
        }
      }`,
      { topic, sub: { callbackUrl, format: "JSON" } }
    );
    const errs = mut?.data?.webhookSubscriptionCreate?.userErrors || [];
    if (errs.length) {
      console.error(`[ensureWebhooks] ${shopDomain} ${topic} errors:`, errs);
    } else {
      created.push(topic);
      console.log(`[ensureWebhooks] ${shopDomain} created ${topic} → ${callbackUrl}`);
    }
  }

  // Record that webhooks are registered (pending first fire)
  try {
    await db.shop.upsert({
      where: { shopDomain },
      create: { shopDomain, webhooksRegisteredAt: new Date() },
      update: { webhooksRegisteredAt: new Date() },
    });
  } catch (err) {
    console.error("[ensureWebhooks] failed to record webhooksRegisteredAt:", err);
  }

  done.add(shopDomain);
  return { created, existingCount: existing.length };
}
