import { json } from "@remix-run/node";
import { useLoaderData } from "@remix-run/react";
import { Page, Card, Text, BlockStack, Button, Banner, InlineStack } from "@shopify/polaris";
import { authenticate } from "../shopify.server";
import db from "../db.server";
import { getMetaAuthUrl, getMetaAdAccounts } from "../services/metaAuth.server";

export const loader = async ({ request }) => {
  const { session } = await authenticate.admin(request);
  const shopDomain = session.shop;

  const url = new URL(request.url);
  const appUrl = `https://${url.host}`;

  const shop = await db.shop.findUnique({ where: { shopDomain } });
  const authUrl = getMetaAuthUrl(shopDomain, appUrl);

  // If connected, fetch current account name from Meta API
  let accountName = null;
  let accountCurrency = null;
  if (shop?.metaAccessToken && shop?.metaAdAccountId) {
    try {
      const accounts = await getMetaAdAccounts(shop.metaAccessToken);
      const current = accounts.find(a => a.id === shop.metaAdAccountId);
      if (current) {
        accountName = current.name;
        accountCurrency = current.currency;
      }
    } catch {
      // Token may be expired — show what we have from DB
      accountCurrency = shop.metaCurrency;
    }
  }

  return json({
    shopDomain,
    metaConnected: !!shop?.metaAccessToken && !!shop?.metaAdAccountId,
    metaAdAccountId: shop?.metaAdAccountId || null,
    metaCurrency: accountCurrency || shop?.metaCurrency || null,
    shopifyCurrency: shop?.shopifyCurrency || "GBP",
    accountName,
    authUrl,
  });
};

export default function MetaConnect() {
  const { metaConnected, metaAdAccountId, metaCurrency, shopifyCurrency, accountName, authUrl } = useLoaderData();

  const handleConnect = () => {
    window.open(authUrl, "meta_oauth", "width=600,height=700");
  };

  const currencyMismatch = metaConnected && metaCurrency && shopifyCurrency && metaCurrency !== shopifyCurrency;

  return (
    <Page title="Connect Meta Ads" backAction={{ url: "/app" }}>
      <BlockStack gap="500">
        {metaConnected ? (
          <Banner tone="success">
            <BlockStack gap="200">
              <Text as="p" fontWeight="semibold">Meta Ads connected</Text>
              <Text as="p" variant="bodySm">
                Account: <strong>{accountName || metaAdAccountId}</strong> ({metaAdAccountId})
              </Text>
              <Text as="p" variant="bodySm">
                Ad account currency: <strong>{metaCurrency}</strong> — Shopify currency: <strong>{shopifyCurrency}</strong>
                {currencyMismatch && ` — Meta figures will be converted to ${shopifyCurrency}`}
              </Text>
            </BlockStack>
          </Banner>
        ) : (
          <Banner tone="warning">
            <p>Meta Ads is not connected yet. Connect your account to start attribution.</p>
          </Banner>
        )}

        <Card>
          <BlockStack gap="300">
            <Text as="h2" variant="headingMd">Meta Ads Account</Text>
            <Text as="p" variant="bodyMd">
              Connect your Meta Ads account to pull campaign data and match it against your Shopify orders.
              Lucidly only requests read access to your ad performance data.
            </Text>
            {metaConnected && (
              <Text as="p" variant="bodySm" tone="subdued">
                Reconnecting will let you choose a different ad account if needed.
              </Text>
            )}
            <InlineStack gap="300">
              <Button variant="primary" onClick={handleConnect}>
                {metaConnected ? "Reconnect Meta Ads" : "Connect Meta Ads"}
              </Button>
            </InlineStack>
          </BlockStack>
        </Card>
      </BlockStack>
    </Page>
  );
}
