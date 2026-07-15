import { json } from "@remix-run/node";
import { useLoaderData, useSubmit, useNavigation } from "@remix-run/react";
import { Page, Card, Text, BlockStack, Button, Banner, InlineStack, Modal } from "@shopify/polaris";
import { useState, useCallback } from "react";
import { authenticate } from "../shopify.server";
import db from "../db.server";
import { getMetaAuthUrl, getMetaAdAccounts } from "../services/metaAuth.server";

export const loader = async ({ request }) => {
  const { session } = await authenticate.admin(request);
  const shopDomain = session.shop;

  const url = new URL(request.url);
  const appUrl = `https://${url.host}`;
  // ?reconsent=1 forces Facebook to re-show the ads_read consent dialog on an
  // already-granted account (for capturing the App Review screencast). Does not
  // revoke anything - see getMetaAuthUrl.
  const forceReconsent = url.searchParams.get("reconsent") === "1";

  const shop = await db.shop.findUnique({ where: { shopDomain } });
  const authUrl = getMetaAuthUrl(shopDomain, appUrl, { forceReconsent });

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
      // Token may be expired - show what we have from DB
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

// Disconnect nulls the stored Meta credentials so Lucidly can no longer read
// the ad account. Previously imported ad metrics (MetaInsight etc.) are left in
// place - this matches the public Data Deletion page, which states that removing
// the connection "deletes the stored access token", not the historical data.
export const action = async ({ request }) => {
  const { session } = await authenticate.admin(request);
  const shopDomain = session.shop;

  const formData = await request.formData();
  if (formData.get("intent") === "disconnect") {
    await db.shop.update({
      where: { shopDomain },
      data: { metaAccessToken: null, metaAdAccountId: null },
    });
  }

  return json({ ok: true });
};

export default function MetaConnect() {
  const { metaConnected, metaAdAccountId, metaCurrency, shopifyCurrency, accountName, authUrl } = useLoaderData();
  const submit = useSubmit();
  const navigation = useNavigation();
  const [confirmOpen, setConfirmOpen] = useState(false);

  const disconnecting =
    navigation.state !== "idle" && navigation.formData?.get("intent") === "disconnect";

  const handleConnect = () => {
    window.open(authUrl, "meta_oauth", "width=600,height=700");
  };

  const handleDisconnect = useCallback(() => {
    submit({ intent: "disconnect" }, { method: "post" });
    setConfirmOpen(false);
  }, [submit]);

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
                Ad account currency: <strong>{metaCurrency}</strong> - Shopify currency: <strong>{shopifyCurrency}</strong>
                {currencyMismatch && ` - Meta figures will be converted to ${shopifyCurrency}`}
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
            <Text as="h2" variant="headingLg">Meta Ads Account</Text>
            <Text as="p" variant="bodyMd">
              Connect your Meta Ads account to pull campaign data and match it against your Shopify orders.
              Lucidly only requests read access to your ad performance data.
            </Text>
            {metaConnected && (
              <Text as="p" variant="bodySm" tone="subdued">
                Reconnecting will let you choose a different ad account if needed.
                Disconnecting removes Lucidly's stored access token so it can no longer read your ad data.
              </Text>
            )}
            <InlineStack gap="300">
              <Button variant="primary" onClick={handleConnect}>
                {metaConnected ? "Reconnect Meta Ads" : "Connect Meta Ads"}
              </Button>
              {metaConnected && (
                <Button tone="critical" onClick={() => setConfirmOpen(true)} loading={disconnecting}>
                  Disconnect Meta Ads
                </Button>
              )}
            </InlineStack>
          </BlockStack>
        </Card>
      </BlockStack>

      <Modal
        open={confirmOpen}
        onClose={() => setConfirmOpen(false)}
        title="Disconnect Meta Ads?"
        primaryAction={{
          content: "Disconnect",
          destructive: true,
          onAction: handleDisconnect,
          loading: disconnecting,
        }}
        secondaryActions={[{ content: "Cancel", onAction: () => setConfirmOpen(false) }]}
      >
        <Modal.Section>
          <Text as="p">
            This removes Lucidly's stored Meta access token, so Lucidly can no longer
            read your ad data. Your previously imported ad metrics are kept, and you can
            reconnect at any time.
          </Text>
        </Modal.Section>
      </Modal>
    </Page>
  );
}
