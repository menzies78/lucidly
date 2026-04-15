import "dotenv/config";
import db from "../db.server";
import { exchangeMetaCode, getMetaAdAccounts, getMetaAttributionWindow } from "../services/metaAuth.server";

function htmlResponse(body) {
  return new Response(`<html><head><meta charset="utf-8"><style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #F6F6F7; color: #1F2937; padding: 24px; }
    h1 { font-size: 20px; font-weight: 600; margin-bottom: 4px; }
    .subtitle { color: #6B7280; font-size: 14px; margin-bottom: 20px; }
    .card { background: #fff; border: 1px solid #E4E5E7; border-radius: 12px; padding: 16px 20px; margin-bottom: 12px; cursor: pointer; transition: all 0.15s; display: block; text-decoration: none; color: inherit; width: 100%; }
    .card:hover { border-color: #7C3AED; box-shadow: 0 0 0 1px #7C3AED; }
    .card-name { font-size: 16px; font-weight: 600; margin-bottom: 4px; }
    .card-detail { font-size: 13px; color: #6B7280; }
    .card-detail span { display: inline-block; margin-right: 16px; }
    .card-currency { display: inline-block; background: #F3F0FF; color: #7C3AED; font-weight: 600; font-size: 12px; padding: 2px 8px; border-radius: 4px; margin-left: 8px; }
    .card-status { display: inline-block; font-size: 11px; padding: 2px 8px; border-radius: 4px; font-weight: 500; }
    .status-active { background: #ECFDF5; color: #059669; }
    .status-inactive { background: #FEF2F2; color: #DC2626; }
    .success { background: #ECFDF5; border: 1px solid #A7F3D0; border-radius: 12px; padding: 20px; }
    .success h1 { color: #059669; }
    .error { background: #FEF2F2; border: 1px solid #FECACA; border-radius: 12px; padding: 20px; }
    .error h1 { color: #DC2626; }
    .detail-row { font-size: 14px; margin-top: 8px; }
    .detail-row strong { min-width: 100px; display: inline-block; }
  </style></head><body>${body}</body></html>`, {
    headers: { "Content-Type": "text/html" },
  });
}

function errorResponse(message) {
  return htmlResponse(`
    <div class="error">
      <h1>Connection Failed</h1>
      <p style="margin-top:8px">${message}</p>
      <p style="margin-top:12px;color:#6B7280;font-size:13px">You can close this window and try again.</p>
    </div>
  `);
}

function successResponse(adAccount, attributionWindow) {
  return htmlResponse(`
    <div class="success">
      <h1>Meta Ads Connected!</h1>
      <div class="detail-row"><strong>Account:</strong> ${adAccount.name} (${adAccount.id})</div>
      <div class="detail-row"><strong>Currency:</strong> ${adAccount.currency}</div>
      <div class="detail-row"><strong>Timezone:</strong> ${adAccount.timezone_name}</div>
      <div class="detail-row"><strong>Attribution:</strong> ${attributionWindow}</div>
      <p style="margin-top:12px;color:#6B7280;font-size:13px">You can close this window and return to Shopify.</p>
    </div>
    <script>setTimeout(()=>window.close(),2000)</script>
    <p style="margin-top:12px;color:#6B7280;font-size:13px">This window will close automatically.</p>
  `);
}

function accountSelectorResponse(adAccounts, shopDomain) {
  const state = Buffer.from(JSON.stringify({ shopDomain })).toString("base64");
  const statusLabel = (s) => s === 1
    ? '<span class="card-status status-active">Active</span>'
    : '<span class="card-status status-inactive">Inactive</span>';

  const cards = adAccounts.map(a => `
    <a class="card" href="?select=${encodeURIComponent(a.id)}&shop=${encodeURIComponent(state)}">
      <div class="card-name">${a.name} ${statusLabel(a.account_status)}<span class="card-currency">${a.currency}</span></div>
      <div class="card-detail">
        <span>ID: ${a.id}</span>
        <span>Timezone: ${a.timezone_name || "UTC"}</span>
      </div>
    </a>
  `).join("");

  return htmlResponse(`
    <h1>Select an Ad Account</h1>
    <p class="subtitle">Your Meta profile has access to ${adAccounts.length} ad accounts. Choose the one for this Shopify store.</p>
    ${cards}
  `);
}

export const loader = async ({ request }) => {
  const url = new URL(request.url);

  // ── Step 2: User selected an account from the list ──
  const selectedAccount = url.searchParams.get("select");
  const shopState = url.searchParams.get("shop");

  if (selectedAccount && shopState) {
    try {
      const { shopDomain } = JSON.parse(Buffer.from(shopState, "base64").toString());
      const shop = await db.shop.findUnique({ where: { shopDomain } });

      if (!shop?.metaAccessToken) {
        return errorResponse("Session expired. Please close this window and reconnect Meta.");
      }

      // Fetch account details using the stored token
      const adAccounts = await getMetaAdAccounts(shop.metaAccessToken);
      const adAccount = adAccounts.find(a => a.id === selectedAccount);

      if (!adAccount) {
        return errorResponse(`Ad account ${selectedAccount} not found. It may have been removed from your Meta profile.`);
      }

      console.log(`[Meta OAuth] User selected: ${adAccount.name} (${adAccount.id}, ${adAccount.currency})`);

      const attributionWindow = await getMetaAttributionWindow(adAccount.id, shop.metaAccessToken);

      await db.shop.update({
        where: { shopDomain },
        data: {
          metaAdAccountId: adAccount.id,
          metaAccountTimezone: adAccount.timezone_name || "UTC",
          metaCurrency: adAccount.currency || "USD",
          metaAttributionWindow: attributionWindow,
        },
      });

      console.log(`[Meta OAuth] Connected ${shopDomain} to ${adAccount.id} (${adAccount.currency})`);
      return successResponse(adAccount, attributionWindow);
    } catch (err) {
      console.error("[Meta OAuth] Account selection error:", err);
      return errorResponse(err.message);
    }
  }

  // ── Step 1: OAuth callback from Meta ──
  const code = url.searchParams.get("code");
  const state = url.searchParams.get("state");
  const error = url.searchParams.get("error");

  if (error || !code || !state) {
    return errorResponse("Meta connection failed. You can close this window.");
  }

  try {
    const { shopDomain } = JSON.parse(Buffer.from(state, "base64").toString());
    const appUrl = `https://${url.host}`;

    console.log("[Meta OAuth] Exchanging code for token...");
    const accessToken = await exchangeMetaCode(code, appUrl);

    console.log("[Meta OAuth] Fetching ad accounts...");
    const adAccounts = await getMetaAdAccounts(accessToken);

    if (adAccounts.length === 0) {
      return errorResponse("No ad accounts found on your Meta profile.");
    }

    // Save token immediately so it's available for account selection
    await db.shop.upsert({
      where: { shopDomain },
      create: { shopDomain, metaAccessToken: accessToken },
      update: { metaAccessToken: accessToken },
    });

    // Single account → auto-select
    if (adAccounts.length === 1) {
      const adAccount = adAccounts[0];

      console.log(`[Meta OAuth] Single account: ${adAccount.name} (${adAccount.id}, ${adAccount.currency})`);

      const attributionWindow = await getMetaAttributionWindow(adAccount.id, accessToken);

      await db.shop.update({
        where: { shopDomain },
        data: {
          metaAdAccountId: adAccount.id,
          metaAccountTimezone: adAccount.timezone_name || "UTC",
          metaCurrency: adAccount.currency || "USD",
          metaAttributionWindow: attributionWindow,
        },
      });

      console.log(`[Meta OAuth] Connected ${shopDomain} to ${adAccount.id}`);
      return successResponse(adAccount, attributionWindow);
    }

    // Multiple accounts → show selector
    console.log(`[Meta OAuth] ${adAccounts.length} ad accounts found, showing selector`);
    return accountSelectorResponse(adAccounts, shopDomain);

  } catch (err) {
    console.error("[Meta OAuth] Error:", err);
    return errorResponse(err.message);
  }
};
