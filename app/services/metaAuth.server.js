import "dotenv/config";

const META_APP_ID = process.env.META_APP_ID;
const META_APP_SECRET = process.env.META_APP_SECRET;

if (!META_APP_ID || !META_APP_SECRET) {
  console.error(
    "[metaAuth] META_APP_ID and META_APP_SECRET env vars are required. " +
    "OAuth flows will fail until these are set."
  );
}

export function getMetaAuthUrl(shopDomain, appUrl) {
  const redirectUri = `${appUrl}/meta/callback`;
  const state = Buffer.from(JSON.stringify({ shopDomain })).toString("base64");
  const scopes = "ads_read,ads_management";

  return `https://www.facebook.com/v21.0/dialog/oauth?client_id=${META_APP_ID}&redirect_uri=${encodeURIComponent(redirectUri)}&scope=${scopes}&state=${state}&response_type=code`;
}

export async function exchangeMetaCode(code, appUrl) {
  const redirectUri = `${appUrl}/meta/callback`;

  const tokenUrl = `https://graph.facebook.com/v21.0/oauth/access_token?client_id=${META_APP_ID}&redirect_uri=${encodeURIComponent(redirectUri)}&client_secret=${META_APP_SECRET}&code=${code}`;

  const response = await fetch(tokenUrl);
  const data = await response.json();

  if (data.error) {
    throw new Error(`Meta OAuth error: ${data.error.message}`);
  }

  const longLivedUrl = `https://graph.facebook.com/v21.0/oauth/access_token?grant_type=fb_exchange_token&client_id=${META_APP_ID}&client_secret=${META_APP_SECRET}&fb_exchange_token=${data.access_token}`;

  const longLivedResponse = await fetch(longLivedUrl);
  const longLivedData = await longLivedResponse.json();

  if (longLivedData.error) {
    throw new Error(`Meta long-lived token error: ${longLivedData.error.message}`);
  }

  return longLivedData.access_token;
}

export async function getMetaAdAccounts(accessToken) {
  const url = `https://graph.facebook.com/v21.0/me/adaccounts?fields=id,name,currency,timezone_name,account_status&access_token=${accessToken}`;

  const response = await fetch(url);
  const data = await response.json();

  if (data.error) {
    throw new Error(`Meta API error: ${data.error.message}`);
  }

  return data.data || [];
}

export async function getMetaAttributionWindow(adAccountId, accessToken) {
  // Query recent campaign insights to detect the attribution setting in use
  const url = `https://graph.facebook.com/v21.0/${adAccountId}/insights?fields=attribution_setting&date_preset=last_7d&limit=1&access_token=${accessToken}`;

  try {
    const response = await fetch(url);
    const data = await response.json();

    if (data.data && data.data.length > 0 && data.data[0].attribution_setting) {
      return data.data[0].attribution_setting;
    }
  } catch (e) {
    console.log("[Meta OAuth] Could not detect attribution window, using default");
  }

  return "7d_click_1d_view";
}
