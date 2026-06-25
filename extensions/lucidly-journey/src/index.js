import { register } from "@shopify/web-pixels-extension";

// Meta ad sources (mirrors app/utils/utmClassification.js META_SOURCES). We
// capture a touch for any of these OR any landing carrying an fbclid - the
// server makes the final paid-vs-organic call. Kept deliberately broad here so
// the journey isn't missing Meta touches; volume stays low because untagged
// pageviews never post.
const META_SOURCES = ["facebook", "fb", "ig", "instagram", "meta", "facebook-sitelink"];

register(({ analytics, settings }) => {
  const shop = settings.shop;
  const ingestUrl = settings.ingestUrl;
  const token = settings.token;
  // Without all three we can't authenticate the post - bail rather than spray
  // unauthenticated junk at the endpoint.
  if (!shop || !ingestUrl || !token) return;

  function send(payload) {
    try {
      // text/plain (not application/json) + no custom headers keeps this a
      // CORS "simple request": no OPTIONS preflight. keepalive lets the
      // checkout_completed post survive the page unloading to the order page.
      fetch(ingestUrl, {
        method: "POST",
        headers: { "Content-Type": "text/plain" },
        body: JSON.stringify(payload),
        keepalive: true,
      }).catch(() => {});
    } catch (_e) {
      // Pixel must never throw into the storefront.
    }
  }

  function parseParams(href) {
    try {
      const u = new URL(href);
      const q = u.searchParams;
      return {
        source: (q.get("utm_source") || "").toLowerCase().trim(),
        medium: (q.get("utm_medium") || "").toLowerCase().trim(),
        campaign: q.get("utm_campaign") || "",
        content: q.get("utm_content") || "",
        term: q.get("utm_term") || "",
        fbclid: q.get("fbclid") || "",
        path: u.pathname || "",
      };
    } catch (_e) {
      return null;
    }
  }

  // Every ad-referred storefront landing → one touch.
  analytics.subscribe("page_viewed", (event) => {
    const href = event && event.context && event.context.document &&
      event.context.document.location && event.context.document.location.href;
    const clientId = event && event.clientId;
    if (!href || !clientId) return;

    const p = parseParams(href);
    if (!p) return;
    const isMeta = !!p.fbclid || META_SOURCES.indexOf(p.source) !== -1;
    if (!isMeta) return;

    send({
      type: "touch",
      shop: shop,
      token: token,
      clientId: clientId,
      occurredAt: event.timestamp,
      source: p.source,
      medium: p.medium,
      campaign: p.campaign,
      content: p.content,
      term: p.term,
      fbclid: p.fbclid,
      landingPath: p.path,
      rawUrl: href,
    });
  });

  // Purchase → link this visitor (clientId) to the order so the server can
  // stitch their touch history onto it later.
  analytics.subscribe("checkout_completed", (event) => {
    const clientId = event && event.clientId;
    if (!clientId) return;
    const checkout = event && event.data && event.data.checkout;
    const orderId = checkout && checkout.order && checkout.order.id;

    send({
      type: "order",
      shop: shop,
      token: token,
      clientId: clientId,
      orderId: orderId ? String(orderId) : "",
      checkoutToken: (checkout && checkout.token) || "",
      occurredAt: event.timestamp,
    });
  });
});
