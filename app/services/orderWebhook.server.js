import db from "../db.server";
import crypto from "crypto";
import { isPaidMetaUtm } from "../utils/utmClassification.js";

/**
 * Processes a single Shopify order from a webhook payload (REST format).
 * Used by both ORDERS_CREATE and ORDERS_UPDATED webhooks.
 *
 * On create: stores the order with its original total (frozenTotalPrice) for attribution matching.
 * On update: updates mutable fields (refunds, financial status, discounts) but preserves
 * frozenTotalPrice/frozenSubtotalPrice — the original values at time of purchase.
 * This ensures the matcher can always compare against the price Meta saw at conversion time.
 *
 * Future: once Web Pixel (Layer 1) is live, real-time order ingestion enables immediate
 * matching against Meta conversions before Shopify edits change the order value.
 * This eliminates the "changed order value" gap that causes unmatched attributions today.
 */

function parseUtms(landingSite) {
  if (!landingSite || !landingSite.includes("utm_")) return {};
  try {
    const url = new URL("https://x.com" + landingSite);
    return {
      utmSource: url.searchParams.get("utm_source") || "",
      utmMedium: url.searchParams.get("utm_medium") || "",
      utmCampaign: url.searchParams.get("utm_campaign") || "",
      utmContent: url.searchParams.get("utm_content") || "",
      utmTerm: url.searchParams.get("utm_term") || "",
      utmId: url.searchParams.get("utm_id") || "",
    };
  } catch { return {}; }
}

function hashEmail(email) {
  if (!email) return null;
  return crypto.createHash("sha256").update(email.toLowerCase().trim()).digest("hex");
}

function buildRefundLineItemsFromWebhook(refunds) {
  const byTitle = {};
  for (const refund of (refunds || [])) {
    for (const rli of (refund.refund_line_items || [])) {
      const li = rli.line_item || {};
      const title = li.title || "Unknown";
      const refundedAmount = parseFloat(rli.subtotal || "0");
      const originalPrice = parseFloat(li.price || "0") * (li.quantity || 1);

      if (!byTitle[title]) {
        byTitle[title] = { title, quantity: 0, refundedAmount: 0, originalPrice };
      }
      byTitle[title].quantity += rli.quantity || 0;
      byTitle[title].refundedAmount += refundedAmount;
    }
  }
  const items = Object.values(byTitle).filter(i => i.refundedAmount > 0);
  return items.length > 0 ? JSON.stringify(items) : "";
}

export async function processOrderWebhook(shopDomain, payload, isCreate) {
  // Mark first webhook fire for the shop (only if not already set)
  try {
    await db.shop.updateMany({
      where: { shopDomain, webhooksFirstFiredAt: null },
      data: { webhooksFirstFiredAt: new Date() },
    });
  } catch {}

  const shopifyOrderId = String(payload.id);
  const totalPrice = parseFloat(payload.total_price || "0");
  const subtotalPrice = parseFloat(payload.subtotal_price || "0");
  const currency = payload.currency || "GBP";

  // Channel detection
  const sourceName = (payload.source_name || "").toLowerCase();
  const isOnlineStore = sourceName === "web" || sourceName === "shopify_draft_order" || sourceName === "";
  const channelName = sourceName === "web" ? "online_store"
    : sourceName === "pos" ? "pos"
    : sourceName || "unknown";

  // Address fields — billing takes priority (better indicator of where customer lives)
  const shipping = payload.shipping_address || {};
  const billing = payload.billing_address || {};
  const country = billing.country || shipping.country || "";
  const countryCode = billing.country_code || shipping.country_code || "";
  const city = billing.city || shipping.city || "";
  const regionCode = billing.province_code || shipping.province_code || "";

  // Line items
  const lineItems = (payload.line_items || []).map(li => li.title).join(", ");
  const productSkus = [...new Set((payload.line_items || []).map(li => li.sku).filter(Boolean))].join(", ");
  // Collections not available in webhook payload — populated by full sync

  // Landing page & UTMs
  const landingSite = payload.landing_site || "";
  const referringSite = payload.referring_site || "";
  const utmParams = parseUtms(landingSite);

  // UTM classification — is this a paid Meta ad click?
  const utmConfirmedMeta = isPaidMetaUtm(utmParams.utmSource, utmParams.utmMedium);

  // Discount codes
  const discountCodes = (payload.discount_codes || []).map(d => d.code || d).join(", ");

  // Refunds
  const totalRefunded = (payload.refunds || []).reduce((sum, r) => {
    const txns = r.transactions || [];
    return sum + txns.reduce((s, t) => s + parseFloat(t.amount || "0"), 0);
  }, 0);
  const refundStatus = totalRefunded === 0 ? "none"
    : totalRefunded >= totalPrice ? "full" : "partial";
  const refundLineItems = buildRefundLineItemsFromWebhook(payload.refunds);

  // Customer
  const customer = payload.customer || {};
  const customerId = customer.id ? String(customer.id) : null;
  const customerFirstName = customer.first_name || "";
  const customerLastInitial = customer.last_name ? customer.last_name.charAt(0) : "";

  // Financial status
  const financialStatus = payload.financial_status || null;

  // For create: freeze the original price. For update: preserve the frozen price.
  const existingOrder = await db.order.findUnique({
    where: { shopDomain_shopifyOrderId: { shopDomain, shopifyOrderId } },
    select: { frozenTotalPrice: true, frozenSubtotalPrice: true, customerFirstName: true, customerLastInitial: true, productCollections: true },
  });

  const frozenTotalPrice = existingOrder?.frozenTotalPrice ?? totalPrice;
  const frozenSubtotalPrice = existingOrder?.frozenSubtotalPrice ?? subtotalPrice;
  // Preserve existing customer name if already populated (protected data workaround)
  const finalFirstName = customerFirstName || existingOrder?.customerFirstName || "";
  const finalLastInitial = customerLastInitial || existingOrder?.customerLastInitial || "";
  // Preserve collections from full sync (not available in webhooks)
  const productCollections = existingOrder?.productCollections || "";

  // Determine if new customer — check if this is their first order in our DB
  let isNewCustomerOrder = null;
  if (customerId) {
    const priorOrders = await db.order.count({
      where: { shopDomain, shopifyCustomerId: customerId, shopifyOrderId: { not: shopifyOrderId } },
    });
    isNewCustomerOrder = priorOrders === 0;
  }

  await db.order.upsert({
    where: { shopDomain_shopifyOrderId: { shopDomain, shopifyOrderId } },
    create: {
      shopDomain, shopifyOrderId, shopifyCustomerId: customerId,
      orderNumber: payload.name || null,
      createdAt: new Date(payload.created_at),
      totalPrice, subtotalPrice, currency,
      financialStatus, channelName, isOnlineStore,
      frozenTotalPrice: totalPrice, frozenSubtotalPrice: subtotalPrice,
      isNewCustomerOrder,
      country, countryCode, city, regionCode,
      customerFirstName: finalFirstName, customerLastInitial: finalLastInitial,
      lineItems, productSkus, productCollections,
      discountCodes, refundStatus, totalRefunded, refundLineItems,
      landingSite, referringSite,
      ...utmParams, utmConfirmedMeta,
    },
    update: {
      totalPrice, subtotalPrice,
      financialStatus,
      // Do NOT update frozenTotalPrice/frozenSubtotalPrice — preserve original values
      discountCodes, refundStatus, totalRefunded, refundLineItems,
      // Update address if it was previously empty
      ...(country ? { country, countryCode, city, regionCode } : {}),
      customerFirstName: finalFirstName, customerLastInitial: finalLastInitial,
      lineItems, productSkus,
      // Only set landing/UTM data if not already populated (preserve original from create)
      ...(landingSite ? { landingSite, referringSite, ...utmParams, utmConfirmedMeta } : {}),
    },
  });

  // Upsert customer
  if (customerId) {
    const emailHash = hashEmail(customer.email);
    const orderDate = new Date(payload.created_at);

    await db.customer.upsert({
      where: { shopDomain_shopifyCustomerId: { shopDomain, shopifyCustomerId: customerId } },
      create: {
        shopDomain, shopifyCustomerId: customerId, emailHash,
        firstOrderDate: orderDate, isNewCustomer: true,
      },
      update: {
        emailHash,
        // Only update firstOrderDate if this order is earlier
        ...(isNewCustomerOrder ? { firstOrderDate: orderDate } : {}),
      },
    });
  }

  // Set customerOrderCountAtPurchase for this order.
  // For webhooks, customer.orders_count IS the correct count at time of purchase.
  if (customerId && isCreate) {
    const ordersCount = customer.orders_count || null;
    if (ordersCount) {
      await db.order.update({
        where: { shopDomain_shopifyOrderId: { shopDomain, shopifyOrderId } },
        data: { customerOrderCountAtPurchase: ordersCount },
      });
    }
    // Update customer's totalOrders for future reference
    await db.customer.update({
      where: { shopDomain_shopifyCustomerId: { shopDomain, shopifyCustomerId: customerId } },
      data: { totalOrders: ordersCount || 0 },
    }).catch(() => {}); // Customer may not exist yet if upsert above failed
  }

  const action = isCreate ? "Created" : "Updated";
  console.log(`[OrderWebhook] ${action} order ${shopifyOrderId} for ${shopDomain} (${currency} ${totalPrice}, refunded: ${totalRefunded})`);
}
