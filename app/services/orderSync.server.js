import db from "../db.server";
import crypto from "crypto";
import { setProgress, completeProgress, failProgress } from "./progress.server";
import { isPaidMetaUtm } from "../utils/utmClassification.js";
import { parseElevarVisitorInfo } from "../utils/parseElevarVisitorInfo.js";
import { withRetry } from "./retry.server.js";
import { backfillShopInferredGender } from "./nameGender.server.js";

// Wraps admin.graphql with retry on transient errors.
async function graphqlWithRetry(admin, query, variables, label) {
  return withRetry(async () => {
    const res = await admin.graphql(query, variables ? { variables } : undefined);
    const data = await res.json();
    if (data.errors && data.errors.length) {
      throw new Error(`Shopify GraphQL errors: ${JSON.stringify(data.errors).slice(0, 300)}`);
    }
    return data;
  }, label);
}

function hashEmail(email) {
  if (!email) return null;
  return crypto.createHash("sha256").update(email.toLowerCase().trim()).digest("hex");
}

function extractCollections(lineItems) {
  const collections = new Set();
  for (const edge of (lineItems?.edges || [])) {
    for (const col of (edge.node.product?.collections?.edges || [])) {
      if (col.node?.title) collections.add(col.node.title);
    }
  }
  return Array.from(collections).join(", ");
}

function extractSkus(lineItems) {
  const skus = [];
  for (const edge of (lineItems?.edges || [])) {
    if (edge.node.sku) skus.push(edge.node.sku);
  }
  return [...new Set(skus)].join(", ");
}

// Build per-row OrderLineItem records from a GraphQL order. Revenue precision:
// `unitPrice` is the post-discount unit price, `totalPrice = unitPrice * qty`.
// Refund amounts / quantities are matched against the per-title refund payload
// we already parse for `Order.refundLineItems`. Title matching is imperfect
// when one product appears on multiple lines of the same order — that's an
// accepted precision loss, same as today's per-product refund attribution.
function buildLineItemRowsFromGraphQL(shopDomain, shopifyOrderId, orderLineItems, refunds) {
  // Accumulate refund totals per title, across all refunds on the order.
  const refundByTitle = {};
  for (const refund of (refunds || [])) {
    for (const edge of (refund.refundLineItems?.edges || [])) {
      const node = edge.node;
      const title = node.lineItem?.title || "Unknown";
      const qty = node.quantity || 0;
      const amount = parseFloat(node.subtotalSet?.shopMoney?.amount || "0");
      if (!refundByTitle[title]) refundByTitle[title] = { quantity: 0, amount: 0 };
      refundByTitle[title].quantity += qty;
      refundByTitle[title].amount += amount;
    }
  }
  // Titles consumed so a second line-item with the same title gets whatever
  // refund slack remains (rare; good enough given the imperfect title match).
  const remainingRefund = Object.fromEntries(
    Object.entries(refundByTitle).map(([k, v]) => [k, { quantity: v.quantity, amount: v.amount }]),
  );
  const rows = [];
  for (const edge of (orderLineItems?.edges || [])) {
    const node = edge.node;
    const title = node.title || "";
    const quantity = node.quantity || 1;
    const unitPrice = parseFloat(node.discountedUnitPriceSet?.shopMoney?.amount || "0");
    const originalUnit = parseFloat(node.originalUnitPriceSet?.shopMoney?.amount || "0");
    const totalPrice = unitPrice * quantity;
    const totalDiscount = Math.max(0, (originalUnit - unitPrice) * quantity);
    const shopifyLineItemId = node.id ? node.id.replace("gid://shopify/LineItem/", "") : null;
    const refund = remainingRefund[title];
    let refundedQuantity = 0;
    let refundedAmount = 0;
    if (refund) {
      refundedQuantity = Math.min(refund.quantity, quantity);
      // Allocate refund amount proportional to the share of refunded quantity
      // against the remaining refund quantity, so two lines with the same
      // title don't double-claim the same £.
      const share = refund.quantity > 0 ? refundedQuantity / refund.quantity : 0;
      refundedAmount = refund.amount * share;
      refund.quantity -= refundedQuantity;
      refund.amount -= refundedAmount;
    }
    rows.push({
      shopDomain,
      shopifyOrderId,
      shopifyLineItemId,
      title,
      sku: node.sku || "",
      quantity,
      unitPrice,
      totalPrice,
      totalDiscount,
      refundedQuantity,
      refundedAmount,
    });
  }
  return rows;
}

function buildRefundLineItems(refunds) {
  const byTitle = {};
  for (const refund of (refunds || [])) {
    for (const edge of (refund.refundLineItems?.edges || [])) {
      const node = edge.node;
      const title = node.lineItem?.title || "Unknown";
      const refundedAmount = parseFloat(node.subtotalSet?.shopMoney?.amount || "0");
      const originalUnitPrice = parseFloat(node.lineItem?.originalUnitPriceSet?.shopMoney?.amount || "0");
      const originalQty = node.lineItem?.quantity || 1;
      const originalPrice = originalUnitPrice * originalQty;

      if (!byTitle[title]) {
        byTitle[title] = { title, quantity: 0, refundedAmount: 0, originalPrice };
      }
      byTitle[title].quantity += node.quantity || 0;
      byTitle[title].refundedAmount += refundedAmount;
    }
  }
  const items = Object.values(byTitle).filter(i => i.refundedAmount > 0);
  return items.length > 0 ? JSON.stringify(items) : "";
}

async function computeOrderCounts(shopDomain, customerIdsToUpdate = null) {
  // Scope: full sweep (initial backfill) or delta (hourly incremental).
  // When customerIdsToUpdate is provided, only those customers are recomputed.
  let customerIds;
  if (customerIdsToUpdate && customerIdsToUpdate.length > 0) {
    customerIds = customerIdsToUpdate.map(id => ({ shopifyCustomerId: id }));
  } else if (customerIdsToUpdate && customerIdsToUpdate.length === 0) {
    console.log(`[OrderSync] computeOrderCounts: no customers touched, skipping`);
    return;
  } else {
    // Full sweep
    customerIds = await db.order.findMany({
      where: { shopDomain, shopifyCustomerId: { not: null } },
      select: { shopifyCustomerId: true },
      distinct: ["shopifyCustomerId"],
    });
  }

  // Build customer total lookup — only for the customers we're processing
  const customers = await db.customer.findMany({
    where: {
      shopDomain,
      ...(customerIdsToUpdate ? { shopifyCustomerId: { in: customerIdsToUpdate } } : {}),
    },
    select: { shopifyCustomerId: true, totalOrders: true },
  });
  const customerTotals = {};
  for (const c of customers) customerTotals[c.shopifyCustomerId] = c.totalOrders || 0;

  let totalUpdated = 0;
  const BATCH = 500;

  for (let b = 0; b < customerIds.length; b += BATCH) {
    const batch = customerIds.slice(b, b + BATCH);
    const custIdList = batch.map(c => c.shopifyCustomerId);

    // Load orders for this batch of customers
    const orders = await db.order.findMany({
      where: { shopDomain, shopifyCustomerId: { in: custIdList } },
      orderBy: { createdAt: "asc" },
      select: { id: true, shopifyCustomerId: true },
    });

    // Group by customer
    const byCustomer = {};
    for (const o of orders) {
      if (!byCustomer[o.shopifyCustomerId]) byCustomer[o.shopifyCustomerId] = [];
      byCustomer[o.shopifyCustomerId].push(o);
    }

    // Build all updates for this batch
    const updates = [];
    for (const [custId, custOrders] of Object.entries(byCustomer)) {
      const shopifyTotal = customerTotals[custId] || 0;
      for (let i = 0; i < custOrders.length; i++) {
        const count = shopifyTotal >= custOrders.length
          ? shopifyTotal - (custOrders.length - 1 - i)
          : i + 1;
        updates.push({ id: custOrders[i].id, count });
      }
    }

    // Batch update in chunks of 200
    for (let i = 0; i < updates.length; i += 200) {
      const chunk = updates.slice(i, i + 200);
      await Promise.all(
        chunk.map(u => db.order.update({
          where: { id: u.id },
          data: { customerOrderCountAtPurchase: u.count },
        }))
      );
    }
    totalUpdated += updates.length;
    setProgress(`syncOrders:${shopDomain}`, {
      status: "running",
      current: totalUpdated,
      total: customerIds.length * 2, // rough estimate (orders > customers)
      message: `Step 2 of 2 — Order counts: ${totalUpdated.toLocaleString()} orders processed (${Math.min(b + BATCH, customerIds.length).toLocaleString()}/${customerIds.length.toLocaleString()} customers)`,
    });
  }

  console.log(`[OrderSync] Computed order counts for ${totalUpdated} orders across ${customerIds.length} customers`);
}

export async function syncOrders(admin, shopDomain) {
  console.log(`[OrderSync] Starting sync for ${shopDomain}`);

  // Auto-detect shop currency and timezone from Shopify
  try {
    const shopInfoData = await graphqlWithRetry(
      admin,
      `{ shop { currencyCode ianaTimezone } }`,
      null,
      "OrderSync/shopInfo"
    );
    const detectedCurrency = shopInfoData.data?.shop?.currencyCode;
    const detectedTimezone = shopInfoData.data?.shop?.ianaTimezone;
    if (detectedCurrency || detectedTimezone) {
      const updateData = {};
      if (detectedCurrency) updateData.shopifyCurrency = detectedCurrency;
      if (detectedTimezone) updateData.shopifyTimezone = detectedTimezone;
      await db.shop.upsert({
        where: { shopDomain },
        create: { shopDomain, ...updateData },
        update: updateData,
      });
      console.log(`[OrderSync] Detected shop currency: ${detectedCurrency}, timezone: ${detectedTimezone}`);
    }
  } catch (err) {
    console.warn(`[OrderSync] Could not detect shop settings:`, err.message);
  }

  const shop = await db.shop.findUnique({ where: { shopDomain } });
  const twoYearsAgo = new Date();
  twoYearsAgo.setUTCFullYear(twoYearsAgo.getUTCFullYear() - 2);
  const sinceDate = shop?.lastOrderSync || twoYearsAgo;
  console.log(`[OrderSync] sinceDate=${sinceDate.toISOString()}, lastOrderSync=${shop?.lastOrderSync || 'null'}`);

  let hasNextPage = true;
  let cursor = null;
  let totalImported = 0;
  let totalCustomers = 0;
  let pageCount = 0;
  const touchedCustomerIds = new Set();
  // Detect whether this is an initial backfill (lastOrderSync was null).
  // Full backfills should do a shop-wide recompute; incremental runs should scope to touched customers only.
  const isInitialBackfill = !shop?.lastOrderSync;

  while (hasNextPage) {
    const query = `
      query GetOrders($cursor: String) {
        orders(first: 50, after: $cursor, sortKey: CREATED_AT, query: "created_at:>='${sinceDate.toISOString()}' status:any") {
          edges {
            cursor
            node {
              id
              name
              createdAt
              totalPriceSet { shopMoney { amount currencyCode } }
              subtotalPriceSet { shopMoney { amount } }
              displayFinancialStatus
              channelInformation { channelDefinition { handle } }
              discountCodes
              refunds {
                totalRefundedSet { shopMoney { amount } }
                refundLineItems(first: 50) {
                  edges {
                    node {
                      quantity
                      subtotalSet { shopMoney { amount } }
                      lineItem {
                        title
                        originalUnitPriceSet { shopMoney { amount } }
                        quantity
                      }
                    }
                  }
                }
              }
              billingAddress { country countryCode city provinceCode firstName }
              shippingAddress { country countryCode city provinceCode }
              lineItems(first: 50) {
                edges {
                  node {
                    id
                    title
                    quantity
                    sku
                    originalUnitPriceSet { shopMoney { amount } }
                    discountedUnitPriceSet { shopMoney { amount } }
                    product {
                      collections(first: 5) {
                        edges { node { title } }
                      }
                    }
                  }
                }
              }
              customerJourneySummary {
                firstVisit {
                  landingPage
                  referrerUrl
                  source
                  sourceType
                  utmParameters {
                    source
                    medium
                    campaign
                    content
                    term
                  }
                }
              }
              customAttributes {
                key
                value
              }
              customer {
                id
                email
                numberOfOrders
                orders(first: 1, sortKey: CREATED_AT) {
                  edges { node { id } }
                }
              }
            }
          }
          pageInfo { hasNextPage }
        }
      }
    `;

    const data = await graphqlWithRetry(admin, query, { cursor }, `OrderSync/GetOrders page ${pageCount + 1}`);
    pageCount++;
    const edges = data.data.orders.edges;
    if (pageCount === 1 && edges.length > 0) {
      console.log(`[OrderSync] First order date: ${edges[0].node.createdAt}`);
    }
    if (!data.data.orders.pageInfo.hasNextPage || edges.length === 0) {
      console.log(`[OrderSync] Last page ${pageCount}: ${edges.length} orders, hasNextPage=${data.data.orders.pageInfo.hasNextPage}`);
      if (edges.length > 0) console.log(`[OrderSync] Last order date: ${edges[edges.length - 1].node.createdAt}`);
    }

    for (const edge of edges) {
      const order = edge.node;
      cursor = edge.cursor;

      const shopifyOrderId = order.id.replace("gid://shopify/Order/", "");
      const totalPrice = parseFloat(order.totalPriceSet.shopMoney.amount);
      const subtotalPrice = parseFloat(order.subtotalPriceSet.shopMoney.amount);
      const currency = order.totalPriceSet.shopMoney.currencyCode;
      const channelHandle = order.channelInformation?.channelDefinition?.handle || "unknown";
      const isOnlineStore = channelHandle === "online_store" || channelHandle === "web" || channelHandle === "unknown";

      const shipping = order.shippingAddress;
      const billing = order.billingAddress;
      const country = billing?.country || shipping?.country || "";
      const countryCode = billing?.countryCode || shipping?.countryCode || "";
      const city = billing?.city || shipping?.city || "";
      const regionCode = billing?.provinceCode || shipping?.provinceCode || "";

      const lineItemTitles = (order.lineItems?.edges || []).map(e => e.node.title).join(", ");
      const productSkus = extractSkus(order.lineItems);
      const productCollections = extractCollections(order.lineItems);

      // Landing page & UTMs from customer journey
      const journey = order.customerJourneySummary?.firstVisit;
      const landingSite = journey?.landingPage || "";
      const referringSite = journey?.referrerUrl || "";
      let utmSource = journey?.utmParameters?.source || "";
      let utmMedium = journey?.utmParameters?.medium || "";
      let utmCampaign = journey?.utmParameters?.campaign || "";
      let utmContent = journey?.utmParameters?.content || "";
      let utmTerm = journey?.utmParameters?.term || "";
      // utm_id is a custom param not in Shopify's utmParameters — extract from landing page URL
      let utmId = "";
      if (landingSite && landingSite.includes("utm_id=")) {
        try {
          const url = new URL("https://x.com" + landingSite);
          utmId = url.searchParams.get("utm_id") || "";
        } catch {}
      }

      // Prefer Elevar's captured values (from order.customAttributes) over
      // Shopify's native journey data — Elevar's first-party cookie survives
      // consent banners that suppress Shopify's _shopify_y session cookie.
      const elevar = parseElevarVisitorInfo(order.customAttributes);
      if (elevar.hasElevar) {
        utmSource   = elevar.utmSource;
        utmMedium   = elevar.utmMedium;
        utmCampaign = elevar.utmCampaign;
        utmContent  = elevar.utmContent;
        utmTerm     = elevar.utmTerm;
        utmId       = elevar.utmId || utmId;
      }
      const fbclid = elevar.fbclid;
      const metaAdIdFromUtm = elevar.metaAdIdFromUtm;

      // UTM classification — is this a paid Meta ad click?
      const utmConfirmedMeta = isPaidMetaUtm(utmSource, utmMedium);

      const discountCodes = (order.discountCodes || []).join(", ");

      const totalRefunded = (order.refunds || []).reduce((sum, r) => {
        return sum + parseFloat(r.totalRefundedSet?.shopMoney?.amount || "0");
      }, 0);
      const refundStatus = totalRefunded === 0 ? "none"
        : totalRefunded >= totalPrice ? "full" : "partial";
      const refundLineItems = buildRefundLineItems(order.refunds);

      // Customer name fields are protected — preserve existing values if already
      // populated. When the DB row has no firstName yet (typical for orders that
      // came in via the GraphQL backfill before this branch was added), fall back
      // to the billing firstName from Shopify so name-based gender inference has
      // something to work with.
      const existingOrder = await db.order.findUnique({
        where: { shopDomain_shopifyOrderId: { shopDomain, shopifyOrderId } },
        select: { customerFirstName: true, customerLastInitial: true },
      });
      const customerFirstName = existingOrder?.customerFirstName
        || (billing?.firstName || "").trim()
        || "";
      const customerLastInitial = existingOrder?.customerLastInitial || "";
      // Don't set customerOrderCountAtPurchase during import — it will be computed
      // correctly by computeOrderCounts() after all orders are imported, using the
      // customer's numberOfOrders as an anchor to count backwards.
      const customerOrderCountAtPurchase = null;

      let isNewOnThisOrder = null;
      if (order.customer?.orders?.edges?.length > 0) {
        const firstOrderId = order.customer.orders.edges[0].node.id;
        isNewOnThisOrder = firstOrderId === order.id;
      }

      await db.order.upsert({
        where: { shopDomain_shopifyOrderId: { shopDomain, shopifyOrderId } },
        create: {
          shopDomain, shopifyOrderId,
          shopifyCustomerId: order.customer?.id?.replace("gid://shopify/Customer/", "") || null,
          orderNumber: order.name,
          createdAt: new Date(order.createdAt),
          totalPrice, subtotalPrice, currency,
          financialStatus: order.displayFinancialStatus,
          channelName: channelHandle, isOnlineStore,
          frozenTotalPrice: totalPrice, frozenSubtotalPrice: subtotalPrice,
          isNewCustomerOrder: isNewOnThisOrder,
          country, countryCode, city, regionCode,
          customerFirstName, customerLastInitial, customerOrderCountAtPurchase,
          lineItems: lineItemTitles, productSkus, productCollections,
          discountCodes, refundStatus, totalRefunded, refundLineItems,
          landingSite, referringSite,
          utmSource, utmMedium, utmCampaign, utmContent, utmTerm, utmId, utmConfirmedMeta,
          fbclid, metaAdIdFromUtm,
        },
        update: {
          orderNumber: order.name,
          totalPrice, subtotalPrice,
          financialStatus: order.displayFinancialStatus,
          channelName: channelHandle, isOnlineStore,
          isNewCustomerOrder: isNewOnThisOrder,
          country, countryCode, city, regionCode,
          customerFirstName, customerLastInitial, customerOrderCountAtPurchase,
          lineItems: lineItemTitles, productSkus, productCollections,
          discountCodes, refundStatus, totalRefunded, refundLineItems,
          // Only overwrite landing/UTM fields when the current GraphQL response
          // actually carries them. An empty journey + no Elevar blob would
          // otherwise wipe UTM data captured by a prior sync (and clobber
          // utmConfirmedMeta to false).
          ...(landingSite ? { landingSite, referringSite } : {}),
          ...(utmSource
            ? { utmSource, utmMedium, utmCampaign, utmContent, utmTerm, utmId, utmConfirmedMeta, fbclid, metaAdIdFromUtm }
            : {}),
        },
      });

      // Replace OrderLineItem rows for this order. delete+createMany is simpler
      // and safer than trying to diff when Shopify sometimes reassigns line
      // item IDs after edits/refunds.
      const lineItemRows = buildLineItemRowsFromGraphQL(
        shopDomain, shopifyOrderId, order.lineItems, order.refunds,
      );
      await db.orderLineItem.deleteMany({ where: { shopDomain, shopifyOrderId } });
      if (lineItemRows.length > 0) {
        await db.orderLineItem.createMany({ data: lineItemRows });
      }
      totalImported++;
      if (totalImported % 50 === 0) {
        setProgress(`syncOrders:${shopDomain}`, {
          status: "running",
          current: totalImported,
          message: `Step 1 of 2 — Importing orders: ${totalImported.toLocaleString()} imported (page ${pageCount})`,
        });
      }

      if (order.customer?.id) {
        const customerId = order.customer.id.replace("gid://shopify/Customer/", "");
        touchedCustomerIds.add(customerId);
        const emailHash = hashEmail(order.customer.email);
        const firstOrderId = order.customer.orders?.edges?.[0]?.node?.id;
        const isFirstOrder = firstOrderId === order.id;

        const numberOfOrders = order.customer.numberOfOrders
          ? parseInt(order.customer.numberOfOrders) : 0;

        await db.customer.upsert({
          where: { shopDomain_shopifyCustomerId: { shopDomain, shopifyCustomerId: customerId } },
          create: {
            shopDomain, shopifyCustomerId: customerId, emailHash,
            firstOrderDate: new Date(order.createdAt), isNewCustomer: true,
            totalOrders: numberOfOrders,
          },
          update: {
            emailHash, totalOrders: numberOfOrders,
            ...(isFirstOrder ? { firstOrderDate: new Date(order.createdAt) } : {}),
          },
        });
        totalCustomers++;
      }
    }

    hasNextPage = data.data.orders.pageInfo.hasNextPage;
  }

  // Compute customerOrderCountAtPurchase.
  // Initial backfill: full shop-wide sweep. Incremental: only customers touched this run.
  setProgress(`syncOrders:${shopDomain}`, {
    status: "running",
    message: `Step 2 of 2 — Computing customer order counts (${totalImported.toLocaleString()} orders)...`,
  });
  if (isInitialBackfill) {
    await computeOrderCounts(shopDomain);
  } else {
    await computeOrderCounts(shopDomain, Array.from(touchedCustomerIds));
  }

  // Initial backfill: also infer gender from billing first names. Skipped on
  // incremental syncs — webhooks handle per-order updates as they arrive.
  if (isInitialBackfill) {
    setProgress(`syncOrders:${shopDomain}`, {
      status: "running",
      message: `Inferring customer demographics from billing names...`,
    });
    try {
      const result = await backfillShopInferredGender(db, shopDomain);
      console.log(`[OrderSync] inferGender: scanned=${result.scanned} inferred=${result.inferred} ambiguous=${result.ambiguous} noName=${result.noName}`);
    } catch (err) {
      console.error(`[OrderSync] inferGender backfill failed:`, err?.message || err);
    }
  }

  await db.shop.upsert({
    where: { shopDomain },
    create: { shopDomain, lastOrderSync: new Date() },
    update: { lastOrderSync: new Date() },
  });

  completeProgress(`syncOrders:${shopDomain}`, { totalImported, totalCustomers });
  console.log(`[OrderSync] Complete: ${totalImported} orders, ${totalCustomers} customers`);
  return { totalImported, totalCustomers };
}

/**
 * Targeted backfill for Order.customerFirstName. The original GraphQL query
 * never asked for billing.firstName, so historical orders imported before
 * that fix have empty names — which makes name-based gender inference a
 * no-op for the vast majority of customers.
 *
 * Memory-conservative implementation:
 *   • Page size 50 (matches existing syncOrders).
 *   • One raw-SQL UPDATE per order, with the empty-only filter pushed into
 *     the WHERE clause. No JS-side findMany or Map/Set accumulation, so
 *     per-page allocations stay bounded and webhook-set names are never
 *     overwritten by accident.
 *   • Sequential updates (no Promise.all of 100 in-flight queries that pile
 *     into the connection pool).
 *   • Manual global.gc() every 5 pages when --expose-gc is on (it is).
 *   • Frequent console logging so progress is visible in `flyctl logs`
 *     even if SIGKILL truncates the in-memory progress map.
 *
 * Returns: { ordersScanned, ordersUpdated, gender }
 */
export async function backfillCustomerFirstNames(admin, shopDomain) {
  console.log(`[backfillFirstNames] Starting for ${shopDomain}`);
  const taskId = `backfillFirstNames:${shopDomain}`;
  setProgress(taskId, { status: "running", message: "Starting Shopify scan..." });

  let cursor = null;
  let hasNextPage = true;
  let pageCount = 0;
  let scanned = 0;
  let updated = 0;
  const PAGE_SIZE = 50;

  while (hasNextPage) {
    const query = `
      query GetFirstNames($cursor: String) {
        orders(first: ${PAGE_SIZE}, after: $cursor, sortKey: CREATED_AT, query: "status:any") {
          edges {
            cursor
            node { id billingAddress { firstName } }
          }
          pageInfo { hasNextPage }
        }
      }
    `;
    let edges;
    try {
      const data = await graphqlWithRetry(admin, query, { cursor }, "backfillFirstNames");
      edges = data.data?.orders?.edges || [];
      hasNextPage = data.data?.orders?.pageInfo?.hasNextPage || false;
      if (edges.length > 0) cursor = edges[edges.length - 1].cursor;
    } catch (err) {
      console.error(`[backfillFirstNames] page ${pageCount + 1} GraphQL failed: ${err?.message || err}`);
      throw err;
    }

    pageCount++;
    scanned += edges.length;

    // Sequential per-row UPDATE. The WHERE clause asserts the row's
    // customerFirstName is currently empty so populated values (typically
    // set via webhook) are never overwritten.
    for (const e of edges) {
      const id = e.node.id?.replace("gid://shopify/Order/", "");
      const fn = (e.node.billingAddress?.firstName || "").trim();
      if (!id || !fn) continue;
      try {
        const affected = await db.$executeRaw`
          UPDATE "Order"
          SET "customerFirstName" = ${fn}
          WHERE "shopDomain" = ${shopDomain}
            AND "shopifyOrderId" = ${id}
            AND ("customerFirstName" IS NULL OR "customerFirstName" = '')
        `;
        if (affected > 0) updated++;
      } catch (err) {
        console.warn(`[backfillFirstNames] update failed for ${id}: ${err?.message || err}`);
      }
    }

    if (pageCount === 1 || pageCount % 20 === 0 || !hasNextPage) {
      console.log(`[backfillFirstNames] page=${pageCount} scanned=${scanned} updated=${updated}`);
    }
    setProgress(taskId, {
      status: "running",
      message: `Page ${pageCount}: ${scanned.toLocaleString()} scanned, ${updated.toLocaleString()} updated`,
    });

    // Encourage GC every few pages so per-page allocations don't accumulate.
    // Node is started with --expose-gc on Fly.
    if (typeof global !== "undefined" && typeof global.gc === "function" && pageCount % 5 === 0) {
      global.gc();
    }
  }

  console.log(`[backfillFirstNames] Order pass complete: ${scanned} scanned, ${updated} updated`);
  setProgress(taskId, { status: "running", message: `Inferring gender from names...` });

  const gender = await backfillShopInferredGender(db, shopDomain);
  console.log(`[backfillFirstNames] Gender inference: scanned=${gender.scanned} inferred=${gender.inferred} ambiguous=${gender.ambiguous} noName=${gender.noName}`);

  const result = { ordersScanned: scanned, ordersUpdated: updated, gender };
  completeProgress(taskId, result);
  return result;
}
