import type { ActionFunctionArgs } from "@remix-run/node";
import { authenticate } from "../shopify.server";
import db from "../db.server";

// GDPR / Shopify Privacy mandatory webhook.
//
// Triggered 48 hours after a merchant uninstalls the app. Requires us to
// delete EVERY record associated with the shop. We sweep all tables that
// carry shopDomain. ExchangeRate is the only multi-shop-shared table and
// is intentionally kept (FX rates are public reference data, not PII).
//
// Order is dependency-aware where it matters - rows pointed to by foreign
// keys (line items, attributions) are removed before their parent Orders.
// SQLite has no ON DELETE CASCADE in our schema so we do this explicitly.
export const action = async ({ request }: ActionFunctionArgs) => {
  const { shop, topic, payload } = await authenticate.webhook(request);
  console.log(`[GDPR] ${topic} for ${shop}:`, JSON.stringify(payload));

  const where = { shopDomain: shop };

  try {
    // Order-dependent rows first.
    await db.orderLineItem.deleteMany({ where });
    await db.attribution.deleteMany({ where });

    // Per-shop primary data.
    await db.order.deleteMany({ where });
    await db.customer.deleteMany({ where });
    await db.metaInsight.deleteMany({ where });
    await db.metaBreakdown.deleteMany({ where });
    await db.metaEntity.deleteMany({ where });
    await db.metaChange.deleteMany({ where });
    await db.metaSnapshot.deleteMany({ where });
    await db.metaCountrySnapshot.deleteMany({ where });

    // Aggregated rollups + caches.
    await db.dailyProductRollup.deleteMany({ where });
    await db.dailyAdRollup.deleteMany({ where });
    await db.dailyGeoRollup.deleteMany({ where });
    await db.dailyAdDemographicRollup.deleteMany({ where });
    await db.dailyCustomerRollup.deleteMany({ where });
    await db.shopAnalysisCache.deleteMany({ where });
    await db.aiInsight.deleteMany({ where });
    await db.ingestJob.deleteMany({ where });

    // Session (already cleared on app/uninstalled, but be defensive).
    await db.session.deleteMany({ where: { shop } });

    // Shop row last - other tables reference it logically by shopDomain
    // but Prisma has no FK constraint enforced for SQLite.
    await db.shop.deleteMany({ where });

    console.log(`[GDPR] shop/redact completed for ${shop}`);
  } catch (err) {
    console.error(`[GDPR] shop/redact failed for ${shop}:`, err);
  }

  return new Response();
};
