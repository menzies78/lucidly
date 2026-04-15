import { json } from "@remix-run/node";
import { authenticate } from "../shopify.server";
import db from "../db.server";
import { shopLocalDayKey } from "../utils/shopTime.server";

// Lightweight API feeding the EntityTimelineDrawer. Returns the full
// life-history of one Meta entity: lifecycle metadata, its change-log
// events, and a per-day spend/revenue series so the drawer can render a
// mini sparkline with annotation markers.

export const loader = async ({ request }: { request: Request }) => {
  const { session } = await authenticate.admin(request);
  const shopDomain = session.shop;

  const url = new URL(request.url);
  const type = url.searchParams.get("type");
  const id = url.searchParams.get("id");
  if (!type || !id) return json({ error: "type and id required" }, { status: 400 });

  const shop = await db.shop.findUnique({ where: { shopDomain } });
  const tz = shop?.shopifyTimezone || "UTC";

  // Lifecycle metadata — may be null if syncMetaEntities hasn't picked this
  // entity up yet. Fall back to the MetaInsight-derived effective window.
  const entity = await db.metaEntity.findUnique({
    where: {
      shopDomain_entityType_entityId: { shopDomain, entityType: type, entityId: id },
    },
  });

  // Change log events for this object — not date-scoped, since the drawer
  // shows the full history.
  const events = await db.metaChange.findMany({
    where: { shopDomain, objectId: id, objectType: type },
    orderBy: { eventTime: "desc" },
    take: 500,
  });

  // Per-day spend/revenue series for the 90 days before today (drawer is
  // not bound to the page's date range; showing recent history is enough).
  // We pull from DailyAdRollup so we also get newCustomerOrders and
  // attributedOrders, which sparkline consumers (Best-to-Worst hover) need
  // when the merchant sorts by a customer-specific metric.
  const since = new Date(Date.now() - 90 * 86400000);
  let rollupWhere: any = { shopDomain, date: { gte: since } };
  if (type === "ad") rollupWhere.adId = id;
  else if (type === "adset") rollupWhere.adSetId = id;
  else if (type === "campaign") rollupWhere.campaignId = id;

  const rollups = await db.dailyAdRollup.findMany({
    where: rollupWhere,
    select: {
      date: true, spend: true, attributedRevenue: true, unverifiedRevenue: true,
      attributedOrders: true, newCustomerOrders: true,
    },
  });

  const byDay = new Map<string, { spend: number; revenue: number; orders: number; newCustomerOrders: number }>();
  for (const r of rollups) {
    const key = shopLocalDayKey(tz, r.date);
    const agg = byDay.get(key) || { spend: 0, revenue: 0, orders: 0, newCustomerOrders: 0 };
    agg.spend += r.spend || 0;
    agg.revenue += (r.attributedRevenue || 0) + (r.unverifiedRevenue || 0);
    agg.orders += r.attributedOrders || 0;
    agg.newCustomerOrders += r.newCustomerOrders || 0;
    byDay.set(key, agg);
  }
  const daily = [...byDay.entries()]
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([date, v]) => ({ date, ...v }));

  const fallbackName = events[0]?.objectName || null;

  return json({
    entity: {
      objectType: type,
      objectId: id,
      objectName: entity?.entityName || fallbackName,
      currentStatus: entity?.currentStatus || null,
      scheduledStartAt: entity?.scheduledStartAt?.toISOString() || null,
      scheduledEndAt: entity?.scheduledEndAt?.toISOString() || null,
      effectiveStartAt: entity?.effectiveStartAt?.toISOString() || null,
      effectiveEndAt: entity?.effectiveEndAt?.toISOString() || null,
      createdTime: entity?.createdTime?.toISOString() || null,
    },
    events: events.map((e) => ({
      id: e.id,
      eventTimeISO: e.eventTime.toISOString(),
      category: e.category,
      summary: e.summary,
      actor: e.actorName || e.actorId || null,
      oldValue: e.oldValue,
      newValue: e.newValue,
      rawEventType: e.rawEventType,
    })),
    daily,
  });
};
