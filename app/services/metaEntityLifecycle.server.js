// Keeps MetaEntity's lifecycle fields fresh:
//
//   - entityName, currentStatus, scheduledStartAt, scheduledEndAt:
//       pulled from the Graph API (/{id}?fields=...).
//   - effectiveStartAt, effectiveEndAt:
//       derived from MetaInsight rows where spend > 0 OR impressions > 0.
//
// Called from the daily cycle; cheap enough to run on every pass once the
// entity row count stabilises.

import db from "../db.server";
import { fetchWithRetry } from "./metaFetch.server";

const GRAPH_FIELDS_BY_TYPE = {
  campaign: "id,name,status,effective_status,start_time,stop_time",
  adset:    "id,name,status,effective_status,start_time,end_time",
  ad:       "id,name,status,effective_status,created_time",
};

export async function refreshEntityLifecycle(shopDomain) {
  const shop = await db.shop.findUnique({ where: { shopDomain } });
  if (!shop?.metaAccessToken) return { updated: 0 };

  const token = shop.metaAccessToken;
  const entities = await db.metaEntity.findMany({
    where: { shopDomain },
    select: { id: true, entityType: true, entityId: true },
  });

  let updated = 0;
  for (const ent of entities) {
    const fields = GRAPH_FIELDS_BY_TYPE[ent.entityType];
    if (!fields) continue;
    const url = `https://graph.facebook.com/v21.0/${ent.entityId}`
      + `?fields=${encodeURIComponent(fields)}&access_token=${token}`;
    let data;
    try {
      data = await fetchWithRetry(url, `EntityLifecycle:${ent.entityType}:${ent.entityId}`);
    } catch (err) {
      // Deleted entities return errors; mark as such and move on.
      if (/does not exist|Cannot retrieve/.test(err.message || "")) {
        await db.metaEntity.update({
          where: { id: ent.id },
          data: { currentStatus: "DELETED", lastStatusAt: new Date() },
        });
        updated++;
      }
      continue;
    }
    if (data?.error) continue;

    const status = data.effective_status || data.status || null;
    const patch = {
      entityName: data.name || null,
      currentStatus: status,
      scheduledStartAt: data.start_time ? new Date(data.start_time) : null,
      scheduledEndAt: (data.stop_time || data.end_time) ? new Date(data.stop_time || data.end_time) : null,
      lastStatusAt: new Date(),
    };
    await db.metaEntity.update({ where: { id: ent.id }, data: patch });
    updated++;
  }

  return { updated };
}

export async function recomputeEntityDeliveryWindows(shopDomain) {
  // One SQL pass per entity type: MIN/MAX date grouped by the relevant id
  // column, filtered to rows that actually spent or served impressions.
  const results = { campaign: 0, adset: 0, ad: 0 };

  async function runForType(entityType, column) {
    const grouped = await db.metaInsight.groupBy({
      by: [column],
      where: {
        shopDomain,
        OR: [{ spend: { gt: 0 } }, { impressions: { gt: 0 } }],
      },
      _min: { date: true },
      _max: { date: true },
    });
    for (const row of grouped) {
      const entityId = row[column];
      if (!entityId) continue;
      const minDate = row._min.date;
      const maxDate = row._max.date;
      try {
        await db.metaEntity.update({
          where: { shopDomain_entityType_entityId: { shopDomain, entityType, entityId } },
          data: {
            effectiveStartAt: minDate || null,
            effectiveEndAt: maxDate || null,
          },
        });
        results[entityType]++;
      } catch {
        // Entity might not exist in MetaEntity yet — skip; syncMetaEntities
        // will create it on the next daily pass.
      }
    }
  }

  await runForType("campaign", "campaignId");
  await runForType("adset", "adSetId");
  await runForType("ad", "adId");

  return results;
}
