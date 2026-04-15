// Sync Meta's ad-account change history (/act_{id}/activities) into the
// MetaChange table. Two call modes:
//
//   syncMetaChanges(shopDomain, { backfillDays: 90 })
//     Full backfill — used from the "Import Meta change history" button.
//
//   syncMetaChanges(shopDomain)
//     Incremental (last 36h window, upsert) — wired into the hourly cycle.
//
// Upserts are keyed on (shopDomain, eventTime, rawEventType, objectId), so
// the same run is safe to execute repeatedly without duplicating rows.

import db from "../db.server";
import { fetchAllPages } from "./metaFetch.server";
import { setProgress, completeProgress, failProgress } from "./progress.server";
import { classifyEvent } from "./metaChangeClassifier.server";

const DEFAULT_INCREMENTAL_HOURS = 36;

/**
 * @param {string} shopDomain
 * @param {{ backfillDays?: number, taskKey?: string }} [opts]
 */
export async function syncMetaChanges(shopDomain, opts = {}) {
  const { backfillDays, taskKey } = opts;
  const shop = await db.shop.findUnique({ where: { shopDomain } });
  if (!shop?.metaAccessToken || !shop?.metaAdAccountId) {
    return { added: 0, total: 0, skipped: "not-connected" };
  }
  const token = shop.metaAccessToken;
  const accountId = shop.metaAdAccountId;

  const now = Math.floor(Date.now() / 1000);
  const since = backfillDays
    ? now - backfillDays * 86400
    : now - DEFAULT_INCREMENTAL_HOURS * 3600;
  const until = now;

  const label = backfillDays
    ? `MetaChangeSync:${shopDomain}:backfill-${backfillDays}d`
    : `MetaChangeSync:${shopDomain}`;

  if (taskKey) {
    setProgress(taskKey, { status: "running", message: `Fetching activities (${backfillDays || "delta"} window)...` });
  }

  const fields = [
    "event_time",
    "event_type",
    "object_id",
    "object_type",
    "object_name",
    "actor_id",
    "actor_name",
    "extra_data",
    "translated_event_type",
  ].join(",");

  const url = `https://graph.facebook.com/v21.0/${accountId}/activities`
    + `?fields=${encodeURIComponent(fields)}`
    + `&since=${since}&until=${until}`
    + `&limit=500`
    + `&access_token=${token}`;

  let rows = [];
  try {
    rows = await fetchAllPages(url, label);
  } catch (err) {
    console.error(`[${label}] Fetch failed: ${err.message}`);
    if (taskKey) failProgress(taskKey, err);
    throw err;
  }
  console.log(`[${label}] Fetched ${rows.length} activity events`);

  let added = 0;
  let updated = 0;
  const CHUNK = 50;

  for (let i = 0; i < rows.length; i += CHUNK) {
    const batch = rows.slice(i, i + CHUNK);
    if (taskKey && backfillDays) {
      setProgress(taskKey, {
        status: "running",
        message: "Saving activities...",
        current: Math.min(i + CHUNK, rows.length),
        total: rows.length,
      });
    }
    await Promise.all(batch.map(async (raw) => {
      const row = classifyEvent(raw, shopDomain);
      const key = {
        shopDomain,
        eventTime: row.eventTime,
        rawEventType: row.rawEventType,
        objectId: row.objectId,
      };
      try {
        const existed = await db.metaChange.findUnique({
          where: { shopDomain_eventTime_rawEventType_objectId: key },
          select: { id: true },
        });
        if (existed) {
          await db.metaChange.update({
            where: { shopDomain_eventTime_rawEventType_objectId: key },
            data: {
              category: row.category,
              objectType: row.objectType,
              objectName: row.objectName,
              actorId: row.actorId,
              actorName: row.actorName,
              oldValue: row.oldValue,
              newValue: row.newValue,
              summary: row.summary,
              rawPayload: row.rawPayload,
            },
          });
          updated++;
        } else {
          await db.metaChange.create({ data: row });
          added++;
        }
      } catch (err) {
        // A race between two concurrent syncs can race on the unique key.
        // Treat as a no-op rather than aborting the batch.
        if (!String(err?.message || "").includes("Unique constraint")) {
          console.error(`[${label}] Upsert failed for event ${row.rawEventType} ${row.objectId}: ${err.message}`);
        }
      }
    }));
  }

  const total = await db.metaChange.count({ where: { shopDomain } });
  console.log(`[${label}] Done: ${added} added, ${updated} updated. Total rows for shop: ${total}`);

  if (taskKey) {
    completeProgress(taskKey, { added, updated, total, fetched: rows.length });
  }
  return { added, updated, total, fetched: rows.length };
}
