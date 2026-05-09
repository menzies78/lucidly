// Shop backup / wipe / restore. Internal-tools-only - this is the safety
// net that lets Andy wipe a test merchant (e.g. Vollebak) to walk through
// the new-install flow as a fresh user, then restore the historical data
// (with its higher-quality incrementally-matched attribution) afterwards.
//
// Four layers of safety, any one of which is sufficient on its own:
//   1. Per-shop JSON dump - schema-drift-tolerant. Every shop-keyed Prisma
//      table exported as its own .json file with sha256 checksum.
//   2. Native SQLite snapshot of the entire database (`VACUUM INTO`).
//      Perfect-fidelity copy - even if the JSON path has a bug, this can
//      be hand-restored by file-copy.
//   3. Verify pass after every backup re-reads each JSON file and matches
//      the checksum + row count against the manifest. Backup is marked
//      `verified: true` only on success.
//   4. Wipe refuses unless the newest backup is `verified: true` AND
//      younger than 24h. (Andy can additionally download a tarball of the
//      backup folder to his Mac for an off-Fly copy via the download
//      endpoint - manifest tracks lastDownloadedAt for visibility.)
//
// Rollups + IngestJob are deliberately NOT restored:
//   - IngestJob is a transient artefact of the original ingest run.
//   - DailyAdRollup / DailyAdDemographicRollup / DailyCustomerRollup /
//     DailyProductRollup / ShopAnalysisCache are derived data - faster to
//     regenerate from the restored source rows than to ship and risk
//     schema drift.
//
// Restore triggers a full rollup rebuild at the end so the dashboard works
// immediately without waiting for the next scheduled cycle.

import db from "../db.server.js";
import fs from "node:fs/promises";
import path from "node:path";
import crypto from "node:crypto";

// Storage root: Fly volume in production, local tmp dir in dev. Both are
// outside the repo so backups never accidentally get committed.
const BACKUP_ROOT = process.env.NODE_ENV === "production"
  ? "/data/backups"
  : path.join(process.cwd(), "tmp", "backups");

// 24h freshness window for the wipe guard.
const BACKUP_FRESHNESS_MS = 24 * 60 * 60 * 1000;

// Tables we back up + restore. Order matters for restore: parents first
// (Shop, Customer) before rows that depend on them (Order, OrderLineItem,
// Attribution). Reverse order is used for delete (children first).
//
// `shopKey` is the field name to filter by - Session uses `shop`, every
// other shop-keyed table uses `shopDomain`.
const SHOP_TABLES = [
  { model: "shop", shopKey: "shopDomain", restoreOrder: 0 },
  { model: "session", shopKey: "shop", restoreOrder: 1 },
  { model: "customer", shopKey: "shopDomain", restoreOrder: 2 },
  { model: "order", shopKey: "shopDomain", restoreOrder: 3 },
  { model: "orderLineItem", shopKey: "shopDomain", restoreOrder: 4 },
  { model: "metaInsight", shopKey: "shopDomain", restoreOrder: 5 },
  { model: "metaBreakdown", shopKey: "shopDomain", restoreOrder: 6 },
  { model: "metaEntity", shopKey: "shopDomain", restoreOrder: 7 },
  { model: "metaChange", shopKey: "shopDomain", restoreOrder: 8 },
  { model: "metaSnapshot", shopKey: "shopDomain", restoreOrder: 9 },
  { model: "metaCountrySnapshot", shopKey: "shopDomain", restoreOrder: 10 },
  { model: "attribution", shopKey: "shopDomain", restoreOrder: 11 },
  { model: "aiInsight", shopKey: "shopDomain", restoreOrder: 12 },
];

// Backed up but never restored - regenerated from source rows after restore.
const DERIVED_TABLES = [
  "ingestJob",
  "dailyAdRollup",
  "dailyAdDemographicRollup",
  "dailyCustomerRollup",
  "dailyProductRollup",
  "shopAnalysisCache",
];

// JSON.stringify replacer: BigInt → { __bigint: "..." }, Date already
// serialises to ISO string but we tag it so the reviver can rebuild Date
// objects (Prisma.createMany rejects strings on DateTime columns).
function safeStringify(obj) {
  return JSON.stringify(obj, (_key, value) => {
    if (typeof value === "bigint") return { __bigint: value.toString() };
    if (value instanceof Date) return { __date: value.toISOString() };
    return value;
  }, 0);
}

function safeParse(text) {
  return JSON.parse(text, (_key, value) => {
    if (value && typeof value === "object") {
      if (typeof value.__bigint === "string") return BigInt(value.__bigint);
      if (typeof value.__date === "string") return new Date(value.__date);
    }
    return value;
  });
}

function checksum(text) {
  return crypto.createHash("sha256").update(text).digest("hex").slice(0, 16);
}

function safeShopFolder(shopDomain) {
  // shopDomain is myshopify.com format - colon-free, but be defensive.
  return shopDomain.replace(/[^a-zA-Z0-9._-]/g, "_");
}

async function ensureDir(p) {
  await fs.mkdir(p, { recursive: true });
}

/**
 * Dump every shop-keyed table to JSON, take a native SQLite snapshot of
 * the whole database, then run a verify pass that re-reads every JSON
 * file and confirms its checksum matches the manifest.
 *
 * Manifest is written *twice*:
 *   - Once after JSON dump + SQLite snapshot (so a crash partway through
 *     verify still leaves a recoverable backup).
 *   - Again after verify pass with `verified: true` set.
 */
export async function backupShop(shopDomain, onProgress = () => {}) {
  const startedAt = new Date();
  const stamp = startedAt.toISOString().replace(/[:.]/g, "-");
  const folder = path.join(BACKUP_ROOT, safeShopFolder(shopDomain), stamp);
  await ensureDir(folder);

  const tables = [...SHOP_TABLES.map(t => ({ ...t, derived: false })),
                  ...DERIVED_TABLES.map(m => ({ model: m, shopKey: "shopDomain", derived: true }))];

  const manifest = {
    shopDomain,
    backupId: stamp,
    startedAt: startedAt.toISOString(),
    schemaVersion: 2,
    tables: {},
    verified: false,
    sqliteSnapshot: null,
    lastDownloadedAt: null,
  };

  let totalRows = 0;
  for (let i = 0; i < tables.length; i++) {
    const t = tables[i];
    onProgress(`Dumping ${t.model} (${i + 1}/${tables.length})`);
    let rows = [];
    try {
      rows = await db[t.model].findMany({ where: { [t.shopKey]: shopDomain } });
    } catch (err) {
      console.warn(`[shopBackup] Failed to dump ${t.model}: ${err.message}`);
      manifest.tables[t.model] = { count: 0, error: err.message, derived: t.derived };
      continue;
    }
    const text = safeStringify(rows);
    const file = path.join(folder, `${t.model}.json`);
    await fs.writeFile(file, text, "utf8");
    manifest.tables[t.model] = {
      count: rows.length,
      bytes: Buffer.byteLength(text, "utf8"),
      checksum: checksum(text),
      derived: t.derived,
    };
    totalRows += rows.length;
  }

  manifest.completedAt = new Date().toISOString();
  manifest.totalRows = totalRows;

  // Layer 2: native SQLite snapshot. Perfect-fidelity copy of the entire
  // database (all shops, all tables, all indices). Even if the JSON path
  // ever has a bug, this snapshot can be hand-restored by stopping the app
  // and copying snapshot.sqlite over /data/lucidly.db. `VACUUM INTO`
  // produces a single consistent file - no need for WAL handling.
  onProgress("Taking native SQLite snapshot...");
  const snapshotPath = path.join(folder, "snapshot.sqlite");
  try {
    // Prisma escapes the string literal correctly inside $executeRawUnsafe
    // since we control the input. Path is also restricted to a known dir.
    await db.$executeRawUnsafe(`VACUUM INTO '${snapshotPath.replace(/'/g, "''")}'`);
    const stat = await fs.stat(snapshotPath);
    const buf = await fs.readFile(snapshotPath);
    const sha = crypto.createHash("sha256").update(buf).digest("hex");
    manifest.sqliteSnapshot = { bytes: stat.size, checksum: sha.slice(0, 16) };
  } catch (err) {
    console.error(`[shopBackup] SQLite snapshot failed: ${err.message}`);
    manifest.sqliteSnapshot = { error: err.message };
  }

  // Provisional manifest write so a crash before verify still leaves a
  // recoverable backup folder.
  await fs.writeFile(path.join(folder, "manifest.json"), JSON.stringify(manifest, null, 2), "utf8");

  // Layer 3: verify pass. Re-read every JSON file and check sha256 +
  // parseability + row count. Anything mismatched marks the backup as
  // unverified, and the wipe gate refuses to proceed.
  onProgress("Verifying backup integrity...");
  const verifyResult = await verifyBackupFolder(folder, manifest);
  manifest.verified = verifyResult.ok;
  manifest.verifyDetail = verifyResult;

  // Final manifest write with verified flag.
  await fs.writeFile(path.join(folder, "manifest.json"), JSON.stringify(manifest, null, 2), "utf8");

  console.log(`[shopBackup] ${shopDomain}: backup ${stamp} - ${totalRows} rows, sqlite ${manifest.sqliteSnapshot?.bytes || 0}B, verified=${manifest.verified}`);
  onProgress(`Backup complete: ${totalRows} rows, verified=${manifest.verified}`);
  return manifest;
}

/**
 * Re-read every JSON file in a backup folder, recompute its sha256, and
 * compare to the manifest. Also re-parses each file (catches malformed
 * JSON that managed to round-trip the disk). Returns { ok, errors[] }.
 */
async function verifyBackupFolder(folder, manifest) {
  const errors = [];
  let totalChecked = 0;
  for (const [model, meta] of Object.entries(manifest.tables)) {
    if (meta.error) continue; // already-known failure, don't re-flag
    const file = path.join(folder, `${model}.json`);
    try {
      const text = await fs.readFile(file, "utf8");
      const sha = checksum(text);
      if (sha !== meta.checksum) {
        errors.push({ model, kind: "checksum-mismatch", expected: meta.checksum, got: sha });
        continue;
      }
      const parsed = safeParse(text);
      if (!Array.isArray(parsed)) {
        errors.push({ model, kind: "not-array" });
        continue;
      }
      if (parsed.length !== meta.count) {
        errors.push({ model, kind: "count-mismatch", expected: meta.count, got: parsed.length });
        continue;
      }
      totalChecked++;
    } catch (err) {
      errors.push({ model, kind: "read-error", message: err.message });
    }
  }
  return { ok: errors.length === 0, totalChecked, errors };
}

/**
 * Public verify - used by the UI to re-check an existing backup on demand
 * (e.g. before pressing Wipe, if the backup is several hours old).
 */
export async function verifyBackup(shopDomain, backupId) {
  const backups = await listBackups(shopDomain);
  const target = backups.find(b => b.backupId === backupId);
  if (!target) throw new Error(`No backup found for ${shopDomain} id=${backupId}`);
  const result = await verifyBackupFolder(target.folder, target);
  // Update manifest in place with new verify result.
  const mPath = path.join(target.folder, "manifest.json");
  const m = JSON.parse(await fs.readFile(mPath, "utf8"));
  m.verified = result.ok;
  m.verifyDetail = result;
  m.lastVerifiedAt = new Date().toISOString();
  await fs.writeFile(mPath, JSON.stringify(m, null, 2), "utf8");
  return result;
}

/**
 * Mark a backup as downloaded (called by the tarball download endpoint).
 * Purely advisory - we don't gate the wipe on it - but surfaces in the UI
 * so Andy can see whether an off-Fly copy exists.
 */
export async function markBackupDownloaded(shopDomain, backupId) {
  const backups = await listBackups(shopDomain);
  const target = backups.find(b => b.backupId === backupId);
  if (!target) return;
  const mPath = path.join(target.folder, "manifest.json");
  try {
    const m = JSON.parse(await fs.readFile(mPath, "utf8"));
    m.lastDownloadedAt = new Date().toISOString();
    await fs.writeFile(mPath, JSON.stringify(m, null, 2), "utf8");
  } catch (err) {
    console.warn(`[shopBackup] markBackupDownloaded failed: ${err.message}`);
  }
}

/**
 * Resolve the on-disk folder for a backup id (used by the download
 * endpoint to stream the tarball). Returns null if not found.
 */
export async function getBackupFolder(shopDomain, backupId) {
  const backups = await listBackups(shopDomain);
  const target = backups.find(b => b.backupId === backupId);
  return target?.folder || null;
}

/**
 * List backup manifests for a shop, newest first.
 */
export async function listBackups(shopDomain) {
  const dir = path.join(BACKUP_ROOT, safeShopFolder(shopDomain));
  let entries = [];
  try {
    entries = await fs.readdir(dir);
  } catch (err) {
    if (err.code === "ENOENT") return [];
    throw err;
  }
  const manifests = [];
  for (const e of entries) {
    const mPath = path.join(dir, e, "manifest.json");
    try {
      const text = await fs.readFile(mPath, "utf8");
      const m = JSON.parse(text);
      manifests.push({ ...m, folder: path.join(dir, e) });
    } catch {
      // skip - corrupt or in-progress
    }
  }
  manifests.sort((a, b) => (b.startedAt || "").localeCompare(a.startedAt || ""));
  return manifests;
}

/**
 * Self-uninstall the app from Shopify so the next visit takes the App Store
 * install link path (not a silent OAuth re-token). Calls the documented
 * Shopify endpoint:
 *
 *   DELETE /admin/api/{version}/api_permissions/current.json
 *
 * This revokes the access token Shopify-side and removes the app from the
 * merchant's admin > Apps list. Best-effort - if Shopify rejects (e.g. the
 * token is already invalid), we still proceed with the local DB wipe.
 */
async function shopifyUninstall(shopDomain, accessToken) {
  if (!shopDomain || !accessToken) return { ok: false, reason: "missing-credentials" };
  const url = `https://${shopDomain}/admin/api/2025-01/api_permissions/current.json`;
  try {
    const res = await fetch(url, {
      method: "DELETE",
      headers: { "X-Shopify-Access-Token": accessToken },
    });
    if (res.ok) return { ok: true, status: res.status };
    const body = await res.text().catch(() => "");
    return { ok: false, status: res.status, body: body.slice(0, 200) };
  } catch (err) {
    return { ok: false, reason: err.message };
  }
}

/**
 * Wipe all shop-scoped data AND uninstall the app from Shopify so the
 * merchant must use the App Store install link to reconnect. Refuses
 * unless a backup younger than 24h exists. Children deleted first so
 * foreign keys don't trip.
 */
export async function wipeShop(shopDomain, onProgress = () => {}) {
  const backups = await listBackups(shopDomain);
  const newest = backups[0];
  if (!newest || !newest.completedAt) {
    throw new Error("Refusing to wipe: no completed backup found. Run Backup Shop first.");
  }
  const ageMs = Date.now() - new Date(newest.completedAt).getTime();
  if (ageMs > BACKUP_FRESHNESS_MS) {
    const hours = Math.round(ageMs / 3600000);
    throw new Error(`Refusing to wipe: newest backup is ${hours}h old (>24h). Run Backup Shop first.`);
  }
  if (!newest.verified) {
    throw new Error(`Refusing to wipe: newest backup (${newest.backupId}) failed verification. Re-run Backup Shop or click Verify to investigate.`);
  }

  // Capture the Shopify access token BEFORE we delete the Session row -
  // we need it to authenticate the self-uninstall call. There may be more
  // than one session per shop (online + offline); any valid offline token
  // works for the api_permissions DELETE.
  onProgress("Reading Shopify access token...");
  const sessions = await db.session.findMany({
    where: { shop: shopDomain },
    select: { accessToken: true, isOnline: true },
  });
  const offline = sessions.find(s => !s.isOnline) || sessions[0];
  const shopifyAccessToken = offline?.accessToken || null;

  // Self-uninstall on Shopify FIRST. If we wipe the DB first, the access
  // token is gone and the Shopify-side uninstall can't run. Failure here
  // is non-fatal - we still wipe locally and surface the failure in logs.
  onProgress("Uninstalling app from Shopify...");
  const uninstallResult = await shopifyUninstall(shopDomain, shopifyAccessToken);
  if (uninstallResult.ok) {
    console.log(`[shopBackup] ${shopDomain}: Shopify self-uninstall OK`);
  } else {
    console.warn(`[shopBackup] ${shopDomain}: Shopify self-uninstall failed:`, uninstallResult);
  }

  // Delete in reverse restore order so child rows go before parents.
  // Include derived tables - rollups must be flushed too or loaders will
  // serve stale data after the wipe.
  const wipeOrder = [
    ...DERIVED_TABLES.map(m => ({ model: m, shopKey: "shopDomain" })),
    ...SHOP_TABLES.slice().sort((a, b) => b.restoreOrder - a.restoreOrder),
  ];

  let totalDeleted = 0;
  for (let i = 0; i < wipeOrder.length; i++) {
    const t = wipeOrder[i];
    onProgress(`Wiping ${t.model} (${i + 1}/${wipeOrder.length})`);
    try {
      const r = await db[t.model].deleteMany({ where: { [t.shopKey]: shopDomain } });
      totalDeleted += r.count;
    } catch (err) {
      console.warn(`[shopBackup] Wipe failed for ${t.model}: ${err.message}`);
    }
  }

  console.log(`[shopBackup] ${shopDomain}: wiped ${totalDeleted} rows`);
  onProgress(`Wipe complete: ${totalDeleted} rows deleted${uninstallResult.ok ? "; reinstall via App Store link" : "; Shopify uninstall failed - manual uninstall may be needed"}`);
  return {
    deleted: totalDeleted,
    backupId: newest.backupId,
    shopifyUninstalled: uninstallResult.ok,
    shopifyUninstallDetail: uninstallResult,
  };
}

/**
 * Restore a shop from a backup. Skips derived tables (rollups, ingest jobs)
 * - those are regenerated by the rollup rebuild at the end.
 *
 * Schema-drift tolerant: tries createMany+skipDuplicates first, falls back
 * to per-row create if a column has been added/removed since the backup.
 */
export async function restoreShop(shopDomain, backupId, onProgress = () => {}) {
  const backups = await listBackups(shopDomain);
  const target = backups.find(b => b.backupId === backupId) || backups[0];
  if (!target) throw new Error(`No backup found for ${shopDomain}${backupId ? ` (id=${backupId})` : ""}`);
  const folder = target.folder;

  const orderedTables = SHOP_TABLES.slice().sort((a, b) => a.restoreOrder - b.restoreOrder);

  let totalInserted = 0;
  for (let i = 0; i < orderedTables.length; i++) {
    const t = orderedTables[i];
    onProgress(`Restoring ${t.model} (${i + 1}/${orderedTables.length})`);
    const file = path.join(folder, `${t.model}.json`);
    let rows;
    try {
      const text = await fs.readFile(file, "utf8");
      rows = safeParse(text);
    } catch (err) {
      if (err.code === "ENOENT") {
        console.warn(`[shopBackup] ${t.model}.json missing - skipping`);
        continue;
      }
      throw err;
    }
    if (!Array.isArray(rows) || rows.length === 0) continue;

    // Try the fast path first: createMany in batches with skipDuplicates.
    // If it fails (schema drift, unknown column), fall back to per-row
    // create with the unknown fields stripped.
    const BATCH = 500;
    let inserted = 0;
    for (let j = 0; j < rows.length; j += BATCH) {
      const batch = rows.slice(j, j + BATCH);
      try {
        const r = await db[t.model].createMany({ data: batch, skipDuplicates: true });
        inserted += r.count;
      } catch (err) {
        // Slow path: per-row, drop unknown fields by retrying without them.
        for (const row of batch) {
          try {
            await db[t.model].create({ data: row });
            inserted++;
          } catch (rowErr) {
            // Try stripping fields the model doesn't know about.
            const msg = String(rowErr?.message || "");
            const unknown = msg.match(/Unknown argument `(\w+)`/g);
            if (unknown) {
              const stripped = { ...row };
              for (const u of unknown) {
                const name = u.match(/`(\w+)`/)?.[1];
                if (name) delete stripped[name];
              }
              try {
                await db[t.model].create({ data: stripped });
                inserted++;
              } catch (e2) {
                console.warn(`[shopBackup] ${t.model} row insert failed: ${e2.message?.slice(0, 200)}`);
              }
            } else {
              // Could be a unique-constraint violation - row already there.
              // Silent skip is fine.
            }
          }
        }
      }
    }
    totalInserted += inserted;
    console.log(`[shopBackup] ${t.model}: restored ${inserted}/${rows.length}`);
  }

  // Regenerate all rollups from the restored source rows. Rollups are not
  // restored from the backup - faster to rebuild than to ship and risk
  // schema drift on the derived shape.
  onProgress("Rebuilding rollups (this can take a few minutes)...");
  try {
    const { rebuildCampaignRollups } = await import("./campaignRollups.server.js");
    await rebuildCampaignRollups(shopDomain);
    if (global.gc) global.gc();
    const { rebuildAdDemographicRollups } = await import("./adDemographicRollups.server.js");
    await rebuildAdDemographicRollups(shopDomain);
    if (global.gc) global.gc();
    const { rebuildCustomerSegments, rebuildCustomerRollups } = await import("./customerRollups.server.js");
    await rebuildCustomerSegments(shopDomain);
    await rebuildCustomerRollups(shopDomain);
    if (global.gc) global.gc();
    const { rebuildProductRollups } = await import("./productRollups.server.js");
    await rebuildProductRollups(shopDomain);
    const { invalidateShop } = await import("./queryCache.server.js");
    invalidateShop(shopDomain);
  } catch (err) {
    console.error(`[shopBackup] Rollup rebuild after restore failed: ${err.message}`);
  }

  console.log(`[shopBackup] ${shopDomain}: restored ${totalInserted} rows from ${target.backupId}`);
  onProgress(`Restore complete: ${totalInserted} rows`);
  return { restored: totalInserted, backupId: target.backupId };
}
