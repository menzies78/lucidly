// Backup tarball download endpoint. Lives at /api/* (NOT /app/*) so that
// a top-level browser navigation works - Shopify embedded-app auth
// requires a session-token JWT that browser file downloads can't carry,
// and any /app/* route bounces to the merchant login page on a plain GET.
//
// Auth model: short-lived HMAC token signed by the dashboard loader (which
// is itself authenticated via Shopify embedded auth + isInternalShop).
// The signed URL carries shop + exp + sig as query params; we verify them
// here before streaming any bytes.
//
// We use the system `tar` binary via execFile - safer than shell exec, and
// avoids adding a Node tar dependency. Child stdout pipes straight to the
// HTTP response so the file never has to fit in memory.

import { isInternalShop } from "../utils/access.server";
import { verifyDownloadToken } from "../utils/backupToken.server.js";
import { getBackupFolder, markBackupDownloaded } from "../services/shopBackup.server.js";
import { spawn } from "node:child_process";
import path from "node:path";

export const loader = async ({ request, params }) => {
  const url = new URL(request.url);
  const shop = url.searchParams.get("shop");
  const exp = url.searchParams.get("exp");
  const sig = url.searchParams.get("sig");
  const backupId = params.backupId;

  if (!backupId) return new Response("missing backupId", { status: 400 });

  // Token check first - rejects unsigned/expired/tampered URLs before we
  // touch the disk.
  const verdict = verifyDownloadToken({ shop, backupId, exp, sig });
  if (!verdict.ok) {
    return new Response(`forbidden: ${verdict.reason}`, { status: 403 });
  }

  // Belt-and-braces: even with a valid sig, the shop must still be on the
  // internal allow-list. Stops a leaked token from working if the shop is
  // later removed from LUCIDLY_INTERNAL_SHOPS.
  if (!isInternalShop(verdict.shop)) {
    return new Response("forbidden", { status: 403 });
  }

  const folder = await getBackupFolder(verdict.shop, backupId);
  if (!folder) return new Response("backup not found", { status: 404 });

  // Spawn `tar -czf - -C <parent> <basename>` and stream stdout.
  const parent = path.dirname(folder);
  const base = path.basename(folder);
  const child = spawn("tar", ["-czf", "-", "-C", parent, base], { stdio: ["ignore", "pipe", "pipe"] });

  // Bridge stderr to logs so a tar failure isn't silent.
  child.stderr.on("data", (b) => console.warn(`[backup-download] tar stderr: ${b.toString()}`));

  // Convert the Node Readable to a web ReadableStream so Remix can stream it.
  const stream = new ReadableStream({
    start(controller) {
      child.stdout.on("data", (chunk) => controller.enqueue(chunk));
      child.stdout.on("end", () => controller.close());
      child.stdout.on("error", (err) => controller.error(err));
      child.on("error", (err) => controller.error(err));
    },
    cancel() { child.kill("SIGTERM"); },
  });

  // Best-effort: stamp the manifest with lastDownloadedAt. Don't await -
  // the response can start streaming straight away.
  markBackupDownloaded(verdict.shop, backupId).catch(() => {});

  const safeName = `${verdict.shop.replace(/[^a-zA-Z0-9._-]/g, "_")}-${backupId}.tar.gz`;
  return new Response(stream, {
    status: 200,
    headers: {
      "Content-Type": "application/gzip",
      "Content-Disposition": `attachment; filename="${safeName}"`,
      "Cache-Control": "no-store",
    },
  });
};
