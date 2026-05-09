// Streams a tar.gz of a backup folder so Andy can pull a copy onto his
// Mac before wiping. Internal-only - gated by isInternalShop().
//
// We use the system `tar` binary (always present on Linux/macOS) via
// execFile with explicit args - safer than shell exec, and avoids adding
// a Node tar dependency. The child stdout is piped straight to the HTTP
// response so the file never has to fit in memory.

import { authenticate } from "../shopify.server";
import { isInternalShop } from "../utils/access.server";
import { getBackupFolder, markBackupDownloaded } from "../services/shopBackup.server.js";
import { spawn } from "node:child_process";
import path from "node:path";

export const loader = async ({ request, params }) => {
  const { session } = await authenticate.admin(request);
  const shopDomain = session.shop;
  if (!isInternalShop(shopDomain)) {
    return new Response("forbidden", { status: 403 });
  }
  const backupId = params.backupId;
  if (!backupId) return new Response("missing backupId", { status: 400 });

  const folder = await getBackupFolder(shopDomain, backupId);
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
  markBackupDownloaded(shopDomain, backupId).catch(() => {});

  const safeName = `${shopDomain.replace(/[^a-zA-Z0-9._-]/g, "_")}-${backupId}.tar.gz`;
  return new Response(stream, {
    status: 200,
    headers: {
      "Content-Type": "application/gzip",
      "Content-Disposition": `attachment; filename="${safeName}"`,
      "Cache-Control": "no-store",
    },
  });
};
