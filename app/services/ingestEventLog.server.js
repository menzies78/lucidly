// Persistent ingest event log. Writes JSONL to /data/ingest-events.jsonl
// (Fly volume — survives container restarts and deploys).
//
// Purpose: when an onboarding ingest runs overnight, the merchant (and we)
// need a durable record of what happened: which phases started, which
// completed, which retried, and the FULL error message + stack for any
// failure. console.log gets rotated; the IngestJob row only stores the most
// recent attempt's error. This file is the souce of truth for "what
// happened during the run".
//
// Surfaced to the UI via /app/diagnostics.
//
// Lines are JSON objects with at least { ts, shopDomain, phase, type }.
// Types: phase-start, phase-complete, phase-failed, phase-retry,
// rate-limit-hit, slow-pace, generic-error. Callers add freeform fields.
//
// Safety: appendFile is async and fire-and-forget — a log write must NEVER
// fail the ingest. Any error is swallowed with a console.warn.

import { promises as fs } from "node:fs";
import path from "node:path";

const LOG_DIR = process.env.INGEST_LOG_DIR || "/data";
const LOG_FILE = path.join(LOG_DIR, "ingest-events.jsonl");

let ensured = false;
async function ensureDir() {
  if (ensured) return;
  try {
    await fs.mkdir(LOG_DIR, { recursive: true });
    ensured = true;
  } catch (err) {
    console.warn(`[ingestEventLog] mkdir failed: ${err.message}`);
  }
}

/**
 * Append a structured event. Fire-and-forget — never throws.
 *
 * @param {object} event { shopDomain, phase, type, message?, ...freeform }
 */
export async function logIngestEvent(event) {
  try {
    await ensureDir();
    const line = JSON.stringify({ ts: new Date().toISOString(), ...event }) + "\n";
    await fs.appendFile(LOG_FILE, line, "utf8");
  } catch (err) {
    console.warn(`[ingestEventLog] append failed: ${err.message}`);
  }
}

/**
 * Read the last N events (most recent first), optionally filtered by shop.
 * Used by the /app/diagnostics route.
 */
export async function readIngestEvents({ shopDomain = null, limit = 500 } = {}) {
  try {
    const raw = await fs.readFile(LOG_FILE, "utf8");
    const lines = raw.split("\n").filter(Boolean);
    const parsed = [];
    for (let i = lines.length - 1; i >= 0 && parsed.length < limit; i--) {
      try {
        const obj = JSON.parse(lines[i]);
        if (!shopDomain || obj.shopDomain === shopDomain) parsed.push(obj);
      } catch {
        // skip malformed line
      }
    }
    return parsed;
  } catch (err) {
    if (err.code === "ENOENT") return [];
    console.warn(`[ingestEventLog] read failed: ${err.message}`);
    return [];
  }
}

/**
 * Convenience helper to extract a safe error string for logging. We keep the
 * full message + stack here even though the merchant-facing UI only shows a
 * sanitised version.
 */
export function errInfo(err) {
  if (!err) return { message: "(no error)" };
  return {
    message: err.message || String(err),
    stack: err.stack ? String(err.stack).slice(0, 2000) : undefined,
    code: err.code,
  };
}
