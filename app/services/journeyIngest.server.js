// Journey ingest buffer - decouples the web-pixel firehose from the SQLite writer.
//
// Why this exists: /api/journey used to do one blocking `await create()` per
// pixel POST. During Vollebak's 2026-07 sale the incoming rate exceeded SQLite's
// single-writer throughput, every pooled connection (connection_limit=8) ended
// up parked behind the file lock, and the merchant-facing app starved with
// P1008 timeouts. The pixel traffic wedged the dashboard.
//
// Fix: the route enqueues rows here and returns immediately. A single flusher
// drains the buffers with `createMany` - hundreds of pixel hits collapse into
// one write transaction, which is the shape single-writer SQLite handles well.
//
// Durability trade-off (accepted): rows buffered in RAM are lost if the process
// dies before a flush. Journey touches are additive best-effort analytics, and
// the loss window is bounded by FLUSH_MS (plus a SIGTERM/SIGINT flush for
// graceful deploys). Order links are rarer and stitch-critical, so enqueueing
// one schedules an immediate flush instead of waiting out the interval.
//
// All mutable state lives on globalThis - Vite/Remix server builds can load a
// module twice, and per-module state would silently fork the buffer
// (see perf_shared_cache_gotcha).

import db from "../db.server";

const FLUSH_MS = 3000; // max staleness of a buffered row (= max loss window)
const FLUSH_ROWS = 500; // drain early when a buffer gets this deep
const MAX_BUFFER = 10000; // hard cap per buffer; beyond this we shed load

function state() {
  if (!globalThis.__lucidlyJourneyIngest) {
    globalThis.__lucidlyJourneyIngest = {
      touches: [],
      orderLinks: [],
      flushing: false,
      dropped: 0,
      timer: null,
      hooked: false,
    };
  }
  return globalThis.__lucidlyJourneyIngest;
}

export function enqueueTouch(row) {
  const s = state();
  if (s.touches.length >= MAX_BUFFER) {
    s.dropped++;
    return; // shed load rather than grow RAM without bound
  }
  s.touches.push(row);
  ensureFlusher();
  if (s.touches.length >= FLUSH_ROWS) void flush();
}

export function enqueueOrderLink(row) {
  const s = state();
  if (s.orderLinks.length >= MAX_BUFFER) {
    s.dropped++;
    return;
  }
  s.orderLinks.push(row);
  ensureFlusher();
  // Order links are the stitch-critical rows (one per checkout, low volume) -
  // flush now instead of waiting out the interval, shrinking their loss window
  // to the flush duration itself.
  void flush();
}

async function flush() {
  const s = state();
  if (s.flushing) return; // single-flight; the interval retries what this run misses
  if (!s.touches.length && !s.orderLinks.length) return;
  s.flushing = true;

  const touches = s.touches;
  const orderLinks = s.orderLinks;
  s.touches = [];
  s.orderLinks = [];

  try {
    if (orderLinks.length) await db.journeyOrderLink.createMany({ data: orderLinks });
    if (touches.length) await db.journeyTouch.createMany({ data: touches });
    if (s.dropped) {
      console.warn(`[JourneyIngest] shed ${s.dropped} row(s) while buffer was full`);
      s.dropped = 0;
    }
  } catch (err) {
    // Transient writer contention (P1008 etc): put the rows back if there's
    // room so the next interval retries them; otherwise shed and count.
    console.error(`[JourneyIngest] flush failed (${touches.length} touches, ${orderLinks.length} links): ${err?.message || err}`);
    if (s.touches.length + touches.length <= MAX_BUFFER) s.touches.unshift(...touches);
    else s.dropped += touches.length;
    if (s.orderLinks.length + orderLinks.length <= MAX_BUFFER) s.orderLinks.unshift(...orderLinks);
    else s.dropped += orderLinks.length;
  } finally {
    s.flushing = false;
  }
}

function ensureFlusher() {
  const s = state();
  if (!s.timer) {
    s.timer = setInterval(() => void flush(), FLUSH_MS);
    // Don't let the flusher hold an otherwise-exiting process open.
    s.timer.unref?.();
  }
  if (!s.hooked) {
    s.hooked = true;
    // Graceful deploys (Fly sends SIGINT/SIGTERM on machine stop): drain the
    // buffers before exiting so a routine deploy loses nothing.
    for (const sig of ["SIGTERM", "SIGINT"]) {
      process.once(sig, () => {
        flush().finally(() => process.exit(0));
      });
    }
  }
}
