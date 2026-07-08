// Order-webhook ingest queue - decouples Shopify webhook delivery from the
// SQLite writer, the same pattern as journeyIngest.server.js.
//
// Why this exists: the orders/create + orders/updated routes used to run
// processOrderWebhook synchronously inside the request. Every delivery is a
// blocking multi-write (order upsert, line items, customer upsert), so any
// time the single SQLite writer was held - most visibly the hourly cycle's
// wipe-and-replace rollup rebuild around :50 past the hour - deliveries piled
// up on the connection pool, timed out (P1008), and Shopify's redelivery
// retries amplified a 2-minute stall into a visible outage (2026-07-08,
// mid-fulfilment of Vollebak's sale orders).
//
// Fix: the routes ack 200 immediately and enqueue the payload here. A single
// serialized drain loop applies them one at a time - one orderly stream to
// the writer instead of N concurrent upserts fighting for the lock. Fast acks
// also stop Shopify's retry amplification at the source.
//
// Dedupe: queue is keyed by shop+orderId and the LATEST payload wins (bulk
// fulfilment/refund ops fire orders/updated for the same order repeatedly in
// seconds; only the newest state needs applying - processOrderWebhook is a
// state-overwrite upsert, not an event log). isCreate is OR'd across collapsed
// events so a create+update pair still sets customerOrderCountAtPurchase.
//
// Durability: unlike journey touches, order data matters - so items that fail
// (e.g. writer still busy) are retried with backoff instead of shed. If the
// process dies with items queued, Shopify's own redelivery (we only suppress
// it by acking; undelivered work acked-but-lost is re-sent as the NEXT update
// for active orders) plus the sync layer's self-healing recover the state.
//
// State lives on globalThis - Vite/Remix server builds can load a module twice
// (see perf_shared_cache_gotcha).

import { processOrderWebhook } from "./orderWebhook.server.js";

const MAX_QUEUE = 5000; // ~payloads are a few KB; 5k is tens of MB worst case
const MAX_ATTEMPTS = 3;
const RETRY_DELAY_MS = 5000; // writer-busy stalls clear in seconds-to-minutes
const DRAIN_IDLE_MS = 250; // poll gap when retry-delayed items are waiting

function state() {
  if (!globalThis.__lucidlyOrderWebhookQueue) {
    globalThis.__lucidlyOrderWebhookQueue = {
      queue: new Map(), // key -> { shop, payload, isCreate, attempts, notBefore }
      draining: false,
      dropped: 0,
    };
  }
  return globalThis.__lucidlyOrderWebhookQueue;
}

export function enqueueOrderWebhook(shop, payload, isCreate) {
  const s = state();
  const orderId = payload?.id ? String(payload.id) : null;
  // No order id = can't dedupe or process meaningfully; use a unique key so it
  // still flows through and fails visibly in the drain loop.
  const key = orderId ? `${shop}:${orderId}` : `${shop}:nokey:${Date.now()}:${Math.random()}`;

  const existing = s.queue.get(key);
  if (existing) {
    // Collapse onto the newest payload; keep isCreate if ANY collapsed event
    // was a create (it gates customerOrderCountAtPurchase). Reset attempts -
    // fresh payload, fresh chances.
    s.queue.set(key, { shop, payload, isCreate: isCreate || existing.isCreate, attempts: 0, notBefore: 0 });
  } else {
    if (s.queue.size >= MAX_QUEUE) {
      // Shed the oldest entry - later webhooks for it will re-carry the state,
      // and the sync layer self-heals residual gaps.
      const oldestKey = s.queue.keys().next().value;
      s.queue.delete(oldestKey);
      s.dropped++;
    }
    s.queue.set(key, { shop, payload, isCreate, attempts: 0, notBefore: 0 });
  }
  void drain();
}

async function drain() {
  const s = state();
  if (s.draining) return;
  s.draining = true;
  try {
    while (s.queue.size > 0) {
      const now = Date.now();
      // First eligible item in insertion order (Map preserves it).
      let picked = null;
      for (const [key, item] of s.queue) {
        if (item.notBefore <= now) { picked = [key, item]; break; }
      }
      if (!picked) {
        // Everything queued is waiting out a retry delay.
        await new Promise((r) => setTimeout(r, DRAIN_IDLE_MS));
        continue;
      }
      const [key, item] = picked;
      // Remove before processing: a webhook arriving for the same order DURING
      // processing must become a fresh queue entry (its payload is newer than
      // the one we're mid-applying), not be silently collapsed into it.
      s.queue.delete(key);

      try {
        await processOrderWebhook(item.shop, item.payload, item.isCreate);
      } catch (err) {
        item.attempts++;
        if (item.attempts < MAX_ATTEMPTS) {
          item.notBefore = Date.now() + RETRY_DELAY_MS * item.attempts;
          // Re-enqueue only if no NEWER payload arrived for this order while
          // we were processing - the newer state supersedes this one.
          if (!s.queue.has(key)) s.queue.set(key, item);
        } else {
          console.error(`[WebhookQueue] giving up on ${key} after ${MAX_ATTEMPTS} attempts: ${err?.message || err}`);
        }
      }

      if (s.dropped) {
        console.warn(`[WebhookQueue] shed ${s.dropped} queued webhook(s) while at capacity`);
        s.dropped = 0;
      }
    }
  } finally {
    s.draining = false;
    // Closing race: an enqueue that saw draining=true just before we flipped it
    // off would strand its item until the next webhook. Re-check.
    if (s.queue.size > 0) void drain();
  }
}
