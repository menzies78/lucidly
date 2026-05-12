// Worker thread entry for the attribution matcher.
//
// One persistent worker handles many days. The main thread (matcher.server.js)
// loads all source data once, builds per-day contexts, and posts them via
// parentPort.postMessage({ type: "match-day", ctx }). This worker replies with
// the day result so the main thread can serialise the SQLite writes.
//
// Why workers instead of async parallelism: the exhaustive backtracker is
// CPU-bound (up to 120 s per ad). Running multiple days "in parallel" on the
// same event loop gains nothing — only worker_threads spread the work across
// real cores. On Fly's 4 vCPU VM we run 3 workers and keep the main thread
// free to do DB writes + book-keeping.
//
// The worker imports matcherCore.server.js directly from source. matcherCore
// has no DB or Prisma dependency, so Node's native ESM loader can load it
// without needing the Vite-bundled build/server output.

import { parentPort, workerData } from "node:worker_threads";
import { matchDay } from "./matcherCore.server.js";

// Re-hydrate plain objects into the shapes matchDay expects. Worker messages
// arrive as structured-cloned plain values, but Date instances + Set survive.
// We accept dayCountries as an array (easier to serialise) and rebuild Set.
function hydrateCtx(ctx) {
  const dayCountries = ctx.dayCountries instanceof Set
    ? ctx.dayCountries
    : new Set(ctx.dayCountries || []);
  const usedOrderIds = ctx.usedOrderIds instanceof Set
    ? ctx.usedOrderIds
    : new Set(ctx.usedOrderIds || []);
  // Order.createdAt must be a Date for the matcher (uses .getUTCHours etc).
  // structured-clone preserves Date, but if a caller serialised over JSON
  // we'd get strings — defensive rehydrate.
  const dayOrders = ctx.dayOrders.map(o => ({
    ...o,
    createdAt: o.createdAt instanceof Date ? o.createdAt : new Date(o.createdAt),
  }));
  return { ...ctx, dayCountries, usedOrderIds, dayOrders };
}

parentPort.on("message", (msg) => {
  if (!msg || msg.type !== "match-day") return;
  const { ctx, jobId } = msg;
  try {
    const result = matchDay(hydrateCtx(ctx));
    parentPort.postMessage({ type: "match-day-result", jobId, result });
  } catch (err) {
    parentPort.postMessage({
      type: "match-day-error",
      jobId,
      day: ctx?.day,
      error: { message: err?.message || String(err), stack: err?.stack || "" },
    });
  }
});

// Optional: announce ready. Main thread doesn't currently wait on this but
// it's useful for debugging worker startup latency.
parentPort.postMessage({ type: "ready", workerId: workerData?.workerId ?? null });
