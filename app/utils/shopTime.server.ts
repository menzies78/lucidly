// Shop-local time helpers.
//
// Storage stays UTC everywhere. These helpers only decide which shop-local
// day a UTC instant belongs to, and produce UTC bounds for a shop-local
// day or range. Built on Intl.DateTimeFormat so DST is handled correctly.
//
// All shopLocalDayKey outputs are ISO YYYY-MM-DD strings. All *Bounds
// outputs are real Date instants (UTC under the hood).

type Tz = string | null | undefined;

function tzOrUtc(tz: Tz): string {
  return tz && tz.length > 0 ? tz : "UTC";
}

// Intl.DateTimeFormat construction is ~100x slower than the .format() call
// itself, so we cache one formatter per timezone. This matters a lot in
// report loaders that bucket thousands of orders in a tight loop.
const dayFormatterCache = new Map<string, Intl.DateTimeFormat>();
const offsetFormatterCache = new Map<string, Intl.DateTimeFormat>();
const weekdayFormatterCache = new Map<string, Intl.DateTimeFormat>();

// en-CA produces YYYY-MM-DD reliably across runtimes.
function formatter(tz: string): Intl.DateTimeFormat {
  let f = dayFormatterCache.get(tz);
  if (!f) {
    f = new Intl.DateTimeFormat("en-CA", {
      timeZone: tz,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
    });
    dayFormatterCache.set(tz, f);
  }
  return f;
}

// Returns the shop-local day key ("YYYY-MM-DD") for a given UTC instant.
export function shopLocalDayKey(tz: Tz, utc: Date): string {
  return formatter(tzOrUtc(tz)).format(utc);
}

// Returns today's shop-local day key.
export function shopLocalToday(tz: Tz): string {
  return shopLocalDayKey(tz, new Date());
}

// Returns { gte, lte } UTC instants bracketing the shop-local day.
// gte = 00:00:00.000 shop-local; lte = 23:59:59.999 shop-local.
// DST-safe: we probe the target tz offset at noon on that local day and
// apply it, then snap to day bounds.
export function shopDayBounds(tz: Tz, localDay: string): { gte: Date; lte: Date } {
  const zone = tzOrUtc(tz);
  const gte = zonedLocalToUtc(zone, localDay, 0, 0, 0, 0);
  const lte = zonedLocalToUtc(zone, localDay, 23, 59, 59, 999);
  return { gte, lte };
}

// Returns { gte, lte } bracketing [fromLocalDay 00:00 .. toLocalDay 23:59:59.999].
export function shopRangeBounds(
  tz: Tz,
  fromLocalDay: string,
  toLocalDay: string,
): { gte: Date; lte: Date } {
  const zone = tzOrUtc(tz);
  return {
    gte: zonedLocalToUtc(zone, fromLocalDay, 0, 0, 0, 0),
    lte: zonedLocalToUtc(zone, toLocalDay, 23, 59, 59, 999),
  };
}

// Monday-anchored week start for a shop-local day (ISO week).
export function shopLocalWeekMonday(tz: Tz, localDay: string): string {
  const zone = tzOrUtc(tz);
  // Pick noon of that local day to avoid any midnight DST edge.
  const noonUtc = zonedLocalToUtc(zone, localDay, 12, 0, 0, 0);
  const dow = zonedDayOfWeek(zone, noonUtc); // 0=Sun..6=Sat
  const mondayOffset = dow === 0 ? 6 : dow - 1;
  const [y, m, d] = localDay.split("-").map(Number);
  // Do day arithmetic in UTC on an anchor date, then re-project through the
  // formatter so we get the right local YYYY-MM-DD.
  const anchor = new Date(Date.UTC(y, m - 1, d, 12, 0, 0, 0));
  anchor.setUTCDate(anchor.getUTCDate() - mondayOffset);
  return shopLocalDayKey(zone, anchor);
}

// --- internals -----------------------------------------------------------

// Returns the UTC Date that corresponds to the given wall-clock time
// (y-m-d H:M:S.ms) interpreted in the given IANA timezone.
function zonedLocalToUtc(
  tz: string,
  localDay: string,
  hh: number,
  mm: number,
  ss: number,
  ms: number,
): Date {
  const [y, m, d] = localDay.split("-").map(Number);
  // First pass: treat as if UTC, then adjust by the tz offset at that instant.
  const asUtc = Date.UTC(y, m - 1, d, hh, mm, ss, ms);
  const offset1 = tzOffsetMs(tz, new Date(asUtc));
  // Second pass catches DST transitions where offset1 differs from the
  // offset actually applicable at the target wall-clock time.
  const offset2 = tzOffsetMs(tz, new Date(asUtc - offset1));
  return new Date(asUtc - offset2);
}

// Offset in milliseconds that must be SUBTRACTED from a "UTC-formatted"
// wall clock to get the real UTC instant. I.e. for Europe/London in BST,
// this returns +3600000.
function tzOffsetMs(tz: string, at: Date): number {
  let dtf = offsetFormatterCache.get(tz);
  if (!dtf) {
    dtf = new Intl.DateTimeFormat("en-US", {
      timeZone: tz,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hour12: false,
    });
    offsetFormatterCache.set(tz, dtf);
  }
  const parts = dtf.formatToParts(at);
  const get = (t: string) => Number(parts.find((p) => p.type === t)?.value);
  let hour = get("hour");
  if (hour === 24) hour = 0; // en-US quirk
  const asIfUtc = Date.UTC(
    get("year"),
    get("month") - 1,
    get("day"),
    hour,
    get("minute"),
    get("second"),
  );
  // Intl drops sub-second precision; compare against at-truncated-to-second
  // so the offset math isn't polluted by ms drift.
  const atSec = Math.floor(at.getTime() / 1000) * 1000;
  return asIfUtc - atSec;
}

function zonedDayOfWeek(tz: string, at: Date): number {
  let dtf = weekdayFormatterCache.get(tz);
  if (!dtf) {
    dtf = new Intl.DateTimeFormat("en-US", { timeZone: tz, weekday: "short" });
    weekdayFormatterCache.set(tz, dtf);
  }
  const weekday = dtf.format(at);
  const map: Record<string, number> = {
    Sun: 0, Mon: 1, Tue: 2, Wed: 3, Thu: 4, Fri: 5, Sat: 6,
  };
  return map[weekday] ?? 0;
}
