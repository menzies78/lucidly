// Server-side date range parser — used by all report loaders.
//
// All day boundaries are computed in the shop's timezone (falls back to
// UTC if the shop has no timezone set). The returned `fromDate` / `toDate`
// are real UTC instants (start of shop-local fromKey / end of shop-local
// toKey) and can be fed directly into Prisma `gte` / `lte` filters.
//
// `fromKey` / `toKey` are the shop-local YYYY-MM-DD strings — handy for
// keys in in-memory maps.

import {
  shopLocalDayKey,
  shopLocalToday,
  shopLocalWeekMonday,
  shopRangeBounds,
} from "./shopTime.server";

export interface DateRange {
  fromDate: Date;
  toDate: Date;
  fromKey: string;
  toKey: string;
  compareFrom: Date | null;
  compareTo: Date | null;
  compareFromKey: string | null;
  compareToKey: string | null;
  hasComparison: boolean;
  compareLabel: string;
  timeZone: string;
}

type Tz = string | null | undefined;

function addDaysKey(tz: Tz, key: string, delta: number): string {
  // Do arithmetic on a shop-local noon anchor so DST can't flip the result.
  const [y, m, d] = key.split("-").map(Number);
  const anchor = new Date(Date.UTC(y, m - 1, d, 12, 0, 0, 0));
  anchor.setUTCDate(anchor.getUTCDate() + delta);
  return shopLocalDayKey(tz, anchor);
}

function computePresetRange(
  preset: string,
  todayKey: string,
  tz: Tz,
): { fromKey: string; toKey: string } {
  const yesterdayKey = addDaysKey(tz, todayKey, -1);

  if (preset === "today") return { fromKey: todayKey, toKey: todayKey };
  if (preset === "yesterday") return { fromKey: yesterdayKey, toKey: yesterdayKey };

  switch (preset) {
    case "last7":
      return { fromKey: addDaysKey(tz, yesterdayKey, -6), toKey: yesterdayKey };
    case "last14":
      return { fromKey: addDaysKey(tz, yesterdayKey, -13), toKey: yesterdayKey };
    case "last30":
      return { fromKey: addDaysKey(tz, yesterdayKey, -29), toKey: yesterdayKey };
    case "last90":
      return { fromKey: addDaysKey(tz, yesterdayKey, -89), toKey: yesterdayKey };
    case "last365":
      return { fromKey: addDaysKey(tz, yesterdayKey, -364), toKey: yesterdayKey };
    case "thisWeek": {
      const monday = shopLocalWeekMonday(tz, yesterdayKey);
      return { fromKey: monday, toKey: yesterdayKey };
    }
    case "lastWeek": {
      const thisMonday = shopLocalWeekMonday(tz, yesterdayKey);
      const lastMonday = addDaysKey(tz, thisMonday, -7);
      const lastSunday = addDaysKey(tz, thisMonday, -1);
      return { fromKey: lastMonday, toKey: lastSunday };
    }
    case "thisMonth": {
      const [y, m] = todayKey.split("-").map(Number);
      return { fromKey: `${y}-${String(m).padStart(2, "0")}-01`, toKey: yesterdayKey };
    }
    case "lastMonth": {
      const [y, m] = todayKey.split("-").map(Number);
      const prevMonthAnchor = new Date(Date.UTC(y, m - 2, 1, 12, 0, 0, 0));
      const prevY = prevMonthAnchor.getUTCFullYear();
      const prevM = prevMonthAnchor.getUTCMonth() + 1;
      const fromKey = `${prevY}-${String(prevM).padStart(2, "0")}-01`;
      // Last day of prev month = day 0 of this month
      const lastDayAnchor = new Date(Date.UTC(y, m - 1, 0, 12, 0, 0, 0));
      const toKey = shopLocalDayKey(tz, lastDayAnchor);
      return { fromKey, toKey };
    }
    case "thisYear": {
      const [y] = todayKey.split("-").map(Number);
      return { fromKey: `${y}-01-01`, toKey: yesterdayKey };
    }
    case "lastYear": {
      const [y] = todayKey.split("-").map(Number);
      return { fromKey: `${y - 1}-01-01`, toKey: `${y - 1}-12-31` };
    }
    case "all":
      return { fromKey: "2020-01-01", toKey: yesterdayKey };
    default:
      return { fromKey: addDaysKey(tz, yesterdayKey, -29), toKey: yesterdayKey };
  }
}

function parseCookieDateRange(request: Request): {
  from: string | null;
  to: string | null;
  preset: string;
  compare: string;
} {
  const cookieHeader = request.headers.get("Cookie") || "";
  const match = cookieHeader.match(/lucidly_date=([^;]+)/);
  if (!match) return { from: null, to: null, preset: "", compare: "none" };
  try {
    const data = JSON.parse(decodeURIComponent(match[1]));
    return {
      from: data.from || null,
      to: data.to || null,
      preset: data.preset || "",
      compare: data.compare || "none",
    };
  } catch {
    return { from: null, to: null, preset: "", compare: "none" };
  }
}

export function parseDateRange(request: Request, tz: Tz = "UTC"): DateRange {
  const url = new URL(request.url);
  let fromParam = url.searchParams.get("from");
  let toParam = url.searchParams.get("to");
  let preset = url.searchParams.get("preset") || "";
  let compare = url.searchParams.get("compare") || "none";

  if (!fromParam && !toParam && !preset) {
    const cookie = parseCookieDateRange(request);
    if (cookie.from || cookie.preset) {
      console.log(
        `[DateRange] Restored from cookie: from=${cookie.from} to=${cookie.to} preset=${cookie.preset}`,
      );
    }
    fromParam = cookie.from;
    toParam = cookie.to;
    preset = cookie.preset;
    compare = cookie.compare;
  }

  const todayKey = shopLocalToday(tz);

  let fromKey: string;
  let toKey: string;

  if (preset) {
    const range = computePresetRange(preset, todayKey, tz);
    fromKey = range.fromKey;
    toKey = range.toKey;
  } else if (fromParam && toParam) {
    fromKey = fromParam;
    toKey = toParam;
  } else {
    const yesterdayKey = addDaysKey(tz, todayKey, -1);
    fromKey = addDaysKey(tz, yesterdayKey, -29);
    toKey = yesterdayKey;
  }

  const { gte: fromDate, lte: toDate } = shopRangeBounds(tz, fromKey, toKey);

  let compareFrom: Date | null = null;
  let compareTo: Date | null = null;
  let compareFromKey: string | null = null;
  let compareToKey: string | null = null;
  let compareLabel = "";

  if (compare === "previous") {
    // Inclusive span length in shop-local days.
    const spanDays =
      Math.round(
        (Date.UTC(
          ...(toKey.split("-").map(Number) as [number, number, number]),
        ) -
          Date.UTC(
            ...(fromKey.split("-").map(Number) as [number, number, number]),
          )) /
          86400000,
      ) + 1;
    compareToKey = addDaysKey(tz, fromKey, -1);
    compareFromKey = addDaysKey(tz, compareToKey, -(spanDays - 1));
    const b = shopRangeBounds(tz, compareFromKey, compareToKey);
    compareFrom = b.gte;
    compareTo = b.lte;
    compareLabel = "vs previous period";
  } else if (compare === "yoy") {
    const [fy, fm, fd] = fromKey.split("-").map(Number);
    const [ty, tm, td] = toKey.split("-").map(Number);
    compareFromKey = `${fy - 1}-${String(fm).padStart(2, "0")}-${String(fd).padStart(2, "0")}`;
    compareToKey = `${ty - 1}-${String(tm).padStart(2, "0")}-${String(td).padStart(2, "0")}`;
    const b = shopRangeBounds(tz, compareFromKey, compareToKey);
    compareFrom = b.gte;
    compareTo = b.lte;
    compareLabel = "vs same period last year";
  }

  return {
    fromDate,
    toDate,
    fromKey,
    toKey,
    compareFrom,
    compareTo,
    compareFromKey,
    compareToKey,
    hasComparison: compare !== "none",
    compareLabel,
    timeZone: tz || "UTC",
  };
}
