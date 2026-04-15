// Server-side date range parser — used by all report loaders

export interface DateRange {
  fromDate: Date;
  toDate: Date;
  compareFrom: Date | null;
  compareTo: Date | null;
  hasComparison: boolean;
  compareLabel: string;
}

function startOfDay(d: Date): Date {
  const copy = new Date(d);
  copy.setUTCHours(0, 0, 0, 0);
  return copy;
}

function endOfDay(d: Date): Date {
  const copy = new Date(d);
  copy.setUTCHours(23, 59, 59, 999);
  return copy;
}

function computePresetRange(preset: string, today: Date): { from: Date; to: Date } {
  // "today" preset includes today; all others end at yesterday
  const yesterday = new Date(today);
  yesterday.setUTCDate(today.getUTCDate() - 1);

  if (preset === "today") {
    return { from: new Date(today), to: new Date(today) };
  }

  if (preset === "yesterday") {
    return { from: new Date(yesterday), to: new Date(yesterday) };
  }

  const to = new Date(yesterday);
  const from = new Date(yesterday);

  switch (preset) {
    case "last7":
      from.setUTCDate(to.getUTCDate() - 6);
      break;
    case "last14":
      from.setUTCDate(to.getUTCDate() - 13);
      break;
    case "last30":
      from.setUTCDate(to.getUTCDate() - 29);
      break;
    case "last90":
      from.setUTCDate(to.getUTCDate() - 89);
      break;
    case "last365":
      from.setUTCDate(to.getUTCDate() - 364);
      break;
    case "thisWeek": {
      const dow = yesterday.getUTCDay();
      const mondayOffset = dow === 0 ? 6 : dow - 1;
      from.setUTCDate(yesterday.getUTCDate() - mondayOffset);
      break;
    }
    case "lastWeek": {
      const dow2 = yesterday.getUTCDay();
      const mondayOffset2 = dow2 === 0 ? 6 : dow2 - 1;
      const thisMonday = new Date(yesterday);
      thisMonday.setUTCDate(yesterday.getUTCDate() - mondayOffset2);
      const lastMonday = new Date(thisMonday);
      lastMonday.setUTCDate(thisMonday.getUTCDate() - 7);
      const lastSunday = new Date(thisMonday);
      lastSunday.setUTCDate(thisMonday.getUTCDate() - 1);
      return { from: lastMonday, to: lastSunday };
    }
    case "thisMonth":
      from.setUTCFullYear(today.getUTCFullYear(), today.getUTCMonth(), 1);
      break;
    case "lastMonth": {
      const lastMonth = new Date(Date.UTC(today.getUTCFullYear(), today.getUTCMonth() - 1, 1));
      const lastDay = new Date(Date.UTC(today.getUTCFullYear(), today.getUTCMonth(), 0));
      return { from: lastMonth, to: lastDay };
    }
    case "thisYear":
      from.setUTCFullYear(today.getUTCFullYear(), 0, 1);
      break;
    case "lastYear": {
      const yearStart = new Date(Date.UTC(today.getUTCFullYear() - 1, 0, 1));
      const yearEnd = new Date(Date.UTC(today.getUTCFullYear() - 1, 11, 31));
      return { from: yearStart, to: yearEnd };
    }
    case "all":
      from.setUTCFullYear(2020, 0, 1);
      break;
    default:
      from.setUTCDate(to.getUTCDate() - 29);
  }

  return { from, to };
}

function parseCookieDateRange(request: Request): { from: string | null; to: string | null; preset: string; compare: string } {
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

export function parseDateRange(request: Request): DateRange {
  const url = new URL(request.url);
  let fromParam = url.searchParams.get("from");
  let toParam = url.searchParams.get("to");
  let preset = url.searchParams.get("preset") || "";
  let compare = url.searchParams.get("compare") || "none";

  // Fall back to cookie if no URL params
  if (!fromParam && !toParam && !preset) {
    const cookie = parseCookieDateRange(request);
    if (cookie.from || cookie.preset) {
      console.log(`[DateRange] Restored from cookie: from=${cookie.from} to=${cookie.to} preset=${cookie.preset}`);
    }
    fromParam = cookie.from;
    toParam = cookie.to;
    preset = cookie.preset;
    compare = cookie.compare;
  }

  const today = new Date();
  today.setUTCHours(0, 0, 0, 0);

  let fromDate: Date;
  let toDate: Date;

  if (preset) {
    // Preset always recomputes fresh dates (e.g. "today" must reflect actual today)
    const range = computePresetRange(preset, today);
    fromDate = range.from;
    toDate = range.to;
  } else if (fromParam && toParam) {
    fromDate = new Date(fromParam + "T00:00:00Z");
    toDate = new Date(toParam + "T00:00:00Z");
  } else {
    // Default: last 30 days ending yesterday
    const yesterday = new Date(today);
    yesterday.setUTCDate(today.getUTCDate() - 1);
    toDate = new Date(yesterday);
    fromDate = new Date(yesterday);
    fromDate.setUTCDate(yesterday.getUTCDate() - 29);
  }

  fromDate = startOfDay(fromDate);
  toDate = endOfDay(toDate);

  let compareFrom: Date | null = null;
  let compareTo: Date | null = null;
  let compareLabel = "";

  if (compare === "previous") {
    const rangeMs = toDate.getTime() - fromDate.getTime();
    compareTo = new Date(fromDate.getTime() - 1);
    compareFrom = new Date(compareTo.getTime() - rangeMs);
    compareFrom = startOfDay(compareFrom);
    compareTo = endOfDay(compareTo);
    compareLabel = "vs previous period";
  } else if (compare === "yoy") {
    compareFrom = new Date(fromDate);
    compareFrom.setUTCFullYear(compareFrom.getUTCFullYear() - 1);
    compareTo = new Date(toDate);
    compareTo.setUTCFullYear(compareTo.getUTCFullYear() - 1);
    compareFrom = startOfDay(compareFrom);
    compareTo = endOfDay(compareTo);
    compareLabel = "vs same period last year";
  }

  return {
    fromDate,
    toDate,
    compareFrom,
    compareTo,
    hasComparison: compare !== "none",
    compareLabel,
  };
}
