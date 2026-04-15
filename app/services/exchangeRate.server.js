import db from "../db.server";

/**
 * Exchange rate service using ECB SDMX API (primary source, no API key, works from cloud).
 * All ECB rates are EUR-denominated. Cross-rates computed as: toCurrency_per_EUR / fromCurrency_per_EUR.
 * Fallback: most recent cached rate from DB (no API call).
 *
 * Caching: in-memory Map + Prisma ExchangeRate table.
 * Weekend/holiday gaps filled with previous business day's rate (ECB convention).
 */

const memoryCache = {};

function cacheKey(date, from, to) {
  return `${date}|${from}|${to}`;
}

/**
 * Get a single exchange rate for one date. Checks memory → DB → ECB API → fallback.
 * Used by incremental sync (small number of dates).
 */
export async function getExchangeRate(dateStr, fromCurrency, toCurrency) {
  if (fromCurrency === toCurrency) return 1.0;

  const key = cacheKey(dateStr, fromCurrency, toCurrency);
  if (memoryCache[key]) return memoryCache[key];

  // DB cache
  const dateObj = new Date(dateStr + "T00:00:00.000Z");
  const cached = await db.exchangeRate.findUnique({
    where: { date_fromCurrency_toCurrency: { date: dateObj, fromCurrency, toCurrency } },
  });
  if (cached) {
    memoryCache[key] = cached.rate;
    return cached.rate;
  }

  // Fetch from ECB for a small window around the requested date (covers weekends)
  try {
    const rates = await fetchEcbRates(fromCurrency, toCurrency, dateStr, dateStr);
    if (rates[dateStr]) {
      await saveRatesToDb([{ date: dateStr, rate: rates[dateStr] }], fromCurrency, toCurrency);
      memoryCache[key] = rates[dateStr];
      return rates[dateStr];
    }
    // Date might be weekend/holiday — ECB returns nothing. Use nearest previous.
    return await fallbackToLatest(dateStr, fromCurrency, toCurrency);
  } catch (err) {
    console.error(`[ExchangeRate] ECB fetch failed for ${dateStr}: ${err.message}`);
    return await fallbackToLatest(dateStr, fromCurrency, toCurrency);
  }
}

/**
 * Bulk pre-fetch exchange rates for a list of dates.
 * Single ECB API call per 365-day chunk. Returns { 'YYYY-MM-DD': rate }.
 * Weekend/holiday dates get previous business day's rate.
 *
 * @param {Function} onProgress - optional callback(message) for progress reporting
 */
export async function prefetchExchangeRates(dates, fromCurrency, toCurrency, onProgress) {
  if (fromCurrency === toCurrency) {
    const result = {};
    for (const d of dates) result[d] = 1.0;
    return result;
  }

  if (!dates.length) return {};

  const sorted = [...dates].sort();
  const result = {};

  if (onProgress) onProgress(`Loading cached rates from DB...`);

  // 1. Load all DB-cached rates in one query
  const startDate = new Date(sorted[0] + "T00:00:00.000Z");
  const endDate = new Date(sorted[sorted.length - 1] + "T00:00:00.000Z");
  const cached = await db.exchangeRate.findMany({
    where: { fromCurrency, toCurrency, date: { gte: startDate, lte: endDate } },
  });

  const cachedByDate = {};
  for (const c of cached) {
    const ymd = c.date.toISOString().split("T")[0];
    cachedByDate[ymd] = c.rate;
    memoryCache[cacheKey(ymd, fromCurrency, toCurrency)] = c.rate;
  }

  // Find which dates we still need
  const missing = [];
  for (const d of sorted) {
    const mk = cacheKey(d, fromCurrency, toCurrency);
    if (memoryCache[mk]) {
      result[d] = memoryCache[mk];
    } else if (cachedByDate[d]) {
      result[d] = cachedByDate[d];
      memoryCache[mk] = cachedByDate[d];
    } else {
      missing.push(d);
    }
  }

  if (!missing.length) {
    console.log(`[ExchangeRate] All ${dates.length} rates from cache`);
    if (onProgress) onProgress(`All ${dates.length} exchange rates from cache`);
    return result;
  }

  console.log(`[ExchangeRate] ${cached.length} cached, ${missing.length} to fetch from ECB`);
  if (onProgress) onProgress(`${cached.length} cached, fetching ${missing.length} rates from ECB...`);

  // 2. Fetch missing dates from ECB in year-sized chunks
  const CHUNK_DAYS = 365;

  for (let i = 0; i < missing.length; i += CHUNK_DAYS) {
    const chunk = missing.slice(i, i + CHUNK_DAYS);
    const chunkStart = chunk[0];
    const chunkEnd = chunk[chunk.length - 1];
    const chunkNum = Math.floor(i / CHUNK_DAYS) + 1;
    const totalChunks = Math.ceil(missing.length / CHUNK_DAYS);

    if (onProgress) onProgress(`Fetching exchange rates: chunk ${chunkNum}/${totalChunks} (${chunkStart} → ${chunkEnd})`);

    try {
      const ecbRates = await fetchEcbRates(fromCurrency, toCurrency, chunkStart, chunkEnd);

      // ECB only returns business days — fill calendar gaps with previous rate
      let lastRate = null;
      const allChunkDates = generateDateRange(chunkStart, chunkEnd);

      for (const ymd of allChunkDates) {
        if (ecbRates[ymd]) lastRate = ecbRates[ymd];
        if (lastRate) {
          result[ymd] = lastRate;
          memoryCache[cacheKey(ymd, fromCurrency, toCurrency)] = lastRate;
        }
      }

      // Batch save new rates to DB
      const toSave = allChunkDates
        .filter(ymd => result[ymd] && !cachedByDate[ymd])
        .map(ymd => ({ date: ymd, rate: result[ymd] }));

      if (toSave.length) {
        await saveRatesToDb(toSave, fromCurrency, toCurrency);
        console.log(`[ExchangeRate] Saved ${toSave.length} rates to DB`);
      }

      if (onProgress) onProgress(`Exchange rates: chunk ${chunkNum}/${totalChunks} done (${Object.keys(ecbRates).length} business days)`);
    } catch (err) {
      console.error(`[ExchangeRate] ECB chunk ${chunkStart}..${chunkEnd} failed: ${err.message}`);
      if (onProgress) onProgress(`Exchange rate API error — using cached fallback rates`);

      // Fill gaps with nearest available rate
      for (const d of chunk) {
        if (!result[d]) result[d] = findNearestRate(d, result);
      }
    }
  }

  // Fill any remaining gaps with DB fallback
  for (const d of dates) {
    if (!result[d]) {
      result[d] = await fallbackToLatest(d, fromCurrency, toCurrency);
    }
  }

  console.log(`[ExchangeRate] Pre-fetched ${Object.keys(result).length} rates for ${fromCurrency}→${toCurrency}`);
  return result;
}

// ── ECB SDMX API ──────────────────────────────────────────────────────────────

/**
 * Fetch cross-rates from ECB SDMX CSV endpoint.
 * ECB publishes rates as "X per 1 EUR". For fromCurrency→toCurrency:
 *   rate = toCurrency_per_EUR / fromCurrency_per_EUR
 *
 * Special case: if either currency IS EUR, only one series is needed.
 * Returns { 'YYYY-MM-DD': rate } for business days only.
 */
async function fetchEcbRates(fromCurrency, toCurrency, startDate, endDate) {
  const isFromEur = fromCurrency === "EUR";
  const isToEur = toCurrency === "EUR";

  // Build currency list for API call
  const currencies = new Set();
  if (!isFromEur) currencies.add(fromCurrency);
  if (!isToEur) currencies.add(toCurrency);
  const currencyParam = [...currencies].join("+");

  const url = `https://data-api.ecb.europa.eu/service/data/EXR/D.${currencyParam}.EUR.SP00.A?format=csvdata&startPeriod=${startDate}&endPeriod=${endDate}&detail=dataonly`;
  console.log(`[ExchangeRate] ECB: ${startDate}..${endDate} ${fromCurrency}→${toCurrency}`);

  const { withRetry } = await import("./retry.server.js");
  const csv = await withRetry(async () => {
    const res = await fetch(url);
    if (!res.ok) {
      const body = await res.text().catch(() => "");
      throw new Error(`ECB API status ${res.status}: ${body.slice(0, 200)}`);
    }
    return res.text();
  }, "ExchangeRate/ECB");
  const lines = csv.trim().split(/\r?\n/);
  if (lines.length < 2) return {};

  // Parse CSV: columns are KEY,FREQ,CURRENCY,CURRENCY_DENOM,EXR_TYPE,EXR_SUFFIX,TIME_PERIOD,OBS_VALUE
  const header = lines[0].split(",");
  const currencyIdx = header.indexOf("CURRENCY");
  const dateIdx = header.indexOf("TIME_PERIOD");
  const valueIdx = header.indexOf("OBS_VALUE");

  // Collect rates by currency and date: { 'USD': { '2026-03-30': 1.1484 }, 'GBP': { ... } }
  const byCurrency = {};
  for (let i = 1; i < lines.length; i++) {
    const cols = lines[i].split(",");
    const cur = cols[currencyIdx];
    const date = cols[dateIdx];
    const value = parseFloat(cols[valueIdx]);
    if (!cur || !date || isNaN(value)) continue;
    if (!byCurrency[cur]) byCurrency[cur] = {};
    byCurrency[cur][date] = value;
  }

  // Compute cross-rates
  const result = {};

  if (isFromEur) {
    // EUR→X: rate is just X_per_EUR directly
    const toRates = byCurrency[toCurrency] || {};
    for (const [date, value] of Object.entries(toRates)) {
      result[date] = Math.round(value * 1e6) / 1e6;
    }
  } else if (isToEur) {
    // X→EUR: rate is 1 / X_per_EUR
    const fromRates = byCurrency[fromCurrency] || {};
    for (const [date, value] of Object.entries(fromRates)) {
      if (value !== 0) result[date] = Math.round((1 / value) * 1e6) / 1e6;
    }
  } else {
    // Cross-rate: toCurrency_per_EUR / fromCurrency_per_EUR
    const fromRates = byCurrency[fromCurrency] || {};
    const toRates = byCurrency[toCurrency] || {};
    for (const date of Object.keys(fromRates)) {
      if (toRates[date] && fromRates[date] !== 0) {
        result[date] = Math.round((toRates[date] / fromRates[date]) * 1e6) / 1e6;
      }
    }
  }

  console.log(`[ExchangeRate] ECB returned ${Object.keys(result).length} business day rates`);
  return result;
}

// ── Helpers ───────────────────────────────────────────────────────────────────

function generateDateRange(startStr, endStr) {
  const dates = [];
  const d0 = new Date(startStr + "T00:00:00Z");
  const d1 = new Date(endStr + "T00:00:00Z");
  for (let d = new Date(d0); d <= d1; d.setUTCDate(d.getUTCDate() + 1)) {
    dates.push(d.toISOString().split("T")[0]);
  }
  return dates;
}

function findNearestRate(dateStr, existingResults) {
  const sorted = Object.keys(existingResults).sort();
  let best = null;
  let bestDist = Infinity;
  const target = new Date(dateStr).getTime();
  for (const d of sorted) {
    if (!existingResults[d] || existingResults[d] === 1.0) continue;
    const dist = Math.abs(new Date(d).getTime() - target);
    if (dist < bestDist) {
      bestDist = dist;
      best = d;
    }
  }
  if (best) {
    memoryCache[cacheKey(dateStr, existingResults.__from, existingResults.__to)] = existingResults[best];
    return existingResults[best];
  }
  return null;
}

async function saveRatesToDb(entries, fromCurrency, toCurrency) {
  const BATCH = 100;
  for (let b = 0; b < entries.length; b += BATCH) {
    const batch = entries.slice(b, b + BATCH);
    await db.$transaction(
      batch.map(({ date, rate }) =>
        db.exchangeRate.upsert({
          where: { date_fromCurrency_toCurrency: { date: new Date(date + "T00:00:00.000Z"), fromCurrency, toCurrency } },
          create: { date: new Date(date + "T00:00:00.000Z"), fromCurrency, toCurrency, rate },
          update: { rate },
        })
      )
    );
  }
}

/**
 * Fallback: use the most recent cached rate from DB. No API call.
 * For incremental sync this is typically yesterday's rate — close enough.
 */
async function fallbackToLatest(dateStr, fromCurrency, toCurrency) {
  const latest = await db.exchangeRate.findFirst({
    where: { fromCurrency, toCurrency },
    orderBy: { date: "desc" },
  });
  if (latest) {
    const rate = latest.rate;
    const fromDate = latest.date.toISOString().split("T")[0];
    console.log(`[ExchangeRate] Fallback: using ${fromDate} rate (${rate}) for ${dateStr}`);
    memoryCache[cacheKey(dateStr, fromCurrency, toCurrency)] = rate;
    return rate;
  }
  console.error(`[ExchangeRate] No fallback rate for ${fromCurrency}→${toCurrency}, using 1.0`);
  return 1.0;
}

/**
 * Converts monetary fields on a Meta data object from metaCurrency to shopifyCurrency.
 * Mutates the object in place. Only converts if currencies differ.
 */
export function convertMetaFields(row, rate) {
  if (rate === 1.0) return row;
  row.spend = Math.round(row.spend * rate * 100) / 100;
  row.conversionValue = Math.round(row.conversionValue * rate * 100) / 100;
  if (row.cpc !== undefined) row.cpc = Math.round(row.cpc * rate * 100) / 100;
  if (row.cpm !== undefined) row.cpm = Math.round(row.cpm * rate * 100) / 100;
  return row;
}
