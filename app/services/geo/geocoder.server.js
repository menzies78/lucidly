// Server-side geocoder. Resolves (countryCode, city) pairs to [lat, lng]
// using a static cities5000 dump from geonames (~68k cities, pop >= 5000).
// Falls back to the country centroid when the city is missing or unmatched.
//
// Module-level state is loaded lazily on first call so server boot stays
// fast and tests that don't need geocoding don't pay the JSON parse cost.
import fs from "fs";
import path from "path";
import url from "url";
import { COUNTRY_CENTROIDS } from "./countryCentroids.js";

const __filename = url.fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

let CITY_INDEX = null;

/** Lazy-load and index the cities dump. Index shape:
 *   Map<countryCode, Map<normalizedCity, [lat, lng]>>
 * The index is kept on globalThis so Vite's dev mode HMR doesn't reload the
 * 3MB JSON on every request. */
function loadIndex() {
  if (CITY_INDEX) return CITY_INDEX;
  if (globalThis.__lucidlyCityIndex) {
    CITY_INDEX = globalThis.__lucidlyCityIndex;
    return CITY_INDEX;
  }
  const file = path.join(__dirname, "cities.json");
  const raw = JSON.parse(fs.readFileSync(file, "utf8"));
  const idx = new Map();
  for (const [cc, rows] of Object.entries(raw)) {
    const m = new Map();
    for (const [name, asciiName, lat, lng] of rows) {
      const k1 = normalizeCity(name);
      const k2 = normalizeCity(asciiName);
      if (k1 && !m.has(k1)) m.set(k1, [lat, lng]);
      if (k2 && k2 !== k1 && !m.has(k2)) m.set(k2, [lat, lng]);
    }
    idx.set(cc, m);
  }
  CITY_INDEX = idx;
  globalThis.__lucidlyCityIndex = idx;
  return idx;
}

/** Normalize a city name for matching. Lowercase, strip diacritics, drop
 * non-alphanumeric characters, collapse whitespace, drop common
 * "saint"/"st"/"st." prefixes since merchants spell those inconsistently. */
export function normalizeCity(s) {
  if (!s) return "";
  return s
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/^st\.?\s+/, "saint ")
    .replace(/[^a-z0-9 ]/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

/** Resolve a (countryCode, city) pair to [lat, lng].
 * Returns the country centroid when city lookup fails, and null when neither
 * the country code nor the city is recognised. */
export function geocodeCity(countryCode, city) {
  if (!countryCode) return null;
  const cc = countryCode.toUpperCase();
  if (city) {
    const idx = loadIndex();
    const m = idx.get(cc);
    if (m) {
      const norm = normalizeCity(city);
      const hit = m.get(norm);
      if (hit) return [hit[0], hit[1]];
    }
  }
  const centroid = COUNTRY_CENTROIDS[cc];
  if (centroid) return [centroid[0], centroid[1]];
  return null;
}

/** Batch helper. Takes [{countryCode, city}, ...] and returns
 * [{lat, lng, source: "city"|"country"|null}, ...] preserving order. Source
 * is exposed so callers can light up "approximate" markers differently. */
export function geocodeBatch(rows) {
  const idx = loadIndex();
  const out = [];
  for (const r of rows) {
    const cc = (r.countryCode || "").toUpperCase();
    if (!cc) { out.push({ lat: null, lng: null, source: null }); continue; }
    if (r.city) {
      const m = idx.get(cc);
      if (m) {
        const hit = m.get(normalizeCity(r.city));
        if (hit) { out.push({ lat: hit[0], lng: hit[1], source: "city" }); continue; }
      }
    }
    const centroid = COUNTRY_CENTROIDS[cc];
    if (centroid) { out.push({ lat: centroid[0], lng: centroid[1], source: "country" }); continue; }
    out.push({ lat: null, lng: null, source: null });
  }
  return out;
}
