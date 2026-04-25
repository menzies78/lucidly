// Build a compact server-side cities lookup from a geonames cities5000.txt
// dump. Run once per refresh; the resulting JSON is committed into the repo
// so production never reaches out to geonames.
//
//   node app/services/geo/buildCitiesJson.mjs /tmp/cities5000.txt
//
// Output: app/services/geo/cities.json with shape:
//   { "GB": [ ["London", "London", 51.50853, -0.12574], ... ], ... }
//
// Each row is [name, asciiName, lat, lng]. asciiName is included so we can
// match shopify-supplied city strings even when they're transliterated.

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const inputPath = process.argv[2];
if (!inputPath) {
  console.error("usage: node buildCitiesJson.mjs <cities5000.txt>");
  process.exit(1);
}

const out = {};
let count = 0;
const text = fs.readFileSync(inputPath, "utf8");
for (const line of text.split("\n")) {
  if (!line) continue;
  const cols = line.split("\t");
  if (cols.length < 9) continue;
  const name = cols[1];
  const asciiName = cols[2];
  const lat = parseFloat(cols[4]);
  const lng = parseFloat(cols[5]);
  const cc = cols[8];
  if (!cc || !Number.isFinite(lat) || !Number.isFinite(lng)) continue;
  if (!out[cc]) out[cc] = [];
  out[cc].push([name, asciiName, lat, lng]);
  count++;
}
const outPath = path.join(__dirname, "cities.json");
fs.writeFileSync(outPath, JSON.stringify(out));
console.log(`Wrote ${count} cities to ${outPath} (${(fs.statSync(outPath).size / 1024 / 1024).toFixed(2)} MB)`);
