// Manual overrides for the name-based gender inference layer.
//
// The upstream `gender-detection-from-name` package is intentionally
// conservative — when *any* cross-population exists in its curated lists
// (e.g. Christine vs. Chris) it returns "unknown" rather than guessing.
// That's the right default, but for downstream analytics it leaves
// ~20% of customers uncategorised on common names that are >95% one
// gender in practice. It also has zero coverage for several markets we
// ship to (Japan, Korea, Netherlands, …).
//
// We layer overrides in three buckets, checked in this order inside
// `inferGender`:
//
//   1. COUNTRY  — country-qualified entries, key = "name|CC".
//                 Used for markets the package doesn't cover (JP, KR, NL).
//   2. PLAIN    — country-agnostic entries; common names the package
//                 marks "unknown" but which have a strong gender skew
//                 in our data.
//   3. TITLES   — salutations typed into the firstName field.
//                 Detected as a leading token; used to extract the real
//                 first name AND as a fallback gender when the rest is
//                 unidentifiable (e.g. "Mr Smith" → male via title).
//
// All lookups use lowercased ASCII; country codes are uppercased.

// ---------------------------------------------------------------------------
// Title prefixes — surprisingly common in firstName fields.
// ---------------------------------------------------------------------------
export const TITLES = {
  mr: "male",
  sir: "male",
  master: "male",
  mister: "male",
  mrs: "female",
  ms: "female",
  miss: "female",
  madam: "female",
  madame: "female",
  // "dr", "prof", "mx" are deliberately NOT mapped — unisex.
};

// ---------------------------------------------------------------------------
// PLAIN — names the package returns "unknown" for that have a clear
// (>95%) real-world skew in English-speaking markets. Lowercase keys.
// Add cautiously: anything genuinely unisex (Alex, Sam, Jordan, Jamie)
// must NOT be added here.
// ---------------------------------------------------------------------------
export const PLAIN = {
  // Male — pulled from the top of the Vollebak ambiguous bucket where the
  // name is overwhelmingly male in EN/US/UK census data.
  chris: "male",
  ryan: "male",
  adam: "male",
  justin: "male",
  neil: "male",
  nathan: "male",
  dan: "male",
  aaron: "male",
  keith: "male",
  kyle: "male",
  tyler: "male",
  evan: "male",
  jesse: "male",
  tony: "male",
  glenn: "male",
  shane: "male",
  spencer: "male",
  lawrence: "male",
  philippe: "male",
  thomas: "male",
  // Common male nicknames the package may not catch
  matt: "male",
  steve: "male",
  mike: "male",
  rich: "male",
  rob: "male",
  jeff: "male",
  nick: "male",
  jake: "male",
  ben: "male",
  joe: "male",
  // Female — rarer in the ambiguous bucket but worth seeding
  // (kept short — only obvious cases)
  sue: "female",
  liz: "female",
  beth: "female",
};

// ---------------------------------------------------------------------------
// COUNTRY — markets the package doesn't ship name lists for.
// Key format: "name|CC". Conservative seed lists; extend over time.
// ---------------------------------------------------------------------------
export const COUNTRY = {
  // -------------------------------------------------------------------------
  // Japan (JP) — clearly gendered names only. Avoids known unisex names
  // like Yuki, Akira, Hikaru, Kaoru, Tsubasa, Makoto.
  // -------------------------------------------------------------------------
  // Male
  "hiroshi|JP": "male",
  "takashi|JP": "male",
  "kenji|JP": "male",
  "daisuke|JP": "male",
  "kazuki|JP": "male",
  "yuto|JP": "male",
  "haruto|JP": "male",
  "sota|JP": "male",
  "riku|JP": "male",
  "ryo|JP": "male",
  "naoto|JP": "male",
  "takeshi|JP": "male",
  "masaru|JP": "male",
  "toshio|JP": "male",
  "kazuo|JP": "male",
  "kenta|JP": "male",
  "shinji|JP": "male",
  "hideo|JP": "male",
  "hiroyuki|JP": "male",
  "akihiro|JP": "male",
  "kazuya|JP": "male",
  "yusuke|JP": "male",
  "tetsuya|JP": "male",
  "tatsuya|JP": "male",
  "koichi|JP": "male",
  "kenichi|JP": "male",
  "shinichi|JP": "male",
  "yoshio|JP": "male",
  "tadashi|JP": "male",
  "noboru|JP": "male",
  "shigeru|JP": "male",
  "satoshi|JP": "male",
  "ryuji|JP": "male",
  "ryota|JP": "male",
  "shota|JP": "male",
  "yuma|JP": "male",
  "ren|JP": "male",
  "sho|JP": "male",
  "atsushi|JP": "male",
  "masaki|JP": "male",
  // Female
  "yuko|JP": "female",
  "yumiko|JP": "female",
  "akiko|JP": "female",
  "naoko|JP": "female",
  "kumiko|JP": "female",
  "mariko|JP": "female",
  "sachiko|JP": "female",
  "tomoko|JP": "female",
  "hiroko|JP": "female",
  "miyuki|JP": "female",
  "keiko|JP": "female",
  "eiko|JP": "female",
  "reiko|JP": "female",
  "setsuko|JP": "female",
  "yoshiko|JP": "female",
  "junko|JP": "female",
  "misako|JP": "female",
  "yumi|JP": "female",
  "naomi|JP": "female",
  "mai|JP": "female",
  "saki|JP": "female",
  "aoi|JP": "female",
  "sakura|JP": "female",
  "mei|JP": "female",
  "mio|JP": "female",
  "yuka|JP": "female",
  "megumi|JP": "female",
  "risa|JP": "female",
  "ayaka|JP": "female",
  "asami|JP": "female",
  "haruka|JP": "female",
  "asuka|JP": "female",
  "ayumi|JP": "female",
  "chika|JP": "female",
  "emi|JP": "female",
  "etsuko|JP": "female",
  "fumiko|JP": "female",
  "kana|JP": "female",
  "kaori|JP": "female",
  "mami|JP": "female",
  "michiko|JP": "female",
  "midori|JP": "female",
  "natsumi|JP": "female",
  "rie|JP": "female",
  "sayaka|JP": "female",
  "tomomi|JP": "female",
  "yui|JP": "female",
  "yukari|JP": "female",

  // -------------------------------------------------------------------------
  // Netherlands (NL) — overlaps with Belgium (BE-NL); duplicated below.
  // -------------------------------------------------------------------------
  "wouter|NL": "male",
  "bram|NL": "male",
  "jeroen|NL": "male",
  "maarten|NL": "male",
  "roel|NL": "male",
  "sven|NL": "male",
  "joris|NL": "male",
  "stijn|NL": "male",
  "sjoerd|NL": "male",
  "hendrik|NL": "male",
  "bart|NL": "male",
  "pieter|NL": "male",
  "sander|NL": "male",
  "tijs|NL": "male",
  "ruud|NL": "male",
  "floris|NL": "male",
  "lennart|NL": "male",
  "lars|NL": "male",
  "coen|NL": "male",
  "daan|NL": "male",
  "mees|NL": "male",
  "hugo|NL": "male",
  "niels|NL": "male",
  "jasper|NL": "male",
  "casper|NL": "male",
  "thijs|NL": "male",
  "rens|NL": "male",
  "siem|NL": "male",
  "gijs|NL": "male",
  "willem|NL": "male",
  "matthijs|NL": "male",
  "rik|NL": "male",
  "remco|NL": "male",
  // Female
  "saskia|NL": "female",
  "marieke|NL": "female",
  "sanne|NL": "female",
  "femke|NL": "female",
  "annelies|NL": "female",
  "inge|NL": "female",
  "joke|NL": "female",
  "ineke|NL": "female",
  "marja|NL": "female",
  "annika|NL": "female",
  "eline|NL": "female",
  "lieke|NL": "female",
  "lotte|NL": "female",
  "maartje|NL": "female",
  "mirjam|NL": "female",
  "tessa|NL": "female",
  "anouk|NL": "female",
  "britt|NL": "female",
  "esmee|NL": "female",
  "iris|NL": "female",
  "janneke|NL": "female",
  "marlies|NL": "female",
  "fleur|NL": "female",
  "noor|NL": "female",
  "evi|NL": "female",
  "sofie|NL": "female",
  "merel|NL": "female",
  "roos|NL": "female",
  "willemijn|NL": "female",

  // -------------------------------------------------------------------------
  // Korea (KR) — kept tight. Many Korean names are genuinely unisex
  // (Min, Sung, Hyun, Joon, Jae) and Romanisation varies across sources,
  // so this list only includes spellings with clear gender skew.
  // -------------------------------------------------------------------------
  "minho|KR": "male",
  "sungmin|KR": "male",
  "junho|KR": "male",
  "hyunwoo|KR": "male",
  "sangwoo|KR": "male",
  "jaewon|KR": "male",
  "donghyuk|KR": "male",
  "jaehyun|KR": "male",
  "junseo|KR": "male",
  "taehyung|KR": "male",
  "jinhyuk|KR": "male",
  "seunghyun|KR": "male",
  "kyungsoo|KR": "male",
  "youngho|KR": "male",
  "donghoon|KR": "male",
  "hyejin|KR": "female",
  "eunji|KR": "female",
  "yeji|KR": "female",
  "soyeon|KR": "female",
  "hyewon|KR": "female",
  "jihye|KR": "female",
  "jiyeon|KR": "female",
  "minji|KR": "female",
  "yejin|KR": "female",
  "sumi|KR": "female",
  "jisoo|KR": "female",
  "soojin|KR": "female",
  "yuna|KR": "female",
  "hayoung|KR": "female",
  "sooyoung|KR": "female",
};

// Allow overrides to apply to closely related markets without
// duplicating every entry. NL entries also apply to BE; nothing else
// fans out today but the door is open.
export const COUNTRY_ALIASES = {
  BE: ["NL"], // Flemish Belgium uses largely the same names as NL
};
