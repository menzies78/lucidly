// Normalises a raw Meta /activities event into the shape we store in
// MetaChange. The dispatch table below intentionally maps Meta's many
// event_type values into a small, scannable category set — filtering +
// chart annotations rely on there being ~10 categories, not ~40.
//
// Anything we don't recognise falls into "other" with a best-effort
// summary. The raw payload is always persisted alongside so we can
// reclassify later without re-fetching.

// Category codes in sync order of importance (used for chart-annotation
// dot rendering when a day has multiple events).
export const CATEGORIES = [
  "launched",
  "killed",
  "paused",
  "resumed",
  "budget",
  "creative",
  "targeting",
  "optimisation",
  "schedule",
  "other",
];

const CATEGORY_RULES = [
  // Lifecycle
  { match: /^create_(campaign|ad_set|adset|ad)$/, category: "launched",
    summary: (o) => `${titleCase(o.objectType)} created` },
  { match: /^delete_(campaign|ad_set|adset|ad)$/, category: "killed",
    summary: (o) => `${titleCase(o.objectType)} deleted` },
  { match: /^archive_(campaign|ad_set|adset|ad)$/, category: "killed",
    summary: (o) => `${titleCase(o.objectType)} archived` },
  { match: /^pause_(campaign|ad_set|adset|ad)$/, category: "paused",
    summary: (o) => `${titleCase(o.objectType)} paused` },
  { match: /^unpause_(campaign|ad_set|adset|ad)$/, category: "resumed",
    summary: (o) => `${titleCase(o.objectType)} resumed` },
  { match: /^(update|change)_(campaign|ad_set|adset|ad)_run_status/, category: "paused",
    summary: (o) => `Run status changed${deltaSuffix(o)}` },

  // Budget + bid
  { match: /budget/i, category: "budget",
    summary: (o) => `Budget changed${deltaSuffix(o, { money: true })}` },
  { match: /bid_(amount|strategy|cap)|roas_target|cost_cap/i, category: "optimisation",
    summary: (o) => `Bid strategy changed${deltaSuffix(o)}` },
  { match: /optimization_goal|optimisation_goal/i, category: "optimisation",
    summary: (o) => `Optimisation goal changed${deltaSuffix(o)}` },
  { match: /attribution_(window|setting)|attribution_spec/i, category: "optimisation",
    summary: (o) => `Attribution setting changed${deltaSuffix(o)}` },

  // Creative
  { match: /_creative|creative_id|creative_changed/i, category: "creative",
    summary: () => "Creative swapped" },
  { match: /_(headline|body|call_to_action|cta|link_url|display_url|image|video)/i, category: "creative",
    summary: (o) => `Creative ${describeField(o.rawEventType)} changed` },

  // Targeting / audience
  { match: /_targeting|audience|geo_location|age_min|age_max|genders|placements|publisher_platforms|devices/i,
    category: "targeting",
    summary: (o) => `Targeting ${describeField(o.rawEventType)} changed` },

  // Schedule
  { match: /start_time|end_time|stop_time|schedule/i, category: "schedule",
    summary: (o) => `Schedule changed${deltaSuffix(o)}` },

  // Name / label tweaks are noise — group under "other"
  { match: /_name/i, category: "other",
    summary: (o) => `Renamed${deltaSuffix(o)}` },
];

// Given a raw event from /activities, produce a stored MetaChange row
// (minus id/createdAt which the DB generates).
export function classifyEvent(raw, shopDomain) {
  const rawEventType = String(raw.event_type || "unknown");
  const objectType = mapObjectType(raw.object_type || rawEventType);
  const objectId = String(raw.object_id || "");
  const objectName = String(raw.object_name || "");
  const actorId = raw.actor_id ? String(raw.actor_id) : null;
  const actorName = raw.actor_name ? String(raw.actor_name) : null;

  // Meta sometimes delivers timestamps as seconds-since-epoch, sometimes as ISO.
  const eventTime = parseEventTime(raw.event_time);

  // Extract old/new from extra_data. Meta wraps these differently per type —
  // fall back to JSON.stringify so we always have something for the UI.
  const { oldValue, newValue } = extractDelta(raw);

  const ctx = { objectType, rawEventType, oldValue, newValue };
  let category = "other";
  let summary = describeField(rawEventType);

  for (const rule of CATEGORY_RULES) {
    if (rule.match.test(rawEventType)) {
      category = rule.category;
      summary = rule.summary(ctx) || summary;
      break;
    }
  }

  return {
    shopDomain,
    eventTime,
    category,
    rawEventType,
    objectType,
    objectId,
    objectName,
    actorId,
    actorName,
    oldValue: oldValue ?? null,
    newValue: newValue ?? null,
    summary,
    rawPayload: JSON.stringify(raw),
  };
}

// ── internals ────────────────────────────────────────────────────────────

function parseEventTime(v) {
  if (!v) return new Date();
  if (typeof v === "number") return new Date(v * 1000);
  const d = new Date(v);
  return isNaN(d.getTime()) ? new Date() : d;
}

function mapObjectType(t) {
  if (!t) return "account";
  const s = String(t).toLowerCase();
  if (s.includes("campaign")) return "campaign";
  if (s.includes("adset") || s.includes("ad_set")) return "adset";
  if (s.includes("ad")) return "ad";
  return "account";
}

function extractDelta(raw) {
  const xd = raw.extra_data;
  // extra_data is a JSON string in most /activities responses.
  if (!xd) return { oldValue: null, newValue: null };
  let parsed;
  if (typeof xd === "string") {
    try { parsed = JSON.parse(xd); } catch { return { oldValue: null, newValue: null }; }
  } else {
    parsed = xd;
  }
  const oldValue = parsed.old_value ?? parsed.old ?? null;
  const newValue = parsed.new_value ?? parsed.new ?? null;
  return {
    oldValue: oldValue == null ? null : stringify(oldValue),
    newValue: newValue == null ? null : stringify(newValue),
  };
}

function stringify(v) {
  if (typeof v === "string") return v;
  if (typeof v === "number" || typeof v === "boolean") return String(v);
  try { return JSON.stringify(v); } catch { return String(v); }
}

function titleCase(s) {
  if (!s) return "";
  return s[0].toUpperCase() + s.slice(1);
}

function describeField(rawEventType) {
  if (!rawEventType) return "Updated";
  // update_adset_budget → "budget", update_ad_creative → "creative"
  const parts = rawEventType.split("_");
  return parts.slice(-1)[0] || rawEventType;
}

// If extra_data gave us old/new, append a ": old → new" suffix — money-aware
// when the caller marks it.
function deltaSuffix({ oldValue, newValue }, { money = false } = {}) {
  if (oldValue == null && newValue == null) return "";
  const format = (v) => {
    if (v == null) return "—";
    if (!money) return v;
    // Meta budgets are typically in minor units (cents/pence), but raw JSON
    // in /activities isn't always — we render as-is and let the UI decide.
    return v;
  };
  return `: ${format(oldValue)} → ${format(newValue)}`;
}
