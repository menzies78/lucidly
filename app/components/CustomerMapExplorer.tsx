/**
 * Customer Map Explorer.
 *
 * Interactive zoomable world map of every geocoded customer, with cohort
 * filters that mirror Product Demographics Explorer. Built on react-leaflet
 * + supercluster — clusters at low zoom collapse into individual city dots
 * as the user zooms in. Tile layer uses CartoDB Positron for the clean
 * flat-vector look from the brief.
 *
 * Data is loader-blob-driven: customerMapBlob.points is an array of every
 * geocoded customer with cohort flags pre-computed at rollup time. All
 * filtering happens client-side against this array.
 *
 * SSR-safe: react-leaflet imports break on the server because Leaflet
 * touches `window`. We gate the map on `typeof window !== "undefined"` and
 * lazy-import the modules inside a useEffect. Until that fires, we render
 * the filter bar + a placeholder so Remix hydrates cleanly.
 */
import React, { useEffect, useMemo, useState } from "react";
import { Card, BlockStack, Text } from "@shopify/polaris";

const AGE_BRACKETS = ["13-17", "18-24", "25-34", "35-44", "45-54", "55-64", "65+"];

type SegCode = "m" | "r" | "o";
type Scope = "metaAcquired" | "allMeta" | "all";
type RecencyBand = "any" | "active" | "dormant" | "lapsed";
type OrderBand = "any" | "1" | "2-3" | "4+";

// Compact wire shape — keys are single letters to keep the JSON blob small.
// Server side: app/services/customerRollups.server.js builds this.
export interface MapPoint {
  i: string;                       // customer id
  la: number;                      // lat
  lo: number;                      // lng
  s: SegCode;                      // segment: m=metaNew, r=metaRetargeted, o=organic
  g: "f" | "m" | null;             // gender: f=female, m=male
  a: string | null;                // age bracket
  c: string | null;                // ISO country code
  t: string | null;                // city ("town")
  $: number;                       // total revenue
  r: number;                       // total refunded
  n: number;                       // order count
  d: number | null;                // days since last order
  x: 0 | 1;                        // approx geocode (country centroid)
  p: 0 | 1;                        // bought on discount ever
  v: 0 | 5 | 10 | 20;              // VIP band (0 = none)
  h: 0 | 1;                        // highest-refunds flag
  pr: number[];                    // distinct product indices into MapBlob.productList
}

export interface MapGem {
  kind: "vip" | "discount" | "refund" | "repeat" | "lapsed" | "product";
  country: string;
  count: number;
  expected: number;
  lift: number;
  filters: {
    country?: string;
    vip?: "any" | "top5" | "top10" | "top20";
    pricing?: "any" | "discount" | "fullPrice";
    refundsTop?: boolean;
    orderBand?: OrderBand;
    recency?: RecencyBand;
    product?: string;
  };
}

export interface MapBlob {
  points: MapPoint[];
  thresholds: { top5: number; top10: number; top20: number };
  highestRefundThreshold: number;
  productList: string[];
  gems: MapGem[];
  computedAt: string;
}

// Browser-side country code → display name. Falls back to the code itself
// if Intl.DisplayNames is unavailable or the code is unknown.
const countryDisplay: (code: string | null) => string | null = (() => {
  if (typeof Intl === "undefined" || !(Intl as any).DisplayNames) {
    return (code) => code;
  }
  let dn: any;
  try {
    dn = new (Intl as any).DisplayNames(["en"], { type: "region" });
  } catch {
    return (code) => code;
  }
  return (code) => {
    if (!code) return null;
    try { return dn.of(code) || code; } catch { return code; }
  };
})();

interface Props {
  blob: MapBlob | null;
  cs: string;
  /** Protomaps API key (referrer-restricted, safe in client). When null we
   *  fall back to the CARTO Voyager raster basemap — fine for dev, NOT
   *  acceptable for App Store launch (CARTO's CDN is fair-use only). */
  protomapsKey?: string | null;
}

export default function CustomerMapExplorer({ blob, cs, protomapsKey = null }: Props) {
  const [scope, setScope] = useState<Scope>("metaAcquired");
  const [gender, setGender] = useState<"All" | "Female" | "Male">("All");
  const [ages, setAges] = useState<string[]>([]);
  const [vip, setVip] = useState<"any" | "top5" | "top10" | "top20">("any");
  const [pricing, setPricing] = useState<"any" | "discount" | "fullPrice">("any");
  const [refundsTop, setRefundsTop] = useState<boolean>(false);
  const [orderBand, setOrderBand] = useState<OrderBand>("any");
  const [recency, setRecency] = useState<RecencyBand>("any");
  // Country/product filters have no permanent UI controls — they're set
  // by clicking a Gem and surfaced as removable chips. Country picker was
  // intentionally removed; product picker is below.
  const [country, setCountry] = useState<string>("All");
  const [product, setProduct] = useState<string>("All");
  const [productSearch, setProductSearch] = useState<string>("");

  // Normalise pr (product indices) on read so older blobs persisted before
  // the productList rollout don't crash the filter passes. We mutate in
  // place rather than .map+spread because a 30k-customer merchant would
  // pay 30k allocations otherwise.
  const points = useMemo(() => {
    const raw: MapPoint[] = blob?.points || [];
    for (const p of raw) { if (!p.pr) p.pr = []; }
    return raw;
  }, [blob]);
  const productList = blob?.productList || [];
  const gems = blob?.gems || [];

  // Demographic filters apply to Meta Acquired only — Meta hasn't given
  // us age/gender for retargeted or organic shoppers. When the user
  // changes scope away from metaAcquired we leave the filter values
  // untouched but the UI greys them out and they no-op in the filter
  // pass. This keeps state stable across scope toggles.
  const isMetaAcquired = scope === "metaAcquired";

  const segmentSet = useMemo<Set<SegCode>>(() => {
    if (scope === "metaAcquired") return new Set(["m"]);
    if (scope === "allMeta") return new Set(["m", "r"]);
    return new Set(["m", "r", "o"]);
  }, [scope]);

  // Resolve product filter to an index into productList. The dropdown stores
  // the raw product name; we look up the index once per change so the inner
  // loop can do a fast Array.includes on numbers.
  const productIdx = useMemo(() => {
    if (product === "All") return -1;
    return productList.indexOf(product);
  }, [product, productList]);

  // Open-text product search → indices that match. Computed once per
  // search-string change. Trim + lowercase substring match.
  const productSearchIndices = useMemo(() => {
    const q = productSearch.trim().toLowerCase();
    if (!q) return null;
    const set = new Set<number>();
    productList.forEach((name, i) => {
      if (name.toLowerCase().includes(q)) set.add(i);
    });
    return set;
  }, [productSearch, productList]);

  const filtered = useMemo(() => {
    return points.filter((p) => {
      if (!segmentSet.has(p.s)) return false;
      if (isMetaAcquired) {
        if (gender === "Female" && p.g !== "f") return false;
        if (gender === "Male" && p.g !== "m") return false;
        if (ages.length > 0 && (!p.a || !ages.includes(p.a))) return false;
      }
      if (country !== "All" && p.c !== country) return false;
      if (productIdx >= 0 && !p.pr.includes(productIdx)) return false;
      if (productSearchIndices) {
        let any = false;
        for (const i of p.pr) { if (productSearchIndices.has(i)) { any = true; break; } }
        if (!any) return false;
      }
      if (vip === "top5" && p.v !== 5) return false;
      if (vip === "top10" && (p.v !== 5 && p.v !== 10)) return false;
      if (vip === "top20" && p.v === 0) return false;
      if (pricing === "discount" && p.p !== 1) return false;
      if (pricing === "fullPrice" && p.p !== 0) return false;
      if (refundsTop && p.h !== 1) return false;
      if (orderBand === "1" && p.n !== 1) return false;
      if (orderBand === "2-3" && (p.n < 2 || p.n > 3)) return false;
      if (orderBand === "4+" && p.n < 4) return false;
      if (recency !== "any") {
        const d = p.d;
        if (d == null) return false;
        if (recency === "active" && d > 90) return false;
        if (recency === "dormant" && (d <= 90 || d > 365)) return false;
        if (recency === "lapsed" && d <= 365) return false;
      }
      return true;
    });
  }, [points, segmentSet, isMetaAcquired, gender, ages, country, productIdx, productSearchIndices, vip, pricing, refundsTop, orderBand, recency]);

  const availableAges = useMemo(() => {
    const set = new Set<string>();
    if (!isMetaAcquired) return set;
    for (const p of points) if (p.a && segmentSet.has(p.s)) set.add(p.a);
    return set;
  }, [points, segmentSet, isMetaAcquired]);

  const totalCount = useMemo(() => {
    let c = 0;
    for (const p of points) if (segmentSet.has(p.s)) c++;
    return c;
  }, [points, segmentSet]);

  const countryCount = useMemo(() => {
    const set = new Set<string>();
    for (const p of filtered) if (p.c) set.add(p.c);
    return set.size;
  }, [filtered]);

  const topCities = useMemo(() => {
    const agg: Record<string, { city: string; country: string | null; customers: number; revenue: number; lat: number; lng: number; }> = {};
    for (const p of filtered) {
      const key = `${p.c}|${p.t || "(unknown)"}`;
      if (!agg[key]) agg[key] = { city: p.t || "(unknown)", country: countryDisplay(p.c), customers: 0, revenue: 0, lat: p.la, lng: p.lo };
      agg[key].customers += 1;
      agg[key].revenue += (p.$ - p.r);
    }
    return Object.values(agg).sort((a, b) => b.customers - a.customers).slice(0, 15);
  }, [filtered]);

  const activeFilterCount =
    (scope !== "metaAcquired" ? 0 : 0) // scope itself is not counted
    + (gender !== "All" ? 1 : 0)
    + (ages.length > 0 ? 1 : 0)
    + (country !== "All" ? 1 : 0)
    + (product !== "All" ? 1 : 0)
    + (productSearch.trim() !== "" ? 1 : 0)
    + (vip !== "any" ? 1 : 0)
    + (pricing !== "any" ? 1 : 0)
    + (refundsTop ? 1 : 0)
    + (orderBand !== "any" ? 1 : 0)
    + (recency !== "any" ? 1 : 0);

  const clearAll = () => {
    setGender("All"); setAges([]); setCountry("All");
    setProduct("All"); setProductSearch("");
    setVip("any"); setPricing("any"); setRefundsTop(false);
    setOrderBand("any"); setRecency("any");
  };

  // Apply a Gem's filter set in one pass. Resets fields the gem doesn't
  // touch so the user sees a clean slice — chasing one anomaly at a time.
  const applyGem = (g: MapGem) => {
    const f = g.filters;
    setGender("All"); setAges([]);
    setCountry(f.country || "All");
    setProduct(f.product || "All");
    setProductSearch("");
    setVip(f.vip || "any");
    setPricing(f.pricing || "any");
    setRefundsTop(f.refundsTop === true);
    setOrderBand(f.orderBand || "any");
    setRecency(f.recency || "any");
  };

  // Product picker options — show the full product list once we have one,
  // sorted alphabetically. Cap at the products that actually have any
  // customers in the active scope, so the list shrinks for All Meta etc.
  const productOptions = useMemo(() => {
    const set = new Set<number>();
    for (const p of points) {
      if (!segmentSet.has(p.s)) continue;
      for (const idx of p.pr) set.add(idx);
    }
    return Array.from(set).map((i) => productList[i]).filter(Boolean).sort();
  }, [points, segmentSet, productList]);

  const toggleAge = (b: string) => {
    setAges((curr) => (curr.includes(b) ? curr.filter((a) => a !== b) : [...curr, b]));
  };

  const labelStyle: React.CSSProperties = {
    fontSize: "12px", fontWeight: 600, color: "#6B7280",
    width: "92px", textTransform: "uppercase", letterSpacing: "0.5px",
  };

  const pillStyle = (active: boolean, disabled = false): React.CSSProperties => ({
    padding: "6px 12px", fontSize: "12px", fontWeight: 600,
    borderRadius: "6px", cursor: disabled ? "not-allowed" : "pointer",
    background: active ? "#7C3AED" : "#fff",
    color: active ? "#fff" : disabled ? "#D1D5DB" : "#4B5563",
    border: `1px solid ${active ? "#7C3AED" : "#E5E7EB"}`,
    opacity: disabled ? 0.55 : 1,
    transition: "all 0.15s",
  });

  const subtitle = scope === "metaAcquired"
    ? "Where your Meta-acquired customers live. Drill into cities, filter by demographics, spotlight VIPs and discount-only buyers."
    : scope === "allMeta"
    ? "All Meta customers — acquired + retargeted. Demographic filters are unavailable here because Meta only reports age/gender for acquired customers."
    : "Every customer in your Shopify, regardless of source. Demographic filters are unavailable for non-Meta customers.";

  return (
    <Card>
      {/* Component-scoped CSS for the scope toggle. Mirrors the
          .toggle-group / .toggle-btn pattern used on app.customers.tsx
          (Customer Geography tile) but uses the Countries-page green
          (#059669) for the active state to match the page palette. */}
      <style dangerouslySetInnerHTML={{ __html: `
        .cme-toggle-group { display: inline-flex; border: 1px solid #D1D5DB; border-radius: 5px; overflow: hidden; }
        .cme-toggle-btn { padding: 4px 10px; font-size: 11px; font-weight: 500; border: none; cursor: pointer; transition: all 0.15s; white-space: nowrap; }
        .cme-toggle-btn.active { background: #059669; color: #fff; }
        .cme-toggle-btn:not(.active) { background: #fff; color: #374151; }
        .cme-toggle-btn:not(.active):hover { background: #F3F4F6; }
      `}} />
      <BlockStack gap="400">
        <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: 16 }}>
          <BlockStack gap="100">
            <Text as="h2" variant="headingLg">Customer Map Explorer</Text>
            <Text as="p" variant="bodySm" tone="subdued">{subtitle}</Text>
          </BlockStack>
          <div className="cme-toggle-group" style={{ flexShrink: 0 }}>
            <button className={`cme-toggle-btn ${scope === "metaAcquired" ? "active" : ""}`} onClick={() => setScope("metaAcquired")}>Meta Acquired</button>
            <button className={`cme-toggle-btn ${scope === "allMeta" ? "active" : ""}`} onClick={() => setScope("allMeta")}>All Meta</button>
            <button className={`cme-toggle-btn ${scope === "all" ? "active" : ""}`} onClick={() => setScope("all")}>All Customers</button>
          </div>
        </div>

        {/* Map + side panel — placed above the filters per Andy's preference:
            users want to see the map first and tweak filters below it. */}
        <div style={{ display: "grid", gridTemplateColumns: "1fr 280px", gap: "16px", minHeight: 540 }}>
          <MapCanvas points={filtered} cs={cs} protomapsKey={protomapsKey} />
          <TopCitiesPanel cities={topCities} cs={cs} />
        </div>

        {/* Stat strip */}
        <div style={{ borderTop: "1px solid #E5E7EB", paddingTop: 10, fontSize: "12px", color: "#6B7280" }}>
          Showing <strong style={{ color: "#111827" }}>{filtered.length.toLocaleString()}</strong> of {totalCount.toLocaleString()} customers
          across <strong style={{ color: "#111827" }}>{countryCount}</strong> countr{countryCount === 1 ? "y" : "ies"}
          {filtered.length > 0 && (
            <> · Avg net spend <strong style={{ color: "#111827" }}>{cs}{Math.round(filtered.reduce((s, p) => s + (p.$ - p.r), 0) / filtered.length).toLocaleString()}</strong></>
          )}
        </div>

        {/* Filter bar + Gems list. Two-column grid below the map: filters on
            the left, gems on the right. The grid collapses to a single
            column under 900px. */}
        <div style={{ display: "grid", gridTemplateColumns: "minmax(0, 1fr) 320px", gap: 16, marginTop: 4 }}>
        <div style={{ display: "flex", flexDirection: "column", gap: "10px" }}>
          {/* Active filter chips — country + product. Both can only be set
              by clicking a Gem (no UI control for country); chip is the
              way to clear. */}
          {(country !== "All" || product !== "All") && (
            <div style={{ display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap" }}>
              {country !== "All" && (
                <span style={{
                  display: "inline-flex", alignItems: "center", gap: 6,
                  padding: "4px 10px", borderRadius: 9999,
                  background: "#ECFDF5", color: "#065F46",
                  fontSize: 12, fontWeight: 600,
                }}>
                  Country: {countryDisplay(country)}
                  <button onClick={() => setCountry("All")} style={{ border: "none", background: "transparent", color: "#065F46", cursor: "pointer", fontSize: 14, lineHeight: 1, padding: 0 }}>×</button>
                </span>
              )}
              {product !== "All" && (
                <span style={{
                  display: "inline-flex", alignItems: "center", gap: 6,
                  padding: "4px 10px", borderRadius: 9999,
                  background: "#EEF2FF", color: "#3730A3",
                  fontSize: 12, fontWeight: 600,
                }}>
                  Product: {product}
                  <button onClick={() => setProduct("All")} style={{ border: "none", background: "transparent", color: "#3730A3", cursor: "pointer", fontSize: 14, lineHeight: 1, padding: 0 }}>×</button>
                </span>
              )}
            </div>
          )}

          {/* Product picker — dropdown + open-text search. Lets the user
              ask "where do buyers of X live?" without having to memorise
              exact product titles. Both inputs filter independently;
              search trumps dropdown when both are set. */}
          <div style={{ display: "flex", alignItems: "center", gap: "10px", flexWrap: "wrap" }}>
            <span style={labelStyle}>Product</span>
            <select
              value={product}
              onChange={(e) => setProduct(e.target.value)}
              style={{
                padding: "6px 10px", fontSize: "12px", fontWeight: 600,
                borderRadius: "6px", border: "1px solid #E5E7EB",
                background: product !== "All" ? "#EEF2FF" : "#fff",
                color: "#4B5563", cursor: "pointer", minWidth: "220px",
                maxWidth: "320px",
              }}
            >
              <option value="All">All products</option>
              {productOptions.map((p) => <option key={p} value={p}>{p}</option>)}
            </select>
            <input
              type="text"
              value={productSearch}
              onChange={(e) => setProductSearch(e.target.value)}
              placeholder="Search product…"
              style={{
                padding: "6px 10px", fontSize: "12px",
                borderRadius: "6px", border: "1px solid #E5E7EB",
                background: productSearch.trim() !== "" ? "#EEF2FF" : "#fff",
                color: "#374151", minWidth: "180px",
              }}
            />
          </div>

          {/* Gender (Meta Acquired only) */}
          <div style={{ display: "flex", alignItems: "center", gap: "10px", flexWrap: "wrap" }}>
            <span style={labelStyle}>Gender</span>
            {(["All", "Female", "Male"] as const).map((g) => (
              <button
                key={g}
                onClick={() => isMetaAcquired && setGender(g)}
                disabled={!isMetaAcquired}
                style={pillStyle(gender === g && isMetaAcquired, !isMetaAcquired)}
              >
                {g}
              </button>
            ))}
            {!isMetaAcquired && (
              <span style={{ fontSize: "11px", fontStyle: "italic", color: "#9CA3AF" }}>
                Available for Meta Acquired only
              </span>
            )}
          </div>

          {/* Age (Meta Acquired only) */}
          <div style={{ display: "flex", alignItems: "center", gap: "10px", flexWrap: "wrap" }}>
            <span style={labelStyle}>Age</span>
            <button
              onClick={() => isMetaAcquired && setAges([])}
              disabled={!isMetaAcquired}
              style={pillStyle(ages.length === 0 && isMetaAcquired, !isMetaAcquired)}
            >
              All
            </button>
            {AGE_BRACKETS.filter((b) => availableAges.has(b) || ages.includes(b)).map((b) => (
              <button
                key={b}
                onClick={() => isMetaAcquired && toggleAge(b)}
                disabled={!isMetaAcquired}
                style={pillStyle(ages.includes(b) && isMetaAcquired, !isMetaAcquired)}
              >
                {b}
              </button>
            ))}
            {!isMetaAcquired && AGE_BRACKETS.every((b) => !availableAges.has(b)) && (
              <span style={{ fontSize: "11px", fontStyle: "italic", color: "#9CA3AF" }}>
                Available for Meta Acquired only
              </span>
            )}
          </div>

          {/* VIPs */}
          <div style={{ display: "flex", alignItems: "center", gap: "10px", flexWrap: "wrap" }}>
            <span style={labelStyle}>VIPs</span>
            {([
              ["any", "All"],
              ["top5", "Top 5%"],
              ["top10", "Top 10%"],
              ["top20", "Top 20%"],
            ] as const).map(([v, l]) => (
              <button key={v} onClick={() => setVip(v)} style={pillStyle(vip === v)}>{l}</button>
            ))}
            {blob && vip !== "any" && (
              <span style={{ fontSize: "11px", color: "#9CA3AF" }}>
                Net spend ≥ {cs}{Math.round(blob.thresholds[vip]).toLocaleString()}
              </span>
            )}
          </div>

          {/* Pricing & Refunds */}
          <div style={{ display: "flex", alignItems: "center", gap: "10px", flexWrap: "wrap" }}>
            <span style={labelStyle}>Pricing</span>
            {([
              ["any", "All"],
              ["discount", "Discount buyers"],
              ["fullPrice", "Full price only"],
            ] as const).map(([v, l]) => (
              <button key={v} onClick={() => setPricing(v)} style={pillStyle(pricing === v)}>{l}</button>
            ))}
            <button
              onClick={() => setRefundsTop((b) => !b)}
              style={pillStyle(refundsTop)}
              title="Top 10% by refund rate among customers who have any refund"
            >
              Highest refunds
            </button>
          </div>

          {/* Order count + Recency */}
          <div style={{ display: "flex", alignItems: "center", gap: "10px", flexWrap: "wrap" }}>
            <span style={labelStyle}>Orders</span>
            {([
              ["any", "All"],
              ["1", "1"],
              ["2-3", "2-3"],
              ["4+", "4+"],
            ] as const).map(([v, l]) => (
              <button key={v} onClick={() => setOrderBand(v)} style={pillStyle(orderBand === v)}>{l}</button>
            ))}
            <span style={{ ...labelStyle, marginLeft: 8 }}>Recency</span>
            {([
              ["any", "All"],
              ["active", "Active 90d"],
              ["dormant", "Dormant 90-365d"],
              ["lapsed", "Lapsed 365d+"],
            ] as const).map(([v, l]) => (
              <button key={v} onClick={() => setRecency(v)} style={pillStyle(recency === v)}>{l}</button>
            ))}
          </div>

          {activeFilterCount > 0 && (
            <div>
              <button
                onClick={clearAll}
                style={{
                  padding: "6px 12px", fontSize: "12px", fontWeight: 500,
                  borderRadius: "6px", cursor: "pointer",
                  background: "transparent", color: "#6B7280",
                  border: "1px solid transparent", textDecoration: "underline",
                }}
              >
                Clear all filters
              </button>
            </div>
          )}
        </div>

        {/* Gems — auto-surfaced statistical anomalies. Click to populate
            the filters with the exact slice the gem describes. Same shape
            as the gems below the Product Demographics Explorer. */}
        <GemsList gems={gems} onApply={applyGem} />
        </div>
      </BlockStack>
    </Card>
  );
}

// ───────────────────────────────────────────────────────────────────────────
// GemsList — right-hand "Gems spotted in this period" panel
// ───────────────────────────────────────────────────────────────────────────

function GemsList({ gems, onApply }: { gems: MapGem[]; onApply: (g: MapGem) => void }) {
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
      <Text as="h3" variant="headingSm">Gems spotted in this period</Text>
      {gems.length === 0 && (
        <div style={{ color: "#9CA3AF", fontSize: 12, padding: "10px 0" }}>
          Nothing statistically unusual yet — gems appear once you have
          enough customers in multiple countries.
        </div>
      )}
      <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
        {gems.map((g, i) => {
          const cn = countryDisplay(g.country) || g.country;
          // Sentence varies by gem kind — each one frames the lift in the
          // language of the filter it'll apply.
          let text = "";
          if (g.kind === "vip") text = `${cn} has ${g.lift}× the share of Top 5% VIP customers (${g.count} vs ~${g.expected} expected)`;
          else if (g.kind === "discount") text = `${cn} buyers are ${g.lift}× more likely to be discount buyers (${g.count} vs ~${g.expected} expected)`;
          else if (g.kind === "refund") text = `${cn} customers refund ${g.lift}× more often than average (${g.count} vs ~${g.expected} expected)`;
          else if (g.kind === "repeat") text = `${cn} customers are ${g.lift}× more likely to be repeat buyers (${g.count} vs ~${g.expected} expected)`;
          else if (g.kind === "lapsed") text = `${cn} has ${g.lift}× the share of lapsed customers (${g.count} vs ~${g.expected} expected)`;
          else if (g.kind === "product" && g.filters.product) text = `${g.filters.product} is ${g.lift}× more popular in ${cn} (${g.count} buyers vs ~${g.expected} expected)`;
          return (
            <button
              key={i}
              onClick={() => onApply(g)}
              style={{
                textAlign: "left", background: "#fff",
                border: "1px solid #E5E7EB", borderRadius: 6,
                padding: "8px 12px", fontSize: 13, color: "#1F2937",
                cursor: "pointer", display: "flex", alignItems: "flex-start", gap: 8,
                lineHeight: 1.45, transition: "border-color 0.15s, background 0.15s",
              }}
              onMouseEnter={(e) => {
                (e.currentTarget as HTMLElement).style.borderColor = "#10B981";
                (e.currentTarget as HTMLElement).style.background = "#ECFDF5";
              }}
              onMouseLeave={(e) => {
                (e.currentTarget as HTMLElement).style.borderColor = "#E5E7EB";
                (e.currentTarget as HTMLElement).style.background = "#fff";
              }}
            >
              <span style={{ flexShrink: 0 }}>💎</span>
              <span style={{ flex: 1 }}>{text}</span>
              <span style={{ flexShrink: 0, color: "#10B981", fontWeight: 600, fontSize: 12 }}>Click Me</span>
            </button>
          );
        })}
      </div>
    </div>
  );
}

// ───────────────────────────────────────────────────────────────────────────
// MapCanvas — leaflet + supercluster, lazy-loaded client-side
// ───────────────────────────────────────────────────────────────────────────

interface MapCanvasProps {
  points: MapPoint[];
  cs: string;
  protomapsKey: string | null;
}

function MapCanvas({ points, cs, protomapsKey }: MapCanvasProps) {
  const [ready, setReady] = useState(false);
  const [mods, setMods] = useState<{
    MapContainer: any; TileLayer: any; useMap: any;
    L: any; Supercluster: any; protomapsL: any;
  } | null>(null);

  useEffect(() => {
    let mounted = true;
    (async () => {
      // protomaps-leaflet is only loaded when we actually have a key —
      // saves ~80kb on the initial chunk for dev/CARTO-fallback users.
      const imports: Promise<any>[] = [
        import("react-leaflet"),
        import("leaflet"),
        import("supercluster"),
      ];
      if (protomapsKey) imports.push(import("protomaps-leaflet"));
      const [rl, leaflet, sc, pm] = await Promise.all(imports);
      // Side-effect: leaflet ships its CSS separately; pull it via a CDN
      // link tag to avoid having Vite bundle the CSS into the route chunk.
      if (!document.querySelector('link[data-lucidly-leaflet]')) {
        const link = document.createElement("link");
        link.rel = "stylesheet";
        link.href = "https://unpkg.com/leaflet@1.9.4/dist/leaflet.css";
        link.setAttribute("data-lucidly-leaflet", "1");
        link.crossOrigin = "anonymous";
        document.head.appendChild(link);
      }
      if (!mounted) return;
      setMods({
        MapContainer: rl.MapContainer,
        TileLayer: rl.TileLayer,
        useMap: rl.useMap,
        L: (leaflet as any).default || leaflet,
        Supercluster: (sc as any).default || sc,
        protomapsL: pm ? ((pm as any).default || pm) : null,
      });
      setReady(true);
    })();
    return () => { mounted = false; };
  }, [protomapsKey]);

  if (!ready || !mods) {
    return (
      <div style={{
        background: "linear-gradient(180deg, #F9FAFB 0%, #F3F4F6 100%)",
        border: "1px solid #E5E7EB", borderRadius: "8px",
        display: "flex", alignItems: "center", justifyContent: "center",
        color: "#9CA3AF", fontSize: "13px",
      }}>
        Loading map…
      </div>
    );
  }

  const { MapContainer, TileLayer } = mods;
  // Protomaps planet build covers z=0-15, so cap maxZoom there when we use
  // it. CARTO Voyager goes to z=20 but we never need that depth for a
  // customer dot map; 16 was fine before, 15 is fine now.
  const maxZoom = protomapsKey ? 15 : 16;
  return (
    <div style={{ borderRadius: "8px", overflow: "hidden", border: "1px solid #E5E7EB", height: "540px" }}>
      <MapContainer
        center={[20, 0] as any}
        zoom={2}
        minZoom={2}
        maxZoom={maxZoom}
        style={{ width: "100%", height: "100%" }}
        worldCopyJump={true}
        scrollWheelZoom={true}
      >
        {protomapsKey ? (
          <ProtomapsBaseLayer apiKey={protomapsKey} protomapsL={mods.protomapsL} useMap={mods.useMap} />
        ) : (
          <TileLayer
            attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> · <a href="https://carto.com/attributions">CARTO</a>'
            url="https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png"
          />
        )}
        <ClusterLayer points={points} L={mods.L} useMap={mods.useMap} Supercluster={mods.Supercluster} cs={cs} />
      </MapContainer>
    </div>
  );
}

// ProtomapsBaseLayer — vector basemap from api.protomaps.com.
//
// Why this exists: react-leaflet has no native binding for protomaps-leaflet,
// which uses an imperative `leafletLayer({ url, theme }).addTo(map)` API.
// We hook in via useMap (same pattern as ClusterLayer) and clean up on
// unmount so HMR / theme switches don't leak layers.
//
// API key is referrer-restricted by Protomaps — safe in client bundle. If
// the key ever needs rotating, swap PROTOMAPS_API_KEY in fly secrets and
// redeploy. No code change required.
function ProtomapsBaseLayer({ apiKey, protomapsL, useMap }: { apiKey: string; protomapsL: any; useMap: any }) {
  const map = useMap();
  useEffect(() => {
    if (!map || !protomapsL) return;
    // v5 API: option is `flavor` (not `theme`), URL must be a Z/X/Y .mvt
    // endpoint or a .pmtiles archive — passing the v4.json TileJSON URL
    // silently fails to render.
    const layer = protomapsL.leafletLayer({
      url: `https://api.protomaps.com/tiles/v4/{z}/{x}/{y}.mvt?key=${apiKey}`,
      flavor: "light",
      lang: "en",
      attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> · <a href="https://protomaps.com">Protomaps</a>',
    });
    layer.addTo(map);
    return () => { map.removeLayer(layer); };
  }, [map, protomapsL, apiKey]);
  return null;
}

interface ClusterLayerProps {
  points: MapPoint[];
  L: any;
  useMap: any;
  Supercluster: any;
  cs: string;
}

function ClusterLayer({ points, L, useMap, Supercluster, cs }: ClusterLayerProps) {
  const map = useMap();
  const [render, setRender] = useState(0);
  const indexRef = React.useRef<any>(null);
  const layerRef = React.useRef<any>(null);

  // Build supercluster index whenever the input points change. We coerce
  // the lon/lat into geojson features inline. Cluster radius 60px is a
  // sensible default — tighter and you get noisy single-customer clusters
  // at country zoom; looser and big cities never separate from continents.
  useEffect(() => {
    const features = points.map((p) => ({
      type: "Feature" as const,
      properties: { id: p.i, point: p },
      geometry: { type: "Point" as const, coordinates: [p.lo, p.la] },
    }));
    const idx = new Supercluster({ radius: 60, maxZoom: 16, minPoints: 2 });
    idx.load(features);
    indexRef.current = idx;
    setRender((r) => r + 1);
  }, [points, Supercluster]);

  // Repaint markers on every move/zoom. We tear down the previous L.layerGroup
  // and rebuild — for ~30k point shops this is fine (supercluster is fast,
  // visible cluster count caps in the hundreds at any given zoom). For
  // higher-volume merchants we'd switch to incremental updates.
  useEffect(() => {
    if (!map || !indexRef.current) return;
    const repaint = () => {
      if (layerRef.current) {
        map.removeLayer(layerRef.current);
        layerRef.current = null;
      }
      const bounds = map.getBounds();
      const bbox: [number, number, number, number] = [
        bounds.getWest(), bounds.getSouth(), bounds.getEast(), bounds.getNorth(),
      ];
      const zoom = Math.round(map.getZoom());
      const clusters = indexRef.current.getClusters(bbox, zoom);
      const group = L.layerGroup();
      for (const c of clusters) {
        const [lng, lat] = c.geometry.coordinates;
        if (c.properties.cluster) {
          const count = c.properties.point_count;
          const size = count >= 500 ? 56 : count >= 100 ? 46 : count >= 25 ? 38 : 30;
          const el = L.divIcon({
            html: `<div style="width:${size}px;height:${size}px;line-height:${size}px;border-radius:50%;background:rgba(124,58,237,0.85);color:#fff;text-align:center;font-weight:700;font-size:${size >= 46 ? 14 : 12}px;border:2px solid #fff;box-shadow:0 1px 4px rgba(0,0,0,0.25);">${c.properties.point_count_abbreviated}</div>`,
            iconSize: [size, size] as any,
            className: "lucidly-cluster",
          });
          const m = L.marker([lat, lng], { icon: el });
          m.on("click", () => {
            const expansion = indexRef.current.getClusterExpansionZoom(c.id);
            map.flyTo([lat, lng], Math.min(expansion, 16), { duration: 0.6 });
          });
          group.addLayer(m);
        } else {
          const p = c.properties.point as MapPoint;
          const isVip = p.v !== 0;
          const radius = isVip ? 7 : 5;
          const fill = isVip ? "#7C3AED" : p.p === 1 ? "#F59E0B" : "#10B981";
          const m = L.circleMarker([lat, lng], {
            radius,
            color: "#fff",
            weight: 1.5,
            fillColor: fill,
            fillOpacity: p.x === 1 ? 0.45 : 0.85,
          });
          const net = p.$ - p.r;
          const refundLine = p.r > 0
            ? `<div style="color:#DC2626;">Refunded: ${cs}${Math.round(p.r).toLocaleString()}</div>`
            : "";
          const genderLabel = p.g === "f" ? "female" : p.g === "m" ? "male" : null;
          const ageGenderLine = genderLabel || p.a
            ? `<div style="color:#6B7280;">${[genderLabel, p.a].filter(Boolean).join(" · ")}</div>`
            : "";
          const countryName = countryDisplay(p.c);
          const segLabel = p.s === "m" ? "Meta acquired" : p.s === "r" ? "Meta retargeted" : "Organic";
          const popup = `
            <div style="font-size:12px;line-height:1.4;">
              <div style="font-weight:700;font-size:13px;margin-bottom:4px;">
                ${p.t || "(unknown city)"}${countryName ? `, ${countryName}` : ""}
                ${isVip ? `<span style="margin-left:6px;padding:1px 6px;border-radius:9999px;background:#F5F3FF;color:#7C3AED;font-size:10px;font-weight:700;">VIP top ${p.v}%</span>` : ""}
              </div>
              <div style="color:#6B7280;text-transform:uppercase;font-size:10px;letter-spacing:0.5px;">${segLabel}</div>
              ${ageGenderLine}
              <div style="margin-top:4px;">Net: <strong>${cs}${Math.round(net).toLocaleString()}</strong> · Orders: ${p.n}</div>
              ${refundLine}
              ${p.p === 1 ? '<div style="color:#92400E;">Bought on discount</div>' : ""}
              ${p.x === 1 ? '<div style="color:#9CA3AF;font-style:italic;margin-top:2px;">Country centroid (no city detail)</div>' : ""}
            </div>`;
          m.bindPopup(popup);
          group.addLayer(m);
        }
      }
      group.addTo(map);
      layerRef.current = group;
    };
    repaint();
    map.on("moveend", repaint);
    map.on("zoomend", repaint);
    return () => {
      map.off("moveend", repaint);
      map.off("zoomend", repaint);
      if (layerRef.current) {
        map.removeLayer(layerRef.current);
        layerRef.current = null;
      }
    };
  }, [map, render, L, cs]);

  return null;
}

// ───────────────────────────────────────────────────────────────────────────
// TopCitiesPanel — right-hand list, click to fly the map to the city
// ───────────────────────────────────────────────────────────────────────────

interface TopCity {
  city: string;
  country: string | null;
  customers: number;
  revenue: number;
  lat: number;
  lng: number;
}

function TopCitiesPanel({ cities, cs }: { cities: TopCity[]; cs: string }) {
  const [sort, setSort] = useState<"customers" | "revenue">("customers");
  const sorted = useMemo(() => {
    return [...cities].sort((a, b) => sort === "customers" ? b.customers - a.customers : b.revenue - a.revenue);
  }, [cities, sort]);

  return (
    <div style={{ border: "1px solid #E5E7EB", borderRadius: "8px", padding: "12px 14px", display: "flex", flexDirection: "column", height: 540, minHeight: 0 }}>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 10 }}>
        <Text as="h3" variant="headingSm">Top cities</Text>
        <div style={{ display: "inline-flex", borderRadius: "6px", border: "1px solid #E5E7EB", overflow: "hidden" }}>
          {(["customers", "revenue"] as const).map((s) => (
            <button
              key={s}
              onClick={() => setSort(s)}
              style={{
                padding: "4px 10px", fontSize: "11px", fontWeight: 600,
                background: sort === s ? "#7C3AED" : "#fff",
                color: sort === s ? "#fff" : "#4B5563",
                border: "none", cursor: "pointer",
              }}
            >
              {s === "customers" ? "Customers" : "Revenue"}
            </button>
          ))}
        </div>
      </div>
      {sorted.length === 0 && (
        <div style={{ color: "#9CA3AF", fontSize: "12px", padding: "20px 0" }}>
          No customers match the current filters.
        </div>
      )}
      <div style={{ overflowY: "auto", flex: 1 }}>
        {sorted.map((c, i) => (
          <div
            key={`${c.country}-${c.city}-${i}`}
            style={{
              display: "flex", justifyContent: "space-between", gap: 8,
              padding: "8px 6px", borderBottom: i < sorted.length - 1 ? "1px solid #F3F4F6" : "none",
              fontSize: "12px",
            }}
          >
            <div style={{ minWidth: 0, flex: 1 }}>
              <div style={{ fontWeight: 600, color: "#111827", whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>{c.city}</div>
              <div style={{ color: "#9CA3AF", fontSize: "11px" }}>{c.country || "—"}</div>
            </div>
            <div style={{ textAlign: "right", flexShrink: 0 }}>
              <div style={{ fontWeight: 700, color: "#7C3AED" }}>
                {sort === "customers" ? c.customers.toLocaleString() : `${cs}${Math.round(c.revenue).toLocaleString()}`}
              </div>
              <div style={{ color: "#9CA3AF", fontSize: "11px" }}>
                {sort === "customers"
                  ? `${cs}${Math.round(c.revenue).toLocaleString()}`
                  : `AOV ${cs}${(c.customers > 0 ? Math.round(c.revenue / c.customers) : 0).toLocaleString()}`}
              </div>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
