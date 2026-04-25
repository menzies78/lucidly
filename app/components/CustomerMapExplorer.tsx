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

type Segment = "metaNew" | "metaRetargeted" | "organic";
type Scope = "metaAcquired" | "allMeta" | "all";
type RecencyBand = "any" | "active" | "dormant" | "lapsed";
type OrderBand = "any" | "1" | "2-3" | "4+";

export interface MapPoint {
  id: string;
  lat: number;
  lng: number;
  seg: Segment;
  gender: string | null;
  age: string | null;
  country: string | null;
  countryCode: string | null;
  city: string | null;
  spent: number;
  refunded: number;
  net: number;
  orders: number;
  refundRate: number;
  discountEver: boolean;
  fullPrice: boolean;
  daysSinceLast: number | null;
  approx: boolean;
  vipBand: 5 | 10 | 20 | null;
  highestRefunds: boolean;
}

export interface MapBlob {
  points: MapPoint[];
  thresholds: { top5: number; top10: number; top20: number };
  highestRefundThreshold: number;
  computedAt: string;
}

interface Props {
  blob: MapBlob | null;
  cs: string;
}

export default function CustomerMapExplorer({ blob, cs }: Props) {
  const [scope, setScope] = useState<Scope>("metaAcquired");
  const [gender, setGender] = useState<"All" | "Female" | "Male">("All");
  const [ages, setAges] = useState<string[]>([]);
  const [country, setCountry] = useState<string>("All");
  const [vip, setVip] = useState<"any" | "top5" | "top10" | "top20">("any");
  const [pricing, setPricing] = useState<"any" | "discount" | "fullPrice">("any");
  const [refundsTop, setRefundsTop] = useState<boolean>(false);
  const [orderBand, setOrderBand] = useState<OrderBand>("any");
  const [recency, setRecency] = useState<RecencyBand>("any");

  const points = blob?.points || [];

  // Demographic filters apply to Meta Acquired only — Meta hasn't given
  // us age/gender for retargeted or organic shoppers. When the user
  // changes scope away from metaAcquired we leave the filter values
  // untouched but the UI greys them out and they no-op in the filter
  // pass. This keeps state stable across scope toggles.
  const isMetaAcquired = scope === "metaAcquired";

  const segmentSet = useMemo<Set<Segment>>(() => {
    if (scope === "metaAcquired") return new Set(["metaNew"]);
    if (scope === "allMeta") return new Set(["metaNew", "metaRetargeted"]);
    return new Set(["metaNew", "metaRetargeted", "organic"]);
  }, [scope]);

  const filtered = useMemo(() => {
    return points.filter((p) => {
      if (!segmentSet.has(p.seg)) return false;
      if (isMetaAcquired) {
        if (gender === "Female" && p.gender !== "female") return false;
        if (gender === "Male" && p.gender !== "male") return false;
        if (ages.length > 0 && (!p.age || !ages.includes(p.age))) return false;
      }
      if (country !== "All" && p.country !== country) return false;
      if (vip === "top5" && p.vipBand !== 5) return false;
      if (vip === "top10" && (p.vipBand !== 5 && p.vipBand !== 10)) return false;
      if (vip === "top20" && p.vipBand == null) return false;
      if (pricing === "discount" && !p.discountEver) return false;
      if (pricing === "fullPrice" && !p.fullPrice) return false;
      if (refundsTop && !p.highestRefunds) return false;
      if (orderBand === "1" && p.orders !== 1) return false;
      if (orderBand === "2-3" && (p.orders < 2 || p.orders > 3)) return false;
      if (orderBand === "4+" && p.orders < 4) return false;
      if (recency !== "any") {
        const d = p.daysSinceLast;
        if (d == null) return false;
        if (recency === "active" && d > 90) return false;
        if (recency === "dormant" && (d <= 90 || d > 365)) return false;
        if (recency === "lapsed" && d <= 365) return false;
      }
      return true;
    });
  }, [points, segmentSet, isMetaAcquired, gender, ages, country, vip, pricing, refundsTop, orderBand, recency]);

  // Country dropdown options + age availability come from the active
  // segmentSet so the user can't pick a value with zero customers.
  const countryOptions = useMemo(() => {
    const counts: Record<string, number> = {};
    for (const p of points) {
      if (!segmentSet.has(p.seg)) continue;
      if (!p.country) continue;
      counts[p.country] = (counts[p.country] || 0) + 1;
    }
    return Object.entries(counts).sort((a, b) => b[1] - a[1]).map(([c]) => c);
  }, [points, segmentSet]);

  const availableAges = useMemo(() => {
    const set = new Set<string>();
    if (!isMetaAcquired) return set;
    for (const p of points) if (p.age && segmentSet.has(p.seg)) set.add(p.age);
    return set;
  }, [points, segmentSet, isMetaAcquired]);

  const totalCount = useMemo(() => {
    let c = 0;
    for (const p of points) if (segmentSet.has(p.seg)) c++;
    return c;
  }, [points, segmentSet]);

  const countryCount = useMemo(() => {
    const set = new Set<string>();
    for (const p of filtered) if (p.country) set.add(p.country);
    return set.size;
  }, [filtered]);

  const topCities = useMemo(() => {
    const agg: Record<string, { city: string; country: string | null; customers: number; revenue: number; lat: number; lng: number; }> = {};
    for (const p of filtered) {
      const key = `${p.country}|${p.city || "(unknown)"}`;
      if (!agg[key]) agg[key] = { city: p.city || "(unknown)", country: p.country, customers: 0, revenue: 0, lat: p.lat, lng: p.lng };
      agg[key].customers += 1;
      agg[key].revenue += p.net;
    }
    return Object.values(agg).sort((a, b) => b.customers - a.customers).slice(0, 15);
  }, [filtered]);

  const activeFilterCount =
    (scope !== "metaAcquired" ? 0 : 0) // scope itself is not counted
    + (gender !== "All" ? 1 : 0)
    + (ages.length > 0 ? 1 : 0)
    + (country !== "All" ? 1 : 0)
    + (vip !== "any" ? 1 : 0)
    + (pricing !== "any" ? 1 : 0)
    + (refundsTop ? 1 : 0)
    + (orderBand !== "any" ? 1 : 0)
    + (recency !== "any" ? 1 : 0);

  const clearAll = () => {
    setGender("All"); setAges([]); setCountry("All");
    setVip("any"); setPricing("any"); setRefundsTop(false);
    setOrderBand("any"); setRecency("any");
  };

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
      <BlockStack gap="400">
        <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: 16 }}>
          <BlockStack gap="100">
            <Text as="h2" variant="headingLg">Customer Map Explorer</Text>
            <Text as="p" variant="bodySm" tone="subdued">{subtitle}</Text>
          </BlockStack>
          <div className="segment-toggle" style={{ flexShrink: 0 }}>
            <button className={scope === "metaAcquired" ? "active" : ""} onClick={() => setScope("metaAcquired")}>Meta Acquired</button>
            <button className={scope === "allMeta" ? "active" : ""} onClick={() => setScope("allMeta")}>All Meta</button>
            <button className={scope === "all" ? "active" : ""} onClick={() => setScope("all")}>All Customers</button>
          </div>
        </div>

        {/* Filter bar */}
        <div style={{ display: "flex", flexDirection: "column", gap: "10px", marginTop: 12 }}>
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

          {/* Country */}
          <div style={{ display: "flex", alignItems: "center", gap: "10px", flexWrap: "wrap" }}>
            <span style={labelStyle}>Country</span>
            <select
              value={country}
              onChange={(e) => setCountry(e.target.value)}
              style={{
                padding: "6px 10px", fontSize: "12px", fontWeight: 600,
                borderRadius: "6px", border: "1px solid #E5E7EB",
                background: country !== "All" ? "#F5F3FF" : "#fff",
                color: "#4B5563", cursor: "pointer", minWidth: "200px",
              }}
            >
              <option value="All">All countries</option>
              {countryOptions.map((c) => <option key={c} value={c}>{c}</option>)}
            </select>
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

        {/* Stat strip */}
        <div style={{ borderTop: "1px solid #E5E7EB", paddingTop: 10, fontSize: "12px", color: "#6B7280" }}>
          Showing <strong style={{ color: "#111827" }}>{filtered.length.toLocaleString()}</strong> of {totalCount.toLocaleString()} customers
          across <strong style={{ color: "#111827" }}>{countryCount}</strong> countr{countryCount === 1 ? "y" : "ies"}
          {filtered.length > 0 && (
            <> · Avg net spend <strong style={{ color: "#111827" }}>{cs}{Math.round(filtered.reduce((s, p) => s + p.net, 0) / filtered.length).toLocaleString()}</strong></>
          )}
        </div>

        {/* Map + side panel */}
        <div style={{ display: "grid", gridTemplateColumns: "1fr 280px", gap: "16px", minHeight: 540 }}>
          <MapCanvas points={filtered} cs={cs} />
          <TopCitiesPanel cities={topCities} cs={cs} />
        </div>
      </BlockStack>
    </Card>
  );
}

// ───────────────────────────────────────────────────────────────────────────
// MapCanvas — leaflet + supercluster, lazy-loaded client-side
// ───────────────────────────────────────────────────────────────────────────

interface MapCanvasProps {
  points: MapPoint[];
  cs: string;
}

function MapCanvas({ points, cs }: MapCanvasProps) {
  const [ready, setReady] = useState(false);
  const [mods, setMods] = useState<{
    MapContainer: any; TileLayer: any; useMap: any;
    L: any; Supercluster: any;
  } | null>(null);

  useEffect(() => {
    let mounted = true;
    (async () => {
      const [rl, leaflet, sc] = await Promise.all([
        import("react-leaflet"),
        import("leaflet"),
        import("supercluster"),
      ]);
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
      });
      setReady(true);
    })();
    return () => { mounted = false; };
  }, []);

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
  return (
    <div style={{ borderRadius: "8px", overflow: "hidden", border: "1px solid #E5E7EB", height: "540px" }}>
      <MapContainer
        center={[20, 0] as any}
        zoom={2}
        minZoom={2}
        maxZoom={16}
        style={{ width: "100%", height: "100%" }}
        worldCopyJump={true}
        scrollWheelZoom={true}
      >
        <TileLayer
          attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> · <a href="https://carto.com/attributions">CARTO</a>'
          url="https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png"
        />
        <ClusterLayer points={points} L={mods.L} useMap={mods.useMap} Supercluster={mods.Supercluster} cs={cs} />
      </MapContainer>
    </div>
  );
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
      properties: { id: p.id, point: p },
      geometry: { type: "Point" as const, coordinates: [p.lng, p.lat] },
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
          const isVip = p.vipBand != null;
          const radius = isVip ? 7 : 5;
          const fill = isVip ? "#7C3AED" : p.discountEver ? "#F59E0B" : "#10B981";
          const m = L.circleMarker([lat, lng], {
            radius,
            color: "#fff",
            weight: 1.5,
            fillColor: fill,
            fillOpacity: p.approx ? 0.45 : 0.85,
          });
          const refundLine = p.refunded > 0
            ? `<div style="color:#DC2626;">Refunded: ${cs}${Math.round(p.refunded).toLocaleString()}</div>`
            : "";
          const ageGenderLine = p.gender || p.age
            ? `<div style="color:#6B7280;">${[p.gender, p.age].filter(Boolean).join(" · ")}</div>`
            : "";
          const popup = `
            <div style="font-size:12px;line-height:1.4;">
              <div style="font-weight:700;font-size:13px;margin-bottom:4px;">
                ${p.city || "(unknown city)"}${p.country ? `, ${p.country}` : ""}
                ${isVip ? `<span style="margin-left:6px;padding:1px 6px;border-radius:9999px;background:#F5F3FF;color:#7C3AED;font-size:10px;font-weight:700;">VIP top ${p.vipBand}%</span>` : ""}
              </div>
              <div style="color:#6B7280;text-transform:uppercase;font-size:10px;letter-spacing:0.5px;">${p.seg === "metaNew" ? "Meta acquired" : p.seg === "metaRetargeted" ? "Meta retargeted" : "Organic"}</div>
              ${ageGenderLine}
              <div style="margin-top:4px;">Net: <strong>${cs}${Math.round(p.net).toLocaleString()}</strong> · Orders: ${p.orders}</div>
              ${refundLine}
              ${p.discountEver ? '<div style="color:#92400E;">Bought on discount</div>' : ""}
              ${p.approx ? '<div style="color:#9CA3AF;font-style:italic;margin-top:2px;">Country centroid (no city detail)</div>' : ""}
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
    <div style={{ border: "1px solid #E5E7EB", borderRadius: "8px", padding: "12px 14px", display: "flex", flexDirection: "column", minHeight: 0 }}>
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
                {sort === "customers" ? `${cs}${Math.round(c.revenue).toLocaleString()}` : `${c.customers.toLocaleString()} cust`}
              </div>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
