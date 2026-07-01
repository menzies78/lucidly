// demoCatalog.server.js
// ─────────────────────────────────────────────────────────────────────
// Static catalogue for the "Explore with sample data" demo store.
//
// Brand: NORVIK — fictional technical adventure apparel. Tagline
// "Engineered for everywhere." Naming theme is terrain/expedition
// (Tundra, Ridgeline, Glacier, Aurora, Drift, Fjord, Cairn…). Nothing
// here references a real merchant: product names, prices, campaigns,
// customers and geo are all invented, but the STRUCTURE (catalogue
// breadth, price tiers, funnel geometry, geo mix, segment split) is
// modelled on a real DTC apparel brand so every chart, cohort and LTV
// curve in the app renders with believable shape.
//
// The generator (demoData.server.js) consumes these constants to
// synthesise 12 months of relative-dated orders / Meta data, then runs
// the real rollup builders so the aggregates are guaranteed-consistent.

export const DEMO_BRAND = "Norvik";
export const DEMO_CURRENCY = "GBP";
export const DEMO_META_CURRENCY = "USD";
export const DEMO_TIMEZONE = "Europe/London";

// ── Products ─────────────────────────────────────────────────────────
// price is the GBP unit price. image is a stable public path; the asset
// drop is non-blocking — until images exist the app falls back to its
// initial-letter placeholder exactly as it does for any product without
// a featured image.
export const DEMO_PRODUCTS = [
  { name: "Tundra Utility Jacket", category: "jacket", price: 295, image: "/demo/products/tundra-utility-jacket.webp" },
  { name: "Ridgeline Tee. Black edition", category: "tee", price: 85, image: "/demo/products/ridgeline-tee-black.webp" },
  { name: "Ridgeline Tee. White edition", category: "tee", price: 85, image: "/demo/products/ridgeline-tee-white.webp" },
  { name: "Basalt Tech Tee. Navy edition", category: "tee", price: 125, image: "/demo/products/basalt-tech-tee-navy.webp" },
  { name: "Granite Tee. Grey edition", category: "tee", price: 75, image: "/demo/products/granite-tee-grey.webp" },
  { name: "Vapor Tech Tee. Black edition", category: "tee", price: 125, image: "/demo/products/vapor-tech-tee-black.webp" },
  { name: "Summit Zip Hoodie. Black edition", category: "hoodie", price: 225, image: "/demo/products/summit-zip-hoodie-black.webp" },
  { name: "Summit Zip Hoodie. Green edition", category: "hoodie", price: 225, image: "/demo/products/summit-zip-hoodie-green.webp" },
  { name: "Slate Crew Sweatshirt. Grey edition", category: "sweatshirt", price: 150, image: "/demo/products/slate-crew-sweatshirt-grey.webp" },
  { name: "Ember Sweatpants. Black edition", category: "sweatpants", price: 165, image: "/demo/products/ember-sweatpants-black.webp" },
  { name: "Ember Sweatpants. Green edition", category: "sweatpants", price: 165, image: "/demo/products/ember-sweatpants-green.webp" },
  { name: "Glacier Fleece Vest. Black edition", category: "vest", price: 95, image: "/demo/products/glacier-fleece-vest-black.webp" },
  { name: "Glacier Fleece. Black edition", category: "fleece", price: 175, image: "/demo/products/glacier-fleece-black.webp" },
  { name: "Tundra Pants. Black edition", category: "pants", price: 225, image: "/demo/products/tundra-pants-black.webp" },
  { name: "Drift Off-Grid Pants. Black edition", category: "pants", price: 195, image: "/demo/products/drift-offgrid-pants-black.webp" },
  { name: "Cairn Field Suit Pants. Navy edition", category: "pants", price: 350, image: "/demo/products/cairn-field-suit-pants-navy.webp" },
  { name: "Nimbus Swim Shorts. Blue edition", category: "shorts", price: 95, image: "/demo/products/nimbus-swim-shorts-blue.webp" },
  { name: "Tempest Waterproof Jacket", category: "jacket", price: 295, image: "/demo/products/tempest-waterproof-jacket.webp" },
  { name: "Tempest Parka. Black edition", category: "parka", price: 395, image: "/demo/products/tempest-parka-black.webp" },
  { name: "Fjord Chore Jacket. Blue edition", category: "jacket", price: 495, image: "/demo/products/fjord-chore-jacket-blue.webp" },
  { name: "Beacon Field Jacket. Khaki edition", category: "jacket", price: 395, image: "/demo/products/beacon-field-jacket-khaki.webp" },
  { name: "Cairn Field Suit Jacket. Navy edition", category: "jacket", price: 550, image: "/demo/products/cairn-field-suit-jacket-navy.webp" },
  { name: "Onyx Bomber Jacket", category: "bomber", price: 350, image: "/demo/products/onyx-bomber-jacket.webp" },
  { name: "Quartz Blazer. Black edition", category: "blazer", price: 295, image: "/demo/products/quartz-blazer-black.webp" },
  { name: "Drift Dyneema Shell", category: "shell", price: 695, image: "/demo/products/drift-dyneema-shell.webp" },
  { name: "Aurora Shell Jacket. Copper edition", category: "shell", price: 1295, image: "/demo/products/aurora-shell-jacket-copper.webp" },
  { name: "Tundra Hat", category: "accessory", price: 45, image: "/demo/products/tundra-hat.webp" },
  { name: "Tundra Belt. Black edition", category: "accessory", price: 95, image: "/demo/products/tundra-belt-black.webp" },
  // ── Extended range ──────────────────────────────────────────────────
  // Broadens catalogue breadth so the Customer Product Journey reads as a
  // wide long-tail (real brands carry 700+ distinct titles) rather than a
  // handful of products forming suspiciously symmetric paths. Colour/edition
  // variants also exercise the listing-pair name-dedup path.
  { name: "Ridgeline Tee. Olive edition", category: "tee", price: 85, image: "/demo/products/ridgeline-tee-olive.webp" },
  { name: "Ridgeline Tee. Stone edition", category: "tee", price: 85, image: "/demo/products/ridgeline-tee-stone.webp" },
  { name: "Vapor Tech Tee. White edition", category: "tee", price: 125, image: "/demo/products/vapor-tech-tee-white.webp" },
  { name: "Basalt Tech Tee. Black edition", category: "tee", price: 125, image: "/demo/products/basalt-tech-tee-black.webp" },
  { name: "Meridian Merino Tee. Charcoal edition", category: "tee", price: 110, image: "/demo/products/meridian-merino-tee-charcoal.webp" },
  { name: "Meridian Merino Tee. Sand edition", category: "tee", price: 110, image: "/demo/products/meridian-merino-tee-sand.webp" },
  { name: "Granite Long Sleeve. Grey edition", category: "tee", price: 95, image: "/demo/products/granite-long-sleeve-grey.webp" },
  { name: "Polar Henley. Cream edition", category: "tee", price: 105, image: "/demo/products/polar-henley-cream.webp" },
  { name: "Summit Zip Hoodie. Navy edition", category: "hoodie", price: 225, image: "/demo/products/summit-zip-hoodie-navy.webp" },
  { name: "Halifax Pullover Hoodie. Grey edition", category: "hoodie", price: 195, image: "/demo/products/halifax-pullover-hoodie-grey.webp" },
  { name: "Halifax Pullover Hoodie. Black edition", category: "hoodie", price: 195, image: "/demo/products/halifax-pullover-hoodie-black.webp" },
  { name: "Thermal Grid Hoodie. Olive edition", category: "hoodie", price: 245, image: "/demo/products/thermal-grid-hoodie-olive.webp" },
  { name: "Slate Crew Sweatshirt. Black edition", category: "sweatshirt", price: 150, image: "/demo/products/slate-crew-sweatshirt-black.webp" },
  { name: "Heath Crew Sweatshirt. Rust edition", category: "sweatshirt", price: 160, image: "/demo/products/heath-crew-sweatshirt-rust.webp" },
  { name: "Moss Crew Sweatshirt. Green edition", category: "sweatshirt", price: 155, image: "/demo/products/moss-crew-sweatshirt-green.webp" },
  { name: "Ember Sweatpants. Grey edition", category: "sweatpants", price: 165, image: "/demo/products/ember-sweatpants-grey.webp" },
  { name: "Cinder Jogger. Charcoal edition", category: "sweatpants", price: 145, image: "/demo/products/cinder-jogger-charcoal.webp" },
  { name: "Cinder Jogger. Navy edition", category: "sweatpants", price: 145, image: "/demo/products/cinder-jogger-navy.webp" },
  { name: "Glacier Fleece Vest. Olive edition", category: "vest", price: 95, image: "/demo/products/glacier-fleece-vest-olive.webp" },
  { name: "Cairn Down Gilet. Black edition", category: "vest", price: 245, image: "/demo/products/cairn-down-gilet-black.webp" },
  { name: "Cairn Down Gilet. Sand edition", category: "vest", price: 245, image: "/demo/products/cairn-down-gilet-sand.webp" },
  { name: "Glacier Fleece. Grey edition", category: "fleece", price: 175, image: "/demo/products/glacier-fleece-grey.webp" },
  { name: "Boreal Sherpa Fleece. Oat edition", category: "fleece", price: 210, image: "/demo/products/boreal-sherpa-fleece-oat.webp" },
  { name: "Boreal Sherpa Fleece. Slate edition", category: "fleece", price: 210, image: "/demo/products/boreal-sherpa-fleece-slate.webp" },
  { name: "Tundra Pants. Olive edition", category: "pants", price: 225, image: "/demo/products/tundra-pants-olive.webp" },
  { name: "Drift Off-Grid Pants. Sand edition", category: "pants", price: 195, image: "/demo/products/drift-offgrid-pants-sand.webp" },
  { name: "Mesa Cargo Pants. Khaki edition", category: "pants", price: 215, image: "/demo/products/mesa-cargo-pants-khaki.webp" },
  { name: "Mesa Cargo Pants. Black edition", category: "pants", price: 215, image: "/demo/products/mesa-cargo-pants-black.webp" },
  { name: "Verglas Trail Pants. Grey edition", category: "pants", price: 240, image: "/demo/products/verglas-trail-pants-grey.webp" },
  { name: "Nimbus Swim Shorts. Black edition", category: "shorts", price: 95, image: "/demo/products/nimbus-swim-shorts-black.webp" },
  { name: "Reef Hybrid Shorts. Olive edition", category: "shorts", price: 110, image: "/demo/products/reef-hybrid-shorts-olive.webp" },
  { name: "Reef Hybrid Shorts. Navy edition", category: "shorts", price: 110, image: "/demo/products/reef-hybrid-shorts-navy.webp" },
  { name: "Tempest Waterproof Jacket. Olive edition", category: "jacket", price: 295, image: "/demo/products/tempest-waterproof-jacket-olive.webp" },
  { name: "Sable Overshirt. Brown edition", category: "jacket", price: 245, image: "/demo/products/sable-overshirt-brown.webp" },
  { name: "Sable Overshirt. Charcoal edition", category: "jacket", price: 245, image: "/demo/products/sable-overshirt-charcoal.webp" },
  { name: "Beacon Field Jacket. Black edition", category: "jacket", price: 395, image: "/demo/products/beacon-field-jacket-black.webp" },
  { name: "Harbour Rain Jacket. Yellow edition", category: "jacket", price: 265, image: "/demo/products/harbour-rain-jacket-yellow.webp" },
  { name: "Tempest Parka. Olive edition", category: "parka", price: 395, image: "/demo/products/tempest-parka-olive.webp" },
  { name: "Borealis Down Parka. Black edition", category: "parka", price: 545, image: "/demo/products/borealis-down-parka-black.webp" },
  { name: "Onyx Bomber Jacket. Green edition", category: "bomber", price: 350, image: "/demo/products/onyx-bomber-jacket-green.webp" },
  { name: "Vector Flight Bomber. Navy edition", category: "bomber", price: 375, image: "/demo/products/vector-flight-bomber-navy.webp" },
  { name: "Quartz Blazer. Grey edition", category: "blazer", price: 295, image: "/demo/products/quartz-blazer-grey.webp" },
  { name: "Slate Travel Blazer. Charcoal edition", category: "blazer", price: 325, image: "/demo/products/slate-travel-blazer-charcoal.webp" },
  { name: "Aurora Shell Jacket. Black edition", category: "shell", price: 1295, image: "/demo/products/aurora-shell-jacket-black.webp" },
  { name: "Stratus Hardshell. Blue edition", category: "shell", price: 575, image: "/demo/products/stratus-hardshell-blue.webp" },
  { name: "Stratus Hardshell. Black edition", category: "shell", price: 575, image: "/demo/products/stratus-hardshell-black.webp" },
  { name: "Norvik Beanie. Charcoal edition", category: "accessory", price: 40, image: "/demo/products/norvik-beanie-charcoal.webp" },
  { name: "Norvik Beanie. Rust edition", category: "accessory", price: 40, image: "/demo/products/norvik-beanie-rust.webp" },
  { name: "Expedition Cap. Black edition", category: "accessory", price: 50, image: "/demo/products/expedition-cap-black.webp" },
  { name: "Trail Crew Socks. 3-pack", category: "accessory", price: 35, image: "/demo/products/trail-crew-socks-3pack.webp" },
  { name: "Summit Gloves. Black edition", category: "accessory", price: 75, image: "/demo/products/summit-gloves-black.webp" },
  { name: "Cargo Duffel 40L. Olive edition", category: "accessory", price: 165, image: "/demo/products/cargo-duffel-40l-olive.webp" },
];

// Hero/launch products get disproportionate ad spend + DPA coverage,
// mirroring how a real brand pushes its flagship pieces.
export const DEMO_HERO_PRODUCTS = [
  "Aurora Shell Jacket. Copper edition",
  "Drift Dyneema Shell",
  "Cairn Field Suit Jacket. Navy edition",
  "Fjord Chore Jacket. Blue edition",
  "Tundra Utility Jacket",
];

// ── Meta campaign / adset / ad structure ─────────────────────────────
// status mirrors Vollebak's live/paused mix. funnelStage drives the
// cold/warm/hot targeting badge. geo scopes which countries an adset's
// orders are drawn from. heroBias adsets skew toward hero products.
export const DEMO_CAMPAIGNS = [
  {
    name: "TOF_Norvik_Prospecting_INSTAGRAM",
    status: "ACTIVE",
    adsets: [
      { name: "UK INSTAGRAM - TOF", funnelStage: "cold", geo: ["GB"] },
      { name: "USA INSTAGRAM - TOF", funnelStage: "cold", geo: ["US"] },
      { name: "EU INSTAGRAM - TOF", funnelStage: "cold", geo: ["DE", "NL", "FR", "IT", "ES", "SE", "AT", "BE", "DK", "PL"] },
      { name: "Middle East INSTAGRAM - TOF", funnelStage: "cold", geo: ["AE"] },
    ],
  },
  {
    name: "Retargeting_Norvik",
    status: "ACTIVE",
    adsets: [
      { name: "UK Retargeting - 180d", funnelStage: "hot", geo: ["GB"] },
      { name: "USA Retargeting - 180d", funnelStage: "hot", geo: ["US"] },
      { name: "Worldwide Retargeting - 90d", funnelStage: "warm", geo: ["US", "GB", "DE", "CA", "AU"] },
    ],
  },
  {
    name: "DPA_Broad_Norvik",
    status: "ACTIVE",
    adsets: [
      { name: "DPA_Broad - UK", funnelStage: "cold", geo: ["GB"], heroBias: true },
      { name: "DPA_Broad - Non US", funnelStage: "cold", geo: ["DE", "CA", "NL", "AU", "JP", "CH"], heroBias: true },
    ],
  },
  {
    name: "Spaceport_Launch_Aurora_Shell",
    status: "ACTIVE",
    adsets: [
      { name: "USA INSTAGRAM - Aurora Launch", funnelStage: "cold", geo: ["US"], heroBias: true },
      { name: "UK INSTAGRAM - Aurora Launch", funnelStage: "warm", geo: ["GB"], heroBias: true },
    ],
  },
  {
    name: "Archive_Sale_Norvik",
    status: "PAUSED",
    adsets: [
      { name: "UK Archive - BROAD", funnelStage: "cold", geo: ["GB"] },
    ],
  },
];

// ── Geo distribution ─────────────────────────────────────────────────
// weight is the relative share of orders. `city`/`lat`/`lng` remain the
// country's headline point (used as a fallback), but `cities` is a pool of
// real towns/cities the generator spreads customers across — then applies a
// small per-customer jitter — so the Customer Map reads as a believable
// scatter of households rather than one dot stacked on each country centroid.
// (The map plots lat/lng; the addresses themselves are fictional — only the
// city coordinates are real, which is all the map needs.)
export const DEMO_GEO = [
  { country: "United States", code: "US", region: "NY", city: "New York", lat: 40.7128, lng: -74.006, weight: 43, aff: 1.12, cities: [
    { city: "New York", region: "NY", lat: 40.7128, lng: -74.006, w: 22 },
    { city: "Los Angeles", region: "CA", lat: 34.0522, lng: -118.2437, w: 16 },
    { city: "Chicago", region: "IL", lat: 41.8781, lng: -87.6298, w: 9 },
    { city: "San Francisco", region: "CA", lat: 37.7749, lng: -122.4194, w: 9 },
    { city: "Seattle", region: "WA", lat: 47.6062, lng: -122.3321, w: 7 },
    { city: "Austin", region: "TX", lat: 30.2672, lng: -97.7431, w: 6 },
    { city: "Boston", region: "MA", lat: 42.3601, lng: -71.0589, w: 6 },
    { city: "Denver", region: "CO", lat: 39.7392, lng: -104.9903, w: 5 },
    { city: "Portland", region: "OR", lat: 45.5152, lng: -122.6784, w: 5 },
    { city: "Miami", region: "FL", lat: 25.7617, lng: -80.1918, w: 5 },
    { city: "Atlanta", region: "GA", lat: 33.749, lng: -84.388, w: 5 },
    { city: "Minneapolis", region: "MN", lat: 44.9778, lng: -93.265, w: 4 },
  ] },
  { country: "United Kingdom", code: "GB", region: "LDN", city: "London", lat: 51.5074, lng: -0.1278, weight: 20, cities: [
    { city: "London", region: "LDN", lat: 51.5074, lng: -0.1278, w: 30 },
    { city: "Manchester", region: "MAN", lat: 53.4808, lng: -2.2426, w: 12 },
    { city: "Bristol", region: "BST", lat: 51.4545, lng: -2.5879, w: 9 },
    { city: "Birmingham", region: "BIR", lat: 52.4862, lng: -1.8904, w: 8 },
    { city: "Leeds", region: "LDS", lat: 53.8008, lng: -1.5491, w: 7 },
    { city: "Edinburgh", region: "EDH", lat: 55.9533, lng: -3.1883, w: 7 },
    { city: "Glasgow", region: "GLG", lat: 55.8642, lng: -4.2518, w: 6 },
    { city: "Brighton", region: "BNH", lat: 50.8225, lng: -0.1372, w: 6 },
  ] },
  { country: "Germany", code: "DE", region: "BE", city: "Berlin", lat: 52.52, lng: 13.405, weight: 6, cities: [
    { city: "Berlin", region: "BE", lat: 52.52, lng: 13.405, w: 26 },
    { city: "Munich", region: "BY", lat: 48.1351, lng: 11.582, w: 20 },
    { city: "Hamburg", region: "HH", lat: 53.5511, lng: 9.9937, w: 16 },
    { city: "Cologne", region: "NW", lat: 50.9375, lng: 6.9603, w: 13 },
    { city: "Frankfurt", region: "HE", lat: 50.1109, lng: 8.6821, w: 13 },
    { city: "Stuttgart", region: "BW", lat: 48.7758, lng: 9.1829, w: 12 },
  ] },
  { country: "Canada", code: "CA", region: "ON", city: "Toronto", lat: 43.6532, lng: -79.3832, weight: 4, cities: [
    { city: "Toronto", region: "ON", lat: 43.6532, lng: -79.3832, w: 30 },
    { city: "Vancouver", region: "BC", lat: 49.2827, lng: -123.1207, w: 24 },
    { city: "Montreal", region: "QC", lat: 45.5017, lng: -73.5673, w: 20 },
    { city: "Calgary", region: "AB", lat: 51.0447, lng: -114.0719, w: 14 },
    { city: "Ottawa", region: "ON", lat: 45.4215, lng: -75.6972, w: 12 },
  ] },
  { country: "Netherlands", code: "NL", region: "NH", city: "Amsterdam", lat: 52.3676, lng: 4.9041, weight: 3, cities: [
    { city: "Amsterdam", region: "NH", lat: 52.3676, lng: 4.9041, w: 36 },
    { city: "Rotterdam", region: "ZH", lat: 51.9244, lng: 4.4777, w: 24 },
    { city: "Utrecht", region: "UT", lat: 52.0907, lng: 5.1214, w: 22 },
    { city: "The Hague", region: "ZH", lat: 52.0705, lng: 4.3007, w: 18 },
  ] },
  { country: "France", code: "FR", region: "IDF", city: "Paris", lat: 48.8566, lng: 2.3522, weight: 2.5, cities: [
    { city: "Paris", region: "IDF", lat: 48.8566, lng: 2.3522, w: 38 },
    { city: "Lyon", region: "ARA", lat: 45.764, lng: 4.8357, w: 22 },
    { city: "Bordeaux", region: "NAQ", lat: 44.8378, lng: -0.5792, w: 20 },
    { city: "Marseille", region: "PAC", lat: 43.2965, lng: 5.3698, w: 20 },
  ] },
  { country: "Switzerland", code: "CH", region: "ZH", city: "Zurich", lat: 47.3769, lng: 8.5417, weight: 2, aff: 1.55, cities: [
    { city: "Zurich", region: "ZH", lat: 47.3769, lng: 8.5417, w: 40 },
    { city: "Geneva", region: "GE", lat: 46.2044, lng: 6.1432, w: 32 },
    { city: "Basel", region: "BS", lat: 47.5596, lng: 7.5886, w: 28 },
  ] },
  { country: "Australia", code: "AU", region: "NSW", city: "Sydney", lat: -33.8688, lng: 151.2093, weight: 1.8, cities: [
    { city: "Sydney", region: "NSW", lat: -33.8688, lng: 151.2093, w: 34 },
    { city: "Melbourne", region: "VIC", lat: -37.8136, lng: 144.9631, w: 30 },
    { city: "Brisbane", region: "QLD", lat: -27.4698, lng: 153.0251, w: 20 },
    { city: "Perth", region: "WA", lat: -31.9505, lng: 115.8605, w: 16 },
  ] },
  { country: "Sweden", code: "SE", region: "ST", city: "Stockholm", lat: 59.3293, lng: 18.0686, weight: 1.3, cities: [
    { city: "Stockholm", region: "ST", lat: 59.3293, lng: 18.0686, w: 50 },
    { city: "Gothenburg", region: "O", lat: 57.7089, lng: 11.9746, w: 30 },
    { city: "Malmö", region: "M", lat: 55.605, lng: 13.0038, w: 20 },
  ] },
  { country: "Japan", code: "JP", region: "13", city: "Tokyo", lat: 35.6762, lng: 139.6503, weight: 1.2, cities: [
    { city: "Tokyo", region: "13", lat: 35.6762, lng: 139.6503, w: 50 },
    { city: "Osaka", region: "27", lat: 34.6937, lng: 135.5023, w: 30 },
    { city: "Kyoto", region: "26", lat: 35.0116, lng: 135.7681, w: 20 },
  ] },
  { country: "Italy", code: "IT", region: "MI", city: "Milan", lat: 45.4642, lng: 9.19, weight: 1.1, cities: [
    { city: "Milan", region: "MI", lat: 45.4642, lng: 9.19, w: 44 },
    { city: "Rome", region: "RM", lat: 41.9028, lng: 12.4964, w: 34 },
    { city: "Turin", region: "TO", lat: 45.0703, lng: 7.6869, w: 22 },
  ] },
  { country: "Austria", code: "AT", region: "W", city: "Vienna", lat: 48.2082, lng: 16.3738, weight: 0.9 },
  { country: "Singapore", code: "SG", region: "SG", city: "Singapore", lat: 1.3521, lng: 103.8198, weight: 0.85, aff: 1.35 },
  { country: "Hong Kong", code: "HK", region: "HK", city: "Hong Kong", lat: 22.3193, lng: 114.1694, weight: 0.8, aff: 1.4 },
  { country: "United Arab Emirates", code: "AE", region: "DU", city: "Dubai", lat: 25.2048, lng: 55.2708, weight: 0.8, aff: 1.7 },
  { country: "Denmark", code: "DK", region: "84", city: "Copenhagen", lat: 55.6761, lng: 12.5683, weight: 0.75 },
  { country: "Belgium", code: "BE", region: "BRU", city: "Brussels", lat: 50.8503, lng: 4.3517, weight: 0.7 },
  { country: "Spain", code: "ES", region: "MD", city: "Madrid", lat: 40.4168, lng: -3.7038, weight: 0.65, cities: [
    { city: "Madrid", region: "MD", lat: 40.4168, lng: -3.7038, w: 50 },
    { city: "Barcelona", region: "CT", lat: 41.3851, lng: 2.1734, w: 50 },
  ] },
  { country: "Poland", code: "PL", region: "MZ", city: "Warsaw", lat: 52.2297, lng: 21.0122, weight: 0.6 },
  { country: "Mexico", code: "MX", region: "CMX", city: "Mexico City", lat: 19.4326, lng: -99.1332, weight: 0.55 },
];

// ── Name pools (gender-tagged for inferredGender) ────────────────────
export const DEMO_FIRST_NAMES_M = [
  "James", "Oliver", "William", "Henry", "Lucas", "Mason", "Ethan", "Daniel",
  "Liam", "Noah", "Felix", "Hugo", "Max", "Leon", "Anders", "Mateo", "Kenji",
  "Marcus", "Theo", "Elliot", "Nathan", "Adrian", "Sven", "Diego", "Tobias",
];
export const DEMO_FIRST_NAMES_F = [
  "Olivia", "Emma", "Sophia", "Isla", "Ava", "Mia", "Charlotte", "Amelia",
  "Freya", "Nora", "Clara", "Lena", "Ingrid", "Elena", "Lucia", "Yuki",
  "Maya", "Hannah", "Astrid", "Camille", "Sofia", "Greta", "Naomi", "Alice",
];
export const DEMO_LAST_NAMES = [
  "Carter", "Bennett", "Hayes", "Brooks", "Reed", "Sullivan", "Foster",
  "Nguyen", "Schmidt", "Müller", "Larsen", "Andersson", "Tanaka", "Rossi",
  "Dubois", "Novak", "Kowalski", "Fischer", "Walsh", "Marsh", "Holt",
  "Vance", "Ellis", "Frost", "Lindqvist", "Moreau", "Costa", "Becker",
];

// Stable seed so the same store always regenerates the same demo dataset
// (idempotent re-seed) while differing from any real merchant's data.
export const DEMO_SEED = 0x4e4f5256; // "NORV"
