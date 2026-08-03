import db from "../db.server.js";
import { shopLocalDayKey } from "../utils/shopTime.server";

/**
 * Rebuild DailyGeoRollup rows for a shop, plus the geo:topProducts
 * ShopAnalysisCache blob.
 *
 * Strategy:
 *  1. Aggregate MetaBreakdown (country) SQL-side — one GROUP BY query per
 *     fan-out level (overall/campaign/adset/ad). The raw table is the largest
 *     in the DB (1.15M+ rows for a mature shop) and loading it into JS
 *     objects cost ~GBs of heap; the grouped results are the same size as
 *     the bucket map we build anyway. Order + Attribution stay JS-side
 *     (small). Geo aggregates roll over time so historical rebuilds are
 *     non-negotiable.
 *  2. For each shop-local day, build one bucket per (level, entityId, country)
 *     where level ∈ {"overall","campaign","adset","ad"} and entityId is "" for
 *     overall. Aggregate Meta breakdown into ALL applicable buckets
 *     (overall + campaign + adset + ad slices a single MetaBreakdown row
 *     contributes to).
 *  3. Join matched attributions to their order's countryCode and stamp the
 *     same four-level fan-out on the order's createdAt day.
 *  4. UTM-only orders (utmConfirmedMeta && unmatched) fan out the same way.
 *  5. Unmatched-attribution unverifiedRevenue is distributed within a day by
 *     that day's entity-country conversion share, mirroring the loader logic
 *     but localised to the day (totals identical, country mix more accurate
 *     over multi-day windows).
 *  6. Per-bucket sets of new-customer IDs are stringified to JSON so the
 *     loader can merge into a Set across the requested window for unique-new
 *     counts (per-day uniqueness is insufficient — a customer's "new" status
 *     applies to the whole window).
 *  7. Wipe + chunked insert.
 *  8. Build a per-country top-products cube (parent-stripped, segment ×
 *     gender) and upsert it under cacheKey "geo:topProducts". This blob is
 *     all-time and not date-filtered — same contract as customers:map.
 *
 * Loader reads rollup rows for the requested window and sums in JS
 * (O(rows-in-window-x-countries)). No raw scans on page load.
 */
export async function rebuildGeoRollups(shopDomain) {
  const t0 = Date.now();

  const shopRow = await db.shop.findUnique({
    where: { shopDomain },
    select: { shopifyTimezone: true, productImagesJson: true },
  });
  const tz = shopRow?.shopifyTimezone || "UTC";

  // SQLite stores DateTime as epoch-ms INTEGER; raw-query integers come back
  // as BigInt, so Int-column aggregates go through num().
  const num = (v) => (typeof v === "bigint" ? Number(v) : v || 0);

  // One grouped query per fan-out level. Every level sums the same measures;
  // entity levels additionally carry denorm names (MAX picks a deterministic
  // representative — names are constant within an entity in practice).
  const AGG = `
        SUM(spend)            AS spend,
        SUM(impressions)      AS impressions,
        SUM(clicks)           AS clicks,
        SUM(reach)            AS reach,
        SUM(conversions)      AS conversions,
        SUM(conversionValue)  AS conversionValue,
        SUM(linkClicks)       AS linkClicks,
        SUM(landingPageViews) AS landingPageViews`;
  const BASE_WHERE = `shopDomain = ? AND breakdownType = 'country'
        AND breakdownValue IS NOT NULL AND breakdownValue != ''`;
  const levelQuery = (level) => {
    if (level === "overall") {
      return `SELECT date, breakdownValue AS cc, ${AGG}
        FROM MetaBreakdown WHERE ${BASE_WHERE}
        GROUP BY date, breakdownValue`;
    }
    if (level === "campaign") {
      return `SELECT date, breakdownValue AS cc, campaignId AS entityId,
        MAX(COALESCE(campaignName, '')) AS campaignName, ${AGG}
        FROM MetaBreakdown WHERE ${BASE_WHERE}
        AND campaignId IS NOT NULL AND campaignId != ''
        GROUP BY date, breakdownValue, campaignId`;
    }
    if (level === "adset") {
      return `SELECT date, breakdownValue AS cc, adSetId AS entityId,
        MAX(COALESCE(adSetName, ''))    AS adSetName,
        MAX(COALESCE(campaignId, ''))   AS campaignId,
        MAX(COALESCE(campaignName, '')) AS campaignName, ${AGG}
        FROM MetaBreakdown WHERE ${BASE_WHERE}
        AND adSetId IS NOT NULL AND adSetId != ''
        GROUP BY date, breakdownValue, adSetId`;
    }
    return `SELECT date, breakdownValue AS cc, adId AS entityId,
        MAX(COALESCE(adName, ''))       AS adName,
        MAX(COALESCE(campaignId, ''))   AS campaignId,
        MAX(COALESCE(campaignName, '')) AS campaignName,
        MAX(COALESCE(adSetId, ''))      AS adSetId,
        MAX(COALESCE(adSetName, ''))    AS adSetName, ${AGG}
        FROM MetaBreakdown WHERE ${BASE_WHERE}
        AND adId IS NOT NULL AND adId != ''
        GROUP BY date, breakdownValue, adId`;
  };

  const [bdOverall, bdCampaign, bdAdset, bdAd, orders, attributions] = await Promise.all([
    db.$queryRawUnsafe(levelQuery("overall"), shopDomain),
    db.$queryRawUnsafe(levelQuery("campaign"), shopDomain),
    db.$queryRawUnsafe(levelQuery("adset"), shopDomain),
    db.$queryRawUnsafe(levelQuery("ad"), shopDomain),
    db.order.findMany({
      where: { shopDomain, isOnlineStore: true },
      select: {
        shopifyOrderId: true, shopifyCustomerId: true,
        createdAt: true, countryCode: true,
        frozenTotalPrice: true, totalRefunded: true,
        utmConfirmedMeta: true, isNewCustomerOrder: true,
        customerOrderCountAtPurchase: true,
        metaAdId: true, metaAdName: true,
        metaAdSetId: true, metaAdSetName: true,
        metaCampaignId: true, metaCampaignName: true,
      },
    }),
    db.attribution.findMany({
      where: { shopDomain },
      select: {
        shopifyOrderId: true, confidence: true, isNewCustomer: true,
        metaConversionValue: true, metaGender: true,
        metaCampaignId: true, metaCampaignName: true,
        metaAdSetId: true, metaAdSetName: true,
        metaAdId: true, metaAdName: true,
        matchedAt: true,
      },
    }),
  ]);

  const orderMap = new Map();
  for (const o of orders) orderMap.set(o.shopifyOrderId, o);

  // bucket key: `${dayKey}|${level}|${entityId}|${country}`
  // entityId="" for level="overall".
  const buckets = new Map();

  const makeBucket = (dayKey, level, entityId, country, seed) => {
    const key = `${dayKey}|${level}|${entityId}|${country}`;
    let b = buckets.get(key);
    if (!b) {
      b = {
        date: new Date(`${dayKey}T00:00:00.000Z`),
        level,
        entityId,
        country,
        entityName: seed?.entityName || null,
        campaignId: seed?.campaignId || null,
        campaignName: seed?.campaignName || null,
        adSetId: seed?.adSetId || null,
        adSetName: seed?.adSetName || null,
        spend: 0, impressions: 0, clicks: 0, reach: 0,
        metaConversions: 0, metaConversionValue: 0,
        linkClicks: 0, landingPageViews: 0,
        attributedOrders: 0, attributedRevenue: 0,
        newCustomerOrders: 0, newCustomerRevenue: 0,
        existingCustomerOrders: 0, existingCustomerRevenue: 0,
        newCustomerIds: new Set(),
        utmOnlyOrders: 0, utmOnlyRevenue: 0,
        utmOnlyNewOrders: 0, utmOnlyNewRevenue: 0,
        utmOnlyNewCustomerIds: new Set(),
        unverifiedRevenue: 0,
        shopifyOrders: 0, shopifyRevenue: 0,
      };
      buckets.set(key, b);
    }
    // Refresh denorm names (latest seen wins)
    if (seed?.entityName) b.entityName = seed.entityName;
    if (seed?.campaignId) b.campaignId = seed.campaignId;
    if (seed?.campaignName) b.campaignName = seed.campaignName;
    if (seed?.adSetId) b.adSetId = seed.adSetId;
    if (seed?.adSetName) b.adSetName = seed.adSetName;
    return b;
  };

  // ── 1. Meta breakdowns → overall + per-entity buckets ──
  // Also stash per-(day,level,entityId,country) → conversions for the
  // unverified-revenue distribution step.
  const dayEntityConv = new Map(); // `${day}|${level}|${entityId}` → Map<country, conv>
  const recordConv = (day, level, entityId, cc, conv) => {
    const k = `${day}|${level}|${entityId}`;
    let m = dayEntityConv.get(k);
    if (!m) { m = new Map(); dayEntityConv.set(k, m); }
    m.set(cc, (m.get(cc) || 0) + conv);
  };

  // Buckets can still merge across raw dates when the tz projection folds two
  // UTC dates into one local day, so these stay += merges.
  const applyGroup = (g, level) => {
    const day = shopLocalDayKey(tz, new Date(num(g.date)));
    const seed =
      level === "overall" ? null
      : level === "campaign" ? {
          entityName: g.campaignName || g.entityId,
          campaignId: g.entityId, campaignName: g.campaignName || null,
        }
      : level === "adset" ? {
          entityName: g.adSetName || g.entityId,
          campaignId: g.campaignId || null, campaignName: g.campaignName || null,
          adSetId: g.entityId, adSetName: g.adSetName || null,
        }
      : {
          entityName: g.adName || g.entityId,
          campaignId: g.campaignId || null, campaignName: g.campaignName || null,
          adSetId: g.adSetId || null, adSetName: g.adSetName || null,
        };
    const b = makeBucket(day, level, level === "overall" ? "" : g.entityId, g.cc, seed);
    b.spend += g.spend || 0;
    b.impressions += num(g.impressions);
    b.clicks += num(g.clicks);
    b.reach += num(g.reach);
    b.metaConversions += num(g.conversions);
    b.metaConversionValue += g.conversionValue || 0;
    b.linkClicks += num(g.linkClicks);
    b.landingPageViews += num(g.landingPageViews);
    if (level !== "overall") recordConv(day, level, g.entityId, g.cc, num(g.conversions));
  };

  for (const g of bdOverall) applyGroup(g, "overall");
  for (const g of bdCampaign) applyGroup(g, "campaign");
  for (const g of bdAdset) applyGroup(g, "adset");
  for (const g of bdAd) applyGroup(g, "ad");

  // ── 2. Matched attributions (confidence>0) ──
  const matchedOrderIds = new Set();
  for (const a of attributions) {
    if (a.confidence <= 0) continue;
    matchedOrderIds.add(a.shopifyOrderId);

    const o = orderMap.get(a.shopifyOrderId);
    if (!o) continue;
    const gross = o.frozenTotalPrice || 0;
    if (gross === 0) continue; // £0 orders excluded
    const rev = Math.max(0, gross - (o.totalRefunded || 0));
    const cc = o.countryCode || "XX";
    const day = shopLocalDayKey(tz, o.createdAt);
    const custId = o.shopifyCustomerId || null;
    const isNew = !!a.isNewCustomer;

    const apply = (b) => {
      b.attributedOrders += 1;
      b.attributedRevenue += rev;
      if (isNew) {
        b.newCustomerOrders += 1;
        b.newCustomerRevenue += rev;
        if (custId) b.newCustomerIds.add(custId);
      } else {
        b.existingCustomerOrders += 1;
        b.existingCustomerRevenue += rev;
      }
    };

    apply(makeBucket(day, "overall", "", cc, null));

    if (a.metaCampaignId) {
      apply(makeBucket(day, "campaign", a.metaCampaignId, cc, {
        entityName: a.metaCampaignName || a.metaCampaignId,
        campaignId: a.metaCampaignId, campaignName: a.metaCampaignName,
      }));
    }
    if (a.metaAdSetId) {
      apply(makeBucket(day, "adset", a.metaAdSetId, cc, {
        entityName: a.metaAdSetName || a.metaAdSetId,
        campaignId: a.metaCampaignId, campaignName: a.metaCampaignName,
        adSetId: a.metaAdSetId, adSetName: a.metaAdSetName,
      }));
    }
    if (a.metaAdId) {
      apply(makeBucket(day, "ad", a.metaAdId, cc, {
        entityName: a.metaAdName || a.metaAdId,
        campaignId: a.metaCampaignId, campaignName: a.metaCampaignName,
        adSetId: a.metaAdSetId, adSetName: a.metaAdSetName,
      }));
    }
  }

  // ── 3. UTM-only orders (utmConfirmedMeta, not in matched set) ──
  for (const o of orders) {
    if (!o.utmConfirmedMeta) continue;
    if (matchedOrderIds.has(o.shopifyOrderId)) continue;
    const gross = o.frozenTotalPrice || 0;
    if (gross === 0) continue;
    const rev = Math.max(0, gross - (o.totalRefunded || 0));
    const cc = o.countryCode || "XX";
    const day = shopLocalDayKey(tz, o.createdAt);
    const custId = o.shopifyCustomerId || null;
    const isNew = !!o.isNewCustomerOrder;

    const apply = (b) => {
      b.attributedOrders += 1;
      b.attributedRevenue += rev;
      b.utmOnlyOrders += 1;
      b.utmOnlyRevenue += rev;
      if (isNew) {
        b.newCustomerOrders += 1;
        b.newCustomerRevenue += rev;
        b.utmOnlyNewOrders += 1;
        b.utmOnlyNewRevenue += rev;
        if (custId) {
          b.newCustomerIds.add(custId);
          b.utmOnlyNewCustomerIds.add(custId);
        }
      } else {
        b.existingCustomerOrders += 1;
        b.existingCustomerRevenue += rev;
      }
    };

    apply(makeBucket(day, "overall", "", cc, null));

    if (o.metaCampaignId) {
      apply(makeBucket(day, "campaign", o.metaCampaignId, cc, {
        entityName: o.metaCampaignName || o.metaCampaignId,
        campaignId: o.metaCampaignId, campaignName: o.metaCampaignName,
      }));
    }
    if (o.metaAdSetId) {
      apply(makeBucket(day, "adset", o.metaAdSetId, cc, {
        entityName: o.metaAdSetName || o.metaAdSetId,
        campaignId: o.metaCampaignId, campaignName: o.metaCampaignName,
        adSetId: o.metaAdSetId, adSetName: o.metaAdSetName,
      }));
    }
    if (o.metaAdId) {
      apply(makeBucket(day, "ad", o.metaAdId, cc, {
        entityName: o.metaAdName || o.metaAdId,
        campaignId: o.metaCampaignId, campaignName: o.metaCampaignName,
        adSetId: o.metaAdSetId, adSetName: o.metaAdSetName,
      }));
    }
  }

  // ── 4. Unmatched (confidence=0) → distribute unverifiedRevenue by that
  //       day's entity-country conversion share (campaign level only feeds
  //       the overall row, matching the loader's prior behaviour). ──
  for (const a of attributions) {
    if (a.confidence > 0) continue;
    const val = a.metaConversionValue || 0;
    if (val === 0) continue;
    // Placeholder shopifyOrderId encodes the date: "unmatched-{YYYY-MM-DD}-..."
    const m = a.shopifyOrderId.match(/(\d{4}-\d{2}-\d{2})/);
    if (!m) continue;
    const day = m[1];

    for (const level of ["campaign", "adset", "ad"]) {
      const entityId = level === "campaign" ? a.metaCampaignId
                     : level === "adset" ? a.metaAdSetId
                     : a.metaAdId;
      if (!entityId) continue;
      const convMap = dayEntityConv.get(`${day}|${level}|${entityId}`);
      if (!convMap) continue;
      let total = 0;
      for (const c of convMap.values()) total += c;
      if (total <= 0) continue;
      for (const [cc, conv] of convMap.entries()) {
        const weight = conv / total;
        const key = `${day}|${level}|${entityId}|${cc}`;
        const b = buckets.get(key);
        if (b) b.unverifiedRevenue += val * weight;
        if (level === "campaign") {
          const ob = buckets.get(`${day}|overall||${cc}`);
          if (ob) ob.unverifiedRevenue += val * weight;
        }
      }
    }
  }

  // ── 5. Shopify-side totals on overall rows (every online order, regardless
  //       of Meta linkage; powers the country map "All Customers" view). ──
  for (const o of orders) {
    const cc = o.countryCode;
    if (!cc) continue;
    const day = shopLocalDayKey(tz, o.createdAt);
    const gross = o.frozenTotalPrice || 0;
    const rev = Math.max(0, gross - (o.totalRefunded || 0));
    const b = makeBucket(day, "overall", "", cc, null);
    b.shopifyOrders += 1;
    b.shopifyRevenue += rev;
  }

  // ── 6. Wipe + chunked insert ──
  // Atomic delete+insert. Without the transaction, concurrent readers see
  // an empty table mid-rebuild and cache zero-value tile data for up to TTL.
  // Destructive conversion: at Vollebak scale the bucket map holds 200k–500k
  // fat objects (incl. two Sets each); materialising the insert rows as a
  // second full-size array on top of it doubles peak heap. Deleting each
  // bucket as it converts lets GC reclaim buckets while rows grow.
  const rows = [];
  for (const [bucketKey, b] of buckets) {
    buckets.delete(bucketKey);
    rows.push({
    shopDomain,
    date: b.date,
    level: b.level,
    entityId: b.entityId,
    country: b.country,
    entityName: b.entityName,
    campaignId: b.campaignId,
    campaignName: b.campaignName,
    adSetId: b.adSetId,
    adSetName: b.adSetName,
    spend: b.spend,
    impressions: b.impressions,
    clicks: b.clicks,
    reach: b.reach,
    metaConversions: b.metaConversions,
    metaConversionValue: b.metaConversionValue,
    linkClicks: b.linkClicks,
    landingPageViews: b.landingPageViews,
    attributedOrders: b.attributedOrders,
    attributedRevenue: b.attributedRevenue,
    newCustomerOrders: b.newCustomerOrders,
    newCustomerRevenue: b.newCustomerRevenue,
    existingCustomerOrders: b.existingCustomerOrders,
    existingCustomerRevenue: b.existingCustomerRevenue,
    newCustomerIdsJson: JSON.stringify(Array.from(b.newCustomerIds)),
    utmOnlyOrders: b.utmOnlyOrders,
    utmOnlyRevenue: b.utmOnlyRevenue,
    utmOnlyNewOrders: b.utmOnlyNewOrders,
    utmOnlyNewRevenue: b.utmOnlyNewRevenue,
    utmOnlyNewCustomerIdsJson: JSON.stringify(Array.from(b.utmOnlyNewCustomerIds)),
    unverifiedRevenue: b.unverifiedRevenue,
    shopifyOrders: b.shopifyOrders,
    shopifyRevenue: b.shopifyRevenue,
    });
  }

  const CHUNK = 500;
  // Vollebak-scale shops (128k+ country breakdown rows, 400+ days, 800+ ads)
  // fan out into 200k–500k DailyGeoRollup rows. SQLite createMany under a
  // transaction lands roughly 5–10k inserts/sec, so the original 60s budget
  // silently rolled back the entire rebuild during onboarding and the merchant
  // was left with an empty Countries tab. 10 minutes is a defensive ceiling
  // that comfortably covers the largest current shops while still bounding
  // any pathological case (the in-memory bucket map would OOM before the
  // wall clock got near 10 min).
  await db.$transaction(async (tx) => {
    await tx.dailyGeoRollup.deleteMany({ where: { shopDomain } });
    for (let i = 0; i < rows.length; i += CHUNK) {
      await tx.dailyGeoRollup.createMany({ data: rows.slice(i, i + CHUNK) });
    }
  }, { timeout: 600000 });

  // Top Products per Country is now built on-demand in the loader for the
  // selected date window (see buildTopProductsCube below + app.geo.tsx).
  // The all-time geo:topProducts ShopAnalysisCache blob was removed because
  // it caused stale-product / wrong-name results when merchants renamed
  // Shopify products mid-history (HM dropped "Cotton Jacquard" from titles
  // in Aug 2025 — old long-name rollup keys still dominated USA top-3
  // even at "Last 30 days").
  console.log(`[geoRollups] ${shopDomain} rebuilt ${rows.length} rows in ${Date.now() - t0}ms (bdGroups=${bdOverall.length + bdCampaign.length + bdAdset.length + bdAd.length}, orders=${orders.length}, attrs=${attributions.length})`);
  return { rows: rows.length, ms: Date.now() - t0 };
}

/**
 * Build a per-country top-products cube (segment × gender) from pre-loaded
 * orders / line items / attributions / customers. Shared between the
 * full-history rebuild path and the geo.tsx loader's on-demand windowed
 * computation. Pure function — no DB access.
 *
 * Returns: [{ cc, products: [{ title, image, mn_F, mn_M, ..., totalUnits,
 * totalRevenue }, ...up to 8], totalCountryUnits }, ...sorted desc].
 */
export function buildTopProductsCube({ orders, lineItems, attributions, customers, productImagesMap, toParentProduct }) {
  const orderMap = new Map();
  for (const o of orders) orderMap.set(o.shopifyOrderId, o);

  const custBySid = new Map();
  for (const c of customers) custBySid.set(c.shopifyCustomerId, c);

  const attrGenderById = new Map();
  for (const a of attributions) {
    if (a.confidence > 0 && a.metaGender && a.metaGender !== "unknown") {
      attrGenderById.set(a.shopifyOrderId, a.metaGender);
    }
  }

  const productsByCountry = {};
  const emptyCell = () => ({
    mn_F: 0, mn_M: 0, mn_U: 0, mr_F: 0, mr_M: 0, mr_U: 0,
    o_F: 0, o_M: 0, o_U: 0, totalUnits: 0, totalRevenue: 0,
  });

  for (const li of lineItems) {
    const ord = orderMap.get(li.shopifyOrderId);
    if (!ord) continue;
    const cc = ord.countryCode;
    if (!cc) continue;
    const cust = ord.shopifyCustomerId ? custBySid.get(ord.shopifyCustomerId) : null;
    const segPrefix = cust?.metaSegment === "metaNew" ? "mn"
                    : cust?.metaSegment === "metaRetargeted" ? "mr"
                    : "o";
    const metaG = attrGenderById.get(li.shopifyOrderId);
    const inferredHighConf = cust?.inferredGender && cust.inferredGenderConfidence != null
      && cust.inferredGenderConfidence >= 0.95 ? cust.inferredGender : null;
    const resolvedG = inferredHighConf || metaG || cust?.inferredGender || null;
    const g = resolvedG === "female" ? "F" : resolvedG === "male" ? "M" : "U";
    const title = toParentProduct(li.title);
    if (!title) continue;
    const netUnits = (li.quantity || 0) - (li.refundedQuantity || 0);
    if (netUnits <= 0) continue;
    const netRev = (li.totalPrice || 0) - (li.refundedAmount || 0);

    if (!productsByCountry[cc]) productsByCountry[cc] = {};
    if (!productsByCountry[cc][title]) productsByCountry[cc][title] = emptyCell();
    const cell = productsByCountry[cc][title];
    cell[`${segPrefix}_${g}`] += netUnits;
    cell.totalUnits += netUnits;
    cell.totalRevenue += netRev;
  }

  return Object.entries(productsByCountry)
    .map(([cc, products]) => {
      const sorted = Object.entries(products)
        .map(([title, cell]) => ({
          title,
          image: productImagesMap[title] || productImagesMap[toParentProduct(title)] || null,
          ...cell,
        }))
        .sort((a, b) => b.totalUnits - a.totalUnits)
        .slice(0, 8);
      const totalCountryUnits = sorted.reduce((s, p) => s + p.totalUnits, 0);
      return { cc, products: sorted, totalCountryUnits };
    })
    .filter(c => c.totalCountryUnits >= 3)
    .sort((a, b) => b.totalCountryUnits - a.totalCountryUnits);
}
