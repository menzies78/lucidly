import db from "../db.server";

/**
 * UTM Manager - audits Meta ads' UTM parameters and recommends fixes.
 *
 * REPORT-ONLY since 2026-07-08: the push-to-Meta write paths (create creative
 * with url_tags + repoint ad) were removed so the OAuth scope could drop to
 * ads_read ahead of Meta App Review. The merchant applies the recommended
 * url_tags themselves in Ads Manager; the UI spells out exactly what to paste
 * and where. If one-click fixing ever returns, it comes back as an opt-in
 * incremental re-auth for ads_management - see git history for the old code.
 *
 * Runs during:
 * 1. On-demand audit from the UTM Manager page
 * 2. Nightly entity sync - scan + flag (never writes)
 *
 * UTMs live on AdCreative.url_tags (not on the Ad object).
 */

const DEFAULT_UTM_TEMPLATE =
  "utm_source=facebook&utm_medium=cpc&utm_campaign={{campaign.name}}&utm_content={{ad.name}}&utm_term={{adset.name}}";

async function fetchWithRetry(url, options = {}, retries = 3) {
  let lastError = null;
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const res = await fetch(url, options);
      const data = await res.json();
      if (!data.error) return data;
      lastError = data.error;
      console.error(`[UTMManager] Attempt ${attempt} failed: ${data.error.message}`);
    } catch (err) {
      lastError = { message: err.message, code: "network_error" };
      console.error(`[UTMManager] Attempt ${attempt} fetch error: ${err.message}`);
    }
    if (attempt < retries) await new Promise(r => setTimeout(r, 2000));
  }
  // Return the last error wrapped so callers can surface a precise message to
  // the merchant. Previously we returned `null` here — that erased Meta's
  // explanation (e.g. "Object cannot have url_tags set" for Advantage+/DPA
  // creatives) and the UI could only say "8 failed".
  return { __failed: true, error: lastError };
}

async function fetchAllPages(url) {
  const results = [];
  let nextUrl = url;
  while (nextUrl) {
    const data = await fetchWithRetry(nextUrl);
    if (!data) break;
    if (data.data) results.push(...data.data);
    nextUrl = data.paging?.next || null;
  }
  return results;
}

// ── Audit ────────────────────────────────────────────────────────────

// Demo shops have a placeholder Meta token and no real ad account, so the
// Graph API can't be called. Reconstruct the ad list from the seeded
// MetaInsight rows (which denormalise campaign/adset/ad names + ids), shaped
// exactly like the Graph `/ads` response so the rest of auditUtms is unchanged.
// Deterministic assignment driven by the shop's persisted counts: the first
// `missingN` ads are missing UTMs, the next `driftedN` carry a drifted pattern,
// and the remainder carry the dominant template. Driving off the DB counts lets
// a demo "fix missing UTMs" action (which zeroes utmAdsMissing) reflect back on
// the next audit, so the fix flow reads coherently without any live API call.
const DEMO_DOMINANT_UTM = "utm_source=facebook&utm_medium=paid&utm_campaign={{campaign.name}}&utm_content={{ad.name}}";
const DEMO_DRIFTED_UTM = "utm_source=fb&utm_medium=cpc&utm_campaign={{campaign.name}}";
async function buildDemoAdsForAudit(shopDomain, missingN = 2, driftedN = 1) {
  const rows = await db.metaInsight.findMany({
    where: { shopDomain, adId: { not: null } },
    select: {
      adId: true, adName: true, adSetId: true, adSetName: true,
      campaignId: true, campaignName: true,
    },
    distinct: ["adId"],
    orderBy: { adId: "asc" },
  });
  return rows.map((r, i) => {
    let urlTags = DEMO_DOMINANT_UTM;
    if (i < missingN) urlTags = "";
    else if (i < missingN + driftedN) urlTags = DEMO_DRIFTED_UTM;
    return {
      id: r.adId,
      name: r.adName || "",
      status: "ACTIVE",
      effective_status: "ACTIVE",
      creative: { id: `${r.adId}-cr`, url_tags: urlTags, effective_object_story_id: `story_${r.adId}` },
      campaign: { id: r.campaignId || "", name: r.campaignName || "Unknown" },
      adset: { id: r.adSetId || "", name: r.adSetName || "" },
    };
  });
}

/**
 * Audits all ads in the Meta ad account for UTM consistency.
 * Returns structured results: patterns found, gaps, recommendations.
 */
export async function auditUtms(shopDomain) {
  const shop = await db.shop.findUnique({ where: { shopDomain } });
  if (!shop?.metaAccessToken || !shop?.metaAdAccountId) {
    return { error: "Meta not connected" };
  }

  const token = shop.metaAccessToken;
  const accountId = shop.metaAdAccountId;

  console.log(`[UTMManager] Starting audit for ${shopDomain}`);

  // Fetch all ads with effective_status for accurate delivery state. Demo shops
  // can't hit the Graph API, so synthesise the same shape from seeded data.
  const allAds = shop.demoMode
    ? await buildDemoAdsForAudit(shopDomain, shop.utmAdsMissing ?? 2, shop.utmAdsInconsistent ?? 1)
    : await fetchAllPages(
        `https://graph.facebook.com/v21.0/${accountId}/ads?fields=id,name,status,effective_status,creative{id,url_tags,effective_object_story_id},campaign{id,name},adset{id,name}&limit=100&access_token=${token}`
      );

  console.log(`[UTMManager] Fetched ${allAds.length} ads`);

  const patterns = {};
  const deliveringPatterns = {};
  const inconsistentCampaigns = {};

  // Group by effective_status
  // Only ACTIVE counts as live. WITH_ISSUES = old broken ads, PENDING_REVIEW = briefly in review.
  const DELIVERING = ["ACTIVE"];
  const statusBreakdown = {};

  for (const ad of allAds) {
    const tags = ad.creative?.url_tags || "";
    const es = ad.effective_status || "UNKNOWN";
    const campaignName = ad.campaign?.name || "Unknown";

    if (!statusBreakdown[es]) statusBreakdown[es] = { total: 0, withUtm: 0, noUtm: 0, missing: [] };
    statusBreakdown[es].total++;

    if (tags) {
      statusBreakdown[es].withUtm++;
      patterns[tags] = (patterns[tags] || 0) + 1;
      if (DELIVERING.includes(es)) {
        deliveringPatterns[tags] = (deliveringPatterns[tags] || 0) + 1;
      }
      if (!inconsistentCampaigns[campaignName]) inconsistentCampaigns[campaignName] = new Set();
      inconsistentCampaigns[campaignName].add(tags);
    } else {
      statusBreakdown[es].noUtm++;
      statusBreakdown[es].missing.push({
        adId: ad.id,
        adName: ad.name,
        creativeId: ad.creative?.id,
        effectiveStatus: es,
        campaignName,
        adsetName: ad.adset?.name || "",
      });
    }
  }

  // Compute delivering vs not-delivering totals
  let deliveringTotal = 0, deliveringMissing = 0;
  let notDeliveringTotal = 0, notDeliveringMissing = 0;
  const deliveringMissingAds = [];

  for (const [es, counts] of Object.entries(statusBreakdown)) {
    if (DELIVERING.includes(es)) {
      deliveringTotal += counts.total;
      deliveringMissing += counts.noUtm;
      deliveringMissingAds.push(...counts.missing);
    } else {
      notDeliveringTotal += counts.total;
      notDeliveringMissing += counts.noUtm;
    }
  }

  // Find the dominant pattern. Judge health against what DELIVERING ads carry -
  // large accounts have thousands of paused/archived ads whose (older) pattern
  // would otherwise outvote the live one and mis-flag every active ad.
  const sortedPatterns = Object.entries(patterns).sort((a, b) => b[1] - a[1]);
  const sortedDeliveringPatterns = Object.entries(deliveringPatterns).sort((a, b) => b[1] - a[1]);
  const dominantPattern =
    sortedDeliveringPatterns[0]?.[0] || sortedPatterns[0]?.[0] || DEFAULT_UTM_TEMPLATE;

  // Flag campaigns with mixed patterns
  const mixedCampaigns = [];
  for (const [name, patternSet] of Object.entries(inconsistentCampaigns)) {
    if (patternSet.size > 1) {
      mixedCampaigns.push({ campaign: name, patterns: Array.from(patternSet) });
    }
  }

  const totalWithUtm = Object.values(statusBreakdown).reduce((s, c) => s + c.withUtm, 0);
  const totalMissing = Object.values(statusBreakdown).reduce((s, c) => s + c.noUtm, 0);

  // Build full ad list for the table view
  const adList = allAds.map(ad => ({
    adId: ad.id,
    adName: ad.name || "",
    campaignId: ad.campaign?.id || "",
    campaignName: ad.campaign?.name || "",
    adsetId: ad.adset?.id || "",
    adsetName: ad.adset?.name || "",
    creativeId: ad.creative?.id || "",
    urlTags: ad.creative?.url_tags || "",
    effectiveStatus: ad.effective_status || "UNKNOWN",
    hasStoryId: !!ad.creative?.effective_object_story_id,
  }));

  const result = {
    totalAds: allAds.length,
    adsWithTags: totalWithUtm,
    adsWithoutTags: totalMissing,
    // Delivering = actually running or about to run
    deliveringTotal,
    deliveringMissing,
    deliveringMissingAds,
    // Not delivering = paused, campaign off, adset off, etc.
    notDeliveringTotal,
    notDeliveringMissing,
    // Breakdown by effective_status
    statusBreakdown: Object.entries(statusBreakdown)
      .sort((a, b) => b[1].total - a[1].total)
      .map(([status, counts]) => ({ status, ...counts, missing: undefined })),
    patternCount: sortedPatterns.length,
    patterns: sortedPatterns.map(([pattern, count]) => ({ pattern, count })),
    dominantPattern,
    recommendedTemplate: shop.utmTemplate || dominantPattern || DEFAULT_UTM_TEMPLATE,
    mixedCampaigns,
    adList,
  };

  // Compute consistency: among DELIVERING ads with UTMs, how many match the
  // dominant pattern vs differ from it. Reported on the dashboard UTM Health
  // tile so merchants notice tag drift without opening the UTM Manager.
  let consistentCount = 0;
  let inconsistentCount = 0;
  for (const ad of allAds) {
    if (!DELIVERING.includes(ad.effective_status)) continue;
    const tags = ad.creative?.url_tags || "";
    if (!tags) continue;
    if (tags === dominantPattern) consistentCount++;
    else inconsistentCount++;
  }

  // Update shop with audit results - store delivering counts as the headline numbers
  await db.shop.update({
    where: { shopDomain },
    data: {
      utmLastAudit: new Date(),
      utmAdsTotal: deliveringTotal,
      utmAdsWithTags: deliveringTotal - deliveringMissing,
      utmAdsMissing: deliveringMissing,
      utmDominantPattern: dominantPattern || "",
      utmAdsConsistent: consistentCount,
      utmAdsInconsistent: inconsistentCount,
      ...(shop.utmTemplate ? {} : { utmTemplate: dominantPattern }),
    },
  });

  console.log(`[UTMManager] Audit complete: ${deliveringMissing} delivering ads missing UTMs (${notDeliveringMissing} non-delivering also missing); ${consistentCount} consistent / ${inconsistentCount} inconsistent`);
  return result;
}

// ── Nightly audit (called during nightly sync) ──────────────────────

/**
 * Nightly audit: checks active ads for missing/inconsistent UTMs.
 * Only updates stats so the UI can flag issues - the merchant applies any
 * fixes themselves in Ads Manager (the app holds no write scope).
 */
export async function nightlyUtmAudit(shopDomain) {
  const shop = await db.shop.findUnique({ where: { shopDomain } });
  if (!shop?.metaAccessToken || !shop?.metaAdAccountId) {
    return { scanned: 0 };
  }

  const token = shop.metaAccessToken;
  const accountId = shop.metaAdAccountId;

  // Only check actually-delivering ads
  const activeAds = await fetchAllPages(
    `https://graph.facebook.com/v21.0/${accountId}/ads?fields=id,name,creative{id,url_tags}&filtering=[{"field":"effective_status","operator":"IN","value":["ACTIVE"]}]&limit=100&access_token=${token}`
  );

  const withTags = activeAds.filter(ad => ad.creative?.url_tags);
  const missing = activeAds.filter(ad => !ad.creative?.url_tags);

  // Tally patterns to find the dominant one + count consistency.
  const patternCounts = {};
  for (const a of withTags) {
    const t = a.creative.url_tags;
    patternCounts[t] = (patternCounts[t] || 0) + 1;
  }
  const sortedPatterns = Object.entries(patternCounts).sort((a, b) => b[1] - a[1]);
  const dominantPattern = sortedPatterns[0]?.[0] || "";
  let consistentCount = 0, inconsistentCount = 0;
  for (const a of withTags) {
    if (a.creative.url_tags === dominantPattern) consistentCount++;
    else inconsistentCount++;
  }
  const hasInconsistency = sortedPatterns.length > 1;

  await db.shop.update({
    where: { shopDomain },
    data: {
      utmLastAudit: new Date(),
      utmAdsTotal: activeAds.length,
      utmAdsWithTags: withTags.length,
      utmAdsMissing: missing.length,
      utmDominantPattern: dominantPattern,
      utmAdsConsistent: consistentCount,
      utmAdsInconsistent: inconsistentCount,
    },
  });

  if (missing.length > 0 || hasInconsistency) {
    console.log(`[UTMManager] Nightly audit: ${missing.length} active ads missing UTMs${hasInconsistency ? ", inconsistent patterns detected" : ""}`);
  } else {
    console.log(`[UTMManager] Nightly audit: all ${activeAds.length} active ads have consistent UTMs`);
  }

  return { scanned: activeAds.length, missing: missing.length, inconsistent: hasInconsistency };
}

// ── Save template ────────────────────────────────────────────────────

export async function saveUtmTemplate(shopDomain, template) {
  await db.shop.update({
    where: { shopDomain },
    data: { utmTemplate: template },
  });
  console.log(`[UTMManager] Template saved for ${shopDomain}: ${template}`);
}
