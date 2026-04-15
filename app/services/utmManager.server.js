import db from "../db.server";

/**
 * UTM Manager — audits, recommends, and pushes UTM parameters to Meta ads.
 *
 * Runs during:
 * 1. Onboarding (after Meta connect) — full audit + optional push
 * 2. Nightly entity sync — scan new ads + auto-fill missing UTMs
 *
 * UTMs live on AdCreative.url_tags (not on the Ad object).
 * To update: POST to /{creative-id} with url_tags parameter.
 */

const DEFAULT_UTM_TEMPLATE =
  "utm_source=facebook&utm_medium=cpc&utm_campaign={{campaign.name}}&utm_content={{ad.name}}&utm_term={{adset.name}}";

async function fetchWithRetry(url, options = {}, retries = 3) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const res = await fetch(url, options);
      const data = await res.json();
      if (!data.error) return data;
      console.error(`[UTMManager] Attempt ${attempt} failed: ${data.error.message}`);
    } catch (err) {
      console.error(`[UTMManager] Attempt ${attempt} fetch error: ${err.message}`);
    }
    if (attempt < retries) await new Promise(r => setTimeout(r, 2000));
  }
  return null;
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

  // Fetch all ads with effective_status for accurate delivery state
  const allAds = await fetchAllPages(
    `https://graph.facebook.com/v21.0/${accountId}/ads?fields=id,name,status,effective_status,creative{id,url_tags,effective_object_story_id},campaign{id,name},adset{id,name}&limit=100&access_token=${token}`
  );

  console.log(`[UTMManager] Fetched ${allAds.length} ads`);

  const patterns = {};
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

  // Find the dominant pattern
  const sortedPatterns = Object.entries(patterns).sort((a, b) => b[1] - a[1]);
  const dominantPattern = sortedPatterns[0]?.[0] || DEFAULT_UTM_TEMPLATE;

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
    recommendedTemplate: shop.utmTemplate || dominantPattern,
    mixedCampaigns,
    adList,
  };

  // Update shop with audit results — store delivering counts as the headline numbers
  await db.shop.update({
    where: { shopDomain },
    data: {
      utmLastAudit: new Date(),
      utmAdsTotal: deliveringTotal,
      utmAdsWithTags: deliveringTotal - deliveringMissing,
      utmAdsMissing: deliveringMissing,
      ...(shop.utmTemplate ? {} : { utmTemplate: dominantPattern }),
    },
  });

  console.log(`[UTMManager] Audit complete: ${deliveringMissing} delivering ads missing UTMs (${notDeliveringMissing} non-delivering also missing)`);
  return result;
}

// ── Push UTMs ────────────────────────────────────────────────────────

/**
 * Pushes UTM template to ads that are missing url_tags.
 *
 * Meta creatives are immutable for url_tags — you can't update an existing creative.
 * Instead we: 1) create a NEW creative using the same object_story_id + url_tags,
 *             2) point the ad to the new creative.
 *
 * @param shopDomain - shop identifier
 * @param options.activeOnly - only fix active ads (default: true)
 * @param options.template - UTM template to use (defaults to shop's saved template)
 * @param options.dryRun - if true, just return what would be changed without changing it
 * @returns { fixed, failed, skipped, errors }
 */
export async function pushUtms(shopDomain, options = {}) {
  const { activeOnly = true, template, dryRun = false } = options;

  const shop = await db.shop.findUnique({ where: { shopDomain } });
  if (!shop?.metaAccessToken || !shop?.metaAdAccountId) {
    return { error: "Meta not connected" };
  }

  const token = shop.metaAccessToken;
  const accountId = shop.metaAdAccountId;
  const utmTemplate = template || shop.utmTemplate || DEFAULT_UTM_TEMPLATE;

  console.log(`[UTMManager] Push UTMs for ${shopDomain} (dryRun: ${dryRun}, activeOnly: ${activeOnly})`);
  console.log(`[UTMManager] Template: ${utmTemplate}`);

  // Fetch ads — use effective_status to only target actually-delivering ads
  const statusValues = activeOnly
    ? '["ACTIVE"]'
    : '["ACTIVE","PAUSED","ADSET_PAUSED","CAMPAIGN_PAUSED","WITH_ISSUES","PENDING_REVIEW"]';
  const statusFilter = `&filtering=[{"field":"effective_status","operator":"IN","value":${statusValues}}]`;
  const allAds = await fetchAllPages(
    `https://graph.facebook.com/v21.0/${accountId}/ads?fields=id,name,effective_status,creative{id,url_tags,effective_object_story_id}&limit=100${statusFilter}&access_token=${token}`
  );

  const toFix = allAds.filter(ad => !ad.creative?.url_tags);
  console.log(`[UTMManager] Found ${toFix.length} ads to fix out of ${allAds.length}`);

  if (dryRun) {
    return {
      dryRun: true,
      wouldFix: toFix.length,
      template: utmTemplate,
      ads: toFix.map(ad => ({ adId: ad.id, adName: ad.name, creativeId: ad.creative?.id, status: ad.status })),
    };
  }

  let fixed = 0;
  let failed = 0;
  let skipped = 0;
  const errors = [];

  for (const ad of toFix) {
    const storyId = ad.creative?.effective_object_story_id;
    if (!storyId) {
      skipped++;
      continue;
    }

    // Step 1: Create a new creative with url_tags using the same story
    const createResult = await fetchWithRetry(
      `https://graph.facebook.com/v21.0/${accountId}/adcreatives`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          object_story_id: storyId,
          url_tags: utmTemplate,
          access_token: token,
        }),
      }
    );

    if (!createResult?.id) {
      failed++;
      errors.push({ adId: ad.id, adName: ad.name, step: "create_creative", error: createResult });
      continue;
    }

    // Step 2: Point the ad to the new creative
    const updateResult = await fetchWithRetry(
      `https://graph.facebook.com/v21.0/${ad.id}`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          creative: { creative_id: createResult.id },
          access_token: token,
        }),
      }
    );

    if (updateResult?.success) {
      fixed++;
      if (fixed % 50 === 0) console.log(`[UTMManager] Fixed ${fixed}/${toFix.length}...`);
    } else {
      failed++;
      errors.push({ adId: ad.id, adName: ad.name, step: "update_ad", newCreativeId: createResult.id, error: updateResult });
    }

    // Rate limit: small delay between writes to avoid hitting Meta API limits
    if ((fixed + failed) % 10 === 0) await new Promise(r => setTimeout(r, 500));
  }

  // Update shop stats
  await db.shop.update({
    where: { shopDomain },
    data: {
      utmAdsFixed: { increment: fixed },
      utmAdsMissing: { decrement: fixed },
    },
  });

  console.log(`[UTMManager] Push complete: ${fixed} fixed, ${failed} failed, ${skipped} skipped`);
  return { fixed, failed, skipped, errors: errors.slice(0, 10), template: utmTemplate };
}

// ── Push UTMs to specific ads ────────────────────────────────────────

/**
 * Push UTM tags to a specific list of ads.
 * Each entry: { adId, urlTags }
 * Creates a new creative with url_tags, then points the ad to it.
 */
export async function pushUtmsToAds(shopDomain, adUpdates) {
  const shop = await db.shop.findUnique({ where: { shopDomain } });
  if (!shop?.metaAccessToken || !shop?.metaAdAccountId) {
    return { error: "Meta not connected" };
  }

  const token = shop.metaAccessToken;
  const accountId = shop.metaAdAccountId;

  // Fetch ads to get their creative story IDs
  const adIds = adUpdates.map(u => u.adId);
  const allAds = await fetchAllPages(
    `https://graph.facebook.com/v21.0/${accountId}/ads?fields=id,creative{id,effective_object_story_id}&filtering=[{"field":"id","operator":"IN","value":${JSON.stringify(adIds)}}]&limit=100&access_token=${token}`
  );
  const adMap = {};
  for (const ad of allAds) adMap[ad.id] = ad;

  let fixed = 0, failed = 0, skipped = 0;
  const errors = [];

  for (const update of adUpdates) {
    const ad = adMap[update.adId];
    const storyId = ad?.creative?.effective_object_story_id;
    if (!storyId) { skipped++; continue; }

    const createResult = await fetchWithRetry(
      `https://graph.facebook.com/v21.0/${accountId}/adcreatives`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          object_story_id: storyId,
          url_tags: update.urlTags,
          access_token: token,
        }),
      }
    );

    if (!createResult?.id) {
      failed++;
      errors.push({ adId: update.adId, error: createResult });
      continue;
    }

    const updateResult = await fetchWithRetry(
      `https://graph.facebook.com/v21.0/${update.adId}`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          creative: { creative_id: createResult.id },
          access_token: token,
        }),
      }
    );

    if (updateResult?.success) { fixed++; }
    else { failed++; errors.push({ adId: update.adId, error: updateResult }); }

    if ((fixed + failed) % 10 === 0) await new Promise(r => setTimeout(r, 500));
  }

  console.log(`[UTMManager] Push to specific ads: ${fixed} fixed, ${failed} failed, ${skipped} skipped`);
  return { fixed, failed, skipped, errors: errors.slice(0, 10) };
}

// ── Nightly audit (called during nightly sync) ──────────────────────

/**
 * Nightly audit: checks active ads for missing/inconsistent UTMs.
 * NEVER auto-pushes — only updates stats so the UI can flag issues.
 * Merchant must explicitly approve and trigger pushes from the UTM Manager page.
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

  // Check for mixed patterns
  const patterns = new Set(withTags.map(a => a.creative.url_tags));
  const hasInconsistency = patterns.size > 1;

  await db.shop.update({
    where: { shopDomain },
    data: {
      utmLastAudit: new Date(),
      utmAdsTotal: activeAds.length,
      utmAdsWithTags: withTags.length,
      utmAdsMissing: missing.length,
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
