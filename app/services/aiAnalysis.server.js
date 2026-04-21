import Anthropic from "@anthropic-ai/sdk";
import { createHash } from "crypto";
import db from "../db.server.js";

// ═══════════════════════════════════════════════════════════════
// AI ANALYSIS SERVICE
// Generates insights per page using Claude API, caches in DB
// ═══════════════════════════════════════════════════════════════

const MODEL = "claude-sonnet-4-20250514";
const MAX_TOKENS = 2048;

function getClient() {
  const key = process.env.ANTHROPIC_API_KEY;
  if (!key) throw new Error("ANTHROPIC_API_KEY not set");
  return new Anthropic({ apiKey: key });
}

// ── Data hash for cache invalidation ──

export function computeDataHash(data) {
  const str = JSON.stringify(data);
  return createHash("sha256").update(str).digest("hex").slice(0, 16);
}

// ── Cache read/write ──

export async function getCachedInsights(shopDomain, pageKey, dateFrom, dateTo) {
  const row = await db.aiInsight.findUnique({
    where: { shopDomain_pageKey_dateFrom_dateTo: { shopDomain, pageKey, dateFrom, dateTo } },
  });
  if (!row) return null;
  try {
    return {
      insights: JSON.parse(row.insights),
      generatedAt: row.generatedAt,
      dataHash: row.dataHash,
      modelId: row.modelId,
    };
  } catch {
    return null;
  }
}

async function saveInsights(shopDomain, pageKey, dateFrom, dateTo, dataHash, insights, tokenCost) {
  await db.aiInsight.upsert({
    where: { shopDomain_pageKey_dateFrom_dateTo: { shopDomain, pageKey, dateFrom, dateTo } },
    create: { shopDomain, pageKey, dateFrom, dateTo, dataHash, insights: JSON.stringify(insights), modelId: MODEL, tokenCost },
    update: { dataHash, insights: JSON.stringify(insights), generatedAt: new Date(), modelId: MODEL, tokenCost },
  });
}

// ── Trim data to keep prompt under budget ──

function trimRows(rows, limit, keepFields) {
  if (!rows || !Array.isArray(rows)) return [];
  return rows.slice(0, limit).map(r => {
    if (!keepFields) return r;
    const out = {};
    for (const f of keepFields) {
      if (r[f] !== undefined) out[f] = typeof r[f] === "number" ? Math.round(r[f] * 100) / 100 : r[f];
    }
    return out;
  });
}

// ═══════════════════════════════════════════════════════════════
// SYSTEM PROMPT (shared)
// ═══════════════════════════════════════════════════════════════

const SYSTEM_BASE = `You are a senior performance marketing analyst embedded in Lucidly, a Meta Ads attribution app for Shopify merchants. Your job is to analyse the merchant's data and produce ACTIONABLE, SPECIFIC insights.

Rules:
- Be blunt and direct. No fluff, no generic advice.
- Use SPECIFIC numbers from the data. "Your CPA is 43" (with the provided currency symbol) not "Your CPA is high."
- Every observation must reference actual data points.
- Every action must be concrete enough to execute TODAY.
- Think about two things: how to MAKE more money, and how to SAVE money.
- Consider new vs existing customers separately — they have very different economics.
- Flag anomalies, trends, and opportunities others would miss.
- If comparing periods, quantify the change.
- Currency symbol is provided — use it.

Respond with valid JSON only, no markdown, no code fences:
{
  "observations": [
    {
      "type": "positive" | "negative" | "warning" | "opportunity",
      "title": "Short headline (max 80 chars)",
      "body": "1-2 sentences with specific numbers from the data",
      "priority": 1-5 (5 = most important)
    }
  ],
  "actions": [
    {
      "title": "Specific actionable step",
      "body": "Why this matters and what to do. Reference the data.",
      "impact": "high" | "medium" | "low",
      "type": "grow" | "save"
    }
  ]
}

Return 4-6 observations and 3-5 actions. Prioritise the most impactful insights.`;

// ═══════════════════════════════════════════════════════════════
// PAGE-SPECIFIC PROMPT BUILDERS
// ═══════════════════════════════════════════════════════════════

function buildCampaignsPrompt(data, cs) {
  const campaigns = trimRows(data.campaignRows, 15, [
    "entityName", "spend", "impressions", "clicks", "roas", "cpa",
    "newCustomerOrders", "existingCustomerOrders", "attributedOrders", "attributedRevenue",
    "newCustomerCPA", "newCustomerROAS", "avgFrequency", "adAgeDays",
    "atcRate", "checkoutRate", "purchaseRate", "repeatRate", "avgLtv90", "ltvCac",
    "spendPerDay", "newCustomersPerDay", "newCustomerRevPerDay",
  ]);

  const prev = trimRows(data.prevCampaignRows, 15, [
    "entityName", "spend", "roas", "cpa", "attributedOrders", "attributedRevenue",
    "newCustomerOrders", "newCustomerCPA",
  ]);

  const payload = {
    currency: cs,
    reportingDays: data.reportingPeriodDays,
    compareTotals: data.compareTotals,
    campaigns,
    previousPeriodCampaigns: prev.length > 0 ? prev : undefined,
    platformPerformance: data.platformPerf,
    placementPerformance: data.placementPerf,
    dailyTrend: trimRows(data.dailyData, 14, ["date", "spend", "revenue", "roas", "orders", "newOrders"]),
    totalStoreRevenue: data.totalStoreRevenue,
  };

  return {
    system: SYSTEM_BASE,
    user: `Analyse this Campaign Performance data for a Shopify merchant (currency: ${cs}).

Focus on:
1. Which campaigns are efficient vs wasteful? (Compare ROAS, CPA, new customer CPA)
2. Ad fatigue signals (high frequency, old campaigns, declining performance)
3. Funnel drop-offs (ATC → checkout → purchase rates)
4. Period-over-period trends (if comparison data available)
5. Platform/placement efficiency
6. LTV:CAC ratios — are acquired customers profitable long-term?
7. Which campaigns should be scaled vs paused?

DATA:
${JSON.stringify(payload, null, 0)}`,
  };
}

function buildCustomersPrompt(data, cs) {
  const payload = {
    currency: cs,
    metaCustomers: data.metaCount,
    organicCustomers: data.organicCount,
    metaAvgLtv: data.metaAvgLtv,
    organicAvgLtv: data.organicAvgLtv,
    ltvCac: data.ltvCac,
    newCustomerCPA: data.newCustomerCPA,
    metaRepeatRate: data.metaRepeatRate,
    organicRepeatRate: data.organicRepeatRate,
    metaRevenue: data.metaRevenue,
    metaRevenuePct: data.metaRevPct,
    metaNew: { count: data.metaNewCount, avgLtv: data.mnAvgLtv, avgOrders: data.mnAvgOrders, repeatRate: data.mnRepeatRate, avgAov: data.mnAvgAov, cpa: data.mnCPA, ltvCac: data.mnLtvCac, paybackOrders: data.mnPaybackOrders },
    metaRepeat: { count: data.metaRepeatCount, avgLtv: data.mrAvgLtv, avgOrders: data.mrAvgOrders },
    metaRetargeted: { count: data.metaRetargetedCount, avgLtv: data.mrtAvgLtv, avgOrders: data.mrtAvgOrders },
    journey: { firstAOV: data.journeyFirstAOV, secondAOV: data.journeySecondAOV, gapDays: data.journeyGapDays, sampleSize: data.journeyCustomerCount },
    reorderWithin90: data.reorderWithin90,
    medianTimeTo2nd: data.medianTimeTo2nd,
    ageBreakdown: data.ageBreakdown,
    genderBreakdown: data.genderBreakdown,
    topCountries: trimRows(data.topCountries, 5),
  };

  return {
    system: SYSTEM_BASE,
    user: `Analyse this Customer Intelligence data (currency: ${cs}).

Focus on:
1. Meta customer quality — is LTV:CAC > 3x? If not, what's the path?
2. New customer economics — CPA vs what they're worth (LTV)
3. Repeat rate comparison: Meta-acquired vs organic. Are Meta customers coming back?
4. Payback period — how many orders to recoup acquisition cost?
5. Customer journey — first vs second purchase AOV, gap between purchases
6. Demographic performance — which age/gender segments are most valuable?
7. Retention opportunities — reorder within 90 days rate, median time to 2nd purchase

DATA:
${JSON.stringify(payload, null, 0)}`,
  };
}

function buildProductsPrompt(data, cs) {
  const products = trimRows(data.rows, 20, [
    "title", "totalOrders", "metaOrders", "organicOrders", "metaNewOrders", "metaRepeatOrders",
    "totalRevenue", "metaRevenue", "organicRevenue", "refundedOrders", "totalRefunded",
    "firstPurchaseCount", "metaFirstPurchaseCount", "avgPrice",
  ]);

  const payload = {
    currency: cs,
    totalProductCount: data.totalProductCount,
    totalMetaOrders: data.totalMetaOrders,
    totalOrganicOrders: data.totalOrganicOrders,
    totalMetaRevenue: data.totalMetaRevenue,
    totalOrganicRevenue: data.totalOrganicRevenue,
    avgItemsPerBasket: data.avgItemsPerBasket,
    metaAvgItemsPerBasket: data.metaAvgItemsPerBasket,
    topGatewayProduct: data.topGatewayProduct,
    topMetaProduct: data.topMetaProduct,
    highestRefundProduct: data.highestRefundProduct,
    products,
    metaFirstPurchaseProducts: trimRows(data.metaFirstPurchaseList, 10, ["title", "count"]),
    productFlows: trimRows(data.flows, 10),
    top20RefundRateMeta: trimRows(data.top20RefundRateMeta, 10, ["title", "refundedOrders", "totalOrders", "refundRate", "refundedValue"]),
  };

  return {
    system: SYSTEM_BASE,
    user: `Analyse this Product Intelligence data (currency: ${cs}).

Focus on:
1. Gateway products — which products acquire new customers? Are they the right ones to advertise?
2. Meta vs organic product mix — is Meta driving the right products?
3. Refund risk — which products have high refund rates when acquired via Meta?
4. Basket analysis — items per basket, cross-sell opportunities
5. Product purchase flows — what do customers buy first, then second?
6. Revenue concentration — is revenue too dependent on a few products?
7. Cost-effectiveness — which products generate Meta revenue efficiently?

DATA:
${JSON.stringify(payload, null, 0)}`,
  };
}

function buildGeoPrompt(data, cs) {
  const countries = trimRows(data.overallRows, 20, [
    "country", "spend", "impressions", "clicks", "reach",
    "attributedOrders", "attributedRevenue", "newCustomerOrders", "newCustomerRevenue",
    "existingCustomerOrders", "existingCustomerRevenue", "blendedROAS", "cpa",
    "newCustomerCPA", "spendPct", "unverifiedRevenue",
  ]);

  const payload = {
    currency: cs,
    countries,
    shopifyByCountry: data.shopifyByCountry,
    topCampaigns: trimRows(data.campaignEntities, 8, [
      "entityName", "totalSpend", "totalAttributedRevenue", "totalAttributedOrders",
      "totalNewCustomerOrders", "totalNewCustomerRevenue",
    ]),
  };

  return {
    system: SYSTEM_BASE,
    user: `Analyse this Geo Performance data (currency: ${cs}).

Focus on:
1. Country efficiency — which countries have best/worst ROAS and CPA?
2. Spend allocation — is spend proportional to return? Where are the mismatches?
3. New customer acquisition by country — where are new customers cheapest?
4. Untapped markets — countries with Shopify orders but zero Meta spend
5. Concentration risk — is spend too concentrated in one country?
6. Geo-specific campaign performance — any campaigns underperforming in specific countries?
7. Expansion opportunities — data-backed recommendation for which country to expand into

DATA:
${JSON.stringify(payload, null, 0)}`,
  };
}

// ═══════════════════════════════════════════════════════════════
// MAIN ENTRY POINT
// ═══════════════════════════════════════════════════════════════

const PROMPT_BUILDERS = {
  campaigns: buildCampaignsPrompt,
  customers: buildCustomersPrompt,
  products: buildProductsPrompt,
  geo: buildGeoPrompt,
};

// Export defaults so the prompt editor can show them
export function getDefaultPrompts(pageKey, currencySymbol = "£") {
  const builder = PROMPT_BUILDERS[pageKey];
  if (!builder) return { system: SYSTEM_BASE, user: "" };
  // Call with empty data to get the template (focus instructions part only)
  return { system: SYSTEM_BASE, pageKey };
}

export const DEFAULT_SYSTEM_PROMPT = SYSTEM_BASE;

export const DEFAULT_PAGE_PROMPTS = {
  campaigns: `Focus on:
1. Which campaigns are efficient vs wasteful? (Compare ROAS, CPA, new customer CPA)
2. Ad fatigue signals (high frequency, old campaigns, declining performance)
3. Funnel drop-offs (ATC → checkout → purchase rates)
4. Period-over-period trends (if comparison data available)
5. Platform/placement efficiency
6. LTV:CAC ratios — are acquired customers profitable long-term?
7. Which campaigns should be scaled vs paused?`,
  customers: `Focus on:
1. Meta customer quality — is LTV:CAC > 3x? If not, what's the path?
2. New customer economics — CPA vs what they're worth (LTV)
3. Repeat rate comparison: Meta-acquired vs organic. Are Meta customers coming back?
4. Payback period — how many orders to recoup acquisition cost?
5. Customer journey — first vs second purchase AOV, gap between purchases
6. Demographic performance — which age/gender segments are most valuable?
7. Retention opportunities — reorder within 90 days rate, median time to 2nd purchase`,
  products: `Focus on:
1. Gateway products — which products acquire new customers? Are they the right ones to advertise?
2. Meta vs organic product mix — is Meta driving the right products?
3. Refund risk — which products have high refund rates when acquired via Meta?
4. Basket analysis — items per basket, cross-sell opportunities
5. Product purchase flows — what do customers buy first, then second?
6. Revenue concentration — is revenue too dependent on a few products?
7. Cost-effectiveness — which products generate Meta revenue efficiently?`,
  geo: `Focus on:
1. Country efficiency — which countries have best/worst ROAS and CPA?
2. Spend allocation — is spend proportional to return? Where are the mismatches?
3. New customer acquisition by country — where are new customers cheapest?
4. Untapped markets — countries with Shopify orders but zero Meta spend
5. Concentration risk — is spend too concentrated in one country?
6. Geo-specific campaign performance — any campaigns underperforming in specific countries?
7. Expansion opportunities — data-backed recommendation for which country to expand into`,
};

export async function generateInsights(shopDomain, pageKey, pageData, dateFrom, dateTo, currencySymbol, promptOverrides = null) {
  const builder = PROMPT_BUILDERS[pageKey];
  if (!builder) throw new Error(`Unknown page key: ${pageKey}`);

  let { system, user } = builder(pageData, currencySymbol);

  // Apply prompt overrides if provided
  if (promptOverrides) {
    if (promptOverrides.system) system = promptOverrides.system;
    if (promptOverrides.page) {
      // Replace the "Focus on:" section in the user prompt with the custom one
      const dataIdx = user.indexOf("\nDATA:");
      if (dataIdx > -1) {
        const header = user.split("\n")[0]; // "Analyse this X data..."
        user = `${header}\n\n${promptOverrides.page}\n${user.slice(dataIdx)}`;
      }
    }
  }

  const dataHash = computeDataHash(pageData);

  // Check cache — skip if data hasn't changed and insights are < 24h old
  // Always skip cache when custom prompts are provided (dev/testing mode)
  if (!promptOverrides) {
    const cached = await getCachedInsights(shopDomain, pageKey, dateFrom, dateTo);
    if (cached && cached.dataHash === dataHash) {
      const ageHours = (Date.now() - new Date(cached.generatedAt).getTime()) / 3600000;
      if (ageHours < 24) {
        return { insights: cached.insights, fromCache: true, tokenCost: 0 };
      }
    }
  }

  const client = getClient();

  const response = await client.messages.create({
    model: MODEL,
    max_tokens: MAX_TOKENS,
    system,
    messages: [{ role: "user", content: user }],
  });

  const text = response.content[0]?.type === "text" ? response.content[0].text : "";
  const tokenCost = (response.usage?.input_tokens || 0) + (response.usage?.output_tokens || 0);

  // Parse JSON response — handle code fences if Claude adds them
  let insights;
  try {
    const cleaned = text.replace(/^```json?\s*/, "").replace(/\s*```$/, "").trim();
    insights = JSON.parse(cleaned);
  } catch (e) {
    console.error("[AI] Failed to parse response:", text.slice(0, 500));
    throw new Error("AI returned invalid JSON");
  }

  // Validate structure
  if (!insights.observations || !insights.actions) {
    throw new Error("AI response missing observations or actions");
  }

  // Sort by priority
  insights.observations.sort((a, b) => (b.priority || 0) - (a.priority || 0));

  // Save to cache
  await saveInsights(shopDomain, pageKey, dateFrom, dateTo, dataHash, insights, tokenCost);

  return { insights, fromCache: false, tokenCost };
}
