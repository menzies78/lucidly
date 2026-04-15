# Lucidly — Project Brief for Claude Code

## What is Lucidly?
A Shopify embedded app providing Meta Ads attribution and customer analytics. Built for distribution via the Shopify App Store. Primary test merchant: Hayley Menzies (fashion brand).

## Tech Stack
- Remix + Prisma + SQLite (dev) + Node.js
- Shopify Polaris UI
- Deployed embedded in Shopify admin

## Codebase Structure
- app/services/ — core server-side services
  - orderSync.server.js — Shopify order import via GraphQL (all channels, isOnlineStore flag for web-only matching)
  - metaSync.server.js — Meta Insights 7-day sync
  - incrementalSync.server.js — hourly Meta sync + incremental attribution
  - matcher.server.js — batch attribution matcher (exhaustive backtracking primary, FAST greedy fallback)
- app/routes/ — Remix routes
  - app._index.tsx — dashboard
  - app.campaigns.tsx — Campaign Performance
  - app.orders.tsx — Order Explorer
  - app.ltv.tsx — Customer LTV
  - app.meta-connect.tsx — Meta OAuth
- (scripts/ folder removed — all dev-only harness deleted 2026-04-11. Any one-off
  backfills now live as temporary actions inside app/services/ and are triggered
  via the authenticated admin UI, not standalone scripts.)

## Credentials
All secrets are managed via environment variables — never hardcoded.
- Local dev: `.env` file (gitignored)
- Production: Fly.io secrets (`fly secrets list -a lucidly`)
- Required env vars: `META_APP_ID`, `META_APP_SECRET`, `ANTHROPIC_API_KEY`, `SHOPIFY_API_KEY`, `SHOPIFY_API_SECRET`, `SHOPIFY_APP_URL`
- Per-shop Shopify tokens are stored in the `Session` table via the authenticated OAuth flow
- Per-shop Meta tokens are stored in `Shop.metaAccessToken` after the OAuth callback

## Attribution Rules
- ONLY isOnlineStore=true orders matched against Meta conversions
- Exhaustive backtracking primary, FAST greedy strictly last resort (5% quality gate)
- Incremental matcher: hourly snapshot diff
- Dedup of Oct 18 2025 - Jan 30 2026 was ONE-TIME for HM pixel bug — never bake into production
- fetchWithRetry() wraps all external API calls (catches JSON parse errors too)
- No end-of-day reconciliation needed. Incremental sync self-heals — each cycle picks up the full delta since the last successful run, so missed cycles are automatically recovered on the next run.

## Attribution Layers & Confidence
- Layer 1: Cookie/UTM based (future — Web Pixel). 100% confidence. Takes priority.
- Layer 2: Statistical matcher (current). Variable confidence %.
- Confidence = `100 / (1 + rival_count)` — where rivals are unpicked candidates with compatible time slots AND value within ±2% of the picked order.
- Examples: 1 candidate = 100%, 2 equal candidates = 50%, 3 = 33%
- `rivalCount` stored on Attribution for transparency
- Confidence field is Int (0-100). 0 = unmatched/NONE.
- Layer 2 matcher skips orders already attributed by Layer 1.

## Customer Tags
Meta New, Meta Repeat, Meta Retargeted, Unattributed

## Current Data State (Mar 8 2026)
- 10,235 Shopify orders (Feb 2025 - Mar 2026)
- 287,370+ Meta insight rows (Feb 5 2025 - Mar 2026)
- 3,174 attributions (full re-match done, confidence now percentage-based)
- Confidence migrated from HIGH/MEDIUM/LOW/NONE to Int 0-100 (migration: confidence_percentage)
- Need to re-run Full Re-match to get proper rival-based confidence scores (currently legacy values: 85/50/0)

## Andy's Preferences
- Direct, no fluff
- Complete file rewrites over partial edits
- Simple surface, actionable insights
- Pull ALL data now, surface it intelligently later
- "Under the hood" section for deep analytics
- Step by step — detail one step at a time

## No Hardcoded Credentials
The scripts/ folder has been removed (was dev-only test harness). All production code uses the authenticated service layer in app/services/. Per-shop Shopify tokens come from the OAuth Session, Meta tokens from Shop.metaAccessToken, Anthropic/Meta app credentials from environment variables.

## Reporting Philosophy
Meta reports a total conversion value that will never fully match Shopify orders. Reasons include: edited orders, partial refunds, currency differences, multi-item orders. This gap is universal across merchants and must be surfaced honestly in the UI.

On Campaign Performance, show three revenue figures:
- Matched Revenue: orders verified via attribution matching (current behaviour)
- Unverified Revenue: Meta-reported conversion value minus matched revenue (gap amount)
- Blended ROAS: (matched + unverified revenue) / spend

Unmatched attribution records (confidence: NONE) already store the Meta conversion value — the data exists, just needs surfacing.

This ensures the app never undercounts Meta's contribution while being honest about what can and cannot be verified at order level.

The gap between Meta-reported conversions and our matched orders is primarily caused by Shopify order values changing after placement (edits, partial refunds, currency adjustments, shipping changes). Meta captured the original conversion value at time of purchase, but by the time we match, the Shopify order total may have shifted outside our matching tolerance. This is a Shopify data issue, not Meta reporting inaccuracies.

## Onboarding — Business Audit (Not "Setup")
The install flow is NOT a technical setup. It's an audit of the merchant's business. Lucidly imports historical Shopify + Meta data, then analyses it to establish baselines and benchmarks. All future performance is measured against these benchmarks.

### Onboarding Sequence (shown to merchant)
1. **Analysing data** — importing orders, Meta campaigns, customer history
2. **Understanding your customers** — segmenting Meta New / Repeat / Retargeted / Organic, computing AOV, LTV, repeat rates per segment
3. **Building your benchmarks** — establishing baseline metrics that future performance is measured against

### Customer Acquisition Health Score
A single composite score (0–100) summarising the health of the merchant's Meta-driven customer acquisition. Displayed prominently on the dashboard.

**Example:**
```
Customer Acquisition Health: 68 / 100
Based on:
  • % new customers (from Meta)
  • CAC efficiency (cost to acquire vs LTV)
  • Repeat rate (do Meta-acquired customers come back?)
  • LTV (lifetime value of Meta-acquired vs organic customers)
```

Each component contributes to the score. The benchmark is personalised — based on the merchant's own historical data, sector, AOV band. The score trends over time so merchants can see if acquisition health is improving or degrading week-over-week.

## Next Steps
1. ~~Prisma migration: add_order_enrichment_fields~~ DONE
2. ~~Decide additional Meta fields to pull~~ DONE — Phase 1 (reach, frequency, cpc, cpm, funnel actions, video metrics) + Phase 2 breakdowns (country, platform, placement, age, gender)
3. ~~Re-run Shopify backfill with enriched fields~~ DONE — backfilled via script. POS orders (3,191) have no address by design. customerOrderCountAtPurchase computed from order history.
4. ~~Full Re-match~~ DONE — 3,060 matched, 113 unmatched. 96.4% HIGH confidence. 45% match rate on online orders.
5. **LTV → Meta Acquisition Intelligence (HIGH PRIORITY)** — replace current LTV page. True lifetime value of Meta-acquired customers: at-a-glance stats (Meta New/Repeat/Retargeted), CAC vs LTV, cohort analysis by acquisition month, payback period, repeat rate + intervals, Meta vs non-Meta customer comparison. Goal: "what should I spend to acquire a customer?"
6a. **Global date range selector** — sticky across all pages, compare periods
6b. **Order Explorer** — sortable columns, filters
7. **Automated sync**
   - ~~7a. Shopify webhooks (orders/create, orders/updated)~~ DONE — registered in shopify.app.toml, handlers in webhooks.orders.create.tsx / webhooks.orders.updated.tsx, processing in orderWebhook.server.js. Creates order with frozenTotalPrice on create, updates mutable fields (refunds, discounts, financial status) on update while preserving frozen price. Computes customerOrderCountAtPurchase per customer.
   - ~~7b. In-process scheduler~~ DONE — hourly incremental sync (Meta insights + matching + breakdowns) for all connected shops, daily 3am 7-day lookback sync. Global singleton guard prevents duplicate intervals in dev. Starts on server boot via shopify.server.ts.
   - ~~7c. On-install hook~~ DONE — auto-triggers historical order backfill on first dashboard load for new merchants (detects no lastOrderSync + 0 orders). Welcome banner shown during import.
   - 7d. **FUTURE IMPROVEMENT: Real-time matching uplift** — With webhooks capturing original order values at creation time (before Shopify edits), AND Layer 1 cookie/UTM providing instant attribution, we can eliminate the "changed order value" matching gap. Today's unmatched attributions are largely caused by order totals shifting post-purchase (refunds, edits, shipping changes) before the matcher runs. Real-time ingestion + immediate matching against the original value should push match rates significantly higher. Revisit after Layer 1 (Web Pixel) is live.
8. ~~Currency conversion~~ DONE — Frankfurter API (ECB, free, no key). Rate cached in DB per date. Converts Meta spend/conversionValue/cpc/cpm to Shopify currency at sync time. Skips API call when currencies match (rate=1.0). Applied in all three sync paths (incremental, 7-day, breakdowns).
9. ~~Timezone handling~~ DONE — Meta hour slots converted from Meta ad account timezone to UTC before matching against Shopify order times. Offset computed per-day via Intl.DateTimeFormat (handles DST). Backward-only padding (6 min) since Meta pixel fires after order placement.
10. AI recommendations layer
11. Web Pixel Layer 1
12. Cloud deployment
13. **Ad Spend Waste Detector** — headline feature, high viral potential. "You wasted £X on ads last week." Breakdown: spend on existing customers (retargeting waste), fatigued audiences (frequency decay), low-converting creative. Concrete, financial, urgent — strong emotional trigger. Works as landing page hook: "See how much ad spend you're wasting each week." Should be one of the first things a new merchant sees.
14. **Product Acquisition Intelligence** — which products acquire new customers, which convert retargeting, which drive repeat purchase. Cross-references attribution data with order line items. Answers "what should I be advertising?"
15. **Recommendation Engine & Decision Feedback Loop** — every recommendation stored as structured data:
    - Schema: recommendationId, storeId, date, type (scale/pause/test/reduce), campaignId, confidenceScore, metricsUsed, recommendedAction
    - Track: userExecutedAction, executionDate, outcomeMetrics (7/14/30 days)
    - Creates a decision feedback loop. Over thousands of stores, learns patterns like "campaigns with this structure fatigue after ~17 days" or "products with AOV >£120 convert poorly in cold audiences without video creative"
    - Becomes a collective intelligence layer — the moat.
16. **View vs Click attribution split** — separate 1d_view and 7d_click attributions. Surface which conversions are view-through vs click-through. Requires pulling attribution window breakdown from Meta API.
17. **Impression time vs order time logging** — track when the ad was last seen (impression/view) relative to when the order was placed. Enables view-through conversion analysis and time-to-convert insights.
18. **UTM management** — pull UTM parameters from Shopify orders, detect missing/mismatched UTMs across Meta campaigns, suggest correct UTM structures, optionally push UTM updates back to Meta. Foundation for Layer 1 cookie/UTM attribution.
19. **Breakdown report pages** — dedicated "say what you see" pages for Country, Platform, Placement, Age, Gender breakdowns. Simple, clear summaries: spend vs revenue vs CPA per breakdown value. No complexity, just the data.
20. **Product Purchases report** — which products are being purchased via Meta ads, spend per product, cost per acquisition per product. Cross-references line items with attribution + Meta spend data.
21. **Weekly Report page** — automated weekly summary: key metrics, week-over-week changes, top/bottom performing campaigns, notable trends. Designed to be glanceable — "what happened this week?"
22. **Column reordering** — allow drag-and-drop reordering of columns in InteractiveTable. Revised order saved to localStorage per tableId, persists across sessions.
23. **Customisable summary tiles** — make the summary tile grids drag-and-drop reorderable and selectable from a longer library of headline stats / micro reports. Each merchant can configure which tiles appear on each page's dashboard. Layout saved per page per merchant.
