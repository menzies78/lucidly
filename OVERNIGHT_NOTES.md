# Overnight fixes — 2026-04-20 (morning brief)

Shipped to Vollebak as `deploy-2026-04-20-4`. Machine `683939df511e18`
reached good state on each deploy. DNS verified.

## What changed (8 commits, in order)

1. **`3d5e4f9` Progress polling: 5-min TTL, drop StrictMode race**
   - `app/services/progress.server.js`, `app/routes/app.api.progress.tsx`
   - Terminal progress entries now linger for 5 min instead of being
     cleared on first loader read, so React StrictMode's double-render
     no longer loses "done" / "error" state mid-flight.

2. **`f48ee54` Campaigns: fix dead comparison-period guard + net-of-refunds store revenue**
   - `app/routes/app.campaigns.tsx`
   - The `compInsightsResult = null` line made the comparison-period
     block unreachable — replaced with the live `compareAgg.campaign`
     path. `totalStoreRevenue` now nets refunds at both computation sites.

3. **`35bdd5a` Weekly Report: stop clobbering new-customer ads in Newly Launched**
   - `app/routes/app.weekly.tsx`
   - Merging `{...adsNew, ...adsExisting}` was silently overwriting
     ads that appeared in both buckets. Replaced with a sum-merge so
     combined orders + revenue are preserved.

4. **`46ab3cf` Changes: surface Resumed events as a tile**
   - `app/routes/app.changes.tsx`
   - The `resumed` category was already aggregated but never rendered.
     Added the summary tile between "Paused / killed" and "Budget changes".

5. **`ed31a24` Products AI insights: parse comma-separated lineItems, net of refunds**
   - `app/routes/app.products.tsx`
   - `JSON.parse(o.lineItems)` was always throwing because
     `orderWebhook.server.js:90` stores titles as comma-separated text.
     Switched to `split(", ")` and divided `(frozen - refunded)` by
     titles.length for per-item revenue. £0 orders skipped. Per-item
     price precision still deferred — will need a schema migration to
     store lineItem unit prices.

6. **`daf02fd` Revenue net-of-refunds in Geo page + campaign rollups**
   - `app/routes/app.geo.tsx`, `app/services/campaignRollups.server.js`
   - Both sites were summing `frozenTotalPrice` only. Added
     `Math.max(0, gross - totalRefunded)` everywhere revenue is attributed
     (matched path + UTM-only path, at both Geo loader and rollup writer).
   - Rollup change takes effect on the next `incrementalSync` cycle that
     rebuilds `DailyAdRollup` — no manual backfill needed.

7. **`d34f630` Add isOnlineStore filter to Meta-specific order queries**
   - `app/routes/app._index.tsx`, `app/routes/app.weekly.tsx`, `app/routes/app.campaigns.tsx`
   - The Meta pipeline only runs on `isOnlineStore=true` orders, but a
     few loader queries feeding Meta tiles (utmConfirmedMeta / matched
     Meta revenue on the dashboard, windowOrders on Campaigns, both
     weekly orders queries) weren't filtering. Any POS sale with a
     stray utm tag or lingering metaAdId could have leaked into Meta
     revenue. Filter added. Whole-store queries (Total Orders, Net
     Revenue, customer count) were left alone by design.

8. **`4c3f229` isOnlineStore filter on remaining Meta rollup writers**
   - `app/services/campaignRollups.server.js`, `app/services/utmLinkage.server.js`
   - Four audit subagents reported back after commit #7 landed. Two
     additional server-side writers had missed the same filter:
     the `DailyAdRollup` builder and the UTM→Meta entity linker.
     Filter added; takes effect on next incremental sync cycle.
   - The same audit agents also flagged `app.campaigns.tsx` lines
     107/110/113/178/183/186 (dead `aggregateInsights` function) and
     `app.geo.tsx` entity-level lines — the first is dead code, the
     second was already fixed at the `rev` variable's origin in
     commit #6 and cascades through entity loops.

## What I deliberately did NOT touch

- **Bug #22 app.additional.tsx "unauthenticated"** — false positive.
  Parent `app.tsx` loader already calls `authenticate.admin(request)`.
  Adding redundant auth would be noise.

- **Dead `aggregateInsights()` in `app.campaigns.tsx:30`** — the
  "refunds bug" lines the auditor flagged (107, 110, 113, 178, 183, 186)
  are all inside a function that is defined but never called. Real fix
  was in `campaignRollups.server.js` + `geo.tsx`. Left dead code alone
  — removing it is a separate cleanup decision.

- **matcher.server.js changes (Theme 4 writers)** — touching the
  matcher without regression tests is too risky for an overnight push.
  Defer until we have a matcher replay harness.

- **metaSync.server.js UTC-midnight date storage** — looked like a bug
  on first pass, but it's actually the convention: `MetaInsight.date`
  stores UTC-midnight of the shop-local calendar day (valid when Meta
  ad account TZ = shop TZ, which is our current case). Changing it
  would require an index/schema coordination with `shopLocalDayKey`.

- **Bug #8 uniqueNewMetaCustomers tile-vs-chart
  (`app.campaigns.tsx:2960`)** — couldn't reconcile tile vs
  `dailyData.reduce((s, d) => s + d.newCustomerOrders, 0)` without
  risking a double-fix. Defer until you can confirm the intended
  definition.

- **Theme 3 cohort-definition items + `app.waste.tsx` placeholder** —
  product decisions, not bugs.

- **Products per-item revenue precision** — needs line-item schema
  migration (unit price + quantity). Left a note in commit #5.

## Verification

- `npm run build` — clean (server bundle 984 kB, warnings are unchanged
  pre-existing dynamic-import notes).
- `npx tsc --noEmit` — 453 errors, identical to baseline. 0 regressions.
  265 errors in `_index / weekly / campaigns` pre- and post-fix.
- Fly deploy passed health checks and reached good state.

## Suggested first look tomorrow

1. **Campaigns comparison period** — commit #2 unblocked a code path
   that had been dark; pull up a campaign, flip the comparison toggle,
   and sanity-check the delta values render.
2. **Geo / Campaigns revenue** — commit #6 + #7 combined. Revenue
   totals on both pages may now be slightly lower (refunds netted,
   POS excluded). Compare to Shopify's own admin revenue report for
   the period to confirm the new numbers look right.
3. **Weekly Report "Newly Launched" section** — commit #3. Any ad
   that acquired both new and repeat customers in its launch week
   should now show the combined total. Spot-check a recent week.

## Known caveat

There's no clean way to verify the rollup refunds fix (#6) without
waiting for the next incremental sync cycle to repopulate
`DailyAdRollup`. If you want the fresh numbers immediately, trigger
a manual rollup rebuild from the dashboard.

## Queued for next push (not tonight)

Discussed 2026-04-21 after reviewing the deferred Theme 4 finding.
The matcher is actually fine for normal days — its `paddingCutoff`
at `matcher.server.js:443` already compensates for the shop/Meta TZ
offset, so UTC-day bucketing works in practice. One narrow edge
remains, plus a general hygiene sweep:

1. **Matcher DST transition day** — `getTimezoneOffsetMinutes` samples
   at noon UTC, which returns the post-transition offset. On fall-back
   days, orders in the 1-hour ambiguous window (e.g. UTC 23:00-23:53
   on UK fall-back Saturday) get filed to the wrong Meta day.
   Fix: sample offset at both 00:00 and 23:00 UTC of the day, use
   `Math.max`. One line, zero behavior change on non-transition days.

2. **Singular day-key hygiene sweep** — grep remaining
   `toISOString().split("T")[0]` / `.slice(0, 10)` calls on Date
   objects and replace with `shopLocalDayKey(tz, date)` from
   `app/utils/shopTime.server.ts`. Known sites:
   `app.campaigns.tsx:1171`, `app.weekly.tsx:302, 307` — plus a fresh
   grep. Skip the matcher's own day-keys (padding already corrects)
   and metaSync's writer (schema-level decision).

**How the code knows about DST** — IANA tzdata via Node ICU.
`Intl.DateTimeFormat("en-US", { timeZone: "Europe/London" })` knows
every DST rule for every zone past/future. `Shop.shopifyTimezone` and
`Shop.metaAccountTimezone` store IANA strings ("Europe/London", etc.).
No hardcoded rules, no manual schedule. Node/OS patches update the
tzdata when governments change rules.
