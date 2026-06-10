import type { HeadersFunction, LoaderFunctionArgs } from "@remix-run/node";
import { redirect } from "@remix-run/node";
import { Link, Outlet, useLoaderData, useRouteError, useNavigation } from "@remix-run/react";
import { boundary } from "@shopify/shopify-app-remix/server";
import { AppProvider } from "@shopify/shopify-app-remix/react";
import { NavMenu } from "@shopify/app-bridge-react";
import polarisStyles from "@shopify/polaris/build/esm/styles.css?url";
import DateRangeSelector from "../components/DateRangeSelector";
import { useState, useEffect } from "react";

import { authenticate } from "../shopify.server";
import { ensureWebhooks } from "../services/ensureWebhooks.server.js";
import db from "../db.server";
import { isInternalShop } from "../utils/access.server";

export const links = () => [{ rel: "stylesheet", href: polarisStyles }];

// Sub-paths under /app that remain reachable while onboarding is in progress.
// Everything else is redirected to /app so the merchant stays focused on the
// onboarding flow until the data is actually there to look at.
//   - /app                  : the onboarding card itself
//   - /app/api/*            : status polling, progress, etc.
const ONBOARDING_ALLOWED = (pathname: string) =>
  pathname === "/app" || pathname === "/app/" || pathname.startsWith("/app/api/");

export const loader = async ({ request }: LoaderFunctionArgs) => {
  const { session } = await authenticate.admin(request);
  // Self-heal: register required webhooks if missing. Cached per-process, idempotent.
  ensureWebhooks(session.shop, session.accessToken!).catch(err =>
    console.error("[app loader] ensureWebhooks failed:", err)
  );

  const shop = await db.shop.findUnique({
    where: { shopDomain: session.shop },
    select: { onboardingCompleted: true },
  });
  const onboardingCompleted = shop?.onboardingCompleted ?? false;
  const isInternal = isInternalShop(session.shop);

  // Server-side gate: if onboarding isn't done, every nav target except the
  // dashboard itself + the polling APIs gets bounced back to /app. Internal
  // shops bypass the gate so we can still debug merchants mid-onboarding.
  const url = new URL(request.url);
  if (!onboardingCompleted && !isInternal && !ONBOARDING_ALLOWED(url.pathname)) {
    throw redirect("/app");
  }

  return { apiKey: process.env.SHOPIFY_API_KEY || "", onboardingCompleted, isInternal };
};

function LoadingIndicator() {
  const navigation = useNavigation();
  const isLoading = navigation.state === "loading";
  const [showLoadingPill, setShowLoadingPill] = useState(false);
  // Sync-status polling - lets the pill say "Hourly sync running…" instead
  // of leaving the merchant wondering why a tab click is sluggish. Polled
  // every 8s while the tab is visible; cheap (single int comparison + JSON
  // response) and pauses when the tab is hidden to avoid wasted load.
  const [syncRunning, setSyncRunning] = useState(false);

  useEffect(() => {
    if (isLoading) {
      const timer = setTimeout(() => setShowLoadingPill(true), 1500);
      return () => clearTimeout(timer);
    }
    setShowLoadingPill(false);
  }, [isLoading]);

  useEffect(() => {
    let cancelled = false;
    let timer: ReturnType<typeof setTimeout> | null = null;
    const poll = async () => {
      try {
        const res = await fetch("/app/api/sync-status", { credentials: "include" });
        if (!cancelled && res.ok) {
          const data = await res.json();
          setSyncRunning(!!data.running);
        }
      } catch {
        // Network blips are not interesting here; just keep polling.
      } finally {
        if (!cancelled) {
          // Slow down when the tab is hidden - no point spamming the server
          // when nobody is looking at the pill.
          const delay = document.visibilityState === "hidden" ? 60_000 : 8_000;
          timer = setTimeout(poll, delay);
        }
      }
    };
    poll();
    return () => {
      cancelled = true;
      if (timer) clearTimeout(timer);
    };
  }, []);

  // Only surface the pill while a navigation is actually pending. If a
  // background sync happens to be running at the same time, upgrade the
  // copy so the merchant knows why the load is sluggish - but never show
  // the pill purely because sync is running in the background.
  const showPill = showLoadingPill;
  const pillText = syncRunning
    ? "Hourly sync running - your data may take a moment"
    : "Loading your data...";

  return (
    <>
      <style>{`
        @keyframes lucidly-shimmer {
          0% { background-position: -200% 0; }
          100% { background-position: 200% 0; }
        }
        @keyframes lucidly-fade-in {
          from { opacity: 0; transform: translateY(-4px); }
          to { opacity: 1; transform: translateY(0); }
        }
        @keyframes lucidly-pulse {
          0%, 100% { opacity: 0.7; }
          50% { opacity: 1; }
        }
      `}</style>

      {/* Slim shimmer bar - only while a navigation is pending */}
      {isLoading && (
        <div style={{
          position: "fixed", top: 0, left: 0, right: 0, height: "3px", zIndex: 99999,
          background: "linear-gradient(90deg, transparent, #7c3aed, #a78bfa, #7c3aed, transparent)",
          backgroundSize: "200% 100%",
          animation: "lucidly-shimmer 1.5s ease-in-out infinite",
        }} />
      )}

      {/* Friendly label - fades in after 1.5s for nav, immediately for sync */}
      {showPill && (
        <div style={{
          position: "fixed", top: "8px", left: 0, right: 0,
          display: "flex", justifyContent: "center",
          zIndex: 99999, pointerEvents: "none",
        }}>
          <div style={{
            background: "rgba(124, 58, 237, 0.95)", color: "#fff",
            padding: "6px 20px", borderRadius: "20px",
            fontSize: "12px", fontWeight: 600, letterSpacing: "0.3px",
            boxShadow: "0 2px 12px rgba(124, 58, 237, 0.3)",
            animation: "lucidly-fade-in 0.3s ease-out, lucidly-pulse 2s ease-in-out infinite",
          }}>
            {pillText}
          </div>
        </div>
      )}
    </>
  );
}

export default function App() {
  const { apiKey, onboardingCompleted, isInternal } = useLoaderData<typeof loader>();
  // While onboarding is in progress (and the merchant isn't an internal user),
  // collapse the nav to just the home/Health link and hide the date selector.
  // Stops the merchant from clicking through to pages that have no data yet.
  const showFullNav = onboardingCompleted || isInternal;

  return (
    <AppProvider isEmbeddedApp apiKey={apiKey}>
      {/* Global: add breathing room beneath tile/chart titles.
          Targets any <Text as="h2"> used as a tile or section heading.
          Pairs with the sitewide bump from headingMd → headingLg. */}
      <style>{`
        h2.Polaris-Text--root { margin-bottom: 6px; }
      `}</style>
      <LoadingIndicator />
      <NavMenu>
        {/* Shopify's NavMenu requires `rel="home"` on the first Link - it
            pins that tab to the app-title slot at the left of the embedded
            nav. The remaining Links render in declared order after it.
            This means the "home" tab is always visually first and cannot
            be moved to the end without breaking embedding. Rename only. */}
        <Link to="/app" rel="home">Health</Link>
        {showFullNav && <Link to="/app/customers">Customers</Link>}
        {showFullNav && <Link to="/app/products">Products</Link>}
        {showFullNav && <Link to="/app/campaigns">Ad Campaigns</Link>}
        {showFullNav && <Link to="/app/geo">Countries</Link>}
        {showFullNav && <Link to="/app/weekly">Weekly Report</Link>}
        {showFullNav && <Link to="/app/utm">UTM Manager</Link>}
      </NavMenu>
      {showFullNav && <DateRangeSelector />}
      <Outlet />
      {/* Merchant-facing data-processing disclosure. Required for the Shopify
          protected-customer-data review: tells merchants what data the app
          processes and why, and links to the full public privacy policy. */}
      <footer
        style={{
          maxWidth: 998,
          margin: "24px auto 40px",
          padding: "0 16px",
          fontSize: 12,
          lineHeight: 1.5,
          color: "#6b7280",
          textAlign: "center",
        }}
      >
        Lucidly processes your store's order and customer data (including
        customer names and hashed email addresses) to provide advertising
        attribution and customer analytics.{" "}
        <a
          href="/privacy"
          target="_blank"
          rel="noopener noreferrer"
          style={{ color: "#7c3aed", textDecoration: "underline" }}
        >
          Privacy Policy
        </a>
      </footer>
    </AppProvider>
  );
}

// Shopify needs Remix to catch some thrown responses, so that their headers are included in the response.
export function ErrorBoundary() {
  return boundary.error(useRouteError());
}

export const headers: HeadersFunction = (headersArgs) => {
  return boundary.headers(headersArgs);
};
