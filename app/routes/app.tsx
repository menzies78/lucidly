import type { HeadersFunction, LoaderFunctionArgs } from "@remix-run/node";
import { Link, Outlet, useLoaderData, useRouteError, useNavigation } from "@remix-run/react";
import { boundary } from "@shopify/shopify-app-remix/server";
import { AppProvider } from "@shopify/shopify-app-remix/react";
import { NavMenu } from "@shopify/app-bridge-react";
import polarisStyles from "@shopify/polaris/build/esm/styles.css?url";
import DateRangeSelector from "../components/DateRangeSelector";
import { useState, useEffect } from "react";

import { authenticate } from "../shopify.server";
import { ensureWebhooks } from "../services/ensureWebhooks.server.js";

export const links = () => [{ rel: "stylesheet", href: polarisStyles }];

export const loader = async ({ request }: LoaderFunctionArgs) => {
  const { session } = await authenticate.admin(request);
  // Self-heal: register required webhooks if missing. Cached per-process, idempotent.
  ensureWebhooks(session.shop, session.accessToken!).catch(err =>
    console.error("[app loader] ensureWebhooks failed:", err)
  );
  return { apiKey: process.env.SHOPIFY_API_KEY || "" };
};

function LoadingIndicator() {
  const navigation = useNavigation();
  const isLoading = navigation.state === "loading";
  const [showLoadingPill, setShowLoadingPill] = useState(false);
  // Sync-status polling — lets the pill say "Hourly sync running…" instead
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
          // Slow down when the tab is hidden — no point spamming the server
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

  // The two pill triggers can overlap. Sync is the more informative
  // message, so it wins when both are true.
  const showPill = syncRunning || showLoadingPill;
  const pillText = syncRunning
    ? "Hourly sync running — your data may take a moment"
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

      {/* Slim shimmer bar — visible during nav loads or while sync runs */}
      {(isLoading || syncRunning) && (
        <div style={{
          position: "fixed", top: 0, left: 0, right: 0, height: "3px", zIndex: 99999,
          background: "linear-gradient(90deg, transparent, #7c3aed, #a78bfa, #7c3aed, transparent)",
          backgroundSize: "200% 100%",
          animation: "lucidly-shimmer 1.5s ease-in-out infinite",
        }} />
      )}

      {/* Friendly label — fades in after 1.5s for nav, immediately for sync */}
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
  const { apiKey } = useLoaderData<typeof loader>();

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
        {/* Shopify's NavMenu requires `rel="home"` on the first Link — it
            pins that tab to the app-title slot at the left of the embedded
            nav. The remaining Links render in declared order after it.
            This means the "home" tab is always visually first and cannot
            be moved to the end without breaking embedding. Rename only. */}
        <Link to="/app" rel="home">Health</Link>
        <Link to="/app/customers">Customers</Link>
        <Link to="/app/products">Products</Link>
        <Link to="/app/campaigns">Ad Campaigns</Link>
        <Link to="/app/geo">Countries</Link>
        <Link to="/app/waste">Waste Detector</Link>
        <Link to="/app/weekly">Weekly Report</Link>
        <Link to="/app/orders">Order Explorer</Link>
        <Link to="/app/utm">UTM Manager</Link>
      </NavMenu>
      <DateRangeSelector />
      <Outlet />
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
