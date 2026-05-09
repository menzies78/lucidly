import { useLocation, useNavigate, useSearchParams } from "@remix-run/react";
import type { ReactNode } from "react";
import { PAGE_TABS, PageThemeContext, getThemeForPath } from "./PageTheme";

export default function ReportTabs({ children }: { children?: ReactNode }) {
  const location = useLocation();
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const theme = getThemeForPath(location.pathname);
  const dateQuery = () => {
    const params = new URLSearchParams();
    for (const key of ["from", "to", "preset", "compare"]) {
      const val = searchParams.get(key);
      if (val) params.set(key, val);
    }
    const str = params.toString();
    return str ? `?${str}` : "";
  };

  const isActive = (tab: typeof PAGE_TABS[number]) =>
    tab.path === "/app"
      ? location.pathname === "/app" || location.pathname === "/app/"
      : location.pathname.startsWith(tab.path);

  return (
    <PageThemeContext.Provider value={theme}>
      <style>{`
        .lucidly-themed-content .Polaris-Card,
        .lucidly-themed-content .Polaris-LegacyCard,
        .lucidly-themed-content .Polaris-ShadowBevel {
          box-shadow: 0 0 0 1px var(--l-border), var(--l-shadow-sm) !important;
        }
      `}</style>
      <div>
        <div style={{ display: "flex", alignItems: "flex-end", flexWrap: "wrap" }}>
          {PAGE_TABS.map(tab => {
            const active = isActive(tab);
            return (
              <button
                key={tab.path}
                onClick={() => navigate(`${tab.path}${dateQuery()}`)}
                style={{
                  padding: "13px 16px",
                  fontSize: "var(--l-font-base)",
                  minWidth: ["Customers", "Products", "Ad Campaigns", "Countries"].includes(tab.label) ? "130px" : undefined,
                  fontWeight: active ? 700 : 500,
                  color: active ? "var(--l-accent-dark)" : "var(--l-text-secondary)",
                  // Selected tab: white (matches content bg), flows seamlessly
                  // into the page below. Unselected: subtle grey fill.
                  background: active ? "var(--l-bg)" : "var(--l-bg-subtle)",
                  borderTop: `1px solid var(--l-border)`,
                  borderLeft: `1px solid var(--l-border)`,
                  borderRight: `1px solid var(--l-border)`,
                  // Selected tab's bottom edge is the page bg colour so it
                  // visually merges with the content area below (uninterrupted
                  // white-on-white). Unselected tabs keep a grey baseline.
                  borderBottom: active ? "1px solid var(--l-bg)" : "1px solid var(--l-border)",
                  borderRadius: 0,
                  cursor: "pointer",
                  marginRight: "-1px",
                  marginBottom: "-1px",
                  position: "relative" as const,
                  zIndex: active ? 2 : 0,
                  transition: "all 0.15s ease",
                  letterSpacing: "-0.01em",
                  whiteSpace: "nowrap",
                }}
                onMouseEnter={e => {
                  if (!active) {
                    e.currentTarget.style.color = "var(--l-accent-dark)";
                    e.currentTarget.style.background = "var(--l-accent-light)";
                  }
                }}
                onMouseLeave={e => {
                  if (!active) {
                    e.currentTarget.style.color = "var(--l-text-secondary)";
                    e.currentTarget.style.background = "var(--l-bg-subtle)";
                  }
                }}
              >
                {tab.label}
              </button>
            );
          })}
          <div style={{ flex: 1, borderBottom: "1px solid var(--l-border)" }} />
        </div>
        {children && (
          <div
            className="lucidly-themed-content"
            style={{
              border: "1px solid var(--l-border)",
              borderTop: "none",
              borderRadius: "0 0 var(--l-radius-md) var(--l-radius-md)",
              background: "var(--l-bg)",
              padding: "var(--l-space-5)",
              position: "relative" as const,
              zIndex: 1,
            }}
          >
            {children}
          </div>
        )}
      </div>
    </PageThemeContext.Provider>
  );
}
