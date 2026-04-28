import { useLocation, useNavigate, useSearchParams } from "@remix-run/react";
import type { ReactNode } from "react";
import { PAGE_TABS, PageThemeContext, getThemeForPath } from "./PageTheme";

const CONTENT_BG = "#fff";

export default function ReportTabs({ children }: { children?: ReactNode }) {
  const location = useLocation();
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const theme = getThemeForPath(location.pathname);

  // Semi-transparent accent for borders (40% opacity)
  const borderAccent = `${theme.accent}66`;

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
          box-shadow: 0 0 0 1px ${borderAccent}, 0 1px 2px rgba(0,0,0,0.05) !important;
        }
      `}</style>
      <div>
        <div style={{ display: "flex", alignItems: "flex-end", flexWrap: "wrap" }}>
          {PAGE_TABS.map(tab => {
            const active = isActive(tab);
            const tabBorder = active ? `${tab.accent}66` : "#c9cccf";
            return (
              <button
                key={tab.path}
                onClick={() => navigate(`${tab.path}${dateQuery()}`)}
                style={{
                  padding: "9px 16px",
                  fontSize: "13px",
                  minWidth: ["Customers", "Products", "Ad Campaigns", "Countries"].includes(tab.label) ? "130px" : undefined,
                  fontWeight: active ? 700 : 500,
                  color: active ? tab.accentDark : "#6d7175",
                  background: active ? tab.accentLight : "#f6f6f7",
                  border: `1px solid ${tabBorder}`,
                  borderBottom: active ? `2px solid ${tab.accent}` : `1px solid ${tabBorder}`,
                  borderRadius: "8px 8px 0 0",
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
                    e.currentTarget.style.color = tab.accentDark;
                    e.currentTarget.style.background = tab.accentLight;
                    e.currentTarget.style.borderColor = `${tab.accent}44`;
                  }
                }}
                onMouseLeave={e => {
                  if (!active) {
                    e.currentTarget.style.color = "#6d7175";
                    e.currentTarget.style.background = "#f6f6f7";
                    e.currentTarget.style.borderColor = "#c9cccf";
                  }
                }}
              >
                {tab.label}
              </button>
            );
          })}
          <div style={{ flex: 1, borderBottom: `1px solid ${borderAccent}` }} />
        </div>
        {children && (
          <div
            className="lucidly-themed-content"
            style={{
              border: `1px solid ${borderAccent}`,
              borderTop: "none",
              borderRadius: "0 0 8px 8px",
              background: CONTENT_BG,
              padding: "20px",
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
