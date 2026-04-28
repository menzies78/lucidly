import { createContext, useContext } from "react";

export interface PageTheme {
  accent: string;      // primary accent colour (e.g. tab active border, header highlights)
  accentLight: string; // light tint for backgrounds (table header, active tab bg)
  accentDark: string;  // darker shade for text on light backgrounds
}

const DEFAULT_THEME: PageTheme = { accent: "#5C6AC4", accentLight: "#F4F5FF", accentDark: "#3F4BAF" };

export const PageThemeContext = createContext<PageTheme>(DEFAULT_THEME);

export function usePageTheme() {
  return useContext(PageThemeContext);
}

// Tab definitions with colours
export const PAGE_TABS = [
  { label: "Health",          path: "/app",           accent: "#5C6AC4", accentLight: "#F0F1FF", accentDark: "#4650A8" },
  { label: "Customers",      path: "/app/customers",  accent: "#0E7490", accentLight: "#ECFEFF", accentDark: "#0B5E73" },
  { label: "Products",       path: "/app/products",   accent: "#7C3AED", accentLight: "#F5F3FF", accentDark: "#6429C9" },
  { label: "Ad Campaigns",   path: "/app/campaigns",  accent: "#2563EB", accentLight: "#EFF6FF", accentDark: "#1D4ED8" },
  { label: "Countries",      path: "/app/geo",        accent: "#059669", accentLight: "#ECFDF5", accentDark: "#047857" },
  { label: "Waste Detector",  path: "/app/waste",     accent: "#DC2626", accentLight: "#FEF2F2", accentDark: "#B91C1C" },
  { label: "Weekly Report",  path: "/app/weekly",     accent: "#D97706", accentLight: "#FFFBEB", accentDark: "#B45309" },
  { label: "Order Explorer", path: "/app/orders",     accent: "#6366F1", accentLight: "#EEF2FF", accentDark: "#4F46E5" },
  { label: "UTM Manager",    path: "/app/utm",        accent: "#9333EA", accentLight: "#FAF5FF", accentDark: "#7E22CE" },
  { label: "Change Log",     path: "/app/changes",    accent: "#334155", accentLight: "#F1F5F9", accentDark: "#1E293B" },
] as const;

export function getThemeForPath(pathname: string): PageTheme {
  // Exact match for dashboard
  if (pathname === "/app" || pathname === "/app/") {
    const t = PAGE_TABS[0];
    return { accent: t.accent, accentLight: t.accentLight, accentDark: t.accentDark };
  }
  // Prefix match for other pages
  const tab = PAGE_TABS.find(t => t.path !== "/app" && pathname.startsWith(t.path));
  if (tab) return { accent: tab.accent, accentLight: tab.accentLight, accentDark: tab.accentDark };
  return DEFAULT_THEME;
}
