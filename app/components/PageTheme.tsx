import { createContext, useContext } from "react";

export interface PageTheme {
  accent: string;
  accentLight: string;
  accentDark: string;
}

// Single accent for all tabs — no more per-tab color tinting
const THEME: PageTheme = { accent: "#5C6AC4", accentLight: "#F0F1FF", accentDark: "#4650A8" };

export const PageThemeContext = createContext<PageTheme>(THEME);

export function usePageTheme() {
  return useContext(PageThemeContext);
}

// Tab definitions — unified accent, labels and paths only
export const PAGE_TABS = [
  { label: "Health",          path: "/app" },
  { label: "Customers",      path: "/app/customers" },
  { label: "Products",       path: "/app/products" },
  { label: "Ads",            path: "/app/campaigns" },
  { label: "Countries",      path: "/app/geo" },
  { label: "Weekly Report",  path: "/app/weekly" },
  { label: "UTM Manager",    path: "/app/utm" },
  { label: "Change Log",     path: "/app/changes" },
] as const;

export function getThemeForPath(_pathname: string): PageTheme {
  return THEME;
}

