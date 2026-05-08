import { createContext, useContext, useState, useEffect, useCallback } from "react";

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
  { label: "Ad Campaigns",   path: "/app/campaigns" },
  { label: "Countries",      path: "/app/geo" },
  { label: "Weekly Report",  path: "/app/weekly" },
  { label: "Order Explorer", path: "/app/orders" },
  { label: "UTM Manager",    path: "/app/utm" },
  { label: "Change Log",     path: "/app/changes" },
] as const;

export function getThemeForPath(_pathname: string): PageTheme {
  return THEME;
}

// Dark mode hook — persists to localStorage, applies data-theme attribute
export function useDarkMode(): [boolean, () => void] {
  const [dark, setDark] = useState(() => {
    if (typeof window === "undefined") return false;
    return window.localStorage.getItem("lucidly.darkMode") === "1";
  });

  useEffect(() => {
    if (typeof document === "undefined") return;
    document.documentElement.setAttribute("data-theme", dark ? "dark" : "light");
    window.localStorage.setItem("lucidly.darkMode", dark ? "1" : "0");
  }, [dark]);

  const toggle = useCallback(() => setDark(d => !d), []);
  return [dark, toggle];
}
