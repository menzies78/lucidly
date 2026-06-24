import { useState, useCallback, useEffect } from "react";
import { useSearchParams, useLocation, useNavigate } from "@remix-run/react";
import {
  Popover, Button, DatePicker,
  InlineStack, BlockStack, Box, Text,
} from "@shopify/polaris";
import { CalendarIcon } from "@shopify/polaris-icons";

const PRESETS = [
  { content: "Today", value: "today" },
  { content: "Yesterday", value: "yesterday" },
  { content: "Last 7 days", value: "last7" },
  { content: "Last 14 days", value: "last14" },
  { content: "Last 30 days", value: "last30" },
  { content: "Last 90 days", value: "last90" },
  { content: "This week", value: "thisWeek" },
  { content: "Last week", value: "lastWeek" },
  { content: "This month", value: "thisMonth" },
  { content: "Last month", value: "lastMonth" },
  { content: "This year", value: "thisYear" },
  { content: "Last year", value: "lastYear" },
  { content: "Last 365 days", value: "last365" },
  { content: "All time", value: "all" },
];

const COMPARE_OPTIONS = [
  { label: "No comparison", value: "none" },
  { label: "Previous period", value: "previous" },
  { label: "Same period last year", value: "yoy" },
];

const STORAGE_KEY = "lucidly_date_range";

function saveDateParams(params: Record<string, string>) {
  try { sessionStorage.setItem(STORAGE_KEY, JSON.stringify(params)); } catch {}
  try {
    document.cookie = `lucidly_date=${encodeURIComponent(JSON.stringify(params))}; path=/; SameSite=Lax; max-age=86400`;
  } catch {}
}

function loadDateParams(): Record<string, string> | null {
  try {
    const stored = sessionStorage.getItem(STORAGE_KEY);
    return stored ? JSON.parse(stored) : null;
  } catch { return null; }
}

function computePresetDates(preset: string): { from: string; to: string } {
  const today = new Date();
  today.setHours(0, 0, 0, 0);

  if (preset === "today") {
    return { from: fmt(today), to: fmt(today) };
  }

  const yesterday = new Date(today);
  yesterday.setDate(today.getDate() - 1);

  if (preset === "yesterday") {
    return { from: fmt(yesterday), to: fmt(yesterday) };
  }

  const to = new Date(yesterday);
  const from = new Date(yesterday);

  switch (preset) {
    case "last7":
      from.setDate(to.getDate() - 6); break;
    case "last14":
      from.setDate(to.getDate() - 13); break;
    case "last30":
      from.setDate(to.getDate() - 29); break;
    case "last90":
      from.setDate(to.getDate() - 89); break;
    case "last365":
      from.setDate(to.getDate() - 364); break;
    case "thisWeek": {
      const dow = today.getDay();
      const mondayOffset = dow === 0 ? 6 : dow - 1;
      const monday = new Date(today);
      monday.setDate(today.getDate() - mondayOffset);
      return { from: fmt(monday), to: fmt(yesterday) };
    }
    case "lastWeek": {
      const dow = today.getDay();
      const mondayOffset = dow === 0 ? 6 : dow - 1;
      const thisMonday = new Date(today);
      thisMonday.setDate(today.getDate() - mondayOffset);
      const lastMonday = new Date(thisMonday);
      lastMonday.setDate(thisMonday.getDate() - 7);
      const lastSunday = new Date(thisMonday);
      lastSunday.setDate(thisMonday.getDate() - 1);
      return { from: fmt(lastMonday), to: fmt(lastSunday) };
    }
    case "thisMonth":
      from.setFullYear(today.getFullYear(), today.getMonth(), 1); break;
    case "lastMonth": {
      const lm = new Date(today.getFullYear(), today.getMonth() - 1, 1);
      const ld = new Date(today.getFullYear(), today.getMonth(), 0);
      return { from: fmt(lm), to: fmt(ld) };
    }
    case "thisYear":
      from.setFullYear(today.getFullYear(), 0, 1); break;
    case "lastYear": {
      const ly = today.getFullYear() - 1;
      return { from: `${ly}-01-01`, to: `${ly}-12-31` };
    }
    case "all":
      return { from: "2020-01-01", to: fmt(yesterday) };
    default:
      from.setDate(to.getDate() - 29);
  }
  return { from: fmt(from), to: fmt(to) };
}

function fmt(d: Date): string {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

function formatLabel(from: string, to: string): string {
  const f = new Date(from + "T12:00:00");
  const t = new Date(to + "T12:00:00");
  const opts: Intl.DateTimeFormatOptions = { month: "short", day: "numeric" };
  const fYear = f.getFullYear();
  const tYear = t.getFullYear();
  const fStr = f.toLocaleDateString("en-GB", { ...opts, ...(fYear !== tYear ? { year: "numeric" } : {}) });
  const tStr = t.toLocaleDateString("en-GB", { ...opts, year: "numeric" });
  return `${fStr} – ${tStr}`;
}

const HIDDEN_PATHS = ["/app/meta-connect", "/app/weekly"];

export default function DateRangeSelector() {
  const location = useLocation();
  const navigate = useNavigate();
  const isHidden = HIDDEN_PATHS.some(p => location.pathname.startsWith(p));
  const [searchParams, setSearchParams] = useSearchParams();

  const fromParam = searchParams.get("from") || "";
  const toParam = searchParams.get("to") || "";
  const preset = searchParams.get("preset") || "";
  const compare = searchParams.get("compare") || "none";

  useEffect(() => {
    if (isHidden) return;
    if (fromParam || toParam || preset) return;

    const saved = loadDateParams();
    if (saved && (saved.from || saved.preset)) {
      const params = new URLSearchParams(searchParams);
      if (saved.preset) {
        // Recompute dates from preset so they're always fresh (e.g. "today" updates to actual today)
        const fresh = computePresetDates(saved.preset);
        params.set("from", fresh.from);
        params.set("to", fresh.to);
        params.set("preset", saved.preset);
      } else {
        if (saved.from) params.set("from", saved.from);
        if (saved.to) params.set("to", saved.to);
      }
      if (saved.compare && saved.compare !== "none") params.set("compare", saved.compare);
      setSearchParams(params, { replace: true });
    }
  }, [location.pathname]);

  // If an old session restored a URL that still carries a preset with stale
  // from/to (e.g. a tab left open overnight), rewrite the URL to today's
  // computed range so every component reading from/to stays in sync and the
  // saved snapshot doesn't drift further. Server-side parseDateRange already
  // recomputes presets, so this only re-aligns the client.
  useEffect(() => {
    if (isHidden || !preset) return;
    const fresh = computePresetDates(preset);
    if (fresh.from !== fromParam || fresh.to !== toParam) {
      const params = new URLSearchParams(searchParams);
      params.set("from", fresh.from);
      params.set("to", fresh.to);
      params.set("preset", preset);
      if (compare !== "none") params.set("compare", compare);
      setSearchParams(params, { replace: true });
      saveDateParams({ from: fresh.from, to: fresh.to, preset, compare });
    }
  }, [location.pathname, preset]);

  // A preset is a deterministic function of "today", so the from/to carried in
  // the URL are only a cached snapshot. Always recompute the displayed range
  // from the preset key, otherwise a stale session (e.g. a tab left open
  // overnight) keeps showing yesterday's dates even though the data is fresh.
  // Only custom ranges (no preset) use the literal from/to.
  let displayFrom = fromParam;
  let displayTo = toParam;
  if (preset) {
    const p = computePresetDates(preset);
    displayFrom = p.from;
    displayTo = p.to;
  } else if (!displayFrom || !displayTo) {
    const p = computePresetDates("last30");
    displayFrom = p.from;
    displayTo = p.to;
  }

  const [popoverActive, setPopoverActive] = useState(false);
  const togglePopover = useCallback(() => setPopoverActive(v => !v), []);

  const startDate = new Date(displayFrom + "T12:00:00");
  const [{ month, year }, setDate] = useState({
    month: startDate.getMonth(),
    year: startDate.getFullYear(),
  });
  const [selectedDates, setSelectedDates] = useState({
    start: startDate,
    end: new Date(displayTo + "T12:00:00"),
  });

  const handleDateChange = useCallback((range: { start: Date; end: Date }) => {
    setSelectedDates(range);
  }, []);

  const handleMonthChange = useCallback((m: number, y: number) => {
    setDate({ month: m, year: y });
  }, []);

  const applyDates = useCallback(() => {
    const params = new URLSearchParams(searchParams);
    params.set("from", fmt(selectedDates.start));
    params.set("to", fmt(selectedDates.end));
    params.delete("preset");
    setSearchParams(params, { preventScrollReset: true });
    setPopoverActive(false);
    saveDateParams({ from: fmt(selectedDates.start), to: fmt(selectedDates.end), compare });
  }, [selectedDates, searchParams, setSearchParams, compare]);

  const applyPreset = useCallback((presetValue: string) => {
    const dates = computePresetDates(presetValue);
    const params = new URLSearchParams(searchParams);
    params.set("from", dates.from);
    params.set("to", dates.to);
    params.set("preset", presetValue);
    if (compare !== "none") params.set("compare", compare);
    setSearchParams(params, { preventScrollReset: true });
    setPopoverActive(false);
    saveDateParams({ from: dates.from, to: dates.to, preset: presetValue, compare });

    const s = new Date(dates.from + "T12:00:00");
    setSelectedDates({ start: s, end: new Date(dates.to + "T12:00:00") });
    setDate({ month: s.getMonth(), year: s.getFullYear() });
  }, [searchParams, setSearchParams, compare]);

  const handleCompareChange = useCallback((value: string) => {
    const params = new URLSearchParams(searchParams);
    if (value === "none") {
      params.delete("compare");
    } else {
      params.set("compare", value);
    }
    setSearchParams(params, { preventScrollReset: true });
    saveDateParams({ from: fromParam || displayFrom, to: toParam || displayTo, preset, compare: value });
  }, [searchParams, setSearchParams, fromParam, toParam, displayFrom, displayTo, preset]);

  if (isHidden) return null;

  const presetLabel = PRESETS.find(p => p.value === preset)?.content;
  const buttonLabel = presetLabel
    ? `${presetLabel} (${formatLabel(displayFrom, displayTo)})`
    : formatLabel(displayFrom, displayTo);

  const activator = (
    <Button onClick={togglePopover} icon={CalendarIcon} disclosure>
      {buttonLabel}
    </Button>
  );

  return (
    <>
      {/* Spacer reserves the page-flow real estate the floating selector
          would otherwise occupy. Polaris Frame has no scroll container we
          can attach `position: sticky` to (the embedded Shopify iframe is
          the actual scroller), so we use position: fixed and a layout-flow
          spacer to keep page content from sliding underneath it. */}
      <div style={{ height: "52px" }} aria-hidden="true" />
      <div style={{
        position: "fixed",
        top: 0,
        left: 0,
        right: 0,
        zIndex: 30,
        background: "#F6F6F7",
        padding: "10px 16px",
        borderBottom: "1px solid #E4E5E7",
        boxShadow: "0 1px 3px rgba(0,0,0,0.04)",
        display: "flex",
        alignItems: "center",
        gap: 16,
      }}>
      {/* Left: Lucidly logo */}
      <img src="/lucidly-logo-brand.svg" alt="Lucidly" height={26} style={{ height: 26, width: "auto", flexShrink: 0 }} />

      {/* Date control, labelled */}
      <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
        <span style={{ fontSize: 13, fontWeight: 600, color: "#42474C", whiteSpace: "nowrap" }}>Select date</span>
      <Popover
        active={popoverActive}
        activator={activator}
        onClose={togglePopover}
        preferredAlignment="left"
        fluidContent
      >
        <div style={{ display: "flex", width: "680px" }}>
          {/* Presets column */}
          <div style={{ width: "160px", borderRight: "1px solid #E4E5E7", padding: "8px 0", overflowY: "auto", maxHeight: "420px" }}>
            {PRESETS.map(p => (
              <button
                key={p.value}
                onClick={() => applyPreset(p.value)}
                style={{
                  display: "block", width: "100%", textAlign: "left",
                  padding: "7px 14px", border: "none", cursor: "pointer",
                  fontSize: "13px", lineHeight: "1.3",
                  background: preset === p.value ? "#F3F0FF" : "transparent",
                  color: preset === p.value ? "#7C3AED" : "#1F2937",
                  fontWeight: preset === p.value ? 600 : 400,
                }}
                onMouseEnter={e => { if (preset !== p.value) e.currentTarget.style.background = "#F6F6F7"; }}
                onMouseLeave={e => { if (preset !== p.value) e.currentTarget.style.background = "transparent"; }}
              >
                {p.content}
              </button>
            ))}
          </div>

          {/* Calendar + compare column */}
          <div style={{ flex: 1, padding: "12px 16px", display: "flex", flexDirection: "column", gap: "12px" }}>
            <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
              <Text as="p" variant="headingSm">Custom range</Text>
              <Text as="span" variant="bodySm" tone="subdued">
                {formatLabel(fmt(selectedDates.start), fmt(selectedDates.end))}
              </Text>
            </div>

            <DatePicker
              month={month}
              year={year}
              onChange={handleDateChange}
              onMonthChange={handleMonthChange}
              selected={selectedDates}
              allowRange
              multiMonth
            />

            {/* Apply row - Compare removed; period-over-period compare lives
                on the individual tiles via the DeltaBadge hover overlay. */}
            <div style={{ display: "flex", alignItems: "center", justifyContent: "flex-end", borderTop: "1px solid #E4E5E7", paddingTop: "10px" }}>
              <Button variant="primary" onClick={applyDates}>Apply</Button>
            </div>
          </div>
        </div>
      </Popover>
      </div>
      </div>
    </>
  );
}
