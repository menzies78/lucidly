import { useState, useCallback, useMemo, useRef, useEffect } from "react";
import { createPortal } from "react-dom";
import {
  useReactTable,
  getCoreRowModel,
  getSortedRowModel,
  getFilteredRowModel,
  flexRender,
  type ColumnDef,
  type SortingState,
  type ColumnFiltersState,
  type VisibilityState,
  type FilterFn,
} from "@tanstack/react-table";
import {
  Popover, Button, Checkbox, BlockStack, InlineStack,
  TextField, Text, Box,
} from "@shopify/polaris";
import { usePageTheme } from "./PageTheme";

// Column meta: { align?: "right", maxWidth?: string, filterType?: "multi-select", description?: string }

// Tooltip styles removed — now using portal-based FixedTooltip

interface TooltipData {
  title?: string;
  description: string;
  calc?: string;
  rect: DOMRect;
}

function FixedTooltip({ tip }: { tip: TooltipData | null }) {
  if (!tip || typeof document === "undefined") return null;
  return createPortal(
    <div style={{
      position: "fixed",
      top: tip.rect.bottom + 6,
      left: Math.min(Math.max(tip.rect.left + tip.rect.width / 2, 160), window.innerWidth - 160),
      transform: "translateX(-50%)",
      background: "#1e1e1e",
      color: "#e8e8e8",
      padding: "8px 12px",
      borderRadius: "8px",
      fontSize: "12.5px",
      fontWeight: 400,
      lineHeight: 1.5,
      whiteSpace: "normal",
      width: "max-content",
      maxWidth: "300px",
      zIndex: 9999,
      pointerEvents: "none",
      boxShadow: "0 4px 12px rgba(0,0,0,0.3)",
      fontFamily: "system-ui, -apple-system, 'Segoe UI', Roboto, sans-serif",
      letterSpacing: "0.01em",
    }}>
      {tip.title && <div style={{ fontWeight: 700, fontSize: "13.5px", color: "#fff", marginBottom: "4px" }}>{tip.title}</div>}
      <div style={{ color: "#e0e0e0", fontWeight: 500, fontSize: "13px", lineHeight: 1.5 }}>{tip.description}</div>
      {tip.calc && <div style={{ color: "#9ab4d0", marginTop: "4px", fontSize: "12px", fontWeight: 500, fontStyle: "italic" }}>{tip.calc}</div>}
    </div>,
    document.body,
  );
}

const multiSelectFilter: FilterFn<any> = (row, columnId, filterValue) => {
  if (!filterValue || filterValue.length === 0) return true;
  const cellValue = String(row.getValue(columnId) ?? "");
  return filterValue.includes(cellValue);
};

function loadSavedColumns(tableId: string): string[] | null {
  try {
    const raw = localStorage.getItem(`lucidly_cols_${tableId}`);
    return raw ? JSON.parse(raw) : null;
  } catch { return null; }
}

function saveColumns(tableId: string, visibleCols: string[]) {
  try {
    localStorage.setItem(`lucidly_cols_${tableId}`, JSON.stringify(visibleCols));
  } catch {}
}

function loadSavedWidths(tableId: string): Record<string, number> | null {
  try {
    const raw = localStorage.getItem(`lucidly_widths_${tableId}`);
    return raw ? JSON.parse(raw) : null;
  } catch { return null; }
}

function saveSavedWidths(tableId: string, widths: Record<string, number>) {
  try {
    localStorage.setItem(`lucidly_widths_${tableId}`, JSON.stringify(widths));
  } catch {}
}

interface ColumnProfile {
  id: string;
  label: string;
  icon: string;
  description?: string;    // Tooltip for the profile pill
  columns: string[];       // Lite columns
  fullColumns?: string[];  // Full columns (if omitted, no Lite/Full toggle)
}

interface InteractiveTableProps {
  columns: ColumnDef<any, any>[];
  data: any[];
  defaultVisibleColumns?: string[];
  defaultColumnWidths?: Record<string, number>;
  tableId?: string;
  toolbarExtra?: React.ReactNode;
  footerRow?: Record<string, React.ReactNode>;
  stickyTopOffset?: number; // px offset for sticky elements (e.g. if external sticky header above)
  rowBackgroundFn?: (original: any, index: number) => string; // custom row background color
  columnProfiles?: ColumnProfile[];
  defaultFilters?: ColumnFiltersState;
  initialSorting?: SortingState;
}

function MultiSelectFilter({
  column,
  data,
  onFilterChange,
}: {
  column: any;
  data: any[];
  onFilterChange: () => void;
}) {
  const [open, setOpen] = useState(false);
  const columnId = column.id;
  const filterValue: string[] = (column.getFilterValue() as string[]) || [];

  const uniqueValues = useMemo(() => {
    const vals = new Set<string>();
    for (const row of data) {
      const v = row[columnId];
      if (v != null && v !== "") vals.add(String(v));
    }
    return [...vals].sort((a, b) => a.localeCompare(b, undefined, { sensitivity: "base" }));
  }, [data, columnId]);

  const activeCount = filterValue.length;
  const label = activeCount > 0 ? `${activeCount} selected` : "All";

  const toggle = (val: string) => {
    const next = filterValue.includes(val)
      ? filterValue.filter(v => v !== val)
      : [...filterValue, val];
    column.setFilterValue(next.length > 0 ? next : undefined);
    onFilterChange();
  };

  const selectAll = () => {
    column.setFilterValue(undefined);
    onFilterChange();
  };

  return (
    <Popover
      active={open}
      activator={
        <button
          onClick={() => setOpen(v => !v)}
          style={{
            padding: "2px 6px",
            fontSize: "11px",
            border: activeCount > 0 ? "1px solid #2c6ecb" : "1px solid #c9cccf",
            borderRadius: "4px",
            cursor: "pointer",
            backgroundColor: activeCount > 0 ? "#f0f5ff" : "#fff",
            color: activeCount > 0 ? "#2c6ecb" : "#6d7175",
            whiteSpace: "nowrap",
            maxWidth: "120px",
            overflow: "hidden",
            textOverflow: "ellipsis",
          }}
        >
          {label}
        </button>
      }
      onClose={() => setOpen(false)}
      preferredAlignment="left"
    >
      <Popover.Pane>
        <Box padding="200" minWidth="180px">
          <BlockStack gap="100">
            <InlineStack gap="200">
              <Button size="slim" onClick={selectAll}>Show All</Button>
            </InlineStack>
            <div style={{ maxHeight: 250, overflowY: "auto" }}>
              <BlockStack gap="050">
                {uniqueValues.map(val => (
                  <Checkbox
                    key={val}
                    label={val || "(empty)"}
                    checked={filterValue.length === 0 || filterValue.includes(val)}
                    onChange={() => {
                      if (filterValue.length === 0) {
                        column.setFilterValue([val]);
                        onFilterChange();
                      } else {
                        toggle(val);
                      }
                    }}
                  />
                ))}
              </BlockStack>
            </div>
          </BlockStack>
        </Box>
      </Popover.Pane>
    </Popover>
  );
}

export default function InteractiveTable({
  columns,
  data,
  defaultVisibleColumns,
  defaultColumnWidths,
  tableId,
  toolbarExtra,
  footerRow,
  stickyTopOffset = 0,
  rowBackgroundFn,
  columnProfiles,
  defaultFilters,
  initialSorting,
}: InteractiveTableProps) {
  const pageTheme = usePageTheme();
  const [sorting, setSorting] = useState<SortingState>(initialSorting || []);
  const [columnFilters, setColumnFilters] = useState<ColumnFiltersState>(defaultFilters || []);
  const [globalFilter, setGlobalFilter] = useState("");

  // Column widths (draggable)
  const initialWidths = useMemo(() => {
    const saved = tableId ? loadSavedWidths(tableId) : null;
    return { ...(defaultColumnWidths || {}), ...(saved || {}) };
  }, [tableId, defaultColumnWidths]);
  const [columnWidths, setColumnWidths] = useState<Record<string, number>>(initialWidths);

  // Column visibility — check for saved user defaults
  const defaultVisibility = useMemo(() => {
    if (!defaultVisibleColumns) return {};
    const vis: VisibilityState = {};
    for (const col of columns) {
      const id = (col as any).accessorKey || (col as any).id;
      if (id) vis[id] = defaultVisibleColumns.includes(id);
    }
    return vis;
  }, [columns, defaultVisibleColumns]);

  const initialVisibility = useMemo(() => {
    if (tableId) {
      const saved = loadSavedColumns(tableId);
      if (saved) {
        const vis: VisibilityState = {};
        for (const col of columns) {
          const id = (col as any).accessorKey || (col as any).id;
          if (id) vis[id] = saved.includes(id);
        }
        return vis;
      }
    }
    return defaultVisibility;
  }, [tableId, columns, defaultVisibility]);

  const [columnVisibility, setColumnVisibility] = useState<VisibilityState>(initialVisibility);
  const [colPickerOpen, setColPickerOpen] = useState(false);
  const [tooltip, setTooltip] = useState<TooltipData | null>(null);
  const [savedFeedback, setSavedFeedback] = useState(false);
  const [activeProfileId, setActiveProfileId] = useState<string | null>(null);
  const [activeProfileMode, setActiveProfileMode] = useState<"lite" | "full">("lite");

  const applyProfileWithMode = useCallback((profile: ColumnProfile, mode: "lite" | "full") => {
    const cols = mode === "full" && profile.fullColumns ? profile.fullColumns : profile.columns;
    const vis: VisibilityState = {};
    for (const col of columns) {
      const id = (col as any).accessorKey || (col as any).id;
      if (id) vis[id] = id === "select" || cols.includes(id);
    }
    setColumnVisibility(vis);
    setActiveProfileId(profile.id);
    setActiveProfileMode(mode);
  }, [columns]);

  const handleProfileClick = useCallback((profile: ColumnProfile) => {
    // No fullColumns = no toggle (e.g. "All"), just apply
    if (!profile.fullColumns) {
      if (activeProfileId === profile.id) {
        // Click again = deactivate, reset to default
        setColumnVisibility(defaultVisibility);
        setActiveProfileId(null);
        setActiveProfileMode("lite");
      } else {
        applyProfileWithMode(profile, "lite");
      }
      return;
    }
    // Cycle: off → lite → full → off
    if (activeProfileId === profile.id) {
      if (activeProfileMode === "lite") {
        applyProfileWithMode(profile, "full");
      } else {
        setColumnVisibility(defaultVisibility);
        setActiveProfileId(null);
        setActiveProfileMode("lite");
      }
    } else {
      applyProfileWithMode(profile, "lite");
    }
  }, [activeProfileId, activeProfileMode, applyProfileWithMode, defaultVisibility]);

  const table = useReactTable({
    data,
    columns,
    state: { sorting, columnFilters, columnVisibility, globalFilter },
    onSortingChange: setSorting,
    onColumnFiltersChange: setColumnFilters,
    onColumnVisibilityChange: setColumnVisibility,
    onGlobalFilterChange: setGlobalFilter,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
    filterFns: { multiSelect: multiSelectFilter },
  });

  const allRows = table.getRowModel().rows;
  const totalRows = allRows.length;

  const hasFilterRow = table.getVisibleLeafColumns().some(
    col => (col.columnDef.meta as any)?.filterType === "multi-select"
  );

  // Columns that should always be visible and not appear in the column picker
  const alwaysOnColumns = useMemo(() => new Set(["select"]), []);

  const toggleAll = useCallback((show: boolean) => {
    const vis: VisibilityState = {};
    for (const col of table.getAllColumns()) {
      vis[col.id] = alwaysOnColumns.has(col.id) ? true : show;
    }
    setColumnVisibility(vis);
    setActiveProfileId(show ? "all" : null);
  }, [table, alwaysOnColumns]);

  const handleSaveDefaults = useCallback(() => {
    if (!tableId) return;
    const visibleCols = table.getVisibleLeafColumns().map(c => c.id);
    saveColumns(tableId, visibleCols);
    setSavedFeedback(true);
    setTimeout(() => setSavedFeedback(false), 1500);
  }, [tableId, table]);

  const handleResetDefaults = useCallback(() => {
    if (tableId) {
      try { localStorage.removeItem(`lucidly_cols_${tableId}`); } catch {}
    }
    setColumnVisibility(defaultVisibility);
    setActiveProfileId(null);
  }, [tableId, defaultVisibility]);

  // Column resize handling
  const resizingRef = useRef<{ colId: string; startX: number; startW: number } | null>(null);

  const onResizeStart = useCallback((e: React.MouseEvent, colId: string, currentWidth: number) => {
    e.preventDefault();
    e.stopPropagation();
    resizingRef.current = { colId, startX: e.clientX, startW: currentWidth };

    const onMouseMove = (ev: MouseEvent) => {
      if (!resizingRef.current) return;
      const diff = ev.clientX - resizingRef.current.startX;
      const newWidth = Math.max(40, resizingRef.current.startW + diff);
      setColumnWidths(prev => ({ ...prev, [colId]: newWidth }));
    };

    const onMouseUp = () => {
      resizingRef.current = null;
      if (tableId) {
        setColumnWidths(prev => {
          saveSavedWidths(tableId, prev);
          return prev;
        });
      }
      document.removeEventListener("mousemove", onMouseMove);
      document.removeEventListener("mouseup", onMouseUp);
    };

    document.addEventListener("mousemove", onMouseMove);
    document.addEventListener("mouseup", onMouseUp);
  }, [tableId]);

  const pickableCols = table.getAllColumns().filter(c => !alwaysOnColumns.has(c.id));
  const visiblePickable = pickableCols.filter(c => c.getIsVisible()).length;

  const colPickerActivator = (
    <Button onClick={() => setColPickerOpen(v => !v)} size="slim">
      {`Columns (${visiblePickable}/${pickableCols.length})`}
    </Button>
  );

  const activeFilterCount = columnFilters.length;

  // Measure toolbar height for sticky thead offset
  const toolbarRef = useRef<HTMLDivElement>(null);
  const [toolbarHeight, setToolbarHeight] = useState(44);
  useEffect(() => {
    if (!toolbarRef.current) return;
    const measure = () => {
      const h = toolbarRef.current?.offsetHeight || 44;
      setToolbarHeight(h);
    };
    measure();
    const obs = new ResizeObserver(measure);
    obs.observe(toolbarRef.current);
    return () => obs.disconnect();
  }, []);

  const headerRowHeight = 37;
  const toolbarTop = stickyTopOffset;
  const headerTop = stickyTopOffset + toolbarHeight;
  const filterRowTop = headerTop + headerRowHeight;

  // Detect when header/footer are floating (stuck)
  const tableRef = useRef<HTMLTableElement>(null);
  const tfootRef = useRef<HTMLTableSectionElement>(null);
  const [footerIsFloating, setFooterIsFloating] = useState(false);
  const [headerIsFloating, setHeaderIsFloating] = useState(false);
  useEffect(() => {
    if (!tableRef.current) return;
    const check = () => {
      const tableEl = tableRef.current;
      if (!tableEl) return;
      const tableRect = tableEl.getBoundingClientRect();
      setFooterIsFloating(footerRow ? tableRect.bottom > window.innerHeight + 5 : false);
      // Header is floating when the table top has scrolled above the sticky offset
      const stickyPoint = headerTop + headerRowHeight + (hasFilterRow ? 30 : 0);
      setHeaderIsFloating(tableRect.top < stickyPoint - 5);
    };
    check();
    window.addEventListener("scroll", check, { passive: true, capture: true });
    window.addEventListener("resize", check, { passive: true });
    return () => {
      window.removeEventListener("scroll", check, { capture: true });
      window.removeEventListener("resize", check);
    };
  }, [footerRow, allRows.length, headerTop, headerRowHeight, hasFilterRow]);

  return (
    <div>
      {/* Sticky toolbar */}
      <div
        ref={toolbarRef}
        style={{
          position: "sticky",
          top: toolbarTop,
          zIndex: 10,
          backgroundColor: "#fff",
          padding: "10px 10px 8px",
          borderBottom: "1px solid #e1e3e5",
        }}
      >
        <InlineStack gap="300" align="start" blockAlign="center" wrap>
          <div style={{ maxWidth: 250 }}>
            <TextField
              label=""
              labelHidden
              placeholder="Search..."
              value={globalFilter}
              onChange={(v) => setGlobalFilter(v)}
              clearButton
              onClearButtonClick={() => setGlobalFilter("")}
              autoComplete="off"
            />
          </div>
          {toolbarExtra}
          <Popover
            active={colPickerOpen}
            activator={colPickerActivator}
            onClose={() => setColPickerOpen(false)}
            preferredAlignment="right"
          >
            <Popover.Pane>
              <Box padding="300" minWidth="220px">
                <BlockStack gap="100">
                  <InlineStack gap="200" wrap>
                    <Button size="slim" onClick={() => toggleAll(true)}>All</Button>
                    <Button size="slim" onClick={handleResetDefaults}>Default</Button>
                    <Button size="slim" onClick={() => toggleAll(false)}>None</Button>
                  </InlineStack>
                  <div style={{ maxHeight: 300, overflowY: "auto" }}>
                    <BlockStack gap="050">
                      {table.getAllColumns().filter(col => !alwaysOnColumns.has(col.id)).map(col => {
                        const meta = col.columnDef.meta as any;
                        const headerLabel = typeof col.columnDef.header === "string" ? col.columnDef.header : col.id;
                        return (
                          <div
                            key={col.id}
                            onMouseEnter={(e) => {
                              if (meta?.description) {
                                setTooltip({ title: headerLabel, description: meta.description, calc: meta.calc, rect: (e.currentTarget as HTMLElement).getBoundingClientRect() });
                              }
                            }}
                            onMouseLeave={() => setTooltip(null)}
                          >
                            <Checkbox
                              label={headerLabel}
                              checked={col.getIsVisible()}
                              onChange={(checked) => { col.toggleVisibility(checked); setActiveProfileId(null); }}
                            />
                          </div>
                        );
                      })}
                    </BlockStack>
                  </div>
                  {tableId && (
                    <InlineStack gap="200" blockAlign="center">
                      <Button size="slim" variant="primary" onClick={handleSaveDefaults}>
                        {savedFeedback ? "Saved!" : "Save as Default"}
                      </Button>
                    </InlineStack>
                  )}
                </BlockStack>
              </Box>
            </Popover.Pane>
          </Popover>
          {columnProfiles && columnProfiles.length > 0 && (
            <div style={{ display: "flex", gap: "4px", flexWrap: "wrap" }}>
              {columnProfiles.map(profile => {
                const isActive = activeProfileId === profile.id;
                const hasModes = !!profile.fullColumns;

                // Inactive or no modes: simple pill
                if (!isActive || !hasModes) {
                  return (
                    <button
                      key={profile.id}
                      onClick={() => handleProfileClick(profile)}
                      onMouseEnter={(e) => {
                        if (profile.description) {
                          const hint = hasModes ? "Click once for Lite, click twice for Full" : undefined;
                          setTooltip({ title: profile.label, description: profile.description, calc: hint, rect: (e.currentTarget as HTMLElement).getBoundingClientRect() });
                        }
                      }}
                      onMouseLeave={() => setTooltip(null)}
                      style={{
                        padding: "4px 10px",
                        fontSize: "12px",
                        fontWeight: isActive ? 600 : 400,
                        border: isActive ? `1px solid ${pageTheme.accent}` : "1px solid #c9cccf",
                        borderRadius: "16px",
                        cursor: "pointer",
                        backgroundColor: isActive ? pageTheme.accentLight : "#fafbfb",
                        color: isActive ? pageTheme.accentDark : "#555",
                        whiteSpace: "nowrap",
                        display: "flex",
                        alignItems: "center",
                        gap: "4px",
                        transition: "all 0.15s ease",
                      }}
                    >
                      <span>{profile.icon}</span>
                      <span>{profile.label}</span>
                    </button>
                  );
                }

                // Active with modes: expanded pill with Lite | Full toggle
                return (
                  <div
                    key={profile.id}
                    onMouseEnter={(e) => {
                      if (profile.description) {
                        setTooltip({ title: profile.label, description: profile.description, rect: (e.currentTarget as HTMLElement).getBoundingClientRect() });
                      }
                    }}
                    onMouseLeave={() => setTooltip(null)}
                    style={{
                      display: "flex",
                      alignItems: "center",
                      border: `1px solid ${pageTheme.accent}`,
                      borderRadius: "16px",
                      backgroundColor: pageTheme.accentLight,
                      overflow: "hidden",
                    }}
                  >
                    <button
                      onClick={() => handleProfileClick(profile)}
                      style={{
                        padding: "4px 8px 4px 10px",
                        fontSize: "12px",
                        fontWeight: 600,
                        border: "none",
                        cursor: "pointer",
                        backgroundColor: "transparent",
                        color: pageTheme.accentDark,
                        display: "flex",
                        alignItems: "center",
                        gap: "4px",
                      }}
                    >
                      <span>{profile.icon}</span>
                      <span>{profile.label}</span>
                    </button>
                    <div style={{ width: "1px", height: "16px", backgroundColor: `${pageTheme.accent}44` }} />
                    <button
                      onClick={() => applyProfileWithMode(profile, "lite")}
                      style={{
                        padding: "4px 6px",
                        fontSize: "11px",
                        fontWeight: activeProfileMode === "lite" ? 700 : 400,
                        border: "none",
                        cursor: "pointer",
                        backgroundColor: activeProfileMode === "lite" ? `${pageTheme.accent}22` : "transparent",
                        color: activeProfileMode === "lite" ? pageTheme.accentDark : `${pageTheme.accent}88`,
                        borderRadius: 0,
                      }}
                    >
                      Lite
                    </button>
                    <button
                      onClick={() => applyProfileWithMode(profile, "full")}
                      style={{
                        padding: "4px 8px 4px 6px",
                        fontSize: "11px",
                        fontWeight: activeProfileMode === "full" ? 700 : 400,
                        border: "none",
                        cursor: "pointer",
                        backgroundColor: activeProfileMode === "full" ? `${pageTheme.accent}22` : "transparent",
                        color: activeProfileMode === "full" ? pageTheme.accentDark : `${pageTheme.accent}88`,
                        borderRadius: "0 16px 16px 0",
                      }}
                    >
                      Full
                    </button>
                  </div>
                );
              })}
            </div>
          )}
          {activeFilterCount > 0 && (
            <Button size="slim" onClick={() => setColumnFilters([])}>
              Clear filters
            </Button>
          )}
          <div style={{ marginLeft: "auto" }}>
            <Text as="p" variant="bodySm" tone="subdued">
              {totalRows} row{totalRows !== 1 ? "s" : ""}
              {activeFilterCount > 0 ? ` (filtered)` : ""}
            </Text>
          </div>
        </InlineStack>
      </div>

      <table ref={tableRef} style={{
        width: "100%",
        borderCollapse: "separate",
        borderSpacing: 0,
        fontSize: "13px",
        tableLayout: "fixed",
        borderLeft: "1px solid #e1e3e5",
        borderRight: "1px solid #e1e3e5",
      }}>
        <colgroup>
          {table.getVisibleLeafColumns().map(col => {
            if (alwaysOnColumns.has(col.id)) return <col key={col.id} style={{ width: "41px" }} />;
            const w = columnWidths[col.id];
            return <col key={col.id} style={w ? { width: `${w}px` } : undefined} />;
          })}
        </colgroup>
        <thead>
          {table.getHeaderGroups().map(headerGroup => (
            <tr key={headerGroup.id}>
              {headerGroup.headers.map((header, hIdx) => {
                const canSort = header.column.getCanSort();
                const sorted = header.column.getIsSorted();
                const w = columnWidths[header.column.id];
                const isLast = hIdx === headerGroup.headers.length - 1;
                const isSelect = header.column.id === "select";
                const showResize = !isLast && !isSelect;
                return (
                  <th
                    key={header.id}
                    onClick={canSort ? header.column.getToggleSortingHandler() : undefined}
                    style={{
                      cursor: canSort ? "pointer" : "default",
                      userSelect: "none",
                      padding: "8px 6px 8px 6px",
                      textAlign: "left",
                      borderBottom: hasFilterRow ? "1px solid #e1e3e5" : `2px solid ${pageTheme.accent}33`,
                      borderRight: isLast ? "none" : "1px solid #d2d5d8",
                      whiteSpace: "nowrap",
                      fontSize: "12px",
                      fontWeight: 600,
                      color: pageTheme.accentDark,
                      position: "sticky",
                      top: headerTop,
                      backgroundColor: pageTheme.accentLight,
                      zIndex: 4,
                      overflow: "hidden",
                      textOverflow: "ellipsis",
                      boxShadow: (!hasFilterRow && headerIsFloating) ? "0 3px 6px rgba(0,0,0,0.10)" : "none",
                      clipPath: (!hasFilterRow && headerIsFloating) ? "inset(0 -1px -6px -1px)" : "none",
                    }}
                  >
                    <div
                      onMouseEnter={(e) => {
                        const meta = header.column.columnDef.meta as any;
                        if (meta?.description) {
                          const title = typeof header.column.columnDef.header === "string" ? header.column.columnDef.header : undefined;
                          setTooltip({ title, description: meta.description, calc: meta.calc, rect: (e.currentTarget as HTMLElement).getBoundingClientRect() });
                        }
                      }}
                      onMouseLeave={() => setTooltip(null)}
                      style={{
                      display: "flex",
                      alignItems: "center",
                      justifyContent: "flex-start",
                      gap: "4px",
                    }}>
                      <span style={{ overflow: "hidden", textOverflow: "ellipsis", flex: "0 1 auto" }}>
                        {flexRender(header.column.columnDef.header, header.getContext())}
                      </span>
                      {canSort && (
                        <span style={{
                          flex: "0 0 auto",
                          fontSize: "10px",
                          color: sorted ? "#2c6ecb" : "#b5b5b5",
                          lineHeight: 1,
                        }}>
                          {sorted === "asc" ? "▲" : sorted === "desc" ? "▼" : "⇅"}
                        </span>
                      )}
                      {showResize && (
                        <div
                          onMouseDown={(e) => onResizeStart(e, header.column.id, w || header.getSize())}
                          onClick={(e) => e.stopPropagation()}
                          style={{
                            position: "absolute",
                            right: "-4px",
                            top: 0,
                            bottom: 0,
                            width: "7px",
                            cursor: "col-resize",
                            zIndex: 5,
                            borderLeft: "1px solid #d2d5d8",
                          }}
                          onMouseEnter={(e) => { (e.currentTarget as HTMLElement).style.borderLeftColor = "#2c6ecb"; }}
                          onMouseLeave={(e) => { (e.currentTarget as HTMLElement).style.borderLeftColor = "#d2d5d8"; }}
                        />
                      )}
                    </div>
                  </th>
                );
              })}
            </tr>
          ))}
          {hasFilterRow && table.getHeaderGroups().map(headerGroup => (
            <tr key={headerGroup.id + "-filter"}>
              {headerGroup.headers.map((header, hIdx) => {
                const isLast = hIdx === headerGroup.headers.length - 1;
                const filterType = (header.column.columnDef.meta as any)?.filterType;
                const borderStyle = isLast ? "none" : "1px solid #d2d5d8";
                return (
                  <th key={header.id} style={{
                    padding: "3px 6px",
                    borderBottom: `2px solid ${pageTheme.accent}33`,
                    borderRight: borderStyle,
                    backgroundColor: pageTheme.accentLight,
                    position: "sticky",
                    top: filterRowTop,
                    zIndex: 4,
                    boxShadow: headerIsFloating ? "0 3px 6px rgba(0,0,0,0.10)" : "none",
                    clipPath: headerIsFloating ? "inset(0 -1px -6px -1px)" : "none",
                  }}>
                    {filterType === "multi-select" ? (
                      <MultiSelectFilter
                        column={header.column}
                        data={data}
                        onFilterChange={() => {}}
                      />
                    ) : null}
                  </th>
                );
              })}
            </tr>
          ))}
        </thead>
        <tbody>
          {allRows.map((row, rowIdx) => {
            const isBdRow = row.original._isBreakdownRow;
            const isParentRow = row.original._isParent;
            // Check if next row is a breakdown row (for parent row bottom border)
            const nextRow = allRows[rowIdx + 1];
            const nextIsBd = nextRow?.original?._isBreakdownRow;
            // Check if this is the last breakdown row before a parent or end
            const isLastBd = isBdRow && (!nextRow || !nextRow.original._isBreakdownRow);

            return (
              <tr key={row.id} style={{
                borderBottom: isLastBd ? "2px solid #d0d3d6" : isBdRow ? "1px solid #e8e8e8" : "1px solid #e1e3e5",
                backgroundColor: rowBackgroundFn ? rowBackgroundFn(row.original, rowIdx) : (rowIdx % 2 === 1 ? "#f7f8fa" : "#fff"),
              }}>
                {row.getVisibleCells().map((cell, cIdx) => {
                  const isLast = cIdx === row.getVisibleCells().length - 1;
                  return (
                    <td
                      key={cell.id}
                      style={{
                        padding: isBdRow ? "6px 10px" : "10px 10px",
                        whiteSpace: "nowrap",
                        textAlign: (cell.column.columnDef.meta as any)?.align === "right" ? "right" : "left",
                        fontSize: isBdRow ? "12px" : "13px",
                        overflow: "hidden",
                        textOverflow: "ellipsis",
                        borderRight: isLast ? "none" : "1px solid #ebebeb",
                        color: isBdRow ? "#555" : undefined,
                      }}
                      title={String(cell.getValue() ?? "")}
                    >
                      {flexRender(cell.column.columnDef.cell, cell.getContext())}
                    </td>
                  );
                })}
              </tr>
            );
          })}
        </tbody>
        {footerRow && (
          <tfoot ref={tfootRef}>
            <tr>
              {table.getVisibleLeafColumns().map((col, cIdx) => {
                const isLast = cIdx === table.getVisibleLeafColumns().length - 1;
                const content = footerRow[col.id];
                return (
                  <td
                    key={col.id}
                    style={{
                      padding: "18px 10px 100px",
                      whiteSpace: "nowrap",
                      textAlign: (col.columnDef.meta as any)?.align === "right" ? "right" : "left",
                      fontSize: "13px",
                      fontWeight: 700,
                      overflow: "hidden",
                      textOverflow: "ellipsis",
                      borderRight: isLast ? "none" : "1px solid #ebebeb",
                      borderTop: `2px solid ${pageTheme.accent}33`,
                      position: "sticky",
                      bottom: 0,
                      backgroundColor: pageTheme.accentLight,
                      zIndex: 3,
                      boxShadow: footerIsFloating ? "0 -3px 6px rgba(0,0,0,0.10)" : "none",
                      clipPath: footerIsFloating ? "inset(-6px -1px 0 -1px)" : "none",
                    }}
                  >
                    {content ?? ""}
                  </td>
                );
              })}
            </tr>
          </tfoot>
        )}
      </table>
      <FixedTooltip tip={tooltip} />
    </div>
  );
}
