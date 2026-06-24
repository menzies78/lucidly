import { Card, Text, BlockStack, InlineStack, Select } from "@shopify/polaris";
import { useMemo } from "react";
import type { ColumnDef } from "@tanstack/react-table";
import InteractiveTable from "./InteractiveTable";

// Order Explorer table + filters, extracted from app/routes/app.orders.tsx so
// it can be embedded inside the Customers tab. Filter state comes in via
// props (`tag`, `campaign`, callbacks) so the host route can wire it to URL
// search params however it likes.

export interface OrderExplorerSectionProps {
  rows: any[];
  campaigns: string[];
  currencySymbol: string;
  tagFilter: string;
  campaignFilter: string;
  onTagChange: (v: string) => void;
  onCampaignChange: (v: string) => void;
}

export default function OrderExplorerSection({
  rows, campaigns, currencySymbol,
  tagFilter, campaignFilter, onTagChange, onCampaignChange,
}: OrderExplorerSectionProps) {
  const cs = currencySymbol;

  const tagOptions = [
    { label: "All", value: "all" },
    { label: "All Meta", value: "meta" },
    { label: "Meta New", value: "Meta New" },
    { label: "Meta Repeat", value: "Meta Repeat" },
    { label: "Meta Retargeted", value: "Meta Retargeted" },
    { label: "Meta Unmatched (All)", value: "Meta Unmatched" },
    { label: "Meta Unmatched New", value: "Meta Unmatched New" },
    { label: "Meta Unmatched Repeat", value: "Meta Unmatched Repeat" },
    { label: "Meta Unmatched Retargeted", value: "Meta Unmatched Retargeted" },
    { label: "Unattributed", value: "Unattributed" },
    { label: "Non-Meta", value: "Non-Meta" },
    { label: "Non-Meta POS", value: "Non-Meta POS" },
  ];

  const campaignOptions = [
    { label: "All Campaigns", value: "all" },
    ...campaigns.map(c => ({ label: c, value: c })),
  ];

  const columns = useMemo<ColumnDef<any, any>[]>(() => [
    // ── Identity (anchor columns) ──
    { accessorKey: "orderNumber", header: "Order",
      meta: { description: "Shopify order ID" },
      cell: ({ getValue }) => getValue() || "-" },
    { accessorKey: "createdAtISO", header: "Date & Time",
      meta: { description: "When the order was placed" },
      cell: ({ getValue, row }) => {
        const iso = getValue() || (row.original.date ? row.original.date + "T12:00:00" : "");
        if (!iso) return "-";
        const d = new Date(iso);
        const date = d.toLocaleDateString("en-GB", { day: "numeric", month: "short", year: "numeric" });
        const time = d.toLocaleTimeString("en-GB", { hour: "2-digit", minute: "2-digit", hour12: false });
        return `${date} ${time}`;
      },
    },
    { accessorKey: "customerFirstName", header: "First Name",
      meta: { maxWidth: "140px", filterType: "text", description: "Customer first name (from Shopify billing address)" },
      cell: ({ getValue }) => getValue() || "-" },
    { accessorKey: "customerLastName", header: "Last Name",
      meta: { maxWidth: "140px", filterType: "text", description: "Customer last name (from Shopify billing address)" },
      cell: ({ getValue }) => getValue() || "-" },
    { accessorKey: "customerEmail", header: "Email",
      meta: { maxWidth: "240px", filterType: "text", description: "Customer email address. Use the download to export for manual segmentation" },
      cell: ({ getValue }) => getValue() || "-" },
    { accessorKey: "orderCount", header: "Orders Placed",
      meta: { align: "right", description: "How many orders this customer had placed at the time of this purchase" },
      cell: ({ getValue }) => {
        const v = getValue();
        if (v == null) return "-";
        return `${v}`;
      } },
    // ── Money ──
    { accessorKey: "revenue", header: "Revenue",
      meta: { align: "right", description: "Order total at time of purchase (frozen - unaffected by later edits)" },
      cell: ({ getValue }) => getValue() ? `${cs}${getValue().toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}` : "-" },
    { accessorKey: "netRevenue", header: "Net Revenue",
      meta: { align: "right", description: "Revenue after refunds", calc: "Revenue − Refunded" },
      cell: ({ getValue }) => {
        const v = getValue();
        if (v == null) return "-";
        return `${cs}${v.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
      } },
    { accessorKey: "totalRefunded", header: "Refunded",
      meta: { align: "right", description: "Amount refunded on this order" },
      cell: ({ getValue }) => getValue() > 0 ? `${cs}${getValue().toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}` : "-" },
    { accessorKey: "refundStatus", header: "Refund Status",
      meta: { filterType: "multi-select", description: "Current refund status of this order (none, partial, full)" },
      filterFn: "multiSelect" as any,
      cell: ({ getValue }) => getValue() === "none" ? "-" : getValue() },
    // ── Attribution ──
    { accessorKey: "tag", header: "Type",
      meta: { filterType: "multi-select", description: "How this order relates to Meta ads. Meta New = first-time customer via Meta. Meta Repeat = returning Meta-acquired customer. Meta Retargeted = existing customer converted by Meta. Meta Unmatched New/Repeat/Retargeted = UTM confirms Meta click but no statistical match. Non-Meta = online order with no Meta attribution. Non-Meta POS = in-store/POS order" },
      filterFn: "multiSelect" as any },
    // ── Campaign details ──
    { accessorKey: "campaign", header: "Campaign",
      meta: { maxWidth: "200px", filterType: "multi-select", description: "Meta campaign that drove this order" },
      filterFn: "multiSelect" as any,
      cell: ({ getValue }) => getValue() || "-" },
    { accessorKey: "adSet", header: "Ad Set",
      meta: { maxWidth: "180px", filterType: "multi-select", description: "Meta ad set that drove this order" },
      filterFn: "multiSelect" as any,
      cell: ({ getValue }) => getValue() || "-" },
    { accessorKey: "adName", header: "Ad",
      meta: { maxWidth: "180px", filterType: "multi-select", description: "Specific Meta ad creative that drove this order" },
      filterFn: "multiSelect" as any,
      cell: ({ getValue }) => getValue() || "-" },
    { accessorKey: "difference", header: "Difference",
      meta: { align: "right", description: "Gap between Shopify order values and Meta-reported conversion values for the same ad+day group. Positive = Shopify higher", calc: "(Shopify value − Meta value) ÷ Meta value × 100" },
      cell: ({ getValue }) => {
        const v = getValue() as number | null | undefined;
        if (v === null || v === undefined) return "-";
        return `${v > 0 ? "+" : ""}${v.toFixed(2)}%`;
      },
    },
    // ── Geography ──
    { accessorKey: "country", header: "Country",
      meta: { filterType: "multi-select", description: "Customer's shipping country" },
      filterFn: "multiSelect" as any,
      cell: ({ getValue }) => getValue() || "-" },
    { accessorKey: "city", header: "City",
      meta: { description: "Customer's shipping city" },
      cell: ({ getValue }) => getValue() || "-" },
    // ── Products ──
    { accessorKey: "lineItems", header: "Products",
      meta: { maxWidth: "200px", description: "Products purchased in this order" },
      cell: ({ getValue }) => getValue() || "-" },
    { accessorKey: "productSkus", header: "SKUs",
      meta: { maxWidth: "160px", description: "Product SKU codes in this order" },
      cell: ({ getValue }) => getValue() || "-" },
    { accessorKey: "productCollections", header: "Collections",
      meta: { maxWidth: "160px", filterType: "multi-select", description: "Shopify collections the ordered products belong to" },
      filterFn: "multiSelect" as any,
      cell: ({ getValue }) => getValue() || "-" },
    { accessorKey: "discountCodes", header: "Discount",
      meta: { filterType: "multi-select", description: "Discount or promo code applied to this order" },
      filterFn: "multiSelect" as any,
      cell: ({ getValue }) => getValue() || "-" },
    // ── Raw UTM (at the very end - rarely consulted) ──
    { accessorKey: "utm", header: "UTM",
      meta: { maxWidth: "300px", description: "Raw UTM parameters from the landing page URL when this order was placed" },
      cell: ({ getValue }) => {
        const v = getValue();
        if (!v) return "-";
        return <span style={{ fontFamily: "monospace", fontSize: "11px" }}>{v}</span>;
      },
    },
    // ── Attribution detail (moved to the very end - rarely the first thing consulted) ──
    { id: "confidence", header: "Confidence",
      meta: { filterType: "multi-select", description: "How confident the attribution match is. 100% = only possible match. Lower % = multiple candidate orders could have matched" },
      filterFn: "multiSelect" as any,
      accessorFn: (row) => {
        if (row.confidence === null || row.confidence === undefined) return "";
        if (row.confidence === 0) return "Unmatched";
        return `${row.confidence}%`;
      },
    },
    { accessorKey: "attributionSource", header: "Source",
      meta: { filterType: "multi-select", description: "How this order was attributed. UTM & Lucidly = both UTM and statistical matcher agree. UTM = UTM confirms Meta ad but no statistical match. Lucidly = statistical match only. Unattributed = neither" },
      filterFn: "multiSelect" as any,
      cell: ({ getValue }) => {
        const v = getValue();
        if (!v || v === "Unattributed") return "-";
        return v;
      },
    },
    { accessorKey: "method", header: "Method",
      meta: { filterType: "multi-select", description: "Attribution method used. Primary = exhaustive backtracking matcher. FAST = greedy fallback. UTM = attributed via UTM parameters" },
      filterFn: "multiSelect" as any,
      cell: ({ getValue }) => getValue() || "-" },
  ], [cs]);

  // Show ALL columns by default — the table is fit-content + horizontal
  // scroll, so the full set fits without truncation. Saved per-merchant
  // selection (via the "Save as Default" button in the column picker)
  // persists in localStorage and takes precedence over this default.
  const defaultVisibleColumns = useMemo(
    () => columns.map(c => (c as any).accessorKey || (c as any).id).filter(Boolean) as string[],
    [columns],
  );

  const fmtPrice = (v: number) => `${cs}${v.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;

  const footerRow = useMemo(() => {
    if (rows.length === 0) return undefined;
    const sum = (key: string) => rows.reduce((s, r) => s + (r[key] || 0), 0);
    const revenue = sum("revenue");
    const refunded = sum("totalRefunded");
    const netRev = sum("netRevenue");
    const confRows = rows.filter(r => r.confidence > 0);
    const avgConf = confRows.length > 0
      ? Math.round(confRows.reduce((s, r) => s + r.confidence, 0) / confRows.length)
      : 0;
    return {
      orderNumber: `${rows.length} orders`,
      createdAtISO: "",
      customerFirstName: "",
      customerLastName: "",
      customerEmail: "",
      orderCount: "",
      revenue: fmtPrice(revenue),
      netRevenue: fmtPrice(netRev),
      totalRefunded: refunded > 0 ? fmtPrice(refunded) : "-",
      refundStatus: "",
      tag: "",
      confidence: avgConf > 0 ? `${avgConf}% avg` : "",
      attributionSource: "",
      method: "",
      campaign: "", adSet: "", adName: "",
      difference: "",
      country: "", city: "",
      lineItems: "", productSkus: "", productCollections: "", discountCodes: "",
      utm: "",
    };
  }, [rows, cs]);

  return (
    <Card>
      <BlockStack gap="300">
        <Text as="h2" variant="headingLg">Order Explorer</Text>
        <Text as="p" variant="bodySm" tone="subdued">
          Every Shopify order in the selected period, enriched with Meta attribution data.
          Each order is tagged - <strong>Meta New</strong> (first-ever purchase via Meta),
          {" "}<strong>Meta Repeat</strong> (returning Meta-acquired customer),
          {" "}<strong>Meta Retargeted</strong> (existing customer converted by a Meta ad),
          {" "}<strong>Meta Unmatched New/Repeat/Retargeted</strong> (UTM confirms Meta click but no statistical match),
          {" "}<strong>Non-Meta</strong> (online order with no Meta attribution),
          or <strong>Non-Meta POS</strong> (in-store/POS order).
        </Text>
        <InlineStack gap="400">
          <Select label="Customer Type" options={tagOptions} value={tagFilter} onChange={onTagChange} />
          <Select label="Campaign" options={campaignOptions} value={campaignFilter} onChange={onCampaignChange} />
        </InlineStack>
        <InteractiveTable
          columns={columns}
          data={rows}
          defaultVisibleColumns={defaultVisibleColumns}
          tableId="orders"
          footerRow={footerRow}
          fitContentColumns
          enableDownload
          downloadFilename="order-explorer"
          initialSorting={[{ id: "createdAtISO", desc: true }]}
          initialRowLimit={20}
        />
      </BlockStack>
    </Card>
  );
}
