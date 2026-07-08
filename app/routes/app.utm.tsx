// UTM Manager - REPORT-ONLY since 2026-07-08.
//
// Lucidly audits every ad's url_tags and tells the merchant exactly what to
// paste and where, but never writes to their ad account (OAuth scope is
// ads_read only - the old push-to-Meta flow was removed ahead of Meta App
// Review). The page therefore leans hard on making the merchant's manual
// action obvious: a copy-ready template, per-ad "what to do" chips, and a
// step-by-step Ads Manager walkthrough.

import { json } from "@remix-run/node";
import { useLoaderData, useActionData, useSubmit, useNavigation } from "@remix-run/react";
import {
  Page, Card, Text, BlockStack, InlineStack, Banner, Button,
  TextField, Badge, Box,
} from "@shopify/polaris";
import { authenticate } from "../shopify.server";
import db from "../db.server";
import { useState, useCallback, useMemo } from "react";
import { auditUtms, saveUtmTemplate } from "../services/utmManager.server";
import { type ColumnDef } from "@tanstack/react-table";
import InteractiveTable from "../components/InteractiveTable";
import ReportTabs from "../components/ReportTabs";

export const loader = async ({ request }) => {
  const { session } = await authenticate.admin(request);
  const shopDomain = session.shop;

  const shop = await db.shop.findUnique({ where: { shopDomain } });

  return json({
    shopDomain,
    metaConnected: !!shop?.metaAccessToken,
    // Numeric ad account id (no act_ prefix) for Ads Manager deep links.
    adAccountNumber: (shop?.metaAdAccountId || "").replace(/^act_/, ""),
    utmTemplate: shop?.utmTemplate || "",
    utmLastAudit: shop?.utmLastAudit?.toISOString() || null,
    utmAdsTotal: shop?.utmAdsTotal || 0,
    utmAdsWithTags: shop?.utmAdsWithTags || 0,
    utmAdsMissing: shop?.utmAdsMissing || 0,
  });
};

export const action = async ({ request }) => {
  const { session } = await authenticate.admin(request);
  const shopDomain = session.shop;
  const formData = await request.formData();
  const actionType = formData.get("action");

  if (actionType === "audit") {
    const result = await auditUtms(shopDomain);
    return json({ actionType: "audit", result });
  }

  if (actionType === "saveTemplate") {
    const template = formData.get("template");
    await saveUtmTemplate(shopDomain, template);
    return json({ actionType: "saveTemplate", success: true });
  }

  return json({ error: "Unknown action" });
};

const TILE_STYLE: React.CSSProperties = {
  background: "#f9fafb",
  border: "1px solid #e1e3e5",
  borderRadius: "8px",
  padding: "16px",
  minWidth: "140px",
  flex: "1",
  textAlign: "center",
};

function Tile({ label, value, tone }: { label: string; value: string | number; tone?: string }) {
  return (
    <div style={TILE_STYLE}>
      <div style={{ fontSize: "24px", fontWeight: 700, color: tone === "critical" ? "#d72c0d" : tone === "success" ? "#008060" : "#1a1a1a" }}>
        {value}
      </div>
      <div style={{ fontSize: "13px", color: "#6d7175", marginTop: "4px" }}>{label}</div>
    </div>
  );
}

const STATUS_LABELS: Record<string, string> = {
  ACTIVE: "Active",
  WITH_ISSUES: "Issues",
  PENDING_REVIEW: "In review",
  PAUSED: "Paused",
  ADSET_PAUSED: "Ad set paused",
  CAMPAIGN_PAUSED: "Campaign paused",
  DISAPPROVED: "Disapproved",
  ARCHIVED: "Archived",
};

const STATUS_TONES: Record<string, "success" | "attention" | "critical" | "info" | undefined> = {
  ACTIVE: "success",
  PAUSED: "attention",
  ADSET_PAUSED: "attention",
  CAMPAIGN_PAUSED: "attention",
  WITH_ISSUES: "critical",
  DISAPPROVED: "critical",
};

// Small copy-to-clipboard button with transient confirmation.
function CopyButton({ text, label = "Copy", size }: { text: string; label?: string; size?: "micro" | "slim" }) {
  const [copied, setCopied] = useState(false);
  const handleCopy = useCallback(() => {
    navigator.clipboard.writeText(text).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 1800);
    });
  }, [text]);
  return (
    <Button size={size || "slim"} onClick={handleCopy} disabled={!text}>
      {copied ? "Copied ✓" : label}
    </Button>
  );
}

export default function UtmManagement() {
  const data = useLoaderData<typeof loader>();
  const actionData = useActionData<typeof action>();
  const submit = useSubmit();
  const navigation = useNavigation();
  const isLoading = navigation.state === "submitting";

  const [template, setTemplate] = useState(data.utmTemplate);

  const auditResult = actionData?.actionType === "audit" ? actionData.result : null;
  // The template every ad should carry: the saved one, else what the audit recommends.
  const recommendedTemplate = data.utmTemplate || auditResult?.recommendedTemplate || "";

  const handleAudit = useCallback(() => {
    const formData = new FormData();
    formData.set("action", "audit");
    submit(formData, { method: "post" });
  }, [submit]);

  const handleSaveTemplate = useCallback(() => {
    const formData = new FormData();
    formData.set("action", "saveTemplate");
    formData.set("template", template);
    submit(formData, { method: "post" });
  }, [submit, template]);

  // Per-ad Ads Manager deep link - lands with the ad pre-selected.
  const adsManagerUrl = useCallback((adId: string) => {
    return `https://adsmanager.facebook.com/adsmanager/manage/ads?act=${data.adAccountNumber}&selected_ad_ids=${adId}`;
  }, [data.adAccountNumber]);

  // Build table data from audit results
  const adRows = useMemo(() => {
    if (!auditResult?.adList) return [];
    return auditResult.adList.map(ad => {
      const current = ad.urlTags || "";
      let utmState: "missing" | "differs" | "ok";
      if (!current) utmState = "missing";
      else if (recommendedTemplate && current !== recommendedTemplate) utmState = "differs";
      else utmState = "ok";
      return { ...ad, currentUtm: current, utmState };
    });
  }, [auditResult, recommendedTemplate]);

  const needsActionCount = useMemo(
    () => adRows.filter(r => r.utmState !== "ok" && r.effectiveStatus === "ACTIVE").length,
    [adRows],
  );

  const columns = useMemo<ColumnDef<any>[]>(() => [
    { accessorKey: "campaignName", header: "Campaign",
      meta: { filterType: "multi-select", description: "Meta campaign name" },
      filterFn: "multiSelect",
    },
    { accessorKey: "adsetName", header: "Ad Set",
      meta: { filterType: "multi-select", description: "Meta ad set name" },
      filterFn: "multiSelect",
    },
    { accessorKey: "adName", header: "Ad Name",
      meta: { description: "Meta ad name" },
    },
    { accessorKey: "effectiveStatus", header: "Status",
      meta: { filterType: "multi-select", description: "Current delivery status of this ad" },
      filterFn: "multiSelect",
      cell: ({ getValue }) => {
        const v = getValue() as string;
        return <Badge tone={STATUS_TONES[v]}>{STATUS_LABELS[v] || v}</Badge>;
      },
    },
    // What (if anything) the merchant needs to do for this ad - the headline
    // column now that fixes happen in Ads Manager, not here.
    { id: "utmState", header: "Action Needed",
      meta: { filterType: "multi-select", description: "Whether this ad's UTM parameters need fixing in Ads Manager" },
      filterFn: "multiSelect",
      accessorFn: (row) => row.utmState === "missing" ? "Add UTMs" : row.utmState === "differs" ? "Update UTMs" : "None",
      cell: ({ row }) => {
        const s = row.original.utmState;
        if (s === "missing") return <Badge tone="critical">Add UTMs</Badge>;
        if (s === "differs") return <Badge tone="attention">Update UTMs</Badge>;
        return <Badge tone="success">None</Badge>;
      },
    },
    { id: "utm", header: "Current UTM Parameters",
      meta: { description: "URL tracking parameters currently on this ad's creative. Fix flagged ads in Meta Ads Manager - see the guide above the table." },
      accessorFn: (row) => row.currentUtm || "",
      cell: ({ row }) => {
        const current = row.original.currentUtm;
        if (!current) {
          return <span style={{ color: "#d72c0d", fontSize: "12px", fontStyle: "italic" }}>None - this ad's traffic can't be tagged</span>;
        }
        return (
          <code style={{
            fontSize: "11px", fontFamily: "monospace", whiteSpace: "normal",
            wordBreak: "break-all", display: "block", maxWidth: "360px",
            color: row.original.utmState === "differs" ? "#916a00" : "#1a1a1a",
          }}>{current}</code>
        );
      },
    },
    // The manual-fix toolkit, per ad: copy the exact value + jump to the ad.
    { id: "fix", header: "Fix It",
      meta: { description: "Copy the correct UTM parameters, then open this ad in Ads Manager and paste them under URL parameters." },
      accessorFn: () => "",
      enableSorting: false,
      cell: ({ row }) => {
        if (row.original.utmState === "ok") return <span style={{ color: "#9ca3af", fontSize: "12px" }}>—</span>;
        return (
          <InlineStack gap="150" wrap={false}>
            <CopyButton text={recommendedTemplate} label="Copy UTMs" size="micro" />
            <Button
              size="micro"
              url={adsManagerUrl(row.original.adId)}
              target="_blank"
            >
              Open in Ads Manager
            </Button>
          </InlineStack>
        );
      },
    },
  ], [recommendedTemplate, adsManagerUrl]);

  const defaultVisibleColumns = useMemo(() => [
    "campaignName", "adsetName", "adName", "effectiveStatus", "utmState", "utm", "fix",
  ], []);

  if (!data.metaConnected) {
    return (
      <Page title="UTM Management">
        <ReportTabs />
        <Banner tone="warning">
          <p>Connect your Meta Ads account first to manage UTM parameters.</p>
        </Banner>
      </Page>
    );
  }

  const lastAudit = data.utmLastAudit
    ? new Date(data.utmLastAudit).toLocaleDateString("en-GB", { day: "numeric", month: "short", year: "numeric", hour: "2-digit", minute: "2-digit" })
    : "Never";

  const coveragePercent = data.utmAdsTotal > 0
    ? Math.round((data.utmAdsWithTags / data.utmAdsTotal) * 100)
    : 0;

  return (
    <Page title="UTM Management" fullWidth>
      <ReportTabs />
      {/* Top padding so the first row of tiles isn't visually flush against the
          ReportTabs strip. Matches the breathing room on other report tabs. */}
      <div style={{ paddingTop: 24 }} />
      <BlockStack gap="500">
        {/* Summary tiles */}
        <div style={{ display: "flex", gap: "12px", flexWrap: "wrap" }}>
          <Tile label="Delivering Ads" value={data.utmAdsTotal} />
          <Tile label="With UTMs" value={data.utmAdsWithTags} tone="success" />
          <Tile label="Missing UTMs" value={data.utmAdsMissing} tone={data.utmAdsMissing > 0 ? "critical" : "success"} />
          <Tile label="Coverage" value={`${coveragePercent}%`} tone={coveragePercent >= 90 ? "success" : "critical"} />
        </div>
        <Text as="p" variant="bodySm" tone="subdued">
          Showing delivering ads only. Last audit: {lastAudit}
        </Text>

        {/* Audit CTA */}
        <Card>
          <BlockStack gap="300">
            <Text as="h2" variant="headingLg">UTM Audit</Text>
            <Text as="p" variant="bodyMd" tone="subdued">
              Lucidly reads every ad in your Meta account and flags the ones with missing or
              inconsistent UTM parameters. Lucidly never edits your ads - you stay in full
              control and apply fixes yourself in Ads Manager, using the guide below.
            </Text>
            <InlineStack gap="300">
              <Button variant="primary" onClick={handleAudit} loading={isLoading} disabled={isLoading}>
                {auditResult ? "Re-run UTM tag audit on all Meta ads" : "Run UTM tag audit on all Meta ads"}
              </Button>
            </InlineStack>
          </BlockStack>
        </Card>

        {/* UTM Template - the thing to copy */}
        <Card>
          <BlockStack gap="300">
            <Text as="h2" variant="headingLg">Your UTM Template</Text>
            <Text as="p" variant="bodySm" tone="subdued">
              The parameters every ad should carry. Meta fills the macros automatically:
              {" "}{"{{campaign.name}}"}, {"{{adset.name}}"}, {"{{ad.name}}"}, {"{{ad.id}}"}
            </Text>
            <TextField
              label=""
              labelHidden
              value={template}
              onChange={setTemplate}
              autoComplete="off"
              monospaced
              multiline={2}
            />
            <InlineStack gap="300">
              <CopyButton text={template} label="Copy Template" />
              <Button onClick={handleSaveTemplate} disabled={isLoading || template === data.utmTemplate}>
                Save Template
              </Button>
              {template !== data.utmTemplate && (
                <Badge tone="attention">Unsaved changes</Badge>
              )}
            </InlineStack>
          </BlockStack>
        </Card>

        {/* How to apply - the manual walkthrough, front and centre */}
        <Card>
          <BlockStack gap="300">
            <Text as="h2" variant="headingLg">How to apply UTMs in Ads Manager</Text>
            {needsActionCount > 0 && (
              <Banner tone="warning">
                <p>
                  <strong>{needsActionCount} active ad{needsActionCount !== 1 ? "s" : ""} need{needsActionCount === 1 ? "s" : ""} fixing.</strong>{" "}
                  Ads without correct UTMs can't be link-verified, so their orders rely on
                  statistical matching alone.
                </p>
              </Banner>
            )}
            <BlockStack gap="200">
              <Text as="p" variant="bodyMd"><strong>1.</strong> Copy your UTM template above (or use the "Copy UTMs" button on any flagged ad below).</Text>
              <Text as="p" variant="bodyMd"><strong>2.</strong> In Meta Ads Manager, tick the flagged ads (you can select several at once) and click <strong>Edit</strong>.</Text>
              <Text as="p" variant="bodyMd"><strong>3.</strong> Scroll to <strong>Tracking → URL parameters</strong> and paste the template.</Text>
              <Text as="p" variant="bodyMd"><strong>4.</strong> Click <strong>Publish</strong>. Then re-run the audit here to confirm coverage.</Text>
            </BlockStack>
            <Text as="p" variant="bodySm" tone="subdued">
              Notes: editing several selected ads applies the same URL parameters to all of them.
              Meta may briefly re-review edited ads. Advantage+ catalog ads don't accept
              ad-level URL parameters - set them on the catalog / campaign settings instead.
            </Text>
          </BlockStack>
        </Card>

        {/* Ad Table */}
        {auditResult?.adList && (
          <Card>
            <BlockStack gap="300">
              <Text as="h2" variant="headingLg">
                All Ads ({auditResult.adList.length})
              </Text>
              <InteractiveTable
                tableId="utm-ads"
                data={adRows}
                columns={columns}
                defaultVisibleColumns={defaultVisibleColumns}
                defaultFilters={[{ id: "effectiveStatus", value: ["ACTIVE"] }]}
              />
            </BlockStack>
          </Card>
        )}

        {/* Audit detail: patterns & inconsistencies */}
        {auditResult && (
          <Card>
            <BlockStack gap="300">
              <Text as="h2" variant="headingLg">Audit Detail</Text>

              {auditResult.patterns?.length > 0 && (
                <BlockStack gap="200">
                  <Text as="h3" variant="headingSm">UTM patterns in use ({auditResult.patternCount})</Text>
                  {auditResult.patterns.slice(0, 5).map((p: any, i: number) => (
                    <Text key={i} as="p" variant="bodySm" fontWeight={i === 0 ? "bold" : "regular"}>
                      [{p.count} ads] <code style={{ fontSize: "11px" }}>{p.pattern}</code>
                    </Text>
                  ))}
                </BlockStack>
              )}

              {auditResult.mixedCampaigns?.length > 0 && (
                <Banner tone="warning">
                  <p>{auditResult.mixedCampaigns.length} campaign{auditResult.mixedCampaigns.length !== 1 ? "s have" : " has"} mixed UTM patterns.</p>
                </Banner>
              )}

              {/* Status breakdown */}
              <BlockStack gap="200">
                <Text as="h3" variant="headingSm">By Status</Text>
                <div style={{ display: "flex", gap: "8px", flexWrap: "wrap" }}>
                  {auditResult.statusBreakdown?.map((row: any) => (
                    <div key={row.status} style={{ ...TILE_STYLE, minWidth: "100px", padding: "10px" }}>
                      <div style={{ fontSize: "16px", fontWeight: 600 }}>{row.total}</div>
                      <div style={{ fontSize: "11px", color: "#6d7175" }}>{STATUS_LABELS[row.status] || row.status}</div>
                      {row.noUtm > 0 && <div style={{ fontSize: "11px", color: "#d72c0d" }}>{row.noUtm} missing</div>}
                    </div>
                  ))}
                </div>
              </BlockStack>
            </BlockStack>
          </Card>
        )}
      </BlockStack>
    </Page>
  );
}
