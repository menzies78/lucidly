import { json } from "@remix-run/node";
import { useLoaderData, useActionData, useSubmit, useNavigation } from "@remix-run/react";
import {
  Page, Card, Text, BlockStack, InlineStack, Banner, Button,
  TextField, Badge, Divider, Box,
} from "@shopify/polaris";
import { authenticate } from "../shopify.server";
import db from "../db.server";
import { useState, useCallback, useMemo } from "react";
import { auditUtms, pushUtms, pushUtmsToAds, saveUtmTemplate } from "../services/utmManager.server";
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
    utmTemplate: shop?.utmTemplate || "",
    utmLastAudit: shop?.utmLastAudit?.toISOString() || null,
    utmAdsTotal: shop?.utmAdsTotal || 0,
    utmAdsWithTags: shop?.utmAdsWithTags || 0,
    utmAdsMissing: shop?.utmAdsMissing || 0,
    utmAdsFixed: shop?.utmAdsFixed || 0,
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

  if (actionType === "dryRun") {
    const activeOnly = formData.get("activeOnly") !== "false";
    const result = await pushUtms(shopDomain, { activeOnly, dryRun: true });
    return json({ actionType: "dryRun", result });
  }

  if (actionType === "push") {
    const activeOnly = formData.get("activeOnly") !== "false";
    const result = await pushUtms(shopDomain, { activeOnly });
    return json({ actionType: "push", result });
  }

  if (actionType === "pushEdits") {
    const editsJson = formData.get("edits");
    const edits = JSON.parse(editsJson);
    const result = await pushUtmsToAds(shopDomain, edits);
    return json({ actionType: "pushEdits", result });
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

export default function UtmManagement() {
  const data = useLoaderData<typeof loader>();
  const actionData = useActionData<typeof action>();
  const submit = useSubmit();
  const navigation = useNavigation();
  const isLoading = navigation.state === "submitting";

  const [template, setTemplate] = useState(data.utmTemplate);
  const [editedUtms, setEditedUtms] = useState<Record<string, string>>({});

  const auditResult = actionData?.actionType === "audit" ? actionData.result : null;
  const pushResult = actionData?.actionType === "push" ? actionData.result : null;
  const pushEditsResult = actionData?.actionType === "pushEdits" ? actionData.result : null;

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

  const handlePush = useCallback(() => {
    const formData = new FormData();
    formData.set("action", "push");
    formData.set("activeOnly", "true");
    submit(formData, { method: "post" });
  }, [submit]);

  const handlePushEdits = useCallback(() => {
    const edits = Object.entries(editedUtms)
      .filter(([, tags]) => tags.trim() !== "")
      .map(([adId, urlTags]) => ({ adId, urlTags }));
    if (edits.length === 0) return;
    const formData = new FormData();
    formData.set("action", "pushEdits");
    formData.set("edits", JSON.stringify(edits));
    submit(formData, { method: "post" });
  }, [submit, editedUtms]);

  const handleFillMissing = useCallback(() => {
    if (!auditResult?.adList || !template) return;
    const updates: Record<string, string> = {};
    for (const ad of auditResult.adList) {
      if (!ad.urlTags) {
        updates[ad.adId] = template;
      }
    }
    setEditedUtms(prev => ({ ...prev, ...updates }));
  }, [auditResult, template]);

  const editCount = Object.keys(editedUtms).length;

  // Build table data from audit results
  const adRows = useMemo(() => {
    if (!auditResult?.adList) return [];
    return auditResult.adList.map(ad => ({
      ...ad,
      currentUtm: ad.urlTags,
      editedUtm: editedUtms[ad.adId] ?? null,
    }));
  }, [auditResult, editedUtms]);

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
    { id: "utm", header: "UTM Parameters",
      meta: { description: "Current URL tracking parameters on this ad's creative. Paste new UTMs to update." },
      accessorFn: (row) => row.editedUtm ?? row.currentUtm ?? "",
      cell: ({ row }) => {
        const adId = row.original.adId;
        const current = row.original.currentUtm || "";
        const edited = editedUtms[adId];
        const value = edited ?? current;
        const isEdited = edited !== undefined && edited !== current;
        return (
          <div style={{ minWidth: "300px" }}>
            <input
              type="text"
              value={value}
              onChange={(e) => {
                const val = e.target.value;
                setEditedUtms(prev => {
                  if (val === current) {
                    const next = { ...prev };
                    delete next[adId];
                    return next;
                  }
                  return { ...prev, [adId]: val };
                });
              }}
              style={{
                width: "100%",
                padding: "4px 8px",
                fontSize: "12px",
                fontFamily: "monospace",
                border: `1px solid ${isEdited ? "#f0b849" : current ? "#c9cccf" : "#d72c0d"}`,
                borderRadius: "4px",
                background: isEdited ? "#fef8e8" : current ? "#fff" : "#fef0f0",
                outline: "none",
              }}
              placeholder="Paste UTM parameters..."
            />
          </div>
        );
      },
    },
  ], [editedUtms]);

  const defaultVisibleColumns = useMemo(() => [
    "campaignName", "adsetName", "adName", "effectiveStatus", "utm",
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

        {/* UTM Template */}
        <Card>
          <BlockStack gap="300">
            <Text as="h2" variant="headingMd">UTM Template</Text>
            <Text as="p" variant="bodySm" tone="subdued">
              Applied when filling missing UTMs. Macros: {"{{campaign.name}}"}, {"{{adset.name}}"}, {"{{ad.name}}"}, {"{{ad.id}}"}
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
              <Button onClick={handleSaveTemplate} disabled={isLoading || template === data.utmTemplate}>
                Save Template
              </Button>
              {template !== data.utmTemplate && (
                <Badge tone="attention">Unsaved changes</Badge>
              )}
            </InlineStack>
          </BlockStack>
        </Card>

        {/* Actions */}
        <Card>
          <BlockStack gap="300">
            <InlineStack gap="300" align="start">
              <Button variant="primary" onClick={handleAudit} loading={isLoading && !pushEditsResult} disabled={isLoading}>
                {auditResult ? "Re-run Audit" : "Run Audit"}
              </Button>
              {auditResult && template && (
                <Button onClick={handleFillMissing} disabled={isLoading}>
                  Fill Missing with Template
                </Button>
              )}
              {auditResult && (
                <Button onClick={handlePush} disabled={isLoading || data.utmAdsMissing === 0}>
                  Apply Template to All Missing ({data.utmAdsMissing})
                </Button>
              )}
              {editCount > 0 && (
                <Button variant="primary" tone="critical" onClick={handlePushEdits} loading={isLoading} disabled={isLoading}>
                  Upload {editCount} Edit{editCount !== 1 ? "s" : ""} to Meta
                </Button>
              )}
            </InlineStack>

            {pushResult && (
              <Banner tone={pushResult.failed === 0 ? "success" : "warning"}>
                <p>
                  {pushResult.fixed} ads updated.
                  {pushResult.failed > 0 && <> {pushResult.failed} failed (likely DPA creatives — edit manually in Ads Manager).</>}
                </p>
              </Banner>
            )}
            {pushEditsResult && (
              <Banner tone={pushEditsResult.failed === 0 ? "success" : "warning"}>
                <p>
                  {pushEditsResult.fixed} ads updated.
                  {pushEditsResult.failed > 0 && <> {pushEditsResult.failed} failed.</>}
                  {pushEditsResult.skipped > 0 && <> {pushEditsResult.skipped} skipped (no story ID).</>}
                </p>
              </Banner>
            )}
          </BlockStack>
        </Card>

        {/* Ad Table */}
        {auditResult?.adList && (
          <Card>
            <BlockStack gap="300">
              <Text as="h2" variant="headingMd">
                All Ads ({auditResult.adList.length})
                {editCount > 0 && <Badge tone="attention">{editCount} pending edit{editCount !== 1 ? "s" : ""}</Badge>}
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
              <Text as="h2" variant="headingMd">Audit Detail</Text>

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
