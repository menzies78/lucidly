import { useState, useEffect, useCallback } from "react";
import { Card, Text, BlockStack, Button, Spinner } from "@shopify/polaris";
import { useSubmit, useActionData, useRevalidator } from "@remix-run/react";

// ── Types ──

interface Observation {
  type: "positive" | "negative" | "warning" | "opportunity";
  title: string;
  body: string;
  priority?: number;
}

interface Action {
  title: string;
  body: string;
  impact: "high" | "medium" | "low";
  type: "grow" | "save";
}

interface AiInsightsPanelProps {
  pageKey: string;
  cachedInsights: { observations: Observation[]; actions: Action[] } | null;
  generatedAt: string | null;
  isStale: boolean;
  currencySymbol: string;
}

// ── Styles ──

const TYPE_STYLES: Record<string, { border: string; bg: string; icon: string; label: string }> = {
  positive:    { border: "#10B981", bg: "#ECFDF5", icon: "▲", label: "Positive" },
  negative:    { border: "#EF4444", bg: "#FEF2F2", icon: "▼", label: "Issue" },
  warning:     { border: "#F59E0B", bg: "#FFFBEB", icon: "●", label: "Warning" },
  opportunity: { border: "#7C3AED", bg: "#F5F3FF", icon: "★", label: "Opportunity" },
};

const IMPACT_COLORS: Record<string, string> = {
  high:   "#EF4444",
  medium: "#F59E0B",
  low:    "#6B7280",
};

// ── Default prompts (must match aiAnalysis.server.js) ──

const DEFAULT_SYSTEM_PROMPT = `You are a senior performance marketing analyst embedded in Lucidly, a Meta Ads attribution app for Shopify merchants. Your job is to analyse the merchant's data and produce ACTIONABLE, SPECIFIC insights.

Rules:
- Be blunt and direct. No fluff, no generic advice.
- Use SPECIFIC numbers from the data. "Your CPA is 43" (with the provided currency symbol) not "Your CPA is high."
- Every observation must reference actual data points.
- Every action must be concrete enough to execute TODAY.
- Think about two things: how to MAKE more money, and how to SAVE money.
- Consider new vs existing customers separately — they have very different economics.
- Flag anomalies, trends, and opportunities others would miss.
- If comparing periods, quantify the change.
- Currency symbol is provided — use it.

Respond with valid JSON only, no markdown, no code fences:
{
  "observations": [
    {
      "type": "positive" | "negative" | "warning" | "opportunity",
      "title": "Short headline (max 80 chars)",
      "body": "1-2 sentences with specific numbers from the data",
      "priority": 1-5 (5 = most important)
    }
  ],
  "actions": [
    {
      "title": "Specific actionable step",
      "body": "Why this matters and what to do. Reference the data.",
      "impact": "high" | "medium" | "low",
      "type": "grow" | "save"
    }
  ]
}

Return 4-6 observations and 3-5 actions. Prioritise the most impactful insights.`;

const DEFAULT_PAGE_PROMPTS: Record<string, string> = {
  campaigns: `Focus on:
1. Which campaigns are efficient vs wasteful? (Compare ROAS, CPA, new customer CPA)
2. Ad fatigue signals (high frequency, old campaigns, declining performance)
3. Funnel drop-offs (ATC → checkout → purchase rates)
4. Period-over-period trends (if comparison data available)
5. Platform/placement efficiency
6. LTV:CAC ratios — are acquired customers profitable long-term?
7. Which campaigns should be scaled vs paused?`,
  customers: `Focus on:
1. Meta customer quality — is LTV:CAC > 3x? If not, what's the path?
2. New customer economics — CPA vs what they're worth (LTV)
3. Repeat rate comparison: Meta-acquired vs organic. Are Meta customers coming back?
4. Payback period — how many orders to recoup acquisition cost?
5. Customer journey — first vs second purchase AOV, gap between purchases
6. Demographic performance — which age/gender segments are most valuable?
7. Retention opportunities — reorder within 90 days rate, median time to 2nd purchase`,
  products: `Focus on:
1. Gateway products — which products acquire new customers? Are they the right ones to advertise?
2. Meta vs organic product mix — is Meta driving the right products?
3. Refund risk — which products have high refund rates when acquired via Meta?
4. Basket analysis — items per basket, cross-sell opportunities
5. Product purchase flows — what do customers buy first, then second?
6. Revenue concentration — is revenue too dependent on a few products?
7. Cost-effectiveness — which products generate Meta revenue efficiently?`,
  geo: `Focus on:
1. Country efficiency — which countries have best/worst ROAS and CPA?
2. Spend allocation — is spend proportional to return? Where are the mismatches?
3. New customer acquisition by country — where are new customers cheapest?
4. Untapped markets — countries with Shopify orders but zero Meta spend
5. Concentration risk — is spend too concentrated in one country?
6. Geo-specific campaign performance — any campaigns underperforming in specific countries?
7. Expansion opportunities — data-backed recommendation for which country to expand into`,
};

// ── SessionStorage helpers for cross-navigation persistence ──

const STORAGE_KEY = "ai_insight_tasks";
const PROMPT_STORAGE_PREFIX = "ai_prompt_";

function getStoredTasks(): Record<string, { taskId: string; startedAt: number }> {
  try {
    return JSON.parse(sessionStorage.getItem(STORAGE_KEY) || "{}");
  } catch { return {}; }
}

function storeTask(pageKey: string, taskId: string) {
  try {
    const tasks = getStoredTasks();
    tasks[pageKey] = { taskId, startedAt: Date.now() };
    sessionStorage.setItem(STORAGE_KEY, JSON.stringify(tasks));
  } catch {}
}

function clearStoredTask(pageKey: string) {
  try {
    const tasks = getStoredTasks();
    delete tasks[pageKey];
    sessionStorage.setItem(STORAGE_KEY, JSON.stringify(tasks));
  } catch {}
}

function getStoredTask(pageKey: string): string | null {
  const tasks = getStoredTasks();
  const entry = tasks[pageKey];
  if (!entry) return null;
  if (Date.now() - entry.startedAt > 120000) {
    clearStoredTask(pageKey);
    return null;
  }
  return entry.taskId;
}

// Prompt persistence (localStorage — survives sessions)
function getSavedPrompts(pageKey: string): { system: string; page: string } | null {
  try {
    const raw = localStorage.getItem(PROMPT_STORAGE_PREFIX + pageKey);
    return raw ? JSON.parse(raw) : null;
  } catch { return null; }
}

function savePrompts(pageKey: string, system: string, page: string) {
  try {
    localStorage.setItem(PROMPT_STORAGE_PREFIX + pageKey, JSON.stringify({ system, page }));
  } catch {}
}

function clearSavedPrompts(pageKey: string) {
  try {
    localStorage.removeItem(PROMPT_STORAGE_PREFIX + pageKey);
  } catch {}
}

// ── Component ──

export default function AiInsightsPanel({
  pageKey,
  cachedInsights,
  generatedAt,
  isStale,
  currencySymbol,
}: AiInsightsPanelProps) {
  const submit = useSubmit();
  const actionData = useActionData<any>();
  const revalidator = useRevalidator();

  const [generating, setGenerating] = useState(() => !!getStoredTask(pageKey));
  const [error, setError] = useState<string | null>(null);
  const [taskId, setTaskId] = useState<string | null>(() => getStoredTask(pageKey));

  // Prompt editor state
  const [promptOpen, setPromptOpen] = useState(false);
  const [systemPrompt, setSystemPrompt] = useState(() => {
    const saved = getSavedPrompts(pageKey);
    return saved?.system || DEFAULT_SYSTEM_PROMPT;
  });
  const [pagePrompt, setPagePrompt] = useState(() => {
    const saved = getSavedPrompts(pageKey);
    return saved?.page || DEFAULT_PAGE_PROMPTS[pageKey] || "";
  });
  const [promptDirty, setPromptDirty] = useState(false);

  const isCustom = systemPrompt !== DEFAULT_SYSTEM_PROMPT || pagePrompt !== (DEFAULT_PAGE_PROMPTS[pageKey] || "");

  // Poll progress
  useEffect(() => {
    if (!taskId || !generating) return;
    let cancelled = false;

    const poll = async () => {
      try {
        const taskParam = taskId.split(":").slice(0, 2).join(":");
        const res = await fetch(`/app/api/progress?task=${encodeURIComponent(taskParam)}`, { credentials: "include" });
        if (!res.ok) {
          if (!cancelled) setTimeout(poll, 3000);
          return;
        }
        const data = await res.json();
        const progress = data.progress;

        if (progress?.status === "complete") {
          setGenerating(false);
          setTaskId(null);
          setError(null);
          clearStoredTask(pageKey);
          revalidator.revalidate();
          return;
        }
        if (progress?.status === "error") {
          setGenerating(false);
          setTaskId(null);
          setError(progress.error || "Failed to generate insights");
          clearStoredTask(pageKey);
          return;
        }
      } catch {}

      if (!cancelled) {
        setTimeout(poll, 3000);
      }
    };

    const timer = setTimeout(poll, 1500);
    return () => { cancelled = true; clearTimeout(timer); };
  }, [taskId, generating, revalidator, pageKey]);

  // Handle action response
  useEffect(() => {
    if (actionData?.aiTaskId) {
      setTaskId(actionData.aiTaskId);
      setGenerating(true);
      setError(null);
      storeTask(pageKey, actionData.aiTaskId);
    }
    if (actionData?.aiError) {
      setError(actionData.aiError);
      setGenerating(false);
      clearStoredTask(pageKey);
    }
  }, [actionData, pageKey]);

  const handleGenerate = useCallback(() => {
    setGenerating(true);
    setError(null);
    const formData = new FormData();
    formData.set("actionType", "generateInsights");
    formData.set("pageKey", pageKey);
    // Pass custom prompts if they differ from defaults
    if (isCustom) {
      formData.set("customSystemPrompt", systemPrompt);
      formData.set("customPagePrompt", pagePrompt);
    }
    submit(formData, { method: "post" });
  }, [pageKey, submit, isCustom, systemPrompt, pagePrompt]);

  const handleSavePrompts = useCallback(() => {
    savePrompts(pageKey, systemPrompt, pagePrompt);
    setPromptDirty(false);
  }, [pageKey, systemPrompt, pagePrompt]);

  const handleResetPrompts = useCallback(() => {
    setSystemPrompt(DEFAULT_SYSTEM_PROMPT);
    setPagePrompt(DEFAULT_PAGE_PROMPTS[pageKey] || "");
    clearSavedPrompts(pageKey);
    setPromptDirty(false);
  }, [pageKey]);

  const insights = cachedInsights;
  const hasInsights = insights && insights.observations?.length > 0;

  const timeAgo = generatedAt ? (() => {
    const ms = Date.now() - new Date(generatedAt).getTime();
    const mins = Math.floor(ms / 60000);
    if (mins < 1) return "just now";
    if (mins < 60) return `${mins}m ago`;
    const hours = Math.floor(mins / 60);
    if (hours < 24) return `${hours}h ago`;
    return `${Math.floor(hours / 24)}d ago`;
  })() : null;

  const textareaStyle: React.CSSProperties = {
    width: "100%", minHeight: "120px", padding: "10px 12px",
    fontSize: "12px", fontFamily: "monospace", lineHeight: "1.5",
    border: "1px solid #D1D5DB", borderRadius: "8px",
    resize: "vertical", background: "#FAFAFA", color: "#1F2937",
  };

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "16px" }}>
      <Card>
        <BlockStack gap="300">
          {/* Header */}
          <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", flexWrap: "wrap", gap: "8px" }}>
            <div style={{ display: "flex", alignItems: "center", gap: "10px" }}>
              <div style={{
                width: "28px", height: "28px", borderRadius: "8px", background: "linear-gradient(135deg, #7C3AED, #A78BFA)",
                display: "flex", alignItems: "center", justifyContent: "center", color: "#fff", fontSize: "14px", fontWeight: 700,
              }}>
                AI
              </div>
              <div>
                <Text as="h2" variant="headingSm">AI Insights</Text>
                {timeAgo && !isStale && !generating && (
                  <span style={{ fontSize: "11px", color: "#9CA3AF" }}>Updated {timeAgo}</span>
                )}
                {isStale && hasInsights && !generating && (
                  <span style={{ fontSize: "11px", color: "#F59E0B" }}>Data changed — refresh for latest</span>
                )}
              </div>
            </div>
            <div style={{ display: "flex", gap: "8px", alignItems: "center" }}>
              {isCustom && (
                <span style={{ fontSize: "10px", fontWeight: 600, color: "#7C3AED", background: "#F5F3FF", padding: "2px 8px", borderRadius: "4px" }}>
                  CUSTOM PROMPT
                </span>
              )}
              {generating ? (
                <div style={{ display: "flex", alignItems: "center", gap: "8px" }}>
                  <Spinner size="small" />
                  <span style={{ fontSize: "12px", color: "#6B7280" }}>Analysing...</span>
                </div>
              ) : (
                <Button size="slim" onClick={handleGenerate}>
                  {hasInsights ? "Refresh" : "Generate Insights"}
                </Button>
              )}
            </div>
          </div>

          {/* Error */}
          {error && (
            <div style={{ padding: "8px 12px", borderRadius: "8px", background: "#FEF2F2", color: "#DC2626", fontSize: "12px", fontWeight: 500 }}>
              {error}
            </div>
          )}

          {/* No insights yet */}
          {!hasInsights && !generating && !error && (
            <div style={{ padding: "20px 0", textAlign: "center" }}>
              <Text as="p" variant="bodySm" tone="subdued">
                Click "Generate Insights" to get AI-powered analysis of your data.
              </Text>
            </div>
          )}

          {/* Insights content */}
          {hasInsights && (
            <div style={{ display: "flex", gap: "20px", flexWrap: "wrap" }}>

              {/* Observations */}
              <div style={{ flex: "1 1 400px", minWidth: "300px" }}>
                <div style={{ fontSize: "11px", fontWeight: 600, color: "#6B7280", textTransform: "uppercase", letterSpacing: "0.5px", marginBottom: "8px" }}>
                  Observations
                </div>
                <div style={{ display: "flex", flexDirection: "column", gap: "6px" }}>
                  {insights!.observations.map((obs, i) => {
                    const s = TYPE_STYLES[obs.type] || TYPE_STYLES.warning;
                    return (
                      <div key={i} style={{
                        padding: "10px 12px", borderRadius: "8px",
                        borderLeft: `3px solid ${s.border}`, background: s.bg,
                      }}>
                        <div style={{ display: "flex", alignItems: "center", gap: "6px", marginBottom: "3px" }}>
                          <span style={{ fontSize: "12px", color: s.border }}>{s.icon}</span>
                          <span style={{ fontSize: "13px", fontWeight: 700, color: "#1F2937" }}>{obs.title}</span>
                        </div>
                        <div style={{ fontSize: "12px", color: "#4B5563", lineHeight: "1.5" }}>{obs.body}</div>
                      </div>
                    );
                  })}
                </div>
              </div>

              {/* Actions */}
              <div style={{ flex: "1 1 350px", minWidth: "280px" }}>
                <div style={{ fontSize: "11px", fontWeight: 600, color: "#6B7280", textTransform: "uppercase", letterSpacing: "0.5px", marginBottom: "8px" }}>
                  Recommended Actions
                </div>
                <div style={{ display: "flex", flexDirection: "column", gap: "6px" }}>
                  {insights!.actions.map((act, i) => (
                    <div key={i} style={{
                      padding: "10px 12px", borderRadius: "8px",
                      background: "#F9FAFB", border: "1px solid #E5E7EB",
                    }}>
                      <div style={{ display: "flex", alignItems: "center", gap: "8px", marginBottom: "3px" }}>
                        <span style={{
                          fontSize: "10px", fontWeight: 700, textTransform: "uppercase",
                          padding: "2px 6px", borderRadius: "4px",
                          background: act.type === "grow" ? "#ECFDF5" : "#FEF9C3",
                          color: act.type === "grow" ? "#059669" : "#854D0E",
                        }}>
                          {act.type === "grow" ? "GROW" : "SAVE"}
                        </span>
                        <span style={{ fontSize: "13px", fontWeight: 700, color: "#1F2937" }}>{act.title}</span>
                        <span style={{
                          marginLeft: "auto", fontSize: "10px", fontWeight: 600,
                          color: IMPACT_COLORS[act.impact] || "#6B7280",
                        }}>
                          {act.impact?.toUpperCase()} IMPACT
                        </span>
                      </div>
                      <div style={{ fontSize: "12px", color: "#4B5563", lineHeight: "1.5" }}>{act.body}</div>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          )}
        </BlockStack>
      </Card>

      {/* ── Prompt Editor (dev tool) ── */}
      <Card>
        <BlockStack gap="300">
          <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
            <button
              onClick={() => setPromptOpen(v => !v)}
              style={{
                background: "none", border: "none", cursor: "pointer", padding: 0,
                display: "flex", alignItems: "center", gap: "8px",
              }}
            >
              <span style={{ fontSize: "12px", color: "#6B7280", transition: "transform 0.15s", transform: promptOpen ? "rotate(90deg)" : "rotate(0)" }}>▶</span>
              <Text as="h2" variant="headingSm">Prompt Editor</Text>
              <span style={{ fontSize: "11px", color: "#9CA3AF" }}>(dev)</span>
            </button>
            {promptOpen && (
              <div style={{ display: "flex", gap: "8px" }}>
                {isCustom && (
                  <button
                    onClick={handleResetPrompts}
                    style={{
                      background: "none", border: "1px solid #D1D5DB", borderRadius: "6px",
                      padding: "4px 10px", fontSize: "11px", fontWeight: 500, color: "#6B7280", cursor: "pointer",
                    }}
                  >
                    Reset to defaults
                  </button>
                )}
                <button
                  onClick={handleSavePrompts}
                  disabled={!promptDirty}
                  style={{
                    background: promptDirty ? "#7C3AED" : "#E5E7EB", color: promptDirty ? "#fff" : "#9CA3AF",
                    border: "none", borderRadius: "6px",
                    padding: "4px 12px", fontSize: "11px", fontWeight: 600, cursor: promptDirty ? "pointer" : "default",
                  }}
                >
                  Save
                </button>
              </div>
            )}
          </div>

          {promptOpen && (
            <div style={{ display: "flex", flexDirection: "column", gap: "16px" }}>
              {/* System prompt */}
              <div>
                <div style={{ display: "flex", alignItems: "center", gap: "8px", marginBottom: "6px" }}>
                  <span style={{ fontSize: "11px", fontWeight: 600, color: "#6B7280", textTransform: "uppercase", letterSpacing: "0.5px" }}>
                    System Prompt
                  </span>
                  <span style={{ fontSize: "10px", color: "#9CA3AF" }}>Shared across all pages — personality, rules, output format</span>
                </div>
                <textarea
                  value={systemPrompt}
                  onChange={e => { setSystemPrompt(e.target.value); setPromptDirty(true); }}
                  style={{ ...textareaStyle, minHeight: "200px" }}
                />
              </div>

              {/* Page-specific prompt */}
              <div>
                <div style={{ display: "flex", alignItems: "center", gap: "8px", marginBottom: "6px" }}>
                  <span style={{ fontSize: "11px", fontWeight: 600, color: "#6B7280", textTransform: "uppercase", letterSpacing: "0.5px" }}>
                    Page Prompt — {pageKey}
                  </span>
                  <span style={{ fontSize: "10px", color: "#9CA3AF" }}>Focus areas for this specific page</span>
                </div>
                <textarea
                  value={pagePrompt}
                  onChange={e => { setPagePrompt(e.target.value); setPromptDirty(true); }}
                  style={textareaStyle}
                />
              </div>

              <div style={{ fontSize: "11px", color: "#9CA3AF", lineHeight: "1.5" }}>
                Changes are sent with the next "Generate Insights" click. Save persists to browser storage.
                The data payload is appended automatically — you only control the instructions above.
              </div>
            </div>
          )}
        </BlockStack>
      </Card>
    </div>
  );
}
