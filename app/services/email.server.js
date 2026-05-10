// Lightweight email sender used by the onboarding orchestrator to ping the
// merchant when their initial Shopify+Meta ingest has finished. The merchant
// almost certainly closed the tab while ingest was running (it takes 10-30
// minutes for a real Plus account), so we need an out-of-band trigger to
// pull them back in.
//
// Provider: Resend. 3k emails/month free tier - generous for our scale.
// Auth via RESEND_API_KEY in env. If the key is missing we no-op silently
// rather than crashing the orchestrator - email is "nice to have", losing
// it must NOT block ingest completion.
//
// We use the bare HTTPS API rather than @resend/node so we don't take on
// a dependency that 99% of the codebase doesn't use. fetch() is built into
// Node 18+.

const RESEND_ENDPOINT = "https://api.resend.com/emails";
const FROM_DEFAULT = process.env.LUCIDLY_FROM_EMAIL || "Lucidly <hello@lucidly.app>";

/**
 * Send an email. Returns { ok, skipped?, error? } - never throws. Callers
 * should fire-and-forget; failure to send must not bubble up into the
 * orchestrator path.
 */
export async function sendEmail({ to, subject, html, text }) {
  const apiKey = process.env.RESEND_API_KEY;
  if (!apiKey) {
    console.log(`[email] RESEND_API_KEY not set - skipping send to ${to}`);
    return { ok: false, skipped: true };
  }
  if (!to) {
    return { ok: false, error: "missing to" };
  }

  try {
    const res = await fetch(RESEND_ENDPOINT, {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        from: FROM_DEFAULT,
        to: Array.isArray(to) ? to : [to],
        subject,
        html: html || undefined,
        text: text || undefined,
      }),
    });
    if (!res.ok) {
      const body = await res.text().catch(() => "");
      console.warn(`[email] Resend ${res.status}: ${body.slice(0, 300)}`);
      return { ok: false, error: `resend ${res.status}` };
    }
    const data = await res.json().catch(() => ({}));
    console.log(`[email] sent to ${to} id=${data.id || "?"}`);
    return { ok: true, id: data.id };
  } catch (err) {
    console.warn(`[email] send failed: ${err?.message || err}`);
    return { ok: false, error: err?.message || String(err) };
  }
}

/**
 * Onboarding completion email. Includes a deep link back into the app so the
 * merchant can return to a fully-loaded dashboard rather than a "Setting up"
 * spinner.
 */
export async function sendOnboardingCompleteEmail({ to, shopDomain, dashboardUrl }) {
  const url = dashboardUrl || `https://${shopDomain}/admin/apps/lucidly`;
  const subject = "Your Lucidly dashboard is ready";
  const html = `
    <div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;max-width:560px;margin:0 auto;padding:32px 24px;color:#111;">
      <div style="text-align:center;margin-bottom:24px;">
        <div style="font-size:28px;font-weight:700;color:#7C3AED;">Lucidly</div>
      </div>
      <h1 style="font-size:22px;line-height:1.3;margin:0 0 12px;">Your dashboard is ready</h1>
      <p style="font-size:15px;line-height:1.6;color:#374151;margin:0 0 16px;">
        We've finished importing your Shopify orders and Meta Ads data, matching
        them, and building your customer benchmarks.
      </p>
      <p style="font-size:15px;line-height:1.6;color:#374151;margin:0 0 24px;">
        Open Lucidly to see your Customer Acquisition Health Score and your
        full attribution breakdown.
      </p>
      <div style="text-align:center;margin:28px 0;">
        <a href="${url}" style="background:#7C3AED;color:#fff;text-decoration:none;font-weight:600;padding:14px 28px;border-radius:8px;display:inline-block;font-size:15px;">
          Open Lucidly
        </a>
      </div>
      <p style="font-size:13px;line-height:1.5;color:#6B7280;margin:24px 0 0;">
        - The Lucidly team
      </p>
    </div>
  `;
  const text = `Your Lucidly dashboard is ready.\n\nWe've finished importing your Shopify orders and Meta Ads data, matching them, and building your customer benchmarks.\n\nOpen Lucidly: ${url}`;
  return sendEmail({ to, subject, html, text });
}
