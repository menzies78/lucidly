// Shared ops-alerting primitive: send an operational alert email (via Resend)
// with per-key deduplication and re-nagging, plus a matching "recovered" notice.
//
// Why this exists: background health checks (e.g. the token watchdog) run on a
// tight interval. Without dedup they'd email on EVERY cycle while a fault
// persists — a self-inflicted spam incident. So we alert once per fault, re-nag
// on a slow cadence while it stays broken, and send a single recovery email when
// it clears.
//
// Delivery is best-effort: sendEmail no-ops silently when RESEND_API_KEY is
// unset and never throws, so a missing key degrades to console logging without
// breaking the caller (health checks must never crash the scheduler).

import { sendEmail } from "./email.server.js";

// While a fault persists, re-send the alert at most once per this window so a
// long-running outage keeps nagging (a single missed email shouldn't mean the
// on-call never hears about it) without flooding the inbox.
const RENAG_MS = 6 * 60 * 60 * 1000; // 6 hours

// Dedup state must survive Vite/Remix server module reloads, so it lives on
// globalThis (same pattern as the scheduler singletons and the perf caches).
function state() {
  if (!globalThis.__lucidlyOpsAlertState) {
    globalThis.__lucidlyOpsAlertState = new Map(); // key -> { firstAt, lastAt, count }
  }
  return globalThis.__lucidlyOpsAlertState;
}

function recipient() {
  return process.env.OPS_ALERT_EMAIL || null;
}

function wrap(title, severity, bodyHtml) {
  const colour = severity === "critical" ? "#DC2626" : severity === "warn" ? "#D97706" : "#7C3AED";
  return `
    <div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;max-width:600px;margin:0 auto;padding:24px;color:#111;">
      <div style="border-left:4px solid ${colour};padding-left:14px;margin-bottom:18px;">
        <div style="font-size:12px;letter-spacing:.08em;text-transform:uppercase;color:${colour};font-weight:700;">Lucidly Ops · ${severity}</div>
        <h1 style="font-size:20px;line-height:1.3;margin:6px 0 0;">${title}</h1>
      </div>
      <div style="font-size:14px;line-height:1.6;color:#374151;">${bodyHtml}</div>
      <p style="font-size:12px;color:#9CA3AF;margin-top:24px;">Sent by the Lucidly ops watchdog · ${new Date().toISOString()}</p>
    </div>`;
}

/**
 * Raise an alert for `key`. First occurrence emails immediately; subsequent
 * calls for the same still-active key are suppressed until RENAG_MS elapses.
 * Returns { sent, suppressed } so callers can log the decision.
 */
export async function alertOps(key, { subject, title, bodyHtml, bodyText, severity = "warn" } = {}) {
  const now = Date.now();
  const s = state();
  const prev = s.get(key);

  if (prev && now - prev.lastAt < RENAG_MS) {
    // Still inside the re-nag window — count it but stay quiet.
    prev.count += 1;
    return { sent: false, suppressed: true };
  }

  const count = prev ? prev.count + 1 : 1;
  s.set(key, { firstAt: prev?.firstAt || now, lastAt: now, count });

  const to = recipient();
  if (!to) {
    console.warn(`[opsAlert] ${severity.toUpperCase()} ${key}: ${title || subject} (OPS_ALERT_EMAIL unset — not emailed)`);
    return { sent: false, suppressed: false };
  }

  const firstSeen = prev ? new Date(prev.firstAt).toISOString() : new Date(now).toISOString();
  const html = wrap(title || subject, severity, `${bodyHtml || ""}<p style="color:#6B7280;font-size:12px;margin-top:16px;">First seen: ${firstSeen} · occurrences: ${count}</p>`);
  const res = await sendEmail({
    to,
    subject: `[Lucidly ${severity}] ${subject}`,
    html,
    text: bodyText || subject,
  });
  console.log(`[opsAlert] ${severity.toUpperCase()} ${key} -> ${res.ok ? "emailed" : res.skipped ? "skipped(no key)" : "send-failed"}`);
  return { sent: !!res.ok, suppressed: false };
}

/**
 * Clear a previously-raised alert for `key`. Emails a one-off "recovered" notice
 * only if the key was actually active (so we never send "recovered" for a fault
 * that never fired).
 */
export async function resolveOps(key, { subject, title, bodyHtml, bodyText } = {}) {
  const s = state();
  const prev = s.get(key);
  if (!prev) return { sent: false }; // was healthy — nothing to clear
  s.delete(key);

  const to = recipient();
  if (!to) {
    console.log(`[opsAlert] RESOLVED ${key} (OPS_ALERT_EMAIL unset — not emailed)`);
    return { sent: false };
  }
  const durMin = Math.round((Date.now() - prev.firstAt) / 60000);
  const html = wrap(title || subject, "info", `${bodyHtml || ""}<p style="color:#6B7280;font-size:12px;margin-top:16px;">Fault duration: ~${durMin} min · ${prev.count} occurrence(s)</p>`);
  const res = await sendEmail({
    to,
    subject: `[Lucidly recovered] ${subject}`,
    html,
    text: bodyText || subject,
  });
  console.log(`[opsAlert] RESOLVED ${key} -> ${res.ok ? "emailed" : "not-sent"}`);
  return { sent: !!res.ok };
}
