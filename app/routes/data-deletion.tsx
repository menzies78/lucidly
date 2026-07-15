import type { MetaFunction } from "@remix-run/node";

export const meta: MetaFunction = () => [
  { title: "Lucidly — Data Deletion" },
  { name: "robots", content: "index" },
];

// Public, unauthenticated route. This is the "Data Deletion Instructions URL"
// referenced in the Meta app settings. Meta requires every app that uses
// Facebook Login to provide either a data-deletion callback or a public page
// explaining how a person can have their data deleted. Lucidly stores no
// Facebook end-user personal data — the Meta connection only reads ad-account
// performance metrics — so a static instructions page is the accurate choice.
// Plain HTML — no Polaris / App Bridge, since this renders outside the embedded
// admin context (same pattern as privacy.tsx / terms.tsx).
export default function DataDeletion() {
  const updated = "8 July 2026";
  return (
    <main
      style={{
        maxWidth: 760,
        margin: "0 auto",
        padding: "48px 24px 96px",
        fontFamily:
          "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif",
        color: "#1a1a1a",
        lineHeight: 1.6,
        fontSize: 16,
      }}
    >
      <h1 style={{ fontSize: 32, marginBottom: 4 }}>Lucidly — Data Deletion</h1>
      <p style={{ color: "#666", marginTop: 0 }}>Last updated: {updated}</p>

      <p>
        This page explains how to request deletion of data that Lucidly holds in
        connection with your Meta (Facebook/Instagram) ad account. Lucidly is a
        Shopify app that provides advertising attribution and customer analytics
        for online merchants.
      </p>

      <h2>What Meta data Lucidly holds</h2>
      <p>
        When a merchant connects their Meta Ads account to Lucidly, Lucidly reads{" "}
        <strong>ad-account performance metrics only</strong> — spend,
        impressions, reach, and aggregate conversion figures for the merchant's
        own campaigns. Lucidly does <strong>not</strong> collect, store, or
        process any Facebook or Instagram end-user personal data, profile
        information, friend lists, or messages. The connection is authorised by
        the merchant and is limited to read-only access to their own advertising
        performance.
      </p>

      <h2>How to delete this data</h2>
      <p>You can have Lucidly delete the Meta-connected data at any time by:</p>
      <ul>
        <li>
          <strong>Disconnecting Meta inside the app</strong> — open Lucidly in
          your Shopify admin and click <em>Disconnect Meta</em> on the Health
          dashboard (next to the status pills), or open <em>Connect Meta Ads</em>{" "}
          and remove the account there. Disconnecting deletes the stored access
          token so Lucidly can no longer read your ad data.
        </li>
        <li>
          <strong>Uninstalling Lucidly</strong> — uninstalling the app from your
          Shopify admin removes Lucidly's access and triggers deletion of the
          associated data, including the stored Meta access token and imported
          advertising metrics.
        </li>
        <li>
          <strong>Emailing us directly</strong> — send a deletion request to{" "}
          <a href="mailto:see.lucidly@gmail.com">see.lucidly@gmail.com</a> from
          the email associated with your store, and we will delete the relevant
          data and confirm once complete.
        </li>
      </ul>

      <h2>Removing Lucidly from your Meta account</h2>
      <p>
        You can also revoke Lucidly's access from Meta's side at any time. In
        Facebook, go to{" "}
        <strong>Settings &amp; Privacy → Settings → Business Integrations</strong>{" "}
        (or, for a business, <strong>Business Settings → Integrations →
        Connected apps</strong>), select Lucidly, and remove it. This revokes the
        access token immediately.
      </p>

      <h2>Timescale</h2>
      <p>
        Requests are actioned promptly. Where a request is sent by email, we aim
        to complete deletion and confirm within 30 days, in line with UK GDPR.
      </p>

      <h2>Contact</h2>
      <p>
        Lucidly is operated by <strong>Andrew Menzies</strong>, an individual
        based in the United Kingdom. For any data-deletion request or question,
        contact{" "}
        <a href="mailto:see.lucidly@gmail.com">see.lucidly@gmail.com</a>. See
        also our <a href="/privacy">Privacy Policy</a>.
      </p>
    </main>
  );
}
