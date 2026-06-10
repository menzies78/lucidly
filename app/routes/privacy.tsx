import type { MetaFunction } from "@remix-run/node";

export const meta: MetaFunction = () => [
  { title: "Lucidly — Privacy Policy" },
  { name: "robots", content: "index" },
];

// Public, unauthenticated route. Reachable by merchants and Shopify reviewers
// without installing or logging in. Linked from the in-app footer and from the
// Shopify App Store listing. Plain HTML — no Polaris / App Bridge, since this
// renders outside the embedded admin context.
export default function Privacy() {
  const updated = "10 June 2026";
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
      <h1 style={{ fontSize: 32, marginBottom: 4 }}>Lucidly — Privacy Policy</h1>
      <p style={{ color: "#666", marginTop: 0 }}>Last updated: {updated}</p>

      <p>
        Lucidly is a Shopify app that provides advertising attribution and
        customer analytics. This policy explains what personal data Lucidly
        processes, why, who it is shared with, and the rights available to
        merchants and their customers under UK GDPR.
      </p>

      <h2>Who is responsible</h2>
      <p>
        Lucidly is operated by <strong>Andrew Menzies</strong>, an individual
        based in the United Kingdom. For data-protection questions or requests,
        contact: <a href="mailto:see.lucidly@gmail.com">see.lucidly@gmail.com</a>.
      </p>
      <p>
        When Lucidly processes a merchant's customer data, it acts as a{" "}
        <strong>data processor</strong> on behalf of the merchant (who is the
        data controller of their own customers' data). For a merchant's own
        account and contact details, Lucidly acts as a controller.
      </p>

      <h2>What data Lucidly processes</h2>
      <p>When a merchant installs Lucidly, it reads, via the Shopify API:</p>
      <ul>
        <li>
          <strong>Order data</strong> — order totals, dates, financial status,
          line items, products, discount codes, refunds, and the landing /
          referring URLs and UTM parameters attached to an order.
        </li>
        <li>
          <strong>Customer data</strong> — customer first and last name,
          email address (stored only as a one-way hash, never in plain text),
          order history, and approximate location (country / city) derived from
          the order.
        </li>
        <li>
          <strong>Product data</strong> — product titles, SKUs, and collections.
        </li>
        <li>
          <strong>Advertising data</strong> — performance metrics pulled from
          the merchant's connected Meta (Facebook/Instagram) ad account. This is
          ad-level data (spend, impressions, conversions); it does not contain
          Lucidly's end-customer personal data.
        </li>
      </ul>
      <p>
        Lucidly does <strong>not</strong> process customer phone numbers,
        payment-card details, or full billing addresses.
      </p>

      <h2>Why Lucidly processes it (purpose &amp; legal basis)</h2>
      <p>
        The data is used solely to provide the app's analytics to the merchant:
        matching orders to advertising campaigns (attribution), calculating
        customer lifetime value, repeat-purchase and acquisition reporting, and
        product-level performance. The lawful basis is the{" "}
        <strong>legitimate interests</strong> of the merchant in understanding
        their own sales and marketing performance, and the performance of the
        service the merchant has chosen to install. Lucidly does not sell
        personal data and does not use it for any purpose other than providing
        these analytics to the merchant.
      </p>

      <h2>Who it is shared with (sub-processors)</h2>
      <ul>
        <li>
          <strong>Fly.io</strong> — cloud hosting; stores the app's database.
        </li>
        <li>
          <strong>Meta Platforms</strong> — Lucidly reads advertising metrics
          from the merchant's own connected ad account. Customer personal data
          is not sent to Meta.
        </li>
        <li>
          <strong>Anthropic</strong> — used to generate written insight
          summaries. Only aggregated, non-identifying metrics are sent; customer
          names and email hashes are not.
        </li>
      </ul>

      <h2>How long it is kept</h2>
      <p>
        Data is retained for as long as the app is installed. When a merchant
        uninstalls Lucidly, or when Shopify sends a data-erasure request on
        behalf of a shop or customer, the associated data is deleted. Lucidly
        implements Shopify's mandatory GDPR webhooks (
        <code>customers/data_request</code>, <code>customers/redact</code>,{" "}
        <code>shop/redact</code>) to handle these requests automatically.
      </p>

      <h2>Your rights</h2>
      <p>
        Under UK GDPR, individuals have the right to access, correct, or request
        deletion of their personal data, and to object to processing. Merchant
        customers should direct such requests to the merchant (the data
        controller); the merchant can fulfil them through Shopify, which relays
        the request to Lucidly. Merchants can exercise their own rights, or ask
        any question about this policy, by emailing{" "}
        <a href="mailto:see.lucidly@gmail.com">see.lucidly@gmail.com</a>.
      </p>

      <h2>Changes to this policy</h2>
      <p>
        This policy may be updated as the app evolves. The "last updated" date
        above reflects the most recent change.
      </p>
    </main>
  );
}
