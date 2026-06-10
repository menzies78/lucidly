import type { MetaFunction } from "@remix-run/node";

export const meta: MetaFunction = () => [
  { title: "Lucidly — Terms of Service & Data Processing Agreement" },
  { name: "robots", content: "index" },
];

// Public, unauthenticated route. The merchant-facing agreement that governs use
// of the app, including the data-processing (DPA) terms required under UK GDPR
// Article 28 (Lucidly as processor, merchant as controller). Linked from the
// in-app footer alongside the Privacy Policy; acceptance occurs on install/use.
export default function Terms() {
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
      <h1 style={{ fontSize: 32, marginBottom: 4 }}>
        Terms of Service &amp; Data Processing Agreement
      </h1>
      <p style={{ color: "#666", marginTop: 0 }}>Last updated: {updated}</p>

      <p>
        These terms govern a merchant's use of Lucidly (the "App"), operated by{" "}
        <strong>Andrew Menzies</strong>, an individual based in the United
        Kingdom ("we", "us"). By installing or using the App, the merchant
        ("you") agrees to these terms, including the Data Processing Agreement in
        section&nbsp;5. Contact:{" "}
        <a href="mailto:see.lucidly@gmail.com">see.lucidly@gmail.com</a>.
      </p>

      <h2>1. The service</h2>
      <p>
        Lucidly reads your Shopify order, customer, and product data and your
        connected Meta advertising data to provide advertising attribution and
        customer analytics. The App is provided on an "as is" basis. We aim for
        accuracy but advertising attribution is inherently estimative; figures
        are for guidance and should not be relied on as the sole basis for
        financial decisions.
      </p>

      <h2>2. Your responsibilities</h2>
      <p>
        You are responsible for having a lawful basis and the necessary notices
        and consents to allow us to process your customers' personal data on
        your behalf as described here and in our{" "}
        <a href="/privacy">Privacy Policy</a>.
      </p>

      <h2>3. Fees</h2>
      <p>
        The App is currently provided free of charge during its beta period. If
        paid plans are introduced, we will give notice and you may choose
        whether to continue.
      </p>

      <h2>4. Termination</h2>
      <p>
        You may stop using the App at any time by uninstalling it from your
        Shopify admin. On uninstall, your data is deleted as described in
        section&nbsp;5.6.
      </p>

      <h2>5. Data Processing Agreement</h2>
      <p>
        This section forms a data-processing agreement under UK GDPR
        Article&nbsp;28 and applies where we process personal data of your
        customers on your behalf.
      </p>
      <p>
        <strong>5.1 Roles.</strong> You are the data controller of your
        customers' personal data. We are the data processor, acting only on your
        documented instructions (which include your use of the App's features).
      </p>
      <p>
        <strong>5.2 Scope.</strong> We process customer name, hashed email
        address, order history, and approximate location, solely to provide
        attribution and analytics. We do not process this data for any other
        purpose and do not sell it.
      </p>
      <p>
        <strong>5.3 Confidentiality.</strong> Access is limited to the operator
        and is subject to a duty of confidentiality.
      </p>
      <p>
        <strong>5.4 Security.</strong> We apply appropriate technical and
        organisational measures, including encryption in transit (TLS) and at
        rest, access controls, and storage of email addresses only as one-way
        hashes.
      </p>
      <p>
        <strong>5.5 Sub-processors.</strong> We use Fly.io (hosting/storage),
        Meta Platforms (advertising data source), and Anthropic (insight
        generation on aggregated, non-identifying metrics). We will give notice
        of any new sub-processor that handles personal data.
      </p>
      <p>
        <strong>5.6 Data subject requests &amp; deletion.</strong> We assist you
        in responding to data-subject requests and implement Shopify's mandatory
        GDPR webhooks (<code>customers/data_request</code>,{" "}
        <code>customers/redact</code>, <code>shop/redact</code>). On uninstall or
        on an erasure request, the relevant data is deleted.
      </p>
      <p>
        <strong>5.7 Breach notification.</strong> We will notify you without
        undue delay after becoming aware of a personal-data breach affecting
        your data.
      </p>
      <p>
        <strong>5.8 International transfers.</strong> Where a sub-processor
        processes data outside the UK, it does so under an appropriate transfer
        mechanism.
      </p>

      <h2>6. Liability</h2>
      <p>
        To the extent permitted by law, our liability arising from the App is
        limited, and we are not liable for indirect or consequential loss, or
        for decisions made in reliance on the App's estimative figures.
      </p>

      <h2>7. Governing law</h2>
      <p>
        These terms are governed by the laws of England and Wales.
      </p>

      <h2>8. Changes</h2>
      <p>
        We may update these terms as the App evolves. The "last updated" date
        reflects the most recent change; continued use after a change
        constitutes acceptance.
      </p>
    </main>
  );
}
