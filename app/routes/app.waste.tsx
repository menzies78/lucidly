import { json } from "@remix-run/node";
import { Page, Card, Text, BlockStack } from "@shopify/polaris";
import ReportTabs from "../components/ReportTabs";
import { authenticate } from "../shopify.server";

export const loader = async ({ request }) => {
  await authenticate.admin(request);
  return json({});
};

export default function WasteDetector() {
  return (
    <Page title="Waste Detector" fullWidth>
      <ReportTabs>
      <BlockStack gap="500">
        <Card>
          <BlockStack gap="300">
            <Text as="h2" variant="headingLg">Ad Spend Waste Detector</Text>
            <Text as="p" variant="bodyMd" tone="subdued">
              "You wasted £X on ads this week." Breakdown: spend on existing customers (retargeting waste),
              fatigued audiences (frequency decay), low-converting creative.
            </Text>
            <Text as="p" variant="bodyMd" tone="subdued">Coming soon — powered by the AI layer.</Text>
          </BlockStack>
        </Card>
      </BlockStack>
      </ReportTabs>
    </Page>
  );
}
