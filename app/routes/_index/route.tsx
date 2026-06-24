import type { LoaderFunctionArgs } from "@remix-run/node";
import { redirect } from "@remix-run/node";

import styles from "./styles.module.css";

export const loader = async ({ request }: LoaderFunctionArgs) => {
  const url = new URL(request.url);

  // When Shopify opens the app with a shop context, hand straight off to the
  // embedded app (OAuth/token-exchange happens there). This public page never
  // asks the merchant to type a shop domain — installation is initiated from
  // the Shopify App Store, per App Store requirement 2.3.1.
  if (url.searchParams.get("shop")) {
    throw redirect(`/app?${url.searchParams.toString()}`);
  }

  return null;
};

export default function App() {
  return (
    <div className={styles.index}>
      <div className={styles.content}>
        <h1 className={styles.heading}>
          Lucidly — Meta Ads attribution &amp; customer LTV for Shopify
        </h1>
        <p className={styles.text}>
          See which customers your Meta ads actually acquire, the true lifetime
          value behind them, and where your ad spend is working — or wasted.
        </p>
        <p className={styles.text}>
          Lucidly is a Shopify embedded app. Install it from the Shopify App
          Store and open it from your Shopify admin to get started.
        </p>
        <ul className={styles.list}>
          <li>
            <strong>True ROAS &amp; customer LTV</strong>. Revenue verified
            against real Shopify orders, not just Meta&rsquo;s reported numbers.
          </li>
          <li>
            <strong>New vs returning breakdown</strong>. Know who&rsquo;s a
            first-time Meta-acquired customer and who came back on their own.
          </li>
          <li>
            <strong>Wasted-spend insights</strong>. Spot fatigued audiences and
            ad spend going to customers you already had.
          </li>
        </ul>
      </div>
    </div>
  );
}
