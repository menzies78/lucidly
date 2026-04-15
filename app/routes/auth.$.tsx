import type { LoaderFunctionArgs } from "@remix-run/node";
import { authenticate } from "../shopify.server";
import { ensureWebhooks } from "../services/ensureWebhooks.server.js";

export const loader = async ({ request }: LoaderFunctionArgs) => {
  const { session } = await authenticate.admin(request);
  try {
    await ensureWebhooks(session.shop, session.accessToken!);
  } catch (err) {
    console.error("[auth] ensureWebhooks failed:", err);
  }
  return null;
};
