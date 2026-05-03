import { json } from "@remix-run/node";
import type { LoaderFunctionArgs } from "@remix-run/node";
import { authenticate } from "../shopify.server";
import { getSyncStatus } from "../services/syncStatus.server";

// Tiny status endpoint polled by LoadingIndicator (app/routes/app.tsx) so
// the merchant can see when the in-process scheduler is mid-cycle and
// understand why a tab click might be slow. Lives under /app to inherit
// the embedded-app auth that the parent loader sets up.
export const loader = async ({ request }: LoaderFunctionArgs) => {
  await authenticate.admin(request);
  return json(getSyncStatus(), {
    headers: { "Cache-Control": "no-store" },
  });
};
