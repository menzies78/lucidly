import type { ActionFunctionArgs } from "@remix-run/node";
import { authenticate } from "../shopify.server";
import db from "../db.server";

/**
 * Managed Pricing: Shopify fires app_subscriptions/update whenever the
 * merchant's subscription changes state (they approve a plan on the
 * Shopify-hosted pricing page, cancel, the trial converts, a charge
 * freezes on non-payment, etc.). This webhook is the ONLY plan-flip
 * authority — the app never mutates billing itself.
 *
 * State mapping:
 *   ACTIVE            → paid (start of subscription or trial-with-charge)
 *   CANCELLED/EXPIRED/DECLINED/FROZEN → free (the audit tier is the floor;
 *                        data is kept — the weekly prune's 30-day grace
 *                        handles history removal later, so an accidental
 *                        cancel or card failure is instantly reversible)
 *
 * On upgrade: freeze dataWindowStart as the history-backfill boundary and
 * mark the backfill pending. The scheduler picks pending backfills up on
 * its next cycle (serialized with the sync chain, resumable via cursor).
 */
export const action = async ({ request }: ActionFunctionArgs) => {
  const { shop, topic, payload } = await authenticate.webhook(request);
  console.log(`[Billing] ${topic} for ${shop}`);

  const sub = (payload as any)?.app_subscription;
  if (!sub?.status) {
    console.warn(`[Billing] ${shop}: no app_subscription in payload — ignoring`);
    return new Response();
  }

  const shopRow = await db.shop.findUnique({
    where: { shopDomain: shop },
    select: { plan: true, dataWindowStart: true, historyBackfillStatus: true },
  });
  if (!shopRow) {
    console.warn(`[Billing] ${shop}: no Shop row — ignoring`);
    return new Response();
  }

  const status = String(sub.status).toUpperCase();
  console.log(`[Billing] ${shop}: subscription "${sub.name}" → ${status} (was plan=${shopRow.plan})`);

  // The public Free plan ("Free" / handle "free-audit") ALSO creates an
  // ACTIVE $0 subscription when selected — it must map to the free tier,
  // not paid. Belt and braces: match by name AND by zero price.
  // Name-only classification. We own the plan catalog ("free"/"free-audit"
  // vs "growth"), and the webhook payload carries NEITHER pricing details
  // nor a test flag (verified live 2026-08-04: name="growth" price absent,
  // test undefined on a dev-store charge) — so any price-based fallback
  // misclassifies test-mode paid plans as free. If a plan is ever renamed
  // or added in the Partner pricing manager, update this list.
  const planName = String(sub.name || "").trim().toLowerCase();
  const isFreePlan = ["free", "free-audit"].includes(planName);

  if (status === "ACTIVE" && isFreePlan) {
    if (shopRow.plan !== "free") {
      await db.shop.update({
        where: { shopDomain: shop },
        data: { plan: "free", planChangedAt: new Date() },
      });
      console.log(`[Billing] ${shop}: on Free plan (audit tier)${shopRow.plan === "paid" ? " — downgraded from paid, 30-day data grace applies" : ""}`);
    }
    return new Response();
  }

  if (status === "ACTIVE") {
    if (shopRow.plan !== "paid") {
      // Freeze the backfill boundary BEFORE flipping the plan: everything
      // older than the shop's current oldest imported day gets fetched by
      // the history backfill; everything newer is already present.
      const oldest = await db.order.findFirst({
        where: { shopDomain: shop },
        orderBy: { createdAt: "asc" },
        select: { createdAt: true },
      });
      await db.shop.update({
        where: { shopDomain: shop },
        data: {
          plan: "paid",
          planChangedAt: new Date(),
          dataWindowStart: shopRow.dataWindowStart ?? oldest?.createdAt ?? null,
          // Only queue a backfill if one hasn't already completed — a
          // cancel/resubscribe inside the grace window keeps full history
          // and needs no re-import.
          ...(shopRow.historyBackfillStatus === "none" || shopRow.historyBackfillStatus === "failed"
            ? { historyBackfillStatus: "pending", historyBackfillCursor: null }
            : {}),
        },
      });
      console.log(`[Billing] ${shop}: upgraded to paid; history backfill ${shopRow.historyBackfillStatus === "complete" ? "already complete" : "queued"}`);
    }
  } else if (["CANCELLED", "EXPIRED", "DECLINED", "FROZEN"].includes(status)) {
    if (shopRow.plan === "paid") {
      await db.shop.update({
        where: { shopDomain: shop },
        data: { plan: "free", planChangedAt: new Date() },
      });
      console.log(`[Billing] ${shop}: downgraded to free (audit tier); 30-day data grace applies`);
    }
  }

  return new Response();
};
