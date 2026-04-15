import { redirect } from "@remix-run/node";

export const loader = async ({ request }) => {
  const url = new URL(request.url);
  const params = url.search;
  return redirect(`/app/customers${params}`);
};

export default function LTV() {
  return null;
}
