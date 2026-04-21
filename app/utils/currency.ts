// Shared currency helpers.
//
// Single source of truth for mapping an ISO 4217 currency code (as stored on
// Shop.shopifyCurrency / Shop.metaAccountCurrency) to its display symbol.
// Use this everywhere instead of inlining `currency === "GBP" ? "£" : ...`
// ternaries — those silently lumped AUD/CAD/JPY into "$".

const CURRENCY_SYMBOL: Record<string, string> = {
  GBP: "£",
  USD: "$",
  EUR: "€",
  AUD: "A$",
  CAD: "C$",
  JPY: "¥",
  NZD: "NZ$",
  CHF: "CHF ",
  SEK: "kr",
  NOK: "kr",
  DKK: "kr",
};

// Returns the display symbol for a currency code. Falls back to the code
// itself followed by a space (e.g. "ZAR 123") for unknown currencies so the
// user still sees *something* accurate instead of a misleading "$".
// `defaultCode` is used when `code` is nullish — defaults to GBP (Lucidly
// merchant base is UK-first).
export function currencySymbolFromCode(
  code: string | null | undefined,
  defaultCode: string = "GBP",
): string {
  const c = (code || defaultCode).toUpperCase();
  return CURRENCY_SYMBOL[c] || `${c} `;
}
