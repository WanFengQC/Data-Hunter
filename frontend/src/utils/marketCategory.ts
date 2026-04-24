export const CANADA_SOURCE_SEGMENT = "animal & pillow";

export const MARKET_CATEGORY_CANADA = "Canada";
export const MARKET_CATEGORY_UNITED_STATES = "United States";

function normalizePath(value: unknown): string {
  const text = String(value ?? "").trim().toLowerCase();
  if (!text) return "";
  return text.replaceAll("/", "\\");
}

export function inferMarketCategory(sourcePath: unknown): string {
  const normalized = normalizePath(sourcePath);
  if (normalized.includes(CANADA_SOURCE_SEGMENT)) {
    return MARKET_CATEGORY_CANADA;
  }
  return MARKET_CATEGORY_UNITED_STATES;
}
