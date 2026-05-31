export function money(n: number, ccy = "USD"): string {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: ccy,
    maximumFractionDigits: 0,
  }).format(n);
}

// Two-decimal currency formatter for per-unit prices, where cents matter.
// money() rounds to whole units because it's used for NAVs / market values
// where cents are noise.
export function price(n: number, ccy = "USD"): string {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: ccy,
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(n);
}

export function pct(n: number, digits = 2): string {
  return `${(n * 100).toFixed(digits)}%`;
}

// Friendly benchmark label: explicit overrides first, then a generic rule
// that drops the leading "^" and a trailing "TR" so total-return tickers
// read cleanly in the UI:
//   "^SP500TR" → "SP500", "^AXJT" → "ASX200", "ACWI" → "ACWI".
const INDEX_LABEL_OVERRIDES: Record<string, string> = {
  "^AXJT": "ASX200",
};
export function indexLabel(ticker: string): string {
  return (
    INDEX_LABEL_OVERRIDES[ticker] ?? ticker.replace(/^\^/, "").replace(/TR$/, "")
  );
}

export function shortDate(iso: string): string {
  return new Date(iso).toLocaleDateString("en-US", {
    year: "numeric",
    month: "short",
  });
}
