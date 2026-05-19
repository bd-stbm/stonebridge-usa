import { cookies } from "next/headers";

// Cookies persist the global Trust and Account filters across navigation.
// Both store JSON-encoded string arrays so they can hold multiple values.
// Empty / missing cookie = "all" (no filter).

export const TRUST_COOKIE = "trust_filter";
export const ACCOUNT_COOKIE = "account_filter";
export const BENCHMARK_COOKIE = "benchmark";

export const DEFAULT_BENCHMARK = "^SP500TR";

function parseList(raw: string | undefined): string[] {
  if (!raw) return [];
  try {
    const parsed = JSON.parse(raw);
    if (Array.isArray(parsed)) return parsed.filter(v => typeof v === "string");
  } catch {
    // Legacy single-value cookie — treat as singleton list.
    return [raw];
  }
  return [];
}

export function getSelectedTrusts(): string[] {
  return parseList(cookies().get(TRUST_COOKIE)?.value);
}

export function getSelectedAccounts(): string[] {
  return parseList(cookies().get(ACCOUNT_COOKIE)?.value);
}

export function getSelectedBenchmark(): string {
  const value = cookies().get(BENCHMARK_COOKIE)?.value;
  return value && value.length > 0 ? value : DEFAULT_BENCHMARK;
}
