import { cookies } from "next/headers";

// Cookies that persist the global Trust and Account filters across page
// navigation. Both are scoped to "/" so they apply everywhere; values are
// the trust_alias and account_node_id respectively.

export const TRUST_COOKIE = "trust_filter";
export const ACCOUNT_COOKIE = "account_filter";
export const BENCHMARK_COOKIE = "benchmark";

export const DEFAULT_BENCHMARK = "^SP500TR";

export function getSelectedTrust(): string | null {
  const value = cookies().get(TRUST_COOKIE)?.value;
  return value && value.length > 0 ? value : null;
}

export function getSelectedAccount(): string | null {
  const value = cookies().get(ACCOUNT_COOKIE)?.value;
  return value && value.length > 0 ? value : null;
}

export function getSelectedBenchmark(): string {
  const value = cookies().get(BENCHMARK_COOKIE)?.value;
  return value && value.length > 0 ? value : DEFAULT_BENCHMARK;
}
