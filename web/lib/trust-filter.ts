import { cookies } from "next/headers";

// Cookies persist the global Trust and Account filters across navigation.
// Both store JSON-encoded string arrays so they can hold multiple values.
// Empty / missing cookie = "all" (no filter).

export const TRUST_COOKIE = "trust_filter";
export const ACCOUNT_COOKIE = "account_filter";
export const BENCHMARK_COOKIE = "benchmark";
export const SUB_CLIENT_COOKIE = "sub_client";

export const DEFAULT_BENCHMARK = "^SP500TR";

// Fallback when no sub_client cookie is set. Today this also matches the
// `DEFAULT_SUB_CLIENT` constant re-exported from lib/queries.ts (kept in
// sync). When Phase 2 RLS lands and per-user binding via user_profile
// replaces the cookie path, this becomes the admin fallback only.
export const DEFAULT_SUB_CLIENT = "Dyne Family (US)";

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

// Resolves the active sub-client for the current request. Cookie-based for
// admin users today; will be replaced by a user_profile lookup once family
// principals start signing in (see admin.ts notes). Returns the default
// sub-client when no cookie is set so pages always have a scope.
export function getSelectedSubClient(): string {
  const value = cookies().get(SUB_CLIENT_COOKIE)?.value;
  return value && value.length > 0 ? value : DEFAULT_SUB_CLIENT;
}
