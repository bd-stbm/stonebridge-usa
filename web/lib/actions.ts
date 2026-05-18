"use server";

import { cookies } from "next/headers";
import {
  ACCOUNT_COOKIE,
  BENCHMARK_COOKIE,
  TRUST_COOKIE,
} from "./trust-filter";

const ONE_YEAR = 60 * 60 * 24 * 365;

// Server actions invoked by the header filter dropdowns. The client follows
// up with router.refresh() so server components re-render with the new
// scope.

export async function setTrustFilter(trust: string): Promise<void> {
  const c = cookies();
  if (!trust) {
    c.delete(TRUST_COOKIE);
  } else {
    c.set(TRUST_COOKIE, trust, {
      path: "/",
      sameSite: "lax",
      maxAge: ONE_YEAR,
    });
  }
  // Changing trusts almost always makes any previously-selected account
  // invalid (different sub-tree). Clear it so the AccountFilter resets
  // to "All accounts".
  c.delete(ACCOUNT_COOKIE);
}

export async function setAccountFilter(account: string): Promise<void> {
  const c = cookies();
  if (!account) {
    c.delete(ACCOUNT_COOKIE);
    return;
  }
  c.set(ACCOUNT_COOKIE, account, {
    path: "/",
    sameSite: "lax",
    maxAge: ONE_YEAR,
  });
}

export async function setBenchmark(ticker: string): Promise<void> {
  const c = cookies();
  if (!ticker) {
    c.delete(BENCHMARK_COOKIE);
    return;
  }
  c.set(BENCHMARK_COOKIE, ticker, {
    path: "/",
    sameSite: "lax",
    maxAge: ONE_YEAR,
  });
}
