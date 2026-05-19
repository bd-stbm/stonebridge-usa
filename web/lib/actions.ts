"use server";

import { cookies } from "next/headers";
import {
  ACCOUNT_COOKIE,
  BENCHMARK_COOKIE,
  TRUST_COOKIE,
} from "./trust-filter";

const ONE_YEAR = 60 * 60 * 24 * 365;

function setOrClearList(name: string, values: string[]): void {
  const c = cookies();
  if (!values || values.length === 0) {
    c.delete(name);
    return;
  }
  c.set(name, JSON.stringify(values), {
    path: "/",
    sameSite: "lax",
    maxAge: ONE_YEAR,
  });
}

export async function setTrustFilter(trusts: string[]): Promise<void> {
  setOrClearList(TRUST_COOKIE, trusts);
  // Changing trusts almost always invalidates previously-selected accounts
  // (the account may now be outside scope). Clear so the AccountFilter
  // resets and the page re-renders with the new trust set.
  cookies().delete(ACCOUNT_COOKIE);
}

export async function setAccountFilter(accounts: string[]): Promise<void> {
  setOrClearList(ACCOUNT_COOKIE, accounts);
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
