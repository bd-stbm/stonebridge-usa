"use server";

import { cookies } from "next/headers";
import {
  ACCOUNT_COOKIE,
  ASSET_CLASS_COOKIE,
  BENCHMARK_COOKIE,
  SUB_CLIENT_COOKIE,
  TRUST_COOKIE,
  VEHICLE_COOKIE,
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
  // Changing trusts almost always invalidates previously-selected accounts and
  // vehicles (both cascade on the entity selection). Clear so those filters
  // reset to the new entity scope.
  cookies().delete(ACCOUNT_COOKIE);
  cookies().delete(VEHICLE_COOKIE);
}

export async function setAccountFilter(accounts: string[]): Promise<void> {
  setOrClearList(ACCOUNT_COOKIE, accounts);
}

export async function setAssetClassFilter(classes: string[]): Promise<void> {
  setOrClearList(ASSET_CLASS_COOKIE, classes);
}

export async function setVehicleFilter(vehicles: string[]): Promise<void> {
  setOrClearList(VEHICLE_COOKIE, vehicles);
  // The Account dropdown cascades on the vehicle selection, so a previously
  // picked account may now be out of scope — clear it.
  cookies().delete(ACCOUNT_COOKIE);
}

export async function setSubClient(subClient: string): Promise<void> {
  const c = cookies();
  if (!subClient) {
    c.delete(SUB_CLIENT_COOKIE);
  } else {
    c.set(SUB_CLIENT_COOKIE, subClient, {
      path: "/",
      sameSite: "lax",
      maxAge: ONE_YEAR,
    });
  }
  // Changing the sub-client invalidates any trust/account selection — both
  // are scoped to a single sub-client and the names won't carry over.
  c.delete(TRUST_COOKIE);
  c.delete(ACCOUNT_COOKIE);
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
