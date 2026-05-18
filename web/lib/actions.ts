"use server";

import { cookies } from "next/headers";
import { TRUST_COOKIE } from "./trust-filter";

// Server action invoked by the header TrustFilter. Empty string clears the
// filter; any other value selects a specific trust. The client follows up
// with router.refresh() so server components re-render with the new value.
export async function setTrustFilter(trust: string): Promise<void> {
  const c = cookies();
  if (!trust) {
    c.delete(TRUST_COOKIE);
    return;
  }
  c.set(TRUST_COOKIE, trust, {
    path: "/",
    sameSite: "lax",
    maxAge: 60 * 60 * 24 * 365,
  });
}
