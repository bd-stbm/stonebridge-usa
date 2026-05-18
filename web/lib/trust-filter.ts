import { cookies } from "next/headers";

// Read the user's currently-selected trust filter from the cookie set by
// the header dropdown. Returns null when "All trusts" is selected (or when
// no cookie is set yet).
export function getSelectedTrust(): string | null {
  const value = cookies().get("trust_filter")?.value;
  return value && value.length > 0 ? value : null;
}

export const TRUST_COOKIE = "trust_filter";
