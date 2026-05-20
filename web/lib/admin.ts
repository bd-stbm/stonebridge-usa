// Who can see the SubClient selector in the header.
//
// Today this is hardcoded — internal Stonebridge users are admins, anyone
// else (family principals, advisors invited later) is not. When Phase 2
// RLS lands the same predicate should drive both: the UI gate here AND
// the database row-level policy on every table. Until then this is a
// COSMETIC gate only: a non-admin who hand-edits their `sub_client`
// cookie would still get cross-family data back, because RLS is
// `USING (true)` on every table. Do NOT invite external users until
// Phase 2 RLS is in place.

const ADMIN_EMAIL_DOMAINS = ["stbm.com.au"] as const;

export function isAdminEmail(email: string | null | undefined): boolean {
  if (!email) return false;
  const at = email.lastIndexOf("@");
  if (at < 0) return false;
  const domain = email.slice(at + 1).toLowerCase();
  return (ADMIN_EMAIL_DOMAINS as readonly string[]).includes(domain);
}
