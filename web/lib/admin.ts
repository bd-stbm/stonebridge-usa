// DEPRECATED — admin identity now lives in the database.
//
// Phase 2a (migration 028) replaced this cosmetic email-domain gate with a
// real role: app_user.role = 'admin', surfaced at runtime via
// getSessionUser().isAdmin and enforced by RLS through is_admin(). The
// @stbm.com.au domain is still used, but only once, to SEED admins in the
// migration — not as a live authorization check.
//
// Kept only so any stale import resolves; prefer getSessionUser() from
// lib/session.ts. Safe to delete once nothing references it.

const ADMIN_EMAIL_DOMAINS = ["stbm.com.au"] as const;

export function isAdminEmail(email: string | null | undefined): boolean {
  if (!email) return false;
  const at = email.lastIndexOf("@");
  if (at < 0) return false;
  const domain = email.slice(at + 1).toLowerCase();
  return (ADMIN_EMAIL_DOMAINS as readonly string[]).includes(domain);
}
