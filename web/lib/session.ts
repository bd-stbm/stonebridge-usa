import { getSupabaseServer } from "./supabase-server";
import { getSelectedSubClient } from "./trust-filter";
import { listSubClients } from "./queries";

// Server-side identity + authorization for the signed-in user. The security
// boundary is the database (RLS, migration 028); everything here is the UX
// layer that mirrors it — choosing a sensible default scope and deciding
// which chrome to show. Never treat these values as a security gate.

export interface SessionUser {
  id: string;
  email: string | null;
  isAdmin: boolean;
}

// The current user plus their admin flag, read from app_user.role. The
// "own row or admin" RLS policy lets a user read their own row; a user with
// no app_user row (not yet provisioned) is treated as a non-admin client.
export async function getSessionUser(): Promise<SessionUser | null> {
  const supabase = getSupabaseServer();
  const {
    data: { user },
  } = await supabase.auth.getUser();
  if (!user) return null;

  const { data } = await supabase
    .from("app_user")
    .select("role")
    .eq("user_id", user.id)
    .maybeSingle();

  return {
    id: user.id,
    email: user.email ?? null,
    isAdmin: data?.role === "admin",
  };
}

// Families the current user may see. Under RLS, listSubClients() (which
// reads the security_invoker v_latest_positions) already returns only the
// user's own families for a client, or every family for an admin — so this
// is both the selector's option list and the allow-list for clamping.
export async function getAccessibleSubClients(): Promise<string[]> {
  return listSubClients();
}

// The effective sub-client for this request: the cookie value when the user
// is actually allowed it, otherwise their first accessible family. This
// keeps a freshly-invited client (no cookie, so the cookie reader returns
// the admin DEFAULT_SUB_CLIENT) from landing on a family they can't see and
// getting an empty dashboard. Pass `accessible` when the caller already has
// it (the Header does) to avoid a second round-trip.
export async function getActiveSubClient(
  accessible?: string[],
): Promise<string> {
  const cookieValue = getSelectedSubClient();
  const allowed = accessible ?? (await listSubClients());
  if (allowed.length === 0) return cookieValue; // no-access user; queries return nothing anyway
  return allowed.includes(cookieValue) ? cookieValue : allowed[0];
}
