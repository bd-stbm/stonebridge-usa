import { getSupabaseAdmin } from "./supabase-admin";
import { getSessionUser, type SessionUser } from "./session";

// Guard for every admin-only server path. Throws if the caller isn't an
// admin — call this BEFORE using the service-role client. The check reads
// app_user.role through the user's own RLS-scoped session, so it can't be
// spoofed from the client.
export async function requireAdmin(): Promise<SessionUser> {
  const user = await getSessionUser();
  if (!user?.isAdmin) {
    throw new Error("Forbidden: admin access required.");
  }
  return user;
}

export type UserRole = "admin" | "client";

export interface ManagedUser {
  id: string;
  email: string | null;
  role: UserRole | null; // null = no app_user row yet (treated as no-access client)
  familyNodeIds: string[];
  lastSignInAt: string | null;
}

export interface FamilyOption {
  nodeId: string;
  alias: string;
}

// All login accounts plus their role and family mappings. Reads auth.users
// via the admin API and joins app_user / user_family_access with the
// service-role client (bypassing RLS so an admin sees every user, not just
// their own row).
export async function listManagedUsers(): Promise<ManagedUser[]> {
  const admin = getSupabaseAdmin();

  const { data: list, error } = await admin.auth.admin.listUsers({
    page: 1,
    perPage: 1000,
  });
  if (error) throw error;

  const [{ data: roles }, { data: fams }] = await Promise.all([
    admin.from("app_user").select("user_id, role"),
    admin.from("user_family_access").select("user_id, sub_client_node_id"),
  ]);

  const roleByUser = new Map<string, UserRole>(
    (roles ?? []).map(r => [r.user_id as string, r.role as UserRole]),
  );
  const famsByUser = new Map<string, string[]>();
  for (const f of fams ?? []) {
    const arr = famsByUser.get(f.user_id as string) ?? [];
    arr.push(f.sub_client_node_id as string);
    famsByUser.set(f.user_id as string, arr);
  }

  return list.users
    .map(u => ({
      id: u.id,
      email: u.email ?? null,
      role: roleByUser.get(u.id) ?? null,
      familyNodeIds: famsByUser.get(u.id) ?? [],
      lastSignInAt: u.last_sign_in_at ?? null,
    }))
    .sort((a, b) => (a.email ?? "").localeCompare(b.email ?? ""));
}

// Every family that actually holds positions, as {nodeId, alias}. Sourced
// from v_latest_positions so junk/empty sub-clients in entity_attribution
// don't appear in the assignment UI. node_id is the stable key written to
// user_family_access; alias is for display.
export async function listAllFamilies(): Promise<FamilyOption[]> {
  const admin = getSupabaseAdmin();
  const { data, error } = await admin
    .from("v_latest_positions")
    .select("sub_client_node_id, sub_client_alias")
    .not("sub_client_node_id", "is", null)
    .limit(100000);
  if (error) throw error;

  const byNode = new Map<string, string>();
  for (const r of data ?? []) {
    byNode.set(r.sub_client_node_id as string, r.sub_client_alias as string);
  }
  return Array.from(byNode.entries())
    .map(([nodeId, alias]) => ({ nodeId, alias }))
    .sort((a, b) => a.alias.localeCompare(b.alias));
}
