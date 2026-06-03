"use server";

import { revalidatePath } from "next/cache";
import { getSupabaseAdmin } from "./supabase-admin";
import { requireAdmin, type UserRole } from "./admin-data";

export interface ActionResult {
  ok: boolean;
  error?: string;
}

// Replace a user's family mappings with exactly `familyNodeIds`. Admins are
// mapped too for completeness, though their access doesn't depend on it
// (is_admin() grants all). Internal helper — not exported, so it is not a
// callable server action.
async function replaceFamilies(
  admin: ReturnType<typeof getSupabaseAdmin>,
  userId: string,
  familyNodeIds: string[],
): Promise<void> {
  await admin.from("user_family_access").delete().eq("user_id", userId);
  const unique = Array.from(new Set(familyNodeIds));
  if (unique.length > 0) {
    await admin
      .from("user_family_access")
      .insert(unique.map(n => ({ user_id: userId, sub_client_node_id: n })));
  }
}

// Create a new client login with a password you set, and map them to the
// given families. No email is sent (email_confirm:true marks the address
// confirmed so they can sign in immediately) — hand them the credentials
// out-of-band. They can change the password later from the user menu.
export async function createClientUser(
  email: string,
  password: string,
  familyNodeIds: string[],
): Promise<ActionResult> {
  await requireAdmin();
  const trimmed = email.trim().toLowerCase();
  if (!trimmed || !trimmed.includes("@")) {
    return { ok: false, error: "Enter a valid email address." };
  }
  if (!password || password.length < 8) {
    return { ok: false, error: "Password must be at least 8 characters." };
  }
  const admin = getSupabaseAdmin();
  const { data, error } = await admin.auth.admin.createUser({
    email: trimmed,
    password,
    email_confirm: true,
  });
  if (error || !data?.user) {
    return { ok: false, error: error?.message ?? "Could not create user." };
  }
  await admin
    .from("app_user")
    .upsert({ user_id: data.user.id, role: "client" }, { onConflict: "user_id" });
  await replaceFamilies(admin, data.user.id, familyNodeIds);
  revalidatePath("/admin/users");
  return { ok: true };
}

// Set/reset a user's password (admin-driven; replaces self-serve email
// resets while there's no SMTP). The user can also change it themselves
// from the user menu.
export async function setUserPassword(
  userId: string,
  password: string,
): Promise<ActionResult> {
  await requireAdmin();
  if (!password || password.length < 8) {
    return { ok: false, error: "Password must be at least 8 characters." };
  }
  const admin = getSupabaseAdmin();
  const { error } = await admin.auth.admin.updateUserById(userId, { password });
  if (error) return { ok: false, error: error.message };
  return { ok: true };
}

// Update an existing user's role + family mappings.
export async function saveUser(
  userId: string,
  role: UserRole,
  familyNodeIds: string[],
): Promise<ActionResult> {
  await requireAdmin();
  if (role !== "admin" && role !== "client") {
    return { ok: false, error: "Invalid role." };
  }
  const admin = getSupabaseAdmin();
  await admin
    .from("app_user")
    .upsert({ user_id: userId, role }, { onConflict: "user_id" });
  await replaceFamilies(admin, userId, familyNodeIds);
  revalidatePath("/admin/users");
  return { ok: true };
}

// Revoke a user's access: drop their role + all family mappings. The login
// itself is kept (they'll see the "access being set up" state); delete the
// auth user from the Supabase dashboard if you want to remove it entirely.
export async function revokeUser(userId: string): Promise<ActionResult> {
  const me = await requireAdmin();
  if (me.id === userId) {
    return { ok: false, error: "You can't revoke your own access." };
  }
  const admin = getSupabaseAdmin();
  await admin.from("user_family_access").delete().eq("user_id", userId);
  await admin.from("app_user").delete().eq("user_id", userId);
  revalidatePath("/admin/users");
  return { ok: true };
}
