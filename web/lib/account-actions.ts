"use server";

import { createClient } from "@supabase/supabase-js";
import { getSupabaseServer } from "./supabase-server";
import { getSessionUser } from "./session";

export interface PasswordResult {
  ok: boolean;
  error?: string;
}

// Let the signed-in user change their own password. Requires the current
// password, verified on a THROWAWAY client (persistSession:false) so the
// check doesn't mint a new cookie session — which would also reset the MFA
// assurance level back to aal1. The actual update runs on the real
// cookie-bound session.
export async function changeOwnPassword(
  currentPassword: string,
  newPassword: string,
): Promise<PasswordResult> {
  const user = await getSessionUser();
  if (!user?.email) return { ok: false, error: "You must be signed in." };
  if (!newPassword || newPassword.length < 8) {
    return { ok: false, error: "New password must be at least 8 characters." };
  }

  const url = process.env.NEXT_PUBLIC_SUPABASE_URL || process.env.SUPABASE_URL;
  const anonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY;
  if (!url || !anonKey) return { ok: false, error: "Server not configured." };

  const probe = createClient(url, anonKey, {
    auth: { persistSession: false, autoRefreshToken: false },
  });
  const { error: verifyError } = await probe.auth.signInWithPassword({
    email: user.email,
    password: currentPassword,
  });
  if (verifyError) return { ok: false, error: "Current password is incorrect." };

  const supabase = getSupabaseServer();
  const { error } = await supabase.auth.updateUser({ password: newPassword });
  if (error) return { ok: false, error: error.message };
  return { ok: true };
}
