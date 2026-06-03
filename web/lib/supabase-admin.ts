import { createClient } from "@supabase/supabase-js";

// SERVER-ONLY service-role client. It bypasses RLS and can call the Supabase
// Auth admin API (list/invite users), so it must NEVER be imported into a
// client component or exposed to the browser. Only the admin server actions
// and admin data loaders use it, and each guards on the caller being an
// admin (requireAdmin) BEFORE touching it.
export function getSupabaseAdmin() {
  const url = process.env.NEXT_PUBLIC_SUPABASE_URL || process.env.SUPABASE_URL;
  const serviceKey = process.env.SUPABASE_SERVICE_ROLE_KEY;
  if (!url || !serviceKey) {
    const missing: string[] = [];
    if (!url) missing.push("NEXT_PUBLIC_SUPABASE_URL");
    if (!serviceKey) missing.push("SUPABASE_SERVICE_ROLE_KEY");
    throw new Error(
      `Missing env var(s): ${missing.join(", ")}. ` +
        "Set in Vercel -> Settings -> Environment Variables.",
    );
  }
  return createClient(url, serviceKey, {
    auth: { autoRefreshToken: false, persistSession: false },
  });
}
