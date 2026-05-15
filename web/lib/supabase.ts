import { createClient, type SupabaseClient } from "@supabase/supabase-js";

// Server-only Supabase client. Lazy-initialised so that importing this module
// (or anything that re-exports from it, like lib/queries) doesn't crash the
// build when env vars happen not to be set in the current step — e.g. while
// Next.js evaluates stub pages during `next build`.
//
// Uses the service_role key — bypasses RLS. Never import this from a
// "use client" component or it will end up in the client bundle.
let _client: SupabaseClient | null = null;

export function supabase(): SupabaseClient {
  if (_client) return _client;
  const url = process.env.SUPABASE_URL;
  const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
  if (!url || !key) {
    throw new Error(
      "SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set in Vercel " +
        "environment variables (or .env.local for local dev).",
    );
  }
  _client = createClient(url, key, { auth: { persistSession: false } });
  return _client;
}
