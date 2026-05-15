import { createClient } from "@supabase/supabase-js";

// Server-only Supabase client. Uses the service_role key — bypasses RLS.
// Never import this module from a "use client" component or it will end up
// in the client bundle.
export const supabase = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!,
  { auth: { persistSession: false } },
);
