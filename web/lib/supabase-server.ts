import { createServerClient, type CookieOptions } from "@supabase/ssr";
import { cookies } from "next/headers";

type CookieToSet = { name: string; value: string; options: CookieOptions };

// Per-request Supabase client authenticated as the signed-in user (anon key
// + user JWT from cookies). RLS policies apply — this is the path the
// dashboard uses for every query.
//
// Do NOT call this from middleware — middleware needs its own client with
// access to the request/response object so it can write refreshed-session
// cookies back. See web/middleware.ts.
export function getSupabaseServer() {
  const cookieStore = cookies();

  const url =
    process.env.NEXT_PUBLIC_SUPABASE_URL || process.env.SUPABASE_URL;
  const anonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY;
  if (!url || !anonKey) {
    const missing: string[] = [];
    if (!url) missing.push("NEXT_PUBLIC_SUPABASE_URL");
    if (!anonKey) missing.push("NEXT_PUBLIC_SUPABASE_ANON_KEY");
    throw new Error(
      `Missing env var(s): ${missing.join(", ")}. ` +
        "Set in Vercel -> Settings -> Environment Variables.",
    );
  }

  return createServerClient(url, anonKey, {
    cookies: {
      getAll() {
        return cookieStore.getAll();
      },
      setAll(cookiesToSet: CookieToSet[]) {
        try {
          cookiesToSet.forEach(({ name, value, options }) =>
            cookieStore.set(name, value, options),
          );
        } catch {
          // Called from a Server Component — Next.js disallows cookie writes
          // here. Middleware handles session refresh, so swallow.
        }
      },
    },
  });
}
