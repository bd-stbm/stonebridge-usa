import { redirect } from "next/navigation";
import { getSupabaseServer } from "@/lib/supabase-server";
import MfaEnroll from "@/components/MfaEnroll";

export const dynamic = "force-dynamic";

export default async function MfaEnrollPage() {
  const supabase = getSupabaseServer();
  const {
    data: { user },
  } = await supabase.auth.getUser();
  if (!user) redirect("/login");

  const { data: aal } = await supabase.auth.mfa.getAuthenticatorAssuranceLevel();
  if (aal?.currentLevel === "aal2") redirect("/"); // already MFA'd this session
  if (aal?.nextLevel === "aal2") redirect("/auth/mfa"); // verified factor exists -> challenge

  return (
    <main className="flex min-h-screen items-center justify-center bg-slate-50 px-4 py-10">
      <div className="w-full max-w-sm rounded-lg border border-slate-200 bg-white p-6 shadow-sm">
        <h1 className="text-lg font-semibold text-slate-900">
          Set up two-factor authentication
        </h1>
        <p className="mt-1 text-sm text-slate-500">
          Required to access your Stonebridge dashboard.
        </p>
        <div className="mt-5">
          <MfaEnroll />
        </div>
        <form action="/auth/signout" method="post" className="mt-4 text-center">
          <button
            type="submit"
            className="text-xs text-slate-400 hover:text-slate-600"
          >
            Sign out
          </button>
        </form>
      </div>
    </main>
  );
}
