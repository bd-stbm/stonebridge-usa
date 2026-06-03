import { redirect } from "next/navigation";
import { getSupabaseServer } from "@/lib/supabase-server";
import MfaChallenge from "@/components/MfaChallenge";

export const dynamic = "force-dynamic";

export default async function MfaChallengePage() {
  const supabase = getSupabaseServer();
  const {
    data: { user },
  } = await supabase.auth.getUser();
  if (!user) redirect("/login");

  const { data: aal } = await supabase.auth.mfa.getAuthenticatorAssuranceLevel();
  if (aal?.currentLevel === "aal2") redirect("/"); // already passed this session

  const { data: factors } = await supabase.auth.mfa.listFactors();
  const totp = (factors?.totp ?? []).find(f => f.status === "verified");
  if (!totp) redirect("/auth/mfa/enroll"); // nothing to challenge -> enroll

  return (
    <main className="flex min-h-screen items-center justify-center bg-slate-50 px-4">
      <div className="w-full max-w-sm rounded-lg border border-slate-200 bg-white p-6 shadow-sm">
        <h1 className="text-lg font-semibold text-slate-900">
          Two-factor authentication
        </h1>
        <p className="mt-1 text-sm text-slate-500">
          Enter the 6-digit code from your authenticator app.
        </p>
        <div className="mt-5">
          <MfaChallenge factorId={totp.id} />
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
