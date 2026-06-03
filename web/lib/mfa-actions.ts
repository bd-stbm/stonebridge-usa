"use server";

import { getSupabaseServer } from "./supabase-server";

export interface EnrollResult {
  ok: boolean;
  factorId?: string;
  qrCode?: string;
  secret?: string;
  error?: string;
}

export interface VerifyResult {
  ok: boolean;
  error?: string;
}

// Begin TOTP enrollment for the signed-in user. Cleans up any half-finished
// (unverified) TOTP factors first so repeated attempts don't pile up, then
// returns the QR + secret for the authenticator app. The factor stays
// "unverified" until verifyTotp() succeeds with a code.
export async function startTotpEnrollment(): Promise<EnrollResult> {
  const supabase = getSupabaseServer();

  const { data: factors } = await supabase.auth.mfa.listFactors();
  for (const f of factors?.all ?? []) {
    if (f.factor_type === "totp" && f.status === "unverified") {
      await supabase.auth.mfa.unenroll({ factorId: f.id });
    }
  }

  const { data, error } = await supabase.auth.mfa.enroll({
    factorType: "totp",
    friendlyName: "Authenticator",
    // `issuer` is what the authenticator app displays (as "Stonebridge:
    // <email>"). Without it Supabase falls back to the project Site URL host.
    issuer: "Stonebridge",
  });
  if (error || !data) {
    return { ok: false, error: error?.message ?? "Could not start enrollment." };
  }
  return {
    ok: true,
    factorId: data.id,
    qrCode: data.totp.qr_code,
    secret: data.totp.secret,
  };
}

// Challenge + verify a 6-digit code against a factor. Used for BOTH finishing
// enrollment (factor becomes verified) and the per-login challenge. On
// success the session is upgraded to aal2 and the cookie is rewritten.
export async function verifyTotp(
  factorId: string,
  code: string,
): Promise<VerifyResult> {
  const supabase = getSupabaseServer();

  const { data: challenge, error: chErr } = await supabase.auth.mfa.challenge({
    factorId,
  });
  if (chErr || !challenge) {
    return { ok: false, error: chErr?.message ?? "Could not start challenge." };
  }

  const { error: vErr } = await supabase.auth.mfa.verify({
    factorId,
    challengeId: challenge.id,
    code: code.replace(/\s/g, ""),
  });
  if (vErr) {
    return { ok: false, error: "That code didn't match. Try again." };
  }
  return { ok: true };
}
