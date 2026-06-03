"use client";

import { useEffect, useState, useTransition } from "react";
import { startTotpEnrollment, verifyTotp } from "@/lib/mfa-actions";

export default function MfaEnroll() {
  const [factorId, setFactorId] = useState<string | null>(null);
  const [qrCode, setQrCode] = useState<string | null>(null);
  const [secret, setSecret] = useState<string | null>(null);
  const [code, setCode] = useState("");
  const [loadError, setLoadError] = useState<string | null>(null);
  const [verifyError, setVerifyError] = useState<string | null>(null);
  const [pending, startTransition] = useTransition();

  // Kick off enrollment once on mount to fetch the QR + secret.
  useEffect(() => {
    let active = true;
    startTotpEnrollment().then(res => {
      if (!active) return;
      if (res.ok) {
        setFactorId(res.factorId ?? null);
        setQrCode(res.qrCode ?? null);
        setSecret(res.secret ?? null);
      } else {
        setLoadError(res.error ?? "Could not start enrollment.");
      }
    });
    return () => {
      active = false;
    };
  }, []);

  const onVerify = () => {
    if (!factorId) return;
    startTransition(async () => {
      setVerifyError(null);
      const res = await verifyTotp(factorId, code);
      if (res.ok) {
        // Hard navigation so middleware re-evaluates the now-aal2 session.
        window.location.assign("/");
      } else {
        setVerifyError(res.error ?? "Verification failed.");
        setCode("");
      }
    });
  };

  const isSvgMarkup = qrCode?.trimStart().startsWith("<svg");

  return (
    <div className="space-y-5">
      <ol className="list-decimal space-y-1 pl-5 text-sm text-slate-600">
        <li>Install an authenticator app (Google Authenticator, Authy, 1Password…).</li>
        <li>Scan the QR code below, or enter the setup key manually.</li>
        <li>Enter the 6-digit code it shows to finish.</li>
      </ol>

      {loadError ? (
        <div className="rounded border border-rose-200 bg-rose-50 p-3 text-sm text-rose-700">
          {loadError}
        </div>
      ) : !qrCode ? (
        <div className="text-sm text-slate-400">Preparing your QR code…</div>
      ) : (
        <div className="flex flex-col items-center gap-3">
          <div className="rounded-lg border border-slate-200 bg-white p-3">
            {isSvgMarkup ? (
              <div
                className="h-44 w-44"
                // QR markup comes from Supabase, not user input.
                dangerouslySetInnerHTML={{ __html: qrCode }}
              />
            ) : (
              // eslint-disable-next-line @next/next/no-img-element
              <img src={qrCode} alt="Authenticator QR code" className="h-44 w-44" />
            )}
          </div>
          {secret ? (
            <div className="text-center text-xs text-slate-500">
              Or enter this key manually:
              <div className="mt-0.5 break-all font-mono text-slate-700">{secret}</div>
            </div>
          ) : null}
        </div>
      )}

      <div>
        <label className="block text-sm font-medium text-slate-700">
          6-digit code
          <input
            inputMode="numeric"
            autoComplete="one-time-code"
            value={code}
            onChange={e => setCode(e.target.value.replace(/\D/g, "").slice(0, 6))}
            placeholder="123456"
            className="mt-1 block w-full rounded border border-slate-300 px-3 py-2 text-center text-lg tracking-widest focus:border-brand focus:outline-none focus:ring-1 focus:ring-brand"
          />
        </label>
        {verifyError ? (
          <div className="mt-2 text-sm text-rose-600">{verifyError}</div>
        ) : null}
      </div>

      <button
        type="button"
        onClick={onVerify}
        disabled={pending || code.length !== 6 || !factorId}
        className="w-full rounded bg-brand px-3 py-2 text-sm font-medium text-white hover:bg-brand-dark disabled:cursor-not-allowed disabled:opacity-40"
      >
        {pending ? "Verifying…" : "Verify & enable"}
      </button>
    </div>
  );
}
