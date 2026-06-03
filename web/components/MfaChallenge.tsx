"use client";

import { useState, useTransition } from "react";
import { verifyTotp } from "@/lib/mfa-actions";

export default function MfaChallenge({ factorId }: { factorId: string }) {
  const [code, setCode] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [pending, startTransition] = useTransition();

  const onVerify = () =>
    startTransition(async () => {
      setError(null);
      const res = await verifyTotp(factorId, code);
      if (res.ok) {
        window.location.assign("/");
      } else {
        setError(res.error ?? "Verification failed.");
        setCode("");
      }
    });

  return (
    <div className="space-y-4">
      <label className="block text-sm font-medium text-slate-700">
        Authenticator code
        <input
          inputMode="numeric"
          autoComplete="one-time-code"
          autoFocus
          value={code}
          onChange={e => setCode(e.target.value.replace(/\D/g, "").slice(0, 6))}
          placeholder="123456"
          className="mt-1 block w-full rounded border border-slate-300 px-3 py-2 text-center text-lg tracking-widest focus:border-brand focus:outline-none focus:ring-1 focus:ring-brand"
        />
      </label>
      {error ? <div className="text-sm text-rose-600">{error}</div> : null}
      <button
        type="button"
        onClick={onVerify}
        disabled={pending || code.length !== 6}
        className="w-full rounded bg-brand px-3 py-2 text-sm font-medium text-white hover:bg-brand-dark disabled:cursor-not-allowed disabled:opacity-40"
      >
        {pending ? "Verifying…" : "Verify"}
      </button>
    </div>
  );
}
