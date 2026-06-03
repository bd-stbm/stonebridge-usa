"use client";

import { useState, useTransition } from "react";
import clsx from "clsx";
import { changeOwnPassword } from "@/lib/account-actions";

export default function ChangePasswordForm() {
  const [current, setCurrent] = useState("");
  const [next, setNext] = useState("");
  const [confirm, setConfirm] = useState("");
  const [pending, startTransition] = useTransition();
  const [msg, setMsg] = useState<{ tone: "ok" | "err"; text: string } | null>(null);

  const mismatch = confirm.length > 0 && next !== confirm;
  const canSubmit =
    current.length > 0 && next.length >= 8 && next === confirm && !pending;

  const onSubmit = () =>
    startTransition(async () => {
      setMsg(null);
      const res = await changeOwnPassword(current, next);
      if (res.ok) {
        setMsg({ tone: "ok", text: "Password updated." });
        setCurrent("");
        setNext("");
        setConfirm("");
      } else {
        setMsg({ tone: "err", text: res.error ?? "Could not change password." });
      }
    });

  const field =
    "mt-1 block w-full rounded border border-slate-300 px-3 py-2 text-sm focus:border-brand focus:outline-none focus:ring-1 focus:ring-brand";

  return (
    <div className="rounded-lg border border-slate-200 bg-white p-6">
      <label className="block text-sm font-medium text-slate-700">
        Current password
        <input
          type="password"
          autoComplete="current-password"
          value={current}
          onChange={e => setCurrent(e.target.value)}
          className={field}
        />
      </label>
      <label className="mt-4 block text-sm font-medium text-slate-700">
        New password
        <input
          type="password"
          autoComplete="new-password"
          value={next}
          onChange={e => setNext(e.target.value)}
          className={field}
        />
        <span className="mt-1 block text-xs text-slate-400">
          At least 8 characters.
        </span>
      </label>
      <label className="mt-4 block text-sm font-medium text-slate-700">
        Confirm new password
        <input
          type="password"
          autoComplete="new-password"
          value={confirm}
          onChange={e => setConfirm(e.target.value)}
          className={field}
        />
        {mismatch ? (
          <span className="mt-1 block text-xs text-rose-600">
            Passwords don&apos;t match.
          </span>
        ) : null}
      </label>

      {msg ? (
        <div
          className={clsx(
            "mt-4 text-sm",
            msg.tone === "ok" ? "text-emerald-600" : "text-rose-600",
          )}
        >
          {msg.text}
        </div>
      ) : null}

      <button
        type="button"
        onClick={onSubmit}
        disabled={!canSubmit}
        className="mt-5 w-full rounded bg-brand px-3 py-2 text-sm font-medium text-white hover:bg-brand-dark disabled:cursor-not-allowed disabled:opacity-40"
      >
        {pending ? "Updating…" : "Update password"}
      </button>
    </div>
  );
}
