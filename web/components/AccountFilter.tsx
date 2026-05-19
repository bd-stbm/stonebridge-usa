"use client";

import { useEffect, useRef, useState, useTransition } from "react";
import { useRouter } from "next/navigation";
import { setAccountFilter } from "@/lib/actions";
import type { AccountOption } from "@/lib/queries";

interface Props {
  accounts: AccountOption[];
  currentAccounts: string[];
}

export default function AccountFilter({ accounts, currentAccounts }: Props) {
  const router = useRouter();
  const [open, setOpen] = useState(false);
  const [selection, setSelection] = useState<Set<string>>(
    () => new Set(currentAccounts),
  );
  const [pending, startTransition] = useTransition();
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    setSelection(new Set(currentAccounts));
  }, [currentAccounts]);

  useEffect(() => {
    if (!open) return;
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        closeAndApply();
      }
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open, selection]);

  const closeAndApply = () => {
    setOpen(false);
    const next = Array.from(selection).sort();
    const prev = [...currentAccounts].sort();
    const same =
      next.length === prev.length && next.every((t, i) => t === prev[i]);
    if (!same) {
      startTransition(async () => {
        await setAccountFilter(next);
        router.refresh();
      });
    }
  };

  const toggle = (id: string) => {
    setSelection(prev => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const clearAll = () => setSelection(new Set());

  const selectedAccount =
    selection.size === 1
      ? accounts.find(a => a.node_id === Array.from(selection)[0])
      : null;
  const label =
    selection.size === 0
      ? "All accounts"
      : selectedAccount
        ? selectedAccount.alias
        : `${selection.size} accounts`;

  return (
    <div ref={ref} className="relative">
      <label className="flex items-center gap-2 text-xs text-slate-500">
        <span className="hidden sm:inline">Account</span>
        <button
          type="button"
          onClick={() => setOpen(o => !o)}
          disabled={pending || accounts.length === 0}
          className="flex items-center gap-2 rounded border border-slate-300 bg-white px-2 py-1 text-xs text-slate-700 hover:bg-slate-50 disabled:opacity-60"
        >
          <span className="max-w-[180px] truncate">{label}</span>
          <span className="text-slate-400">▾</span>
        </button>
      </label>
      {open ? (
        <div className="absolute right-0 top-full z-20 mt-1 max-h-80 w-80 overflow-auto rounded-md border border-slate-200 bg-white p-1 shadow-lg">
          <label className="flex cursor-pointer items-center gap-2 rounded px-2 py-1.5 text-xs text-slate-700 hover:bg-slate-50">
            <input
              type="checkbox"
              checked={selection.size === 0}
              onChange={clearAll}
              className="h-3.5 w-3.5"
            />
            <span className="font-medium">All accounts</span>
          </label>
          <div className="my-1 border-t border-slate-100" />
          {accounts.map(a => (
            <label
              key={a.node_id}
              className="flex cursor-pointer items-center gap-2 rounded px-2 py-1.5 text-xs text-slate-700 hover:bg-slate-50"
            >
              <input
                type="checkbox"
                checked={selection.has(a.node_id)}
                onChange={() => toggle(a.node_id)}
                className="h-3.5 w-3.5"
              />
              <span className="min-w-0 flex-1 truncate">
                {a.alias}
                {a.custodian ? (
                  <span className="ml-1 text-slate-400">· {a.custodian}</span>
                ) : null}
              </span>
            </label>
          ))}
        </div>
      ) : null}
    </div>
  );
}
