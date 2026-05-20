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

  const cancel = () => {
    setSelection(new Set(currentAccounts));
    setOpen(false);
  };

  const apply = () => {
    const next = Array.from(selection).sort();
    const prev = [...currentAccounts].sort();
    const same =
      next.length === prev.length && next.every((t, i) => t === prev[i]);
    setOpen(false);
    if (!same) {
      startTransition(async () => {
        await setAccountFilter(next);
        router.refresh();
      });
    }
  };

  useEffect(() => {
    if (!open) return;
    const onMouseDown = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        cancel();
      }
    };
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") cancel();
      else if (e.key === "Enter") apply();
    };
    document.addEventListener("mousedown", onMouseDown);
    document.addEventListener("keydown", onKey);
    return () => {
      document.removeEventListener("mousedown", onMouseDown);
      document.removeEventListener("keydown", onKey);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open, selection, currentAccounts]);

  const toggle = (id: string) => {
    setSelection(prev => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const clearAll = () => setSelection(new Set());

  const currentAccount =
    currentAccounts.length === 1
      ? accounts.find(a => a.node_id === currentAccounts[0])
      : null;
  const label =
    currentAccounts.length === 0
      ? "All accounts"
      : currentAccount
        ? currentAccount.alias
        : `${currentAccounts.length} accounts`;

  return (
    <div ref={ref} className="relative">
      <label className="flex items-center gap-2 text-xs text-slate-500">
        <span className="hidden sm:inline">Account</span>
        <button
          type="button"
          onClick={() => setOpen(o => !o)}
          disabled={pending || accounts.length === 0}
          className="flex items-center gap-2 rounded border border-slate-300 bg-white px-2 py-1 text-xs text-slate-700 hover:bg-slate-50 disabled:cursor-wait disabled:opacity-70"
        >
          {pending ? (
            <>
              <span
                aria-hidden
                className="h-3 w-3 animate-spin rounded-full border-2 border-slate-300 border-t-brand"
              />
              <span>Updating…</span>
            </>
          ) : (
            <>
              <span className="max-w-[180px] truncate">{label}</span>
              <span className="text-slate-400">▾</span>
            </>
          )}
        </button>
      </label>
      {open ? (
        <div className="absolute right-0 top-full z-20 mt-1 w-80 rounded-md border border-slate-200 bg-white shadow-lg">
          <div className="max-h-72 overflow-auto p-1">
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
          <div className="flex items-center justify-end gap-2 border-t border-slate-100 px-2 py-2">
            <button
              type="button"
              onClick={cancel}
              className="rounded px-2 py-1 text-xs text-slate-600 hover:bg-slate-100"
            >
              Cancel
            </button>
            <button
              type="button"
              onClick={apply}
              className="rounded bg-brand px-3 py-1 text-xs font-medium text-white hover:bg-brand-dark"
            >
              Apply
            </button>
          </div>
        </div>
      ) : null}
    </div>
  );
}
