"use client";

import { useEffect, useRef, useState, useTransition } from "react";
import { useRouter } from "next/navigation";
import { setTrustFilter } from "@/lib/actions";

interface Props {
  trusts: string[];
  currentTrusts: string[];
}

export default function TrustFilter({ trusts, currentTrusts }: Props) {
  const router = useRouter();
  const [open, setOpen] = useState(false);
  const [selection, setSelection] = useState<Set<string>>(
    () => new Set(currentTrusts),
  );
  const [pending, startTransition] = useTransition();
  const ref = useRef<HTMLDivElement>(null);

  // Keep local selection in sync if the URL/cookie value changes externally
  // (e.g. after a different filter clears the account).
  useEffect(() => {
    setSelection(new Set(currentTrusts));
  }, [currentTrusts]);

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
    const prev = [...currentTrusts].sort();
    const same =
      next.length === prev.length && next.every((t, i) => t === prev[i]);
    if (!same) {
      startTransition(async () => {
        await setTrustFilter(next);
        router.refresh();
      });
    }
  };

  const toggle = (trust: string) => {
    setSelection(prev => {
      const next = new Set(prev);
      if (next.has(trust)) next.delete(trust);
      else next.add(trust);
      return next;
    });
  };

  const clearAll = () => setSelection(new Set());

  const label =
    selection.size === 0
      ? "All trusts"
      : selection.size === 1
        ? Array.from(selection)[0]
        : `${selection.size} trusts`;

  return (
    <div ref={ref} className="relative">
      <label className="flex items-center gap-2 text-xs text-slate-500">
        <span className="hidden sm:inline">Trust</span>
        <button
          type="button"
          onClick={() => setOpen(o => !o)}
          disabled={pending}
          className="flex items-center gap-2 rounded border border-slate-300 bg-white px-2 py-1 text-xs text-slate-700 hover:bg-slate-50 disabled:opacity-60"
        >
          <span className="max-w-[160px] truncate">{label}</span>
          <span className="text-slate-400">▾</span>
        </button>
      </label>
      {open ? (
        <div className="absolute right-0 top-full z-20 mt-1 max-h-80 w-72 overflow-auto rounded-md border border-slate-200 bg-white p-1 shadow-lg">
          <label className="flex cursor-pointer items-center gap-2 rounded px-2 py-1.5 text-xs text-slate-700 hover:bg-slate-50">
            <input
              type="checkbox"
              checked={selection.size === 0}
              onChange={clearAll}
              className="h-3.5 w-3.5"
            />
            <span className="font-medium">All trusts</span>
          </label>
          <div className="my-1 border-t border-slate-100" />
          {trusts.map(t => (
            <label
              key={t}
              className="flex cursor-pointer items-center gap-2 rounded px-2 py-1.5 text-xs text-slate-700 hover:bg-slate-50"
            >
              <input
                type="checkbox"
                checked={selection.has(t)}
                onChange={() => toggle(t)}
                className="h-3.5 w-3.5"
              />
              <span className="truncate">{t}</span>
            </label>
          ))}
        </div>
      ) : null}
    </div>
  );
}
