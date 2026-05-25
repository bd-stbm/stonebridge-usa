"use client";

import { useEffect, useRef, useState, useTransition } from "react";
import { useRouter } from "next/navigation";
import { setAssetClassFilter } from "@/lib/actions";

interface Props {
  classes: string[];
  currentClasses: string[];
}

export default function AssetClassFilter({ classes, currentClasses }: Props) {
  const router = useRouter();
  const [open, setOpen] = useState(false);
  const [selection, setSelection] = useState<Set<string>>(
    () => new Set(currentClasses),
  );
  const [pending, startTransition] = useTransition();
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    setSelection(new Set(currentClasses));
  }, [currentClasses]);

  const cancel = () => {
    setSelection(new Set(currentClasses));
    setOpen(false);
  };

  const apply = () => {
    const next = Array.from(selection).sort();
    const prev = [...currentClasses].sort();
    const same =
      next.length === prev.length && next.every((c, i) => c === prev[i]);
    setOpen(false);
    if (!same) {
      startTransition(async () => {
        await setAssetClassFilter(next);
        router.refresh();
      });
    }
  };

  useEffect(() => {
    if (!open) return;
    const onMouseDown = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) cancel();
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
  }, [open, selection, currentClasses]);

  const toggle = (cls: string) => {
    setSelection(prev => {
      const next = new Set(prev);
      if (next.has(cls)) next.delete(cls);
      else next.add(cls);
      return next;
    });
  };

  const clearAll = () => setSelection(new Set());

  const label =
    currentClasses.length === 0
      ? "All asset classes"
      : currentClasses.length === 1
        ? currentClasses[0]
        : `${currentClasses.length} classes`;

  return (
    <div ref={ref} className="relative">
      <label className="flex items-center gap-2 text-xs text-slate-500">
        <span className="hidden sm:inline">Asset class</span>
        <button
          type="button"
          onClick={() => setOpen(o => !o)}
          disabled={pending}
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
              <span className="max-w-[160px] truncate">{label}</span>
              <span className="text-slate-400">▾</span>
            </>
          )}
        </button>
      </label>
      {open ? (
        <div className="absolute right-0 top-full z-20 mt-1 w-64 rounded-md border border-slate-200 bg-white shadow-lg">
          <div className="max-h-72 overflow-auto p-1">
            <label className="flex cursor-pointer items-center gap-2 rounded px-2 py-1.5 text-xs text-slate-700 hover:bg-slate-50">
              <input
                type="checkbox"
                checked={selection.size === 0}
                onChange={clearAll}
                className="h-3.5 w-3.5"
              />
              <span className="font-medium">All asset classes</span>
            </label>
            <div className="my-1 border-t border-slate-100" />
            {classes.map(c => (
              <label
                key={c}
                className="flex cursor-pointer items-center gap-2 rounded px-2 py-1.5 text-xs text-slate-700 hover:bg-slate-50"
              >
                <input
                  type="checkbox"
                  checked={selection.has(c)}
                  onChange={() => toggle(c)}
                  className="h-3.5 w-3.5"
                />
                <span className="truncate">{c}</span>
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
