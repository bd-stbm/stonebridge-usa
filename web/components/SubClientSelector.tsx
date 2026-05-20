"use client";

import { useEffect, useRef, useState, useTransition } from "react";
import { useRouter } from "next/navigation";
import { setSubClient } from "@/lib/actions";

interface Props {
  subClients: string[];
  currentSubClient: string;
}

export default function SubClientSelector({
  subClients,
  currentSubClient,
}: Props) {
  const router = useRouter();
  const [open, setOpen] = useState(false);
  const [selection, setSelection] = useState<string>(currentSubClient);
  const [pending, startTransition] = useTransition();
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    setSelection(currentSubClient);
  }, [currentSubClient]);

  const cancel = () => {
    setSelection(currentSubClient);
    setOpen(false);
  };

  const apply = () => {
    setOpen(false);
    if (selection !== currentSubClient) {
      startTransition(async () => {
        await setSubClient(selection);
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
  }, [open, selection, currentSubClient]);

  return (
    <div ref={ref} className="relative">
      <label className="flex items-center gap-2 text-xs text-slate-500">
        <span className="hidden sm:inline">Family</span>
        <button
          type="button"
          onClick={() => setOpen(o => !o)}
          disabled={pending || subClients.length <= 1}
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
              <span className="max-w-[200px] truncate">{currentSubClient}</span>
              {subClients.length > 1 ? (
                <span className="text-slate-400">▾</span>
              ) : null}
            </>
          )}
        </button>
      </label>
      {open ? (
        <div className="absolute right-0 top-full z-20 mt-1 w-80 rounded-md border border-slate-200 bg-white shadow-lg">
          <div className="max-h-72 overflow-auto p-1">
            {subClients.map(sc => (
              <label
                key={sc}
                className="flex cursor-pointer items-center gap-2 rounded px-2 py-1.5 text-xs text-slate-700 hover:bg-slate-50"
              >
                <input
                  type="radio"
                  name="sub-client"
                  checked={selection === sc}
                  onChange={() => setSelection(sc)}
                  className="h-3.5 w-3.5"
                />
                <span className="min-w-0 flex-1 truncate">{sc}</span>
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
