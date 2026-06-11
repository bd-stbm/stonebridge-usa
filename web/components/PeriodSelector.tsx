"use client";

import { useTransition } from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { RETURN_PERIODS } from "@/lib/networth";

interface Props {
  current: number;
}

// Period toggle for the Net Worth return column. Writes ?period=<code> so the
// server component re-renders with the selected period's returns.
export default function PeriodSelector({ current }: Props) {
  const router = useRouter();
  const pathname = usePathname();
  const params = useSearchParams();
  const [pending, startTransition] = useTransition();

  const select = (code: number) => {
    const next = new URLSearchParams(params.toString());
    next.set("period", String(code));
    startTransition(() => router.replace(`${pathname}?${next.toString()}`));
  };

  return (
    <div className="inline-flex items-center gap-2 text-xs text-slate-500">
      <span>Return</span>
      <div className="inline-flex rounded-md border border-slate-300 bg-white p-0.5">
        {RETURN_PERIODS.map(p => (
          <button
            key={p.code}
            type="button"
            onClick={() => select(p.code)}
            disabled={pending}
            className={
              "rounded px-2.5 py-1 transition disabled:opacity-60 " +
              (p.code === current
                ? "bg-brand text-white"
                : "text-slate-600 hover:bg-slate-50")
            }
          >
            {p.label}
          </button>
        ))}
      </div>
    </div>
  );
}
