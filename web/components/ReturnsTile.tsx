"use client";

import { useState } from "react";
import clsx from "clsx";
import { PERIODS, type PeriodKey, type PeriodReturn } from "@/lib/returns";
import { pct } from "@/lib/format";

interface Props {
  returns: Record<PeriodKey, PeriodReturn>;
  defaultPeriod?: PeriodKey;
}

function formatDate(iso: string | null): string {
  if (!iso) return "—";
  // yyyy-mm-dd → "DD MMM YYYY"
  const d = new Date(iso + "T00:00:00Z");
  return d.toLocaleDateString("en-AU", {
    day: "2-digit",
    month: "short",
    year: "numeric",
    timeZone: "UTC",
  });
}

export default function ReturnsTile({
  returns,
  defaultPeriod = "ytd",
}: Props) {
  const [selected, setSelected] = useState<PeriodKey>(defaultPeriod);
  const r = returns[selected];

  const tone =
    r.return_pct == null
      ? "default"
      : r.return_pct >= 0
        ? "positive"
        : "negative";

  return (
    <div className="rounded-lg border border-slate-200 bg-white p-5">
      <div className="flex items-center justify-between">
        <div className="text-xs uppercase tracking-wide text-slate-500">
          Return
        </div>
        <div className="flex gap-1">
          {PERIODS.map(p => (
            <button
              key={p.key}
              type="button"
              onClick={() => setSelected(p.key)}
              className={clsx(
                "rounded px-2 py-0.5 text-xs font-medium",
                p.key === selected
                  ? "bg-slate-900 text-white"
                  : "text-slate-500 hover:bg-slate-100",
              )}
            >
              {p.label}
            </button>
          ))}
        </div>
      </div>
      <div
        className={clsx(
          "mt-2 text-2xl font-semibold",
          tone === "positive" && "text-emerald-600",
          tone === "negative" && "text-rose-600",
          tone === "default" && "text-slate-900",
        )}
      >
        {r.return_pct != null ? pct(r.return_pct, 2) : "—"}
      </div>
      <div className="mt-1 text-xs text-slate-400">
        {r.start_date && r.end_date
          ? `${formatDate(r.start_date)} → ${formatDate(r.end_date)}`
          : "Insufficient history"}
      </div>
    </div>
  );
}
