"use client";

import { useState, useTransition } from "react";
import { useRouter } from "next/navigation";
import clsx from "clsx";
import { PERIODS, type PeriodKey, type PeriodReturn } from "@/lib/returns";
import type { IndexOption } from "@/lib/queries";
import { setBenchmark } from "@/lib/actions";
import { money, indexLabel } from "@/lib/format";

interface Props {
  returns: Record<PeriodKey, PeriodReturn>;
  indexReturns?: Record<PeriodKey, number | null>;
  benchmark?: IndexOption | null;
  availableBenchmarks?: IndexOption[];
  defaultPeriod?: PeriodKey;
  reportingCcy?: string;
}

function formatDate(iso: string | null): string {
  if (!iso) return "—";
  const d = new Date(iso + "T00:00:00Z");
  return d.toLocaleDateString("en-AU", {
    day: "2-digit",
    month: "short",
    year: "numeric",
    timeZone: "UTC",
  });
}

// Percentage with an explicit "+" on non-negative values (negatives already
// carry their sign from toFixed). Used for the hero return and benchmark line.
function signedPct(n: number): string {
  return `${n >= 0 ? "+" : ""}${(n * 100).toFixed(2)}%`;
}

export default function ReturnsTile({
  returns,
  indexReturns,
  benchmark = null,
  availableBenchmarks = [],
  defaultPeriod = "ytd",
  reportingCcy = "USD",
}: Props) {
  const [selected, setSelected] = useState<PeriodKey>(defaultPeriod);
  const [pending, startTransition] = useTransition();
  const router = useRouter();

  const r = returns[selected];
  const ir = indexReturns?.[selected] ?? null;

  const tone =
    r.return_pct == null
      ? "default"
      : r.return_pct >= 0
        ? "positive"
        : "negative";

  const delta = r.return_pct != null && ir != null ? r.return_pct - ir : null;
  const deltaTone =
    delta == null ? "default" : delta >= 0 ? "positive" : "negative";

  const showBenchmark = benchmark && availableBenchmarks.length > 0;

  // Compact reporting-currency formatter for the hero gain (e.g. "$47.25M").
  // The currency code propagates so an AUD client sees "A$…". The full,
  // unabbreviated value stays available via the title attribute.
  const compactMoney = new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: reportingCcy,
    notation: "compact",
    maximumFractionDigits: 2,
  });

  return (
    <div className="rounded-lg border border-slate-200 bg-white p-5">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex flex-wrap items-center gap-3">
          <div className="text-xs uppercase tracking-wide text-slate-500">
            Return
            {r.start_date && r.end_date ? (
              <span className="ml-2 text-[11px] normal-case tracking-normal text-slate-400">
                · {formatDate(r.start_date)} – {formatDate(r.end_date)}
              </span>
            ) : null}
          </div>
          <div className="flex rounded-full bg-slate-100 p-0.5">
            {PERIODS.map(p => (
              <button
                key={p.key}
                type="button"
                onClick={() => setSelected(p.key)}
                className={clsx(
                  "rounded-full px-2.5 py-1 text-xs font-medium transition-colors",
                  p.key === selected
                    ? "border border-slate-200 bg-white text-slate-900 shadow-sm"
                    : "border border-transparent text-slate-500 hover:text-slate-700",
                )}
              >
                {p.label}
              </button>
            ))}
          </div>
        </div>
        {showBenchmark ? (
          <select
            value={benchmark!.ticker}
            disabled={pending}
            onChange={e => {
              const value = e.target.value;
              startTransition(async () => {
                await setBenchmark(value);
                router.refresh();
              });
            }}
            className="rounded border border-slate-300 bg-white px-2 py-1 text-xs font-medium text-slate-700 disabled:opacity-60"
          >
            {availableBenchmarks.map(b => (
              <option key={b.ticker} value={b.ticker}>
                {indexLabel(b.ticker)}
              </option>
            ))}
          </select>
        ) : null}
      </div>

      <div className="mt-4 flex items-center justify-between gap-4">
        <div className="flex items-baseline gap-3">
          <span
            className={clsx(
              "text-4xl font-semibold tracking-tight",
              tone === "positive" && "text-emerald-600",
              tone === "negative" && "text-rose-600",
              tone === "default" && "text-slate-900",
            )}
          >
            {r.return_pct != null ? signedPct(r.return_pct) : "—"}
          </span>
          {r.gain != null ? (
            <span
              title={money(r.gain, reportingCcy)}
              className={clsx(
                "text-lg font-medium",
                tone === "positive" && "text-emerald-600/80",
                tone === "negative" && "text-rose-600/80",
                tone === "default" && "text-slate-500",
              )}
            >
              {r.gain >= 0 ? "+" : ""}
              {compactMoney.format(r.gain)}
            </span>
          ) : null}
        </div>

        {showBenchmark ? (
          <div className="border-l border-slate-200 pl-4 text-right">
            <div className="text-xs uppercase tracking-wide text-slate-400">
              vs {benchmark!.name}
            </div>
            <div
              className={clsx(
                "text-lg font-semibold",
                deltaTone === "positive" && "text-emerald-600",
                deltaTone === "negative" && "text-rose-600",
                deltaTone === "default" && "text-slate-500",
              )}
            >
              {delta != null
                ? `${delta >= 0 ? "+" : ""}${(delta * 100).toFixed(2)} pp`
                : "—"}
            </div>
            <div className="text-xs text-slate-500">
              benchmark {ir != null ? signedPct(ir) : "—"}
            </div>
          </div>
        ) : null}
      </div>
    </div>
  );
}
