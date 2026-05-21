"use client";

import { useMemo, useState, useTransition } from "react";
import { useRouter } from "next/navigation";
import clsx from "clsx";
import { PERIODS, type PeriodKey, type PeriodReturn } from "@/lib/returns";
import type { IndexOption } from "@/lib/queries";
import { setBenchmark } from "@/lib/actions";
import { money, pct } from "@/lib/format";

const TOTAL = "__total__";

interface Props {
  returns: Record<PeriodKey, PeriodReturn>;
  indexReturns?: Record<PeriodKey, number | null>;
  benchmark?: IndexOption | null;
  availableBenchmarks?: IndexOption[];
  // returnsByAssetClass: { Equity: {...}, "Fixed Income": {...}, ... }
  returnsByAssetClass?: Record<string, Record<PeriodKey, PeriodReturn>>;
  indexReturnsByAssetClass?: Record<string, Record<PeriodKey, number | null>>;
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

export default function ReturnsTile({
  returns,
  indexReturns,
  benchmark = null,
  availableBenchmarks = [],
  returnsByAssetClass = {},
  indexReturnsByAssetClass = {},
  defaultPeriod = "ytd",
  reportingCcy = "USD",
}: Props) {
  const [selected, setSelected] = useState<PeriodKey>(defaultPeriod);
  const [assetClass, setAssetClass] = useState<string>(TOTAL);
  const [pending, startTransition] = useTransition();
  const router = useRouter();

  // Asset-class options are sorted alphabetically, with "Total" pinned first.
  const assetClassOptions = useMemo(
    () => Object.keys(returnsByAssetClass).sort((a, b) => a.localeCompare(b)),
    [returnsByAssetClass],
  );

  const isClassView = assetClass !== TOTAL;
  const activeReturns: Record<PeriodKey, PeriodReturn> = isClassView
    ? (returnsByAssetClass[assetClass] ?? returns)
    : returns;
  const activeIndexReturns: Record<PeriodKey, number | null> | undefined =
    isClassView
      ? indexReturnsByAssetClass[assetClass]
      : indexReturns;

  const r = activeReturns[selected];
  const ir = activeIndexReturns?.[selected] ?? null;

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

  return (
    <div className="rounded-lg border border-slate-200 bg-white p-5">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="text-xs uppercase tracking-wide text-slate-500">
          Return
        </div>
        <div className="flex flex-wrap items-center gap-3">
          <div className="flex gap-1">
            {PERIODS.map(p => (
              <button
                key={p.key}
                type="button"
                onClick={() => setSelected(p.key)}
                className={clsx(
                  "rounded px-2 py-0.5 text-xs font-medium",
                  p.key === selected
                    ? "bg-brand text-white"
                    : "text-slate-500 hover:bg-slate-100",
                )}
              >
                {p.label}
              </button>
            ))}
          </div>
          {assetClassOptions.length > 0 ? (
            <select
              value={assetClass}
              onChange={e => setAssetClass(e.target.value)}
              className="rounded border border-slate-300 bg-white px-2 py-1 text-xs font-medium text-slate-700"
            >
              <option value={TOTAL}>Total</option>
              {assetClassOptions.map(ac => (
                <option key={ac} value={ac}>
                  {ac}
                </option>
              ))}
            </select>
          ) : null}
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
                  {b.ticker}
                </option>
              ))}
            </select>
          ) : null}
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
        {r.gain != null ? (
          <span
            className={clsx(
              "ml-2 text-sm font-medium",
              tone === "positive" && "text-emerald-600/80",
              tone === "negative" && "text-rose-600/80",
              tone === "default" && "text-slate-500",
            )}
          >
            {r.gain >= 0 ? "+" : ""}
            {money(r.gain, reportingCcy)}
          </span>
        ) : null}
      </div>
      <div className="mt-1 text-xs text-slate-400">
        {r.start_date && r.end_date
          ? `${formatDate(r.start_date)} → ${formatDate(r.end_date)}`
          : "Insufficient history"}
        {isClassView ? <> · {assetClass} · price-only</> : null}
      </div>
      {showBenchmark ? (
        <div className="mt-2 border-t border-slate-100 pt-2 text-xs text-slate-500">
          vs <span className="font-medium text-slate-700">{benchmark!.name}</span>:{" "}
          <span className="text-slate-700">{ir != null ? pct(ir, 2) : "—"}</span>
          {delta != null ? (
            <span
              className={clsx(
                "ml-2 font-medium",
                deltaTone === "positive" && "text-emerald-600",
                deltaTone === "negative" && "text-rose-600",
              )}
            >
              ({delta >= 0 ? "+" : ""}
              {(delta * 100).toFixed(2)} pp)
            </span>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}
