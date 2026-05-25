import clsx from "clsx";
import { PERIODS, type PeriodKey, type PeriodReturn } from "@/lib/returns";
import { pct } from "@/lib/format";

interface Props {
  title: string;
  rowLabel: string; // e.g. "Entity" or "Asset class"
  returns: Record<string, Record<PeriodKey, PeriodReturn>>;
  navAtToday: Record<string, number>; // ordering hint — sort by NAV desc
  // Same-shape map keyed identically to `returns`, used for the bottom
  // benchmark row + optional "vs" tone tweaks. Optional.
  indexReturns?: Record<PeriodKey, number | null>;
  benchmarkLabel?: string;
}

function cellTone(v: number | null | undefined): string {
  if (v == null) return "text-slate-400";
  if (v >= 0) return "text-emerald-600";
  return "text-rose-600";
}

export default function PerformanceMatrix({
  title,
  rowLabel,
  returns,
  navAtToday,
  indexReturns,
  benchmarkLabel,
}: Props) {
  const rowKeys = Object.keys(returns).sort(
    (a, b) => (navAtToday[b] ?? 0) - (navAtToday[a] ?? 0),
  );

  return (
    <section>
      <div className="mb-3 flex items-baseline justify-between">
        <h2 className="text-base font-semibold text-slate-900">{title}</h2>
      </div>
      <div className="overflow-x-auto rounded-lg border border-slate-200 bg-white">
        <table className="min-w-full text-sm">
          <thead className="bg-slate-50 text-xs uppercase tracking-wide text-slate-500">
            <tr>
              <th className="px-4 py-3 text-left">{rowLabel}</th>
              {PERIODS.map(p => (
                <th key={p.key} className="px-4 py-3 text-right">
                  {p.label}
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-100">
            {rowKeys.length === 0 ? (
              <tr>
                <td
                  colSpan={1 + PERIODS.length}
                  className="px-4 py-8 text-center text-sm text-slate-500"
                >
                  No data in scope.
                </td>
              </tr>
            ) : (
              rowKeys.map(key => (
                <tr key={key} className="hover:bg-slate-50">
                  <td className="px-4 py-3 font-medium text-slate-900">
                    {key}
                  </td>
                  {PERIODS.map(p => {
                    const r = returns[key][p.key];
                    const v = r?.return_pct;
                    return (
                      <td
                        key={p.key}
                        className={clsx(
                          "px-4 py-3 text-right font-medium",
                          cellTone(v),
                        )}
                      >
                        {v != null ? pct(v, 2) : "—"}
                      </td>
                    );
                  })}
                </tr>
              ))
            )}
          </tbody>
          {indexReturns && benchmarkLabel ? (
            <tfoot className="bg-slate-50 text-sm">
              <tr className="border-t border-slate-200">
                <td className="px-4 py-3 text-xs font-medium uppercase tracking-wide text-slate-500">
                  {benchmarkLabel}
                </td>
                {PERIODS.map(p => {
                  const v = indexReturns[p.key];
                  return (
                    <td
                      key={p.key}
                      className={clsx(
                        "px-4 py-3 text-right font-medium",
                        cellTone(v),
                      )}
                    >
                      {v != null ? pct(v, 2) : "—"}
                    </td>
                  );
                })}
              </tr>
            </tfoot>
          ) : null}
        </table>
      </div>
    </section>
  );
}
