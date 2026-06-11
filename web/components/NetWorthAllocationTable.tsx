import { money, pct } from "@/lib/format";
import type { AllocationSummary } from "@/lib/networth";

interface Props {
  summary: AllocationSummary;
  periodLabel: string;
  totalReturn: number | null;
}

function ReturnCell({ value }: { value: number | null | undefined }) {
  if (value == null) {
    return <td className="px-4 py-3 text-right text-slate-400">—</td>;
  }
  const tone =
    value > 0 ? "text-emerald-600" : value < 0 ? "text-rose-600" : "text-slate-700";
  return (
    <td className={`px-4 py-3 text-right ${tone}`}>
      {value > 0 ? "+" : ""}
      {pct(value, 1)}
    </td>
  );
}

// Allocation by Masttro category, matching the Masttro UI layout: each asset
// class with its share of Total Assets, market value, and the blended return for
// the selected period, then Total Assets, Loan Payable, Net Worth.
export default function NetWorthAllocationTable({
  summary,
  periodLabel,
  totalReturn,
}: Props) {
  const { categories, totalAssets, loanPayable, netWorth, reportingCcy } = summary;
  return (
    <div className="overflow-x-auto rounded-lg border border-slate-200 bg-white">
      <table className="min-w-full text-sm">
        <thead className="bg-slate-50 text-xs uppercase tracking-wide text-slate-500">
          <tr>
            <th className="px-4 py-3 text-left">Asset class</th>
            <th className="px-4 py-3 text-right">Allocation</th>
            <th className="px-4 py-3 text-right">Market value ({reportingCcy})</th>
            <th className="px-4 py-3 text-right">Return ({periodLabel})</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-100">
          {categories.length === 0 ? (
            <tr>
              <td colSpan={4} className="px-4 py-8 text-center text-sm text-slate-500">
                No assets in scope.
              </td>
            </tr>
          ) : (
            categories.map(c => (
              <tr key={c.asset_class} className="hover:bg-slate-50">
                <td className="px-4 py-3 font-medium text-slate-900">{c.asset_class}</td>
                <td className="px-4 py-3 text-right text-slate-700">{pct(c.pct, 2)}</td>
                <td className="px-4 py-3 text-right text-slate-700">
                  {money(c.mv, reportingCcy)}
                </td>
                <ReturnCell value={c.periodReturn} />
              </tr>
            ))
          )}
        </tbody>
        <tfoot className="border-t-2 border-slate-200 text-sm">
          <tr className="bg-slate-50 font-semibold text-slate-900">
            <td className="px-4 py-3">Total assets</td>
            <td className="px-4 py-3 text-right">{pct(1, 2)}</td>
            <td className="px-4 py-3 text-right">{money(totalAssets, reportingCcy)}</td>
            <ReturnCell value={totalReturn} />
          </tr>
          {loanPayable !== 0 ? (
            <tr className="text-slate-700">
              <td className="px-4 py-3">Loan payable</td>
              <td className="px-4 py-3" />
              <td className="px-4 py-3 text-right text-rose-600">
                {money(loanPayable, reportingCcy)}
              </td>
              <td className="px-4 py-3" />
            </tr>
          ) : null}
          <tr className="bg-brand-tint/40 font-semibold text-slate-900">
            <td className="px-4 py-3">Net worth</td>
            <td className="px-4 py-3" />
            <td className="px-4 py-3 text-right">{money(netWorth, reportingCcy)}</td>
            <td className="px-4 py-3" />
          </tr>
        </tfoot>
      </table>
    </div>
  );
}
