import { money } from "@/lib/format";
import type { BreakdownRow } from "@/lib/networth";

interface Props {
  rows: BreakdownRow[];
  reportingCcy: string;
}

// Net worth grouped by Entity (the tool's existing entity grain), with listed
// vs non-listed split.
export default function NetWorthBreakdown({ rows, reportingCcy }: Props) {
  return (
    <div className="space-y-3">
      <h2 className="text-base font-semibold text-slate-900">Net worth by entity</h2>
      <div className="overflow-x-auto rounded-lg border border-slate-200 bg-white">
        <table className="min-w-full text-sm">
          <thead className="bg-slate-50 text-xs uppercase tracking-wide text-slate-500">
            <tr>
              <th className="sticky left-0 z-10 bg-slate-50 px-4 py-3 text-left">Entity</th>
              <th className="px-4 py-3 text-right">Listed</th>
              <th className="px-4 py-3 text-right">Non-listed</th>
              <th className="px-4 py-3 text-right">Total</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-100">
            {rows.length === 0 ? (
              <tr>
                <td colSpan={4} className="px-4 py-8 text-center text-sm text-slate-500">
                  Nothing in scope.
                </td>
              </tr>
            ) : (
              rows.map(r => (
                <tr key={r.key} className="group hover:bg-slate-50">
                  <td className="sticky left-0 z-10 bg-white px-4 py-3 font-medium text-slate-900 group-hover:bg-slate-50">
                    {r.key}
                  </td>
                  <td className="px-4 py-3 text-right text-slate-700">
                    {r.listed ? money(r.listed, reportingCcy) : "—"}
                  </td>
                  <td className="px-4 py-3 text-right text-slate-700">
                    {r.nonListed ? money(r.nonListed, reportingCcy) : "—"}
                  </td>
                  <td className="px-4 py-3 text-right font-medium text-slate-900">
                    {money(r.total, reportingCcy)}
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
