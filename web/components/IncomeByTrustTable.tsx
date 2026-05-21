import { money, pct } from "@/lib/format";

export interface TrustIncomeRow {
  trust_alias: string;
  ttm_income: number;
  ytd_income: number;
  last_month_income: number;
  current_nav: number;
}

interface Props {
  rows: TrustIncomeRow[];
  reportingCcy: string;
}

export default function IncomeByTrustTable({ rows, reportingCcy }: Props) {
  return (
    <div className="overflow-x-auto rounded-lg border border-slate-200 bg-white">
      <table className="min-w-full text-sm">
        <thead className="bg-slate-50 text-xs uppercase tracking-wide text-slate-500">
          <tr>
            <th className="px-4 py-3 text-left">Entity</th>
            <th className="px-4 py-3 text-right">TTM income</th>
            <th className="px-4 py-3 text-right">YTD income</th>
            <th className="px-4 py-3 text-right">Last month</th>
            <th className="px-4 py-3 text-right">Current NAV</th>
            <th className="px-4 py-3 text-right">TTM yield</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-100">
          {rows.length === 0 ? (
            <tr>
              <td colSpan={6} className="px-4 py-8 text-center text-sm text-slate-500">
                No income recorded in the last 12 months.
              </td>
            </tr>
          ) : (
            rows.map(r => {
              const yld = r.current_nav > 0 ? r.ttm_income / r.current_nav : null;
              return (
                <tr key={r.trust_alias} className="hover:bg-slate-50">
                  <td className="px-4 py-3 font-medium text-slate-900">
                    {r.trust_alias}
                  </td>
                  <td className="px-4 py-3 text-right font-medium text-slate-900">
                    {money(r.ttm_income, reportingCcy)}
                  </td>
                  <td className="px-4 py-3 text-right text-slate-700">
                    {money(r.ytd_income, reportingCcy)}
                  </td>
                  <td className="px-4 py-3 text-right text-slate-700">
                    {money(r.last_month_income, reportingCcy)}
                  </td>
                  <td className="px-4 py-3 text-right text-slate-700">
                    {r.current_nav > 0 ? money(r.current_nav, reportingCcy) : "—"}
                  </td>
                  <td className="px-4 py-3 text-right text-slate-700">
                    {yld != null ? pct(yld, 2) : "—"}
                  </td>
                </tr>
              );
            })
          )}
        </tbody>
      </table>
    </div>
  );
}
