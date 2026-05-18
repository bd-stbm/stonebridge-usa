import { money, pct } from "@/lib/format";

export interface PayerRow {
  security_id: number | null;
  asset_name: string;
  asset_class: string | null;
  ticker_masttro: string | null;
  ttm_income: number;
  current_mv: number; // 0 if security no longer held
  weight_of_income: number; // ttm_income / total ttm income
}

interface Props {
  rows: PayerRow[];
  reportingCcy: string;
  limit?: number;
}

export default function TopPayersTable({ rows, reportingCcy, limit = 15 }: Props) {
  const top = rows.slice(0, limit);

  return (
    <div className="overflow-x-auto rounded-lg border border-slate-200 bg-white">
      <table className="min-w-full text-sm">
        <thead className="bg-slate-50 text-xs uppercase tracking-wide text-slate-500">
          <tr>
            <th className="px-4 py-3 text-left">Security</th>
            <th className="px-4 py-3 text-left">Ticker</th>
            <th className="px-4 py-3 text-left">Asset class</th>
            <th className="px-4 py-3 text-right">TTM income</th>
            <th className="px-4 py-3 text-right">% of income</th>
            <th className="px-4 py-3 text-right">Current MV</th>
            <th className="px-4 py-3 text-right">TTM yield</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-100">
          {top.length === 0 ? (
            <tr>
              <td colSpan={7} className="px-4 py-8 text-center text-sm text-slate-500">
                No income recorded in the last 12 months.
              </td>
            </tr>
          ) : (
            top.map((p, i) => {
              const yld = p.current_mv > 0 ? p.ttm_income / p.current_mv : null;
              return (
                <tr key={p.security_id ?? i} className="hover:bg-slate-50">
                  <td className="px-4 py-3 font-medium text-slate-900">
                    {p.asset_name}
                  </td>
                  <td className="px-4 py-3 text-slate-600">
                    {p.ticker_masttro ?? "—"}
                  </td>
                  <td className="px-4 py-3 text-slate-600">
                    {p.asset_class ?? "—"}
                  </td>
                  <td className="px-4 py-3 text-right font-medium text-slate-900">
                    {money(p.ttm_income, reportingCcy)}
                  </td>
                  <td className="px-4 py-3 text-right text-slate-700">
                    {pct(p.weight_of_income, 1)}
                  </td>
                  <td className="px-4 py-3 text-right text-slate-700">
                    {p.current_mv > 0 ? money(p.current_mv, reportingCcy) : "—"}
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
