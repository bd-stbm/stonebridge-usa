import { Position } from "@/lib/queries";
import { money, pct } from "@/lib/format";

interface Props {
  positions: Position[];
  limit?: number;
}

export default function HoldingsTable({ positions, limit = 10 }: Props) {
  const totalNav = positions.reduce((s, p) => s + (p.mv_reporting ?? 0), 0);
  const top = positions.slice(0, limit);

  return (
    <div className="overflow-x-auto rounded-lg border border-slate-200 bg-white">
      <table className="min-w-full text-sm">
        <thead className="bg-slate-50 text-xs uppercase tracking-wide text-slate-500">
          <tr>
            <th className="px-4 py-3 text-left">Asset</th>
            <th className="px-4 py-3 text-left">Ticker</th>
            <th className="px-4 py-3 text-left">Trust</th>
            <th className="px-4 py-3 text-right">Quantity</th>
            <th className="px-4 py-3 text-right">Price</th>
            <th className="px-4 py-3 text-right">Value</th>
            <th className="px-4 py-3 text-right">Weight</th>
            <th className="px-4 py-3 text-right">Unrealized G/L</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-100">
          {top.map((p, i) => {
            const weight = totalNav > 0 ? (p.mv_reporting ?? 0) / totalNav : 0;
            const gl = p.unrealized_gl_local ?? 0;
            return (
              <tr key={i} className="hover:bg-slate-50">
                <td className="px-4 py-3 font-medium text-slate-900">{p.asset_name}</td>
                <td className="px-4 py-3 text-slate-600">{p.ticker_masttro ?? "—"}</td>
                <td className="px-4 py-3 text-slate-600">{p.trust_alias ?? "—"}</td>
                <td className="px-4 py-3 text-right text-slate-700">
                  {(p.quantity ?? 0).toLocaleString()}
                </td>
                <td className="px-4 py-3 text-right text-slate-700">
                  {p.price_local != null ? money(p.price_local, "USD") : "—"}
                </td>
                <td className="px-4 py-3 text-right font-medium text-slate-900">
                  {money(p.mv_reporting, "USD")}
                </td>
                <td className="px-4 py-3 text-right text-slate-700">{pct(weight, 1)}</td>
                <td
                  className={
                    "px-4 py-3 text-right font-medium " +
                    (gl >= 0 ? "text-emerald-600" : "text-rose-600")
                  }
                >
                  {money(gl, "USD")}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
