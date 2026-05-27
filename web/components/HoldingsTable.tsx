import type { Position } from "@/lib/queries";
import { money, pct, price } from "@/lib/format";

interface Props {
  positions: Position[];
  limit?: number;
  reportingCcy?: string;
}

interface AggregatedHolding {
  key: string;
  asset_name: string;
  ticker_masttro: string | null;
  asset_class: string | null;
  local_ccy: string | null;
  quantity: number;
  price_display: number | null;
  mv_reporting: number;
}

const CASH_ASSET_CLASS = "Cash and Equivalents";

function aggregateByHolding(positions: Position[]): AggregatedHolding[] {
  // Roll up the same security across accounts/trusts so "Top holdings"
  // reflects total exposure per security rather than per custody line.
  // Group key prefers ticker (same security across custodians joins
  // cleanly), falling back to asset_name for alternatives without one.
  const map = new Map<string, AggregatedHolding>();
  for (const p of positions) {
    const key = p.ticker_masttro ?? p.asset_name;
    const existing = map.get(key);
    const px = p.yf_price ?? p.price_local;
    if (existing) {
      existing.quantity += Number(p.quantity ?? 0);
      existing.mv_reporting += Number(p.mv_reporting ?? 0);
      if (existing.price_display == null && px != null) existing.price_display = px;
      if (existing.local_ccy == null && p.local_ccy) existing.local_ccy = p.local_ccy;
    } else {
      map.set(key, {
        key,
        asset_name: p.asset_name,
        ticker_masttro: p.ticker_masttro,
        asset_class: p.asset_class,
        local_ccy: p.local_ccy,
        quantity: Number(p.quantity ?? 0),
        price_display: px ?? null,
        mv_reporting: Number(p.mv_reporting ?? 0),
      });
    }
  }
  return Array.from(map.values()).sort((a, b) => b.mv_reporting - a.mv_reporting);
}

export default function HoldingsTable({
  positions,
  limit = 10,
  reportingCcy = "USD",
}: Props) {
  const aggregated = aggregateByHolding(positions);
  const totalNav = aggregated.reduce((s, h) => s + h.mv_reporting, 0);
  const top = aggregated.slice(0, limit);

  return (
    <div className="overflow-x-auto rounded-lg border border-slate-200 bg-white">
      <table className="min-w-full text-sm">
        <thead className="bg-slate-50 text-xs uppercase tracking-wide text-slate-500">
          <tr>
            <th className="px-4 py-3 text-left">Asset</th>
            <th className="px-4 py-3 text-left">Ticker</th>
            <th className="px-4 py-3 text-right">Quantity</th>
            <th className="px-4 py-3 text-right">Price</th>
            <th className="px-4 py-3 text-right">Value</th>
            <th className="px-4 py-3 text-right">Weight</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-100">
          {top.map(h => {
            const weight = totalNav > 0 ? h.mv_reporting / totalNav : 0;
            const isCash = h.asset_class === CASH_ASSET_CLASS;
            return (
              <tr key={h.key} className="hover:bg-slate-50">
                <td className="px-4 py-3 font-medium text-slate-900">{h.asset_name}</td>
                <td className="px-4 py-3 text-slate-600">{h.ticker_masttro ?? "—"}</td>
                <td className="px-4 py-3 text-right text-slate-700">
                  {isCash ? "—" : h.quantity.toLocaleString()}
                </td>
                <td className="px-4 py-3 text-right text-slate-700">
                  {isCash
                    ? "—"
                    : h.price_display != null
                      ? price(h.price_display, h.local_ccy ?? reportingCcy)
                      : "—"}
                </td>
                <td className="px-4 py-3 text-right font-medium text-slate-900">
                  {money(h.mv_reporting, reportingCcy)}
                </td>
                <td className="px-4 py-3 text-right text-slate-700">{pct(weight, 1)}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
