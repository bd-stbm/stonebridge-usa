import Header from "@/components/Header";
import KpiTile from "@/components/KpiTile";
import ReturnsTile from "@/components/ReturnsTile";
import HoldingsTable from "@/components/HoldingsTable";
import NavChart from "@/components/NavChart";
import {
  DEFAULT_SUB_CLIENT,
  computeKpis,
  getIndexPrices,
  getLatestPositions,
  getNavSeries,
  getPeriodReturns,
  listIndices,
} from "@/lib/queries";
import {
  getSelectedAccount,
  getSelectedBenchmark,
  getSelectedTrust,
} from "@/lib/trust-filter";
import { computeIndexReturnsForAllPeriods } from "@/lib/returns";
import { money } from "@/lib/format";

export const dynamic = "force-dynamic";

export default async function OverviewPage() {
  const trust = getSelectedTrust();
  const account = getSelectedAccount();
  const benchmarkTicker = getSelectedBenchmark();
  const [positions, navSeries, indices] = await Promise.all([
    getLatestPositions(DEFAULT_SUB_CLIENT, trust, account),
    getNavSeries(DEFAULT_SUB_CLIENT, trust, account),
    listIndices(),
  ]);
  const kpis = computeKpis(positions);

  // Use the yfinance-refreshed sums as the end-of-period NAV for every return,
  // and as the 1D start NAV (today's positions × yfinance's previous close).
  // Falls back to the Masttro snapshot value when yfinance data is missing.
  const endNav = positions.reduce(
    (s, p) => s + Number(p.mv_reporting ?? 0),
    0,
  );
  const endNavYesterday = positions.reduce(
    (s, p) =>
      s + Number(p.mv_reporting_yesterday ?? p.mv_reporting ?? 0),
    0,
  );
  const returns = await getPeriodReturns(DEFAULT_SUB_CLIENT, trust, account, {
    endNav,
    endNavYesterday,
  });

  // Pull benchmark price history starting from the earliest portfolio snapshot
  // (or 5y back if there's no portfolio data yet). Then compute index returns
  // over the same period dates the portfolio returns used.
  const benchmarkFromDate =
    navSeries[0]?.snapshot_date ??
    new Date(Date.UTC(new Date().getUTCFullYear() - 5, 0, 1))
      .toISOString()
      .slice(0, 10);
  const indexPrices = await getIndexPrices(benchmarkTicker, benchmarkFromDate);
  const indexReturns = computeIndexReturnsForAllPeriods(indexPrices, returns);
  const benchmark =
    indices.find(i => i.ticker === benchmarkTicker) ?? indices[0] ?? null;

  // Bump the chart's rightmost point to the refreshed NAV so the line lands
  // on the same number as the NAV tile (which is yfinance-priced). If the
  // latest Masttro snapshot is before today, append a new point for today;
  // if it's today already, overwrite that point's value.
  const today = new Date().toISOString().slice(0, 10);
  const chartData = (() => {
    if (navSeries.length === 0) return navSeries;
    const last = navSeries[navSeries.length - 1];
    if (last.snapshot_date < today) {
      return [...navSeries, { snapshot_date: today, nav: endNav }];
    }
    return [
      ...navSeries.slice(0, -1),
      { snapshot_date: last.snapshot_date, nav: endNav },
    ];
  })();

  const navFromHistory =
    navSeries.length > 0 ? navSeries[navSeries.length - 1].nav : null;

  return (
    <>
      <Header subClient={DEFAULT_SUB_CLIENT} />
      <main className="mx-auto max-w-7xl px-6 py-8">
        <div className="grid grid-cols-1 gap-4 md:grid-cols-4">
          <KpiTile label="NAV (latest)" value={money(kpis.nav, kpis.reporting_ccy)} />
          <div className="md:col-span-2">
            <ReturnsTile
              returns={returns}
              indexReturns={indexReturns}
              benchmark={benchmark}
              availableBenchmarks={indices}
            />
          </div>
          <KpiTile
            label="Unrealized G/L"
            value={money(kpis.unrealized_gl, kpis.reporting_ccy)}
            tone={kpis.unrealized_gl >= 0 ? "positive" : "negative"}
          />
        </div>

        <section className="mt-8">
          <NavChart data={chartData} />
        </section>

        <section className="mt-8">
          <div className="mb-3 flex items-center justify-between">
            <h2 className="text-base font-semibold text-slate-900">Top holdings</h2>
            <span className="text-xs text-slate-500">
              {positions.length} positions
              {navFromHistory != null &&
                ` • history NAV ${money(navFromHistory, kpis.reporting_ccy)}`}
            </span>
          </div>
          <HoldingsTable positions={positions} limit={10} />
        </section>
      </main>
    </>
  );
}
