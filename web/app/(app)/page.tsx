import KpiTile from "@/components/KpiTile";
import ReturnsTile from "@/components/ReturnsTile";
import HoldingsTable from "@/components/HoldingsTable";
import NavChart from "@/components/NavChart";
import AssetAllocationTable from "@/components/AssetAllocationTable";
import {
  computeKpis,
  getIndexPrices,
  getLatestPositions,
  getNavSeries,
  getNavAtOrBeforeForClasses,
  getPeriodReturns,
  listIndices,
} from "@/lib/queries";
import {
  getSelectedAccounts,
  getSelectedAssetClasses,
  getSelectedBenchmark,
  getSelectedSubClient,
  getSelectedTrusts,
} from "@/lib/trust-filter";
import {
  computeIndexReturnsForAllPeriods,
  computePeriodStart,
  type PeriodKey,
} from "@/lib/returns";
import { money } from "@/lib/format";

export const dynamic = "force-dynamic";

export default async function OverviewPage() {
  const _renderStart = Date.now();
  const subClient = getSelectedSubClient();
  const trusts = getSelectedTrusts();
  const accounts = getSelectedAccounts();
  const assetClasses = getSelectedAssetClasses();
  const benchmarkTicker = getSelectedBenchmark();
  // 6M / 12M start NAVs via the nav_at_or_before RPC. Masttro only exposes
  // month-end historicals, so the RPC returns the raw NAV at the most
  // recent snapshot ≤ target date — and also returns that anchor_date so
  // we can label the period start honestly. Other periods (1D / MTD / YTD)
  // already align to dates we have exactly, so they use the snapshot-grid
  // path. When the global asset_class filter is set, the *ForClasses helper
  // fans out one RPC call per class and sums.
  const today = new Date();
  const target6M = computePeriodStart("6m", today);
  const target1Y = computePeriodStart("1y", today);
  const [positions, navSeries, indices, nav6M, nav1Y] = await Promise.all([
    getLatestPositions(subClient, trusts, accounts, assetClasses),
    getNavSeries(subClient, trusts, accounts, assetClasses),
    listIndices(),
    getNavAtOrBeforeForClasses(subClient, trusts, accounts, target6M, assetClasses),
    getNavAtOrBeforeForClasses(subClient, trusts, accounts, target1Y, assetClasses),
  ]);
  const kpis = computeKpis(positions);

  // Use the yfinance-refreshed sums as the end-of-period NAV for every return,
  // and as the 1D start NAV (today's positions × yfinance's previous close).
  const endNav = positions.reduce(
    (s, p) => s + Number(p.mv_reporting ?? 0),
    0,
  );
  const endNavYesterday = positions.reduce(
    (s, p) =>
      s + Number(p.mv_reporting_yesterday ?? p.mv_reporting ?? 0),
    0,
  );
  const startNavByPeriod: Partial<Record<PeriodKey, { nav: number; date: string }>> = {};
  if (nav6M != null) startNavByPeriod["6m"] = { nav: nav6M.nav, date: nav6M.anchorDate };
  if (nav1Y != null) startNavByPeriod["1y"] = { nav: nav1Y.nav, date: nav1Y.anchorDate };

  // Pull benchmark price history from the earliest portfolio snapshot
  // (or 5y back if there's no portfolio data yet).
  const benchmarkFromDate =
    navSeries[0]?.snapshot_date ??
    new Date(Date.UTC(new Date().getUTCFullYear() - 5, 0, 1))
      .toISOString()
      .slice(0, 10);
  const [returns, indexPrices] = await Promise.all([
    getPeriodReturns(subClient, trusts, accounts, assetClasses, {
      endNav,
      endNavYesterday,
      startNavByPeriod,
      navs: navSeries,
    }),
    getIndexPrices(benchmarkTicker, benchmarkFromDate),
  ]);
  const indexReturns = computeIndexReturnsForAllPeriods(indexPrices, returns);
  const benchmark =
    indices.find(i => i.ticker === benchmarkTicker) ?? indices[0] ?? null;

  // Asset allocation: aggregate today's refreshed positions by asset_class.
  // Drives the AssetAllocationTable; clicking a row sets the global filter.
  // Computed in JS off the positions list — no extra round-trip.
  const allocation = (() => {
    const totals = new Map<string, number>();
    for (const p of positions) {
      const ac = p.asset_class ?? "Unclassified";
      totals.set(ac, (totals.get(ac) ?? 0) + Number(p.mv_reporting ?? 0));
    }
    const arr = Array.from(totals.entries())
      .map(([asset_class, nav]) => ({ asset_class, nav }))
      .sort((a, b) => b.nav - a.nav);
    const total = arr.reduce((s, r) => s + r.nav, 0);
    return arr.map(r => ({ ...r, share: total > 0 ? r.nav / total : 0 }));
  })();

  // Collapse the series to one point per calendar month — pick the latest
  // snapshot in each month. For completed months that's the month-end
  // snapshot; for the current month it's whatever daily snapshot is most
  // recent. Keeps the line readable instead of stacking intra-month dailies
  // on top of the prior month-end.
  const monthlyNavSeries = (() => {
    const byMonth = new Map<string, (typeof navSeries)[number]>();
    for (const point of navSeries) {
      const monthKey = point.snapshot_date.slice(0, 7);
      const existing = byMonth.get(monthKey);
      if (!existing || point.snapshot_date > existing.snapshot_date) {
        byMonth.set(monthKey, point);
      }
    }
    return Array.from(byMonth.values()).sort((a, b) =>
      a.snapshot_date.localeCompare(b.snapshot_date),
    );
  })();

  // Bump the chart's rightmost point to the refreshed NAV so the line lands
  // on the same number as the NAV tile (which is yfinance-priced). If the
  // latest Masttro snapshot is before today, append a new point for today;
  // if it's today already, overwrite that point's value.
  const todayIso = today.toISOString().slice(0, 10);
  const chartData = (() => {
    if (monthlyNavSeries.length === 0) return monthlyNavSeries;
    const last = monthlyNavSeries[monthlyNavSeries.length - 1];
    if (last.snapshot_date < todayIso) {
      return [...monthlyNavSeries, { snapshot_date: todayIso, nav: endNav }];
    }
    return [
      ...monthlyNavSeries.slice(0, -1),
      { snapshot_date: last.snapshot_date, nav: endNav },
    ];
  })();

  const navFromHistory =
    navSeries.length > 0 ? navSeries[navSeries.length - 1].nav : null;

  console.log(
    `[page] Overview total ${Date.now() - _renderStart}ms ` +
      `(${trusts.length}t,${accounts.length}a,${assetClasses.length}c)`,
  );

  return (
    <main className="mx-auto max-w-7xl px-6 py-8">
      <div className="grid grid-cols-1 gap-4 md:grid-cols-4">
        <KpiTile label="NAV (latest)" value={money(kpis.nav, kpis.reporting_ccy)} />
        <div className="md:col-span-2">
          <ReturnsTile
            returns={returns}
            indexReturns={indexReturns}
            benchmark={benchmark}
            availableBenchmarks={indices}
            reportingCcy={kpis.reporting_ccy}
          />
        </div>
        <KpiTile
          label="Unrealized G/L"
          value={money(kpis.unrealized_gl, kpis.reporting_ccy)}
          tone={kpis.unrealized_gl >= 0 ? "positive" : "negative"}
        />
      </div>

      <section className="mt-8">
        <AssetAllocationTable
          rows={allocation}
          currentClasses={assetClasses}
          reportingCcy={kpis.reporting_ccy}
        />
      </section>

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
  );
}
