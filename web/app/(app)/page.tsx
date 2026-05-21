import KpiTile from "@/components/KpiTile";
import ReturnsTile from "@/components/ReturnsTile";
import HoldingsTable from "@/components/HoldingsTable";
import NavChart from "@/components/NavChart";
import {
  computeKpis,
  getIndexPrices,
  getLatestPositions,
  getNavSeries,
  getNavSeriesByAssetClass,
  getPeriodReturns,
  getReconstructedNavAt,
  listIndices,
} from "@/lib/queries";
import {
  getSelectedAccounts,
  getSelectedBenchmark,
  getSelectedSubClient,
  getSelectedTrusts,
} from "@/lib/trust-filter";
import {
  computeAllPeriodReturns,
  computeIndexReturnsForAllPeriods,
  computePeriodStart,
  type PeriodKey,
  type PeriodReturn,
} from "@/lib/returns";
import { money } from "@/lib/format";

export const dynamic = "force-dynamic";

export default async function OverviewPage() {
  const _renderStart = Date.now();
  const subClient = getSelectedSubClient();
  const trusts = getSelectedTrusts();
  const accounts = getSelectedAccounts();
  const benchmarkTicker = getSelectedBenchmark();
  // Precise 6M and 1Y start NAVs via the reconstructed_nav_at RPC. Other
  // periods (1D / MTD / YTD) already align to dates we have exactly, so
  // we leave them on the snapshot-grid path.
  const today = new Date();
  const target6M = computePeriodStart("6m", today);
  const target1Y = computePeriodStart("1y", today);
  // Everything in this batch is independent — fire in parallel for one
  // network round-trip instead of three sequential ones.
  const [positions, navSeries, indices, navByClass, nav6M, nav1Y] =
    await Promise.all([
      getLatestPositions(subClient, trusts, accounts),
      getNavSeries(subClient, trusts, accounts),
      listIndices(),
      getNavSeriesByAssetClass(subClient, trusts, accounts),
      getReconstructedNavAt(subClient, trusts, accounts, target6M),
      getReconstructedNavAt(subClient, trusts, accounts, target1Y),
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
  const startNavByPeriod: Partial<Record<PeriodKey, { nav: number; date: string }>> = {};
  if (nav6M != null) startNavByPeriod["6m"] = { nav: nav6M, date: target6M };
  if (nav1Y != null) startNavByPeriod["1y"] = { nav: nav1Y, date: target1Y };

  // Pull benchmark price history starting from the earliest portfolio snapshot
  // (or 5y back if there's no portfolio data yet). Run in parallel with
  // getPeriodReturns — they're independent.
  const benchmarkFromDate =
    navSeries[0]?.snapshot_date ??
    new Date(Date.UTC(new Date().getUTCFullYear() - 5, 0, 1))
      .toISOString()
      .slice(0, 10);
  const [returns, indexPrices] = await Promise.all([
    getPeriodReturns(subClient, trusts, accounts, {
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

  // Per-asset-class returns. Group today's positions by asset_class so the
  // end NAV uses the same refreshed (yfinance) values, then run modified
  // Dietz with flows = [] since trust-level deposits aren't asset-typed
  // (so the math is a clean price-only return on the held positions).
  const positionsByClass = new Map<string, typeof positions>();
  for (const p of positions) {
    const ac = p.asset_class ?? "Unclassified";
    const arr = positionsByClass.get(ac) ?? [];
    arr.push(p);
    positionsByClass.set(ac, arr);
  }
  // Fetch precise 6M / 1Y start NAVs per asset class via the asset-class-
  // aware reconstructed_nav_at RPC (migration 014). Without this, asset-
  // class returns used the snapshot-grid path and their start_date snapped
  // to the previous month-end — which made the benchmark column for that
  // slice differ from Total, since computeIndexReturn slices the index
  // series by the portfolio's own dates. All 2*N calls fire in parallel.
  const acNames = Array.from(new Set(Object.keys(navByClass)));
  const acStartNavPromises = acNames.flatMap(ac => [
    getReconstructedNavAt(subClient, trusts, accounts, target6M, ac),
    getReconstructedNavAt(subClient, trusts, accounts, target1Y, ac),
  ]);
  const acStartNavResults = await Promise.all(acStartNavPromises);
  const acStartNavByPeriod: Record<
    string,
    Partial<Record<PeriodKey, { nav: number; date: string }>>
  > = {};
  acNames.forEach((ac, i) => {
    const nav6 = acStartNavResults[i * 2];
    const nav1 = acStartNavResults[i * 2 + 1];
    const overrides: Partial<Record<PeriodKey, { nav: number; date: string }>> = {};
    if (nav6 != null) overrides["6m"] = { nav: nav6, date: target6M };
    if (nav1 != null) overrides["1y"] = { nav: nav1, date: target1Y };
    acStartNavByPeriod[ac] = overrides;
  });
  const returnsByAssetClass: Record<string, Record<PeriodKey, PeriodReturn>> = {};
  for (const [ac, navs] of Object.entries(navByClass)) {
    const classPositions = positionsByClass.get(ac) ?? [];
    const acEndNav = classPositions.reduce(
      (s, p) => s + Number(p.mv_reporting ?? 0),
      0,
    );
    const acEndNavYesterday = classPositions.reduce(
      (s, p) => s + Number(p.mv_reporting_yesterday ?? p.mv_reporting ?? 0),
      0,
    );
    returnsByAssetClass[ac] = computeAllPeriodReturns(
      navs.map(n => ({ date: n.snapshot_date, nav: n.nav })),
      [],
      {
        endNav: acEndNav,
        endNavYesterday: acEndNavYesterday,
        startNavByPeriod: acStartNavByPeriod[ac],
      },
    );
  }
  const indexReturnsByAssetClass: Record<
    string,
    Record<PeriodKey, number | null>
  > = {};
  for (const ac of Object.keys(returnsByAssetClass)) {
    indexReturnsByAssetClass[ac] = computeIndexReturnsForAllPeriods(
      indexPrices,
      returnsByAssetClass[ac],
    );
  }

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
      `(${trusts.length}t,${accounts.length}a)`,
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
            returnsByAssetClass={returnsByAssetClass}
            indexReturnsByAssetClass={indexReturnsByAssetClass}
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
  );
}
