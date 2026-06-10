import KpiTile from "@/components/KpiTile";
import ReturnsTile from "@/components/ReturnsTile";
import HoldingsTable from "@/components/HoldingsTable";
import NavChart from "@/components/NavChart";
import AllocationSummaryTile from "@/components/AllocationSummaryTile";
import {
  computeKpis,
  getIndexPrices,
  getLatestPositions,
  getNavSeries,
  getNavCarryforward,
  getPeriodReturns,
  listIndices,
} from "@/lib/queries";
import {
  getSelectedAccounts,
  getSelectedAssetClasses,
  getSelectedBenchmark,
  getSelectedTrusts,
  getSelectedVehicles,
} from "@/lib/trust-filter";
import { getActiveSubClient } from "@/lib/session";
import VehicleScopeNote from "@/components/VehicleScopeNote";
import {
  computeIndexReturnsForAllPeriods,
  computePeriodStart,
  type PeriodKey,
} from "@/lib/returns";
import { money } from "@/lib/format";

export const dynamic = "force-dynamic";

export default async function OverviewPage() {
  const _renderStart = Date.now();
  const subClient = await getActiveSubClient();
  const trusts = getSelectedTrusts();
  const accounts = getSelectedAccounts();
  const assetClasses = getSelectedAssetClasses();
  const vehicles = getSelectedVehicles();
  const benchmarkTicker = getSelectedBenchmark();
  // Start NAVs for MTD / YTD / 6M / 1Y via the per-account carry-forward RPC.
  // Each account is valued at its latest snapshot ≤ the target date — the
  // same latest-per-account basis as endNav — so a stale account (e.g. an AU
  // super fund reporting monthly) is carried into BOTH the end and the start
  // and can't read as a phantom gain. This matters most when the Entity
  // filter narrows to a small entity (migration 032). At month-end anchors
  // it equals the old nav_at_or_before; only the near-stale-edge MTD differs.
  const today = new Date();
  const targetMtd = computePeriodStart("mtd", today);
  const targetYtd = computePeriodStart("ytd", today);
  const target6M = computePeriodStart("6m", today);
  const target1Y = computePeriodStart("1y", today);
  const [positions, navSeries, indices, navMtd, navYtd, nav6M, nav1Y] =
    await Promise.all([
      getLatestPositions(subClient, trusts, accounts, assetClasses),
      getNavSeries(subClient, trusts, accounts, assetClasses),
      listIndices(),
      getNavCarryforward(subClient, trusts, accounts, targetMtd, assetClasses),
      getNavCarryforward(subClient, trusts, accounts, targetYtd, assetClasses),
      getNavCarryforward(subClient, trusts, accounts, target6M, assetClasses),
      getNavCarryforward(subClient, trusts, accounts, target1Y, assetClasses),
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
  if (navMtd != null) startNavByPeriod["mtd"] = { nav: navMtd.nav, date: navMtd.anchorDate };
  if (navYtd != null) startNavByPeriod["ytd"] = { nav: navYtd.nav, date: navYtd.anchorDate };
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
  // Drives the AllocationSummaryTile in the top row.
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
  // on the same number as the NAV tile (which is yfinance-priced). Compare by
  // calendar month, not exact date, to keep one point per month: if the latest
  // monthly point is already in the current month, overwrite it with the
  // refreshed NAV; only append a fresh point when the latest belongs to an
  // earlier month. (Comparing exact dates appended a second current-month point
  // whenever the latest Masttro snapshot fell earlier in the same month.)
  const todayIso = today.toISOString().slice(0, 10);
  const todayMonth = todayIso.slice(0, 7);
  const chartData = (() => {
    if (monthlyNavSeries.length === 0) return monthlyNavSeries;
    const last = monthlyNavSeries[monthlyNavSeries.length - 1];
    if (last.snapshot_date.slice(0, 7) === todayMonth) {
      return [
        ...monthlyNavSeries.slice(0, -1),
        { snapshot_date: todayIso, nav: endNav },
      ];
    }
    return [...monthlyNavSeries, { snapshot_date: todayIso, nav: endNav }];
  })();

  console.log(
    `[page] Overview total ${Date.now() - _renderStart}ms ` +
      `(${trusts.length}t,${accounts.length}a,${assetClasses.length}c)`,
  );

  return (
    <main className="mx-auto max-w-7xl px-4 py-8 sm:px-6">
      {vehicles.length > 0 ? (
        <div className="mb-4">
          <VehicleScopeNote vehicles={vehicles} />
        </div>
      ) : null}
      <div className="flex flex-col gap-4 md:flex-row md:items-stretch">
        <div className="min-w-0 md:flex-[0.8]">
          <KpiTile
            label="NAV (latest)"
            value={money(kpis.nav, kpis.reporting_ccy)}
            className="h-full"
          />
        </div>
        <div className="min-w-0 md:flex-[1.5]">
          <ReturnsTile
            returns={returns}
            indexReturns={indexReturns}
            benchmark={benchmark}
            availableBenchmarks={indices}
            reportingCcy={kpis.reporting_ccy}
          />
        </div>
        <div className="min-w-0 md:flex-[1.1]">
          <AllocationSummaryTile rows={allocation} />
        </div>
      </div>

      <section className="mt-8">
        <NavChart data={chartData} reportingCcy={kpis.reporting_ccy} />
      </section>

      <section className="mt-8">
        <h2 className="mb-3 text-base font-semibold text-slate-900">Top holdings</h2>
        <HoldingsTable positions={positions} limit={10} reportingCcy={kpis.reporting_ccy} />
      </section>
    </main>
  );
}
