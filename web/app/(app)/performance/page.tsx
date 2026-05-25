import PerformanceMatrix from "@/components/PerformanceMatrix";
import RebasedChart, { type RebasedPoint } from "@/components/RebasedChart";
import {
  getFlowsByAssetClass,
  getFlowsByTrust,
  getIndexPrices,
  getLatestPositions,
  getNavSeries,
  getNavSeriesByAssetClass,
  getNavSeriesByTrust,
  getPeriodReturns,
  listIndices,
  type Position,
} from "@/lib/queries";
import {
  getSelectedAccounts,
  getSelectedAssetClasses,
  getSelectedBenchmark,
  getSelectedSubClient,
  getSelectedTrusts,
} from "@/lib/trust-filter";
import {
  computeAllPeriodReturns,
  computeIndexReturnsForAllPeriods,
  type PeriodKey,
  type PeriodReturn,
} from "@/lib/returns";

export const dynamic = "force-dynamic";

function groupBy<T, K extends string>(
  items: T[],
  key: (t: T) => K | null | undefined,
): Map<K, T[]> {
  const out = new Map<K, T[]>();
  for (const item of items) {
    const k = key(item);
    if (k == null) continue;
    const arr = out.get(k) ?? [];
    arr.push(item);
    out.set(k, arr);
  }
  return out;
}

function sumPosition(positions: Position[], field: "mv_reporting" | "mv_reporting_yesterday"): number {
  return positions.reduce((s, p) => {
    const raw = p[field] ?? p.mv_reporting ?? 0;
    return s + Number(raw);
  }, 0);
}

export default async function PerformancePage() {
  const subClient = getSelectedSubClient();
  const trusts = getSelectedTrusts();
  const accounts = getSelectedAccounts();
  const assetClasses = getSelectedAssetClasses();
  const benchmarkTicker = getSelectedBenchmark();

  const [
    positions,
    navSeries,
    navByTrust,
    navByClass,
    indices,
    returns,
  ] = await Promise.all([
    getLatestPositions(subClient, trusts, accounts, assetClasses),
    getNavSeries(subClient, trusts, accounts, assetClasses),
    getNavSeriesByTrust(subClient, trusts, accounts, assetClasses),
    getNavSeriesByAssetClass(subClient, trusts, accounts, assetClasses),
    listIndices(),
    // Total scope returns — reused as the "Total" row anchor + comparison
    // baseline. Page-level overrides applied below.
    getPeriodReturns(subClient, trusts, accounts, assetClasses, {}),
  ]);

  const benchmarkFromDate =
    navSeries[0]?.snapshot_date ??
    new Date(Date.UTC(new Date().getUTCFullYear() - 5, 0, 1))
      .toISOString()
      .slice(0, 10);
  const indexPrices = await getIndexPrices(benchmarkTicker, benchmarkFromDate);
  const benchmark =
    indices.find(i => i.ticker === benchmarkTicker) ?? indices[0] ?? null;

  // Flows for trust-level Modified Dietz. When asset_class filter is set,
  // getFlowsByTrust switches to per-class flow rule internally (Buy + Sell +
  // dividends + interest, sign-flipped) so each trust's return reflects just
  // the selected classes' performance.
  const [flowsByTrust, flowsByClass] = await Promise.all([
    getFlowsByTrust(subClient, trusts, accounts, benchmarkFromDate, assetClasses),
    getFlowsByAssetClass(subClient, trusts, accounts, benchmarkFromDate, assetClasses),
  ]);

  // --- Trust matrix ---------------------------------------------------------
  const positionsByTrust = groupBy(positions, p => p.trust_alias);
  const trustReturns: Record<string, Record<PeriodKey, PeriodReturn>> = {};
  const trustNav: Record<string, number> = {};
  for (const [trustAlias, navs] of Object.entries(navByTrust)) {
    const trustPositions = positionsByTrust.get(trustAlias) ?? [];
    const endNav = sumPosition(trustPositions, "mv_reporting");
    const endNavYesterday = sumPosition(trustPositions, "mv_reporting_yesterday");
    trustReturns[trustAlias] = computeAllPeriodReturns(
      navs.map(n => ({ date: n.snapshot_date, nav: n.nav })),
      flowsByTrust[trustAlias] ?? [],
      { endNav, endNavYesterday },
    );
    trustNav[trustAlias] = endNav;
  }

  // --- Asset-class matrix --------------------------------------------------
  // Flow rule matches Masttro's per-asset-class transferInOut (Buy + Sell +
  // dividends + interest, sign-flipped so positive = inflow to the class).
  // See queries.ts::getFlowsByAssetClass.
  const positionsByClass = groupBy(positions, p => p.asset_class ?? "Unclassified");
  const classReturns: Record<string, Record<PeriodKey, PeriodReturn>> = {};
  const classNav: Record<string, number> = {};
  for (const [className, navs] of Object.entries(navByClass)) {
    const cp = positionsByClass.get(className) ?? [];
    const endNav = sumPosition(cp, "mv_reporting");
    const endNavYesterday = sumPosition(cp, "mv_reporting_yesterday");
    classReturns[className] = computeAllPeriodReturns(
      navs.map(n => ({ date: n.snapshot_date, nav: n.nav })),
      flowsByClass[className] ?? [],
      { endNav, endNavYesterday },
    );
    classNav[className] = endNav;
  }

  // --- Rebased chart -------------------------------------------------------
  // Bump portfolio's last point to the refreshed NAV so the line ends where
  // the Returns tile / NAV tile do. Compute index value at each snapshot
  // date by walking forwards to find the nearest price on/before.
  const endNavRefreshed = sumPosition(positions, "mv_reporting");
  const portfolioPoints = navSeries.length
    ? (() => {
        const today = new Date().toISOString().slice(0, 10);
        const last = navSeries[navSeries.length - 1];
        if (last.snapshot_date < today) {
          return [
            ...navSeries,
            { snapshot_date: today, nav: endNavRefreshed },
          ];
        }
        return [
          ...navSeries.slice(0, -1),
          { snapshot_date: last.snapshot_date, nav: endNavRefreshed },
        ];
      })()
    : [];
  const portfolioStart = portfolioPoints[0]?.nav ?? 0;
  let benchmarkStart: number | null = null;
  if (portfolioPoints.length && indexPrices.length) {
    for (const p of indexPrices) {
      if (p.date <= portfolioPoints[0].snapshot_date) benchmarkStart = p.price;
      else break;
    }
    if (benchmarkStart == null) benchmarkStart = indexPrices[0].price;
  }
  const rebasedData: RebasedPoint[] = portfolioPoints.map(pt => {
    let benchPx: number | null = null;
    for (const ip of indexPrices) {
      if (ip.date <= pt.snapshot_date) benchPx = ip.price;
      else break;
    }
    return {
      date: pt.snapshot_date,
      portfolio: portfolioStart > 0 ? (pt.nav / portfolioStart) * 100 : null,
      benchmark:
        benchPx != null && benchmarkStart && benchmarkStart > 0
          ? (benchPx / benchmarkStart) * 100
          : null,
    };
  });

  // --- Index returns over the same dates the Total scope uses --------------
  // (Just for the benchmark row of each matrix.)
  const indexReturns = computeIndexReturnsForAllPeriods(indexPrices, returns);

  const scopeNote =
    [
      trusts.length === 1
        ? `Entity: ${trusts[0]}`
        : trusts.length > 1
          ? `${trusts.length} entities`
          : null,
      accounts.length > 0 ? `${accounts.length} account${accounts.length > 1 ? "s" : ""} scoped` : null,
    ]
      .filter(Boolean)
      .join(" · ") || "All entities under " + subClient;

  return (
    <main className="mx-auto max-w-7xl space-y-8 px-6 py-8">
      <div className="flex items-baseline justify-between">
        <h1 className="text-2xl font-semibold text-slate-900">Performance</h1>
        <span className="text-xs text-slate-500">
          {scopeNote}
          {benchmark ? ` · benchmark ${benchmark.ticker}` : ""}
        </span>
      </div>

      <RebasedChart
        data={rebasedData}
        benchmarkLabel={benchmark?.ticker ?? "Benchmark"}
      />

      <PerformanceMatrix
        title="Returns by entity"
        rowLabel="Entity"
        returns={trustReturns}
        navAtToday={trustNav}
        indexReturns={indexReturns}
        benchmarkLabel={benchmark?.ticker}
      />

      <PerformanceMatrix
        title="Returns by asset class"
        rowLabel="Asset class"
        returns={classReturns}
        navAtToday={classNav}
        indexReturns={indexReturns}
        benchmarkLabel={benchmark?.ticker}
      />
    </main>
  );
}
