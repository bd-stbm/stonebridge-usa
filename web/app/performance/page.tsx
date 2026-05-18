import Header from "@/components/Header";
import PerformanceMatrix from "@/components/PerformanceMatrix";
import RebasedChart, { type RebasedPoint } from "@/components/RebasedChart";
import {
  DEFAULT_SUB_CLIENT,
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
  getSelectedAccount,
  getSelectedBenchmark,
  getSelectedTrust,
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
  const trust = getSelectedTrust();
  const account = getSelectedAccount();
  const benchmarkTicker = getSelectedBenchmark();

  const [
    positions,
    navSeries,
    navByTrust,
    navByClass,
    indices,
    returns,
  ] = await Promise.all([
    getLatestPositions(DEFAULT_SUB_CLIENT, trust, account),
    getNavSeries(DEFAULT_SUB_CLIENT, trust, account),
    getNavSeriesByTrust(DEFAULT_SUB_CLIENT, trust, account),
    getNavSeriesByAssetClass(DEFAULT_SUB_CLIENT, trust, account),
    listIndices(),
    // Total scope returns — reused as the "Total" row anchor + comparison
    // baseline. Page-level overrides applied below.
    getPeriodReturns(DEFAULT_SUB_CLIENT, trust, account, {}),
  ]);

  const benchmarkFromDate =
    navSeries[0]?.snapshot_date ??
    new Date(Date.UTC(new Date().getUTCFullYear() - 5, 0, 1))
      .toISOString()
      .slice(0, 10);
  const indexPrices = await getIndexPrices(benchmarkTicker, benchmarkFromDate);
  const benchmark =
    indices.find(i => i.ticker === benchmarkTicker) ?? indices[0] ?? null;

  // Flows per trust — for trust-level Modified Dietz.
  const flowsByTrust = await getFlowsByTrust(
    DEFAULT_SUB_CLIENT,
    trust,
    account,
    benchmarkFromDate,
  );

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
  const positionsByClass = groupBy(positions, p => p.asset_class ?? "Unclassified");
  const classReturns: Record<string, Record<PeriodKey, PeriodReturn>> = {};
  const classNav: Record<string, number> = {};
  for (const [className, navs] of Object.entries(navByClass)) {
    const cp = positionsByClass.get(className) ?? [];
    const endNav = sumPosition(cp, "mv_reporting");
    const endNavYesterday = sumPosition(cp, "mv_reporting_yesterday");
    classReturns[className] = computeAllPeriodReturns(
      navs.map(n => ({ date: n.snapshot_date, nav: n.nav })),
      [], // asset-class scope: price-only (flows aren't class-typed)
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
    [trust ? `Trust: ${trust}` : null, account ? "Account scoped" : null]
      .filter(Boolean)
      .join(" · ") || "All trusts under " + DEFAULT_SUB_CLIENT;

  return (
    <>
      <Header subClient={DEFAULT_SUB_CLIENT} />
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
          title="Returns by trust"
          rowLabel="Trust"
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
          priceOnly
        />
      </main>
    </>
  );
}
