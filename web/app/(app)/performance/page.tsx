import PerformanceMatrix from "@/components/PerformanceMatrix";
import MonthlyAttributionSection from "@/components/MonthlyAttributionSection";
import type { MonthlyReturnRow } from "@/components/MonthlyReturnsBar";
import {
  getFlowsByAssetClass,
  getFlowsByTrust,
  getIndexPrices,
  getLatestPositions,
  getMonthlySecurityAttribution,
  getNavSeries,
  getNavSeriesByAssetClass,
  getNavSeriesByTrust,
  getPeriodReturns,
  listIndices,
  type MonthlyAttributionRow,
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

const ATTRIBUTION_MONTHS = 24;

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

const MONTH_LABELS = [
  "Jan", "Feb", "Mar", "Apr", "May", "Jun",
  "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

function monthIsoToLabel(monthIso: string): string {
  // monthIso is yyyy-mm-dd (first of month). Build "Mon yyyy".
  const [y, m] = monthIso.split("-");
  const monthIdx = Number(m) - 1;
  return `${MONTH_LABELS[monthIdx] ?? m} ${y}`;
}

export default async function PerformancePage() {
  const subClient = getSelectedSubClient();
  const trusts = getSelectedTrusts();
  const accounts = getSelectedAccounts();
  const assetClasses = getSelectedAssetClasses();
  const benchmarkTicker = getSelectedBenchmark();

  // Attribution window: last N completed months + current month.
  const today = new Date();
  const fromMonthDate = new Date(
    Date.UTC(today.getUTCFullYear(), today.getUTCMonth() - ATTRIBUTION_MONTHS, 1),
  );
  const fromMonth = fromMonthDate.toISOString().slice(0, 10);

  const [
    positions,
    navSeries,
    navByTrust,
    navByClass,
    indices,
    returns,
    attribution,
  ] = await Promise.all([
    getLatestPositions(subClient, trusts, accounts, assetClasses),
    getNavSeries(subClient, trusts, accounts, assetClasses),
    getNavSeriesByTrust(subClient, trusts, accounts, assetClasses),
    getNavSeriesByAssetClass(subClient, trusts, accounts, assetClasses),
    listIndices(),
    getPeriodReturns(subClient, trusts, accounts, assetClasses, {}),
    getMonthlySecurityAttribution(subClient, trusts, accounts, assetClasses, fromMonth),
  ]);

  const benchmarkFromDate =
    navSeries[0]?.snapshot_date ??
    new Date(Date.UTC(new Date().getUTCFullYear() - 5, 0, 1))
      .toISOString()
      .slice(0, 10);
  const indexPrices = await getIndexPrices(benchmarkTicker, benchmarkFromDate);
  const benchmark =
    indices.find(i => i.ticker === benchmarkTicker) ?? indices[0] ?? null;

  // Flows for the trust matrix + per-month portfolio totals. Same rule as
  // getPeriodReturns: external flows when no asset_class filter; per-class
  // flows when filter is active (since external deposits aren't class-typed).
  const [flowsByTrust, flowsByClass] = await Promise.all([
    getFlowsByTrust(subClient, trusts, accounts, benchmarkFromDate, assetClasses),
    getFlowsByAssetClass(subClient, trusts, accounts, benchmarkFromDate, assetClasses),
  ]);

  // --- Monthly portfolio returns (drives the new bar chart) ----------------
  // Bucket nav series by calendar month, take the latest snapshot per month
  // as that month's end NAV. Bucket flows by month too. Then per month-pair
  // (M-1, M): return = (end - start - flows) / (start + 0.5*flows).
  const navByMonth = (() => {
    const map = new Map<string, { date: string; nav: number }>();
    for (const point of navSeries) {
      const monthKey = `${point.snapshot_date.slice(0, 7)}-01`;
      const cur = map.get(monthKey);
      if (!cur || point.snapshot_date > cur.date) {
        map.set(monthKey, { date: point.snapshot_date, nav: point.nav });
      }
    }
    return map;
  })();

  const flowsByMonth = (() => {
    const map = new Map<string, number>();
    const flowSets = assetClasses.length
      ? Object.values(flowsByClass).filter((arr): arr is NonNullable<typeof arr> => arr != null)
      : Object.values(flowsByTrust).filter((arr): arr is NonNullable<typeof arr> => arr != null);
    for (const arr of flowSets) {
      for (const f of arr) {
        const monthKey = `${f.date.slice(0, 7)}-01`;
        map.set(monthKey, (map.get(monthKey) ?? 0) + f.amount);
      }
    }
    return map;
  })();

  const sortedMonths = Array.from(navByMonth.keys()).sort();
  const monthlyReturns: MonthlyReturnRow[] = [];
  for (let i = 1; i < sortedMonths.length; i++) {
    const month = sortedMonths[i];
    if (month < fromMonth) continue;
    const start_nav = navByMonth.get(sortedMonths[i - 1])?.nav ?? 0;
    const end_nav = navByMonth.get(month)?.nav ?? 0;
    const flows = flowsByMonth.get(month) ?? 0;
    const gain = end_nav - start_nav - flows;
    const denom = start_nav + 0.5 * flows;
    const return_pct = denom > 0 ? gain / denom : null;
    monthlyReturns.push({
      month,
      label: monthIsoToLabel(month),
      return_pct,
      gain,
      start_nav,
      end_nav,
      flows,
    });
  }

  // Group attribution rows by month for instant client-side drill-in.
  const attributionByMonth: Record<string, MonthlyAttributionRow[]> = {};
  for (const r of attribution) {
    if (!attributionByMonth[r.month]) attributionByMonth[r.month] = [];
    attributionByMonth[r.month].push(r);
  }

  // --- Trust matrix --------------------------------------------------------
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
      flowsByClass[className] ?? [],
      { endNav, endNavYesterday },
    );
    classNav[className] = endNav;
  }

  // Index returns for the matrices' benchmark row.
  const indexReturns = computeIndexReturnsForAllPeriods(indexPrices, returns);

  const reportingCcy = positions[0]?.reporting_ccy ?? "USD";

  const scopeNote =
    [
      trusts.length === 1
        ? `Entity: ${trusts[0]}`
        : trusts.length > 1
          ? `${trusts.length} entities`
          : null,
      accounts.length > 0 ? `${accounts.length} account${accounts.length > 1 ? "s" : ""} scoped` : null,
      assetClasses.length > 0
        ? `${assetClasses.length === 1 ? assetClasses[0] : `${assetClasses.length} asset classes`}`
        : null,
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

      <MonthlyAttributionSection
        monthlyReturns={monthlyReturns}
        attributionByMonth={attributionByMonth}
        reportingCcy={reportingCcy}
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
