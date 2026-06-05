import PerformanceMatrix from "@/components/PerformanceMatrix";
import MonthlyAttributionSection from "@/components/MonthlyAttributionSection";
import type { MonthlyReturnRow } from "@/components/MonthlyReturnsBar";
import {
  getFlowsByAssetClass,
  getFlowsByTrust,
  getIndexPrices,
  getLatestPositions,
  getMonthlySecurityAttribution,
  getNavCarryforwardByTrust,
  getNavSeries,
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
  getSelectedTrusts,
} from "@/lib/trust-filter";
import { getActiveSubClient } from "@/lib/session";
import {
  computeAllPeriodReturns,
  computeIndexReturnsForAllPeriods,
  computePeriodStart,
  type PeriodKey,
  type PeriodReturn,
} from "@/lib/returns";

export const dynamic = "force-dynamic";

// Months of attribution data to FETCH from the RPC. Pull one extra so
// the displayed window has a real start_mv for its earliest month.
const ATTRIBUTION_FETCH_MONTHS = 13;
// Months to DISPLAY on the bar chart + aggregate. Standard reporting
// window; keeps the panel label as "Last 12M" and avoids bar-chart
// clutter once we accumulate more history.
const ATTRIBUTION_DISPLAY_MONTHS = 12;

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
  const subClient = await getActiveSubClient();
  const trusts = getSelectedTrusts();
  const accounts = getSelectedAccounts();
  const assetClasses = getSelectedAssetClasses();
  const benchmarkTicker = getSelectedBenchmark();

  // Attribution window: last N completed months + current month.
  const today = new Date();
  const fromMonthDate = new Date(
    Date.UTC(today.getUTCFullYear(), today.getUTCMonth() - ATTRIBUTION_FETCH_MONTHS, 1),
  );
  const fromMonth = fromMonthDate.toISOString().slice(0, 10);

  const [
    positions,
    navSeries,
    navByTrust,
    indices,
    returns,
    attribution,
  ] = await Promise.all([
    getLatestPositions(subClient, trusts, accounts, assetClasses),
    getNavSeries(subClient, trusts, accounts, assetClasses),
    getNavSeriesByTrust(subClient, trusts, accounts, assetClasses),
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
  // navByMonth comes from v_nav_monthly_by_asset_class — Masttro's
  // previous-close snapshot. For the LATEST month, substitute the
  // yfinance-refreshed sum of current positions so the End-NAV KPI
  // in MonthlyAttributionSection matches Overview's NAV tile (both
  // sources scope identically: same sub_client + trusts + accounts +
  // asset_classes). Without this, intraday market movement between
  // Masttro's close and today's live price shows as a 5–6-figure
  // mismatch between the two pages. The PerformanceMatrix already
  // does this via the {endNav, endNavYesterday} override below.
  const refreshedEndNav = sumPosition(positions, "mv_reporting");
  const latestMonth = sortedMonths.length > 0
    ? sortedMonths[sortedMonths.length - 1]
    : null;
  const monthlyReturnsAll: MonthlyReturnRow[] = [];
  for (let i = 1; i < sortedMonths.length; i++) {
    const month = sortedMonths[i];
    if (month < fromMonth) continue;
    const start_nav = navByMonth.get(sortedMonths[i - 1])?.nav ?? 0;
    const end_nav = month === latestMonth
      ? refreshedEndNav
      : (navByMonth.get(month)?.nav ?? 0);
    const flows = flowsByMonth.get(month) ?? 0;
    const gain = end_nav - start_nav - flows;
    const denom = start_nav + 0.5 * flows;
    const return_pct = denom > 0 ? gain / denom : null;
    monthlyReturnsAll.push({
      month,
      label: monthIsoToLabel(month),
      return_pct,
      gain,
      start_nav,
      end_nav,
      flows,
    });
  }
  // Cap display window. Pulling one extra month via ATTRIBUTION_FETCH_MONTHS
  // makes sure the earliest displayed month has a real prior-month
  // start_nav rather than zero — otherwise its bar would look like a
  // huge one-period swing.
  const monthlyReturns = monthlyReturnsAll.slice(-ATTRIBUTION_DISPLAY_MONTHS);

  // Group attribution rows by month for instant client-side drill-in.
  const attributionByMonth: Record<string, MonthlyAttributionRow[]> = {};
  for (const r of attribution) {
    if (!attributionByMonth[r.month]) attributionByMonth[r.month] = [];
    attributionByMonth[r.month].push(r);
  }

  // --- Trust matrix --------------------------------------------------------
  // Per-trust carry-forward start NAVs for each period. Each account is valued
  // at its latest snapshot on/before the period target — the same
  // latest-per-account basis as endNav — so a stale account (e.g. an AU super
  // fund that reports monthly) doesn't surface as a phantom gain in the entity
  // return. These feed computePeriodReturn as startNavByPeriod overrides,
  // replacing the date-exact NAV-series snap for MTD/YTD/6M/1Y (migration 031).
  const matrixPeriods: PeriodKey[] = ["mtd", "ytd", "6m", "1y"];
  const matrixTargets = matrixPeriods.map(p => computePeriodStart(p, today));
  const carryforwardByTarget = await Promise.all(
    matrixTargets.map(date =>
      getNavCarryforwardByTrust(subClient, trusts, accounts, date, assetClasses),
    ),
  );

  const positionsByTrust = groupBy(positions, p => p.trust_alias);
  const trustReturns: Record<string, Record<PeriodKey, PeriodReturn>> = {};
  const trustNav: Record<string, number> = {};
  for (const [trustAlias, navs] of Object.entries(navByTrust)) {
    const trustPositions = positionsByTrust.get(trustAlias) ?? [];
    const endNav = sumPosition(trustPositions, "mv_reporting");
    const endNavYesterday = sumPosition(trustPositions, "mv_reporting_yesterday");
    const startNavByPeriod: Partial<
      Record<PeriodKey, { nav: number; date: string }>
    > = {};
    matrixPeriods.forEach((period, i) => {
      const cf = carryforwardByTarget[i][trustAlias];
      if (cf) startNavByPeriod[period] = { nav: cf.nav, date: cf.anchorDate };
    });
    trustReturns[trustAlias] = computeAllPeriodReturns(
      navs.map(n => ({ date: n.snapshot_date, nav: n.nav })),
      flowsByTrust[trustAlias] ?? [],
      { endNav, endNavYesterday, startNavByPeriod },
    );
    trustNav[trustAlias] = endNav;
  }

  // Index returns for the matrix's benchmark row.
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
    <main className="mx-auto max-w-7xl space-y-8 px-4 py-8 sm:px-6">
      <div className="flex items-baseline justify-between">
        <h1 className="text-2xl font-semibold text-slate-900">Performance</h1>
        <span className="text-xs text-slate-500">
          {scopeNote}
          {benchmark ? ` · benchmark ${benchmark.name}` : ""}
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
        benchmarkLabel={benchmark ? benchmark.name : undefined}
      />
    </main>
  );
}
