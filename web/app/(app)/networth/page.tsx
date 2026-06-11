import KpiTile from "@/components/KpiTile";
import NetWorthAllocationTable from "@/components/NetWorthAllocationTable";
import NetWorthBreakdown from "@/components/NetWorthBreakdown";
import PeriodSelector from "@/components/PeriodSelector";
import {
  getNetWorthRows,
  getOneDayByClass,
  getPerformanceByClass,
} from "@/lib/queries";
import {
  computeAllocation,
  computeBreakdown,
  ONE_DAY,
  periodReturn,
  RETURN_PERIODS,
} from "@/lib/networth";
import { getSelectedTrusts, getSelectedVehicles } from "@/lib/trust-filter";
import { getActiveSubClient } from "@/lib/session";
import { money } from "@/lib/format";

export const dynamic = "force-dynamic";

export default async function NetWorthPage({
  searchParams,
}: {
  searchParams: { period?: string };
}) {
  const subClient = await getActiveSubClient();
  const trusts = getSelectedTrusts();
  const vehicles = getSelectedVehicles();
  const period = RETURN_PERIODS.some(p => p.code === Number(searchParams.period))
    ? Number(searchParams.period)
    : 4;
  const periodLabel = RETURN_PERIODS.find(p => p.code === period)?.label ?? "12M";

  const isOneDay = period === ONE_DAY;
  const [rows, perfRaw] = await Promise.all([
    getNetWorthRows(subClient, trusts, vehicles),
    isOneDay
      ? getOneDayByClass(subClient, trusts, vehicles)
      : getPerformanceByClass(subClient, period, trusts),
  ]);

  const baseSummary = computeAllocation(rows);
  // baseSummary.mv is the live net-worth value per class — yfinance-refreshed for
  // listed (migration 042) + current non-listed. That is the END NAV for returns.
  const mvByClass: Record<string, number> = {};
  for (const c of baseSummary.categories) mvByClass[c.asset_class] = c.mv;

  // Per-class return components {start, end, flows} for the selected period.
  let perfByClass: Record<string, { start: number; end: number; flows: number }> = {};
  let totalComp: { start: number; end: number; flows: number };

  if (isOneDay) {
    // 1D is computed live (listed today-vs-yesterday from yfinance); non-liquid
    // assets are flat but still part of the NAV base (so they dilute the move).
    const nonListed: Record<string, number> = {};
    for (const r of rows) {
      if (r.book === "non-listed") {
        const ac = r.asset_class ?? "(unclassified)";
        nonListed[ac] = (nonListed[ac] ?? 0) + r.mv_reporting;
      }
    }
    for (const ac of new Set([...Object.keys(perfRaw), ...Object.keys(nonListed)])) {
      const l = perfRaw[ac] ?? { start: 0, end: 0, flows: 0 };
      const flat = nonListed[ac] ?? 0;
      perfByClass[ac] = { start: l.start + flat, end: l.end + flat, flows: 0 };
    }
    totalComp = Object.values(perfByClass).reduce(
      (a, c) => ({ start: a.start + c.start, end: a.end + c.end, flows: a.flows + c.flows }),
      { start: 0, end: 0, flows: 0 },
    );
  } else {
    // MTD/etc: START + flows from Masttro /Performance; END = the live net-worth
    // value (yfinance listed + current non-listed) — so returns use yfinance like
    // the rest of the tool, not Masttro's lagged price.
    for (const ac in perfRaw) {
      perfByClass[ac] = {
        start: perfRaw[ac].start,
        end: mvByClass[ac] ?? perfRaw[ac].end,
        flows: perfRaw[ac].flows,
      };
    }
    totalComp = {
      start: Object.values(perfRaw).reduce((a, c) => a + c.start, 0),
      end: baseSummary.netWorth, // live, incl loan payable — matches /Performance scope
      flows: Object.values(perfRaw).reduce((a, c) => a + c.flows, 0),
    };
  }

  const summary = {
    ...baseSummary,
    categories: baseSummary.categories.map(c => ({
      ...c,
      periodReturn: perfByClass[c.asset_class]
        ? periodReturn(perfByClass[c.asset_class])
        : null,
    })),
  };
  const totalReturn = periodReturn(totalComp);
  // branchMap unused for the entity grouping — pass an empty map.
  const byEntity = computeBreakdown(rows, {}, "entity");
  const ccy = summary.reportingCcy || "USD";

  const listedTotal = rows
    .filter(r => r.book === "listed")
    .reduce((s, r) => s + r.mv_reporting, 0);
  const nonListedTotal = rows
    .filter(r => r.book === "non-listed")
    .reduce((s, r) => s + r.mv_reporting, 0);

  const scopeNote =
    [
      trusts.length === 1
        ? `Entity: ${trusts[0]}`
        : trusts.length > 1
          ? `${trusts.length} entities`
          : null,
      vehicles.length > 0
        ? `${vehicles.length} vehicle${vehicles.length > 1 ? "s" : ""}`
        : null,
    ]
      .filter(Boolean)
      .join(" · ") || `All assets under ${subClient}`;

  return (
    <main className="mx-auto max-w-7xl space-y-8 px-4 py-8 sm:px-6">
      <div className="flex flex-wrap items-baseline justify-between gap-2">
        <h1 className="text-2xl font-semibold text-slate-900">Net Worth</h1>
        <span className="text-xs text-slate-500">{scopeNote}</span>
      </div>

      <div className="grid grid-cols-1 gap-4 md:grid-cols-3">
        <KpiTile label="Total assets" value={money(summary.totalAssets, ccy)} />
        <KpiTile
          label="Net worth"
          value={money(summary.netWorth, ccy)}
          hint={summary.loanPayable !== 0 ? `incl. ${money(summary.loanPayable, ccy)} loan payable` : undefined}
        />
        <KpiTile
          label="Listed / non-listed"
          value={`${money(listedTotal, ccy)} / ${money(nonListedTotal, ccy)}`}
          hint="Public-priced vs alternatives & direct holdings"
        />
      </div>

      <section className="space-y-3">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <h2 className="text-base font-semibold text-slate-900">Allocation by asset class</h2>
          <PeriodSelector current={period} />
        </div>
        <NetWorthAllocationTable
          summary={summary}
          periodLabel={periodLabel}
          totalReturn={totalReturn}
        />
        <p className="text-xs text-slate-500">
          {isOneDay ? (
            <>
              1D is today&apos;s price move on listed holdings (yfinance previous
              close); non-liquid assets are held flat but remain in the NAV base.
            </>
          ) : (
            <>
              Blended modified-Dietz over all assets{" "}
              {trusts.length
                ? `for the selected ${trusts.length === 1 ? "entity" : "entities"}`
                : `at the ${subClient} level`}
              : end NAV is live (yfinance-refreshed for listed), period start +
              flows from Masttro /Performance. Non-listed NAVs are point-in-time
              (often quarter-lagged).
            </>
          )}
        </p>
      </section>

      <section>
        <NetWorthBreakdown rows={byEntity} reportingCcy={ccy} />
      </section>
    </main>
  );
}
