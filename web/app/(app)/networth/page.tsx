import KpiTile from "@/components/KpiTile";
import NetWorthAllocationTable from "@/components/NetWorthAllocationTable";
import NetWorthBreakdown from "@/components/NetWorthBreakdown";
import PeriodSelector from "@/components/PeriodSelector";
import { getNetWorthRows, getPerformanceByClass } from "@/lib/queries";
import {
  computeAllocation,
  computeBreakdown,
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

  const [rows, perfByClass] = await Promise.all([
    getNetWorthRows(subClient, trusts, vehicles),
    getPerformanceByClass(subClient, period, trusts),
  ]);

  const baseSummary = computeAllocation(rows);
  // Attach the period return per asset class (family-level, all-assets — exact
  // per the performance_snapshot reconciliation).
  const summary = {
    ...baseSummary,
    categories: baseSummary.categories.map(c => ({
      ...c,
      periodReturn: perfByClass[c.asset_class]
        ? periodReturn(perfByClass[c.asset_class])
        : null,
    })),
  };
  const totals = Object.values(perfByClass).reduce(
    (a, c) => ({ start: a.start + c.start, end: a.end + c.end, flows: a.flows + c.flows }),
    { start: 0, end: 0, flows: 0 },
  );
  const totalReturn = periodReturn(totals);
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
          Returns are a blended modified-Dietz over all assets (listed +
          non-listed) from Masttro /Performance,{" "}
          {trusts.length
            ? `for the selected ${trusts.length === 1 ? "entity" : "entities"}`
            : `at the ${subClient} level`}
          . Non-listed NAVs are point-in-time (often quarter-lagged).
        </p>
      </section>

      <section>
        <NetWorthBreakdown rows={byEntity} reportingCcy={ccy} />
      </section>
    </main>
  );
}
