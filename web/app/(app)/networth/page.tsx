import KpiTile from "@/components/KpiTile";
import NetWorthAllocationTable from "@/components/NetWorthAllocationTable";
import NetWorthBreakdown from "@/components/NetWorthBreakdown";
import {
  getEntityBranchMap,
  getNetWorthRows,
} from "@/lib/queries";
import { computeAllocation, computeBreakdown } from "@/lib/networth";
import { getSelectedTrusts, getSelectedVehicles } from "@/lib/trust-filter";
import { getActiveSubClient } from "@/lib/session";
import { money } from "@/lib/format";

export const dynamic = "force-dynamic";

export default async function NetWorthPage() {
  const subClient = await getActiveSubClient();
  const trusts = getSelectedTrusts();
  const vehicles = getSelectedVehicles();

  const [rows, branchMap] = await Promise.all([
    getNetWorthRows(subClient, trusts, vehicles),
    getEntityBranchMap(subClient),
  ]);

  const summary = computeAllocation(rows);
  const byEntity = computeBreakdown(rows, branchMap, "entity");
  const byBranch = computeBreakdown(rows, branchMap, "branch");
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
        <h2 className="text-base font-semibold text-slate-900">Allocation by asset class</h2>
        <NetWorthAllocationTable summary={summary} />
        <p className="text-xs text-slate-500">
          Non-listed values are point-in-time NAVs (often quarter-lagged) and carry
          no daily price or benchmark — returns are not computed on this view.
        </p>
      </section>

      <section>
        <NetWorthBreakdown byEntity={byEntity} byBranch={byBranch} reportingCcy={ccy} />
      </section>
    </main>
  );
}
