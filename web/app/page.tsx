import Header from "@/components/Header";
import KpiTile from "@/components/KpiTile";
import HoldingsTable from "@/components/HoldingsTable";
import NavChart from "@/components/NavChart";
import {
  DEFAULT_SUB_CLIENT,
  computeKpis,
  getLatestPositions,
  getNavSeries,
} from "@/lib/queries";
import { money, pct } from "@/lib/format";

export const dynamic = "force-dynamic";

export default async function OverviewPage() {
  const [positions, navSeries] = await Promise.all([
    getLatestPositions(DEFAULT_SUB_CLIENT),
    getNavSeries(DEFAULT_SUB_CLIENT),
  ]);
  const kpis = computeKpis(positions);

  const periodReturn =
    navSeries.length >= 2 && navSeries[0].nav > 0
      ? (navSeries[navSeries.length - 1].nav - navSeries[0].nav) /
        navSeries[0].nav
      : null;
  const navFromHistory =
    navSeries.length > 0 ? navSeries[navSeries.length - 1].nav : null;

  return (
    <>
      <Header subClient={DEFAULT_SUB_CLIENT} />
      <main className="mx-auto max-w-7xl px-6 py-8">
        <div className="grid grid-cols-1 gap-4 md:grid-cols-4">
          <KpiTile label="NAV (latest)" value={money(kpis.nav, kpis.reporting_ccy)} />
          <KpiTile
            label="Return (period)"
            value={periodReturn != null ? pct(periodReturn, 2) : "—"}
            hint={
              navSeries.length >= 2
                ? `${navSeries[0].snapshot_date.slice(0, 7)} → ${navSeries[navSeries.length - 1].snapshot_date.slice(0, 7)}`
                : undefined
            }
            tone={
              periodReturn == null
                ? "default"
                : periodReturn >= 0
                  ? "positive"
                  : "negative"
            }
          />
          <KpiTile label="Trusts" value={kpis.trusts.toString()} hint={`${kpis.positions} positions`} />
          <KpiTile
            label="Unrealized G/L"
            value={money(kpis.unrealized_gl, kpis.reporting_ccy)}
            tone={kpis.unrealized_gl >= 0 ? "positive" : "negative"}
          />
        </div>

        <section className="mt-8">
          <NavChart data={navSeries} />
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
    </>
  );
}
