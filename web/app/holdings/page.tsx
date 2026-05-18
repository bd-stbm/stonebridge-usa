import Header from "@/components/Header";
import HoldingsFullTable from "@/components/HoldingsFullTable";
import KpiTile from "@/components/KpiTile";
import {
  DEFAULT_SUB_CLIENT,
  computeKpis,
  getLatestPositions,
} from "@/lib/queries";
import { getSelectedTrust } from "@/lib/trust-filter";
import { money } from "@/lib/format";

export const dynamic = "force-dynamic";

export default async function HoldingsPage() {
  const trust = getSelectedTrust();
  const positions = await getLatestPositions(DEFAULT_SUB_CLIENT, trust);
  const kpis = computeKpis(positions);
  const assetClasses = new Set(
    positions
      .map(p => p.asset_class)
      .filter((c): c is string => !!c && c.length > 0),
  );

  return (
    <>
      <Header subClient={DEFAULT_SUB_CLIENT} />
      <main className="mx-auto max-w-7xl px-6 py-8">
        <div className="mb-6 flex items-baseline justify-between">
          <h1 className="text-2xl font-semibold text-slate-900">Holdings</h1>
          <span className="text-xs text-slate-500">
            Latest snapshot · {positions.length} positions across {kpis.trusts} trusts
          </span>
        </div>

        <div className="mb-6 grid grid-cols-2 gap-4 md:grid-cols-4">
          <KpiTile label="NAV" value={money(kpis.nav, kpis.reporting_ccy)} />
          <KpiTile
            label="Unrealized G/L"
            value={money(kpis.unrealized_gl, kpis.reporting_ccy)}
            tone={kpis.unrealized_gl >= 0 ? "positive" : "negative"}
          />
          <KpiTile label="Positions" value={kpis.positions.toString()} />
          <KpiTile
            label="Asset classes"
            value={assetClasses.size.toString()}
            hint={`${kpis.trusts} trusts`}
          />
        </div>

        <HoldingsFullTable positions={positions} reportingCcy={kpis.reporting_ccy} />
      </main>
    </>
  );
}
