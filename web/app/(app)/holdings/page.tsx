import HoldingsFullTable from "@/components/HoldingsFullTable";
import {
  computeKpis,
  getHoldingsPeriodGains,
  getLatestPositions,
} from "@/lib/queries";
import {
  getSelectedAccounts,
  getSelectedAssetClasses,
  getSelectedTrusts,
} from "@/lib/trust-filter";
import { getActiveSubClient } from "@/lib/session";

export const dynamic = "force-dynamic";

export default async function HoldingsPage() {
  const subClient = await getActiveSubClient();
  const trusts = getSelectedTrusts();
  const accounts = getSelectedAccounts();
  const assetClasses = getSelectedAssetClasses();
  const [positions, periodGains] = await Promise.all([
    getLatestPositions(subClient, trusts, accounts, assetClasses),
    getHoldingsPeriodGains(subClient, trusts, accounts, assetClasses),
  ]);
  // Serialise the Map for the client component boundary — Map isn't
  // a serialisable type across server-to-client props in Next.js App
  // Router. Rebuilt back into a Map inside HoldingsFullTable.
  const periodGainsEntries = Array.from(periodGains.entries());
  const kpis = computeKpis(positions);
  const visibleAssetClassesCount = new Set(
    positions
      .map(p => p.asset_class)
      .filter((c): c is string => !!c && c.length > 0),
  ).size;

  return (
    <main className="mx-auto max-w-7xl px-6 py-8">
      <div className="mb-6 flex items-baseline justify-between">
        <h1 className="text-2xl font-semibold text-slate-900">Holdings</h1>
        <span className="text-xs text-slate-500">
          Latest snapshot · {positions.length} positions across {kpis.trusts} entities
        </span>
      </div>

      <HoldingsFullTable
        positions={positions}
        reportingCcy={kpis.reporting_ccy}
        periodGainsEntries={periodGainsEntries}
        nav={kpis.nav}
        positionsCount={kpis.positions}
        entitiesCount={kpis.trusts}
        assetClassesCount={visibleAssetClassesCount}
      />
    </main>
  );
}
