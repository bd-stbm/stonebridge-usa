import KpiTile from "@/components/KpiTile";
import MonthlyIncomeChart, {
  type MonthlyIncomePoint,
} from "@/components/MonthlyIncomeChart";
import TopPayersTable, { type PayerRow } from "@/components/TopPayersTable";
import IncomeByTrustTable, {
  type TrustIncomeRow,
} from "@/components/IncomeByTrustTable";
import {
  computeKpis,
  getIncomeRows,
  getLatestPositions,
  type IncomeRow,
} from "@/lib/queries";
import {
  getSelectedAccounts,
  getSelectedAssetClasses,
  getSelectedSubClient,
  getSelectedTrusts,
} from "@/lib/trust-filter";
import { money, pct } from "@/lib/format";

export const dynamic = "force-dynamic";

const CHART_MONTHS = 18;
const TYPE_DIVIDENDS = "Cash Dividends";
const TYPE_INTEREST = "Interest";
const TYPE_OTHER = "Income";

function isoMonth(d: Date): string {
  return d.toISOString().slice(0, 10);
}

function sumAmount(rows: IncomeRow[]): number {
  return rows.reduce((s, r) => s + r.amount, 0);
}

export default async function IncomePage() {
  const subClient = getSelectedSubClient();
  const trusts = getSelectedTrusts();
  const accounts = getSelectedAccounts();
  const assetClasses = getSelectedAssetClasses();

  const today = new Date();
  // We need at least 13 months of history so TTM works regardless of where
  // we are in the current month; pad a bit more for the chart's 18-month
  // window.
  const fromDate = isoMonth(
    new Date(Date.UTC(today.getUTCFullYear() - 2, today.getUTCMonth(), 1)),
  );

  const [positions, incomeRows] = await Promise.all([
    getLatestPositions(subClient, trusts, accounts, assetClasses),
    getIncomeRows(subClient, trusts, accounts, fromDate, assetClasses),
  ]);
  const kpis = computeKpis(positions);

  // --- Date windows ---------------------------------------------------------
  const ttmStartIso = isoMonth(
    new Date(Date.UTC(today.getUTCFullYear() - 1, today.getUTCMonth(), 1)),
  );
  const ytdStartIso = `${today.getUTCFullYear()}-01-01`;
  const lastMonthStart = isoMonth(
    new Date(Date.UTC(today.getUTCFullYear(), today.getUTCMonth() - 1, 1)),
  );

  const ttmRows = incomeRows.filter(r => r.month >= ttmStartIso);
  const ytdRows = incomeRows.filter(r => r.month >= ytdStartIso);
  const lastMonthRows = incomeRows.filter(r => r.month === lastMonthStart);

  const ttmIncome = sumAmount(ttmRows);
  const ytdIncome = sumAmount(ytdRows);
  const lastMonthIncome = sumAmount(lastMonthRows);
  const ttmYield = kpis.nav > 0 ? ttmIncome / kpis.nav : null;

  // --- Monthly chart series -------------------------------------------------
  const monthlySeries: MonthlyIncomePoint[] = [];
  for (let i = CHART_MONTHS - 1; i >= 0; i--) {
    const d = new Date(
      Date.UTC(today.getUTCFullYear(), today.getUTCMonth() - i, 1),
    );
    const m = isoMonth(d);
    const rowsForMonth = incomeRows.filter(r => r.month === m);
    monthlySeries.push({
      month: m,
      Dividends: rowsForMonth
        .filter(r => r.transaction_type === TYPE_DIVIDENDS)
        .reduce((s, r) => s + r.amount, 0),
      Interest: rowsForMonth
        .filter(r => r.transaction_type === TYPE_INTEREST)
        .reduce((s, r) => s + r.amount, 0),
      Other: rowsForMonth
        .filter(r => r.transaction_type === TYPE_OTHER)
        .reduce((s, r) => s + r.amount, 0),
    });
  }

  // --- Top payers (by TTM income) ------------------------------------------
  // Bucket TTM rows by security_id, then enrich with current MV from the
  // positions array so we can compute yield.
  type Bucket = {
    security_id: number | null;
    asset_name: string;
    asset_class: string | null;
    ticker_masttro: string | null;
    ttm_income: number;
  };
  const securityBuckets = new Map<string, Bucket>();
  for (const r of ttmRows) {
    const key = r.security_id != null ? String(r.security_id) : `name:${r.asset_name}`;
    const existing = securityBuckets.get(key);
    if (existing) {
      existing.ttm_income += r.amount;
    } else {
      securityBuckets.set(key, {
        security_id: r.security_id,
        asset_name: r.asset_name ?? "Unknown",
        asset_class: r.asset_class,
        ticker_masttro: r.ticker_masttro,
        ttm_income: r.amount,
      });
    }
  }

  // Current MV per security: positions don't currently carry security_id, so
  // we match on asset_name (which is what the table groups on). Good enough
  // for v1; tighten with a security_id pass once we add it to Position.
  const mvByName = new Map<string, number>();
  for (const p of positions) {
    mvByName.set(
      p.asset_name,
      (mvByName.get(p.asset_name) ?? 0) + Number(p.mv_reporting ?? 0),
    );
  }

  const totalIncomeForWeights = sumAmount(ttmRows);
  const payerRows: PayerRow[] = Array.from(securityBuckets.values())
    .map(b => ({
      security_id: b.security_id,
      asset_name: b.asset_name,
      asset_class: b.asset_class,
      ticker_masttro: b.ticker_masttro,
      ttm_income: b.ttm_income,
      current_mv: mvByName.get(b.asset_name) ?? 0,
      weight_of_income:
        totalIncomeForWeights > 0 ? b.ttm_income / totalIncomeForWeights : 0,
    }))
    .filter(r => r.ttm_income > 0)
    .sort((a, b) => b.ttm_income - a.ttm_income);

  // --- Income by trust ------------------------------------------------------
  // Only show this table when the user hasn't already filtered to a single
  // trust (in which case it'd be redundant).
  // Show the by-trust table when we're not already narrowed to one trust.
  const showByTrust = trusts.length !== 1;
  let trustRows: TrustIncomeRow[] = [];
  if (showByTrust) {
    const trustBuckets = new Map<
      string,
      { ttm: number; ytd: number; lastMonth: number }
    >();
    for (const r of incomeRows) {
      const t = r.trust_alias;
      if (!t) continue;
      const b =
        trustBuckets.get(t) ?? { ttm: 0, ytd: 0, lastMonth: 0 };
      if (r.month >= ttmStartIso) b.ttm += r.amount;
      if (r.month >= ytdStartIso) b.ytd += r.amount;
      if (r.month === lastMonthStart) b.lastMonth += r.amount;
      trustBuckets.set(t, b);
    }
    const navByTrust = new Map<string, number>();
    for (const p of positions) {
      if (!p.trust_alias) continue;
      navByTrust.set(
        p.trust_alias,
        (navByTrust.get(p.trust_alias) ?? 0) + Number(p.mv_reporting ?? 0),
      );
    }
    trustRows = Array.from(trustBuckets.entries())
      .map(([trust_alias, b]) => ({
        trust_alias,
        ttm_income: b.ttm,
        ytd_income: b.ytd,
        last_month_income: b.lastMonth,
        current_nav: navByTrust.get(trust_alias) ?? 0,
      }))
      .sort((a, b) => b.ttm_income - a.ttm_income);
  }

  const scopeNote =
    [
      trusts.length === 1
        ? `Entity: ${trusts[0]}`
        : trusts.length > 1
          ? `${trusts.length} entities`
          : null,
      accounts.length > 0
        ? `${accounts.length} account${accounts.length > 1 ? "s" : ""} scoped`
        : null,
    ]
      .filter(Boolean)
      .join(" · ") || "All entities under " + subClient;

  return (
    <main className="mx-auto max-w-7xl space-y-8 px-6 py-8">
      <div className="flex items-baseline justify-between">
        <h1 className="text-2xl font-semibold text-slate-900">Income</h1>
        <span className="text-xs text-slate-500">{scopeNote}</span>
      </div>

      <div className="grid grid-cols-1 gap-4 md:grid-cols-4">
        <KpiTile
          label="TTM income"
          value={money(ttmIncome, kpis.reporting_ccy)}
        />
        <KpiTile
          label="YTD income"
          value={money(ytdIncome, kpis.reporting_ccy)}
        />
        <KpiTile
          label="Last month"
          value={money(lastMonthIncome, kpis.reporting_ccy)}
          hint={lastMonthStart.slice(0, 7)}
        />
        <KpiTile
          label="TTM yield"
          value={ttmYield != null ? pct(ttmYield, 2) : "—"}
          hint="TTM income / NAV"
        />
      </div>

      <MonthlyIncomeChart
        data={monthlySeries}
        reportingCcy={kpis.reporting_ccy}
      />

      <section>
        <div className="mb-3 flex items-baseline justify-between">
          <h2 className="text-base font-semibold text-slate-900">Top payers (TTM)</h2>
          <span className="text-xs text-slate-500">
            {payerRows.length} securities · top {Math.min(15, payerRows.length)} shown
          </span>
        </div>
        <TopPayersTable rows={payerRows} reportingCcy={kpis.reporting_ccy} />
      </section>

      {showByTrust ? (
        <section>
          <h2 className="mb-3 text-base font-semibold text-slate-900">Income by entity</h2>
          <IncomeByTrustTable rows={trustRows} reportingCcy={kpis.reporting_ccy} />
        </section>
      ) : null}
    </main>
  );
}
