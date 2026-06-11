// Pure compute for the Net Worth / All Assets view. No SQL, no next/headers —
// safe to import from client components. Mirrors lib/returns.ts conventions.
//
// Input rows come from v_net_worth_positions (listed + non-listed combined).
// These assets have NO daily price and NO benchmark — this module only sums
// point-in-time NAVs into allocation + entity/branch breakdowns. It never
// computes a return (see docs/all_assets_integration_design.md §7).

export interface NetWorthRow {
  book: "listed" | "non-listed";
  entity_alias: string | null;
  vehicle_alias: string | null;
  account_alias: string | null;
  asset_class: string | null;
  security_type: string | null;
  mv_reporting: number;
  reporting_ccy: string;
}

export interface AllocationRow {
  asset_class: string;
  mv: number;
  pct: number; // share of total assets, 0..1
  periodReturn?: number | null; // blended return for the selected period, 0..1
}

// The return periods we ingest into performance_snapshot (period code -> label).
// 3M (code 2) is intentionally not pulled.
export const RETURN_PERIODS: { code: number; label: string }[] = [
  { code: 0, label: "MTD" },
  { code: 1, label: "YTD" },
  { code: 3, label: "6M" },
  { code: 4, label: "12M" },
];

// Modified-Dietz return from period components. null when there's no base.
export function periodReturn(c: {
  start: number;
  end: number;
  flows: number;
}): number | null {
  const den = c.start + 0.5 * c.flows;
  if (!den) return null;
  return (c.end - c.start - c.flows) / den;
}

export interface AllocationSummary {
  categories: AllocationRow[];
  totalAssets: number; // sum of asset categories (Loans = receivable only)
  loanPayable: number; // negative; shown below Total Assets
  netWorth: number; // totalAssets + loanPayable
  reportingCcy: string;
}

export interface BreakdownRow {
  key: string; // entity or branch label
  total: number;
  listed: number;
  nonListed: number;
}

const LOANS = "Loans";

// Masttro shows "Loans" (receivable) as an asset line and "Loan Payable" as a
// negative line below Total Assets. Within asset_class "Loans" we therefore
// split by sign: positive = receivable (an asset), negative = payable. Every
// other class is summed net (e.g. an overdraft nets within Cash, as Masttro does).
export function computeAllocation(rows: NetWorthRow[]): AllocationSummary {
  const cat = new Map<string, number>();
  let loanPayable = 0;
  let reportingCcy = "";
  for (const r of rows) {
    if (!reportingCcy && r.reporting_ccy) reportingCcy = r.reporting_ccy;
    const ac = r.asset_class ?? "(unclassified)";
    const mv = r.mv_reporting;
    if (ac === LOANS && mv < 0) {
      loanPayable += mv;
      continue;
    }
    cat.set(ac, (cat.get(ac) ?? 0) + mv);
  }
  const totalAssets = Array.from(cat.values()).reduce((s, v) => s + v, 0);
  const categories: AllocationRow[] = Array.from(cat.entries())
    .map(([asset_class, mv]) => ({
      asset_class,
      mv,
      pct: totalAssets !== 0 ? mv / totalAssets : 0,
    }))
    .sort((a, b) => b.mv - a.mv);
  return {
    categories,
    totalAssets,
    loanPayable,
    netWorth: totalAssets + loanPayable,
    reportingCcy,
  };
}

// Group net worth by entity (default) or branch. branchMap maps entity_alias ->
// branch; entities absent from it (e.g. branch-fallback entities, which already
// ARE a branch) fall back to their own label.
export function computeBreakdown(
  rows: NetWorthRow[],
  branchMap: Record<string, string>,
  groupBy: "entity" | "branch",
): BreakdownRow[] {
  const g = new Map<string, { total: number; listed: number; nonListed: number }>();
  for (const r of rows) {
    const entity = r.entity_alias ?? "(unattributed)";
    const key = groupBy === "branch" ? branchMap[entity] ?? entity : entity;
    const b = g.get(key) ?? { total: 0, listed: 0, nonListed: 0 };
    b.total += r.mv_reporting;
    if (r.book === "listed") b.listed += r.mv_reporting;
    else b.nonListed += r.mv_reporting;
    g.set(key, b);
  }
  return Array.from(g.entries())
    .map(([key, v]) => ({ key, ...v }))
    .sort((a, b) => b.total - a.total);
}
