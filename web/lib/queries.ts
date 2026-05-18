import { getSupabaseServer } from "./supabase-server";
import {
  computeAllPeriodReturns,
  type Flow,
  type NavPoint as DietzNavPoint,
  type PeriodKey,
  type PeriodReturn,
} from "./returns";

export const DEFAULT_SUB_CLIENT = "Dyne Family (US)";

export interface Position {
  account_alias: string;
  custodian: string | null;
  trust_alias: string | null;
  asset_name: string;
  asset_class: string | null;
  security_type: string | null;
  sector: string | null;
  ticker_masttro: string | null;
  isin: string | null;
  local_ccy: string | null;
  quantity: number;
  price_local: number | null;
  mv_local: number | null;
  // mv_reporting is the yfinance-refreshed value when yfinance has the
  // security; otherwise falls back to Masttro's recorded value. Aliased
  // server-side from mv_reporting_refreshed in v_positions_refreshed.
  mv_reporting: number;
  // mv_reporting_yesterday is today's positions priced at yfinance's
  // previous-close. Used for the 1D return only.
  mv_reporting_yesterday: number | null;
  reporting_ccy: string;
  unit_cost_local: number | null;
  total_cost_local: number | null;
  unrealized_gl_local: number | null;
}

export interface NavPoint {
  snapshot_date: string;
  nav: number;
}

export interface Kpis {
  nav: number;
  positions: number;
  trusts: number;
  unrealized_gl: number;
  reporting_ccy: string;
}

export async function listSubClients(): Promise<string[]> {
  const { data, error } = await getSupabaseServer()
    .from("entity_attribution")
    .select("sub_client_alias")
    .not("sub_client_alias", "is", null);
  if (error) throw error;
  const rows = (data ?? []) as unknown as Array<{ sub_client_alias: string }>;
  return Array.from(new Set(rows.map(r => r.sub_client_alias))).sort();
}

export async function listTrusts(
  subClient: string = DEFAULT_SUB_CLIENT,
): Promise<string[]> {
  const { data, error } = await getSupabaseServer()
    .from("entity_attribution")
    .select("trust_alias")
    .eq("sub_client_alias", subClient)
    .not("trust_alias", "is", null);
  if (error) throw error;
  const rows = (data ?? []) as unknown as Array<{ trust_alias: string }>;
  return Array.from(new Set(rows.map(r => r.trust_alias))).sort();
}

export async function getLatestPositions(
  subClient: string = DEFAULT_SUB_CLIENT,
  trust: string | null = null,
): Promise<Position[]> {
  // Query v_positions_refreshed (joins yfinance via pricing_refresh) so the
  // NAV figures throughout the dashboard reflect today's market price when
  // yfinance has the security. PostgREST alias `mv_reporting:mv_reporting_refreshed`
  // renames the refreshed column to mv_reporting so every consumer of Position
  // (Overview NAV, Holdings table, computeKpis) picks up the refreshed value
  // without a per-call change. mv_reporting_yesterday is exposed separately
  // for the 1D return.
  let q = getSupabaseServer()
    .from("v_positions_refreshed")
    .select(
      "account_alias, custodian, trust_alias, asset_name, asset_class, " +
        "security_type, sector, ticker_masttro, isin, local_ccy, quantity, " +
        "price_local, mv_local, mv_reporting:mv_reporting_refreshed, " +
        "mv_reporting_yesterday, reporting_ccy, unit_cost_local, " +
        "total_cost_local, unrealized_gl_local",
    )
    .eq("sub_client_alias", subClient);
  if (trust) q = q.eq("trust_alias", trust);
  const { data, error } = await q.order("mv_reporting_refreshed", {
    ascending: false,
    nullsFirst: false,
  });
  if (error) throw error;
  return (data ?? []) as unknown as Position[];
}

export async function getNavSeries(
  subClient: string = DEFAULT_SUB_CLIENT,
  trust: string | null = null,
): Promise<NavPoint[]> {
  let q = getSupabaseServer()
    .from("v_nav_monthly_by_account")
    .select("snapshot_date, nav_reporting")
    .eq("sub_client_alias", subClient);
  if (trust) q = q.eq("trust_alias", trust);
  const { data, error } = await q;
  if (error) throw error;

  // Aggregate per snapshot_date across accounts in JS — PostgREST doesn't
  // expose SUM/GROUP BY without an RPC, and the row count is small.
  const rows = (data ?? []) as unknown as Array<{
    snapshot_date: string;
    nav_reporting: number | null;
  }>;
  const byDate = new Map<string, number>();
  for (const row of rows) {
    byDate.set(
      row.snapshot_date,
      (byDate.get(row.snapshot_date) ?? 0) + Number(row.nav_reporting ?? 0),
    );
  }
  return Array.from(byDate.entries())
    .map(([snapshot_date, nav]) => ({ snapshot_date, nav }))
    .sort((a, b) => a.snapshot_date.localeCompare(b.snapshot_date));
}

export interface PeriodReturnOverrides {
  // Refreshed end NAV (sum of mv_reporting from getLatestPositions). Used in
  // place of the last point in the historical NAV series, since historical
  // is Masttro-only and may be a day or two behind market.
  endNav?: number;
  // Today's positions valued at yfinance previous-close. Used as the start
  // NAV for the 1D return so it reflects pure intraday price movement.
  endNavYesterday?: number;
}

export async function getPeriodReturns(
  subClient: string = DEFAULT_SUB_CLIENT,
  trust: string | null = null,
  overrides: PeriodReturnOverrides = {},
): Promise<Record<PeriodKey, PeriodReturn>> {
  // NAV series across the whole history (we re-use this for the chart anyway,
  // so cost is one row-set per request — small).
  const navs = await getNavSeries(subClient, trust);
  if (navs.length === 0) {
    return computeAllPeriodReturns([], [], overrides);
  }

  // Pull external flows back to the earliest NAV date. PostgREST filters
  // can't see our app-level cookie; we filter by date/scope here.
  const earliest = navs[0].snapshot_date;
  let q = getSupabaseServer()
    .from("v_external_flows")
    .select("transaction_date, net_amount_reporting, trust_alias, sub_client_alias")
    .eq("sub_client_alias", subClient)
    .gte("transaction_date", earliest);
  if (trust) q = q.eq("trust_alias", trust);
  const { data, error } = await q;
  if (error) throw error;

  const flows: Flow[] = (data ?? []).map(r => ({
    date: (r as { transaction_date: string }).transaction_date,
    amount: Number((r as { net_amount_reporting: number | null }).net_amount_reporting ?? 0),
  }));

  const navPoints: DietzNavPoint[] = navs.map(n => ({
    date: n.snapshot_date,
    nav: n.nav,
  }));

  return computeAllPeriodReturns(navPoints, flows, overrides);
}

export function computeKpis(positions: Position[]): Kpis {
  const trusts = new Set(
    positions.map(p => p.trust_alias).filter((t): t is string => !!t),
  );
  // Number() coercion is mandatory — Supabase serialises NUMERIC as strings
  // when values risk overflowing JS precision, and "0" + "123" is "0123",
  // not 123. Without this, the sum silently produces a concatenated string
  // that Intl.NumberFormat displays as $NaN once it gets large enough.
  const nav = positions.reduce((s, p) => s + Number(p.mv_reporting ?? 0), 0);
  const unrealized_gl = positions.reduce(
    (s, p) => s + Number(p.unrealized_gl_local ?? 0),
    0,
  );
  return {
    nav,
    positions: positions.length,
    trusts: trusts.size,
    unrealized_gl,
    reporting_ccy: positions[0]?.reporting_ccy ?? "USD",
  };
}
