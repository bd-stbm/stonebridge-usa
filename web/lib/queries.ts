import { supabase } from "./supabase";

export const DEFAULT_SUB_CLIENT = "Dyne Family US";

export interface Position {
  account_alias: string;
  trust_alias: string | null;
  asset_name: string;
  asset_class: string | null;
  sector: string | null;
  ticker_masttro: string | null;
  quantity: number;
  price_local: number | null;
  mv_local: number | null;
  mv_reporting: number;
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
  const { data, error } = await supabase()
    .from("entity_attribution")
    .select("sub_client_alias")
    .not("sub_client_alias", "is", null);
  if (error) throw error;
  const rows = (data ?? []) as unknown as Array<{ sub_client_alias: string }>;
  return Array.from(new Set(rows.map(r => r.sub_client_alias))).sort();
}

export async function getLatestPositions(
  subClient: string = DEFAULT_SUB_CLIENT,
): Promise<Position[]> {
  const { data, error } = await supabase()
    .from("v_latest_positions")
    .select(
      "account_alias, trust_alias, asset_name, asset_class, sector, " +
        "ticker_masttro, quantity, price_local, mv_local, mv_reporting, " +
        "reporting_ccy, unit_cost_local, total_cost_local, unrealized_gl_local",
    )
    .eq("sub_client_alias", subClient)
    .order("mv_reporting", { ascending: false, nullsFirst: false });
  if (error) throw error;
  return (data ?? []) as unknown as Position[];
}

export async function getNavSeries(
  subClient: string = DEFAULT_SUB_CLIENT,
): Promise<NavPoint[]> {
  const { data, error } = await supabase()
    .from("v_nav_monthly_by_account")
    .select("snapshot_date, nav_reporting")
    .eq("sub_client_alias", subClient);
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

export function computeKpis(positions: Position[]): Kpis {
  const trusts = new Set(
    positions.map(p => p.trust_alias).filter((t): t is string => !!t),
  );
  const nav = positions.reduce((s, p) => s + (p.mv_reporting ?? 0), 0);
  const unrealized_gl = positions.reduce(
    (s, p) => s + (p.unrealized_gl_local ?? 0),
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
