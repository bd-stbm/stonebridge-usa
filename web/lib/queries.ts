import { getSupabaseServer } from "./supabase-server";
import {
  computeAllPeriodReturns,
  type Flow,
  type IndexPricePoint,
  type NavPoint as DietzNavPoint,
  type PeriodKey,
  type PeriodReturn,
} from "./returns";

// Supabase enforces a project-wide `db-max-rows` cap (default 1000) that
// silently truncates responses. For our single-tenant dashboard we always
// want the full set, so every query that could plausibly exceed 1000 rows
// asks explicitly. Project setting also needs to allow at least this much
// (Settings -> API -> Max rows). Without that, this limit is still bounded
// at the server's cap.
const LIMIT_LARGE = 100000;

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
  // price_local is Masttro's last recorded price for the security.
  price_local: number | null;
  // yf_price is yfinance's latest price (same security currency). When
  // present, it's the right number to display alongside mv_reporting
  // (which uses the same refreshed price). Null for any security yfinance
  // doesn't cover.
  yf_price: number | null;
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
    .not("sub_client_alias", "is", null)
    .limit(LIMIT_LARGE);
  if (error) throw error;
  const rows = (data ?? []) as unknown as Array<{ sub_client_alias: string }>;
  return Array.from(new Set(rows.map(r => r.sub_client_alias))).sort();
}

export interface AccountOption {
  node_id: string;
  alias: string;
  custodian: string | null;
  trust_alias: string | null;
}

export async function listAccounts(
  subClient: string = DEFAULT_SUB_CLIENT,
  trust: string | null = null,
): Promise<AccountOption[]> {
  // Source from v_latest_positions so we only get accounts that actually
  // hold positions in the latest snapshot. Multiple rows per account get
  // deduped in JS.
  let q = getSupabaseServer()
    .from("v_latest_positions")
    .select("account_node_id, account_alias, custodian, trust_alias")
    .eq("sub_client_alias", subClient);
  if (trust) q = q.eq("trust_alias", trust);
  const { data, error } = await q.limit(LIMIT_LARGE);
  if (error) throw error;
  const rows = (data ?? []) as unknown as Array<{
    account_node_id: string;
    account_alias: string | null;
    custodian: string | null;
    trust_alias: string | null;
  }>;
  const seen = new Set<string>();
  const accounts: AccountOption[] = [];
  for (const r of rows) {
    if (seen.has(r.account_node_id)) continue;
    seen.add(r.account_node_id);
    accounts.push({
      node_id: r.account_node_id,
      alias: r.account_alias ?? r.account_node_id,
      custodian: r.custodian,
      trust_alias: r.trust_alias,
    });
  }
  return accounts.sort((a, b) => a.alias.localeCompare(b.alias));
}

export async function listTrusts(
  subClient: string = DEFAULT_SUB_CLIENT,
): Promise<string[]> {
  // Query v_latest_positions rather than entity_attribution so we only
  // surface trusts that actually hold positions in the latest snapshot.
  // entity_attribution includes every node whose ancestor name matches
  // the substring "trust", which catches LLCs like "Deltrust LLC" and
  // other non-position-holding shells. Real fix is at sync-level, but
  // this filter keeps the dropdown clean either way.
  const { data, error } = await getSupabaseServer()
    .from("v_latest_positions")
    .select("trust_alias")
    .eq("sub_client_alias", subClient)
    .not("trust_alias", "is", null)
    .limit(LIMIT_LARGE);
  if (error) throw error;
  const rows = (data ?? []) as unknown as Array<{ trust_alias: string }>;
  return Array.from(new Set(rows.map(r => r.trust_alias))).sort();
}

export async function getLatestPositions(
  subClient: string = DEFAULT_SUB_CLIENT,
  trust: string | null = null,
  account: string | null = null,
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
        "price_local, yf_price, mv_local, " +
        "mv_reporting:mv_reporting_refreshed, mv_reporting_yesterday, " +
        "reporting_ccy, unit_cost_local, total_cost_local, " +
        "unrealized_gl_local",
    )
    .eq("sub_client_alias", subClient);
  if (trust) q = q.eq("trust_alias", trust);
  if (account) q = q.eq("account_node_id", account);
  const { data, error } = await q
    .order("mv_reporting_refreshed", { ascending: false, nullsFirst: false })
    .limit(LIMIT_LARGE);
  if (error) throw error;
  return (data ?? []) as unknown as Position[];
}

export async function getNavSeries(
  subClient: string = DEFAULT_SUB_CLIENT,
  trust: string | null = null,
  account: string | null = null,
): Promise<NavPoint[]> {
  let q = getSupabaseServer()
    .from("v_nav_monthly_by_account")
    .select("snapshot_date, nav_reporting")
    .eq("sub_client_alias", subClient);
  if (trust) q = q.eq("trust_alias", trust);
  if (account) q = q.eq("account_node_id", account);
  const { data, error } = await q.limit(LIMIT_LARGE);
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
  account: string | null = null,
  overrides: PeriodReturnOverrides = {},
): Promise<Record<PeriodKey, PeriodReturn>> {
  // NAV series across the whole history (we re-use this for the chart anyway,
  // so cost is one row-set per request — small).
  const navs = await getNavSeries(subClient, trust, account);
  if (navs.length === 0) {
    return computeAllPeriodReturns([], [], overrides);
  }

  // Pull external flows back to the earliest NAV date. PostgREST filters
  // can't see our app-level cookie; we filter by date/scope here.
  const earliest = navs[0].snapshot_date;
  let q = getSupabaseServer()
    .from("v_external_flows")
    .select(
      "transaction_date, net_amount_reporting, trust_alias, " +
        "sub_client_alias, account_node_id",
    )
    .eq("sub_client_alias", subClient)
    .gte("transaction_date", earliest);
  if (trust) q = q.eq("trust_alias", trust);
  if (account) q = q.eq("account_node_id", account);
  const { data, error } = await q.limit(LIMIT_LARGE);
  if (error) throw error;

  const flowRows = (data ?? []) as unknown as Array<{
    transaction_date: string;
    net_amount_reporting: number | null;
  }>;
  const flows: Flow[] = flowRows.map(r => ({
    date: r.transaction_date,
    amount: Number(r.net_amount_reporting ?? 0),
  }));

  const navPoints: DietzNavPoint[] = navs.map(n => ({
    date: n.snapshot_date,
    nav: n.nav,
  }));

  return computeAllPeriodReturns(navPoints, flows, overrides);
}

export async function getNavSeriesByTrust(
  subClient: string = DEFAULT_SUB_CLIENT,
  trust: string | null = null,
  account: string | null = null,
): Promise<Record<string, NavPoint[]>> {
  // Per-(snapshot_date, account) NAV rows from v_nav_monthly_by_account,
  // aggregated by trust_alias in JS for the Performance page's matrix.
  // When global trust filter is set we get one trust back; when null we get
  // all trusts under the sub-client.
  let q = getSupabaseServer()
    .from("v_nav_monthly_by_account")
    .select("snapshot_date, trust_alias, nav_reporting")
    .eq("sub_client_alias", subClient)
    .not("trust_alias", "is", null);
  if (trust) q = q.eq("trust_alias", trust);
  if (account) q = q.eq("account_node_id", account);
  const { data, error } = await q.limit(LIMIT_LARGE);
  if (error) throw error;

  const rows = (data ?? []) as unknown as Array<{
    snapshot_date: string;
    trust_alias: string;
    nav_reporting: number | null;
  }>;

  const byTrust: Map<string, Map<string, number>> = new Map();
  for (const r of rows) {
    let dateMap = byTrust.get(r.trust_alias);
    if (!dateMap) {
      dateMap = new Map();
      byTrust.set(r.trust_alias, dateMap);
    }
    dateMap.set(
      r.snapshot_date,
      (dateMap.get(r.snapshot_date) ?? 0) + Number(r.nav_reporting ?? 0),
    );
  }

  const result: Record<string, NavPoint[]> = {};
  for (const [trustAlias, dateMap] of byTrust.entries()) {
    result[trustAlias] = Array.from(dateMap.entries())
      .map(([snapshot_date, nav]) => ({ snapshot_date, nav }))
      .sort((a, b) => a.snapshot_date.localeCompare(b.snapshot_date));
  }
  return result;
}

export async function getFlowsByTrust(
  subClient: string = DEFAULT_SUB_CLIENT,
  trust: string | null = null,
  account: string | null = null,
  fromDate: string = "2020-01-01",
): Promise<Record<string, Flow[]>> {
  let q = getSupabaseServer()
    .from("v_external_flows")
    .select("transaction_date, net_amount_reporting, trust_alias")
    .eq("sub_client_alias", subClient)
    .gte("transaction_date", fromDate)
    .not("trust_alias", "is", null);
  if (trust) q = q.eq("trust_alias", trust);
  if (account) q = q.eq("account_node_id", account);
  const { data, error } = await q.limit(LIMIT_LARGE);
  if (error) throw error;

  const rows = (data ?? []) as unknown as Array<{
    transaction_date: string;
    net_amount_reporting: number | null;
    trust_alias: string;
  }>;

  const out: Record<string, Flow[]> = {};
  for (const r of rows) {
    if (!out[r.trust_alias]) out[r.trust_alias] = [];
    out[r.trust_alias].push({
      date: r.transaction_date,
      amount: Number(r.net_amount_reporting ?? 0),
    });
  }
  return out;
}

export async function getNavSeriesByAssetClass(
  subClient: string = DEFAULT_SUB_CLIENT,
  trust: string | null = null,
  account: string | null = null,
): Promise<Record<string, NavPoint[]>> {
  // Per-(snapshot_date, account, asset_class) rows from the view. We
  // aggregate across accounts in JS, keyed by asset_class, to get one NAV
  // series per class for the Returns tile's split-by-class dropdown.
  let q = getSupabaseServer()
    .from("v_nav_monthly_by_asset_class")
    .select("snapshot_date, asset_class, nav_reporting")
    .eq("sub_client_alias", subClient);
  if (trust) q = q.eq("trust_alias", trust);
  if (account) q = q.eq("account_node_id", account);
  const { data, error } = await q.limit(LIMIT_LARGE);
  if (error) throw error;

  const rows = (data ?? []) as unknown as Array<{
    snapshot_date: string;
    asset_class: string;
    nav_reporting: number | null;
  }>;

  const byClass: Map<string, Map<string, number>> = new Map();
  for (const r of rows) {
    let dateMap = byClass.get(r.asset_class);
    if (!dateMap) {
      dateMap = new Map();
      byClass.set(r.asset_class, dateMap);
    }
    dateMap.set(
      r.snapshot_date,
      (dateMap.get(r.snapshot_date) ?? 0) + Number(r.nav_reporting ?? 0),
    );
  }

  const result: Record<string, NavPoint[]> = {};
  for (const [ac, dateMap] of byClass.entries()) {
    result[ac] = Array.from(dateMap.entries())
      .map(([snapshot_date, nav]) => ({ snapshot_date, nav }))
      .sort((a, b) => a.snapshot_date.localeCompare(b.snapshot_date));
  }
  return result;
}

// ---------------------------------------------------------------------------
// Income — long-format rows for the Income page.
// ---------------------------------------------------------------------------

export interface IncomeRow {
  month: string;
  account_node_id: string;
  account_alias: string | null;
  trust_alias: string | null;
  security_id: number | null;
  asset_name: string | null;
  asset_class: string | null;
  ticker_masttro: string | null;
  transaction_type: string;
  reporting_ccy: string | null;
  amount: number;
}

export async function getIncomeRows(
  subClient: string = DEFAULT_SUB_CLIENT,
  trust: string | null = null,
  account: string | null = null,
  fromDate: string = "2020-01-01",
): Promise<IncomeRow[]> {
  let q = getSupabaseServer()
    .from("v_income_monthly")
    .select(
      "month, account_node_id, account_alias, trust_alias, security_id, " +
        "asset_name, asset_class, ticker_masttro, transaction_type, " +
        "reporting_ccy, amount",
    )
    .eq("sub_client_alias", subClient)
    .gte("month", fromDate)
    .order("month", { ascending: true });
  if (trust) q = q.eq("trust_alias", trust);
  if (account) q = q.eq("account_node_id", account);
  const { data, error } = await q.limit(LIMIT_LARGE);
  if (error) throw error;
  const rows = (data ?? []) as unknown as Array<
    Omit<IncomeRow, "amount"> & { amount: number | string | null }
  >;
  return rows.map(r => ({ ...r, amount: Number(r.amount ?? 0) }));
}

// ---------------------------------------------------------------------------
// Transactions — generic per-event rows for the Transactions page.
// ---------------------------------------------------------------------------

export interface Transaction {
  transaction_id: number;
  transaction_date: string | null;
  account_node_id: string;
  account_alias: string | null;
  custodian: string | null;
  trust_alias: string | null;
  security_id: number | null;
  asset_name: string | null;
  asset_class: string | null;
  ticker_masttro: string | null;
  transaction_type_clean: string | null;
  comments: string | null;
  quantity: number | null;
  net_price_local: number | null;
  net_amount_local: number | null;
  net_amount_reporting: number | null;
  local_ccy: string | null;
  reporting_ccy: string | null;
  is_external_flow: boolean;
}

export async function getTransactions(
  subClient: string = DEFAULT_SUB_CLIENT,
  trust: string | null = null,
  account: string | null = null,
  fromDate: string = "2020-01-01",
  toDate: string | null = null,
): Promise<Transaction[]> {
  let q = getSupabaseServer()
    .from("v_transactions")
    .select(
      "transaction_id, transaction_date, account_node_id, account_alias, " +
        "custodian, trust_alias, security_id, asset_name, asset_class, " +
        "ticker_masttro, transaction_type_clean, comments, quantity, " +
        "net_price_local, net_amount_local, net_amount_reporting, " +
        "local_ccy, reporting_ccy, is_external_flow",
    )
    .eq("sub_client_alias", subClient)
    .gte("transaction_date", fromDate)
    .order("transaction_date", { ascending: false });
  if (toDate) q = q.lte("transaction_date", toDate);
  if (trust) q = q.eq("trust_alias", trust);
  if (account) q = q.eq("account_node_id", account);
  const { data, error } = await q.limit(LIMIT_LARGE);
  if (error) throw error;
  return (data ?? []) as unknown as Transaction[];
}

// ---------------------------------------------------------------------------
// Index benchmarks — for the Returns-vs-index comparison on the Returns tile.
// ---------------------------------------------------------------------------

export interface IndexOption {
  ticker: string;
  name: string;
}

export async function listIndices(): Promise<IndexOption[]> {
  const { data, error } = await getSupabaseServer()
    .from("index_definition")
    .select("ticker, name")
    .order("ticker");
  if (error) throw error;
  return (data ?? []) as unknown as IndexOption[];
}

export async function getIndexPrices(
  ticker: string,
  fromDate: string,
): Promise<IndexPricePoint[]> {
  const { data, error } = await getSupabaseServer()
    .from("index_price_history")
    .select("price_date, close")
    .eq("ticker", ticker)
    .gte("price_date", fromDate)
    .order("price_date", { ascending: true })
    .limit(LIMIT_LARGE);
  if (error) throw error;
  const rows = (data ?? []) as unknown as Array<{
    price_date: string;
    close: number | string;
  }>;
  return rows.map(r => ({ date: r.price_date, price: Number(r.close) }));
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
