import { getSupabaseServer } from "./supabase-server";
import { DEFAULT_SUB_CLIENT } from "./trust-filter";
import {
  computeAllPeriodReturns,
  computePeriodStart,
  type Flow,
  type IndexPricePoint,
  type NavPoint as DietzNavPoint,
  type PeriodKey,
  type PeriodReturn,
} from "./returns";

// Re-export so existing `import { DEFAULT_SUB_CLIENT } from "@/lib/queries"`
// sites keep working without churn. The canonical home is trust-filter.ts.
export { DEFAULT_SUB_CLIENT };

// Supabase enforces a project-wide `db-max-rows` cap (default 1000) that
// silently truncates responses. For our single-tenant dashboard we always
// want the full set, so every query that could plausibly exceed 1000 rows
// asks explicitly. Project setting also needs to allow at least this much
// (Settings -> API -> Max rows). Without that, this limit is still bounded
// at the server's cap.
const LIMIT_LARGE = 100000;

// Entities (trusts / shared vehicles) to hide from the dashboard per sub
// client. Excluded entries don't appear in the Entity filter dropdown, get
// stripped out of every scoped query's WHERE clause, and are passed as
// p_excluded_trusts to reconstructed_nav_at so 6M / 1Y returns also drop
// them. Driven by client preference; edit here to add or remove.
// Always hidden across every sub-client. Cross-family shared vehicles
// (Mamidy, ECA Europe Partners) that surfaced after the shared-vehicle
// canonical-account fix (commit 67571cf) with sub-$25k NAVs — user
// opted to hide as rounding-error noise. Optsia was originally in this
// list for the same reason but turns out to hold ~$6.3M of Goldman
// Sachs positions under Dyne (US) — re-introduced after a review
// confirmed the per-trust slicing attributes correctly to "Optsia
// Investments LLC" (not double-counted under the parent trusts).
const EXCLUDED_ENTITIES_GLOBAL: string[] = [
  "Mamidy Investments LLC",
  "ECA Europe Partners",
];

const EXCLUDED_ENTITIES_BY_SUB_CLIENT: Record<string, string[]> = {
  "Dyne Family (US)": [
    // Berdy is excluded for the US tree only — it surfaced as a small
    // residual under the Dyne US Retirement → Australian Superannuation
    // subtree (tiny US-side NAV, user opted to hide). The same logical
    // entity holds a real ~AUD 1.1m position under Dyne Family (AU),
    // so it remains visible there.
    "Berdy Investment Trust",
    "Sibling Trust IFO Colin Dyne",
    "Sibling Trust IFO Larry Dyne",
    "Silbling Trust IFO Rozanne Bur",
    "The Family Trust",
  ],
};

function excludedEntities(subClient: string): string[] {
  return [
    ...EXCLUDED_ENTITIES_GLOBAL,
    ...(EXCLUDED_ENTITIES_BY_SUB_CLIENT[subClient] ?? []),
  ];
}

// PostgREST `in` value list with safe quoting for values containing
// spaces, commas, or other reserved chars. We just refuse to handle
// embedded double quotes — none of the current entity names have them.
function postgrestInList(values: string[]): string {
  return `(${values.map(v => `"${v}"`).join(",")})`;
}

// Asset classes the dashboard surfaces by default.
//
// Masttro's taxonomy includes private alternatives (PE / direct PE,
// private RE / RE funds, private debt / loans) plus other non-listed
// categories. This dashboard is intentionally focused on public-listed
// exposures — those have daily yfinance pricing, intraday refresh, and
// tie out to public benchmarks; alternatives don't. Hidden classes
// stay in the underlying tables (no destructive ingest filter) so
// adding a class back here is a one-line change.
//
// Every scoped query funnels its incoming `assetClasses` argument
// through `effectiveAssetClasses()` so:
//   - A stale cookie containing a now-hidden class is silently
//     stripped (UI never offers hidden classes anyway).
//   - An empty selection means "all visible" rather than "everything
//     in the database".
const VISIBLE_ASSET_CLASSES: readonly string[] = [
  "Equity",
  "Fixed Income",
  "Cash and Equivalents",
];

// Resolve a raw user asset-class selection into the list to push
// down to PostgREST / RPCs. Always intersected with VISIBLE so a
// stale-cookie request for a hidden class never re-introduces it.
// Returns the full VISIBLE list when the user has no selection (or
// when their selection has no overlap with VISIBLE) so callers can
// always apply an `.in()` clause.
function effectiveAssetClasses(userSelection: string[]): string[] {
  if (userSelection.length === 0) return VISIBLE_ASSET_CLASSES.slice();
  const filtered = userSelection.filter(c =>
    VISIBLE_ASSET_CLASSES.includes(c),
  );
  return filtered.length > 0 ? filtered : VISIBLE_ASSET_CLASSES.slice();
}

// True iff the user explicitly picked at least one visible class. Used
// by getPeriodReturns / getFlowsByTrust to decide between trust-level
// external flows (no explicit filter — flows route into the cash
// bucket which is visible) vs per-class flows from v_transactions.
function userHasAssetClassFilter(userSelection: string[]): boolean {
  return userSelection.some(c => VISIBLE_ASSET_CLASSES.includes(c));
}

// Apply an asset_class filter to a Supabase query builder. Callers
// always pass the effective (post-allowlist) list, so this is a plain
// `.in()` push-down — no Unclassified / NULL bucket handling needed
// because VISIBLE never contains "Unclassified". Caller must ensure
// the underlying view/table has an asset_class column.
//
// Typed via a structural Q so each caller can pass either a freshly-
// chained query (PostgrestFilterBuilder) or an already-narrowed one
// and get the same return type back.
function applyAssetClassFilter<Q>(q: Q, classes: string[]): Q {
  if (!classes.length) return q;
  return (q as unknown as { in: (c: string, v: string[]) => Q }).in(
    "asset_class",
    classes,
  );
}

// Per-query timing wrapper. Logs `[q] <label> <ms>ms (<rows> rows)` to
// stdout — picked up by Vercel function logs. Wraps every exported query
// in this file so we can compare wall-clock costs after a filter change.
// Opt out by setting QUERY_TIMING=0 in the environment.
async function timed<T>(label: string, fn: () => Promise<T>): Promise<T> {
  if (process.env.QUERY_TIMING === "0") return fn();
  const t0 = Date.now();
  try {
    const result = await fn();
    const ms = Date.now() - t0;
    const size = Array.isArray(result)
      ? `${result.length} rows`
      : result && typeof result === "object"
        ? `${Object.keys(result).length} keys`
        : "scalar";
    console.log(`[q] ${label} ${ms}ms (${size})`);
    return result;
  } catch (err) {
    console.log(`[q] ${label} ERR after ${Date.now() - t0}ms`);
    throw err;
  }
}


export interface Position {
  account_node_id: string;
  account_alias: string;
  custodian: string | null;
  trust_alias: string | null;
  security_id: number;
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
  return timed("listSubClients", async () => {
    // Query v_latest_positions rather than entity_attribution so only
    // families that actually hold positions in the latest snapshot show
    // up. Newly-onboarded families that haven't been backfilled yet would
    // otherwise appear in the selector with empty dashboards.
    const { data, error } = await getSupabaseServer()
      .from("v_latest_positions")
      .select("sub_client_alias")
      .not("sub_client_alias", "is", null)
      .limit(LIMIT_LARGE);
    if (error) throw error;
    const rows = (data ?? []) as unknown as Array<{ sub_client_alias: string }>;
    return Array.from(new Set(rows.map(r => r.sub_client_alias))).sort();
  });
}

export async function listAssetClasses(
  subClient: string = DEFAULT_SUB_CLIENT,
): Promise<string[]> {
  return timed(`listAssetClasses`, async () => {
    // Source from v_latest_positions so only classes with at least one
    // current holding under this sub-client show up. Intersected with
    // VISIBLE_ASSET_CLASSES so hidden classes don't appear in the
    // filter dropdown — keeping the UI consistent with what every
    // other query strips out.
    const { data, error } = await getSupabaseServer()
      .from("v_latest_positions")
      .select("asset_class")
      .eq("sub_client_alias", subClient)
      .limit(LIMIT_LARGE);
    if (error) throw error;
    const rows = (data ?? []) as unknown as Array<{ asset_class: string | null }>;
    const present = new Set<string>();
    for (const r of rows) if (r.asset_class) present.add(r.asset_class);
    return VISIBLE_ASSET_CLASSES.filter(c => present.has(c));
  });
}

export interface AccountOption {
  // Stable identity for React keys / dedup. The lowest-sorted underlying
  // node_id in the group (deterministic, no extra meaning).
  key: string;
  // Every account_node_id that backs this physical custody account.
  // Pushed into the account_filter cookie as a single unit when the
  // user toggles the row — see AccountFilter.tsx.
  node_ids: string[];
  alias: string;
  custodian: string | null;
  // Entities (trust_aliases) the underlying reflections roll up to.
  // Usually one when the dropdown is already entity-scoped; can be
  // several when no entity filter is active.
  trust_aliases: string[];
}

export async function listAccounts(
  subClient: string = DEFAULT_SUB_CLIENT,
  trusts: string[] = [],
): Promise<AccountOption[]> {
  return timed(`listAccounts(${trusts.length}t)`, async () => {
    // Source from v_latest_positions so we only get accounts that
    // actually hold positions in the latest snapshot. The same physical
    // custody account often appears under many account_node_ids — once
    // per ownership branch (principal × holding structure) — each
    // carrying a pro-rata slice of the underlying value. Sum across
    // reflections = true account NAV (per-trust slicing is correct in
    // tracker/sync_supabase.py); duplicating the row in the dropdown is
    // pure UI noise. Group here by (bank_broker, account_number) so
    // the user picks one logical account; the cookie still stores the
    // full set of node_ids so downstream .in("account_node_id", ...)
    // queries continue to sum every slice correctly.
    let q = getSupabaseServer()
      .from("v_latest_positions")
      .select(
        "account_node_id, account_alias, custodian, trust_alias, " +
          "account_number",
      )
      .eq("sub_client_alias", subClient);
    if (trusts.length) q = q.in("trust_alias", trusts);
    const excluded = excludedEntities(subClient);
    if (excluded.length) q = q.not("trust_alias", "in", postgrestInList(excluded));
    const { data, error } = await q.limit(LIMIT_LARGE);
    if (error) throw error;
    const rows = (data ?? []) as unknown as Array<{
      account_node_id: string;
      account_alias: string | null;
      custodian: string | null;
      trust_alias: string | null;
      account_number: string | null;
    }>;
    // Group by physical custody identity. Accounts missing both
    // custodian and account_number fall back to the node_id itself so
    // they appear as their own row rather than getting bucketed together.
    const groups = new Map<string, {
      node_ids: Set<string>;
      alias: string;
      custodian: string | null;
      trust_aliases: Set<string>;
    }>();
    for (const r of rows) {
      const physicalKey =
        r.custodian && r.account_number
          ? `${r.custodian}|${r.account_number}`
          : `node:${r.account_node_id}`;
      let g = groups.get(physicalKey);
      if (!g) {
        g = {
          node_ids: new Set(),
          alias: r.account_alias ?? r.account_node_id,
          custodian: r.custodian,
          trust_aliases: new Set(),
        };
        groups.set(physicalKey, g);
      }
      g.node_ids.add(r.account_node_id);
      if (r.trust_alias) g.trust_aliases.add(r.trust_alias);
    }
    const accounts: AccountOption[] = [];
    for (const g of groups.values()) {
      const node_ids = [...g.node_ids].sort();
      accounts.push({
        key: node_ids[0],
        node_ids,
        alias: g.alias,
        custodian: g.custodian,
        trust_aliases: [...g.trust_aliases].sort(),
      });
    }
    return accounts.sort((a, b) => a.alias.localeCompare(b.alias));
  });
}

export async function listTrusts(
  subClient: string = DEFAULT_SUB_CLIENT,
): Promise<string[]> {
  return timed("listTrusts", async () => {
    // Query v_latest_positions rather than entity_attribution so we only
    // surface trusts that actually hold positions in the latest snapshot.
    // entity_attribution includes every node whose ancestor name matches
    // the substring "trust", which catches LLCs like "Deltrust LLC" and
    // other non-position-holding shells. Real fix is at sync-level, but
    // this filter keeps the dropdown clean either way.
    const excluded = excludedEntities(subClient);
    let q = getSupabaseServer()
      .from("v_latest_positions")
      .select("trust_alias")
      .eq("sub_client_alias", subClient)
      .not("trust_alias", "is", null);
    // Cast the builder to `any` before the second `.not()` — supabase-js's
    // PostgrestFilterBuilder tracks each filter at the type level, and two
    // `.not()` calls on one builder otherwise blow TS's "Type instantiation
    // is excessively deep" limit. The cast back to `typeof q` keeps the
    // post-assignment value typed normally.
    if (excluded.length)
      q = (q as unknown as {
        not: (col: string, op: string, val: string) => typeof q;
      }).not("trust_alias", "in", postgrestInList(excluded));
    const { data, error } = await q.limit(LIMIT_LARGE);
    if (error) throw error;
    const rows = (data ?? []) as unknown as Array<{ trust_alias: string }>;
    return Array.from(new Set(rows.map(r => r.trust_alias))).sort();
  });
}

export async function getLatestPositions(
  subClient: string = DEFAULT_SUB_CLIENT,
  trusts: string[] = [],
  accounts: string[] = [],
  assetClasses: string[] = [],
): Promise<Position[]> {
  const effective = effectiveAssetClasses(assetClasses);
  return timed(`getLatestPositions(${trusts.length}t,${accounts.length}a,${effective.length}c)`, async () => {
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
        "account_node_id, account_alias, custodian, trust_alias, security_id, " +
          "asset_name, asset_class, security_type, sector, ticker_masttro, " +
          "isin, local_ccy, quantity, price_local, yf_price, mv_local, " +
          "mv_reporting:mv_reporting_refreshed, mv_reporting_yesterday, " +
          "reporting_ccy, unit_cost_local, total_cost_local, " +
          "unrealized_gl_local",
      )
      .eq("sub_client_alias", subClient);
    if (trusts.length) q = q.in("trust_alias", trusts);
    if (accounts.length) q = q.in("account_node_id", accounts);
    const excluded = excludedEntities(subClient);
    if (excluded.length) q = q.not("trust_alias", "in", postgrestInList(excluded));
    q = applyAssetClassFilter(q, effective);
    const { data, error } = await q
      .order("mv_reporting_refreshed", { ascending: false, nullsFirst: false })
      .limit(LIMIT_LARGE);
    if (error) throw error;
    return (data ?? []) as unknown as Position[];
  });
}

// ---------------------------------------------------------------------------
// Per-(period × account × security) gain pieces for the Holdings table.
// ---------------------------------------------------------------------------

// Types + the key helper live in lib/holdings-gains.ts so the client-
// component table can import them without dragging in next/headers.
export type {
  HoldingsGainPieces,
  HoldingsPeriodGainMap,
  HoldingsPeriodKey,
} from "./holdings-gains";
export { holdingsGainKey } from "./holdings-gains";
import {
  holdingsGainKey,
  type HoldingsPeriodGainMap,
  type HoldingsPeriodKey,
} from "./holdings-gains";

export async function getHoldingsPeriodGains(
  subClient: string = DEFAULT_SUB_CLIENT,
  trusts: string[] = [],
  accounts: string[] = [],
  assetClasses: string[] = [],
  endDate: Date = new Date(),
): Promise<HoldingsPeriodGainMap> {
  const effective = effectiveAssetClasses(assetClasses);
  return timed(
    `getHoldingsPeriodGains(${trusts.length}t,${accounts.length}a,${effective.length}c)`,
    async () => {
      const excluded = excludedEntities(subClient);
      const endIso = endDate.toISOString().slice(0, 10);
      // Period anchors mirror lib/returns.ts::shiftDate so per-holding
      // gain windows line up with the portfolio-level Returns tile.
      const mtdStart = computePeriodStart("mtd", endDate);
      const ytdStart = computePeriodStart("ytd", endDate);
      const sixMStart = computePeriodStart("6m", endDate);
      const oneYStart = computePeriodStart("1y", endDate);

      const { data, error } = await getSupabaseServer().rpc(
        "holdings_period_attribution",
        {
          p_sub_client:      subClient,
          p_trusts:          trusts.length ? trusts : null,
          p_accounts:        accounts.length ? accounts : null,
          p_asset_classes:   effective,
          p_excluded_trusts: excluded.length ? excluded : null,
          p_end_date:        endIso,
          p_mtd_start:       mtdStart,
          p_ytd_start:       ytdStart,
          p_six_m_start:     sixMStart,
          p_one_y_start:     oneYStart,
        },
      );
      if (error) throw error;
      const rows = (data ?? []) as unknown as Array<{
        period: HoldingsPeriodKey;
        account_node_id: string;
        security_id: number | string;
        start_mv: number | string | null;
        flows: number | string | null;
        income: number | string | null;
      }>;
      const out: HoldingsPeriodGainMap = new Map();
      for (const r of rows) {
        const key = holdingsGainKey(
          r.period,
          r.account_node_id,
          Number(r.security_id),
        );
        out.set(key, {
          start_mv: Number(r.start_mv ?? 0),
          flows: Number(r.flows ?? 0),
          income: Number(r.income ?? 0),
        });
      }
      return out;
    },
  );
}

export async function getNavSeries(
  subClient: string = DEFAULT_SUB_CLIENT,
  trusts: string[] = [],
  accounts: string[] = [],
  assetClasses: string[] = [],
): Promise<NavPoint[]> {
  const effective = effectiveAssetClasses(assetClasses);
  return timed(`getNavSeries(${trusts.length}t,${accounts.length}a,${effective.length}c)`, async () => {
    // Always query v_nav_monthly_by_asset_class with an explicit
    // .in() filter on asset_class. The cheaper v_nav_monthly_by_account
    // path was dropped (it doesn't carry asset_class so it can't
    // exclude hidden alternatives — using it silently inflated NAV).
    // This view IS pre-aggregated per (snapshot_date, account, class)
    // and the asset_class column is in its GROUP BY, so the filter
    // pushes down through PostgREST cleanly — no need for the RPC
    // workaround that migration 020 added (it remains for callers
    // that still want a single-round-trip aggregate).
    const excluded = excludedEntities(subClient);
    let q = getSupabaseServer()
      .from("v_nav_monthly_by_asset_class")
      .select("snapshot_date, nav_reporting")
      .eq("sub_client_alias", subClient)
      .in("asset_class", effective);
    if (trusts.length) q = q.in("trust_alias", trusts);
    if (accounts.length) q = q.in("account_node_id", accounts);
    if (excluded.length) q = q.not("trust_alias", "in", postgrestInList(excluded));
    const { data, error } = await q.limit(LIMIT_LARGE);
    if (error) throw error;

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
  });
}

export interface PeriodReturnOverrides {
  // Refreshed end NAV (sum of mv_reporting from getLatestPositions). Used in
  // place of the last point in the historical NAV series, since historical
  // is Masttro-only and may be a day or two behind market.
  endNav?: number;
  // Today's positions valued at yfinance previous-close. Used as the start
  // NAV for the 1D return so it reflects pure intraday price movement.
  endNavYesterday?: number;
  // Precise per-period start NAVs (and the actual date used). Populated
  // from the reconstructed_nav_at RPC for periods that need date precision
  // (currently 6M and 1Y).
  startNavByPeriod?: Partial<Record<PeriodKey, { nav: number; date: string }>>;
  // Pre-fetched NAV series — pass this when the caller already loaded
  // getNavSeries(subClient, trusts, accounts) on the same page render, to
  // skip the duplicate round-trip inside this function.
  navs?: NavPoint[];
}

export async function getPeriodReturns(
  subClient: string = DEFAULT_SUB_CLIENT,
  trusts: string[] = [],
  accounts: string[] = [],
  assetClasses: string[] = [],
  overrides: PeriodReturnOverrides = {},
): Promise<Record<PeriodKey, PeriodReturn>> {
  const effective = effectiveAssetClasses(assetClasses);
  const userFiltered = userHasAssetClassFilter(assetClasses);
  return timed(`getPeriodReturns(${trusts.length}t,${accounts.length}a,${effective.length}c)`, async () => {
    const navs =
      overrides.navs ?? (await getNavSeries(subClient, trusts, accounts, assetClasses));
    if (navs.length === 0) {
      return computeAllPeriodReturns([], [], overrides);
    }

    const earliest = navs[0].snapshot_date;

    // Flow source switches on whether the user actively picked a class
    // subset (not just on `effective` having entries — which is always
    // the full visible list when the user has no selection):
    //   - User has no selection → v_external_flows (trust-level
    //     Deposit / Withdrawal). External flows route into cash, which
    //     is a visible class, so they belong against the visible-only
    //     NAV that getNavSeries returns.
    //   - User picked specific classes → per-class flows (Buy / Sell /
    //     dividends / interest against securities of the selected
    //     classes). External cash flows drop out because they have no
    //     asset_class — landing in cash before being deployed.
    let flows: Flow[];
    if (!userFiltered) {
      let q = getSupabaseServer()
        .from("v_external_flows")
        .select(
          "transaction_date, net_amount_reporting, trust_alias, " +
            "sub_client_alias, account_node_id",
        )
        .eq("sub_client_alias", subClient)
        .gte("transaction_date", earliest);
      if (trusts.length) q = q.in("trust_alias", trusts);
      if (accounts.length) q = q.in("account_node_id", accounts);
      const excluded = excludedEntities(subClient);
      if (excluded.length) q = q.not("trust_alias", "in", postgrestInList(excluded));
      const { data, error } = await q.limit(LIMIT_LARGE);
      if (error) throw error;
      const flowRows = (data ?? []) as unknown as Array<{
        transaction_date: string;
        net_amount_reporting: number | null;
      }>;
      flows = flowRows.map(r => ({
        date: r.transaction_date,
        amount: Number(r.net_amount_reporting ?? 0),
      }));
    } else {
      const byClass = await getFlowsByAssetClass(
        subClient,
        trusts,
        accounts,
        earliest,
        assetClasses,
      );
      flows = [];
      for (const ac of effective) {
        const arr = byClass[ac];
        if (arr) flows.push(...arr);
      }
    }

    const navPoints: DietzNavPoint[] = navs.map(n => ({
      date: n.snapshot_date,
      nav: n.nav,
    }));

    return computeAllPeriodReturns(navPoints, flows, overrides);
  });
}

export async function getNavSeriesByTrust(
  subClient: string = DEFAULT_SUB_CLIENT,
  trusts: string[] = [],
  accounts: string[] = [],
  assetClasses: string[] = [],
): Promise<Record<string, NavPoint[]>> {
  const effective = effectiveAssetClasses(assetClasses);
  return timed(`getNavSeriesByTrust(${trusts.length}t,${accounts.length}a,${effective.length}c)`, async () => {
    // Always query v_nav_monthly_by_asset_class with .in() filter on
    // asset_class. Drops the previous Path A (v_nav_monthly_by_account)
    // which couldn't exclude hidden classes since it lacks the column.
    const excluded = excludedEntities(subClient);
    let q = getSupabaseServer()
      .from("v_nav_monthly_by_asset_class")
      .select("snapshot_date, trust_alias, nav_reporting")
      .eq("sub_client_alias", subClient)
      .not("trust_alias", "is", null)
      .in("asset_class", effective);
    if (trusts.length) q = q.in("trust_alias", trusts);
    if (accounts.length) q = q.in("account_node_id", accounts);
    // Two .not()s on one builder otherwise hit TS's deep-instantiation limit.
    if (excluded.length)
      q = (q as unknown as {
        not: (col: string, op: string, val: string) => typeof q;
      }).not("trust_alias", "in", postgrestInList(excluded));
    const { data, error } = await q.limit(LIMIT_LARGE);
    if (error) throw error;
    const rows = (data ?? []) as unknown as Array<{
      snapshot_date: string;
      trust_alias: string;
      nav_reporting: number | string | null;
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
  });
}

export async function getFlowsByTrust(
  subClient: string = DEFAULT_SUB_CLIENT,
  trusts: string[] = [],
  accounts: string[] = [],
  fromDate: string = "2020-01-01",
  assetClasses: string[] = [],
): Promise<Record<string, Flow[]>> {
  const effective = effectiveAssetClasses(assetClasses);
  const userFiltered = userHasAssetClassFilter(assetClasses);
  return timed(`getFlowsByTrust(${trusts.length}t,${accounts.length}a,${effective.length}c)`, async () => {
    // User has no class selection → external flows (trust-level
    // deposits/withdrawals). They route through cash which is visible,
    // so they're the right thing to subtract from the visible NAV.
    // User picked specific classes → per-class flows from v_transactions
    // for the selected (∩ visible) classes, sign-flipped to "into the
    // class", grouped by trust.
    const useClassFlows = userFiltered;
    const view = useClassFlows ? "v_transactions" : "v_external_flows";
    let q = getSupabaseServer()
      .from(view)
      .select("transaction_date, net_amount_reporting, trust_alias")
      .eq("sub_client_alias", subClient)
      .gte("transaction_date", fromDate)
      .not("trust_alias", "is", null);
    if (useClassFlows) {
      q = q.in("transaction_type_clean", [
        "Buy",
        "Sell",
        "Cash Dividends",
        "Interest",
        "Income",
      ]);
      q = q.in("asset_class", effective);
    }
    if (trusts.length) q = q.in("trust_alias", trusts);
    if (accounts.length) q = q.in("account_node_id", accounts);
    const excluded = excludedEntities(subClient);
    // Two .not()s on one builder otherwise hit TS's deep-instantiation limit.
    if (excluded.length)
      q = (q as unknown as {
        not: (col: string, op: string, val: string) => typeof q;
      }).not("trust_alias", "in", postgrestInList(excluded));
    const { data, error } = await q.limit(LIMIT_LARGE);
    if (error) throw error;

    const rows = (data ?? []) as unknown as Array<{
      transaction_date: string;
      net_amount_reporting: number | null;
      trust_alias: string;
    }>;
    // For class-filter path, negate so flow is "into the class" (Buy → +,
    // Sell/Dividend/Interest → -). Mirror getFlowsByAssetClass sign rule.
    const sign = useClassFlows ? -1 : 1;

    const out: Record<string, Flow[]> = {};
    for (const r of rows) {
      if (!out[r.trust_alias]) out[r.trust_alias] = [];
      out[r.trust_alias].push({
        date: r.transaction_date,
        amount: sign * Number(r.net_amount_reporting ?? 0),
      });
    }
    return out;
  });
}

// Per-asset-class flows for proper Modified-Dietz returns at the class
// level. Sign convention: returned `amount` is the flow *into* the class
// (positive = inflow). A Buy moves cash into the class → +amount. A Sell
// or a Dividend (paid out to cash) moves value out of the class → -amount.
// Both come from negating the raw `net_amount_reporting`, which is the
// cash-side amount with the opposite sign.
//
// Matches Masttro's per-asset-class `transferInOut` definition to within a
// few percent — verified against /Performance for Morgan Dyne 12M (Equity
// API transferInOut = -$418k, our rule sums to -$389k, residual ~$30k is
// corporate-action cash residuals + fees / taxes we don't yet classify).
//
// Transaction types included:
//   - Buy / Sell     — security purchases & sales (the obvious flows)
//   - Cash Dividends — equity dividends paid to cash (outflow from equity)
//   - Interest       — bond / cash-equivalent interest paid to cash
//   - Income         — generic "Income" rows when neither dividend nor
//                       interest, e.g. private investment distributions
export async function getFlowsByAssetClass(
  subClient: string = DEFAULT_SUB_CLIENT,
  trusts: string[] = [],
  accounts: string[] = [],
  fromDate: string = "2020-01-01",
  assetClasses: string[] = [],
): Promise<Record<string, Flow[]>> {
  const effective = effectiveAssetClasses(assetClasses);
  return timed(`getFlowsByAssetClass(${trusts.length}t,${accounts.length}a,${effective.length}c)`, async () => {
    let q = getSupabaseServer()
      .from("v_transactions")
      .select("transaction_date, asset_class, net_amount_reporting")
      .eq("sub_client_alias", subClient)
      .gte("transaction_date", fromDate)
      .in("transaction_type_clean", [
        "Buy",
        "Sell",
        "Cash Dividends",
        "Interest",
        "Income",
      ]);
    if (trusts.length) q = q.in("trust_alias", trusts);
    if (accounts.length) q = q.in("account_node_id", accounts);
    const excluded = excludedEntities(subClient);
    if (excluded.length) q = q.not("trust_alias", "in", postgrestInList(excluded));
    q = applyAssetClassFilter(q, effective);
    const { data, error } = await q.limit(LIMIT_LARGE);
    if (error) throw error;

    const rows = (data ?? []) as unknown as Array<{
      transaction_date: string;
      asset_class: string | null;
      net_amount_reporting: number | null;
    }>;

    const out: Record<string, Flow[]> = {};
    for (const r of rows) {
      // Match v_nav_monthly_by_asset_class which COALESCEs NULL → 'Unclassified'.
      const ac = r.asset_class ?? "Unclassified";
      if (!out[ac]) out[ac] = [];
      out[ac].push({
        date: r.transaction_date,
        // Negate so flow is "into the class": Buy (cash-side negative) → +,
        // Sell / Dividend / Interest (cash-side positive) → -.
        amount: -Number(r.net_amount_reporting ?? 0),
      });
    }
    return out;
  });
}

export async function getNavSeriesByAssetClass(
  subClient: string = DEFAULT_SUB_CLIENT,
  trusts: string[] = [],
  accounts: string[] = [],
  assetClasses: string[] = [],
): Promise<Record<string, NavPoint[]>> {
  const effective = effectiveAssetClasses(assetClasses);
  return timed(`getNavSeriesByAssetClass(${trusts.length}t,${accounts.length}a,${effective.length}c)`, async () => {
    // Per-(snapshot_date, account, asset_class) rows from the view. We
    // aggregate across accounts in JS, keyed by asset_class, to get one NAV
    // series per class for the Performance page's per-class matrix and
    // the Overview allocation table. Filter is always non-empty (defaults
    // to VISIBLE_ASSET_CLASSES) so hidden alternatives never leak in.
    let q = getSupabaseServer()
      .from("v_nav_monthly_by_asset_class")
      .select("snapshot_date, asset_class, nav_reporting")
      .eq("sub_client_alias", subClient)
      .in("asset_class", effective);
    if (trusts.length) q = q.in("trust_alias", trusts);
    if (accounts.length) q = q.in("account_node_id", accounts);
    const excluded = excludedEntities(subClient);
    if (excluded.length) q = q.not("trust_alias", "in", postgrestInList(excluded));
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
  });
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
  trusts: string[] = [],
  accounts: string[] = [],
  fromDate: string = "2020-01-01",
  assetClasses: string[] = [],
): Promise<IncomeRow[]> {
  const effective = effectiveAssetClasses(assetClasses);
  return timed(`getIncomeRows(${trusts.length}t,${accounts.length}a,${effective.length}c)`, async () => {
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
    if (trusts.length) q = q.in("trust_alias", trusts);
    if (accounts.length) q = q.in("account_node_id", accounts);
    const excluded = excludedEntities(subClient);
    if (excluded.length) q = q.not("trust_alias", "in", postgrestInList(excluded));
    q = applyAssetClassFilter(q, effective);
    const { data, error } = await q.limit(LIMIT_LARGE);
    if (error) throw error;
    const rows = (data ?? []) as unknown as Array<
      Omit<IncomeRow, "amount"> & { amount: number | string | null }
    >;
    return rows.map(r => ({ ...r, amount: Number(r.amount ?? 0) }));
  });
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
  trusts: string[] = [],
  accounts: string[] = [],
  fromDate: string = "2020-01-01",
  toDate: string | null = null,
  assetClasses: string[] = [],
): Promise<Transaction[]> {
  const effective = effectiveAssetClasses(assetClasses);
  return timed(`getTransactions(${trusts.length}t,${accounts.length}a,${effective.length}c)`, async () => {
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
    if (trusts.length) q = q.in("trust_alias", trusts);
    if (accounts.length) q = q.in("account_node_id", accounts);
    const excluded = excludedEntities(subClient);
    if (excluded.length) q = q.not("trust_alias", "in", postgrestInList(excluded));
    q = applyAssetClassFilter(q, effective);
    const { data, error } = await q.limit(LIMIT_LARGE);
    if (error) throw error;
    return (data ?? []) as unknown as Transaction[];
  });
}

// ---------------------------------------------------------------------------
// Index benchmarks — for the Returns-vs-index comparison on the Returns tile.
// ---------------------------------------------------------------------------

export interface IndexOption {
  ticker: string;
  name: string;
}

export async function listIndices(): Promise<IndexOption[]> {
  return timed("listIndices", async () => {
    const { data, error } = await getSupabaseServer()
      .from("index_definition")
      .select("ticker, name")
      .order("ticker");
    if (error) throw error;
    return (data ?? []) as unknown as IndexOption[];
  });
}

export interface NavAnchor {
  nav: number;
  anchorDate: string; // ISO YYYY-MM-DD of the actual snapshot used
}

/**
 * Raw NAV at the latest position_snapshot on or before targetDate, via
 * the nav_at_or_before RPC. Returns the actual anchor date alongside
 * the value so the caller can label the start of the period honestly
 * (Masttro has no daily historicals, so the anchor is typically the
 * previous month-end). Returns null when no snapshot exists on or
 * before the target.
 *
 * Per migration 020 the RPC takes a `text[]` of asset classes (empty /
 * undefined = no filter) so we do a single round-trip even when several
 * classes are selected via the global filter.
 */
export async function getNavAtOrBefore(
  subClient: string,
  trusts: string[],
  accounts: string[],
  targetDate: string,
  assetClasses: string[] = [],
): Promise<NavAnchor | null> {
  const effective = effectiveAssetClasses(assetClasses);
  const label = `nav_at_or_before(${targetDate},${effective.length}c)`;
  return timed(label, async () => {
    const excluded = excludedEntities(subClient);
    const { data, error } = await getSupabaseServer().rpc(
      "nav_at_or_before",
      {
        p_sub_client:       subClient,
        p_trusts:           trusts.length ? trusts : null,
        p_accounts:         accounts.length ? accounts : null,
        p_target_date:      targetDate,
        p_asset_classes:    effective,
        p_excluded_trusts:  excluded.length ? excluded : null,
      },
    );
    if (error) {
      const code = (error as { code?: string }).code;
      if (code === "PGRST202" || code === "PGRST203") {
        console.log(`[q] ${label} fallback: ${code} ${error.message}`);
        return null;
      }
      throw error;
    }
    // RETURNS TABLE shapes come back as a row array. No rows → no snapshot
    // exists on or before the target.
    const rows = (data ?? []) as Array<{ nav: unknown; anchor_date: string }>;
    if (rows.length === 0) return null;
    const row = rows[0];
    const n = Number(row.nav);
    if (!Number.isFinite(n)) return null;
    return { nav: n, anchorDate: row.anchor_date };
  });
}

// ---------------------------------------------------------------------------
// Per-(month × security) attribution for the Performance page drill-in.
// ---------------------------------------------------------------------------

export interface MonthlyAttributionRow {
  month: string;            // ISO yyyy-mm-dd (first of month)
  security_id: number;
  asset_name: string | null;
  ticker_masttro: string | null;
  asset_class: string;
  start_mv: number;
  end_mv: number;
  flows: number;            // +ve = net cash flowed INTO the security
  income: number;           // divs + interest received
  gain: number;             // (end - start) - flows + income (total return)
}

// Top contributors + detractors per month to fetch. UI shows 10 of each;
// 5 extra rows on each side covers cases where the same security ranks
// in both the top of the gain DESC and the top of the gain ASC lists
// for the same month (which shouldn't happen, but the buffer is cheap).
const ATTRIBUTION_TOP_PER_MONTH = 15;

export async function getMonthlySecurityAttribution(
  subClient: string = DEFAULT_SUB_CLIENT,
  trusts: string[] = [],
  accounts: string[] = [],
  assetClasses: string[] = [],
  fromMonth: string = "2020-01-01",
): Promise<MonthlyAttributionRow[]> {
  const effective = effectiveAssetClasses(assetClasses);
  return timed(
    `getMonthlySecurityAttribution(${trusts.length}t,${accounts.length}a,${effective.length}c)`,
    async () => {
      const excluded = excludedEntities(subClient);
      // p_top_per_month bounds the response server-side at ~30 rows per
      // month (top 15 gainers + top 15 losers). Without it we hit
      // Supabase's project db-max-rows cap for larger trusts and lose
      // recent months from the drill-in entirely. .limit(LIMIT_LARGE)
      // remains as belt-and-braces — even at top_per_month=15 across a
      // 14-month window we're well under 1k rows.
      const { data, error } = await getSupabaseServer()
        .rpc("monthly_security_attribution", {
          p_sub_client:      subClient,
          p_trusts:          trusts.length ? trusts : null,
          p_accounts:        accounts.length ? accounts : null,
          p_asset_classes:   effective,
          p_from_month:      fromMonth,
          p_excluded_trusts: excluded.length ? excluded : null,
          p_top_per_month:   ATTRIBUTION_TOP_PER_MONTH,
        })
        .limit(LIMIT_LARGE);
      if (error) throw error;
      const rows = (data ?? []) as unknown as Array<
        Omit<MonthlyAttributionRow, "start_mv" | "end_mv" | "flows" | "income" | "gain"> & {
          start_mv: number | string | null;
          end_mv: number | string | null;
          flows: number | string | null;
          income: number | string | null;
          gain: number | string | null;
        }
      >;
      return rows.map(r => ({
        ...r,
        start_mv: Number(r.start_mv ?? 0),
        end_mv: Number(r.end_mv ?? 0),
        flows: Number(r.flows ?? 0),
        income: Number(r.income ?? 0),
        gain: Number(r.gain ?? 0),
      }));
    },
  );
}

export async function getIndexPrices(
  ticker: string,
  fromDate: string,
): Promise<IndexPricePoint[]> {
  return timed(`getIndexPrices(${ticker})`, async () => {
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
  });
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
