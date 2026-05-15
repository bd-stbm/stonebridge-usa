"""Query API for the tracker — returns pandas DataFrames.

`scope` is a node_id (sub-client / trust / account) or 'all'. The scope is
resolved to the set of canonical investment accounts within it via a single
shared helper (`scope_accounts`).
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable

import pandas as pd

from tracker import DEFAULT_DB_PATH


def connect(db_path=DEFAULT_DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


# ---------------------------------------------------------------------------
# Scope resolution
# ---------------------------------------------------------------------------

def scope_accounts(conn, scope: str) -> list[str]:
    """Resolve a scope (node_id or 'all') to a list of canonical account node_ids."""
    cur = conn.cursor()
    if scope == "all":
        cur.execute("SELECT node_id FROM entity WHERE is_canonical_account = 1")
        return [r[0] for r in cur.fetchall()]

    # Quick path: scope IS a canonical account
    cur.execute(
        "SELECT is_canonical_account FROM entity WHERE node_id = ?",
        (scope,),
    )
    row = cur.fetchone()
    if row and row[0] == 1:
        return [scope]

    # Walk descendants (in Python — small tree, simple)
    cur.execute("SELECT node_id, parent_node_id FROM entity")
    children_of: dict[str, list[str]] = {}
    for nid, pid in cur.fetchall():
        children_of.setdefault(pid, []).append(nid)

    descendants = {scope}
    stack = [scope]
    while stack:
        nid = stack.pop()
        for c in children_of.get(nid, []):
            if c not in descendants:
                descendants.add(c)
                stack.append(c)

    if not descendants:
        return []
    placeholders = ",".join("?" * len(descendants))
    cur.execute(
        f"""SELECT node_id FROM entity
            WHERE node_id IN ({placeholders}) AND is_canonical_account = 1""",
        list(descendants),
    )
    return [r[0] for r in cur.fetchall()]


def _accounts_clause(accounts: list[str]) -> tuple[str, list]:
    if not accounts:
        return "1=0", []
    return f"({','.join('?' * len(accounts))})", list(accounts)


# ---------------------------------------------------------------------------
# Latest-as-of helpers
# ---------------------------------------------------------------------------

def latest_snapshot_date(conn, scope: str) -> str | None:
    accounts = scope_accounts(conn, scope)
    if not accounts:
        return None
    clause, params = _accounts_clause(accounts)
    cur = conn.cursor()
    cur.execute(
        f"SELECT MAX(snapshot_date) FROM position_snapshot WHERE account_node_id IN {clause}",
        params,
    )
    return cur.fetchone()[0]


# ---------------------------------------------------------------------------
# Core queries
# ---------------------------------------------------------------------------

def current_positions(conn, scope: str, as_of: str | None = None,
                      include_refresh: bool = True) -> pd.DataFrame:
    """One row per position in the latest (or given) snapshot, with security details
    and optionally yfinance refresh prices."""
    accounts = scope_accounts(conn, scope)
    if not accounts:
        return pd.DataFrame()
    as_of = as_of or latest_snapshot_date(conn, scope)
    if not as_of:
        return pd.DataFrame()

    clause, params = _accounts_clause(accounts)
    sql = f"""
        SELECT
            p.snapshot_date,
            p.account_node_id,
            e.alias AS account_alias,
            e.bank_broker AS custodian,
            e.account_number,
            ea.trust_alias,
            ea.sub_client_alias,
            p.security_id,
            s.asset_name,
            s.asset_class,
            s.security_type,
            s.sector,
            s.geographic_exposure,
            s.ticker_masttro,
            s.ticker_yf,
            s.isin,
            s.local_ccy,
            p.quantity,
            p.price_local,
            p.mv_local,
            p.mv_reporting,
            p.reporting_ccy,
            p.unit_cost_local,
            p.total_cost_local,
            (p.mv_local - p.total_cost_local) AS unrealized_gl_local,
            p.accrued_interest_reporting
        FROM position_snapshot p
        JOIN entity e ON p.account_node_id = e.node_id
        LEFT JOIN entity_attribution ea ON p.account_node_id = ea.node_id
        LEFT JOIN security s ON p.security_id = s.security_id
        WHERE p.snapshot_date = ?
          AND p.account_node_id IN {clause}
        ORDER BY p.mv_reporting DESC NULLS LAST
    """
    df = pd.read_sql_query(sql, conn, params=[as_of] + params)

    if include_refresh and not df.empty:
        # Latest yfinance refresh per security
        cur = conn.cursor()
        cur.execute("SELECT MAX(refresh_date) FROM pricing_refresh")
        latest_refresh = cur.fetchone()[0]
        if latest_refresh:
            refresh = pd.read_sql_query(
                """SELECT security_id, price AS yf_price, price_ccy AS yf_ccy,
                          yf_as_of_date, source AS yf_source
                   FROM pricing_refresh WHERE refresh_date = ?""",
                conn, params=[latest_refresh],
            )
            df = df.merge(refresh, on="security_id", how="left")
            df["refresh_date"] = latest_refresh
            mask = df["price_local"].notna() & df["yf_price"].notna() & (df["price_local"] != 0)
            df["price_delta_pct"] = (df["yf_price"] / df["price_local"] - 1).where(mask)
            df["mv_refreshed"] = (
                df["mv_local"] * (df["yf_price"] / df["price_local"])
            ).where(mask)
            df["mv_refreshed_delta"] = (df["mv_refreshed"] - df["mv_local"]).where(mask)

    return df


def one_day_return(conn, scope: str) -> dict:
    """Portfolio-level 1-day return computed from yfinance latest & previous closes.

    Logic: for each position in the latest snapshot, if the security has both
    a yfinance latest price AND previous price, value the position at both and
    take the delta. Positions without yfinance prices are treated as 0% drift
    (reasonable for cash and approximately so for fixed-income overnight).

    Returns dict with: nav_today, nav_yesterday, change_usd, return_pct,
    as_of_date, previous_date, priced_nav, priced_pct.
    """
    accounts = scope_accounts(conn, scope)
    if not accounts:
        return {}
    cur = conn.cursor()
    cur.execute("SELECT MAX(refresh_date) FROM pricing_refresh")
    latest_refresh = cur.fetchone()[0]
    if not latest_refresh:
        return {"error": "no pricing_refresh on disk — run refresh_pricing first"}

    latest_snap = latest_snapshot_date(conn, scope)
    if not latest_snap:
        return {}

    clause, params = _accounts_clause(accounts)
    sql = f"""
        SELECT
            SUM(p.mv_reporting) AS masttro_nav,
            SUM(CASE
                WHEN pr.price IS NOT NULL AND p.price_local IS NOT NULL AND p.price_local != 0
                THEN p.mv_reporting * (pr.price / p.price_local)
                ELSE p.mv_reporting
            END) AS nav_today,
            SUM(CASE
                WHEN pr.price IS NOT NULL AND pr.price_previous IS NOT NULL
                 AND p.price_local IS NOT NULL AND p.price_local != 0
                THEN p.mv_reporting * (pr.price_previous / p.price_local)
                ELSE p.mv_reporting
            END) AS nav_yesterday,
            SUM(CASE
                WHEN pr.price IS NOT NULL AND pr.price_previous IS NOT NULL
                 AND p.price_local IS NOT NULL AND p.price_local != 0
                THEN p.mv_reporting * (pr.price / p.price_local)
                ELSE 0
            END) AS priced_today,
            SUM(p.mv_reporting) AS total_nav,
            MAX(pr.yf_as_of_date) AS as_of_date,
            MAX(pr.yf_previous_date) AS previous_date
        FROM position_snapshot p
        LEFT JOIN pricing_refresh pr
            ON pr.security_id = p.security_id
           AND pr.refresh_date = ?
        WHERE p.snapshot_date = ?
          AND p.account_node_id IN {clause}
    """
    cur.execute(sql, [latest_refresh, latest_snap] + params)
    row = cur.fetchone()
    if not row:
        return {}
    masttro_nav, nav_today, nav_yesterday, priced_today, total_nav, as_of, prev = row
    if not nav_today or not nav_yesterday:
        return {"error": "insufficient pricing data"}
    change = nav_today - nav_yesterday
    return_pct = change / nav_yesterday if nav_yesterday else None
    return {
        "scope": scope,
        "snapshot_date": latest_snap,
        "as_of_date": as_of,
        "previous_date": prev,
        "nav_today": float(nav_today),
        "nav_yesterday": float(nav_yesterday),
        "change_usd": float(change),
        "return_pct": float(return_pct) if return_pct is not None else None,
        "priced_nav": float(priced_today or 0),
        "priced_pct": float((priced_today or 0) / total_nav) if total_nav else 0,
    }


def one_day_movers(conn, scope: str, n: int = 20) -> pd.DataFrame:
    """Top N contributors and bottom N detractors to 1-day return by USD impact.

    Returns one row per security in latest snapshot with both latest and previous
    yfinance prices, sorted by absolute USD contribution.
    """
    accounts = scope_accounts(conn, scope)
    if not accounts:
        return pd.DataFrame()
    cur = conn.cursor()
    cur.execute("SELECT MAX(refresh_date) FROM pricing_refresh")
    latest_refresh = cur.fetchone()[0]
    if not latest_refresh:
        return pd.DataFrame()
    latest_snap = latest_snapshot_date(conn, scope)
    if not latest_snap:
        return pd.DataFrame()

    clause, params = _accounts_clause(accounts)
    df = pd.read_sql_query(
        f"""
        SELECT
            s.asset_name,
            s.ticker_masttro,
            s.ticker_yf,
            s.asset_class,
            s.sector,
            SUM(p.quantity) AS total_quantity,
            SUM(p.mv_reporting) AS mv_today_masttro,
            MAX(p.price_local) AS price_masttro,
            MAX(pr.price) AS yf_price,
            MAX(pr.price_previous) AS yf_price_previous,
            SUM(
              CASE WHEN pr.price IS NOT NULL AND p.price_local IS NOT NULL AND p.price_local != 0
                   THEN p.mv_reporting * (pr.price / p.price_local)
                   ELSE NULL END
            ) AS mv_today_refreshed,
            SUM(
              CASE WHEN pr.price IS NOT NULL AND pr.price_previous IS NOT NULL
                    AND p.price_local IS NOT NULL AND p.price_local != 0
                   THEN p.mv_reporting * (pr.price_previous / p.price_local)
                   ELSE NULL END
            ) AS mv_yesterday_refreshed
        FROM position_snapshot p
        JOIN security s ON p.security_id = s.security_id
        LEFT JOIN pricing_refresh pr
            ON pr.security_id = p.security_id
           AND pr.refresh_date = ?
        WHERE p.snapshot_date = ?
          AND p.account_node_id IN {clause}
        GROUP BY s.asset_name, s.ticker_masttro, s.ticker_yf, s.asset_class, s.sector
        """,
        conn, params=[latest_refresh, latest_snap] + params,
    )
    if df.empty:
        return df
    df = df[df["mv_today_refreshed"].notna() & df["mv_yesterday_refreshed"].notna()].copy()
    df["change_usd"] = df["mv_today_refreshed"] - df["mv_yesterday_refreshed"]
    df["return_pct"] = (
        (df["yf_price"] / df["yf_price_previous"] - 1)
        .where(df["yf_price_previous"].notna() & (df["yf_price_previous"] != 0))
    )
    df = df.sort_values("change_usd", key=lambda s: s.abs(), ascending=False)
    return df.head(n).reset_index(drop=True)


def one_day_return_by(conn, scope: str, group_by: str = "account") -> pd.DataFrame:
    """Per-bucket 1-day return. Returns DataFrame with one row per account/trust."""
    if group_by not in {"account", "trust"}:
        raise ValueError(f"group_by must be 'account' or 'trust'; got {group_by!r}")
    accounts = scope_accounts(conn, scope)
    if not accounts:
        return pd.DataFrame()
    rows = []
    for acct in accounts:
        cur = conn.cursor()
        if group_by == "account":
            cur.execute(
                "SELECT alias FROM entity WHERE node_id = ?", (acct,),
            )
            label = cur.fetchone()
            bucket = label[0] if label else acct
        else:
            cur.execute(
                "SELECT trust_alias, trust_node_id FROM entity_attribution WHERE node_id = ?",
                (acct,),
            )
            r = cur.fetchone()
            bucket = (r[0] if r else None) or "(unattributed)"
        odr = one_day_return(conn, acct)
        if not odr or "error" in odr:
            continue
        rows.append({
            "bucket": bucket,
            "account_node_id": acct,
            "nav_today": odr["nav_today"],
            "nav_yesterday": odr["nav_yesterday"],
            "change_usd": odr["change_usd"],
            "return_pct": odr["return_pct"],
            "priced_pct": odr["priced_pct"],
        })
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    if group_by == "trust":
        df = (
            df.groupby("bucket", as_index=False)
              .agg(nav_today=("nav_today", "sum"),
                   nav_yesterday=("nav_yesterday", "sum"),
                   change_usd=("change_usd", "sum"),
                   n_accounts=("account_node_id", "nunique"))
        )
        df["return_pct"] = df["change_usd"] / df["nav_yesterday"]
    return df.sort_values("nav_today", ascending=False).reset_index(drop=True)


def _refreshed_end_nav(conn, scope: str, snapshot_date: str) -> float | None:
    """Recompute the NAV for one snapshot_date using the latest yfinance refresh
    where available. Falls back to Masttro mv_reporting for positions with no
    refresh price (cash, bonds, unmatched equities).

    Uses the price-ratio formula: refreshed_mv = mv_reporting * (yf_price / masttro_price).
    This implicitly preserves Masttro's FX rate. Returns None if there's no
    refresh on disk or no positions for the scope on that date.
    """
    accounts = scope_accounts(conn, scope)
    if not accounts:
        return None
    cur = conn.cursor()
    cur.execute("SELECT MAX(refresh_date) FROM pricing_refresh")
    latest_refresh = cur.fetchone()[0]
    if not latest_refresh:
        return None
    clause, params = _accounts_clause(accounts)
    sql = f"""
        SELECT SUM(
            CASE
                WHEN pr.price IS NOT NULL
                 AND p.price_local IS NOT NULL
                 AND p.price_local != 0
                THEN p.mv_reporting * (pr.price / p.price_local)
                ELSE p.mv_reporting
            END
        ) AS nav_refreshed
        FROM position_snapshot p
        LEFT JOIN pricing_refresh pr
            ON pr.security_id = p.security_id
           AND pr.refresh_date = ?
        WHERE p.snapshot_date = ?
          AND p.account_node_id IN {clause}
    """
    cur.execute(sql, [latest_refresh, snapshot_date] + params)
    row = cur.fetchone()
    return float(row[0]) if row and row[0] is not None else None


def nav_series(conn, scope: str, use_refresh: bool = False) -> pd.DataFrame:
    """Date-indexed NAV series (DataFrame indexed by datetime with column 'nav').

    If use_refresh=True, the *latest* NAV point is recomputed using the most
    recent yfinance pricing_refresh; historical points are left untouched.
    """
    accounts = scope_accounts(conn, scope)
    if not accounts:
        return pd.DataFrame()
    clause, params = _accounts_clause(accounts)
    df = pd.read_sql_query(
        f"""SELECT snapshot_date AS date, SUM(mv_reporting) AS nav
            FROM position_snapshot
            WHERE account_node_id IN {clause}
            GROUP BY snapshot_date
            ORDER BY snapshot_date""",
        conn, params=params,
    )
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date")

    if use_refresh and not df.empty:
        last_date_str = df.index[-1].strftime("%Y-%m-%d")
        refreshed = _refreshed_end_nav(conn, scope, last_date_str)
        if refreshed is not None:
            df.iloc[-1, df.columns.get_loc("nav")] = refreshed

    return df


def top_positions(conn, scope: str, n: int = 10) -> pd.DataFrame:
    """Top-N positions by mv_reporting, aggregated across accounts (same security
    may appear in multiple accounts in scope)."""
    df = current_positions(conn, scope, include_refresh=False)
    if df.empty:
        return df
    agg = (
        df.groupby(
            ["asset_name", "isin", "asset_class", "security_type", "sector", "ticker_masttro"],
            dropna=False,
            as_index=False,
        )
        .agg(
            quantity=("quantity", "sum"),
            mv_reporting=("mv_reporting", "sum"),
            total_cost_local=("total_cost_local", "sum"),
            unrealized_gl_local=("unrealized_gl_local", "sum"),
        )
        .sort_values("mv_reporting", ascending=False)
    )
    total = agg["mv_reporting"].sum()
    agg["weight"] = agg["mv_reporting"] / total if total else 0
    return agg.head(n).reset_index(drop=True)


def allocation(conn, scope: str, by: str = "asset_class") -> pd.DataFrame:
    """Allocation pivot. `by` can be one of: asset_class, security_type, sector,
    geographic_exposure, local_ccy, custodian, account_alias, trust_alias."""
    valid = {"asset_class", "security_type", "sector", "geographic_exposure",
             "local_ccy", "custodian", "account_alias", "trust_alias"}
    if by not in valid:
        raise ValueError(f"by must be one of {sorted(valid)}; got {by!r}")
    df = current_positions(conn, scope, include_refresh=False)
    if df.empty:
        return df
    agg = (
        df.groupby(by, dropna=False, as_index=False)
        .agg(mv_reporting=("mv_reporting", "sum"),
             n_positions=("security_id", "nunique"))
        .sort_values("mv_reporting", ascending=False)
    )
    total = agg["mv_reporting"].sum()
    agg["weight"] = agg["mv_reporting"] / total if total else 0
    return agg.reset_index(drop=True)


def transactions(conn, scope: str, start: str | None = None, end: str | None = None,
                 types: Iterable[str] | None = None) -> pd.DataFrame:
    """Flat transaction list for the scope. Dates in 'YYYY-MM-DD' format."""
    accounts = scope_accounts(conn, scope)
    if not accounts:
        return pd.DataFrame()
    clause, params = _accounts_clause(accounts)
    sql = f"""
        SELECT
            t.transaction_date,
            t.account_node_id,
            e.alias AS account_alias,
            t.security_id,
            s.asset_name,
            t.transaction_type_clean AS transaction_type,
            t.gwm_in_ex_type,
            t.quantity,
            t.net_price_local,
            t.net_amount_local,
            t.net_amount_reporting,
            t.local_ccy,
            t.reporting_ccy,
            t.is_external_flow,
            t.comments
        FROM transaction_log t
        JOIN entity e ON t.account_node_id = e.node_id
        LEFT JOIN security s ON t.security_id = s.security_id
        WHERE t.account_node_id IN {clause}
    """
    if start:
        sql += " AND t.transaction_date >= ?"
        params.append(start)
    if end:
        sql += " AND t.transaction_date <= ?"
        params.append(end)
    if types:
        types = list(types)
        sql += f" AND t.transaction_type_clean IN ({','.join('?' * len(types))})"
        params.extend(types)
    sql += " ORDER BY t.transaction_date"
    return pd.read_sql_query(sql, conn, params=params)


def income_series(conn, scope: str, freq: str = "M") -> pd.DataFrame:
    """Dividends + interest + income aggregated by month (or other freq)."""
    income_types = ["Cash Dividends", "Interest", "Income"]
    df = transactions(conn, scope, types=income_types)
    if df.empty:
        return df
    df["transaction_date"] = pd.to_datetime(df["transaction_date"])
    df["period"] = df["transaction_date"].dt.to_period(freq)
    out = (
        df.groupby(["period", "transaction_type"], as_index=False)
        ["net_amount_reporting"].sum()
        .pivot(index="period", columns="transaction_type", values="net_amount_reporting")
        .fillna(0.0)
    )
    out["Total Income"] = out.sum(axis=1)
    out.index = out.index.to_timestamp()
    return out


def external_flows(conn, scope: str, start: str | None = None,
                   end: str | None = None) -> pd.DataFrame:
    """Net external capital flows (Deposit + Withdrawal only) — the TWR
    denominator-adjustment series."""
    df = transactions(conn, scope, start=start, end=end)
    if df.empty:
        return df
    return df[df["is_external_flow"] == 1][[
        "transaction_date", "account_alias", "transaction_type",
        "net_amount_reporting"
    ]].reset_index(drop=True)


def concentration(conn, scope: str) -> dict:
    """Concentration metrics: top-10 weight + HHI."""
    df = top_positions(conn, scope, n=1000)
    if df.empty:
        return {}
    total = df["mv_reporting"].sum()
    weights = (df["mv_reporting"] / total).fillna(0)
    return {
        "total_positions": len(df),
        "top_1_weight": float(weights.iloc[0]) if len(weights) else 0.0,
        "top_5_weight": float(weights.head(5).sum()),
        "top_10_weight": float(weights.head(10).sum()),
        "hhi": float((weights ** 2).sum() * 10000),  # Herfindahl, 0-10000 scale
    }


def nav_by(conn, scope: str, group_by: str = "account") -> pd.DataFrame:
    """NAV trajectory pivoted by account or trust.

    Returns wide DataFrame: index=snapshot_date, columns=account/trust aliases,
    values=NAV in reporting CCY. NaN where the account had no positions in that month.
    """
    if group_by not in {"account", "trust"}:
        raise ValueError(f"group_by must be 'account' or 'trust'; got {group_by!r}")
    accounts = scope_accounts(conn, scope)
    if not accounts:
        return pd.DataFrame()
    clause, params = _accounts_clause(accounts)
    if group_by == "account":
        sql = f"""
            SELECT p.snapshot_date AS date,
                   COALESCE(e.alias, e.node_id) AS bucket,
                   SUM(p.mv_reporting) AS nav
            FROM position_snapshot p
            JOIN entity e ON p.account_node_id = e.node_id
            WHERE p.account_node_id IN {clause}
            GROUP BY p.snapshot_date, bucket
            ORDER BY p.snapshot_date, bucket
        """
    else:  # trust
        sql = f"""
            SELECT p.snapshot_date AS date,
                   COALESCE(ea.trust_alias, e.alias, e.node_id) AS bucket,
                   SUM(p.mv_reporting) AS nav
            FROM position_snapshot p
            JOIN entity e ON p.account_node_id = e.node_id
            LEFT JOIN entity_attribution ea ON p.account_node_id = ea.node_id
            WHERE p.account_node_id IN {clause}
            GROUP BY p.snapshot_date, bucket
            ORDER BY p.snapshot_date, bucket
        """
    df = pd.read_sql_query(sql, conn, params=params)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    pivot = df.pivot(index="date", columns="bucket", values="nav")
    # Order columns by latest-date NAV desc
    if not pivot.empty:
        last = pivot.iloc[-1].fillna(0).sort_values(ascending=False)
        pivot = pivot[last.index.tolist()]
        pivot["Total"] = pivot.sum(axis=1, min_count=1)
    return pivot


def income_by(conn, scope: str, group_by: str = "account",
              freq: str = "M") -> pd.DataFrame:
    """Income (Cash Dividends + Interest + Income) pivoted by account/trust × month.

    Returns wide DataFrame: index=period, columns=account/trust aliases, values=total income.
    """
    if group_by not in {"account", "trust"}:
        raise ValueError(f"group_by must be 'account' or 'trust'; got {group_by!r}")
    df = transactions(conn, scope, types=["Cash Dividends", "Interest", "Income"])
    if df.empty:
        return df

    df["transaction_date"] = pd.to_datetime(df["transaction_date"])
    df["period"] = df["transaction_date"].dt.to_period(freq).dt.to_timestamp()

    if group_by == "account":
        df["bucket"] = df["account_alias"]
    else:
        # Need trust_alias — join via attribution
        cur = conn.cursor()
        cur.execute(
            "SELECT node_id, trust_alias FROM entity_attribution WHERE trust_alias IS NOT NULL"
        )
        trust_map = dict(cur.fetchall())
        df["bucket"] = df["account_node_id"].map(trust_map).fillna(df["account_alias"])

    pivot = (
        df.groupby(["period", "bucket"], as_index=False)["net_amount_reporting"].sum()
          .pivot(index="period", columns="bucket", values="net_amount_reporting")
          .fillna(0.0)
    )
    if not pivot.empty:
        pivot["Total"] = pivot.sum(axis=1)
    return pivot


def external_flows_by(conn, scope: str, group_by: str = "account") -> pd.DataFrame:
    """External flows (Deposits + Withdrawals) totaled by account or trust."""
    if group_by not in {"account", "trust"}:
        raise ValueError(f"group_by must be 'account' or 'trust'; got {group_by!r}")
    df = external_flows(conn, scope)
    if df.empty:
        return df
    if group_by == "trust":
        cur = conn.cursor()
        accounts = scope_accounts(conn, scope)
        if accounts:
            clause, params = _accounts_clause(accounts)
            cur.execute(
                f"SELECT node_id, trust_alias FROM entity_attribution WHERE node_id IN {clause}",
                params,
            )
            trust_map = dict(cur.fetchall())
        else:
            trust_map = {}
        df["bucket"] = df.get("account_node_id", pd.NA)
        # We don't have account_node_id in external_flows output; rebuild from full txns
        full = transactions(conn, scope)
        full = full[full["is_external_flow"] == 1]
        full["bucket"] = full["account_node_id"].map(trust_map).fillna(full["account_alias"])
        agg = (
            full.groupby("bucket", as_index=False)
                .agg(deposits=("net_amount_reporting", lambda s: s[s > 0].sum()),
                     withdrawals=("net_amount_reporting", lambda s: s[s < 0].sum()),
                     net=("net_amount_reporting", "sum"),
                     n_events=("net_amount_reporting", "count"))
                .sort_values("net", ascending=False)
        )
        return agg
    else:
        agg = (
            df.groupby("account_alias", as_index=False)
              .agg(deposits=("net_amount_reporting", lambda s: s[s > 0].sum()),
                   withdrawals=("net_amount_reporting", lambda s: s[s < 0].sum()),
                   net=("net_amount_reporting", "sum"),
                   n_events=("net_amount_reporting", "count"))
              .sort_values("net", ascending=False)
        )
        return agg


def _account_meta(conn, scope: str) -> pd.DataFrame:
    """Helper: account-level metadata for grouping (alias, custodian, acct#, trust)."""
    accounts = scope_accounts(conn, scope)
    if not accounts:
        return pd.DataFrame()
    clause, params = _accounts_clause(accounts)
    return pd.read_sql_query(
        f"""SELECT e.node_id AS account_node_id,
                   e.alias AS account_alias,
                   e.bank_broker AS custodian,
                   e.account_number,
                   ea.trust_node_id, ea.trust_alias
            FROM entity e
            LEFT JOIN entity_attribution ea ON e.node_id = ea.node_id
            WHERE e.node_id IN {clause}""",
        conn, params=params,
    )


def top_positions_by(conn, scope: str, group_by: str = "account",
                     n: int = 10) -> pd.DataFrame:
    """Top-N positions within each account or trust.

    Long format, one row per (bucket, position). Includes bucket-level weight.
    """
    if group_by not in {"account", "trust"}:
        raise ValueError(f"group_by must be 'account' or 'trust'; got {group_by!r}")
    df = current_positions(conn, scope, include_refresh=False)
    if df.empty:
        return df
    bucket_col = "account_alias" if group_by == "account" else "trust_alias"
    df = df.copy()
    df["bucket"] = df[bucket_col].fillna("(unattributed)")

    # Aggregate same security across multiple accounts within a bucket (only
    # matters for trust-level — same security held in multiple accounts of the
    # same trust collapses to one row).
    agg = (
        df.groupby(
            ["bucket", "asset_name", "isin", "asset_class", "security_type",
             "sector", "ticker_masttro"],
            dropna=False, as_index=False,
        ).agg(
            quantity=("quantity", "sum"),
            mv_reporting=("mv_reporting", "sum"),
            total_cost_local=("total_cost_local", "sum"),
            unrealized_gl_local=("unrealized_gl_local", "sum"),
        )
    )
    # Within each bucket, rank by mv and keep top n.
    agg["rank"] = agg.groupby("bucket")["mv_reporting"].rank(
        method="min", ascending=False,
    )
    agg = agg[agg["rank"] <= n].copy()
    agg["bucket_total_mv"] = agg.groupby("bucket")["mv_reporting"].transform("sum")
    # bucket_total here is sum of top-N only; for true bucket totals we'd need
    # the un-truncated agg. Replace with full bucket total:
    bucket_full = df.groupby("bucket")["mv_reporting"].sum().to_dict()
    agg["bucket_total_mv"] = agg["bucket"].map(bucket_full)
    agg["weight_in_bucket"] = agg["mv_reporting"] / agg["bucket_total_mv"]
    agg["rank"] = agg["rank"].astype(int)
    return (
        agg.sort_values(["bucket_total_mv", "bucket", "rank"], ascending=[False, True, True])
           .reset_index(drop=True)
    )


def allocation_by(conn, scope: str, group_by: str = "account",
                  by_dimension: str = "asset_class") -> pd.DataFrame:
    """Allocation pivot: rows = bucket (account/trust), columns = by_dimension values,
    values = weight (0..1) of that bucket's NAV.

    Also includes a 'Total NAV' column as the rightmost data column.
    """
    if group_by not in {"account", "trust"}:
        raise ValueError(f"group_by must be 'account' or 'trust'; got {group_by!r}")
    valid_dims = {"asset_class", "security_type", "sector", "geographic_exposure",
                  "local_ccy", "custodian"}
    if by_dimension not in valid_dims:
        raise ValueError(f"by_dimension must be one of {sorted(valid_dims)}; got {by_dimension!r}")

    df = current_positions(conn, scope, include_refresh=False)
    if df.empty:
        return df
    bucket_col = "account_alias" if group_by == "account" else "trust_alias"
    df = df.copy()
    df["bucket"] = df[bucket_col].fillna("(unattributed)")

    agg = (
        df.groupby(["bucket", by_dimension], dropna=False, as_index=False)
          ["mv_reporting"].sum()
    )
    pivot = agg.pivot(index="bucket", columns=by_dimension, values="mv_reporting").fillna(0.0)
    total_mv = pivot.sum(axis=1)
    weights = pivot.div(total_mv.replace(0, pd.NA), axis=0)
    weights["Total NAV"] = total_mv
    # Sort buckets by Total NAV desc.
    weights = weights.sort_values("Total NAV", ascending=False)
    return weights


def concentration_by(conn, scope: str, group_by: str = "account") -> pd.DataFrame:
    """Per-bucket concentration metrics. One row per account/trust."""
    if group_by not in {"account", "trust"}:
        raise ValueError(f"group_by must be 'account' or 'trust'; got {group_by!r}")
    df = current_positions(conn, scope, include_refresh=False)
    if df.empty:
        return df
    bucket_col = "account_alias" if group_by == "account" else "trust_alias"
    df = df.copy()
    df["bucket"] = df[bucket_col].fillna("(unattributed)")

    # Aggregate same security within bucket.
    agg = (
        df.groupby(["bucket", "asset_name", "isin"], dropna=False, as_index=False)
          ["mv_reporting"].sum()
    )

    rows = []
    for bucket, g in agg.groupby("bucket"):
        g = g.sort_values("mv_reporting", ascending=False)
        total = g["mv_reporting"].sum()
        weights = g["mv_reporting"] / total if total else pd.Series([0.0])
        rows.append({
            "bucket": bucket,
            "n_positions": int(len(g)),
            "mv_reporting": float(total),
            "top_1_weight": float(weights.iloc[0]) if len(weights) else 0.0,
            "top_3_weight": float(weights.head(3).sum()),
            "top_5_weight": float(weights.head(5).sum()),
            "top_10_weight": float(weights.head(10).sum()),
            "hhi": float((weights ** 2).sum() * 10000),
        })
    out = pd.DataFrame(rows).sort_values("mv_reporting", ascending=False).reset_index(drop=True)
    return out


def trusts_in_scope(conn, scope: str) -> pd.DataFrame:
    """List of distinct trusts (and their account counts) contributing to a scope."""
    accounts = scope_accounts(conn, scope)
    if not accounts:
        return pd.DataFrame()
    clause, params = _accounts_clause(accounts)
    return pd.read_sql_query(
        f"""SELECT ea.trust_alias, ea.trust_node_id,
                   COUNT(DISTINCT ea.node_id) AS n_accounts,
                   SUM(p.mv_reporting) AS mv_reporting
            FROM entity_attribution ea
            JOIN position_snapshot p ON p.account_node_id = ea.node_id
            WHERE ea.node_id IN {clause}
              AND p.snapshot_date = (
                  SELECT MAX(snapshot_date) FROM position_snapshot
                  WHERE account_node_id IN {clause}
              )
            GROUP BY ea.trust_alias, ea.trust_node_id
            ORDER BY mv_reporting DESC""",
        conn, params=params + params,
    )
