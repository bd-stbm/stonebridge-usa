"""Performance calculations: TWR (time-weighted), modified Dietz, IRR.

All functions take a scope + connection and return either a scalar/dict (for
single-period perf) or a DataFrame (for time series).
"""

from __future__ import annotations

import math
from datetime import date

import pandas as pd

from tracker import api


def _period_start_date(end_date: pd.Timestamp, period: str,
                       earliest: pd.Timestamp) -> pd.Timestamp:
    """Resolve a period keyword to a start date. `earliest` clamps long-period
    lookbacks to the data we actually have."""
    if period == "mtd":
        start = pd.Timestamp(end_date.year, end_date.month, 1) - pd.Timedelta(days=1)
    elif period == "ytd":
        start = pd.Timestamp(end_date.year - 1, 12, 31)
    elif period == "3m":
        start = end_date - pd.DateOffset(months=3)
    elif period == "6m":
        start = end_date - pd.DateOffset(months=6)
    elif period == "1y":
        start = end_date - pd.DateOffset(years=1)
    elif period == "itd":
        start = earliest
    else:
        raise ValueError(f"Unknown period: {period}")
    return max(start, earliest)


def _nearest_nav_on_or_before(nav: pd.DataFrame, target: pd.Timestamp) -> tuple[pd.Timestamp, float]:
    """Get the NAV on the last snapshot date on or before `target`."""
    on_or_before = nav[nav.index <= target]
    if on_or_before.empty:
        return nav.index[0], float(nav.iloc[0]["nav"])
    return on_or_before.index[-1], float(on_or_before.iloc[-1]["nav"])


def period_performance(conn, scope: str, period: str = "ytd",
                       use_refresh: bool = False) -> dict:
    """Modified-Dietz return for a period: r = (end - start - flows) / (start + 0.5*flows).

    If use_refresh=True, the end NAV is computed using the latest yfinance
    refresh (start NAV stays as Masttro's recorded value, since it's a
    historical anchor).

    Returns a dict with start/end NAV, external flows, gain, and return_pct.
    """
    nav = api.nav_series(conn, scope, use_refresh=use_refresh)
    if nav.empty:
        return {}
    end_date = nav.index.max()
    earliest = nav.index.min()
    start_date = _period_start_date(end_date, period.lower(), earliest)

    start_date_actual, start_nav = _nearest_nav_on_or_before(nav, start_date)
    end_nav = float(nav.loc[end_date]["nav"])

    flows_df = api.external_flows(
        conn, scope,
        start=start_date_actual.strftime("%Y-%m-%d"),
        end=end_date.strftime("%Y-%m-%d"),
    )
    flows = (
        float(flows_df.loc[flows_df["transaction_date"] > start_date_actual.strftime("%Y-%m-%d"),
                           "net_amount_reporting"].sum())
        if not flows_df.empty else 0.0
    )

    gain = end_nav - start_nav - flows
    denom = start_nav + 0.5 * flows
    return_pct = (gain / denom) if denom else None

    return {
        "scope": scope,
        "period": period,
        "start_date": start_date_actual.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d"),
        "start_nav": start_nav,
        "end_nav": end_nav,
        "external_flows": flows,
        "gain": gain,
        "return_pct": return_pct,
    }


def twr_series(conn, scope: str, use_refresh: bool = False) -> pd.DataFrame:
    """Time-weighted return chain across all snapshot dates.

    Per-period return r_i = (mv_end - flows_in_period) / mv_start - 1.
    Cumulative TWR is the geometric chain. Returns DataFrame with columns
    nav, flows, period_return, cumulative_twr.

    If use_refresh=True, the final NAV uses the latest yfinance refresh.
    """
    nav = api.nav_series(conn, scope, use_refresh=use_refresh)
    if nav.empty:
        return nav
    nav = nav.copy()
    nav["nav"] = nav["nav"].astype(float)

    flows = api.external_flows(conn, scope)
    if not flows.empty:
        flows["transaction_date"] = pd.to_datetime(flows["transaction_date"])

    out_rows = []
    prev_date = None
    prev_nav = None
    cumulative = 1.0
    for d, row in nav.iterrows():
        cur_nav = float(row["nav"])
        if prev_nav is None:
            out_rows.append({
                "date": d, "nav": cur_nav, "flows": 0.0,
                "period_return": None, "cumulative_twr": 0.0,
            })
        else:
            if flows.empty:
                period_flows = 0.0
            else:
                mask = (flows["transaction_date"] > prev_date) & (flows["transaction_date"] <= d)
                period_flows = float(flows.loc[mask, "net_amount_reporting"].sum())
            denom = prev_nav
            if denom and denom != 0:
                period_r = (cur_nav - period_flows) / denom - 1
            else:
                period_r = None
            if period_r is not None and not math.isnan(period_r):
                cumulative *= (1 + period_r)
            out_rows.append({
                "date": d, "nav": cur_nav, "flows": period_flows,
                "period_return": period_r,
                "cumulative_twr": cumulative - 1,
            })
        prev_date = d
        prev_nav = cur_nav

    return pd.DataFrame(out_rows).set_index("date")


def _xnpv(rate: float, cashflows: list[tuple[date, float]]) -> float:
    if not cashflows:
        return 0.0
    t0 = cashflows[0][0]
    total = 0.0
    for (d, cf) in cashflows:
        t = (d - t0).days / 365.0
        total += cf / (1 + rate) ** t
    return total


def _xirr(cashflows: list[tuple[date, float]], guess: float = 0.1,
          max_iter: int = 100, tol: float = 1e-7) -> float | None:
    """Newton-Raphson XIRR. Returns None if it fails to converge."""
    if not cashflows or len(cashflows) < 2:
        return None
    rate = guess
    for _ in range(max_iter):
        # numerical derivative
        npv = _xnpv(rate, cashflows)
        d_npv = (_xnpv(rate + 1e-6, cashflows) - npv) / 1e-6
        if d_npv == 0:
            return None
        new_rate = rate - npv / d_npv
        if abs(new_rate - rate) < tol:
            return new_rate
        rate = new_rate
        if rate < -0.999:
            return None
    return None


def irr_for_period(conn, scope: str, period: str = "ytd",
                   use_refresh: bool = False) -> dict:
    """Money-weighted return (XIRR) for a period.

    Treats: start_nav as an outflow at start_date, external flows as outflows
    when positive (deposit) / inflows when negative (withdrawal), and end_nav
    as an inflow at end_date.

    If use_refresh=True, end NAV uses the latest yfinance refresh.
    """
    nav = api.nav_series(conn, scope, use_refresh=use_refresh)
    if nav.empty:
        return {}
    end_date = nav.index.max()
    start_date = _period_start_date(end_date, period.lower(), nav.index.min())
    start_date_actual, start_nav = _nearest_nav_on_or_before(nav, start_date)
    end_nav = float(nav.loc[end_date]["nav"])

    cashflows: list[tuple[date, float]] = [
        (start_date_actual.date(), -start_nav)  # invested at start
    ]
    flows_df = api.external_flows(
        conn, scope,
        start=start_date_actual.strftime("%Y-%m-%d"),
        end=end_date.strftime("%Y-%m-%d"),
    )
    if not flows_df.empty:
        flows_df["transaction_date"] = pd.to_datetime(flows_df["transaction_date"])
        for _, r in flows_df.iterrows():
            if r["transaction_date"] <= start_date_actual:
                continue
            cashflows.append(
                (r["transaction_date"].date(), -float(r["net_amount_reporting"]))
            )
    cashflows.append((end_date.date(), end_nav))

    rate = _xirr(cashflows)
    return {
        "scope": scope,
        "period": period,
        "start_date": start_date_actual.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d"),
        "n_cashflows": len(cashflows),
        "xirr": rate,
    }


def performance_summary(conn, scope: str, use_refresh: bool = False) -> pd.DataFrame:
    """One-stop summary: modified Dietz + XIRR over the standard periods."""
    rows = []
    for p in ["mtd", "3m", "6m", "ytd", "1y", "itd"]:
        d = period_performance(conn, scope, p, use_refresh=use_refresh) or {}
        x = irr_for_period(conn, scope, p, use_refresh=use_refresh) or {}
        rows.append({
            "period": p,
            "start_date": d.get("start_date"),
            "end_date": d.get("end_date"),
            "start_nav": d.get("start_nav"),
            "end_nav": d.get("end_nav"),
            "flows": d.get("external_flows"),
            "gain": d.get("gain"),
            "dietz_return": d.get("return_pct"),
            "xirr": x.get("xirr"),
        })
    return pd.DataFrame(rows)


def performance_summary_comparison(conn, scope: str) -> pd.DataFrame:
    """Side-by-side: Masttro-priced vs yfinance-refreshed performance, one row per period."""
    masttro = performance_summary(conn, scope, use_refresh=False)
    refreshed = performance_summary(conn, scope, use_refresh=True)
    merged = masttro.merge(
        refreshed[["period", "end_nav", "gain", "dietz_return", "xirr"]],
        on="period", suffixes=("_masttro", "_refreshed"),
    )
    merged["dietz_delta_bps"] = (
        (merged["dietz_return_refreshed"] - merged["dietz_return_masttro"]) * 10000
    )
    merged["xirr_delta_bps"] = (
        (merged["xirr_refreshed"] - merged["xirr_masttro"]) * 10000
    )
    merged["end_nav_delta"] = merged["end_nav_refreshed"] - merged["end_nav_masttro"]
    cols = [
        "period", "start_date", "end_date", "start_nav", "flows",
        "end_nav_masttro", "end_nav_refreshed", "end_nav_delta",
        "gain_masttro", "gain_refreshed",
        "dietz_return_masttro", "dietz_return_refreshed", "dietz_delta_bps",
        "xirr_masttro", "xirr_refreshed", "xirr_delta_bps",
    ]
    return merged[cols]


def performance_by(conn, scope: str, group_by: str = "account",
                   periods: list[str] | None = None,
                   use_refresh: bool = False) -> pd.DataFrame:
    """Performance summary per account or per trust.

    Wide DataFrame, one row per bucket (account or trust). Columns include
    start/end NAV and Dietz + XIRR per period for the requested periods.
    """
    if group_by not in {"account", "trust"}:
        raise ValueError(f"group_by must be 'account' or 'trust'; got {group_by!r}")
    periods = periods or ["mtd", "3m", "6m", "ytd", "1y", "itd"]

    from tracker import api
    meta = api._account_meta(conn, scope)
    if meta.empty:
        return pd.DataFrame()

    if group_by == "account":
        meta["bucket_id"] = meta["account_node_id"]
        meta["bucket_label"] = meta["account_alias"]
        meta["trust"] = meta["trust_alias"]
        meta["custodian_col"] = meta["custodian"]
    else:
        meta["bucket_id"] = meta["trust_node_id"]
        meta["bucket_label"] = meta["trust_alias"]
        # Aggregate metadata per trust
        meta = (
            meta.dropna(subset=["bucket_id"])
                .groupby(["bucket_id", "bucket_label"], dropna=False, as_index=False)
                .agg(n_accounts=("account_node_id", "nunique"))
        )

    rows = []
    for _, m in meta.iterrows():
        bucket_id = m["bucket_id"]
        if not bucket_id:
            continue
        row = {"bucket_id": bucket_id, "bucket_label": m["bucket_label"]}
        if group_by == "account":
            row["custodian"] = m.get("custodian_col")
            row["trust"] = m.get("trust")
        else:
            row["n_accounts"] = m.get("n_accounts")

        # Latest NAV via api.nav_series scoped to this bucket
        nav = api.nav_series(conn, bucket_id, use_refresh=use_refresh)
        if not nav.empty:
            row["current_nav"] = float(nav.iloc[-1]["nav"])
        else:
            row["current_nav"] = None

        for p in periods:
            d = period_performance(conn, bucket_id, p, use_refresh=use_refresh) or {}
            x = irr_for_period(conn, bucket_id, p, use_refresh=use_refresh) or {}
            row[f"{p}_dietz"] = d.get("return_pct")
            row[f"{p}_xirr"] = x.get("xirr")
            row[f"{p}_gain"] = d.get("gain")
            row[f"{p}_flows"] = d.get("external_flows")
        rows.append(row)

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("current_nav", ascending=False)
    return df


def performance_by_long(conn, scope: str, group_by: str = "account") -> pd.DataFrame:
    """Long-format per-bucket performance: one row per (bucket, period)."""
    if group_by not in {"account", "trust"}:
        raise ValueError(f"group_by must be 'account' or 'trust'; got {group_by!r}")

    from tracker import api
    meta = api._account_meta(conn, scope)
    if meta.empty:
        return pd.DataFrame()

    if group_by == "account":
        buckets = list(zip(meta["account_node_id"], meta["account_alias"]))
    else:
        meta_t = meta.dropna(subset=["trust_node_id"]).drop_duplicates("trust_node_id")
        buckets = list(zip(meta_t["trust_node_id"], meta_t["trust_alias"]))

    rows = []
    for bucket_id, label in buckets:
        if not bucket_id:
            continue
        for p in ["mtd", "3m", "6m", "ytd", "1y", "itd"]:
            d = period_performance(conn, bucket_id, p) or {}
            x = irr_for_period(conn, bucket_id, p) or {}
            rows.append({
                "bucket_id": bucket_id,
                "bucket_label": label,
                "period": p,
                "start_date": d.get("start_date"),
                "end_date": d.get("end_date"),
                "start_nav": d.get("start_nav"),
                "end_nav": d.get("end_nav"),
                "flows": d.get("external_flows"),
                "gain": d.get("gain"),
                "dietz_return": d.get("return_pct"),
                "xirr": x.get("xirr"),
            })
    return pd.DataFrame(rows)
