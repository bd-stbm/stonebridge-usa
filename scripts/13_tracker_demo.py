"""Exercise the tracker API against the built DB.

Demonstrates every public query function for Dyne Family US, then drills into
one trust and one account to show scope flexibility.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.stdout.reconfigure(encoding="utf-8")

import pandas as pd

from tracker import api, compute
from tracker.api import connect


def section(title):
    print(f"\n{'='*78}\n{title}\n{'='*78}")


def show(df, max_rows=15, fmt=None):
    if df is None or (hasattr(df, "empty") and df.empty):
        print("  (empty)")
        return
    if isinstance(df, dict):
        for k, v in df.items():
            print(f"  {k}: {v}")
        return
    with pd.option_context("display.max_rows", max_rows,
                           "display.max_columns", None,
                           "display.width", 200,
                           "display.float_format", "{:,.4f}".format):
        print(df.head(max_rows).to_string())


def main():
    conn = connect()

    DYNE_US = "102_93356"
    DYLAN_TRUST = "102_93412"
    DYLAN_TRUST_BIG_ACCT = "101_52240"  # Dylan Trust 2010 #3806 ($24M, 8 positions)

    section("Scope sanity: how many canonical accounts under each level?")
    for label, scope in [
        ("Dyne Family US (whole family)", DYNE_US),
        ("Dylan Dyne Irrevocable Trust", DYLAN_TRUST),
        ("Dylan Trust 2010 #3806 (single account)", DYLAN_TRUST_BIG_ACCT),
    ]:
        accts = api.scope_accounts(conn, scope)
        print(f"  {label:<48} → {len(accts)} accounts")

    section("Trusts contributing to Dyne Family US")
    show(api.trusts_in_scope(conn, DYNE_US), max_rows=20)

    section("NAV trajectory — Dyne Family US (last 12 months)")
    nav = api.nav_series(conn, DYNE_US)
    show(nav.reset_index(), max_rows=15)

    section("Performance summary — Dyne Family US")
    show(compute.performance_summary(conn, DYNE_US))

    section("TWR chain (monthly period returns + cumulative) — Dyne Family US")
    show(compute.twr_series(conn, DYNE_US).reset_index(), max_rows=15)

    section("Top 10 positions — Dyne Family US")
    show(api.top_positions(conn, DYNE_US, n=10))

    section("Allocation by asset class — Dyne Family US")
    show(api.allocation(conn, DYNE_US, by="asset_class"))

    section("Allocation by sector — Dyne Family US")
    show(api.allocation(conn, DYNE_US, by="sector"), max_rows=20)

    section("Allocation by custodian — Dyne Family US")
    show(api.allocation(conn, DYNE_US, by="custodian"))

    section("Concentration metrics — Dyne Family US")
    show(api.concentration(conn, DYNE_US))

    section("Monthly income series — Dyne Family US")
    show(api.income_series(conn, DYNE_US, freq="M").reset_index(), max_rows=15)

    section("External flows (Deposits + Withdrawals) — Dyne Family US, last 6 months")
    flows = api.external_flows(conn, DYNE_US, start="2025-11-01")
    show(flows, max_rows=15)
    print(f"\n  Net external flow: {flows['net_amount_reporting'].sum():,.2f}" if not flows.empty else "")

    section("Drill-down: Dylan Dyne Irrevocable Trust — performance summary")
    show(compute.performance_summary(conn, DYLAN_TRUST))

    section("Drill-down: Dylan Trust 2010 #3806 — current positions (with refresh prices)")
    cp = api.current_positions(conn, DYLAN_TRUST_BIG_ACCT)
    cols = ["asset_name", "ticker_masttro", "quantity", "price_local", "mv_local",
            "yf_price", "price_delta_pct", "mv_refreshed", "mv_refreshed_delta"]
    cols = [c for c in cols if c in cp.columns]
    show(cp[cols], max_rows=20)


if __name__ == "__main__":
    main()
