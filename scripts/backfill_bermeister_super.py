"""One-off / on-update backfill: Bermeister Superannuation Fund holdings.

Background
----------
The Bermeister Superannuation Fund is a self-managed super fund whose
portfolio (ANZ, CBA, NAB, Westpac, Macquarie, Wesfarmers, Woodside, Telstra,
the Betashares/Vaneck/Vanguard ETFs, NAB term deposits, CMA cash, etc.) is
held in Masttro as `is_account=false` direct-equity GWM nodes — so, like the
AU broker-migration nodes, none of it is ingested by the normal sync.

It's an ownership-reflected shared vehicle (group_node_id 93382) split three
ways, confirmed against the ANZ position:
    Kevin Bermeister    56.72%   (reflection 102_93382, under Bermeister Family)
    Beverley Bermeister 11.25%   (reflection 102_94931, under Bermeister Family)
    Dyne US Retirement  32.03%   (reflection 102_133920, under Dyne Family US)
Masttro returns each owner's ownership-sliced quantities per reflection, so
the three slices sum to the full ~$10.19M fund.

What this does
--------------
Reuses the reflection nodes themselves as canonical "accounts" (they already
attribute correctly via rebuild_attribution: the Bermeister reflections ->
entity "Bermeister Superannuation Fund"; the Dyne US reflection -> the
stronger "Dyne US Retirement" wrapper). is_canonical_account is set true while
is_account stays false, so the weekly mark_canonical_accounts (which only
resets is_account=true rows) leaves it alone.

  - Bermeister side: Kevin + Beverley slices AGGREGATED per (security, month)
    into 102_93382  -> entity "Bermeister Superannuation Fund" (~$6.92M).
  - Dyne US side:    the 32% slice into 102_133920 -> "Dyne US Retirement".
  - Berdy is excluded (it keeps its own "Berdy Investment Trust" entity).

Positions are aggregated (the position PK has no quantity dimension, so the
two Bermeister member slices for one security must be summed). Transactions
are remapped without aggregation — member slices have distinct quantities so
they stay separate rows and sum correctly for income/flows.

Idempotent (cleans the two target accounts first). Defaults to dry run.

Env: MASTTRO_API_KEY, MASTTRO_API_SECRET, SUPABASE_DB_URL.
"""

from __future__ import annotations

import argparse
import datetime as dt
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from client import MasttroClient
from tracker.db import connect, log_sync
from tracker.sync_supabase import (
    upsert_positions,
    upsert_securities,
    upsert_transactions,
)

CLIENT_ID = 7693

# Masttro 500s on historicalMonths pulls for this large family / complex
# super-fund subtree (Berdy's 731 positions nested under it), and the
# reflection-node pull 500s too. Single-month family pulls work (the daily
# sync does them), so we walk month-by-month at the family level. MONTHS is
# the set of month-ends to ingest; the current month reuses the daily-sync
# cache (0 extra calls). Add history months here to extend the NAV series.
BERM_FAMILY = "102_93363"      # Bermeister Family (reports AUD)
DYNE_US_FAMILY = "102_93356"   # Dyne Family (US) (reports USD)
BERM_ACCOUNT = "102_93382"     # Kevin's reflection -> entity "Bermeister Superannuation Fund"
DYNE_US_ACCOUNT = "102_133920" # Dyne US reflection -> entity "Dyne US Retirement"

FAMILY_CCY = {BERM_FAMILY: "AUD", DYNE_US_FAMILY: "USD"}
# target account -> (family to pull from, reporting ccy for its rows)
TARGET = {
    BERM_ACCOUNT: (BERM_FAMILY, "AUD"),
    DYNE_US_ACCOUNT: (DYNE_US_FAMILY, "USD"),
}
TARGET_ACCOUNTS = sorted(TARGET)
# reflection nodes whose is_account=false descendants we fold in
REFLECTIONS = {
    "102_93382": BERM_ACCOUNT,     # Kevin
    "102_94931": BERM_ACCOUNT,     # Beverley
    "102_133920": DYNE_US_ACCOUNT,  # Dyne US
}


def months_back(today, n):
    """Current month + the previous n-1 calendar months (newest first)."""
    out = []
    y, m = today.year, today.month
    for _ in range(n):
        out.append(f"{y:04d}{m:02d}")
        m -= 1
        if m == 0:
            m, y = 12, y - 1
    return out


def _f(x):
    return float(str(x).replace(",", "")) if x not in (None, "") else 0.0


def _direct_equity_nodes(conn):
    """node_id -> target_account, for every is_account=false descendant of a
    reflection (excludes Berdy, which is_account=true, and any account)."""
    out = {}
    with conn.cursor() as cur:
        for refl, target in REFLECTIONS.items():
            cur.execute(
                """WITH RECURSIVE d AS (
                       SELECT node_id FROM entity WHERE node_id=%s
                     UNION SELECT e.node_id FROM entity e JOIN d ON e.parent_node_id=d.node_id)
                   SELECT e.node_id FROM entity e JOIN d ON e.node_id=d.node_id
                   WHERE e.is_account = FALSE""",
                (refl,),
            )
            for r in cur.fetchall():
                out[r["node_id"]] = target
    # Drop the reflection/wrapper nodes themselves — Masttro reports those as
    # a row whose "quantity" is the owner's ownership percentage (e.g.
    # "56.72%"), not a share count. We only want the leaf security holdings.
    for refl in REFLECTIONS:
        out.pop(refl, None)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="write to the DB (default: dry run)")
    ap.add_argument("--months", type=int, default=1,
                    help="how many months back to ingest (1 = current only). "
                         "History months are fetched as single-month family pulls.")
    args = ap.parse_args()
    mode = "APPLY" if args.apply else "DRY-RUN"

    today = dt.date.today()
    cur_ym = f"{today.year:04d}{today.month:02d}"
    months = months_back(today, args.months)

    conn = connect()
    node_target = _direct_equity_nodes(conn)
    berm_nodes = sum(1 for t in node_target.values() if t == BERM_ACCOUNT)
    us_nodes = sum(1 for t in node_target.values() if t == DYNE_US_ACCOUNT)
    print(f"[{mode}] Bermeister Super backfill — months={months}")
    print(f"  direct-equity nodes: Bermeister(Kevin+Beverley)={berm_nodes} -> {BERM_ACCOUNT}, "
          f"Dyne US={us_nodes} -> {DYNE_US_ACCOUNT}")

    if not args.apply:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT node_id, sub_client_alias, trust_alias FROM entity_attribution
                   WHERE node_id = ANY(%s)""",
                (TARGET_ACCOUNTS,),
            )
            print("  target-account attribution:")
            for r in cur.fetchall():
                print(f"    {r['node_id']} -> {r['sub_client_alias']} / {r['trust_alias']}")
        conn.close()
        print("[DRY-RUN] would mark the 2 target nodes canonical, delete prior rows under "
              "them, then ingest the listed months (single-month family pulls; current "
              "month reuses the daily cache). Re-run with --apply.")
        return 0

    masttro = MasttroClient()
    try:
        # Mark the two target nodes canonical (is_account stays false -> survives
        # the weekly mark_canonical_accounts) and clear prior rows for a clean
        # idempotent reload.
        with conn.cursor() as cur:
            cur.execute("UPDATE entity SET is_canonical_account = TRUE WHERE node_id = ANY(%s)",
                        (TARGET_ACCOUNTS,))
            cur.execute("DELETE FROM position_snapshot WHERE account_node_id = ANY(%s)",
                        (TARGET_ACCOUNTS,))
            pos_del = cur.rowcount
            cur.execute("DELETE FROM transaction_log WHERE account_node_id = ANY(%s)",
                        (TARGET_ACCOUNTS,))
            txn_del = cur.rowcount
        conn.commit()
        print(f"  cleaned prior run: positions deleted={pos_del} txns deleted={txn_del}")

        pos_total = txn_total = 0
        for ym in months:
            for target, (fam, ccy) in TARGET.items():
                is_current = ym == cur_ym
                # Current month reuses the daily-sync cache (descriptor + ccy
                # must match scripts/sync_masttro_daily.py exactly).
                h_desc = (f"daily_{fam}_{ccy.lower()}_{ym}" if is_current
                          else f"berm_super_{fam}_{ccy.lower()}_{ym}")
                hold = masttro.get_cached_or_fetch(
                    f"Holdings/{CLIENT_ID}",
                    {"ccy": ccy, "yearMonth": ym, "investmentVehicle": fam},
                    descriptor=h_desc, timeout=180,
                ) or []
                # Aggregate the family's super-fund rows per (security, date).
                pos_agg: dict[tuple, dict] = {}
                secsrc = []
                for hh in hold:
                    if node_target.get(hh.get("nodeId")) != target or hh.get("securityId") is None:
                        continue
                    if "%" in str(hh.get("quantity") or ""):
                        continue  # ownership-percentage wrapper row, not a holding
                    secsrc.append(hh)
                    key = (hh["securityId"], hh.get("date"))
                    b = pos_agg.get(key)
                    if b is None:
                        pos_agg[key] = b = {
                            "nodeId": target, "securityId": hh["securityId"], "date": hh.get("date"),
                            "quantity": 0.0, "localMarketValue": 0.0, "marketValue": 0.0,
                            "totalCost": 0.0, "accruedInterest": 0.0, "localAccruedInterest": 0.0,
                            "price": hh.get("price"), "unitCost": hh.get("unitCost"),
                        }
                    b["quantity"] += _f(hh.get("quantity"))
                    b["localMarketValue"] += _f(hh.get("localMarketValue"))
                    b["marketValue"] += _f(hh.get("marketValue"))
                    b["totalCost"] += _f(hh.get("totalCost"))
                    b["accruedInterest"] += _f(hh.get("accruedInterest"))
                    b["localAccruedInterest"] += _f(hh.get("localAccruedInterest"))

                # Transactions: YTD pull (daily cache) for the current month only.
                txn_rows = []
                if is_current:
                    txns = masttro.get_cached_or_fetch(
                        f"Transactions/{CLIENT_ID}",
                        {"ccy": ccy, "yearMonth": ym, "period": 1, "investmentVehicle": fam},
                        descriptor=f"daily_{fam}_{ccy.lower()}_{ym}_p1", timeout=180,
                    ) or []
                    for t in txns:
                        if node_target.get(t.get("nodeId")) != target:
                            continue
                        t = dict(t); t["nodeId"] = target
                        txn_rows.append(t); secsrc.append(t)

                upsert_securities(conn, secsrc, None)
                ps = upsert_positions(conn, list(pos_agg.values()), [target], reporting_ccy=ccy)
                ts = upsert_transactions(conn, txn_rows, [target], reporting_ccy=ccy)
                pos_total += ps["inserted"]
                txn_total += ts["submitted"]
                print(f"  {ym} {target} ({ccy}): positions+={ps['inserted']:3} txns+={ts['submitted']:3}")

        log_sync(conn, "bermeister_super_backfill", BERM_ACCOUNT,
                 f"months={len(months)} positions={pos_total} txns={txn_total}", pos_total)
        print(f"\n  DONE — positions written={pos_total} txns submitted={txn_total}")
    finally:
        conn.close()
        masttro.report()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
