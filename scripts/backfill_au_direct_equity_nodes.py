"""One-off backfill: AU direct-equity holding nodes (Jamindy + Saulos).

Background
----------
Dyne Family (AU) migrated brokers. For historical-reporting continuity the
prior holdings were recreated in Masttro as standalone "direct equity" GWM
nodes (one node per listed security) hanging directly off the trust, rather
than under a fed brokerage account. Each node carries a real position
(quantity, cost basis) and full Buy/Sell/Income history in Masttro, but as
`is_account = false` nodes they are not canonical accounts, so the normal
sync drops them.

These nodes are frozen — future trades of the same or new assets flow through
the live IBKR account, and the existing shares will NOT also appear in the
IBKR feed (confirmed with ops), so each share is represented exactly once.

We do NOT want each security surfaced as its own account, so the nodes are
folded into the trust's IBKR account via tracker.node_remap: their
Holdings/Transactions rows are rewritten to the IBKR account nodeId before
upsert. The same remap runs in scripts/sync_masttro_daily.py so the holdings
are carried forward under IBKR each day.

What this does
--------------
1. Reverses any prior run that wrote rows under the equity nodes themselves:
   un-marks those nodes canonical and deletes their position_snapshot /
   transaction_log rows.
2. Backfills ~5 years of month-end positions (Holdings, historicalMonths=12
   across yearly anchors) and transactions (period=4 across yearly anchors)
   for the equity nodes only, REMAPPED onto the IBKR account. IBKR's own
   rows are left untouched (we filter to equity-node rows before upsert).

Idempotent. Defaults to a dry run; pass --apply to write.

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
from tracker.node_remap import DIRECT_EQUITY_NODE_REMAP, apply_node_remap
from tracker.sync_supabase import (
    upsert_positions,
    upsert_securities,
    upsert_transactions,
)

FAMILY_NODE = "102_93362"   # Dyne Family (AU) GWM nodeId
CLIENT_ID = 7693
CCY = "AUD"
YEARS = 5

EQUITY_NODES = sorted(DIRECT_EQUITY_NODE_REMAP)
IBKR_TARGETS = sorted(set(DIRECT_EQUITY_NODE_REMAP.values()))


def _ym(d: dt.date) -> str:
    return d.strftime("%Y%m")


def _minus_months(d: dt.date, months: int) -> dt.date:
    y, m = d.year, d.month - months
    while m <= 0:
        m += 12
        y -= 1
    return dt.date(y, m, 1)


def _yearly_anchors(today: dt.date, years: int) -> list[str]:
    return [_ym(_minus_months(today, 12 * i)) for i in range(years)]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true",
                    help="write to the DB (default: dry run)")
    args = ap.parse_args()
    mode = "APPLY" if args.apply else "DRY-RUN"

    today = dt.date.today()
    anchors = _yearly_anchors(today, YEARS)
    print(f"[{mode}] AU direct-equity backfill -> IBKR accounts")
    print(f"  {len(EQUITY_NODES)} equity nodes -> {IBKR_TARGETS}")
    print(f"  anchors={anchors}")

    if not args.apply:
        print("[DRY-RUN] would un-mark the equity nodes canonical, delete any "
              "rows written under them, then reload positions+transactions "
              "remapped onto the IBKR account. Re-run with --apply.")
        return 0

    masttro = MasttroClient()
    conn = connect()
    try:
        # 1. Reverse any prior run: un-mark canonical + delete equity-node rows.
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE entity SET is_canonical_account = FALSE WHERE node_id = ANY(%s)",
                (EQUITY_NODES,),
            )
            cur.execute(
                "DELETE FROM position_snapshot WHERE account_node_id = ANY(%s)",
                (EQUITY_NODES,),
            )
            pos_del = cur.rowcount
            cur.execute(
                "DELETE FROM transaction_log WHERE account_node_id = ANY(%s)",
                (EQUITY_NODES,),
            )
            txn_del = cur.rowcount
        conn.commit()
        print(f"  cleaned prior run: positions deleted={pos_del} txns deleted={txn_del}")

        # 2. Reload equity-node rows REMAPPED onto IBKR. Filter to equity-node
        #    rows first so IBKR's own holdings/txns are left untouched.
        pos_total = txn_total = 0
        for anchor in anchors:
            hold = masttro.get_cached_or_fetch(
                f"Holdings/{CLIENT_ID}",
                {"ccy": CCY, "yearMonth": anchor, "historicalMonths": 12,
                 "investmentVehicle": FAMILY_NODE},
                descriptor=f"backfill_sub{FAMILY_NODE}_aud_{anchor}_h12",
            ) or []
            txns = masttro.get_cached_or_fetch(
                f"Transactions/{CLIENT_ID}",
                {"ccy": CCY, "yearMonth": anchor, "period": 4,
                 "investmentVehicle": FAMILY_NODE},
                descriptor=f"backfill_sub{FAMILY_NODE}_aud_{anchor}_p4",
            ) or []
            hold_eq = apply_node_remap(
                [h for h in hold if h.get("nodeId") in DIRECT_EQUITY_NODE_REMAP])
            txns_eq = apply_node_remap(
                [t for t in txns if t.get("nodeId") in DIRECT_EQUITY_NODE_REMAP])
            upsert_securities(conn, hold_eq, txns_eq)
            ps = upsert_positions(conn, hold_eq, IBKR_TARGETS, reporting_ccy=CCY)
            ts = upsert_transactions(conn, txns_eq, IBKR_TARGETS, reporting_ccy=CCY)
            pos_total += ps["inserted"]
            txn_total += ts["submitted"]
            print(f"  anchor {anchor}: positions+={ps['inserted']:4} "
                  f"txns+={ts['submitted']:4}")

        log_sync(conn, "au_direct_equity_backfill", FAMILY_NODE,
                 f"nodes={len(EQUITY_NODES)} -> ibkr positions={pos_total} "
                 f"txns={txn_total}", pos_total)
        print(f"\n  DONE — positions written={pos_total} txns submitted={txn_total}")
    finally:
        conn.close()
        masttro.report()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
