"""Daily Masttro sync: Holdings + Transactions + DataFeedUpdates per family.

Runs every business day from the GitHub Actions cron. Pulls current-month
Holdings (which the API returns as current-day positions for the current month)
and YTD Transactions, then upserts into Supabase. Idempotent — re-running
overwrites today's snapshot and dedupes transactions.

Env: MASTTRO_API_KEY, MASTTRO_API_SECRET, SUPABASE_DB_URL.
"""

from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from client import MasttroClient
from tracker.db import connect, log_sync
from tracker.families import FAMILIES
from tracker.sync_supabase import (
    canonical_accounts_under,
    upsert_positions,
    upsert_securities,
    upsert_transactions,
)


def main() -> int:
    today = dt.date.today()
    yyyymm_now = today.strftime("%Y%m")
    print(f"Daily Masttro sync — date={today.isoformat()} yearMonth={yyyymm_now}")

    masttro = MasttroClient()
    conn = connect()
    summary = []

    try:
        for fam in FAMILIES:
            label = fam["label"]
            family_node = fam["node_id"]
            client_id = fam["client_id"]
            ccy = fam["reporting_ccy"]
            print(f"\n--- {label} ({family_node}) ---")

            accounts = canonical_accounts_under(conn, family_node)
            if not accounts:
                print(f"  WARN: no canonical accounts under {family_node} — "
                      "run the weekly GWM sync first.")
                summary.append({"family": label, "skipped": True})
                continue
            print(f"  {len(accounts)} canonical accounts in scope")

            holdings = masttro.get(
                f"Holdings/{client_id}",
                {"ccy": ccy, "yearMonth": yyyymm_now,
                 "investmentVehicle": family_node},
            ) or []
            masttro.save_response(
                f"Holdings/{client_id}", holdings,
                descriptor=f"daily_{family_node}_{ccy.lower()}_{yyyymm_now}",
            )

            txns = masttro.get(
                f"Transactions/{client_id}",
                {"ccy": ccy, "yearMonth": yyyymm_now, "period": 1,  # YTD
                 "investmentVehicle": family_node},
            ) or []
            masttro.save_response(
                f"Transactions/{client_id}", txns,
                descriptor=f"daily_{family_node}_{ccy.lower()}_{yyyymm_now}_p1",
            )

            feeds = masttro.get(
                f"DataFeedUpdates/{client_id}",
                {"yearMonth": yyyymm_now, "investmentVehicle": family_node},
            ) or []

            sec_n = upsert_securities(conn, holdings, txns)
            pos_stats = upsert_positions(conn, holdings, accounts, reporting_ccy=ccy)
            txn_stats = upsert_transactions(conn, txns, accounts, reporting_ccy=ccy)

            stale = []
            cutoff = (today - dt.timedelta(days=2)).isoformat()
            for f in feeds or []:
                last = (f.get("lastUpdate") or "")[:10]
                if last and last < cutoff:
                    stale.append(f.get("alias") or f.get("nodeId"))
            if stale:
                print(f"  STALE FEEDS (>2d behind): {stale}")

            log_sync(conn, "daily_sync", family_node,
                     f"positions={pos_stats['inserted']} "
                     f"txns_submitted={txn_stats['submitted']} "
                     f"securities={sec_n} stale_feeds={len(stale)}",
                     pos_stats["inserted"])
            summary.append({
                "family": label,
                "accounts": len(accounts),
                "positions": pos_stats["inserted"],
                "txns_submitted": txn_stats["submitted"],
                "securities": sec_n,
                "stale_feeds": len(stale),
            })

    finally:
        conn.close()
        masttro.report()

    print("\n=== Summary ===")
    for s in summary:
        print(f"  {s}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
