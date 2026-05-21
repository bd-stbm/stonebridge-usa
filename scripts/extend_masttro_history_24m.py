"""Pull an additional 12 months of Masttro Holdings (the year before the
existing 12-month backfill) and upsert into position_snapshot.

Masttro caps historicalMonths at 12 per call (per CLAUDE.md). So this
makes a second Holdings call with yearMonth pointing 12 months before the
current month, which gives May 2024 → May 2025 month-ends on top of the
already-loaded May 2025 → May 2026 window. Net: 24 months of history,
enough to give the dashboard's 1Y return a real anchor.

Idempotent — upserts on (snapshot_date, account_node_id, security_id).
No Transactions call: we already have flows from May 2025 onwards via the
original backfill, which is the window the 1Y return needs.

Env: MASTTRO_API_KEY, MASTTRO_API_SECRET, SUPABASE_POOL_URL / SUPABASE_DB_URL.
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
)


HIST_MONTHS = 12  # Masttro caps at 12.


def _yearmonth_n_months_ago(today: dt.date, n: int) -> int:
    """Return YYYYMM for the month that is `n` months before today's month."""
    y = today.year
    m = today.month - n
    while m <= 0:
        m += 12
        y -= 1
    return y * 100 + m


def main() -> int:
    today = dt.date.today()
    # The anchor month for the older 12-month window is today's month minus
    # 12. With historicalMonths=12 this covers (today − 24m) → (today − 12m)
    # — exactly the gap not covered by the original 12-month backfill.
    older_yyyymm = _yearmonth_n_months_ago(today, 12)
    print(
        f"Older-12m backfill — yearMonth={older_yyyymm}, "
        f"historicalMonths={HIST_MONTHS}"
    )

    client = MasttroClient()
    conn = connect()
    family_roots = [f["node_id"] for f in FAMILIES]

    try:
        for fam in FAMILIES:
            label = fam["label"]
            node = fam["node_id"]
            client_id = fam["client_id"]
            ccy = fam["reporting_ccy"]
            print(f"\n--- {label} ({node}) ---")

            accounts = canonical_accounts_under(conn, node, family_roots)
            print(f"  {len(accounts)} canonical accounts in scope")
            if not accounts:
                print("  WARN: no accounts. Run the weekly GWM sync first.")
                continue

            holdings = client.get(
                f"Holdings/{client_id}",
                {
                    "ccy": ccy,
                    "yearMonth": older_yyyymm,
                    "historicalMonths": HIST_MONTHS,
                    "investmentVehicle": node,
                },
            ) or []
            client.save_response(
                f"Holdings/{client_id}",
                holdings,
                descriptor=(
                    f"older12m_sub{node}_{ccy.lower()}_"
                    f"{older_yyyymm}_h{HIST_MONTHS}"
                ),
            )
            print(f"  Holdings: {len(holdings):,} rows fetched")

            # Update security master from anything new that appears in this
            # window. Then upsert positions.
            sec_n = upsert_securities(conn, holdings, [])
            pos_stats = upsert_positions(
                conn, holdings, accounts, reporting_ccy=ccy,
            )
            log_sync(
                conn,
                "masttro_older12m_backfill",
                node,
                f"positions={pos_stats['inserted']} securities={sec_n}",
                pos_stats["inserted"],
            )
            print(
                f"  Upserts: positions={pos_stats['inserted']:,} "
                f"securities={sec_n}"
            )

    finally:
        conn.close()
        client.report()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
