"""One-off: re-pull 24 months of Holdings per family to populate the
newly-canonical shared-vehicle reflections (Modyl LP extras within Dyne,
Dendell LLC across all 3 families) into historical position_snapshot rows.

The daily sync only pulls the current month, so historical month-ends
still carry the pre-fix data. This script issues two Holdings calls per
family — current-month-anchored with historicalMonths=12, plus the same
anchored 12 months earlier — covering ~24 months of month-ends. Idempotent
on (snapshot_date, account_node_id, security_id).

Cost: 3 families × 2 calls = 6 Holdings calls. Well within the 50-call
session budget.

Env: MASTTRO_API_KEY, MASTTRO_API_SECRET, SUPABASE_POOL_URL / SUPABASE_DB_URL.
"""

from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from client import MasttroClient  # noqa: E402
from tracker.db import connect, log_sync  # noqa: E402
from tracker.families import FAMILIES  # noqa: E402
from tracker.sync_supabase import (  # noqa: E402
    canonical_accounts_under,
    upsert_positions,
    upsert_securities,
)


HIST_MONTHS = 12  # Masttro caps at 12.


def _yearmonth_n_months_ago(today: dt.date, n: int) -> int:
    y = today.year
    m = today.month - n
    while m <= 0:
        m += 12
        y -= 1
    return y * 100 + m


def main() -> int:
    today = dt.date.today()
    recent_ym = today.year * 100 + today.month
    older_ym = _yearmonth_n_months_ago(today, HIST_MONTHS)

    print(
        f"Shared-vehicle backfill — anchors yearMonth={recent_ym} "
        f"and yearMonth={older_ym} (historicalMonths={HIST_MONTHS} each)"
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

            for ym, label_tag in [(recent_ym, "recent"), (older_ym, "older")]:
                holdings = client.get(
                    f"Holdings/{client_id}",
                    {
                        "ccy": ccy,
                        "yearMonth": ym,
                        "historicalMonths": HIST_MONTHS,
                        "investmentVehicle": node,
                    },
                ) or []
                client.save_response(
                    f"Holdings/{client_id}",
                    holdings,
                    descriptor=(
                        f"shared_backfill_{label_tag}_sub{node}_"
                        f"{ccy.lower()}_{ym}_h{HIST_MONTHS}"
                    ),
                )
                print(f"  {label_tag} (yearMonth={ym}): {len(holdings):,} rows")

                sec_n = upsert_securities(conn, holdings, [])
                pos_stats = upsert_positions(
                    conn, holdings, accounts, reporting_ccy=ccy,
                )
                log_sync(
                    conn,
                    "shared_vehicle_backfill",
                    node,
                    f"window={label_tag} positions={pos_stats['inserted']} securities={sec_n}",
                    pos_stats["inserted"],
                )
                print(
                    f"    upserted positions={pos_stats['inserted']:,} "
                    f"securities={sec_n}"
                )

    finally:
        conn.close()
        client.report()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
