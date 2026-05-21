"""One-off: 24-month Holdings + Transactions backfill for new families.

Targets families that weren't in `FAMILIES` when the original Dyne
backfill was done (Markiles, Miller). For each, makes 4 API calls:

  - Holdings yearMonth = current month,  historicalMonths = 12
      → covers (today − 12m) → current month, month-end snapshots
  - Holdings yearMonth = (today − 12m),  historicalMonths = 12
      → covers (today − 24m) → (today − 12m), month-end snapshots
  - Transactions yearMonth = current month, period = 4 (12M)
      → covers (today − 12m) → today
  - Transactions yearMonth = (today − 12m), period = 4 (12M)
      → covers (today − 24m) → (today − 12m)

Idempotent: position_snapshot is keyed on (snapshot_date,
account_node_id, security_id); transaction_log dedupes on
(transaction_date, account_node_id, security_id, transaction_type,
net_amount_reporting). Safe to re-run.

Hardcoded TARGET_NODES below — edit if you add more families and want
to backfill them in one go. Defaults skip Dyne (already backfilled
via the original 11_us_families_backfill + extend_masttro_history_24m
pair).

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


HIST_MONTHS = 12   # Masttro per-call cap
PERIOD_12M = 4

# GWM nodeIds of the families to backfill. Keep this narrow so re-runs
# don't burn API calls on already-loaded families.
TARGET_NODES = {"102_93361", "102_93360"}  # Markiles, Miller


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
    older_ym = _yearmonth_n_months_ago(today, 12)

    print(
        f"Backfill — recent_ym={recent_ym}, older_ym={older_ym}, "
        f"targets={sorted(TARGET_NODES)}"
    )

    masttro = MasttroClient()
    conn = connect()
    summary = []
    family_roots = [f["node_id"] for f in FAMILIES]

    try:
        for fam in FAMILIES:
            node = fam["node_id"]
            if node not in TARGET_NODES:
                continue
            label = fam["label"]
            client_id = fam["client_id"]
            ccy = fam["reporting_ccy"]
            print(f"\n--- {label} ({node}) ---")

            accounts = canonical_accounts_under(conn, node, family_roots)
            if not accounts:
                print(
                    f"  WARN: no accounts under {node} — "
                    "run the weekly GWM sync first."
                )
                summary.append({"family": label, "skipped": True})
                continue
            print(f"  {len(accounts)} canonical accounts in scope")

            pos_total = 0
            txn_submitted = 0
            sec_total = 0

            for yyyymm in (recent_ym, older_ym):
                holdings = masttro.get(
                    f"Holdings/{client_id}",
                    {
                        "ccy": ccy,
                        "yearMonth": yyyymm,
                        "historicalMonths": HIST_MONTHS,
                        "investmentVehicle": node,
                    },
                ) or []
                masttro.save_response(
                    f"Holdings/{client_id}",
                    holdings,
                    descriptor=(
                        f"backfill_sub{node}_{ccy.lower()}_"
                        f"{yyyymm}_h{HIST_MONTHS}"
                    ),
                )
                print(f"  Holdings ym={yyyymm}: {len(holdings):,} rows")

                txns = masttro.get(
                    f"Transactions/{client_id}",
                    {
                        "ccy": ccy,
                        "yearMonth": yyyymm,
                        "period": PERIOD_12M,
                        "investmentVehicle": node,
                    },
                ) or []
                masttro.save_response(
                    f"Transactions/{client_id}",
                    txns,
                    descriptor=(
                        f"backfill_sub{node}_{ccy.lower()}_"
                        f"{yyyymm}_p{PERIOD_12M}"
                    ),
                )
                print(f"  Transactions ym={yyyymm}: {len(txns):,} rows")

                sec_n = upsert_securities(conn, holdings, txns)
                pos_stats = upsert_positions(
                    conn, holdings, accounts, reporting_ccy=ccy,
                )
                txn_stats = upsert_transactions(
                    conn, txns, accounts, reporting_ccy=ccy,
                )
                sec_total += sec_n
                pos_total += pos_stats["inserted"]
                txn_submitted += txn_stats["submitted"]
                print(
                    f"    upserts: positions={pos_stats['inserted']:,} "
                    f"txns_submitted={txn_stats['submitted']:,} "
                    f"securities={sec_n}"
                )

            log_sync(
                conn,
                "backfill_new_families",
                node,
                f"positions={pos_total} txns_submitted={txn_submitted} "
                f"securities={sec_total}",
                pos_total,
            )
            summary.append({
                "family": label,
                "accounts": len(accounts),
                "positions": pos_total,
                "txns_submitted": txn_submitted,
                "securities": sec_total,
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
