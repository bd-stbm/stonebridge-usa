"""One-off backfill of index_price_history from yfinance.

Run once after migration 007 to populate ~5y of daily closes for every
ticker in index_definition. Re-runnable.

Env: SUPABASE_DB_URL.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tracker.db import connect, log_sync
from tracker.sync_indices import backfill_indices


def main() -> int:
    print("Backfilling index_price_history (5y)")
    conn = connect()
    try:
        stats = backfill_indices(conn, years=5)
        total = sum(stats.values())
        log_sync(
            conn,
            "index_backfill",
            "all",
            ", ".join(f"{k}={v}" for k, v in stats.items()),
            total,
        )
        print(f"Done — {total} total rows across {len(stats)} indices")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
