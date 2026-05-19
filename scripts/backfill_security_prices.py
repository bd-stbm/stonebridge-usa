"""One-off backfill of security_price_history from yfinance.

Run once after migration 011 to populate ~5y of daily closes for every
held public security. Re-runnable. Took ~3-4 minutes locally for the
Dyne universe (~30-40 tickers).

Env: SUPABASE_DB_URL.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tracker.db import connect, log_sync
from tracker.sync_security_prices import backfill_security_prices


def main() -> int:
    print("Backfilling security_price_history (5y)")
    conn = connect()
    try:
        stats = backfill_security_prices(conn, years=5)
        total = sum(stats.values())
        log_sync(
            conn,
            "security_price_backfill",
            "all",
            f"tickers={len(stats)} rows={total}",
            total,
        )
        print(f"\nDone — {total} total rows across {len(stats)} tickers")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
