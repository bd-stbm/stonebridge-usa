"""Build the tracker SQLite DB for one family. Runs end-to-end:

  1. Schema
  2. Ingest GWM + attribution
  3. Find canonical investment accounts under the family
  4. Ingest security master, positions, transactions
  5. Refresh prices (yfinance + OpenFIGI)

  Usage: python scripts/12_build_tracker_db.py
  (Family is hard-coded to Dyne Family US for now — flip the constants below.)
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.stdout.reconfigure(encoding="utf-8")

from tracker import DEFAULT_DB_PATH
from tracker.ingest import build_db_for_family
from tracker.enrich import refresh_pricing


FAMILY_NODE_ID = "102_93356"
FAMILY_LABEL = "Dyne Family US"


def main() -> int:
    print(f"Building tracker DB at: {DEFAULT_DB_PATH}")
    print(f"Family: {FAMILY_LABEL} (nodeId {FAMILY_NODE_ID})\n")

    result = build_db_for_family(FAMILY_NODE_ID, FAMILY_LABEL)
    print("\n=== Ingestion ===")
    for k, v in result.items():
        print(f"  {k}: {v}")

    print("\n=== Pricing refresh (yfinance + OpenFIGI) ===")
    refresh = refresh_pricing(scope=FAMILY_NODE_ID)
    for k, v in refresh.items():
        print(f"  {k}: {v}")

    print(f"\nDone. DB ready at {DEFAULT_DB_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
