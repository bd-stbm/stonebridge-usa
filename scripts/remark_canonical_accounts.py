"""One-off: re-mark canonical accounts using the shared-vehicle rule.

The rule now keeps ALL reflections of shared-multi-trust vehicles as
canonical (Modyl LP per-trust slices within Dyne, Dendell LLC per-family
slices across families). Without this, fingerprint dedup picks just one
reflection and the other slices' positions never make it into
position_snapshot.

Run after deploying the sync_supabase.py change; the weekly sync will
re-run this Sunday anyway, but you don't want to wait.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from tracker.db import connect  # noqa: E402
from tracker.sync_supabase import mark_canonical_accounts  # noqa: E402


def main() -> int:
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) AS n FROM entity WHERE is_canonical_account = TRUE"
        )
        before = cur.fetchone()["n"]
        mark_canonical_accounts(conn)
        cur.execute(
            "SELECT COUNT(*) AS n FROM entity WHERE is_canonical_account = TRUE"
        )
        after = cur.fetchone()["n"]
    print(f"is_canonical_account=TRUE  before={before}  after={after}  diff={after - before:+d}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
