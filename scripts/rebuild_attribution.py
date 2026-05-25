"""One-off: re-run rebuild_attribution against Supabase.

Use when the attribution rule changes (e.g. adding the retirement-grouping
predicate) and you don't want to wait for the next Sunday weekly sync.
Operates on the existing public.entity rows — no Masttro API calls.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from tracker.db import connect  # noqa: E402
from tracker.families import FAMILIES  # noqa: E402
from tracker.sync_supabase import rebuild_attribution  # noqa: E402


def main() -> int:
    with connect() as conn:
        # Same root the weekly sync uses — the tenant's top-level client id.
        client_id = FAMILIES[0]["client_id"]
        n = rebuild_attribution(conn, root_node_id=f"0_{client_id}")
        print(f"Rebuilt attribution for {n} nodes (root=0_{client_id})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
