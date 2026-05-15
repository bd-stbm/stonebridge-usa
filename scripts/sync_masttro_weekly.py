"""Weekly Masttro sync: GWM tree refresh + attribution rebuild.

Captures account-structure changes (new/closed accounts, ownership flips).
One /GWM call per tenant; idempotent UPSERT. The daily sync depends on
canonical-account flags maintained here — run this before the first daily
sync, then once a week.

Env: MASTTRO_API_KEY, MASTTRO_API_SECRET, SUPABASE_DB_URL.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from client import MasttroClient
from tracker.db import connect, log_sync
from tracker.families import FAMILIES
from tracker.sync_supabase import rebuild_attribution, upsert_gwm


def main() -> int:
    print("Weekly Masttro sync — GWM tree refresh")
    masttro = MasttroClient()
    conn = connect()

    try:
        # Tenant-wide GWM (all families under one client_id come back in one call).
        client_id = FAMILIES[0]["client_id"]
        gwm = masttro.get(f"GWM/{client_id}", {"ccy": "AUD"}) or []
        masttro.save_response(f"GWM/{client_id}", gwm, descriptor="weekly_aud")
        print(f"  fetched {len(gwm)} nodes")

        n_entities = upsert_gwm(conn, gwm)
        n_attr = rebuild_attribution(conn, root_node_id=f"0_{client_id}")
        print(f"  upserted {n_entities} entities, rebuilt {n_attr} attributions")

        log_sync(conn, "weekly_sync", None,
                 f"entities={n_entities} attribution_rows={n_attr}",
                 n_entities)
    finally:
        conn.close()
        masttro.report()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
