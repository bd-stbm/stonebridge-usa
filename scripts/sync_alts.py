"""Alt (non-listed) sync: Holdings + cef + GWM per family -> alt_position_snapshot.

Writes the ownership-weighted non-listed book (alts, direct PE/RE, business,
loans, collections, non-canonical cash) at the Security -> Vehicle/SPV -> Entity
grain. See docs/all_assets_integration_design.md and tracker/alt_attribution.py.

Runs AFTER the listed daily sync (the attribution engine reads the existing-
entity set from v_latest_positions). Idempotent: each (snapshot_date, family) is
rebuilt clean, so vanished holdings drop out.

Usage:
  python scripts/sync_alts.py                 # current month -> snapshot_date = today
  python scripts/sync_alts.py --year-month 202603   # backfill -> snapshot_date = month-end
  python scripts/sync_alts.py --dry-run        # compute + reconcile, no DB write

Env: MASTTRO_API_KEY, MASTTRO_API_SECRET, SUPABASE_DB_URL.
"""

from __future__ import annotations

import argparse
import calendar
import datetime as dt
import glob
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from client import MasttroClient
from tracker.alt_attribution import ROW_FIELDS, compute_alt_rows
from tracker.db import connect, log_sync
from tracker.families import FAMILIES
from tracker.sync_supabase import upsert_securities

UPSERT_SQL = """
INSERT INTO alt_position_snapshot
  (snapshot_date, security_id, holding_node_id, sub_client_node_id, sub_client_alias,
   entity_node_id, entity_alias, vehicle_node_id, vehicle_alias, ownership_pct,
   full_value_reporting, mv_reporting, reporting_ccy, value_source, valuation_date,
   entity_rollup)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (snapshot_date, holding_node_id, security_id) DO UPDATE SET
  sub_client_node_id   = EXCLUDED.sub_client_node_id,
  sub_client_alias     = EXCLUDED.sub_client_alias,
  entity_node_id       = EXCLUDED.entity_node_id,
  entity_alias         = EXCLUDED.entity_alias,
  vehicle_node_id      = EXCLUDED.vehicle_node_id,
  vehicle_alias        = EXCLUDED.vehicle_alias,
  ownership_pct        = EXCLUDED.ownership_pct,
  full_value_reporting = EXCLUDED.full_value_reporting,
  mv_reporting         = EXCLUDED.mv_reporting,
  reporting_ccy        = EXCLUDED.reporting_ccy,
  value_source         = EXCLUDED.value_source,
  valuation_date       = EXCLUDED.valuation_date,
  entity_rollup        = EXCLUDED.entity_rollup
"""


def snapshot_date_for(year_month: str, today: dt.date) -> str:
    """Current month -> today (Holdings returns current-day positions). Past
    month -> that month's last calendar day (Holdings returns month-end)."""
    y, m = int(year_month[:4]), int(year_month[4:6])
    if (y, m) == (today.year, today.month):
        return today.isoformat()
    return dt.date(y, m, calendar.monthrange(y, m)[1]).isoformat()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--year-month", default=dt.date.today().strftime("%Y%m"),
                    help="YYYYMM to pull (default current month)")
    ap.add_argument("--dry-run", action="store_true",
                    help="compute + print totals, no DB write")
    ap.add_argument("--from-saved", action="store_true",
                    help="load the latest saved responses/ payloads instead of "
                         "calling the API (zero API calls)")
    args = ap.parse_args()

    today = dt.date.today()
    ym = args.year_month
    snap = snapshot_date_for(ym, today)
    print(f"Alt sync — yearMonth={ym} snapshot_date={snap} "
          f"{'(DRY RUN)' if args.dry_run else ''}")

    masttro = None if args.from_saved else MasttroClient()
    conn = connect()
    summary = []
    try:
        for fam in FAMILIES:
            label, family_node = fam["label"], fam["node_id"]
            client_id, ccy = fam["client_id"], fam["reporting_ccy"]
            print(f"\n--- {label} ({family_node}) {ccy} ---")
            desc = f"alts_{family_node}_{ccy.lower()}_{ym}"

            def pull(endpoint: str, params: dict):
                """Fetch from the API (and save), or load the latest saved
                responses/ file for this endpoint+descriptor when --from-saved."""
                if args.from_saved:
                    ep = endpoint.split("/")[0]
                    hits = sorted(glob.glob(f"responses/{ep}-{client_id}_*{desc}.json"))
                    if not hits:
                        raise FileNotFoundError(
                            f"--from-saved: no saved {ep} for {desc}. Run a "
                            f"normal/dry-run pull first.")
                    return json.load(open(hits[-1], encoding="utf-8"))
                data = masttro.get(endpoint, params) or []
                masttro.save_response(endpoint, data, descriptor=desc)
                return data

            gwm = pull(f"GWM/{client_id}", {"ccy": ccy})
            holdings = pull(f"Holdings/{client_id}",
                            {"ccy": ccy, "yearMonth": ym, "investmentVehicle": family_node})
            cef = pull(f"cef/{client_id}",
                       {"ccy": ccy, "yearMonth": ym, "period": 1,
                        "investmentVehicle": family_node})

            upsert_securities(conn, holdings)
            rows = compute_alt_rows(
                conn, holdings=holdings, cef=cef, gwm_payload=gwm,
                sub_client_node_id=family_node, sub_client_alias=label_to_alias(conn, family_node),
                reporting_ccy=ccy, snapshot_date=snap)

            total = sum(r[ROW_FIELDS.index("mv_reporting")] for r in rows)
            spvs = sum(1 for r in rows if r[ROW_FIELDS.index("vehicle_alias")])
            print(f"  {len(rows)} rows  net {total/1e6:.2f}M  ({spvs} with an SPV)")

            if not args.dry_run and rows:
                with conn.cursor() as cur:
                    cur.execute(
                        "DELETE FROM alt_position_snapshot "
                        "WHERE snapshot_date = %s AND sub_client_node_id = %s",
                        (snap, family_node))
                    cur.executemany(UPSERT_SQL, rows)
                conn.commit()
                log_sync(conn, "alt_sync", family_node,
                         f"rows={len(rows)} net={total:.0f} snapshot={snap}", len(rows))
            summary.append({"family": label, "rows": len(rows),
                            "net_m": round(total / 1e6, 2), "spv_rows": spvs})
    finally:
        conn.close()
        if masttro is not None:
            masttro.report()

    print("\n=== Summary ===")
    for s in summary:
        print(f"  {s}")
    return 0


def label_to_alias(conn, family_node: str) -> str:
    """The sub_client_alias as stored on the tree (the Masttro GWM name, with
    parens etc.) — the engine and the dashboard both key on this exact string."""
    with conn.cursor() as cur:
        cur.execute("SELECT alias, name FROM entity WHERE node_id = %s", (family_node,))
        r = cur.fetchone()
    return (r["alias"] or r["name"]) if r else family_node


if __name__ == "__main__":
    raise SystemExit(main())
