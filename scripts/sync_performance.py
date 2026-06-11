"""Performance sync: Masttro /Performance per family per period -> performance_snapshot.

Stores the period return components (marketValueInitial/marketValue/flows/totalPL/
irr/twr) for EVERY holding across all asset classes, with each holding rolled up
to its entity (nearest-existing-entity walk — same grain as the net-worth view).
This backs a blended ALL-ASSET return (listed + non-listed) computed as
modified-Dietz over the summed components. See migration 040 + the design doc.

Usage:
  python scripts/sync_performance.py                 # current month, default periods
  python scripts/sync_performance.py --year-month 202603
  python scripts/sync_performance.py --from-saved    # reuse saved responses/ pulls
  python scripts/sync_performance.py --dry-run        # compute + reconcile, no write

Periods: 0 MTD, 1 YTD, 2 3M, 3 6M, 4 12M (default 0,1,3,4).
Env: MASTTRO_API_KEY, MASTTRO_API_SECRET, SUPABASE_DB_URL.
"""
from __future__ import annotations

import argparse
import datetime as dt
import glob
import json
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from client import MasttroClient
from tracker.db import connect, log_sync
from tracker.families import FAMILIES
from tracker.ingest import ROOT_NODE_ID

DEFAULT_PERIODS = [0, 1, 3, 4]

INSERT_SQL = """
INSERT INTO performance_snapshot
  (pull_date, period, year_month, sub_client_node_id, sub_client_alias, node_id,
   security_id, asset_class, security_type, entity_node_id, entity_alias,
   vehicle_alias, market_value_initial, market_value, deposits, withdrawals,
   transfer_in_out, realized_gl, unrealized_gl, income, total_pl, avg_cap_base,
   irr, twr, reporting_ccy, initial_date, as_of_date)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""


def f(x):
    try:
        return float(str(x).replace(",", ""))
    except (TypeError, ValueError):
        return None


def iso(s):
    """YYYYMMDD or YYYY-MM-DD -> date string, else None."""
    if not s:
        return None
    s = str(s).strip()
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:]}"
    return s[:10] if len(s) >= 10 else None


def load_family_attribution(conn, family_node):
    """Build the per-node entity resolver for one family (nearest-existing
    walk + branch fallback), mirroring tracker/alt_attribution."""
    cur = conn.cursor()
    cur.execute("SELECT node_id, parent_node_id FROM entity")
    parent = {r["node_id"]: r["parent_node_id"] for r in cur.fetchall()}
    cur.execute("SELECT node_id, trust_node_id, trust_alias, vehicle_alias, family_path "
                "FROM entity_attribution")
    attr = {r["node_id"]: dict(r) for r in cur.fetchall()}
    cur.execute("SELECT DISTINCT trust_alias FROM v_latest_positions "
                "WHERE sub_client_node_id=%s AND trust_alias IS NOT NULL", (family_node,))
    existing = {r["trust_alias"] for r in cur.fetchall()}
    # branch (family_path L3) per trust_alias, most common
    br = defaultdict(lambda: defaultdict(int))
    for n, a in attr.items():
        if a.get("trust_alias") and a.get("family_path"):
            parts = [p.strip() for p in a["family_path"].split(">")]
            br[a["trust_alias"]][parts[2] if len(parts) > 2 else "(direct)"] += 1
    branch_of = {t: max(d, key=d.get) for t, d in br.items()}

    def resolve(node):
        """-> (entity_node, entity_alias, vehicle_alias)."""
        cur_id, ent_node, ent_alias = node, None, None
        for _ in range(15):
            ta = attr.get(cur_id, {}).get("trust_alias")
            if ta in existing:
                ent_node = attr[cur_id].get("trust_node_id")
                ent_alias = ta
                break
            par = parent.get(cur_id)
            if not par or par == ROOT_NODE_ID or par == "_":
                break
            cur_id = par
        if ent_alias is None:  # orphan -> branch fallback
            ent_alias = branch_of.get(attr.get(node, {}).get("trust_alias"))
        veh = attr.get(node, {}).get("vehicle_alias") or attr.get(node, {}).get("trust_alias")
        if veh == ent_alias:
            veh = None
        return ent_node, ent_alias, veh

    return resolve


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--year-month", default=dt.date.today().strftime("%Y%m"))
    ap.add_argument("--periods", default=",".join(map(str, DEFAULT_PERIODS)))
    ap.add_argument("--from-saved", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    periods = [int(p) for p in args.periods.split(",")]
    ym = args.year_month
    pull_date = dt.date.today().isoformat()
    print(f"Performance sync — yearMonth={ym} periods={periods} pull_date={pull_date} "
          f"{'(DRY RUN)' if args.dry_run else ''}")

    masttro = None if args.from_saved else MasttroClient()
    conn = connect()
    summary = []
    try:
        for fam in FAMILIES:
            label, family_node = fam["label"], fam["node_id"]
            client_id, ccy = fam["client_id"], fam["reporting_ccy"]
            print(f"\n--- {label} ({family_node}) {ccy} ---")
            resolve = load_family_attribution(conn, family_node)
            # Store the GWM sub_client_alias ("Dyne Family (US)" with parens),
            # NOT the FAMILIES label — every other table + the web filter key on it.
            with conn.cursor() as cur:
                cur.execute("SELECT alias, name FROM entity WHERE node_id=%s", (family_node,))
                _r = cur.fetchone()
            sub_alias = (_r["alias"] or _r["name"]) if _r else label

            for period in periods:
                desc = f"perf_{family_node}_{ccy.lower()}_{ym}_p{period}"
                if args.from_saved:
                    hits = sorted(glob.glob(f"responses/Performance-{client_id}_*{desc}.json"))
                    if not hits:
                        print(f"  p{period}: no saved pull — skipping")
                        continue
                    rows = json.load(open(hits[-1], encoding="utf-8"))
                else:
                    rows = masttro.get(
                        f"Performance/{client_id}",
                        {"ccy": ccy, "yearMonth": ym, "period": period,
                         "investmentVehicle": family_node}) or []
                    masttro.save_response(f"Performance/{client_id}", rows, descriptor=desc)

                out = []
                for r in rows:
                    e_node, e_alias, veh = resolve(r.get("nodeId"))
                    out.append((
                        pull_date, period, ym, family_node, sub_alias, r.get("nodeId"),
                        r.get("securityId"), r.get("assetClass"), r.get("securityType"),
                        e_node, e_alias, veh,
                        f(r.get("marketValueInitial")), f(r.get("marketValue")),
                        f(r.get("deposits")), f(r.get("withdrawals")), f(r.get("transferInOut")),
                        f(r.get("periodRealizedGL")), f(r.get("periodUnrealizedGL")),
                        f(r.get("income")), f(r.get("totalPL")), f(r.get("avgCapBase")),
                        f(r.get("irr")), f(r.get("twr")), ccy,
                        iso(r.get("initialDate")), iso(r.get("date")),
                    ))
                end = sum(x[13] or 0 for x in out) / 1e6
                print(f"  p{period}: {len(out)} holdings  end NAV {end:.2f}M")
                if not args.dry_run and out:
                    with conn.cursor() as cur:
                        cur.execute("DELETE FROM performance_snapshot WHERE pull_date=%s "
                                    "AND period=%s AND sub_client_node_id=%s",
                                    (pull_date, period, family_node))
                        cur.executemany(INSERT_SQL, out)
                    conn.commit()
                    log_sync(conn, "performance_sync", family_node,
                             f"period={period} rows={len(out)} ym={ym}", len(out))
                summary.append({"family": label, "period": period, "rows": len(out)})
    finally:
        conn.close()
        if masttro is not None:
            masttro.report()

    print("\n=== Summary ===")
    for s in summary:
        print(f"  {s}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
