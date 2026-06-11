"""Performance sync: Masttro /Performance -> performance_snapshot.

Stores the period return components (marketValueInitial/marketValue/flows/totalPL/
irr/twr) for every holding across all asset classes, backing a blended ALL-ASSET
return (modified-Dietz over the summed components). Two scopes:

  scope='family' — one pull per family (investmentVehicle=family). EXACT total +
                   per-asset-class (matches Masttro). Holdings rolled up to entity
                   approximately (not used for per-entity).
  scope='entity' — one pull per displayed entity (investmentVehicle=entity node).
                   The whole pull IS that entity, so per-entity returns are EXACT
                   (match Masttro). Masttro scopes each entity by its subtree as a
                   unit, which a family-pull rollup can't reproduce — hence the
                   per-entity pulls. See migration 041 + the design notes.

Usage:
  python scripts/sync_performance.py                      # both scopes, current month
  python scripts/sync_performance.py --scope family       # family only (fast, 5x4 calls)
  python scripts/sync_performance.py --scope entity        # entity only (~51x4 calls)
  python scripts/sync_performance.py --year-month 202603 --from-saved --dry-run

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
  (pull_date, period, scope, year_month, sub_client_node_id, sub_client_alias,
   node_id, security_id, asset_class, security_type, entity_node_id, entity_alias,
   vehicle_alias, market_value_initial, market_value, deposits, withdrawals,
   transfer_in_out, realized_gl, unrealized_gl, income, total_pl, avg_cap_base,
   irr, twr, reporting_ccy, initial_date, as_of_date)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""


def f(x):
    try:
        return float(str(x).replace(",", ""))
    except (TypeError, ValueError):
        return None


def iso(s):
    if not s:
        return None
    s = str(s).strip()
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:]}"
    return s[:10] if len(s) >= 10 else None


def load_family_attribution(conn, family_node):
    """Per-node entity resolver (nearest-existing walk) — used to tag the family
    pull's holdings + to recover each holding's vehicle on the entity pull."""
    cur = conn.cursor()
    cur.execute("SELECT node_id, parent_node_id FROM entity")
    parent = {r["node_id"]: r["parent_node_id"] for r in cur.fetchall()}
    cur.execute("SELECT node_id, trust_node_id, trust_alias, vehicle_alias, family_path "
                "FROM entity_attribution")
    attr = {r["node_id"]: dict(r) for r in cur.fetchall()}
    cur.execute("SELECT DISTINCT trust_alias FROM v_latest_positions "
                "WHERE sub_client_node_id=%s AND trust_alias IS NOT NULL", (family_node,))
    existing = {r["trust_alias"] for r in cur.fetchall()}
    br = defaultdict(lambda: defaultdict(int))
    for n, a in attr.items():
        if a.get("trust_alias") and a.get("family_path"):
            parts = [p.strip() for p in a["family_path"].split(">")]
            br[a["trust_alias"]][parts[2] if len(parts) > 2 else "(direct)"] += 1
    branch_of = {t: max(d, key=d.get) for t, d in br.items()}

    def resolve(node):
        cur_id, ent_node, ent_alias = node, None, None
        for _ in range(15):
            ta = attr.get(cur_id, {}).get("trust_alias")
            if ta in existing:
                ent_node, ent_alias = attr[cur_id].get("trust_node_id"), ta
                break
            par = parent.get(cur_id)
            if not par or par == ROOT_NODE_ID or par == "_":
                break
            cur_id = par
        if ent_alias is None:
            ent_alias = branch_of.get(attr.get(node, {}).get("trust_alias"))
        veh = attr.get(node, {}).get("vehicle_alias") or attr.get(node, {}).get("trust_alias")
        if veh == ent_alias:
            veh = None
        return ent_node, ent_alias, veh

    return resolve


def list_entities(conn, family_node):
    """Displayed entities for a family -> {trust_alias: entity_node}. One node per
    alias (the entity's own node, for investmentVehicle)."""
    cur = conn.cursor()
    cur.execute("""SELECT DISTINCT ea.trust_alias, ea.trust_node_id
                   FROM entity_attribution ea
                   JOIN v_latest_positions lp
                     ON lp.trust_alias = ea.trust_alias
                    AND lp.sub_client_node_id = ea.sub_client_node_id
                   WHERE ea.sub_client_node_id = %s AND ea.trust_node_id IS NOT NULL""",
                (family_node,))
    out = {}
    for r in cur.fetchall():
        out.setdefault(r["trust_alias"], r["trust_node_id"])  # first node per alias
    return out


def build_rows(rows, *, scope, pull_date, period, ym, family_node, sub_alias, ccy,
               resolve, fixed_entity=None):
    """fixed_entity = (node, alias) for the entity pass; None for the family pass."""
    out = []
    for r in rows:
        if fixed_entity:
            e_node, e_alias = fixed_entity
            _, _, veh = resolve(r.get("nodeId"))
        else:
            e_node, e_alias, veh = resolve(r.get("nodeId"))
        out.append((
            pull_date, period, scope, ym, family_node, sub_alias, r.get("nodeId"),
            r.get("securityId"), r.get("assetClass"), r.get("securityType"),
            e_node, e_alias, veh,
            f(r.get("marketValueInitial")), f(r.get("marketValue")),
            f(r.get("deposits")), f(r.get("withdrawals")), f(r.get("transferInOut")),
            f(r.get("periodRealizedGL")), f(r.get("periodUnrealizedGL")),
            f(r.get("income")), f(r.get("totalPL")), f(r.get("avgCapBase")),
            f(r.get("irr")), f(r.get("twr")), ccy,
            iso(r.get("initialDate")), iso(r.get("date")),
        ))
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--year-month", default=dt.date.today().strftime("%Y%m"))
    ap.add_argument("--periods", default=",".join(map(str, DEFAULT_PERIODS)))
    ap.add_argument("--scope", choices=["family", "entity", "both"], default="both")
    ap.add_argument("--family", default=None,
                    help="limit to families whose label contains this substring")
    ap.add_argument("--from-saved", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    families = [f for f in FAMILIES
                if not args.family or args.family.lower() in f["label"].lower()]
    periods = [int(p) for p in args.periods.split(",")]
    ym = args.year_month
    pull_date = dt.date.today().isoformat()
    do_family = args.scope in ("family", "both")
    do_entity = args.scope in ("entity", "both")
    print(f"Performance sync — yearMonth={ym} periods={periods} scope={args.scope} "
          f"pull_date={pull_date} {'(DRY RUN)' if args.dry_run else ''}")

    masttro = None if args.from_saved else MasttroClient()
    conn = connect()
    summary = []

    def pull(client_id, params, desc):
        if args.from_saved:
            hits = sorted(glob.glob(f"responses/Performance-{client_id}_*{desc}.json"))
            return json.load(open(hits[-1], encoding="utf-8")) if hits else None
        data = masttro.get(f"Performance/{client_id}", params) or []
        masttro.save_response(f"Performance/{client_id}", data, descriptor=desc)
        return data

    def store(out, *, period, family_node, scope, entity_node=None):
        if args.dry_run or not out:
            return
        with conn.cursor() as cur:
            if entity_node is None:
                cur.execute("DELETE FROM performance_snapshot WHERE pull_date=%s AND period=%s "
                            "AND sub_client_node_id=%s AND scope=%s",
                            (pull_date, period, family_node, scope))
            else:
                cur.execute("DELETE FROM performance_snapshot WHERE pull_date=%s AND period=%s "
                            "AND sub_client_node_id=%s AND scope='entity' AND entity_node_id=%s",
                            (pull_date, period, family_node, entity_node))
            cur.executemany(INSERT_SQL, out)
        conn.commit()

    try:
        for fam in families:
            label, family_node = fam["label"], fam["node_id"]
            client_id, ccy = fam["client_id"], fam["reporting_ccy"]
            print(f"\n--- {label} ({family_node}) {ccy} ---")
            resolve = load_family_attribution(conn, family_node)
            with conn.cursor() as cur:
                cur.execute("SELECT alias, name FROM entity WHERE node_id=%s", (family_node,))
                _r = cur.fetchone()
            sub_alias = (_r["alias"] or _r["name"]) if _r else label

            for period in periods:
                if do_family:
                    desc = f"perf_{family_node}_{ccy.lower()}_{ym}_p{period}"
                    rows = pull(client_id, {"ccy": ccy, "yearMonth": ym, "period": period,
                                            "investmentVehicle": family_node}, desc)
                    if rows is not None:
                        out = build_rows(rows, scope="family", pull_date=pull_date, period=period,
                                         ym=ym, family_node=family_node, sub_alias=sub_alias,
                                         ccy=ccy, resolve=resolve)
                        store(out, period=period, family_node=family_node, scope="family")
                        print(f"  [family] p{period}: {len(out)} holdings "
                              f"end {sum(x[14] or 0 for x in out)/1e6:.2f}M")

            if do_entity:
                entities = list_entities(conn, family_node)
                print(f"  {len(entities)} entities to pull per-entity")
                ecount = 0
                for ealias, enode in entities.items():
                    for period in periods:
                        desc = f"perfent_{enode}_{ccy.lower()}_{ym}_p{period}"
                        rows = pull(client_id, {"ccy": ccy, "yearMonth": ym, "period": period,
                                                "investmentVehicle": enode}, desc)
                        if rows is None:
                            continue
                        out = build_rows(rows, scope="entity", pull_date=pull_date, period=period,
                                         ym=ym, family_node=family_node, sub_alias=sub_alias,
                                         ccy=ccy, resolve=resolve, fixed_entity=(enode, ealias))
                        store(out, period=period, family_node=family_node, scope="entity",
                              entity_node=enode)
                        ecount += len(out)
                    if not args.dry_run:
                        log_sync(conn, "performance_entity", family_node,
                                 f"entity={ealias} ym={ym}", 0)
                print(f"  [entity] {ecount} rows across {len(entities)} entities")
            summary.append({"family": label})
    finally:
        conn.close()
        if masttro is not None:
            masttro.report()
    print("\n=== done ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
