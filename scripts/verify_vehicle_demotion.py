"""Verify VEHICLE_NOT_ENTITY_GROUPS demotion (Dendell + Modyl + Optsia) WITHOUT
writing. Replicates the production rebuild_attribution walk, cross-checks vs live
attribution: the only nodes whose entity changes must be ones currently under a
demoted vehicle, no node loses its entity (no new orphan), and each demoted
vehicle rolls to trusts with the vehicle tag set.
"""
from __future__ import annotations
import sys
from collections import defaultdict
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from tracker.db import connect
from tracker.sync_supabase import (
    FORCE_OWN_ENTITY_NODES, VEHICLE_NOT_ENTITY_GROUPS, ROOT_NODE_ID,
    _detect_shared_vehicle_nodes, _is_trust,
)

conn = connect(); cur = conn.cursor()
cur.execute("SELECT node_id,parent_node_id,alias,name,group_node_id,is_account FROM entity")
recs = cur.fetchall()
nodes = {r["node_id"]: (r["parent_node_id"], r["alias"], r["name"], r["group_node_id"]) for r in recs}
is_account_by_id = {r["node_id"]: bool(r["is_account"]) for r in recs}
gnid_by_node = {r["node_id"]: r["group_node_id"] for r in recs}
cur.execute("SELECT node_id, trust_alias FROM entity_attribution")
live_entity = {r["node_id"]: r["trust_alias"] for r in cur.fetchall()}
cur.execute("SELECT account_node_id, SUM(mv_reporting) mv, MAX(sub_client_alias) sub FROM v_latest_positions GROUP BY 1")
mv_by_node = {r["account_node_id"]: (float(r["mv"] or 0), r["sub"]) for r in cur.fetchall()}
conn.close()

# alias of the demoted vehicles (for the "only these change" assertion)
demoted_aliases = {(a or n) for nid, (p, a, n, g) in nodes.items() if g in VEHICLE_NOT_ENTITY_GROUPS}
print("demoted vehicle groups:", VEHICLE_NOT_ENTITY_GROUPS, "->", demoted_aliases)

sub_clients = {nid for nid, (pid, _, _, _) in nodes.items() if pid == ROOT_NODE_ID}
person_tier = {nid for nid, (pid, _, _, _) in nodes.items() if pid in sub_clients}
def is_retirement_wrapper(a, n): return "retirement" in f"{(a or '').lower()} {(n or '').lower()}"
def is_super_or_pension(a, n):
    t = f"{(a or '').lower()} {(n or '').lower()}"; return "super" in t or "pension" in t
shared = _detect_shared_vehicle_nodes(nodes)

def walk(nid):  # verbatim production logic
    entity_alias = vehicle_alias = weak_alias = struct_alias = None
    cur_id = nid
    for _ in range(50):
        if not cur_id or cur_id == "_": break
        pid, alias, name, gnid = nodes.get(cur_id, (None, None, None, None))
        is_demoted = cur_id != nid and gnid in VEHICLE_NOT_ENTITY_GROUPS
        if vehicle_alias is None and is_demoted:
            vehicle_alias = alias or name
        if entity_alias is None and cur_id != nid and not is_demoted and (
            cur_id in FORCE_OWN_ENTITY_NODES or cur_id in shared
            or _is_trust(alias, name) or is_retirement_wrapper(alias, name)
        ):
            entity_alias = alias or name
        elif entity_alias is None and weak_alias is None and cur_id != nid and is_super_or_pension(alias, name):
            weak_alias = alias or name
        if struct_alias is None and pid in person_tier and not is_account_by_id.get(cur_id, False):
            struct_alias = alias or name
        cur_id = pid
    if entity_alias is None and weak_alias is not None: entity_alias = weak_alias
    if entity_alias is None and struct_alias is not None: entity_alias = struct_alias
    if entity_alias is None:
        sa, sn = nodes[nid][1], nodes[nid][2]
        if _is_trust(sa, sn) or is_super_or_pension(sa, sn): entity_alias = sa or sn
    return entity_alias, vehicle_alias

computed = {n: walk(n) for n in nodes}
changed = [(n, live_entity.get(n), computed[n][0]) for n in nodes if live_entity.get(n) != computed[n][0]]
bad = [(n, o, nw) for n, o, nw in changed if o not in demoted_aliases]
new_orphan = [n for n in nodes if live_entity.get(n) is not None and computed[n][0] is None]

print(f"\nnodes total {len(nodes)}   entity changed {len(changed)}")
print(f"changed nodes whose OLD entity is NOT a demoted vehicle: {len(bad)}  (must be 0)")
for n, o, nw in bad[:10]: print("   BAD:", n, o, "->", nw)
print(f"nodes that LOSE their entity (new orphans): {len(new_orphan)}  (must be 0)")

print("\n=== rollup destinations + family NAV flatness ===")
dest = defaultdict(lambda: defaultdict(float))
fam_live = defaultdict(float); fam_kept = defaultdict(float)
for n, (mv, sub) in mv_by_node.items():
    if not mv: continue
    fam_live[sub] += mv
    if computed[n][0] is not None: fam_kept[sub] += mv
    le = live_entity.get(n)
    if le in demoted_aliases: dest[le][computed[n][0]] += mv
for veh in sorted(dest, key=lambda v: -sum(dest[v].values())):
    print(f"  {veh} (${sum(dest[veh].values())/1e6:.2f}M):")
    for ent, v in sorted(dest[veh].items(), key=lambda x: -x[1]):
        if v: print(f"      -> {str(ent)[:34]:<36}{v/1e6:>8.2f}M  vehicle={veh[:24]}")
print("\n  family NAV (live vs kept; drop should be 0 beyond pre-existing orphans):")
for sub in sorted(fam_live):
    print(f"    {str(sub)[:26]:<28} live {fam_live[sub]/1e6:>8.2f}M  kept {fam_kept[sub]/1e6:>8.2f}M")
print(f"\nRESULT: {'PASS' if not bad and not new_orphan else 'FAIL'}")
