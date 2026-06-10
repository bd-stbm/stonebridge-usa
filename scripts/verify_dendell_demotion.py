"""Verify the Dendell Dell&Broadcom vehicle-demotion change to rebuild_attribution
WITHOUT writing to the DB (migration 037 not applied yet).

Replicates the new leaf-to-root walk verbatim, then cross-checks the computed
entity against the CURRENT entity_attribution (produced by the OLD walk). Since
the only logic difference is the demote branch (group 532580), the computed
entity must match the live attribution on EVERY node except the Dendell
Dell&Broadcom subtree. That match proves both (a) the replica is faithful and
(b) the change is surgical.
"""
from __future__ import annotations
import sys
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
cur.execute("SELECT node_id, trust_alias FROM entity_attribution")
live_entity = {r["node_id"]: r["trust_alias"] for r in cur.fetchall()}
conn.close()

# --- walk replicated verbatim from rebuild_attribution (new logic) ---
sub_clients = {nid for nid, (pid, _, _, _) in nodes.items() if pid == ROOT_NODE_ID}
person_tier = {nid for nid, (pid, _, _, _) in nodes.items() if pid in sub_clients}
def is_retirement_wrapper(alias, name):
    text = f"{(alias or '').lower()} {(name or '').lower()}"; return "retirement" in text
def is_super_or_pension(alias, name):
    text = f"{(alias or '').lower()} {(name or '').lower()}"; return "super" in text or "pension" in text
shared_vehicle_nodes = _detect_shared_vehicle_nodes(nodes)

computed = {}   # node_id -> (entity_alias, vehicle_alias)
for nid in nodes:
    entity_nid = entity_alias = None
    weak_nid = weak_alias = None
    struct_nid = struct_alias = None
    vehicle_nid = vehicle_alias = None
    cur_id = nid
    for _ in range(50):
        if not cur_id or cur_id == "_":
            break
        pid, alias, name, gnid = nodes.get(cur_id, (None, None, None, None))
        is_demoted_vehicle = cur_id != nid and gnid in VEHICLE_NOT_ENTITY_GROUPS
        if vehicle_nid is None and is_demoted_vehicle:
            vehicle_nid = cur_id; vehicle_alias = alias or name
        if entity_nid is None and cur_id != nid and not is_demoted_vehicle and (
            cur_id in FORCE_OWN_ENTITY_NODES or cur_id in shared_vehicle_nodes
            or _is_trust(alias, name) or is_retirement_wrapper(alias, name)
        ):
            entity_nid = cur_id; entity_alias = alias or name
        elif (entity_nid is None and weak_nid is None and cur_id != nid
              and is_super_or_pension(alias, name)):
            weak_nid = cur_id; weak_alias = alias or name
        if struct_nid is None and pid in person_tier and not is_account_by_id.get(cur_id, False):
            struct_nid = cur_id; struct_alias = alias or name
        cur_id = pid
    if entity_nid is None and weak_nid is not None:
        entity_alias = weak_alias
    if entity_nid is None and struct_nid is not None and entity_alias is None:
        entity_alias = struct_alias
    if entity_alias is None:
        sa, sn = nodes[nid][1], nodes[nid][2]
        if _is_trust(sa, sn) or is_super_or_pension(sa, sn):
            entity_alias = sa or sn
    computed[nid] = (entity_alias, vehicle_alias)

# --- compare computed entity vs live attribution ---
changed = [(n, live_entity.get(n), computed[n][0]) for n in nodes
           if live_entity.get(n) != computed[n][0]]
print(f"nodes total: {len(nodes)}   entity changed vs live: {len(changed)}")
print("\n=== every node whose entity changed (should be ONLY Dendell Dell&Broadcom) ===")
non_dendell = 0
for n, old, new in sorted(changed, key=lambda x: str(x[1])):
    veh = computed[n][1]
    is_db = old == "Dendell LLC - Dell & Broadcom"
    if not is_db: non_dendell += 1
    print(f"  {n:<12} {str(old)[:30]:<32} -> entity={str(new)[:26]:<28} vehicle={veh}")
print(f"\n  changed nodes NOT from 'Dendell LLC - Dell & Broadcom': {non_dendell}  (must be 0)")

# --- the 5 GS account nodes: explicit expectations ---
print("\n=== Dendell GS account nodes (the ones holding Dell/Broadcom) ===")
expect = {"101_274874":"Morgan Dyne Trust","101_274875":"Dylan Dyne Irrevocable Trust",
          "101_274876":"Mark I Dyne 2010 Irrevocable T","101_52656":"Murray Markiles 2015 Investmen",
          "101_274873":"Lia Markiles 2015 Investment T"}
ok = True
for n, exp in expect.items():
    ent, veh = computed[n]
    good = ent == exp and veh == "Dendell LLC - Dell & Broadcom"
    ok = ok and good
    print(f"  {n}: entity={ent!r} vehicle={veh!r}  {'OK' if good else 'WRONG (exp '+exp+')'}")
print(f"\nRESULT: {'PASS' if ok and non_dendell == 0 else 'FAIL'}")
