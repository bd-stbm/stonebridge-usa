"""Scope the impact of GENERALIZING vehicle demotion to ALL shared vehicles
(not just Dendell 532580). Dry-run: replicate the walk with the generalized
rule, diff vs the live attribution, quantify value moved, list entities that
disappear, new vehicles, and any orphans (shared vehicle with no trust above).
No DB writes.
"""
from __future__ import annotations
import sys
from collections import defaultdict
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from tracker.db import connect
from tracker.sync_supabase import (
    FORCE_OWN_ENTITY_NODES, ROOT_NODE_ID, _detect_shared_vehicle_nodes, _is_trust,
)

conn = connect(); cur = conn.cursor()
cur.execute("SELECT node_id,parent_node_id,alias,name,group_node_id,is_account FROM entity")
recs = cur.fetchall()
nodes = {r["node_id"]: (r["parent_node_id"], r["alias"], r["name"], r["group_node_id"]) for r in recs}
is_account_by_id = {r["node_id"]: bool(r["is_account"]) for r in recs}
cur.execute("SELECT node_id, trust_alias FROM entity_attribution")
live_entity = {r["node_id"]: r["trust_alias"] for r in cur.fetchall()}
# listed mv per account node (current)
cur.execute("SELECT account_node_id, SUM(mv_reporting) mv, MAX(sub_client_alias) sub FROM v_latest_positions GROUP BY 1")
mv_by_node = {r["account_node_id"]: (float(r["mv"] or 0), r["sub"]) for r in cur.fetchall()}
conn.close()

sub_clients = {nid for nid, (pid, _, _, _) in nodes.items() if pid == ROOT_NODE_ID}
person_tier = {nid for nid, (pid, _, _, _) in nodes.items() if pid in sub_clients}
def is_retirement_wrapper(a, n): return "retirement" in f"{(a or '').lower()} {(n or '').lower()}"
def is_super_or_pension(a, n):
    t = f"{(a or '').lower()} {(n or '').lower()}"; return "super" in t or "pension" in t
shared = _detect_shared_vehicle_nodes(nodes)

def walk(nid):
    entity_alias = vehicle_alias = weak_alias = struct_alias = None
    cur_id = nid
    for _ in range(50):
        if not cur_id or cur_id == "_": break
        pid, alias, name, gnid = nodes.get(cur_id, (None, None, None, None))
        # REFINED demote: a shared vehicle that is a non-trust / non-super /
        # non-retirement OPERATING vehicle (LLC/LP/Pty Ltd). Trusts and super
        # funds stay entities even when shared.
        is_vehicle = (cur_id != nid and cur_id in shared
                      and cur_id not in FORCE_OWN_ENTITY_NODES
                      and not _is_trust(alias, name)
                      and not is_super_or_pension(alias, name)
                      and not is_retirement_wrapper(alias, name))
        if vehicle_alias is None and is_vehicle:
            vehicle_alias = alias or name
        # Entity = first trust / retirement / force-own above (NOT a demoted vehicle).
        if entity_alias is None and cur_id != nid and not is_vehicle and (
            cur_id in FORCE_OWN_ENTITY_NODES or _is_trust(alias, name)
            or is_retirement_wrapper(alias, name)
        ):
            entity_alias = alias or name
        elif entity_alias is None and weak_alias is None and cur_id != nid and is_super_or_pension(alias, name):
            weak_alias = alias or name
        if struct_alias is None and pid in person_tier and not is_account_by_id.get(cur_id, False):
            struct_alias = alias or name
        cur_id = pid
    rolled = "trust"
    if entity_alias is None and weak_alias is not None:
        entity_alias, rolled = weak_alias, "super/pension"
    # UN-DEMOTE: no real entity above the vehicle -> the vehicle IS the entity
    # (preserves AU holding-company structures with no trust tier).
    if entity_alias is None and vehicle_alias is not None:
        entity_alias, vehicle_alias, rolled = vehicle_alias, None, "vehicle-as-entity"
    if entity_alias is None and struct_alias is not None:
        entity_alias, rolled = struct_alias, "struct-fallback"
    if entity_alias is None:
        sa, sn = nodes[nid][1], nodes[nid][2]
        if _is_trust(sa, sn) or is_super_or_pension(sa, sn):
            entity_alias, rolled = sa or sn, "self"
        else:
            rolled = "ORPHAN"
    return entity_alias, vehicle_alias, rolled

computed = {n: walk(n) for n in nodes}

changed = [(n, live_entity.get(n), computed[n][0]) for n in nodes if live_entity.get(n) != computed[n][0]]
print(f"nodes total {len(nodes)}   entity changed {len(changed)}")

# entities that DISAPPEAR (were an entity with listed value, now not the entity of any node)
old_entities_with_value = defaultdict(float)
for n, (mv, sub) in mv_by_node.items():
    if live_entity.get(n): old_entities_with_value[live_entity[n]] += mv
new_entity_set = {computed[n][0] for n in nodes if computed[n][0]}
disappearing = sorted([(e, v) for e, v in old_entities_with_value.items() if e not in new_entity_set],
                      key=lambda x: -x[1])
print(f"\n=== Entities that DISAPPEAR (become vehicles), with listed value ({len(disappearing)}) ===")
for e, v in disappearing[:40]:
    print(f"  {e[:40]:<42}{v/1e6:>9.2f}M")

# new vehicles + their listed value
veh_val = defaultdict(float)
for n, (mv, sub) in mv_by_node.items():
    veh = computed[n][1]
    if veh: veh_val[veh] += mv
print(f"\n=== New listed VEHICLES populated ({len(veh_val)}) ===")
for v, val in sorted(veh_val.items(), key=lambda x: -x[1])[:40]:
    print(f"  {v[:40]:<42}{val/1e6:>9.2f}M")

# orphans: nodes WITH listed value whose new entity is None/ORPHAN
orphans = [(n, mv) for n, (mv, sub) in mv_by_node.items()
           if mv and (computed[n][2] == "ORPHAN" or computed[n][0] is None)]
print(f"\n=== ORPHANS — listed value with NO trust entity after demotion ({len(orphans)}) ===")
for n, mv in sorted(orphans, key=lambda x: -x[1])[:20]:
    print(f"  {n:<12} live_entity={str(live_entity.get(n))[:30]:<32} mv={mv/1e6:.3f}M  vehicle={computed[n][1]}")
orph_total = sum(mv for _, mv in orphans)
print(f"  orphan listed value total: {orph_total/1e6:.2f}M")

# rollup fallbacks distribution
roll = defaultdict(int)
for n in nodes:
    if mv_by_node.get(n, (0,0))[0]:
        roll[computed[n][2]] += 1
print(f"\n=== rollup type for value-bearing nodes: {dict(roll)} ===")

# family NAV must stay flat (value only regroups within a family)
fam_live = defaultdict(float); fam_new = defaultdict(float)
for n, (mv, sub) in mv_by_node.items():
    if not mv: continue
    fam_live[sub] += mv
    # 'new' family = same sub (walk never changes sub_client); count only if it still has an entity
    if computed[n][0] is not None:
        fam_new[sub] += mv
print("\n=== Family listed NAV: live vs after (drop = newly orphaned) ===")
for sub in sorted(fam_live):
    d = fam_live[sub] - fam_new[sub]
    print(f"  {str(sub)[:26]:<28} live {fam_live[sub]/1e6:>8.2f}M  after {fam_new[sub]/1e6:>8.2f}M  drop {d/1e6:>+6.2f}M")

# where each disappearing vehicle's value rolls TO (new entity split)
print("\n=== Roll-up destinations for the disappearing vehicles ===")
dis_names = {e for e, _ in disappearing}
dest = defaultdict(lambda: defaultdict(float))
for n, (mv, sub) in mv_by_node.items():
    le = live_entity.get(n)
    if le in dis_names and mv:
        dest[le][computed[n][0]] += mv
for veh in sorted(dest, key=lambda v: -sum(dest[v].values())):
    print(f"  {veh}:")
    for ent, v in sorted(dest[veh].items(), key=lambda x: -x[1]):
        print(f"      -> {str(ent)[:34]:<36}{v/1e6:>8.2f}M")
