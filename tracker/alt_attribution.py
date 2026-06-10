"""Non-listed (alt) attribution engine.

Turns a family's Holdings + cef + GWM payloads into ownership-weighted
alt_position_snapshot rows at the Security -> Vehicle/SPV -> Entity grain.

Validated in scripts/rollup_nonlisted_to_existing_entities.py and
scripts/build_dyne_us_entity_allassets.py; reconciles to the Masttro UI.

Method:
  - Non-listed = assetClass not in the 4 listed classes.
  - Full value = cef marketValueRepCCY for fund vehicles (joined by group_node_id),
    else the GWM node valuation (signed; loans negative). Liveness gate: if not in
    cef and the current Holdings mv < $50k, skip (kills dead legacy nodes).
  - Per reflection path that resolves to the family: ownership = product of GWM
    ownershipPct from the leaf up to the family root; entity = nearest ancestor
    whose trust_alias already carries listed value (else fall back to the branch =
    family_path level 3, so nothing is dropped); vehicle = the leaf node's own
    trust_alias (NULL when it equals the entity = held directly);
    mv = full_value * ownership.
  - Non-canonical cash: family-specific bank/deposit nodes the canonical filter
    drops. Current Holdings mv, 100% owned, attributed to the nearest-existing
    entity, vehicle NULL.

Family-agnostic. Reads the entity tree + attribution from the DB (kept current by
the weekly GWM sync + rebuild_attribution); takes the live Holdings/cef/GWM
payloads for values and the liveness check.
"""

from __future__ import annotations

from collections import defaultdict

from tracker.ingest import ROOT_NODE_ID, _to_float, yymmdd_to_iso

LISTED_CLASSES = {"Equity", "Fixed Income", "Cash and Equivalents", "Commodities"}
LIVENESS_FLOOR = 50_000.0   # GWM-valued node skipped below this live Holdings mv
CASH_FLOOR = 1_000.0        # ignore dust cash balances


def _f(x) -> float:
    v = _to_float(x)
    return v if v is not None else 0.0


# Column order matches the INSERT in scripts/sync_alts.py.
ROW_FIELDS = (
    "snapshot_date", "security_id", "holding_node_id",
    "sub_client_node_id", "sub_client_alias", "entity_node_id", "entity_alias",
    "vehicle_node_id", "vehicle_alias", "ownership_pct", "full_value_reporting",
    "mv_reporting", "reporting_ccy", "value_source", "valuation_date", "entity_rollup",
)


def _load_tree(conn, sub_client_node_id: str):
    """Entity tree, attribution and the existing-entity set, from the DB."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT node_id, parent_node_id, alias, name, group_node_id, "
            "is_account, is_canonical_account FROM entity"
        )
        ent = {r["node_id"]: dict(r) for r in cur.fetchall()}
        cur.execute(
            "SELECT node_id, trust_node_id, trust_alias FROM entity_attribution"
        )
        attr = {r["node_id"]: dict(r) for r in cur.fetchall()}
        # entities the tool already shows for this family = trust_alias with
        # listed value in v_latest_positions
        cur.execute(
            "SELECT DISTINCT trust_alias FROM v_latest_positions "
            "WHERE sub_client_node_id = %s AND trust_alias IS NOT NULL",
            (sub_client_node_id,),
        )
        existing = {r["trust_alias"] for r in cur.fetchall()}
    return ent, attr, existing


def compute_alt_rows(
    conn,
    *,
    holdings: list[dict],
    cef: list[dict],
    gwm_payload: list[dict],
    sub_client_node_id: str,
    sub_client_alias: str,
    reporting_ccy: str,
    snapshot_date: str,
) -> list[tuple]:
    """Return alt_position_snapshot row tuples (see ROW_FIELDS) for one family."""
    ent, attr, existing = _load_tree(conn, sub_client_node_id)
    own = {r["nodeId"]: r.get("ownershipPct") for r in gwm_payload}
    gval = {r["nodeId"]: r.get("valuation") for r in gwm_payload}
    trust_alias_of = {n: a.get("trust_alias") for n, a in attr.items()}
    trust_node_of = {n: a.get("trust_node_id") for n, a in attr.items()}

    bygroup: dict[str, list[str]] = defaultdict(list)
    for nid, e in ent.items():
        g = e.get("group_node_id")
        if g:
            bygroup[str(g)].append(nid)

    # cef full NAV + valuation date, keyed by the holding's group_node_id
    cef_nav: dict[str, float] = {}
    cef_vdate: dict[str, str | None] = {}
    for r in cef:
        nid = r.get("nodeId")
        g = str(ent.get(nid, {}).get("group_node_id") or f"NOGRP_{nid}")
        v = _f(r.get("marketValueRepCCY"))
        if g not in cef_nav or abs(v) > abs(cef_nav[g]):
            cef_nav[g] = v
            cef_vdate[g] = yymmdd_to_iso(r.get("lastValuationDate"))

    def resolve(node: str):
        """Walk a reflection node to the family root.
        Returns (is_this_family, ownership_fraction, entity_node, entity_alias,
                 branch_node, branch_alias)."""
        frac, cur, fam = 1.0, node, None
        ent_node = ent_alias = branch_node = branch_alias = None
        for _ in range(20):
            e = ent.get(cur)
            if not e:
                break
            par = e.get("parent_node_id")
            if par == sub_client_node_id:                       # person/branch tier
                branch_node, branch_alias = cur, e.get("alias") or e.get("name")
            if ent_node is None and trust_alias_of.get(cur) in existing:
                ent_node = trust_node_of.get(cur)
                ent_alias = trust_alias_of.get(cur)
            if par == ROOT_NODE_ID or not par or par == "_":
                fam = cur
                break
            frac *= _f(own.get(cur)) / 100.0
            cur = par
        return (fam == sub_client_node_id), frac, ent_node, ent_alias, branch_node, branch_alias

    rows: list[tuple] = []

    # ---- non-listed investments (alts / RE / business / loans / collections) ----
    sec: dict = {}
    for h in holdings:
        nid = h.get("nodeId")
        if (h.get("assetClass") or "") in LISTED_CLASSES:
            continue
        # only securities this family holds (some reflection of it tags the family)
        ok, _frac, *_ = resolve(nid)
        if not ok:
            continue
        sid = h.get("securityId")
        if sid is None:
            continue
        d = sec.setdefault(sid, {"nodes": set(), "hmv": 0.0})
        d["nodes"].add(nid)
        d["hmv"] += _f(h.get("marketValue"))

    for sid, d in sec.items():
        groups = {str(ent.get(n, {}).get("group_node_id")) for n in d["nodes"]
                  if ent.get(n, {}).get("group_node_id")}
        if groups:
            g = sorted(groups)[0]
            refl = bygroup.get(g, list(d["nodes"]))
        else:
            g = f"NOGRP_{sorted(d['nodes'])[0]}"
            refl = list(d["nodes"])
        if g in cef_nav:
            full, vsrc, vdate = cef_nav[g], "cef", cef_vdate.get(g)
        else:
            if abs(d["hmv"]) < LIVENESS_FLOOR:        # liveness: skip dead nodes
                continue
            vals = [_f(gval.get(n)) for n in refl if gval.get(n) not in (None, 0)]
            full = max(vals, key=abs) if vals else 0.0
            vsrc, vdate = "gwm", snapshot_date
        if full == 0:
            continue
        for n in refl:
            is_fam, frac, e_node, e_alias, b_node, b_alias = resolve(n)
            if not is_fam or frac == 0:
                continue
            rollup = "existing"
            if e_alias is None:                       # no existing entity on path
                e_node, e_alias, rollup = b_node, b_alias, "branch-fallback"
            veh_alias = trust_alias_of.get(n)
            veh_node = trust_node_of.get(n)
            if veh_alias == e_alias:                  # held directly — no SPV
                veh_node = veh_alias = None
            rows.append((
                snapshot_date, sid, n, sub_client_node_id, sub_client_alias,
                e_node, e_alias, veh_node, veh_alias, round(frac, 8),
                round(full, 2), round(full * frac, 2), reporting_ccy,
                vsrc, vdate, rollup,
            ))

    # ---- non-canonical cash (family-specific accounts dropped by canonical filter) ----
    cash: dict = {}
    for h in holdings:
        if (h.get("assetClass") or "") != "Cash and Equivalents":
            continue
        nid = h.get("nodeId")
        e = ent.get(nid)
        if not e or e.get("is_canonical_account"):    # canonical cash is in position_snapshot
            continue
        is_fam, _frac, *_ = resolve(nid)
        if not is_fam:
            continue
        sid = h.get("securityId")
        if sid is None:
            continue
        c = cash.setdefault((sid, nid), {"mv": 0.0})
        c["mv"] += _f(h.get("marketValue"))

    for (sid, nid), c in cash.items():
        if abs(c["mv"]) < CASH_FLOOR:
            continue
        is_fam, _frac, e_node, e_alias, b_node, b_alias = resolve(nid)
        rollup = "existing"
        if e_alias is None:
            e_node, e_alias, rollup = b_node, b_alias, "branch-fallback"
        rows.append((
            snapshot_date, sid, nid, sub_client_node_id, sub_client_alias,
            e_node, e_alias, None, None, 1.0,
            round(c["mv"], 2), round(c["mv"], 2), reporting_ccy,
            "holdings", snapshot_date, rollup,
        ))

    return rows
