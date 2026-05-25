"""Postgres ingest — UPSERT Masttro JSON payloads into Supabase tables.

Mirrors the SQLite logic in tracker/ingest.py but uses psycopg + ON CONFLICT.
JSON parsing helpers (yymmdd_to_iso, _to_float) and constants (ROOT_NODE_ID,
EXTERNAL_FLOW_TYPES) are reused from tracker/ingest.py.
"""

from __future__ import annotations

from tracker.db import log_sync
from tracker.ingest import (
    EXTERNAL_FLOW_TYPES,
    ROOT_NODE_ID,
    _to_float,
    yymmdd_to_iso,
)


# ---------------------------------------------------------------------------
# Entity tree + attribution
# ---------------------------------------------------------------------------

def upsert_gwm(conn, gwm_payload: list[dict]) -> int:
    """Upsert entity rows from a /GWM response; recompute canonical-account flag."""
    if not gwm_payload:
        return 0
    snapshot_iso = yymmdd_to_iso(gwm_payload[0].get("date"))
    rows = []
    for n in gwm_payload:
        bb = (n.get("bankBroker") or "").strip() or None
        an = (n.get("accountNumber") or "").strip() or None
        gnid = n.get("groupNodeId")
        # Masttro emits groupNodeId as an int; store as TEXT for consistency
        # with node_id and to keep joins/comparisons unambiguous.
        gnid_text = str(gnid) if gnid is not None else None
        rows.append((
            n["nodeId"], n.get("parentNodeId"), n.get("alias"), n.get("name"),
            bb, an, n.get("ownershipPct"),
            bool(bb and an), False,
            n.get("valuation"), "AUD", snapshot_iso, n.get("status"),
            gnid_text,
        ))

    with conn.cursor() as cur:
        cur.executemany(
            """INSERT INTO entity
                 (node_id, parent_node_id, alias, name, bank_broker, account_number,
                  ownership_pct, is_account, is_canonical_account,
                  gwm_valuation, gwm_valuation_ccy, snapshot_date, status,
                  group_node_id)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT (node_id) DO UPDATE SET
                 parent_node_id    = EXCLUDED.parent_node_id,
                 alias             = EXCLUDED.alias,
                 name              = EXCLUDED.name,
                 bank_broker       = EXCLUDED.bank_broker,
                 account_number    = EXCLUDED.account_number,
                 ownership_pct     = EXCLUDED.ownership_pct,
                 is_account        = EXCLUDED.is_account,
                 gwm_valuation     = EXCLUDED.gwm_valuation,
                 gwm_valuation_ccy = EXCLUDED.gwm_valuation_ccy,
                 snapshot_date     = EXCLUDED.snapshot_date,
                 status            = EXCLUDED.status,
                 group_node_id     = EXCLUDED.group_node_id
            """,
            rows,
        )
    conn.commit()
    mark_canonical_accounts(conn)
    log_sync(conn, "gwm", None, f"snapshot={snapshot_iso}", len(rows))
    return len(rows)


def _is_trust(alias: str | None, name: str | None) -> bool:
    return "trust" in (alias or "").lower() or "trust" in (name or "").lower()


def _detect_shared_vehicle_nodes(
    nodes: dict[str, tuple[str | None, str | None, str | None, str | None]],
) -> set[str]:
    """Return the set of node_ids whose group_node_id is "shared across
    multiple distinct trust ancestors" — i.e. each reflection of the
    same physical vehicle represents a different trust's slice.

    Covers two flavours uniformly:
      - Within-family sharing (Modyl LP held by 3 Dyne trusts) — each
        reflection is one trust's slice of Modyl's holdings.
      - Cross-family sharing (Dendell LLC held by Dyne + Markiles +
        Miller) — each family's reflection is its pro-rata slice.

    In both cases the per-reflection positions must all be ingested for
    totals to be correct. Single-reflection groups and groups whose
    reflections all share one trust ancestor are NOT included (they're
    safely deduped via the (bank, account#) fingerprint).

    `nodes` shape: {node_id: (parent_node_id, alias, name, group_node_id)}.
    """
    def trust_ancestor_of(start_nid: str) -> str | None:
        cur_id = nodes[start_nid][0]  # skip self
        for _ in range(50):
            if not cur_id or cur_id == "_":
                return None
            pid, alias, name, _ = nodes.get(cur_id, (None, None, None, None))
            if _is_trust(alias, name):
                return cur_id
            cur_id = pid
        return None

    from collections import defaultdict
    trust_ancestors_by_group: dict[str, set[str]] = defaultdict(set)
    for nid, (_, _, _, gnid) in nodes.items():
        if gnid is None:
            continue
        ta = trust_ancestor_of(nid)
        if ta is not None:
            trust_ancestors_by_group[gnid].add(ta)
    shared_groups = {g for g, tas in trust_ancestors_by_group.items() if len(tas) > 1}
    return {nid for nid, (_, _, _, gnid) in nodes.items() if gnid in shared_groups}


def mark_canonical_accounts(conn) -> None:
    """Re-mark is_canonical_account across all accounts.

    Two rules combined:
      1. Non-shared accounts → one canonical per (bank, account#) fingerprint
         (the original dedup logic). Handles the typical case where a single
         physical account appears under multiple ownership reflections that
         all show identical totals.
      2. Shared-vehicle accounts (parent's group_node_id is shared across
         multiple distinct trust ancestors) → ALL reflections canonical.
         Each reflection here is its own trust's slice; deduping would
         silently drop the other slices and undercount (Modyl was short by
         ~18% in Dyne pre-fix). Different node_ids per reflection means no
         position_snapshot PK conflicts.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT node_id, parent_node_id, alias, name, group_node_id FROM entity"
        )
        nodes = {
            r["node_id"]: (
                r["parent_node_id"], r["alias"], r["name"], r["group_node_id"],
            )
            for r in cur.fetchall()
        }
        shared_vehicle_nids = _detect_shared_vehicle_nodes(nodes)

        cur.execute(
            "UPDATE entity SET is_canonical_account = FALSE WHERE is_account = TRUE"
        )
        # Rule 1: fingerprint dedup for non-shared accounts.
        cur.execute(
            """
            WITH ranked AS (
                SELECT node_id,
                       ROW_NUMBER() OVER (
                           PARTITION BY bank_broker, account_number ORDER BY node_id
                       ) AS rn
                FROM entity
                WHERE is_account = TRUE
                  AND parent_node_id <> ALL(%s::text[])
            )
            UPDATE entity SET is_canonical_account = TRUE
            WHERE node_id IN (SELECT node_id FROM ranked WHERE rn = 1)
            """,
            (list(shared_vehicle_nids) or [""],),
        )
        # Rule 2: every reflection of a shared vehicle stays canonical.
        cur.execute(
            """
            UPDATE entity SET is_canonical_account = TRUE
            WHERE is_account = TRUE
              AND parent_node_id = ANY(%s::text[])
            """,
            (list(shared_vehicle_nids) or [""],),
        )
    conn.commit()


def rebuild_attribution(conn, root_node_id: str = ROOT_NODE_ID) -> int:
    """Recompute entity_attribution from the current entity tree.

    The trust_alias / trust_node_id columns store the position's owning
    "entity" — which is the first ancestor (walking leaf-to-root) that
    matches one of:

      - a shared vehicle whose group_node_id has 2+ distinct trust
        ancestors (covers both within-family sharing like Modyl LP held
        by multiple Dyne trusts, AND cross-family sharing like Dendell
        LLC held by Dyne + Markiles + Miller — each reflection
        attributes to the vehicle itself), OR
      - a trust (the default for positions held by a single trust), OR
      - a retirement grouping (the "Dyne US Retirement" / "Markiles
        Retirement" / "Miller Retirement" subtree under a sub-client),
        OR
      - the immediate child of the sub-client (the person fallback —
        unused by default, since adding it would surface 5+ person
        entities under Miller; not yet enabled).
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT node_id, parent_node_id, alias, name, group_node_id FROM entity"
        )
        nodes = {
            r["node_id"]: (r["parent_node_id"], r["alias"], r["name"], r["group_node_id"])
            for r in cur.fetchall()
        }

    sub_clients = {nid for nid, (pid, _, _, _) in nodes.items() if pid == root_node_id}

    def is_retirement_grouping(alias, name):
        # Matches the sub-client-level grouping nodes (e.g. "Dyne US
        # Retirement", "Markiles Retirement"). Sub-account aliases like
        # "Murray Markiles Roth IRA" or "Mark Dyne Roth IRA" don't
        # contain "retirement" and so don't match — walk skips them
        # and picks the grouping node above instead, which is what we
        # want as the Entity filter label.
        return (
            "retirement" in (alias or "").lower()
            or "retirement" in (name or "").lower()
        )

    shared_vehicle_nodes = _detect_shared_vehicle_nodes(nodes)

    rows = []
    for nid in nodes:
        sub_client_nid = sub_client_alias = entity_nid = entity_alias = None
        path = []
        cur_id = nid
        for _ in range(50):
            if not cur_id or cur_id == "_":
                break
            pid, alias, name, _ = nodes.get(cur_id, (None, None, None, None))
            path.append(alias or name or cur_id)
            if cur_id in sub_clients:
                sub_client_nid = cur_id
                sub_client_alias = nodes[cur_id][1] or nodes[cur_id][2]
            # First-encountered entity wins. The walk goes leaf-to-root, so
            # a shared vehicle directly above a Goldman account (e.g. Modyl
            # LP) beats a higher-up trust (Mark I Dyne 2010). For accounts
            # not under any shared vehicle, the first trust ancestor wins.
            # Retirement groupings are checked too so IRAs / 401Ks held
            # directly under a person (no trust between) get attributed
            # to e.g. "Markiles Retirement" instead of NULL.
            if entity_nid is None and cur_id != nid and (
                cur_id in shared_vehicle_nodes
                or _is_trust(alias, name)
                or is_retirement_grouping(alias, name)
            ):
                entity_nid = cur_id
                entity_alias = alias or name
            cur_id = pid
        if entity_nid is None and _is_trust(nodes[nid][1], nodes[nid][2]):
            entity_nid = nid
            entity_alias = nodes[nid][1] or nodes[nid][2]
        rows.append((nid, sub_client_nid, sub_client_alias,
                     entity_nid, entity_alias, " > ".join(reversed(path))))

    with conn.cursor() as cur:
        cur.executemany(
            """INSERT INTO entity_attribution
                 (node_id, sub_client_node_id, sub_client_alias,
                  trust_node_id, trust_alias, family_path)
               VALUES (%s,%s,%s,%s,%s,%s)
               ON CONFLICT (node_id) DO UPDATE SET
                 sub_client_node_id = EXCLUDED.sub_client_node_id,
                 sub_client_alias   = EXCLUDED.sub_client_alias,
                 trust_node_id      = EXCLUDED.trust_node_id,
                 trust_alias        = EXCLUDED.trust_alias,
                 family_path        = EXCLUDED.family_path
            """,
            rows,
        )
    conn.commit()
    log_sync(conn, "attribution", None, f"root={root_node_id}", len(rows))
    return len(rows)


def canonical_accounts_under(
    conn,
    scope_node_id: str,
    all_family_roots: list[str] | None = None,
) -> list[str]:
    """Canonical investment-account node_ids inside scope_node_id.

    Previously this function ALSO stripped accounts under cross-family
    shared vehicles (Dendell, Deltrust, etc.) to avoid double-counting.
    That's no longer needed: mark_canonical_accounts keeps ALL
    reflections of shared-multi-trust vehicles as canonical, which means
    each family's daily sync naturally ingests its own per-trust slice
    of the shared vehicle (different node_ids, no PK conflicts, totals
    sum correctly).

    `all_family_roots` is now accepted but ignored — kept on the
    signature so existing callers don't break.
    """
    _ = all_family_roots  # intentionally unused; see docstring
    with conn.cursor() as cur:
        cur.execute(
            """WITH RECURSIVE descendants AS (
                   SELECT node_id FROM entity WHERE node_id = %s
                 UNION
                   SELECT e.node_id
                   FROM entity e JOIN descendants d
                     ON e.parent_node_id = d.node_id
               )
               SELECT e.node_id FROM entity e
               JOIN descendants d ON e.node_id = d.node_id
               WHERE e.is_canonical_account = TRUE
            """,
            (scope_node_id,),
        )
        return [r["node_id"] for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# Securities, positions, transactions, pricing
# ---------------------------------------------------------------------------

def upsert_securities(conn, holdings_payload: list[dict],
                       txns_payload: list[dict] | None = None) -> int:
    """Build/update security master from Holdings + Transactions payloads.
    First non-empty value per field wins (gaps filled in later passes)."""
    by_sid: dict[int, dict] = {}
    for payload in (holdings_payload or [], txns_payload or []):
        for h in payload:
            sid = h.get("securityId")
            if sid is None:
                continue
            rec = by_sid.setdefault(sid, {})
            for k, v in [
                ("asset_name",          h.get("assetName")),
                ("asset_class",         h.get("assetClass")),
                ("security_type",       h.get("securityType")),
                ("sector",              h.get("sector")),
                ("geographic_exposure", h.get("geographicExposure")),
                ("isin",  (h.get("isin")  or "").strip() or None),
                ("sedol", (h.get("sedol") or "").strip() or None),
                ("cusip", (h.get("cusip") or "").strip() or None),
                ("ticker_masttro",      h.get("ticker")),
                ("local_ccy",           h.get("localCCY")),
            ]:
                if v and not rec.get(k):
                    rec[k] = v

    rows = [
        (sid, v.get("asset_name"), v.get("asset_class"), v.get("security_type"),
         v.get("sector"), v.get("geographic_exposure"), v.get("isin"),
         v.get("sedol"), v.get("cusip"), v.get("ticker_masttro"), v.get("local_ccy"))
        for sid, v in by_sid.items()
    ]
    if not rows:
        return 0

    with conn.cursor() as cur:
        cur.executemany(
            """INSERT INTO security
                 (security_id, asset_name, asset_class, security_type, sector,
                  geographic_exposure, isin, sedol, cusip, ticker_masttro, local_ccy)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT (security_id) DO UPDATE SET
                 asset_name          = COALESCE(EXCLUDED.asset_name,          security.asset_name),
                 asset_class         = COALESCE(EXCLUDED.asset_class,         security.asset_class),
                 security_type       = COALESCE(EXCLUDED.security_type,       security.security_type),
                 sector              = COALESCE(EXCLUDED.sector,              security.sector),
                 geographic_exposure = COALESCE(EXCLUDED.geographic_exposure, security.geographic_exposure),
                 isin                = COALESCE(EXCLUDED.isin,                security.isin),
                 sedol               = COALESCE(EXCLUDED.sedol,               security.sedol),
                 cusip               = COALESCE(EXCLUDED.cusip,               security.cusip),
                 ticker_masttro      = COALESCE(EXCLUDED.ticker_masttro,      security.ticker_masttro),
                 local_ccy           = COALESCE(EXCLUDED.local_ccy,           security.local_ccy)
            """,
            rows,
        )
    conn.commit()
    log_sync(conn, "securities", None, f"distinct={len(rows)}", len(rows))
    return len(rows)


def upsert_positions(conn, holdings_payload: list[dict],
                      canonical_accounts: list[str],
                      reporting_ccy: str = "USD") -> dict:
    """Upsert Holdings rows into position_snapshot, filtered to canonical accounts."""
    canonical = set(canonical_accounts)
    inserted = skipped_acct = skipped_sid = 0
    rows = []
    for h in holdings_payload or []:
        if h.get("nodeId") not in canonical:
            skipped_acct += 1
            continue
        if h.get("securityId") is None:
            skipped_sid += 1
            continue
        rows.append((
            yymmdd_to_iso(h.get("date")),
            h["nodeId"], h["securityId"],
            _to_float(h.get("quantity")),
            _to_float(h.get("price")),
            _to_float(h.get("localMarketValue")),
            _to_float(h.get("marketValue")),
            reporting_ccy,
            _to_float(h.get("localAccruedInterest")),
            _to_float(h.get("accruedInterest")),
            _to_float(h.get("unitCost")),
            _to_float(h.get("totalCost")),
        ))
        inserted += 1

    if rows:
        with conn.cursor() as cur:
            cur.executemany(
                """INSERT INTO position_snapshot
                     (snapshot_date, account_node_id, security_id, quantity, price_local,
                      mv_local, mv_reporting, reporting_ccy,
                      accrued_interest_local, accrued_interest_reporting,
                      unit_cost_local, total_cost_local)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT (snapshot_date, account_node_id, security_id) DO UPDATE SET
                     quantity                   = EXCLUDED.quantity,
                     price_local                = EXCLUDED.price_local,
                     mv_local                   = EXCLUDED.mv_local,
                     mv_reporting               = EXCLUDED.mv_reporting,
                     reporting_ccy              = EXCLUDED.reporting_ccy,
                     accrued_interest_local     = EXCLUDED.accrued_interest_local,
                     accrued_interest_reporting = EXCLUDED.accrued_interest_reporting,
                     unit_cost_local            = EXCLUDED.unit_cost_local,
                     total_cost_local           = EXCLUDED.total_cost_local
                """,
                rows,
            )
        conn.commit()
    log_sync(conn, "positions", None,
             f"inserted={inserted} skip_acct={skipped_acct} skip_sid={skipped_sid}",
             inserted)
    return {"inserted": inserted, "skipped_acct": skipped_acct, "skipped_sid": skipped_sid}


def upsert_transactions(conn, txns_payload: list[dict],
                         canonical_accounts: list[str],
                         reporting_ccy: str = "USD") -> dict:
    """Upsert /Transactions rows. ON CONFLICT DO NOTHING against the natural
    dedup key (see migration 001) — re-runs are idempotent, new trades append."""
    canonical = set(canonical_accounts)
    submitted = skipped = 0
    rows = []
    for t in txns_payload or []:
        if t.get("nodeId") not in canonical:
            skipped += 1
            continue
        ttype_raw = t.get("transactionType") or ""
        ttype_clean = ttype_raw.strip()
        rows.append((
            yymmdd_to_iso(t.get("transactionDate")),
            yymmdd_to_iso(t.get("date")),
            t["nodeId"], t.get("securityId"),
            ttype_raw, ttype_clean,
            t.get("gwmInExType"), t.get("invVehicle"), t.get("invVehicleCode"),
            t.get("comments"),
            _to_float(t.get("quantity")),
            _to_float(t.get("netPriceLocalCCY")),
            _to_float(t.get("netAmountLocalCCY")),
            _to_float(t.get("netAmountRepCCY")),
            t.get("localCCY"), reporting_ccy,
            ttype_clean in EXTERNAL_FLOW_TYPES,
        ))
        submitted += 1

    if rows:
        with conn.cursor() as cur:
            cur.executemany(
                """INSERT INTO transaction_log
                     (transaction_date, snapshot_date, account_node_id, security_id,
                      transaction_type, transaction_type_clean, gwm_in_ex_type,
                      inv_vehicle, inv_vehicle_code, comments,
                      quantity, net_price_local, net_amount_local, net_amount_reporting,
                      local_ccy, reporting_ccy, is_external_flow)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT ON CONSTRAINT transaction_log_dedup_uniq DO NOTHING
                """,
                rows,
            )
        conn.commit()
    log_sync(conn, "transactions", None,
             f"submitted={submitted} skip_acct={skipped}", submitted)
    return {"submitted": submitted, "skipped_acct": skipped}


def insert_pricing_refresh(conn, rows: list[tuple]) -> int:
    """Upsert pricing_refresh rows on (refresh_date, ticker_yf)."""
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """INSERT INTO pricing_refresh
                 (refresh_date, ticker_yf, security_id, price, price_previous,
                  price_ccy, yf_as_of_date, yf_previous_date, source)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT (refresh_date, ticker_yf) DO UPDATE SET
                 security_id      = EXCLUDED.security_id,
                 price            = EXCLUDED.price,
                 price_previous   = EXCLUDED.price_previous,
                 yf_as_of_date    = EXCLUDED.yf_as_of_date,
                 yf_previous_date = EXCLUDED.yf_previous_date,
                 source           = EXCLUDED.source
            """,
            rows,
        )
    conn.commit()
    return len(rows)


def set_security_ticker_yf(conn, updates: list[tuple]) -> int:
    """updates: list of (ticker_yf, ticker_yf_source, security_id)."""
    if not updates:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            "UPDATE security SET ticker_yf = %s, ticker_yf_source = %s WHERE security_id = %s",
            updates,
        )
    conn.commit()
    return len(updates)
