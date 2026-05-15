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
        rows.append((
            n["nodeId"], n.get("parentNodeId"), n.get("alias"), n.get("name"),
            bb, an, n.get("ownershipPct"),
            bool(bb and an), False,
            n.get("valuation"), "AUD", snapshot_iso, n.get("status"),
        ))

    with conn.cursor() as cur:
        cur.executemany(
            """INSERT INTO entity
                 (node_id, parent_node_id, alias, name, bank_broker, account_number,
                  ownership_pct, is_account, is_canonical_account,
                  gwm_valuation, gwm_valuation_ccy, snapshot_date, status)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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
                 status            = EXCLUDED.status
            """,
            rows,
        )
        # Recompute canonical-account flag — one per (bank, account#) fingerprint.
        cur.execute("UPDATE entity SET is_canonical_account = FALSE WHERE is_account = TRUE")
        cur.execute("""
            WITH ranked AS (
                SELECT node_id,
                       ROW_NUMBER() OVER (
                           PARTITION BY bank_broker, account_number ORDER BY node_id
                       ) AS rn
                FROM entity WHERE is_account = TRUE
            )
            UPDATE entity SET is_canonical_account = TRUE
            WHERE node_id IN (SELECT node_id FROM ranked WHERE rn = 1)
        """)
    conn.commit()
    log_sync(conn, "gwm", None, f"snapshot={snapshot_iso}", len(rows))
    return len(rows)


def rebuild_attribution(conn, root_node_id: str = ROOT_NODE_ID) -> int:
    """Recompute entity_attribution from the current entity tree."""
    with conn.cursor() as cur:
        cur.execute("SELECT node_id, parent_node_id, alias, name FROM entity")
        nodes = {r["node_id"]: (r["parent_node_id"], r["alias"], r["name"])
                 for r in cur.fetchall()}

    sub_clients = {nid for nid, (pid, _, _) in nodes.items() if pid == root_node_id}

    def is_trust(alias, name):
        return "trust" in (alias or "").lower() or "trust" in (name or "").lower()

    rows = []
    for nid in nodes:
        sub_client_nid = sub_client_alias = trust_nid = trust_alias = None
        path = []
        cur_id = nid
        for _ in range(50):
            if not cur_id or cur_id == "_":
                break
            pid, alias, name = nodes.get(cur_id, (None, None, None))
            path.append(alias or name or cur_id)
            if cur_id in sub_clients:
                sub_client_nid = cur_id
                sub_client_alias = nodes[cur_id][1] or nodes[cur_id][2]
            if trust_nid is None and is_trust(alias, name) and cur_id != nid:
                trust_nid = cur_id
                trust_alias = alias or name
            cur_id = pid
        if trust_nid is None and is_trust(nodes[nid][1], nodes[nid][2]):
            trust_nid = nid
            trust_alias = nodes[nid][1] or nodes[nid][2]
        rows.append((nid, sub_client_nid, sub_client_alias,
                     trust_nid, trust_alias, " > ".join(reversed(path))))

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


def canonical_accounts_under(conn, scope_node_id: str) -> list[str]:
    """Canonical investment-account node_ids directly under any 'trust' node
    within the scope subtree. Postgres-native via WITH RECURSIVE."""
    with conn.cursor() as cur:
        cur.execute(
            """WITH RECURSIVE descendants AS (
                   SELECT node_id, alias, name FROM entity WHERE node_id = %s
                 UNION
                   SELECT e.node_id, e.alias, e.name
                   FROM entity e JOIN descendants d ON e.parent_node_id = d.node_id
               ),
               trust_nodes AS (
                   SELECT node_id FROM descendants
                   WHERE lower(coalesce(alias,'')) LIKE %s
                      OR lower(coalesce(name,''))  LIKE %s
               )
               SELECT e.node_id FROM entity e
               WHERE e.is_canonical_account = TRUE
                 AND e.parent_node_id IN (SELECT node_id FROM trust_nodes)
            """,
            (scope_node_id, "%trust%", "%trust%"),
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
