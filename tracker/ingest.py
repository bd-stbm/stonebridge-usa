"""Ingest saved Masttro JSON responses into the tracker SQLite DB.

Filters positions and transactions to **directly-held investment accounts**
(public positions only) — same scope as the Dylan/Morgan trust spreadsheets.

Dedupes accounts via (bank_broker, account_number) fingerprint to handle the
full-duplication beneficial-ownership pattern (e.g. Dyne 2020 Irrevocable
Trust appearing under both Dylan and Morgan).
"""

from __future__ import annotations

import datetime as dt
import json
import sqlite3
from pathlib import Path

from tracker import DEFAULT_DB_PATH, RESPONSES_DIR
from tracker.schema import create_tables

EXTERNAL_FLOW_TYPES = {"Deposit", "Withdrawal"}
ROOT_NODE_ID = "0_7693"


def yymmdd_to_iso(s) -> str | None:
    """20260513 -> '2026-05-13'. Returns None for empty/invalid."""
    if s is None:
        return None
    s = str(s)
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:]}"
    return s or None


def _to_float(v):
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def latest_response(pattern: str) -> Path | None:
    matches = sorted(RESPONSES_DIR.glob(pattern))
    return matches[-1] if matches else None


def load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def connect(db_path: Path = DEFAULT_DB_PATH) -> sqlite3.Connection:
    """Open the tracker DB, applying schema if needed."""
    db_path.parent.mkdir(exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    create_tables(conn)
    return conn


def _log(conn, sync_type: str, scope: str | None, description: str, rows: int) -> None:
    conn.execute(
        """INSERT INTO sync_log (sync_timestamp, sync_type, scope, description, rows_affected)
           VALUES (?, ?, ?, ?, ?)""",
        (dt.datetime.utcnow().isoformat(timespec="seconds"), sync_type, scope, description, rows),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Entity tree + attribution
# ---------------------------------------------------------------------------

def ingest_gwm(conn, gwm_file: Path | None = None) -> int:
    """Load entity tree from a /GWM response. Sets is_account flag."""
    gwm_file = gwm_file or latest_response("GWM-7693_*.json")
    if not gwm_file:
        raise FileNotFoundError("No GWM response on disk.")
    gwm = load_json(gwm_file)

    snapshot_iso = yymmdd_to_iso(gwm[0].get("date")) if gwm else None
    rows = []
    for n in gwm:
        bb = (n.get("bankBroker") or "").strip()
        an = (n.get("accountNumber") or "").strip()
        is_acct = 1 if (bb and an) else 0
        rows.append((
            n["nodeId"],
            n.get("parentNodeId"),
            n.get("alias"),
            n.get("name"),
            bb or None,
            an or None,
            n.get("ownershipPct"),
            is_acct,
            0,  # is_canonical_account — set in a second pass
            n.get("valuation"),
            "AUD",  # GWM was pulled with ccy=AUD historically
            snapshot_iso,
            n.get("status"),
        ))

    conn.executemany(
        """INSERT OR REPLACE INTO entity
           (node_id, parent_node_id, alias, name, bank_broker, account_number,
            ownership_pct, is_account, is_canonical_account,
            gwm_valuation, gwm_valuation_ccy, snapshot_date, status)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        rows,
    )
    conn.commit()

    # Second pass: mark canonical accounts (one per bank+acct# fingerprint).
    # Pick the lexicographically smallest node_id as canonical for determinism.
    conn.execute("UPDATE entity SET is_canonical_account = 0 WHERE is_account = 1")
    conn.execute("""
        WITH ranked AS (
            SELECT node_id,
                   ROW_NUMBER() OVER (
                       PARTITION BY bank_broker, account_number
                       ORDER BY node_id
                   ) AS rn
            FROM entity
            WHERE is_account = 1
        )
        UPDATE entity SET is_canonical_account = 1
        WHERE node_id IN (SELECT node_id FROM ranked WHERE rn = 1)
    """)
    conn.commit()

    _log(conn, "gwm", None, str(gwm_file.name), len(rows))
    return len(rows)


def ingest_attribution(conn, root_node_id: str = ROOT_NODE_ID) -> int:
    """Walk the entity tree from root, attribute each node to its sub-client
    (direct child of root) and nearest trust ancestor."""
    cur = conn.cursor()

    # Build parent map + node info in memory.
    cur.execute("SELECT node_id, parent_node_id, alias, name FROM entity")
    nodes = {nid: (pid, alias, name) for nid, pid, alias, name in cur.fetchall()}

    # Direct children of root = sub-clients.
    sub_clients = {nid for nid, (pid, _, _) in nodes.items() if pid == root_node_id}

    def is_trust(alias: str | None, name: str | None) -> bool:
        return "trust" in (alias or "").lower() or "trust" in (name or "").lower()

    rows = []
    for nid in nodes:
        sub_client_nid = None
        sub_client_alias = None
        trust_nid = None
        trust_alias = None
        path_labels = []

        cur_id = nid
        for _ in range(50):
            if cur_id is None or cur_id == "_":
                break
            pid, alias, name = nodes.get(cur_id, (None, None, None))
            label = alias or name or cur_id
            path_labels.append(label)
            if cur_id in sub_clients:
                sub_client_nid = cur_id
                sub_client_alias = nodes[cur_id][1] or nodes[cur_id][2]
            # Nearest trust ancestor (only first one encountered)
            if trust_nid is None and is_trust(alias, name) and cur_id != nid:
                trust_nid = cur_id
                trust_alias = alias or name
            cur_id = pid

        # If the node itself is a trust, record it too.
        if trust_nid is None and is_trust(nodes[nid][1], nodes[nid][2]):
            trust_nid = nid
            trust_alias = nodes[nid][1] or nodes[nid][2]

        family_path = " > ".join(reversed(path_labels))
        rows.append((nid, sub_client_nid, sub_client_alias, trust_nid, trust_alias, family_path))

    conn.executemany(
        """INSERT OR REPLACE INTO entity_attribution
           (node_id, sub_client_node_id, sub_client_alias, trust_node_id, trust_alias, family_path)
           VALUES (?, ?, ?, ?, ?, ?)""",
        rows,
    )
    conn.commit()
    _log(conn, "attribution", None, f"root={root_node_id}", len(rows))
    return len(rows)


# ---------------------------------------------------------------------------
# Account scoping — find the canonical investment accounts under a scope
# ---------------------------------------------------------------------------

def canonical_account_ids_under(conn, scope_node_id: str) -> list[str]:
    """Return the node_ids of canonical (deduped) investment accounts in scope.

    Includes only accounts that sit directly under a 'trust' node — i.e. excludes
    SPV-held accounts, retirement vehicles without 'trust' in their name, etc.

    Set scope_node_id to a sub-client to get all trusts under that sub-client.
    Set to a specific trust to get just that trust's accounts.
    """
    cur = conn.cursor()

    # Walk descendants.
    cur.execute("SELECT node_id, parent_node_id FROM entity")
    parent_of = dict(cur.fetchall())
    children_of: dict[str, list[str]] = {}
    for nid, pid in parent_of.items():
        children_of.setdefault(pid, []).append(nid)

    descendants = {scope_node_id}
    stack = [scope_node_id]
    while stack:
        nid = stack.pop()
        for c in children_of.get(nid, []):
            if c not in descendants:
                descendants.add(c)
                stack.append(c)

    # Identify trust nodes within the scope's subtree.
    placeholders = ",".join("?" * len(descendants))
    cur.execute(
        f"""SELECT node_id, alias, name FROM entity
            WHERE node_id IN ({placeholders})""",
        list(descendants),
    )
    trust_nodes = []
    for n_id, alias, name in cur.fetchall():
        if "trust" in (alias or "").lower() or "trust" in (name or "").lower():
            trust_nodes.append(n_id)

    if not trust_nodes:
        return []

    # Canonical direct-child accounts of those trusts.
    cur.execute(
        f"""SELECT node_id FROM entity
            WHERE parent_node_id IN ({",".join("?" * len(trust_nodes))})
              AND is_account = 1
              AND is_canonical_account = 1""",
        trust_nodes,
    )
    return [r[0] for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# Securities, positions, transactions
# ---------------------------------------------------------------------------

def ingest_securities_from(conn, sources: list[Path]) -> int:
    """Build/update security master from one or more Holdings/Transactions JSON files."""
    by_sid = {}
    for src in sources:
        if not src or not src.exists():
            continue
        data = load_json(src)
        for h in data:
            sid = h.get("securityId")
            if sid is None:
                continue
            # First occurrence wins; later occurrences only fill missing fields
            if sid not in by_sid:
                by_sid[sid] = {
                    "asset_name": h.get("assetName"),
                    "asset_class": h.get("assetClass"),
                    "security_type": h.get("securityType"),
                    "sector": h.get("sector"),
                    "geographic_exposure": h.get("geographicExposure"),
                    "isin": (h.get("isin") or "").strip() or None,
                    "sedol": (h.get("sedol") or "").strip() or None,
                    "cusip": (h.get("cusip") or "").strip() or None,
                    "ticker_masttro": h.get("ticker"),
                    "local_ccy": h.get("localCCY"),
                }
            else:
                existing = by_sid[sid]
                for k, src_key in [
                    ("asset_name", "assetName"), ("asset_class", "assetClass"),
                    ("security_type", "securityType"), ("sector", "sector"),
                    ("geographic_exposure", "geographicExposure"),
                    ("ticker_masttro", "ticker"), ("local_ccy", "localCCY"),
                ]:
                    if not existing.get(k) and h.get(src_key):
                        existing[k] = h.get(src_key)
                for k, src_key in [("isin", "isin"), ("sedol", "sedol"), ("cusip", "cusip")]:
                    if not existing.get(k) and (h.get(src_key) or "").strip():
                        existing[k] = h.get(src_key).strip()

    rows = [
        (
            sid, v["asset_name"], v["asset_class"], v["security_type"], v["sector"],
            v["geographic_exposure"], v["isin"], v["sedol"], v["cusip"],
            v["ticker_masttro"], None, None, v["local_ccy"],
        )
        for sid, v in by_sid.items()
    ]
    conn.executemany(
        """INSERT OR REPLACE INTO security
           (security_id, asset_name, asset_class, security_type, sector,
            geographic_exposure, isin, sedol, cusip, ticker_masttro,
            ticker_yf, ticker_yf_source, local_ccy)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        rows,
    )
    conn.commit()
    _log(conn, "securities", None, f"sources={len(sources)}", len(rows))
    return len(rows)


def ingest_positions(conn, holdings_file: Path, canonical_account_ids: list[str],
                     reporting_ccy: str = "USD") -> dict:
    """Load Holdings rows into position_snapshot, filtered to canonical accounts."""
    holdings = load_json(holdings_file)
    canonical = set(canonical_account_ids)

    inserted = skipped_not_account = skipped_no_sid = 0
    rows = []
    for h in holdings:
        if h.get("nodeId") not in canonical:
            skipped_not_account += 1
            continue
        if h.get("securityId") is None:
            skipped_no_sid += 1
            continue
        rows.append((
            yymmdd_to_iso(h.get("date")),
            h.get("nodeId"),
            h.get("securityId"),
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

    conn.executemany(
        """INSERT OR REPLACE INTO position_snapshot
           (snapshot_date, account_node_id, security_id, quantity, price_local,
            mv_local, mv_reporting, reporting_ccy,
            accrued_interest_local, accrued_interest_reporting,
            unit_cost_local, total_cost_local)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        rows,
    )
    conn.commit()
    _log(conn, "positions", holdings_file.name,
         f"inserted={inserted} skip_not_acct={skipped_not_account} skip_no_sid={skipped_no_sid}",
         inserted)
    return {"inserted": inserted, "skipped_not_account": skipped_not_account,
            "skipped_no_sid": skipped_no_sid}


def ingest_transactions(conn, txns_file: Path, canonical_account_ids: list[str],
                        reporting_ccy: str = "USD") -> dict:
    """Load Transactions rows into transaction_log, filtered to canonical accounts.

    Re-ingestion idempotency: deletes prior rows for the source's snapshot_date
    before inserting, so re-running on the same file doesn't double-load.
    """
    txns = load_json(txns_file)
    canonical = set(canonical_account_ids)

    # Determine the snapshot_date of this file so we can wipe prior rows for it
    snapshot_iso = None
    if txns:
        snapshot_iso = yymmdd_to_iso(txns[0].get("date"))
    if snapshot_iso:
        conn.execute(
            "DELETE FROM transaction_log WHERE snapshot_date = ? AND account_node_id IN "
            f"({','.join('?'*len(canonical))})",
            [snapshot_iso] + list(canonical),
        )

    inserted = skipped = 0
    rows = []
    for t in txns:
        if t.get("nodeId") not in canonical:
            skipped += 1
            continue
        ttype_raw = t.get("transactionType") or ""
        ttype_clean = ttype_raw.strip()
        rows.append((
            yymmdd_to_iso(t.get("transactionDate")),
            yymmdd_to_iso(t.get("date")),
            t.get("nodeId"),
            t.get("securityId"),
            ttype_raw,
            ttype_clean,
            t.get("gwmInExType"),
            t.get("invVehicle"),
            t.get("invVehicleCode"),
            t.get("comments"),
            _to_float(t.get("quantity")),
            _to_float(t.get("netPriceLocalCCY")),
            _to_float(t.get("netAmountLocalCCY")),
            _to_float(t.get("netAmountRepCCY")),
            t.get("localCCY"),
            reporting_ccy,
            1 if ttype_clean in EXTERNAL_FLOW_TYPES else 0,
        ))
        inserted += 1

    conn.executemany(
        """INSERT INTO transaction_log
           (transaction_date, snapshot_date, account_node_id, security_id,
            transaction_type, transaction_type_clean, gwm_in_ex_type,
            inv_vehicle, inv_vehicle_code, comments,
            quantity, net_price_local, net_amount_local, net_amount_reporting,
            local_ccy, reporting_ccy, is_external_flow)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        rows,
    )
    conn.commit()
    _log(conn, "transactions", txns_file.name,
         f"inserted={inserted} skip_not_acct={skipped}", inserted)
    return {"inserted": inserted, "skipped_not_account": skipped}


# ---------------------------------------------------------------------------
# Top-level orchestrator
# ---------------------------------------------------------------------------

def build_db_for_family(family_node_id: str, family_label: str,
                        db_path: Path = DEFAULT_DB_PATH) -> dict:
    """Build the tracker DB from on-disk responses for one family."""
    conn = connect(db_path)

    # 1. Entity tree + attribution
    gwm_count = ingest_gwm(conn)
    attr_count = ingest_attribution(conn)

    # 2. Find canonical investment accounts under the family.
    accounts = canonical_account_ids_under(conn, family_node_id)
    if not accounts:
        raise RuntimeError(
            f"No canonical investment accounts found under {family_node_id}. "
            f"Did you ingest GWM first?"
        )

    # 3. Locate the family's saved Holdings + Transactions backfill files.
    holdings_files = sorted(
        RESPONSES_DIR.glob(f"Holdings-7693_*sub{family_node_id}*h12*.json")
    )
    if not holdings_files:
        raise FileNotFoundError(
            f"No 12-month-history Holdings response for {family_node_id}. "
            f"Run scripts/11_us_families_backfill.py first."
        )
    holdings_file = holdings_files[-1]

    txns_files = sorted(
        RESPONSES_DIR.glob(f"Transactions-7693_*sub{family_node_id}*p4*.json")
    )
    txns_file = txns_files[-1] if txns_files else None

    # 4. Security master from both files.
    sec_count = ingest_securities_from(
        conn, [holdings_file] + ([txns_file] if txns_file else [])
    )

    # 5. Positions + transactions.
    pos_stats = ingest_positions(conn, holdings_file, accounts)
    txn_stats = ingest_transactions(conn, txns_file, accounts) if txns_file else {"inserted": 0}

    conn.close()
    return {
        "family": family_label,
        "family_node_id": family_node_id,
        "canonical_accounts": len(accounts),
        "entity_nodes": gwm_count,
        "attribution_rows": attr_count,
        "securities": sec_count,
        "positions": pos_stats,
        "transactions": txn_stats,
    }
