"""One-time migration: copy the local SQLite tracker DB into Supabase.

Reads from data/tracker.db, UPSERTs every row into Postgres. Idempotent —
safe to re-run if you change schema or want to repair a partial migration.
Skips sync_log (each environment has its own local audit trail).

Run order:
    1. scripts/apply_schema.py          (creates tables in Supabase)
    2. scripts/migrate_sqlite_to_supabase.py  (this script — seeds history)
    3. From here, daily/weekly GitHub Actions take over.

Env: SUPABASE_DB_URL.
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.stdout.reconfigure(encoding="utf-8")

from tracker import DEFAULT_DB_PATH
from tracker.db import connect as connect_pg


def copy_table(sq, pg, label: str, select_sql: str,
                insert_sql: str, transform=None) -> None:
    rows = sq.execute(select_sql).fetchall()
    if transform:
        rows = [transform(r) for r in rows]
    if rows:
        with pg.cursor() as cur:
            cur.executemany(insert_sql, rows)
        pg.commit()
    with pg.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) AS n FROM {label}")
        pg_count = cur.fetchone()["n"]
    print(f"  {label:<22} {len(rows):>6} sent  →  {pg_count:>6} in Postgres")


def main() -> int:
    sqlite_path = DEFAULT_DB_PATH
    if not sqlite_path.exists():
        print(f"ERROR: SQLite DB not found at {sqlite_path}")
        print("Run scripts/12_build_tracker_db.py first.")
        return 1

    print(f"Source: {sqlite_path}")
    print("Target: Supabase (from SUPABASE_DB_URL)\n")

    sq = sqlite3.connect(sqlite_path)
    pg = connect_pg()

    try:
        copy_table(sq, pg, "entity",
            """SELECT node_id, parent_node_id, alias, name, bank_broker, account_number,
                      ownership_pct, is_account, is_canonical_account,
                      gwm_valuation, gwm_valuation_ccy, snapshot_date, status
               FROM entity""",
            """INSERT INTO entity (node_id, parent_node_id, alias, name, bank_broker,
                                    account_number, ownership_pct, is_account,
                                    is_canonical_account, gwm_valuation,
                                    gwm_valuation_ccy, snapshot_date, status)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT (node_id) DO UPDATE SET
                 parent_node_id       = EXCLUDED.parent_node_id,
                 alias                = EXCLUDED.alias,
                 name                 = EXCLUDED.name,
                 bank_broker          = EXCLUDED.bank_broker,
                 account_number       = EXCLUDED.account_number,
                 ownership_pct        = EXCLUDED.ownership_pct,
                 is_account           = EXCLUDED.is_account,
                 is_canonical_account = EXCLUDED.is_canonical_account,
                 gwm_valuation        = EXCLUDED.gwm_valuation,
                 gwm_valuation_ccy    = EXCLUDED.gwm_valuation_ccy,
                 snapshot_date        = EXCLUDED.snapshot_date,
                 status               = EXCLUDED.status""",
            transform=lambda r: (r[0], r[1], r[2], r[3], r[4], r[5], r[6],
                                 bool(r[7]), bool(r[8]), r[9], r[10], r[11], r[12]))

        copy_table(sq, pg, "entity_attribution",
            """SELECT node_id, sub_client_node_id, sub_client_alias,
                      trust_node_id, trust_alias, family_path
               FROM entity_attribution""",
            """INSERT INTO entity_attribution
                 (node_id, sub_client_node_id, sub_client_alias,
                  trust_node_id, trust_alias, family_path)
               VALUES (%s,%s,%s,%s,%s,%s)
               ON CONFLICT (node_id) DO UPDATE SET
                 sub_client_node_id = EXCLUDED.sub_client_node_id,
                 sub_client_alias   = EXCLUDED.sub_client_alias,
                 trust_node_id      = EXCLUDED.trust_node_id,
                 trust_alias        = EXCLUDED.trust_alias,
                 family_path        = EXCLUDED.family_path""")

        copy_table(sq, pg, "security",
            """SELECT security_id, asset_name, asset_class, security_type, sector,
                      geographic_exposure, isin, sedol, cusip, ticker_masttro,
                      ticker_yf, ticker_yf_source, local_ccy
               FROM security""",
            """INSERT INTO security
                 (security_id, asset_name, asset_class, security_type, sector,
                  geographic_exposure, isin, sedol, cusip, ticker_masttro,
                  ticker_yf, ticker_yf_source, local_ccy)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT (security_id) DO UPDATE SET
                 asset_name          = EXCLUDED.asset_name,
                 asset_class         = EXCLUDED.asset_class,
                 security_type       = EXCLUDED.security_type,
                 sector              = EXCLUDED.sector,
                 geographic_exposure = EXCLUDED.geographic_exposure,
                 isin                = EXCLUDED.isin,
                 sedol               = EXCLUDED.sedol,
                 cusip               = EXCLUDED.cusip,
                 ticker_masttro      = EXCLUDED.ticker_masttro,
                 ticker_yf           = EXCLUDED.ticker_yf,
                 ticker_yf_source    = EXCLUDED.ticker_yf_source,
                 local_ccy           = EXCLUDED.local_ccy""")

        copy_table(sq, pg, "position_snapshot",
            """SELECT snapshot_date, account_node_id, security_id, quantity,
                      price_local, mv_local, mv_reporting, reporting_ccy,
                      accrued_interest_local, accrued_interest_reporting,
                      unit_cost_local, total_cost_local
               FROM position_snapshot""",
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
                 total_cost_local           = EXCLUDED.total_cost_local""")

        # transaction_log: skip transaction_id (BIGSERIAL in PG generates it).
        # is_external_flow is INTEGER 0/1 in SQLite → BOOLEAN in PG.
        copy_table(sq, pg, "transaction_log",
            """SELECT transaction_date, snapshot_date, account_node_id, security_id,
                      transaction_type, transaction_type_clean, gwm_in_ex_type,
                      inv_vehicle, inv_vehicle_code, comments,
                      quantity, net_price_local, net_amount_local, net_amount_reporting,
                      local_ccy, reporting_ccy, is_external_flow
               FROM transaction_log""",
            """INSERT INTO transaction_log
                 (transaction_date, snapshot_date, account_node_id, security_id,
                  transaction_type, transaction_type_clean, gwm_in_ex_type,
                  inv_vehicle, inv_vehicle_code, comments,
                  quantity, net_price_local, net_amount_local, net_amount_reporting,
                  local_ccy, reporting_ccy, is_external_flow)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT ON CONSTRAINT transaction_log_dedup_uniq DO NOTHING""",
            transform=lambda r: (*r[:-1], bool(r[-1])))

        copy_table(sq, pg, "pricing_refresh",
            """SELECT refresh_date, ticker_yf, security_id, price, price_previous,
                      price_ccy, yf_as_of_date, yf_previous_date, source
               FROM pricing_refresh""",
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
                 source           = EXCLUDED.source""")

    finally:
        sq.close()
        pg.close()

    print("\nDone. Verify in Supabase SQL editor:")
    print("  SELECT COUNT(*) FROM position_snapshot;")
    print("  SELECT * FROM v_latest_positions LIMIT 5;")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
