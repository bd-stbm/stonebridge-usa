"""SQLite schema for the portfolio tracker.

Postgres-compatible: uses TEXT for dates (ISO YYYY-MM-DD), no AUTOINCREMENT
on PKs that don't need it, no SQLite-specific functions.
"""

SCHEMA_SQL = """
-- =====================================================================
-- entity — GWM tree (one row per GWM node)
-- =====================================================================
CREATE TABLE IF NOT EXISTS entity (
    node_id              TEXT PRIMARY KEY,
    parent_node_id       TEXT,
    alias                TEXT,
    name                 TEXT,
    bank_broker          TEXT,
    account_number       TEXT,
    ownership_pct        REAL,
    is_account           INTEGER NOT NULL DEFAULT 0,  -- 1 if a custodian account
    is_canonical_account INTEGER NOT NULL DEFAULT 0,  -- 1 if this is the canonical
                                                     -- (deduped) account for its
                                                     -- (bank, acct#) fingerprint
    gwm_valuation        REAL,
    gwm_valuation_ccy    TEXT,
    snapshot_date        TEXT,   -- ISO YYYY-MM-DD (when GWM was pulled)
    status               TEXT
);
CREATE INDEX IF NOT EXISTS entity_parent_idx ON entity(parent_node_id);
CREATE INDEX IF NOT EXISTS entity_is_account_idx ON entity(is_account);
CREATE INDEX IF NOT EXISTS entity_canonical_idx ON entity(is_canonical_account);

-- =====================================================================
-- entity_attribution — for each node, the sub-client / trust / family path
-- =====================================================================
CREATE TABLE IF NOT EXISTS entity_attribution (
    node_id              TEXT PRIMARY KEY,
    sub_client_node_id   TEXT,
    sub_client_alias     TEXT,
    trust_node_id        TEXT,   -- nearest trust ancestor, if any
    trust_alias          TEXT,
    family_path          TEXT    -- "Stonebridge > Dyne Family US > … > Account"
);

-- =====================================================================
-- security — security master (one row per Masttro securityId)
-- =====================================================================
CREATE TABLE IF NOT EXISTS security (
    security_id          INTEGER PRIMARY KEY,
    asset_name           TEXT,
    asset_class          TEXT,
    security_type        TEXT,
    sector               TEXT,
    geographic_exposure  TEXT,
    isin                 TEXT,
    sedol                TEXT,
    cusip                TEXT,
    ticker_masttro       TEXT,
    ticker_yf            TEXT,   -- normalised yfinance ticker (set by enrich)
    ticker_yf_source     TEXT,   -- 'masttro' / 'openfigi' / NULL
    local_ccy            TEXT
);
CREATE INDEX IF NOT EXISTS security_isin_idx ON security(isin);
CREATE INDEX IF NOT EXISTS security_ticker_yf_idx ON security(ticker_yf);
CREATE INDEX IF NOT EXISTS security_assetclass_idx ON security(asset_class);

-- =====================================================================
-- position_snapshot — one row per (snapshot_date, account, security)
-- =====================================================================
CREATE TABLE IF NOT EXISTS position_snapshot (
    snapshot_date              TEXT NOT NULL,   -- ISO YYYY-MM-DD
    account_node_id            TEXT NOT NULL,
    security_id                INTEGER NOT NULL,
    quantity                   REAL,
    price_local                REAL,
    mv_local                   REAL,
    mv_reporting               REAL,
    reporting_ccy              TEXT,
    accrued_interest_local     REAL,
    accrued_interest_reporting REAL,
    unit_cost_local            REAL,
    total_cost_local           REAL,
    PRIMARY KEY (snapshot_date, account_node_id, security_id)
);
CREATE INDEX IF NOT EXISTS pos_snap_account_date_idx
    ON position_snapshot(account_node_id, snapshot_date);
CREATE INDEX IF NOT EXISTS pos_snap_security_date_idx
    ON position_snapshot(security_id, snapshot_date);
CREATE INDEX IF NOT EXISTS pos_snap_date_idx
    ON position_snapshot(snapshot_date);

-- =====================================================================
-- transaction_log — one row per transaction event
-- =====================================================================
CREATE TABLE IF NOT EXISTS transaction_log (
    transaction_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    transaction_date        TEXT,   -- ISO YYYY-MM-DD
    snapshot_date           TEXT,   -- snapshot the response carried
    account_node_id         TEXT NOT NULL,
    security_id             INTEGER,
    transaction_type        TEXT,   -- raw from Masttro
    transaction_type_clean  TEXT,   -- .strip()ed
    gwm_in_ex_type          TEXT,
    inv_vehicle             TEXT,
    inv_vehicle_code        TEXT,
    comments                TEXT,
    quantity                REAL,
    net_price_local         REAL,
    net_amount_local        REAL,
    net_amount_reporting    REAL,
    local_ccy               TEXT,
    reporting_ccy           TEXT,
    is_external_flow        INTEGER NOT NULL DEFAULT 0
        -- 1 for transactions that are external capital flows (Deposit /
        -- Withdrawal). Used as the TWR denominator-adjustment series.
);
CREATE INDEX IF NOT EXISTS txn_account_date_idx
    ON transaction_log(account_node_id, transaction_date);
CREATE INDEX IF NOT EXISTS txn_type_idx
    ON transaction_log(transaction_type_clean);
CREATE INDEX IF NOT EXISTS txn_external_idx
    ON transaction_log(is_external_flow);

-- =====================================================================
-- pricing_refresh — yfinance prices, keyed by ticker_yf
-- =====================================================================
CREATE TABLE IF NOT EXISTS pricing_refresh (
    refresh_date      TEXT NOT NULL,   -- when WE pulled the price
    ticker_yf         TEXT NOT NULL,
    security_id       INTEGER,
    price             REAL,            -- latest close from yfinance
    price_previous    REAL,            -- close just before that (for 1-day return)
    price_ccy         TEXT,
    yf_as_of_date     TEXT,            -- date of the latest yfinance close
    yf_previous_date  TEXT,            -- date of the previous close
    source            TEXT,            -- 'yfinance' / 'openfigi+yfinance'
    PRIMARY KEY (refresh_date, ticker_yf)
);
CREATE INDEX IF NOT EXISTS pricing_security_date_idx
    ON pricing_refresh(security_id, refresh_date);

-- =====================================================================
-- sync_log — audit trail of ingestion / refresh runs
-- =====================================================================
CREATE TABLE IF NOT EXISTS sync_log (
    sync_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    sync_timestamp   TEXT NOT NULL,  -- ISO datetime
    sync_type        TEXT NOT NULL,  -- 'gwm' / 'positions' / 'transactions' /
                                     -- 'pricing_refresh' / 'attribution'
    scope            TEXT,           -- e.g. family name or 'all'
    description      TEXT,
    rows_affected    INTEGER
);
"""


def create_tables(conn) -> None:
    """Apply the schema to a SQLite connection."""
    conn.executescript(SCHEMA_SQL)
    _migrate(conn)
    conn.commit()


def _migrate(conn) -> None:
    """Apply schema migrations to existing DBs. Idempotent — safe to re-run."""
    migrations = [
        "ALTER TABLE pricing_refresh ADD COLUMN price_previous REAL",
        "ALTER TABLE pricing_refresh ADD COLUMN yf_previous_date TEXT",
    ]
    import sqlite3 as _sqlite3
    for sql in migrations:
        try:
            conn.execute(sql)
        except _sqlite3.OperationalError as e:
            if "duplicate column name" not in str(e).lower():
                raise
    conn.commit()


def drop_all(conn) -> None:
    """Drop all tracker tables. Use only for clean rebuilds in dev."""
    tables = [
        "sync_log", "pricing_refresh", "transaction_log",
        "position_snapshot", "security", "entity_attribution", "entity",
    ]
    for t in tables:
        conn.execute(f"DROP TABLE IF EXISTS {t}")
    conn.commit()
