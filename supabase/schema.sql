-- =============================================================================
-- Stonebridge tracker — Supabase / Postgres schema
-- =============================================================================
-- Migrated from tracker/schema.py (SQLite). Same table shape, with Postgres-
-- native types (BOOLEAN, DATE, NUMERIC, TIMESTAMPTZ) and updated_at triggers.
--
-- Apply to a Supabase project via the SQL Editor or `supabase db push`.
-- Tables sit in the `public` schema because that's where Supabase exposes its
-- auto-generated PostgREST API. RLS is enabled per table at the bottom; add
-- policies based on your auth model before exposing the API to clients.
-- =============================================================================

BEGIN;

-- -----------------------------------------------------------------------------
-- Shared trigger function for updated_at
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$;

-- =============================================================================
-- entity — GWM tree (one row per GWM node)
-- =============================================================================
CREATE TABLE IF NOT EXISTS public.entity (
    node_id              TEXT PRIMARY KEY,
    parent_node_id       TEXT,            -- '_'/NULL for root; no FK because Masttro
                                          -- sometimes references nodes we haven't ingested yet
    alias                TEXT,
    name                 TEXT,
    bank_broker          TEXT,
    account_number       TEXT,
    ownership_pct        NUMERIC(9, 4),   -- e.g. 100.0000
    is_account           BOOLEAN NOT NULL DEFAULT FALSE,
    is_canonical_account BOOLEAN NOT NULL DEFAULT FALSE,  -- TRUE for one node per
                                                         -- (bank_broker, account_number)
                                                         -- fingerprint; collapses
                                                         -- beneficial-owner duplicates
    gwm_valuation        NUMERIC(20, 4),
    gwm_valuation_ccy    CHAR(3),
    snapshot_date        DATE,            -- when GWM was pulled
    status               TEXT,
    group_node_id        TEXT,            -- Masttro groupNodeId; cross-structure
                                          -- shared vehicles share one groupNodeId
                                          -- across their reflections
    sub_client_node_id   TEXT,            -- denormalised owning family for RLS
                                          -- (migration 028); kept fresh by
                                          -- rebuild_attribution
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS entity_parent_idx ON public.entity (parent_node_id);
CREATE INDEX IF NOT EXISTS entity_is_account_idx
    ON public.entity (is_account) WHERE is_account = TRUE;
CREATE INDEX IF NOT EXISTS entity_canonical_idx
    ON public.entity (is_canonical_account) WHERE is_canonical_account = TRUE;
CREATE INDEX IF NOT EXISTS entity_bank_acct_idx
    ON public.entity (bank_broker, account_number) WHERE is_account = TRUE;
CREATE INDEX IF NOT EXISTS entity_group_node_idx
    ON public.entity (group_node_id) WHERE group_node_id IS NOT NULL;

DROP TRIGGER IF EXISTS entity_set_updated_at ON public.entity;
CREATE TRIGGER entity_set_updated_at BEFORE UPDATE ON public.entity
    FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

COMMENT ON TABLE  public.entity IS 'GWM tree from Masttro /GWM. One row per node.';
COMMENT ON COLUMN public.entity.is_account IS 'TRUE when bank_broker and account_number are both populated.';
COMMENT ON COLUMN public.entity.is_canonical_account IS 'TRUE for the single canonical node per (bank, account#) — dedupes the full-duplication beneficial-ownership pattern (e.g. Dyne 2020 Irrevocable Trust seen via Dylan AND Morgan).';

-- =============================================================================
-- entity_attribution — derived sub-client / trust mapping per node
-- =============================================================================
CREATE TABLE IF NOT EXISTS public.entity_attribution (
    node_id              TEXT PRIMARY KEY REFERENCES public.entity(node_id) ON DELETE CASCADE,
    sub_client_node_id   TEXT,
    sub_client_alias     TEXT,
    trust_node_id        TEXT,            -- nearest 'trust' ancestor
    trust_alias          TEXT,
    family_path          TEXT,            -- e.g. "Stonebridge > Dyne Family US > Dylan Dyne > Dylan Trust"
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS attr_sub_client_idx ON public.entity_attribution (sub_client_node_id);
CREATE INDEX IF NOT EXISTS attr_trust_idx      ON public.entity_attribution (trust_node_id);

DROP TRIGGER IF EXISTS entity_attribution_set_updated_at ON public.entity_attribution;
CREATE TRIGGER entity_attribution_set_updated_at BEFORE UPDATE ON public.entity_attribution
    FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

-- =============================================================================
-- security — security master keyed by Masttro securityId
-- =============================================================================
CREATE TABLE IF NOT EXISTS public.security (
    security_id          BIGINT PRIMARY KEY,
    asset_name           TEXT,
    asset_class          TEXT,
    security_type        TEXT,
    sector               TEXT,
    geographic_exposure  TEXT,
    isin                 TEXT,
    sedol                TEXT,
    cusip                TEXT,
    ticker_masttro       TEXT,
    ticker_yf            TEXT,            -- normalised yfinance ticker
    ticker_yf_source     TEXT,            -- 'masttro' or 'openfigi'
    local_ccy            CHAR(3),
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS security_isin_idx        ON public.security (isin);
CREATE INDEX IF NOT EXISTS security_ticker_yf_idx   ON public.security (ticker_yf);
CREATE INDEX IF NOT EXISTS security_asset_class_idx ON public.security (asset_class);

DROP TRIGGER IF EXISTS security_set_updated_at ON public.security;
CREATE TRIGGER security_set_updated_at BEFORE UPDATE ON public.security
    FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

-- =============================================================================
-- position_snapshot — one row per (snapshot_date, account, security)
-- =============================================================================
CREATE TABLE IF NOT EXISTS public.position_snapshot (
    snapshot_date              DATE NOT NULL,
    account_node_id            TEXT NOT NULL REFERENCES public.entity(node_id) ON DELETE RESTRICT,
    security_id                BIGINT NOT NULL REFERENCES public.security(security_id) ON DELETE RESTRICT,
    quantity                   NUMERIC(24, 8),
    price_local                NUMERIC(20, 8),
    mv_local                   NUMERIC(20, 4),
    mv_reporting               NUMERIC(20, 4),
    reporting_ccy              CHAR(3),
    accrued_interest_local     NUMERIC(20, 4),
    accrued_interest_reporting NUMERIC(20, 4),
    unit_cost_local            NUMERIC(20, 8),
    total_cost_local           NUMERIC(20, 4),
    sub_client_node_id         TEXT,   -- denormalised owning family for RLS
                                       -- (migration 028); set on INSERT by the
                                       -- set_sub_client_node_id() trigger
    created_at                 TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (snapshot_date, account_node_id, security_id)
);
CREATE INDEX IF NOT EXISTS pos_snap_account_date_idx
    ON public.position_snapshot (account_node_id, snapshot_date);
CREATE INDEX IF NOT EXISTS pos_snap_security_date_idx
    ON public.position_snapshot (security_id, snapshot_date);
CREATE INDEX IF NOT EXISTS pos_snap_date_idx
    ON public.position_snapshot (snapshot_date);

-- BRIN index pays off if position_snapshot grows past ~10M rows.
-- Skip until then.
-- CREATE INDEX pos_snap_date_brin ON public.position_snapshot USING BRIN (snapshot_date);

-- =============================================================================
-- transaction_log — one row per transaction event
-- =============================================================================
CREATE TABLE IF NOT EXISTS public.transaction_log (
    transaction_id          BIGSERIAL PRIMARY KEY,
    transaction_date        DATE,
    snapshot_date           DATE,         -- snapshot the response carried
    account_node_id         TEXT NOT NULL REFERENCES public.entity(node_id) ON DELETE RESTRICT,
    security_id             BIGINT REFERENCES public.security(security_id) ON DELETE SET NULL,
    transaction_type        TEXT,
    transaction_type_clean  TEXT,
    gwm_in_ex_type          TEXT,
    inv_vehicle             TEXT,
    inv_vehicle_code        TEXT,
    comments                TEXT,
    quantity                NUMERIC(24, 8),
    net_price_local         NUMERIC(20, 8),
    net_amount_local        NUMERIC(20, 4),
    net_amount_reporting    NUMERIC(20, 4),
    local_ccy               CHAR(3),
    reporting_ccy           CHAR(3),
    is_external_flow        BOOLEAN NOT NULL DEFAULT FALSE,
    sub_client_node_id      TEXT,   -- denormalised owning family for RLS
                                    -- (migration 028); set on INSERT by the
                                    -- set_sub_client_node_id() trigger
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS txn_account_date_idx
    ON public.transaction_log (account_node_id, transaction_date);
CREATE INDEX IF NOT EXISTS txn_type_idx
    ON public.transaction_log (transaction_type_clean);
CREATE INDEX IF NOT EXISTS txn_external_idx
    ON public.transaction_log (is_external_flow) WHERE is_external_flow = TRUE;

-- Idempotency: re-running ingest for a given snapshot_date should not create
-- duplicates. The ingest code uses the (snapshot_date, account_node_id) tuple
-- to delete prior rows, but a hard unique constraint here would catch bugs.
-- Don't add until we audit transaction_id consumption — adding now would
-- break the BIGSERIAL ID generation pattern. Revisit before production.

-- =============================================================================
-- pricing_refresh — yfinance prices, latest + previous close
-- =============================================================================
CREATE TABLE IF NOT EXISTS public.pricing_refresh (
    refresh_date      DATE NOT NULL,
    ticker_yf         TEXT NOT NULL,
    security_id       BIGINT REFERENCES public.security(security_id) ON DELETE CASCADE,
    price             NUMERIC(20, 8),
    price_previous    NUMERIC(20, 8),
    price_ccy         CHAR(3),
    yf_as_of_date     DATE,
    yf_previous_date  DATE,
    source            TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (refresh_date, ticker_yf)
);
CREATE INDEX IF NOT EXISTS pricing_security_date_idx
    ON public.pricing_refresh (security_id, refresh_date);

-- =============================================================================
-- index_definition / index_price_history — yfinance benchmark series
-- =============================================================================
-- Indices used for portfolio-vs-benchmark comparison on the Returns tile.
-- Separate from pricing_refresh (which is keyed to held securities and
-- retains only the last two prices) — benchmarks need years of daily history.

CREATE TABLE IF NOT EXISTS public.index_definition (
    ticker     TEXT PRIMARY KEY,
    name       TEXT NOT NULL,
    ccy        CHAR(3) NOT NULL DEFAULT 'USD',
    notes      TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.index_price_history (
    ticker     TEXT NOT NULL REFERENCES public.index_definition(ticker) ON DELETE CASCADE,
    price_date DATE NOT NULL,
    close      NUMERIC(20, 8) NOT NULL,
    source     TEXT NOT NULL DEFAULT 'yfinance',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (ticker, price_date)
);
CREATE INDEX IF NOT EXISTS index_price_date_idx
    ON public.index_price_history (price_date);

-- =============================================================================
-- security_price_history — daily yfinance closes for held public securities,
-- used to reconstruct NAVs at arbitrary dates (6M / 1Y precision).
-- =============================================================================
CREATE TABLE IF NOT EXISTS public.security_price_history (
    ticker_yf  TEXT NOT NULL,
    price_date DATE NOT NULL,
    close      NUMERIC(20, 8) NOT NULL,
    source     TEXT NOT NULL DEFAULT 'yfinance',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (ticker_yf, price_date)
);
CREATE INDEX IF NOT EXISTS security_price_history_date_idx
    ON public.security_price_history (price_date);
CREATE INDEX IF NOT EXISTS security_price_history_ticker_idx
    ON public.security_price_history (ticker_yf);

-- =============================================================================
-- sync_log — audit trail of ingestion / refresh runs
-- =============================================================================
CREATE TABLE IF NOT EXISTS public.sync_log (
    sync_id          BIGSERIAL PRIMARY KEY,
    sync_timestamp   TIMESTAMPTZ NOT NULL DEFAULT now(),
    sync_type        TEXT NOT NULL,
    scope            TEXT,
    description      TEXT,
    rows_affected    INTEGER
);
CREATE INDEX IF NOT EXISTS sync_log_ts_idx ON public.sync_log (sync_timestamp DESC);

-- =============================================================================
-- Views — read models for the frontend / downstream apps
-- =============================================================================

-- All views use security_invoker so RLS on the underlying tables gates row
-- access through the view too. Without this, the views run as the owner
-- (postgres) and bypass RLS — flagged by Supabase linter as 0010_security_
-- definer_view, and a real leak once family-scoped policies are added.

-- Convenience view: latest snapshot per account-position
-- "Latest snapshot per account" — see migration 027. The latest_per_account
-- CTE is AS MATERIALIZED so the join-back range-scans only the latest slice
-- via the primary key instead of re-reading all position history per account
-- (which used to breach the 8s authenticated statement_timeout cold).
CREATE OR REPLACE VIEW public.v_latest_positions
WITH (security_invoker = true) AS
WITH latest_per_account AS MATERIALIZED (
    SELECT e.node_id AS account_node_id, m.snapshot_date
    FROM public.entity e
    CROSS JOIN LATERAL (
        SELECT p.snapshot_date
        FROM public.position_snapshot p
        WHERE p.account_node_id = e.node_id
        ORDER BY p.snapshot_date DESC
        LIMIT 1
    ) m
)
SELECT
    p.snapshot_date,
    p.account_node_id,
    e.alias AS account_alias,
    e.bank_broker AS custodian,
    e.account_number,
    ea.trust_node_id,
    ea.trust_alias,
    ea.sub_client_node_id,
    ea.sub_client_alias,
    p.security_id,
    s.asset_name,
    s.asset_class,
    s.security_type,
    s.sector,
    s.geographic_exposure,
    s.ticker_masttro,
    s.ticker_yf,
    s.isin,
    s.local_ccy,
    p.quantity,
    p.price_local,
    p.mv_local,
    p.mv_reporting,
    p.reporting_ccy,
    p.unit_cost_local,
    p.total_cost_local,
    (p.mv_local - p.total_cost_local) AS unrealized_gl_local,
    p.accrued_interest_reporting
FROM public.position_snapshot p
JOIN latest_per_account la
       ON la.account_node_id = p.account_node_id
      AND la.snapshot_date   = p.snapshot_date
JOIN      public.entity              e  ON p.account_node_id = e.node_id
LEFT JOIN public.entity_attribution  ea ON p.account_node_id = ea.node_id
LEFT JOIN public.security            s  ON p.security_id     = s.security_id;

-- Monthly NAV per account
CREATE OR REPLACE VIEW public.v_nav_monthly_by_account
WITH (security_invoker = true) AS
SELECT
    p.snapshot_date,
    p.account_node_id,
    e.alias AS account_alias,
    ea.trust_alias,
    ea.sub_client_alias,
    SUM(p.mv_reporting) AS nav_reporting,
    SUM(p.mv_local) AS nav_local
FROM public.position_snapshot p
JOIN public.entity e ON p.account_node_id = e.node_id
LEFT JOIN public.entity_attribution ea ON p.account_node_id = ea.node_id
GROUP BY p.snapshot_date, p.account_node_id, e.alias, ea.trust_alias, ea.sub_client_alias;

-- Monthly NAV per (account × asset class) — for the Returns tile split-by-class.
CREATE OR REPLACE VIEW public.v_nav_monthly_by_asset_class
WITH (security_invoker = true) AS
SELECT
    ps.snapshot_date,
    ps.account_node_id,
    e.alias AS account_alias,
    ea.trust_alias,
    ea.sub_client_alias,
    COALESCE(s.asset_class, 'Unclassified') AS asset_class,
    SUM(ps.mv_reporting) AS nav_reporting
FROM public.position_snapshot ps
JOIN public.security s          ON s.security_id = ps.security_id
JOIN public.entity   e          ON e.node_id     = ps.account_node_id
LEFT JOIN public.entity_attribution ea ON ea.node_id = ps.account_node_id
GROUP BY ps.snapshot_date, ps.account_node_id, e.alias,
         ea.trust_alias, ea.sub_client_alias, s.asset_class;

-- Monthly NAV per trust
CREATE OR REPLACE VIEW public.v_nav_monthly_by_trust
WITH (security_invoker = true) AS
SELECT
    p.snapshot_date,
    ea.trust_node_id,
    ea.trust_alias,
    ea.sub_client_alias,
    SUM(p.mv_reporting) AS nav_reporting
FROM public.position_snapshot p
JOIN public.entity_attribution ea ON p.account_node_id = ea.node_id
WHERE ea.trust_node_id IS NOT NULL
GROUP BY p.snapshot_date, ea.trust_node_id, ea.trust_alias, ea.sub_client_alias;

-- Refreshed position values (joins latest pricing_refresh)
CREATE OR REPLACE VIEW public.v_positions_refreshed
WITH (security_invoker = true) AS
WITH latest_refresh AS (
    SELECT MAX(refresh_date) AS d FROM public.pricing_refresh
)
SELECT
    lp.*,
    pr.price       AS yf_price,
    pr.price_previous AS yf_price_previous,
    pr.yf_as_of_date,
    pr.yf_previous_date,
    pr.source      AS yf_source,
    CASE
        WHEN pr.price IS NOT NULL AND lp.price_local IS NOT NULL AND lp.price_local != 0
        THEN lp.mv_reporting * (pr.price / lp.price_local)
        ELSE lp.mv_reporting
    END AS mv_reporting_refreshed,
    CASE
        WHEN pr.price IS NOT NULL AND pr.price_previous IS NOT NULL
         AND lp.price_local IS NOT NULL AND lp.price_local != 0
        THEN lp.mv_reporting * (pr.price_previous / lp.price_local)
        ELSE lp.mv_reporting
    END AS mv_reporting_yesterday
FROM public.v_latest_positions lp
LEFT JOIN public.pricing_refresh pr
    ON pr.security_id = lp.security_id
   AND pr.refresh_date = (SELECT d FROM latest_refresh);

-- Long-format income events: one row per (month, account, security, type).
-- Used by the Income page for KPIs, monthly chart, top payers, by-trust.
CREATE OR REPLACE VIEW public.v_income_monthly
WITH (security_invoker = true) AS
SELECT
    date_trunc('month', t.transaction_date)::DATE AS month,
    t.account_node_id,
    e.alias AS account_alias,
    ea.trust_alias,
    ea.sub_client_alias,
    t.security_id,
    s.asset_name,
    s.asset_class,
    s.ticker_masttro,
    t.transaction_type_clean AS transaction_type,
    t.reporting_ccy,
    SUM(t.net_amount_reporting) AS amount
FROM public.transaction_log t
JOIN public.entity e ON t.account_node_id = e.node_id
LEFT JOIN public.entity_attribution ea ON t.account_node_id = ea.node_id
LEFT JOIN public.security s ON t.security_id = s.security_id
WHERE t.transaction_date IS NOT NULL
  AND t.transaction_type_clean IN ('Cash Dividends', 'Interest', 'Income')
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11;

-- Income time series (Cash Dividends + Interest + Income), by account, by month
CREATE OR REPLACE VIEW public.v_income_monthly_by_account
WITH (security_invoker = true) AS
SELECT
    date_trunc('month', t.transaction_date)::DATE AS month,
    t.account_node_id,
    e.alias AS account_alias,
    SUM(CASE WHEN t.transaction_type_clean = 'Cash Dividends' THEN t.net_amount_reporting ELSE 0 END) AS dividends,
    SUM(CASE WHEN t.transaction_type_clean = 'Interest'       THEN t.net_amount_reporting ELSE 0 END) AS interest,
    SUM(CASE WHEN t.transaction_type_clean = 'Income'         THEN t.net_amount_reporting ELSE 0 END) AS other_income,
    SUM(CASE WHEN t.transaction_type_clean IN ('Cash Dividends', 'Interest', 'Income')
             THEN t.net_amount_reporting ELSE 0 END) AS total_income
FROM public.transaction_log t
JOIN public.entity e ON t.account_node_id = e.node_id
WHERE t.transaction_date IS NOT NULL
GROUP BY 1, 2, 3;

-- Generic per-transaction view joining account / attribution / security.
CREATE OR REPLACE VIEW public.v_transactions
WITH (security_invoker = true) AS
SELECT
    t.transaction_id,
    t.transaction_date,
    t.snapshot_date,
    t.account_node_id,
    e.alias            AS account_alias,
    e.bank_broker      AS custodian,
    ea.trust_alias,
    ea.sub_client_alias,
    t.security_id,
    s.asset_name,
    s.asset_class,
    s.ticker_masttro,
    t.transaction_type,
    t.transaction_type_clean,
    t.gwm_in_ex_type,
    t.comments,
    t.quantity,
    t.net_price_local,
    t.net_amount_local,
    t.net_amount_reporting,
    t.local_ccy,
    t.reporting_ccy,
    t.is_external_flow
FROM public.transaction_log t
JOIN      public.entity             e  ON e.node_id     = t.account_node_id
LEFT JOIN public.entity_attribution ea ON ea.node_id    = t.account_node_id
LEFT JOIN public.security           s  ON s.security_id = t.security_id;

-- External flows per account
CREATE OR REPLACE VIEW public.v_external_flows
WITH (security_invoker = true) AS
SELECT
    t.transaction_date,
    t.account_node_id,
    e.alias AS account_alias,
    ea.trust_alias,
    t.transaction_type_clean AS transaction_type,
    t.net_amount_reporting,
    t.reporting_ccy,
    ea.sub_client_alias,
    ea.sub_client_node_id
FROM public.transaction_log t
JOIN public.entity e ON t.account_node_id = e.node_id
LEFT JOIN public.entity_attribution ea ON t.account_node_id = ea.node_id
WHERE t.is_external_flow = TRUE;

-- =============================================================================
-- RLS — enable, no policies yet (block all by default until policies added)
-- =============================================================================
ALTER TABLE public.entity              ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.entity_attribution  ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.security            ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.position_snapshot   ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.transaction_log     ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.pricing_refresh     ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.sync_log            ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.index_definition       ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.index_price_history    ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.security_price_history ENABLE ROW LEVEL SECURITY;

-- User-management tables (Phase 2a, migration 028).
CREATE TABLE IF NOT EXISTS public.app_user (
    user_id    UUID PRIMARY KEY REFERENCES auth.users(id) ON DELETE CASCADE,
    role       TEXT NOT NULL DEFAULT 'client' CHECK (role IN ('admin', 'client')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS public.user_family_access (
    user_id            UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
    sub_client_node_id TEXT NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, sub_client_node_id)
);
ALTER TABLE public.app_user           ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.user_family_access ENABLE ROW LEVEL SECURITY;

-- The RLS helper functions (is_admin, current_user_sub_clients), the
-- set_sub_client_node_id() insert trigger, and the family-scoped SELECT
-- policies on entity / entity_attribution / position_snapshot /
-- transaction_log (plus authenticated-read on shared reference tables and
-- admin-only on sync_log) are defined in
-- supabase/migrations/028_user_management_rls.sql, with the policy
-- predicates perf-tuned in 029 (wrap is_admin()/current_user_sub_clients()
-- in (SELECT ...) so they evaluate once per statement, not per row). Apply
-- both after this schema on a fresh deploy. Admins are seeded in 028 from
-- the @stbm.com.au email domain; map clients via user_family_access.

COMMIT;
