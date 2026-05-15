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

-- Convenience view: latest snapshot per account-position
CREATE OR REPLACE VIEW public.v_latest_positions AS
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
JOIN public.entity e             ON p.account_node_id = e.node_id
LEFT JOIN public.entity_attribution ea ON p.account_node_id = ea.node_id
LEFT JOIN public.security s      ON p.security_id     = s.security_id
WHERE p.snapshot_date = (
    SELECT MAX(snapshot_date) FROM public.position_snapshot
    WHERE account_node_id = p.account_node_id
);

-- Monthly NAV per account
CREATE OR REPLACE VIEW public.v_nav_monthly_by_account AS
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

-- Monthly NAV per trust
CREATE OR REPLACE VIEW public.v_nav_monthly_by_trust AS
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
CREATE OR REPLACE VIEW public.v_positions_refreshed AS
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

-- Income time series (Cash Dividends + Interest + Income), by account, by month
CREATE OR REPLACE VIEW public.v_income_monthly_by_account AS
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

-- External flows per account
CREATE OR REPLACE VIEW public.v_external_flows AS
SELECT
    t.transaction_date,
    t.account_node_id,
    e.alias AS account_alias,
    ea.trust_alias,
    t.transaction_type_clean AS transaction_type,
    t.net_amount_reporting,
    t.reporting_ccy
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

-- Example "allow all reads for authenticated users" policy template
-- (uncomment and customise per table once your auth model is decided):
--
-- CREATE POLICY "allow read for authenticated"
--     ON public.position_snapshot
--     FOR SELECT
--     TO authenticated
--     USING (true);
--
-- For sub-client-scoped access (each user sees only their family):
--
-- CREATE POLICY "user sees own family"
--     ON public.position_snapshot
--     FOR SELECT
--     TO authenticated
--     USING (
--         account_node_id IN (
--             SELECT a.node_id
--             FROM public.entity a
--             JOIN public.entity_attribution ea ON a.node_id = ea.node_id
--             WHERE ea.sub_client_node_id = (
--                 SELECT sub_client_node_id FROM public.user_profile
--                 WHERE user_id = auth.uid()
--             )
--         )
--     );

COMMIT;
