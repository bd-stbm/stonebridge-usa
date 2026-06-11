-- 040_performance_snapshot.sql
-- Blended ALL-ASSET returns: store Masttro /Performance period components per
-- holding so we can aggregate a total / per-entity / per-asset-class return that
-- spans listed + non-listed (the §7 "blended total return"). /Performance already
-- carries marketValueInitial / marketValue / flows / totalPL / irr / twr for every
-- asset class, so the blended return = modified-Dietz over the summed components
-- (the same method the listed tile uses). See docs/all_assets_integration_design.md.
--
-- Pulled family-level per period; each holding node rolled up to its entity
-- (nearest-existing-entity walk, same grain as the net-worth view).
-- Apply manually in the Supabase SQL Editor.

CREATE TABLE IF NOT EXISTS public.performance_snapshot (
    id                    BIGSERIAL PRIMARY KEY,
    pull_date             DATE     NOT NULL,   -- when we pulled it
    period                SMALLINT NOT NULL,   -- 0 MTD, 1 YTD, 2 3M, 3 6M, 4 12M
    year_month            TEXT,                -- the yearMonth param
    sub_client_node_id    TEXT     NOT NULL,   -- family (RLS key)
    sub_client_alias      TEXT,
    node_id               TEXT,                -- holding node
    security_id           BIGINT,
    asset_class           TEXT,
    security_type         TEXT,
    entity_node_id        TEXT,                -- rolled-up entity (nearest-existing)
    entity_alias          TEXT,
    vehicle_alias         TEXT,                -- SPV, NULL when held directly
    market_value_initial  NUMERIC,             -- start NAV (period open)
    market_value          NUMERIC,             -- end NAV (period close)
    deposits              NUMERIC,
    withdrawals           NUMERIC,
    transfer_in_out       NUMERIC,
    realized_gl           NUMERIC,
    unrealized_gl         NUMERIC,
    income                NUMERIC,
    total_pl              NUMERIC,
    avg_cap_base          NUMERIC,
    irr                   NUMERIC,             -- Masttro's own per-holding XIRR
    twr                   NUMERIC,             -- Masttro's own per-holding TWR
    reporting_ccy         CHAR(3),
    initial_date          DATE,
    as_of_date            DATE,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS perf_snap_scope_idx
    ON public.performance_snapshot (sub_client_node_id, period, pull_date);
CREATE INDEX IF NOT EXISTS perf_snap_entity_idx
    ON public.performance_snapshot (entity_node_id, period);

ALTER TABLE public.performance_snapshot ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "family scoped read" ON public.performance_snapshot;
CREATE POLICY "family scoped read" ON public.performance_snapshot
    FOR SELECT TO authenticated
    USING (public.is_admin()
           OR sub_client_node_id = ANY (public.current_user_sub_clients()));

-- Blended return components per (asset_class, period) for the latest pull. This
-- is the EXACT, Masttro-matching aggregation (asset_class is a direct field, no
-- rollup): the overall return = sum across classes, the per-class returns match
-- the Masttro category table. The app computes the ratio (modified-Dietz).
CREATE OR REPLACE VIEW public.v_performance_by_class
WITH (security_invoker = true) AS
WITH latest AS (
    SELECT sub_client_node_id, period, MAX(pull_date) AS pull_date
    FROM public.performance_snapshot GROUP BY 1, 2
)
SELECT p.sub_client_node_id, p.sub_client_alias, p.asset_class, p.period,
       p.pull_date, MAX(p.reporting_ccy) AS reporting_ccy,
       SUM(p.market_value_initial)                          AS start_nav,
       SUM(p.market_value)                                  AS end_nav,
       SUM(COALESCE(p.deposits,0) + COALESCE(p.withdrawals,0)
           + COALESCE(p.transfer_in_out,0))                 AS flows,
       SUM(p.total_pl)                                      AS total_pl,
       SUM(p.income)                                        AS income
FROM public.performance_snapshot p
JOIN latest l ON l.sub_client_node_id = p.sub_client_node_id
             AND l.period = p.period AND l.pull_date = p.pull_date
GROUP BY p.sub_client_node_id, p.sub_client_alias, p.asset_class, p.period, p.pull_date;

-- Per-entity components. NOTE: APPROXIMATE — the entity rollup (nearest-existing
-- walk) divides shared vehicles differently than Masttro's per-entity subtree
-- scoping, so per-entity returns drift ~2% from the Masttro UI. The total and
-- per-class (above) are exact. Match Masttro per-entity via per-entity
-- /Performance pulls (investmentVehicle = entity node), not this view.
CREATE OR REPLACE VIEW public.v_performance_by_entity
WITH (security_invoker = true) AS
WITH latest AS (
    SELECT sub_client_node_id, period, MAX(pull_date) AS pull_date
    FROM public.performance_snapshot GROUP BY 1, 2
)
SELECT p.sub_client_node_id, p.sub_client_alias, p.entity_alias, p.period,
       p.pull_date, MAX(p.reporting_ccy) AS reporting_ccy,
       SUM(p.market_value_initial)                          AS start_nav,
       SUM(p.market_value)                                  AS end_nav,
       SUM(COALESCE(p.deposits,0) + COALESCE(p.withdrawals,0)
           + COALESCE(p.transfer_in_out,0))                 AS flows,
       SUM(p.total_pl)                                      AS total_pl,
       SUM(p.income)                                        AS income
FROM public.performance_snapshot p
JOIN latest l ON l.sub_client_node_id = p.sub_client_node_id
             AND l.period = p.period AND l.pull_date = p.pull_date
GROUP BY p.sub_client_node_id, p.sub_client_alias, p.entity_alias, p.period, p.pull_date;
