-- 041_performance_entity_scope.sql
-- Option A: per-entity /Performance pulls so entity-level returns match Masttro.
-- Masttro computes each entity's return over its investmentVehicle SUBTREE as a
-- unit (composition/transfers), which can't be reconstructed by rolling up a
-- family pull. So we pull per entity and tag the rows scope='entity' (the whole
-- pull IS that entity, so entity_alias is exact). Family pulls (scope='family')
-- still drive the exact total + per-asset-class aggregation.
-- Apply manually in the Supabase SQL Editor (BEFORE re-running sync_performance).

ALTER TABLE public.performance_snapshot
    ADD COLUMN IF NOT EXISTS scope TEXT;   -- 'family' | 'entity'
-- Existing rows were family-level pulls.
UPDATE public.performance_snapshot SET scope = 'family' WHERE scope IS NULL;

CREATE INDEX IF NOT EXISTS perf_snap_entity_scope_idx
    ON public.performance_snapshot (sub_client_node_id, scope, period, entity_alias);

-- v_performance_by_class — FAMILY scope only (exact total + per-asset-class).
CREATE OR REPLACE VIEW public.v_performance_by_class
WITH (security_invoker = true) AS
WITH latest AS (
    SELECT sub_client_node_id, period, MAX(pull_date) AS pull_date
    FROM public.performance_snapshot WHERE scope = 'family' GROUP BY 1, 2
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
WHERE p.scope = 'family'
GROUP BY p.sub_client_node_id, p.sub_client_alias, p.asset_class, p.period, p.pull_date;

-- v_performance_by_entity — ENTITY scope (EXACT, matches Masttro per-entity).
CREATE OR REPLACE VIEW public.v_performance_by_entity
WITH (security_invoker = true) AS
WITH latest AS (
    SELECT sub_client_node_id, period, MAX(pull_date) AS pull_date
    FROM public.performance_snapshot WHERE scope = 'entity' GROUP BY 1, 2
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
WHERE p.scope = 'entity'
GROUP BY p.sub_client_node_id, p.sub_client_alias, p.entity_alias, p.period, p.pull_date;

-- v_performance_entity_class — ENTITY scope by (entity, asset_class). Drives the
-- Net Worth return column when an entity filter is active (exact per-entity-class).
CREATE OR REPLACE VIEW public.v_performance_entity_class
WITH (security_invoker = true) AS
WITH latest AS (
    SELECT sub_client_node_id, period, MAX(pull_date) AS pull_date
    FROM public.performance_snapshot WHERE scope = 'entity' GROUP BY 1, 2
)
SELECT p.sub_client_node_id, p.sub_client_alias, p.entity_alias, p.asset_class,
       p.period, p.pull_date, MAX(p.reporting_ccy) AS reporting_ccy,
       SUM(p.market_value_initial)                          AS start_nav,
       SUM(p.market_value)                                  AS end_nav,
       SUM(COALESCE(p.deposits,0) + COALESCE(p.withdrawals,0)
           + COALESCE(p.transfer_in_out,0))                 AS flows,
       SUM(p.total_pl)                                      AS total_pl,
       SUM(p.income)                                        AS income
FROM public.performance_snapshot p
JOIN latest l ON l.sub_client_node_id = p.sub_client_node_id
             AND l.period = p.period AND l.pull_date = p.pull_date
WHERE p.scope = 'entity'
GROUP BY p.sub_client_node_id, p.sub_client_alias, p.entity_alias, p.asset_class,
         p.period, p.pull_date;
