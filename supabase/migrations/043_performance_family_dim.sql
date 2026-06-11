-- 043_performance_family_dim.sql
-- Vehicle-scoped blended returns on the Net Worth page. When a Vehicle (e.g.
-- Modyl LP) is selected, the allocation END NAV is vehicle-filtered but the
-- returns were using the FAMILY start NAV -> nonsense ratio. The family pulls
-- already tag each holding with vehicle_alias (+ rollup entity_alias), so expose
-- those dimensions for filtering. Scope='family' is the right source: a vehicle
-- spans entities, and the family pull has exactly one row set (entity pulls would
-- double-count a shared vehicle across the entities it sits under).
-- Vehicle returns are approximate (family-pull subtree scoping, like the entity
-- rollup) but correct in scale — unlike the previous garbage.
-- Apply manually in the Supabase SQL Editor.

CREATE OR REPLACE VIEW public.v_performance_family_dim
WITH (security_invoker = true) AS
WITH latest AS (
    SELECT sub_client_node_id, period, MAX(pull_date) AS pull_date
    FROM public.performance_snapshot WHERE scope = 'family' GROUP BY 1, 2
)
SELECT p.sub_client_node_id, p.sub_client_alias, p.entity_alias, p.vehicle_alias,
       p.asset_class, p.period, p.pull_date,
       MAX(p.reporting_ccy) AS reporting_ccy,
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
GROUP BY p.sub_client_node_id, p.sub_client_alias, p.entity_alias, p.vehicle_alias,
         p.asset_class, p.period, p.pull_date;
