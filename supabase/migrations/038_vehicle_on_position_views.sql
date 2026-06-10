-- 038_vehicle_on_position_views.sql
-- Option C, Phase 1: expose vehicle_alias on the position + aggregation views so
-- the Vehicle/SPV filter can scope Holdings, Overview (NAV tile / top holdings /
-- asset-class allocation), Income and Transactions. The returns / NAV-history
-- pipeline (carry-forward grid + RPCs) is threaded separately in Phase 2.
-- See docs/option_c_global_vehicle_filter.md.
--
-- Apply manually in the Supabase SQL Editor.

-- v_positions_refreshed selects lp.* from v_latest_positions, whose column list
-- was frozen before 037 added vehicle_alias — so a CREATE OR REPLACE can't pick
-- it up (the new column lands mid-list, not appended). DROP+CREATE re-expands
-- lp.* to include vehicle_alias. No view depends on it (verified).
DROP VIEW IF EXISTS public.v_positions_refreshed;
CREATE VIEW public.v_positions_refreshed
WITH (security_invoker = true) AS
WITH latest_refresh AS (
    SELECT MAX(refresh_date) AS d FROM public.pricing_refresh
)
SELECT
    lp.*,
    pr.price          AS yf_price,
    pr.price_previous AS yf_price_previous,
    pr.yf_as_of_date,
    pr.yf_previous_date,
    pr.source         AS yf_source,
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

-- Append vehicle_alias to the monthly NAV + income + transaction views.
-- account_node_id already keys each group and an account has exactly one
-- vehicle_alias, so adding it to GROUP BY does not change the grain.
CREATE OR REPLACE VIEW public.v_nav_monthly_by_account
WITH (security_invoker = true) AS
SELECT
    p.snapshot_date,
    p.account_node_id,
    e.alias AS account_alias,
    ea.trust_alias,
    ea.sub_client_alias,
    SUM(p.mv_reporting) AS nav_reporting,
    SUM(p.mv_local) AS nav_local,
    ea.vehicle_alias
FROM public.position_snapshot p
JOIN public.entity e ON p.account_node_id = e.node_id
LEFT JOIN public.entity_attribution ea ON p.account_node_id = ea.node_id
GROUP BY p.snapshot_date, p.account_node_id, e.alias, ea.trust_alias,
         ea.sub_client_alias, ea.vehicle_alias;

CREATE OR REPLACE VIEW public.v_nav_monthly_by_asset_class
WITH (security_invoker = true) AS
SELECT
    ps.snapshot_date,
    ps.account_node_id,
    e.alias AS account_alias,
    ea.trust_alias,
    ea.sub_client_alias,
    COALESCE(s.asset_class, 'Unclassified') AS asset_class,
    SUM(ps.mv_reporting) AS nav_reporting,
    ea.vehicle_alias
FROM public.position_snapshot ps
JOIN public.security s ON s.security_id = ps.security_id
JOIN public.entity e   ON e.node_id     = ps.account_node_id
LEFT JOIN public.entity_attribution ea ON ea.node_id = ps.account_node_id
GROUP BY ps.snapshot_date, ps.account_node_id, e.alias, ea.trust_alias,
         ea.sub_client_alias, s.asset_class, ea.vehicle_alias;

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
    SUM(t.net_amount_reporting) AS amount,
    ea.vehicle_alias
FROM public.transaction_log t
JOIN public.entity e ON t.account_node_id = e.node_id
LEFT JOIN public.entity_attribution ea ON t.account_node_id = ea.node_id
LEFT JOIN public.security s ON t.security_id = s.security_id
WHERE t.transaction_date IS NOT NULL
  AND t.transaction_type_clean IN ('Cash Dividends', 'Interest', 'Income')
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, ea.vehicle_alias;

-- v_transactions is row-level (no aggregation) — just append vehicle_alias.
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
    t.is_external_flow,
    ea.vehicle_alias
FROM public.transaction_log t
JOIN      public.entity             e  ON e.node_id     = t.account_node_id
LEFT JOIN public.entity_attribution ea ON ea.node_id    = t.account_node_id
LEFT JOIN public.security           s  ON s.security_id = t.security_id;
