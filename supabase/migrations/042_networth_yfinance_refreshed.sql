-- 042_networth_yfinance_refreshed.sql
-- Use yfinance-refreshed prices for the LISTED side of the net-worth view, so the
-- Net Worth page market value (and the returns' end NAV, which is the displayed
-- value) is live and ties to Overview/Holdings — instead of Masttro's ~24h-lagged
-- snapshot. Non-listed has no daily price, so it stays at its current NAV.
-- (The blended returns still take their period START + flows from
-- performance_snapshot / Masttro /Performance; only the END moves to yfinance.)
-- Apply manually in the Supabase SQL Editor.

CREATE OR REPLACE VIEW public.v_net_worth_positions
WITH (security_invoker = true) AS
SELECT sub_client_alias,
       trust_alias                 AS entity_alias,
       vehicle_alias,
       account_alias,
       asset_class, security_type,
       mv_reporting_refreshed      AS mv_reporting,   -- yfinance-refreshed (was Masttro snapshot)
       reporting_ccy,
       'listed'::text              AS book
FROM public.v_positions_refreshed
UNION ALL
SELECT sub_client_alias,
       entity_alias,
       vehicle_alias,
       NULL::text                  AS account_alias,
       asset_class, security_type,
       mv_reporting, reporting_ccy,
       'non-listed'::text          AS book
FROM public.v_latest_alt_positions;
