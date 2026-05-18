-- 009 — v_income_monthly: long-format income events for the Income page.
--
-- One row per (month, account, security, income_type). Wider than the
-- existing v_income_monthly_by_account (which was account-only) so the
-- dashboard can slice income by trust, security, or type without extra
-- queries.
--
-- security_invoker = true so RLS on the underlying tables applies through
-- the view. The older v_income_monthly_by_account stays in place for now
-- (nothing in the dashboard uses it) — can drop in a later cleanup
-- migration.

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
