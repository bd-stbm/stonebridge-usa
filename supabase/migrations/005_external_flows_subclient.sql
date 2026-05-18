-- 005 — Add sub_client_alias and sub_client_node_id to v_external_flows.
--
-- The dashboard needs to filter external flows by sub-client to compute
-- modified-Dietz returns (deposits/withdrawals back out of NAV change).
-- The view already joins entity_attribution but only exposed trust_alias.
--
-- New columns are appended at the END of the SELECT because Postgres'
-- CREATE OR REPLACE VIEW only allows adding columns at the tail — it
-- can't reorder or rename existing positional columns. security_invoker
-- preserved.

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
