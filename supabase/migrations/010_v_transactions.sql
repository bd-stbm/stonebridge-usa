-- 010 — v_transactions: one row per transaction joined with security,
-- account, and attribution info — the data shape the Transactions page
-- needs. Existing scoped queries elsewhere (flows, income) build on
-- transaction_log directly, but for a generic transactions list it's
-- cleaner to pre-join the lookup tables once at the view layer.

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
JOIN      public.entity              e  ON e.node_id     = t.account_node_id
LEFT JOIN public.entity_attribution  ea ON ea.node_id    = t.account_node_id
LEFT JOIN public.security            s  ON s.security_id = t.security_id;
