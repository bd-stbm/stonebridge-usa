-- 021 — rewrite v_latest_positions to remove the correlated subquery.
--
-- The previous definition (schema.sql:306-309) filtered "latest snapshot
-- per account" with:
--
--   WHERE p.snapshot_date = (
--       SELECT MAX(snapshot_date) FROM public.position_snapshot
--       WHERE account_node_id = p.account_node_id
--   )
--
-- PostgreSQL runs that subquery once per candidate row. For small
-- selections (the header's listTrusts / listAssetClasses / listSubClients
-- only need a few distinct values) the planner picked a path that
-- couldn't amortize the per-row cost — production Vercel logs showed
-- those header queries each taking ~6 seconds despite returning 3-8
-- rows. getLatestPositions, which is more selective, was a tolerable
-- 1.2 seconds against the same view.
--
-- Rewrite to a CTE that computes latest_per_account once with a single
-- GROUP BY, then joins back. Column shape preserved exactly — the
-- `CREATE OR REPLACE VIEW` rule about not reordering / renaming columns
-- (web/CLAUDE.md DB conventions) is respected.
--
-- Index (account_node_id, snapshot_date) on position_snapshot already
-- exists, so the CTE's GROUP BY is an index-only scan.

CREATE OR REPLACE VIEW public.v_latest_positions
WITH (security_invoker = true) AS
WITH latest_per_account AS (
    SELECT account_node_id, MAX(snapshot_date) AS snapshot_date
    FROM public.position_snapshot
    GROUP BY account_node_id
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
