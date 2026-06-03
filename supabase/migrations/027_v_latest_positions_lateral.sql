-- 027 — make v_latest_positions scale with account count, not history depth.
--
-- SYMPTOM: the Holdings page (and the Overview NAV tile) intermittently
-- failed the first load of the day with Postgres 57014 "canceling statement
-- due to statement timeout" — the `authenticated` role has an 8s
-- statement_timeout. A refresh always succeeded.
--
-- CAUSE: migration 021 computed "latest snapshot per account" with a
-- GROUP BY over the ENTIRE position_snapshot table:
--
--     WITH latest_per_account AS (
--         SELECT account_node_id, MAX(snapshot_date)
--         FROM position_snapshot GROUP BY account_node_id)
--
-- That aggregate scans every row of history (583k rows back to 2021-06,
-- 67 distinct dates and growing daily) just to find ~461 maxima. Worse,
-- the nightly sync inserts ~20k fresh rows that autovacuum hasn't marked
-- all-visible yet, so the "index-only" scan degrades into ~157k random
-- heap fetches. Warm (buffer-cached) that's ~225ms; cold — the first user
-- after the overnight sync — it reads ~440MB off disk and blows past 8s.
-- The refresh is warm, hence "fails once, then works".
--
-- FIX: drive the latest-date lookup from the small `entity` table (4.4k
-- nodes) and probe position_snapshot once per account via LATERAL +
-- ORDER BY snapshot_date DESC LIMIT 1, which is an index backward-scan on
-- pos_snap_account_date_idx (account_node_id, snapshot_date). Cost now
-- scales with the number of accounts, not the depth of history.
--
-- The CTE is AS MATERIALIZED on purpose. Without it the planner inlines
-- the LATERAL and re-derives the latest date by reading EVERY historical
-- position row per account (an Index Scan on account_node_id that filters
-- the date afterwards) — buffers stay ~100k. MATERIALIZED forces the
-- 461-row (account, latest_date) barrier to be built first, so the
-- join-back to position_snapshot range-scans only that slice via the
-- (snapshot_date, account_node_id, security_id) primary key.
--
--   Full Holdings query — EXPLAIN (ANALYZE, BUFFERS), warm:
--     before (021 GROUP BY):   buffers hit=93,392   heap fetches=157,107   ~260ms
--     after  (027 MATERIALIZED): buffers hit=19,614   heap fetches=700      ~48ms
--   Cold (first load after the nightly sync) is where the 157k random heap
--   fetches used to breach the 8s authenticated statement_timeout; at 700
--   they no longer do.
--
-- SEMANTICS UNCHANGED: an account_node_id only appears in
-- position_snapshot if it also exists in `entity` (the outer view already
-- INNER JOINs entity, dropping any orphan rows), and a node with no
-- snapshots produces no LATERAL row — so the result set is identical to
-- the GROUP BY version, account-for-account (verified: 21,440 rows,
-- 461 accounts, sum(mv_reporting) = 449,749,507.73 before and after).
--
-- COLUMN SHAPE PRESERVED EXACTLY — CREATE OR REPLACE VIEW cannot reorder
-- or rename columns (web/CLAUDE.md DB conventions). Only the CTE changed.
-- v_positions_refreshed selects lp.* from this view and is unaffected.

CREATE OR REPLACE VIEW public.v_latest_positions
WITH (security_invoker = true) AS
WITH latest_per_account AS MATERIALIZED (
    SELECT e.node_id AS account_node_id, m.snapshot_date
    FROM public.entity e
    CROSS JOIN LATERAL (
        SELECT p.snapshot_date
        FROM public.position_snapshot p
        WHERE p.account_node_id = e.node_id
        ORDER BY p.snapshot_date DESC
        LIMIT 1
    ) m
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
