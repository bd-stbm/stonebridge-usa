-- 025 — per-(period × account × security) attribution for the Holdings tab.
--
-- Powers the Holdings table's $gain and %gain columns when the user
-- picks a period (MTD / YTD / 6M / 12M). One row per
-- (period, account_node_id, security_id) for every (account, security)
-- pair currently held in scope. 1D is handled client-side from
-- v_positions_refreshed's mv_reporting + mv_reporting_yesterday, so it
-- isn't covered here.
--
-- For each period P × (account A × security S):
--   start_mv = mv_reporting from the latest position_snapshot row for
--              (A, S) on or before P's start date. 0 if S wasn't held
--              in A on or before then — gain since first held, matching
--              the user-chosen short-tenure rule.
--   flows    = -SUM(net_amount_reporting) for Buy + Sell on (A, S) in
--              (start_date, end_date]. Sign: +ve = cash INTO the
--              security, mirroring monthly_security_attribution and
--              getFlowsByAssetClass.
--   income   = SUM(net_amount_reporting) for Cash Dividends + Interest
--              + Income on (A, S) in the same window. Positive =
--              received.
--
-- The client combines these with the refreshed end_mv (already on
-- v_positions_refreshed) to compute:
--   $gain  = (end_mv - start_mv) - flows + income
--   %gain  = $gain / (start_mv + 0.5 * flows)    (Modified Dietz)
--
-- Filtering rules mirror monthly_security_attribution exactly so the
-- two RPCs return consistent attribution.

CREATE OR REPLACE FUNCTION public.holdings_period_attribution(
    p_sub_client       text,
    p_trusts           text[]  DEFAULT NULL,
    p_accounts         text[]  DEFAULT NULL,
    p_asset_classes    text[]  DEFAULT NULL,
    p_excluded_trusts  text[]  DEFAULT NULL,
    p_end_date         date    DEFAULT NULL,
    p_mtd_start        date    DEFAULT NULL,
    p_ytd_start        date    DEFAULT NULL,
    p_six_m_start      date    DEFAULT NULL,
    p_one_y_start      date    DEFAULT NULL
)
RETURNS TABLE(
    period           text,
    account_node_id  text,
    security_id      bigint,
    start_mv         numeric,
    flows            numeric,
    income           numeric
)
LANGUAGE plpgsql
STABLE
SECURITY INVOKER
AS $$
#variable_conflict use_column
DECLARE
    v_end_date date;
BEGIN
    v_end_date := COALESCE(p_end_date, CURRENT_DATE);

    RETURN QUERY
    WITH
    params AS (
        SELECT period, start_date
        FROM (VALUES
            ('mtd', p_mtd_start),
            ('ytd', p_ytd_start),
            ('6m',  p_six_m_start),
            ('1y',  p_one_y_start)
        ) AS t(period, start_date)
        WHERE start_date IS NOT NULL
    ),
    -- Currently-held (account × security) pairs in scope. Mirrors
    -- v_positions_refreshed: per-account MAX(snapshot_date) (no filter),
    -- then the asset_class filter is applied to the resulting rows. So
    -- the universe here matches the rows the Holdings table displays.
    latest_held AS (
        SELECT DISTINCT
            ps.account_node_id,
            ps.security_id
        FROM public.position_snapshot ps
        LEFT JOIN public.entity_attribution ea ON ea.node_id = ps.account_node_id
        LEFT JOIN public.security s            ON s.security_id = ps.security_id
        WHERE ea.sub_client_alias = p_sub_client
          AND (COALESCE(array_length(p_trusts,          1), 0) = 0
               OR ea.trust_alias    = ANY(p_trusts))
          AND (COALESCE(array_length(p_accounts,        1), 0) = 0
               OR ps.account_node_id = ANY(p_accounts))
          AND (COALESCE(array_length(p_excluded_trusts, 1), 0) = 0
               OR NOT (ea.trust_alias = ANY(p_excluded_trusts)))
          AND (COALESCE(array_length(p_asset_classes,   1), 0) = 0
               OR s.asset_class = ANY(p_asset_classes)
               OR (s.asset_class IS NULL AND 'Unclassified' = ANY(p_asset_classes)))
          AND ps.snapshot_date = (
              SELECT MAX(ps2.snapshot_date)
              FROM public.position_snapshot ps2
              WHERE ps2.account_node_id = ps.account_node_id
          )
    ),
    -- For each (period × held pair), mv_reporting at the latest snapshot
    -- on or before the period's start_date. Correlated subquery: walks
    -- pos_snap_account_date_idx backwards from start_date. Bounded at
    -- ~(periods × holdings) iterations — small.
    start_mv_per AS (
        SELECT
            pa.period,
            lh.account_node_id,
            lh.security_id,
            COALESCE((
                SELECT ps.mv_reporting
                FROM public.position_snapshot ps
                WHERE ps.account_node_id = lh.account_node_id
                  AND ps.security_id     = lh.security_id
                  AND ps.snapshot_date  <= pa.start_date
                ORDER BY ps.snapshot_date DESC
                LIMIT 1
            ), 0)::numeric AS start_mv
        FROM params pa
        CROSS JOIN latest_held lh
    ),
    -- Per (period × held pair), Buy/Sell + income aggregates over the
    -- period window. JOIN to latest_held filters out transactions on
    -- pairs no longer held — consistent with the Holdings page only
    -- showing current positions.
    txns_per AS (
        SELECT
            pa.period,
            t.account_node_id,
            t.security_id,
            SUM(CASE WHEN t.transaction_type_clean IN ('Buy', 'Sell')
                     THEN -t.net_amount_reporting ELSE 0 END)::numeric AS flows,
            SUM(CASE WHEN t.transaction_type_clean IN ('Cash Dividends', 'Interest', 'Income')
                     THEN t.net_amount_reporting ELSE 0 END)::numeric AS income
        FROM params pa
        JOIN public.transaction_log t
          ON t.transaction_date >  pa.start_date
         AND t.transaction_date <= v_end_date
        JOIN latest_held lh
          ON lh.account_node_id = t.account_node_id
         AND lh.security_id     = t.security_id
        WHERE t.transaction_type_clean IN ('Buy', 'Sell',
                                           'Cash Dividends', 'Interest', 'Income')
          AND t.security_id IS NOT NULL
        GROUP BY pa.period, t.account_node_id, t.security_id
    )
    SELECT
        smv.period::text,
        smv.account_node_id::text,
        smv.security_id::bigint,
        smv.start_mv,
        COALESCE(tp.flows,  0)::numeric AS flows,
        COALESCE(tp.income, 0)::numeric AS income
    FROM start_mv_per smv
    LEFT JOIN txns_per tp
           ON tp.period          = smv.period
          AND tp.account_node_id = smv.account_node_id
          AND tp.security_id     = smv.security_id;
END;
$$;

GRANT EXECUTE ON FUNCTION public.holdings_period_attribution(
    text, text[], text[], text[], text[], date, date, date, date, date
) TO anon, authenticated, service_role;
