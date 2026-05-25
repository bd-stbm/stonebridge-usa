-- 024 — server-side top-N filter for monthly_security_attribution.
--
-- Symptom: with the drill-in panel showing only top 10 contributors and
-- 10 detractors per month, we don't need every per-security row — but
-- the full RPC result was hitting Supabase's project-level db-max-rows
-- cap, silently truncating later months for larger trusts (commit
-- a6bf03b's client-side .limit(100000) couldn't override the server
-- ceiling). User report: some trusts had months past Jul 2025 missing
-- from the drill-in entirely.
--
-- Fix: add p_top_per_month parameter. When non-null, the RPC ranks
-- rows within each month and returns only top N positive (by gain DESC)
-- UNION top N negative (by gain ASC). For p_top_per_month=15 and the
-- ~14-month window we typically render, the result is bounded at ~420
-- rows — well under any reasonable cap.
--
-- Sorting by gain (not abs gain) intentionally: keeps the two
-- contributor/detractor tails distinct so a strongly-skewed month
-- (lots of positives, few negatives) still surfaces the few real
-- detractors instead of getting drowned out.

CREATE OR REPLACE FUNCTION public.monthly_security_attribution(
    p_sub_client       text,
    p_trusts           text[]  DEFAULT NULL,
    p_accounts         text[]  DEFAULT NULL,
    p_asset_classes    text[]  DEFAULT NULL,
    p_from_month       date    DEFAULT NULL,
    p_excluded_trusts  text[]  DEFAULT NULL,
    p_top_per_month    integer DEFAULT NULL
)
RETURNS TABLE(
    month            date,
    security_id      bigint,
    asset_name       text,
    ticker_masttro   text,
    asset_class      text,
    start_mv         numeric,
    end_mv           numeric,
    flows            numeric,
    income           numeric,
    gain             numeric
)
LANGUAGE plpgsql
STABLE
SECURITY INVOKER
AS $$
#variable_conflict use_column
DECLARE
    v_internal_from date;
BEGIN
    v_internal_from := COALESCE(
        (date_trunc('month', p_from_month) - interval '1 month')::date,
        '1900-01-01'::date
    );

    RETURN QUERY
    WITH
    scoped_positions AS (
        SELECT
            ps.snapshot_date,
            ps.security_id,
            SUM(ps.mv_reporting) AS mv_total
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
          AND ps.snapshot_date >= v_internal_from
        GROUP BY ps.snapshot_date, ps.security_id
    ),
    monthly_end AS (
        SELECT DISTINCT ON (date_trunc('month', sp.snapshot_date), sp.security_id)
            date_trunc('month', sp.snapshot_date)::date AS month,
            sp.security_id,
            sp.mv_total AS end_mv
        FROM scoped_positions sp
        ORDER BY date_trunc('month', sp.snapshot_date),
                 sp.security_id,
                 sp.snapshot_date DESC
    ),
    monthly_txns AS (
        SELECT
            date_trunc('month', t.transaction_date)::date AS month,
            t.security_id,
            SUM(CASE WHEN t.transaction_type_clean IN ('Buy', 'Sell')
                     THEN -t.net_amount_reporting ELSE 0 END)::numeric AS flows,
            SUM(CASE WHEN t.transaction_type_clean IN ('Cash Dividends', 'Interest', 'Income')
                     THEN t.net_amount_reporting ELSE 0 END)::numeric AS income
        FROM public.transaction_log t
        LEFT JOIN public.entity_attribution ea ON ea.node_id = t.account_node_id
        LEFT JOIN public.security s            ON s.security_id = t.security_id
        WHERE ea.sub_client_alias = p_sub_client
          AND (COALESCE(array_length(p_trusts,          1), 0) = 0
               OR ea.trust_alias    = ANY(p_trusts))
          AND (COALESCE(array_length(p_accounts,        1), 0) = 0
               OR t.account_node_id = ANY(p_accounts))
          AND (COALESCE(array_length(p_excluded_trusts, 1), 0) = 0
               OR NOT (ea.trust_alias = ANY(p_excluded_trusts)))
          AND (COALESCE(array_length(p_asset_classes,   1), 0) = 0
               OR s.asset_class = ANY(p_asset_classes)
               OR (s.asset_class IS NULL AND 'Unclassified' = ANY(p_asset_classes)))
          AND t.transaction_type_clean IN ('Buy', 'Sell',
                                           'Cash Dividends', 'Interest', 'Income')
          AND t.transaction_date >= v_internal_from
          AND t.security_id IS NOT NULL
        GROUP BY date_trunc('month', t.transaction_date), t.security_id
    ),
    universe AS (
        SELECT month, security_id FROM monthly_end
        UNION
        SELECT month, security_id FROM monthly_txns
    ),
    enriched AS (
        SELECT
            u.month,
            u.security_id,
            COALESCE(me.end_mv,      0)::numeric AS end_mv,
            COALESCE(prev_me.end_mv, 0)::numeric AS start_mv,
            COALESCE(mt.flows,       0)::numeric AS flows,
            COALESCE(mt.income,      0)::numeric AS income,
            ((COALESCE(me.end_mv, 0) - COALESCE(prev_me.end_mv, 0))
                - COALESCE(mt.flows, 0)
                + COALESCE(mt.income, 0))::numeric AS gain
        FROM universe u
        LEFT JOIN monthly_end me
               ON me.month = u.month AND me.security_id = u.security_id
        LEFT JOIN monthly_end prev_me
               ON prev_me.security_id = u.security_id
              AND prev_me.month = (u.month - interval '1 month')::date
        LEFT JOIN monthly_txns mt
               ON mt.month = u.month AND mt.security_id = u.security_id
    ),
    ranked AS (
        SELECT
            e.*,
            ROW_NUMBER() OVER (PARTITION BY e.month ORDER BY e.gain DESC) AS rank_pos,
            ROW_NUMBER() OVER (PARTITION BY e.month ORDER BY e.gain ASC)  AS rank_neg
        FROM enriched e
        WHERE e.month >= COALESCE(p_from_month, '1900-01-01'::date)
    )
    SELECT
        r.month,
        r.security_id::bigint,
        s.asset_name::text,
        s.ticker_masttro::text,
        COALESCE(s.asset_class, 'Unclassified')::text AS asset_class,
        r.start_mv,
        r.end_mv,
        r.flows,
        r.income,
        r.gain
    FROM ranked r
    LEFT JOIN public.security s ON s.security_id = r.security_id
    WHERE p_top_per_month IS NULL
       OR r.rank_pos <= p_top_per_month
       OR r.rank_neg <= p_top_per_month
    ORDER BY r.month, r.gain DESC;
END;
$$;
