-- 023 — fix "column reference 'month' is ambiguous" in
-- monthly_security_attribution (migration 022).
--
-- Cause: the function declares RETURNS TABLE(month date, ...). In
-- plpgsql those output column names are function-scoped variables. The
-- body's CTEs also produce a column named `month` (from date_trunc),
-- so unqualified `SELECT month FROM monthly_end` inside the UNION
-- becomes ambiguous between the variable and the column, and Postgres
-- raises 42702 rather than guessing.
--
-- Fix: add `#variable_conflict use_column` at the top of the function
-- body. Tells plpgsql to resolve ambiguous identifiers to column
-- references first, OUT variables second. Standard idiom — same body
-- is otherwise unchanged from 022.

CREATE OR REPLACE FUNCTION public.monthly_security_attribution(
    p_sub_client       text,
    p_trusts           text[]  DEFAULT NULL,
    p_accounts         text[]  DEFAULT NULL,
    p_asset_classes    text[]  DEFAULT NULL,
    p_from_month       date    DEFAULT NULL,
    p_excluded_trusts  text[]  DEFAULT NULL
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
            COALESCE(mt.income,      0)::numeric AS income
        FROM universe u
        LEFT JOIN monthly_end me
               ON me.month = u.month AND me.security_id = u.security_id
        LEFT JOIN monthly_end prev_me
               ON prev_me.security_id = u.security_id
              AND prev_me.month = (u.month - interval '1 month')::date
        LEFT JOIN monthly_txns mt
               ON mt.month = u.month AND mt.security_id = u.security_id
    )
    SELECT
        e.month,
        e.security_id::bigint,
        s.asset_name::text,
        s.ticker_masttro::text,
        COALESCE(s.asset_class, 'Unclassified')::text AS asset_class,
        e.start_mv,
        e.end_mv,
        e.flows,
        e.income,
        ((e.end_mv - e.start_mv) - e.flows + e.income)::numeric AS gain
    FROM enriched e
    LEFT JOIN public.security s ON s.security_id = e.security_id
    WHERE e.month >= COALESCE(p_from_month, '1900-01-01'::date)
    ORDER BY e.month, e.security_id;
END;
$$;
