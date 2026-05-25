-- 022 — per-(month, security) attribution for the Performance page drill-in.
--
-- Powers the Top Contributors / Top Detractors panel: when a user clicks
-- a month in the monthly-returns bar chart we need each security's
-- contribution to that month's portfolio gain, plus its own per-holding
-- return for the same window.
--
-- Per security S in calendar month M:
--   end_mv = sum of mv_reporting across in-scope accounts at the last
--            snapshot in M (month-end for completed months, latest
--            daily for the current month)
--   start_mv = end_mv from the prior month, or 0 if S wasn't held
--   flows = -sum(net_amount_reporting) for Buy + Sell on S during M
--           Sign convention: +ve = net cash flowed INTO the security
--           (matching getFlowsByAssetClass).
--   income = sum(net_amount_reporting) for Cash Dividends + Interest +
--            Income on S during M. Positive = received.
--   gain = (end_mv - start_mv) - flows + income
--          Total-return view: dividends/interest received count toward
--          the security's gain even though they leave the security's MV
--          (per user preference, matches the asset-class flow rule
--          shipped in commit 77c9f35).
--
-- The universe of (month, security) is the UNION of positions and
-- transactions in scope — so a security that was fully sold mid-month
-- still appears with end_mv=0 + flows < 0, attributing the realized
-- gain/loss correctly.
--
-- p_from_month is interpreted inclusively at the month-level; we pull
-- one extra calendar month back internally so the earliest reported
-- month has a real start_mv (the prior month-end). Callers should pass
-- the earliest month they want in the result.

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
DECLARE
    v_internal_from date;
BEGIN
    -- Pull one extra month back internally so start_mv is non-null for
    -- the earliest reported month. Final result is filtered to
    -- p_from_month at the bottom.
    v_internal_from := COALESCE(
        (date_trunc('month', p_from_month) - interval '1 month')::date,
        '1900-01-01'::date
    );

    RETURN QUERY
    WITH
    -- All positions in scope, grouped per (date, security) across accounts.
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
    -- Per (year-month × security), MV at the last snapshot in that month.
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
    -- Per (year-month × security), flow + income aggregates.
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
    -- Universe of (month × security) — UNION so sold-out positions still
    -- attribute the realized gain in the month they exited.
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

GRANT EXECUTE ON FUNCTION public.monthly_security_attribution(
    text, text[], text[], text[], date, text[]
) TO anon, authenticated, service_role;
