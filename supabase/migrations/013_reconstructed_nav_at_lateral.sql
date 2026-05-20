-- 013 — reconstructed_nav_at(): rewrite with LATERAL subqueries.
--
-- Original 012 version used `DISTINCT ON (ticker_yf) ... ORDER BY
-- ticker_yf, price_date DESC` against security_price_history with a date
-- ceiling. For 6M / 1Y targets that ceiling covers most of the table, so
-- Postgres scanned ~5 years of daily closes per ticker, sorted, and
-- deduped — measured at ~5.5s per call on Vercel against the prod
-- Supabase project. With two calls per Overview render (6M + 1Y), that
-- dominated total page time.
--
-- This rewrite:
--   1. Pulls unique ticker_yf values out of `scoped` once.
--   2. For each unique ticker, runs two LATERAL `LIMIT 1` lookups —
--      latest close on/before the target date, latest on/before the
--      anchor date. Each is a B-tree seek on the existing PK
--      `(ticker_yf, price_date)`, O(log N).
--
-- Semantics are identical: same anchor-date logic, same close-ratio
-- formula, same NULL handling. Only the join shape changed.

CREATE OR REPLACE FUNCTION public.reconstructed_nav_at(
    p_sub_client  text,
    p_trusts      text[]  DEFAULT NULL,
    p_accounts    text[]  DEFAULT NULL,
    p_target_date date    DEFAULT NULL
)
RETURNS numeric
LANGUAGE sql
STABLE
SECURITY INVOKER
AS $$
    WITH anchor AS (
        SELECT MAX(snapshot_date) AS d
        FROM public.position_snapshot
        WHERE snapshot_date <= p_target_date
    ),
    scoped AS (
        SELECT
            ps.security_id,
            ps.mv_reporting,
            s.ticker_yf
        FROM anchor a
        JOIN public.position_snapshot ps ON ps.snapshot_date = a.d
        LEFT JOIN public.entity_attribution ea ON ea.node_id = ps.account_node_id
        LEFT JOIN public.security s ON s.security_id = ps.security_id
        WHERE ea.sub_client_alias = p_sub_client
          AND (COALESCE(array_length(p_trusts,   1), 0) = 0
               OR ea.trust_alias    = ANY(p_trusts))
          AND (COALESCE(array_length(p_accounts, 1), 0) = 0
               OR ps.account_node_id = ANY(p_accounts))
    ),
    ticker_prices AS (
        SELECT
            t.ticker_yf,
            pt.close AS target_close,
            pa.close AS anchor_close
        FROM (SELECT DISTINCT ticker_yf FROM scoped WHERE ticker_yf IS NOT NULL) t
        LEFT JOIN LATERAL (
            SELECT close
            FROM public.security_price_history sph
            WHERE sph.ticker_yf  = t.ticker_yf
              AND sph.price_date <= p_target_date
            ORDER BY sph.price_date DESC
            LIMIT 1
        ) pt ON TRUE
        LEFT JOIN LATERAL (
            SELECT close
            FROM public.security_price_history sph
            WHERE sph.ticker_yf  = t.ticker_yf
              AND sph.price_date <= (SELECT d FROM anchor)
            ORDER BY sph.price_date DESC
            LIMIT 1
        ) pa ON TRUE
    )
    SELECT
        CASE
            WHEN (SELECT d FROM anchor) IS NULL THEN NULL
            ELSE SUM(
                CASE
                    WHEN sp.ticker_yf IS NOT NULL
                     AND tp.target_close IS NOT NULL
                     AND tp.anchor_close IS NOT NULL
                     AND tp.anchor_close <> 0
                    THEN sp.mv_reporting * (tp.target_close / tp.anchor_close)
                    ELSE sp.mv_reporting
                END
            )
        END
    FROM scoped sp
    LEFT JOIN ticker_prices tp ON tp.ticker_yf = sp.ticker_yf;
$$;

GRANT EXECUTE ON FUNCTION public.reconstructed_nav_at(text, text[], text[], date)
    TO anon, authenticated, service_role;
