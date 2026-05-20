-- 014 — reconstructed_nav_at(): add optional p_asset_class filter.
--
-- Why: the Overview page's per-asset-class returns previously used the
-- snapshot-grid path (month-end NAVs only), so the start date for 6M / 1Y
-- snapped to the previous month-end. That mismatch with the Total path
-- (which already used the precise reconstructed RPC) caused the benchmark
-- column to read different values when switching between Total and an
-- asset-class slice — because computeIndexReturn() uses the portfolio's
-- own start/end dates to slice the index price series.
--
-- New parameter:
--   p_asset_class TEXT DEFAULT NULL
--     - NULL → no filter (original Total behaviour, unchanged)
--     - non-empty string → match security.asset_class exactly
--     - empty string → match security.asset_class IS NULL
--       (mirrors the page's "Unclassified" bucket convention)
--
-- Body is otherwise identical to 013's LATERAL-seek rewrite.

CREATE OR REPLACE FUNCTION public.reconstructed_nav_at(
    p_sub_client  text,
    p_trusts      text[]  DEFAULT NULL,
    p_accounts    text[]  DEFAULT NULL,
    p_target_date date    DEFAULT NULL,
    p_asset_class text    DEFAULT NULL
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
          AND (p_asset_class IS NULL
               OR (p_asset_class = '' AND s.asset_class IS NULL)
               OR s.asset_class = p_asset_class)
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

GRANT EXECUTE ON FUNCTION public.reconstructed_nav_at(text, text[], text[], date, text)
    TO anon, authenticated, service_role;
