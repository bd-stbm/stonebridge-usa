-- 012 — reconstructed_nav_at(): precise historical NAV at any target date.
--
-- Used by the Returns tile to compute exact-date 6M and 1Y start NAVs
-- (instead of snapping to the nearest month-end position_snapshot).
--
-- Inputs:
--   p_sub_client   — required sub-client alias (e.g. 'Dyne Family (US)')
--   p_trusts       — optional array of trust_alias filters. NULL/empty = all.
--   p_accounts     — optional array of account_node_id filters. NULL/empty = all.
--   p_target_date  — the date to value positions at.
--
-- Method:
--   1. Find anchor = MAX(snapshot_date) ≤ p_target_date — gives quantities.
--   2. For each anchor position, look up the nearest yfinance close on or
--      before p_target_date AND on or before anchor_date. Scale Masttro's
--      mv_reporting by the ratio (close_at_target / close_at_anchor).
--      Same TR-style ratio approach used by v_positions_refreshed for
--      today's NAV, but with both endpoints in the past.
--   3. Positions without a ticker_yf (private equity, real estate, hedge
--      funds, custom funds) fall back to Masttro's recorded mv_reporting at
--      the anchor — those NAVs aren't daily even at source.
--
-- Returns NULL when no snapshot exists on or before p_target_date — the
-- caller should fall back to the snapshot-grid approximation in that case.

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
    needed_tickers AS (
        SELECT DISTINCT ticker_yf FROM scoped WHERE ticker_yf IS NOT NULL
    ),
    price_at_target AS (
        SELECT DISTINCT ON (sph.ticker_yf)
            sph.ticker_yf,
            sph.close
        FROM public.security_price_history sph
        JOIN needed_tickers t ON t.ticker_yf = sph.ticker_yf
        WHERE sph.price_date <= p_target_date
        ORDER BY sph.ticker_yf, sph.price_date DESC
    ),
    price_at_anchor AS (
        SELECT DISTINCT ON (sph.ticker_yf)
            sph.ticker_yf,
            sph.close
        FROM public.security_price_history sph
        JOIN needed_tickers t ON t.ticker_yf = sph.ticker_yf
        WHERE sph.price_date <= (SELECT d FROM anchor)
        ORDER BY sph.ticker_yf, sph.price_date DESC
    )
    SELECT
        CASE
            WHEN (SELECT d FROM anchor) IS NULL THEN NULL
            ELSE SUM(
                CASE
                    WHEN sp.ticker_yf IS NOT NULL
                     AND pt.close IS NOT NULL
                     AND pa.close IS NOT NULL
                     AND pa.close <> 0
                    THEN sp.mv_reporting * (pt.close / pa.close)
                    ELSE sp.mv_reporting
                END
            )
        END
    FROM scoped sp
    LEFT JOIN price_at_target pt ON pt.ticker_yf = sp.ticker_yf
    LEFT JOIN price_at_anchor pa ON pa.ticker_yf = sp.ticker_yf;
$$;

GRANT EXECUTE ON FUNCTION public.reconstructed_nav_at(text, text[], text[], date)
    TO anon, authenticated, service_role;
