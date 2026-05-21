-- 018 — reconstructed_nav_at(): add p_excluded_trusts text[].
--
-- The dashboard hides a few entities by client preference (Sibling Trusts,
-- The Family Trust) via a config in web/lib/queries.ts. PostgREST queries
-- apply this as a .not("trust_alias", "in", ...) clause, but the
-- reconstructed_nav_at RPC aggregates server-side and bypasses that
-- per-query filter — so a 6M / 1Y start NAV without the exclusion would
-- silently include the hidden entities, throwing off the period return.
--
-- Add p_excluded_trusts text[] DEFAULT NULL. When non-empty, drop any
-- scoped row whose trust_alias is in the list. Empty/null preserves the
-- pre-018 behaviour exactly. Drop the old 5-arg signature so PostgREST
-- can resolve calls unambiguously.

DROP FUNCTION IF EXISTS public.reconstructed_nav_at(text, text[], text[], date, text);

CREATE OR REPLACE FUNCTION public.reconstructed_nav_at(
    p_sub_client       text,
    p_trusts           text[]  DEFAULT NULL,
    p_accounts         text[]  DEFAULT NULL,
    p_target_date      date    DEFAULT NULL,
    p_asset_class      text    DEFAULT NULL,
    p_excluded_trusts  text[]  DEFAULT NULL
)
RETURNS numeric
LANGUAGE plpgsql
STABLE
SECURITY INVOKER
AS $$
DECLARE
    v_anchor_date date;
    v_result numeric;
BEGIN
    SELECT MAX(snapshot_date) INTO v_anchor_date
    FROM public.position_snapshot
    WHERE snapshot_date <= p_target_date;

    IF v_anchor_date IS NULL THEN
        RETURN NULL;
    END IF;

    WITH scoped AS (
        SELECT
            ps.security_id,
            ps.mv_reporting,
            s.ticker_yf
        FROM public.position_snapshot ps
        LEFT JOIN public.entity_attribution ea ON ea.node_id = ps.account_node_id
        LEFT JOIN public.security s            ON s.security_id = ps.security_id
        WHERE ps.snapshot_date = v_anchor_date
          AND ea.sub_client_alias = p_sub_client
          AND (COALESCE(array_length(p_trusts,   1), 0) = 0
               OR ea.trust_alias    = ANY(p_trusts))
          AND (COALESCE(array_length(p_accounts, 1), 0) = 0
               OR ps.account_node_id = ANY(p_accounts))
          AND (COALESCE(array_length(p_excluded_trusts, 1), 0) = 0
               OR NOT (ea.trust_alias = ANY(p_excluded_trusts)))
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
              AND sph.price_date <= v_anchor_date
            ORDER BY sph.price_date DESC
            LIMIT 1
        ) pa ON TRUE
    )
    SELECT SUM(
        CASE
            WHEN sp.ticker_yf IS NOT NULL
             AND tp.target_close IS NOT NULL
             AND tp.anchor_close IS NOT NULL
             AND tp.anchor_close <> 0
            THEN sp.mv_reporting * (tp.target_close / tp.anchor_close)
            ELSE sp.mv_reporting
        END
    )
    INTO v_result
    FROM scoped sp
    LEFT JOIN ticker_prices tp ON tp.ticker_yf = sp.ticker_yf;

    RETURN v_result;
END;
$$;

GRANT EXECUTE ON FUNCTION public.reconstructed_nav_at(text, text[], text[], date, text, text[])
    TO anon, authenticated, service_role;
