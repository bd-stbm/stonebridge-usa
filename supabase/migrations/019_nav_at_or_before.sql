-- 019 — drop reconstructed_nav_at, introduce nav_at_or_before.
--
-- Background: the dashboard used reconstructed_nav_at to estimate 1Y / 6M
-- start NAVs by taking the latest position_snapshot ≤ target_date and
-- repricing it forward to target_date via yfinance closes. That gave a
-- "true 1Y" estimate but didn't match Masttro because:
--   1. Masttro's API only exposes month-end historicals, so its UI shows
--      the raw month-end NAV (e.g. Apr 30) as the 1Y start, not a
--      yfinance reconstruction at the exact 1Y date.
--   2. Our reconstruction held ~6% of equity securities flat (no yfinance
--      ticker mapping) and didn't reprice FX, so it diverged further.
--
-- Decision: match Masttro. Use the raw snapshot NAV at the latest snapshot
-- date on or before the target, and surface that anchor date so the UI
-- labels the start honestly.
--
-- Function shape:
--   nav_at_or_before(...) RETURNS TABLE(nav numeric, anchor_date date)
--   Same filters as the old RPC (sub_client, trusts, accounts, asset_class,
--   excluded_trusts). Empty result when there's no snapshot ≤ target.

DROP FUNCTION IF EXISTS public.reconstructed_nav_at(text, text[], text[], date, text, text[]);

CREATE OR REPLACE FUNCTION public.nav_at_or_before(
    p_sub_client       text,
    p_trusts           text[]  DEFAULT NULL,
    p_accounts         text[]  DEFAULT NULL,
    p_target_date      date    DEFAULT NULL,
    p_asset_class      text    DEFAULT NULL,
    p_excluded_trusts  text[]  DEFAULT NULL
)
RETURNS TABLE(nav numeric, anchor_date date)
LANGUAGE plpgsql
STABLE
SECURITY INVOKER
AS $$
DECLARE
    v_anchor_date date;
BEGIN
    SELECT MAX(snapshot_date) INTO v_anchor_date
    FROM public.position_snapshot
    WHERE snapshot_date <= p_target_date;

    IF v_anchor_date IS NULL THEN
        RETURN;
    END IF;

    RETURN QUERY
    SELECT
        COALESCE(SUM(ps.mv_reporting), 0)::numeric AS nav,
        v_anchor_date AS anchor_date
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
           OR s.asset_class = p_asset_class);
END;
$$;

GRANT EXECUTE ON FUNCTION public.nav_at_or_before(text, text[], text[], date, text, text[])
    TO anon, authenticated, service_role;
