-- 020 — push asset_class filter down into the NAV aggregation.
--
-- After the global asset_class filter shipped (commit 77c9f35),
-- getNavSeries and getNavSeriesByTrust started routing through
-- v_nav_monthly_by_asset_class when a filter was selected. That view
-- GROUP BYs the full position_snapshot first, then PostgREST filters
-- on asset_class — so the filter never short-circuits the heavy
-- aggregation step. Page loads with a class selected were noticeably
-- slower than with no filter, contrary to the natural intuition that
-- "less data should be faster".
--
-- Separately, nav_at_or_before only accepts a single text asset_class,
-- so the TS wrapper getNavAtOrBeforeForClasses fanned out N parallel
-- RPC calls when N classes were selected — also avoidable.
--
-- This migration:
--   1. Replaces nav_at_or_before's signature: p_asset_class text →
--      p_asset_classes text[]. Internally the filter still pushes
--      ANY(...) onto position_snapshot before the SUM, just like the
--      previous version did for a single class. NULL / empty array =
--      no asset_class filter.
--   2. Adds nav_series_filtered(...) — per-date sum of mv_reporting
--      with the asset_class filter applied directly against
--      position_snapshot, before GROUP BY. Replaces the PostgREST hit
--      to v_nav_monthly_by_asset_class for getNavSeries when a filter
--      is active.
--   3. Adds nav_series_by_trust_filtered(...) — same idea but returns
--      per-(date, trust) for the Performance page's trust matrix.
--
-- "Unclassified" maps to NULL asset_class (matching the
-- v_nav_monthly_by_asset_class COALESCE convention surfaced through
-- the UI).

DROP FUNCTION IF EXISTS public.nav_at_or_before(text, text[], text[], date, text, text[]);

CREATE OR REPLACE FUNCTION public.nav_at_or_before(
    p_sub_client       text,
    p_trusts           text[]  DEFAULT NULL,
    p_accounts         text[]  DEFAULT NULL,
    p_target_date      date    DEFAULT NULL,
    p_asset_classes    text[]  DEFAULT NULL,
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
      AND (COALESCE(array_length(p_trusts,          1), 0) = 0
           OR ea.trust_alias    = ANY(p_trusts))
      AND (COALESCE(array_length(p_accounts,        1), 0) = 0
           OR ps.account_node_id = ANY(p_accounts))
      AND (COALESCE(array_length(p_excluded_trusts, 1), 0) = 0
           OR NOT (ea.trust_alias = ANY(p_excluded_trusts)))
      AND (COALESCE(array_length(p_asset_classes,   1), 0) = 0
           OR s.asset_class = ANY(p_asset_classes)
           OR (s.asset_class IS NULL AND 'Unclassified' = ANY(p_asset_classes)));
END;
$$;

GRANT EXECUTE ON FUNCTION public.nav_at_or_before(text, text[], text[], date, text[], text[])
    TO anon, authenticated, service_role;


-- Per-date NAV with asset_class filter pushed down to position_snapshot.
-- Returns one row per snapshot_date with the SUM of mv_reporting across
-- matching positions. Caller is responsible for filtering historical
-- snapshots client-side if needed (no fromDate parameter — we typically
-- want the full series for chart and reconciliation).
CREATE OR REPLACE FUNCTION public.nav_series_filtered(
    p_sub_client       text,
    p_trusts           text[]  DEFAULT NULL,
    p_accounts         text[]  DEFAULT NULL,
    p_asset_classes    text[]  DEFAULT NULL,
    p_excluded_trusts  text[]  DEFAULT NULL
)
RETURNS TABLE(snapshot_date date, nav_reporting numeric)
LANGUAGE sql
STABLE
SECURITY INVOKER
AS $$
    SELECT
        ps.snapshot_date,
        SUM(ps.mv_reporting)::numeric AS nav_reporting
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
    GROUP BY ps.snapshot_date
    ORDER BY ps.snapshot_date;
$$;

GRANT EXECUTE ON FUNCTION public.nav_series_filtered(text, text[], text[], text[], text[])
    TO anon, authenticated, service_role;


-- Per-(date, trust) NAV with the same filter pushdown — drives the
-- Performance page's by-trust matrix when an asset_class filter is set.
CREATE OR REPLACE FUNCTION public.nav_series_by_trust_filtered(
    p_sub_client       text,
    p_trusts           text[]  DEFAULT NULL,
    p_accounts         text[]  DEFAULT NULL,
    p_asset_classes    text[]  DEFAULT NULL,
    p_excluded_trusts  text[]  DEFAULT NULL
)
RETURNS TABLE(snapshot_date date, trust_alias text, nav_reporting numeric)
LANGUAGE sql
STABLE
SECURITY INVOKER
AS $$
    SELECT
        ps.snapshot_date,
        ea.trust_alias,
        SUM(ps.mv_reporting)::numeric AS nav_reporting
    FROM public.position_snapshot ps
    LEFT JOIN public.entity_attribution ea ON ea.node_id = ps.account_node_id
    LEFT JOIN public.security s            ON s.security_id = ps.security_id
    WHERE ea.sub_client_alias = p_sub_client
      AND ea.trust_alias IS NOT NULL
      AND (COALESCE(array_length(p_trusts,          1), 0) = 0
           OR ea.trust_alias    = ANY(p_trusts))
      AND (COALESCE(array_length(p_accounts,        1), 0) = 0
           OR ps.account_node_id = ANY(p_accounts))
      AND (COALESCE(array_length(p_excluded_trusts, 1), 0) = 0
           OR NOT (ea.trust_alias = ANY(p_excluded_trusts)))
      AND (COALESCE(array_length(p_asset_classes,   1), 0) = 0
           OR s.asset_class = ANY(p_asset_classes)
           OR (s.asset_class IS NULL AND 'Unclassified' = ANY(p_asset_classes)))
    GROUP BY ps.snapshot_date, ea.trust_alias
    ORDER BY ps.snapshot_date;
$$;

GRANT EXECUTE ON FUNCTION public.nav_series_by_trust_filtered(text, text[], text[], text[], text[])
    TO anon, authenticated, service_role;
