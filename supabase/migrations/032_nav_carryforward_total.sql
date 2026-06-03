-- 032 — scoped-total per-account carry-forward NAV (Overview returns tile).
--
-- Companion to 031's per-trust function. Migration 031 fixed the per-entity
-- performance MATRIX, but the Overview Returns tile uses a different path:
-- date-exact NAV series for MTD/YTD and nav_at_or_before (single anchor date)
-- for 6M/1Y. Both still drop a stale account that endNav carries forward, so
-- with the Entity filter set to a small entity (e.g. Dyne US Retirement) the
-- Overview still showed the +42% MTD phantom gain.
--
-- This returns the scoped-total carry-forward NAV at a target date: each
-- account valued at its latest snapshot ON OR BEFORE the target, summed
-- across the scope — the same basis as endNav. Same filter semantics as
-- nav_at_or_before (migration 019) so it's a drop-in for the Overview's start
-- NAVs, just per-account-carried-forward instead of single-anchor-date.
--
-- SECURITY INVOKER (family RLS + aal2 apply). Scoped to the sub-client's
-- accounts via the denormalised sub_client_node_id index for speed.

CREATE OR REPLACE FUNCTION public.nav_carryforward(
    p_sub_client       text,
    p_trusts           text[]  DEFAULT NULL,
    p_accounts         text[]  DEFAULT NULL,
    p_target_date      date    DEFAULT NULL,
    p_asset_classes    text[]  DEFAULT NULL,
    p_excluded_trusts  text[]  DEFAULT NULL
)
RETURNS TABLE(nav numeric, anchor_date date)
LANGUAGE sql
STABLE
SECURITY INVOKER
AS $$
    WITH scope AS (
        SELECT DISTINCT sub_client_node_id
        FROM public.entity_attribution
        WHERE sub_client_alias = p_sub_client
          AND sub_client_node_id IS NOT NULL
    ),
    latest_per_account AS (
        SELECT ps.account_node_id, MAX(ps.snapshot_date) AS md
        FROM public.position_snapshot ps
        WHERE ps.snapshot_date <= p_target_date
          AND ps.sub_client_node_id IN (SELECT sub_client_node_id FROM scope)
        GROUP BY ps.account_node_id
    )
    SELECT COALESCE(SUM(ps.mv_reporting), 0)::numeric AS nav,
           MAX(ps.snapshot_date) AS anchor_date
    FROM public.position_snapshot ps
    JOIN latest_per_account lpa
      ON lpa.account_node_id = ps.account_node_id
     AND lpa.md = ps.snapshot_date
    LEFT JOIN public.entity_attribution ea ON ea.node_id   = ps.account_node_id
    LEFT JOIN public.security           s  ON s.security_id = ps.security_id
    WHERE ea.sub_client_alias = p_sub_client
      AND (COALESCE(array_length(p_trusts, 1), 0) = 0
           OR ea.trust_alias = ANY(p_trusts))
      AND (COALESCE(array_length(p_accounts, 1), 0) = 0
           OR ps.account_node_id = ANY(p_accounts))
      AND (COALESCE(array_length(p_asset_classes, 1), 0) = 0
           OR s.asset_class = ANY(p_asset_classes))
      AND (COALESCE(array_length(p_excluded_trusts, 1), 0) = 0
           OR NOT (ea.trust_alias = ANY(p_excluded_trusts)));
$$;

GRANT EXECUTE ON FUNCTION
    public.nav_carryforward(text, text[], text[], date, text[], text[])
    TO anon, authenticated, service_role;
