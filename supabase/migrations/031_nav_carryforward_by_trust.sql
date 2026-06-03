-- 031 — per-account carry-forward NAV per trust (fix spurious entity returns).
--
-- The performance matrix computed each entity's period return as
--   (endNav - startNav - flows) / (startNav + 0.5*flows)
-- where endNav is latest-snapshot-PER-ACCOUNT (v_positions_refreshed, which
-- carries a stale account's last value forward to "today") but startNav came
-- from the DATE-EXACT NAV series (v_nav_monthly_by_asset_class on one date).
-- When an account's feed goes stale, it's in endNav but NOT in startNav, so
-- its whole value shows up as a phantom gain. Example: Dyne US Retirement
-- showed +42% MTD because "Bermeister Superannuation Fund" (~$2.3M) last
-- reported 2026-05-28 — carried into endNav but absent from the May/June
-- date-exact start. (nav_at_or_before, migration 019, can't fix this: it
-- also keys off a single anchor date and drops accounts not present on it.)
--
-- This function returns, per trust, the carry-forward NAV at a target date:
-- each account's latest snapshot ON OR BEFORE the target, summed. So a stale
-- account contributes its last-known value to BOTH the start and the end,
-- and the spurious gain disappears. Returned to computePeriodReturn as the
-- startNavByPeriod override (same mechanism the Overview uses for 6M/1Y).
--
-- SECURITY INVOKER so family RLS + the aal2 requirement apply, exactly like
-- nav_at_or_before. Filters mirror the matrix's scoping (sub_client / trusts
-- / accounts / visible asset_classes / excluded trusts).

CREATE OR REPLACE FUNCTION public.nav_carryforward_by_trust(
    p_sub_client       text,
    p_trusts           text[]  DEFAULT NULL,
    p_accounts         text[]  DEFAULT NULL,
    p_target_date      date    DEFAULT NULL,
    p_asset_classes    text[]  DEFAULT NULL,
    p_excluded_trusts  text[]  DEFAULT NULL
)
RETURNS TABLE(trust_alias text, nav numeric, anchor_date date)
LANGUAGE sql
STABLE
SECURITY INVOKER
AS $$
    -- Restrict the per-account MAX scan to this sub-client's accounts via the
    -- denormalised sub_client_node_id (+ its index, migration 028) instead of
    -- scanning every family's history.
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
    SELECT ea.trust_alias,
           COALESCE(SUM(ps.mv_reporting), 0)::numeric AS nav,
           MAX(ps.snapshot_date) AS anchor_date
    FROM public.position_snapshot ps
    JOIN latest_per_account lpa
      ON lpa.account_node_id = ps.account_node_id
     AND lpa.md = ps.snapshot_date
    JOIN public.entity_attribution ea ON ea.node_id    = ps.account_node_id
    JOIN public.security           s  ON s.security_id  = ps.security_id
    WHERE ea.sub_client_alias = p_sub_client
      AND ea.trust_alias IS NOT NULL
      AND (COALESCE(array_length(p_trusts, 1), 0) = 0
           OR ea.trust_alias = ANY(p_trusts))
      AND (COALESCE(array_length(p_accounts, 1), 0) = 0
           OR ps.account_node_id = ANY(p_accounts))
      AND (COALESCE(array_length(p_asset_classes, 1), 0) = 0
           OR s.asset_class = ANY(p_asset_classes))
      AND (COALESCE(array_length(p_excluded_trusts, 1), 0) = 0
           OR NOT (ea.trust_alias = ANY(p_excluded_trusts)))
    GROUP BY ea.trust_alias;
$$;

GRANT EXECUTE ON FUNCTION
    public.nav_carryforward_by_trust(text, text[], text[], date, text[], text[])
    TO anon, authenticated, service_role;
