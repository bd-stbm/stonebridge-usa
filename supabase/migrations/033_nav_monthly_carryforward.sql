-- 033 — per-account carry-forward MONTHLY NAV series (NAV-over-time chart +
-- monthly-returns bar chart).
--
-- The chart path (getNavSeries -> v_nav_monthly_by_asset_class) summed
-- positions stamped on the single LATEST snapshot date present in each
-- calendar month. When one account's last business-day snapshot lags the
-- others, that account is dropped from the month's bucket entirely, and its
-- value vanishes from the line as a spurious dip. Example: for Dyne US
-- Retirement the two IRAs last reported Fri 29 May but the Bermeister
-- Superannuation Fund (~$2.3M) last reported Thu 28 May, so the May point
-- collapsed to $5.5M instead of $7.8M, then "recovered" in June. (31 May 2026
-- was a Sunday, so 29 May is the real month-end — but the per-account date
-- skew is the bug.)
--
-- Same fix as the returns side (031/032): value each account at its latest
-- snapshot ON OR BEFORE the month-end, then sum — the carry-forward basis that
-- matches endNav (v_positions_refreshed) and nav_carryforward. Here it is
-- applied across a continuous monthly grid so the whole series is consistent.
--
-- Each completed month anchors on its calendar month-end (carry-forward fills
-- the last-business-day skew); the current month anchors on the latest
-- snapshot in scope. NULL-market-value snapshots (e.g. a partially-ingested
-- day) are excluded from the per-account "latest" pick so a broken sync day
-- can't zero an account out.
--
-- SECURITY INVOKER (family RLS + aal2 apply). Scoped via the denormalised
-- sub_client_node_id index for speed, same as nav_carryforward (032).

CREATE OR REPLACE FUNCTION public.nav_monthly_carryforward(
    p_sub_client       text,
    p_trusts           text[]  DEFAULT NULL,
    p_accounts         text[]  DEFAULT NULL,
    p_asset_classes    text[]  DEFAULT NULL,
    p_excluded_trusts  text[]  DEFAULT NULL,
    p_floor            date    DEFAULT NULL
)
RETURNS TABLE(month_end date, nav numeric)
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
    bounds AS (
        SELECT MIN(ps.snapshot_date) AS min_d,
               MAX(ps.snapshot_date) AS max_d
        FROM public.position_snapshot ps
        WHERE ps.sub_client_node_id IN (SELECT sub_client_node_id FROM scope)
          AND (p_floor IS NULL OR ps.snapshot_date >= p_floor)
    ),
    months AS (
        -- One target date per calendar month: the calendar month-end, clamped
        -- to the latest snapshot we actually have (so the current month lands
        -- on its newest snapshot rather than a future month-end).
        SELECT LEAST(
                   (date_trunc('month', gs) + interval '1 month - 1 day')::date,
                   (SELECT max_d FROM bounds)
               ) AS target
        FROM generate_series(
                 (SELECT date_trunc('month', min_d) FROM bounds),
                 (SELECT date_trunc('month', max_d) FROM bounds),
                 interval '1 month'
             ) AS gs
    ),
    latest_per_account AS (
        -- For each month target, each account's latest real snapshot on/before
        -- it. mv NULL excluded so a broken-sync day can't win the MAX().
        SELECT mo.target,
               ps.account_node_id,
               MAX(ps.snapshot_date) AS md
        FROM months mo
        JOIN public.position_snapshot ps
          ON ps.snapshot_date <= mo.target
         AND ps.mv_reporting IS NOT NULL
         AND ps.sub_client_node_id IN (SELECT sub_client_node_id FROM scope)
        GROUP BY mo.target, ps.account_node_id
    )
    SELECT lpa.target AS month_end,
           COALESCE(SUM(ps.mv_reporting), 0)::numeric AS nav
    FROM latest_per_account lpa
    JOIN public.position_snapshot ps
      ON ps.account_node_id = lpa.account_node_id
     AND ps.snapshot_date   = lpa.md
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
           OR NOT (ea.trust_alias = ANY(p_excluded_trusts)))
    GROUP BY lpa.target
    ORDER BY lpa.target;
$$;

GRANT EXECUTE ON FUNCTION
    public.nav_monthly_carryforward(text, text[], text[], text[], text[], date)
    TO anon, authenticated, service_role;
