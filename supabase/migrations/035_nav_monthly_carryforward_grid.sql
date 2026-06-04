-- 035 — materialise the per-account carry-forward monthly NAV grid.
--
-- 033/034 computed the carry-forward series live on every getNavSeries call
-- (~2.2s at full scope). The carry-forward basis is correct (it's the only
-- honest representation while Masttro finalises each account's month-end on
-- its own multi-week lag — a super fund / slow custodian is genuinely absent
-- from a recent month-end, so a date-exact sum drops it and the chart dips).
-- But recomputing it per request is wasteful: the inputs only change when the
-- daily sync writes new snapshots.
--
-- This precomputes the carry-forward NAV per (month_end, account, asset_class)
-- into a table, refreshed once per sync. Reads collapse to a fast indexed
-- aggregate that still respects every filter (trust / account / asset_class /
-- excluded). nav_monthly_carryforward()'s signature is unchanged — only its
-- body swaps from live-compute to grid-read — so getNavSeries needs no change.

-- --- Grid table -----------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.nav_monthly_carryforward_grid (
    month_end          date    NOT NULL,
    account_node_id    text    NOT NULL,
    trust_alias        text,
    sub_client_alias   text,
    sub_client_node_id text,            -- for the family-scoped RLS policy
    asset_class        text    NOT NULL,
    nav                numeric NOT NULL,
    PRIMARY KEY (month_end, account_node_id, asset_class)
);

CREATE INDEX IF NOT EXISTS nav_grid_subclient_month_idx
    ON public.nav_monthly_carryforward_grid (sub_client_alias, month_end);
CREATE INDEX IF NOT EXISTS nav_grid_scn_idx
    ON public.nav_monthly_carryforward_grid (sub_client_node_id);

-- --- RLS: same family-scoped read as position_snapshot (028/029) ----------
-- security_invoker views / SECURITY INVOKER functions read this as the
-- calling user, so the policy is the boundary. No write policy: only
-- service_role (RLS-bypass, used by the Python refresh) can mutate it.
ALTER TABLE public.nav_monthly_carryforward_grid ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "family scoped read" ON public.nav_monthly_carryforward_grid;
CREATE POLICY "family scoped read" ON public.nav_monthly_carryforward_grid
    FOR SELECT TO authenticated
    USING ((SELECT public.is_admin())
           OR sub_client_node_id IN (SELECT unnest(public.current_user_sub_clients())));

GRANT SELECT ON public.nav_monthly_carryforward_grid TO authenticated, anon, service_role;

-- --- Refresh: full recompute across ALL sub-clients -----------------------
-- Run by the daily sync (service_role) after position_snapshot is updated.
-- Carry-forward per (month, account): each account valued at its latest
-- snapshot ON OR BEFORE each calendar month-end (clamped to the latest
-- snapshot that exists), broken out by asset_class. Same logic as 034, minus
-- the read-time filters and without collapsing account/class granularity.
-- NULL-mv snapshots are excluded so a broken sync day can't win the MAX().
CREATE OR REPLACE FUNCTION public.refresh_nav_monthly_carryforward_grid()
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
    n integer;
BEGIN
    TRUNCATE public.nav_monthly_carryforward_grid;

    WITH bounds AS (
        SELECT MIN(snapshot_date) AS min_d, MAX(snapshot_date) AS max_d
        FROM public.position_snapshot
    ),
    months AS (
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
    acct_dates AS (
        SELECT DISTINCT account_node_id, snapshot_date
        FROM public.position_snapshot
        WHERE mv_reporting IS NOT NULL
    ),
    latest_per_account AS (
        SELECT mo.target, ad.account_node_id, MAX(ad.snapshot_date) AS md
        FROM months mo
        JOIN acct_dates ad ON ad.snapshot_date <= mo.target
        GROUP BY mo.target, ad.account_node_id
    )
    INSERT INTO public.nav_monthly_carryforward_grid
        (month_end, account_node_id, trust_alias, sub_client_alias,
         sub_client_node_id, asset_class, nav)
    SELECT lpa.target,
           ps.account_node_id,
           ea.trust_alias,
           ea.sub_client_alias,
           ea.sub_client_node_id,
           COALESCE(s.asset_class, 'Unclassified') AS asset_class,
           SUM(ps.mv_reporting)::numeric AS nav
    FROM latest_per_account lpa
    JOIN public.position_snapshot ps
      ON ps.account_node_id = lpa.account_node_id
     AND ps.snapshot_date   = lpa.md
    LEFT JOIN public.entity_attribution ea ON ea.node_id   = ps.account_node_id
    LEFT JOIN public.security           s  ON s.security_id = ps.security_id
    GROUP BY lpa.target, ps.account_node_id, ea.trust_alias,
             ea.sub_client_alias, ea.sub_client_node_id,
             COALESCE(s.asset_class, 'Unclassified');

    GET DIAGNOSTICS n = ROW_COUNT;
    RETURN n;
END
$$;

GRANT EXECUTE ON FUNCTION public.refresh_nav_monthly_carryforward_grid()
    TO service_role;

-- --- Read side: same signature as 033/034, now a grid lookup --------------
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
    SELECT g.month_end,
           COALESCE(SUM(g.nav), 0)::numeric AS nav
    FROM public.nav_monthly_carryforward_grid g
    WHERE g.sub_client_alias = p_sub_client
      AND (p_floor IS NULL OR g.month_end >= p_floor)
      AND (COALESCE(array_length(p_trusts, 1), 0) = 0
           OR g.trust_alias = ANY(p_trusts))
      AND (COALESCE(array_length(p_accounts, 1), 0) = 0
           OR g.account_node_id = ANY(p_accounts))
      AND (COALESCE(array_length(p_asset_classes, 1), 0) = 0
           OR g.asset_class = ANY(p_asset_classes))
      AND (COALESCE(array_length(p_excluded_trusts, 1), 0) = 0
           OR NOT (g.trust_alias = ANY(p_excluded_trusts)))
    GROUP BY g.month_end
    ORDER BY g.month_end;
$$;

-- Populate immediately so the first read after deploy is served from the grid.
SELECT public.refresh_nav_monthly_carryforward_grid();
