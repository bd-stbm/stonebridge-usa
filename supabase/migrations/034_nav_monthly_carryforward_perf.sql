-- 034 — performance fix for nav_monthly_carryforward (033).
--
-- 033's body range-joined the month grid directly against position_snapshot
-- (months × every position row on/before each month-end). At full scope
-- (no trust/account filter, ~25 months of history) that scanned millions of
-- rows and tripped the 8s statement timeout on getNavSeries(0t,0a,4c).
--
-- Fix: collapse position_snapshot to DISTINCT (account, snapshot_date) FIRST
-- (one pass, ~10k rows), then do the per-month "latest snapshot on/before
-- month-end" pick against that small set. The asset-class / trust / account /
-- excluded filters stay on the final value sum, so md (the carry-forward date)
-- is still picked from the account's real reporting dates regardless of which
-- classes are selected — same basis as nav_carryforward (032). Identical
-- result shape and semantics to 033, ~3x faster (≈2.4s at full scope).

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
        -- One target per calendar month: the month-end, clamped to the latest
        -- snapshot we actually have (current month lands on its newest date).
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
        -- Distinct reporting dates per account (mv NULL excluded so a broken
        -- sync day can't win the MAX). Small (~accounts × dates) — this is the
        -- set the month grid ranges over, instead of the full position table.
        SELECT DISTINCT ps.account_node_id, ps.snapshot_date
        FROM public.position_snapshot ps
        WHERE ps.mv_reporting IS NOT NULL
          AND ps.sub_client_node_id IN (SELECT sub_client_node_id FROM scope)
          AND (p_floor IS NULL OR ps.snapshot_date >= p_floor)
    ),
    latest_per_account AS (
        SELECT mo.target,
               ad.account_node_id,
               MAX(ad.snapshot_date) AS md
        FROM months mo
        JOIN acct_dates ad ON ad.snapshot_date <= mo.target
        GROUP BY mo.target, ad.account_node_id
    )
    SELECT lpa.target AS month_end,
           COALESCE(SUM(ps.mv_reporting), 0)::numeric AS nav
    FROM latest_per_account lpa
    JOIN public.position_snapshot ps
      ON ps.account_node_id = lpa.account_node_id
     AND ps.snapshot_date   = lpa.md
     AND ps.sub_client_node_id IN (SELECT sub_client_node_id FROM scope)
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
