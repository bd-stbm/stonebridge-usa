-- 008 — v_nav_monthly_by_asset_class.
--
-- Per (snapshot_date × account × asset_class) NAV aggregation. Lets the
-- dashboard compute historical period returns scoped to a single asset
-- class (Equity, Fixed Income, etc.) for the Returns tile's "split by"
-- dropdown.
--
-- Positions without an asset class on security (rare but possible) are
-- bucketed under 'Unclassified' so they don't disappear from the totals.
--
-- security_invoker = true so RLS on the underlying tables gates row
-- visibility through the view.

CREATE OR REPLACE VIEW public.v_nav_monthly_by_asset_class
WITH (security_invoker = true) AS
SELECT
    ps.snapshot_date,
    ps.account_node_id,
    e.alias AS account_alias,
    ea.trust_alias,
    ea.sub_client_alias,
    COALESCE(s.asset_class, 'Unclassified') AS asset_class,
    SUM(ps.mv_reporting) AS nav_reporting
FROM public.position_snapshot ps
JOIN public.security s          ON s.security_id = ps.security_id
JOIN public.entity   e          ON e.node_id     = ps.account_node_id
LEFT JOIN public.entity_attribution ea ON ea.node_id = ps.account_node_id
GROUP BY
    ps.snapshot_date,
    ps.account_node_id,
    e.alias,
    ea.trust_alias,
    ea.sub_client_alias,
    s.asset_class;
