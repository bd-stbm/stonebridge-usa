-- 036_alt_position_snapshot.sql
-- Non-listed book (alts, direct PE/RE, business, loans, collections,
-- non-canonical cash) stored OUTSIDE position_snapshot so it can never leak
-- into the returns/Dietz pipeline. One row per (asset x ownership path): a
-- split-owned asset lands as several rows, each ownership-weighted.
-- See docs/all_assets_integration_design.md.
--
-- Apply manually in the Supabase SQL Editor (migrations don't auto-deploy).

CREATE TABLE IF NOT EXISTS public.alt_position_snapshot (
    snapshot_date         DATE   NOT NULL,
    security_id           BIGINT NOT NULL REFERENCES public.security(security_id),
    holding_node_id       TEXT   NOT NULL,   -- leaf reflection node for this path

    -- three display dimensions
    sub_client_node_id    TEXT   NOT NULL,   -- family (RLS key)
    sub_client_alias      TEXT,
    entity_node_id        TEXT,              -- rolled-up entity (nearest-existing walk)
    entity_alias          TEXT,
    vehicle_node_id       TEXT,              -- SPV = leaf trust_alias; NULL when == entity
    vehicle_alias         TEXT,

    -- ownership + value (reporting ccy only; these assets have no daily price)
    ownership_pct         NUMERIC(12, 8),    -- chain-product fraction for THIS path, 0..1
    full_value_reporting  NUMERIC,           -- 100% NAV of the asset
    mv_reporting          NUMERIC,           -- = full_value_reporting * ownership_pct (signed)
    reporting_ccy         CHAR(3),

    -- provenance / freshness
    value_source          TEXT,              -- 'cef' | 'gwm' | 'holdings'
    valuation_date        DATE,              -- point-in-time NAV date (often quarter-lagged)
    entity_rollup         TEXT,              -- 'existing' | 'branch-fallback'

    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (snapshot_date, holding_node_id, security_id)
);

COMMENT ON TABLE public.alt_position_snapshot IS
    'Non-listed holdings, ownership-weighted slice per (asset x owner path). Kept separate from position_snapshot so it never enters returns math.';

CREATE INDEX IF NOT EXISTS alt_pos_family_date_idx
    ON public.alt_position_snapshot (sub_client_node_id, snapshot_date);
CREATE INDEX IF NOT EXISTS alt_pos_entity_idx
    ON public.alt_position_snapshot (entity_node_id);
CREATE INDEX IF NOT EXISTS alt_pos_vehicle_idx
    ON public.alt_position_snapshot (vehicle_node_id) WHERE vehicle_node_id IS NOT NULL;

-- Family-scoped RLS, same shape as position_snapshot (migration 028).
-- The Python sync writes via service_role and bypasses RLS.
ALTER TABLE public.alt_position_snapshot ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "family scoped read" ON public.alt_position_snapshot;
CREATE POLICY "family scoped read" ON public.alt_position_snapshot
    FOR SELECT TO authenticated
    USING (public.is_admin()
           OR sub_client_node_id = ANY (public.current_user_sub_clients()));

-- Latest snapshot per (holding_node_id, security_id), joined to security for
-- the Masttro taxonomy. security_invoker so family RLS flows through.
CREATE OR REPLACE VIEW public.v_latest_alt_positions
WITH (security_invoker = true) AS
SELECT a.snapshot_date, a.security_id, a.holding_node_id,
       a.sub_client_node_id, a.sub_client_alias,
       a.entity_node_id, a.entity_alias, a.vehicle_node_id, a.vehicle_alias,
       a.ownership_pct, a.full_value_reporting, a.mv_reporting, a.reporting_ccy,
       a.value_source, a.valuation_date, a.entity_rollup,
       s.asset_class, s.security_type, s.asset_name
FROM (
    SELECT DISTINCT ON (holding_node_id, security_id) *
    FROM public.alt_position_snapshot
    ORDER BY holding_node_id, security_id, snapshot_date DESC
) a
JOIN public.security s ON s.security_id = a.security_id;

-- Combined net-worth view (listed + non-listed) on the common columns the
-- /networth page needs. account_alias is custody (listed only); vehicle_alias
-- is the SPV (non-listed only).
CREATE OR REPLACE VIEW public.v_net_worth_positions
WITH (security_invoker = true) AS
SELECT sub_client_alias,
       trust_alias        AS entity_alias,
       NULL::text         AS vehicle_alias,
       account_alias,
       asset_class, security_type,
       mv_reporting, reporting_ccy,
       'listed'::text     AS book
FROM public.v_latest_positions
UNION ALL
SELECT sub_client_alias,
       entity_alias,
       vehicle_alias,
       NULL::text         AS account_alias,
       asset_class, security_type,
       mv_reporting, reporting_ccy,
       'non-listed'::text AS book
FROM public.v_latest_alt_positions;
