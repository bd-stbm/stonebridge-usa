-- 037_entity_vehicle_dimension.sql
-- Vehicle (SPV) dimension on the LISTED side, mirroring alt_position_snapshot.
-- A client-demoted shared vehicle (keyed by group_node_id in
-- rebuild_attribution.VEHICLE_NOT_ENTITY_GROUPS) becomes a vehicle rather than
-- its own entity: its positions roll up to the trust above it (entity), and the
-- vehicle name is carried on vehicle_alias. First use: "Dendell LLC - Dell &
-- Broadcom" (group 532580). See docs/dendell_dellbroadcom_vehicle_demotion_spec.md.
--
-- Apply manually in the Supabase SQL Editor. Run BEFORE the new
-- rebuild_attribution (the INSERT writes the two new columns), then re-run
-- rebuild_attribution to repopulate.

ALTER TABLE public.entity_attribution
    ADD COLUMN IF NOT EXISTS vehicle_node_id TEXT,
    ADD COLUMN IF NOT EXISTS vehicle_alias   TEXT;

COMMENT ON COLUMN public.entity_attribution.vehicle_alias IS
    'Nearest client-demoted shared vehicle (SPV) between the node and its entity; NULL when none. trust_alias holds the entity (the trust above the vehicle).';

-- v_latest_positions: same body as migration 027, with vehicle_alias appended.
-- CREATE OR REPLACE can only append columns, so vehicle_alias goes last.
CREATE OR REPLACE VIEW public.v_latest_positions
WITH (security_invoker = true) AS
WITH latest_per_account AS MATERIALIZED (
    SELECT e.node_id AS account_node_id, m.snapshot_date
    FROM public.entity e
    CROSS JOIN LATERAL (
        SELECT p.snapshot_date
        FROM public.position_snapshot p
        WHERE p.account_node_id = e.node_id
        ORDER BY p.snapshot_date DESC
        LIMIT 1
    ) m
)
SELECT
    p.snapshot_date,
    p.account_node_id,
    e.alias AS account_alias,
    e.bank_broker AS custodian,
    e.account_number,
    ea.trust_node_id,
    ea.trust_alias,
    ea.sub_client_node_id,
    ea.sub_client_alias,
    p.security_id,
    s.asset_name,
    s.asset_class,
    s.security_type,
    s.sector,
    s.geographic_exposure,
    s.ticker_masttro,
    s.ticker_yf,
    s.isin,
    s.local_ccy,
    p.quantity,
    p.price_local,
    p.mv_local,
    p.mv_reporting,
    p.reporting_ccy,
    p.unit_cost_local,
    p.total_cost_local,
    (p.mv_local - p.total_cost_local) AS unrealized_gl_local,
    p.accrued_interest_reporting,
    ea.vehicle_alias                       -- appended for 037
FROM public.position_snapshot p
JOIN latest_per_account la
       ON la.account_node_id = p.account_node_id
      AND la.snapshot_date   = p.snapshot_date
JOIN      public.entity              e  ON p.account_node_id = e.node_id
LEFT JOIN public.entity_attribution  ea ON p.account_node_id = ea.node_id
LEFT JOIN public.security            s  ON p.security_id     = s.security_id;

-- v_net_worth_positions: expose the listed vehicle (was a hardcoded NULL).
CREATE OR REPLACE VIEW public.v_net_worth_positions
WITH (security_invoker = true) AS
SELECT sub_client_alias,
       trust_alias        AS entity_alias,
       vehicle_alias,
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
