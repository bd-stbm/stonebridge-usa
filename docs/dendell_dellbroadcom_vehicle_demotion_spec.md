# Spec: demote "Dendell LLC - Dell & Broadcom" from entity → vehicle

Status: **spec for review — nothing implemented yet.**

## Goal

`Dendell LLC - Dell & Broadcom` (group_node_id **532580**, ~$20.5M of listed Dell
+ Broadcom across Dyne US + Markiles) currently shows as its own **entity**. Make
it a **vehicle** instead: its value rolls up to the underlying trusts by
ownership, and it's still tagged as the "Dendell LLC - Dell & Broadcom" vehicle
(kept distinct from the separate `Dendell LLC` vehicle on the alt side).

This is the surgical scope: **only** group 532580. Other shared vehicles (Modyl,
Optsia, Contango, Fairways, Deltrust…) stay entities — unchanged.

## Why it's safe / what moves

Each of the 5 Dendell Dell&Broadcom account nodes sits under exactly one trust;
the per-trust slices are already separate canonical rows in `position_snapshot`.
Demoting only changes the **label**, never the values — no double-count.

| Trust (entity it moves to) | Listed value moved |
|---|---|
| Morgan Dyne Trust | +$4.28M |
| Dylan Dyne Irrevocable Trust | +$4.28M |
| Mark I Dyne 2010 Irrevocable T | +$4.28M |
| Murray Markiles 2015 Investment T | +$3.85M |
| Lia Markiles 2015 Investment T | +$3.85M |

Family-level NAV is unchanged (pure regrouping). Applies retroactively to all
history (attribution is recomputed from the tree; `position_snapshot` is keyed by
account node) — no backfill.

---

## Change set

### 1. Migration `037_entity_vehicle_dimension.sql`

```sql
-- Vehicle (SPV) dimension on the listed side, mirroring alt_position_snapshot.
-- vehicle = nearest client-demoted shared vehicle; entity = the trust above it.
ALTER TABLE public.entity_attribution
    ADD COLUMN IF NOT EXISTS vehicle_node_id TEXT,
    ADD COLUMN IF NOT EXISTS vehicle_alias   TEXT;

-- v_latest_positions: append vehicle_alias (CREATE OR REPLACE can only append).
-- Re-run the full 027 definition with ", ea.vehicle_alias" added at the end of
-- the SELECT (column shape preserved otherwise). [full body in the migration]

-- v_net_worth_positions: expose the listed vehicle instead of a hardcoded NULL.
CREATE OR REPLACE VIEW public.v_net_worth_positions
WITH (security_invoker = true) AS
SELECT sub_client_alias, trust_alias AS entity_alias,
       vehicle_alias,                              -- was NULL::text
       account_alias, asset_class, security_type, mv_reporting, reporting_ccy,
       'listed'::text AS book
FROM public.v_latest_positions
UNION ALL
SELECT sub_client_alias, entity_alias, vehicle_alias, NULL::text AS account_alias,
       asset_class, security_type, mv_reporting, reporting_ccy,
       'non-listed'::text AS book
FROM public.v_latest_alt_positions;
```

Order matters: `ALTER TABLE` (add columns) before the `CREATE OR REPLACE VIEW`s,
and before the new `rebuild_attribution` runs.

### 2. `tracker/sync_supabase.py` — `rebuild_attribution`

Add the demote constant near `FORCE_OWN_ENTITY_NODES`:

```python
# Shared vehicles the client wants shown as a VEHICLE (SPV) rather than an
# entity — keyed by group_node_id. The leaf-to-root walk records them as the
# vehicle and continues up to the real owning entity (the trust above).
VEHICLE_NOT_ENTITY_GROUPS = {"532580"}  # Dendell LLC - Dell & Broadcom
```

In the per-node walk: capture `gnid`, init `vehicle_nid/alias`, add the demote
branch, and exclude demoted vehicles from the entity match:

```python
        vehicle_nid = vehicle_alias = None          # init with the other vars
        ...
        for _ in range(50):
            ...
            pid, alias, name, gnid = nodes.get(cur_id, (None, None, None, None))  # was _
            path.append(alias or name or cur_id)
            if cur_id in sub_clients:
                ...
            # Vehicle demotion (independent of entity): record the SPV, keep walking.
            is_demoted_vehicle = cur_id != nid and gnid in VEHICLE_NOT_ENTITY_GROUPS
            if vehicle_nid is None and is_demoted_vehicle:
                vehicle_nid, vehicle_alias = cur_id, alias or name
            # Strong entity match — a demoted vehicle never counts as the entity.
            if entity_nid is None and cur_id != nid and not is_demoted_vehicle and (
                cur_id in FORCE_OWN_ENTITY_NODES
                or cur_id in shared_vehicle_nodes
                or _is_trust(alias, name)
                or is_retirement_wrapper(alias, name)
            ):
                entity_nid = cur_id; entity_alias = alias or name
            elif (entity_nid is None and weak_nid is None and cur_id != nid
                  and is_super_or_pension(alias, name)):
                weak_nid, weak_alias = cur_id, alias or name
            ...
        rows.append((nid, sub_client_nid, sub_client_alias,
                     entity_nid, entity_alias, " > ".join(reversed(path)),
                     vehicle_nid, vehicle_alias))          # +2 fields
```

Extend the INSERT to write the two columns:

```sql
INSERT INTO entity_attribution
  (node_id, sub_client_node_id, sub_client_alias,
   trust_node_id, trust_alias, family_path, vehicle_node_id, vehicle_alias)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (node_id) DO UPDATE SET
  sub_client_node_id = EXCLUDED.sub_client_node_id,
  sub_client_alias   = EXCLUDED.sub_client_alias,
  trust_node_id      = EXCLUDED.trust_node_id,
  trust_alias        = EXCLUDED.trust_alias,
  family_path        = EXCLUDED.family_path,
  vehicle_node_id    = EXCLUDED.vehicle_node_id,
  vehicle_alias      = EXCLUDED.vehicle_alias
```

### 3. `web/lib/queries.ts` — `listVehicles`

Include listed vehicles, not just alt vehicles, so the Net Worth filter lists
"Dendell LLC - Dell & Broadcom". Read distinct `vehicle_alias` from
`v_net_worth_positions` (covers both books) instead of `v_latest_alt_positions`.

### 4. Net Worth page — no code change needed

`getNetWorthRows` already selects `vehicle_alias`; once the listed side of
`v_net_worth_positions` exposes it, the Vehicle/SPV filter and breakdown pick it
up automatically. Dell&Broadcom now appears under the trusts (entity) with the
Dendell Dell&Broadcom vehicle tag. (Optional follow-up: surface a Vehicle column
on the listed Holdings page — not required for this change.)

### 5. schema.sql mirror

Update the `entity_attribution` DDL (+2 columns) and the two view bodies in
`supabase/schema.sql` per the repo convention.

---

## Apply order

1. Run migration 037 in Supabase SQL Editor (ALTER + both view replaces).
2. Deploy the `rebuild_attribution` change; run it once now
   (`python scripts/sync_masttro_weekly.py`, or a one-off calling
   `rebuild_attribution(conn)`) to repopulate attribution + vehicle columns.
3. No position/alt re-ingest needed. Deploy the web change.

## Verification

- `v_latest_positions`: `trust_alias = 'Dendell LLC - Dell & Broadcom'` returns 0
  rows; Morgan/Dylan/Mark2010 +4.28M each, Murray/Lia +3.85M each.
- Per-family total NAV unchanged (Dyne US, Markiles).
- `v_net_worth_positions`: Dell&Broadcom listed rows carry
  `vehicle_alias = 'Dendell LLC - Dell & Broadcom'`, `entity_alias = <trust>`.
- Net Worth total + allocation unchanged; Entity filter no longer lists the
  Dendell entity; Vehicle filter now lists it.

## Risks / notes

- **Performance by-trust matrix shifts**: those 5 trusts now include their
  Dell&Broadcom slice (correct, but historical per-trust returns change). Family
  totals unchanged.
- **Miller asymmetry**: Miller's Dell/Broadcom already rolls to "Contango
  Investments LLC" (no Dendell entity), so it's untouched — expected.
- Demote is keyed on group_node_id 532580 (stable). Reversible: revert the code,
  re-run rebuild (vehicle columns go null, entity reverts).
- The general version (demote ALL shared vehicles to vehicles) is explicitly NOT
  in scope — it would move Modyl/Optsia/etc. and needs its own review.
