# Option C — global Vehicle/SPV filter

Status: **in progress.** Make the Vehicle/SPV filter a first-class global filter
(header sub-bar) that scopes every tab, including the returns / NAV-over-time
surfaces — not just the in-page /networth filter.

## Key decision — flows
**Vehicle-scoped returns are flow-aware, using per-vehicle transaction flows**
(the same mechanism asset-class uses — NOT price-only). `getPeriodReturns`
already switches the flow source when the user picks a class subset: from
trust-level external Deposit/Withdrawal (`v_external_flows`) to per-class
Buy/Sell/dividend/interest flows (`getFlowsByAssetClass` → `v_transactions`
bucketed by `asset_class`). External cash flows drop out (no asset_class — they
sit in cash before deployment); the in-class trading/income flows are fully
counted.

The vehicle analog: when a vehicle is selected, source flows from
`v_transactions` filtered by `vehicle_alias` (a `getFlowsByVehicle`, mirroring
`getFlowsByAssetClass`). Migration 038 adds `vehicle_alias` to `v_transactions`,
so this is enabled by the same foundation — no change to `v_external_flows`.

## Phasing

### Phase 1 — position & aggregation layer (no returns-engine risk)
- **Migration 038** (`038_vehicle_on_position_views.sql`):
  - `v_positions_refreshed` DROP+CREATE → picks up `vehicle_alias` via `lp.*`
    (its column list was frozen before 037; CREATE OR REPLACE can't insert
    mid-list). No view depends on it (verified).
  - Append `vehicle_alias` to `v_nav_monthly_by_account`,
    `v_nav_monthly_by_asset_class`, `v_income_monthly`, `v_transactions`
    (account → vehicle is 1:1, so GROUP-BY grain is unchanged).
- **Queries** (`lib/queries.ts`): add `vehicles: string[] = []` +
  `.in("vehicle_alias", vehicles)` to `getLatestPositions` (and add
  `vehicle_alias` to its select), `getNavSeries`, `getNavSeriesByTrust`,
  `getNavSeriesByAssetClass`, `getFlowsByAssetClass`, `getIncomeRows`,
  `getTransactions`.
- **UI**: `VehicleFilter` into `Header.tsx`'s sub-bar (data from
  `listVehicles(scope)`); `getSelectedVehicles()` threaded into every page;
  drop the in-page filter on `/networth`.
- **Covers**: Holdings, Overview (NAV tile, top holdings, asset-class
  allocation + chart), Income, Transactions, Net Worth.

### Phase 2 — returns / NAV-history layer (the delicate part)
- **Migration 039**: add `vehicle_alias` to `nav_monthly_carryforward_grid`
  + update `refresh_nav_monthly_carryforward_grid()` and re-materialise; add a
  `p_vehicles text[] DEFAULT NULL` param to `nav_carryforward`,
  `nav_carryforward_by_trust`, `holdings_period_attribution`,
  `monthly_security_attribution`, and `reconstructed_nav_at` (mirror the existing
  `p_asset_classes` plumbing).
- **Queries**: pass `vehicles` to those RPC calls; source per-vehicle flows from
  `v_transactions` (a `getFlowsByVehicle`, mirroring `getFlowsByAssetClass`) in
  `getPeriodReturns` when a vehicle is active.
- **Covers**: returns numbers, NAV-over-time chart, Performance matrix.

## Notes
- Only the demoted-vehicle set currently carries `vehicle_alias` on the listed
  side (Dendell Dell&Broadcom, Modyl, Optsia) — see
  [[entity_rollup_existing_entities]]. Alt vehicles span the full SPV set on the
  non-listed book. Adding more listed vehicles is a one-line
  `VEHICLE_NOT_ENTITY_GROUPS` addition.
- The empty-state matters: selecting an alt-only vehicle (e.g. Goldenberry) on a
  listed tab legitimately yields nothing — that holding isn't listed. Make the
  empty state read clearly ("no listed positions in this vehicle").
