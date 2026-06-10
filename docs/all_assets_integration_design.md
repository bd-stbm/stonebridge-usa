# All-Assets Integration — Storage & Attribution Design

Status: **draft for review.** Folds the non-listed book (alts, direct PE/RE,
business assets, loans, collections, non-canonical cash) into the dashboard
alongside the existing listed portfolio, with a **Security → Vehicle/SPV →
Entity** lookup hierarchy. Dyne US is the worked example; the engine is built
family-agnostic.

Prereqs proven in exploration (see memory `entity_rollup_existing_entities`,
`cef_alternatives_reflection_trap`):
- Non-listed value reconciles to the Masttro UI within drift.
- 97.7% rolls into the existing 17 entities along ownership lines; the rest
  falls back to branch.
- The SPV is already named by the holding leaf-node's `trust_alias`.

---

## 1. Principles (what must NOT break)

1. **Listed performance math stays untouched.** Alts/RE/loans have no daily
   yfinance price and no benchmark. They must never enter modified-Dietz,
   `reconstructed_nav_at`, or the returns tile. → Store them in a **separate
   table**, not `position_snapshot`, so they cannot leak into the returns
   pipeline by omission of a filter.
2. **Store the ownership-weighted slice, not the reflection.** The value written
   is `full_value × chain-product ownership`, never the 100%/full reflection the
   `/cef` and `/Holdings` feeds repeat on every node.
3. **Don't loosen the canonical filter.** It correctly dedups brokerage
   reflections for listed holdings. The non-listed path is parallel and additive.
4. **Family-scoped RLS + `security_invoker` views**, same as every existing
   table/view.

---

## 2. Storage — `alt_position_snapshot` (sibling table)

One row per **(asset × ownership path)**. A split-owned asset (e.g. Bestow,
50/50-ish across three trusts) produces one row per owning path, each with its
own entity, vehicle, ownership % and weighted value. Summing rows = the family's
value in that asset.

```sql
-- migration 036_alt_position_snapshot.sql
CREATE TABLE public.alt_position_snapshot (
    snapshot_date         DATE    NOT NULL,
    security_id           BIGINT  NOT NULL REFERENCES public.security(security_id),
    holding_node_id       TEXT    NOT NULL,   -- the leaf reflection node for this path

    -- three display dimensions
    sub_client_node_id    TEXT    NOT NULL,   -- family (RLS key)
    sub_client_alias      TEXT,
    entity_node_id        TEXT,               -- rolled-up existing entity (nearest-existing walk)
    entity_alias          TEXT,
    vehicle_node_id       TEXT,               -- SPV = leaf trust_alias; NULL when == entity (held direct)
    vehicle_alias         TEXT,

    -- ownership + value (reporting ccy only; no daily price for these)
    ownership_pct         NUMERIC(9,6),       -- chain-product fraction for THIS path, 0..1
    full_value_reporting  NUMERIC,            -- 100% NAV of the asset
    mv_reporting          NUMERIC,            -- = full_value_reporting × ownership_pct (signed; loans negative)
    reporting_ccy         CHAR(3),

    -- provenance / freshness
    value_source          TEXT,               -- 'cef' | 'gwm' | 'holdings'
    valuation_date        DATE,               -- lastValuationDate (point-in-time, often quarter-lagged)
    entity_rollup         TEXT,               -- 'existing' | 'branch-fallback'  (audit of the walk)

    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (snapshot_date, holding_node_id, security_id)
);

CREATE INDEX alt_pos_family_date_idx ON public.alt_position_snapshot (sub_client_node_id, snapshot_date);
CREATE INDEX alt_pos_entity_idx      ON public.alt_position_snapshot (entity_node_id);
CREATE INDEX alt_pos_vehicle_idx     ON public.alt_position_snapshot (vehicle_node_id);

ALTER TABLE public.alt_position_snapshot ENABLE ROW LEVEL SECURITY;
CREATE POLICY alt_pos_family ON public.alt_position_snapshot
    USING (is_admin() OR sub_client_node_id = ANY (current_user_sub_clients()));
```

Notes:
- **`quantity` / `price_local` are omitted on purpose.** For direct RE, business
  assets and loans there is no meaningful unit/price — only a NAV. Keep the table
  honest: it carries values, not prices.
- **Append-only**, same as `position_snapshot`. Re-running a day UPSERTs that
  day's rows (idempotent on the PK).
- `security` must already contain the alt securities. The listed ingest only
  writes canonical-account securities, so the alt ingest **must upsert
  securities first** (asset_class/security_type already come from `/Holdings`).

### Why sibling, not a flag on `position_snapshot`
The returns pipeline reads `position_snapshot` broadly (NAV grids, Dietz, carry-
forward). A flag means every one of those queries must remember to exclude
non-listed rows — one miss silently corrupts a return. A separate table is
fail-safe: performance code physically cannot see alts.

---

## 3. Attribution engine (port into `tracker/`)

New module `tracker/alt_attribution.py`. Family-agnostic; iterate `FAMILIES`.
Reuses the resolvers already validated in
`scripts/rollup_nonlisted_to_existing_entities.py` and
`scripts/build_dyne_us_entity_allassets.py`.

For each family, for each non-listed security (`assetClass ∉ {Equity, Fixed
Income, Cash and Equivalents, Commodities}`):

1. **Reflection group** → all nodes sharing the security's `group_node_id`.
2. **Full value** (100% NAV):
   - `cef` `marketValueRepCCY` when the vehicle is in the `/cef` feed (fund
     structures), else the **GWM node `valuation`** (direct holdings; loans carry
     a **negative** valuation). `value_source` records which.
   - **Liveness gate:** if not in `cef` and the current `/Holdings` mv `< $50k`,
     skip — kills dead legacy nodes (e.g. Cheniere shows $23M stale in GWM, ~$0
     live).
3. **Per reflection path** (each node that resolves to this family):
   - `ownership_pct` = product of GWM `ownershipPct` from node → family root.
   - `entity` = **nearest-existing-entity walk**: climb parents; stop at the
     first node whose `trust_alias` is in the family's *existing entity set*
     (= distinct `trust_alias` with listed value in `v_latest_positions`).
     - If none on the path → **fall back to the branch** (`family_path` level 3,
       e.g. "Wendi Dyne"); set `entity_rollup = 'branch-fallback'`. (Covers
       Chambers Road, California Forever — nothing is ever dropped.)
   - `vehicle` = the **leaf node's own `trust_alias`** (the immediate SPV). Set
     `vehicle_alias = NULL` when it equals `entity_alias` (held directly — 52 of
     92 Dyne cases).
   - `mv_reporting = full_value × ownership_pct`.
   - `valuation_date` = `cef.lastValuationDate` or GWM `snapshot_date`.

**Non-canonical cash** (family-specific bank/deposit accounts dropped by the
canonical filter): separate small pass — current `/Holdings` mv per
non-canonical Cash node, attributed to the nearest-existing entity, vehicle
NULL. (These are real balances, not reflections, so no ownership split.)

**Ordering:** runs **after** the listed sync each day, because the existing-
entity set is read from `v_latest_positions`. The trust/shared-vehicle entities
that catch the rollup (Mark trusts, Optsia, Modyl, Europlay…) always carry
listed value, so the set is stable run-to-run.

---

## 4. Views

```sql
-- security_invoker so family RLS flows through (mandatory)
CREATE VIEW public.v_latest_alt_positions
WITH (security_invoker = true) AS
SELECT a.*, s.asset_class, s.security_type, s.name AS asset_name
FROM (  -- latest snapshot per (holding_node_id, security_id)
  SELECT DISTINCT ON (holding_node_id, security_id) *
  FROM public.alt_position_snapshot
  ORDER BY holding_node_id, security_id, snapshot_date DESC
) a
JOIN public.security s ON s.security_id = a.security_id;
```

A combined net-worth view for the dashboard (listed + non-listed, common
columns only):

```sql
CREATE VIEW public.v_net_worth_positions
WITH (security_invoker = true) AS
SELECT sub_client_alias, trust_alias AS entity_alias, NULL::text AS vehicle_alias,
       account_alias, asset_class, security_type, mv_reporting, 'listed' AS book
FROM public.v_latest_positions
UNION ALL
SELECT sub_client_alias, entity_alias, vehicle_alias,
       NULL AS account_alias, asset_class, security_type, mv_reporting, 'non-listed' AS book
FROM public.v_latest_alt_positions;
```

---

## 5. Dashboard

Keep the existing **/holdings, /performance, /income** pages **listed-only and
unchanged** — they are the trading/performance tool and their math depends on it.

Add a new **/networth** ("All Assets") page:
- **Allocation by Masttro category** (asset_class), matching the Masttro UI
  screenshot: Equity / Alternatives / Real Estate / Fixed Income / Cash /
  Business / Loans / Commodities / Collections, with Loan Payable shown as a
  negative line below Total Assets.
- **Grouping toggle: Branch (8) ↔ Entity (17)** — the rollup tiers.
- **Third filter — Vehicle / SPV** (new). Lets you pull "everything in Goldenberry
  LLC" across entities. Reads `vehicle_alias`; listed rows show "—".
- **No returns/IRR computed here.** If a performance number is wanted, surface
  Masttro's own point-in-time `xirrCumulative` / `tvpi` from `/cef`, clearly
  labelled "as of `valuation_date`" — never run it through our Dietz/benchmark
  engine.
- **Staleness labelling** from `valuation_date` (alt NAVs are quarter-lagged).

Filter plumbing (per `web/CLAUDE.md` "Adding a new global filter"): new
`vehicle_filter` cookie + reader in `lib/trust-filter.ts`, `setVehicle` action,
`<VehicleFilter>` component, query param threaded into the /networth queries
only. `VISIBLE_ASSET_CLASSES` is extended **for the /networth view only** to
include Alternatives/RE/Business/Loans/Collections.

---

## 6. Open decisions

1. **Entity-set stability.** Defining "existing entity" as "has listed value"
   is empirically stable (the catching trusts/vehicles always hold listed
   securities) but value-derived. Alternative: freeze a curated entity allow-list
   per family. Recommend starting value-derived, add an override table only if a
   rollup target ever flickers.
2. **Branch as a first-class tier.** Branch (`family_path` L3) is currently
   derived on the fly. If /networth leans on it, consider persisting
   `branch_node_id`/`branch_alias` on `entity_attribution` during
   `rebuild_attribution`.
3. **Loans presentation.** Receivable as a positive asset class; Payable as a
   negative line below Total Assets (matches Masttro). Confirm we don't net them.
4. **Refresh cadence.** Ownership changes only on the weekly GWM sync; NAVs on
   `/cef` are point-in-time. Daily idempotent re-run is cheap — recommend daily
   so new commitments/calls appear promptly.
5. **Generalisation order.** Dyne US first (USD). Then Miller (largest alt book,
   AUD), Markiles, Bermeister, Dyne-AU — watch `reporting_ccy` and family-
   specific structures; reconcile each vs its Masttro UI screenshot before any
   client sees it.

---

## 7. Forward-compatibility: blended total return (phase 2)

v1 ships net worth + allocation, *not* a blended return. This section records
how that goal stays open so we don't design ourselves out of it. **Nothing here
needs building now** — it's a compatibility checklist.

**The design already enables it:**
- We store the **ownership-weighted slice** (the hard part) — exactly the basis a
  household return needs.
- `alt_position_snapshot` is **append-only, `snapshot_date`-keyed**, so it accrues
  a NAV history natively from first ingest.
- Sibling-table separation is storage only; a blended return is a **month-end
  compute layer** that reads both `v_nav_monthly_by_account` (listed) and the alt
  month-end NAVs and combines them. The split only keeps alts out of the *daily*
  TWR tile.

**Cheap insurance to take now (keeps the trailing window):**
1. **Start writing alt snapshots early**, before the blended-return UI exists, so
   history accrues natively rather than depending on Masttro's 12-month cap later.
2. **Make the alt ingest backfill-capable** (accept a `yearMonth` param, write
   month-end rows) — mirrors the listed backfill; recovers up to 12 months of
   Masttro month-end NAV so a trailing-1Y blend is possible from launch.
3. Both NAV (month-end, 12mo) and flows (dated `/Transactions`) are backfillable,
   so deferring the feature loses nothing permanently.

**Phase-2 build (when needed):**
- New **`alt_flow`** table: capital calls (in) / distributions (out) per
  (security, date), from `/cef` `capitalCalled`/`capitalDistributed` or
  `/Transactions`. No conflict with anything in §2–5.
- **Month-end blended measure only.** Compute modified-Dietz / money-weighted IRR
  at month-ends where both books have a real NAV. Surface it clearly labelled
  "money-weighted, as of month-end" — **never** in the daily returns tile.

**The genuine difficulty is methodology, not storage:**
- **Internal-transfer netting.** A capital call funded from a household bank
  account into a household fund is internal (cash ↓, fund ↑ — net zero) and must
  NOT count as an external flow. External flows must be defined at the
  **sub-client (household)** level. This is a compute concern; storage is unaffected.
- **Stale-NAV inheritance.** A blended month-end return is only as fresh as the
  latest alt valuations (quarter-lagged). Label accordingly — Masttro's own total
  IRR carries the same caveat.
```
