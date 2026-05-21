# Stonebridge Dashboard — App Guide

This file is the operating manual for the **Next.js dashboard under `/web`**.
The repo-root `CLAUDE.md` is the Masttro API reference and applies to
exploration / ingest work under `/scripts` and `/tracker`.

> **Status:** production-deployed on Vercel. Single-tenant (one Supabase
> project, one sub-client — "Dyne Family (US)"). Locked behind Supabase
> Auth (invite-only). Daily syncs from Masttro + yfinance via GitHub
> Actions.

---

## Stack

- **Next.js 14** (App Router, server components by default)
- **Tailwind CSS** — brand palette sampled from `/public/stonebridge-logo.png`
  is registered in `tailwind.config.ts` as `brand.{DEFAULT,dark,light,tint,navy}`.
  Use `bg-brand`, `text-brand`, etc. instead of hard-coded purple hex.
- **Recharts** — all charts (NAV area, rebased line, monthly bar)
- **@supabase/ssr** — cookie-based auth. Sessions persist across page loads.
- **TypeScript strict mode** — coerce values from supabase-js with `Number()`
  before arithmetic (PostgreSQL `NUMERIC` arrives as strings).

---

## Data flow

```
Masttro API
   │ (scripts/sync_masttro_{daily,weekly}.py, scheduled in GitHub Actions)
   ▼
tracker/sync_supabase.py  ──UPSERT──▶  Supabase Postgres
                                          │
              yfinance daily + 5y backfill ─┤
                                          ▼
                          views + RPCs (security_invoker)
                                          │
                                          ▼
                  Next.js server components via @supabase/ssr
```

- Historical Masttro data is **month-end only** (the API's hard limit).
  Daily snapshots exist only for the current month.
- yfinance fills in the daily granularity for the public-priced portion of
  the portfolio — see `reconstructed_nav_at` below.
- `scripts/sync_yfinance.py` runs daily and calls three things: pricing
  refresh (today's + previous close per held security), index sync, and
  security price history append.

---

## Pages

| Route | Purpose | Key view / RPC |
|---|---|---|
| `/` | NAV + 1D/MTD/YTD/6M/1Y returns with benchmark + asset-class split, NAV-over-time chart, top holdings. | `v_positions_refreshed`, `v_nav_monthly_by_account`, `v_nav_monthly_by_asset_class`, `index_price_history`, `reconstructed_nav_at` (RPC) |
| `/holdings` | Sortable/filterable table of every current position. Search + asset-class + custodian filters in-page (Trust + Account come from header). | `v_positions_refreshed` |
| `/performance` | Rebased portfolio-vs-benchmark line chart, returns-by-trust matrix, returns-by-asset-class matrix. | `v_nav_monthly_by_account`, `v_nav_monthly_by_asset_class`, `v_external_flows`, `index_price_history` |
| `/income` | TTM/YTD income KPIs, monthly bar (Dividends/Interest/Other), top payers, by-trust. | `v_income_monthly` |
| `/transactions` | Every transaction with category chip, date range, search, type filter. | `v_transactions` |
| `/login` | Email/password form. Middleware redirects unauthenticated users here. | (Supabase Auth) |

The header sits above every authenticated page: logo top-left, nav tabs,
person-icon user menu, and a sub-bar with the global Trust + Account
multi-select filters.

---

## Key files

```
lib/queries.ts          — single source of truth for data fetching
                          (Position, NavPoint, Transaction, IncomeRow types).
                          Every page imports from here. Adds .limit(LIMIT_LARGE)
                          to every PostgREST call.
lib/returns.ts          — modified Dietz, index returns, period-key plumbing.
                          Pure compute, no SQL.
lib/trust-filter.ts     — cookie readers (server-side). Returns string[].
lib/actions.ts          — "use server" — cookie writers + benchmark setter.
lib/supabase-server.ts  — per-request Supabase client (cookies-aware).

middleware.ts           — gates routes. Unauthenticated -> /login.

components/Header.tsx           — logo + nav + filters bar + UserMenu.
components/TrustFilter.tsx      — multi-select popover.
components/AccountFilter.tsx    — multi-select popover, cascades on trust.
components/ReturnsTile.tsx      — period buttons + benchmark + asset-class.
components/PerformanceMatrix.tsx — trust / asset-class matrix table.
components/RebasedChart.tsx     — portfolio + benchmark, both rebased to 100.
components/HoldingsFullTable.tsx — Holdings page table.
components/TransactionsTable.tsx — Transactions page table.
components/MonthlyIncomeChart.tsx — stacked bar.
components/TopPayersTable.tsx + IncomeByTrustTable.tsx — Income tables.
```

---

## Filter system

Three cookies, all JSON-encoded so they hold arrays:

- `trust_filter` — `string[]` of trust aliases. Empty = all trusts under
  the sub-client.
- `account_filter` — `string[]` of account node IDs. Cascading: when a
  trust is picked, only that trust's accounts appear in the dropdown.
- `benchmark` — single ticker (`^SP500TR` default).

Readers in `lib/trust-filter.ts`:

```ts
const trusts   = getSelectedTrusts();   // string[]
const accounts = getSelectedAccounts(); // string[]
const benchmark = getSelectedBenchmark(); // string, defaults to ^SP500TR
```

Writers in `lib/actions.ts` (server actions). Setting a new trust auto-
clears the account cookie (the previously-selected account may no longer
be in scope).

**Query convention**: every scoped query takes `trusts: string[] = []` and
`accounts: string[] = []`. Empty arrays mean "no filter". Use PostgREST
`.in("trust_alias", trusts)` when non-empty — single-element arrays work
identically to the old `.eq()` calls.

**Adding a new global filter** (e.g. asset class):
1. New cookie constant + reader in `lib/trust-filter.ts`.
2. New `setX` server action in `lib/actions.ts`.
3. New `<XFilter>` client component (copy `TrustFilter.tsx`).
4. Add to `Header.tsx`'s sub-bar.
5. Add the parameter to every scoped query in `lib/queries.ts` plus
   `.in()` clause when non-empty.
6. Thread it through every page's `await getX(SUB, trusts, accounts, …)`.

---

## DB conventions

- **Every view is `security_invoker = true`** — RLS on the underlying
  tables gates row visibility through the view. Without this, family-
  scoped RLS would silently leak through views. Linter flag
  `0010_security_definer_view` is the canary.
- **Every table has RLS enabled**. Phase 1 policy is `phase1
  authenticated read` = `FOR SELECT TO authenticated USING (true)` — every
  signed-in user sees everything. Phase 2 (per-family scoping) is planned
  but not built.
- **service_role bypasses RLS**; used by the Python sync, not by the web
  app. The web app uses `@supabase/ssr` with the anon key + user session.
- **Migrations append-only** under `supabase/migrations/NNN_*.sql`.
  Apply via Supabase SQL Editor. `supabase/schema.sql` is the canonical
  fresh-deploy schema and gets mirrored manually when a migration changes
  table or view shapes.
- **`LIMIT_LARGE = 100000`** in `lib/queries.ts` — explicit per-query
  limit. Must be paired with Supabase **Settings → API → Max rows = 100000**
  or higher. The default of 1000 silently truncates anything over and
  causes subtly-wrong aggregations.
- **`CREATE OR REPLACE VIEW` can only append columns**, never reorder or
  rename existing ones. Always add new columns at the end of the SELECT.
- **Postgres `NUMERIC` arrives from supabase-js as strings**. Always
  `Number(value ?? 0)` before arithmetic. `0 + "1" = "01"`, not `1`.

---

## Returns math

Modified Dietz formula matches `tracker/compute.py::period_performance`:

```
gain    = end_nav − start_nav − flows
denom   = start_nav + 0.5 × flows
return  = gain / denom
```

For each period:

| Period | End NAV | Start NAV | Flows |
|---|---|---|---|
| 1D | Sum of `mv_reporting_refreshed` | Sum of `mv_reporting_yesterday` (today's positions × yfinance previous close) | None — pure price move |
| MTD | Refreshed sum | Last day of previous month from `v_nav_monthly_by_account` | External flows in window |
| YTD | Refreshed sum | Dec 31 prev year from `v_nav_monthly_by_account` | External flows in window |
| 6M | Refreshed sum | **`reconstructed_nav_at(today − 6m)` RPC** — falls back to snapshot grid if RPC returns NULL | External flows in window |
| 1Y | Refreshed sum | **`reconstructed_nav_at(today − 1y)` RPC** — currently clamps to Jun 2025 anchor (earliest Masttro snapshot) | External flows in window |

When an **asset class** is selected in the Returns tile, flows are zeroed
(trust-level deposits aren't asset-class-typed) and the result is labelled
"price-only" in the subline.

Benchmarks (`^SP500TR`, `ACWI`) come from `index_price_history`. The TR
comparison is apples-to-apples since portfolio mv_reporting includes
received dividends.

---

## Common gotchas

- **Masttro Holdings `quantity` arrives as a string with commas** like
  `"23,677"`. `tracker/ingest.py::_to_float` strips commas; without that
  it returned None for any holding above 999 shares.
- **Masttro JSON field is `assetName` not `name`**, `transactionType` not
  `type`. Don't assume nice camelCase.
- **`sub_client_alias` in the DB is `"Dyne Family (US)"` with parens** —
  comes straight from the Masttro GWM payload. The dashboard fallback
  `DEFAULT_SUB_CLIENT` in `lib/trust-filter.ts` (re-exported from
  `lib/queries.ts`) must match exactly. Pages now read the active
  scope via `getSelectedSubClient()` (cookie-backed, admin-set);
  the default is used only when no `sub_client` cookie is present.
- **Substring "trust" matches "Deltrust LLC"** — `listTrusts()` queries
  `v_latest_positions` rather than `entity_attribution` so empty LLC
  shells with "trust" in their name don't appear in the dropdown.
- **`trust_alias` / `trust_node_id` now store the "entity"** — defined
  as the nearest shared-within-family vehicle ancestor (e.g. "Modyl
  LP") OR, if none, the nearest trust ancestor. Computed in
  `tracker/sync_supabase.py::rebuild_attribution`. The UI labels this
  filter "Entity" / "Entities"; the DB column names are unchanged for
  brevity. A vehicle is shared-within-family when its `group_node_id`
  has 2+ distinct trust ancestors (cross-family shared vehicles are
  filtered out earlier in `canonical_accounts_under`, so any
  group_node_id with 2+ trust ancestors is by definition in-family).
- **A new view must be created with `security_invoker = true`** or the
  linter complains and family-scoped RLS will silently leak through it.
- **A new migration won't auto-apply on Vercel deploy** — push the code,
  then apply the SQL manually in Supabase. Comment on the PR / commit
  reminds future-you.

---

## Auth model

- Supabase Auth, email + password. Sign-ups disabled at the dashboard
  level — invite from Supabase → Authentication → Users → Invite.
- `middleware.ts` runs on every route except `/login`, `/auth/*`, and
  Next internals. Refreshes the session cookie via `@supabase/ssr` and
  redirects unauthenticated requests to `/login`.
- Web app uses the **anon** key + per-request user JWT (not service_role).
  RLS applies — currently every signed-in user sees everything (Phase 1).
- Vercel env vars required:
  - `NEXT_PUBLIC_SUPABASE_URL`
  - `NEXT_PUBLIC_SUPABASE_ANON_KEY`
  - `SUPABASE_SERVICE_ROLE_KEY` (kept for future admin server actions; not
    actually used by the dashboard today)

---

## Operational

- **Apply a migration**: Supabase Dashboard → SQL Editor → paste the new
  `supabase/migrations/NNN_*.sql` file → Run.
- **Backfill yfinance histories**:
  - `python scripts/backfill_indices.py` (one-off, ~2 min) — 5y for
    `^SP500TR` and `ACWI`.
  - `python scripts/backfill_security_prices.py` (one-off, ~3–4 min) —
    5y for every held public security.
  - Both are idempotent on `(ticker, date)`.
- **Daily/weekly syncs**: GitHub Actions `daily-sync.yml` runs Mon–Fri
  22:00 UTC. `weekly-sync.yml` runs Sunday 22:00 UTC. Both need the
  repo secrets `MASTTRO_API_KEY`, `MASTTRO_API_SECRET`, `SUPABASE_DB_URL`
  (pooler URL, port 6543, password URL-encoded). Direct DB URLs are
  IPv6-only and fail from GitHub runners.
- **Local dev**: `cd web && npm install && npm run dev` — `.env.local`
  needs `NEXT_PUBLIC_SUPABASE_URL`, `NEXT_PUBLIC_SUPABASE_ANON_KEY`,
  `SUPABASE_SERVICE_ROLE_KEY`. Python scripts auto-load the repo-root
  `.env.local` via `tracker/db.py`.

---

## Outstanding work

- **Phase 2 RLS** — family-scoped policies + `user_profile` mapping table.
  Planned in the original auth conversation; not built. Currently every
  authenticated user sees the entire dataset.
- **Migration 012 must be applied manually** (the `reconstructed_nav_at`
  RPC). Until applied, 6M / 1Y use the snapshot-grid fallback.
- **Masttro backfill is 12 months** — extends to Jun 2025. To get true 1Y
  precision (today minus 1Y has a real anchor), run
  `scripts/11_us_families_backfill.py` with a longer `historicalMonths`.
- **Global asset class / custodian filters** are deferred. Today they
  live in-page on `/holdings` only. A previous design conversation laid
  out the work needed if you want them global.
- **The reconstruction RPC is wired into the Overview Returns tile only.**
  The Performance page matrices and the Returns tile's by-asset-class
  split still use the snapshot-grid path. Easy to extend if needed.
