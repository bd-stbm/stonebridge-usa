# Supabase migration plan

## Files

- `schema.sql` — full Postgres-flavoured DDL plus a small set of read-model views and RLS bootstrapping. Translates `tracker/schema.py` (SQLite) to native Postgres types and idioms.

## Key differences vs. the local SQLite schema

| Concern | SQLite (`tracker/schema.py`) | Postgres (`supabase/schema.sql`) |
|---|---|---|
| Booleans | `INTEGER 0/1` | `BOOLEAN` |
| Dates | `TEXT` (ISO `YYYY-MM-DD`) | `DATE` (native — supports range / interval queries) |
| Timestamps | n/a | `TIMESTAMPTZ NOT NULL DEFAULT now()` for `created_at` / `updated_at` |
| Numeric | implicit | `NUMERIC(p, s)` — explicit precision per field. Money 20,4; price 20,8; pct 9,4 |
| Auto IDs | `INTEGER PRIMARY KEY AUTOINCREMENT` | `BIGSERIAL PRIMARY KEY` |
| Currency codes | `TEXT` | `CHAR(3)` |
| Triggers | n/a | `set_updated_at()` trigger on every table |
| Foreign keys | none (SQLite tolerates) | declared with `ON DELETE` semantics |
| Indexes | regular | partial indexes where useful (`WHERE is_account = TRUE` etc.) |

The shape of every table is otherwise identical — same columns, same primary keys, same data semantics. Python ingest/api code will need a connection swap (`psycopg2` / `supabase-py` instead of `sqlite3`) but the SQL bodies are largely portable.

## Read-model views

Five views are created out of the box, exposed via PostgREST:

- `v_latest_positions` — latest snapshot per account-position, joined to entity + attribution + security
- `v_nav_monthly_by_account` — date × account NAV
- `v_nav_monthly_by_trust` — date × trust NAV
- `v_positions_refreshed` — latest positions with yfinance refresh prices and computed `mv_reporting_refreshed` / `mv_reporting_yesterday`
- `v_income_monthly_by_account` — monthly dividends / interest / income breakdown
- `v_external_flows` — deposits + withdrawals only, joined for display

These cover the bulk of the dashboard reads. Heavier compute (TWR / IRR / per-bucket allocation) stays in Python for now — can be materialised later if the frontend needs them as SQL views.

## RLS (Row Level Security)

Every table has RLS **enabled** but **no policies**. That means *no rows are visible* until you add policies — safe default for Supabase.

Two policy templates are commented in `schema.sql`:

1. **"allow read for authenticated"** — open read access to any logged-in user. Use during early dev.
2. **"user sees own family"** — restrict each user to their own sub-client subtree via a `user_profile` table. Use when you have multiple families on one DB.

Discuss the auth model before going live.

## Migration steps (when ready)

1. **Create the Supabase project** (or use an existing one).
2. **Apply `schema.sql`** via the SQL Editor or `supabase db push` if using the CLI.
3. **Load the data** — easiest path:
   - `pip install psycopg2-binary pandas`
   - Read each SQLite table into a pandas DataFrame.
   - `df.to_sql('entity', engine, if_exists='append', index=False)` — repeat per table.
   - Use `COPY FROM STDIN` (via `psycopg2.copy_expert`) for `position_snapshot` and `transaction_log` since they're the big ones (~50k rows for Dyne US public-only; bulk-copy ~10x faster than row-by-row INSERT).
4. **Update Python connection** — swap `sqlite3.connect(DEFAULT_DB_PATH)` for psycopg2 or supabase-py. Most of the SQL in `tracker/api.py` and `tracker/compute.py` runs unchanged.
5. **Add RLS policies** once auth model is decided.
6. **Schedule the daily refresh** — Supabase Edge Functions or a GitHub Action running `tracker.enrich.refresh_pricing()` against the Postgres endpoint.

## What to defer

- **Multi-tenant support** — if other families (Stonebridge AU clients, e.g.) are loaded into the same DB later, add a `tenant_id` column on each table and key RLS off it. Cheap to add via migration when needed.
- **Materialised views for performance** — `mv_performance_summary` etc. with `REFRESH MATERIALIZED VIEW CONCURRENTLY` on a schedule. Only worth it if the SELECT queries get slow (currently <100ms locally).
- **Realtime subscriptions** — Supabase realtime works out of the box on any table; enable when the frontend wants live updates on `pricing_refresh` writes.
