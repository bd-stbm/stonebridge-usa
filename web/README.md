# Stonebridge Dashboard (USA)

Next.js 14 (App Router) + Tailwind + Recharts, reading from the Supabase
tracker DB. Inspired by the Sharesight-fed dashboard, adapted to the
Masttro/yfinance data model.

## Local development

    cd web
    npm install
    cp .env.local.example .env.local
    # Fill in SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY
    npm run dev
    # http://localhost:3000

The two env vars come from Supabase **Project Settings -> API**:
- `SUPABASE_URL` is the project URL
- `SUPABASE_SERVICE_ROLE_KEY` is the `service_role` secret (server-only)

## Deploy to Vercel

1. Import the `stonebridge-usa` repo on Vercel.
2. **Root Directory** = `web`. Vercel auto-detects Next.js.
3. Project Settings -> Environment Variables: add `SUPABASE_URL` and
   `SUPABASE_SERVICE_ROLE_KEY` for the Production environment.
4. Deploy.

## Pages

- `/` - Overview: NAV tile, period summary, NAV-over-time chart, top holdings
- `/holdings` - placeholder
- `/performance` - placeholder
- `/income` - placeholder

The Overview page queries two Supabase views shipped in
`supabase/schema.sql` at the repo root:
- `v_latest_positions` - latest snapshot per account-security
- `v_nav_monthly_by_account` - monthly NAV trajectory per account

Both are scoped to `sub_client_alias = 'Dyne Family (US)'` for v1. Wire a
scope selector when adding more families.
