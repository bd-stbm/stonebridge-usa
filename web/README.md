# Stonebridge Dashboard (USA)

Next.js 14 (App Router) + Tailwind + Recharts + Supabase Auth, reading from
the Supabase tracker DB. Inspired by the Sharesight-fed dashboard, adapted
to the Masttro/yfinance data model.

## Local development

    cd web
    npm install
    cp .env.local.example .env.local
    # Fill in the three SUPABASE_* env vars
    npm run dev
    # http://localhost:3000

Env vars come from Supabase **Project Settings -> API**:
- `NEXT_PUBLIC_SUPABASE_URL` — project URL
- `NEXT_PUBLIC_SUPABASE_ANON_KEY` — anon (publishable) key
- `SUPABASE_SERVICE_ROLE_KEY` — service role secret (currently unused by the
  dashboard, kept for future admin server actions)

## Auth

Email + password via Supabase Auth, invite-only (signups disabled in the
Supabase dashboard). Every route except `/login` and `/auth/*` is gated by
`middleware.ts` — unauthenticated users are redirected to `/login`.

Server components query Supabase through `lib/supabase-server.ts`, which
returns a per-request client carrying the user's JWT. RLS policies on every
table decide what they can read.

**Phase 1 (current):** every authenticated user sees all rows.
**Phase 2 (planned):** family-scoped — a `user_profile` table maps each user
to a `sub_client_node_id`; staff see all, family members see only their own.

To invite a user: Supabase dashboard -> Authentication -> Users -> Invite.
They get an email with a link to set their password.

## Deploy to Vercel

1. Import the `stonebridge-usa` repo on Vercel.
2. **Root Directory** = `web`. Vercel auto-detects Next.js.
3. Project Settings -> Environment Variables (Production env):
   - `NEXT_PUBLIC_SUPABASE_URL`
   - `NEXT_PUBLIC_SUPABASE_ANON_KEY`
   - `SUPABASE_SERVICE_ROLE_KEY`
4. Deploy.

## Pages

- `/` — Overview: NAV tile, period summary, NAV-over-time chart, top holdings
- `/holdings` — placeholder
- `/performance` — placeholder
- `/income` — placeholder
- `/login` — sign-in form

The Overview page queries two Supabase views shipped in
`supabase/schema.sql` at the repo root:
- `v_latest_positions` — latest snapshot per account-security
- `v_nav_monthly_by_account` — monthly NAV trajectory per account

Both are scoped to `sub_client_alias = 'Dyne Family (US)'` for v1. Wire a
scope selector when adding more families.
