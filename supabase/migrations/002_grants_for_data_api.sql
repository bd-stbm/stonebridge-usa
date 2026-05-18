-- 002 — Explicit GRANTs for Supabase Data API roles.
--
-- Supabase historically auto-granted SELECT to anon/authenticated/service_role
-- on new objects in `public`. That behaviour is being removed (May 30 for new
-- projects, Oct 30 for existing). Without these grants PostgREST returns
-- 42501 "permission denied for ..." and the dashboard 500s — which is exactly
-- what we hit.
--
-- RLS is still enabled on every table with no policies, so anon/authenticated
-- get zero rows back even with SELECT granted. service_role bypasses RLS,
-- so the dashboard (which uses service_role) starts working again.

GRANT USAGE ON SCHEMA public TO anon, authenticated, service_role;

GRANT SELECT ON ALL TABLES IN SCHEMA public
    TO anon, authenticated, service_role;

GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO service_role;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO service_role;

-- Future-proof: any new table/view/sequence created in public inherits the
-- same grants automatically, so we don't have to re-grant on every migration.
-- Scoped to objects created by the role running this migration (postgres).
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON TABLES TO anon, authenticated, service_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT INSERT, UPDATE, DELETE ON TABLES TO service_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT USAGE, SELECT ON SEQUENCES TO service_role;
