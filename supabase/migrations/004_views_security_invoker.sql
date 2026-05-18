-- 004 — Switch all read-model views to SECURITY INVOKER.
--
-- Postgres views default to SECURITY DEFINER: they execute with the owner's
-- (postgres superuser's) privileges and bypass RLS on the underlying tables.
-- Supabase's linter flags this as ERROR (0010_security_definer_view) and it
-- is the blocker for Phase 2 family-scoped RLS — without security_invoker,
-- a family member querying v_latest_positions would see ALL families' rows
-- even though base-table RLS blocks them.
--
-- security_invoker = true makes the view execute with the querying user's
-- privileges, so base-table RLS gates rows through the view as expected.
--
-- Phase 1 behaviour is unchanged: the current policy is
-- `authenticated USING (true)` so everyone still sees everything, just now
-- correctly going through RLS rather than around it.
--
-- ALTER VIEW ... SET is idempotent — safe to re-run.

ALTER VIEW public.v_latest_positions          SET (security_invoker = true);
ALTER VIEW public.v_nav_monthly_by_account    SET (security_invoker = true);
ALTER VIEW public.v_nav_monthly_by_trust      SET (security_invoker = true);
ALTER VIEW public.v_positions_refreshed       SET (security_invoker = true);
ALTER VIEW public.v_income_monthly_by_account SET (security_invoker = true);
ALTER VIEW public.v_external_flows            SET (security_invoker = true);
