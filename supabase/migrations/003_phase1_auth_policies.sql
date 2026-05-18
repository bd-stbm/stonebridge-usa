-- 003 — Phase 1 auth: every authenticated user sees all rows.
--
-- Phase 1 of the auth rollout. Any logged-in user has read access to
-- everything. Phase 2 will replace these with family-scoped policies that
-- read from a user_profile mapping table.
--
-- RLS is already enabled on every table (schema.sql). Without policies, RLS
-- blocks everything by default — which is why the dashboard was relying on
-- the service_role bypass. Once the web app switches to the anon key + user
-- session (Phase 1 web changes), these policies are what unblock reads for
-- signed-in users.
--
-- service_role still bypasses RLS, so the sync pipeline and any future
-- admin actions are unaffected.

-- DROP/CREATE pattern keeps the migration re-runnable; CREATE POLICY has no
-- IF NOT EXISTS across all supported Postgres versions.
DROP POLICY IF EXISTS "phase1 authenticated read" ON public.entity;
CREATE POLICY "phase1 authenticated read" ON public.entity
    FOR SELECT TO authenticated USING (true);

DROP POLICY IF EXISTS "phase1 authenticated read" ON public.entity_attribution;
CREATE POLICY "phase1 authenticated read" ON public.entity_attribution
    FOR SELECT TO authenticated USING (true);

DROP POLICY IF EXISTS "phase1 authenticated read" ON public.security;
CREATE POLICY "phase1 authenticated read" ON public.security
    FOR SELECT TO authenticated USING (true);

DROP POLICY IF EXISTS "phase1 authenticated read" ON public.position_snapshot;
CREATE POLICY "phase1 authenticated read" ON public.position_snapshot
    FOR SELECT TO authenticated USING (true);

DROP POLICY IF EXISTS "phase1 authenticated read" ON public.transaction_log;
CREATE POLICY "phase1 authenticated read" ON public.transaction_log
    FOR SELECT TO authenticated USING (true);

DROP POLICY IF EXISTS "phase1 authenticated read" ON public.pricing_refresh;
CREATE POLICY "phase1 authenticated read" ON public.pricing_refresh
    FOR SELECT TO authenticated USING (true);

DROP POLICY IF EXISTS "phase1 authenticated read" ON public.sync_log;
CREATE POLICY "phase1 authenticated read" ON public.sync_log
    FOR SELECT TO authenticated USING (true);
