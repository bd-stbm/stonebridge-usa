-- 030 — Phase C: require MFA (aal2) at the database layer, defence-in-depth.
--
-- Phase B made TOTP mandatory in the web app (middleware gates every route
-- on aal2). This adds the same requirement to the RLS policies on the
-- family-data tables, so even a valid-but-aal1 token (password verified,
-- MFA not yet completed) cannot read financial data through the API — not
-- just the web UI. The aal claim is set to 'aal2' only after a TOTP
-- challenge succeeds.
--
-- Applies to EVERYONE, admins included. Keeps the 029 InitPlan wrapping so
-- the functions still evaluate once per statement (no per-row cost). The
-- aal check is likewise wrapped in (SELECT ...).
--
-- NOT applied to app_user / user_family_access: those must stay readable at
-- aal1 so getSessionUser() and the MFA enrol/challenge flow work before the
-- user has stepped up. Shared reference tables stay authenticated-read.
-- service_role (the Python sync, admin server actions) BYPASSES RLS, so it
-- is unaffected.
--
-- Verified: aal2 admin sees all, aal2 client sees only their families, an
-- aal1 session (any role) sees zero family rows. Reversible by re-applying
-- migration 029 (drops the aal2 requirement).

DROP POLICY IF EXISTS "family scoped read" ON public.entity;
CREATE POLICY "family scoped read" ON public.entity
    FOR SELECT TO authenticated
    USING ((SELECT (auth.jwt() ->> 'aal')) = 'aal2'
           AND ((SELECT public.is_admin())
                OR sub_client_node_id IN (SELECT unnest(public.current_user_sub_clients()))));

DROP POLICY IF EXISTS "family scoped read" ON public.entity_attribution;
CREATE POLICY "family scoped read" ON public.entity_attribution
    FOR SELECT TO authenticated
    USING ((SELECT (auth.jwt() ->> 'aal')) = 'aal2'
           AND ((SELECT public.is_admin())
                OR sub_client_node_id IN (SELECT unnest(public.current_user_sub_clients()))));

DROP POLICY IF EXISTS "family scoped read" ON public.position_snapshot;
CREATE POLICY "family scoped read" ON public.position_snapshot
    FOR SELECT TO authenticated
    USING ((SELECT (auth.jwt() ->> 'aal')) = 'aal2'
           AND ((SELECT public.is_admin())
                OR sub_client_node_id IN (SELECT unnest(public.current_user_sub_clients()))));

DROP POLICY IF EXISTS "family scoped read" ON public.transaction_log;
CREATE POLICY "family scoped read" ON public.transaction_log
    FOR SELECT TO authenticated
    USING ((SELECT (auth.jwt() ->> 'aal')) = 'aal2'
           AND ((SELECT public.is_admin())
                OR sub_client_node_id IN (SELECT unnest(public.current_user_sub_clients()))));
