-- 029 — make the family-scoped RLS policies cheap on large scans.
--
-- SYMPTOM: after migration 028, admin (and client) queries that scan a lot
-- of position_snapshot — e.g. v_nav_monthly_by_account behind
-- getNavSeries — slowed to multiple seconds and tripped the 8s statement
-- timeout (57014). getLatestPositions also went from ~250ms to ~2.6s.
--
-- CAUSE: the 028 policies wrote the predicate as
--     is_admin() OR sub_client_node_id = ANY (current_user_sub_clients())
-- Postgres evaluated BOTH functions PER ROW (visible as an inline Filter in
-- EXPLAIN). is_admin() runs a SELECT EXISTS each call, so on a 583k-row scan
-- that's hundreds of thousands of sub-queries — ~4s for one view alone.
--
-- FIX: wrap the function calls so the planner runs them ONCE per statement
-- (the standard Supabase RLS perf pattern):
--   * (SELECT public.is_admin())                         -> InitPlan, once
--   * sub_client_node_id IN (SELECT unnest(current_user_sub_clients()))
--                                                         -> hashed SubPlan,
--                                                            built once, O(1)
-- Same rows in/out — only the evaluation strategy changes.
--   v_nav_monthly_by_account scan, admin: 4136ms -> 436ms (verified);
--   admin still sees all 5 families, a client still only their own.
--
-- Semantics are identical to 028; this only rewrites the four SELECT
-- policies. (Phase C will later add an aal2 requirement on top of this form.)

DROP POLICY IF EXISTS "family scoped read" ON public.entity;
CREATE POLICY "family scoped read" ON public.entity
    FOR SELECT TO authenticated
    USING ((SELECT public.is_admin())
           OR sub_client_node_id IN (SELECT unnest(public.current_user_sub_clients())));

DROP POLICY IF EXISTS "family scoped read" ON public.entity_attribution;
CREATE POLICY "family scoped read" ON public.entity_attribution
    FOR SELECT TO authenticated
    USING ((SELECT public.is_admin())
           OR sub_client_node_id IN (SELECT unnest(public.current_user_sub_clients())));

DROP POLICY IF EXISTS "family scoped read" ON public.position_snapshot;
CREATE POLICY "family scoped read" ON public.position_snapshot
    FOR SELECT TO authenticated
    USING ((SELECT public.is_admin())
           OR sub_client_node_id IN (SELECT unnest(public.current_user_sub_clients())));

DROP POLICY IF EXISTS "family scoped read" ON public.transaction_log;
CREATE POLICY "family scoped read" ON public.transaction_log
    FOR SELECT TO authenticated
    USING ((SELECT public.is_admin())
           OR sub_client_node_id IN (SELECT unnest(public.current_user_sub_clients())));
