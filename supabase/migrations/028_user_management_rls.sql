-- 028 — Phase 2a: real multi-tenant user management + family-scoped RLS.
--
-- Replaces the Phase-1 "every authenticated user sees everything"
-- (USING (true), migration 003) and the cosmetic email-domain admin gate
-- (web/lib/admin.ts) with database-enforced row-level security so an
-- external client sees ONLY the families they're mapped to. Admins
-- (internal Stonebridge staff) see everything.
--
-- DESIGN (agreed 2026-06-03):
--   * Family-level granularity, keyed on the stable GWM sub_client_node_id
--     (e.g. '102_93356'), never the display alias.
--   * Admin identity lives in a DB role table, not an email check.
--   * Many-to-many user<->family mapping (a client can own several
--     families; a family can have several logins).
--
-- The migration is in two parts so it can be rolled out and verified in
-- stages:
--   PART A — additive only (tables, helpers, grants, denormalised column +
--            backfill, admin seed). Changes NO row visibility; the Phase-1
--            USING(true) policies are still in force after Part A.
--   PART B — the breaking-but-reversible swap: drop Phase-1 policies, add
--            family-scoped ones. Reverse by re-running migration 003.
--
-- Safe to apply now: the only auth users are internal @stbm.com.au accounts,
-- seeded as admin in Part A, so the swap locks nobody out. The Python sync
-- uses service_role, which bypasses RLS entirely and is unaffected.
-- =====================================================================
-- PART A — additive (non-breaking)
-- =====================================================================

-- --- Tables -----------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.app_user (
    user_id    UUID PRIMARY KEY REFERENCES auth.users(id) ON DELETE CASCADE,
    role       TEXT NOT NULL DEFAULT 'client' CHECK (role IN ('admin', 'client')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.user_family_access (
    user_id            UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
    sub_client_node_id TEXT NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, sub_client_node_id)
);
CREATE INDEX IF NOT EXISTS user_family_access_user_idx
    ON public.user_family_access (user_id);

-- --- Helper functions -------------------------------------------------
-- Both are SECURITY DEFINER so they read app_user / user_family_access
-- bypassing those tables' own RLS (no recursion), and STABLE so the
-- planner can cache them within a statement. search_path is pinned to
-- public to keep SECURITY DEFINER safe.

CREATE OR REPLACE FUNCTION public.is_admin()
RETURNS boolean
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
    SELECT EXISTS (
        SELECT 1 FROM public.app_user
        WHERE user_id = auth.uid() AND role = 'admin'
    );
$$;

-- The set of sub_client_node_ids a NON-admin user may see. Admins are
-- handled by the OR is_admin() in every policy, so this deliberately does
-- not special-case them.
CREATE OR REPLACE FUNCTION public.current_user_sub_clients()
RETURNS text[]
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
    SELECT COALESCE(array_agg(sub_client_node_id), '{}')
    FROM public.user_family_access
    WHERE user_id = auth.uid();
$$;

-- --- Grants -----------------------------------------------------------
GRANT SELECT ON public.app_user            TO authenticated;
GRANT SELECT ON public.user_family_access  TO authenticated;
GRANT ALL    ON public.app_user            TO service_role;
GRANT ALL    ON public.user_family_access  TO service_role;
GRANT EXECUTE ON FUNCTION public.is_admin()                   TO authenticated;
GRANT EXECUTE ON FUNCTION public.current_user_sub_clients()   TO authenticated;

-- --- RLS on the two new tables ---------------------------------------
ALTER TABLE public.app_user           ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.user_family_access ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "own row or admin" ON public.app_user;
CREATE POLICY "own row or admin" ON public.app_user
    FOR SELECT TO authenticated
    USING (user_id = auth.uid() OR public.is_admin());

DROP POLICY IF EXISTS "own rows or admin" ON public.user_family_access;
CREATE POLICY "own rows or admin" ON public.user_family_access
    FOR SELECT TO authenticated
    USING (user_id = auth.uid() OR public.is_admin());

-- --- Denormalise sub_client_node_id onto the scoped tables ------------
-- entity, position_snapshot (583k rows) and transaction_log carry only
-- account_node_id (entity carries node_id). A per-row join/function in the
-- RLS policy is expensive (cf. the cold statement-timeout incident,
-- migration 027), so we copy the owning family onto each row and index it.
-- Every policy then reduces to a cheap column = ANY(...) check.
--   * entity_attribution already has sub_client_node_id natively.
--   * entity is 1:1 with entity_attribution on node_id; the weekly sync
--     keeps the column fresh (rebuild_attribution).
--   * position_snapshot / transaction_log are populated on every INSERT by
--     the trigger below — no writer (daily sync, backfill, manual) needs to
--     know about the column, which keeps it from silently going NULL (a
--     NULL row is admin-only-visible, i.e. hidden from the owning client).
-- IMPORTANT: these mass UPDATEs rewrite every row — run VACUUM (ANALYZE)
-- on the three tables right after applying, or the next index-only scan
-- degrades into millions of heap fetches (the 027 lesson again).
ALTER TABLE public.entity            ADD COLUMN IF NOT EXISTS sub_client_node_id TEXT;
ALTER TABLE public.position_snapshot ADD COLUMN IF NOT EXISTS sub_client_node_id TEXT;
ALTER TABLE public.transaction_log   ADD COLUMN IF NOT EXISTS sub_client_node_id TEXT;

UPDATE public.entity e
   SET sub_client_node_id = ea.sub_client_node_id
  FROM public.entity_attribution ea
 WHERE ea.node_id = e.node_id
   AND e.sub_client_node_id IS DISTINCT FROM ea.sub_client_node_id;

UPDATE public.position_snapshot ps
   SET sub_client_node_id = ea.sub_client_node_id
  FROM public.entity_attribution ea
 WHERE ea.node_id = ps.account_node_id
   AND ps.sub_client_node_id IS DISTINCT FROM ea.sub_client_node_id;

UPDATE public.transaction_log tl
   SET sub_client_node_id = ea.sub_client_node_id
  FROM public.entity_attribution ea
 WHERE ea.node_id = tl.account_node_id
   AND tl.sub_client_node_id IS DISTINCT FROM ea.sub_client_node_id;

CREATE INDEX IF NOT EXISTS entity_sub_client_idx
    ON public.entity (sub_client_node_id);
CREATE INDEX IF NOT EXISTS pos_snap_sub_client_idx
    ON public.position_snapshot (sub_client_node_id);
CREATE INDEX IF NOT EXISTS txn_log_sub_client_idx
    ON public.transaction_log (sub_client_node_id);

-- Auto-populate sub_client_node_id on insert into the daily-written tables
-- from entity_attribution (the source of truth). Only fills when the caller
-- left it NULL, so an explicit value (or a re-derive) is never clobbered.
CREATE OR REPLACE FUNCTION public.set_sub_client_node_id()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.sub_client_node_id IS NULL THEN
        SELECT ea.sub_client_node_id INTO NEW.sub_client_node_id
        FROM public.entity_attribution ea
        WHERE ea.node_id = NEW.account_node_id;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_position_snapshot_sub_client ON public.position_snapshot;
CREATE TRIGGER trg_position_snapshot_sub_client
    BEFORE INSERT ON public.position_snapshot
    FOR EACH ROW EXECUTE FUNCTION public.set_sub_client_node_id();

DROP TRIGGER IF EXISTS trg_transaction_log_sub_client ON public.transaction_log;
CREATE TRIGGER trg_transaction_log_sub_client
    BEFORE INSERT ON public.transaction_log
    FOR EACH ROW EXECUTE FUNCTION public.set_sub_client_node_id();

-- --- Seed internal admins --------------------------------------------
-- Bootstrap: every existing @stbm.com.au login is an admin. New admins are
-- added by inserting here (or via the future Users admin page).
INSERT INTO public.app_user (user_id, role)
SELECT id, 'admin'
  FROM auth.users
 WHERE lower(email) LIKE '%@stbm.com.au'
ON CONFLICT (user_id) DO UPDATE SET role = 'admin';

-- =====================================================================
-- PART B — family-scoped policies (breaking, reversible via migration 003)
-- =====================================================================
-- Replace the Phase-1 USING(true) read policies on the family-scoped
-- tables. Shared reference data (security, pricing_refresh, index_*,
-- security_price_history) keeps its authenticated-read policy — it holds
-- no per-family information. sync_log becomes admin-only.

-- entity_attribution — native sub_client_node_id column
DROP POLICY IF EXISTS "phase1 authenticated read" ON public.entity_attribution;
DROP POLICY IF EXISTS "family scoped read" ON public.entity_attribution;
CREATE POLICY "family scoped read" ON public.entity_attribution
    FOR SELECT TO authenticated
    USING (public.is_admin()
           OR sub_client_node_id = ANY (public.current_user_sub_clients()));

-- entity — denormalised column
DROP POLICY IF EXISTS "phase1 authenticated read" ON public.entity;
DROP POLICY IF EXISTS "family scoped read" ON public.entity;
CREATE POLICY "family scoped read" ON public.entity
    FOR SELECT TO authenticated
    USING (public.is_admin()
           OR sub_client_node_id = ANY (public.current_user_sub_clients()));

-- position_snapshot — denormalised column
DROP POLICY IF EXISTS "phase1 authenticated read" ON public.position_snapshot;
DROP POLICY IF EXISTS "family scoped read" ON public.position_snapshot;
CREATE POLICY "family scoped read" ON public.position_snapshot
    FOR SELECT TO authenticated
    USING (public.is_admin()
           OR sub_client_node_id = ANY (public.current_user_sub_clients()));

-- transaction_log — denormalised column
DROP POLICY IF EXISTS "phase1 authenticated read" ON public.transaction_log;
DROP POLICY IF EXISTS "family scoped read" ON public.transaction_log;
CREATE POLICY "family scoped read" ON public.transaction_log
    FOR SELECT TO authenticated
    USING (public.is_admin()
           OR sub_client_node_id = ANY (public.current_user_sub_clients()));

-- sync_log — internal only
DROP POLICY IF EXISTS "phase1 authenticated read" ON public.sync_log;
DROP POLICY IF EXISTS "admin only read" ON public.sync_log;
CREATE POLICY "admin only read" ON public.sync_log
    FOR SELECT TO authenticated
    USING (public.is_admin());
