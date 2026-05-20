-- 015 — drop the 4-arg reconstructed_nav_at overload.
--
-- Migration 014 added a 5-arg version with the new p_asset_class param,
-- but `CREATE OR REPLACE FUNCTION` only replaces an exact-signature
-- match — so the original 4-arg version (from 012 / 013) still exists.
-- PostgREST then fails any 4-arg call with PGRST203 ("Could not choose
-- the best candidate function").
--
-- The 5-arg version with p_asset_class DEFAULT NULL is a strict
-- superset — calling it without the asset-class arg behaves identically
-- to the old 4-arg version. So we drop the old overload.

DROP FUNCTION IF EXISTS public.reconstructed_nav_at(text, text[], text[], date);
