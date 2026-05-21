-- Capture Masttro's groupNodeId on entity so we can detect vehicles shared
-- across families. The same LP/LLC held by multiple families surfaces as
-- separate entity rows that share a groupNodeId; the cross-family check in
-- canonical_accounts_under uses this column to exclude shared subtrees.
-- Family-internal sharing (e.g. Modyl LP under Dyne only) keeps a
-- groupNodeId, but if that groupNodeId only appears under one family root
-- it is not flagged as shared.

ALTER TABLE public.entity
    ADD COLUMN IF NOT EXISTS group_node_id TEXT;

CREATE INDEX IF NOT EXISTS entity_group_node_idx
    ON public.entity (group_node_id) WHERE group_node_id IS NOT NULL;

COMMENT ON COLUMN public.entity.group_node_id IS
  'Masttro groupNodeId — the cross-structure identifier. The same vehicle '
  'appearing under multiple families shares one groupNodeId across rows.';
