-- 001 — Add natural-key dedup constraint to transaction_log.
--
-- Masttro doesn't return a stable transaction id, so we synthesise uniqueness
-- from the columns that identify the event. NULLS NOT DISTINCT (Postgres 15+)
-- treats NULLs as equal so partial-null rows still dedupe.
--
-- The daily Masttro sync re-pulls YTD transactions and relies on this
-- constraint to make ON CONFLICT DO NOTHING work.

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'transaction_log_dedup_uniq'
    ) THEN
        ALTER TABLE public.transaction_log
        ADD CONSTRAINT transaction_log_dedup_uniq
        UNIQUE NULLS NOT DISTINCT (
            account_node_id,
            transaction_date,
            security_id,
            transaction_type_clean,
            quantity,
            net_amount_local
        );
    END IF;
END $$;
