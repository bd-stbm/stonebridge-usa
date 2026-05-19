-- 011 — security_price_history: daily yfinance closes for held public
-- securities. Used to reconstruct historical NAVs at exact dates (instead
-- of snapping to month-end snapshots) for 6M / 1Y period returns.
--
-- Mirrors index_price_history's shape. Separate table because:
--   * pricing_refresh only keeps the last two prices per security.
--   * index_price_history holds benchmark levels, not security closes.
--
-- ticker_yf is the FK — same identifier we use everywhere else for
-- yfinance lookups. Securities without a ticker_yf are private positions
-- (real estate, hedge funds, custom funds, etc.) and stay on Masttro's
-- month-end valuations during reconstruction.

CREATE TABLE IF NOT EXISTS public.security_price_history (
    ticker_yf  TEXT NOT NULL,
    price_date DATE NOT NULL,
    close      NUMERIC(20, 8) NOT NULL,
    source     TEXT NOT NULL DEFAULT 'yfinance',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (ticker_yf, price_date)
);
CREATE INDEX IF NOT EXISTS security_price_history_date_idx
    ON public.security_price_history (price_date);
CREATE INDEX IF NOT EXISTS security_price_history_ticker_idx
    ON public.security_price_history (ticker_yf);

ALTER TABLE public.security_price_history ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "phase1 authenticated read" ON public.security_price_history;
CREATE POLICY "phase1 authenticated read" ON public.security_price_history
    FOR SELECT TO authenticated USING (true);

GRANT SELECT ON public.security_price_history
    TO anon, authenticated, service_role;
GRANT INSERT, UPDATE, DELETE ON public.security_price_history TO service_role;
