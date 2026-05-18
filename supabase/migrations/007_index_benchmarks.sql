-- 007 — Index benchmark tables for the Returns-vs-index comparison.
--
-- Stores yfinance-fetched price history for benchmark indices used in the
-- Returns tile. Separate from pricing_refresh (which is keyed to held
-- securities and only retains the last two prices) because benchmarks need
-- years of daily history.

CREATE TABLE IF NOT EXISTS public.index_definition (
    ticker     TEXT PRIMARY KEY,
    name       TEXT NOT NULL,
    ccy        CHAR(3) NOT NULL DEFAULT 'USD',
    notes      TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.index_price_history (
    ticker     TEXT NOT NULL REFERENCES public.index_definition(ticker) ON DELETE CASCADE,
    price_date DATE NOT NULL,
    close      NUMERIC(20, 8) NOT NULL,
    source     TEXT NOT NULL DEFAULT 'yfinance',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (ticker, price_date)
);
CREATE INDEX IF NOT EXISTS index_price_date_idx
    ON public.index_price_history (price_date);

-- Seed the two indices we want for v1.
INSERT INTO public.index_definition (ticker, name, ccy, notes) VALUES
    ('^SP500TR', 'S&P 500 Total Return', 'USD',
     'Index level; already a total-return series so dividends are baked in.'),
    ('ACWI',     'MSCI ACWI ETF',         'USD',
     'iShares MSCI ACWI ETF. Sync uses auto-adjusted closes so dividend distributions are reinvested back into price (approximate TR).')
ON CONFLICT (ticker) DO NOTHING;

-- RLS — Phase 1 read policy matches the pattern in migration 003.
ALTER TABLE public.index_definition    ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.index_price_history ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "phase1 authenticated read" ON public.index_definition;
CREATE POLICY "phase1 authenticated read" ON public.index_definition
    FOR SELECT TO authenticated USING (true);

DROP POLICY IF EXISTS "phase1 authenticated read" ON public.index_price_history;
CREATE POLICY "phase1 authenticated read" ON public.index_price_history
    FOR SELECT TO authenticated USING (true);

-- Explicit grants in case ALTER DEFAULT PRIVILEGES from migration 002
-- didn't propagate (e.g. tables created by a different role). Idempotent.
GRANT SELECT ON public.index_definition, public.index_price_history
    TO anon, authenticated, service_role;
GRANT INSERT, UPDATE, DELETE ON public.index_definition, public.index_price_history
    TO service_role;
