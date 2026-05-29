-- 026 — add S&P/ASX 200 Gross TR as a selectable benchmark index.
--
-- Adds the S&P/ASX 200 (gross total return, AUD) index to index_definition so
-- it appears in the benchmark dropdown on the Returns tile / Performance page.
-- listIndices() reads straight from this table, so no app change is needed —
-- just the seed row plus a one-off price backfill (scripts/backfill_indices.py);
-- the daily sync (sync_indices_recent) picks it up automatically thereafter.
--
-- ^AXJT is a true gross total-return series (dividends reinvested), so it's an
-- apples-to-apples comparison against the portfolio TR. It's denominated in
-- AUD; index returns are computed in the index's native currency (no FX
-- conversion), matching how ^SP500TR / ACWI are handled.

INSERT INTO public.index_definition (ticker, name, ccy, notes) VALUES
    ('^AXJT', 'S&P/ASX 200 Gross TR', 'AUD',
     'S&P/ASX 200 gross total-return index level (AUD). Dividends reinvested, so apples-to-apples vs portfolio TR. Returns computed in native AUD; no FX conversion.')
ON CONFLICT (ticker) DO NOTHING;
