"""Daily yfinance price-history sync for held public securities.

Populates security_price_history with daily closes per (ticker_yf, date).
Used by the dashboard to reconstruct NAV at arbitrary historical dates
(for precise 6M / 1Y returns instead of snapping to month-end).

auto_adjust=True so the Close column is split- and dividend-adjusted
— same setup as the index sync. Securities yfinance doesn't have, or
private holdings without a ticker_yf, simply aren't included; the
dashboard falls back to Masttro's month-end valuation for those.
"""

from __future__ import annotations

import datetime as dt
import math
import warnings

import yfinance as yf

warnings.filterwarnings("ignore")


def _tracked_tickers(conn) -> list[str]:
    """Distinct ticker_yf values from securities that have actually been
    held (i.e. appear in position_snapshot) — no point fetching history
    for tickers we don't track."""
    with conn.cursor() as cur:
        cur.execute(
            """SELECT DISTINCT s.ticker_yf
               FROM security s
               WHERE s.ticker_yf IS NOT NULL
                 AND EXISTS (SELECT 1 FROM position_snapshot p
                             WHERE p.security_id = s.security_id)
               ORDER BY s.ticker_yf"""
        )
        return [r["ticker_yf"] for r in cur.fetchall()]


def _fetch_history(ticker: str, start: dt.date, end: dt.date) -> list[tuple[dt.date, float]]:
    tk = yf.Ticker(ticker)
    hist = tk.history(
        start=start.isoformat(),
        end=(end + dt.timedelta(days=1)).isoformat(),
        auto_adjust=True,
    )
    if hist is None or hist.empty:
        return []
    out: list[tuple[dt.date, float]] = []
    for row in hist.itertuples():
        close = row.Close
        if close is None or (isinstance(close, float) and math.isnan(close)):
            continue
        d = row.Index.date() if hasattr(row.Index, "date") else row.Index
        out.append((d, float(close)))
    return out


def _upsert_prices(conn, ticker: str, rows: list[tuple[dt.date, float]]) -> int:
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """INSERT INTO security_price_history (ticker_yf, price_date, close, source)
               VALUES (%s, %s, %s, 'yfinance')
               ON CONFLICT (ticker_yf, price_date) DO UPDATE SET
                 close = EXCLUDED.close,
                 source = EXCLUDED.source""",
            [(ticker, d, p) for d, p in rows],
        )
    conn.commit()
    return len(rows)


def backfill_security_prices(conn, years: int = 5) -> dict[str, int]:
    """One-off: pull `years` of daily history for every held public security."""
    end = dt.date.today()
    start = end - dt.timedelta(days=years * 365 + 30)
    stats: dict[str, int] = {}
    tickers = _tracked_tickers(conn)
    print(f"  {len(tickers)} held tickers to backfill")
    for i, ticker in enumerate(tickers, start=1):
        print(f"  [{i}/{len(tickers)}] {ticker}", end="", flush=True)
        try:
            rows = _fetch_history(ticker, start, end)
            n = _upsert_prices(conn, ticker, rows)
            stats[ticker] = n
            print(f" -> {n} rows")
        except Exception as e:
            print(f" FAILED: {e}")
            stats[ticker] = 0
    return stats


def sync_security_prices_recent(conn, days_back: int = 10) -> dict[str, int]:
    """Append latest closes for every tracked ticker. Generous window so
    a missed sync day is recovered automatically."""
    end = dt.date.today()
    start = end - dt.timedelta(days=days_back)
    stats: dict[str, int] = {}
    for ticker in _tracked_tickers(conn):
        try:
            rows = _fetch_history(ticker, start, end)
            n = _upsert_prices(conn, ticker, rows)
            stats[ticker] = n
        except Exception:
            stats[ticker] = 0
    return stats
