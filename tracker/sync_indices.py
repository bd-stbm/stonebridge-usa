"""Index benchmark sync — backfill and daily incremental.

Maintains index_price_history (Postgres) from yfinance.

auto_adjust=True is used so the Close column is split- AND dividend-adjusted,
which for ETFs like ACWI approximates total return by treating each
distribution as if it had been reinvested. For ^SP500TR (already a TR
index) the adjustment is a no-op.
"""

from __future__ import annotations

import datetime as dt
import math
import warnings

import yfinance as yf

warnings.filterwarnings("ignore")


def _tracked_tickers(conn) -> list[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT ticker FROM index_definition ORDER BY ticker")
        return [r["ticker"] for r in cur.fetchall()]


def _fetch_history(ticker: str, start: dt.date, end: dt.date) -> list[tuple[dt.date, float]]:
    """Returns a list of (price_date, close) tuples."""
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
            """INSERT INTO index_price_history (ticker, price_date, close, source)
               VALUES (%s, %s, %s, 'yfinance')
               ON CONFLICT (ticker, price_date) DO UPDATE SET
                 close = EXCLUDED.close,
                 source = EXCLUDED.source""",
            [(ticker, d, p) for d, p in rows],
        )
    conn.commit()
    return len(rows)


def backfill_indices(conn, years: int = 5) -> dict[str, int]:
    """Pull `years` of history for every index in index_definition.
    Idempotent — upserts on (ticker, price_date)."""
    end = dt.date.today()
    start = end - dt.timedelta(days=years * 365 + 30)
    stats: dict[str, int] = {}
    for ticker in _tracked_tickers(conn):
        print(f"  backfilling {ticker} {start} -> {end}")
        rows = _fetch_history(ticker, start, end)
        n = _upsert_prices(conn, ticker, rows)
        stats[ticker] = n
        print(f"    {n} rows")
    return stats


def sync_indices_recent(conn, days_back: int = 10) -> dict[str, int]:
    """Pull the latest ~days_back days of closes for every tracked index.
    Generous window so a missed sync day is auto-recovered."""
    end = dt.date.today()
    start = end - dt.timedelta(days=days_back)
    stats: dict[str, int] = {}
    for ticker in _tracked_tickers(conn):
        rows = _fetch_history(ticker, start, end)
        n = _upsert_prices(conn, ticker, rows)
        stats[ticker] = n
    return stats
