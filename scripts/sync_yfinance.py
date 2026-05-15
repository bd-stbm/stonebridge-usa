"""Daily yfinance pricing refresh against Supabase.

Reads every equity security in security with a held position, normalises
the ticker, fetches latest + previous close from yfinance, and writes one
pricing_refresh row per (refresh_date, ticker_yf). Failed lookups fall back
to OpenFIGI → yfinance. Idempotent — re-runs the same day overwrite that
day's rows.

Env: SUPABASE_DB_URL.
"""

from __future__ import annotations

import datetime as dt
import sys
import warnings
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
warnings.filterwarnings("ignore")

from tracker.db import connect, log_sync
from tracker.enrich import _fetch_yf, _openfigi_resolve, normalize_ticker
from tracker.sync_supabase import insert_pricing_refresh, set_security_ticker_yf


def main() -> int:
    refresh_iso = dt.date.today().isoformat()
    print(f"yfinance refresh — refresh_date={refresh_iso}")

    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT s.security_id, s.ticker_masttro, s.ticker_yf, s.isin
                FROM security s
                WHERE s.asset_class = 'Equity'
                  AND EXISTS (SELECT 1 FROM position_snapshot p WHERE p.security_id = s.security_id)
            """)
            rows = cur.fetchall()

        by_norm: dict[str, list[int]] = {}
        isin_by_sid: dict[int, str] = {}
        for r in rows:
            sid = r["security_id"]
            ticker = r["ticker_yf"] or r["ticker_masttro"]
            nt = normalize_ticker(ticker)
            if nt:
                by_norm.setdefault(nt, []).append(sid)
            if r["isin"]:
                isin_by_sid[sid] = r["isin"].strip()

        unique = sorted(by_norm.keys())
        print(f"  {len(unique)} tickers to price ({len(rows)} securities)")
        prices, failures = _fetch_yf(unique)
        print(f"  yfinance pass: priced={len(prices)} failed={len(failures)}")

        ticker_source = {tk: "masttro" for tk in prices}
        ticker_remap: dict[str, str] = {}

        # OpenFIGI fallback for failures with an ISIN
        failed_isins = set()
        isin_to_old: dict[str, str] = {}
        for tk in failures:
            for sid in by_norm.get(tk, []):
                isin = isin_by_sid.get(sid)
                if isin:
                    failed_isins.add(isin)
                    isin_to_old[isin] = tk
                    break

        if failed_isins:
            print(f"  OpenFIGI resolving {len(failed_isins)} ISINs...")
            resolved = _openfigi_resolve(sorted(failed_isins))
            new_tickers = []
            for isin, new_tk in resolved.items():
                nt_norm = normalize_ticker(new_tk)
                if nt_norm and nt_norm not in prices:
                    old = isin_to_old.get(isin)
                    if old:
                        ticker_remap[old] = nt_norm
                    if nt_norm not in new_tickers:
                        new_tickers.append(nt_norm)
            if new_tickers:
                extra, _ = _fetch_yf(new_tickers)
                print(f"  OpenFIGI->yfinance: priced={len(extra)}")
                for nt in extra:
                    ticker_source[nt] = "openfigi"
                prices.update(extra)

        # Persist resolved ticker_yf on the security rows.
        sec_updates = []
        for tk, sids in by_norm.items():
            actual = ticker_remap.get(tk, tk)
            if actual in prices:
                src = ticker_source[actual]
                for sid in sids:
                    sec_updates.append((actual, src, sid))
        set_security_ticker_yf(conn, sec_updates)

        # Build pricing_refresh rows (one per (ticker, security_id)).
        refresh_rows = []
        for tk, payload in prices.items():
            price, asof, price_prev, asof_prev = payload
            asof_prev_iso = asof_prev.isoformat() if asof_prev else None
            for sid in by_norm.get(tk, []):
                refresh_rows.append((
                    refresh_iso, tk, sid, price, price_prev, "USD",
                    asof.isoformat(), asof_prev_iso, "yfinance",
                ))
            for old_tk, new_tk in ticker_remap.items():
                if new_tk == tk:
                    for sid in by_norm.get(old_tk, []):
                        refresh_rows.append((
                            refresh_iso, tk, sid, price, price_prev, "USD",
                            asof.isoformat(), asof_prev_iso, "yfinance+openfigi",
                        ))

        n = insert_pricing_refresh(conn, refresh_rows)
        log_sync(conn, "pricing_refresh", "all",
                 f"unique={len(unique)} priced={len(prices)} written={n}", n)
        print(f"  wrote {n} pricing_refresh rows")

    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
