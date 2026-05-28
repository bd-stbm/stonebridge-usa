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
from tracker.sync_indices import sync_indices_recent
from tracker.sync_security_prices import sync_security_prices_recent
from tracker.sync_supabase import insert_pricing_refresh, set_security_ticker_yf


def main() -> int:
    refresh_iso = dt.date.today().isoformat()
    print(f"yfinance refresh — refresh_date={refresh_iso}")

    conn = connect()
    try:
        with conn.cursor() as cur:
            # Restrict to currently-held equities (latest snapshot per
            # account, quantity > 0). The earlier `EXISTS … FROM
            # position_snapshot` matched every equity that had ever
            # been held — historical month-end positions of since-sold
            # securities included. That inflated the daily request
            # volume well past 1,500 tickers and tripped Yahoo's rate
            # limit. v_latest_positions already implements the
            # per-account "latest snapshot" filter the dashboard reads
            # from, so the universe here now matches what the holdings
            # table actually needs prices for.
            # Exclude Structured Products: Masttro tags equity-linked
            # structured notes (e.g. "BNS 0 08/06/27") as asset_class
            # 'Equity' but they trade like bonds — quantity is face
            # value, price is per $100. yfinance returns the equity
            # ticker's per-share price, which the refresh layer then
            # multiplies against face-value quantities → wildly wrong
            # NAVs (sweep on 2026-05-28 found 11 such securities,
            # net +$692k overstatement on Dyne (US), with one Optsia
            # BNS bond alone underwater by $895k vs Masttro). Fall
            # back to Masttro's price for these by simply not pricing
            # them here. See ticker_yf clearing in commit message.
            cur.execute("""
                SELECT DISTINCT s.security_id, s.ticker_masttro, s.ticker_yf,
                                s.isin, s.local_ccy
                FROM security s
                WHERE s.asset_class = 'Equity'
                  AND s.security_type IS DISTINCT FROM 'Structured Products'
                  AND EXISTS (
                      SELECT 1 FROM v_latest_positions lp
                      WHERE lp.security_id = s.security_id
                        AND lp.quantity > 0
                  )
            """)
            rows = cur.fetchall()

        by_norm: dict[str, list[int]] = {}
        isin_by_sid: dict[int, str] = {}
        for r in rows:
            sid = r["security_id"]
            # For non-USD-denominated listings, prefer ticker_masttro: a
            # previously-resolved ticker_yf is likely a US OTC ADR-equivalent
            # (BCLYF for Barclays, NSRGF for Nestle, MCQEF for Macquarie,
            # PCI for Perpetual Credit Income Trust AU instead of US PCI),
            # which would either round-trip the same wrong choice or fetch
            # an illiquid OTC stale price. Masttro's ticker is closer to
            # the local-exchange symbol and combines with the .AX/.L/etc.
            # suffix below to find the real listing.
            local_ccy = r["local_ccy"]
            if local_ccy and local_ccy != "USD":
                ticker = r["ticker_masttro"] or r["ticker_yf"]
            else:
                ticker = r["ticker_yf"] or r["ticker_masttro"]
            nt = normalize_ticker(ticker)
            if nt:
                # Append the local-exchange suffix so Yahoo returns the
                # primary listing's price, not the bare US-listed match.
                # Limited to AUD for now — the next iteration will add
                # GBP/EUR/HKD/CHF/JPY mappings (sweep on 2026-05-28 showed
                # $24M of AUD overstatement vs ~$300k combined for the
                # others, so AUD is the urgent one).
                if local_ccy == "AUD" and "." not in nt:
                    nt = nt + ".AX"
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

        # Append the latest closes for benchmark indices used by the dashboard
        # Returns tile. Uses a generous 10-day window so a missed run recovers
        # automatically.
        print("syncing index benchmarks...")
        idx_stats = sync_indices_recent(conn, days_back=10)
        idx_total = sum(idx_stats.values())
        log_sync(conn, "index_sync", "all",
                 ", ".join(f"{k}={v}" for k, v in idx_stats.items()),
                 idx_total)
        print(f"  index rows written: {idx_total}")

        # Append latest closes for every held public security — feeds the
        # historical NAV reconstruction used by 6M / 1Y returns.
        print("syncing security price history...")
        sec_stats = sync_security_prices_recent(conn, days_back=10)
        sec_total = sum(sec_stats.values())
        log_sync(conn, "security_price_sync", "all",
                 f"tickers={len(sec_stats)} rows={sec_total}",
                 sec_total)
        print(f"  security price rows written: {sec_total}")

    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
