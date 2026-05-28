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
import re
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


_MASTTRO_EXCHANGE_TAG_RE = re.compile(
    r"_(US|AU|LN|GB|FR|DE|GR|EU|JP|CA|HK|CH|NL|ES|IT)$", re.IGNORECASE,
)


def yfinance_lookup_ticker(ticker_masttro: str | None, local_ccy: str | None) -> str | None:
    """Convert a Masttro ticker + currency to a Yahoo Finance symbol.

    The bare ticker from Masttro almost always resolves to a US OTC
    ADR-equivalent on Yahoo (BCLYF for Barclays, MCQEF for Macquarie,
    NSRGF for Nestle, PCI = US PIMCO Dynamic Credit Fund instead of AU
    Perpetual Credit Income Trust), with illiquid stale prices. Append
    the local-exchange suffix so Yahoo returns the primary listing.

    Quirks handled:
      - HK: stock codes get zero-padded to 4 digits (700 -> 0700.HK).
      - JP: Masttro tags some tickers with trailing "je" (7011je for
        Mitsubishi Heavy Industries) -- strip to leading digits.
      - GBP: Masttro uses "BP/" for BP plc on LSE -- strip the slash
        (Yahoo wants "BP.L").
      - EUR: home exchange varies (DE/PA/AS/MC). Default to .DE; for
        Paris-/Amsterdam-/Madrid-listed names that aren't cross-listed
        on Frankfurt the lookup will fail and the sync will skip them
        (dashboard falls back to Masttro's price, which is the right
        behaviour given EUR damage was ~$116k vs $24M for AUD).
      - Already-suffixed tickers ("AENA.MC") are trusted as-is.
    """
    if not ticker_masttro:
        return None
    t = ticker_masttro.strip().upper()
    t = _MASTTRO_EXCHANGE_TAG_RE.sub("", t)
    if not t:
        return None
    if "." in t:
        return t
    if not local_ccy or local_ccy == "USD":
        return t.replace("/", "-")
    if local_ccy == "AUD":
        return t + ".AX"
    if local_ccy == "GBP":
        return t.replace("/", "") + ".L"
    if local_ccy == "CHF":
        return t + ".SW"
    if local_ccy == "HKD":
        m = re.match(r"^(\d+)", t)
        return (m.group(1).zfill(4) + ".HK") if m else t
    if local_ccy == "JPY":
        m = re.match(r"^(\d+)", t)
        return (m.group(1) + ".T") if m else t
    if local_ccy == "EUR":
        return t + ".DE"
    return t


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
            local_ccy = r["local_ccy"]
            # Non-USD: build the Yahoo symbol from ticker_masttro via
            # yfinance_lookup_ticker (handles per-exchange suffixes and
            # Masttro's per-market quirks). A pre-existing ticker_yf is
            # likely a US OTC ADR-equivalent and not worth re-trying.
            # USD: keep the original path (ticker_yf if previously
            # resolved, else ticker_masttro, then normalize_ticker).
            if local_ccy and local_ccy != "USD":
                nt = yfinance_lookup_ticker(
                    r["ticker_masttro"] or r["ticker_yf"], local_ccy,
                )
            else:
                nt = normalize_ticker(r["ticker_yf"] or r["ticker_masttro"])
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
