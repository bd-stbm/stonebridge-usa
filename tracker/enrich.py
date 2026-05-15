"""yfinance + OpenFIGI price refresh for the tracker.

Run on demand to enrich the security master with current prices. Results are
written to the pricing_refresh table, keyed by (refresh_date, ticker_yf).
"""

from __future__ import annotations

import datetime as dt
import json
import re
import urllib.error
import urllib.request
import warnings

import yfinance as yf

from tracker import DEFAULT_DB_PATH
from tracker.ingest import connect, _log

warnings.filterwarnings("ignore")

OPENFIGI_URL = "https://api.openfigi.com/v3/mapping"
US_EXCH_PRIORITY = ["US", "UN", "UQ", "UA", "UR", "UF", "UV", "UB", "UW", "UP"]
COUNTRY_SUFFIX_RE = re.compile(r"_(US|AU|LN|GB|FR|DE|JP|CA|HK)$", re.IGNORECASE)


def normalize_ticker(t):
    if not t:
        return None
    t = t.strip().upper()
    t = COUNTRY_SUFFIX_RE.sub("", t)
    t = t.replace("/", "-")
    return t or None


def _fetch_yf(tickers: list[str], chunk: int = 50) -> tuple[dict, list]:
    """Fetch latest + previous close for each ticker.

    Returns:
        ok: {ticker: (price_latest, latest_date, price_previous_or_None, previous_date_or_None)}
        failed: [tickers]
    """
    ok, failed = {}, []
    for i in range(0, len(tickers), chunk):
        batch = tickers[i:i + chunk]
        try:
            data = yf.download(
                tickers=batch, period="5d", group_by="ticker",
                auto_adjust=False, progress=False, threads=True,
            )
        except Exception:
            failed.extend(batch)
            continue
        for t in batch:
            try:
                closes = data["Close"].dropna() if len(batch) == 1 else data[t]["Close"].dropna()
                if len(closes) >= 2:
                    ok[t] = (
                        float(closes.iloc[-1]), closes.index[-1].date(),
                        float(closes.iloc[-2]), closes.index[-2].date(),
                    )
                elif len(closes) == 1:
                    ok[t] = (float(closes.iloc[-1]), closes.index[-1].date(), None, None)
                else:
                    failed.append(t)
            except (KeyError, TypeError):
                failed.append(t)
    return ok, failed


def _openfigi_resolve(isins: list[str], chunk: int = 10) -> dict:
    out = {}
    if not isins:
        return out
    for i in range(0, len(isins), chunk):
        batch = isins[i:i + chunk]
        body = json.dumps([{"idType": "ID_ISIN", "idValue": v} for v in batch]).encode("utf-8")
        req = urllib.request.Request(
            OPENFIGI_URL, data=body,
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                response = json.loads(resp.read().decode("utf-8"))
        except Exception:
            for v in batch:
                out[v] = None
            continue
        for isin_, result in zip(batch, response):
            matches = result.get("data") or []
            chosen = None
            for ex in US_EXCH_PRIORITY:
                chosen = next((m for m in matches if m.get("exchCode") == ex and m.get("ticker")), None)
                if chosen:
                    break
            if not chosen:
                chosen = next((m for m in matches if m.get("ticker")), None)
            out[isin_] = chosen["ticker"] if chosen else None
    return out


def refresh_pricing(db_path=DEFAULT_DB_PATH, scope: str | None = None) -> dict:
    """Refresh equity prices for all securities currently held in scope.

    - For each Equity security with a normalised Masttro ticker, batch-fetch via yfinance.
    - For failures with an ISIN, try OpenFIGI → yfinance.
    - Persist ticker_yf + ticker_yf_source on the security row.
    - Append (refresh_date, ticker_yf, security_id, price, …) to pricing_refresh.

    Only refreshes securities present in CURRENT (latest snapshot) positions for
    the scope — no point pricing securities that aren't held.
    """
    conn = connect(db_path)
    cur = conn.cursor()

    # Find currently-held equity securities. If scope given, restrict to it.
    if scope:
        from tracker.api import scope_accounts, latest_snapshot_date
        accounts = scope_accounts(conn, scope)
        latest = latest_snapshot_date(conn, scope)
        if not accounts or not latest:
            return {"priced": 0, "failed": 0, "note": "no scope data"}
        placeholders = ",".join("?" * len(accounts))
        cur.execute(
            f"""SELECT DISTINCT s.security_id, s.ticker_masttro, s.isin
                FROM position_snapshot p
                JOIN security s ON p.security_id = s.security_id
                WHERE s.asset_class = 'Equity'
                  AND p.snapshot_date = ?
                  AND p.account_node_id IN ({placeholders})""",
            [latest] + accounts,
        )
    else:
        cur.execute(
            """SELECT DISTINCT s.security_id, s.ticker_masttro, s.isin
               FROM position_snapshot p
               JOIN security s ON p.security_id = s.security_id
               WHERE s.asset_class = 'Equity'"""
        )

    records = cur.fetchall()
    if not records:
        return {"priced": 0, "failed": 0}

    # Build normalised ticker → list of security_ids
    by_norm_ticker: dict[str, list[int]] = {}
    isin_by_sid: dict[int, str] = {}
    for sid, tk, isin in records:
        nt = normalize_ticker(tk)
        if nt:
            by_norm_ticker.setdefault(nt, []).append(sid)
        if isin:
            isin_by_sid[sid] = isin.strip()

    # Pass 1: yfinance with Masttro-derived tickers
    unique_tickers = sorted(by_norm_ticker.keys())
    print(f"yfinance pass 1: {len(unique_tickers)} tickers...")
    prices, failures = _fetch_yf(unique_tickers)
    print(f"  priced {len(prices)}, failed {len(failures)}")

    ticker_source = {tk: "masttro" for tk in prices}
    ticker_remap: dict[str, str] = {}  # old ticker → new ticker

    # Pass 2: OpenFIGI fallback for failures with an ISIN
    failed_isins = set()
    isin_to_old_ticker: dict[str, str] = {}
    for tk in failures:
        for sid in by_norm_ticker.get(tk, []):
            isin = isin_by_sid.get(sid)
            if isin:
                failed_isins.add(isin)
                isin_to_old_ticker[isin] = tk
                break  # one isin per failed ticker is enough

    if failed_isins:
        print(f"OpenFIGI fallback: resolving {len(failed_isins)} ISINs...")
        isin_to_ticker = _openfigi_resolve(sorted(failed_isins))
        new_tickers = []
        for isin, new_tk in isin_to_ticker.items():
            if not new_tk:
                continue
            new_tk_norm = normalize_ticker(new_tk)
            if new_tk_norm and new_tk_norm not in prices:
                old_tk = isin_to_old_ticker.get(isin)
                if old_tk:
                    ticker_remap[old_tk] = new_tk_norm
                if new_tk_norm not in new_tickers:
                    new_tickers.append(new_tk_norm)
        if new_tickers:
            print(f"yfinance pass 2: {len(new_tickers)} OpenFIGI-derived tickers...")
            extra_prices, _ = _fetch_yf(new_tickers)
            print(f"  priced {len(extra_prices)}")
            for nt in extra_prices:
                ticker_source[nt] = "openfigi"
            prices.update(extra_prices)

    # ----- Persist -----
    refresh_iso = dt.date.today().isoformat()

    # 1. security.ticker_yf + ticker_yf_source per security_id
    security_updates = []
    for tk, sids in by_norm_ticker.items():
        actual_tk = ticker_remap.get(tk, tk)
        if actual_tk in prices:
            for sid in sids:
                security_updates.append((actual_tk, ticker_source[actual_tk], sid))
    if security_updates:
        cur.executemany(
            "UPDATE security SET ticker_yf = ?, ticker_yf_source = ? WHERE security_id = ?",
            security_updates,
        )

    # 2. pricing_refresh rows
    refresh_rows = []
    for tk, payload in prices.items():
        price, asof, price_prev, asof_prev = payload
        prev_iso = asof_prev.isoformat() if asof_prev else None
        for sid in by_norm_ticker.get(tk, []):
            refresh_rows.append((
                refresh_iso, tk, sid, price, price_prev, "USD",
                asof.isoformat(), prev_iso,
                "yfinance+" + ticker_source[tk],
            ))
        # Rows for OpenFIGI-derived tickers (same payload, mapped to old ticker's security_ids)
        for old_tk, new_tk in ticker_remap.items():
            if new_tk == tk:
                for sid in by_norm_ticker.get(old_tk, []):
                    refresh_rows.append((
                        refresh_iso, tk, sid, price, price_prev, "USD",
                        asof.isoformat(), prev_iso,
                        "yfinance+openfigi",
                    ))
    if refresh_rows:
        cur.executemany(
            """INSERT OR REPLACE INTO pricing_refresh
               (refresh_date, ticker_yf, security_id, price, price_previous,
                price_ccy, yf_as_of_date, yf_previous_date, source)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            refresh_rows,
        )

    conn.commit()
    _log(conn, "pricing_refresh", scope or "all",
         f"unique_tickers={len(unique_tickers)} priced={len(prices)}",
         len(refresh_rows))
    conn.close()

    return {
        "unique_tickers": len(unique_tickers),
        "priced_via_masttro": sum(1 for v in ticker_source.values() if v == "masttro"),
        "priced_via_openfigi": sum(1 for v in ticker_source.values() if v == "openfigi"),
        "still_failed": len(unique_tickers) - len(prices),
        "rows_written": len(refresh_rows),
        "refresh_date": refresh_iso,
    }
