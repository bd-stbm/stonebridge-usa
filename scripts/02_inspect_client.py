"""Step 2 — deep inspection of one client.

Hits four endpoints (GWM, Holdings, Transactions, cef) for a single client id,
saves each raw response, and prints a short summary of size, shape, and a
handful of sample rows.

Defaults to Stonebridge Stonebridge (id=7693). Override with CLI arg:
    python scripts/02_inspect_client.py 19595
"""

from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from client import MasttroClient  # noqa: E402

DEFAULT_ID = 7693
CCY = "AUD"
YEAR_MONTH = 202605  # 2026-05 per the today's-date context
PERIOD_YTD = 1


def summarise(name: str, data) -> None:
    print(f"\n=== {name} ===")
    if data is None:
        print("  (no data)")
        return
    if isinstance(data, list):
        print(f"  rows: {len(data)}")
        if not data:
            return
        first = data[0]
        if isinstance(first, dict):
            keys = sorted(first.keys())
            print(f"  fields ({len(keys)}): {', '.join(keys)}")
            print(f"  first row:")
            for k in keys:
                v = first[k]
                v_repr = repr(v)
                if len(v_repr) > 80:
                    v_repr = v_repr[:77] + "..."
                print(f"    {k}: {v_repr}")
        else:
            print(f"  element type: {type(first).__name__}")
    elif isinstance(data, dict):
        print(f"  keys ({len(data)}): {', '.join(sorted(data.keys()))}")
    else:
        print(f"  type: {type(data).__name__}  value: {data!r}")


def field_coverage(name: str, data, sample_keys: list[str]) -> None:
    """For a few selected fields, show how many rows have a non-null value."""
    if not isinstance(data, list) or not data:
        return
    n = len(data)
    print(f"\n  {name} field coverage (n={n}):")
    for k in sample_keys:
        non_null = sum(1 for row in data if isinstance(row, dict) and row.get(k) not in (None, ""))
        print(f"    {k}: {non_null}/{n} populated")


def main() -> int:
    client_id = int(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_ID
    print(f"Inspecting client id={client_id}  ccy={CCY}  yearMonth={YEAR_MONTH}")

    client = MasttroClient()
    desc = f"id{client_id}_{CCY.lower()}_{YEAR_MONTH}"

    gwm = client.get_cached_or_fetch(
        f"GWM/{client_id}",
        {"ccy": CCY},
        descriptor=f"id{client_id}_{CCY.lower()}",
    )
    holdings = client.get_cached_or_fetch(
        f"Holdings/{client_id}",
        {"ccy": CCY, "yearMonth": YEAR_MONTH},
        descriptor=desc,
    )
    txns = client.get_cached_or_fetch(
        f"Transactions/{client_id}",
        {"ccy": CCY, "yearMonth": YEAR_MONTH, "period": PERIOD_YTD},
        descriptor=f"{desc}_p{PERIOD_YTD}",
    )
    cef = client.get_cached_or_fetch(
        f"cef/{client_id}",
        {"ccy": CCY, "yearMonth": YEAR_MONTH, "period": PERIOD_YTD},
        descriptor=f"{desc}_p{PERIOD_YTD}",
    )

    summarise("GWM", gwm)
    if isinstance(gwm, list):
        # Quick view of the entity tree shape
        parents = Counter(row.get("parentNodeId") for row in gwm if isinstance(row, dict))
        roots = parents.get(None, 0) + parents.get(0, 0)
        print(f"\n  GWM tree: {len(gwm)} nodes, ~{roots} root(s)")
        statuses = Counter(row.get("status") for row in gwm if isinstance(row, dict))
        print(f"  GWM status counts: {dict(statuses)}")

    summarise("Holdings", holdings)
    field_coverage(
        "Holdings",
        holdings,
        ["isin", "sedol", "cusip", "ticker", "securityId", "assetClass", "marketValue"],
    )
    if isinstance(holdings, list):
        total_mv = sum(
            (row.get("marketValue") or 0) for row in holdings if isinstance(row, dict)
        )
        print(f"\n  Holdings total marketValue (AUD): {total_mv:,.2f}")

    summarise("Transactions", txns)
    if isinstance(txns, list):
        types = Counter(row.get("transactionType") for row in txns if isinstance(row, dict))
        print(f"\n  Transaction type counts: {dict(types)}")

    summarise("cef", cef)
    if isinstance(cef, list):
        print(f"\n  CEF positions: {len(cef)}")
        total_commitment = sum(
            (row.get("commitment") or 0) for row in cef if isinstance(row, dict)
        )
        total_called = sum(
            (row.get("capitalCalled") or 0) for row in cef if isinstance(row, dict)
        )
        total_unfunded = sum(
            (row.get("unfundedCommitment") or 0) for row in cef if isinstance(row, dict)
        )
        total_mv_rep = sum(
            (row.get("marketValueRepCCY") or 0) for row in cef if isinstance(row, dict)
        )
        print(f"  CEF total commitment (AUD):    {total_commitment:,.2f}")
        print(f"  CEF total called (AUD):        {total_called:,.2f}")
        print(f"  CEF total unfunded (AUD):      {total_unfunded:,.2f}")
        print(f"  CEF total marketValue (AUD):   {total_mv_rep:,.2f}")

    client.report()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
