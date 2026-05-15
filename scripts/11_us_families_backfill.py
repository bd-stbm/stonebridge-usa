"""One-shot historical backfill for the three US-domiciled Dyne sub-clients.

Pulls 12 months of Holdings snapshots + 12 months of Transactions for each of:
- Dyne Family (US)        — sub-client nodeId 102_93356
- Markiles Family         — sub-client nodeId 102_93361
- Miller Family           — sub-client nodeId 102_93360

Uses `investmentVehicle={sub_client_nodeId}` parameter to filter Stonebridge
container (id=7693) pulls to a specific sub-client subtree. ccy=USD native.

6 API calls. Saves all responses to responses/ and prints a sanity summary
(NAV trajectory + transaction type breakdown) per family.
"""

from __future__ import annotations

import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from client import MasttroClient  # noqa: E402


CLIENT_ID = 7693  # Stonebridge container
CCY = "USD"
YEAR_MONTH = 202605
HIST_MONTHS = 12
PERIOD_12M = 4

FAMILIES = [
    ("Dyne Family US", "102_93356"),
    ("Markiles Family", "102_93361"),
    ("Miller Family",   "102_93360"),
]


def summarise_holdings(name: str, data) -> None:
    if not isinstance(data, list):
        return
    print(f"\n  Holdings: {len(data)} rows total")
    by_date_count = Counter()
    by_date_mv = defaultdict(float)
    by_date_secids = defaultdict(set)
    for h in data:
        d = h.get("date")
        by_date_count[d] += 1
        by_date_mv[d] += h.get("marketValue") or 0
        if h.get("securityId") is not None:
            by_date_secids[d].add(h.get("securityId"))
    print(f"  Distinct as-of dates: {len(by_date_count)}")
    print(f"  NAV by date (sum of marketValue in {CCY}):")
    for d in sorted(by_date_count):
        print(f"    {d}:  {by_date_count[d]:>5} rows  "
              f"{by_date_mv[d]:>16,.2f}  ({len(by_date_secids[d])} distinct securities)")


def summarise_transactions(name: str, data) -> None:
    if not isinstance(data, list):
        return
    print(f"\n  Transactions: {len(data)} rows total")
    if not data:
        return
    types = Counter((t.get("transactionType") or "").strip() for t in data)
    print(f"  Transaction type counts:")
    for tt, n in types.most_common():
        print(f"    {tt:<30} {n}")
    # Date range of transactionDate
    dates = sorted((t.get("transactionDate") for t in data if t.get("transactionDate")))
    if dates:
        print(f"  transactionDate range: {dates[0]} -> {dates[-1]}")
    # Net cashflow in reporting CCY
    total_in = sum(
        (t.get("netAmountRepCCY") or 0)
        for t in data if (t.get("netAmountRepCCY") or 0) > 0
    )
    total_out = sum(
        (t.get("netAmountRepCCY") or 0)
        for t in data if (t.get("netAmountRepCCY") or 0) < 0
    )
    print(f"  Net inflows  ({CCY}): {total_in:>16,.2f}")
    print(f"  Net outflows ({CCY}): {total_out:>16,.2f}")
    print(f"  Net total    ({CCY}): {(total_in + total_out):>16,.2f}")


def main() -> int:
    client = MasttroClient()

    for fam_name, sub_nid in FAMILIES:
        print(f"\n{'='*72}\n{fam_name}  (sub-client {sub_nid})\n{'='*72}")

        # 12-month historical Holdings
        h_desc = f"id{CLIENT_ID}_sub{sub_nid}_{CCY.lower()}_{YEAR_MONTH}_h{HIST_MONTHS}"
        holdings = client.get_cached_or_fetch(
            f"Holdings/{CLIENT_ID}",
            {
                "ccy": CCY,
                "yearMonth": YEAR_MONTH,
                "historicalMonths": HIST_MONTHS,
                "investmentVehicle": sub_nid,
            },
            descriptor=h_desc,
        )
        summarise_holdings(fam_name, holdings)

        # 12-month Transactions
        t_desc = f"id{CLIENT_ID}_sub{sub_nid}_{CCY.lower()}_{YEAR_MONTH}_p{PERIOD_12M}"
        txns = client.get_cached_or_fetch(
            f"Transactions/{CLIENT_ID}",
            {
                "ccy": CCY,
                "yearMonth": YEAR_MONTH,
                "period": PERIOD_12M,
                "investmentVehicle": sub_nid,
            },
            descriptor=t_desc,
        )
        summarise_transactions(fam_name, txns)

    client.report()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
