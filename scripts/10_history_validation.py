"""Validate that /Performance and historicalMonths support a tracking tool.

Two API calls against Russel Steingold (id=19595):
1. GET /Performance/19595?ccy=AUD&yearMonth=202605&period=1  (YTD perf)
2. GET /Holdings/19595?ccy=AUD&yearMonth=202605&historicalMonths=12

Saves both to responses/ and prints summary of:
- Performance: which fields are populated (TWR, IRR, deposits, etc.)
- Holdings history: how many distinct months represented, row count per month,
  whether positions appear/disappear correctly across the year
"""

from __future__ import annotations

import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from client import MasttroClient  # noqa: E402


CLIENT_ID = 19595  # Russel Steingold
CCY = "AUD"
YEAR_MONTH = 202605
PERIOD_YTD = 1


def main() -> int:
    client = MasttroClient()

    # 1. Performance
    perf = client.get_cached_or_fetch(
        f"Performance/{CLIENT_ID}",
        {"ccy": CCY, "yearMonth": YEAR_MONTH, "period": PERIOD_YTD},
        descriptor=f"id{CLIENT_ID}_{CCY.lower()}_{YEAR_MONTH}_p{PERIOD_YTD}",
    )

    print("\n=== /Performance ===")
    if isinstance(perf, list):
        print(f"rows: {len(perf)}")
        if perf:
            keys = sorted(perf[0].keys())
            print(f"fields ({len(keys)}): {', '.join(keys)}")
            # Populated-field coverage on the first 100 rows
            sample = perf[:100] if len(perf) > 100 else perf
            print(f"\nFirst row:")
            for k in keys:
                v = perf[0][k]
                vr = repr(v)
                if len(vr) > 80:
                    vr = vr[:77] + "..."
                print(f"  {k}: {vr}")

            print(f"\nField coverage on first {len(sample)} rows (% non-null):")
            for k in keys:
                non_null = sum(
                    1 for r in sample
                    if r.get(k) not in (None, "", 0) or (isinstance(r.get(k), bool))
                )
                # Reasonable focus: numeric/non-trivial fields
                pct = non_null / len(sample) * 100
                print(f"  {k:<30} {pct:>6.1f}%")

    # 2. Holdings with 12 months history
    holdings_hist = client.get_cached_or_fetch(
        f"Holdings/{CLIENT_ID}",
        {"ccy": CCY, "yearMonth": YEAR_MONTH, "historicalMonths": 12},
        descriptor=f"id{CLIENT_ID}_{CCY.lower()}_{YEAR_MONTH}_h12",
    )

    print("\n=== /Holdings with historicalMonths=12 ===")
    if isinstance(holdings_hist, list):
        print(f"total rows: {len(holdings_hist)}")
        # Group by date - what distinct as-of dates are represented?
        date_counts = Counter(h.get("date") for h in holdings_hist)
        print(f"\nDistinct as-of dates ({len(date_counts)}):")
        for d in sorted(date_counts):
            print(f"  {d}: {date_counts[d]} rows")

        # Sum marketValue per date — should show NAV trajectory
        by_date_mv = defaultdict(float)
        for h in holdings_hist:
            by_date_mv[h.get("date")] += h.get("marketValue") or 0
        print(f"\nNAV by date (sum of marketValue in {CCY}):")
        for d in sorted(by_date_mv):
            print(f"  {d}:  {by_date_mv[d]:>16,.2f}")

        # How many distinct securityIds across the whole year?
        # And how many appear in only some months (turnover indicator)?
        sec_by_date = defaultdict(set)
        for h in holdings_hist:
            sec = h.get("securityId")
            if sec is not None:
                sec_by_date[h.get("date")].add(sec)
        all_secs = set().union(*sec_by_date.values())
        print(f"\nTurnover: distinct securityIds across year = {len(all_secs)}")
        appearance = Counter()
        for sec in all_secs:
            appearance[sum(1 for d in sec_by_date if sec in sec_by_date[d])] += 1
        print("Histogram (how many months each security appears in):")
        for n_months in sorted(appearance):
            print(f"  appears in {n_months:>2} dates: {appearance[n_months]} securities")

    client.report()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
