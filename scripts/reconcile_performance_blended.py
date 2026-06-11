"""Reconcile a blended ALL-ASSET return per entity from Masttro /Performance.

Pulls /Performance scoped to an entity node (investmentVehicle=entity) — the
whole response is that entity's all-asset book (listed + alts via SPVs under it).
Aggregates the period components into a blended modified-Dietz return + a
per-asset-class breakdown, to compare against the Masttro UI.
"""
from __future__ import annotations
import sys
from collections import defaultdict
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from client import MasttroClient

CLIENT = 7693
CCY = "USD"
YM = "202606"
ENTITIES = {
    "Dylan Dyne Irrevocable Trust": "102_93412",
    "Morgan Dyne Trust": "102_93413",
}
PERIODS = {4: "12M", 1: "YTD"}

def f(x):
    try: return float(str(x).replace(",", ""))
    except (TypeError, ValueError): return 0.0

def main() -> int:
    m = MasttroClient()
    for ename, node in ENTITIES.items():
        print(f"\n{'='*64}\n{ename}  (node {node})")
        for period, plabel in PERIODS.items():
            rows = m.get(f"Performance/{CLIENT}",
                         {"ccy": CCY, "yearMonth": YM, "period": period,
                          "investmentVehicle": node}) or []
            m.save_response(f"Performance/{CLIENT}", rows,
                            descriptor=f"blended_{node}_{CCY.lower()}_{YM}_p{period}")
            S = defaultdict(float); byc = defaultdict(lambda: defaultdict(float))
            for r in rows:
                mvi, mve = f(r.get("marketValueInitial")), f(r.get("marketValue"))
                fl = f(r.get("deposits")) + f(r.get("withdrawals")) + f(r.get("transferInOut"))
                S["i"] += mvi; S["e"] += mve; S["fl"] += fl; S["pl"] += f(r.get("totalPL"))
                ac = r.get("assetClass") or "(none)"
                byc[ac]["i"] += mvi; byc[ac]["e"] += mve; byc[ac]["fl"] += fl
            den = S["i"] + 0.5 * S["fl"]
            ret = (S["e"] - S["i"] - S["fl"]) / den * 100 if den else 0
            print(f"\n  --- {plabel} ({len(rows)} holdings) ---")
            print(f"    start {S['i']/1e6:8.2f}M  end {S['e']/1e6:8.2f}M  "
                  f"flows {S['fl']/1e6:7.2f}M  totalPL {S['pl']/1e6:7.2f}M  "
                  f"=> blended return {ret:6.2f}%")
            if period == 4:  # per-class only for 12M to keep it readable
                for ac, v in sorted(byc.items(), key=lambda x: -x[1]["e"]):
                    d = v["i"] + 0.5 * v["fl"]
                    rr = (v["e"] - v["i"] - v["fl"]) / d * 100 if d else 0
                    print(f"      {ac[:28]:30}{v['i']/1e6:8.2f} ->{v['e']/1e6:8.2f}M  {rr:7.2f}%")
    m.report()
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
