"""Walk the GWM tree to figure out how to map Holdings rows to real sub-clients.

Pure offline analysis - reads the saved JSON in responses/, makes NO API calls.

Goals:
1. Describe the shape of the GWM tree (depth, branching, root info).
2. Identify the "sub-client" level (almost certainly the direct children of root).
3. For each direct child of root, count and total the Holdings that resolve up to it.
4. Show a worked example: pick one Holding and print the full parent chain to root.
"""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RESPONSES = PROJECT_ROOT / "responses"


def latest(pattern: str) -> Path:
    matches = sorted(RESPONSES.glob(pattern))
    if not matches:
        raise FileNotFoundError(f"No file matched {pattern} in {RESPONSES}")
    return matches[-1]


def load(pattern: str):
    p = latest(pattern)
    print(f"loaded: {p.relative_to(PROJECT_ROOT)}")
    return json.loads(p.read_text(encoding="utf-8"))


def main() -> int:
    gwm = load("GWM-7693_*.json")
    holdings = load("Holdings-7693_*.json")

    # --- Build GWM index -----------------------------------------------------
    nodes = {row["nodeId"]: row for row in gwm}
    parent_of = {nid: row.get("parentNodeId") for nid, row in nodes.items()}

    # Identify root(s). CLAUDE.md / observation: root has parentNodeId == '_'.
    roots = [nid for nid, pid in parent_of.items() if pid == "_" or pid is None]
    print(f"\nGWM nodes: {len(nodes)}")
    print(f"Roots: {roots}")

    # --- Direct children of root = candidate sub-clients ---------------------
    root = roots[0]
    direct_children = [nid for nid, pid in parent_of.items() if pid == root]
    print(f"\nDirect children of root '{root}' (likely real sub-clients): {len(direct_children)}")
    print("First 25:")
    for nid in direct_children[:25]:
        n = nodes[nid]
        print(
            f"  {nid:<20}  alias={n.get('alias')!r:<40} "
            f"bank={n.get('bankBroker')!r:<20} "
            f"valuation={n.get('valuation')}"
        )

    # --- Walk from a holding up to root --------------------------------------
    # Pick a non-trivial holding (skip rows where parentNodeId is the root itself).
    sample = None
    for row in holdings:
        if row.get("parentNodeId") and row.get("parentNodeId") != root:
            sample = row
            break
    if sample is None:
        sample = holdings[0]

    print(f"\nWorked example -- one Holdings row:")
    print(f"  assetName       = {sample.get('assetName')!r}")
    print(f"  marketValue AUD = {sample.get('marketValue')}")
    print(f"  nodeId          = {sample.get('nodeId')!r}")
    print(f"  parentNodeId    = {sample.get('parentNodeId')!r}")

    def walk_to_root(start: str) -> list[str]:
        chain = [start]
        cur = start
        for _ in range(50):  # safety cap
            pid = parent_of.get(cur)
            if pid in (None, "_"):
                break
            chain.append(pid)
            cur = pid
        return chain

    chain = walk_to_root(sample["parentNodeId"])
    print(f"\n  Parent chain (leaf-side parent -> ... -> root):")
    for nid in chain:
        n = nodes.get(nid)
        if n is None:
            print(f"    {nid:<20}  (NOT FOUND IN GWM)")
            continue
        print(
            f"    {nid:<20}  alias={n.get('alias')!r:<40} "
            f"name={n.get('name')!r:<30} "
            f"acct={n.get('accountNumber')!r}"
        )

    # The "real sub-client" is the second-to-last entry in the chain (the
    # direct child of root reached by walking up).
    sub_client_nid = chain[-1] if chain[-1] in direct_children else (
        chain[-2] if len(chain) > 1 and chain[-2] in direct_children else None
    )
    if sub_client_nid:
        sc = nodes[sub_client_nid]
        print(f"\n  Resolved sub-client: nodeId={sub_client_nid!r}  alias={sc.get('alias')!r}")

    # --- For every holding, resolve to its sub-client and aggregate ----------
    # Cache walks for speed.
    direct_children_set = set(direct_children)

    def resolve_subclient(nid: str | None) -> str | None:
        cur = nid
        for _ in range(60):
            if cur is None:
                return None
            if cur in direct_children_set:
                return cur
            cur = parent_of.get(cur)
            if cur in (None, "_"):
                return None
        return None

    bucket_count: Counter = Counter()
    bucket_mv: defaultdict[str | None, float] = defaultdict(float)
    unresolved = 0
    for row in holdings:
        # Walk from the Holding's parentNodeId (the leaf account/wrapper node).
        sub = resolve_subclient(row.get("parentNodeId")) or resolve_subclient(row.get("nodeId"))
        if sub is None:
            unresolved += 1
            continue
        bucket_count[sub] += 1
        bucket_mv[sub] += row.get("marketValue") or 0

    print(f"\nHoldings resolved to a direct-child sub-client: "
          f"{sum(bucket_count.values())} / {len(holdings)}  (unresolved={unresolved})")

    print(f"\nHoldings rollup by sub-client (top 25 by marketValue AUD):")
    rows = sorted(bucket_mv.items(), key=lambda kv: kv[1], reverse=True)
    print(f"  {'nodeId':<20}  {'alias':<40}  {'rows':>8}  {'marketValue AUD':>20}")
    for nid, mv in rows[:25]:
        n = nodes[nid]
        print(
            f"  {nid:<20}  {str(n.get('alias'))[:38]:<40}  "
            f"{bucket_count[nid]:>8}  {mv:>20,.2f}"
        )

    grand_total = sum(bucket_mv.values())
    print(f"\nResolved Holdings total marketValue AUD: {grand_total:,.2f}")
    print(f"(Compare to Step 2 total: 1,090,384,457.50)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
