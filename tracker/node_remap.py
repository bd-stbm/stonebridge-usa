"""Fold the AU broker-migration direct-equity nodes into their IBKR account.

Background
----------
Jamindy Trust and The Saulos Family Trust hold a set of legacy "direct
equity" GWM nodes (one node per listed security) created during a broker
migration to preserve pre-migration history. The live shares sit in each
trust's IBKR account, and these nodes are frozen (future trades flow through
IBKR). We don't want each security surfaced as its own "account", so their
Holdings/Transactions are folded into the trust's IBKR account node.

Applied in the ingest path (scripts/sync_masttro_daily.py and the one-off
backfill) BEFORE the canonical-account filter: any payload row whose `nodeId`
is a key below is rewritten to the mapped IBKR account node. The source nodes
are frozen so this mapping is stable; it was derived from
scripts/backfill_au_direct_equity_nodes.py's discovery pass (canonical,
is_account=false nodes under the two trusts that carry a securityId).
"""

from __future__ import annotations

# equity-node nodeId -> IBKR account nodeId
DIRECT_EQUITY_NODE_REMAP: dict[str, str] = {
    # Jamindy Trust -> Jamindy IBKR (101_235818)
    "102_269327": "101_235818",  # AIRBNB INC-CLASS A
    "102_110470": "101_235818",  # Alphabet
    "102_98320": "101_235818",   # Amazon
    "102_348456": "101_235818",  # ASML
    "102_98315": "101_235818",   # CSL
    "102_434991": "101_235818",  # Duolingo Inc
    "102_101503": "101_235818",  # Meta Platforms
    "102_348455": "101_235818",  # Nike
    "102_434992": "101_235818",  # Salesforce
    "102_98317": "101_235818",   # Sonic Healthcare
    "102_98318": "101_235818",   # Telstra
    "102_434990": "101_235818",  # Wise PLC
    # The Saulos Family Trust -> Saulos FT - IBKR (101_235421)
    "102_98020": "101_235421",   # Advanced Micro Devices
    "102_98019": "101_235421",   # Airbnb
    "102_263680": "101_235421",  # Betashares Australia 200 ETF
    "102_385861": "101_235421",  # BETASHARES AUSTRALIA QUALITY
    "102_263681": "101_235421",  # Betashares FTSE 100 ETF
    "102_376450": "101_235421",  # Betashares Global Defence ETF
    "102_263682": "101_235421",  # Betashares Nasdaq 100 ETF
    "102_450186": "101_235421",  # Betashares Wealth Builder Global Shares Geared ETF
    "102_397287": "101_235421",  # Health Care Select Sector SPDR Fund
    "102_263683": "101_235421",  # iShares MSCI Emerging Markets
    "102_263684": "101_235421",  # iShares S&P 500 ETF AUD
    "102_396959": "101_235421",  # Light & Wonder
    "102_395697": "101_235421",  # Salesforce Inc
    "102_98289": "101_235421",   # Saulos Tesla
    "102_385862": "101_235421",  # Vaneck Asx Midcap
    "102_325171": "101_235421",  # Vaneck Bitcoin ETF
    "102_263685": "101_235421",  # Vanguard FTSE Europe Shares ET
}


def apply_node_remap(payload, remap: dict[str, str] = DIRECT_EQUITY_NODE_REMAP):
    """Rewrite each row's `nodeId` per `remap`, in place. Rows whose nodeId
    isn't mapped pass through unchanged. Returns the same payload list.

    Must run BEFORE upsert_positions / upsert_transactions, which key the
    account_node_id off `nodeId` and filter to canonical accounts — after
    remap the rows land on the (canonical) IBKR account.
    """
    for row in payload or []:
        nid = row.get("nodeId")
        if nid in remap:
            row["nodeId"] = remap[nid]
    return payload
