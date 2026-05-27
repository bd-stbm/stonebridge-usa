"""Registry of Masttro sub-client subtrees to sync.

Each entry maps to a sub-client GWM nodeId directly under the tenant
root. `reporting_ccy` drives the `ccy=` query parameter passed to every
Holdings / Transactions / cef call for the family, and lands on every
position_snapshot / transaction_log row written for it. Different
currencies across families coexist cleanly because the dashboard
scopes every read by sub_client_alias and pulls reporting_ccy off the
rows it loaded.
"""

FAMILIES = [
    {
        "node_id": "102_93356",      # GWM nodeId for Dyne Family US
        "label": "Dyne Family US",
        "client_id": 7693,           # /Clients id (parent tenant)
        "reporting_ccy": "USD",
    },
    {
        "node_id": "102_93361",
        "label": "Markiles Family",
        "client_id": 7693,
        "reporting_ccy": "USD",
    },
    {
        "node_id": "102_93360",
        "label": "Miller Family",
        "client_id": 7693,
        "reporting_ccy": "USD",
    },
    {
        "node_id": "102_93362",      # GWM nodeId for Dyne Family (AU)
        "label": "Dyne Family (AU)",
        "client_id": 7693,
        "reporting_ccy": "AUD",
    },
    {
        "node_id": "102_93363",      # GWM nodeId for Bermeister Family
        "label": "Bermeister Family",
        "client_id": 7693,
        "reporting_ccy": "AUD",
    },
]
