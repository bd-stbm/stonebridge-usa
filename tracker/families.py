"""Registry of Masttro sub-client subtrees to sync.

Each entry maps to a sub-client GWM nodeId directly under the tenant root.
Add Markiles / Miller (and AU families later) by appending to this list —
the sync workflows iterate over it.
"""

FAMILIES = [
    {
        "node_id": "102_93356",      # GWM nodeId for Dyne Family US
        "label": "Dyne Family US",
        "client_id": 7693,           # /Clients id (parent tenant)
        "reporting_ccy": "USD",
    },
    # {"node_id": "102_93361", "label": "Markiles Family", "client_id": 7693, "reporting_ccy": "USD"},
    # {"node_id": "102_93360", "label": "Miller Family",   "client_id": 7693, "reporting_ccy": "USD"},
]
