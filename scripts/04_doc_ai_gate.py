"""Step 6 - Doc AI gate test.

Hits GET /DAI/Valuation/{id}?initUploadDate=YYYYMM&endUploadDate=YYYYMM
to check whether the API key has Doc AI module access.

Interpretation:
- 200 + non-empty list  -> Doc AI is provisioned and has processed statements in this window.
- 200 + empty list      -> module probably provisioned, but no documents in the window.
                           (Inconclusive on access -- could be either.)
- 403 / 401             -> module not provisioned on the API-generating user.
- Other HTTP error      -> investigate.

Uses id=7693 (Stonebridge container) because that's where the bulk of statements
should live. Window is the last two months (initUploadDate=YYYYMM of previous
month, endUploadDate=YYYYMM of current month).
"""

from __future__ import annotations

import sys
import urllib.error
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from client import MasttroClient  # noqa: E402

CLIENT_ID = 7693
INIT_UPLOAD_DATE = 202604  # April 2026
END_UPLOAD_DATE = 202605   # May 2026


def main() -> int:
    client = MasttroClient()
    path = f"DAI/Valuation/{CLIENT_ID}"
    descriptor = f"id{CLIENT_ID}_upload{INIT_UPLOAD_DATE}-{END_UPLOAD_DATE}"

    print(f"Testing Doc AI gate: GET /{path}?"
          f"initUploadDate={INIT_UPLOAD_DATE}&endUploadDate={END_UPLOAD_DATE}")

    try:
        data = client.get_cached_or_fetch(
            path,
            {"initUploadDate": INIT_UPLOAD_DATE, "endUploadDate": END_UPLOAD_DATE},
            descriptor=descriptor,
        )
    except urllib.error.HTTPError as e:
        print(f"\nVERDICT: Doc AI access DENIED (HTTP {e.code}).")
        print("Action: ask Masttro to provision the Doc AI module on the user "
              "that generated the API key.")
        client.report()
        return 0
    except urllib.error.URLError as e:
        print(f"\nNetwork error: {e.reason}")
        client.report()
        return 1

    print()
    if data is None:
        print("VERDICT: response was null/empty body. Treat as inconclusive; "
              "try a wider date window before concluding.")
    elif isinstance(data, list):
        print(f"VERDICT: Doc AI endpoint returned 200 with {len(data)} record(s).")
        if not data:
            print("  Empty list -- access is *likely* provisioned but no "
                  "statements were processed in this window.")
            print("  Suggested next step: widen the window or ask the user "
                  "to confirm Doc AI uploads exist.")
        else:
            print(f"  Access is provisioned and statements are flowing.")
            sample = data[0]
            if isinstance(sample, dict):
                print(f"\n  Field names ({len(sample)}): {', '.join(sorted(sample.keys()))}")
                print(f"\n  First record:")
                for k in sorted(sample.keys()):
                    v = sample[k]
                    v_repr = repr(v)
                    if len(v_repr) > 100:
                        v_repr = v_repr[:97] + "..."
                    print(f"    {k}: {v_repr}")

            # Quick aggregate: how many distinct funds / investors?
            funds = {row.get("fund_name") for row in data if isinstance(row, dict)}
            investors = {row.get("investor_name") for row in data if isinstance(row, dict)}
            doc_ids = [row.get("documentId") for row in data if isinstance(row, dict) and row.get("documentId")]
            print(f"\n  Distinct fund_name values: {len(funds)}")
            print(f"  Distinct investor_name values: {len(investors)}")
            print(f"  Rows with a documentId: {len(doc_ids)}")
    elif isinstance(data, dict):
        print(f"VERDICT: response is a dict (unexpected shape).")
        print(f"  Top-level keys: {sorted(data.keys())}")
    else:
        print(f"VERDICT: unexpected response type: {type(data).__name__}")

    client.report()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
