"""Step 1 — Auth & connection sanity check.

Hits GET /Clients, saves the raw response, and prints a short summary
(client count, sample of names/ids, naming pattern).
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from client import MasttroClient  # noqa: E402


def main() -> int:
    client = MasttroClient()
    data = client.get("Clients")

    if data is None:  # dry-run
        client.report()
        return 0

    client.save_response("Clients", data, descriptor="initial")

    if not isinstance(data, list):
        print(f"\nUnexpected response shape: {type(data).__name__}")
        print(repr(data)[:500])
        client.report()
        return 1

    print(f"\nClients returned: {len(data)}")
    if data:
        first = data[0]
        print(f"Sample record keys: {sorted(first.keys()) if isinstance(first, dict) else type(first).__name__}")
        print("\nFirst 10 clients:")
        for c in data[:10]:
            if isinstance(c, dict):
                print(
                    f"  id={c.get('id')!r:>8}  "
                    f"clientId={c.get('clientId')!r:<20}  "
                    f"name={c.get('name')!r}"
                )
            else:
                print(f"  {c!r}")

    client.report()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
