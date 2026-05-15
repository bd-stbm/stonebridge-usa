"""Apply supabase/schema.sql then any migrations in supabase/migrations/.

Idempotent — every DDL statement is guarded by IF NOT EXISTS or wrapped in a
DO block. Safe to re-run after schema changes.

Run once on a fresh Supabase project, then re-run whenever a new migration is
added to supabase/migrations/. Local-only; CI does not run this.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tracker.db import connect

ROOT = Path(__file__).resolve().parent.parent
SCHEMA_FILE = ROOT / "supabase" / "schema.sql"
MIGRATIONS_DIR = ROOT / "supabase" / "migrations"


def _exec_file(conn, path: Path) -> None:
    sql = path.read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def main() -> int:
    conn = connect()
    try:
        print(f"Applying {SCHEMA_FILE.relative_to(ROOT)}")
        _exec_file(conn, SCHEMA_FILE)
        print("  ok")

        if MIGRATIONS_DIR.exists():
            for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
                print(f"Applying {path.relative_to(ROOT)}")
                _exec_file(conn, path)
                print("  ok")
        else:
            print("(no migrations directory)")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
