"""Postgres / Supabase connection helper.

Reads SUPABASE_DB_URL from environment (or .env.local for local runs). In CI
the secret is injected by GitHub Actions. Use the Supavisor pooler URL
(port 6543) for short-lived workloads — it survives serverless connection churn.
"""

from __future__ import annotations

import os
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

ENV_FILE = Path(__file__).resolve().parent.parent / ".env.local"


def _load_env_file(path: Path = ENV_FILE) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        os.environ.setdefault(k.strip(), v.strip())


_load_env_file()


def connect():
    url = os.environ.get("SUPABASE_DB_URL")
    if not url:
        raise RuntimeError(
            "SUPABASE_DB_URL is not set. Add it to .env.local or as a GitHub Actions secret."
        )
    return psycopg.connect(url, row_factory=dict_row, autocommit=False)


def log_sync(conn, sync_type: str, scope: str | None,
             description: str, rows: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO sync_log (sync_type, scope, description, rows_affected)
               VALUES (%s, %s, %s, %s)""",
            (sync_type, scope, description, rows),
        )
    conn.commit()
