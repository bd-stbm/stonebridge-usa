"""Postgres / Supabase connection helper.

Reads SUPABASE_DB_URL from environment (or .env.local for local runs). In CI
the secret is injected by GitHub Actions. Use the Supavisor pooler URL
(port 6543) for short-lived workloads — it survives serverless connection churn.
"""

from __future__ import annotations

import os
import time
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

ENV_FILE = Path(__file__).resolve().parent.parent / ".env.local"

# Fail fast instead of hanging on the OS default when the pooler is saturated,
# and retry a couple of times to ride out transient Supavisor hiccups.
CONNECT_TIMEOUT = int(os.environ.get("DB_CONNECT_TIMEOUT", "15"))
CONNECT_ATTEMPTS = int(os.environ.get("DB_CONNECT_ATTEMPTS", "3"))


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
    # Prefer the pooler URL — direct connections are IPv6-only on new
    # Supabase projects and fail from many local networks (incl. Windows
    # without IPv6 routing). SUPABASE_DB_URL kept as fallback.
    url = os.environ.get("SUPABASE_POOL_URL") or os.environ.get("SUPABASE_DB_URL")
    if not url:
        raise RuntimeError(
            "Set SUPABASE_POOL_URL (preferred) or SUPABASE_DB_URL in .env.local "
            "or as a GitHub Actions secret."
        )
    # prepare_threshold=None disables client-side prepared statements — REQUIRED
    # on the transaction-mode pooler (port 6543), which multiplexes connections
    # and otherwise errors with "prepared statement already exists". Harmless on
    # session mode (5432).
    last_exc: Exception | None = None
    for attempt in range(1, CONNECT_ATTEMPTS + 1):
        try:
            return psycopg.connect(
                url,
                row_factory=dict_row,
                autocommit=False,
                connect_timeout=CONNECT_TIMEOUT,
                prepare_threshold=None,
            )
        except psycopg.OperationalError as exc:  # timeouts, transient pooler errors
            last_exc = exc
            if attempt < CONNECT_ATTEMPTS:
                time.sleep(2 * attempt)  # 2s, 4s backoff
    raise last_exc  # type: ignore[misc]


def log_sync(conn, sync_type: str, scope: str | None,
             description: str, rows: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO sync_log (sync_type, scope, description, rows_affected)
               VALUES (%s, %s, %s, %s)""",
            (sync_type, scope, description, rows),
        )
    conn.commit()
