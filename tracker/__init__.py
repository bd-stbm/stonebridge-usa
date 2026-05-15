"""Portfolio tracker data layer.

SQLite-backed. Postgres-compatible SQL throughout (so migration to Supabase
is a straight pg_dump-style move when the schema settles).

Modules:
- schema.py   — DDL for entity / security / position_snapshot / transaction_log /
                pricing_refresh tables.
- ingest.py   — Load saved Masttro JSON responses into the DB. Filters to
                directly-held investment accounts (public positions only).
- enrich.py   — yfinance + OpenFIGI pricing refresh.
- api.py      — Query functions returning pandas DataFrames.
- compute.py  — Performance calcs (TWR / IRR / Dietz return).
"""

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RESPONSES_DIR = PROJECT_ROOT / "responses"
DEFAULT_DB_PATH = DATA_DIR / "tracker.db"
