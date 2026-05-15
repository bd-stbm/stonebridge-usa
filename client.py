"""Thin Masttro API client for exploration.

Per CLAUDE.md:
- HTTP Basic auth via env vars MASTTRO_API_KEY / MASTTRO_API_SECRET
- Every script uses this wrapper; no direct urllib/requests in scripts
- Every raw response saved to responses/<endpoint>_<timestamp>_<descriptor>.json
- Logs call count, response time (ms), and response size (bytes) per request
- Dry-run mode via MASTTRO_DRY_RUN=1 prints the URL without calling
- Stdlib-only (no external deps)
"""

from __future__ import annotations

import base64
import datetime as dt
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

BASE_URL = "https://dfo.masttro.com/api/"
PROJECT_ROOT = Path(__file__).resolve().parent
RESPONSES_DIR = PROJECT_ROOT / "responses"
ENV_FILE = PROJECT_ROOT / ".env.local"


def _load_env_file(path: Path) -> None:
    """Minimal .env loader: KEY=VALUE per line, no quotes, no expansion."""
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip()
        # Only set if not already in env (env wins over file)
        os.environ.setdefault(key, value)


_load_env_file(ENV_FILE)


class MasttroClient:
    def __init__(self) -> None:
        key = os.environ.get("MASTTRO_API_KEY")
        secret = os.environ.get("MASTTRO_API_SECRET")
        if not key or not secret:
            raise RuntimeError(
                "MASTTRO_API_KEY and MASTTRO_API_SECRET must be set "
                "(check .env.local)"
            )
        token = base64.b64encode(f"{key}:{secret}".encode("utf-8")).decode("ascii")
        self._auth_header = f"Basic {token}"
        self.call_count = 0
        self.dry_run = os.environ.get("MASTTRO_DRY_RUN", "").lower() in {"1", "true", "yes"}

    @staticmethod
    def _build_url(path: str, params: dict | None) -> str:
        path = path.lstrip("/")
        url = urllib.parse.urljoin(BASE_URL, path)
        if params:
            # Drop None values; stringify the rest.
            clean = {k: str(v) for k, v in params.items() if v is not None}
            if clean:
                url = f"{url}?{urllib.parse.urlencode(clean)}"
        return url

    def get(self, path: str, params: dict | None = None, timeout: int = 120) -> dict | list:
        """GET {BASE_URL}{path}?<params>. Returns parsed JSON. Logs metrics.

        Raises on non-2xx with a short error including status and body excerpt.
        In dry-run mode, prints the URL and returns None without calling.
        """
        url = self._build_url(path, params)
        if self.dry_run:
            print(f"[dry-run] GET {url}")
            return None  # type: ignore[return-value]

        req = urllib.request.Request(
            url,
            headers={
                "Authorization": self._auth_header,
                "Accept": "application/json",
            },
            method="GET",
        )
        self.call_count += 1
        started = time.perf_counter()
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                body = resp.read()
                status = resp.status
        except urllib.error.HTTPError as e:
            elapsed_ms = int((time.perf_counter() - started) * 1000)
            err_body = e.read().decode("utf-8", errors="replace")[:500]
            print(
                f"[call #{self.call_count}] GET {path} -> HTTP {e.code} "
                f"({elapsed_ms} ms): {err_body}",
                file=sys.stderr,
            )
            raise
        except urllib.error.URLError as e:
            elapsed_ms = int((time.perf_counter() - started) * 1000)
            print(
                f"[call #{self.call_count}] GET {path} -> URLError "
                f"({elapsed_ms} ms): {e.reason}",
                file=sys.stderr,
            )
            raise

        elapsed_ms = int((time.perf_counter() - started) * 1000)
        size_bytes = len(body)
        print(
            f"[call #{self.call_count}] GET {path} -> {status} "
            f"({elapsed_ms} ms, {size_bytes} bytes)"
        )
        # Empty body is valid JSON-null for our purposes.
        if not body:
            return None  # type: ignore[return-value]
        return json.loads(body.decode("utf-8"))

    @staticmethod
    def save_response(endpoint: str, data, descriptor: str = "") -> Path:
        """Pretty-print to responses/<endpoint>_<YYYYMMDD-HHMMSS>_<descriptor>.json.

        endpoint slashes are replaced with '-' for filesystem safety.
        """
        RESPONSES_DIR.mkdir(exist_ok=True)
        safe_endpoint = re.sub(r"[^A-Za-z0-9._-]+", "-", endpoint.strip("/"))
        safe_descriptor = re.sub(r"[^A-Za-z0-9._-]+", "-", descriptor).strip("-")
        ts = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
        parts = [safe_endpoint, ts]
        if safe_descriptor:
            parts.append(safe_descriptor)
        path = RESPONSES_DIR / ("_".join(parts) + ".json")
        path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"  saved -> {path.relative_to(PROJECT_ROOT)}")
        return path

    def get_cached_or_fetch(
        self,
        path: str,
        params: dict | None = None,
        descriptor: str = "",
        timeout: int = 120,
    ):
        """Honour CLAUDE.md's "no re-hit same params if a saved response exists" rule.

        Looks for any responses/<endpoint-slug>_*_<descriptor>.json file.
        If found, loads from disk (no API call). Otherwise fetches via self.get
        and saves with self.save_response.

        The descriptor must uniquely identify the parameter set you care about
        (e.g. "id7693_aud_202605"). It is the caller's contract — this method
        does not parse params back out of filenames.
        """
        safe_endpoint = re.sub(r"[^A-Za-z0-9._-]+", "-", path.strip("/"))
        safe_descriptor = re.sub(r"[^A-Za-z0-9._-]+", "-", descriptor).strip("-")
        pattern = f"{safe_endpoint}_*"
        if safe_descriptor:
            pattern += f"_{safe_descriptor}"
        pattern += ".json"
        RESPONSES_DIR.mkdir(exist_ok=True)
        matches = sorted(RESPONSES_DIR.glob(pattern))
        if matches:
            cached = matches[-1]  # most recent if multiple
            print(f"[cached]  {path} ({descriptor}) -> {cached.relative_to(PROJECT_ROOT)}")
            return json.loads(cached.read_text(encoding="utf-8"))
        data = self.get(path, params, timeout=timeout)
        if data is not None:
            self.save_response(path, data, descriptor=descriptor)
        return data

    def report(self) -> None:
        """Print total call count for this session (call at script end)."""
        print(f"\nTotal API calls this session: {self.call_count}")
