"""Static JSON API endpoints published alongside the dashboard.

Output layout under `dist/api/`:

  manifest.json              schema version + endpoint inventory + asof
  temperature_latest.json    per-market latest overall + sub-temps
  temperature_history.json.gz long time series (gzipped, full history)
  indicators_latest.json     all underlying factor latest values + pct
  divergence_latest.json     active divergence signals (last 90 days)

Consumers:
  - external AI agents wanting the latest reading without HTML scraping
  - downstream tools / notebooks that want a stable JSON contract

Schema versioning: bump `API_SCHEMA_VERSION` whenever a field is removed
or its meaning changes. Adding new fields is non-breaking.
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path
from typing import Any

import pandas as pd

from finsynapse.dashboard.data import MARKETS, DashboardData

API_SCHEMA_VERSION = "1.0.0"

ENDPOINT_DESCRIPTIONS: dict[str, str] = {
    "manifest.json": "Schema version, asof date, and inventory of all endpoints.",
    "temperature_latest.json": "Per-market latest temperature: overall + valuation/sentiment/liquidity sub-temps + 1-week change attribution.",
    "temperature_history.json.gz": "Per-market full daily history of overall + sub-temps. Gzipped JSON.",
    "indicators_latest.json": "All underlying factor latest values and rolling percentiles (5y/10y).",
    "divergence_latest.json": "Active divergence signals from the last 90 days, sorted by strength.",
}


def build_manifest(asof: str, endpoints: list[str]) -> dict[str, Any]:
    """Assemble the manifest payload describing what's published and when."""
    return {
        "schema_version": API_SCHEMA_VERSION,
        "asof": asof,
        "generated_at_utc": pd.Timestamp.now("UTC").strftime("%Y-%m-%dT%H:%M:%SZ"),
        "endpoints": {
            name: {"path": f"api/{name}", "description": ENDPOINT_DESCRIPTIONS.get(name, "")} for name in endpoints
        },
    }


def write_all(data: DashboardData, out_dir: Path) -> list[Path]:
    """Write every API file. Returns the list of written paths.

    Caller is responsible for creating `out_dir`. Files land in `out_dir/api/`.
    Empty `data.temperature` is treated as a no-op (returns []).
    """
    api_dir = out_dir / "api"
    api_dir.mkdir(parents=True, exist_ok=True)
    if data.temperature.empty:
        return []
    written: list[Path] = []
    # Subsequent tasks fill in the actual builders; manifest must be last
    # so the asof reflects what was actually written.
    asof = pd.to_datetime(data.temperature["date"].max()).strftime("%Y-%m-%d")
    manifest = build_manifest(asof=asof, endpoints=["manifest.json"])
    p = api_dir / "manifest.json"
    p.write_text(json.dumps(manifest, indent=2, ensure_ascii=False))
    written.append(p)
    return written
