"""Fetch CNN Fear & Greed Index historical data.

CNN publishes daily 0-100 sentiment readings at:
  https://production.dataviz.cnn.io/index/fearandgreed/graphdata

The API returns timestamped (epoch ms) entries in 'fear_and_greed_historical'.data[].
Each entry: {"x": epoch_ms, "y": 0-100, "rating": "extreme fear"|"fear"|"neutral"|"greed"|"extreme greed"}

Usage:
    uv run python scripts/fetch_external_anchors.py
    uv run python scripts/fetch_external_anchors.py --output /tmp/cnn_fg.csv

Output:
    scripts/cnn_fear_greed.csv — date | value | rating
"""

from __future__ import annotations

import json
import sys
import urllib.request
from datetime import UTC, datetime
from pathlib import Path

CNN_URL = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "application/json",
    "Origin": "https://edition.cnn.com",
    "Referer": "https://edition.cnn.com/",
}
SCRIPTS_DIR = Path(__file__).parent


def _epoch_to_date(epoch_ms: float) -> str:
    dt = datetime.fromtimestamp(epoch_ms / 1000.0, tz=UTC)
    return dt.date().isoformat()


def fetch_cnn_fear_greed(output_path: Path | None = None) -> Path:
    """Fetch CNN F&G data and write to CSV. Returns output path."""
    req = urllib.request.Request(CNN_URL, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=15) as r:
        if r.status != 200:
            raise RuntimeError(f"CNN F&G fetch failed: HTTP {r.status}")
        data = json.loads(r.read())

    historical = data.get("fear_and_greed_historical", {})
    entries = historical.get("data", [])
    if not entries:
        raise RuntimeError("No historical data in CNN F&G response")

    rows = []
    for entry in entries:
        rows.append(
            {
                "date": _epoch_to_date(entry["x"]),
                "value": float(entry["y"]),
                "rating": entry.get("rating", ""),
            }
        )

    output_path = output_path or SCRIPTS_DIR / "cnn_fear_greed.csv"
    import csv

    with output_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["date", "value", "rating"])
        writer.writeheader()
        writer.writerows(rows)

    # Also fetch current reading
    fg = data.get("fear_and_greed", {})
    current = fg.get("now", {}).get("value", "")
    rating = fg.get("now", {}).get("rating", "")
    print(f"[cnn] {len(rows):,} historical entries written -> {output_path}")
    if current:
        print(f"[cnn] current reading: {current} ({rating})")
    date_range = f"{rows[0]['date']} .. {rows[-1]['date']}" if rows else "empty"
    print(f"[cnn] date range: {date_range}")
    return output_path


if __name__ == "__main__":
    out = Path(sys.argv[2]) if len(sys.argv) >= 3 and sys.argv[1] == "--output" else None
    fetch_cnn_fear_greed(out)
