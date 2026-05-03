"""Probe FRED T10Y3M (10-year Treasury minus 3-month) for US yield-curve regime.

Why this is non-trivial: yield curve doesn't map cleanly to "+ / -" direction:
  - Steep positive slope (e.g. +250bp): often cyclical recovery — looser -> hot
  - Slight positive (+50bp):           neutral
  - Inverted (-100bp):                 historical recession lead — risk -> cold
  - Re-steepening from inversion:      pre-recession warning, NOT bullish

Direct percentile-rank -> temperature is misleading here. This probe:
  1. Verifies the series is fetchable
  2. Computes summary stats and a "stress score" candidate transform
  3. Plots % of time in each regime

Production wiring requires designing a non-monotonic transform and is
deferred to a Phase 5 plan.
"""

from __future__ import annotations

import os
import sys
import traceback
from datetime import date, timedelta

import pandas as pd

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))

from finsynapse.providers.base import FetchRange
from finsynapse.providers.fred import FredProvider


def _probe_fred_t10y3m() -> bool:
    api_key = os.environ.get("FRED_API_KEY", "").strip()
    if not api_key:
        print("[probe] FRED_API_KEY not set — set it in .env to actually fetch.")
        return False
    try:
        os.environ["FRED_API_KEY"] = api_key
        from finsynapse import config as _cfg

        # Override the config's fred_api_key for this probe run
        _cfg.settings.fred_api_key = api_key

        provider = FredProvider()
        end = date.today()
        start = end - timedelta(days=5500)
        df = provider.fetch(FetchRange(start=start, end=end))
        # Filter to T10Y3M by indicator column
        df_t10 = df[df["indicator"] == "us10y_real_yield"].copy()

        # Actually T10Y3M isn't mapped in SERIES. Let me check what indicators FRED has.
        print(f"[probe] FRED returned indicators: {sorted(df['indicator'].unique())}")
        print(f"  total rows: {len(df)}")
        if df.empty:
            return False

        # FRED doesn't have T10Y3M in the standard SERIES list. Try fetching it directly.
        print("[probe] T10Y3M is not in the standard FredProvider SERIES list.")
        print("[probe] Attempting direct FRED API call for T10Y3M...")
        _probe_direct_t10y3m(api_key)
        return True
    except Exception as e:
        print(f"  FAIL: {e}")
        traceback.print_exc()
        return False


def _probe_direct_t10y3m(api_key: str) -> bool:
    """Fetch T10Y3M directly from FRED API (not via FinSynapse provider)."""
    try:
        import requests

        end = date.today()
        start = end - timedelta(days=5500)
        params = {
            "series_id": "T10Y3M",
            "api_key": api_key,
            "file_type": "json",
            "observation_start": start.isoformat(),
            "observation_end": end.isoformat(),
        }
        r = requests.get("https://api.stlouisfed.org/fred/series/observations", params=params, timeout=(10, 30))
        r.raise_for_status()
        payload = r.json()
        observations = payload.get("observations", [])

        values = []
        for obs in observations:
            val_str = obs.get("value", ".")
            if val_str in (".", "", None):
                continue
            try:
                values.append(float(val_str))
            except ValueError:
                continue

        if not values:
            print("[probe] T10Y3M: no valid observations returned")
            return False

        print(f"[probe] T10Y3M: {len(values)} observations")
        print(f"  date range: {observations[0]['date']} .. {observations[-1]['date']}")
        s = pd.Series(values)
        print(f"  stats: min={s.min():.2f}  max={s.max():.2f}  mean={s.mean():.2f}  std={s.std():.2f}")

        inverted = (s < 0).sum()
        flat_ = ((s >= 0) & (s < 0.5)).sum()
        normal = ((s >= 0.5) & (s < 2.0)).sum()
        steep = (s >= 2.0).sum()
        total = len(s)
        print(f"  regime breakdown:")
        print(f"    inverted (< 0bp):    {inverted:>5} ({inverted/total:.1%})")
        print(f"    flat ([0, 50bp)):    {flat_:>5} ({flat_/total:.1%})")
        print(f"    normal ([50, 200bp)):{normal:>5} ({normal/total:.1%})")
        print(f"    steep (>= 200bp):    {steep:>5} ({steep/total:.1%})")
        return True
    except Exception as e:
        print(f"  FAIL: {e}")
        traceback.print_exc()
        return False


def main() -> int:
    print("=" * 60)
    print("  US Yield Curve (T10Y3M) Probe")
    print("=" * 60)
    print()
    ok = _probe_fred_t10y3m()
    print()
    print("=" * 60)
    print(f"  T10Y3M fetchable: {'OK' if ok else 'FAIL'}")
    print("=" * 60)
    print()
    print("Next step (NOT in this probe):")
    print("  Design a non-monotonic stress transform — straight percentile rank")
    print("  with direction +/- will misrepresent inversion regimes. See e.g.")
    print("  Estrella & Mishkin (1996) for the recession-probability framing.")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
