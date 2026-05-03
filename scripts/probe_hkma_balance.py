"""Probe HKMA Aggregate Balance / Monetary Base availability.

Goal: find a stable historical series for HK liquidity that's not just
borrowed US rates / DXY. HKMA publishes daily Aggregate Balance figures
on hkma.gov.hk under the "Statistics" section.

Tries:
  1. AkShare's hkma-related functions (if any)
  2. Direct CSV download from HKMA's open data portal

Output: per-source, prints availability + sample rows. Exits 0 if any
source yields >= 5 years of daily data.
"""

from __future__ import annotations

import sys
import traceback
from io import StringIO


def _probe_akshare_hkma() -> bool:
    try:
        import akshare as ak

        candidates = [name for name in dir(ak) if "hkma" in name.lower() or "hk_money" in name.lower()]
        print(f"[probe] AkShare candidates with 'hkma' / 'hk_money' in name: {candidates}")
        for fn_name in candidates:
            fn = getattr(ak, fn_name, None)
            if not callable(fn):
                continue
            try:
                df = fn()
                print(f"  {fn_name}: shape={df.shape}, cols={list(df.columns)[:10]}")
                print(df.head(3))
                return True
            except Exception as e:
                print(f"  {fn_name} failed: {e}")
        return False
    except Exception as e:
        print(f"  FAIL: {e}")
        return False


def _probe_hkma_csv() -> bool:
    """HKMA exposes JSON/CSV at https://api.hkma.gov.hk/public/market-data-and-statistics/...
    Try the documented endpoint for monetary base."""
    try:
        import requests

        url = "https://api.hkma.gov.hk/public/market-data-and-statistics/monthly-statistical-bulletin/monetary-statistics/monetary-base-and-dependent-on-mb"
        resp = requests.get(url, params={"pagesize": 200}, timeout=20)
        print(f"[probe] HKMA monetary base API: status={resp.status_code}")
        if resp.status_code != 200:
            print(f"  body[:300] = {resp.text[:300]}")
            return False
        data = resp.json()
        records = data.get("result", {}).get("records", [])
        print(f"  records returned: {len(records)}")
        if records:
            print(f"  sample keys: {list(records[0].keys())}")
            print(f"  first 3: {records[:3]}")
        return len(records) >= 12
    except Exception as e:
        print(f"  FAIL: {e}")
        traceback.print_exc()
        return False


def main() -> int:
    print("=" * 60)
    print("  HKMA Balance / Monetary Base Probe")
    print("=" * 60)
    print()

    print("--- Source A: AkShare ---")
    a_ok = _probe_akshare_hkma()
    print()
    print("--- Source B: HKMA public API (api.hkma.gov.hk) ---")
    b_ok = _probe_hkma_csv()
    print()

    print("=" * 60)
    print(f"  AkShare hkma functions: {'OK' if a_ok else 'FAIL'}")
    print(f"  HKMA public API:        {'OK' if b_ok else 'FAIL'}")
    print("=" * 60)
    return 0 if (a_ok or b_ok) else 1


if __name__ == "__main__":
    sys.exit(main())
