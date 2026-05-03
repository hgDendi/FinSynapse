"""Probe AkShare manufacturing PMI series for CN growth-regime factor.

Candidates:
  - macro_china_pmi (NBS official manufacturing PMI total)
  - macro_china_pmi_yearly (annualized)
  - macro_china_non_man_pmi (non-manufacturing — service sector)

Print columns, date range, sample rows. Exit 0 if at least one yields
>=10 years of monthly data.
"""

from __future__ import annotations

import sys
import traceback

CANDIDATES = [
    "macro_china_pmi",
    "macro_china_pmi_yearly",
    "macro_china_non_man_pmi",
    "macro_china_caixin_pmi",
]


def _probe(fn_name: str) -> bool:
    try:
        import akshare as ak

        fn = getattr(ak, fn_name, None)
        if fn is None:
            print(f"[probe] AkShare has no `{fn_name}`")
            return False
        df = fn()
        print(f"[probe] {fn_name}: shape={df.shape}, cols={list(df.columns)}")
        print(df.head(3))
        print(df.tail(3))
        # Heuristic: needs at least ~120 monthly rows for 10y of history
        return len(df) >= 120
    except Exception as e:
        print(f"  {fn_name} FAIL: {e}")
        traceback.print_exc()
        return False


def main() -> int:
    print("=" * 60)
    print("  CN Manufacturing PMI Probe (AkShare)")
    print("=" * 60)
    print()
    results = {fn: _probe(fn) for fn in CANDIDATES}
    print()
    print("=" * 60)
    for fn, ok in results.items():
        print(f"  {'OK' if ok else 'FAIL':<5}  {fn}")
    return 0 if any(results.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
