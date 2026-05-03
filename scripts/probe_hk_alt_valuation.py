"""Probe alternative HK valuation sources (Phase 3 Option C in
docs/_local/2026-05-03-factor-weight-improvement-plan.md).

Tests three candidates that, if stable, would let us reduce HK valuation's
100% dependence on EWH ETF dividend yield:

  1. yfinance ^HSI dividend yield field
  2. HSCEI / HSTECH ETF (2828.HK / 3033.HK) TTM dividend yield (mirrors EWH path)
  3. AkShare AH premium index historical series

Output: prints availability + 3 sample rows per candidate. Exits 0 if
ANY candidate yields stable historical data; 1 if all three fail.
"""

from __future__ import annotations

import sys
import traceback


def _probe_hsi_yfinance() -> bool:
    try:
        import yfinance as yf

        t = yf.Ticker("^HSI")
        info = t.info
        print("[probe] ^HSI info keys (first 20):", list(info.keys())[:20])
        for field in ("trailingAnnualDividendYield", "dividendYield", "yield"):
            print(f"  {field} = {info.get(field)}")
        div = t.dividends
        print(f"  dividends history: {len(div)} entries, range "
              f"{div.index.min() if len(div) else 'N/A'} .. {div.index.max() if len(div) else 'N/A'}")
        return len(div) > 0
    except Exception as e:
        print(f"  FAIL: {e}")
        traceback.print_exc()
        return False


def _probe_hk_etf_yield(ticker: str) -> bool:
    try:
        import yfinance as yf

        t = yf.Ticker(ticker)
        div = t.dividends
        hist = t.history(period="10y", interval="1mo")
        print(f"[probe] {ticker}: dividends={len(div)} entries, history rows={len(hist)}")
        if len(div):
            print(f"  div range: {div.index.min()} .. {div.index.max()}")
            print(f"  sample dividends:\n{div.tail(3)}")
        return len(div) >= 8 and len(hist) >= 60  # ~2y of monthly data + 8 dividends
    except Exception as e:
        print(f"  FAIL: {e}")
        return False


def _probe_ah_premium() -> bool:
    try:
        import akshare as ak

        for fn_name in ("stock_zh_ah_spread", "stock_zh_ah_name"):
            fn = getattr(ak, fn_name, None)
            if fn is None:
                print(f"[probe] AH premium: AkShare has no `{fn_name}` function")
                continue
            try:
                df = fn()
                print(f"[probe] AH premium via {fn_name}: shape={df.shape}, cols={list(df.columns)[:10]}")
                print(df.head(3))
                return len(df) > 0
            except Exception as e:
                print(f"  {fn_name} call failed: {e}")
        return False
    except Exception as e:
        print(f"  FAIL: {e}")
        return False


def main() -> int:
    print("=" * 60)
    print("  HK Alternative Valuation Probe")
    print("=" * 60)
    print()

    print("--- Candidate 1: yfinance ^HSI dividend yield ---")
    hsi_ok = _probe_hsi_yfinance()
    print()
    print("--- Candidate 2a: HSCEI ETF (2828.HK) ---")
    hscei_ok = _probe_hk_etf_yield("2828.HK")
    print()
    print("--- Candidate 2b: HSTECH ETF (3033.HK) ---")
    hstech_ok = _probe_hk_etf_yield("3033.HK")
    print()
    print("--- Candidate 3: AkShare AH premium ---")
    ah_ok = _probe_ah_premium()
    print()

    results = {
        "hsi_dividend_yield": hsi_ok,
        "hscei_etf_yield": hscei_ok,
        "hstech_etf_yield": hstech_ok,
        "ah_premium_history": ah_ok,
    }
    print("=" * 60)
    print("  SUMMARY")
    print("=" * 60)
    for name, ok in results.items():
        print(f"  {'OK' if ok else 'FAIL':<5}  {name}")
    print()
    if any(results.values()):
        print("\u2713 At least one candidate is viable. Document findings in")
        print("  docs/_local/2026-05-03-p2-probe-results.md before proceeding.")
        return 0
    print("\u2717 All candidates failed. HK stays on EWH-only valuation (Option A).")
    return 1


if __name__ == "__main__":
    sys.exit(main())
