"""Probe AkShare interfaces for Phase B P0+P1 indicators.

Targets (per economist review on weights.yaml):
    CN sentiment   : 两融 (margin balance)
    CN liquidity   : DR007 / SHIBOR-1W
    HK valuation   : HSI PE
    HK sentiment   : AH 溢价指数 (Hang Seng AH Premium Index)
    HK liquidity   : HIBOR 1M

AkShare interfaces rename across versions; this probe is the source of truth
for which call to wire into the Phase B providers. It does NOT modify any
data; only prints what works and what doesn't.

Usage:
    uv run python scripts/probe_phase_b.py
"""

from __future__ import annotations

import sys
import traceback

import akshare as ak
import pandas as pd


def probe(name: str, fn, *, expect_min_rows: int = 1):
    print(f"\n>>> {name}")
    try:
        df = fn()
    except Exception:
        print("    ✗ FAILED")
        traceback.print_exc(limit=3)
        return None
    if df is None:
        print("    ✗ returned None")
        return None
    if isinstance(df, pd.DataFrame):
        print(f"    ✓ rows={len(df):,} cols={list(df.columns)[:10]}")
        if len(df) < expect_min_rows:
            print(f"    ⚠ rows < expected {expect_min_rows}")
        if not df.empty:
            print(f"    head:\n{df.head(3).to_string()[:600]}")
            print(f"    tail:\n{df.tail(2).to_string()[:400]}")
        return df
    print(f"    type={type(df).__name__}: {df}")
    return df


# ---- CN margin balance (两融) -----------------------------------------------
def cn_margin_sh_macro():
    """Shanghai-side margin total time series, macro-style monthly+daily mix."""
    return ak.macro_china_market_margin_sh()


def cn_margin_sz_macro():
    return ak.macro_china_market_margin_sz()


def cn_margin_sse_total():
    """SSE margin daily totals via stock_margin_sse (returns multi-year history)."""
    return ak.stock_margin_sse()


def cn_margin_szse_total():
    return ak.stock_margin_szse()


# ---- CN short-rate (DR007 / SHIBOR) -----------------------------------------
def cn_shibor_all():
    """SHIBOR all tenors history."""
    return ak.macro_china_shibor_all()


def cn_repo_rate():
    """DR007 / R007 via interbank repo. Try multiple candidates."""
    return ak.rate_interbank(market="上海银行同业拆借市场", symbol="Shibor人民币", indicator="1周")


def cn_dr007_em():
    """DR007 sometimes published under bond_china_close_return or money market."""
    return ak.bond_china_close_return(symbol="国债", period="7天")


# ---- HK valuation (HSI PE) ---------------------------------------------------
def hk_hsi_value_em():
    return ak.stock_hk_index_value_em(symbol="HSI")


def hk_hsi_value_em_lower():
    return ak.hk_stock_index_value_em(symbol="HSI")


def hk_hsi_index_daily():
    """Last-resort: HSI price history (no PE), would need EPS data separately."""
    return ak.stock_hk_index_daily_em(symbol="HSI")


# ---- HK AH premium ----------------------------------------------------------
def hk_ah_premium_index():
    """Hang Seng Stock Connect AH Premium Index (HSAHP)."""
    return ak.stock_hk_index_daily_em(symbol="HSAHP")


def hk_ah_premium_em():
    return ak.stock_zh_ah_spot()  # per-stock A/H spot — could aggregate to a market premium


def hk_ah_premium_value_em():
    return ak.stock_hk_index_value_em(symbol="HSAHP")


# ---- HK HIBOR ---------------------------------------------------------------
def hk_hibor_interbank():
    """HIBOR via the interbank rate router."""
    return ak.rate_interbank(market="香港银行同业拆借市场", symbol="Hibor港币", indicator="1月")


def hk_hibor_macro():
    """Some AkShare versions expose HIBOR under macro_china_hk_*."""
    # Placeholder name — try if exists:
    return ak.macro_china_hk_market_info()


def main() -> int:
    print(f"akshare {ak.__version__}")

    sections = {
        "CN MARGIN (两融)": [
            ("macro_china_market_margin_sh", cn_margin_sh_macro),
            ("macro_china_market_margin_sz", cn_margin_sz_macro),
            ("stock_margin_sse", cn_margin_sse_total),
            ("stock_margin_szse", cn_margin_szse_total),
        ],
        "CN SHORT-RATE (DR007/SHIBOR)": [
            ("macro_china_shibor_all", cn_shibor_all),
            ("rate_interbank Shibor 1周", cn_repo_rate),
            ("bond_china_close_return 7d", cn_dr007_em),
        ],
        "HK HSI PE": [
            ("stock_hk_index_value_em(HSI)", hk_hsi_value_em),
            ("hk_stock_index_value_em(HSI)", hk_hsi_value_em_lower),
            ("stock_hk_index_daily_em(HSI)", hk_hsi_index_daily),
        ],
        "HK AH PREMIUM": [
            ("stock_hk_index_daily_em(HSAHP)", hk_ah_premium_index),
            ("stock_zh_ah_spot", hk_ah_premium_em),
            ("stock_hk_index_value_em(HSAHP)", hk_ah_premium_value_em),
        ],
        "HK HIBOR": [
            ("rate_interbank Hibor 1M", hk_hibor_interbank),
            ("macro_china_hk_market_info", hk_hibor_macro),
        ],
    }
    for header, items in sections.items():
        print(f"\n{'=' * 10} {header} {'=' * 10}")
        for name, fn in items:
            probe(name, fn)
    return 0


if __name__ == "__main__":
    sys.exit(main())
