"""Probe AkShare interfaces for CN/HK macro + flow + valuation data.

AkShare wraps many flaky upstream sources (EastMoney scrape, Sina, etc.) and
endpoints rename or break across versions. This script tests each candidate
once, reports what works, and is the source of truth for which interfaces
the akshare_* providers should call.

Usage:
    uv run python scripts/probe_akshare.py
"""
from __future__ import annotations

import sys
import traceback

import akshare as ak
import pandas as pd


def probe(name: str, fn, *, sample_rows: int = 3, expect_min_rows: int = 1):
    print(f"\n>>> {name}")
    try:
        df = fn()
    except Exception:
        print(f"    ✗ FAILED")
        traceback.print_exc(limit=2)
        return None
    if df is None:
        print("    ✗ returned None")
        return None
    if isinstance(df, pd.DataFrame):
        print(f"    ✓ rows={len(df):,} cols={list(df.columns)[:8]}{'...' if len(df.columns) > 8 else ''}")
        if len(df) < expect_min_rows:
            print(f"    ⚠ rows < expected {expect_min_rows}")
        if not df.empty:
            print(f"    head:\n{df.head(sample_rows).to_string()[:600]}")
        return df
    print(f"    ✓ type={type(df).__name__} value={df}")
    return df


# === CN VALUATION =========================================================
def cn_index_pe_csi300():
    # CSI300 historical PE - try multiple candidates
    return ak.stock_index_pe_lg(symbol="沪深300")


def cn_index_pb_csi300():
    return ak.stock_index_pb_lg(symbol="沪深300")


def cn_market_pe_a():
    return ak.stock_market_pe_lg(symbol="上证")


def cn_market_pb_a():
    return ak.stock_market_pb_lg(symbol="上证")


# === CN FLOW (北向) ========================================================
def cn_north_flow():
    """北向资金净流入。"""
    return ak.stock_hsgt_hist_em(symbol="北向资金")


def cn_north_summary():
    return ak.stock_hsgt_fund_flow_summary_em()


# === CN MACRO =============================================================
def cn_m2():
    return ak.macro_china_money_supply()


def cn_social_financing():
    return ak.macro_china_shrzgm()


# === HK FLOW (南向) =======================================================
def hk_south_flow():
    return ak.stock_hsgt_hist_em(symbol="南向资金")


# === AH PREMIUM ===========================================================
def ah_premium_index():
    """恒生 AH 股溢价指数，AkShare 接口名 stock_zh_ah_*."""
    return ak.stock_zh_ah_spot()


def ah_premium_index_alt():
    """另一种 AH 比价 / 溢价指数。"""
    return ak.hk_stock_index_value_em(symbol="HSAHP")


# === HK INDEX VALUATION ===================================================
def hk_hsi_pe():
    """HSI PE 历史值 — try multiple known interfaces."""
    return ak.stock_hk_index_value_em(symbol="HSI")


def hk_hsi_pe_alt():
    return ak.hk_stock_index_value_em(symbol="HSI")


def main() -> int:
    print(f"akshare {ak.__version__}")

    sections = {
        "CN VALUATION": [
            ("CSI300 PE history (stock_index_pe_lg)", cn_index_pe_csi300),
            ("CSI300 PB history (stock_index_pb_lg)", cn_index_pb_csi300),
            ("A-share market PE (stock_market_pe_lg)", cn_market_pe_a),
            ("A-share market PB (stock_market_pb_lg)", cn_market_pb_a),
        ],
        "CN FLOW": [
            ("Northbound flow (stock_hsgt_hist_em)", cn_north_flow),
            ("HSGT summary (stock_hsgt_fund_flow_summary_em)", cn_north_summary),
        ],
        "CN MACRO": [
            ("China M2 (macro_china_money_supply)", cn_m2),
            ("China Social Financing (macro_china_shrzgm)", cn_social_financing),
        ],
        "HK FLOW": [
            ("Southbound flow (stock_hsgt_hist_em)", hk_south_flow),
        ],
        "AH PREMIUM": [
            ("AH spot (stock_zh_ah_spot)", ah_premium_index),
            ("HSAHP via hk_stock_index_value_em", ah_premium_index_alt),
        ],
        "HK INDEX VALUATION": [
            ("HSI value via stock_hk_index_value_em", hk_hsi_pe),
            ("HSI value via hk_stock_index_value_em", hk_hsi_pe_alt),
        ],
    }
    for header, items in sections.items():
        print(f"\n{'='*8} {header} {'='*8}")
        for name, fn in items:
            probe(name, fn)
    return 0


if __name__ == "__main__":
    sys.exit(main())
