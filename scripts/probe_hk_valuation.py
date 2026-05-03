"""Probe HK native valuation data sources.

Tests akshare interfaces for HSI PE, PB, dividend yield, and index-level data.
This probe establishes data source reliability before wiring into production
(Phase 3 of factor-weight improvement plan).

Usage:
    uv run python scripts/probe_hk_valuation.py
"""

from __future__ import annotations

import sys
import traceback

import akshare as ak
import pandas as pd


def probe(name: str, fn, *, expect_min_rows: int = 100):
    print(f"\n>>> {name}")
    try:
        df = fn()
    except Exception:
        print("    \u2717 FAILED")
        traceback.print_exc(limit=3)
        return None
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        print("    \u2717 returned empty/None")
        return None
    if isinstance(df, pd.DataFrame):
        print(f"    \u2713 rows={len(df):,}  cols={list(df.columns)[:12]}")
        if len(df) < expect_min_rows:
            print(f"    \u26a0 rows < expected {expect_min_rows}")
        date_cols = [c for c in df.columns if "date" in c.lower() or "\u65e5\u671f" in c.lower()]
        if date_cols:
            d = pd.to_datetime(df[date_cols[0]], errors="coerce")
            valid_d = d.dropna()
            if not valid_d.empty:
                print(f"    date range: {valid_d.min().date()} .. {valid_d.max().date()}")
                print(f"    missing rate (date): {(1 - len(valid_d) / len(df)):.1%}")
        numeric_cols = df.select_dtypes(include=["number"]).columns[:5]
        if not numeric_cols.empty:
            for col in numeric_cols[:3]:
                s = df[col].dropna()
                if not s.empty:
                    print(f"    {col}: nan_rate={1 - len(s) / len(df):.1%}  min={s.min():.4g}  max={s.max():.4g}")
            print(f"    head:\n{df.head(3).to_string()[:800]}")
            print(f"    tail:\n{df.tail(2).to_string()[:600]}")
        return df
    print(f"    type={type(df).__name__}: {df}")
    return df


# --- HSI PE / PB / Dividend Yield via stock_hk_index_value_em ---


def hsi_value_em():
    return ak.stock_hk_index_value_em(symbol="HSI")


def hscei_value_em():
    return ak.stock_hk_index_value_em(symbol="HSCEI")


def hstech_value_em():
    return ak.stock_hk_index_value_em(symbol="HSTECH")


# --- Alternative akshare entry points ---
def hsi_value_em_lower():
    return ak.hk_stock_index_value_em(symbol="HSI")


# --- HSI price history (for dividend yield derivation if needed) ---
def hsi_daily_em():
    return ak.stock_hk_index_daily_em(symbol="HSI")


def hscei_daily_em():
    return ak.stock_hk_index_daily_em(symbol="HSCEI")


def hstech_daily_em():
    return ak.stock_hk_index_daily_em(symbol="HSTECH")


# --- HK stock connect individual valuation (alternative route) ---
def hk_spot_em():
    """HK spot data — may carry PE for individual stocks."""
    return ak.stock_hk_spot_em()


def main() -> int:
    print(f"akshare {ak.__version__}")

    sections = {
        "HSI Valuation (PE/PB/DY)": [
            ("stock_hk_index_value_em HSI", hsi_value_em),
            ("hk_stock_index_value_em HSI", hsi_value_em_lower),
        ],
        "HSCEI Valuation": [
            ("stock_hk_index_value_em HSCEI", hscei_value_em),
        ],
        "HSTECH Valuation": [
            ("stock_hk_index_value_em HSTECH", hstech_value_em),
        ],
        "Index Price History": [
            ("stock_hk_index_daily_em HSI", hsi_daily_em),
            ("stock_hk_index_daily_em HSCEI", hscei_daily_em),
            ("stock_hk_index_daily_em HSTECH", hstech_daily_em),
        ],
        "HK Spot (individual stock PE)": [
            ("stock_hk_spot_em", hk_spot_em),
        ],
    }
    for header, items in sections.items():
        print(f"\n{'=' * 12} {header} {'=' * 12}")
        for name, fn in items:
            probe(name, fn, expect_min_rows=100)

    return 0


if __name__ == "__main__":
    sys.exit(main())
