"""HK valuation proxy via EWH (iShares MSCI Hong Kong ETF, US-listed).

Why EWH and not ^HSI directly:
    - ^HSI doesn't carry dividends in yfinance
    - 2800.HK (Tracker Fund) does, but yfinance HK feed is less reliable
    - EWH has 30-yr daily history with reliable dividend column from 1996
    - MSCI Hong Kong basket overlaps heavily with HSI (large-cap HK names)

Indicator: hk_ewh_yield_ttm = trailing-12-month dividend yield (%).
    High yield = cheap (think: dividend stocks bid down) = COLD valuation.
    weights.yaml uses direction "-" for this.
"""
from __future__ import annotations

from datetime import date

import pandas as pd
import yfinance as yf

from finsynapse.providers.base import FetchRange, Provider


class YFinanceHkValuationProvider(Provider):
    name = "yfinance_hk"
    layer = "valuation"

    def fetch(self, fetch_range: FetchRange) -> pd.DataFrame:
        # Always pull the full history — TTM dividend rolling needs 365d
        # of past dividends regardless of the slicing window.
        raw = yf.download(
            tickers="EWH",
            period="max",
            auto_adjust=False,
            actions=True,
            progress=False,
            threads=False,
        )
        if raw is None or raw.empty:
            raise RuntimeError("yfinance returned empty frame for EWH")

        # Multi-index columns: ('Close','EWH'), ('Dividends','EWH'), ...
        # Flatten if needed.
        if isinstance(raw.columns, pd.MultiIndex):
            raw.columns = raw.columns.get_level_values(0)

        if "Close" not in raw.columns or "Dividends" not in raw.columns:
            raise RuntimeError(f"EWH frame missing expected cols: {list(raw.columns)}")

        df = raw[["Close", "Dividends"]].copy()
        df.index = pd.to_datetime(df.index).date
        # 252 trading days ≈ 1 year — sum of dividends paid in that window.
        ttm_div = df["Dividends"].rolling(252, min_periods=200).sum()
        # Yield = TTM dividends / today's price * 100 (as percentage).
        yield_pct = (ttm_div / df["Close"]) * 100.0
        yield_pct = yield_pct.dropna()

        out = pd.DataFrame({
            "date": yield_pct.index,
            "indicator": "hk_ewh_yield_ttm",
            "value": yield_pct.values,
            "source_symbol": "EWH/TTM-dividend-yield",
        })
        out["date"] = pd.to_datetime(out["date"]).dt.date
        out = out[(out["date"] >= fetch_range.start) & (out["date"] <= fetch_range.end)]
        if out.empty:
            raise RuntimeError(
                f"yfinance_hk returned 0 rows in range {fetch_range.start}..{fetch_range.end}"
            )
        return out.sort_values("date").reset_index(drop=True)


def run(fetch_range: FetchRange, fetch_date: date | None = None) -> tuple[pd.DataFrame, str]:
    provider = YFinanceHkValuationProvider()
    df = provider.fetch(fetch_range)
    path = provider.write_bronze(df, fetch_date or date.today())
    return df, str(path)
