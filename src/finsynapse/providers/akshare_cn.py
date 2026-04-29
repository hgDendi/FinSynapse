"""AkShare CN macro & valuation provider.

Sources (validated by scripts/probe_akshare.py 2026-04-29):
    - stock_index_pe_lg(symbol="沪深300") — CSI300 PE history (5,116 rows from 2005-04)
    - stock_index_pb_lg(symbol="沪深300") — CSI300 PB history (same period)
    - macro_china_money_supply()         — M2 monthly (219 rows)
    - macro_china_shrzgm()               — Social Financing increment monthly (132 rows)
"""
from __future__ import annotations

from datetime import date
from functools import lru_cache

import akshare as ak
import pandas as pd

from finsynapse.providers.base import FetchRange, Provider


@lru_cache(maxsize=4)
def _csi300_pe() -> pd.DataFrame:
    return ak.stock_index_pe_lg(symbol="沪深300")


@lru_cache(maxsize=4)
def _csi300_pb() -> pd.DataFrame:
    return ak.stock_index_pb_lg(symbol="沪深300")


@lru_cache(maxsize=4)
def _m2() -> pd.DataFrame:
    return ak.macro_china_money_supply()


@lru_cache(maxsize=4)
def _shrzgm() -> pd.DataFrame:
    return ak.macro_china_shrzgm()


def _slice_dates(df: pd.DataFrame, start: date, end: date, date_col: str = "date") -> pd.DataFrame:
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col]).dt.date
    return df[(df[date_col] >= start) & (df[date_col] <= end)]


class AkShareCnProvider(Provider):
    name = "akshare_cn"
    layer = "macro"

    def fetch(self, fetch_range: FetchRange) -> pd.DataFrame:
        records: list[pd.DataFrame] = []

        # --- CSI300 valuation (daily) ---
        pe = _csi300_pe()
        pe_long = pd.DataFrame({
            "date": pd.to_datetime(pe["日期"]).dt.date,
            # `滚动市盈率` is TTM PE — preferred over `静态市盈率` (last-FY).
            "value": pd.to_numeric(pe["滚动市盈率"], errors="coerce"),
            "indicator": "csi300_pe_ttm",
            "source_symbol": "stock_index_pe_lg/沪深300/滚动市盈率",
        }).dropna(subset=["value"])
        records.append(_slice_dates(pe_long, fetch_range.start, fetch_range.end))

        pb = _csi300_pb()
        pb_long = pd.DataFrame({
            "date": pd.to_datetime(pb["日期"]).dt.date,
            "value": pd.to_numeric(pb["市净率"], errors="coerce"),
            "indicator": "csi300_pb",
            "source_symbol": "stock_index_pb_lg/沪深300/市净率",
        }).dropna(subset=["value"])
        records.append(_slice_dates(pb_long, fetch_range.start, fetch_range.end))

        # --- M2 yoy growth (monthly; reported with ~2-week lag) ---
        m2 = _m2()
        m2["_date"] = pd.to_datetime(m2["月份"].str.extract(r"(\d{4})年(\d{1,2})月")[0] + "-"
                                      + m2["月份"].str.extract(r"(\d{4})年(\d{1,2})月")[1].str.zfill(2) + "-01")
        m2_long = pd.DataFrame({
            "date": m2["_date"].dt.date,
            "value": pd.to_numeric(m2["货币和准货币(M2)-同比增长"], errors="coerce"),
            "indicator": "cn_m2_yoy",
            "source_symbol": "macro_china_money_supply/M2-同比",
        }).dropna(subset=["value"])
        records.append(_slice_dates(m2_long, fetch_range.start, fetch_range.end))

        # --- Social Financing 12m rolling sum (monthly) ---
        # `增量` is monthly flow (亿元); take 12m trailing sum to get a stable
        # credit-creation proxy. Percentile of THAT is the liquidity signal.
        srf = _shrzgm()
        srf["_date"] = pd.to_datetime(srf["月份"].str[:4] + "-" + srf["月份"].str[4:6] + "-01")
        srf = srf.sort_values("_date").reset_index(drop=True)
        srf["_rolling_12m"] = pd.to_numeric(srf["社会融资规模增量"], errors="coerce").rolling(12).sum()
        srf_long = pd.DataFrame({
            "date": srf["_date"].dt.date,
            "value": srf["_rolling_12m"],
            "indicator": "cn_social_financing_12m",
            "source_symbol": "macro_china_shrzgm/12m-rolling-sum",
        }).dropna(subset=["value"])
        records.append(_slice_dates(srf_long, fetch_range.start, fetch_range.end))

        out = pd.concat(records, ignore_index=True)
        if out.empty:
            raise RuntimeError(
                f"akshare_cn returned 0 rows in range {fetch_range.start}..{fetch_range.end}"
            )
        return out.sort_values(["indicator", "date"]).reset_index(drop=True)


def run(fetch_range: FetchRange, fetch_date: date | None = None) -> tuple[pd.DataFrame, str]:
    provider = AkShareCnProvider()
    df = provider.fetch(fetch_range)
    path = provider.write_bronze(df, fetch_date or date.today())
    return df, str(path)
