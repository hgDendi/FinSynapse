"""AkShare HK Stock Connect flow provider.

Sources (validated 2026-04-29):
    - stock_hsgt_hist_em(symbol="北向资金") — Northbound daily net (2,660 rows from 2014-11)
    - stock_hsgt_hist_em(symbol="南向资金") — Southbound daily net (2,622 rows)

We collapse daily flows into a 5-day rolling SUM. Single-day flow is too noisy;
5-day captures the meaningful stance (per plan §11.2 sentiment design).
AH premium index (HSAHP) and HSI options PCR are intentionally NOT here —
neither has a free CI-friendly source (probed 2026-04-29).
"""
from __future__ import annotations

from datetime import date
from functools import lru_cache

import akshare as ak
import pandas as pd

from finsynapse.providers.base import FetchRange, Provider


@lru_cache(maxsize=4)
def _hsgt(direction: str) -> pd.DataFrame:
    return ak.stock_hsgt_hist_em(symbol=direction)


def _to_long(direction_label: str, indicator: str) -> pd.DataFrame:
    df = _hsgt(direction_label).copy()
    df["date"] = pd.to_datetime(df["日期"]).dt.date
    df["net_buy"] = pd.to_numeric(df["当日成交净买额"], errors="coerce")
    df = df.dropna(subset=["net_buy"]).sort_values("date").reset_index(drop=True)
    df["value"] = df["net_buy"].rolling(5).sum()
    df["indicator"] = indicator
    df["source_symbol"] = f"stock_hsgt_hist_em/{direction_label}/5d-sum"
    return df.dropna(subset=["value"])[["date", "indicator", "value", "source_symbol"]]


class AkShareFlowProvider(Provider):
    name = "akshare_flow"
    layer = "flow"

    def fetch(self, fetch_range: FetchRange) -> pd.DataFrame:
        records = [
            _to_long("北向资金", "cn_north_5d"),
            _to_long("南向资金", "cn_south_5d"),
        ]
        out = pd.concat(records, ignore_index=True)
        out = out[(out["date"] >= fetch_range.start) & (out["date"] <= fetch_range.end)]
        if out.empty:
            raise RuntimeError(
                f"akshare_flow returned 0 rows in range {fetch_range.start}..{fetch_range.end}"
            )
        return out.sort_values(["indicator", "date"]).reset_index(drop=True)


def run(fetch_range: FetchRange, fetch_date: date | None = None) -> tuple[pd.DataFrame, str]:
    provider = AkShareFlowProvider()
    df = provider.fetch(fetch_range)
    path = provider.write_bronze(df, fetch_date or date.today())
    return df, str(path)
