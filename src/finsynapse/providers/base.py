from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd

from finsynapse.config import settings


@dataclass(frozen=True)
class FetchRange:
    start: date
    end: date


class Provider(ABC):
    """Base class for all data providers.

    Each provider owns one logical data source (e.g. "yfinance_macro", "fred",
    "akshare_cn_macro"). It is responsible for fetching raw data and writing it
    to the bronze layer in a deterministic, idempotent way.

    Bronze path convention:
        data/bronze/{layer}/{name}/{YYYY-MM-DD}.parquet

    where `layer` is one of {"macro", "flow", "valuation"} and the date is the
    fetch date (overwrite-safe; reruns produce the same file).
    """

    name: str
    layer: str  # "macro" | "flow" | "valuation"

    @abstractmethod
    def fetch(self, fetch_range: FetchRange) -> pd.DataFrame:
        """Return a long-format DataFrame with at minimum: date, indicator, value."""

    def bronze_path(self, fetch_date: date) -> Path:
        d = settings.bronze_dir / self.layer / self.name
        d.mkdir(parents=True, exist_ok=True)
        return d / f"{fetch_date.isoformat()}.parquet"

    def write_bronze(self, df: pd.DataFrame, fetch_date: date) -> Path:
        path = self.bronze_path(fetch_date)
        df.to_parquet(path, index=False)
        return path
