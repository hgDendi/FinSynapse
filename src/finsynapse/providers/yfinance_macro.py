from __future__ import annotations

from datetime import date

import pandas as pd
import yfinance as yf

from finsynapse.providers.base import FetchRange, Provider

# Symbol -> canonical indicator name. Stable names matter: silver/percentile
# layers reference these. Renames here cascade — change with care.
SYMBOLS: dict[str, str] = {
    "^TNX": "us10y_yield",
    "DX-Y.NYB": "dxy",
    "^VIX": "vix",
    "USDCNY=X": "usdcny",
    "HKDCNY=X": "hkdcny",
    "^GSPC": "sp500",
    "^HSI": "hsi",
    "000300.SS": "csi300",
    "GC=F": "gold_futures",
}


class YFinanceMacroProvider(Provider):
    name = "yfinance_macro"
    layer = "macro"

    def fetch(self, fetch_range: FetchRange) -> pd.DataFrame:
        symbols = list(SYMBOLS.keys())
        raw = yf.download(
            tickers=symbols,
            start=fetch_range.start.isoformat(),
            end=fetch_range.end.isoformat(),
            interval="1d",
            group_by="ticker",
            auto_adjust=False,
            progress=False,
            threads=True,
        )
        if raw is None or raw.empty:
            raise RuntimeError("yfinance returned empty frame for macro symbols")

        records: list[dict] = []
        for symbol, indicator in SYMBOLS.items():
            if symbol not in raw.columns.get_level_values(0):
                continue
            sub = raw[symbol]
            if "Close" not in sub.columns:
                continue
            closes = sub["Close"].dropna()
            for ts, value in closes.items():
                records.append(
                    {
                        "date": pd.Timestamp(ts).date(),
                        "indicator": indicator,
                        "value": float(value),
                        "source_symbol": symbol,
                    }
                )

        df = pd.DataFrame.from_records(records)
        if df.empty:
            raise RuntimeError("yfinance returned no usable rows after parsing")
        df = df.sort_values(["indicator", "date"]).reset_index(drop=True)
        return df


def run(fetch_range: FetchRange, fetch_date: date | None = None) -> tuple[pd.DataFrame, str]:
    """Convenience: fetch + write bronze. Returns (df, bronze_path_str)."""
    provider = YFinanceMacroProvider()
    df = provider.fetch(fetch_range)
    path = provider.write_bronze(df, fetch_date or date.today())
    return df, str(path)
