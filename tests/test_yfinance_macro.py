from __future__ import annotations

import pickle
from datetime import date
from pathlib import Path
from unittest.mock import patch

from finsynapse.providers.base import FetchRange
from finsynapse.providers.yfinance_macro import SYMBOLS, YFinanceMacroProvider

FIXTURE = Path(__file__).parent / "fixtures" / "yfinance_macro_2026-04-01_2026-04-15.pkl"


def _load_fixture():
    with FIXTURE.open("rb") as f:
        return pickle.load(f)


def test_parser_produces_expected_long_frame_schema(tmp_data_dir):
    """yfinance returns a wide multi-index frame; we must flatten to long format
    with the canonical schema that silver layer depends on."""
    raw = _load_fixture()

    with patch("finsynapse.providers.yfinance_macro.yf.download", return_value=raw):
        provider = YFinanceMacroProvider()
        df = provider.fetch(FetchRange(start=date(2026, 4, 1), end=date(2026, 4, 15)))

    assert set(df.columns) == {"date", "indicator", "value", "source_symbol"}
    assert len(df) > 0
    seen = set(df["indicator"].unique())
    expected = set(SYMBOLS.values())
    missing = expected - seen
    assert len(missing) <= 2, f"too many missing indicators: {missing}"
    assert df["value"].notna().all()
    assert (df["value"] > 0).all()


def test_bronze_write_is_idempotent_and_path_uses_fetch_date(tmp_data_dir):
    raw = _load_fixture()
    with patch("finsynapse.providers.yfinance_macro.yf.download", return_value=raw):
        provider = YFinanceMacroProvider()
        df = provider.fetch(FetchRange(start=date(2026, 4, 1), end=date(2026, 4, 15)))

    p1 = provider.write_bronze(df, date(2026, 4, 15))
    p2 = provider.write_bronze(df, date(2026, 4, 15))

    assert p1 == p2
    assert p1.exists()
    assert p1.name == "2026-04-15.parquet"
    assert "bronze/macro/yfinance_macro" in str(p1)


def test_empty_yfinance_response_raises(tmp_data_dir):
    """If upstream returns nothing, fail loudly — never write empty bronze."""
    import pandas as pd

    with patch("finsynapse.providers.yfinance_macro.yf.download", return_value=pd.DataFrame()):
        provider = YFinanceMacroProvider()
        try:
            provider.fetch(FetchRange(start=date(2026, 4, 1), end=date(2026, 4, 15)))
        except RuntimeError as e:
            assert "empty" in str(e).lower()
        else:
            raise AssertionError("expected RuntimeError on empty response")
