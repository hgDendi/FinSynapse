"""Tests for finsynapse.warehouse.store — DuckDB-based persistent data warehouse."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pandas as pd
import pytest

from finsynapse.warehouse.store import _REGISTRY, Warehouse


@pytest.fixture
def temp_warehouse():
    """Create a warehouse backed by a temporary DuckDB file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        wh = Warehouse(db_path=db_path)
        yield wh
        wh.close()


@pytest.fixture
def sample_macro_parquet(tmp_path: Path) -> Path:
    """Write a minimal macro_daily.parquet for testing."""
    df = pd.DataFrame(
        {
            "date": pd.date_range("2024-01-01", periods=5, freq="D"),
            "indicator": ["sp500"] * 5,
            "value": [4700.0, 4710.0, 4690.0, 4720.0, 4750.0],
            "source": ["yfinance"] * 5,
        }
    )
    path = tmp_path / "macro_daily.parquet"
    df.to_parquet(path)
    return path


@pytest.fixture
def sample_temp_parquet(tmp_path: Path) -> Path:
    """Write a minimal temperature_daily.parquet for testing."""
    df = pd.DataFrame(
        {
            "date": pd.date_range("2024-01-01", periods=3, freq="D"),
            "market": ["us", "us", "us"],
            "overall": [45.0, 47.0, 50.0],
            "valuation": [60.0, 62.0, 65.0],
            "sentiment": [30.0, 32.0, 35.0],
            "liquidity": [40.0, 40.0, 40.0],
            "valuation_ffilled": [False] * 3,
            "sentiment_ffilled": [False] * 3,
            "liquidity_ffilled": [False] * 3,
            "data_quality": ["ok"] * 3,
            "subtemp_completeness": [3, 3, 3],
            "is_complete": [True] * 3,
            "subtemp_ffilled": [0, 0, 0],
            "effective_completeness": [3, 3, 3],
            "is_publishable": [True] * 3,
            "overall_short": [44.0, 46.0, 49.0],
            "overall_long": [48.0, 50.0, 52.0],
            "divergence": [0.0, 0.0, 0.0],
            "conf_ok": [1, 1, 1],
            "valuation_contribution_1w": [0.0, 0.0, 0.0],
            "sentiment_contribution_1w": [0.0, 0.0, 0.0],
            "liquidity_contribution_1w": [0.0, 0.0, 0.0],
            "overall_change_1w": [0.0, 0.0, 0.0],
            "algo_version": ["test"] * 3,
        }
    )
    path = tmp_path / "temperature_daily.parquet"
    df.to_parquet(path)
    return path


class TestWarehouseBasic:
    def test_creates_db_file(self, temp_warehouse):
        _ = temp_warehouse.conn  # trigger lazy connection + file creation
        assert temp_warehouse.db_path.exists()

    def test_meta_table_exists(self, temp_warehouse):
        df = temp_warehouse.query("SELECT * FROM _warehouse_meta")
        assert len(df) == 0  # empty at start

    def test_status_empty(self, temp_warehouse):
        df = temp_warehouse.status()
        assert df.empty


class TestAppendSingle:
    def test_append_macro(self, temp_warehouse, sample_macro_parquet):
        result = temp_warehouse.append_file(sample_macro_parquet, "macro_daily")
        assert result["status"] == "appended"
        assert result["new_rows"] == 5
        assert result["total_rows"] == 5
        assert result["min_date"] == "2024-01-01"
        assert result["max_date"] == "2024-01-05"

        # verify data is queryable
        df = temp_warehouse.query("SELECT COUNT(*) AS n FROM macro_daily")
        assert df["n"].iloc[0] == 5

    def test_append_temperature(self, temp_warehouse, sample_temp_parquet):
        result = temp_warehouse.append_file(sample_temp_parquet, "temperature_daily")
        assert result["status"] == "appended"
        assert result["new_rows"] == 3

    def test_append_idempotent(self, temp_warehouse, sample_macro_parquet):
        temp_warehouse.append_file(sample_macro_parquet, "macro_daily")
        result = temp_warehouse.append_file(sample_macro_parquet, "macro_daily")
        assert result["new_rows"] == 0
        assert result["total_rows"] == 5

    def test_append_partial_overlap(self, temp_warehouse, tmp_path):
        # first batch: days 1-3
        df1 = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", periods=3, freq="D"),
                "indicator": ["sp500"] * 3,
                "value": [4700.0, 4710.0, 4690.0],
                "source": ["yfinance"] * 3,
            }
        )
        p1 = tmp_path / "batch1.parquet"
        df1.to_parquet(p1)
        temp_warehouse.append_file(p1, "macro_daily")

        # second batch: days 2-5 (overlap on days 2-3)
        df2 = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-02", periods=4, freq="D"),
                "indicator": ["sp500"] * 4,
                "value": [4710.0, 4690.0, 4800.0, 4810.0],
                "source": ["yfinance"] * 4,
            }
        )
        p2 = tmp_path / "batch2.parquet"
        df2.to_parquet(p2)
        result = temp_warehouse.append_file(p2, "macro_daily")

        # only the 2 new rows should be appended
        assert result["new_rows"] == 2
        assert result["total_rows"] == 5


class TestWarehouseMeta:
    def test_status_after_append(self, temp_warehouse, sample_macro_parquet):
        temp_warehouse.append_file(sample_macro_parquet, "macro_daily")
        df = temp_warehouse.status()
        assert len(df) == 1
        assert df["table_name"].iloc[0] == "macro_daily"
        assert df["row_count"].iloc[0] == 5

    def test_multiple_tables_meta(self, temp_warehouse, sample_macro_parquet, sample_temp_parquet):
        temp_warehouse.append_file(sample_macro_parquet, "macro_daily")
        temp_warehouse.append_file(sample_temp_parquet, "temperature_daily")
        df = temp_warehouse.status()
        assert len(df) == 2


class TestRebuild:
    def test_rebuild_clears_and_reloads(self, temp_warehouse, tmp_path):
        # use the registered filename so rebuild_all can find it
        df1 = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", periods=3, freq="D"),
                "indicator": ["sp500"] * 3,
                "value": [4700.0, 4710.0, 4690.0],
                "source": ["yfinance"] * 3,
            }
        )
        p1 = tmp_path / "macro_daily.parquet"
        df1.to_parquet(p1)
        temp_warehouse.append_file(p1, "macro_daily")

        # rebuild with different data (same filename, overwrite)
        df2 = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", periods=5, freq="D"),
                "indicator": ["sp500"] * 5,
                "value": [4700.0, 4710.0, 4690.0, 4720.0, 4750.0],
                "source": ["yfinance"] * 5,
            }
        )
        p2 = tmp_path / "macro_daily.parquet"
        df2.to_parquet(p2)
        temp_warehouse.rebuild_all(silver_dir=tmp_path)

        df = temp_warehouse.query("SELECT COUNT(*) AS n FROM macro_daily")
        assert df["n"].iloc[0] == 5
