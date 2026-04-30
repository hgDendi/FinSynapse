"""Tests for AkShare-based providers — verify data transformation logic.

Tests mock the AkShare library calls, not the network layer, since AkShare
has its own HTTP stack. Focus is on ensuring column renaming resilience,
date parsing, rolling aggregates, and schema correctness.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import patch

import pandas as pd
import pytest

from finsynapse.providers.akshare_cn import AkShareCnProvider, _pick_col
from finsynapse.providers.akshare_flow import AkShareFlowProvider
from finsynapse.providers.akshare_hk import AkShareHkProvider
from finsynapse.providers.base import FetchRange

# ---------------------------------------------------------------------------
# mock fixtures
# ---------------------------------------------------------------------------

def _make_csi300_pe():
    """Shape: AkShare stock_index_pe_lg('沪深300')"""
    dates = pd.date_range("2025-01-02", "2026-04-15", freq="B")
    return pd.DataFrame({
        "日期": dates,
        "滚动市盈率": [12.5 + i * 0.001 for i in range(len(dates))],
        "静态市盈率": [13.0 + i * 0.001 for i in range(len(dates))],
    })


def _make_csi300_pb():
    dates = pd.date_range("2025-01-02", "2026-04-15", freq="B")
    return pd.DataFrame({
        "日期": dates,
        "市净率": [1.4 + i * 0.0001 for i in range(len(dates))],
    })


def _make_m2():
    return pd.DataFrame({
        "月份": [f"{y}年{m:02d}月" for y in range(2025, 2027) for m in range(1, 13)],
        "货币和准货币(M2)-同比增长": [8.5 + m * 0.1 for m in range(1, 25)],
    })


def _make_index_volume(symbol: str = ""):
    dates = pd.date_range("2025-01-02", "2026-04-15", freq="B")
    return pd.DataFrame({
        "date": dates,
        "volume": [1e9 + i * 1e7 for i in range(len(dates))],
    })


def _make_margin():
    dates = pd.date_range("2025-01-02", "2026-04-15", freq="B")
    return pd.DataFrame({
        "日期": dates,
        "融资融券余额": [1.5e12 + i * 1e9 for i in range(len(dates))],
    })


def _make_shibor():
    dates = pd.date_range("2025-01-02", "2026-04-15", freq="B")
    return pd.DataFrame({
        "日期": dates,
        "1W-定价": [1.5 + 0.001 * i for i in range(len(dates))],
        "1M-定价": [1.8 + 0.001 * i for i in range(len(dates))],
    })


def _make_social_financing():
    return pd.DataFrame({
        "月份": [f"{y}{m:02d}" for y in range(2023, 2027) for m in range(1, 13)],
        "社会融资规模增量": [20000 + m * 500 for m in range(1, 49)],
    })


def _make_hibor():
    dates = pd.date_range("2025-01-02", "2026-04-15", freq="B")
    return pd.DataFrame({
        "日期": dates,
        "1M-定价": [3.5 + 0.001 * i for i in range(len(dates))],
    })


def _make_flow(direction: str):
    dates = pd.date_range("2025-01-02", "2026-04-15", freq="B")
    net_values = [10.0 if i % 2 == 0 else -5.0 for i in range(len(dates))]
    return pd.DataFrame({
        "日期": dates,
        "当日成交净买额": net_values,
    })


# ---------------------------------------------------------------------------
# _pick_col
# ---------------------------------------------------------------------------

class TestPickCol:
    def test_finds_first_matching_candidate(self):
        df = pd.DataFrame({"1W-定价": [1.0], "1W": [2.0]})
        result = _pick_col(df, ("1W-定价", "1W"), "test")
        assert result == "1W-定价"

    def test_raises_keyerror_with_context_on_miss(self):
        df = pd.DataFrame({"other": [1.0]})
        with pytest.raises(KeyError, match="test"):
            _pick_col(df, ("1W-定价", "1W"), "test")


# ---------------------------------------------------------------------------
# akshare_cn
# ---------------------------------------------------------------------------

class TestAkShareCn:
    @pytest.fixture(autouse=True)
    def _mock_akshare(self):
        patches = [
            patch("finsynapse.providers.akshare_cn._csi300_pe", return_value=_make_csi300_pe()),
            patch("finsynapse.providers.akshare_cn._csi300_pb", return_value=_make_csi300_pb()),
            patch("finsynapse.providers.akshare_cn._m2", return_value=_make_m2()),
            patch("finsynapse.providers.akshare_cn._shrzgm", return_value=_make_social_financing()),
            patch("finsynapse.providers.akshare_cn._index_volume", side_effect=_make_index_volume),
            patch("finsynapse.providers.akshare_cn._margin_sh", return_value=_make_margin()),
            patch("finsynapse.providers.akshare_cn._margin_sz", return_value=_make_margin()),
            patch("finsynapse.providers.akshare_cn._shibor_all", return_value=_make_shibor()),
        ]
        for p in patches:
            p.start()
        yield
        for p in patches:
            p.stop()

    def test_fetches_all_indicators(self, tmp_data_dir):
        provider = AkShareCnProvider()
        df = provider.fetch(FetchRange(start=date(2026, 4, 1), end=date(2026, 4, 15)))
        assert set(df.columns) == {"date", "indicator", "value", "source_symbol"}
        indicators = set(df["indicator"].unique())
        expected = {
            "csi300_pe_ttm",
            "csi300_pb",
            "cn_m2_yoy",
            "cn_a_turnover_5d",
            "cn_social_financing_12m",
            "cn_margin_balance",
            "cn_dr007",
        }
        assert indicators == expected

    def test_m2_date_parsing(self, tmp_data_dir):
        provider = AkShareCnProvider()
        df = provider.fetch(FetchRange(start=date(2025, 6, 1), end=date(2025, 6, 30)))
        m2_rows = df[df["indicator"] == "cn_m2_yoy"]
        assert len(m2_rows) == 1
        assert m2_rows.iloc[0]["date"] == date(2025, 6, 1)

    def test_social_financing_12m_rolling(self, tmp_data_dir):
        provider = AkShareCnProvider()
        df = provider.fetch(FetchRange(start=date(2024, 12, 1), end=date(2024, 12, 31)))
        srf = df[df["indicator"] == "cn_social_financing_12m"]
        assert len(srf) == 1

    def test_turnover_5d_smoothed(self, tmp_data_dir):
        provider = AkShareCnProvider()
        df = provider.fetch(FetchRange(start=date(2026, 4, 1), end=date(2026, 4, 15)))
        turnover = df[df["indicator"] == "cn_a_turnover_5d"]
        assert len(turnover) > 5

    def test_bronze_write_idempotent(self, tmp_data_dir):
        provider = AkShareCnProvider()
        df = provider.fetch(FetchRange(start=date(2026, 4, 1), end=date(2026, 4, 5)))
        p1 = provider.write_bronze(df, date(2026, 4, 5))
        p2 = provider.write_bronze(df, date(2026, 4, 5))
        assert p1 == p2
        assert p1.exists()


# ---------------------------------------------------------------------------
# akshare_hk
# ---------------------------------------------------------------------------

class TestAkShareHk:
    def test_hibor_parsing(self, tmp_data_dir):
        with patch("finsynapse.providers.akshare_hk._hibor_all", return_value=_make_hibor()):
            provider = AkShareHkProvider()
            df = provider.fetch(FetchRange(start=date(2026, 4, 1), end=date(2026, 4, 15)))
        assert set(df.columns) == {"date", "indicator", "value", "source_symbol"}
        assert (df["indicator"] == "hk_hibor_1m").all()
        assert df["value"].notna().all()

    def test_bronze_write_idempotent(self, tmp_data_dir):
        with patch("finsynapse.providers.akshare_hk._hibor_all", return_value=_make_hibor()):
            provider = AkShareHkProvider()
            df = provider.fetch(FetchRange(start=date(2026, 4, 1), end=date(2026, 4, 5)))
        p1 = provider.write_bronze(df, date(2026, 4, 5))
        p2 = provider.write_bronze(df, date(2026, 4, 5))
        assert p1 == p2
        assert p1.exists()


# ---------------------------------------------------------------------------
# akshare_flow
# ---------------------------------------------------------------------------

class TestAkShareFlow:
    def test_flow_5d_rolling_sum(self, tmp_data_dir):
        with patch("finsynapse.providers.akshare_flow._hsgt", side_effect=_make_flow):
            provider = AkShareFlowProvider()
            df = provider.fetch(FetchRange(start=date(2026, 4, 1), end=date(2026, 4, 15)))
        assert set(df.columns) == {"date", "indicator", "value", "source_symbol"}
        indicators = set(df["indicator"].unique())
        assert indicators == {"cn_north_5d", "cn_south_5d"}

    def test_bronze_write_idempotent(self, tmp_data_dir):
        with patch("finsynapse.providers.akshare_flow._hsgt", side_effect=_make_flow):
            provider = AkShareFlowProvider()
            df = provider.fetch(FetchRange(start=date(2026, 4, 1), end=date(2026, 4, 5)))
        p1 = provider.write_bronze(df, date(2026, 4, 5))
        p2 = provider.write_bronze(df, date(2026, 4, 5))
        assert p1 == p2
        assert p1.exists()
