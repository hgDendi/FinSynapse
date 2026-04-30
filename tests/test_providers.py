"""Tests for providers not covered by test_yfinance_macro.py.

Tests focus on parsing logic and schema correctness — upstream responses are mocked
so tests never hit the network and are deterministic.
"""

from __future__ import annotations

import json
from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from finsynapse.providers.base import FetchRange
from finsynapse.providers.fred import SERIES as FRED_SERIES
from finsynapse.providers.fred import FredProvider
from finsynapse.providers.multpl import MultplProvider
from finsynapse.providers.treasury_real_yield import TreasuryRealYieldProvider
from finsynapse.providers.yfinance_hk import YFinanceHkValuationProvider

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _mock_response(json_data=None, text="", status_code=200):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data or {}
    resp.text = text
    resp.raise_for_status = lambda: None
    return resp


# ---------------------------------------------------------------------------
# fred
# ---------------------------------------------------------------------------

FRED_FIXTURE = {
    "observations": [
        {"date": "2026-04-01", "value": "2.15"},
        {"date": "2026-04-02", "value": "2.18"},
        {"date": "2026-04-03", "value": "."},
        {"date": "2026-04-04", "value": "2.20"},
    ]
}


def _fred_df(start=date(2026, 4, 1), end=date(2026, 4, 4)):
    with patch("finsynapse.providers.fred.requests_session") as mock_session:
        mock_session.return_value.get.return_value = _mock_response(json_data=FRED_FIXTURE)
        provider = FredProvider()
        return provider.fetch(FetchRange(start=start, end=end))


class TestFred:
    def test_parses_observations_into_long_schema(self, tmp_data_dir, monkeypatch):
        monkeypatch.setenv("FRED_API_KEY", "test-key")
        from finsynapse import config as cfg

        cfg.settings = cfg.Settings()
        df = _fred_df()
        assert set(df.columns) == {"date", "indicator", "value", "source_symbol"}
        assert len(df) > 0
        assert df["value"].notna().all()

    def test_skips_missing_dot_values(self, tmp_data_dir, monkeypatch):
        monkeypatch.setenv("FRED_API_KEY", "test-key")
        from finsynapse import config as cfg

        cfg.settings = cfg.Settings()
        df = _fred_df()
        dates = {str(d) for d in df["date"]}
        assert "2026-04-03" not in dates

    def test_empty_response_raises(self, tmp_data_dir, monkeypatch):
        monkeypatch.setenv("FRED_API_KEY", "test-key")
        from finsynapse import config as cfg

        cfg.settings = cfg.Settings()
        with patch("finsynapse.providers.fred.requests_session") as mock_session:
            mock_session.return_value.get.return_value = _mock_response(json_data={"observations": []})
            provider = FredProvider()
            with pytest.raises(RuntimeError):
                provider.fetch(FetchRange(start=date(2026, 4, 1), end=date(2026, 4, 1)))

    def test_bronze_write_idempotent(self, tmp_data_dir, monkeypatch):
        monkeypatch.setenv("FRED_API_KEY", "test-key")
        from finsynapse import config as cfg

        cfg.settings = cfg.Settings()
        df = _fred_df()
        provider = FredProvider()
        p1 = provider.write_bronze(df, date(2026, 4, 4))
        p2 = provider.write_bronze(df, date(2026, 4, 4))
        assert p1 == p2
        assert p1.exists()
        assert p1.name == "2026-04-04.parquet"

    def test_each_series_has_canonical_columns(self):
        for s in FRED_SERIES:
            assert s.indicator
            assert isinstance(s.indicator, str)


# ---------------------------------------------------------------------------
# multpl
# ---------------------------------------------------------------------------

MULTIPL_HTML = """<html><body>
<table id="datatable"><tr><th>Date</th></tr>
<tr><td>Apr 1, 2026</td><td>32.50</td></tr>
<tr><td>Mar 1, 2026</td><td>31.80</td></tr>
<tr><td>Feb 1, 2026</td><td>30.20</td></tr>
</table></body></html>"""


def _mock_multpl_get(url, headers=None, timeout=None):
    return _mock_response(text=MULTIPL_HTML)


class TestMultpl:
    def test_parses_html_table_into_long_schema(self, tmp_data_dir):
        with patch("finsynapse.providers.multpl.requests_session") as mock_session:
            mock_session.return_value.get.side_effect = _mock_multpl_get
            provider = MultplProvider()
            df = provider.fetch(FetchRange(start=date(2020, 1, 1), end=date(2026, 12, 31)))
        assert set(df.columns) == {"date", "indicator", "value", "source_symbol"}
        assert len(df) > 0
        assert df["value"].notna().all()

    def test_filters_to_requested_range(self, tmp_data_dir):
        with patch("finsynapse.providers.multpl.requests_session") as mock_session:
            mock_session.return_value.get.side_effect = _mock_multpl_get
            provider = MultplProvider()
            df = provider.fetch(FetchRange(start=date(2026, 3, 1), end=date(2026, 3, 31)))
        assert all(d.month == 3 for d in df["date"])
        assert all(d.year == 2026 for d in df["date"])

    def test_empty_html_raises(self, tmp_data_dir):
        with patch("finsynapse.providers.multpl.requests_session") as mock_session:
            mock_session.return_value.get.return_value = _mock_response(text="<html><body></body></html>")
            provider = MultplProvider()
            with pytest.raises(RuntimeError):
                provider.fetch(FetchRange(start=date(2026, 4, 1), end=date(2026, 4, 1)))

    def test_bronze_write_idempotent(self, tmp_data_dir):
        with patch("finsynapse.providers.multpl.requests_session") as mock_session:
            mock_session.return_value.get.side_effect = _mock_multpl_get
            provider = MultplProvider()
            df = provider.fetch(FetchRange(start=date(2020, 1, 1), end=date(2026, 12, 31)))
        p1 = provider.write_bronze(df, date(2026, 4, 1))
        p2 = provider.write_bronze(df, date(2026, 4, 1))
        assert p1 == p2
        assert p1.exists()


# ---------------------------------------------------------------------------
# treasury_real_yield
# ---------------------------------------------------------------------------

TREASURY_CSV = """Date,10 YR
04/29/2026,2.42
04/28/2026,2.40
04/27/2026,2.41
"""


def _mock_treasury_get(url, params=None, headers=None, timeout=None):
    return _mock_response(text=TREASURY_CSV)


class TestTreasuryRealYield:
    def test_parses_csv_into_long_schema(self, tmp_data_dir):
        with patch("finsynapse.providers.treasury_real_yield.requests_session") as mock_session:
            mock_session.return_value.get.side_effect = _mock_treasury_get
            provider = TreasuryRealYieldProvider()
            df = provider.fetch(FetchRange(start=date(2026, 4, 27), end=date(2026, 4, 29)))
        assert set(df.columns) == {"date", "indicator", "value", "source_symbol"}
        assert len(df) == 3
        assert (df["indicator"] == "us10y_real_yield").all()

    def test_bronze_write_idempotent(self, tmp_data_dir):
        with patch("finsynapse.providers.treasury_real_yield.requests_session") as mock_session:
            mock_session.return_value.get.side_effect = _mock_treasury_get
            provider = TreasuryRealYieldProvider()
            df = provider.fetch(FetchRange(start=date(2026, 4, 27), end=date(2026, 4, 29)))
        p1 = provider.write_bronze(df, date(2026, 4, 29))
        p2 = provider.write_bronze(df, date(2026, 4, 29))
        assert p1 == p2
        assert p1.exists()


# ---------------------------------------------------------------------------
# yfinance_hk (EWH TTM yield)
# ---------------------------------------------------------------------------

def _mock_ewh_frame():
    import numpy as np

    idx = pd.date_range("2025-01-02", "2026-04-15", freq="B")
    closes = pd.Series(18.0 + np.sin(np.arange(len(idx)) * 0.01), index=idx)
    divs = pd.Series([0.12 if i % 60 == 0 else 0.0 for i in range(len(idx))], index=idx)
    raw = pd.DataFrame({"Close": closes, "Dividends": divs})
    raw.columns = pd.MultiIndex.from_tuples([("Close", "EWH"), ("Dividends", "EWH")])
    return raw


class TestYFinanceHk:
    def test_computes_ttm_dividend_yield(self, tmp_data_dir):
        raw = _mock_ewh_frame()
        with patch("finsynapse.providers.yfinance_hk.yf.download", return_value=raw):
            provider = YFinanceHkValuationProvider()
            df = provider.fetch(FetchRange(start=date(2026, 4, 1), end=date(2026, 4, 15)))
        assert set(df.columns) == {"date", "indicator", "value", "source_symbol"}
        assert len(df) > 0
        assert (df["indicator"] == "hk_ewh_yield_ttm").all()
        assert df["value"].notna().all()
        assert (df["value"] > 0).all()

    def test_bronze_write_idempotent(self, tmp_data_dir):
        raw = _mock_ewh_frame()
        with patch("finsynapse.providers.yfinance_hk.yf.download", return_value=raw):
            provider = YFinanceHkValuationProvider()
            df = provider.fetch(FetchRange(start=date(2026, 4, 1), end=date(2026, 4, 15)))
        p1 = provider.write_bronze(df, date(2026, 4, 15))
        p2 = provider.write_bronze(df, date(2026, 4, 15))
        assert p1 == p2
        assert p1.exists()

    def test_empty_response_raises(self, tmp_data_dir):
        with patch("finsynapse.providers.yfinance_hk.yf.download", return_value=pd.DataFrame()):
            provider = YFinanceHkValuationProvider()
            with pytest.raises(RuntimeError):
                provider.fetch(FetchRange(start=date(2026, 4, 1), end=date(2026, 4, 15)))


# ---------------------------------------------------------------------------
# retry
# ---------------------------------------------------------------------------

class TestRetry:
    def test_session_is_created_with_retry_adapter(self):
        from finsynapse.providers.retry import requests_session

        session = requests_session()
        assert session is not None
        adapters = session.adapters
        assert "https://" in adapters

    def test_session_is_cached(self):
        from finsynapse.providers.retry import requests_session

        s1 = requests_session()
        s2 = requests_session()
        assert s1 is s2

    def test_with_backoff_retries_on_exception(self):
        import requests as r

        from finsynapse.providers.retry import with_backoff

        call_count = 0

        @with_backoff(max_retries=2, base_delay=0.0)
        def flaky():
            nonlocal call_count
            call_count += 1
            raise r.exceptions.ConnectionError("transient")

        with pytest.raises(r.exceptions.ConnectionError):
            flaky()
        assert call_count == 3  # initial + 2 retries

    def test_with_backoff_returns_on_success(self):
        from finsynapse.providers.retry import with_backoff

        call_count = 0

        @with_backoff(max_retries=3, base_delay=0.0)
        def flaky():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                import requests as r

                raise r.exceptions.ConnectionError("transient")
            return "success"

        result = flaky()
        assert result == "success"
        assert call_count == 3
