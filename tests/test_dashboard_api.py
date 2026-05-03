from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from finsynapse.dashboard.api import API_SCHEMA_VERSION, build_manifest, write_all
from finsynapse.dashboard.data import DashboardData


def _empty_dashboard_data(tmp_path: Path) -> DashboardData:
    return DashboardData(
        temperature=pd.DataFrame(),
        macro=pd.DataFrame(),
        percentile=pd.DataFrame(),
        divergence=pd.DataFrame(),
        health=pd.DataFrame(),
        silver_dir=tmp_path,
    )


def _sample_dashboard_data(tmp_path: Path) -> DashboardData:
    temp = pd.DataFrame(
        [
            {
                "date": "2026-04-30",
                "market": "us",
                "overall": 75.2,
                "valuation": 80.0,
                "sentiment": 70.0,
                "liquidity": 65.0,
                "overall_change_1w": 1.2,
                "valuation_contribution_1w": 0.4,
                "sentiment_contribution_1w": 0.5,
                "liquidity_contribution_1w": 0.3,
                "subtemp_completeness": 3,
                "is_complete": True,
                "data_quality": "ok",
            },
            {
                "date": "2026-04-30",
                "market": "cn",
                "overall": 55.0,
                "valuation": 60.0,
                "sentiment": 50.0,
                "liquidity": 55.0,
                "overall_change_1w": -0.5,
                "valuation_contribution_1w": -0.2,
                "sentiment_contribution_1w": -0.1,
                "liquidity_contribution_1w": -0.2,
                "subtemp_completeness": 3,
                "is_complete": True,
                "data_quality": "ok",
            },
        ]
    )
    pct = pd.DataFrame(
        [
            {"date": "2026-04-30", "indicator": "vix", "value": 18.5, "pct_5y": 45.0, "pct_10y": 50.0},
            {"date": "2026-04-30", "indicator": "us_pe_ttm", "value": 28.0, "pct_5y": 80.0, "pct_10y": 85.0},
        ]
    )
    div = pd.DataFrame(
        [
            {
                "date": "2026-04-30",
                "pair_name": "sp500_vix",
                "is_divergent": True,
                "strength": 0.55,
                "description": "SP\u2191+VIX\u2191 divergence",
                "a_change": 0.02,
                "b_change": 0.10,
            }
        ]
    )
    return DashboardData(
        temperature=temp,
        macro=pd.DataFrame(),
        percentile=pct,
        divergence=div,
        health=pd.DataFrame(),
        silver_dir=tmp_path,
    )


def test_manifest_lists_endpoints_and_schema():
    manifest = build_manifest(asof="2026-04-30", endpoints=["temperature_latest.json", "indicators_latest.json"])
    assert manifest["schema_version"] == API_SCHEMA_VERSION
    assert manifest["asof"] == "2026-04-30"
    assert "temperature_latest.json" in manifest["endpoints"]
    assert manifest["endpoints"]["temperature_latest.json"]["description"]
