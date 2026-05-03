from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
import pytest
from scripts.validation_lib import (
    INDEX_MAP,
    ForwardReturnRow,
    compute_forward_returns,
    directional_ok,
    spearman_rho,
    strict_ok,
    zone_of,
)


def test_zone_classification():
    assert zone_of(75.0) == "hot"
    assert zone_of(50.0) == "mid"
    assert zone_of(20.0) == "cold"
    assert zone_of(float("nan")) == "nan"


def test_directional_ok_handles_mid_pivots():
    # mid pivots use 25..75 band per backtest_temperature.py:142.
    assert directional_ok(50.0, "mid") is True
    assert directional_ok(20.0, "mid") is False
    assert directional_ok(80.0, "mid") is False
    assert directional_ok(75.0, "hot") is True
    assert directional_ok(20.0, "cold") is True


def test_strict_ok_zone_bounds():
    assert strict_ok(75.0, "hot") is True
    assert strict_ok(65.0, "hot") is False
    assert strict_ok(50.0, "mid") is True
    assert strict_ok(20.0, "cold") is True


def test_spearman_rho_returns_float_when_enough_data():
    rows = [
        ForwardReturnRow(
            date=date(2020, 1, 1) + pd.Timedelta(days=i).to_pytimedelta(),
            market="us",
            temperature=float(i),
            return_3m=-float(i) / 100,
        )
        for i in range(1, 60)
    ]
    rho = spearman_rho(rows, "us", "3m")
    assert rho is not None
    assert rho < 0  # constructed inverse relationship
