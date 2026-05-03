from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts" / "oos_backtest.py"


def _load_oos_module():
    spec = importlib.util.spec_from_file_location("oos_backtest", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["oos_backtest"] = mod
    spec.loader.exec_module(mod)
    return mod


def test_oos_module_importable():
    """The script should be importable as a module without executing main."""
    mod = _load_oos_module()
    assert hasattr(mod, "rolling_ic")
    assert hasattr(mod, "main")


def test_rolling_ic_handles_short_series():
    """Series too short to roll must return an empty list, not raise."""
    mod = _load_oos_module()
    result = mod.rolling_ic(forward_rows=[], market="us", horizon="3m", window_months=36, step_months=3)
    assert result == []
