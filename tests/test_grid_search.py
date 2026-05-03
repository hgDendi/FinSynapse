from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts" / "grid_search_weights.py"


def _load_grid_module():
    spec = importlib.util.spec_from_file_location("grid_search_weights", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["grid_search_weights"] = mod
    spec.loader.exec_module(mod)
    return mod


def test_grid_search_module_loads_and_exposes_helpers():
    mod = _load_grid_module()
    assert hasattr(mod, "enumerate_weights")
    assert hasattr(mod, "evaluate_weight")
    assert hasattr(mod, "main")


def test_enumerate_weights_sums_to_one_within_tolerance():
    mod = _load_grid_module()
    combos = list(mod.enumerate_weights(step=0.1))
    assert combos, "grid must produce at least one combination"
    for v, s, liq in combos:
        assert abs(v + s + liq - 1.0) < 1e-6, (v, s, liq)
