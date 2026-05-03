"""Grid search over sub_weight combinations for one or more markets.

Writes results to `scripts/grid_search_results.json` as an append-only
audit trail. Each entry records:
  - timestamp + algo_version
  - the grid step + market scope
  - per-combination metrics: directional hit rate, strict hit rate, ρ_3m, ρ_6m

Why audit-only: any weight change in `config/weights.yaml` should cite
a specific grid_search_results.json entry (by index or hash) in its
commit message. This prevents post-hoc rationalization of weight tweaks.

Usage:
  uv run python scripts/grid_search_weights.py --market us --step 0.05
  uv run python scripts/grid_search_weights.py --all-markets --step 0.1
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import sys
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path

import pandas as pd
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from finsynapse.transform.normalize import collect_bronze, derive_indicators
from finsynapse.transform.percentile import compute_percentiles
from finsynapse.transform.temperature import MARKETS, WeightsConfig, compute_temperature
from finsynapse.transform.version import ALGO_VERSION
from scripts.validation_lib import compute_forward_returns, directional_ok, spearman_rho

SCRIPTS_DIR = Path(__file__).parent
RESULTS_PATH = SCRIPTS_DIR / "grid_search_results.json"
PIVOTS_PATH = SCRIPTS_DIR / "backtest_pivots.yaml"


def enumerate_weights(step: float = 0.05) -> list[tuple[float, float, float]]:
    """Yield (val, sent, liq) tuples summing to 1.0 on a step-spaced grid."""
    steps = round(1.0 / step)
    out: list[tuple[float, float, float]] = []
    for i in range(steps + 1):
        for j in range(steps + 1 - i):
            k = steps - i - j
            if k < 0:
                continue
            v = round(i * step, 4)
            s = round(j * step, 4)
            liq = round(k * step, 4)
            if v == 0 or s == 0 or liq == 0:
                continue  # skip degenerate single/double-factor combos
            out.append((v, s, liq))
    return out


@dataclass
class GridResult:
    valuation: float
    sentiment: float
    liquidity: float
    directional_rate: float | None
    strict_rate: float | None
    rho_3m: float | None
    rho_6m: float | None
    n_pivots_evaluated: int


def _load_pivots() -> list[dict]:
    with PIVOTS_PATH.open() as f:
        return yaml.safe_load(f)["pivots"]


def evaluate_weight(
    market: str,
    weight: tuple[float, float, float],
    cfg_template: WeightsConfig,
    macro,
    pct,
    pivots: list[dict],
) -> GridResult:
    """Re-compute temperature with the given (val, sent, liq) for `market`,
    score it against all pivots in that market, and compute forward IC."""
    cfg = copy.deepcopy(cfg_template)
    cfg.sub_weights[market] = {"valuation": weight[0], "sentiment": weight[1], "liquidity": weight[2]}
    temp = compute_temperature(pct, cfg)

    market_pivots = [p for p in pivots if p["market"] == market]
    directional_hits = 0
    strict_hits = 0
    evaluated = 0
    for p in market_pivots:
        target = date.fromisoformat(p["date"])
        sub = temp[temp["market"] == market].copy()
        sub["date"] = pd.to_datetime(sub["date"])
        sel = sub[sub["date"] <= pd.Timestamp(target)].sort_values("date").tail(1)
        if sel.empty:
            continue
        overall = sel.iloc[0]["overall"]
        evaluated += 1
        if directional_ok(overall, p["expected_zone"]):
            directional_hits += 1
        zone = p["expected_zone"]
        if (zone == "hot" and overall >= 70) or (zone == "cold" and overall < 30) or (zone == "mid" and 30 <= overall < 70):
            strict_hits += 1

    forward = compute_forward_returns(macro, temp[temp["market"] == market])
    rho_3m = spearman_rho(forward, market, "3m")
    rho_6m = spearman_rho(forward, market, "6m")

    return GridResult(
        valuation=weight[0],
        sentiment=weight[1],
        liquidity=weight[2],
        directional_rate=round(directional_hits / evaluated, 4) if evaluated else None,
        strict_rate=round(strict_hits / evaluated, 4) if evaluated else None,
        rho_3m=round(rho_3m, 4) if rho_3m is not None else None,
        rho_6m=round(rho_6m, 4) if rho_6m is not None else None,
        n_pivots_evaluated=evaluated,
    )


def _hash_run(payload: dict) -> str:
    blob = json.dumps(payload, sort_keys=True).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()[:12]


def main() -> int:
    parser = argparse.ArgumentParser(description="Sub_weight grid search for FinSynapse")
    parser.add_argument("--market", choices=[*list(MARKETS), "all"], default="us")
    parser.add_argument("--step", type=float, default=0.05)
    args = parser.parse_args()

    print("=" * 60)
    print(f"  FinSynapse — Grid Search ({args.market}, step={args.step})")
    print("=" * 60)
    print()

    macro = collect_bronze()
    if macro.empty:
        print("FAIL: no bronze data.")
        return 1
    macro = derive_indicators(macro)
    pct = compute_percentiles(macro)
    cfg_template = WeightsConfig.load()
    pivots = _load_pivots()

    targets = list(MARKETS) if args.market == "all" else [args.market]
    combos = enumerate_weights(args.step)
    print(f"[grid] {len(combos)} combinations per market × {len(targets)} markets = {len(combos) * len(targets)} runs")

    all_results: dict[str, list[dict]] = {}
    for market in targets:
        print(f"[grid] running {market} ...")
        rows: list[GridResult] = []
        for w in combos:
            rows.append(evaluate_weight(market, w, cfg_template, macro, pct, pivots))
        rows.sort(key=lambda r: (-(r.directional_rate or 0), r.rho_3m or 0))
        all_results[market] = [asdict(r) for r in rows]
        top = rows[:5]
        print("  Top 5 by directional_rate (then ρ_3m):")
        for r in top:
            print(
                f"    val={r.valuation:.2f} sent={r.sentiment:.2f} liq={r.liquidity:.2f}  "
                f"dir={r.directional_rate}  ρ3m={r.rho_3m}  ρ6m={r.rho_6m}"
            )

    run_payload = {
        "timestamp": date.today().isoformat(),
        "algo_version": ALGO_VERSION,
        "step": args.step,
        "markets": targets,
        "results": all_results,
    }
    run_payload["run_hash"] = _hash_run({k: v for k, v in run_payload.items() if k != "run_hash"})

    existing: list[dict] = []
    if RESULTS_PATH.exists():
        existing = json.loads(RESULTS_PATH.read_text())
    existing.append(run_payload)
    RESULTS_PATH.write_text(json.dumps(existing, indent=2, ensure_ascii=False))
    print()
    print(f"[audit] appended -> {RESULTS_PATH}  (run_hash={run_payload['run_hash']})")
    print("[audit] cite this hash in any future commit that changes config/weights.yaml")
    return 0


if __name__ == "__main__":
    sys.exit(main())
