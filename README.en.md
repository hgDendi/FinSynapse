<div align="right">

**English** | [中文](./README.md)

</div>

# FinSynapse 🌡️

> **Personal multi-market macro pipeline + market thermometer + optional LLM briefs** — a T+1 investment workbench.

[![CI](https://github.com/hgDendi/FinSynapse/actions/workflows/ci.yml/badge.svg)](https://github.com/hgDendi/FinSynapse/actions/workflows/ci.yml)
[![Daily](https://github.com/hgDendi/FinSynapse/actions/workflows/daily.yml/badge.svg)](https://github.com/hgDendi/FinSynapse/actions/workflows/daily.yml)
[![CodeQL](https://github.com/hgDendi/FinSynapse/actions/workflows/codeql.yml/badge.svg)](https://github.com/hgDendi/FinSynapse/actions/workflows/codeql.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](./LICENSE)

📊 **Live dashboard**: <https://hgdendi.github.io/FinSynapse/> ([中文](https://hgdendi.github.io/FinSynapse/) | [English](https://hgdendi.github.io/FinSynapse/en.html))

Refreshed daily at 06:00 Beijing (22:00 UTC the day before): composite thermometers for CN / HK / US, valuation / sentiment / liquidity sub-temperatures, weekly attribution, five hard-coded divergence pairs, and a daily macro brief.

---

## 1. What it is / isn't

**Is**:

- A cross-market (A-share / HK / US) macro data pipeline — Bronze / Silver / Gold layers, idempotent and replayable
- A market thermometer — three-dimensional weighted score (0–100°) over valuation / sentiment / liquidity
- Optional LLM-narrated briefs — runs Ollama / DeepSeek / Anthropic locally; CI never burns paid tokens or leaks keys
- Fully automated on GitHub Actions + GitHub Pages — **$0/month**

**Isn't**:

- Not a Bloomberg / Wind replacement
- Not a real-time trading or quote system
- Not a backtesting framework
- Single-stock coverage is deferred to Phase 4; the mainline is macro-only

---

## 2. 5-minute quickstart

```bash
git clone https://github.com/hgDendi/FinSynapse.git
cd FinSynapse

# uv is 10–100x faster than pip — see https://github.com/astral-sh/uv
uv sync --all-extras

cp .env.example .env
# Only FRED_API_KEY is recommended (free at https://fred.stlouisfed.org/docs/api/api_key.html).
# Without it, only the US liquidity sub-temperature loses one factor (DFII10).

# Pull data (default lookback 5500 days ≈ 15Y, ~5min on first run)
uv run finsynapse ingest all

# bronze → silver (percentile, health check, temperature, divergence)
uv run finsynapse transform run --layer all

# Render bilingual static dashboard to dist/
uv run finsynapse dashboard render

open dist/index.html        # macOS
# Or launch interactive Streamlit locally
uv run finsynapse dashboard serve
```

Optional: generate a daily LLM brief locally.

```bash
# After setting DEEPSEEK_API_KEY in .env
uv run finsynapse report brief --provider deepseek
# Writes data/gold/brief/YYYY-MM-DD.md — commit by hand to keep history.
```

---

## 3. Repository layout

```
FinSynapse/
├── .github/
│   ├── workflows/         CI / daily refresh / CodeQL workflows
│   └── ISSUE_TEMPLATE/    bug / feature / ci_failure templates
├── config/
│   └── weights.yaml       Thermometer weights — hot-editable, takes effect immediately (see §5)
├── data/                  ── Medallion layers ──
│   ├── bronze/            Raw API output (gitignored, rebuilt by CI)
│   ├── silver/            Cleaned + derived (gitignored, rebuilt by CI)
│   └── gold/              Narrative artefacts (committed; git history = thinking history)
│       └── brief/         Daily macro briefs in .md (auto-committed by CI)
├── dist/                  Bilingual static dashboard build target (gitignored, pushed to gh-pages by CI)
├── docs/
│   └── _local/            Personal drafts and execution plans (gitignored)
├── scripts/               Upstream API probes (probe_*.py) — for reproducing breakage
├── src/finsynapse/
│   ├── config.py          pydantic-settings reads .env
│   ├── cli.py             Typer entry point (ingest / transform / dashboard / notify / report)
│   ├── providers/         Data source abstractions: akshare / yfinance / fred / multpl / treasury
│   ├── ingest/            bronze writes (thin shell)
│   ├── transform/         normalize → percentile → health_check → temperature → divergence
│   ├── dashboard/         Streamlit app + bilingual static HTML (i18n + plotly)
│   ├── notify/            Bark / Telegram state-change alerts
│   └── report/            brief.py (LLM-first, template fallback)
├── tests/                 pytest + VCR offline tests (CI never hits the network)
├── pyproject.toml         deps, ruff rules, Python 3.11+
├── .env.example           Secret inventory
└── LICENSE                MIT
```

---

## 4. Data layers

```
bronze/  raw: API output + timestamp, idempotent overwrite
   ↓     (rebuilt daily by CI at 22:00 UTC, gitignored)
silver/  derived: clean → percentile → health → temperature → divergence
   ↓     (also uploaded as a 30-day artifact for replay)
gold/    narrative: human/LLM-readable conclusions
         (committed; `git log` is your thinking timeline)
```

| Layer | Writer | Committed? | Key files |
|---|---|---|---|
| bronze | `finsynapse ingest` | ❌ | `data/bronze/{macro,flow,valuation}/<source>_<date>.parquet` |
| silver | `finsynapse transform run` | ❌ | `data/silver/{macro,percentile,temperature,divergence,health_log}_daily.parquet` |
| gold | LLM or template (CI / local) | ✅ | `data/gold/brief/YYYY-MM-DD.md` |

---

## 5. Dashboard semantics

### 5.1 Market thermometer (0–100°)

Each indicator is first converted to its **trailing 10-year rolling percentile** (window controlled by `percentile_window` in [`config/weights.yaml`](./config/weights.yaml); monthly indicators like CAPE / M2 are forward-filled to daily).

Within each sub-temperature the `direction` field decides sign:

- `+` : high percentile → high temperature (e.g. CAPE high = expensive = hot)
- `-` : high percentile → low temperature (e.g. VIX high = fear = cold; strong DXY = tight liquidity = cold)

Sub-temperature = weighted average across that block's indicators. **Missing indicators auto-renormalize across the available weights** — so when HK options PCR has no source (see §5.5), the rest of HK sentiment still holds up.

The composite temperature combines sub-temperatures with per-market weights:

| Market | valuation | sentiment | liquidity | Rationale |
|---|---:|---:|---:|---|
| CN | 0.50 | 0.30 | 0.20 | Valuation-led; M2 / social financing are slow movers |
| HK | 0.60 | 0.25 | 0.15 | Offshore valuation anchor is strongest; sentiment weight reduced absent PCR |
| US | 0.40 | 0.35 | 0.25 | CAPE/PE saturated near highs — VIX + real-rate components add edge |

**Temperature bands** ([`src/finsynapse/notify/state.py`](./src/finsynapse/notify/state.py)):

- `< 30°` ❄️ cold
- `30–70°` 🌤 mid
- `≥ 70°` 🔥 hot

The full indicator → sub-temperature mapping lives in [`config/weights.yaml`](./config/weights.yaml). Editing it and rerunning `transform run --layer temperature` is enough — **no need to re-ingest bronze**, since percentile baselines don't depend on the weights.

### 5.2 Weekly attribution

The 7-day `Δoverall` is decomposed into `Δval / Δsent / Δliq` contributions in both the dashboard and the brief. **No dynamic weights** — once weights are set they're frozen, so directional changes come purely from the indicators themselves (avoiding curve-fitting).

### 5.3 Divergence signals

Five hard-coded `SignalPair`s ([`src/finsynapse/transform/divergence.py`](./src/finsynapse/transform/divergence.py)):

| Pair | Normal | Divergent meaning |
|---|---|---|
| `sp500_vix` | inverse | rally + rising fear → suspicious breakout |
| `us10y_dxy` | same | yields up, dollar down → credit / inflation regime shift? |
| `gold_real_rate` | inverse | both up → safe-haven / de-dollar dominates |
| `hsi_southbound` | same | HSI up but mainland flow out → foreign-led, no local follow-through |
| `csi300_volume` | same | up on falling volume → distribution warning |

`strength = |a%Δ - b%Δ| × inverse_factor`, bucketed into four tiers (≥ 0.5 / 0.1 / 0.01 / other).

> Five hard-coded pairs instead of statistical anomaly detection — each pair carries explicit financial meaning; over-generalization would drown signal in noise.

### 5.4 Data health

Every indicator has a plausibility bound in [`src/finsynapse/transform/health_check.py`](./src/finsynapse/transform/health_check.py) (e.g. `vix: 5–200`, `us10y_yield: 0.1–25`, `csi300: 1000–20000`):

- Out of bounds → `fail`, row dropped
- Jump > 5σ → `warn`, row kept but logged in `health_log.parquet`

Intent: catch unit drift / parsing bugs (e.g. price suddenly 100×), not "extreme but legitimate" market moves (those are exactly what the percentile machinery is for).

### 5.5 Quality flags

The `data_quality` field on `temperature_daily.parquet`:

- `ok` — all sub-components present
- `partial` — at least one sub-component missing for the day
- `pcr_unavailable` — HK options PCR has no free, stable source ([Phase 0 conclusion](./docs/_local/2026-04-29-execution-plan.md), v0.6); HK sentiment falls back to 100% southbound 5d, temperature still usable

### 5.6 Daily brief (gold/brief)

`finsynapse report brief` priority: `auto` mode tries `ollama → deepseek → anthropic` in order, **falling back to a deterministic Jinja template if all fail** — the output is always a valid `.md`.

CI defaults to `deepseek-v4-pro` (priced like v4-flash through 2026-05-31; revert to v4-flash afterwards). Locally, `--provider ollama` runs offline at zero cost.

---

## 6. GitHub Actions & branch strategy

### 6.1 Three workflows

| Workflow | Trigger | Job | Write scope |
|---|---|---|---|
| [`ci.yml`](./.github/workflows/ci.yml) | push main / PR | ruff lint + format check + pytest (py 3.11/3.12 matrix) | read-only |
| [`daily.yml`](./.github/workflows/daily.yml) | cron `0 22 * * *` UTC (06:00 BJT) + manual | ingest → transform → brief → render → push brief back to main + push dist to gh-pages + notify + upload silver artifact | `contents:write` + `issues:write` |
| [`codeql.yml`](./.github/workflows/codeql.yml) | push / PR / Mon 03:00 UTC | Python static security & quality (`security-and-quality`) | `security-events:write` |

`daily.yml` **opens an issue automatically on failure** (label `ci-failure`) listing common culprits and the rerun entry — upstream API breakages (AkShare / multpl / yfinance) almost always surface here first.

### 6.2 Branch responsibilities

| Branch | Role | Writers |
|---|---|---|
| `main` | source + config + tests + public docs + `data/gold/brief/*.md` | humans + `daily.yml` (auto-commit + rebase retry ×3) |
| `gh-pages` | static site only (`dist/` content); `force_orphan` keeps history clean | `daily.yml` exclusively — **never touch locally** |
| `feature/*` | every change opens a new branch → PR → main | contributors |

### 6.3 Data update strategy

- bronze / silver are **always gitignored** and fully re-pulled / rebuilt daily (lookback 5500 days ≈ 15Y) — guarantees idempotent replay
- `data/gold/brief/*.md` is committed → narratives are diffable / `git blame`-able over time
- silver is also uploaded as a **30-day artifact** (`silver-<run_id>.zip`) for emergency replay or local reproduction

### 6.4 Secrets policy

| Secret | Required? | Purpose |
|---|---|---|
| `FRED_API_KEY` | recommended | US real yield (DFII10) |
| `DEEPSEEK_API_KEY` | optional | Daily brief in CI; falls back to template if absent |
| `BARK_DEVICE_KEY` | optional | iOS push |
| `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` | optional | Telegram push |

🚫 **`ANTHROPIC_API_KEY` and any premium paid LLM key NEVER enter CI.** All paid LLM calls run locally to keep costs bounded and keys off the CI surface.

---

## 7. Local development

```bash
# Install with dev extras
uv sync --all-extras

# Help on any subcommand
uv run finsynapse --help
uv run finsynapse transform --help

# Offline tests (VCR / fixtures; if anything ever hits the network, CI times out — by design)
uv run pytest -q

# Run before every commit
uv run ruff check src tests
uv run ruff format --check src tests

# Interactive dashboard locally
uv run finsynapse dashboard serve --port 8501
```

A more detailed local → CI deployment walkthrough lives in [`docs/_local/2026-04-29-deploy-guide.md`](./docs/_local/2026-04-29-deploy-guide.md) (gitignored — author's local copy only).

---

## 8. Contributing

PRs welcome. Before submitting, please run through the checks below.

### 8.1 Issue entry points

The repository ships with [issue templates](./.github/ISSUE_TEMPLATE/):

- 🐛 **Bug report** — behaviour does not match expectation
- ✨ **Feature request** — new indicator / market / dashboard view
- 🔥 **CI failure** — usually opened automatically by `daily.yml`; rarely needs manual filing

### 8.2 PR flow

1. Fork → branch `feature/<short-slug>`
2. Pass `uv run ruff check && uv run ruff format --check && uv run pytest -q` locally
3. Open PR → main; CI must be **fully green** before review
4. PR description should cover *what changed*, *why*, *how to verify*

### 8.3 Commit convention

Use type prefixes: `feat / fix / chore / docs / test / refactor / ci`. Body in English or Chinese is fine.

```
feat(temperature): add HK index PCR via HKEX scrape
fix(akshare): handle empty north-flow response post-2024-08
chore(deps): bump pandas to 3.0.3
```

### 8.4 Code style

- ruff line-length 120, configured in [`pyproject.toml`](./pyproject.toml); enable format-on-save in your editor
- **New provider**: implement the [`providers/base.py`](./src/finsynapse/providers/base.py) interface, write to bronze, `return (df, path)`; ship a VCR cassette or pickle fixture
- **New silver transform**: wire it into [`cli.py`](./src/finsynapse/cli.py) `transform run` and add a corresponding pytest
- **New thermometer indicator**:
  1. Add weights in [`config/weights.yaml`](./config/weights.yaml) (sub-block must sum to 1.0)
  2. Add bounds in [`health_check.PLAUSIBLE_BOUNDS`](./src/finsynapse/transform/health_check.py)
  3. Run [`scripts/backtest_temperature.py`](./scripts/backtest_temperature.py) to confirm direction holds at known checkpoints

### 8.5 Test requirements

- Provider PRs **must** include a VCR cassette or pickle fixture (CI never hits the network)
- Transform changes **must** include unit tests covering the new behaviour
- Anything that turns the existing 12+ tests red must be justified in the PR description

### 8.6 Will not accept

- Storing `ANTHROPIC_API_KEY` / `OPENAI_API_KEY` or other paid LLM keys in workflows or GitHub Secrets
- Introducing a paid-only data source that breaks the $0/month guarantee
- Adding a frontend framework beyond Streamlit (the "single static HTML on GitHub Pages" minimal-dependency principle)
- Direct pushes of `data/bronze/` or `data/silver/` onto `main` (gitignore protects this — please don't force-push around it)

---

## 9. License & acknowledgements

[MIT](./LICENSE) © 2026 hg.dendi

Data and ecosystem credits:

- **AkShare** — A-share / HK / macro data
- **yfinance** — US and cross-market quotes
- **FRED** — US macro time series
- **multpl.com** — Shiller CAPE history
- **DeepSeek / Anthropic / Ollama** — LLM narrative generation
- **uv / ruff / pytest** — astral-sh toolchain
