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

## 4.5 JSON API endpoints

Each daily build publishes machine-readable JSON endpoints alongside the dashboard, so external agents / tooling can consume temperature data without scraping HTML:

| Endpoint | Contents |
|---|---|
| `/api/manifest.json` | Schema version + asof + endpoint inventory |
| `/api/temperature_latest.json` | Per-market latest overall + sub-temps + 1-week change attribution |
| `/api/temperature_history.json.gz` | Full historical time series (gzipped) |
| `/api/indicators_latest.json` | All underlying factors' latest values + 5y/10y rolling percentiles |
| `/api/divergence_latest.json` | Active divergence signals from the last 90 days, sorted by strength |

Live: `https://hgdendi.github.io/FinSynapse/api/manifest.json`

Schema version follows SemVer: adding fields is non-breaking (no bump); removing or changing field semantics bumps major.

---

## 5. Dashboard semantics

### 5.1 Market thermometer (0–100°)

Each indicator is first converted to its **trailing N-year rolling percentile**. **Window is per-indicator overridable** (`window:` field in [`config/weights.yaml`](./config/weights.yaml)): slow fundamentals (PE / CAPE / M2 / social financing / real yield) default to `pct_10y` to anchor long-term mean reversion, while fast indicators (VIX / credit spreads / flows / DXY / short rates) use `pct_5y` to reflect current regime. Monthly/weekly indicators (CAPE / M2 / social financing / NFCI) are forward-filled to daily automatically.

Within each sub-temperature the `direction` field decides sign:

- `+` : high percentile → high temperature (e.g. CAPE high = expensive = hot)
- `-` : high percentile → low temperature (e.g. VIX high = fear = cold; strong DXY = tight liquidity = cold)

Sub-temperature = weighted average across that block's indicators. **Missing indicators auto-renormalize across the available weights** — so when CN northbound stops publishing post-2024-08, the rest of CN sentiment still holds up (see §5.6).

The composite temperature combines sub-temperatures with per-market weights:

| Market | valuation | sentiment | liquidity | Rationale |
|---|---:|---:|---:|---|
| CN | 0.65 | 0.20 | 0.15 | Valuation-led; sentiment 4-factor (north / turnover / margin / CNY pressure); M2 + social financing + credit impulse + SHIBOR-1W |
| HK | 0.60 | 0.25 | 0.15 | EWH yield as valuation anchor; southbound+VHSI sentiment; HIBOR-1M + US real yield/DXY for liquidity |
| US | 0.35 | 0.45 | 0.20 | PE+CAPE+ERP valuation; VIX+HY OAS+UMich sentiment; real yield+DXY+NFCI+WALCL liquidity |

**Temperature bands** ([`src/finsynapse/notify/state.py`](./src/finsynapse/notify/state.py)):

- `< 30°` ❄️ cold
- `30–70°` 🌤 mid
- `≥ 70°` 🔥 hot

The full indicator → sub-temperature mapping is in §5.2 and [`config/weights.yaml`](./config/weights.yaml). Editing it and rerunning `transform run --layer temperature` is enough — **no need to re-ingest bronze**, since percentile baselines don't depend on the weights.

Backtest verification ([`scripts/backtest_temperature.py`](./scripts/backtest_temperature.py) + [`scripts/run_validation.py`](./scripts/run_validation.py)): loads 25 historical pivots (US 9, CN 8, HK 8) from [`backtest_pivots.yaml`](./scripts/backtest_pivots.yaml). Gate requires multi-factor to beat PE single-factor in ≥2/3 markets AND show negative Spearman ρ (mean-reversion signal). Currently gate 3/3 PASS.

### 5.2 Indicator inventory

Each market's three sub-temperatures are weighted combinations of base indicators. **Direction**: `+` = high percentile → hot; `-` = inverse. **Window**: `5y` for fast-regime indicators, `10y` for slow fundamentals.

#### US (composite 0.35 val + 0.45 sent + 0.20 liq)

| Sub | Indicator | Weight | Dir | Window | Source | Notes |
|---|---|---|---:|---|---|---|---|
| val | `us_pe_ttm` | 0.35 | + | 10y | multpl.com | S&P500 TTM PE |
| val | `us_cape` | 0.35 | + | 10y | multpl.com | Shiller 10Y-smoothed EPS |
| val | `us_erp` | 0.30 | − | 10y | derived | `100/PE − real yield` |
| sent | `vix` | 0.40 | − | 5y | yfinance | implied vol = fear |
| sent | `us_hy_oas` | 0.35 | − | 5y | FRED `BAMLH0A0HYM2` | HY credit spread |
| sent | `us_umich_sentiment` | 0.25 | + | 10y | FRED `UMCSENT` | U. Michigan consumer sentiment |
| liq | `us10y_real_yield` | 0.25 | − | 10y | FRED `DFII10` | high real rate = tight |
| liq | `dxy` | 0.15 | − | 5y | yfinance | strong USD = tight global liquidity |
| liq | `us_nfci` | 0.35 | − | 5y | FRED `NFCI` | Chicago Fed financial conditions |
| liq | `us_walcl` | 0.25 | + | 5y | FRED `WALCL` | Fed balance sheet (QE/QT cycle) |

#### CN (composite 0.65 val + 0.20 sent + 0.15 liq)

| Sub | Indicator | Weight | Dir | Window | Source | Notes |
|---|---|---|---:|---|---|---|---|
| val | `csi300_pe_ttm` | 0.50 | + | 10y | AkShare | CSI300 TTM PE |
| val | `csi300_pb` | 0.50 | + | 10y | AkShare | CSI300 PB |
| sent | `cn_north_5d` | 0.25 | + | 5y | AkShare | northbound 5d net buy |
| sent | `cn_a_turnover_5d` | 0.25 | + | 5y | AkShare | A-share total turnover 5d mean |
| sent | `cn_margin_balance` | 0.35 | + | 5y | AkShare | SH+SZ margin balance |
| sent | `cn_usdcny_pressure` | 0.15 | − | 5y | derived | USD/CNY → RMB stress, high = outflow pressure = cold |
| liq | `cn_m2_yoy` | 0.25 | + | 10y | AkShare | M2 YoY |
| liq | `cn_social_financing_12m` | 0.25 | + | 10y | AkShare | social financing 12M rolling sum |
| liq | `cn_credit_impulse` | 0.25 | + | 5y | derived | SF YoY acceleration — captures credit expansion/deceleration |
| liq | `cn_dr007` | 0.25 | − | 5y | AkShare | actually SHIBOR-1W |

#### HK (composite 0.60 val + 0.25 sent + 0.15 liq)

| Sub | Indicator | Weight | Dir | Window | Source | Notes |
|---|---|---|---:|---|---|---|---|
| val | `hk_ewh_yield_ttm` | 1.00 | − | 10y | yfinance EWH | TTM dividend yield (high = cheap = cold) |
| sent | `cn_south_5d` | 0.60 | + | 5y | AkShare | southbound 5d net buy |
| sent | `hk_vhsi` | 0.40 | − | 5y | AkShare | HSI Volatility Index (HK native VIX) |
| liq | `us10y_real_yield` | 0.30 | − | 10y | FRED `DFII10` | borrowed via USD peg |
| liq | `dxy` | 0.20 | − | 5y | yfinance | borrowed via USD peg |
| liq | `hk_hibor_1m` | 0.50 | − | 5y | AkShare | HKD-side funding cost |

> **HK native valuation not yet available**: AkShare `stock_hk_index_value_em` does not exist in the current version; `stock_hk_index_daily_em` returns only price data. HSI PE/PB would require scraping HSI factsheet PDFs (same fragility tier as the abandoned HSI PCR attempt). EWH dividend yield serves as the valuation proxy for now. See `scripts/probe_hk_valuation.py`.

### 5.3 Weekly attribution

The 7-day `Δoverall` is decomposed into `Δval / Δsent / Δliq` contributions in both the dashboard and the brief. **No dynamic weights** — once weights are set they're frozen, so directional changes come purely from the indicators themselves (avoiding curve-fitting).

### 5.4 Divergence signals

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

### 5.5 Data health

Every indicator has a plausibility bound in [`src/finsynapse/transform/health_check.py`](./src/finsynapse/transform/health_check.py) (e.g. `vix: 5–200`, `us10y_yield: 0.1–25`, `csi300: 1000–20000`):

- Out of bounds → `fail`, row dropped
- Jump > 5σ → `warn`, row kept but logged in `health_log.parquet`

Intent: catch unit drift / parsing bugs (e.g. price suddenly 100×), not "extreme but legitimate" market moves (those are exactly what the percentile machinery is for).

### 5.6 Quality flags

The `data_quality` field on `temperature_daily.parquet` records actual availability per row without blocking output:

- `ok` — all three sub-temperatures produced
- `<sub>_unavailable` — that sub-temperature had every input missing for the day (e.g. `liquidity_unavailable`)
- Single-indicator gaps inside a sub-temperature: weights auto-renormalize across what's available, no flag emitted (see §5.1 — e.g. CN sentiment 0.35/0.25/0.40 → 0/0.38/0.62 after northbound stopped publishing 2024-08)

### 5.7 Daily brief (gold/brief)

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
| `FRED_API_KEY` | recommended | US real yield (DFII10) + HY credit spread (BAMLH0A0HYM2, FRED returns 3Y rolling only) + financial conditions (NFCI, full history) |
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
  1. Add weights in [`config/weights.yaml`](./config/weights.yaml) (sub-block must sum to 1.0; optional `window: pct_5y` overrides the default — recommend 5y for fast-regime indicators)
  2. Add bounds in [`health_check.PLAUSIBLE_BOUNDS`](./src/finsynapse/transform/health_check.py) (catches unit drift)
  3. Run [`scripts/backtest_temperature.py`](./scripts/backtest_temperature.py) to confirm direction holds at known checkpoints
  4. Derived indicators (computed from other indicators, e.g. `us_erp`) live in [`transform/normalize.py:derive_indicators()`](./src/finsynapse/transform/normalize.py)
  5. Indicators with uncertain upstream APIs: write a `scripts/probe_*.py` first to validate the call before implementing the provider (see `probe_phase_b.py`)

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
