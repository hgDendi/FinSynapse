<div align="right">

[English](./README.en.md) | **中文**

</div>

# FinSynapse 🌡️

> **个人多市场宏观流水线 + 市场温度计 + 可选 LLM 简评**，T+1 投资工作台。

[![CI](https://github.com/hgDendi/FinSynapse/actions/workflows/ci.yml/badge.svg)](https://github.com/hgDendi/FinSynapse/actions/workflows/ci.yml)
[![Daily](https://github.com/hgDendi/FinSynapse/actions/workflows/daily.yml/badge.svg)](https://github.com/hgDendi/FinSynapse/actions/workflows/daily.yml)
[![CodeQL](https://github.com/hgDendi/FinSynapse/actions/workflows/codeql.yml/badge.svg)](https://github.com/hgDendi/FinSynapse/actions/workflows/codeql.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](./LICENSE)

📊 **在线看板**：<https://hgdendi.github.io/FinSynapse/>（[中文](https://hgdendi.github.io/FinSynapse/) ｜ [English](https://hgdendi.github.io/FinSynapse/en.html)）

每日北京时间 06:00 自动更新：CN / HK / US 三市场综合温度 + 估值 / 情绪 / 流动性子分量 + 一周变化归因 + 五对硬编码背离信号 + 日度宏观简评。

---

## 1. 这是什么 / 不是什么

**是**：

- 跨市场（A/HK/US）宏观数据流水线 — Bronze/Silver/Gold 分层，幂等可重放
- 市场温度计 — 估值 / 情绪 / 流动性三维加权打分（0-100°）
- 可选 LLM 简评 — 本地跑 Ollama / DeepSeek / Anthropic，CI 不付费、不掉 key
- GitHub Actions 全自动跑通 + GitHub Pages 静态发布，**总成本 $0/月**

**不是**：

- 不是 Bloomberg / Wind 替代
- 不是实时交易 / 行情系统
- 不是回测框架
- 个股层延后到 Phase 4，主线只跑宏观

---

## 2. 5 分钟快速上手

```bash
git clone https://github.com/hgDendi/FinSynapse.git
cd FinSynapse

# uv 比 pip 快 10-100x，详见 https://github.com/astral-sh/uv
uv sync --all-extras

cp .env.example .env
# .env 里只有 FRED_API_KEY 是推荐项（免费注册 https://fred.stlouisfed.org/docs/api/api_key.html）
# 没填也能跑，仅美股流动性子温度会少一个 DFII10 因子

# 拉数据（默认 lookback 5500 天 ≈ 15 年，首次 ~5 分钟）
uv run finsynapse ingest all

# bronze → silver（含百分位、健康检查、温度、背离）
uv run finsynapse transform run --layer all

# 渲染双语静态看板到 dist/
uv run finsynapse dashboard render

open dist/index.html        # macOS
# 或本地起 Streamlit 做交互式探索
uv run finsynapse dashboard serve
```

可选：本地跑 LLM 日报。

```bash
# .env 填好 DEEPSEEK_API_KEY 后
uv run finsynapse report brief --provider deepseek
# 产出 data/gold/brief/YYYY-MM-DD.md，可手动 commit 入库
```

---

## 3. 仓库结构

```
FinSynapse/
├── .github/
│   ├── workflows/         CI / 每日更新 / CodeQL 三个 workflow
│   └── ISSUE_TEMPLATE/    bug / feature / ci_failure 模板
├── config/
│   └── weights.yaml       温度计权重，可热改、立即生效（见 §5）
├── data/                  ── Medallion 分层 ──
│   ├── bronze/            原始 API 直出（gitignored，CI 重建）
│   ├── silver/            清洗 + 加工（gitignored，CI 重建）
│   └── gold/              叙事产物（入库，git 历史 = 认知历史）
│       └── brief/         每日宏观简评 .md（CI 自动 commit）
├── dist/                  双语静态看板渲染目标（gitignored，CI 推 gh-pages）
├── docs/
│   └── _local/            个人草稿与执行规划（gitignored）
├── scripts/               上游 API 探针（probe_*.py，upstream 改时复现）
├── src/finsynapse/
│   ├── config.py          pydantic-settings 读 .env
│   ├── cli.py             Typer 入口（ingest / transform / dashboard / notify / report）
│   ├── providers/         数据源抽象：akshare / yfinance / fred / multpl / treasury
│   ├── ingest/            bronze 落盘（薄壳）
│   ├── transform/         normalize → percentile → health_check → temperature → divergence
│   ├── dashboard/         Streamlit app + 双语静态 HTML 渲染（i18n + plotly）
│   ├── notify/            Bark / Telegram 状态变化推送
│   └── report/            brief.py（LLM 优先，模板兜底）
├── tests/                 pytest + VCR 离线测试（CI 不发真实请求）
├── pyproject.toml         依赖、ruff 规则、Python 3.11+
├── .env.example           secret 清单
└── LICENSE                MIT
```

---

## 4. 数据分层

```
bronze/  原始：API 直出 + 时间戳，幂等覆盖
   ↓     （CI 每日 22:00 UTC 重建，gitignored）
silver/  加工：清洗 → 百分位 → 健康检查 → 温度 → 背离
   ↓     （同步 30 天 artifact 备份）
gold/    叙事：人类/LLM 读得懂的结论
         （入库；git log = 你的认知演化时间线）
```

| 层 | 写入方 | 是否入库 | 关键文件 |
|---|---|---|---|
| bronze | `finsynapse ingest` | ❌ | `data/bronze/{macro,flow,valuation}/<source>_<date>.parquet` |
| silver | `finsynapse transform run` | ❌ | `data/silver/{macro,percentile,temperature,divergence,health_log}_daily.parquet` |
| gold | LLM 或模板（CI / 本地） | ✅ | `data/gold/brief/YYYY-MM-DD.md` |

---

## 5. 看板数值口径

### 5.1 市场温度计（0-100°）

每个 indicator 取**过去 10 年滚动百分位**作为基础信号（窗口由 [`config/weights.yaml`](./config/weights.yaml) 的 `percentile_window` 控制；月频指标如 CAPE / M2 自动 ffill 到日度）。

每个子温度的 indicator 用 `direction` 字段决定方向：

- `+` ：高分位 → 高温（如 CAPE 高 = 估值热）
- `-` ：高分位 → 低温（如 VIX 高 = 恐慌 = 冷；DXY 强 = 流动性紧 = 冷）

子温度 = 该子项下各 indicator 的加权平均；**任一指标缺失会自动按可用权重重归一**（如 HK 期权 PCR 无源时不会拉垮整个 HK 情绪温度，详见 §5.5）。

综合温度按市场各自的子权重合成：

| 市场 | valuation | sentiment | liquidity | 设计依据 |
|---|---:|---:|---:|---|
| CN | 0.50 | 0.30 | 0.20 | A 股估值定调，社融 / M2 慢变量 |
| HK | 0.60 | 0.25 | 0.15 | 离岸盘估值锚最强，缺 PCR 后情绪权重让出 |
| US | 0.40 | 0.35 | 0.25 | CAPE/PE 估值高位钝化，VIX + 实际利率分量更敏感 |

**温度区间**（[`src/finsynapse/notify/state.py`](./src/finsynapse/notify/state.py)）：

- `< 30°` ❄️ 冷
- `30 - 70°` 🌤 中
- `≥ 70°` 🔥 热

完整指标 → 子温度映射在 [`config/weights.yaml`](./config/weights.yaml)，改完 `transform run --layer temperature` 立刻生效，**不需要重抓 bronze**（百分位基线本身不依赖权重）。

### 5.2 一周温度变化归因

把过去 7 日 `Δoverall` 拆成 `Δval / Δsent / Δliq` 各自贡献，看板和简评里都列出来。**不用动态权重**——避免曲线拟合，权重一旦订就锁死，方向变化只来自指标本身。

### 5.3 背离信号

固定 5 对硬编码 `SignalPair`（[`src/finsynapse/transform/divergence.py`](./src/finsynapse/transform/divergence.py)）：

| 信号对 | 正常关系 | 背离含义 |
|---|---|---|
| `sp500_vix` | 反向 | 涨同时恐慌也涨 → 警惕假突破 |
| `us10y_dxy` | 同向 | 利率上美元下 → 信用 / 通胀机制切换？ |
| `gold_real_rate` | 反向 | 实际利率上金价也上 → 避险 / 去美元化主导 |
| `hsi_southbound` | 同向 | 港股涨但南向流出 → 外资主导，缺内地接力 |
| `csi300_volume` | 同向 | 上涨但缩量 → 警惕派发 |

`strength = |a%Δ - b%Δ| × 反向系数`，分四档（≥ 0.5 / 0.1 / 0.01 / 其他）。

> 选硬编码 5 对而非统计异常检测——每对都有明确金融含义，过度通用化会淹没信号。

### 5.4 数据健康

每个 indicator 在 [`src/finsynapse/transform/health_check.py`](./src/finsynapse/transform/health_check.py) 有 plausibility bound（如 `vix: 5-200`、`us10y_yield: 0.1-25`、`csi300: 1000-20000`）：

- 越界 → `fail`，丢弃该行
- 跳跃 > 5σ → `warn`，保留但记录到 `health_log.parquet`

设计意图：抓 unit drift / 解析错误（如某天价格 ×100），不抓「极端但合法」的行情移动（那是百分位机器要识别的）。

### 5.5 数据降级标记

`temperature_daily.parquet` 的 `data_quality` 字段：

- `ok` — 全部子分量齐
- `partial` — 某个子分量当天缺数据
- `pcr_unavailable` — HK 期权 PCR 无免费稳定源（[Phase 0 验证结论](./docs/_local/2026-04-29-execution-plan.md) 见 v0.6），HK 情绪降级为 100% 南向 5d，温度仍可用

### 5.6 日报（gold/brief）

`finsynapse report brief` 优先级：`auto` 模式按 `ollama → deepseek → anthropic` 顺序尝试，**全失败时落到 deterministic Jinja 模板**，永远产出有效 .md。

CI 默认用 `deepseek-v4-pro`（2026-05-31 前折扣同 v4-flash 价位；过期后改回 v4-flash）。本地可以用 `--provider ollama` 跑离线模型零成本。

---

## 6. GitHub Actions 与分支策略

### 6.1 三个 workflow

| Workflow | 触发 | 职责 | 写权限 |
|---|---|---|---|
| [`ci.yml`](./.github/workflows/ci.yml) | push main / PR | ruff lint + format check + pytest（py 3.11/3.12 矩阵） | 只读 |
| [`daily.yml`](./.github/workflows/daily.yml) | cron `0 22 * * *` UTC（北京 06:00） + 手动 | ingest → transform → brief → render → 推 brief 回 main + 推 dist 到 gh-pages + notify + 上传 silver artifact | `contents:write` + `issues:write` |
| [`codeql.yml`](./.github/workflows/codeql.yml) | push / PR / 周一 03:00 UTC | Python 静态安全 / 质量分析（`security-and-quality`） | `security-events:write` |

`daily.yml` 失败会**自动开 issue**（label `ci-failure`），列出常见 culprit 与重跑入口；任何上游 API 改动（AkShare / multpl / yfinance）通常都先在这里浮现。

### 6.2 分支职责

| 分支 | 职责 | 写入方 |
|---|---|---|
| `main` | 源码 + 配置 + tests + 公开 docs + `data/gold/brief/*.md` | 人 + `daily.yml`（自动 commit + rebase 重试 ×3） |
| `gh-pages` | 纯静态站点，`dist/` 内容；`force_orphan` 不留历史 | `daily.yml` 唯一写入，**本地永不动** |
| `feature/*` | 任何变更都开新分支 → PR → main | 协作者 |

### 6.3 数据更新策略

- bronze / silver **永远 gitignored**，每天 CI 全量重抓重建（lookback 5500 天 ≈ 15 年），保证幂等可重放
- `data/gold/brief/*.md` 入库 → 历史可比对、可 git blame 追溯叙事变化
- silver 同时上传成 **30 天 artifact**（`silver-<run_id>.zip`），应急回放或本地复现用

### 6.4 Secrets 红线

| Secret | 必需？ | 用途 |
|---|---|---|
| `FRED_API_KEY` | 推荐 | 美股实际利率（DFII10） |
| `DEEPSEEK_API_KEY` | 可选 | CI 里跑日报；不填用模板兜底 |
| `BARK_DEVICE_KEY` | 可选 | iOS 推送 |
| `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` | 可选 | TG 推送 |

🚫 **`ANTHROPIC_API_KEY` 与任何高价付费 LLM key 永不入 CI**。所有付费 LLM 调用一律本地跑，避免成本失控与 key 暴露。

---

## 7. 本地开发

```bash
# 装含 dev 依赖
uv sync --all-extras

# 任何子命令查 help
uv run finsynapse --help
uv run finsynapse transform --help

# 跑离线测试（VCR/fixture，不发真实请求；如果哪天联网了，CI 会超时报错——by design）
uv run pytest -q

# 提交前必跑
uv run ruff check src tests
uv run ruff format --check src tests

# 本地交互式看板
uv run finsynapse dashboard serve --port 8501
```

详细的本地 → CI 部署指引见 [`docs/_local/2026-04-29-deploy-guide.md`](./docs/_local/2026-04-29-deploy-guide.md)（gitignored，仅作者本地可见）。

---

## 8. CONTRIBUTING

欢迎 PR。提交前请过下面这些。

### 8.1 Issue 入口

仓库已配 [issue 模板](./.github/ISSUE_TEMPLATE/)：

- 🐛 **Bug report** — 行为不符合预期
- ✨ **Feature request** — 想加新指标 / 新市场 / 新看板
- 🔥 **CI failure** — 通常 daily.yml 会自动开，无需手动

### 8.2 PR 流程

1. fork → 开 `feature/<short-slug>` 分支
2. 跑通 `uv run ruff check && uv run ruff format --check && uv run pytest -q`
3. 提 PR → main，CI 必须**全绿**才进 review
4. PR 描述里说清「改了什么 + 为什么 + 怎么验证」

### 8.3 Commit 规范

使用类型前缀：`feat / fix / chore / docs / test / refactor / ci`，正文中文 / 英文均可。

```
feat(temperature): add HK index PCR via HKEX scrape
fix(akshare): handle empty north-flow response post-2024-08
chore(deps): bump pandas to 3.0.3
```

### 8.4 代码规范

- ruff line-length 120，配置在 [`pyproject.toml`](./pyproject.toml)，编辑器请打开 format on save
- **新增 provider**：实现 [`providers/base.py`](./src/finsynapse/providers/base.py) 的统一接口，落 bronze、`return (df, path)`；带 VCR cassette 或 pickle fixture
- **新增 silver transform**：在 [`cli.py`](./src/finsynapse/cli.py) `transform run` 中显式串入 + 写一条对应 pytest
- **新增温度计 indicator**：
  1. 在 [`config/weights.yaml`](./config/weights.yaml) 加权重（确保子项权重和为 1.0）
  2. 在 [`health_check.PLAUSIBLE_BOUNDS`](./src/finsynapse/transform/health_check.py) 加上下界
  3. 跑 [`scripts/backtest_temperature.py`](./scripts/backtest_temperature.py) 验证关键时点方向不翻车

### 8.5 测试要求

- provider PR **必须**带 VCR cassette 或 pickle fixture（CI 不发网络请求）
- transform 改动**必须**带覆盖新行为的单测
- 任何会让现有 12+ 测试变红的改动都需要在 PR 里说清楚为什么

### 8.6 不接受的内容

- 把 `ANTHROPIC_API_KEY` / `OPENAI_API_KEY` 等付费 LLM key 写进 workflow 或 GitHub Secret
- 引入需付费才能稳定访问的数据源
- 引入 Streamlit 之外的前端框架（保持「单文件 HTML 出 GitHub Pages」的最小依赖原则）
- 在 `main` 分支直接推 `data/bronze/` 或 `data/silver/`（已被 gitignore 保护，但请别 force push 绕过）

---

## 9. License & 致谢

[MIT](./LICENSE) © 2026 hg.dendi

数据与生态致谢：

- **AkShare** — A 股 / HK / 宏观数据
- **yfinance** — 美股与跨市场行情
- **FRED** — 美国宏观时间序列
- **multpl.com** — Shiller CAPE 历史回溯
- **DeepSeek / Anthropic / Ollama** — LLM 叙事生成
- **uv / ruff / pytest** — astral-sh 系工具链
