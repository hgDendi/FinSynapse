"""Daily macro brief generator (Phase 3).

Pipeline:
    silver layer  ->  fact pack (deterministic numbers)
                  ->  LLM narrative (or template fallback when no LLM available)
                  ->  data/gold/brief/YYYY-MM-DD.md

Design:
    - Numbers are NEVER produced by the LLM. We assemble a strict fact pack
      from silver and feed it into the prompt as ground truth; the LLM only
      writes connective prose. The fact tables in the output markdown are
      rendered by us, not by the model — so even a hallucinating model can't
      inject wrong figures into the published file.
    - Provider order: ollama (local) -> deepseek -> anthropic -> deterministic
      template. "Local first" matches the Phase 3 plan and lets the daily
      build run offline; cloud APIs are for when you want a richer narrative.
    - Output is checked into `data/gold/brief/`. One file per day, idempotent
      (re-running overwrites). Filename = YYYY-MM-DD.md so they sort.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import pandas as pd
import requests

from finsynapse import config as _cfg
from finsynapse.dashboard.data import MARKETS, load
from finsynapse.dashboard.i18n import translate_div


@dataclass
class FactPack:
    """Numerical snapshot the brief is built from. All numbers are real."""

    asof: str
    markets: dict[str, dict] = field(default_factory=dict)
    divergences: list[dict] = field(default_factory=list)
    health_warn_count: int = 0
    health_fail_count: int = 0
    notable_indicators: list[dict] = field(default_factory=list)


def _zone(v: float | None) -> str:
    if v is None or pd.isna(v):
        return "unknown"
    if v < 30:
        return "cold"
    if v < 70:
        return "mid"
    return "hot"


def _zone_emoji(z: str) -> str:
    return {"cold": "🧊", "mid": "🌤", "hot": "🔥"}.get(z, "❔")


def assemble_facts() -> FactPack:
    data = load()
    if data.temperature.empty:
        raise RuntimeError("No silver data — run `finsynapse transform run --layer all` first.")

    asof = data.asof().date().isoformat()
    fp = FactPack(asof=asof)

    latest = data.latest_per_market()
    for market in MARKETS:
        if market not in latest:
            continue
        row = latest[market]
        fp.markets[market] = {
            "date": pd.to_datetime(row["date"]).date().isoformat(),
            "overall": float(row["overall"]) if not pd.isna(row["overall"]) else None,
            "overall_zone": _zone(row.get("overall")),
            "valuation": None if pd.isna(row.get("valuation")) else float(row["valuation"]),
            "sentiment": None if pd.isna(row.get("sentiment")) else float(row["sentiment"]),
            "liquidity": None if pd.isna(row.get("liquidity")) else float(row["liquidity"]),
            "overall_change_1w": (None if pd.isna(row.get("overall_change_1w")) else float(row["overall_change_1w"])),
            "valuation_contribution_1w": (
                None if pd.isna(row.get("valuation_contribution_1w")) else float(row["valuation_contribution_1w"])
            ),
            "sentiment_contribution_1w": (
                None if pd.isna(row.get("sentiment_contribution_1w")) else float(row["sentiment_contribution_1w"])
            ),
            "liquidity_contribution_1w": (
                None if pd.isna(row.get("liquidity_contribution_1w")) else float(row["liquidity_contribution_1w"])
            ),
            "data_quality": str(row.get("data_quality", "ok")),
        }

    # Recent divergences (last 5 trading days, ranked by strength). Limit to
    # the top 6 to keep prompts tight; the model gets to pick which to discuss.
    if not data.divergence.empty:
        div = data.divergence[data.divergence["is_divergent"]].copy()
        div["date"] = pd.to_datetime(div["date"])
        cutoff = div["date"].max() - pd.Timedelta(days=5)
        recent = div[div["date"] >= cutoff].nlargest(6, "strength")
        fp.divergences = [
            {
                "date": d["date"].date().isoformat(),
                "pair": d["pair_name"],
                "a_change_pct": float(d["a_change"]) * 100,
                "b_change_pct": float(d["b_change"]) * 100,
                "strength": float(d["strength"]),
                "description": d["description"],
            }
            for _, d in recent.iterrows()
        ]

    # Notable indicators: top/bottom percentile readings on the latest available date.
    if not data.percentile.empty:
        pct = data.percentile.copy()
        pct["date"] = pd.to_datetime(pct["date"])
        latest_dt = pct["date"].max()
        snap = pct[pct["date"] == latest_dt].dropna(subset=["pct_10y"])
        # Keep extremes (>=85 or <=15) — those are the percentile-wise interesting ones.
        extreme = snap[(snap["pct_10y"] >= 85) | (snap["pct_10y"] <= 15)]
        fp.notable_indicators = [
            {
                "indicator": r["indicator"],
                "value": float(r["value"]),
                "pct_10y": float(r["pct_10y"]),
            }
            for _, r in extreme.sort_values("pct_10y", ascending=False).head(8).iterrows()
        ]

    if not data.health.empty:
        h = data.health
        fp.health_fail_count = int((h["severity"] == "fail").sum())
        fp.health_warn_count = int((h["severity"] == "warn").sum())

    return fp


# --- Prompt construction --------------------------------------------------

_SYSTEM_PROMPT = """你是 FinSynapse 的宏观市场观察员，负责为投资者撰写**每日中文市场简评**。

要求：
1. 只引用我提供的事实数字，**严禁编造任何数字**。如果某个值缺失，直接说"暂缺"而非估算。
2. 风格：克制、专业、短句。每段 2-3 句。不使用感叹号、不使用"震撼""暴涨"等情绪词。
3. 围绕三件事展开：
   - 三市场温度结构（哪个最热、哪个最冷、本周方向）
   - 最值得注意的 1-2 个背离信号（解释为什么重要）
   - 1 句话风险提示（基于温度区间或极端百分位）
4. 输出**纯 markdown**，不要包含 markdown 代码块围栏（```）。不要写标题（标题由我加）。
5. 总长度 250-400 字。
"""


def build_prompt(facts: FactPack) -> str:
    return (
        _SYSTEM_PROMPT
        + "\n\n以下是事实数据：\n\n"
        + json.dumps(
            {
                "asof": facts.asof,
                "markets": facts.markets,
                "recent_divergences": facts.divergences,
                "notable_indicators_pct10y": facts.notable_indicators,
                "data_health": {"fail": facts.health_fail_count, "warn": facts.health_warn_count},
            },
            ensure_ascii=False,
            indent=2,
        )
    )


# --- LLM providers (try in order) -----------------------------------------


@dataclass
class LLMResult:
    text: str
    provider: str  # "ollama" | "deepseek" | "anthropic" | "template"
    model: str | None = None
    error: str | None = None


def _call_ollama(prompt: str, model: str = "qwen2.5:7b", timeout: int = 120) -> str:
    """Local Ollama. Default model is qwen2.5:7b — strong CN capability and small enough
    to run on a laptop. User can override via FINSYNAPSE_LLM_MODEL env."""
    base_url = _cfg.settings.ollama_base_url.rstrip("/")
    r = requests.post(
        f"{base_url}/api/generate",
        json={"model": model, "prompt": prompt, "stream": False},
        timeout=timeout,
    )
    r.raise_for_status()
    return r.json().get("response", "").strip()


def _call_deepseek(prompt: str, model: str = "deepseek-chat", timeout: int = 300) -> str:
    api_key = _cfg.settings.deepseek_api_key
    if not api_key:
        raise RuntimeError("no DEEPSEEK_API_KEY")
    r = requests.post(
        "https://api.deepseek.com/chat/completions",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.4,
            "stream": False,
        },
        timeout=timeout,
    )
    r.raise_for_status()
    return r.json()["choices"][0]["message"]["content"].strip()


def _call_anthropic(prompt: str, model: str = "claude-haiku-4-5-20251001", timeout: int = 60) -> str:
    """Anthropic Messages API. Supports both direct (ANTHROPIC_API_KEY → x-api-key)
    and gateway/proxy setups (ANTHROPIC_AUTH_TOKEN → Bearer + ANTHROPIC_BASE_URL)."""
    api_key = _cfg.settings.anthropic_api_key
    auth_token = os.getenv("ANTHROPIC_AUTH_TOKEN")
    base_url = os.getenv("ANTHROPIC_BASE_URL", "https://api.anthropic.com").rstrip("/")
    if not api_key and not auth_token:
        raise RuntimeError("no ANTHROPIC_API_KEY or ANTHROPIC_AUTH_TOKEN")

    headers = {"anthropic-version": "2023-06-01", "Content-Type": "application/json"}
    if api_key:
        headers["x-api-key"] = api_key
    if auth_token:
        headers["Authorization"] = f"Bearer {auth_token}"

    r = requests.post(
        f"{base_url}/v1/messages",
        headers=headers,
        json={
            "model": model,
            "max_tokens": 800,
            "messages": [{"role": "user", "content": prompt}],
        },
        timeout=timeout,
    )
    r.raise_for_status()
    payload = r.json()
    return "".join(b.get("text", "") for b in payload.get("content", [])).strip()


def call_llm(prompt: str, provider: str = "auto", model: str | None = None) -> LLMResult:
    """Try providers in order; return first success. `provider="auto"` walks
    ollama -> deepseek -> anthropic. Explicit provider skips the fallback."""
    order = ["ollama", "deepseek", "anthropic"] if provider == "auto" else [provider]

    last_err = None
    for p in order:
        try:
            if p == "ollama":
                text = _call_ollama(prompt, model=model or os.getenv("FINSYNAPSE_LLM_MODEL", "qwen2.5:7b"))
            elif p == "deepseek":
                text = _call_deepseek(prompt, model=model or "deepseek-chat")
            elif p == "anthropic":
                text = _call_anthropic(prompt, model=model or "claude-haiku-4-5-20251001")
            else:
                raise RuntimeError(f"unknown provider {p!r}")
            if text:
                return LLMResult(text=text, provider=p, model=model)
        except Exception as exc:
            last_err = f"{p}: {type(exc).__name__}: {exc}"
            continue

    return LLMResult(text="", provider="template", error=last_err)


# --- Deterministic fallback narrative -------------------------------------


def _template_narrative(facts: FactPack) -> str:
    """Rule-based narrative when no LLM is reachable. Picks the hottest/coldest
    market, the strongest divergence, and a percentile-extreme note. Boring
    but never wrong."""
    parts: list[str] = []

    if facts.markets:
        rated = [(m, info["overall"]) for m, info in facts.markets.items() if info["overall"] is not None]
        if rated:
            rated.sort(key=lambda x: x[1], reverse=True)
            hottest_m, hottest_v = rated[0]
            coldest_m, coldest_v = rated[-1]
            parts.append(
                f"今日三市场温度：{hottest_m.upper()} 最热（{hottest_v:.0f}°，{_zone_emoji(_zone(hottest_v))}），"
                f"{coldest_m.upper()} 最冷（{coldest_v:.0f}°，{_zone_emoji(_zone(coldest_v))}）。"
            )

        for m in MARKETS:
            info = facts.markets.get(m, {})
            chg = info.get("overall_change_1w")
            if chg is not None and abs(chg) >= 5:
                direction = "升" if chg > 0 else "降"
                parts.append(f"{m.upper()} 一周综合温度{direction} {abs(chg):.1f}°。")

    if facts.divergences:
        top = facts.divergences[0]
        zh = translate_div(top["description"], "zh")
        parts.append(f"最近背离：**{top['pair']}**（{top['date']}）— {zh}。")

    if facts.notable_indicators:
        for n in facts.notable_indicators[:2]:
            tag = "极热" if n["pct_10y"] >= 85 else "极冷"
            parts.append(f"`{n['indicator']}` 当前 {n['value']:.2f}，处于 10 年 {n['pct_10y']:.0f}% 分位（{tag}）。")

    if not parts:
        parts.append("今日 silver 数据齐全，但未触发显著的温度/背离/百分位异常。")

    return "\n\n".join(parts)


# --- Markdown rendering ---------------------------------------------------


def _fmt(v: float | None, suffix: str = "°", digits: int = 1) -> str:
    if v is None:
        return "—"
    return f"{v:.{digits}f}{suffix}"


def _fmt_signed(v: float | None, suffix: str = "°") -> str:
    if v is None:
        return "—"
    return f"{v:+.1f}{suffix}"


def render_markdown(facts: FactPack, narrative: str, llm: LLMResult) -> str:
    lines: list[str] = []
    lines.append(f"# FinSynapse 宏观简评 · {facts.asof}")
    lines.append("")
    lines.append(
        f"> 数据截至 **{facts.asof}** · "
        f"叙事生成: `{llm.provider}`" + (f" / `{llm.model}`" if llm.model else "") + " · "
        "数字直接来自 silver 层"
    )
    lines.append("")

    # --- 三市场温度快照（fact, deterministic）
    lines.append("## 一、三市场温度快照")
    lines.append("")
    lines.append("| 市场 | 综合 | 区间 | 估值 | 情绪 | 流动性 | 一周Δ | 数据 |")
    lines.append("|------|-----:|:----:|-----:|-----:|-------:|------:|------|")
    for m in MARKETS:
        info = facts.markets.get(m)
        if not info:
            lines.append(f"| {m.upper()} | — | — | — | — | — | — | _missing_ |")
            continue
        zone_label = f"{_zone_emoji(info['overall_zone'])} {info['overall_zone']}"
        lines.append(
            f"| {m.upper()} "
            f"| {_fmt(info['overall'])} "
            f"| {zone_label} "
            f"| {_fmt(info['valuation'])} "
            f"| {_fmt(info['sentiment'])} "
            f"| {_fmt(info['liquidity'])} "
            f"| {_fmt_signed(info['overall_change_1w'])} "
            f"| {info['data_quality']} |"
        )
    lines.append("")

    # --- 一周贡献度
    lines.append("### 一周温度变化贡献分解")
    lines.append("")
    lines.append("| 市场 | Δ估值 | Δ情绪 | Δ流动性 |")
    lines.append("|------|------:|------:|--------:|")
    for m in MARKETS:
        info = facts.markets.get(m)
        if not info:
            continue
        lines.append(
            f"| {m.upper()} "
            f"| {_fmt_signed(info['valuation_contribution_1w'])} "
            f"| {_fmt_signed(info['sentiment_contribution_1w'])} "
            f"| {_fmt_signed(info['liquidity_contribution_1w'])} |"
        )
    lines.append("")

    # --- 叙事
    lines.append("## 二、今日观察")
    lines.append("")
    lines.append(narrative.strip())
    lines.append("")

    # --- 背离明细 (fact)
    lines.append("## 三、最近背离信号")
    lines.append("")
    if facts.divergences:
        lines.append("| 日期 | 信号对 | a Δ% | b Δ% | 强度 | 含义 |")
        lines.append("|------|--------|-----:|-----:|-----:|------|")
        for d in facts.divergences:
            zh = translate_div(d["description"], "zh")
            lines.append(
                f"| {d['date']} | `{d['pair']}` "
                f"| {d['a_change_pct']:+.2f}% "
                f"| {d['b_change_pct']:+.2f}% "
                f"| {d['strength']:.4f} "
                f"| {zh} |"
            )
    else:
        lines.append("_近 5 个交易日无显著背离。_")
    lines.append("")

    # --- 极端百分位指标
    if facts.notable_indicators:
        lines.append("## 四、10 年百分位极值指标")
        lines.append("")
        lines.append("| 指标 | 当前值 | 10Y 百分位 | 标签 |")
        lines.append("|------|-------:|----------:|:----:|")
        for n in facts.notable_indicators:
            tag = "🔥 极热" if n["pct_10y"] >= 85 else "🧊 极冷"
            lines.append(f"| `{n['indicator']}` | {n['value']:.4g} | {n['pct_10y']:.1f}% | {tag} |")
        lines.append("")

    # --- 数据健康
    if facts.health_fail_count or facts.health_warn_count:
        lines.append("## 五、数据健康")
        lines.append("")
        lines.append(f"- fail: **{facts.health_fail_count}** 条；warn: **{facts.health_warn_count}** 条")
        lines.append("- 详见 `data/silver/health_log.parquet`")
        lines.append("")

    if llm.error:
        lines.append("---")
        lines.append(f"<sub>LLM fallback: {llm.error}</sub>")

    return "\n".join(lines).rstrip() + "\n"


_NARRATIVE_HEADER = "## 二、今日观察"
_NEXT_SECTION_PREFIX = "## "


def extract_narrative(md_text: str) -> str:
    """Slice out just the '今日观察' body — the only LLM-written part.

    The dashboard already renders temperature, divergence and percentile facts
    as charts/tables, so re-embedding the brief in full would be redundant.
    Returns the trimmed body (excluding the section heading itself); empty
    string when the section is absent (e.g. older brief format)."""
    lines = md_text.splitlines()
    try:
        start = next(i for i, ln in enumerate(lines) if ln.strip() == _NARRATIVE_HEADER)
    except StopIteration:
        return ""
    body: list[str] = []
    for ln in lines[start + 1 :]:
        if ln.startswith(_NEXT_SECTION_PREFIX):
            break
        body.append(ln)
    return "\n".join(body).strip()


def latest_brief_path() -> Path | None:
    brief_dir = _cfg.settings.gold_dir / "brief"
    if not brief_dir.exists():
        return None
    candidates = sorted(brief_dir.glob("*.md"))
    return candidates[-1] if candidates else None


def load_latest_narrative() -> tuple[str, str | None]:
    """Return (narrative_md, asof_date_str) of the most recent brief, or
    ('', None) when no brief has been generated yet."""
    p = latest_brief_path()
    if p is None:
        return "", None
    text = p.read_text(encoding="utf-8")
    return extract_narrative(text), p.stem  # filename stem = YYYY-MM-DD


# Pattern matches the meta line written by render_markdown(), e.g.:
#   > 数据截至 **2026-04-29** · 叙事生成: `deepseek` / `deepseek-v4-pro` · ...
# Both `provider` and `/ model` are captured; model is optional (template
# fallback writes only the provider).
_META_PATTERN = re.compile(r"叙事生成:\s*`(?P<provider>[^`]+)`(?:\s*/\s*`(?P<model>[^`]+)`)?")


@dataclass(frozen=True)
class BriefMeta:
    """Lightweight summary of a stored brief — used by the archive page."""

    asof: str  # YYYY-MM-DD (filename stem)
    path: Path  # absolute path to .md
    provider: str  # "deepseek" | "anthropic" | "ollama" | "template" | "unknown"
    model: str | None  # model id, or None for template/older briefs


def _parse_meta(md_text: str) -> tuple[str, str | None]:
    """Pull (provider, model) out of the meta blockquote. Returns
    ('unknown', None) when the file pre-dates the meta line format."""
    for line in md_text.splitlines()[:10]:  # meta sits in the top few lines
        m = _META_PATTERN.search(line)
        if m:
            return m.group("provider"), m.group("model")
    return "unknown", None


def list_briefs() -> list[BriefMeta]:
    """Return every brief on disk, newest first. Used by render_static to
    build the /briefs.html archive index and per-date HTML pages."""
    brief_dir = _cfg.settings.gold_dir / "brief"
    if not brief_dir.exists():
        return []
    out: list[BriefMeta] = []
    for p in sorted(brief_dir.glob("*.md"), reverse=True):
        provider, model = _parse_meta(p.read_text(encoding="utf-8"))
        out.append(BriefMeta(asof=p.stem, path=p, provider=provider, model=model))
    return out


def write_brief(md: str, asof: str | date) -> Path:
    asof_str = asof.isoformat() if isinstance(asof, date) else asof
    out_dir = _cfg.settings.gold_dir / "brief"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{asof_str}.md"
    path.write_text(md, encoding="utf-8")
    return path


def generate(provider: str = "auto", model: str | None = None) -> tuple[Path, LLMResult]:
    """End-to-end: facts -> prompt -> LLM -> markdown -> file. Returns (path, llm_result)."""
    facts = assemble_facts()
    prompt = build_prompt(facts)
    llm = call_llm(prompt, provider=provider, model=model)
    narrative = llm.text if llm.text else _template_narrative(facts)
    md = render_markdown(facts, narrative, llm)
    path = write_brief(md, facts.asof)
    return path, llm
