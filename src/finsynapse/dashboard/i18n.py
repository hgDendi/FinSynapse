from __future__ import annotations

DEFAULT_LANG = "zh"
SUPPORTED = ("zh", "en")

# Translation table. Key = canonical English snake_case identifier.
# Add new keys here, never inline strings in templates/charts.
TRANSLATIONS: dict[str, dict[str, str]] = {
    # --- Page chrome ---
    "page_title":            {"zh": "FinSynapse — 宏观温度计", "en": "FinSynapse — Macro Thermometer"},
    "header_title":          {"zh": "🌡️ FinSynapse — 宏观温度计", "en": "🌡️ FinSynapse — Macro Thermometer"},
    "data_as_of":            {"zh": "数据截至", "en": "Data as of"},
    "data_meta_suffix":      {"zh": "由 GitHub Actions 每日重建 · 权重见", "en": "auto-rebuilt by GitHub Actions · weights in"},
    "lang_toggle_label":     {"zh": "EN", "en": "中文"},
    "lang_toggle_hint":      {"zh": "切换语言", "en": "Switch language"},
    "footer":                {"zh": "FinSynapse · 计划 §11 温度计", "en": "FinSynapse · Plan §11 thermometer"},
    "footer_source":         {"zh": "源代码", "en": "source"},

    # --- Sections ---
    "section_market_temps":  {"zh": "市场温度", "en": "Market temperatures"},
    "section_long_history":  {"zh": "长期温度走势", "en": "Long-history temperature"},
    "section_divergence":    {"zh": "背离信号（最近 90 天）", "en": "Divergence signals (last 90 days)"},
    "section_data_health":   {"zh": "数据健康", "en": "Data health"},

    # --- Card / state messages ---
    "no_data_card":          {"zh": "暂无数据 — Phase 1b 接入 AkShare", "en": "no data yet — Phase 1b adds AkShare"},
    "no_health_issues":      {"zh": "无健康问题记录。", "en": "No health issues recorded."},
    "no_divergence":         {"zh": "区间内未检出背离。", "en": "No divergences in window."},
    "data_quality_label":    {"zh": "数据质量", "en": "data_quality"},
    "no_silver_data":        {"zh": "尚无 silver 数据。请先运行 `uv run finsynapse transform run --layer all`。",
                              "en": "No silver data yet. Run `uv run finsynapse transform run --layer all` first."},

    # --- Health summary ---
    "issues_total":          {"zh": "条问题", "en": "issues"},
    "issues_metric":         {"zh": "问题", "en": "Issues"},

    # --- Sub-temperature names ---
    "valuation":             {"zh": "估值", "en": "Valuation"},
    "sentiment":             {"zh": "情绪", "en": "Sentiment"},
    "liquidity":             {"zh": "流动性", "en": "Liquidity"},
    "overall":               {"zh": "综合", "en": "Overall"},

    # --- Chart titles / labels ---
    "chart_overall_temp":    {"zh": "{market} — 综合温度", "en": "{market} — Overall temperature"},
    "chart_sub_temps":       {"zh": "分量温度", "en": "Sub-temperatures"},
    "chart_sub_temps_short": {"zh": "分量", "en": "sub-temps"},
    "chart_1w_contribution": {"zh": "近一周贡献度", "en": "1W contribution"},
    "chart_overall_change":  {"zh": "综合变化", "en": "overall Δ"},
    "chart_recent_div":      {"zh": "最近背离（90 天）", "en": "Recent divergences (last 90d)"},

    # --- Streamlit-specific ---
    "select_market":         {"zh": "市场", "en": "Market"},
    "sidebar_lang_label":    {"zh": "语言", "en": "Language"},

    # --- Table headers ---
    "th_date":               {"zh": "日期", "en": "date"},
    "th_pair":               {"zh": "信号对", "en": "pair"},
    "th_a_change":           {"zh": "a 涨跌", "en": "a Δ"},
    "th_b_change":           {"zh": "b 涨跌", "en": "b Δ"},
    "th_strength":           {"zh": "强度", "en": "strength"},
    "th_signal":             {"zh": "信号", "en": "signal"},
    "th_indicator":          {"zh": "指标", "en": "indicator"},
    "th_rule":               {"zh": "规则", "en": "rule"},
    "th_severity":           {"zh": "严重度", "en": "severity"},
    "th_detail":             {"zh": "详情", "en": "detail"},
}

# Divergence pair descriptions: stored canonically in English in
# `transform/divergence.py` (so silver parquet stays one source of truth and
# LLM prompts in english/chinese both work). Translation map below — keep keys
# byte-identical to the source strings.
DIVERGENCE_TRANSLATIONS: dict[str, str] = {
    "SP500 ↑ + VIX ↓: risk-on as expected":
        "标普↑+VIX↓: 风险偏好如常",
    "SP500 ↑ + VIX ↑: rising on rising fear — beware":
        "标普↑+VIX↑: 上涨伴恐慌, 需警惕",
    "10Y ↑ + DXY ↑: tightening in sync":
        "10Y利率↑+美元↑: 紧缩同步",
    "10Y ↑ + DXY ↓: yields up, dollar down — credit/inflation regime shift?":
        "10Y利率↑+美元↓: 利率涨美元跌 — 信用/通胀范式切换?",
    "Gold ↑ + real yield ↓: classic safe-haven":
        "黄金↑+实际利率↓: 经典避险",
    "Gold ↑ + real yield ↑: de-dollarization / sovereign hedge bid?":
        "黄金↑+实际利率↑: 去美元化/主权对冲买盘?",
    "SP500 and 10Y move together: growth narrative dominant":
        "标普与10Y同向: 增长叙事主导",
    "SP500 ↑ + 10Y ↓: liquidity-driven rally without growth confirmation":
        "标普↑+10Y↓: 流动性驱动反弹, 无增长背书",
    "HSI ↑ + DXY ↓: weak USD supports HK / EM":
        "恒指↑+美元↓: 弱美元利好港股/EM",
    "HSI ↑ + DXY ↑: HK rallies despite USD strength — domestic catalyst?":
        "恒指↑+美元↑: 港股逆美元上涨 — 本地催化?",
}


def t(key: str, lang: str = DEFAULT_LANG) -> str:
    """Return translated string for `key`. Falls back to en if missing in lang;
    returns the bare key if missing in both — that surfaces typos fast."""
    block = TRANSLATIONS.get(key)
    if not block:
        return key
    return block.get(lang) or block.get("en") or key


def translate_div(description: str, lang: str = DEFAULT_LANG) -> str:
    if lang == "en":
        return description
    return DIVERGENCE_TRANSLATIONS.get(description, description)
