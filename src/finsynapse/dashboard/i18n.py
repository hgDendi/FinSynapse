from __future__ import annotations

DEFAULT_LANG = "zh"
SUPPORTED = ("zh", "en")

# Translation table. Key = canonical English snake_case identifier.
# Add new keys here, never inline strings in templates/charts.
TRANSLATIONS: dict[str, dict[str, str]] = {
    # --- Page chrome ---
    "page_title": {"zh": "FinSynapse — 宏观温度计", "en": "FinSynapse — Macro Thermometer"},
    "header_title": {"zh": "🌡️ FinSynapse — 宏观温度计", "en": "🌡️ FinSynapse — Macro Thermometer"},
    "data_as_of": {"zh": "数据截至", "en": "Data as of"},
    "data_meta_suffix": {
        "zh": "由 GitHub Actions 每日重建 · 权重见",
        "en": "auto-rebuilt by GitHub Actions · weights in",
    },
    "lang_toggle_label": {"zh": "EN", "en": "中文"},
    "lang_toggle_hint": {"zh": "切换语言", "en": "Switch language"},
    "footer": {"zh": "FinSynapse · 计划 §11 温度计", "en": "FinSynapse · Plan §11 thermometer"},
    "footer_source": {"zh": "源代码", "en": "source"},
    # --- Sections ---
    "section_market_temps": {"zh": "市场温度", "en": "Market temperatures"},
    "section_long_history": {"zh": "长期温度走势", "en": "Long-history temperature"},
    "section_divergence": {"zh": "背离信号（最近 90 天）", "en": "Divergence signals (last 90 days)"},
    "section_data_health": {"zh": "数据健康", "en": "Data health"},
    # --- Card / state messages ---
    "no_data_card": {"zh": "暂无数据 — Phase 1b 接入 AkShare", "en": "no data yet — Phase 1b adds AkShare"},
    "no_health_issues": {"zh": "无健康问题记录。", "en": "No health issues recorded."},
    "no_divergence": {"zh": "区间内未检出背离。", "en": "No divergences in window."},
    "data_quality_label": {"zh": "数据质量", "en": "data_quality"},
    "no_silver_data": {
        "zh": "尚无 silver 数据。请先运行 `uv run finsynapse transform run --layer all`。",
        "en": "No silver data yet. Run `uv run finsynapse transform run --layer all` first.",
    },
    # --- Health summary ---
    "issues_total": {"zh": "条问题", "en": "issues"},
    "issues_metric": {"zh": "问题", "en": "Issues"},
    # --- Sub-temperature names ---
    "valuation": {"zh": "估值", "en": "Valuation"},
    "sentiment": {"zh": "情绪", "en": "Sentiment"},
    "liquidity": {"zh": "流动性", "en": "Liquidity"},
    "overall": {"zh": "综合", "en": "Overall"},
    # Plain-language aliases (suggestion B): displayed as a small subtitle next
    # to the canonical factor name, not replacing it. Lets first-time visitors
    # parse the radar without finance background.
    "valuation_plain": {"zh": "贵不贵", "en": "How pricey?"},
    "sentiment_plain": {"zh": "多兴奋", "en": "How excited?"},
    "liquidity_plain": {"zh": "钱多不多", "en": "How much liquidity?"},
    # Temperature zone labels — emoji + plain Chinese (suggestion B).
    "zone_cold": {"zh": "偏冷", "en": "Cold"},
    "zone_mid": {"zh": "中性", "en": "Neutral"},
    "zone_hot": {"zh": "偏热", "en": "Hot"},
    "zone_cold_label": {"zh": "🧊 偏冷", "en": "🧊 Cold"},
    "zone_mid_label": {"zh": "🌤 中性", "en": "🌤 Neutral"},
    "zone_hot_label": {"zh": "🔥 偏热", "en": "🔥 Hot"},
    # 1-week change copy
    "weekly_change": {"zh": "本周变化", "en": "Weekly change"},
    "weekly_attribution": {"zh": "本周由谁贡献", "en": "Weekly attribution"},
    # --- Chart titles / labels ---
    "chart_overall_temp": {"zh": "{market} — 综合温度", "en": "{market} — Overall temperature"},
    "chart_sub_temps": {"zh": "分量温度", "en": "Sub-temperatures"},
    "chart_sub_temps_short": {"zh": "分量", "en": "sub-temps"},
    "chart_1w_contribution": {"zh": "近一周贡献度", "en": "1W contribution"},
    "chart_overall_change": {"zh": "综合变化", "en": "overall Δ"},
    "chart_recent_div": {"zh": "最近背离（90 天）", "en": "Recent divergences (last 90d)"},
    "chart_cross_market_radar": {"zh": "三市场子温度对比", "en": "Cross-market sub-temps"},
    "section_cross_market": {"zh": "三市场对比", "en": "Cross-market comparison"},
    "section_brief": {"zh": "今日观察（LLM 叙事）", "en": "Today's note (LLM narrative)"},
    # Suggestion C: structured "today's takeaways" above the long narrative.
    "section_takeaways": {"zh": "今日要点", "en": "Today's takeaways"},
    "takeaways_subtitle": {
        "zh": "三件最值得花 30 秒看完的事 — 自动从今日数据生成，不用读完整篇叙事。",
        "en": "Three things worth 30 seconds — auto-generated from today's silver data.",
    },
    "narrative_collapse": {"zh": "展开完整叙事", "en": "Expand full narrative"},
    # Divergence cards (suggestion A): replaces the cryptic table.
    "div_card_strength": {"zh": "信号强度", "en": "Signal strength"},
    "div_card_pair": {"zh": "信号对", "en": "Pair"},
    "div_change_today": {"zh": "今日变动", "en": "Today's move"},
    "div_no_recent": {"zh": "近 90 天暂无显著背离信号。", "en": "No notable divergences in the last 90 days."},
    # Risk-level labels for divergence strength buckets
    "risk_high": {"zh": "高", "en": "High"},
    "risk_med": {"zh": "中", "en": "Medium"},
    "risk_low": {"zh": "低", "en": "Low"},
    "risk_weak": {"zh": "弱", "en": "Weak"},
    # Glossary helper
    "glossary_what_is_temp": {"zh": "什么是温度计？", "en": "What is the thermometer?"},
    "glossary_body": {
        "zh": "0–100 的综合分。<b>估值</b>看贵不贵、<b>情绪</b>看大家多兴奋、<b>流动性</b>看场内有多少钱。三者加权 → 综合温度。<b>0–30 偏冷</b>，<b>30–70 中性</b>，<b>70–100 偏热</b>。每天由 GitHub Actions 自动重算。",
        "en": "0–100 composite. <b>Valuation</b> = how pricey; <b>Sentiment</b> = how excited people are; <b>Liquidity</b> = how much money is in the system. Weighted → overall temperature. <b>0–30 Cold</b>, <b>30–70 Neutral</b>, <b>70–100 Hot</b>. Recomputed daily.",
    },
    # --- Glossary explainer page ---
    "glossary_page_title": {"zh": "FinSynapse · 温度计说明", "en": "FinSynapse · Thermometer guide"},
    "glossary_back_to_dashboard": {"zh": "← 回到温度计", "en": "← Back to dashboard"},
    "glossary_intro_title": {"zh": "市场温度计是什么", "en": "What the market thermometer is"},
    "glossary_intro_body": {
        "zh": "把<b>估值</b>、<b>情绪</b>、<b>流动性</b>三类指标，统一换算到 0–100 的相对刻度上。0 = 这个市场历史上最冷的水平；100 = 历史上最热。每天 06:00（北京时间）由 GitHub Actions 自动重算，所有数字都来自 silver 层、可在 git 历史里追溯。",
        "en": "We collapse <b>valuation</b>, <b>sentiment</b>, and <b>liquidity</b> indicators onto a single 0–100 relative scale. 0 = the coldest the market has ever been within its lookback; 100 = the hottest ever. Recomputed every day at 22:00 UTC by GitHub Actions; all numbers come from the silver layer and are reproducible from git history.",
    },
    "glossary_step1_title": {
        "zh": "第一步：把每个指标换成历史百分位",
        "en": "Step 1: convert each indicator to its historical percentile",
    },
    "glossary_step1_body": {
        "zh": "对每个指标（CAPE、VIX、社融、美元指数 …）取过去 <b>10 年</b>的滚动百分位作为基础信号。月频指标（CAPE、M2、社融）会先 forward-fill 到日度，避免窗口里只有十几条点。",
        "en": "Each indicator (CAPE, VIX, social financing, DXY …) is converted to its trailing <b>10-year</b> rolling percentile. Monthly series (CAPE, M2, social financing) are forward-filled to daily first so the window has enough data points.",
    },
    "glossary_step2_title": {"zh": "第二步：按方向决定升降温", "en": "Step 2: direction determines hot or cold"},
    "glossary_step2_body": {
        "zh": "每个指标在 weights.yaml 里都标了一个方向：<code>+</code> 表示分位高 → 温度高（如 CAPE 高 = 估值贵 = 热）；<code>-</code> 表示分位高 → 温度低（如 VIX 高 = 恐慌 = 冷；DXY 强 = 全球流动性紧 = 冷）。",
        "en": "Each indicator carries a direction in weights.yaml: <code>+</code> means high percentile → high temperature (e.g. high CAPE = expensive = hot); <code>-</code> means high percentile → low temperature (e.g. high VIX = fear = cold; strong DXY = tight global liquidity = cold).",
    },
    "glossary_step3_title": {"zh": "第三步：合成子温度", "en": "Step 3: combine into sub-temperatures"},
    "glossary_step3_body": {
        "zh": "在每个子项（估值 / 情绪 / 流动性）下，按权重加权平均所有可用指标。<b>缺失指标会自动按可用权重重归一</b>——比如港股期权 PCR 没有免费源时，HK 情绪不会拉垮，而是把 100% 权重给到南向 5 日。",
        "en": "Within each sub-temperature (valuation / sentiment / liquidity) we take a weighted average across the available indicators. <b>Missing indicators auto-renormalise</b> — e.g. when HK options PCR has no free source, HK sentiment falls back to 100% on southbound 5d instead of going blank.",
    },
    "glossary_step4_title": {"zh": "第四步：合成综合温度", "en": "Step 4: combine into overall temperature"},
    "glossary_step4_body": {
        "zh": "三个子温度按市场各自的权重加权得到 <b>综合温度</b>（0–100）。区间：<b>&lt; 30 偏冷</b> · <b>30–70 中性</b> · <b>≥ 70 偏热</b>。",
        "en": "The three sub-temperatures are combined per-market into the <b>overall temperature</b> (0–100). Bands: <b>&lt; 30 Cold</b> · <b>30–70 Neutral</b> · <b>≥ 70 Hot</b>.",
    },
    "glossary_formula_title": {"zh": "公式速览", "en": "Formula at a glance"},
    "glossary_section_weights": {"zh": "各市场权重明细", "en": "Per-market weights"},
    "glossary_weights_subtitle": {
        "zh": "实时从 <code>config/weights.yaml</code> 读取——改完它再跑 <code>finsynapse transform run --layer temperature</code> 立刻生效。",
        "en": "Loaded live from <code>config/weights.yaml</code> — edit and rerun <code>finsynapse transform run --layer temperature</code> to apply.",
    },
    "glossary_th_sub": {"zh": "子温度", "en": "Sub-temperature"},
    "glossary_th_sub_weight": {"zh": "占比", "en": "Weight"},
    "glossary_th_indicator": {"zh": "指标", "en": "Indicator"},
    "glossary_th_indicator_weight": {"zh": "指标权重", "en": "Ind. weight"},
    "glossary_th_direction": {"zh": "方向", "en": "Direction"},
    "glossary_dir_pos": {"zh": "+ 分位高 → 热", "en": "+ high pct → hot"},
    "glossary_dir_neg": {"zh": "− 分位高 → 冷", "en": "− high pct → cold"},
    "glossary_section_history": {"zh": "历史极值与对应事件", "en": "Historical extremes & matched events"},
    "glossary_history_subtitle": {
        "zh": "下表展示每个市场综合温度的历史最热 / 最冷点，并匹配那段时间的代表性宏观事件。今天的「历史分位」就是当前温度在这条时间线里的位置。",
        "en": "Below are the all-time hottest and coldest readings for each market's overall temperature, matched to the dominant macro event around that date. Today's \"historical percentile\" is where the current reading sits on this timeline.",
    },
    "glossary_history_th_market": {"zh": "市场", "en": "Market"},
    "glossary_history_th_kind": {"zh": "类型", "en": "Type"},
    "glossary_history_th_date": {"zh": "日期", "en": "Date"},
    "glossary_history_th_temp": {"zh": "温度", "en": "Temp"},
    "glossary_history_th_event": {"zh": "对应事件", "en": "Matched event"},
    "glossary_history_kind_hot": {"zh": "🔥 历史最热", "en": "🔥 All-time hot"},
    "glossary_history_kind_cold": {"zh": "🧊 历史最冷", "en": "🧊 All-time cold"},
    "glossary_history_kind_today": {"zh": "📍 今日水平", "en": "📍 Today"},
    "glossary_history_no_event": {"zh": "（区间内无显著事件标记）", "en": "(no labelled event in window)"},
    "glossary_history_pct_explainer": {
        "zh": "「历史分位」= 当前综合温度在自身有数据以来全部交易日里的百分位排序。99% 表示比历史上 99% 的日子都热。",
        "en": '"Historical percentile" = where today\'s overall temperature ranks against every trading day since data started. 99% means hotter than 99% of historical days.',
    },
    "glossary_section_caveats": {"zh": "需要知道的几个细节", "en": "A few things worth knowing"},
    "glossary_caveats": {
        "zh": (
            "<li><b>权重不会随时间变化</b>——一旦订就锁死，避免曲线拟合。一周温度变化的归因也是用这套固定权重做的。</li>"
            "<li><b>10 年滚动窗口</b>意味着遇到从未有过的极端值会被压在 0 或 100。这是设计——温度计衡量的是「相对历史的位置」，不是绝对水平。</li>"
            "<li><b>data_quality 字段</b>会标 <code>ok</code> / <code>partial</code> / <code>pcr_unavailable</code>。partial 表示当天有子分量缺数据；pcr_unavailable 是 HK 期权 PCR 永久无源的降级标记。</li>"
            "<li><b>日报 (gold/brief)</b> 是另一回事——LLM 或模板只是把上面这些 silver 数字串成一段叙事，不会引入新计算。</li>"
        ),
        "en": (
            "<li><b>Weights are frozen</b> once set — no curve fitting. Weekly attribution uses the same fixed weights.</li>"
            '<li><b>The 10-year rolling window</b> means truly unprecedented extremes get clipped at 0 or 100. By design — the thermometer measures "position relative to history", not absolute level.</li>'
            "<li><b>The data_quality field</b> tags rows as <code>ok</code> / <code>partial</code> / <code>pcr_unavailable</code>. partial = a sub-component was missing that day; pcr_unavailable = HK options PCR has no permanent free source.</li>"
            "<li><b>The daily brief</b> in gold/ is just narrative — LLM or template stitches these silver numbers into prose, no new calculations.</li>"
        ),
    },
    # Card-level historical percentile widget
    "card_history_pct_label": {"zh": "历史分位", "en": "Hist. percentile"},
    "card_history_pct_hover": {
        "zh": "在自身有数据以来全部交易日中，今天的温度排第 {pct} 位。点击看详细公式与历史极值。",
        "en": "Across every trading day since data started, today ranks at the {pct}th percentile. Click for the formula and historical extremes.",
    },
    "card_history_extremes_hint": {
        "zh": "历史最热 {hot_temp:.0f}° · 最冷 {cold_temp:.0f}°",
        "en": "All-time hot {hot_temp:.0f}° · cold {cold_temp:.0f}°",
    },
    # Footer
    "footer_powered_by": {"zh": "源自", "en": "Powered by"},
    "footer_repo_label": {"zh": "在 GitHub 上查看代码", "en": "view on GitHub"},
    "brief_meta": {
        "zh": "由本地 LLM/模板生成 · 数字均来自 silver",
        "en": "generated by local LLM/template · numbers sourced from silver",
    },
    "brief_full_link": {"zh": "查看完整简评 (.md)", "en": "view full brief (.md)"},
    # --- Brief archive page ---
    "nav_brief_archive": {"zh": "📚 历史简评", "en": "📚 Brief archive"},
    "archive_title": {"zh": "FinSynapse · 历史简评归档", "en": "FinSynapse · Brief archive"},
    "archive_subtitle": {
        "zh": "全部由 daily workflow 自动生成，每日一份。点开看完整 brief 或下载原始 markdown。",
        "en": "Auto-generated by the daily workflow, one per trading day. Click to read the full brief or download raw markdown.",
    },
    "archive_back_to_dashboard": {"zh": "← 回到温度计", "en": "← Back to dashboard"},
    "archive_total_count": {"zh": "共 {n} 篇", "en": "{n} brief(s) total"},
    "archive_th_asof": {"zh": "数据日期", "en": "as of"},
    "archive_th_provider": {"zh": "叙事生成", "en": "narrated by"},
    "archive_th_model": {"zh": "模型", "en": "model"},
    "archive_th_actions": {"zh": "查看", "en": "view"},
    "archive_view_html": {"zh": "网页", "en": "HTML"},
    "archive_view_md": {"zh": "Markdown", "en": "Markdown"},
    "archive_empty": {
        "zh": "尚无历史 brief — 等 daily workflow 跑过一次就会出现。",
        "en": "No briefs yet — waiting for the first daily workflow run.",
    },
    "single_back_to_archive": {"zh": "← 历史归档", "en": "← Archive"},
    "single_download_md": {"zh": "下载 .md", "en": "download .md"},
    # --- Streamlit-specific ---
    "select_market": {"zh": "市场", "en": "Market"},
    "sidebar_lang_label": {"zh": "语言", "en": "Language"},
    # --- Table headers ---
    "th_date": {"zh": "日期", "en": "date"},
    "th_pair": {"zh": "信号对", "en": "pair"},
    "th_a_change": {"zh": "a 涨跌", "en": "a Δ"},
    "th_b_change": {"zh": "b 涨跌", "en": "b Δ"},
    "th_strength": {"zh": "强度", "en": "strength"},
    "th_signal": {"zh": "信号", "en": "signal"},
    "th_indicator": {"zh": "指标", "en": "indicator"},
    "th_rule": {"zh": "规则", "en": "rule"},
    "th_severity": {"zh": "严重度", "en": "severity"},
    "th_detail": {"zh": "详情", "en": "detail"},
}

# Divergence pair descriptions: stored canonically in English in
# `transform/divergence.py` (so silver parquet stays one source of truth and
# LLM prompts in english/chinese both work). Translation map below — keep keys
# byte-identical to the source strings.
DIVERGENCE_TRANSLATIONS: dict[str, str] = {
    "SP500 ↑ + VIX ↓: risk-on as expected": "标普↑+VIX↓: 风险偏好如常",
    "SP500 ↑ + VIX ↑: rising on rising fear — beware": "标普↑+VIX↑: 上涨伴恐慌, 需警惕",
    "10Y ↑ + DXY ↑: tightening in sync": "10Y利率↑+美元↑: 紧缩同步",
    "10Y ↑ + DXY ↓: yields up, dollar down — credit/inflation regime shift?": "10Y利率↑+美元↓: 利率涨美元跌 — 信用/通胀范式切换?",
    "Gold ↑ + real yield ↓: classic safe-haven": "黄金↑+实际利率↓: 经典避险",
    "Gold ↑ + real yield ↑: de-dollarization / sovereign hedge bid?": "黄金↑+实际利率↑: 去美元化/主权对冲买盘?",
    "SP500 and 10Y move together: growth narrative dominant": "标普与10Y同向: 增长叙事主导",
    "SP500 ↑ + 10Y ↓: liquidity-driven rally without growth confirmation": "标普↑+10Y↓: 流动性驱动反弹, 无增长背书",
    "HSI ↑ + DXY ↓: weak USD supports HK / EM": "恒指↑+美元↓: 弱美元利好港股/EM",
    "HSI ↑ + DXY ↑: HK rallies despite USD strength — domestic catalyst?": "恒指↑+美元↑: 港股逆美元上涨 — 本地催化?",
    "CSI300 and A-share turnover move together: trend confirmed by participation": "沪深300与A股成交同向: 参与度确认行情",
    "CSI300 ↑ + turnover ↓: rally without participation — distribution risk": "沪深300↑+成交↓: 缩量上涨 — 警惕派发",
    "HSI and southbound 5d in sync: mainland flow drives HK as expected": "恒指与南向5日同向: 内地资金主导港股如常",
    "HSI ↑ + southbound ↓: HK rises without mainland support — foreign-led rally?": "恒指↑+南向↓: 港股上涨无内地支撑 — 外资主导?",
}


# Plain-language pair display names — suggestion D. Shown alongside the
# canonical snake_case code so power users still recognise it. Each entry is
# (zh, en). When a pair is missing here, callers should fall back to the
# raw code.
PAIR_PLAIN_NAMES: dict[str, dict[str, str]] = {
    "sp500_vix": {"zh": "美股 vs 恐慌指数", "en": "S&P 500 vs VIX"},
    "us10y_dxy": {"zh": "美债利率 vs 美元", "en": "US 10Y yield vs DXY"},
    "gold_real_rate": {"zh": "黄金 vs 实际利率", "en": "Gold vs Real yield"},
    "sp500_us10y": {"zh": "美股 vs 美债利率", "en": "S&P 500 vs US 10Y"},
    "hsi_dxy": {"zh": "港股 vs 美元", "en": "HSI vs DXY"},
    "csi300_volume": {"zh": "A 股 vs 成交量", "en": "CSI 300 vs A-share turnover"},
    "hsi_southbound": {"zh": "港股 vs 内地南向资金", "en": "HSI vs Southbound flow"},
}

# Plain-language explanation per *signal description*. Keyed on the canonical
# English description string (same key as DIVERGENCE_TRANSLATIONS) so every
# divergence row gets both a tagline (zh of the original) and a friendly
# 1–2 sentence "what this means in plain English/Chinese" body.
DIVERGENCE_PLAIN: dict[str, dict[str, str]] = {
    "SP500 ↑ + VIX ↓: risk-on as expected": {
        "zh": "美股上涨、恐慌指数下行，市场情绪健康，是常态走势。",
        "en": "Stocks up while fear is down — healthy risk-on, as expected.",
    },
    "SP500 ↑ + VIX ↑: rising on rising fear — beware": {
        "zh": "美股在涨，但恐慌指数同步上升 — 投资者一边追涨一边买保险，往往是情绪转折前兆。",
        "en": "Stocks rise even as fear rises — investors are hedging on the way up. Often a turning-point tell.",
    },
    "10Y ↑ + DXY ↑: tightening in sync": {
        "zh": "美债利率与美元同步走强，是货币紧缩的典型组合。",
        "en": "Yields and the dollar tighten together — textbook tightening regime.",
    },
    "10Y ↑ + DXY ↓: yields up, dollar down — credit/inflation regime shift?": {
        "zh": "利率涨美元却跌，反常组合，可能预示信用风险上升或通胀范式切换。",
        "en": "Yields rise but the dollar falls — unusual; can flag credit stress or a regime shift on inflation expectations.",
    },
    "Gold ↑ + real yield ↓: classic safe-haven": {
        "zh": "实际利率下降推动金价上涨，经典避险逻辑成立。",
        "en": "Falling real yields lift gold — classic safe-haven mechanics.",
    },
    "Gold ↑ + real yield ↑: de-dollarization / sovereign hedge bid?": {
        "zh": "实际利率上升金价反而涨，往往来自央行/主权对冲资金，去美元化叙事的信号。",
        "en": "Gold rises despite higher real yields — typically central-bank or sovereign hedging. A de-dollarization tell.",
    },
    "SP500 and 10Y move together: growth narrative dominant": {
        "zh": "股票与利率同向，市场以增长叙事为主导。",
        "en": "Stocks and yields move together — growth narrative is leading.",
    },
    "SP500 ↑ + 10Y ↓: liquidity-driven rally without growth confirmation": {
        "zh": "美股涨但利率下行，意味着这波上涨主要由流动性推动，缺乏增长基本面背书。",
        "en": "Stocks up while yields drop — rally is liquidity-driven, lacking growth confirmation.",
    },
    "HSI ↑ + DXY ↓: weak USD supports HK / EM": {
        "zh": "美元走弱时港股/新兴市场获得资金回流支撑，是惯常关系。",
        "en": "Weak dollar supports HK / EM equities — the usual relationship.",
    },
    "HSI ↑ + DXY ↑: HK rallies despite USD strength — domestic catalyst?": {
        "zh": "美元走强港股仍上涨，反常 — 通常意味着本地有独立催化（政策/盈利/事件）。",
        "en": "HK rallies despite a strong dollar — atypical; usually points to a local catalyst (policy / earnings / event).",
    },
    "CSI300 and A-share turnover move together: trend confirmed by participation": {
        "zh": "沪深300与成交量同向，行情被参与度确认，趋势更可靠。",
        "en": "CSI 300 and turnover move together — the trend is confirmed by participation.",
    },
    "CSI300 ↑ + turnover ↓: rally without participation — distribution risk": {
        "zh": "A 股在涨但成交在缩。买盘集中在少数手里，存量资金内部博弈，警惕高位派发。",
        "en": "A-shares rise on shrinking turnover — narrow participation; watch for distribution into retail.",
    },
    "HSI and southbound 5d in sync: mainland flow drives HK as expected": {
        "zh": "港股与南向资金同步，内地资金主导港股表现，关系稳定。",
        "en": "HSI and southbound flow in sync — mainland money is driving HK as usual.",
    },
    "HSI ↑ + southbound ↓: HK rises without mainland support — foreign-led rally?": {
        "zh": "港股上涨，但内地通过港股通的资金在撤。这波反弹靠外资推动，缺少本地买盘确认，持续性存疑。",
        "en": "HK rallies while southbound money exits — driven by foreign capital, lacking domestic confirmation. Sustainability questionable.",
    },
}

# Plain-language indicator names for the percentile-extreme takeaway. Keep
# stable across releases; missing keys fall back to the raw code.
INDICATOR_PLAIN: dict[str, dict[str, str]] = {
    "us_cape": {"zh": "美股 CAPE 估值（席勒市盈率）", "en": "US CAPE (Shiller P/E)"},
    "us_pe_ttm": {"zh": "美股 PE-TTM 估值", "en": "US trailing P/E"},
    "csi300_pe_ttm": {"zh": "沪深 300 PE-TTM 估值", "en": "CSI 300 trailing P/E"},
    "csi300_pb": {"zh": "沪深 300 市净率", "en": "CSI 300 P/B"},
    "cn_a_turnover_5d": {"zh": "A 股 5 日换手率", "en": "A-share 5-day turnover"},
    "cn_social_financing_12m": {"zh": "中国社融 12 个月累计", "en": "CN social financing (12M)"},
    "cn_south_5d": {"zh": "南向资金 5 日净流入", "en": "Southbound flow (5D)"},
    "hsi": {"zh": "恒生指数", "en": "HSI"},
    "csi300": {"zh": "沪深 300", "en": "CSI 300"},
    "sp500": {"zh": "标普 500", "en": "S&P 500"},
    "vix": {"zh": "VIX 恐慌指数", "en": "VIX"},
    "dxy": {"zh": "美元指数", "en": "DXY"},
    "us10y": {"zh": "10 年期美债收益率", "en": "US 10Y yield"},
    "gold_futures": {"zh": "黄金期货", "en": "Gold futures"},
    "hk_ewh_yield_ttm": {"zh": "港股 ETF 滚动股息率", "en": "HK ETF TTM dividend yield"},
}


def pair_plain_name(pair: str, lang: str = DEFAULT_LANG) -> str:
    """Friendly pair name; falls back to the raw code when no mapping exists."""
    block = PAIR_PLAIN_NAMES.get(pair)
    if not block:
        return pair
    return block.get(lang) or block.get("en") or pair


def divergence_plain(description: str, lang: str = DEFAULT_LANG) -> str:
    """Long-form plain explanation of a divergence row; empty string when
    we don't have one (caller should hide the secondary line)."""
    block = DIVERGENCE_PLAIN.get(description)
    if not block:
        return ""
    return block.get(lang) or block.get("en") or ""


def indicator_plain_name(indicator: str, lang: str = DEFAULT_LANG) -> str:
    block = INDICATOR_PLAIN.get(indicator)
    if not block:
        return indicator
    return block.get(lang) or block.get("en") or indicator


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
