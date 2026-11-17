# -*- coding: utf-8 -*-
"""★2026-08-13 #298 选股 + 日报自动化（DSHQuant-AIReview 自动化 20:00 调用——选股/日报全自动，知识库 AI 不再参与）

职责：
  1. 自动选股（宁缺毋滥规则，结果入远期池 D 池 ai_select——来源标记"自动精选"）：
     门槛：①择时 level 含"适合"且 score>=60 ②候选 pitch_v2 score>=80（长线）/ tech confidence 中高（短线，可选）
     ③因子实测过滤：factors 中实测 T+1 均值为负的因子 → 该候选降权跳过（Pitch v3 精神初现）
     ④上限 5 只（持股≤5 总原则）；不满足门槛 → 今日不选（自动 skip，宁缺毋滥）
  2. 生成日报 report/daily_report_YYYY-MM-DD.md（市场环境/信号/自动选股/候选/池状态/因子实测）
  3. --dry-run：只出日报不写池（验证用）；默认真写池

用法：python data/daily_report_auto.py [--dry-run]
"""
# ★2026-08-13 黑框隐藏（自动化运行不弹黑框）
try:
    import ctypes
    _h = ctypes.windll.kernel32.GetConsoleWindow()
    if _h:
        ctypes.windll.user32.ShowWindow(_h, 0)
except Exception:
    pass

import sys
import json
import math
from pathlib import Path
from datetime import datetime, timedelta

BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE))
LOGS = BASE / "logs"
REPORT = BASE / "report"
OUTPUT = BASE / "output"

MIN_SCORE_CORE = 88.0      # 一等核心门槛（raw_score，无弱实测因子）
MIN_SCORE_BACKUP = 85.0    # 二等备选门槛（核心不足时补足，宁缺毋滥）
MIN_TIMING = 60.0          # 择时门槛（<60 不选）
MAX_PICKS = 5              # 持股≤5（上限；可少于上限，绝不硬凑）


def _read(p):
    try:
        with open(p, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _trade_date(core_cfg=None, *, now=None):
    """Use the official bootstrap's completed date without rescanning bar tables."""
    from data.research_bootstrap import TZ, validate_close_cutoff
    if core_cfg is None:
        from data.research_bootstrap import load_config
        core_cfg = load_config()
    now = now or datetime.now(TZ)
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError('日报日期核验需要带时区的真实时间')
    now = now.astimezone(TZ)
    cutoff = validate_close_cutoff(core_cfg.get('completed_session_cutoff', '18:00'))
    eligible_end = (now.date() if now.strftime('%H:%M') >= cutoff else now.date() - timedelta(days=1)).isoformat()
    manifest = _read(Path(core_cfg['runtime_dir'])/'bootstrap-manifest.json') or {}
    candidate = (manifest.get('coverage', {}).get('daily') or {}).get('end')
    checkpoint = (manifest.get('completed') or {}).get('daily:' + str(candidate)) or {}
    try:
        valid_date = datetime.strptime(candidate, '%Y-%m-%d').strftime('%Y-%m-%d') == candidate
        valid = (valid_date and candidate <= eligible_end and manifest.get('source') == 'tushare-official'
                 and manifest.get('target_end') == candidate
                 and all(manifest.get('stages', {}).get(stage) == 'complete'
                         and manifest.get('stage_dates', {}).get(stage) == candidate
                         for stage in ('daily', 'finance', 'assets'))
                 and type(checkpoint.get('rows')) is int
                 and checkpoint['rows'] >= core_cfg['min_daily_rows'])
        if valid:
            return candidate
    except (TypeError, ValueError, KeyError):
        pass
    raise ValueError('日报没有同日完整验收的官方研究行情，不能使用系统日期或未验证缓存代替')


def load_timing():
    # ★#364 读时间戳 glob 取最新（固定名 timing_system.json 是副本，被锁/不更新时会读到旧择时）
    fs = sorted(OUTPUT.glob("timing_system*.json"), key=lambda x: x.stat().st_mtime)
    d = _read(fs[-1]) if fs else None
    if not d:
        return {"score": None, "level": "未知", "dims": {}, "reason": ""}
    dims = d.get("dims") or {}
    return {"score": d.get("score"), "level": d.get("level"),
            "dims": {k: (v or {}).get("score") for k, v in dims.items()},
            "reason": d.get("reason", "")}


def load_pitch_long():
    fs = sorted((LOGS).glob("pitch_v2*.json"), key=lambda x: x.stat().st_mtime)
    d = _read(fs[-1]) if fs else {}
    return d.get("pitch") or []


def load_tech():
    """短线 Pitch（tech_pitch_v3 输出，含 add_date = pitch 时间）"""
    fs = sorted((LOGS).glob("tech_pitch*.json"), key=lambda x: x.stat().st_mtime)
    d = _read(fs[-1]) if fs else {}
    return d.get("entries") or []


def load_holdings():
    """当前持仓 code 集合（portfolio 的 holding/over_limit）——★#422 自动选股跳过已持仓股，
    避免"推荐已持仓股"（持股≤5 总原则下，已持仓股不应再被 D 池重复推荐）"""
    fs = sorted((LOGS).glob("portfolio_*.json"), key=lambda x: x.stat().st_mtime)
    d = _read(fs[-1]) if fs else {}
    return {p.get("code") for p in (d.get("positions") or [])
            if p.get("status") in ("holding", "over_limit")}


def load_factor_live():
    d = _read(REPORT / "market_kb_dump.json") or {}
    return {f.get("factor"): f for f in (d.get("factor_perf_top") or [])}


def load_market_crowding(date, cfg=None):
    """Use the same dated market-turnover percentile as the factor dashboard.

    The legacy knowledge-base crowd_pctile can be an average of individual
    factor ranks. It is not a substitute for this market statistic.
    """
    if cfg is None:
        from data.research_factors import load_config
        cfg = load_config()
    output = Path(cfg['output_dir'])
    if not output.is_absolute():
        output = BASE / output
    data = _read(output / f'crowding_{date}.json') or {}
    value = data.get('crowding_pctile_252')
    if data.get('ok') is not True or data.get('data_date') != date or isinstance(value, bool):
        return None
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(value) or not 0 <= value <= 100:
        return None
    sessions = cfg['crowding_sessions']
    if data.get('n_history_sessions') != sessions:
        return None
    return {'percent': value, 'sessions': sessions, 'turnover_window': cfg['turnover_window']}


def report_data_status_lines():
    """Describe saved receipts and selected sources, never certify live readiness.

    This runs inside the research pipeline lock. Only the selected worker's
    lightweight metadata is read; the unified deep verifier remains independent.
    """
    lines = []
    try:
        from data.minute_readiness import selected_service
        module = selected_service()
        cfg = module.load_config()
        status = module.public_status(cfg)
        label = {'baostock': 'BaoStock', 'tushare': 'Tushare'}.get(cfg.get('provider', 'tushare'), '所选分钟源')
        if any(str(source).startswith('sina:') for source in status.get('sources', [])):
            label += '（含新浪分钟数据修补）'
        done, total = status.get('completed_symbols'), status.get('expected_symbols')
        if (status.get('database_path') and type(done) is int and type(total) is int
                and 0 <= done <= total and total > 0 and status.get('start_date') and status.get('target_date')):
            lines.append(f"- 历史5分钟来源：{label}；生成时下载回执 {done}/{total} 只，窗口 {status['start_date']} 至 {status['target_date']}。")
        else:
            lines.append(f'- 历史5分钟来源：{label}；生成时暂无可核对的下载回执。')
    except (OSError, ValueError, KeyError, TypeError):
        lines.append('- 生成时分钟来源或下载回执暂不可读。')
    lines.append('- 下载回执数量不等于当前窗口已通过完整核验；完整核验与补齐进度见[数据页](/data)。')
    try:
        from paperlive.config import load_config
        market = load_config()['market']
        lines.append(f"- 当前配置的盘口来源：{market.get('source_label') or market['provider']}。静态日报不判定实时可交易性，请查看[实时模拟盘](/paperlive)。")
        # A previous rt_k failure says nothing about a selected Tencent feed.
        if market['provider'] == 'tushare':
            probe = _read(BASE/'runtime/research/rt-permission-verification.json') or {}
            if probe.get('endpoint') == 'rt_k' and type(probe.get('ok')) is bool and probe.get('checked_at'):
                try:
                    stamp = datetime.fromisoformat(probe['checked_at']).isoformat()
                except (TypeError, ValueError):
                    stamp = None
                if stamp:
                    result = '通过' if probe['ok'] else '未通过'
                    lines.append(f'- Tushare rt_k 历史权限探测：{stamp} {result}；仅代表当次探测，不代表当前盘口可交易。')
    except (OSError, ValueError, KeyError, TypeError):
        lines.append('- 生成时盘口来源配置暂不可读；当前状态见[实时模拟盘](/paperlive)。')
    return lines


def _load_bars_1y(codes):
    """只读取候选近 1 年日线（牛散纪律用：当日/3日/一年涨幅）——尽力而为，失败返回 {}
    ★2026-08-13 超级牛散谨慎化：不追高（当日>7%/3日>15%）/ 涨多不看多（一年>150%）硬纪律"""
    try:
        from data.cache import DailyCache
        dc = DailyCache()
        # ★返回 {code.upper(): DataFrame(日期升序)}——dict of DataFrame，非 rows
        asof = _trade_date()
        start = (datetime.fromisoformat(asof) - timedelta(days=400)).date().isoformat()
        data = dc.get_daily_batch(list(codes), start=start, end=asof)
        out = {}
        for code, df in data.items():
            if df is None or df.empty:
                continue
            out[code] = dict(zip(df["date"].astype(str), df["close"].astype(float)))
        return out
    except Exception:
        return {}


def _discipline(code, bars):
    """牛散纪律检查：返回 None（通过）或降级理由（触发纪律 → 不进池，日报标注观察）"""
    closes = bars.get(code)
    if not closes or len(closes) < 5:
        return '近期行情不足，无法完成纪律检查，暂列观察'
    if max(closes) != _trade_date():
        return '个股行情未更新至本次数据日，暂列观察'
    ds = sorted(closes)
    last = closes[ds[-1]]
    pre = closes[ds[-2]]
    d1 = last / pre - 1 if pre else 0
    d3 = last / closes[ds[-4]] - 1 if len(ds) >= 4 and closes[ds[-4]] else 0
    if len(ds) >= 252:
        y1 = last / closes[ds[-252]] - 1
        if y1 > 1.5:
            return f"涨多不看多（一年+{y1:.0%}，>150%）——放弃"
    if d1 > 0.07:
        return f"不追高（当日+{d1:.1%}，>7%）——降级观察"
    if d3 > 0.15:
        return f"不追高（3日+{d3:.1%}，>15%）——降级观察"
    return None


def auto_pick(timing, pitch_es, dry_run: bool):
    """自动选股（超级牛散·宁缺毋滥）：可 0-5 只，绝不硬凑——
    一等核心（raw≥88 且无弱实测因子）优先；二等备选（raw≥85）只在核心不足时补足；
    牛散纪律（当日>7% / 3日>15% / 一年>150%）触发 → 降级观察不进池"""
    if timing["score"] is None or timing["score"] < MIN_TIMING:
        return [], f"择时 {timing['score']} 分低于门槛 {MIN_TIMING}（{timing['level']}）——今日不选（牛散管住手）"
    live = load_factor_live()
    held = load_holdings()   # ★#422 当前持仓（跳过，不重复推荐）
    bars = _load_bars_1y([c.get("code") for c in pitch_es if c.get("code")])
    core, backup, disciplined = [], [], []
    for c in pitch_es:
        if c.get("code") in held:
            continue   # ★#422 已持仓股不进 D 池（持股≤5 宁缺毋滥）
        score = c.get("score")
        if score is None or score < MIN_SCORE_BACKUP:
            continue
        facs = (c.get("factors") or {})
        # 因子实测过滤：factors 中实测 T+1 均值为负的因子（实测证据优先——Pitch v3 精神）
        neg = [f for f in facs if f in live and (live[f].get("t1_avg") or 0) < 0]
        # 高分例外：raw_score>=85 容忍弱实测因子（保留但降权标注）；raw_score<85 且有弱实测因子 → 跳过
        if neg and score < 85:
            continue
        # ★#298 降权：每个弱实测因子 -3 分（改排序优先级不改接口——权重精神）
        penalty = -3.0 * len(neg)
        score_adj = score + penalty if penalty else score
        if score_adj < MIN_SCORE_BACKUP and neg:
            continue
        # ★2026-08-13 牛散纪律：涨跌幅硬门槛（不追高/涨多不做）
        dnote = _discipline(c.get("code"), bars)
        item = {"code": c.get("code"), "name": c.get("name"),
                "otype": c.get("otype_name") or c.get("otype"),
                "score": round(score_adj, 1), "raw_score": score,
                "factors": list(facs.keys())[:4],
                "neg_live_factors": neg,
                # ★#325 三问核心信息透传（机制/止损/赔率/风控——日报信息密度对齐 Pitch 卡）
                "mechanism": c.get("mechanism") or "",
                "stop_plan": (c.get("stop_plan") or {}).get("desc") or "",
                "winrate_est": c.get("winrate_est"),
                "upside_est": c.get("upside_est"),
                "risk_level": c.get("risk_level") or "—",
                # ★#332 日报密度对齐决策页：因子家族 + 强因子直通 + 分 Regime 情景赔率
                "signal_family": c.get("signal_family") or "",
                "express_strong": bool(c.get("express_strong")),
                "regime": ((c.get("horizons") or {}).get("1y") or {}).get("regime") or {},
                "factors_all": facs}
        if dnote:
            item["discipline_note"] = dnote
            disciplined.append(item)
            continue
        # 质量分档：一等核心（≥88 无弱实测）优先；其余进二等备选
        item["tier"] = "core" if (score_adj >= MIN_SCORE_CORE and not neg) else "backup"
        if item["tier"] == "core":
            core.append(item)
        else:
            backup.append(item)
    core.sort(key=lambda x: -(x["score"] or 0))
    backup.sort(key=lambda x: -(x["score"] or 0))
    # ★2026-08-13 超级牛散谨慎化（宁缺毋滥）：核心优先；核心不足时备选只象征性补 ≤2 只
    #   （且 raw≥90 才够格）——绝不为凑满 5 只出手；可 0 只
    picks = core[:MAX_PICKS]
    backup_room = MAX_PICKS - len(picks)
    if backup_room > 0:
        strong_backup = [b for b in backup if (b["raw_score"] or 0) >= 90]
        picks += strong_backup[:min(backup_room, 2)]
    if not picks:
        msg = f"候选均未达牛散门槛（核心≥{MIN_SCORE_CORE:.0f}无弱实测 / 备选≥90）——今日不选（宁缺毋滥，不为凑数出手）"
        if disciplined:
            msg += f"；纪律剔除 {len(disciplined)} 只（{disciplined[0].get('discipline_note')} 等）"
        return [], msg
    return picks, ""


def write_pool(picks, date, skip_reason):
    """选中的入远期池 D 池（ai_select + ai_reason=自动精选）；不选 → skip 记录
    ★#300：append_ai_select 对池中已存在的候选不重复入池（防入池假象）
    ★2026-08-13：已在池的候选记录"今日复核"（last_ai_check）——D 池每日有可见更新"""
    sys.path.insert(0, str(BASE))
    from factors.opportunities.pitch_track import append_ai_select
    if picks:
        payload = [{"code": p["code"], "reason": f"自动精选：{p['otype']} score {p['score']:.0f}"
                                              f"{'（容忍弱实测因子并降权）' if p['neg_live_factors'] else ''}",
                    "confidence": round(min(p["score"] / 100, 0.9), 2)} for p in picks]
        pool = append_ai_select(payload, date)
        # ★2026-08-13：优先读池内 last_action（本次精确动作），缺失时回退条目反推（兼容旧文件）
        _la = pool.get("last_action") or {}
        if _la.get("date") == date:
            n_new, n_recheck = int(_la.get("added", 0)), int(_la.get("rechecked", 0))
        else:
            n_new = sum(1 for e in pool["entries"]
                        if e.get("pool_type") == "ai_select" and e.get("entry_date") == date)
            n_recheck = sum(1 for e in pool["entries"]
                            if e.get("pool_type") == "ai_select" and e.get("last_ai_check") == date
                            and e.get("entry_date") != date)
        if n_new and n_recheck:
            return f"自动精选新增 {n_new} 只入 D 池 + 复核 {n_recheck} 只"
        if n_new:
            return f"自动精选 {n_new} 只入 D 池"
        if n_recheck:
            return f"自动精选复核 {n_recheck} 只（候选均在池中，今日确认延续）"
        return f"候选 {len(picks)} 只均已在池中（无新增无复核）"
    else:
        append_ai_select([], date, skip_reason=skip_reason)
        return f"今日不选（{skip_reason[:50]}...）"


def build_report(date, timing, picks, skip_reason, pitch_es, tech_es, dry_run):
    kb = _read(REPORT / "market_kb_dump.json") or {}
    md = (kb.get("market_daily_recent") or [{}])[0]
    fperf = kb.get("factor_perf_top") or []
    crowd = load_market_crowding(date)
    crowd_text = (f"{crowd['percent']:.1f}%（{crowd['turnover_window']}日平均换手的{crowd['sessions']}日历史分位）"
                  if crowd else '尚无同日可验证数值')

    def _fmt_regime(rg):
        # ★#332 分 Regime 情景赔率（牛/震荡/熊 各胜率/均收益，1y 口径）
        if not rg:
            return ""
        parts = []
        for k, lb in (("bull", "牛"), ("base", "震荡"), ("bear", "熊")):
            v = rg.get(k) or {}
            if v.get("n"):
                wr = f"{v['winrate']*100:.0f}%" if v.get("winrate") is not None else "—"
                ar = f"{v['avg_ret']*100:+.0f}%" if v.get("avg_ret") is not None else "—"
                parts.append(f"{lb}{wr}/{ar}")
        return "  ".join(parts) if parts else ""

    def _fmt_fac(p):
        # 因子（前 4 个触发因子：名称 + 值）
        facs = p.get("factors_all") or {}
        fam = p.get("signal_family") or ""
        es = p.get("express_strong")
        if not facs:
            return ""
        items = " / ".join(f"{k} {v:.2f}" for k, v in list(facs.items())[:4] if isinstance(v, (int, float)))
        tag = (fam + " · ") if fam else ""
        tag += "强因子直通" if es else ""
        return (tag + items) if items else (tag or "")

    lines = []
    lines.append(f"# DeepSeek HARNESS Quant日报 {date}")
    lines.append(f"> 生成 {datetime.now():%Y-%m-%d %H:%M:%S} · {'仅生成研究报告，未下单' if dry_run else '已更新观察池，未下单'}")
    lines.append("")
    lines.append("## 一、市场环境")
    d = timing["dims"]
    lines.append(f"- 择时：**{timing['level']} {timing['score']} 分** | 政策 {d.get('政策')} / 宏观 {d.get('宏观')} / 情绪 {d.get('情绪')} / 宽度 {d.get('宽度')}")
    lines.append(f"- 宽度：涨 {md.get('n_up')} / 跌 {md.get('n_down')} | 成交额 {md.get('turnover_亿')} 亿 | 市场拥挤度 {crowd_text}")
    lines.append(f"- 择时理由：{str(timing['reason'])[:150]}")
    lines.append("")
    lines.append("## 二、当日研究筛选")
    if picks:
        for p in picks:
            if p.get("neg_live_factors") and p.get("raw_score") and p["raw_score"] >= 85:
                neg = f"（高分容忍弱实测因子 {','.join(p['neg_live_factors'])}，已降权 {len(p['neg_live_factors'])*3} 分）"
            elif p.get("neg_live_factors"):
                neg = f"（剔除弱实测因子 {','.join(p['neg_live_factors'])}）"
            else:
                neg = ""
            lines.append(f"- **{p['code']} {p['name']}** | {p['otype']} | score {p['score']:.1f}{neg}")
            # ★#325 三问核心（机制/赔率/止损——信息密度对齐 Pitch 卡）
            wr = f"{p['winrate_est']*100:.0f}%" if p.get("winrate_est") is not None else "—"
            up = f"{p['upside_est']}%" if p.get("upside_est") is not None else "—"
            mech = p.get("mechanism") or ""
            sp = p.get("stop_plan") or ""
            lines.append(f"  · 机制 {mech} · 同类历史胜率 {wr} 规则估值空间 {up} · 风控 {p.get('risk_level','—')}" + (f" · 止损 {sp}" if sp else ""))
            # ★#332 因子归因 + 分 Regime 情景赔率（对齐决策页三问密度）
            fac_txt = _fmt_fac(p)
            if fac_txt:
                lines.append(f"  · 因子 {fac_txt}")
            rg_txt = _fmt_regime(p.get("regime"))
            if rg_txt:
                lines.append(f"  · 分Regime(1y) {rg_txt}")
        if not dry_run:
            lines.append(f"\n> ✅ 已入远期池 D 池（自动精选，自动 fwd 验证）")
    else:
        lines.append(f"- 今日不选：{skip_reason}")
    lines.append("")
    lines.append(f"## 三、候选池（Pitch 长线 · 共 {len(pitch_es)} 只）")
    for c in sorted(pitch_es, key=lambda x: -(x.get("score") or 0)):
        pd = c.get("pitch_date") or ""
        lines.append(f"- {c.get('code')} {c.get('name')} | {c.get('otype_name') or c.get('otype')} | {c.get('score')}" + (f" | Pitch {pd}" if pd else ""))
        mech = c.get("mechanism") or ""
        sp = (c.get("stop_plan") or {}).get("desc") or ""
        wr = f"{c['winrate_est']*100:.0f}%" if c.get("winrate_est") is not None else "—"
        up = f"{c['upside_est']}%" if c.get("upside_est") is not None else "—"
        lines.append(f"  · 机制 {mech} · 同类历史胜率 {wr} 规则估值空间 {up} · 风控 {c.get('risk_level','—')}" + (f" · 止损 {sp}" if sp else ""))
        # ★#332 因子归因 + 分 Regime 情景赔率（对齐决策页三问密度）
        facs = c.get("factors") or {}
        fam = c.get("signal_family") or ""
        es = c.get("express_strong")
        fac_items = " / ".join(f"{k} {v:.2f}" for k, v in list(facs.items())[:4] if isinstance(v, (int, float)))
        if fac_items:
            fac_txt = (fam + " · " if fam else "") + ("强因子直通 · " if es else "") + fac_items
            lines.append(f"  · 因子 {fac_txt}")
        rg = ((c.get("horizons") or {}).get("1y") or {}).get("regime") or {}
        rg_txt = _fmt_regime(rg)
        if rg_txt:
            lines.append(f"  · 分Regime(1y) {rg_txt}")
    lines.append("")
    lines.append(f"## 四、短线 Pitch（Tech · 情绪/事件 · 共 {len(tech_es)} 只）")
    if tech_es:
        for t in sorted(tech_es, key=lambda x: -(x.get("score") or 0)):
            td = t.get("add_date") or ""
            lines.append(f"- {t.get('code')} {t.get('name')} | {t.get('otype_name') or t.get('otype')} | score {t.get('score'):.1f}" + (f" | Pitch {td}" if td else ""))
            mech = t.get("mechanism") or ""
            if mech:
                lines.append(f"  · 机制 {mech[:44]}")
            trig = t.get("trigger") or ""
            if trig:
                lines.append(f"  · 触发 {trig[:70]}")
            sp = t.get("stop_plan") or {}
            stop_parts = []
            if sp.get("atr_stop"):
                stop_parts.append(f"ATR 观察线 {sp['atr_stop']:.2f}")
            if sp.get("stop_loss_pct") is not None:
                stop_parts.append(f"硬止损 {sp['stop_loss_pct']*100:.0f}%")
            if t.get("risk_level"):
                stop_parts.append(f"风控 {t.get('risk_level')}")
            if stop_parts:
                lines.append(f"  · 止损 {' · '.join(stop_parts)}")
    else:
        lines.append("- 今日无短线候选")
    lines.append("")
    lines.append("## 五、池状态")
    lines.append(f"- 机会池 {md.get('n_opp')} / Pitch {md.get('n_pitch')} / 短线 {md.get('n_tech')}")
    lines.append("")
    lines.append("## 六、因子前向观察")
    if not fperf:
        lines.append('- 已初始化前向观察，尚无到期样本；不把历史回测冒充实盘收益。')
    for f in fperf[:5]:
        t1 = f"{f.get('t1_avg')*100:+.2f}%" if f.get("t1_avg") is not None else "—"
        lines.append(f"- {f.get('factor')}：pitch {f.get('n_pitch')} 次 | T+1 {t1}")
    lines.extend(['', '## 七、研究口径与数据状态',
        '- 股票研究使用当前沪深300成分股回看历史，存在成分股选择偏差；历史胜率不是个股未来上涨概率，规则估值空间不是经校准的收益预测。',
        '- 换手类研究从2019年起；交易检验按下一交易日开盘及可成交条件处理，各模块成本假设保留在对应产物中；历史印花税税率变化未分段还原。',
        '- 短线规则尚未校准，仅供观察；前向收益从本次真实登记日开始积累，未到期值保持空缺。'])
    lines.extend(report_data_status_lines())
    report = REPORT / f"daily_report_{date}.md"
    report.write_text("\n".join(lines), encoding="utf-8")
    return report


def main():
    dry_run = "--dry-run" in sys.argv
    date = _trade_date()   # ★#336 数据日（非系统日期）
    kb = _read(REPORT / 'market_kb_dump.json') or {}
    if kb.get('data_date') != date:
        raise ValueError('市场知识库与日报数据日期不一致')
    timing = load_timing()
    if timing.get('score') is None:
        raise ValueError('择时尚未就绪，不能生成完整日报')
    pitch_es = load_pitch_long()
    tech_es = load_tech()
    picks, skip_reason = auto_pick(timing, pitch_es, dry_run)

    pool_txt = "dry-run 不写池"
    if not dry_run:
        pool_txt = write_pool(picks, date, skip_reason)

    report = build_report(date, timing, picks, skip_reason, pitch_es, tech_es, dry_run)
    status = f"自动选股 {'✅ ' + str(len(picks)) + ' 只' if picks else '❌ 今日不选'} | {pool_txt} | 日报 {report.name}"
    print(f"日报自动化: {status}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
