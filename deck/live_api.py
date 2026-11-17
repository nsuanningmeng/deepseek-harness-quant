# -*- coding: utf-8 -*-
"""deck/live_api.py — 实时 API 层（任务包 F1，2026-08-10 总指导自接）

★目标：页面从"静态 HTML 快照"升级为"JS 拉实时接口"。
  /api/live/forward  → 远期收益池实时（update_fwd 重算 + 数据源状态）
  /api/live/factors  → 因子池聚合（档案/增强/拥挤度/裁决 + 生成时间）
  /api/live/audit    → 数据审计实时（logs/audit 最新）
  /api/live/pools    → 各池状态汇总（机会/Pitch/科技/止损/待处理计数）
"""
import glob
import json
import os
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
if str(BASE) not in sys.path:
    sys.path.insert(0, str(BASE))

_fwd_cache = {"ts": 0, "data": None}
_cal_cache = {"ts": 0, "data": None}   # ★#150 日历缓存（数据源版本 5min）
_alrt_cache = {"ts": 0, "data": None}  # ★#150 预警缓存（数据源版本 2min）
_chain_cache = {"ts": 0, "data": None} # ★#150 决策链缓存（数据源版本 2min）


def _latest_file(pattern: str, subdir: str = "logs") -> Path:
    files = sorted(glob.glob(str(BASE / subdir / pattern)), key=os.path.getmtime)
    return Path(files[-1]) if files else None


# ★2026-08-14 效率优化：_read mtime 版本缓存（live_opp 读 1.7MB opp_pool + pitch_v2 每次全解析 75ms）
#   数据文件 mtime 不变 → 直接命中缓存（0ms）；文件更新 → 自动失效重读。
#   与 live_forward 的"数据源版本缓存"模式一致；内存上限受文件数约束（时间戳文件不累积）。
_READ_CACHE = {}


def _read(pattern: str, subdir: str = "logs") -> dict:
    f = _latest_file(pattern, subdir)
    if f:
        try:
            _key = f"{subdir}/{pattern}|{f.name}"
            _mt = f.stat().st_mtime
            _hit = _READ_CACHE.get(_key)
            if _hit and _hit[0] == _mt:
                return _hit[1]
            _data = json.loads(f.read_text(encoding="utf-8"))
            # 缓存上限保护：>64 条清空（防时间戳文件累积导致内存膨胀）
            if len(_READ_CACHE) > 64:
                _READ_CACHE.clear()
            _READ_CACHE[_key] = (_mt, _data)
            return _data
        except Exception:
            pass
    return {}


def _bars_version() -> tuple:
    """Physical version only invalidates snapshots; readiness requires actual content queries."""
    try:
        from deck.daily_status import source_version
        return source_version()
    except Exception:
        return ()


def _complete_daily_status(*, wait=False):
    from deck.daily_status import latest_complete_status
    minimum = int(_health_config().get("daily_complete_min_symbols", 4000))
    if minimum < 1:
        return {"ready": False, "state": "failed", "date": None, "reason": "日线完整性门槛配置无效"}
    return latest_complete_status(minimum, wait=wait)


def _research_signal_view(signal):
    """Research layers are observations, never an account, order or implicit hold plan."""
    research = signal.get("kind") == "research_summary"
    return {"kind": signal.get("kind"), "research_only": research,
            "auto_trade_allowed": False if research else None,
            "has_hold_plan": isinstance(signal.get("hold_plan"), list) and not research,
            "has_buy_plan": isinstance(signal.get("buy_plan"), list) and not research,
            "counts": {name: len(signal[name]) if isinstance(signal.get(name), list) else None for name in ("watch", "candidate", "decision", "pitch")},
            "date": signal.get("date"), "regime": signal.get("regime") or {},
            "source_files": signal.get("source_files") or [],
            "note": signal.get("note") or ("当前产物只汇总研究层，不代表持仓、委托或成交" if research else "本项为历史研究记录，实时账户另见实时模拟盘")}


def live_forward(force: bool = False) -> dict:
    """只读已落盘远期池及数据源状态；收益更新仅由研究管道负责。
    ★2026-08-10 性能优化：缓存键 = 数据文件版本（mtime）——数据源不变则 1h 内直接命中
    （重算 2.3s 只发生一次；命中 0ms 不查库）"""
    now = time.time()
    pool_file = _latest_file("pitch_track_pool_*.json")
    proof_file = BASE / "runtime/research/pipeline-state.json"
    ver = (_bars_version(), pool_file.name if pool_file else None,
           pool_file.stat().st_mtime if pool_file else None,
           proof_file.stat().st_mtime if proof_file.is_file() else None)
    if not force and _fwd_cache["data"] is not None \
            and _fwd_cache.get("ver") == ver \
            and now - _fwd_cache["ts"] < 3600:
        return _fwd_cache["data"]
    try:
        from factors.opportunities.pitch_track import load_latest, summary
        pool = load_latest()
        s = summary()
    except Exception as e:
        return {"ok": False, "error": str(e)[:120]}
    daily_status = _complete_daily_status()
    mx = daily_status.get("date") if daily_status.get("ready") else None
    out = {
        "ok": True,
        "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "pool_ts": pool.get("ts", ""),
        "n_entries": len(pool.get("entries", [])),
        "entries": pool.get("entries", []),
        "summary": s,
        "bars_latest": mx,
        "daily_status": daily_status,
        "note": f"数据源（bars.db）最新 {mx}；下一交易日收盘后 T+1 自动填充" if mx else "",
    }
    from deck.research_views import verified_artifact, unavailable
    verified = verified_artifact(pool_file)
    mature = any(any(isinstance(value,dict) and value.get("ret") is not None for key,value in (entry.get("fwd") or {}).items() if key in ("t1","t5","t20","t60")) for entry in pool.get("entries",[]))
    if not verified:
        out.update(unavailable("远期观察产物尚未经过本轮算法及来源校验"))
    else:
        state="ready" if mature else "pending_history"
        reason="已读取实际到期样本" if mature else "真实观察起点已登记，等待下一实际交易日和持有窗口成熟；没有补造未来收益"
        out.update(state=state,initialized=True,ready=mature,reason=reason)
    if daily_status.get("state") != "probing":
        _fwd_cache.update({"ts": now, "data": out, "ver": ver})
    return out


def live_factors() -> dict:
    """因子池聚合：档案（或最新生成）+ 增强报告 + 拥挤度 + 裁决 + 各文件生成时间"""
    out = {"ok": True, "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "sources": {}}
    # 档案（★F3：glob 时间戳版优先；_2* 排除旧的 _h120/_h20 变体）
    arch_f = _latest_file("因子档案_2*.json", "output")
    if arch_f:
        try:
            out["archive"] = json.loads(arch_f.read_text(encoding="utf-8"))
            out["sources"]["archive"] = time.strftime("%Y-%m-%d %H:%M",
                                                      time.localtime(arch_f.stat().st_mtime))
        except Exception:
            pass
    # 增强报告
    enh = _read("factor_pool_report_enhanced_*.json", "report")
    if enh:
        out["enhanced"] = enh
        ef = _latest_file("factor_pool_report_enhanced_*.json", "report")
        out["sources"]["enhanced"] = time.strftime("%Y-%m-%d %H:%M",
                                                   time.localtime(ef.stat().st_mtime)) if ef else ""
    # 拥挤度（★F5：glob 时间戳版）
    crowd = _read("factor_crowding_*.json", "report")
    if crowd:
        out["crowding"] = crowd
    # 裁决
    verdict = _read("factor_pool_report_verdict_*.json", "report")
    if verdict:
        out["verdict"] = verdict
    # 基本面因子（★F5：glob 时间戳版）
    fund = _read("fundamental_factor_report_*.json", "report")
    if fund:
        out["fundamental"] = fund
    # 因子池报告（基础版固定名 factor_pool_report.json；enhanced/verdict 由各自 _read 独立读取，
    #   ★#357 勿用宽 glob 会错拿 enhanced/verdict 文件）
    fp = _read("factor_pool_report.json", "report")
    if fp:
        out["pool"] = fp
    # ★因子池回测全景（外包 60 因子全历史回测，2026-08-10 15:59 → 解析器 factor_pool_backtest.py）
    btf = _latest_file("factor_pool_backtest_*.json", "report")
    if btf:
        try:
            bt = json.loads(btf.read_text(encoding="utf-8"))
            st = bt.get("reports", {}).get("stats", {})
            out["backtest"] = {
                "ts": bt.get("ts", ""),
                "n_reports": st.get("n_reports", 0),
                "n_ok": st.get("n_ok", 0),
                "n_fail": st.get("n_fail", 0),
                "n_mono": st.get("n_mono", 0),
                "n_strong": st.get("n_strong", 0),
                "n_neg": st.get("n_neg", 0),
                "top": [{"factor": r["factor"], "cn": r.get("cn", ""), "icir": r["icir"],
                         "ls_sharpe": r.get("ls_sharpe"), "mono": r.get("mono")}
                        for r in sorted([x for x in bt.get("reports", {}).get("reports", [])
                                         if x.get("ok") and x.get("icir") and abs(x["icir"]) >= 0.5
                                         and x["factor"] not in {u["factor"] for u in bt.get("short_term", {}).get("future_func", [])}],
                                        key=lambda x: -abs(x["icir"]))[:8]],
                "leak": [u["factor"] for u in bt.get("short_term", {}).get("future_func", [])],
                "usable_event": [u["factor"] for u in bt.get("short_term", {}).get("usable", [])],
                "fund_top": [{"factor": r["factor"], "hold": r["hold"], "icir": r["icir"]}
                             for r in bt.get("fundamental", {}).get("top", [])],
                "p2_sharpe": (bt.get("combo", {}).get("p2") or {}).get("main", {}).get("sharpe"),
            }
            out["sources"]["backtest"] = time.strftime("%Y-%m-%d %H:%M",
                                                       time.localtime(btf.stat().st_mtime))
        except Exception:
            pass
    return out


def live_audit() -> dict:
    """数据审计实时（report/data_audit_report_*.json 最新——时间戳文件名，写保护免疫）"""
    f = _latest_file("data_audit_report_*.json", "report")
    if f:
        try:
            d = json.loads(f.read_text(encoding="utf-8"))
            d["generated_at"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(f.stat().st_mtime))
            d["file"] = f.name
            return d
        except Exception:
            pass
    return {"ok": False, "error": "审计文件缺失"}


def live_opp() -> dict:
    """机会池全量（★F5：Python 端预打双轨标记 track=科技/价值，JS 端纯渲染）"""
    d = _read("opp_pool_*.json")
    if not d:
        return {"ok": False, "error": "opp_pool 未生成"}
    ops = d.get("opportunities", [])
    # ★2026-08-12 百轮#74 修复：in_pitch 标记（第 45 轮功能——scan 从未写该字段，
    #   机会池"✅ 审批"绿标一直失效）→ live_opp 响应时按 pitch 列表补标
    # ★2026-08-12 百轮#75：改用 pitch_v2（过滤后）——与 Pitch 展示一致（opp_pool 原始未过滤 11 vs 展示 10）
    try:
        _pv = _read("pitch_v2_*.json")
        _pcodes = {p.get("code") for p in _pv.get("pitch", [])} if _pv else \
                  {p.get("code") for p in d.get("pitch", [])}
        for o in ops:
            o["in_pitch"] = o.get("code") in _pcodes
    except Exception:
        pass
    # ★2026-08-12 百轮#103：降权类型标记（实盘裁决体系落池——浏览机会池即知哪些类型降权）
    #   按 otype_name 匹配 down_warn label（短线情绪/价值重估 → 🔻 降权提示中）
    try:
        _dwl = {x["label"] for x in ((live_validation().get("diagnosis") or {}).get("down_warn") or [])}
        for o in ops:
            _otn = o.get("otype_name") or ""
            o["down_type"] = _otn if _otn in _dwl else None
    except Exception:
        pass
    cards = {}
    try:
        cf = _latest_file("策略标注卡片*.json", "output")
        if cf:
            cd = json.loads(cf.read_text(encoding="utf-8"))
            if isinstance(cd, dict):
                cd = cd.get("cards", [])
            for c in cd:
                if isinstance(c, dict) and c.get("id"):
                    cards[c["id"]] = c
    except Exception:
        pass
    # 双轨标记（复用 stock_check._is_tech：行业白名单 + 创业板/科创板）
    try:
        import sys as _s
        if str(BASE) not in _s.path:
            _s.path.insert(0, str(BASE))
        from factors.opportunities.stock_check import _is_tech
        for o in ops:
            o["track"] = "科技" if _is_tech(o.get("code", ""), o.get("industry", "")) else "价值"
    except Exception:
        for o in ops:
            o["track"] = "价值"
    # 摘要
    # ★2026-08-14 #423 修复：stats 口径与 opportunities 一致——原 d.get("stats") 是"扫描候选"564 只
    #   （阈值筛选前），而 opportunities 是"筛选后入池"449 只 → 前端类型统计条数字与标题 449 对不上
    #   （用户"机会分类改没了"的观感根因）。改用 opportunities 实算 otype 分布。
    from collections import Counter as _Counter
    # ★2026-08-14 stats 排除 suggest 补位（系统建议非机会类型，不混入类型分布统计）
    stats = dict(_Counter(o.get("otype") for o in ops
                          if o.get("otype") and o.get("otype") != "suggest"))
    n_tech = sum(1 for o in ops if o.get("track") == "科技")
    n_value = sum(1 for o in ops if o.get("track") == "价值")
    # ★F5：总览摘要数据（gate 闸门/审计/择时——JS 端重建 overview 用）
    overview = {}
    try:
        ds = _read("daily_signal_*.json", "output")   # ★2026-08-10 时间戳版 glob（固定名被锁）
        af = _latest_file("data_audit_report_*.json", "report")
        da = json.loads(af.read_text(encoding="utf-8")) if af else {}
        overview = {
            "gate_ok": ds.get("gate", {}).get("ok"),
            "gate_reason": ds.get("gate", {}).get("reason") or ("研究汇总不提供交易闸门结论" if ds.get("kind")=="research_summary" else ""),
            "audit_blocked": da.get("blocked"),
            "audit_block_reason": da.get("block_reason", ""),
            "audit_health": da.get("health"),
            "audit_pass": da.get("n_pass"),
            "audit_fail": da.get("n_fail"),
            "regime_cn": {"strong_uptrend": "强势上行", "uptrend": "上行", "choppy": "震荡",
                          "downtrend": "下行", "strong_downtrend": "深度下行"}.get(
                              ds.get("regime_label", ""), ds.get("regime_label", "—")),
            "regime_level": {"full": "满仓", "half": "减仓", "exit": "离场"}.get(
                ds.get("regime_level", ""), "—"),
            "regime_cash": ds.get("regime_cash_ratio"),
            "n_passed": ds.get("n_passed"),
            "advice": str(ds.get("note") or ds.get("advice") or "尚无结论")[:100],
            "signal_summary": _research_signal_view(ds),
        }
    except Exception:
        pass
    # ★2026-08-11 百轮#46：Pitch 一致性——机会池页展示的 pitch 用 pitch_v2（已过持有期过滤），
    #   与 Pitch 决策台同源（opp_pool.pitch 11 只未过滤 vs pitch_v2 10 只会让两页数字不一致）
    pitch_show = d.get("pitch", [])
    try:
        _pv = _read("pitch_v2_*.json")
        if _pv and _pv.get("pitch"):
            pitch_show = _pv.get("pitch")
    except Exception:
        pass
    # ★2026-08-12 用户需求#180：系统建议标记 + 补位——daily_signal 的 hold_plan（策略决策池 20 只）
    #   与 scan 触发信号（196 只）是两条管线，建议里不在机会池的补进来（📌 建议标记），
    #   用户机会池一屏看全系统建议
    n_suggested = None
    try:
        _sdg = _read("daily_signal_*.json", "output")
        _holds = _sdg.get("hold_plan") if _sdg.get("kind")!="research_summary" else None
        n_suggested = 0 if isinstance(_holds,list) else None
        if _holds:
            _hmap = {x.get("code"): x for x in _holds}
            _have = {o.get("code") for o in ops}
            for o in ops:
                if o.get("code") in _hmap:
                    o["suggested"] = True
                    n_suggested += 1
            for _c, _h in _hmap.items():
                if _c not in _have:
                    # ★2026-08-14 补位条目标记修复：otype 用 "suggest"（非有效机会类型），
                    #   避免 stats 类型分布统计出"今日信号"8 条污染（用户"机会分类"观感）；
                    #   前端按 suggested=True 显示"系统建议"标记，不混入正常机会统计
                    ops.append({"code": _c, "name": _h.get("name") or _c, "otype": "suggest",
                                "score": None, "suggested": True, "track": "价值",
                                "note": "系统建议（策略决策池）", "industry": ""})
                    _have.add(_c)
                    n_suggested += 1
    except Exception:
        pass
    return {
        "ok": True,
        "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "date": d.get("date", ""),
        "n": d.get("n", len(ops)),
        "n_suggested": n_suggested,
        "thresholds": d.get("thresholds", {}),
        "stats": stats,
        "n_tech": n_tech, "n_value": n_value,
        "opportunities": ops,
        "pitch": pitch_show,
        "cards": cards,
        "overview": overview,
        "file": _latest_file("opp_pool_*.json").name if _latest_file("opp_pool_*.json") else "",
    }


def live_watch() -> dict:
    """观察池三层（output/pool_layers_*.json 最新，★U1-3 固定名被锁改 glob）
    ★2026-08-12 百轮#103：decision 层加 down_type 降权标记（实盘裁决体系落池）"""
    d = _read("pool_layers_*.json", "output")
    # 降权类型标记（按 otype 匹配 down_warn label）
    try:
        _dwl = {x["label"] for x in ((live_validation().get("diagnosis") or {}).get("down_warn") or [])}
        for _layer in ("watch", "candidate", "decision"):
            for it in d.get(_layer, []):
                _otn = it.get("otype_name") or it.get("otype") or ""
                it["down_type"] = _otn if _otn in _dwl else None
    except Exception:
        pass
    return {
        "ok": True,
        "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "date": d.get("date", ""),
        "capital": d.get("capital", ""),
        "regime_cash": d.get("regime_cash", ""),
        "n_watch": len(d.get("watch", [])), "n_candidate": len(d.get("candidate", [])),
        "n_decision": len(d.get("decision", [])),
        "watch": d.get("watch", []), "candidate": d.get("candidate", []),
        "decision": d.get("decision", []),
        "rules": d.get("rules", {}),
    }


def live_holdings() -> dict:
    """持有池聚合：组合风控 + 模拟盘双轨 + 今日清单 + 真实持仓 + ★止盈状态（2026-08-11 百轮#4）"""
    pr = _read("position_risk_*.json", "report")   # ★2026-08-11 百轮#11 时间戳版 glob（固定名被锁）
    st = _read("sim_tracks*.json")   # ★#357 时间戳 glob（固定名 sim_tracks.json 是 08-09 旧文件，读它=3 天滞后）
    ds = _read("daily_signal_*.json", "output")   # ★2026-08-10 时间戳版 glob
    tp = _read("take_profit_signals_*.json")      # ★2026-08-11 止盈引擎（持仓止盈状态）
    sa = _read("stop_alerts_*.json")              # ★2026-08-11 止损预警（池级监测）
    pf = {}
    portfolio_recorded = False
    try:
        import sys as _s
        if str(BASE) not in _s.path:
            _s.path.insert(0, str(BASE))
        from strategy.portfolio import _load, _latest_glob, PORTFOLIO_JSON
        portfolio_recorded = _latest_glob("portfolio_*.json",PORTFOLIO_JSON).is_file()
        pf = _load()
    except Exception:
        pass
    # ★止盈状态挂到持仓条目（按 code 匹配）
    tp_map = {}
    if tp and isinstance(tp.get("positions"), list):
        for _s2 in tp["positions"]:
            tp_map[_s2.get("code")] = _s2
    for _p in pf.get("positions", []):
        _t = tp_map.get(_p.get("code"))
        if _t:
            _p["take_profit"] = _t
    # ★2026-08-12 百轮后#116：持仓降权类型标注（#104 复盘发现 3/4 持仓属降权提示类型——
    #   持仓页显示"所持类型降权观察中"，提醒持有者理解类型状态；降权针对新审批从严）
    try:
        _vt = live_validation().get("diagnosis") or {}
        _type_map = {t.get("otype"): t for t in _vt.get("by_type") or []}
        _down_set = {t.get("otype") for t in _vt.get("by_type") or [] if t.get("action") == "降权提示"}
        # 远期池补 otype（tech 线持仓 portfolio 可能无类型字段）
        _pool_map = {}
        _pool0 = _read("pitch_track_pool_*.json", "logs")
        if _pool0:
            for _e in _pool0.get("entries", []):
                if _e.get("code") and _e.get("otype"):
                    _pool_map[_e["code"]] = _e["otype"]
        for _p in pf.get("positions", []):
            _ot = _p.get("otype") or _pool_map.get(_p.get("code"))
            _p["otype"] = _ot
            _ti = _type_map.get(_ot)
            _p["otype_name"] = (_ti or {}).get("label")
            if _ot in _down_set:
                _p["down_type"] = (_ti or {}).get("label") or _ot
    except Exception:
        pass
    # ★2026-08-12 百轮后#119：持仓名称/行业补全（portfolio 无 name/industry 字段——
    #   stock_basic 匹配；名称缺失曾导致持仓显示为裸 code）
    try:
        import sqlite3 as _sq
        _con = _sq.connect("file:data/cache/stock_basic.db?mode=ro&immutable=1", uri=True, timeout=3)
        for _p in pf.get("positions", []):
            _c = _p.get("code", "")
            if not _p.get("name") or _p["name"] == _c:
                _r = _con.execute("SELECT name, industry FROM stock_basic WHERE code=?", (_c,)).fetchone()
                if _r:
                    _p["name"] = _r[0] or _c
                    _p["industry"] = _r[1]
        _con.close()
    except Exception:
        pass
    # ★2026-08-11 百轮#38：盈亏总览（持仓有 entry_price 才能算；缺失尝试回填）
    pnl = portfolio_pnl()
    # ★2026-08-12 百轮后#119：行业敞口（持仓行业分布——集中度风险可视化；Pitch 候选分布对照）
    _ind_expo = {}
    try:
        for _p in pf.get("positions", []):
            _ind = _p.get("industry") or "未知"
            _ind_expo[_ind] = _ind_expo.get(_ind, 0) + 1
    except Exception:
        pass
    # ★2026-08-11 百轮#40：组合绩效（净值曲线 + 回撤 + 交易统计）
    perf = portfolio_perf()
    # ★2026-08-12 百轮后#124：pnl 由 portfolio_pnl() 独立 _load() 原始文件（name=裸 code）——
    #   用补全后的 pf 同步修正 pnl.rows 的 name（#119 补全只作用于 pf 内存副本）
    try:
        _name_map = {_p.get("code"): (_p.get("name") or _p.get("code")) for _p in pf.get("positions", [])}
        for _r in (pnl or {}).get("rows", []):
            _n = _name_map.get(_r.get("code"))
            if _n and _n != _r.get("code"):
                _r["name"] = _n
    except Exception:
        pass
    # ★#354 数据流通：pnl.rows 的 last_price/ret 回填到 pf.positions——持仓卡直接用现价
    #   （原 pf 原始 JSON 无 last_price 字段，前端持仓卡"现价"永远 —，盈亏/现价不通）
    try:
        _prow = {_r.get("code"): _r for _r in (pnl or {}).get("rows", [])}
        for _p in pf.get("positions", []):
            _pr = _prow.get(_p.get("code"))
            if _pr:
                if _pr.get("last_price") is not None:
                    _p["last_price"] = _pr["last_price"]
                if _pr.get("ret") is not None:
                    _p["ret"] = _pr["ret"]
                if _pr.get("price_src"):
                    _p["price_src"] = _pr["price_src"]
    except Exception:
        pass
    # ★2026-08-14 持仓页超限透明：over_limit 记录（超 5 只纪律拒绝）与 holding 分离——
    #   前端 KPI 只数 holding，over_limit 单独列"超限待处理"（原混在一起显示 6/5 且无标注）
    _held = [p for p in pf.get("positions", []) if p.get("status") == "holding"]
    _over = [p for p in pf.get("positions", []) if p.get("status") == "over_limit"]
    pf["_held"] = _held
    pf["_over"] = _over
    pf["n_holding"] = len(_held) if portfolio_recorded else None
    pf["n_over_limit"] = len(_over) if portfolio_recorded else None
    # ★2026-08-14 效率优化：stop_alerts 全池监测（114 条）仅持仓页使用其中持仓相关几条——
    #   裁剪为持仓相关条目（前端只用这些），payload 130KB→小，且语义聚焦"我的持仓预警"
    try:
        _held_codes = {p.get("code") for p in _held}
        if isinstance(sa, dict) and isinstance(sa.get("entries"), list):
            sa = dict(sa)
            sa["entries"] = [e for e in sa["entries"] if e.get("code") in _held_codes]
    except Exception:
        pass
    return {
        "ok": True,
        "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        # ★#144 UI 联动补全：持仓页数据更新 toast（原无 date/file → 前端"数据已更新"提示失效）
        "date": (pnl or {}).get("date") or (pr or {}).get("date") or "",
        "file": (pnl or {}).get("file") or "",
        "position_risk": pr,
        "sim_tracks": st,
        "daily_signal": ds,
        "signal_summary": _research_signal_view(ds),
        "portfolio_recorded": portfolio_recorded,
        "portfolio": pf,
        "pnl": pnl,           # ★2026-08-11 百轮#38 持仓盈亏总览（组合/个股）
        "industry_exposure": _ind_expo,   # ★2026-08-12 百轮后#119 行业敞口（持仓分布）
        "perf": perf,         # ★2026-08-11 百轮#40 组合绩效（净值/回撤/交易统计）
        "take_profit": tp,   # ★2026-08-11 止盈信号总览（alerts 触发列表）
        "stop_alerts": sa,   # ★2026-08-11 止损预警（池级监测 triggered/near）
    }


def portfolio_pnl() -> dict:
    """Read-only valuation. Missing recorded cost stays unknown; no historical price is written as a fill."""
    import math
    def positive(value):
        try:
            parsed = float(value)
            return parsed if math.isfinite(parsed) and parsed > 0 else None
        except (TypeError, ValueError):
            return None
    try:
        from strategy.portfolio import _load, _latest_close_of
        pf = _load()
    except Exception:
        return {"ok":False,"error":"portfolio加载失败"}
    positions = [p for p in pf.get("positions",[]) if p.get("status")=="holding"]
    realtime = None
    try:
        realtime = live_realtime() if positions else None
    except Exception:
        pass
    quotes = (realtime or {}).get("quotes") if (realtime or {}).get("ok") and (realtime or {}).get("market_open") is True else {}
    if isinstance(quotes,list):
        quotes = {q.get("symbol") or q.get("code"):q for q in quotes if isinstance(q,dict)}
    if not isinstance(quotes,dict):
        quotes = {}
    rows = []
    for position in positions:
        code = position.get("code","")
        entry = positive(position.get("entry_price"))
        quote = quotes.get(code) or {}
        last = positive(quote.get("price") if quote.get("price") is not None else quote.get("last"))
        price_source = "realtime" if last is not None else "daily"
        if last is None:
            last = positive(_latest_close_of(code))
        quantity = positive(position.get("quantity") if position.get("quantity") is not None else position.get("shares"))
        cost = positive(position.get("cost"))
        if cost is None and entry is not None and quantity is not None:
            cost = entry * quantity
        value = last * quantity if last is not None and quantity is not None else cost * last / entry if cost is not None and last is not None and entry is not None else None
        ret = last / entry - 1 if last is not None and entry is not None else None
        reason = "缺少已记录的真实成交成本，未用市场价格或历史价格补写" if entry is None else "缺少实际股数或成本金额，金额估值不可核验" if cost is None or value is None else "按已记录成本与实际行情估值"
        rows.append({"code":code,"name":position.get("name",code),"entry_date":position.get("entry_date"),
                     "entry_price":entry,"last_price":last,"ret":ret,"quantity":quantity,"cost":cost,"market_value":value,
                     "status":position.get("status"),"price_src":price_source,"reason":reason})
    complete = bool(rows) and all(row["cost"] is not None and row["market_value"] is not None and row["entry_price"] is not None for row in rows)
    total_cost = sum(row["cost"] for row in rows) if complete else None
    total_value = sum(row["market_value"] for row in rows) if complete else None
    return {"ok":True,"ts":datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"n_holdings":len(rows),
            "valuation_complete":complete,"total_cost":round(total_cost,2) if total_cost is not None else None,
            "total_value":round(total_value,2) if total_value is not None else None,
            "total_ret":round(total_value/total_cost-1,4) if total_cost else None,"rows":rows,
            "reason":"真实持仓金额与成本已核验" if complete else "当前无持仓" if not rows else "部分持仓缺少实际成本或数量，组合金额收益保持未知"}


def live_tech() -> dict:
    """科技突破池全量（logs/tech_pitch_*.json 最新）"""
    d = _read("tech_pitch_*.json")
    if not d:
        return {"ok": False, "error": "tech_pitch 未生成"}
    return {
        **d,
        "ok": True,
        "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "threshold": d.get("threshold"),
        "pool_date": d.get("pool_date", ""),
        "entries": d.get("entries", []),
        "new_codes": d.get("new_codes", []),
        "file": _latest_file("tech_pitch_*.json").name if _latest_file("tech_pitch_*.json") else "",
    }


def live_pools() -> dict:
    """各池状态汇总（待处理面板数据源）"""
    pool = _read("opp_pool_*.json")
    pitch = _read("pitch_v2_*.json")
    tech = _read("tech_pitch_*.json")
    stop = _read("stop_alerts_*.json")
    track = _read("pitch_track_pool_*.json")
    # ★#347 持仓止损预警：只统计真实持仓（holding）的临近/触发——远期池全量监测（monitored 86 只）
    #   不是"我的持仓预警"，门户 KPI 应显示持仓维度（机器池/自动池自动止损止盈不在此提醒，#340 精神）
    held_trig = held_near = held_n = None
    portfolio_recorded = False
    try:
        # ★2026-08-14 口径修复：只算真实持仓（holding），排除 over_limit（超 5 只纪律拒绝非持仓）
        #   （原 positions() 含 over_limit → 门户 KPI"持仓 6"与实际 5 不符）
        from strategy.portfolio import _load as _pl, _latest_glob, PORTFOLIO_JSON
        portfolio_recorded = _latest_glob("portfolio_*.json",PORTFOLIO_JSON).is_file()
        _pos_codes = {p["code"] for p in _pl().get("positions", [])
                      if p.get("status") == "holding"}
        if portfolio_recorded:
            held_n = len(_pos_codes)
            if not _pos_codes or isinstance(stop.get("entries"),list):
                held_trig = held_near = 0
        for _e in (stop.get("entries") or []):
            if portfolio_recorded and held_trig is not None and _e.get("code") in _pos_codes:
                if _e.get("status") == "TRIGGERED":
                    held_trig += 1
                elif _e.get("status") == "NEAR":
                    held_near += 1
    except Exception:
        pass
    # ★#348 类型分布（top 3 类型 + 数量）——前端动态显示，不写死"价值/重估/质量"这类文字
    def _otype_dist(items, key="otype"):
        from collections import Counter
        c = Counter()
        for it in items:
            ot = it.get(key) or it.get("otype_name") or ""
            if ot:
                c[ot] += 1
        return [{"otype": k, "n": v} for k, v in c.most_common(3)]
    return {
        "ok": True,
        "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "opp_pool": {"n": len(pool.get("opportunities", [])), "date": pool.get("date", "")},
        "pitch": {"n": len(pitch.get("pitch", [])), "date": pitch.get("date", ""),
                  "types": _otype_dist(pitch.get("pitch", []))},
        "tech_pitch": {"n": len(tech.get("entries", [])), "new": len(tech.get("new_codes", [])),
                       "date": tech.get("pool_date", ""),
                       "types": _otype_dist(tech.get("entries", []), key="otype")},
        "stop_alerts": {"triggered": stop.get("triggered"), "near": stop.get("near"),
                        "monitored": stop.get("monitored"), "ts": stop.get("ts", ""),
                        "portfolio_recorded":portfolio_recorded,
                        "reason":"尚无历史审批组合记录，持仓预警数量未知" if not portfolio_recorded else "已读取实际审批持仓及预警记录" if held_trig is not None else "持仓存在，但预警产物尚未生成",
                        "held_triggered": held_trig, "held_near": held_near, "held_n": held_n},
        "forward_track": {"n": len(track.get("entries", [])), "ts": track.get("ts", "")},
    }


def live_pool() -> dict:
    """★2026-08-10 池子总览聚合（UI 合并设计：观察池+机会池+科技池一页 tab 切换）"""
    return {"ok": True, "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "opp": live_opp(), "watch": live_watch(), "tech": live_tech()}


def live_alerts() -> dict:
    """★2026-08-11 百轮#41：全局预警中心——聚合需要人注意的信号
    止盈触发 / 止损临近触发 / 择时不适合 / 组合风控告警 / 数据断链
    ★2026-08-12 #150 性能：数据源 mtime 版本缓存（原每次 2.3s → 命中 5ms）"""
    import glob as _g
    import os as _os
    from pathlib import Path as _P
    _now = time.time()
    _ver = _bars_version()
    if _alrt_cache["data"] is not None and _alrt_cache.get("ver") == _ver \
            and _now - _alrt_cache["ts"] < 120:
        return _alrt_cache["data"]
    alerts = []

    def _latest(pat, sub):
        fs = sorted([_P(p) for p in _g.glob(str(BASE / sub / pat))], key=lambda x: x.stat().st_mtime)
        return fs[-1] if fs else None

    # 1) 止盈触发
    _tp = _latest("take_profit_signals_*.json", "logs")
    if _tp:
        try:
            tp = json.loads(_tp.read_text(encoding="utf-8"))
            for a in (tp.get("alerts") or []):
                alerts.append({"level": "high", "cat": "止盈",
                               "msg": f"{a.get('code')} {a.get('name','')} 触发止盈（{a.get('type','')}）",
                               "code": a.get("code")})
        except Exception:
            pass
    # ★2026-08-16 止损预警条整体删除（已触发/接近，用户要求）
    # 3) 择时不适合
    _tm = _latest("timing_system_*.json", "output")
    if _tm:
        try:
            tm = json.loads(_tm.read_text(encoding="utf-8"))
            if "不适合" in str(tm.get("level", "")):
                alerts.append({"level": "high", "cat": "择时",
                               "msg": f"市场择时判定不适合买入（{tm.get('score')} 分）"})
            elif "谨慎" in str(tm.get("level", "")):
                alerts.append({"level": "mid", "cat": "择时",
                               "msg": f"市场择时谨慎买入（{tm.get('score')} 分）"})
        except Exception:
            pass
    # 4) 组合风控告警
    _pr = _latest("position_risk_*.json", "report")
    if _pr:
        try:
            pr = json.loads(_pr.read_text(encoding="utf-8"))
            fl = pr.get("flags") or {}
            if fl.get("industry_high"):
                pct = pr.get("concentration_industry")
                alerts.append({"level": "mid", "cat": "风控",
                               "msg": f"单行业超限 {pr.get('top_industry','')}"
                                      + (f" {float(pct)*100:.0f}%" if isinstance(pct, (int, float)) else "")})
            if fl.get("high_corr"):
                alerts.append({"level": "mid", "cat": "风控", "msg": "组合高相关（伪分散）"})
            if fl.get("deep_drawdown"):
                alerts.append({"level": "mid", "cat": "风控",
                               "msg": f"{fl.get('deep_drawdown')} 只持仓深回撤（-60日>阈值）"})
        except Exception:
            pass
    # 5) 数据断链（决策链非绿）——★#392 supplier-lag（note 含"内容滞后/供应商"）不算断链（竞价信号供应商缺口非系统故障）
    try:
        chain = live_chain().get("chain", [])
        for n in chain:
            if n.get("state") in ("optional_unconfigured","pending_history"):
                continue  # Expected history/extensions remain visible in the chain, not a broken pipeline alarm.
            if not n.get("ok") and n.get("state") in ("rate_limited","permission_blocked","probing","initializing"):
                alerts.append({"level":"mid","cat":"数据核验","msg":n["name"]+"："+(n.get("note") or n.get("state"))})
                continue
            if not n.get("ok") and not ("内容滞后" in (n.get("note") or "") or "供应商" in (n.get("note") or "")):
                alerts.append({"level": "high", "cat": "数据",
                               "msg": f"{n['name']} 数据断链（{n.get('file') or '无文件'}）"})
    except Exception:
        pass
    # 6) 因子池面板质量（★百轮#70：08-11 真发生过——daily CSV 五强 rank 缺失自动回退旧文件）
    try:
        import pandas as _pd
        _F5 = ("turn_mid_prox", "sentiment", "turnover", "reversal20", "lowvol")
        # ★2026-08-12 百轮后#130：mtime 排序（#124 同款免疫——文件名排序会选中半成品 v2 文件
        #   五强 2/5 导致误报"面板退化"；与 scan.py _best_daily_file 语义对齐）
        _fs = sorted(_g.glob("data/factorpool/output/daily_scores/daily_*.csv"),
                     key=lambda p: _P(p).stat().st_mtime)
        if _fs:
            # ★2026-08-12 百轮后#130：nrows=3000 采样误判（#101 同款——新面板 turn 类列头部缺失
            #   但全列非空率 1.0 → 五强误判 2/5 假预警）→ 全量判定（5816 行可接受）
            _df = _pd.read_csv(_fs[-1])
            _ok5 = sum(1 for f in _F5 if f"{f}_rank" in _df.columns
                       and _df[f"{f}_rank"].notna().mean() >= 0.5)
            if _ok5 < 4:
                alerts.append({"level": "high", "cat": "数据",
                               "msg": f"因子池面板退化（{_P(_fs[-1]).name} 五强仅 {_ok5}/5 列可用）→ 已回退旧文件"})
            # ★2026-08-15 覆盖率告警（08-14 案例：turnover_rank 58% 静默通过 0.5 门槛 → 五强共识降级、候选宇宙收窄）
            #   五强任一因子覆盖 <75% → 门户 mid 告警，决策链使用者可见而非静默降级
            _cov_low = []
            for _f5 in _F5:
                _c = f"{_f5}_rank"
                if _c in _df.columns:
                    _cov = float(_df[_c].notna().mean())
                    if _cov < 0.75:
                        _cov_low.append(f"{_f5} {_cov*100:.0f}%")
            if _cov_low:
                alerts.append({"level": "mid", "cat": "数据",
                               "msg": f"因子池五强覆盖偏低：{'、'.join(_cov_low)}（<75% → 决策链共识降级，待外包重生成）"})
        for _pat, _label, _dir in (("market_snapshot_ext*.json", "市场快照", None),
                                   ("risk_multiplier_*.json", "FRC 风控系数", "risk")):
            _sd = "data/factorpool/output"
            _base = (_P(_sd).parent / _dir) if _dir else _P(_sd)   # risk 在 因子池/risk/（output 的父级）
            if not _g.glob(str(_base / _pat)) and not _g.glob(_sd + "/" + _pat):
                alerts.append({"level": "mid", "cat": "数据", "msg": f"外包 {_label} 缺失（择时/风控降级 bars 兜底）"})
    except Exception:
        pass
    # 7) 日历窗口（H16/H17 实证，★百轮#70）
    try:
        from data.calendar_hook import get_window, upcoming
        _cw = get_window()
        if _cw:
            alerts.append({"level": "mid" if _cw["bonus"] < 0 else "ok", "cat": "日历",
                           "msg": f"{_cw['label']}（{'减分' if _cw['bonus']<0 else '加分'} {_cw['bonus']}，{_cw['evidence']}）"})
        else:
            for _up in upcoming(horizon_days=7):
                alerts.append({"level": "ok", "cat": "日历",
                               "msg": f"{_up['days_to']} 天后 {_up['label']}（{'+' if _up['bonus']>=0 else ''}{_up['bonus']} 分）"})
    except Exception:
        pass
    # 8) FRC 因子风控生效（★百轮#70）
    try:
        # ★2026-08-14 #434：取「因子数最多」而非文件名最新（#268 同款——外包 --only 增量
        #   重算覆盖主文件，文件名最新可能是残缺 5 因子版；与 scan.py load_risk_multiplier 对齐）
        _rmfs = sorted(_g.glob("data/factorpool/risk/risk_multiplier_*.json"),
                       key=lambda x: _os.path.getmtime(x))
        if _rmfs:
            _rm, _best_n = None, -1
            for _rp in _rmfs:
                try:
                    _d = json.loads(_P(_rp).read_text(encoding="utf-8"))
                    _f = _d.get("factors", {})
                    _n_eff = sum(1 for v in _f.values() if isinstance(v, dict) and "eff" in v)
                    if _n_eff > _best_n:
                        _rm, _best_n = _d, _n_eff
                except Exception:
                    continue
            if _rm is None:
                _rm = json.loads(_P(_rmfs[-1]).read_text(encoding="utf-8"))
            _facs = _rm.get("factors", {})
            _n0 = sum(1 for v in _facs.values() if v.get("eff", 1) == 0)
            _nlt = sum(1 for v in _facs.values() if 0 < v.get("eff", 1) < 1)
            _tot = len(_facs)
            if _tot and (_n0 + _nlt) / _tot >= 0.30:
                alerts.append({"level": "mid", "cat": "风控",
                               "msg": f"FRC 因子风控生效中（{_n0} 禁用/{_nlt} 降权，共 {_tot} 因子）"})
    except Exception:
        pass
    # 9) 大小盘分化（H27，★百轮#70）
    try:
        _snfs = sorted(_g.glob("data/factorpool/output/market_snapshot_ext*.json"),
                       key=lambda x: _os.path.getmtime(x))
        if _snfs:
            _sp = json.loads(_P(_snfs[-1]).read_text(encoding="utf-8"))
            _dv = _sp.get("divergence_60")
            _m60 = _sp.get("mkt_mom60")
            if _dv is not None and abs(float(_dv)) >= 0.095:
                alerts.append({"level": "mid", "cat": "风控",
                               "msg": f"大小盘分化 {abs(float(_dv))*100:.0f}pp（等权 {float(_m60)*100:+.1f}%）——结构性行情，注意风格"})
    except Exception:
        pass
    # 10) 强因子直通极强（≥6 家族，机会参考——★百轮#70）
    try:
        _sh = live_strong_hits()
        if _sh.get("ok") and _sh.get("n_extreme"):
            alerts.append({"level": "ok", "cat": "机会",
                           "msg": f"强因子直通极强 {_sh['n_extreme']} 只（≥6 家族独立证据，见机会池💪直通榜）"})
    except Exception:
        pass
    # 11) ★全站健康扫描（2026-08-12 百轮#82：页面/API/一致性——dev_auto 8.59 每 4h 产出）
    try:
        _hs = _latest_file("health_scan_*.json", "report")
        if _hs:
            hs = json.loads(_hs.read_text(encoding="utf-8"))
            _age = round((datetime.now().timestamp() - _os.path.getmtime(_hs)) / 3600, 1)
            if not hs.get("all_ok", True):
                for _b in (hs.get("bad") or [])[:3]:
                    alerts.append({"level": "high", "cat": "系统",
                                   "msg": f"健康扫描失败：{_b}"})
            elif _age > 26:
                alerts.append({"level": "mid", "cat": "系统",
                               "msg": f"健康扫描 {_age}h 未更新（dev_auto 可能异常）"})
    except Exception:
        pass
    # 12) ★类型降权提示（2026-08-12 百轮#89/#93：实盘归因 → 触发建议——数据驱动决策）
    try:
        _vw = live_validation().get("diagnosis") or {}
        for _dw in (_vw.get("down_warn") or [])[:3]:
            _rs = _dw.get("reason", "低于基准")
            alerts.append({"level": "mid", "cat": "类型",
                           "msg": f"⚠️ {_dw.get('text')} {_rs} → 降权提示（Pitch 中该类型审批从严）"})
    except Exception:
        pass
    # ★2026-08-12 十轮#175：管道健康告警（data/pipeline_health.py 落盘——每日信号/机会池等
    #   产物超过阈值未更新 → high 告警，防管道静默中断无人知，如 18:30 exit=143 案例）
    try:
        _phf = sorted(_g.glob(str(BASE / "logs" / "pipeline_health_*.json")),
                      key=os.path.getmtime)
        if _phf:
            _ph = json.loads(_P(_phf[-1]).read_text(encoding="utf-8"))
            for _pa in (_ph.get("alerts") or []):
                alerts.append({"level": _pa.get("level", "mid"), "cat": _pa.get("cat", "管道"),
                               "msg": _pa.get("msg", "")})
    except Exception:
        pass
    alerts.sort(key=lambda x: {"high": 0, "mid": 1, "ok": 2}.get(x.get("level"), 2))
    _out = {
        "ok": True,
        "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "n": len(alerts),
        "n_high": sum(1 for a in alerts if a.get("level") == "high"),
        "n_mid": sum(1 for a in alerts if a.get("level") == "mid"),
        "alerts": alerts,
    }
    _alrt_cache.update({"ts": _now, "data": _out, "ver": _ver})
    return _out


def live_funnel() -> dict:
    """★2026-08-11 百轮#42：决策漏斗——机会池 → Pitch 候选 → 已审批（"为什么只推这几个"）
    ★百轮#71 升级：加淘汰原因统计（分数未达门槛 vs 达标但名额/子分类受限）+ Pitch 类型分布"""
    import glob as _g
    import os as _os
    from pathlib import Path as _P
    from collections import Counter as _Cnt
    def _latest(pat, sub):
        fs = sorted([_P(p) for p in _g.glob(str(BASE / sub / pat))], key=lambda x: x.stat().st_mtime)
        return fs[-1] if fs else None

    opp_n, pitch_n, tech_n, approved_n = 0, 0, 0, 0
    opp_date = None
    elim, pitch_types = None, []
    _opp = _latest("opp_pool_*.json", "logs")
    if _opp:
        try:
            od = json.loads(_opp.read_text(encoding="utf-8"))
            ops = od.get("opportunities", [])
            opp_n = len(ops)
            pitch_list = od.get("pitch", [])
            pitch_n = len(pitch_list)
            opp_date = od.get("date")
            # ★#71 淘汰原因统计：未进 Pitch 的 = 分数未达门槛 or 达标但同类名额/子分类受限
            pitch_codes = {p.get("code") for p in pitch_list}
            n_gate, n_quota = 0, 0
            for o in ops:
                if o.get("code") in pitch_codes:
                    continue
                if (o.get("score") or 0) < (o.get("pitch_gate") or 70):
                    n_gate += 1
                else:
                    n_quota += 1
            elim = {"n_gate": n_gate, "n_quota": n_quota,
                    "gate_desc": "分数未达门槛(≥70 全局/≥80 同类)",
                    "quota_desc": "达标但同类型名额满(每类≤5)/子分类上限"}
            _tc = _Cnt((p.get("otype_name") or p.get("otype") or "?") for p in pitch_list)
            pitch_types = [{"name": k, "n": v} for k, v in _tc.most_common()]
        except Exception:
            pass
    _tp = _latest("tech_pitch_*.json", "logs")
    if _tp:
        try:
            tech_n = len(json.loads(_tp.read_text(encoding="utf-8")).get("entries", []))
        except Exception:
            pass
    _dc = _latest("deck_decisions_*.json", "logs")
    if _dc:
        try:
            decs = json.loads(_dc.read_text(encoding="utf-8"))
            if isinstance(decs, list):
                approved_n = len({r.get("code") for r in decs if r.get("action") == "buy"})
        except Exception:
            pass
    pitch_total = pitch_n + tech_n
    # ★2026-08-15 全流程漏斗扩展（说明页动态决策流）：源头 → 硬过滤 → 机会池 → Pitch → 待审批 → 持仓 → 远期池
    # Source inventory is already produced by the full offline audit. Do not scan
    # millions of bars again on each UI/endpoint-monitor request.
    bars_rows = None
    try:
        from deck.research_views import verified_artifact
        inventory_path=_latest_file("data_asset_audit_*.json","report")
        if verified_artifact(inventory_path):
            inventory=json.loads(inventory_path.read_text(encoding="utf-8"))
            bars=next((db for db in inventory.get("dbs",[]) if db.get("name")=="bars.db"),{})
            bars_rows=next((table.get("rows") for table in bars.get("tables",[]) if table.get("name")=="daily_bar"),None)
    except Exception:
        pass
    #   2) 硬过滤通过（daily_signal n_passed）
    n_passed = None
    research_signal = {}
    _ds = _latest("daily_signal_*.json", "output")
    if _ds:
        try:
            research_signal = json.loads(_ds.read_text(encoding="utf-8"))
            n_passed = research_signal.get("n_passed")
        except Exception:
            pass
    #   3) 待审批（pitch_v2 + tech 中未 buy/drop 的）
    _decided = set()
    if _dc:
        try:
            _decs = json.loads(_dc.read_text(encoding="utf-8"))
            if isinstance(_decs, list):
                _decided = {r.get("code") for r in _decs if r.get("action") in ("buy", "drop")}
        except Exception:
            pass
    n_pending = 0
    for _pat, _sub in (("pitch_v2_*.json", "pitch"), ("tech_pitch_*.json", "entries")):
        _f2 = _latest(_pat, "logs")
        if not _f2:
            continue
        try:
            _dd = json.loads(_f2.read_text(encoding="utf-8"))
            n_pending += sum(1 for p in _dd.get(_sub, []) if p.get("code") not in _decided)
        except Exception:
            pass
    #   4) 持仓 + 远期池
    n_held = n_forward = None
    try:
        # ★2026-08-15 修复：固定名 portfolio.json 可能是空残留 → 优先取时间戳最新版
        _pf = _latest("portfolio_*.json", "logs") or _latest("portfolio.json", "logs")
        if _pf:
            _pd = json.loads(_pf.read_text(encoding="utf-8"))
            n_held = sum(1 for p in _pd.get("positions", []) if p.get("status") == "holding")
    except Exception:
        pass
    try:
        _pt = _latest("pitch_track_pool_*.json", "logs")
        if _pt:
            n_forward = len(json.loads(_pt.read_text(encoding="utf-8")).get("entries", []))
    except Exception:
        pass
    #   5) 因子数（manifest）
    n_factors = None
    try:
        from deck.research_views import factor_inputs
        _md = factor_inputs()[2]
        if isinstance(_md.get("factors"),list):
            n_factors = len(_md["factors"])
    except Exception as _e:
        print(f"[live_funnel] manifest 读取失败: {_e}", flush=True)
        pass
    return {
        "ok": True,
        "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "opp_date": opp_date,
        "signal_summary": _research_signal_view(research_signal),
        "funnel": [
            {"name": "主库行情", "n": bars_rows, "desc": "真实离线资产审计记录的主库日线总行数（各复权口径合计）；覆盖日期另见数据健康" if bars_rows is not None else "尚无通过来源及文件哈希核验的日线资产清点，行数保持未知"},
            {"name": "实际支持因子", "n": n_factors, "desc": "当前真实因子清单，不沿用旧的静态因子总数"},
            {"name": "候选研究层" if research_signal.get("kind")=="research_summary" else "硬过滤通过", "n": len(research_signal["candidate"]) if research_signal.get("kind")=="research_summary" and isinstance(research_signal.get("candidate"),list) else n_passed, "desc": "研究汇总候选数量，不代表持仓或通过交易风控" if research_signal.get("kind")=="research_summary" else "历史信号明确记录的过滤结果；缺值保持未知"},
            {"name": "机会池", "n": opp_n, "desc": "本次实际研究股票池中的信号机会"},
            {"name": "Pitch 候选", "n": pitch_total, "desc": f"长线 {pitch_n} + 短线 {tech_n}，通过门槛/直通/共识"},
            {"name": "待审批", "n": n_pending, "desc": "Pitch 候选 - 已 buy/drop（决策台待你审批）"},
            {"name": "研究审批持仓", "n": n_held, "desc": "历史审批记录，实时AI账户另见实时模拟盘"},
            {"name": "远期池", "n": n_forward, "desc": "入池后等待真实交易日窗口成熟的前向观察"},
        ],
        "ratio_pitch": round(pitch_total / opp_n, 3) if opp_n else None,
        "elimination": elim,        # ★#71 淘汰原因（为什么 443 → 10）
        "pitch_types": pitch_types, # ★#71 Pitch 类型分布（长线侧重哪些类型）
    }


def live_turnlow_top(top_n: int = 20) -> dict:
    from deck.research_views import turnlow_top
    return turnlow_top(top_n)


def portfolio_perf() -> dict:
    """★2026-08-11 百轮#40：组合绩效——持仓逐日等权收益串成净值曲线 + 回撤 + 交易统计
    ★百轮#69 升级：① bars 读取合并增量库（主库 + 最近 3 个 immutable——原只读主库，
    增量写入后曲线缺最新日，load_panel 同类隐患）② 基准对比 SH.000300（沪深300 同期净值 + 超额）。
    数据源：持仓 entry_date→最新 逐日收盘（bars immutable）+ portfolio.history"""
    import sys as _s
    if str(BASE) not in _s.path:
        _s.path.insert(0, str(BASE))
    try:
        import sqlite3
        from pathlib import Path as _P
        from strategy.portfolio import _load
        pf = _load()
        positions = [p for p in pf.get("positions", [])
                     if p.get("status") in ("holding", "over_limit")]
        if not positions:
            return {"ok": True, "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "n_pos": 0, "series": [], "stats": {}, "note": "无持仓"}
        # ★#69 双库合并（主库 + 最近 3 个增量库 immutable）
        def _dbs():
            import glob as _g
            dbs = ["data/cache/bars.db"]
            inc = sorted(_g.glob("data/cache/bars_incr_*.db"))[-3:]
            return dbs + inc
        def _ro(p):
            return f"{_P(p).as_uri()}?mode=ro&immutable=1"
        def _fetch(code, ed, adjust="qfq"):
            out = {}
            for db in _dbs():
                try:
                    con = sqlite3.connect(_ro(db), uri=True, timeout=3)
                    rows = con.execute(
                        "SELECT date, close FROM daily_bar WHERE code=? AND date>=? AND adjust=? ORDER BY date",
                        (code, ed, adjust)).fetchall()
                    con.close()
                    for dt, cl in rows:
                        if cl is not None:
                            out[dt] = float(cl)   # 增量库覆盖主库（同 key 后写覆盖）
                except Exception:
                    continue
            return out
        daily = {p.get("code", ""): _fetch(p.get("code", ""), p.get("entry_date") or "2000-01-01")
                 for p in positions}
        # 基准：沪深300（从最早持仓日对齐）
        ed0 = min((p.get("entry_date") or "2000-01-01") for p in positions)
        bench = _fetch("SH.000300", ed0, adjust="none")
        # 等权组合日收益 → 净值
        date_set = sorted(set().union(*[set(d.keys()) for d in daily.values()]))
        nav, dates, cum = 1.0, [], []
        prev = {c: None for c in daily}
        daily_rows = []   # ★#109 逐日明细（组合日收益/基准日收益——T+1 首日起自动填充）
        for dt in date_set:
            rets = []
            for c, m in daily.items():
                if dt in m and prev[c] is not None:
                    rets.append(m[dt] / prev[c] - 1)
            for c, m in daily.items():
                if dt in m:
                    prev[c] = m[dt]
            if rets:
                nav *= (1 + sum(rets) / len(rets))
            dates.append(dt)
            cum.append(nav)
            daily_rows.append({
                "date": dt,
                "comb_ret": round(sum(rets) / len(rets), 4) if rets else None,
            })
        # 基准净值（对齐组合日期序列）
        b_prev = None
        b_nav, bench_cum = 1.0, []
        for dt in dates:
            if dt in bench:
                if b_prev is not None:
                    b_nav *= bench[dt] / b_prev
                b_prev = bench[dt]
            bench_cum.append(b_nav)
        # 回撤
        peak, dd = 1.0, []
        for v in cum:
            peak = max(peak, v)
            dd.append(v / peak - 1)
        max_dd = min(dd) if dd else 0.0
        total_ret = cum[-1] - 1 if cum else 0.0
        bench_ret = bench_cum[-1] - 1 if bench_cum else None
        excess = (total_ret - bench_ret) if bench_ret is not None else None
        # 交易统计
        hist = pf.get("history", [])
        wins = [h for h in hist if h.get("entry_price") and h.get("exit_price")
                and h["exit_price"] > h["entry_price"]]
        trades = [h for h in hist if h.get("entry_price") and h.get("exit_price")]
        avg_trade = (sum(h["exit_price"] / h["entry_price"] - 1 for h in trades) / len(trades)
                     if trades else None)
        return {
            "ok": True,
            "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "n_pos": len(positions),
            "dates": dates[-120:],        # 最近 120 个交易日（曲线显示）
            "nav": [round(v, 4) for v in cum[-120:]],
            "drawdown": [round(v, 4) for v in dd[-120:]],
            "bench_nav": [round(v, 4) for v in bench_cum[-120:]],   # ★#69 沪深300 同期
            "daily_rows": daily_rows[-20:],   # ★#109 逐日明细（最近 20 日：date/组合日收益）
            "stats": {
                "total_ret": round(total_ret, 4),
                "max_dd": round(max_dd, 4),
                "days": len(dates),
                "n_trades": len(trades),
                "winrate": round(len(wins) / len(trades), 3) if trades else None,
                "avg_trade": round(avg_trade, 4) if avg_trade is not None else None,
                "bench_ret": round(bench_ret, 4) if bench_ret is not None else None,
                "excess": round(excess, 4) if excess is not None else None,   # ★#69 超额
            },
        }
    except Exception as e:
        return {"ok": False, "error": str(e)[:120]}


def live_actions() -> dict:
    """★2026-08-11 百轮#51：待处理面板动态化——止损触发 / 待审批 Pitch / 新突破（与 dashboard_actions 同构）"""
    import glob as _g
    import os as _os
    from pathlib import Path as _P
    def _latest(pat, sub):
        fs = sorted([_P(p) for p in _g.glob(str(BASE / sub / pat))], key=lambda x: x.stat().st_mtime)
        return fs[-1] if fs else None

    # ① 止损预警（stop_alerts：TRIGGERED 高优先 / NEAR 关注）
    stop_list, n_stop = [], 0
    _sa = _latest("stop_alerts_*.json", "logs")
    if _sa:
        try:
            sa = json.loads(_sa.read_text(encoding="utf-8"))
            es = sa.get("entries", [])
            for e in es:
                st = e.get("status")
                if st not in ("TRIGGERED", "NEAR"):
                    continue
                al = e.get("alerts") or []
                msg = "、".join(f"{a.get('rule')}({a.get('detail','')})" for a in al[:2]) or st
                stop_list.append({"code": e.get("code"), "name": e.get("name"),
                                  "status": st, "level": "high" if st == "TRIGGERED" else "mid",
                                  "msg": msg})
            n_stop = len(stop_list)
        except Exception:
            pass
    # ② 待审批 Pitch（未在 decisions）
    decided = set()
    _dc = _latest("deck_decisions_*.json", "logs")
    if _dc:
        try:
            decs = json.loads(_dc.read_text(encoding="utf-8"))
            if isinstance(decs, list):
                decided = {r.get("code") for r in decs if r.get("action") in ("buy", "drop")}
        except Exception:
            pass
    pending = []
    for pat, sub in (("pitch_v2_*.json", "pitch"), ("tech_pitch_*.json", "entries")):
        _f = _latest(pat, "logs")
        if not _f:
            continue
        try:
            d = json.loads(_f.read_text(encoding="utf-8"))
            items = d.get(sub, [])
            for p in items:
                if p.get("code") not in decided:
                    pending.append({"code": p.get("code"), "name": p.get("name"),
                                    "score": p.get("score"), "line": p.get("pitch_line", "long"),
                                    "tier": p.get("tier", "")})
        except Exception:
            pass
    # ③ 新突破（tech_pitch NEW）
    new_tech = []
    _tp = _latest("tech_pitch_*.json", "logs")
    if _tp:
        try:
            for e in json.loads(_tp.read_text(encoding="utf-8")).get("entries", []):
                if e.get("is_new"):
                    new_tech.append({"code": e.get("code"), "name": e.get("name"), "score": e.get("score")})
        except Exception:
            pass
    return {"ok": True, "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "n_stop": n_stop, "n_pending": len(pending), "n_new": len(new_tech),
            "stop": stop_list, "pending": pending, "new_tech": new_tech}


def live_brief() -> dict:
    """★2026-08-11 百轮#52：每日操作简报——择时/待审批/止损/止盈/组合风险 5 项聚合
    "今天该做什么"一屏结论（actions 页顶部 + 门户）"""
    import glob as _g
    import os as _os
    from pathlib import Path as _P
    def _latest(pat, sub):
        fs = sorted([_P(p) for p in _g.glob(str(BASE / sub / pat))], key=lambda x: x.stat().st_mtime)
        return fs[-1] if fs else None

    items = []
    # 1) 择时
    _tm = _latest("timing_system_*.json", "output")
    if _tm:
        try:
            tm = json.loads(_tm.read_text(encoding="utf-8"))
            items.append({"cat": "择时", "level": "high" if "不适合" in str(tm.get("level", "")) else
                          ("mid" if "谨慎" in str(tm.get("level", "")) else "ok"),
                          "msg": f"市场择时 {tm.get('level')}（{tm.get('score')} 分）"})
        except Exception:
            pass
    # 1.5) 日历窗口（H16/H17 实证，2026-08-11 百轮#65 接入）
    try:
        from data.calendar_hook import get_window, upcoming
        _cw = get_window()
        if _cw:
            _lv = "mid" if _cw["bonus"] < 0 else "ok"
            items.append({"cat": "日历窗口", "level": _lv,
                          "msg": f"📅{_cw['label']}（{_cw['start']}~{_cw['end']}，全局 {'+' if _cw['bonus']>=0 else ''}{_cw['bonus']} 分，{_cw['evidence']}）"})
        else:
            for _up in upcoming(horizon_days=14):
                items.append({"cat": "日历窗口", "level": "ok",
                              "msg": f"⏳{_up['days_to']} 天后进入 {_up['label']}（{'+' if _up['bonus']>=0 else ''}{_up['bonus']} 分）"})
    except Exception:
        pass
    # 1.6) 大小盘分化度（H27，2026-08-11 百轮#65 接入；|divergence|>10pp 降档提示）
    try:
        _snap = None
        for _sd in ("data/factorpool/output",
                    "data/factorpool/output/daily_scores"):
            _fs = sorted([_P(p) for p in _g.glob(_sd + "/market_snapshot_ext*.json")],
                         key=lambda x: x.stat().st_mtime)
            if _fs:
                _snap = _fs[-1]
                break
        if _snap:
            sp = json.loads(_snap.read_text(encoding="utf-8"))
            dv = sp.get("divergence_60")
            if dv is not None:
                dvpp = abs(float(dv)) * 100
                m60 = sp.get("mkt_mom60")
                if dvpp >= 10:
                    items.append({"cat": "大小盘分化", "level": "mid",
                                  "msg": f"⚠分化 {dvpp:.0f}pp（等权 {m60*100:+.1f}%）——>10pp 建议降档观察"})
                elif dvpp >= 5:
                    items.append({"cat": "大小盘分化", "level": "ok",
                                  "msg": f"分化 {dvpp:.0f}pp（等权 {m60*100:+.1f}%）——结构性行情，注意风格"})
    except Exception:
        pass
    # 1.7) 全站健康扫描（★百轮#84：dev_auto 8.59 每 4h 产出，页面/API/一致性）
    try:
        _hs = _latest("health_scan_*.json", "report")
        if _hs:
            hs = json.loads(_hs.read_text(encoding="utf-8"))
            if not hs.get("all_ok", True):
                items.append({"cat": "系统健康", "level": "high",
                              "msg": f"健康扫描失败 {(hs.get('bad') or [''])[0][:40]}"})
            else:
                items.append({"cat": "系统健康", "level": "ok",
                              "msg": f"全站健康（{hs.get('pages',{}).get('ok','?')}/{hs.get('pages',{}).get('n','?')} 页 · {hs.get('apis',{}).get('ok','?')}/{hs.get('apis',{}).get('n','?')} API · 一致性{'✅' if hs.get('consistency') else '❌'}）"})
    except Exception:
        pass
    # 1.8) T+5 批次就绪（★百轮#84：#80 预研落地——08-14 首批）
    try:
        _v = live_validation()
        if _v.get("ok") and _v.get("t5_first_due"):
            _d = _v.get("t5_first_due_days")
            items.append({"cat": "验证进度", "level": "ok",
                          "msg": f"T+5 首批 {_v['t5_first_due']} 就绪" + (f"（还有 {_d} 天，{_v['batches'][0]['n']} 只）" if _d and _d > 0 and _v.get('batches') else "已到期可验证")})
    except Exception:
        pass
    # 1.9) 类型降权 + 审批指引（★百轮#94：长短线实盘偏弱类型 → 今日审批从严）
    try:
        _dg = (live_validation().get("diagnosis") or {})
        _dw = _dg.get("down_warn") or []
        if _dw:
            _labels = "、".join(x["label"] for x in _dw)
            items.append({"cat": "类型降权", "level": "mid",
                          "msg": f"{len(_dw)} 类降权提示：{_labels} 实盘偏弱 → 审批从严（卡片带 🔻 标记）"})
            # 审批指引：可关注类型 = 有实盘样本且非降权（value/quality_gap 等观察级）
            _ok_ty = [t["label"] for t in _dg.get("by_type", [])
                      if t["action"] not in ("降权提示", "降权候选") and t["n"] >= 3]
            if _ok_ty:
                items.append({"cat": "审批指引", "level": "ok",
                              "msg": f"实盘健康类型：{'、'.join(_ok_ty)}（正常路径审批）——revalue/短线情绪从严"})
            else:
                items.append({"cat": "审批指引", "level": "mid",
                              "msg": "当前所有有实盘样本的类型均处降权/观察——宁缺毋滥，仅质量折价等新样本类型可审"})
    except Exception:
        pass
    # ★2026-08-16「待审批」提示卡片整体删除（用户要求，与止损条同处理）
    # ★2026-08-16 止损提示条整体删除（含已触发/接近止损，用户要求）
    # 4) 止盈
    _tp = _latest("take_profit_signals_*.json", "logs")
    n_tp = 0
    if _tp:
        try:
            for p in json.loads(_tp.read_text(encoding="utf-8")).get("positions", []):
                if any(s.get("type") in ("target", "pullback", "time") for s in p.get("signals", [])):
                    n_tp += 1
        except Exception:
            pass
    if n_tp:
        items.append({"cat": "止盈", "level": "mid", "msg": f"{n_tp} 只触发止盈信号"})
    # 5) 组合风险
    _pr = _latest("position_risk_*.json", "report")
    if _pr:
        try:
            pr = json.loads(_pr.read_text(encoding="utf-8"))
            fl = pr.get("flags") or {}
            risk_msgs = []
            if fl.get("industry_high"):
                risk_msgs.append(f"单行业超限 {pr.get('top_industry','')}")
            if fl.get("high_corr"):
                risk_msgs.append("组合高相关")
            if fl.get("deep_drawdown"):
                risk_msgs.append(f"{fl.get('deep_drawdown')} 只深回撤")
            if risk_msgs:
                items.append({"cat": "组合风险", "level": "mid", "msg": "、".join(risk_msgs)})
        except Exception:
            pass
    items.sort(key=lambda x: {"high": 0, "mid": 1, "ok": 2}.get(x.get("level"), 2))
    return {"ok": True, "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "n": len(items), "n_high": sum(1 for i in items if i["level"] == "high"),
            "items": items}


def live_calendar() -> dict:
    """★2026-08-11 百轮#53：数据就绪日历——远期池各 horizon（T+1/T+5/T+20/T+60）当前样本数 + 就绪进度
    用户等待数据时的确定性：今天哪些入池股到了哪个 horizon？下一个就绪日是哪天、届时新增多少？
    ★2026-08-12 #150 性能：数据源 mtime 版本缓存（原每次 5.7s 全量重算 → 命中 5ms）"""
    import glob as _g
    import os as _os
    _now = time.time()
    _ver = _bars_version()
    if _cal_cache["data"] is not None and _cal_cache.get("ver") == _ver \
            and _now - _cal_cache["ts"] < 300:
        return _cal_cache["data"]
    import sqlite3
    from pathlib import Path as _P
    # 交易日历（★#102 双库合并——主库写保护后新交易日只在增量库，单读主库会漏最新日）
    try:
        import glob as _g2
        from pathlib import Path as _P2
        _dayset = set()
        _db_paths = ["data/cache/bars.db"] + [
            str(p) for p in sorted(_P2("data/cache").glob("bars_incr_*.db"))[-3:]]
        for _p in _db_paths:
            try:
                con = sqlite3.connect(_P2(_p).as_uri() + "?mode=ro&immutable=1",
                                      uri=True, timeout=3)
                _dayset.update(r[0] for r in con.execute(
                    "SELECT DISTINCT date FROM daily_bar").fetchall())
                con.close()
            except Exception:
                continue
        days = sorted(_dayset)
    except Exception:
        days = []
    # 远期池
    fs = sorted([_P(p) for p in _g.glob(str(BASE / "logs" / "pitch_track_pool_*.json"))],
                key=lambda x: x.stat().st_mtime)
    entries = []
    if fs:
        try:
            entries = json.loads(Path(fs[-1]).read_text(encoding="utf-8")).get("entries", [])
        except Exception:
            entries = []
    horizons = {"t1": 1, "t5": 5, "t20": 20, "t60": 60}

    def _fwd_days(from_date, n):
        """从 from_date 起第 n 个未来交易日（工作日预估：跳周末；bars 未覆盖的未来用日历推）"""
        import datetime as _dt
        try:
            d = _dt.date.fromisoformat(from_date)
        except Exception:
            return None
        cnt = 0
        while cnt < n:
            d += _dt.timedelta(days=1)
            if d.weekday() < 5:
                cnt += 1
        return d.isoformat()

    out = {}
    for h, n in horizons.items():
        have = 0
        rets = []
        due_dates = set()
        for e in entries:
            ed = e.get("entry_date")
            fw = e.get("fwd", {}) or {}
            if fw.get(h) and fw[h].get("ret") is not None:
                have += 1
                rets.append(fw[h]["ret"])   # ★#87 收集已就绪样本收益
                continue
            if ed and ed in days:
                i = days.index(ed)
                j = i + n
                if j < len(days):
                    due_dates.add(days[j])
                else:
                    est = _fwd_days(days[-1], n - (len(days) - 1 - i))
                    if est:
                        due_dates.add(est)   # ★未来日期预估（bars 未覆盖）
        # 下一个就绪日（最早的未来到期日）
        next_due = None
        if due_dates:
            today = days[-1]
            future = sorted(d for d in due_dates if d > today)
            next_due = future[0] if future else sorted(due_dates)[0]
        out[h] = {
            "label": f"T+{n}",
            "have": have,
            "total": len([e for e in entries if e.get("entry_date") in days]),
            "pending": len(due_dates),
            "next_due": next_due,
            # ★#87 已就绪样本平均收益（红涨绿跌口径，UI 用：正=红/负=绿）
            "avg_ret": round(sum(rets) / len(rets), 4) if rets else None,
            "win": round(sum(1 for r in rets if r > 0) / len(rets), 3) if rets else None,
            "n_done": len(rets),
        }
    _out = {"ok": True, "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "last_trade_day": days[-1] if days else None,
            "n_entries": len(entries), "horizons": out}
    _cal_cache.update({"ts": _now, "data": _out, "ver": _ver})
    return _out


def live_validation() -> dict:
    """★2026-08-11 百轮#63：策略验证对照——远期池实际 T+1 vs 17 年基准（1 月 horizon）
    "历史说有效，实盘验证中"——各类型实际表现相对基准的超/低预期标记
    ★2026-08-12 百轮#80：双 horizon（T+1/T+5）+ T+5 批次就绪预告（首批 08-14 到期）
    ★2026-08-13 #308 加 120s 缓存（键 = pitch_track_pool mtime，消除 live_brief 内 2 次重复计算）"""
    import glob as _g
    import os as _os
    import datetime as _dt
    from pathlib import Path as _P
    from collections import defaultdict
    def _latest(pat, sub):
        fs = sorted([_P(p) for p in _g.glob(str(BASE / sub / pat))], key=lambda x: x.stat().st_mtime)
        return fs[-1] if fs else None

    entries = []
    _pool = _latest("pitch_track_pool_*.json", "logs")
    if _pool:
        try:
            entries = json.loads(_pool.read_text(encoding="utf-8")).get("entries", [])
        except Exception:
            entries = []
    # ★#308 缓存：pitch_track_pool 未变（mtime 相同）且 120s 内 → 直接返回
    _pool_ver = _pool.stat().st_mtime if _pool else 0.0
    if _validation_cache["data"] is not None and _validation_cache.get("ver") == _pool_ver \
            and time.time() - _validation_cache["ts"] < 120:
        cached = dict(_validation_cache["data"])
        cached["batches_review"] = _batch_review_payload(entries)
        return _validation_comparison_readiness(cached)
    # ★#80 实际（双 horizon）
    act = {h: defaultdict(list) for h in ("t1", "t5")}
    for e in entries:
        for h in ("t1", "t5"):
            f = e.get("fwd", {}).get(h)
            if f and f.get("ret") is not None:
                act[h][e.get("otype", "?")].append(f["ret"])
    # 基准（17 年 1 月）
    bench = {}
    _wr = _latest("opportunity_winrates_full_2011_2026.json", "logs")
    if _wr:
        try:
            for ot, r in json.loads(_wr.read_text(encoding="utf-8")).get("results", {}).items():
                v = r.get("1") or {}
                if v:
                    bench[ot] = {"winrate": v.get("winrate"), "avg_ret": v.get("avg_ret")}
        except Exception:
            pass
    def _horizon_rows(h, hname):
        rows = []
        for ot, vals in sorted(act[h].items()):
            n = len(vals)
            act_avg = sum(vals) / n
            act_win = sum(1 for v in vals if v > 0) / n
            b = bench.get(ot) or {}
            b_avg = b.get("avg_ret")
            diff = (act_avg - b_avg) if (b_avg is not None and n >= 3) else None
            rows.append({
                "otype": ot, "n": n,
                "act_avg": round(act_avg, 4), "act_win": round(act_win, 3),
                "bench_avg": round(b_avg, 4) if b_avg is not None else None,
                "bench_win": b.get("winrate"),
                "diff": round(diff, 4) if diff is not None else None,
            })
        rows.sort(key=lambda x: -(x["diff"] if x["diff"] is not None else -9))
        return rows
    # ★#88 实盘 T+1 归因（为什么偏弱——按类型 + 评分段）
    diag = None
    try:
        from collections import defaultdict as _dd
        by_ot = _dd(list); by_score = _dd(list); by_ot_t5 = _dd(list)   # ★#106 加 t5 样本统计
        for e in entries:
            fw = e.get("fwd") or {}
            t1 = fw.get("t1") or {}
            r = t1.get("ret")
            if r is None:
                continue
            by_ot[e.get("otype", "?")].append(r)
            sc = e.get("score") or 0
            band = "≥90" if sc >= 90 else ("80-90" if sc >= 80 else ("70-80" if sc >= 70 else "<70"))
            by_score[band].append(r)
            # ★#106 T+5 样本（就绪才计入——08-14 首批后自动积累）
            t5 = fw.get("t5") or {}
            if t5.get("ret") is not None:
                by_ot_t5[e.get("otype", "?")].append(t5["ret"])
        # ★#89 类型触发建议（数据驱动决策规则，宁缺毋滥）：
        #   n<3 → 数据不足（继续观察）；diff 无基准 → 中性
        #   diff>+0.01 → 维持；-0.01≤diff≤0.01 → 观察
        #   diff<-0.01 且 n≥3 → 降权候选；diff<-0.02 且 n≥5 → 降权提示（入预警）
        def _verdict(diff, n, bench_ok, win=None):
            if bench_ok and n >= 3:
                if diff > 0.01:
                    return "超预期"
                if diff < -0.01:
                    return "低于预期"
                return "持平"
            if n >= 3:
                return "无基准"
            return "数据不足"
        def _action(diff, n, bench_ok, win=None):
            # 有 17 年基准：diff 判定（#89 规则）
            if bench_ok and n >= 3:
                if diff > 0.01:
                    return "维持"
                if diff < -0.02 and n >= 5:
                    return "降权提示"
                if diff < -0.01:
                    return "降权候选"
                return "观察"
            # ★#93 无基准类型：实盘自身胜率判定（样本足够时——实盘就是参考）
            #   短线情绪等无 17 年日基准，但实盘样本 n≥10 已可统计 → 用实盘胜率
            if n >= 14 and win is not None and win < 0.28:
                return "降权提示"
            if n >= 10 and win is not None and win < 0.35:
                return "降权候选"
            return "观察"
        _TYPE_LABEL = {"value": "低估值", "revalue": "价值重估", "quality_gap": "质量折价",
                       "pv_consensus": "量价共识", "breakout": "突破", "reversal": "反转",
                       "event": "事件", "tech_sentiment": "短线情绪", "momentum": "动量"}
        # 当前触发状态（机会池内该类型数量——按 otype 计）
        _opp_cnt = defaultdict(int)
        try:
            _oppd = json.loads(_latest("opp_pool_*.json", "logs").read_text(encoding="utf-8"))
            for _o in _oppd.get("opportunities", []):
                _opp_cnt[_o.get("otype") or "?"] += 1
        except Exception:
            pass
        diag = {
            "by_type": [],
            "by_score": [{"band": b, "n": len(v), "avg": round(sum(v) / len(v), 4),
                          "win": round(sum(1 for x in v if x > 0) / len(v), 3)}
                         for b, v in sorted(by_score.items())],
        }
        for ot, v in sorted(by_ot.items()):
            n = len(v)
            avg = sum(v) / n
            win = sum(1 for x in v if x > 0) / n
            b = bench.get(ot) or {}
            b_avg = b.get("avg_ret")
            bench_ok = b_avg is not None
            diff = (avg - b_avg) if bench_ok else None
            # ★#106 T+5 自动复核：T+1 降权提示的类型，T+5 样本就绪后复审
            #   t5_n≥5 且 t5_avg>0 且 t5_win≥0.5 → 降权解除（"T+5 恢复"，08-14 首批后自动生效）
            _t5v = by_ot_t5.get(ot) or []
            _t5_n = len(_t5v)
            _t5_avg = (sum(_t5v) / _t5_n) if _t5_n else None
            _t5_win = (sum(1 for x in _t5v if x > 0) / _t5_n) if _t5_n else None
            _action0 = _action(diff, n, bench_ok, win)
            _verdict0 = _verdict(diff, n, bench_ok, win)
            _review_tag = ""
            if _action0 == "降权提示" and _t5_n >= 5:
                if _t5_avg > 0 and _t5_win >= 0.5:
                    _action0 = "观察"      # 降权解除
                    _verdict0 = "T+5 恢复"
                    _review_tag = "｜T+5 恢复（样本" + str(_t5_n) + "，降权解除）"
                else:
                    _review_tag = f"｜T+5 确认偏弱（{_t5_n} 样本 {_t5_avg*100:+.2f}%），维持降权"
            diag["by_type"].append({
                "otype": ot, "label": _TYPE_LABEL.get(ot, ot), "n": n,
                "avg": round(avg, 4), "win": round(win, 3),
                "bench_avg": round(b_avg, 4) if bench_ok else None,
                "bench_win": b.get("winrate"),
                "diff": round(diff, 4) if diff is not None else None,
                "verdict": _verdict0, "action": _action0,
                "t5_n": _t5_n,
                "t5_avg": round(_t5_avg, 4) if _t5_avg is not None else None,
                "t5_win": round(_t5_win, 3) if _t5_win is not None else None,
                "review_tag": _review_tag,
                "trigger_n": _opp_cnt.get(ot, 0),
            })
        diag["by_type"].sort(key=lambda x: (x["diff"] if x["diff"] is not None else -9))
        # 降权提示级类型 → 汇总（供预警/横幅消费；reason 区分"低于基准"vs"实盘胜率偏低"）
        _down_types = [t for t in diag["by_type"] if t["action"] == "降权提示"]
        if _down_types:
            diag["down_warn"] = [{
                "label": t["label"], "n": t["n"], "avg": t["avg"],
                "reason": "低于17年基准" if t["bench_avg"] is not None else "实盘胜率偏低",
                "text": f"{t['label']}({t['n']}只 {t['avg']*100:+.2f}%)",
            } for t in _down_types]
    except Exception:
        pass
    # ★#80 T+5 批次就绪预告（首批 08-14）
    batches = []
    try:
        by_date = defaultdict(list)
        for e in entries:
            by_date[e.get("entry_date") or ""].append(e)
        for ed, es in sorted(by_date.items()):
            n_t5_done = sum(1 for e in es if (e.get("fwd") or {}).get("t5"))
            # ★2026-08-12 百轮后#118：首批样本明细（让 08-14 复核"从抽象变具体"——
            #   用户看到哪几只即将进入复核、当前 T+1 表现如何）
            _samples = []
            if not batches:   # 只给最早批次附明细
                for e in es:
                    _f = e.get("fwd") or {}
                    _t1 = _f.get("t1") or {}
                    _lt = _f.get("latest") or {}
                    _samples.append({
                        "code": e.get("code"), "name": e.get("name"),
                        "otype": e.get("otype"), "t1": _t1.get("ret"),
                        "latest": _lt.get("ret"),
                    })
            batches.append({"entry_date": ed, "n": len(es), "n_t5_done": n_t5_done,
                            "samples": _samples})
    except Exception:
        pass
    t5_due = None
    try:
        # 从最早批次算 T+5 到期日（工作日推算——bars 未覆盖未来交易日，索引法会越界）
        _ed0 = min((b["entry_date"] for b in batches if b["entry_date"]), default=None)
        if _ed0:
            _d0 = _dt.date.fromisoformat(_ed0)
            _cnt, _cur = 0, _d0
            while _cnt < 5:
                _cur += _dt.timedelta(days=1)
                if _cur.weekday() < 5:
                    _cnt += 1
            t5_due = _cur.isoformat()
    except Exception:
        pass
    _res = {"ok": True, "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "note": "T+1/T+5 实际 vs 17 年 1 月基准（diff = 实际-基准，n≥3 判超/低预期；T+5 无 17 年日基准，对照 T+1 参考）",
            "rows": _horizon_rows("t1", "T+1"),
            "rows_t5": _horizon_rows("t5", "T+5"),
            "batches": batches,
            "t5_first_due": t5_due,
            "t5_first_due_days": max(0, (_dt.date.fromisoformat(t5_due) - _dt.date.today()).days) if t5_due else None,
            "diagnosis": diag,   # ★#88 实盘 T+1 归因（类型/评分段）
            # ★2026-08-12 百轮#102/#106：降权复核计划 + T+5 自动复核状态
            #   08-14 T+5 首批到期 → t5 样本自动积累 → 降权解除/维持自动判定（预埋已生效）
            "review": {
                "note": (f"降权复核：{t5_due} T+5 到期后样本加厚自动复审（解除条件：t5≥5 样本且均值为正胜率≥50%）"
                         if (diag or {}).get("down_warn") and t5_due else
                         "无降权类型（或 T+5 未排期）"),
                "t5_due": t5_due,
                "down_types": [x["label"] for x in ((diag or {}).get("down_warn") or [])],
                "recovered": [t["label"] for t in ((diag or {}).get("by_type") or [])
                              if t.get("verdict") == "T+5 恢复"],
                "confirmed": [t["label"] + f"({t.get('t5_avg') and round(t['t5_avg']*100,1)}%)"
                              for t in ((diag or {}).get("by_type") or [])
                              if t.get("review_tag", "").startswith("｜T+5 确认")],
            },
            # ★2026-08-13 #231：批次级复核 + 市场对照（08-14 首批到期的展示层——
            #   pitch_review.py 的批次表/三池/市场基准对照接入 API，pitchtrack 页直接渲染；
            #   复用 risk.pitch_review.review_batch/_pool_stats（异常不阻断，降级空对象））
            "batches_review": _batch_review_payload(entries),
            # ★2026-08-12 百轮后#115：裁决历史（快照仓聚合——降权/观察/维持演进 + 持续天数，
            #   数据底座 = #114 verdict_snapshot 每日落盘）
            "history": _verdict_history(),
            }
    _validation_comparison_readiness(_res)
    _validation_cache.update({"ver": _pool_ver, "ts": time.time(), "data": _res})
    return _res


# live_validation 缓存（键 = pitch_track_pool mtime，消除 live_brief 内 2 次重复计算）
_validation_cache = {"ver": None, "ts": 0.0, "data": None}


def _validation_comparison_readiness(result):
    """The pending market comparison must not be counted as a ready API check."""
    comparison = (result.get('batches_review') or {}).get('market_comparison') or {}
    state = comparison.get('state', 'unknown')
    result['readiness'] = {'scope': 'market_comparison', 'ready': comparison.get('ready') is True,
                           'state': 'initializing' if state == 'waiting_core_update' else state,
                           'reason': comparison.get('reason') or '市场对照尚未核验'}
    return result


def _batch_review_payload(entries: list) -> dict:
    """★2026-08-13 #231：批次复核 + 市场对照（pitch_review 逻辑轻量版，供 /api/live/validation 展示）
    按入池批次聚合：T+1 平均/胜率 + 全市场 T+1 中位对照（选股超额）+ T+5 进度 + 三池分布。
    08-14 首批到期后 t5 自动填充——页面无需改版即可呈现批次质量（alpha vs beta 解读）。"""
    out = {"batches": [], "pool_stats": {}, "note": ""}
    try:
        from collections import defaultdict, Counter
        from deck.market_benchmark import public_state as market_comparison
        groups = defaultdict(list)
        for e in entries:
            d = str(e.get("entry_date") or "")[:10]
            if d:
                groups[d].append(e)
        pairs = []
        for d0, group in groups.items():
            dates = [((e.get("fwd") or {}).get("t1") or {}).get("date") for e in group]
            d1 = next((day for day in dates if day and day > d0), None)
            if d1:
                pairs.append((d0, d1))
        comparison = market_comparison(pairs)
        out["market_comparison"] = comparison
        for d0 in sorted(groups):
            ents = groups[d0]
            t1s = [(e.get("fwd") or {}).get("t1") for e in ents if (e.get("fwd") or {}).get("t1")]
            t5s = [(e.get("fwd") or {}).get("t5") for e in ents if (e.get("fwd") or {}).get("t5")]
            r1 = [v["ret"] for v in t1s] if t1s else []
            r5 = [v["ret"] for v in t5s] if t5s else []
            _d1 = t1s[0].get("date") if t1s else None
            _comparison = comparison.get("values", {}).get(d0+"|"+str(_d1), {})
            _mkt = _comparison.get("value")
            out["batches"].append({
                "batch": d0, "n": len(ents),
                "t1_avg": round(sum(r1) / len(r1), 4) if r1 else None,
                "t1_win": round(sum(1 for r in r1 if r > 0) / len(r1), 4) if r1 else None,
                "t5_avg": round(sum(r5) / len(r5), 4) if r5 else None,
                "t5_win": round(sum(1 for r in r5 if r > 0) / len(r5), 4) if r5 else None,
                "n_t5_done": len(r5),
                "mkt_t1_median": _mkt,
                "market_state": comparison["state"] if _d1 else "pending_history",
                "market_matched_symbols": _comparison.get("matched_symbols"),
                "excess": round(round(sum(r1) / len(r1), 4) - _mkt, 4) if r1 and _mkt is not None else None,
                # ★2026-08-13 #264：类型构成（otype Counter）——批次双维度解读（类型归因 × 入池时点市场环境）
                "types": dict(Counter(str(e.get("otype") or "?") for e in ents)),
            })
        # 三池分布
        _pc = {"auto_pitch": "🅰", "machine_top01": "🅱", "human_select": "🅲"}
        pgrp = defaultdict(list)
        for e in entries:
            _pt = e.get("pool_type") or "unmarked"
            pgrp[_pt].append(e)
        for pt, pes in pgrp.items():
            t5s = [(e.get("fwd") or {}).get("t5") for e in pes if (e.get("fwd") or {}).get("t5")]
            r5 = [v["ret"] for v in t5s] if t5s else []
            out["pool_stats"][pt] = {
                "name": _pc.get(pt, "🅾" if pt == "unmarked" else pt),
                "n": len(pes), "n_t5_done": len(r5),
                "t5_avg": round(sum(r5) / len(r5), 4) if r5 else None,
                "t5_win": round(sum(1 for r in r5 if r > 0) / len(r5), 4) if r5 else None,
            }
        out["note"] = "选股超额 = 批次 T+1 平均 − 全市场中位；T+5 待真实交易日样本到期。" + comparison["reason"]
    except Exception as e:
        out["note"] = f"批次复核暂不可用（{str(e)[:60]}）"
    return out


def _verdict_history(days: int = 60) -> dict:
    """读裁决快照仓 → 类型演进时间线 + 降权持续天数（同日期多份去重取最新）"""
    import glob as _g
    import os as _os
    _snaps = []
    for _f in sorted(_g.glob(str(BASE / "logs" / "verdict_snapshot_*_*.json")))[-days * 6:]:
        try:
            _snaps.append(json.loads(Path(_f).read_text(encoding="utf-8")))
        except Exception:
            continue
    if not _snaps:
        return {"n": 0, "timeline": {}, "down_days": {}}
    _by_d = {}
    for _s in _snaps:
        _by_d[_s.get("date", "")] = _s
    _snaps = [_by_d[d] for d in sorted(_by_d)]
    _timeline = {}
    for _s in _snaps:
        _d = _s.get("date", "")
        for _t in _s.get("by_type") or []:
            _ot = _t.get("otype") or "?"
            _timeline.setdefault(_ot, []).append({"d": _d[5:], "a": _t.get("action"), "n": _t.get("n")})
    _down_days = {}
    for _ot, _seq in _timeline.items():
        _n = 0
        for _x in reversed(_seq):
            if _x["a"] in ("降权提示", "降权候选"):
                _n += 1
            else:
                break
        if _n:
            _down_days[_ot] = _n
    return {"n": len(_snaps), "first": _snaps[0].get("date", ""), "last": _snaps[-1].get("date", ""),
            "timeline": _timeline, "down_days": _down_days}


def live_review() -> dict:
    """★2026-08-11 百轮#37 审批复盘：历史审批 buy 记录 × 远期池实际收益交叉
    回答"审批过的买入实际表现如何"（t1/t5/t20 + 环境留痕），决策质量闭环"""
    import glob as _g
    import os as _os
    _dcur = None
    _dfs = sorted(_g.glob(str(BASE / "logs" / "deck_decisions_*.json")), key=_os.path.getmtime)
    if _dfs:
        try:
            _dcur = json.loads(Path(_dfs[-1]).read_text(encoding="utf-8"))
        except Exception:
            _dcur = None
    buys = []
    if isinstance(_dcur, list):
        seen = set()
        for x in _dcur:
            if x.get("action") != "buy":
                continue
            k = (x.get("code"), x.get("date"))
            if k in seen:
                continue
            seen.add(k)
            buys.append(x)
    # 远期池 fwd（最新）
    pool_map = {}
    _pfs = sorted(_g.glob(str(BASE / "logs" / "pitch_track_pool_*.json")), key=_os.path.getmtime)
    if _pfs:
        try:
            _pool = json.loads(Path(_pfs[-1]).read_text(encoding="utf-8"))
            for e in _pool.get("entries", []):
                pool_map[e.get("code")] = e
        except Exception:
            pass
    rows = []
    # ★2026-08-12 百轮#104：类型当前状态映射（该审批股类型现在是否降权——复盘时知道
    #   "这笔 revalue 买入的类型现处降权提示"——实盘裁决体系延伸到历史视图）
    #   注：远期池 entry 只有 otype（英文），by_type 以 otype 为键
    _ts_map = {}
    try:
        for _t in ((live_validation().get("diagnosis") or {}).get("by_type") or []):
            _ts_map[_t.get("otype")] = {"label": _t.get("label"), "action": _t.get("action")}
    except Exception:
        pass
    for b in buys:
        code = b.get("code", "")
        pe = pool_map.get(code, {})
        fw = pe.get("fwd", {}) or {}
        _ot = pe.get("otype") or ""
        _ts = _ts_map.get(_ot)
        row = {
            "code": code, "name": pe.get("name", code),
            "decide_date": b.get("date"), "env_level": b.get("env_level") or "—",
            "env_score": b.get("env_score"),
            "entry_date": pe.get("entry_date"),
            "otype": _ot,
            "otype_name": (_ts or {}).get("label", _ot),
            "type_status": _ts["action"] if _ts else None,
            "t1": (fw.get("t1") or {}).get("ret"),
            "t5": (fw.get("t5") or {}).get("ret"),
            "t20": (fw.get("t20") or {}).get("ret"),
            "latest": (fw.get("latest") or {}).get("ret"),
            "in_pool": bool(pe),
        }
        rows.append(row)
    # ★2026-08-11 百轮#60：审批质量分层统计——按环境档位（适合/谨慎/不适合/历史未记）× T+1 表现
    #   样本积累后回答"我在什么环境下审批的质量最高"（env_level 08-11 起留痕，历史审批归"未记"）
    env_groups = {}
    for r in rows:
        if r.get("t1") is None:
            continue
        env = r.get("env_level") or "历史未记"
        g = env_groups.setdefault(env, {"n": 0, "win": 0, "sum_ret": 0.0})
        g["n"] += 1
        if r["t1"] > 0:
            g["win"] += 1
        g["sum_ret"] += r["t1"]
    env_summary = []
    for env, g in sorted(env_groups.items()):
        env_summary.append({"env": env, "n": g["n"],
                            "winrate": round(g["win"] / g["n"], 3),
                            "avg_ret": round(g["sum_ret"] / g["n"], 4)})
    return {"ok": True, "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "n_approved": len(rows), "rows": rows,
            "env_summary": env_summary}


_minute_meta_cache = {"ts": 0, "k5": "", "i1": ""}


def _minute_node() -> dict:
    """Actual 5-minute history, independent of unavailable 1-minute auction inputs."""
    from deck.research_views import minute_snapshot
    data = minute_snapshot(1)
    return {"name":"5分钟历史行情", "ok":data.get("ready") is True,
            "required_for_core":False,
            "initialized":data.get("initialized",False), "state":data.get("state"),
            "date":data.get("data_date", ""), "file":Path(data.get("database_path") or "minute.db").name+"/minute_5m",
            "frequency":data.get("frequency"), "rows":data.get("rows",0),
            "note":data.get("reason"), "age_h":None}


def live_minute() -> dict:
    """Read-only sample of downloaded 5-minute history; no real-time or auction substitution."""
    from deck.research_views import minute_snapshot
    return minute_snapshot()


def _extract_content_date(name: str, data) -> str:
    """★#345 从各环节 JSON 提取「数据内容日期」（YYYY-MM-DD）——判断数据是否覆盖到最新交易日。
    各环节字段：date / pool_date（数据日）；updated_at / ts / generated_at（生成时间，取日期）。
    竞价信号顶层 keys 是 8 位日期（如 20260506）。"""
    if not isinstance(data, dict):
        return ""
    try:
        if name in ("远期池", "实盘裁决"):
            # A review may be generated the next morning from the prior close.
            # Its wall-clock timestamp is never the observed market date.
            from deck.system_live import _content_date
            return _content_date(data.get("data_date"))
        if name == "竞价信号":
            keys = [k for k in data.keys() if isinstance(k, str) and k.isdigit() and len(k) == 8]
            if keys:
                m = max(keys)
                return f"{m[:4]}-{m[4:6]}-{m[6:]}"
            return ""
        for k in ("date", "pool_date", "updated_at", "ts", "generated_at"):
            v = data.get(k)
            if isinstance(v, str) and len(v) >= 10:
                return v[:10]
    except Exception:
        pass
    return ""


def _forward_chain_readiness(path, data, pipeline, target):
    """Use the same schema, content hash and mature-sample proof as system health."""
    from deck.system_live import _product_schema, _pipeline_evidence, _has_forward_observation, _content_date
    result = {"ok": False, "initialized": False, "state": "unknown", "date": ""}
    if not isinstance(data, dict):
        return {**result, "state": "failed", "note": "前向产物不是有效业务对象"}
    valid, rows, reason = _product_schema("pitch_track", data)
    proof = _pipeline_evidence(pipeline, path)
    declared = _extract_content_date("远期池", data)
    date = declared or _content_date(proof.get("data_date"))
    result.update(date=date, rows=rows)
    if not valid:
        return {**result, "state": "failed", "note": reason}
    if not proof:
        return {**result, "note": "当前前向产物缺少匹配内容哈希的成功运行证据"}
    if not date or (declared and declared != _content_date(proof.get("data_date"))):
        return {**result, "note": "前向内容日期与运行证据不一致"}
    if not target or date != target:
        return {**result, "state": "stale" if target and date < target else "unknown",
                "note": f"前向内容至 {date}，已核验收盘日为 {target or '未知'}"}
    result["initialized"] = True
    if not _has_forward_observation(data):
        return {**result, "state": "pending_history", "note": "前向观察已初始化，尚无到期收益样本；不据此判定交易效果"}
    return {**result, "ok": True, "state": "ready", "note": "前向结构、同日内容、运行哈希及实际到期样本已核验"}


def live_chain(_date_status=None) -> dict:
    """★2026-08-11 百轮#16 决策链状态：数据管道 → 各环节最新数据日期/新鲜度（一条链看全系统）
    环节：观察池 → 择时 → 今日信号 → 机会池 → Pitch 长线/短线 → 持仓 → 止盈 → 风控 → 远期池
    ★2026-08-12 #150 性能：数据源 mtime 版本缓存（原每次 1.9s → 命中 5ms）
    ★#345 改造：每环节返回「数据内容日期 date」，对照最新交易日 latest_td 判定是否覆盖到最新
    （原只返回 age_h 文件新鲜度，用户点开看到"几小时前"无法判断数据到底新不新）"""
    import glob as _g
    import os as _os
    from pathlib import Path as _P
    _now = time.time()
    _date_status = _complete_daily_status() if _date_status is None else _date_status
    _optional_inputs = _health_config().get("chain_optional_inputs") or {}
    from deck.research_views import _minute_background_job
    _minute_job=_minute_background_job()
    _pipeline_state = BASE / "runtime/research/pipeline-state.json"
    _forward_file = _latest_file("pitch_track_pool_*.json")
    _forward_version = (str(_forward_file), _forward_file.stat().st_mtime_ns, _forward_file.stat().st_size) if _forward_file else None
    _ver = (_bars_version(), _pipeline_state.stat().st_mtime if _pipeline_state.is_file() else None,
            _forward_version,
            _date_status.get("state"), _date_status.get("date"), json.dumps(_optional_inputs,sort_keys=True,ensure_ascii=False),
            _minute_job.get("job_id"),_minute_job.get("status"),_minute_job.get("completed_symbols"),
            _minute_job.get("cache_verified"),_minute_job.get("current_date_ready"))
    if _chain_cache["data"] is not None and _chain_cache.get("ver") == _ver \
            and _now - _chain_cache["ts"] < 120:
        return _chain_cache["data"]
    _latest_td = str(_date_status.get("date") or "") if _date_status.get("ready") else ""
    CHAIN = [
        ("观察池", "output", "pool_layers_*.json"),
        ("新择时", "output", "timing_system_*.json"),
        ("今日信号", "output", "daily_signal_*.json"),
        ("竞价信号", "logs", "auction_signal_*.json"),
        ("机会池", "logs", "opp_pool_*.json"),
        ("Pitch 长线", "logs", "pitch_v2_*.json"),
        ("Pitch 短线", "logs", "tech_pitch_*.json"),
        ("持仓", "logs", "portfolio_*.json"),
        ("止盈引擎", "logs", "take_profit_signals_*.json"),
        ("风控", "logs", "stock_risk_map_*.json"),
        ("远期池", "logs", "pitch_track_pool_*.json"),
        ("实盘裁决", "logs", "pitch_track_pool_*.json"),   # ★2026-08-12 百轮#97：实盘裁决体系（样本+基准双源，pitch_track_pool 主）
    ]
    nodes = []
    for name, sub, pat in CHAIN:
        fs = sorted([_P(p) for p in _g.glob(str(BASE / sub / pat))],
                    key=lambda x: x.stat().st_mtime)
        # ★2026-08-11 百轮#57 → ★#341 分钟数据环节（修复：检查根目录分钟因子 parquet，非旧 incr_parquet）
        if name == "远期池":
            try:
                nodes.append(_minute_node())
            except Exception:
                nodes.append({"name": "分钟数据", "ok": False, "age_h": None, "file": None, "date": ""})
        if not fs:
            optional = _optional_inputs.get(name) or {}
            nodes.append({"name": name, "ok": False, "initialized": False, "age_h": None, "file": None, "date": "",
                          "state": optional.get("state") or "missing_data",
                          "note": optional.get("reason") or "尚无本环节实际产物"})
            continue
        f = fs[-1]
        try:
            mt = _os.path.getmtime(f)
        except Exception:
            mt = None
        age_h = round((datetime.now().timestamp() - mt) / 3600, 1) if mt else None
        # Content dates are required; file mtime alone never makes a node healthy.
        _cdate = ""
        _chain_data = None
        try:
            _chain_data = json.loads(f.read_text(encoding="utf-8"))
            _cdate = _extract_content_date(name, _chain_data)
        except Exception:
            pass
        if _cdate and _latest_td:
            _ok = _cdate == _latest_td
        else:
            _ok = False
        _nd = {"name": name, "ok": _ok, "age_h": age_h, "file": f.name, "date": _cdate,
               "state": "ready" if _ok else _date_status.get("state") if not _latest_td else "stale" if _cdate and _cdate<_latest_td else "unknown"}
        if not _latest_td:
            _nd["note"] = _date_status.get("reason") or "完整行情日期尚未核验"
        elif not _cdate:
            _nd["note"] = "产物缺少可核验的内容日期，文件时间不能替代"
        if name in ("远期池", "实盘裁决") and _latest_td:
            try:
                _pipeline = json.loads(_pipeline_state.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                _pipeline = {}
            _nd.update(_forward_chain_readiness(f, _chain_data, _pipeline, _latest_td))
        # ★#123 → #345 竞价信号：内容日期 < 最新交易日 → 明确滞后（1 分钟竞价数据未更新）
        #   ★#381 note 加"供应商"关键词——check_consistency 按"内容滞后/供应商"过滤判定 supplier-lag（非系统断链），
        #   原"1分钟竞价数据未更新"不含关键词被误判 hard failure → 每晚 health_scan 假 ❌
        if name == "竞价信号" and _cdate and _latest_td and _cdate < _latest_td:
            _nd["ok"] = False
            _nd["note"] = f"数据至 {_cdate}，滞后于最新交易日 {_latest_td}（供应商1分钟竞价数据未交付，需数据新版或 Tushare 分钟权限）"
        # ★2026-08-12 百轮#97：实盘裁决环节附降权状态（门户/Pitch 决策链直接看到裁决结论）
        if name == "实盘裁决" and _nd.get("ok"):
            try:
                _dw = (live_validation().get("diagnosis") or {}).get("down_warn") or []
                if _dw:
                    _nd["note"] = f"降权中：{'、'.join(x['label'] for x in _dw)}（{len(_dw)} 类审批从严）"
                    _nd["down_n"] = len(_dw)
                else:
                    _nd["note"] = "无降权类型"
                    _nd["down_n"] = 0
            except Exception:
                pass
        nodes.append(_nd)
    _out = {"ok": True, "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "latest_td": _latest_td, "chain": nodes, "daily_status": _date_status,
            "ready": bool(_date_status.get("ready") and nodes and all(node.get("ok") for node in nodes)),
            "state": _date_status.get("state") if not _date_status.get("ready") else "ready" if nodes and all(node.get("ok") for node in nodes) else "incomplete"}
    unresolved=[node for node in nodes if not node.get("ok")]
    if unresolved and _date_status.get("ready"):
        severe=[node for node in unresolved if node.get("required_for_core") is not False and node.get("state") not in ("optional_unconfigured","pending_history","rate_limited","permission_blocked")]
        if not severe:
            independent=next((node for node in unresolved if node.get("required_for_core") is False),None)
            _out["state"] = independent.get("state","unknown") if independent else "rate_limited" if any(node.get("state")=="rate_limited" for node in unresolved) else "permission_blocked" if any(node.get("state")=="permission_blocked" for node in unresolved) else "pending_history" if any(node.get("state")=="pending_history" for node in unresolved) else "optional_unconfigured"
    core_nodes=[node for node in nodes if node.get("required_for_core") is not False and node.get("state") not in ("optional_unconfigured","pending_history")]
    _out["core_ready"]=bool(_date_status.get("ready") and core_nodes and all(node.get("ok") for node in core_nodes))
    _out["reason"] = "；".join(node["name"]+"："+(node.get("note") or node.get("state") or "尚未核验") for node in unresolved) if unresolved else "各环节内容日期已读取；不表示自动交易放行"
    _chain_cache.update({"ts": _now, "data": _out, "ver": _ver})
    return _out


def live_chain_refresh() -> dict:
    """★2026-08-13 #339 决策链实时更新：清决策链 + 门户聚合缓存，强制重扫 13 环节
    （live_chain 只是检查各数据文件 mtime/新鲜度，秒级完成，无副作用）"""
    _chain_cache["data"] = None
    _chain_cache["ver"] = None
    _chain_cache["ts"] = 0.0
    _pd_cache["data"] = None
    _pd_cache["ts"] = 0.0
    _portal_snapshot.invalidate()
    return live_chain()


def live_factor_perf() -> dict:
    from deck.research_views import factor_performance
    return factor_performance()


# ══════════════════════════════════════════════════════════════════
# ★2026-08-13 #313：因子综合排名（Pitch v3 落地——回测 ICIR + 实测远期加权增强）
# 综合排名分 = 回测排名 ×(1-live_eff) + 实战排名 ×live_eff
#   live_eff = live_w × min(1, n_pitch/min_live_samples)  实测样本越多，实战权重越大
#   实战强（正收益）的因子排名被拉前 → 影响 pitch 优先级（实测优先于回测的 Pitch v3 精神）
# ══════════════════════════════════════════════════════════════════
def live_factor_ranking() -> dict:
    from deck.research_views import factor_ranking
    return factor_ranking()


# ★2026-08-13 #319：数据库面板（unified.db 关键表内容浏览——因子实战库 + 加权结果）
# 总指挥诉求：实战远期效果入数据库（factor_agg/factor_pitch）→ 面板直接看数据 + 加权结果
def live_db_view() -> dict:
    import sqlite3 as _sq
    from deck.research_views import locations, factor_history
    out = {"ok": True, "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "tables": {}, "ranking": []}
    try:
        _, cache = locations()
        con = _sq.connect((cache/'unified.db').resolve().as_uri()+'?mode=ro', uri=True, timeout=3)
        for tbl, name in (("factor_agg", "因子实战聚合"), ("factor_pitch", "因子×Pitch 明细"),
                          ("evidence_weights", "证据权重配置"), ("market_daily", "市场日线快照")):
            try:
                cols = [c[1] for c in con.execute(f"PRAGMA table_info({tbl})").fetchall()]
                rows = [list(r) for r in con.execute(f"SELECT * FROM {tbl}").fetchall()]
                out["tables"][tbl] = {"name": name, "columns": cols, "rows": rows}
            except Exception:
                out["tables"][tbl] = {"name": name, "columns": [], "rows": []}
        con.close()
    except Exception as e:
        out["ok"] = False
        out["err"] = str(e)[:120]
        return out
    # 加权结果（复用 live_factor_ranking 的动态加权——回测排名依赖 factor_health 每日更新）
    try:
        ranking = live_factor_ranking()
        out["ranking"] = ranking.get("ranking", [])
        out["config"] = ranking.get("config", {})
    except Exception:
        out["ranking"] = []
    out['factor_history'] = factor_history()
    # ★#332 持仓数据同步到数据页（复盘：当前持仓 + 止盈 + 历史决策）
    try:
        h = live_holdings()
        pf = h.get("portfolio") or {}
        out["holdings"] = {
            "positions": pf.get("positions") or [],
            "take_profit": (h.get("take_profit") or {}).get("positions") or [],
            "pnl": h.get("pnl") or {},
            "position_risk": h.get("position_risk") or {},
        }
    except Exception:
        out["holdings"] = {}
    try:
        # ★#361 读时间戳 glob（固定名 deck_decisions.json 是旧残留 1 条，实际最新是时间戳版 5 条）
        _dcd = _read("deck_decisions_*.json", "logs")
        out["decisions"] = _dcd if isinstance(_dcd, list) else (_dcd.get("entries") or [])
    except Exception:
        out["decisions"] = []
    return out


# ★2026-08-13 #323：因子池 UI 数据包接入（外包 output/ui_data/ 5 个 JSON）
#   用途分层/生命周期/相关性/FRC 规则/数据时效——总指挥"因子池做好了就等你接入"
_UI_DATA_DIR = Path("data/factorpool/output/ui_data")


def _clean_nan(o):
    """★2026-08-13 #326：递归清洗 NaN/Infinity → None。
    factor_corr 矩阵含 676 个 NaN，Python json.dumps 默认输出非法字面量 `NaN`，
    前端浏览器 JSON.parse 会抛 SyntaxError → 整个 factor_ui_pack（含生命周期）加载失败。
    必须清洗成 null，前端才能解析。"""
    import math
    if isinstance(o, float):
        return None if (math.isnan(o) or math.isinf(o)) else o
    if isinstance(o, dict):
        return {k: _clean_nan(v) for k, v in o.items()}
    if isinstance(o, list):
        return [_clean_nan(v) for v in o]
    return o


def live_factor_ui_pack() -> dict:
    from deck.research_views import factor_ui_pack
    return factor_ui_pack()


# ★2026-08-13 #329 A股择时轮动日历（总指挥"还欠我"）——F3 风格状态机数据源
_STYLE_STATE_CSV = Path("data/factorpool/output/f3_style_state.csv")


def live_rotation_calendar() -> dict:
    """★2026-08-13 #329 A股择时轮动日历：F3 月度风格状态机（dominant 历史）+
    factor_style_rotation（当前风格 + boost/trim 因子族）+ 日历效应窗口（H16/H17/CAL）。
    一次 fetch 全渲染：当前风格徽章 + 近 24 月风格轮动时间轴 + 未来日历节点。"""
    import csv as _csv
    from collections import Counter as _Counter
    out = {"ok": True, "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
           "current_style": None, "style_history": [], "style_dist": {},
           "weights": {}, "cal_win": None, "cal_month": {}, "cal_upcoming": []}
    # The public validator checks the current source date/configuration. A stale
    # F3 CSV or an older glob-matched JSON must never resurrect a style state.
    from deck.research_views import style_diagnostic_state
    sr=style_diagnostic_state()
    if sr.get("ready") is True and sr.get("fresh") is True:
        out["current_style"] = sr["current_style"]
        out["style_history"] = sr.get("style_history") or []
        out["style_dist"] = dict(_Counter(row.get("dominant") for row in out["style_history"] if row.get("dominant")))
        out.update({key:sr[key] for key in ("kind","calibrated","weights_enabled","auto_trade_allowed","source","method","limitations","strength","data_date") if key in sr})
        out["weights"] = {}  # This observed diagnostic does not supply calibrated weights.
    # 3) 日历效应窗口（复用 timing_dash）
    try:
        td = live_timing_dash()
        out["cal_win"] = td.get("cal_win")
        out["cal_month"] = td.get("cal_month") or {}
        out["cal_upcoming"] = td.get("cal_upcoming") or []
    except Exception:
        pass
    if not out.get("current_style"):
        configured=(BASE/"config/style_diagnostic.yaml").is_file()
        out.update({"state":"missing_data" if configured else "optional_unconfigured","initialized":False,"ready":False,
                    "reason":sr.get("reason") or "市值风格诊断尚未通过当前日期和配置验收；日历信息不代表加减权策略"})
    else:
        limited=out.get("calibrated") is False or out.get("weights_enabled") is False
        out.update({"state":"limited_research" if limited else "ready","initialized":True,"ready":True,
                    "reason":"市值风格诊断未校准，不产生加减权或自动交易" if limited else "已读取实际风格研究产物"})
    return out


def live_strong_hits() -> dict:
    """★2026-08-11 百轮#68：强因子直通展示（用户"强因子直通 Deck"完整形态）
    跨家族≥2 的原始直通榜（factor_risk 家族代表 × daily CSV rank≤0.10）：
    分级（极强≥6 家族 / 强 4-5 / 一般 2-3）+ Top20（家族数降序 + min_rank 升序）
    + 机会池内标记。5min 缓存（load_strong_hits 重算约 1-2s）。"""
    import time as _t
    _c = getattr(live_strong_hits, "_cache", None)
    if _c and _t.time() - _c[0] < 300:
        return _c[1]
    try:
        if str(BASE) not in sys.path:
            sys.path.insert(0, str(BASE))
        from factors.opportunities.scan import load_strong_hits
        hits = load_strong_hits()
        import sqlite3 as _sq
        from data.cache import CACHE_DIR
        basic_db = CACHE_DIR / "stock_basic.db"
        if not basic_db.exists():
            return {"ok": False, "error": "股票资料库尚未初始化（stock_basic.db）"}
        con = _sq.connect(basic_db.resolve().as_uri() + "?mode=ro&immutable=1", uri=True)
        meta = {r[0]: (r[1], r[2] or "") for r in con.execute(
            "SELECT code, name, industry FROM stock_basic").fetchall()}
        con.close()
        rows = []
        for c, fs in hits.items():
            fams = sorted({v["family"] for v in fs.values()})
            rows.append({
                "code": c,
                "name": (meta.get(c) or ("", ""))[0],
                "industry": (meta.get(c) or ("", ""))[1],
                "n_family": len(fams),
                "families": fams,
                "factors": list(fs.keys()),
                "min_rank": round(min(v["rank"] for v in fs.values()), 3),
            })
        rows.sort(key=lambda x: (-x["n_family"], x["min_rank"]))
        try:
            d = _read("opp_pool_*.json")
            in_pool = {o["code"] for o in d.get("opportunities", [])} if d else set()
        except Exception:
            in_pool = set()
        for r in rows:
            r["in_pool"] = r["code"] in in_pool
        out = {
            "ok": True,
            "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "n": len(rows),
            "n_extreme": sum(1 for r in rows if r["n_family"] >= 6),
            "n_strong": sum(1 for r in rows if 4 <= r["n_family"] <= 5),
            "n_common": sum(1 for r in rows if r["n_family"] <= 3),
            "n_in_pool": sum(1 for r in rows if r["in_pool"]),
            "top": rows[:20],
            # ★百轮#86：全量 code→n_family 映射（卡片 💪 徽章用——top 20 覆盖不到全部 Pitch 候选）
            "all": {r["code"]: r["n_family"] for r in rows},
        }
        live_strong_hits._cache = (_t.time(), out)
        return out
    except Exception as e:
        return {"ok": False, "error": str(e)[:120]}


def live_auction() -> dict:
    """★2026-08-14 #424 竞价因子展示（用户"竞价因子界面没有"）
    当日竞价强度排名 + 过热回避标记（strength≥6 高开放量 → 机会池减分防追高）
    + 机会池内过热股（auction_heat）。5min 缓存。"""
    import time as _t
    _c = getattr(live_auction, "_cache", None)
    if _c and _t.time() - _c[0] < 300:
        return _c[1]
    try:
        if str(BASE) not in sys.path:
            sys.path.insert(0, str(BASE))
        from factors.opportunities.scan import load_auction_signals
        sig = load_auction_signals()
        if not sig:
            reason = "集合竞价研究未启用：需要可核验的真实集合竞价成交及足够历史基准，普通1分钟和当前5分钟行情均不能替代；不属于连续竞价模拟的必要条件"
            evidence=_read("auction-permission-verification.json","runtime/research")
            return {"ok":False,"state":"optional_unconfigured","initialized":False,"ready":False,
                    "error":reason,"reason":reason,"source":"unconfigured","top":[],"hot":[],
                    "permission_evidence":{key:evidence.get(key) for key in ("endpoint","checked_at","ok","phase")},
                    "documentation":"https://tushare.pro/document/2?doc_id=353"}
        d8 = sorted(sig.keys())[-1]
        day = sig[d8]
        import sqlite3 as _sq
        con = _sq.connect("file:data/cache/stock_basic.db?mode=ro&immutable=1", uri=True)
        meta = {r[0]: (r[1], r[2] or "") for r in con.execute(
            "SELECT code, name, industry FROM stock_basic").fetchall()}
        con.close()
        rows = []
        for c, s in day.items():
            if not isinstance(s, dict):
                continue
            rows.append({
                "code": c,
                "name": (meta.get(c) or ("", ""))[0],
                "industry": (meta.get(c) or ("", ""))[1],
                "gap": round(float(s.get("gap") or 0), 4),
                "v30_ratio": round(float(s.get("v30_ratio") or 0), 3),
                "first5_ratio": round(float(s.get("first5_ratio") or 0), 3),
                "strength": round(float(s.get("strength") or 0), 1),
            })
        rows.sort(key=lambda x: -x["strength"])
        try:
            _d = _read("opp_pool_*.json")
            in_pool = {o["code"] for o in _d.get("opportunities", [])} if _d else set()
        except Exception:
            in_pool = set()
        for r in rows:
            r["in_pool"] = r["code"] in in_pool
            r["hot"] = r["strength"] >= 6
        hot = [r for r in rows if r["hot"]]
        out = {
            "ok": True,
            "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "date": d8,
            "n": len(rows),
            "n_hot": len(hot),
            "hot_in_pool": [r for r in hot if r["in_pool"]],
            "top": rows[:30],
            "hot": hot[:30],
        }
        live_auction._cache = (_t.time(), out)
        return out
    except Exception as e:
        return {"ok": False, "error": str(e)[:120]}


# ══════════════════════════════════════════════════════════════════
# ★2026-08-12 #155 择时面板深度打磨（timing_dash）
# 聚合：/api/timing 全字段 + regime 近12月序列（sparkline）+ calendar_hook
# 当前窗口/未来预告 + CAL 月收益 bonus（#154 补落地）+ 拥挤度/温度 + 新鲜度
# ══════════════════════════════════════════════════════════════════
_td_cache = {"ts": 0, "data": None, "ver": None}


def live_timing_dash() -> dict:
    """择时面板单 API 聚合（60s 缓存 + 数据源版本失效）：
    score/dims/regime_fit/style_state + spark 序列（regime 近12月 pos）+
    cal_win 当前窗口 + cal_upcoming 未来预告 + cal_month CAL 月收益 +
    温度/拥挤度 + fresh 新鲜度。页面一次 fetch 全渲染。"""
    _now = time.time()
    _ver = _bars_version()
    if _td_cache["data"] is not None and _td_cache["ver"] == _ver \
            and _now - _td_cache["ts"] < 60:
        return _td_cache["data"]
    out = {"ok": True, "schema_version": "1.0", "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    try:
        # 1) 主数据：最新 timing_system_*.json（mtime 取最新）
        _fs = sorted(glob.glob(str(BASE / "output" / "timing_system_*.json")),
                     key=os.path.getmtime)
        if _fs:
            out["timing"] = json.loads(Path(_fs[-1]).read_text(encoding="utf-8"))
        _breadth=((out.get("timing") or {}).get("dims") or {}).get("宽度") or {}
        _total=_breadth.get("total");_up=_breadth.get("up")
        out["breadth"]={"score":_breadth.get("score"),"source":_breadth.get("source"),"asof":_breadth.get("asof"),
                        "up_fraction":_up/_total if isinstance(_up,(int,float)) and isinstance(_total,(int,float)) and _total>0 else None,
                        "note":_breadth.get("note"),"score_unit":"0_to_100"}
        _sentiment=((out.get("timing") or {}).get("dims") or {}).get("情绪") or {}
        out["sentiment"]={"score":_sentiment.get("score"),"source":_sentiment.get("source") or "timing_system.sentiment_score",
                          "asof":(out.get("timing") or {}).get("date"),"note":_sentiment.get("note"),"unit":"0_to_100"}
        out["temperature"]={"value":None,"supported":False,"state":"legacy_unsupported",
                            "reason":"旧外包独立温度模型未提供可执行算法，不属于当前研究初始化项目；当前情绪评分为独立命名的真实指标"}
        # 2) sparkline 序列（★#155 深度打磨：真实历史，按时间序取全部历史文件）
        #    ① score_hist：★#172 优先读 timing_history 跨日序列（择时历史归档器），
        #       fallback timing_system_*.json 当日多次评估走势
        #    ② temp_hist：外包情绪温度计 temp（低=恐慌买入区）
        #    ③ width_hist：快照 width5（行业宽度 5 日动量占比）
        #    ④ crowd_hist：crowding pctile_252（冷清=机会区）
        #    ⑤ temp_components：温度计五指标构成（迷你条）
        try:
            _ths = []
            _thf = sorted(glob.glob(str(BASE / "logs" / "timing_history_*.json")),
                          key=os.path.getmtime)
            if _thf:
                try:
                    _thd = json.loads(Path(_thf[-1]).read_text(encoding="utf-8"))
                    _ths = _thd.get("score_series") or []
                except Exception:
                    _ths = []
            if not _ths:
                for _f in sorted(glob.glob(str(BASE / "output" / "timing_system_*.json")),
                                 key=os.path.getmtime):
                    try:
                        _tj = json.loads(Path(_f).read_text(encoding="utf-8"))
                        _ths.append(round(_tj.get("score", 0), 1))
                    except Exception:
                        continue
            if _ths:
                out["score_hist"] = _ths
        except Exception:
            pass
        try:
            _sd = "data/factorpool/output"
            _whs = []
            for _f in sorted(glob.glob(_sd + "/market_snapshot_ext_*.json"),
                             key=os.path.getmtime):
                try:
                    _wj = json.loads(Path(_f).read_text(encoding="utf-8"))
                    if _wj.get("width5") is not None:
                        _whs.append(round(float(_wj["width5"]), 3))
                except Exception:
                    continue
            if _whs:
                out["width_hist"] = _whs
        except Exception:
            pass
        try:
            _sd = "data/factorpool/output"
            _chs = []
            for _f in sorted(glob.glob(_sd + "/crowding_*.json"),
                             key=os.path.getmtime):
                try:
                    _cj = json.loads(Path(_f).read_text(encoding="utf-8"))
                    if _cj.get("crowding_pctile_252") is not None:
                        _chs.append(round(float(_cj["crowding_pctile_252"])/100, 6))
                except Exception:
                    continue
            if _chs:
                out["crowd_hist"] = _chs
                out["crowd_hist_unit"] = "fraction"
        except Exception:
            pass
        # 3) 日历窗口 + 未来预告（H16/H17）
        try:
            from data.calendar_hook import get_window, upcoming
            out["cal_win"] = get_window()
            out["cal_upcoming"] = upcoming(horizon_days=90)
        except Exception:
            pass
        # 4) CAL 月收益（#154：2月+5/11月+5/10月+3/1月4月12月-5）
        try:
            import datetime as _dt
            _mon = _dt.datetime.now().month
            _mb = 0
            if _mon == 2:
                _mb = 5
            elif _mon == 11:
                _mb = 5
            elif _mon == 10:
                _mb = 3
            elif _mon in (1, 4, 12):
                _mb = -5
            out["cal_month"] = {
                "month": _mon,
                "bonus": _mb,
                "label": {2: "2月春季躁动", 11: "11月Q4核心", 10: "10月Q4起始",
                          1: "1月弱月", 4: "4月弱月", 12: "12月弱月"}.get(_mon, ""),
                "note": "CAL 月收益日历（2015-2026 实证：2月+4.89%/75% 春季躁动、1月-3.11%/25% 全年第二弱、4月-0.67%/33%、11月+3.9%/73%、10月+2.5%；CAL-4 12月小切大已推翻）" if _mb else "CAL 月收益日历：当前月无显著日历效应（中性）",
            }
        except Exception:
            pass
        # 5) 拥挤度/温度（外包 crowding 最新 + 快照温度）
        try:
            _cf = sorted(glob.glob("data/factorpool/output/crowding_*.json"),
                         key=os.path.getmtime)
            if _cf:
                _cd = json.loads(Path(_cf[-1]).read_text(encoding="utf-8"))
                out["crowding"] = {
                    "mkt": _cd.get("crowding_mkt"), "pctile": float(_cd["crowding_pctile_252"])/100 if _cd.get("crowding_pctile_252") is not None else None,
                    "pctile_percent":_cd.get("crowding_pctile_252"),"pctile_unit":"fraction",
                    "zone": _cd.get("zone"), "n_crowded": _cd.get("n_crowded_stocks"),
                    "date": _cd.get("date"),
                }
        except Exception:
            pass
        # 6) ★2026-08-13 跨日评分走势（timing_history 每日归档——"择时动起来"数据源）
        try:
            _th = sorted(glob.glob(str(BASE / "logs" / "timing_history_*.json")),
                         key=os.path.getmtime)
            if _th:
                _thd = json.loads(Path(_th[-1]).read_text(encoding="utf-8"))
                out["score_series"] = _thd.get("score_series", [])
                out["score_days"] = _thd.get("days", [])
                out["level_series"] = _thd.get("level_series", [])
        except Exception:
            pass
        _td_cache.update({"ts": _now, "data": out, "ver": _ver})
        return out
    except Exception as e:
        return {"ok": False, "error": str(e)[:120]}


# ══════════════════════════════════════════════════════════════════
# ★2026-08-12 #158 S8 因子池页聚合（factor_dash）
# 全景区：manifest 全因子（category/direction/icir/status/usage）
# 健康：health CSV（icir20/60/120 + ic60_short 衰减预警）
# 反向信号：daily CSV 6 flag 命中数 + crowding + FRC risk_multiplier
# 衰减预警：health ⚠️ 因子列表 + 新因子培育（manifest 最近 2 周）
# ══════════════════════════════════════════════════════════════════
_fd_cache = {"ts": 0, "data": None}


def live_factor_dash() -> dict:
    """Actual supported factors, with typed missing metrics and explicit research provenance."""
    from deck.research_views import factor_dashboard
    return factor_dashboard()


# ══════════════════════════════════════════════════════════════════
# ★2026-08-12 #163 阶段 3：门户 1 API 聚合（portal_dash）
# 门户 JS 原 3 次 fetch（chain/timing/pools）→ 1 次，全部走缓存（4ms 级）
# ══════════════════════════════════════════════════════════════════
_pd_cache = {"ts": 0, "data": None}
from deck.background_snapshot import BackgroundSnapshot
_portal_snapshot = BackgroundSnapshot("portal-summary")


def _portal_version():
    from deck.daily_status import file_versions
    from deck.research_views import _minute_background_job
    job=_minute_background_job()
    paths = [BASE / "runtime/research/pipeline-state.json", BASE / "runtime/research/bootstrap-manifest.json"]
    # Include every existing file read by the aggregate, not only its newest timestamp.
    for folder in ("logs", "output", "report"):
        paths.extend(sorted((BASE / folder).glob("*.json")))
    minute_version=(job.get("job_id"),job.get("status"),job.get("database_path"),job.get("current_date_ready"),
                    job.get("cache_verified") if job.get("status")=="complete" else None)
    return (_bars_version(), file_versions(paths),minute_version)


def _portal_public_status():
    """Fast public progress is refreshed even while an old aggregate is being rebuilt."""
    from deck.system_live import _api_health, _research_status, _autotrade_status, _next_schedule
    products = _api_health()
    auto = _autotrade_status()
    return {"api_health": products, "research_data": _research_status(products),
            "autotrade": auto, "next_schedule": _next_schedule(auto)}


def live_portal_dash() -> dict:
    """Never make an HTTP request wait for full-database completeness queries."""
    result = _portal_snapshot.read(_portal_version(), _collect_portal_dash, max_age=300)
    out = {**(result["data"] or {}), "ok": True, "schema_version": "1.1"}
    try:
        out.update(_portal_public_status())
    except Exception:
        out["research_data"] = {"ready": False, "message": "研究进度暂不可读，请检查本机服务"}
    research = out.get("research_data") or {}
    daily = out.get("daily_status") or {}
    out.update(loading=result["loading"], stale=result["stale"], snapshot_at=result["updated_at"],
               ready=bool(result["current"] and out.get("aggregate_complete") and research.get("ready") and daily.get("ready")),
               state=("failed" if out.get("section_errors") else daily.get("state","unknown") if not daily.get("ready") else research.get("status", "incomplete")) if result["current"] else result["state"])
    if not result["current"]:
        out["reason"] = ("完整门户汇总正在后台核验；当前保留上次真实快照" if result["stale"] else "完整门户汇总正在后台核验；初始化进度已实时显示") if result["loading"] else "门户汇总失败，请检查本机研究服务"
        if result["error"]:
            out["error"] = "门户后台核验失败；未将旧结果视为最新状态"
    else:
        out["reason"] = "部分门户模块读取失败，详情保留在 section_errors" if out.get("section_errors") else research.get("message", "已读取当前真实研究结果")
    return out


def _collect_portal_dash() -> dict:
    """Full aggregate, including actual completeness evidence, runs off the request thread."""
    daily_status = _complete_daily_status(wait=True)
    out = {"ok": True, "schema_version": "1.0", "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    out["daily_status"] = daily_status
    out["section_errors"] = {}
    try:
        out["pools"] = live_pools()
    except Exception:
        out["section_errors"]["pools"] = "候选池汇总读取失败"
    try:
        out["timing"] = live_timing_dash()
    except Exception:
        out["section_errors"]["timing"] = "市场择时读取失败"
    try:
        out["chain"] = live_chain(_date_status=daily_status)
    except Exception:
        out["section_errors"]["chain"] = "决策链读取失败"
    try:
        out["brief"] = live_brief()
    except Exception:
        out["section_errors"]["brief"] = "研究简报读取失败"
    try:
        # ★2026-08-14 跨资产轮动防守信号（a_share_weak + global_rotation，因子池 P0 落地）
        _gr = BASE / "output" / "global_rotation.json"
        if _gr.exists():
            import json as _jgr
            out["global_rotation"] = _jgr.loads(_gr.read_text(encoding="utf-8"))
    except Exception:
        pass
    try:
        # ★2026-08-14 择时红绿灯（贪婪/观望/恐慌，系统性风险三档状态机）
        _tl = BASE / "output" / "traffic_light.json"
        if _tl.exists():
            import json as _jtl
            out["traffic_light"] = _jtl.loads(_tl.read_text(encoding="utf-8"))
    except Exception:
        pass
    try:
        # ★#347 数据日（顶层，供门户摘要条"数据日"显示——完整交易日门槛）
        _td = daily_status.get("date") if daily_status.get("ready") else None
        if _td:
            out["data_date"] = str(_td)
            out["data_semantic"] = f"最新核验完整日线 {_td} · 后续更新以研究服务实际进度为准"
        else:
            out["data_semantic"] = daily_status.get("reason") or "完整日线日期尚未核验"
    except Exception:
        pass
    try:
        # ★2026-08-14 数据即时性优化：下次计划运行（门户运行状态窗口数据源）
        from deck.system_live import _next_schedule, _autotrade_status
        out["next_schedule"] = _next_schedule()
        out["autotrade"] = _autotrade_status()
    except Exception:
        pass
    try:
        # ★#347 API/数据源健康（各关键数据文件新鲜度——门户"当前 API 状态"）
        from deck.system_live import _api_health
        out["api_health"] = _api_health()
    except Exception:
        pass
    try:
        # ★#289 KPI 历史序列（sparkline 数据源）：从历史文件聚合最近 12 个时点的
        #   机会池 n / Pitch长 n / Pitch短 n（按文件名 mtime 排序取最新 12 个）
        import glob as _gl, os as _os, json as _js
        out["kpi_series"] = {}
        _logs_dir = BASE / "logs"
        _series_map = {
            "opp": (sorted(_gl.glob(str(_logs_dir / "opp_pool_*.json")), key=os.path.getmtime)[-12:], lambda d: (d.get("n") if isinstance(d, dict) and isinstance(d.get("n"), int) else (len(d.get("items") or d.get("opps") or []) if isinstance(d, dict) else None))),
            "pitch": (sorted(_gl.glob(str(_logs_dir / "pitch_v2*.json")), key=os.path.getmtime)[-12:], lambda d: len(d.get("pitch") or []) if isinstance(d, dict) else None),
            "tech": (sorted(_gl.glob(str(_logs_dir / "tech_pitch*.json")), key=os.path.getmtime)[-12:], lambda d: len(d.get("pitch") or d.get("tech") or d.get("entries") or []) if isinstance(d, dict) else None),
        }
        for _k, (_fs, _fn) in _series_map.items():
            _ser = []
            for _f in _fs:
                try:
                    _d = _js.load(open(_f, encoding="utf-8"))
                    _v = _fn(_d)
                    if _v is not None:
                        _ser.append(_v)
                except Exception:
                    pass
            out["kpi_series"][_k] = _ser[-12:]
    except Exception:
        pass
    out["aggregate_complete"] = not out["section_errors"]
    return out


# ══════════════════════════════════════════════════════════════════
# ★2026-08-12 #165 阶段 3.5：枚举动态下发（enums）
# 前端不硬编码类型/分类中文名——registry + manifest + signal_family 动态构建，
# 新因子/新类型自动出现（可塑性设计，免改前端）
# ══════════════════════════════════════════════════════════════════
_enums_cache = {"ts": 0, "data": None}


def live_enums() -> dict:
    """枚举动态下发（60s 缓存）：
    otypes（registry 7 类 + tech_sentiment）+ categories（manifest 分类 + 中文名）+
    families（signal_family 族 + 中文名）。前端 fetch 后动态建映射，无匹配回退原文。"""
    _now = time.time()
    if _enums_cache["data"] is not None and _now - _enums_cache["ts"] < 60:
        return _enums_cache["data"]
    out = {"ok": True, "schema_version": "1.0",
           "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    try:
        from factors.opportunities.registry import OPPORTUNITY_TYPES
        otypes = {}
        for ot, spec in OPPORTUNITY_TYPES.items():
            otypes[ot] = spec.get("name") or ot
        otypes["tech_sentiment"] = "短线情绪"  # tech_pitch_v3 专类（registry 外）
        # ★#377 池类型中文名（四池归属，非机会类型——machine_top01 因子强/auto_pitch 自动/ai_select AI精选，
        #   否则持仓页/决策页渲染回退英文 key）
        otypes["machine_top01"] = "机器强因子"
        otypes["auto_pitch"] = "自动 Pitch"
        otypes["ai_select"] = "AI 精选"
        # ★2026-08-14 suggest 补位中文名（系统建议补位股，非机会类型，避免前端显示英文 key）
        otypes["suggest"] = "系统建议"
        out["otypes"] = otypes
    except Exception:
        pass
    try:
        from factors.signal_family import CATEGORY_TO_FAMILY, SIGNAL_FAMILY_CN, SIGNAL_FAMILY_COLOR
        out["families"] = SIGNAL_FAMILY_CN
        out["family_colors"] = SIGNAL_FAMILY_COLOR   # ★#348 族色动态下发（前端不写死颜色）
        cat_cn = {}
        for cat, fam in CATEGORY_TO_FAMILY.items():
            cat_cn[cat] = SIGNAL_FAMILY_CN.get(fam, fam)
        out["categories"] = cat_cn
    except Exception:
        pass
    try:
        # ★#348 择时四维（维度中文名 + 权重 + 颜色——前端不写死 DIM_COLORS）
        _dim_w = {"政策": 0.40, "宏观": 0.25, "情绪": 0.20, "宽度": 0.15}
        _dim_colors = {"政策": "#2563eb", "宏观": "#7c3aed", "情绪": "#0891b2", "宽度": "#ea580c"}
        out["timing_dims"] = {k: {"weight": w, "color": _dim_colors.get(k, "#64748b")}
                              for k, w in _dim_w.items()}
    except Exception:
        pass
    try:
        # ★#348 因子风格颜色（F3 风格状态机 dominant：低波/动量/反转/质量/价值——前端不写死 STYLE_COLORS）
        out["style_colors"] = {"低波": "#16a34a", "动量": "#dc2626", "反转": "#2563eb",
                               "质量": "#8b5cf6", "价值": "#f59e0b"}
    except Exception:
        pass
    try:
        # ★#351 短线四维打分（tech_pitch_v3 score_breakdown 维度名 + 颜色——前端不写死 sbDims）
        out["tech_dims"] = {"短线表现": {"color": "#2563eb", "weight": 40},
                            "止损安全": {"color": "#16a34a", "weight": 30},
                            "情绪": {"color": "#ea580c", "weight": 20},
                            "龙虎榜": {"color": "#7c3aed", "weight": 10}}
    except Exception:
        pass
    try:
        # ★2026-08-14 #427 FRC 排雷红旗中文名（r1_cfo_np_low/r4_roe_no_cfo 等 → 中文说明）
        #   前端短线卡原直接渲染英文 id，用户看不懂——动态从 stock_risk_map 提取 id→desc 映射下发
        _rm = _read("stock_risk_map*.json")
        _flag_cn = {}
        for _r in (_rm.get("results") or []):
            for _f in (_r.get("flags") or []):
                _fid = _f.get("id")
                if _fid and _fid not in _flag_cn and _f.get("desc"):
                    _flag_cn[_fid] = _f.get("desc")
        if _flag_cn:
            out["risk_flags"] = _flag_cn
    except Exception:
        pass
    _enums_cache.update({"ts": _now, "data": out})
    return out


# ══════════════════════════════════════════════════════════════════
# ★2026-08-13 #310：数据资产概览（unified.db asset_inventory + data_lineage + 表统计）
# 供"数据"页可读——动态数据库（整合库）的可视化入口
# ══════════════════════════════════════════════════════════════════
_data_assets_cache = {"ts": 0.0, "data": None}


def live_data_assets() -> dict:
    """数据资产概览：unified.db 各表统计 + asset_inventory（资产清单）+ data_lineage（消费链路）"""
    _now = time.time()
    from deck.research_views import locations, data_assets
    _, cache = locations()
    path = cache/'unified.db'
    version = (str(path), path.stat().st_mtime_ns, path.stat().st_size) if path.exists() else (str(path), None)
    if _data_assets_cache["data"] is not None and _data_assets_cache.get('version') == version and _now - _data_assets_cache["ts"] < 120:
        return _data_assets_cache["data"]
    out = data_assets()
    _data_assets_cache.update({"ts": _now, "data": out, 'version': version})
    return out


# ══════════════════════════════════════════════════════════════════
# ★2026-08-14 盘中实时行情（用户需求：数据实时更新）
#   新浪全市场实时快照（akshare stock_zh_a_spot，5542 只，~12s）→ 市场实时指标
#   60s 缓存（盘中频繁请求不重复拉；非交易时段自动降级返回最近快照）
# ══════════════════════════════════════════════════════════════════
_rt_cache = {"ts": 0.0, "data": None, "err": None, "loading": False, "attempt_ts": 0.0}
_rt_lock = threading.Lock()


def _fetch_realtime() -> dict:
    """盘中实时行情快照：涨跌家数/涨停数/成交额/涨跌幅分布/领涨领跌。
    数据源：新浪实时快照（akshare stock_zh_a_spot，60s 缓存）。
    非交易时段（周末/盘前/盘后）返回最近一次快照 + market_open=False。"""
    global _rt_cache
    import time as _t
    now = _t.time()
    out = {"ok": True, "market_open": True,
           "ts": datetime.now().strftime("%H:%M:%S"),
           "source": "sina_realtime"}
    try:
        import akshare as _ak
        import pandas as _pd
        df = _ak.stock_zh_a_spot()   # 新浪全市场实时（5542 只）
        if df is None or df.empty:
            raise ValueError("空快照")
        # 列名兼容（新浪：最新价/涨跌幅/成交额/成交量 等）
        cols = {c: c for c in df.columns}
        for a, b in [("最新价", "最新价"), ("涨跌幅", "涨跌幅"), ("成交额", "成交额"),
                     ("成交量", "成交量"), ("名称", "名称"), ("代码", "代码"),
                     ("昨收", "昨收"), ("最高", "最高"), ("最低", "最低")]:
            if b in df.columns:
                cols[b] = b
        price = _pd.to_numeric(df.get("最新价"), errors="coerce")
        chg = _pd.to_numeric(df.get("涨跌幅"), errors="coerce")
        amt = _pd.to_numeric(df.get("成交额"), errors="coerce")
        up = int((chg > 0).sum())
        down = int((chg < 0).sum())
        flat = int((chg == 0).sum())
        limit_up = int((chg >= 9.8).sum())     # 近涨停（含 10%/20% 板块近似）
        limit_down = int((chg <= -9.8).sum())
        med = float(chg.median()) if chg.notna().any() else 0.0
        total_amt = float(amt.sum()) if amt.notna().any() else 0.0
        # 领涨/领跌（按涨跌幅排序取 5）
        idx = chg.dropna().nlargest(5).index
        gainers = [{"code": df.loc[i, "代码"], "name": df.loc[i, "名称"],
                    "pct": float(chg.loc[i])} for i in idx if str(df.loc[i, "代码"]) != "nan"]
        idx2 = chg.dropna().nsmallest(5).index
        losers = [{"code": df.loc[i, "代码"], "name": df.loc[i, "名称"],
                   "pct": float(chg.loc[i])} for i in idx2 if str(df.loc[i, "代码"]) != "nan"]
        _snap = ""
        for _c in ("时间", "时间戳", "更新时间"):
            if _c in df.columns and not df[_c].isna().all():
                _snap = str(df[_c].iloc[0])
                break
        # ★2026-08-14 持仓页实时收益：全市场现价映射 {code: {price, pct}}（新浪 code 如 sz300684 → 统一 300684.SZ）
        _quotes = {}
        try:
            _codes = df.get("代码")
            for _i in range(len(df)):
                _c = str(_codes.iloc[_i]).strip().lower()
                if not _c or _c == "nan":
                    continue
                _std = _c[2:] + "." + _c[:2].upper()
                _pr = price.iloc[_i]
                _pc = chg.iloc[_i]
                if _pr is not None and not _pd.isna(_pr):
                    _quotes[_std] = {"price": round(float(_pr), 3),
                                     "pct": round(float(_pc), 2) if (_pc is not None and not _pd.isna(_pc)) else None}
        except Exception:
            pass
        out.update({
            "n_stocks": int(len(df)),
            "up": up, "down": down, "flat": flat,
            "limit_up": limit_up, "limit_down": limit_down,
            "median_chg": round(med, 2),
            "total_amount_yi": round(total_amt / 1e8, 0) if total_amt else 0,
            "gainers": gainers[:5], "losers": losers[:5],
            "snap_time": _snap,
            "quotes": _quotes,   # ★2026-08-14 持仓页实时价映射（60s 缓存内 0 成本复用）
        })
        _rt_cache.update({"ts": now, "data": out, "err": None})
    except Exception as e:
        # 拉取失败：有缓存则返回最近（标注陈旧），无则 error
        _rt_cache["err"] = str(e)[:120]
        if _rt_cache["data"] is not None:
            out = dict(_rt_cache["data"])
            out["stale"] = True
            out["stale_ts"] = datetime.now().strftime("%H:%M:%S")
        else:
            out = {"ok": False, "market_open": False,
                   "ts": datetime.now().strftime("%H:%M:%S"),
                   "error": str(e)[:120]}
    return out


def live_realtime() -> dict:
    """Keep slow upstream market downloads out of request and health-check threads."""
    from paperlive.config import load_config as _paper_config
    cfg = _paper_config()
    if cfg['market']['provider'] in ('tushare','tencent_tushare'):
        configured_source=cfg['market'].get('source_label') or ('tushare:rt_k' if cfg['market']['provider']=='tushare' else cfg['market']['provider'])
        # The quote daemon is the only live downloader. A read endpoint must not
        # silently switch to a different supplier or turn historical bars into ticks.
        try:
            from paperlive.runtime import public_state
            from datetime import timezone, timedelta
            try:
                raw = json.loads((Path(cfg['runtime_dir']) / 'state.json').read_text())
            except (OSError,ValueError):
                raw = {}
            state = public_state(cfg, raw, {})
            market = state.get('market', {})
            source=market.get('source') or configured_source
            activation=state.get('activation') or {}
            readiness = state.get('readiness') or {}
            if readiness.get('state') == 'waiting_market':
                return {'ok': True, **readiness, 'readiness': readiness, 'paused': True,
                        'activation': activation, 'market_open': False, 'source': source,
                        'loading': False, 'quotes': {}, 'ts': state.get('updated_at')}
            if activation.get('enabled') and activation.get('armed') is not False and activation.get('status')=='waiting_data':
                return {'ok':True,'ready':False,'state':'waiting_data','paused':True,'activation':activation,
                        'market_open':False,'source':source,'loading':False,'quotes':{},
                        'reason':'等待所需数据验收后自动启动；当前尚未开始盘中模拟',
                        'historical_error':market.get('error'),'ts':state.get('updated_at')}
            if state.get('paused'):
                return {'ok':True,'ready':False,'state':'deferred','paused':True,'market_open':False,
                        'source':source,'loading':False,'quotes':{},
                        'reason':'盘中模拟已按当前选择暂停；核心研究与分钟历史补齐继续运行',
                        'historical_error':market.get('error'),'ts':state.get('updated_at')}
            now_dt = datetime.now(timezone(timedelta(hours=8)))
            quotes = []
            for row in state.get('quotes', []):
                stamp = datetime.fromisoformat(row['timestamp'])
                age = (now_dt - stamp).total_seconds()
                if not 0 <= age <= cfg['market']['max_quote_age_seconds']:
                    continue
                last, previous = float(row['last']), float(row['previous_close'])
                if last <= 0 or previous <= 0:
                    continue
                quotes.append({**row, 'price': last, 'pct': (last/previous-1)*100})
            if market.get('error') or market.get('heartbeat_stale') or not quotes:
                return {'ok': False, 'market_open': False, 'source': source,
                        'scope': '当前沪深300可交易成分', 'loading': False, 'quotes': {},
                        'error': market.get('error') or '尚无可验证的新鲜盘中报价',
                        'ts': state.get('updated_at'), 'phase': market.get('phase')}
            ranked = sorted(quotes, key=lambda row: row['pct'], reverse=True)
            simple = lambda row: {'code':row['symbol'], 'name':row['name'], 'pct':round(row['pct'], 2)}
            return {'ok': True, 'market_open': bool(market.get('open')), 'source': source,
                    'scope': '当前沪深300可交易成分', 'n_stocks':len(quotes), 'ts':state.get('updated_at'),
                    'up':sum(row['pct']>0 for row in quotes), 'down':sum(row['pct']<0 for row in quotes),
                    'flat':sum(row['pct']==0 for row in quotes),
                    'limit_up':sum(row.get('limit_up') is not None and row['price']>=float(row['limit_up']) for row in quotes),
                    'limit_down':sum(row.get('limit_down') is not None and row['price']<=float(row['limit_down']) for row in quotes),
                    'total_amount_yi':None, 'gainers':list(map(simple,ranked[:5])),
                    'losers':list(map(simple,ranked[-5:][::-1])),
                    'quotes':{row['symbol']:{'price':row['price'],'pct':round(row['pct'],2)} for row in quotes}}
        except Exception:
            return {'ok':False, 'source':configured_source, 'market_open':False, 'loading':False,
                    'error':'本机行情状态尚未就绪', 'quotes':{}}
    now = time.time()
    refresh_seconds = _health_config().get("realtime_refresh_seconds", 60)
    with _rt_lock:
        if _rt_cache["data"] is not None and now - _rt_cache["ts"] < refresh_seconds:
            return dict(_rt_cache["data"])
        if not _rt_cache["loading"] and now - _rt_cache["attempt_ts"] >= refresh_seconds:
            _rt_cache.update({"loading": True, "attempt_ts": now})

            def refresh():
                try:
                    _fetch_realtime()
                except Exception as exc:
                    _rt_cache["err"] = str(exc)[:120]
                finally:
                    with _rt_lock:
                        _rt_cache["loading"] = False

            threading.Thread(target=refresh, name="market-snapshot", daemon=True).start()
        if _rt_cache["data"] is not None:
            return {**_rt_cache["data"], "stale": True, "loading": _rt_cache["loading"],
                    "error": _rt_cache["err"] or "行情快照正在刷新"}
        return {"ok": False, "loading": _rt_cache["loading"],
                "ts": datetime.now().strftime("%H:%M:%S"),
                "error": _rt_cache["err"] or "实时行情正在加载，请稍后刷新"}


# ══════════════════════════════════════════════════════════════════
# ★2026-08-14 API 接口状态探测（用户需求："再做一个API接口状态的信息块"）
# 并发 HTTP 自探测全部 /api 端点：每个端点 200/耗时/错误 → 门户"API 接口状态"块
# 60s 缓存（与 realtime 同级）；失败端点保留上次状态 + 标注
# ══════════════════════════════════════════════════════════════════
_ep_cache = {"ts": 0.0, "data": None, "err": None}

# 全部探测端点（只读；排除 export_data 内部子进程导出、stock_check 个股重查询——非稳态端点）
def _health_config() -> dict:
    import yaml
    for name in ("web_health.yaml", "web_health.yaml.example"):
        try:
            cfg = yaml.safe_load((BASE / "config" / name).read_text(encoding="utf-8"))
            if isinstance(cfg, dict) and isinstance(cfg.get("endpoints"), list):
                return cfg
        except (OSError, ValueError, yaml.YAMLError):
            continue
    return {"endpoints": []}


def _probe_one(path: str, port: int = 8787) -> dict:
    import urllib.request as _ur, time as _t
    from urllib.error import HTTPError
    _st = _t.monotonic()
    try:
        timeout = float(_health_config().get("timeout_seconds", 5))
        opener = _ur.build_opener(_ur.ProxyHandler({}))
        try:
            response = opener.open(f"http://127.0.0.1:{port}{path}", timeout=timeout)
        except HTTPError as exc:
            response = exc
        errors = []
        with response as _r:
            status = _r.code
            try:
                body = json.loads(_r.read())
            except (ValueError, UnicodeError):
                body = None
                errors.append("接口返回的内容不是有效 JSON")
        business_state = None
        if isinstance(body, dict) and status == 200:
            from paperlive.readiness import classify_market_wait
            waiting = classify_market_wait(path, body)
            if waiting:
                return {'path': path, 'status': status, 'reachable': True,
                        'ms': int((_t.monotonic()-_st)*1000), 'ok': False,
                        'state': waiting['state'], 'initialized': waiting['initialized'],
                        'error': waiting['reason'], 'next_open': waiting.get('next_open')}
        if isinstance(body,dict) and status==200 and (((body.get("activation") or {}).get("status")=="waiting_data" and (body.get("activation") or {}).get("armed") is not False) or body.get("state")=="waiting_data"):
            return {"path":path,"status":status,"reachable":True,"ms":int((_t.monotonic()-_st)*1000),
                    "ok":False,"state":"waiting_data","error":"等待所需数据验收后自动启动；当前尚未开始盘中模拟"}
        if isinstance(body,dict) and body.get("paused") is True and status==200:
            return {"path":path,"status":status,"reachable":True,"ms":int((_t.monotonic()-_st)*1000),
                    "ok":False,"state":"deferred","error":body.get("reason") or "盘中模拟已暂停，暂不启用"}
        if not isinstance(body, (dict, list)):
            errors.append("接口没有返回有效业务对象")
        if isinstance(body, dict):
            readiness = body.get("readiness") if isinstance(body.get("readiness"), dict) else {}
            business_state = readiness.get("state") or body.get("state")
            for key in ("error", "err"):
                if body.get(key):
                    errors.append(str(body[key]))
            for value in body.values():
                if isinstance(value, dict) and (value.get("error") or value.get("err")):
                    errors.append(str(value.get("error") or value.get("err")))
            if body.get("ok") is False and not errors:
                errors.append("数据尚未就绪")
            if body.get("stale"):
                errors.append("当前返回上次快照")
            if (body.get("ready") is False or readiness.get("ready") is False
                    or business_state in {"missing_data", "missing_schema", "initializing", "pending_history", "optional_unconfigured", "permission_blocked", "unknown", "failed", "stale"}):
                errors.append(str(readiness.get("reason") or body.get("reason") or body.get("message") or "业务尚未就绪"))
        ready = status == 200 and not errors
        row = {"path": path, "status": status, "reachable": True,
               "ms": int((_t.monotonic() - _st) * 1000), "ok": ready,
               "state": (business_state or "ready") if ready else (business_state or "not_ready")}
        if isinstance(body, dict) and "initialized" in body:
            row["initialized"] = body["initialized"] is True
        if not ready:
            row["error"] = ("；".join(dict.fromkeys(errors)) or f"HTTP {status}")[:200]
        return row
    except Exception as _e:
        _ms = int((_t.monotonic() - _st) * 1000)
        return {"path": path, "status": 0, "ms": _ms, "ok": False, "reachable": False,
                "state": "connection_error",
                "error": str(_e)[:80]}


_endpoint_snapshot = BackgroundSnapshot('endpoint-health', retry_seconds=5)
_endpoint_progress = {'version':None,'endpoints':[],'started_at':0}
_endpoint_progress_lock = threading.Lock()


def _endpoint_counts(rows, total):
    return {'total':total,'checked_count':len(rows),'ok_count':sum(bool(row.get('ok')) for row in rows),
            'connected_count':sum(bool(row.get('reachable')) for row in rows),
            'unready_count':sum(bool(row.get('reachable')) and not row.get('ok') and row.get('state')!='deferred' for row in rows),
            'deferred_count':sum(row.get('state')=='deferred' for row in rows),
            'waiting_market_count':sum(row.get('state')=='waiting_market' for row in rows),
            'fail':[row for row in rows if not row.get('ok')],'endpoints':rows}


def _collect_endpoints(endpoints, workers, version):
    from concurrent.futures import ThreadPoolExecutor, as_completed
    with _endpoint_progress_lock:
        _endpoint_progress.update(version=version,endpoints=[],started_at=time.time())
    rows=[]
    with ThreadPoolExecutor(max_workers=workers,thread_name_prefix='endpoint-check') as pool:
        futures={pool.submit(_probe_one,path):path for path in endpoints}
        for future in as_completed(futures):
            rows.append(future.result())
            with _endpoint_progress_lock:
                _endpoint_progress.update(version=version,endpoints=list(rows))
    rows.sort(key=lambda row:endpoints.index(row['path']))
    return {**_endpoint_counts(rows,len(endpoints)),'ts':datetime.now().isoformat(timespec='seconds')}


def live_endpoints() -> dict:
    """A read-only, deduplicated background audit; no warm-up or scans in HTTP."""
    from validation.check_portal_readiness import SAFE_APIS
    cfg=_health_config();allowed=set(SAFE_APIS)
    configured=[path for path in cfg.get('endpoints',[]) if isinstance(path,str)]
    endpoints=list(dict.fromkeys(path for path in configured if path in allowed))
    excluded=[path for path in configured if path not in allowed]
    if not endpoints:
        return {**_endpoint_counts([],0),'ok':True,'ready':False,'state':'optional_unconfigured',
                'loading':False,'excluded_routes':excluded,'reason':'没有配置已核定安全的只读检查项'}
    workers=max(1,min(4,int(cfg.get('max_workers',4))))
    version=(tuple(endpoints),workers,float(cfg.get('timeout_seconds',5)))
    result=_endpoint_snapshot.read(version,lambda:_collect_endpoints(endpoints,workers,version),
                                   max_age=max(5,float(cfg.get('cache_seconds',120))))
    if result['current']:
        data=dict(result['data'])
    else:
        with _endpoint_progress_lock:
            rows=(list(_endpoint_progress['endpoints']) if _endpoint_progress['version']==version
                  and _endpoint_progress.get('started_at',0)>(result['updated_at'] or 0) else [])
        data=_endpoint_counts(rows,len(endpoints))
    return {**data,'ok':True,'ready':result['current'],'state':result['state'],
            'loading':result['loading'],'stale':False,'excluded_routes':excluded,
            'reason':'只读接口检查已完成' if result['current'] else '正在后台检查只读接口；未完成项不计为可用' if result['loading'] else '后台接口检查失败，冷却后重试',
            'error':result['error'],'updated_at':result['updated_at']}


def live_wufu_rotation() -> dict:
    """Read the current explicitly identified proxy-rotation artifact only."""
    try:
        from data.global_rotation import widget
        return widget()
    except Exception:
        return {"ok": False, "ready": False, "initialized": False, "state": "missing_data",
                "assets": [], "date": "", "a_share_weak": None, "weak_vote": None, "defensive": None,
                "error": "全球资产当前日期、身份、配置或覆盖尚未通过验收"}


if __name__ == "__main__":
    print(json.dumps(live_pools(), ensure_ascii=False, indent=1)[:400])
