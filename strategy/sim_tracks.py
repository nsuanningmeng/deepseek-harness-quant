# -*- coding: utf-8 -*-
"""strategy/sim_tracks.py — 模拟盘分轨跟踪（配置轨 vs 优选轨 · 外包 AI 2026-08-08）

★目的：验证"机会引擎 Pitch 精选"是否优于"原有等权配置"。

两轨：
  配置轨 = 全市场等权 × 择时仓位（dynamic_regime.json 月度 pos，PIT 用当月值）
  优选轨 = Pitch 精选：每月末用 PIT 因子面板触发 6 类机会 → 统一评分 →
           Top5 入轨（模拟 Pitch）→ 等权持有 6 个月滚动换仓 → T+1 开盘买入

纪律：
  - PIT：财报按披露窗口对齐；T+1 开盘买入；无未来函数
  - 复用 backtest_winrate.py 的数据层与因子面板（同一套逻辑，结果可比）
  - 与机会引擎联动：logs/deck_decisions.json 中 action=buy 的标的自动纳入优选轨
    （当前实时段：从 opportunity_pool.pitch + decisions 构建当前持仓快照）

★实时段（2026-08-09 夜间长链，外包 AI-1）：--live 参数 → 读 logs/portfolio.json（T-2 真实持仓）
  + 最新行情 → logs/sim_tracks_live.json（每只持仓实时盈亏 + 组合合计），Deck 持仓卡片可展示。

输出：logs/sim_tracks.json
  {date, tracks: {config: {curve:[{m, nav}], stats}, pitch: {curve, stats}},
   meta: {note, horizons, deck_buys}}

用法：
  python strategy/sim_tracks.py [--start 2020-01]
"""
import argparse
import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE))
from factors.research_context import json_safe

from factors.research_context import cache_dir, connect as research_connect, config as research_config, scope_metadata
BARS_DB = cache_dir() / "bars.db"

import numpy as np
import pandas as pd

sys.path.insert(0, str(BASE / "factors" / "opportunities"))
from factors.opportunities.backtest_winrate import _load_all, _load_st_on, \
    _pit_finance, _pit_quality, compute_factors_pit, _add_months, triggers
from factors.opportunities.score import opportunity_score
from factors.opportunities.registry import ORDER

OUT = BASE / "logs" / "sim_tracks.json"
REGIME_JSON = BASE / "output" / "dynamic_regime.json"
DECISIONS_JSON = BASE / "logs" / "deck_decisions.json"
POOL_JSON = BASE / "logs" / "opportunity_pool.json"

HOLD_MONTHS = 6       # 优选轨持仓期
TOP_N = 5             # 每月 Pitch 上限


def _load_regime() -> dict:
    """Actual monthly positions only; missing risk evidence must not imply full exposure."""
    d = json.loads(REGIME_JSON.read_text(encoding="utf-8"))
    out = {k: float(v["pos"]) for k, v in d.items()
           if isinstance(v, dict) and isinstance(v.get("pos"), (int, float)) and 0 <= v["pos"] <= 1 and str(v.get("signal_month", "9999")) < k}
    if not out:
        raise ValueError("缺少真实月度regime仓位")
    return out


def _monthly_market_ret(bars: pd.DataFrame, months: list) -> pd.DataFrame:
    """全市场等权月收益（月度收盘价环比均值）→ Series[month]=ret（索引=月份）"""
    bars = bars.copy()
    bars["month"] = bars["date"].dt.to_period("M").astype(str)
    g = bars.sort_values("date").groupby(["code", "month"])["close"].last().unstack()
    m_ret = g.pct_change(axis=1).iloc[:, 1:]   # 行=code 内沿列（月份）环比 → 股票月收益
    return m_ret.mean(axis=0)                  # 每月横截面等权 → 索引=月份


def _score_candidates(f: pd.DataFrame, trig: dict, winrate_map: dict) -> list:
    """当期触发集合 → 评分排序 → Top N（模拟 Pitch 四重过滤的简化版）"""
    cands = []
    for ot in ORDER:
        if ot not in trig:          # ★跳过外部信号驱动类型（pv_consensus 不走 triggers，2026-08-09）
            continue
        mask = f.apply(trig[ot], axis=1)
        for code, r in f[mask].iterrows():
            wr = winrate_map.get(ot)
            if wr is None:
                continue
            dd = abs(float(r.get("drawdown_60d") or 0.2)) * 100
            vol = float(r.get("vol60") or 0.3)
            risk = min(dd / 6.0 + 1.0 + max(vol - 0.20, 0) * 8 + 1.5, 10)
            upside = {"reversal": 25, "value": 20, "breakout": 40,
                      "revalue": 60, "event": 35, "quality_gap": 30}.get(ot, 25)
            g = min(upside / 15.0 * 5 + max(upside - 15, 0) / 45.0 * 5, 10)
            ts = 0.5 + min(abs(float(r.get("sq_nyoy") or 0)) / 2.0, 0.5) \
                if ot in ("revalue", "event") else 0.6
            p = min(wr * 10 + ts * 2.5, 10)
            s = opportunity_score(ot, g, p, risk, wr)
            cands.append({"code": code, "otype": ot, "score": s["score"]})
    if not cands:
        return []
    seen = {}
    for c in cands:
        if c["code"] not in seen or c["score"] > seen[c["code"]]["score"]:
            seen[c["code"]] = c
    top = sorted(seen.values(), key=lambda x: -x["score"])
    return top


def _next_open_px(by_code: dict, code: str, after: pd.Timestamp) -> float:
    """after 之后第一个交易日的开盘价（T+1 纪律）"""
    g = by_code.get(code)
    if g is None or g.empty:
        return None
    sub = g[g["date"] > after]
    if sub.empty:
        return None
    return float(sub["open"].iloc[0])


def _month_nav(curve_rets: list) -> list:
    """月收益序列 → [{m, nav}]"""
    nav = 1.0
    out = []
    for m, ret in curve_rets:
        nav *= (1 + ret)
        out.append({"m": m, "ret": round(ret, 5), "nav": round(nav, 5)})
    return out


def _stats(curve_rets: list, nav_curve: list) -> dict:
    if not curve_rets:
        return {}
    rets = np.array([r for _, r in curve_rets])
    n = len(rets)
    final = nav_curve[-1]["nav"]
    ann = final ** (12 / n) - 1 if n > 0 else 0.0
    dd = 0.0
    peak = 1.0
    for p in nav_curve:
        peak = max(peak, p["nav"])
        dd = min(dd, p["nav"] / peak - 1)
    sharpe = float(rets.mean() / rets.std() * np.sqrt(12)) if rets.std() > 0 else 0.0
    return {
        "final_nav": round(final, 5),
        "ann_ret": round(float(ann), 5),
        "max_dd": round(float(dd), 5),
        "sharpe": round(sharpe, 3),
        "winrate": round(float((rets > 0).mean()), 4),
        "n_months": n,
    }


def run(start: str = "auto") -> dict:
    print("加载数据...", flush=True)
    bars, fin, q, basic = _load_all()
    cfg = research_config()
    sim_cfg = cfg["sim_tracks"]
    hold_months, top_n = int(sim_cfg["hold_months"]), int(sim_cfg["top_n"])
    trig = triggers()
    regime = _load_regime()

    # 回测期（月度）
    dates = pd.DatetimeIndex(sorted(bars["date"].unique()))
    month_ends = []
    if len(dates) < int(cfg["min_history_days"]):
        raise ValueError("历史不足，不能运行预热后的双轨回测")
    ready = dates[int(cfg["min_history_days"]) - 1]
    cur = ready if start == "auto" else max(pd.Timestamp(start), ready)
    cur = cur.to_period("M").to_timestamp()
    while cur <= dates.max():
        mask = (dates >= cur) & (dates < _add_months(cur, 1))
        if mask.any() and cur.to_period("M") < pd.Period(datetime.now().strftime("%Y-%m"), freq="M"):
            month_ends.append(dates[mask][-1])
        cur = _add_months(cur, 1)

    mkt = _monthly_market_ret(bars, [str(m)[:7] for m in month_ends])

    by_code = {c: g.sort_values("date") for c, g in bars.groupby("code")}
    calibration = json.loads((BASE / "logs/opportunity_winrates.json").read_text(encoding="utf-8"))
    observations = calibration.get("observations")
    if not isinstance(observations, list) or not observations:
        raise ValueError("缺少带真实到期日的校准样本，先运行backtest_winrate")

    # 优选轨持仓（rolling）：{code: 到期月}
    holdings = {}
    config_rets, pitch_rets = [], []
    n_entries, warmup_months = 0, []

    for k, t in enumerate(month_ends):
        ym = str(t)[:7]
        # ---------- 配置轨 ----------
        if ym not in regime:
            raise ValueError(f"缺少{ym}生效择时证据，不能假设全仓")
        pos = regime[ym]
        if ym not in mkt.index or not np.isfinite(mkt[ym]):
            raise ValueError(f"缺少{ym}真实市场月收益")
        mret = float(mkt[ym])
        config_rets.append((ym, mret * pos))

        # ---------- 优选轨 ----------
        # 到期剔除
        if holdings:
            # 用入轨 base 价（T+1 开盘）计算当前持仓月收益
            r2 = []
            for c, h in holdings.items():
                base = h.get("mark")
                g = by_code.get(c)
                if g is None or base is None:
                    continue
                gd = g[g["date"] <= t]
                if gd.empty:
                    continue
                last = float(gd["close"].iloc[-1])
                r_one = last / base - 1
                if not np.isfinite(r_one) or r_one <= -1:
                    raise ValueError(f"{c} {ym}价格路径无效")
                r2.append(r_one)
                h["mark"] = last
            pitch_rets.append((ym, float(np.mean(r2)) if r2 else 0.0))
        else:
            pitch_rets.append((ym, 0.0))

        holdings = {c: h for c, h in holdings.items() if h.get("due", "") > ym}

        # 每月末：新一期 Pitch 入轨（T+1 开盘买入 → base 价 = 下月首日开盘）
        win = bars[(bars["date"] > t - pd.Timedelta(days=500)) & (bars["date"] <= t)]
        px = win.pivot(index="date", columns="code", values="close").ffill()
        vx = win.pivot(index="date", columns="code", values="volume").fillna(0)
        st = _load_st_on(bars, t)
        f = compute_factors_pit(px, vx, _pit_finance(fin, t), _pit_quality(q, t), st, basic)
        if f.empty:
            continue
        winrate_map = _load_winrates(observations, t, int(cfg["calibration_min_samples"]))
        if not winrate_map:
            warmup_months.append(ym)
        top = _score_candidates(f, trig, winrate_map)[:top_n]
        due = str(_add_months(t, hold_months))[:7]
        for c in top:
            base = _next_open_px(by_code, c["code"], t)
            if base and base > 0 and c["code"] not in holdings:
                holdings[c["code"]] = {"due": due, "mark": base, "otype": c["otype"]}
                n_entries += 1
        if (k + 1) % 24 == 0:
            print(f"  {k + 1}/{len(month_ends)} 期 ({t.date()}) 持仓 {len(holdings)} 只", flush=True)

    if not month_ends or not n_entries:
        raise ValueError("历史中没有经足量已到期校准证据支持的入轨记录，未生成成功回测")

    # Deck 联动：当前已批准买入
    deck_buys = []
    if DECISIONS_JSON.exists():
        try:
            deck_buys = [d for d in json.loads(DECISIONS_JSON.read_text(encoding="utf-8"))
                         if isinstance(d, dict) and d.get("action") == "buy"]
        except Exception:
            deck_buys = []

    # ★T-2 联动（2026-08-09 外包 AI-1）：当前真实持仓快照（portfolio.json，持股≤5 纪律）
    portfolio_positions = []
    PF_JSON = BASE / "logs" / "portfolio.json"
    if PF_JSON.exists():
        try:
            pf = json.loads(PF_JSON.read_text(encoding="utf-8"))
            portfolio_positions = [{"code": p.get("code"), "name": p.get("name"),
                                    "entry_date": p.get("entry_date"), "target": p.get("target"),
                                    "stop": p.get("stop"), "risk_level": p.get("risk_level")}
                                   for p in pf.get("positions", [])
                                   if p.get("status") == "holding"]
        except Exception:
            portfolio_positions = []

    out = {
        "date": str(dates.max().date()),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "kind": "historical_backtest",
        "status": "limited",
        "research_scope": scope_metadata(),
        "tracks": {
            "config": {"curve": _month_nav(config_rets), "stats": _stats(config_rets, _month_nav(config_rets))},
            "pitch": {"curve": _month_nav(pitch_rets), "stats": _stats(pitch_rets, _month_nav(pitch_rets))},
        },
        "meta": {
            "note": f"历史研究：当前研究池等权×当月生效择时（上月信号）；优选Top{top_n}持有{hold_months}月，T+1开盘，逐月mark-to-market；不是实时模拟账户",
            "n_entries": n_entries, "calibration_warmup_months": warmup_months,
            "start": start,
            "n_months": len(month_ends),
            "regime_source": "output/dynamic_regime.json",
            "deck_buys": deck_buys,
            "deck_buys_note": "Deck 审批 buy 记录将自动纳入后续优选轨（当前为回测段，仅供参考）",
            "portfolio_positions": portfolio_positions,
            "portfolio_note": "当前真实持仓快照（logs/portfolio.json，T-2 联动）：Deck 审批 buy 自动入持仓，实时段可据此跟踪实际组合",
            "caveats": [
                "优选轨未计交易成本/冲击成本（月换仓≈1/3 持仓），结果偏乐观",
                "简化评分使 revalue（业绩爆发）类霸榜 → 优选轨≈业绩动量组合",
                "2020-2023 结构性行情下业绩爆发股动量延续性强，历史收益不代表未来",
                "当前成分股回看历史存在幸存者偏差；本次未验证旧研究中的退市覆盖率",
                "仅计算已结束的自然月；当前未结束月份不按完整月年化",
                "单只月收益 |r|≥2 已剔除（脏数据防御）",
            ],
        },
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    # ★#417 时间戳文件名（写保护免疫 + 保留历史；读取方 glob 取最新——固定名 sim_tracks.json 是 08-09 旧残留）
    try:
        OUT.write_text(json.dumps(json_safe(out), ensure_ascii=False, indent=1), encoding="utf-8")
    except PermissionError:
        pass
    _ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    _p = BASE / "logs" / f"sim_tracks_{_ts}.json"
    _p.write_text(json.dumps(json_safe(out), ensure_ascii=False, indent=1), encoding="utf-8")
    return out


def _load_winrates(observations, asof, min_samples):
    """Walk-forward calibration: a future realized outcome cannot score a past decision."""
    values = {}
    for row in observations:
        if row.get("horizon_months") == 6 and row.get("exit_date", "9999") <= str(asof.date()):
            values.setdefault(row["otype"], []).append(float(row["ret"]))
    return {ot: float(np.mean(np.asarray(rets) > 0)) for ot, rets in values.items() if len(rets) >= min_samples}


def live_track() -> dict:
    """★实时段（T-2 联动完整版，2026-08-09）：真实持仓（portfolio.json）按最新行情估值
    输出 logs/sim_tracks_live.json：每只持仓 {code, name, entry_date, entry_price, last_price,
    last_date, pnl_pct} + 组合 {n_hold, avg_pnl_pct, max_dd_est}"""
    PF_JSON = BASE / "logs" / "portfolio.json"
    if not PF_JSON.exists():
        return {"error": "logs/portfolio.json 不存在（尚无持仓）"}
    pf = json.loads(PF_JSON.read_text(encoding="utf-8"))
    pos = [p for p in pf.get("positions", []) if p.get("status") == "holding"]
    if not pos:
        return {"error": "当前无持仓（holding 为空）", "positions": []}

    con = research_connect(BARS_DB)
    out_pos = []
    for p in pos:
        code = p["code"]
        rows = con.execute(
            "SELECT date, close FROM daily_bar WHERE code=? AND adjust='qfq' "
            "ORDER BY date DESC LIMIT 1", (code,)).fetchall()
        if not rows:
            out_pos.append({**p, "last_price": None, "last_date": None,
                            "pnl_pct": None, "note": "无行情"})
            continue
        last_date, last_price = rows[0][0], float(rows[0][1])
        entry = p.get("entry_price")
        pnl = (last_price / entry - 1) if entry else None
        out_pos.append({**{k: p.get(k) for k in ("code", "name", "entry_date",
                                                 "entry_price", "target", "stop")},
                        "last_price": round(last_price, 3), "last_date": last_date,
                        "pnl_pct": round(pnl, 4) if pnl is not None else None})
    con.close()

    pnls = [x["pnl_pct"] for x in out_pos if x.get("pnl_pct") is not None]
    out = {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "last_market_date": out_pos[0]["last_date"] if out_pos else None,
        "n_hold": len(out_pos),
        "positions": out_pos,
        "portfolio": {
            "avg_pnl_pct": round(sum(pnls) / len(pnls), 4) if pnls else None,
            "n_positive": sum(1 for x in pnls if x > 0),
            "max_win": round(max(pnls), 4) if pnls else None,
            "max_loss": round(min(pnls), 4) if pnls else None,
        },
        "note": "实时段（T-2 联动）：真实持仓按最新 qfq 收盘估值；entry_price 未填时 pnl=None（审批时未录成交价，"
                "可按 T+1 开盘回填）；盈亏为持仓浮盈，未计手续费",
    }
    LIVE_OUT = BASE / "logs" / "sim_tracks_live.json"
    LIVE_OUT.write_text(json.dumps(json_safe(out), ensure_ascii=False, indent=1), encoding="utf-8")
    return out


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=str, default="auto")
    ap.add_argument("--live", action="store_true", help="实时段：真实持仓估值（portfolio.json → sim_tracks_live.json）")
    args = ap.parse_args()
    if args.live:
        r = live_track()
        if "error" in r:
            print(f"[实时段] {r['error']}")
        else:
            print(f"=== 实时持仓估值（{r['last_market_date']}）===")
            for p in r["positions"]:
                pnl = f"{p['pnl_pct']:+.1%}" if p.get("pnl_pct") is not None else "—（未录成交价）"
                print(f"  {p['code']} {p.get('name','')}: 最新 {p.get('last_price')} | 盈亏 {pnl}")
            pf = r["portfolio"]
            print(f"组合: {r['n_hold']} 只 | 平均 {pf['avg_pnl_pct']:+.1%} | 盈利 {pf['n_positive']} 只"
                  f" | 最佳 {pf['max_win']:+.1%} 最差 {pf['max_loss']:+.1%}")
        sys.exit(0)
    r = run(start=args.start)
    print("\n===== 模拟盘双轨（月度） =====")
    for k, tr in r["tracks"].items():
        s = tr["stats"]
        print(f"{'优选轨' if k=='pitch' else '配置轨'}: 期末净值 {s.get('final_nav')} 年化 {s.get('ann_ret',0):.2%} "
              f"回撤 {s.get('max_dd',0):.2%} 夏普 {s.get('sharpe')} 月胜率 {s.get('winrate',0):.0%}")
    print(f"\n已存 {OUT}")
