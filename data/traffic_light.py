# -*- coding: utf-8 -*-
"""data/traffic_light.py — 择时红绿灯（贪婪/观望/恐慌）· 均线金叉6/12 趋势信号（2026-08-14 v2）

信号源（★v2 从"回撤触发"换成"均线金叉6/12"领先型趋势）：
    沪深300 月末收盘 → 6月均线 MA6 vs 12月均线 MA12
    ratio = MA6/MA12 - 1
      greedy(绿) : ratio >  +1%   明确上升趋势 → 满仓
      wait(黄)   : -1% ≤ ratio ≤ +1%   均线纠缠/方向不明 → 半仓观望
      panic(红)  : ratio <  -1%   明确下降趋势 → 空仓/降杠杆

信号只使用已完成月份，仓位指示从次月生效。
历史区间展示当前成分股的月度收益关联统计，不是逐笔可成交回测。
数据源：本机已验证的Tushare指数和股票缓存；不带预填收益结论。
产出：output/traffic_light.json（当前信号、月度统计、下降周期和近24月历史）
用法：python data/traffic_light.py
"""
import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

BASE = Path(__file__).resolve().parent.parent
if str(BASE) not in sys.path:
    sys.path.insert(0, str(BASE))
from data.cache import CACHE_DIR
BARS_DB = str(CACHE_DIR / "bars.db")
PARQUET = BASE / "output" / "hs300_monthly.parquet"
EQ_PARQUET = BASE / "output" / "eq_monthly.parquet"
OUTPUT = BASE / "output" / "traffic_light.json"
REGIME_JSON = BASE / "output" / "dynamic_regime.json"   # ★仓位档（供 pool_layers/equal_weight_timing 消费）

# ---------------- 参数 ----------------
MA_FAST = 6           # 快线：6月均线
MA_SLOW = 12          # 慢线：12月均线
BAND = 0.01           # 观望死区：|MA6/MA12-1| <= 1% 视为方向不明

LABELS = {"greedy": "贪婪", "wait": "观望", "panic": "恐慌"}
COLORS = {"greedy": "green", "wait": "yellow", "panic": "red"}


def _ensure_parquet():
    """Refresh a derived local artifact; analysis never downloads another source."""
    with sqlite3.connect(f"file:{BARS_DB}?mode=ro", uri=True) as con:
        rows = con.execute("SELECT date,close FROM daily_bar WHERE code IN ('000300.SH','SH.000300') AND adjust='none' ORDER BY date").fetchall()
    if not rows:
        raise ValueError("沪深300指数历史尚未初始化")
    df = pd.DataFrame(rows, columns=["date", "close"])
    df["date"] = pd.to_datetime(df["date"])
    df["close"] = df["close"].astype(float)
    df = df.set_index("date")
    PARQUET.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(PARQUET)


def _bars_latest() -> tuple:
    con = sqlite3.connect(f"file:{BARS_DB}?mode=ro&immutable=1", uri=True)
    row = con.execute(
        "SELECT date, close FROM daily_bar WHERE code IN ('000300.SH','SH.000300') AND adjust='none' ORDER BY date DESC LIMIT 1").fetchone()
    con.close()
    return (row[0], float(row[1])) if row else (None, None)


def load_monthly() -> pd.Series:
    """真实沪深300历史月末收盘；当前未完成月不参与信号。"""
    _ensure_parquet()
    monthly = pd.read_parquet(PARQUET)["close"].resample("ME").last().dropna()
    monthly = monthly[~monthly.index.duplicated(keep="last")].sort_index()
    return monthly[monthly.index.to_period('M') < pd.Timestamp.now().to_period('M')].astype(float)


def compute_signal(monthly: pd.Series):
    """月度信号：6月均线 MA6、12月均线 MA12、ratio=MA6/MA12-1"""
    ma_fast = monthly.rolling(MA_FAST, min_periods=MA_FAST).mean()
    ma_slow = monthly.rolling(MA_SLOW, min_periods=MA_SLOW).mean()
    ratio = ma_fast / ma_slow - 1
    return ma_fast, ma_slow, ratio


def run_state_machine(monthly, ratio):
    """三态（死区阈值）：ratio>+1% 贪婪 / <-1% 恐慌 / 之间观望；返回 states + 恐慌周期"""
    states = {}
    episodes = []
    cur_start = None
    for mo in monthly.index:
        r = ratio.get(mo, float("nan"))
        if pd.isna(r):
            continue
        if r > BAND:
            st = "greedy"
        elif r < -BAND:
            st = "panic"
        else:
            st = "wait"
        states[mo] = st
        if st == "panic" and cur_start is None:
            cur_start = mo
        elif st != "panic" and cur_start is not None:
            episodes.append((cur_start, mo))
            cur_start = None
    if cur_start is not None:
        episodes.append((cur_start, monthly.index[-1]))
    return states, episodes


def load_eq() -> pd.Series:
    """当前HS300成分的完整月收盘收益统计；不包含指数ETF和未完成月。"""
    from factors.research_context import read_bars
    df = read_bars('code,date,close')
    df = df[df['close'] > 0]
    p = df.pivot_table(index="date", columns="code", values="close").sort_index()
    p.index = pd.to_datetime(p.index)
    m = p.resample("ME").last()
    m = m[m.index.to_period('M') < pd.Timestamp.now().to_period('M')]
    m.index = m.index.strftime("%Y-%m")
    eq = m.pct_change(fill_method=None).mean(axis=1).dropna()
    EQ_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"eq": eq}).to_parquet(EQ_PARQUET)
    return eq


def _perf(r: pd.Series):
    if r.empty:
        raise ValueError("没有足够的已完成样本计算择时历史统计")
    nav = (1 + r).cumprod()
    ann = nav.iloc[-1] ** (12 / len(nav)) - 1
    mdd = (nav / nav.cummax().clip(lower=1) - 1).min()
    sharpe = r.mean() / r.std() * np.sqrt(12) if r.std() > 0 else 0
    return ann, mdd, sharpe, nav.iloc[-1]


_REGIME_LEVEL = {"greedy": (1.0, "full"), "wait": (0.5, "half"), "panic": (0.0, "exit")}


def build_regime() -> dict:
    """均线金叉6/12 → dynamic_regime.json 格式 {month: {pos, top, level}}（回测/信号同源）
    greedy→full(1.0) / wait→half(0.5) / panic→exit(0.0)，替代原"满仓主义大波段"回撤档"""
    monthly = load_monthly()
    _, _, ratio = compute_signal(monthly)
    states, _ = run_state_machine(monthly, ratio)
    out = {}
    for mo, st in states.items():
        if mo.to_period('M') >= pd.Timestamp.now().to_period('M'):
            continue
        pos, level = _REGIME_LEVEL[st]
        effective = (mo.to_period('M') + 1).strftime('%Y-%m')
        out[effective] = {"pos": pos, "top": "均线金叉6/12", "level": level,
                          "signal_month": str(mo)[:7], "source": "tushare:index_daily"}
    return out


def build() -> dict:
    monthly = load_monthly()
    ma_fast, ma_slow, ratio = compute_signal(monthly)
    states, episodes = run_state_machine(monthly, ratio)
    if not states:
        raise ValueError("择时至少需要12个完整月份的指数数据")
    eq = load_eq()          # 全池等权月收益（用户实际持仓宇宙）

    # T+1 回测：仓位 贪婪1.0/观望0.5/恐慌0.0，基准=全池等权买入持有
    pos_map = {"greedy": 1.0, "wait": 0.5, "panic": 0.0}
    states_s = {str(mo)[:7]: v for mo, v in states.items()}
    pos = pd.Series({(pd.Period(mo, freq='M') + 1).strftime('%Y-%m'): pos_map[state]
                     for mo, state in states_s.items()}).reindex(eq.index)

    def _win(since):
        eqw = eq[eq.index >= since]
        pw = pos[eq.index >= since]
        strat = (eqw * pw).dropna()
        a_s, d_s, sh_s, nav_s = _perf(strat)
        a_b, d_b, sh_b, nav_b = _perf(eqw.loc[strat.index])
        return {"annual": round(a_s * 100, 1), "bench_annual": round(a_b * 100, 1),
                "mdd": round(d_s * 100, 1), "bench_mdd": round(d_b * 100, 1),
                "sharpe": round(sh_s, 2), "bench_sharpe": round(sh_b, 2),
                "nav": round(nav_s, 2), "bench_nav": round(nav_b, 2),
                "start": str(strat.index.min()), "end": str(strat.index.max()), "n_months": len(strat),
                "execution_validated": False,
                "note": "当前沪深300成分股月度收盘收益关联统计，存在成分选择偏差；仅用完整月份，月末信号次月使用。没有验证次日开盘可成交性、停牌退出及交易成本，不作为模拟成交收益。"}

    bt_recent = _win("2019-08")
    bt_full = _win("2010-02")

    _asof, _hs300 = _bars_latest()
    complete = [mo for mo in states if mo.to_period('M') < pd.Timestamp.now().to_period('M')]
    if not complete:
        raise ValueError("缺少已完成月份的择时信号")
    latest = complete[-1]
    cur = states[latest]
    s = pd.Series(states)
    yrs = bt_full['n_months'] / 12

    hist = {}
    for mo in list(states.keys())[-24:]:
        hist[str(mo)[:7]] = {
            "s": states[mo],
            "ratio": round(float(ratio.get(mo, 0)) * 100, 2),
            "ma6": round(float(ma_fast.get(mo, 0)), 0),
            "ma12": round(float(ma_slow.get(mo, 0)), 0),
        }

    # 最近一次金叉/死叉（乖离率穿越 0 线）
    last_cross = None
    prev_r = None
    for mo, r in ratio.items():
        if pd.isna(r) or prev_r is None:
            prev_r = r
            continue
        if prev_r <= 0 < r:
            last_cross = {"month": str(mo)[:7], "dir": "金叉", "to": "greedy"}
        elif prev_r >= 0 > r:
            last_cross = {"month": str(mo)[:7], "dir": "死叉", "to": "panic"}
        prev_r = r

    return {
        "ok": True,
        "schema_version": "2.0",
        "signal": "均线金叉6/12",
        "state": cur,
        "label": LABELS[cur],
        "color": COLORS[cur],
        "ratio": round(float(ratio.loc[latest]) * 100, 2),
        "ma6": round(float(ma_fast.loc[latest]), 1),
        "ma12": round(float(ma_slow.loc[latest]), 1),
        "signal_month": str(latest)[:7],
        "source": "tushare:index_daily+daily",
        "hs300": round(_hs300 if _hs300 else float(monthly.iloc[-1]), 2),
        "asof": str(_asof) if _asof else str(latest)[:10],
        "last_cross": last_cross,
        "updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "params": {"ma_fast": MA_FAST, "ma_slow": MA_SLOW, "band": BAND},
        "backtest": {
            "years": round(yrs, 1),
            "recent": bt_recent,       # 2019-08+（数据可靠、贴近当前）
            "full": bt_full,           # 2010-02+（含回填历史）
            "episodes": len(episodes),
            "greedy_pct": round(int((s == "greedy").sum()) / len(s) * 100),
            "wait_pct": round(int((s == "wait").sum()) / len(s) * 100),
            "panic_pct": round(int((s == "panic").sum()) / len(s) * 100),
        },
        "episodes": [
            {"start": str(st)[:7], "end": str(en)[:7],
             "months": (pd.Timestamp(en) - pd.Timestamp(st)).days // 30 + 1,
             "ratio": round(float(ratio.get(st, 0)) * 100, 1)}
            for st, en in episodes
        ],
        "history": hist,
    }


def main():
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    out = build()
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")
    # ★写 dynamic_regime.json（仓位档，替换回撤 regime，供 pool_layers/equal_weight_timing/pitch 消费）
    reg = build_regime()
    REGIME_JSON.write_text(json.dumps(reg, ensure_ascii=False, indent=1), encoding="utf-8")
    cur = reg.get(str(out["asof"])[:7], {})
    print(f"[择时红绿灯·均线金叉6/12] 当前 {out['label']}({out['state']})  "
          f"MA6 {out['ma6']} / MA12 {out['ma12']}（ratio {out['ratio']}%）  →  {OUTPUT}")
    print(f"  当前仓位档: {cur.get('level', '?')} (pos {cur.get('pos', '?')})  →  {REGIME_JSON}")
    r = out["backtest"]["recent"]
    print(f"  月度研究统计({r['start']}至{r['end']}): 年化 {r['annual']}% vs 满仓 {r['bench_annual']}%  |  "
          f"回撤 {r['mdd']}% vs {r['bench_mdd']}%  |  夏普 {r['sharpe']} vs {r['bench_sharpe']}")
    print(f"  完整可用历史统计: 年化 {out['backtest']['full']['annual']}% vs 满仓 {out['backtest']['full']['bench_annual']}%  |  "
          f"下降周期 {out['backtest']['episodes']} 次")


if __name__ == "__main__":
    main()
