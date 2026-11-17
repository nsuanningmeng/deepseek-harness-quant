"""Sweep Pitch execution parameters in development; replay frozen candidates out of sample."""

from datetime import date, datetime
from itertools import product
from math import ceil, isfinite, prod
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

from backtest.bt_report import compute_metrics
from backtest.short_rule_study import (
    EXIT_GRACE, PRIMARY, Replay, features, interval, load_inputs, parity_check,
)
from etf.etf_map import execution_config
from factors.research_context import json_safe, scope_metadata

CONFIG = Path(__file__).resolve().parents[1] / "config" / "pitch_scan.yaml"
AXES = ("topn", "hold", "stop", "target", "size")
FIRST_DATE = "2019-01-01"


def _config():
    path = CONFIG if CONFIG.is_file() else CONFIG.with_suffix(".yaml.example")
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def get_defaults():
    cfg = _config()
    return {
        **{key: cfg[key] for key in ("train_start", "test_start", "end", "grid", "limits")},
        "drawdown_grid": cfg.get("drawdown", {}).get("grid"),
    }


def _date(value, label):
    try:
        parsed = date.fromisoformat(value)
    except (ValueError, TypeError):
        raise ValueError(f"{label}须使用 YYYY-MM-DD 格式") from None
    if parsed.isoformat() != value:
        raise ValueError(f"{label}须使用 YYYY-MM-DD 格式")
    return value


def _axis(values, key, limit):
    if not isinstance(values, list) or not values or len(values) > 32:
        raise ValueError(f"{key} 须提供 1 至 32 个参数值")
    clean = []
    for value in values:
        if key in ("topn", "hold"):
            if type(value) is not int or not 1 <= value <= limit:
                raise ValueError(f"{key} 须为 1 至 {limit} 的整数")
        elif key == "size":
            if type(value) not in (int, float) or not 0 < value <= 1:
                raise ValueError("仓位系数须大于 0 且不超过 1")
            value = float(value)
        elif value is not None:
            if (type(value) not in (int, float) or not 0 <= value <= limit
                    or not isfinite(value)):
                raise ValueError(f"{key} 须为 0 至 {limit} 倍 ATR，停用填 0 或 null")
            value = float(value) if value else None
        if value not in clean:
            clean.append(value)
    return clean


def validate_request(payload):
    if not isinstance(payload, dict) or payload.keys() - {"train_start", "test_start", "end", "grid", "profile"}:
        raise ValueError("扫描请求字段无效")
    defaults = get_defaults()
    start = _date(payload.get("train_start", defaults["train_start"]), "训练起点")
    split = _date(payload.get("test_start", defaults["test_start"]), "样本外起点")
    end = payload.get("end", defaults["end"])
    end = None if end in (None, "") else _date(end, "行情截止日")
    if start < FIRST_DATE or start >= split or (end and split >= end):
        raise ValueError("训练从 2019 年起，且训练起点、样本外起点、截止日必须按时间递增")
    profile = payload.get("profile", "return")
    if profile not in ("return", "drawdown"):
        raise ValueError("未知扫参目标")
    default_grid = defaults["drawdown_grid"] if profile == "drawdown" else defaults["grid"]
    grid = payload.get("grid", default_grid)
    if not isinstance(grid, dict) or not set(AXES[:-1]) <= set(grid) <= set(AXES):
        raise ValueError("参数网格须包含 TopN、持有期、ATR 止损及目标")
    limits = defaults["limits"]
    caps = (limits["topn_max"], limits["hold_max"], limits["atr_max"], limits["atr_max"], 1)
    grid = {key: _axis(grid.get(key, [1]), key, cap) for key, cap in zip(AXES, caps)}
    if profile == "drawdown" and (len(grid["topn"]) != 1 or len(grid["hold"]) != 1):
        raise ValueError("回撤细扫请固定 TopN 和持有期，以便与同口径参考组合比较")
    if prod(map(len, grid.values())) > limits["max_combinations"]:
        raise ValueError(f"参数组合最多 {limits['max_combinations']} 组")
    return {"train_start": start, "test_start": split, "end": end, "grid": grid, "profile": profile}


def _window(dates, start, end, max_hold, min_sessions):
    first = int(dates.searchsorted(start))
    signal_end = end - max_hold - EXIT_GRACE - 1
    if signal_end - first < min_sessions:
        raise ValueError(f"{start} 起不足 {min_sessions} 个完整信号交易日，请扩大日期区间")
    return first, signal_end, end


def _portfolio(rows, cols, exits, slots, replay, marks, fees, window, hold, size=1):
    """Buy a daily sleeve using available cash; use sale proceeds from the next session."""
    first, _, end = window
    entry_fee = 1 + fees["commission_rate"] + fees["slippage_rate"]
    exit_factor = 1 - fees["commission_rate"] - fees["slippage_rate"] - fees["sell_tax_rate"]
    offsets = exits.offset.to_numpy()
    net = exits.net.to_numpy()
    cash = 1.0
    active = np.empty(0, dtype=int)
    shares = np.zeros(len(rows))
    sell_at = np.where(np.isfinite(offsets), rows + offsets, end)
    exit_cash = replay.op[rows + 1, cols] * entry_fee * (1 + net)
    pnl = np.ones(end - first + 1)
    stale = 0
    boundaries = rows.searchsorted(np.arange(first - 1, end))
    for day in range(first, end):
        entered = np.arange(boundaries[day - first], boundaries[day - first + 1])
        if entered.size:
            # Slots with blocked entries stay in cash; previous close sizes today's cohort.
            budget = min(cash / len(entered), pnl[day - first] * size / ((hold + 1) * slots[day - 1]))
            if budget > 0:
                shares[entered] = budget / (replay.op[day, cols[entered]] * entry_fee)
                cash = max(0., cash - budget * len(entered))
                active = np.concatenate((active, entered))
        sold = sell_at[active] == day
        cash += float(np.dot(shares[active[sold]], exit_cash[active[sold]]))
        active = active[~sold]
        held = cols[active]
        stale += int((~np.isfinite(replay.close[day, held]) | (replay.volume[day, held] <= 0)).sum())
        pnl[day - first + 1] = cash + float(np.dot(shares[active], marks[day, held])) * exit_factor
    return pnl[1:] / pnl[:-1] - 1, stale, int(active.size), shares > 0


def _metrics(returns):
    result = compute_metrics(returns)
    # Include initial capital as a possible high-water mark on every reporting interval.
    equity = np.r_[1., (1. + returns.to_numpy()).cumprod()]
    result["max_drawdown"] = float(np.min(equity / np.maximum.accumulate(equity) - 1.))
    return result


def _prepare_trades(topn, hold, stop, target, window, dates, rank, replay):
    first, signal_end, _ = window
    rows, cols = np.where(rank[first:signal_end] <= topn)
    rows += first
    slots = np.bincount(rows, minlength=len(dates))
    # Compare every exit rule on the same observable-ATR entries for a given TopN.
    entry_ok = replay.buyable[rows + 1, cols] & (replay.atr[rows, cols] > 0)
    blocked = int((~replay.buyable[rows + 1, cols]).sum())
    selected_count = len(rows)
    rows, cols = rows[entry_ok], cols[entry_ok]
    exits = replay.run(rows, cols, hold, stop, target)
    return rows, cols, slots, exits, blocked, selected_count


def _evaluate_window(params, window, dates, replay, marks, fees, trades):
    first, _, end = window
    rows, cols, slots, exits, blocked, selected_count = trades
    pnl, stale, open_lots, funded = _portfolio(
        rows, cols, exits, slots, replay, marks, fees, window, params["hold"], params["size"],
    )
    returns = pd.Series(pnl, index=pd.to_datetime(dates[first:end]))
    events = pd.Series(exits.net.to_numpy()[funded], index=pd.to_datetime(dates[rows[funded]])).dropna()
    tail_count = max(1, ceil(.05 * len(events)))
    metrics = {
        **_metrics(returns),
        "n_trades": len(events), "signal_days": int(events.index.nunique()),
        "mean": events.groupby(level=0).mean().mean(), "tail": events.nsmallest(tail_count).mean(),
        "win": (events > 0).mean(), "blocked": blocked,
        "missing_atr": selected_count - blocked - len(rows),
        "unfunded": int((~funded).sum()), "unresolved": open_lots, "stale_lot_days": stale,
        "ambiguous_exits": int(exits.ambiguous.to_numpy()[funded].sum()),
        "minute_resolved": int(exits.minute_resolved.to_numpy()[funded].sum()),
    }
    return metrics, returns, events


def _years(returns, events, baseline):
    for year in sorted(set(returns.index.year)):
        values = returns[returns.index.year == year]
        trades = events[events.index.year == year]
        metrics = _metrics(values)
        yield {
            "year": int(year), "mean": trades.groupby(level=0).mean().mean(),
            "n_trades": len(trades), "annual_return": metrics["annual_return"],
            "total_return": metrics["total_return"], "max_drawdown": metrics["max_drawdown"],
            "edge": metrics["annual_return"] - _metrics(baseline.loc[values.index])["annual_return"],
            "total_edge": metrics["total_return"] - _metrics(baseline.loc[values.index])["total_return"],
        }


def run_scan(payload):
    request = validate_request(payload)
    cfg = _config()
    settings = execution_config()
    tech, fees = settings["tech"], settings["stock_fees"]
    dates, codes, fields, raw = load_inputs()
    if dates.empty:
        raise ValueError("缺少同时覆盖研究股票池和沪深 300 指数的日线")
    baseline = {"topn": tech["top_n"], "hold": PRIMARY, "stop": tech["stop_rule"]["atr_multiplier"], "target": None, "size": 1.0}
    grid = request["grid"]
    max_hold = max(baseline["hold"], *grid["hold"])
    split = int(dates.searchsorted(request["test_start"]))
    end = int(dates.searchsorted(request["end"], side="right")) if request["end"] else len(dates)
    min_sessions = cfg["min_signal_sessions"]
    train = _window(dates, request["train_start"], min(split, end), max_hold, min_sessions)
    test = _window(dates, request["test_start"], end, max_hold, min_sessions)

    score, candidates, _, atr, _ = features(fields, raw, codes, tech)
    parity = parity_check(fields, score, candidates, atr, tech)
    rank = score.where(candidates).rank(axis=1, method="first", ascending=False).to_numpy()
    replay = Replay(fields, raw, codes, dates, atr, fees)
    # Last observable close marks suspended holdings. Replay still forbids fills without quotes.
    marks = fields["close"].where((fields["close"] > 0) & (fields["volume"] > 0)).ffill().to_numpy()
    try:
        return _scan(request, cfg, dates, rank, replay, marks, fees, baseline, parity, train, test)
    finally:
        replay.minute.day.cache_clear()
        if replay.minute.db is not None:
            replay.minute.db.close()


def _drawdown_shortlist(ranking, baseline, reference, count):
    groups = {"size": [], "exit": [], "both": []}
    for item in ranking:
        p = item["params"]
        if p == baseline["params"] or p == reference or item["train"]["max_drawdown"] <= baseline["train"]["max_drawdown"]:
            continue
        resized = p["size"] != 1
        changed_exit = p["stop"] is not None or p["target"] is not None
        group = "both" if resized and changed_exit else "size" if resized else "exit"
        groups[group].append({**item, "group": group})
    finalists = []
    for group, items in groups.items():
        distinct = set()
        for item in items:
            p = item["params"]
            key = p["size"] if group == "size" else (p["stop"], p["target"])
            if key not in distinct:
                distinct.add(key)
                finalists.append(item)
            if len(distinct) >= count:
                break
    finalists.sort(key=lambda item: -item["train"]["annual_return"])
    return finalists, {group: len(items) for group, items in groups.items()}


def _scan(request, cfg, dates, rank, replay, marks, fees, baseline, parity, train, test):
    from functools import lru_cache

    @lru_cache(maxsize=16)
    def trades(window, topn, hold, stop, target):
        return _prepare_trades(topn, hold, stop, target, window, dates, rank, replay)

    def evaluate(params, window):
        execution = trades(window, *(params[key] for key in AXES[:-1]))
        return _evaluate_window(params, window, dates, replay, marks, fees, execution)

    grid = request["grid"]
    baseline_train, _, _ = evaluate(baseline, train)
    ranking = []
    evaluated = set()
    for values in product(*(grid[key] for key in AXES)):
        params = dict(zip(AXES, values))
        # A one-session holding ends at the first legal sell open for any ATR exit.
        effective = (*values[:2], None, None, values[-1]) if params["hold"] == 1 else values
        if effective in evaluated:
            continue
        evaluated.add(effective)
        params = dict(zip(AXES, effective))
        metrics = baseline_train if params == baseline else evaluate(params, train)[0]
        if metrics["signal_days"] >= cfg["min_signal_sessions"]:
            ranking.append({"params": params, "train": metrics})
    ranking.sort(key=lambda item: (-item["train"]["annual_return"], -item["train"]["max_drawdown"]))
    if not ranking:
        raise ValueError("训练期没有达到最少有效信号日的参数组合")
    risk_mode = request["profile"] == "drawdown"
    reference = dict(zip(AXES, (grid["topn"][0], grid["hold"][0], None, None, 1.0)))
    qualified = None
    if risk_mode:
        shortlist, qualified = _drawdown_shortlist(
            ranking, {"params": baseline, "train": baseline_train}, reference,
            cfg.get("drawdown", {}).get("finalists_per_group", 2),
        )
    else:
        shortlist = [row for row in ranking if row["params"] != baseline][:cfg["shortlist_size"]]

    baseline_test, baseline_returns, baseline_events = evaluate(baseline, test)

    def out_of_sample(item):
        metrics, returns, events = evaluate(item["params"], test)
        delta = returns - baseline_returns
        years = list(_years(returns, events, baseline_returns))
        return {
            **item, "test": metrics, "years": years,
            "edge": {
                "mean": delta.mean(), "ci": interval(delta),
                "annual_return": metrics["annual_return"] - baseline_test["annual_return"],
            },
            "below_baseline_drawdown": metrics["max_drawdown"] > baseline_test["max_drawdown"],
        }

    selected = [out_of_sample(item) for item in shortlist]
    reference_result = out_of_sample({"params": reference, "train": evaluate(reference, train)[0]}) if risk_mode else None
    risk_passed = [
        item["params"] for item in selected
        if item["below_baseline_drawdown"] and not item["test"]["unresolved"]
        and not baseline_test["unresolved"] and item["test"]["signal_days"] >= cfg["min_signal_sessions"]
    ]
    passed = [item for item in selected if (
        item["test"]["signal_days"] >= cfg["min_signal_sessions"]
        and item["test"]["annual_return"] > 0 and item["test"]["sharpe"] > .5
        and not item["test"]["unresolved"] and not baseline_test["unresolved"]
        and item["edge"]["ci"][0] > 0 and all(year["total_edge"] > 0 for year in item["years"])
    )]
    verdict = (
        f"观察｜{len(passed)} 组通过本次样本外收益稳定性检查，仍受成分股选择偏差影响"
        if passed else "观察｜入围参数未同时满足样本外收益稳定性条件"
    )
    objective = "训练期扣费后组合年化收益优先，同收益时优先较小回撤"
    if risk_mode:
        objective = "训练期最大回撤低于基线，按扣费后年化收益筛选"
        verdict = f"观察｜{len(risk_passed)} 组在样本外的最大回撤低于基线，且有效信号日达标、退出完整"
    windows = {name: {
        "start": dates[first], "end": dates[signal_end - 1], "execution_end": dates[end - 1],
        "signal_sessions": signal_end - first,
    } for name, (first, signal_end, end) in (("train", train), ("test", test))}
    return json_safe({
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "objective": objective,
        "request": request, "windows": windows, "parity": parity, "scope": scope_metadata(),
        "candidate_count": prod(map(len, grid.values())), "evaluated_count": len(evaluated),
        "eligible_count": len(ranking),
        "ranking": ranking, "selected": selected, "verdict": verdict,
        "reference": reference_result, "train_drawdown_qualified": qualified,
        "drawdown_passed": risk_passed if risk_mode else None,
        "stability_passed": [item["params"] for item in passed],
        "baseline": {"params": baseline, "train": baseline_train, "test": baseline_test,
                     "years": list(_years(baseline_returns, baseline_events, baseline_returns))},
        "notes": [
            "用训练期收益排名锁定参数，仅基线与入围组进入样本外，结果保留训练排名。各组使用相同信号截止日，为最长持有期和延迟退出留足空间。持有一天时 ATR 参数不影响成交，等价组合合并。",
            "信号收盘后生成，次日开盘买入，买入日不可卖；过滤不可成交开盘和一字板。止损锚定信号收盘，目标锚定买入价，缺少 ATR 的买单留现金。",
            "资金每日最多使用上一日净值 × 仓位系数/(持有期+1)，入选股等权，余款持现，卖出款次日再投资。允许分数股，未计最低佣金、整手或成交容量。",
            f"双边佣金各 {fees['commission_rate']:.3%}、滑点各 {fees['slippage_rate']:.2%}，卖出印花税固定 {fees['sell_tax_rate']:.2%}，历史税率未分段。停牌持仓按最近可用收盘估值；未退出仓位保留至期末估值。",
            "日线同日触及止损和目标时优先使用可核对的分钟数据判顺序，其余按止损优先。盘口排队、成交深度及实际滑点未重建。",
            "当前沪深 300 成分回看历史存在选择及幸存者偏差。历史风控快照和龙虎榜评分未回填。默认样本外区间已出现在旧专项研究中。",
            ("回撤细扫分别从只调整仓位、只调整退出、同时调整两者三组中按训练年化入围。样本外对照两条基准，检查最大回撤、保留的收益和分年表现。"
             if risk_mode else "稳定性条件为有效信号日达标、样本外年化为正、夏普高于 0.5、双方退出完整、逐年区间收益超过基线且月分块重抽样的相对日收益差 95% 区间下限高于零。"),
        ],
    })
