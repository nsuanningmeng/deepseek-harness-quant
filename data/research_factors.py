"""Rebuild supported research factors from real local bars, without a trading entry point.

Signal t uses information through t close. Holding h buys at t+1 open and exits
at t+h+1 open. Incomplete labels are missing, never zero. Health icir20/60/120
are rolling observation windows of the primary holding horizon, not horizons.
The separate evaluations field reports each configured holding horizon.
"""
import argparse
from collections import Counter
from datetime import datetime, timezone, timedelta
import json
from pathlib import Path
import sys

import numpy as np
import pandas as pd
import yaml

BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE))

from factors import research_context as context
from factors.factor_engine import FACTOR_FUNCS
from factors.pool.registry import FactorRegistry

TZ = timezone(timedelta(hours=8))
ALGORITHM = "research_factors.factor_engine_rankic_t1_open_v1"


def load_config(path=None):
    path = Path(path) if path else BASE / "config/research_factors.yaml"
    if not path.is_file():
        path = path.with_name(path.name + ".example")
    cfg = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(cfg, dict) or cfg.get("scope") != "current_hs300":
        raise ValueError("因子配置必须明确 current_hs300 范围")
    if pd.Timestamp(cfg["start"]) < pd.Timestamp("2019-01-01"):
        raise ValueError("含换手率因子的研究不得使用2019年前数据")
    if not 0 < cfg["top_fraction"] < 1 or cfg["primary_horizon"] not in cfg["holding_horizons"]:
        raise ValueError("持有窗口或截面比例配置无效")
    for key in ("holding_horizons", "ic_windows"):
        if not cfg[key] or any(type(value) is not int or value <= 0 for value in cfg[key]):
            raise ValueError(key + " 必须为正交易日数")
    supported = [*FACTOR_FUNCS, "turn_low"]
    if set(supported) - set(cfg.get("factors") or {}):
        raise ValueError("新增公式尚缺名称、方向、用途元数据，请补充research_factors配置")
    for name in supported:
        if cfg["factors"][name]["direction"] not in (-1, 0, 1):
            raise ValueError("因子方向只允许-1、0或1")
    return cfg


def clean(value):
    if isinstance(value, dict):
        return {str(k): clean(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [clean(v) for v in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (float, np.floating)):
        return float(value) if np.isfinite(value) else None
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    return value


def atomic_json(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(clean(data), ensure_ascii=False, allow_nan=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def atomic_csv(path, frame):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    frame.to_csv(tmp, index=False, na_rep="")
    tmp.replace(path)


def factor_panels(bars, cfg, sessions=None):
    """No forward filling: a suspended/missing observation is not a quoted price."""
    fields = {}
    for name in ("open", "high", "low", "close", "volume", "turn", "is_st"):
        fields[name] = bars.pivot(index="date", columns="code", values=name).sort_index().apply(pd.to_numeric, errors="coerce")
        if sessions is not None:
            fields[name] = fields[name].reindex(sessions)
    close = fields["close"].where(fields["close"] > 0)
    raw, directed = {}, {}
    for name in [*FACTOR_FUNCS, "turn_low"]:
        meta = cfg["factors"][name]
        if name == "turn_low":
            series = fields["turn"].where(fields["turn"] >= 0).rolling(cfg["turnover_window"], min_periods=cfg["turnover_window"]).mean()
        else:
            # Use the existing engine formula; mask its implicit missing-price/early-history defaults.
            series = FACTOR_FUNCS[name](close)
        warm = close.rolling(meta["lookback"], min_periods=meta["lookback"]).count() == meta["lookback"]
        series = series.where(warm & close.notna()).replace([np.inf, -np.inf], np.nan)
        raw[name] = series
        directed[name] = series * (meta["direction"] or 1)  # direction=0 is diagnostic only, never an execution rank.
    return fields, raw, directed


def forward_returns(fields, horizon, cfg):
    """A complete, tradable entry AND exit are required at exact market sessions."""
    opens, high, low, volume = (fields[key] for key in ("open", "high", "low", "volume"))
    tradable = (opens > 0) & (volume > 0) & (high > low) & (fields["is_st"] != 1)
    entry = opens.shift(-1).where(tradable.shift(-1, fill_value=False))
    exit_price = opens.shift(-(horizon + 1)).where(tradable.shift(-(horizon + 1), fill_value=False))
    gross = (exit_price / entry - 1).replace([np.inf, -np.inf], np.nan)
    costs = cfg["costs"]
    buy = (costs["commission_bps_each_side"] + costs["slippage_bps_each_side"]) / 10000
    sell = (costs["commission_bps_each_side"] + costs["slippage_bps_each_side"] + costs["sell_stamp_duty_bps"]) / 10000
    net = exit_price * (1-sell) / (entry * (1+buy)) - 1
    return gross, net.replace([np.inf, -np.inf], np.nan)


def rank_ic(factor, labels, minimum):
    valid = factor.notna() & labels.notna()
    ranks = factor.where(valid).rank(axis=1, method="average")
    returns = labels.where(valid).rank(axis=1, method="average")
    count = valid.sum(axis=1)
    ic = ranks.corrwith(returns, axis=1).where(count >= minimum)
    return ic.replace([np.inf, -np.inf], np.nan), count


def statistics(ic, horizon):
    values = ic.dropna().astype(float)
    n = len(values)
    result = {"n_sections": n, "first_signal_date": values.index[0] if n else None,
              "last_signal_date": values.index[-1] if n else None, "mean_ic": None, "ic_std": None,
              "icir": None, "hac_t": None, "ic_win_rate": None}
    if n < 2:
        return result
    array = values.to_numpy()
    mean, std = float(array.mean()), float(array.std(ddof=1))
    centered = array - mean
    lags = min(horizon-1, n-1)
    lrv = float(np.dot(centered, centered) / n)
    for lag in range(1, lags+1):
        lrv += 2 * (1-lag/(lags+1)) * float(np.dot(centered[lag:], centered[:-lag]) / n)
    result.update(mean_ic=mean, ic_std=std, icir=mean/std if std > 0 else None,
                  hac_t=mean/np.sqrt(lrv/n) if lrv > 0 else None,
                  ic_win_rate=float((values > 0).mean()), hac_lags=lags)
    return clean(result)


def evaluate_factor(factor, labels, cfg):
    result, series = {}, {}
    for horizon, (gross, net) in labels.items():
        ic, counts = rank_ic(factor, gross, cfg["minimum_cross_section"])
        stat = statistics(ic, horizon)
        mask = factor.where(gross.notna()).rank(axis=1, pct=True) > 1-cfg["top_fraction"]
        top_gross = gross.where(mask).mean(axis=1).where(counts >= cfg["minimum_cross_section"])
        top_net = net.where(mask).mean(axis=1).where(counts >= cfg["minimum_cross_section"])
        bench = net.where(factor.notna()).mean(axis=1).where(counts >= cfg["minimum_cross_section"])
        stat.update(holding_sessions=horizon, state="evaluated" if stat["n_sections"] >= cfg["minimum_mature_sections"] else "insufficient_samples", n_security_labels=int((factor.notna() & gross.notna()).sum().sum()),
                    top_fraction=cfg["top_fraction"], mean_top_gross_return=top_gross.mean(), mean_top_net_return=top_net.mean(),
                    mean_benchmark_net_return=bench.mean(), mean_top_excess=(top_net-bench).mean(),
                    return_note="各信号截面的固定持有期平均收益，不是可叠加年化收益或实盘净值")
        result[str(horizon)] = clean(stat)
        series[horizon] = ic
    return result, series


def health_row(name, meta, evaluations, series, raw, cfg, date):
    horizon = cfg["primary_horizon"]
    primary = series[horizon].dropna()
    row = {"factor": name, "direction": meta["direction"], "test_date": date, "last_date": date,
           "n_days": len(primary), "primary_holding_sessions": horizon,
           "ic_win_rate": None, "crowding": None, "status": "数据不足", "coverage": float(raw.loc[date].notna().mean())}
    for window in cfg["ic_windows"]:
        stat = statistics(primary.tail(window), horizon)
        row["icir"+str(window)] = stat["icir"] if len(primary) >= window else None
        row["t"+str(window)] = stat["hac_t"] if len(primary) >= window else None
    if len(primary):
        row["ic_win_rate"] = float((primary.tail(max(cfg["ic_windows"])) > 0).mean())
        magnitude = primary.abs().rolling(cfg["correlation_sessions"], min_periods=cfg["correlation_sessions"]).mean().dropna()
        if len(magnitude):
            row["crowding"] = float((magnitude <= magnitude.iloc[-1]).mean()*100)
    threshold = cfg["health_thresholds"]
    longest = max(cfg["ic_windows"])
    icir, t = row.get("icir"+str(longest)), row.get("t"+str(longest))
    if meta["direction"] == 0:
        row["status"] = "停用"
    elif len(primary) >= cfg["minimum_mature_sections"] and icir is not None:
        if icir < 0:
            row["status"] = "反向漂移"
        elif icir >= threshold["effective_icir"] and t is not None and t >= threshold["effective_hac_t"] and row["ic_win_rate"] >= threshold["effective_ic_win_rate"]:
            row["status"] = "研究有效"
        elif icir >= threshold["weak_icir"]:
            row["status"] = "弱有效"
        else:
            row["status"] = "待观察"
    row["note"] = "方向0仅诊断；其他指标为已成熟标签；t值已修正持有期重叠。拥挤为成熟IC绝对值滚动均值的历史分位代理。"
    return clean(row)


def market_snapshot(cfg, fields, date):
    """Aggregate actual available A-share rows; report measured coverage, not assumed coverage."""
    dates = list(fields["close"].index)
    first = dates[max(0, len(dates)-cfg["crowding_sessions"]-cfg["turnover_window"])]
    frames = []
    root = context.cache_dir()
    for db in (root/"bars.db", root/"bars_incr.db", *sorted(root.glob("bars_incr_*.db"))):
        if not db.is_file():
            continue
        with context.connect(db) as connection:
            frame = pd.read_sql_query("SELECT date,COUNT(DISTINCT code) securities,AVG(CASE WHEN turn>=0 THEN turn END) mean_turn, "
                                      "AVG(CASE WHEN pct_chg IS NOT NULL THEN CASE WHEN pct_chg>0 THEN 1.0 ELSE 0.0 END END) advance_ratio "
                                      "FROM daily_bar WHERE adjust='none' AND date>=? AND date<=? GROUP BY date", connection, params=(first,date))
        if len(frame):
            frame["source_db"] = db.name
            frames.append(frame)
    if not frames:
        return {"date": date, "ok": False, "error": "全市场未复权日线不可读，不能生成换手与宽度拥挤代理"}, pd.DataFrame()
    # A partial increment must not be added to a full-market snapshot and double counted.
    market = pd.concat(frames).sort_values(["date","securities"]).drop_duplicates("date",keep="last").set_index("date").sort_index()
    market["mean_turn20"] = market["mean_turn"].rolling(cfg["turnover_window"], min_periods=cfg["turnover_window"]).mean()
    history = market["mean_turn20"].dropna().tail(cfg["crowding_sessions"])
    value = market["mean_turn20"].get(date)
    percentile = float((history <= value).mean()*100) if len(history) >= cfg["minimum_mature_sections"] and pd.notna(value) else None
    turn20 = fields["turn"].rolling(cfg["turnover_window"], min_periods=cfg["turnover_window"]).mean()
    count_crowded = 0
    for code in turn20:
        sub = turn20[code].dropna().tail(cfg["crowding_sessions"])
        if len(sub) >= cfg["minimum_mature_sections"] and sub.iloc[-1] >= sub.quantile(cfg["health_thresholds"]["maximum_crowding_percentile"]/100):
            count_crowded += 1
    current = market.loc[date] if date in market.index else None
    return clean({"date": date, "ok": current is not None, "scope": "available_a_shares", "coverage": int(current["securities"]) if current is not None else 0,
                  "source_db": current["source_db"] if current is not None else None,
                  "crowding_mkt": value, "crowding_pctile_252": percentile,
                  "zone": "样本不足" if percentile is None else "换手偏高" if percentile >= cfg["health_thresholds"]["maximum_crowding_percentile"] else "换手未处高分位",
                  "n_crowded_stocks": count_crowded, "crowded_stock_scope": cfg["scope"],
                  "advance_ratio": current["advance_ratio"] if current is not None else None, "n_history_sessions": len(history),
                  "method": "每日选证券覆盖最多的单库截面；20日平均换手的历史分位。宽度为当日上涨证券占有涨跌幅数据证券比例。不是拥挤风险消除证明。"}), market


def build(cfg, end=None):
    now = datetime.now(TZ).isoformat(timespec="seconds")
    runtime = context.path(cfg["runtime_dir"])
    state_path = runtime / "factors-state.json"
    atomic_json(state_path,{"status":"running","phase":"loading_real_bars","started_at":now,"updated_at":now})
    try:
        bars = context.read_bars("code,date,open,high,low,close,volume,turn,is_st,source", scoped=True)
        bars = bars[(bars["date"] >= cfg["start"]) & (bars["date"] <= (str(end) if end else datetime.now(TZ).date().isoformat()))].copy()
        if bars.empty:
            raise ValueError("没有真实本地日线；不生成空因子清单")
        sources = sorted(set(bars["source"].dropna().astype(str)))
        if bars["source"].isna().any() or not sources or any(not any(source.lower().startswith(prefix.lower()) for prefix in cfg["allowed_source_prefixes"]) for source in sources):
            raise ValueError("本机日线来源未通过真实数据源白名单检查")
        input_manifest = json.loads((runtime/"bootstrap-manifest.json").read_text(encoding="utf-8"))
        completed = input_manifest.get("completed") or {}
        verified_end = (input_manifest.get("coverage",{}).get("daily") or {}).get("end")
        if not verified_end:
            raise ValueError("日线清单缺少实际内容截止日期")
        requested_end = min(str(end) if end else datetime.now(TZ).date().isoformat(), str(verified_end))
        verified_sessions = sorted(key.split(":",1)[1] for key in completed if key.startswith("daily:") and cfg["start"] <= key.split(":",1)[1] <= requested_end)
        if input_manifest.get("source") != "tushare-official" or not verified_sessions:
            raise ValueError("缺少按官方交易日历逐日完成的本地行情清单")
        if set(bars["date"]) - set(verified_sessions):
            raise ValueError("本地行情含尚未验收的交易日，不能用作成熟窗口基准")
        fields, raw, directed = factor_panels(bars, cfg, verified_sessions)
        if len(fields["close"]) < cfg["minimum_history_sessions"]:
            raise ValueError("本机历史交易日不足，不能完成成熟持有窗口评估")
        date = str(fields["close"].index[-1])
        if fields["close"].loc[date].notna().sum() < cfg["minimum_cross_section"]:
            raise ValueError("当前真实行情截面样本不足")
        labels = {h:forward_returns(fields,h,cfg) for h in cfg["holding_horizons"]}
        scope = context.scope_metadata()
        analysis = {"algorithm":ALGORITHM,"source":"tushare-official-local-bars","source_values":sources,
                    **scope,"scope":cfg["scope"],"start":str(fields["close"].index[0]),"data_date":date,
                    "signal":"t收盘信息","entry":"t+1交易日开盘","exit":"t+h+1交易日开盘","holding_horizons":cfg["holding_horizons"],
                    "health_icir_fields":"主持有窗口的最近20/60/120个成熟截面ICIR；不是持有天数",
                    "costs":cfg["costs"],"one_price_filter":"入场与到期退出均剔除high==low及无量条目；未声称已核验历史实际涨跌停价格",
                    "incomplete_labels":"不足完整持有窗口、停牌、缺报价均为null且不计入成熟统计",
                    "historical_st_unknown_rows":int(bars["is_st"].isna().sum()),
                    "limitations":[scope["limitation"],"历史ST标签缺失的样本不能声称已完整剔除ST。","截面IC不是组合可交易收益证明；HAC校正不消除多重检验及样本选择偏差。","rps_120与mom_120为同一公式，不是两份独立证据。"]}
        health, manifest, lifecycle, evaluated = [], [], {}, {}
        all_ic_records = []
        session_position = {date:i for i,date in enumerate(fields["close"].index)}
        snapshot = pd.DataFrame({"date":date,"code":fields["close"].columns})
        for name, panel in directed.items():
            meta = cfg["factors"][name]
            evaluations, series = evaluate_factor(panel,labels,cfg)
            evaluated[name] = evaluations
            row = health_row(name,meta,evaluations,series,raw[name],cfg,date)
            health.append(row)
            ranked = panel.loc[date].rank(pct=True) if meta["direction"] else pd.Series(np.nan,index=panel.columns)
            snapshot[name] = snapshot["code"].map(raw[name].loc[date])
            snapshot[name+"_rank"] = snapshot["code"].map(ranked)
            manifest.append({"code":name,"name_cn":meta["name_cn"],"category":meta["category"],"direction":meta["direction"],
                             "status":row["status"],"usage":meta["usage"],"icir_60":row.get("icir60"),"data_date":date,
                             "coverage":row["coverage"],"evaluations":evaluations})
            primary = series[cfg["primary_horizon"]].dropna()
            maturity = {signal_date:str(fields["close"].index[session_position[signal_date]+cfg["primary_horizon"]+1]) for signal_date in primary.index}
            month_ends = primary.groupby([maturity[value][:7] for value in primary.index]).tail(1).index if len(primary) else []
            timeline = []
            for signal_date in month_ends:
                stat = statistics(primary.loc[:signal_date].tail(max(cfg["ic_windows"])),cfg["primary_horizon"])
                if stat["n_sections"] >= max(cfg["ic_windows"]):
                    timeline.append({"date":maturity[signal_date],"last_signal_date":signal_date,"icir120":stat["icir"],"n_sections":stat["n_sections"]})
            lifecycle[name] = {"series":timeline,"latest":row.get("icir120"),"status":row["status"]}
            for horizon, ic in series.items():
                all_ic_records.extend({"factor":name,"holding_sessions":horizon,"signal_date":idx,"matured_at":str(fields["close"].index[session_position[idx]+horizon+1]),"rank_ic":value} for idx,value in ic.dropna().items())
        # A fully missing maturity evaluation is a failed initialization, not a zero score.
        incomplete = [name for name, evaluation in evaluated.items() if cfg["factors"][name]["direction"] and
                      (raw[name].loc[date].notna().sum() < cfg["minimum_cross_section"] or any(value["state"] != "evaluated" for value in evaluation.values()))]
        if incomplete:
            raise ValueError("支持因子的当前样本或成熟持有窗口不足："+"、".join(incomplete))
        crowding, market = market_snapshot(cfg,fields,date)
        if not crowding.get("ok"):
            raise ValueError(crowding.get("error") or "全市场换手与宽度数据尚未就绪")
        outputs = context.path(cfg["output_dir"])
        reports = context.path(cfg["report_dir"])
        ui = outputs / "ui_data"
        # Legacy turnover_rank means high rank is low turnover, the same supported factor.
        snapshot["turnover_rank"] = snapshot["turn_low_rank"]
        artifacts = []
        def save_json(path,payload):
            atomic_json(path,{"date":date,"data_date":date,"source":analysis["source"],"analysis":analysis,**payload})
            artifacts.append(str(path.relative_to(BASE)) if path.is_relative_to(BASE) else str(path))
        atomic_csv(outputs/"daily_scores"/f"daily_{date}.csv",snapshot)
        artifacts.append(str(outputs/"daily_scores"/f"daily_{date}.csv"))
        atomic_csv(outputs/"health"/f"health_{date}.csv",pd.DataFrame(health))
        artifacts.append(str(outputs/"health"/f"health_{date}.csv"))
        atomic_csv(outputs/f"rank_ic_{date}.csv",pd.DataFrame(all_ic_records))
        artifacts.append(str(outputs/f"rank_ic_{date}.csv"))
        save_json(outputs/f"crowding_{date}.json",crowding)
        if len(market):
            atomic_csv(outputs/"daily_scores"/f"market_{date}.csv",pd.DataFrame([{"date":date,"crowding_turnover":crowding.get("crowding_pctile_252"),"advance_ratio":crowding.get("advance_ratio"),"coverage":crowding.get("coverage")}]))
            artifacts.append(str(outputs/"daily_scores"/f"market_{date}.csv"))
        rank_frames = {name: panel.tail(cfg["correlation_sessions"]).stack() for name,panel in directed.items()}
        correlation = pd.DataFrame(rank_frames).corr(method="spearman",min_periods=cfg["minimum_cross_section"])
        save_json(ui/f"factor_corr_{date}.json",{"factors":list(correlation.columns),"matrix":correlation.to_numpy().tolist(),"period":f"最近{cfg['correlation_sessions']}个已观测交易日","method":"同日期×证券的方向化因子值Spearman相关；不代表独立alpha"})
        save_json(ui/f"factor_lifecycle_{date}.json",{"factors":lifecycle})
        layers = {"ext_decision":{},"frc_mine":{},"deferred":{},"disabled":{}}
        rules=[]
        threshold=cfg["health_thresholds"]
        coefficients=[]
        for row in health:
            name=row["factor"]
            layers["disabled" if not row["direction"] else "deferred"][name]=cfg["factors"][name]["usage"]
            observed=row.get("icir120") is not None and row.get("crowding") is not None and bool(row["direction"])
            coefficient=(threshold["reduced_risk_coefficient"] if row.get("crowding",0)>=threshold["maximum_crowding_percentile"] or row.get("icir120",0)<0 else 1.0) if observed else threshold["insufficient_risk_coefficient"]
            if row["direction"]:
                coefficients.append(coefficient)
            rules.append({"factor":name,"name":cfg["factors"][name]["name_cn"],"status":row["status"],"coefficient":coefficient,"icir120":row.get("icir120"),"crowding":row.get("crowding"),"observed":observed,
                          "ex60":"研究系数 "+str(coefficient),"stability":str(row["n_days"])+"个成熟截面；未作交易放行",
                          "reason":"仅按已观测IC与拥挤代理计算的研究系数；无数据用配置的不足系数，不代表实际风险解除"})
        save_json(ui/f"factor_usage_{date}.json",{"layers":layers,"note":"全部属于研究参考；未通过独立组合验证前不授予自动决策直通。"})
        save_json(ui/f"frc_mine_rules_{date}.json",{"rules":rules,"trigger":"实际指标研究系数","iron_rule":"实际指标公式系数，不是实际风险已解除或交易放行；没有生成静态减分。","note":"实际指标公式系数，不是实际风险已解除或交易放行。"})
        save_json(outputs/"risk"/f"risk_multiplier_{date}.json",{"multiplier":min(coefficients) if coefficients else None,"factor_coefficients":rules,"note":"研究展示系数；没有修改实时模拟盘风控或交易配置。"})
        save_json(ui/f"factor_data_freshness_{date}.json",{"updated":date,"manifest":{"date":date,"coverage":f"{len(health)}个实际支持因子"},"health_csv":{"date":date,"coverage":f"{len(health)}行"},"note":"本模块不生成分钟因子，不把5min宣称为1min。"})
        alerts=[{"factor":row["factor"],"status":row["status"],"icir120":row.get("icir120"),"level":"red" if row["status"] in ("反向漂移","停用") else "orange" if row["status"]=="弱有效" else "yellow"} for row in health if row["status"]!="研究有效"]
        save_json(ui/f"factor_alert_rules_{date}.json",{"rules":rules,"alerts":alerts,"note":"阈值来自research_factors配置；有风险或数据不足均保留。"})
        registry = FactorRegistry(context.cache_dir()/"factor_pool.db")
        for row in health:
            name=row["factor"];meta=cfg["factors"][name]
            registry.register(name,family=meta["family"],kind="cross_sectional",source=analysis["source"],freq="daily",direction=meta["direction"],note=meta["usage"])
            icir=row.get("icir120")
            score=float(100*(.5+.5*np.tanh(icir))) if icir is not None and row["direction"] else None
            status="monitoring" if row["status"]=="研究有效" else "retired" if row["status"] in ("反向漂移","停用") else "candidate"
            registry.update_score(name,score,status=status,detail={"data_date":date,"health":row,"evaluations":evaluated[name],"analysis":analysis})
        entries=registry.list_factors()
        artifacts.append(str(registry.db_path))
        save_json(reports/"factor_pool_report.json",{"ts":now,"generated_at":now,"stats":registry.stats(),"factors":entries,
                    "health":health,"evaluations":evaluated,"sample_maturity":"仅完整持有期标签；新近日期留空；不是前向实盘跟踪收益。"})
        # Manifest is the final artifact, so readers do not see a completed list before its data.
        save_json(outputs/f"factor_manifest_{date}.json",{"health_date":date,"n":len(manifest),"factors":manifest,"generated_at":now})
        result={"status":"complete","phase":"completed","started_at":now,"updated_at":datetime.now(TZ).isoformat(timespec="seconds"),
                "data_date":date,"scope":cfg["scope"],"rows":len(snapshot),"factor_count":len(health),"statuses":dict(Counter(row["status"] for row in health)),
                "source":analysis["source"],"algorithm":ALGORITHM,"artifacts":artifacts,"analysis":analysis}
        atomic_json(state_path,result)
        return result
    except Exception as exc:
        atomic_json(state_path,{"status":"failed","phase":"failed","started_at":now,"updated_at":datetime.now(TZ).isoformat(timespec="seconds"),"error_type":type(exc).__name__,"error":str(exc)[:300]})
        raise


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config")
    parser.add_argument("--end")
    args=parser.parse_args()
    try:
        result=build(load_config(args.config),args.end)
        print(json.dumps({k:result[k] for k in ("status","data_date","scope","factor_count","statuses","source","algorithm","artifacts")},ensure_ascii=False))
        return 0
    except Exception as exc:
        print(json.dumps({"status":"failed","error_type":type(exc).__name__,"error":str(exc)[:300]},ensure_ascii=False),file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
