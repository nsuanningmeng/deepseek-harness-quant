"""Completed-session and signal inputs for the isolated Pitch forward comparison."""

from contextlib import closing
from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
import json
import math
from pathlib import Path
import sqlite3

import pandas as pd

from autotrade.providers import TushareHttpClient
from autotrade.rules import board_for_symbol
from backtest.short_rule_study import BARS, features
from data.research_bootstrap import TZ, compact, iso
from etf.etf_map import execution_config
from factors.research_context import cache_dir, config, connect, path, universe
from paperlive.config import load_config as load_paper_config
from paperlive.data_settings import tushare_connection


def _read_json(file):
    try:
        return json.loads(file.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def _official_rows(endpoint, fields, **params):
    client = TushareHttpClient(tushare_connection(load_paper_config()), config()["request_timeout"])
    reply = client.query(endpoint, fields=",".join(fields), **params)
    if not set(fields) <= set(reply.columns):
        raise ValueError(f"官方 {endpoint} 返回缺少必要字段")
    return reply.to_dict("records")


def _calendar_days(start, end):
    first, last = date.fromisoformat(start), date.fromisoformat(end)
    if first > last:
        raise ValueError("前瞻日历查询起始日晚于截止日")
    return [(first + timedelta(days=i)).isoformat() for i in range((last - first).days + 1)]


def _cached_calendar():
    research = _read_json(path(config()["runtime_dir"]) / "service-calendar.json")
    dates = {}
    if research.get("source") == "tushare:trade_cal:SSE" and research.get("start") and research.get("through"):
        sessions = set(research.get("sessions", []))
        dates = {day: day in sessions for day in _calendar_days(research["start"], research["through"])}
    live = _read_json(Path(load_paper_config()["runtime_dir"]) / "market" / "tushare" / "calendar.json")
    if live.get("source") == "tushare:trade_cal" and isinstance(live.get("dates"), dict):
        for day, is_open in live["dates"].items():
            if type(is_open) is bool:
                if day in dates and dates[day] != is_open:
                    return {}
                dates[day] = is_open
    return dates


def market_calendar(start=None, end=None):
    """Return explicit SSE session dates; use official input for any coverage gap."""
    today = datetime.now(TZ).date()
    start = iso(start) if start else (today - timedelta(days=35)).isoformat()
    end = iso(end) if end else (today + timedelta(days=7)).isoformat()
    requested = _calendar_days(start, end)
    dates = _cached_calendar()
    if not set(requested) <= dates.keys():
        fields = ("exchange", "cal_date", "is_open")
        rows = _official_rows("trade_cal", fields, exchange="SSE", start_date=compact(start), end_date=compact(end))
        dates = {}
        for row in rows:
            day = iso(row["cal_date"])
            if row["exchange"] != "SSE" or day in dates or str(row["is_open"]) not in ("0", "1"):
                raise ValueError("前瞻官方交易日历内容异常")
            dates[day] = str(row["is_open"]) == "1"
        if dates.keys() != set(requested):
            raise ValueError("前瞻官方交易日历覆盖不足")
    return [day for day in requested if dates[day]]


def _sealed_day(day):
    cfg = config()
    manifest = _read_json(path(cfg["runtime_dir"]) / "bootstrap-manifest.json")
    completed = manifest.get("completed", {}).get(f"daily:{day}", {})
    if (manifest.get("source") != "tushare-official" or not completed.get("completed_at")
            or completed.get("rows", 0) < cfg["min_daily_rows"]
            or not completed.get("historical_st_verified")):
        raise ValueError(f"{day} 官方日线、复权因子、换手率或 ST 数据尚未验收")
    return completed


def _read_inputs(day, symbols, window):
    start = (date.fromisoformat(day) - timedelta(days=max(65, window * 3))).isoformat()
    db = cache_dir() / "bars.db"
    if not db.is_file():
        raise ValueError("缺少已验收的官方日线库")
    placeholders = ",".join("?" for _ in symbols)
    with closing(connect(db)) as con:
        con.execute("BEGIN")
        daily = pd.read_sql_query(
            f"SELECT {BARS} FROM daily_bar WHERE code IN ({placeholders}) "
            "AND source='tushare' AND adjust='qfq' AND date BETWEEN ? AND ?",
            con, params=(*symbols, start, day),
        )
        raw = pd.read_sql_query(
            "SELECT b.code,b.date,b.open,b.high,b.low,b.close,b.preclose,b.volume,b.is_st,a.factor "
            "FROM daily_bar b LEFT JOIN adjustment_factor a ON a.code=b.code AND a.date=b.date "
            f"WHERE b.code IN ({placeholders}) AND b.source='tushare' AND b.adjust='none' AND b.date BETWEEN ? AND ?",
            con, params=(*symbols, start, day),
        )
        sessions = con.execute(
            "SELECT date FROM daily_bar WHERE code=? AND source='tushare' AND adjust='none' "
            "AND date BETWEEN ? AND ? AND close>0 ORDER BY date",
            (config()["index_code"], start, day),
        ).fetchall()
    return daily, raw, pd.Index(row[0] for row in sessions)


def _confirmed_suspensions(day, missing):
    if not missing:
        return set()
    fields = ("ts_code", "trade_date", "suspend_type", "suspend_timing")
    rows = _official_rows("suspend_d", fields, trade_date=compact(day))
    if any(iso(row["trade_date"]) != day or row["suspend_type"] not in ("S", "R") for row in rows):
        raise ValueError("停复牌数据日期或状态异常")
    halted = {row["ts_code"] for row in rows if row["suspend_type"] == "S" and row["suspend_timing"] in ("", None)}
    if missing - halted:
        raise ValueError(f"{day} 有 {len(missing - halted)} 个研究或持仓标的缺少官方行情和已确认停牌状态")
    return missing


def _quote(row, day):
    try:
        op, hi, lo, cl, prior, volume, factor = (
            float(getattr(row, key)) for key in ("open", "high", "low", "close", "preclose", "volume", "factor")
        )
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{day} 官方行情价格或复权因子缺失") from exc
    if (not all(math.isfinite(x) for x in (op, hi, lo, cl, prior, volume, factor))
            or not 0 < lo <= min(op, cl) <= max(op, cl) <= hi or prior <= 0 or factor <= 0
            or volume <= 0 or row.is_st not in (0, 1)):
        raise ValueError(f"{day} 官方 OHLC、复权因子或 ST 状态异常")
    # Same conservative one-price and limit-open model as the Pitch backtest.
    board = board_for_symbol(row.code)
    rate = Decimal("0.20") if board in ("STAR", "CHINEXT") else Decimal("0.10")
    if board == "MAIN" and row.is_st and day < "2026-07-06":
        rate = Decimal("0.05")
    preclose = Decimal(str(prior))
    upper = float((preclose * (1 + rate)).quantize(Decimal(".01"), rounding=ROUND_HALF_UP))
    lower = float((preclose * (1 - rate)).quantize(Decimal(".01"), rounding=ROUND_HALF_UP))
    return {
        "open": op * factor, "high": hi * factor, "low": lo * factor, "close": cl * factor,
        "raw_open": op, "raw_close": cl, "preclose": prior, "factor": factor, "volume": volume,
        "buyable": bool(hi > lo and row.is_st == 0 and op < upper),
        "open_sellable": hi > lo and op > lower,
        "intraday_sellable": hi > lo,
    }


def _panels(frame, fields, sessions, codes):
    return {field: frame.pivot(index="date", columns="code", values=field)
            .reindex(index=sessions, columns=codes).astype(float) for field in fields}


def snapshot(day, symbols=(), *, topn=None, now=None, settings=None):
    """Read a sealed session and derive the Pitch signal using only its available history."""
    day = iso(day)
    moment = (now or datetime.now(TZ)).astimezone(TZ)
    cutoff = config()["completed_session_cutoff"]
    if day > moment.date().isoformat() or (day == moment.date().isoformat() and moment.strftime("%H:%M") < cutoff):
        raise ValueError(f"{day} 尚未到官方收盘数据验收时间")
    sealed = _sealed_day(day)
    codes = sorted(universe())
    all_codes = sorted(set(codes).union(symbols))
    settings = execution_config()["tech"] if settings is None else settings
    topn = settings["top_n"] if topn is None else topn
    window = max(settings["atr_n"], settings["consec_days"], settings["limup_days"]) + 1
    try:
        daily, raw, sessions = _read_inputs(day, all_codes, window)
    except (sqlite3.Error, pd.errors.DatabaseError) as exc:
        raise ValueError(f"{day} 官方日线库暂不可读") from exc
    if len(sessions) < window or sessions[-1] != day:
        raise ValueError(f"{day} 缺少已验收的沪深 300 指数日线或 Pitch 预热历史")
    current = raw[raw.date == day]
    quoted = current[current.volume > 0]
    suspended = _confirmed_suspensions(day, set(all_codes) - set(quoted.code))
    current_qfq = daily[(daily.date == day) & daily.code.isin(quoted.code)].set_index("code")
    if set(current_qfq.index) != set(quoted.code) or not current_qfq[["close", "turn", "is_st"]].notna().all().all():
        raise ValueError(f"{day} 前复权行情、换手率或 ST 覆盖尚不完整")
    bars = {row.code: _quote(row, day) for row in quoted.itertuples(index=False)}
    for code in suspended:
        bars[code] = {
            "open": None, "high": None, "low": None, "close": None, "volume": 0, "suspended": True,
            "buyable": False, "open_sellable": False, "intraday_sellable": False,
        }
    fields = _panels(daily, BARS.split(",")[2:], sessions, codes)
    original = _panels(raw, ("open", "high", "low", "close", "preclose"), sessions, codes)
    score, candidates, _, atr, _ = features(fields, original, pd.Index(codes), settings)
    ranked = score.loc[day].where(candidates.loc[day]).dropna().sort_values(ascending=False, kind="stable")
    signal = []
    for code, value in ranked.head(topn).items():
        quote = bars[code]
        if quote["volume"] == 0:
            raise ValueError(f"{day} 前复权候选与官方停牌状态冲突")
        qfq_close = fields["close"].at[day, code]
        qfq_atr = atr.at[day, code]
        signal.append({
            "code": code, "score": float(value), "close": quote["close"],
            "atr": float(qfq_atr * quote["close"] / qfq_close) if math.isfinite(qfq_atr) else None,
        })
    return {
        "date": day, "bars": bars, "signal": signal,
        "readiness": {
            "source": "tushare-official", "official_daily_completed_at": sealed["completed_at"],
            "universe_count": len(codes), "priced_universe_count": len(set(codes).intersection(quoted.code)),
            "suspended_count": len(suspended), "signal_candidates": len(ranked),
            "selected": len(signal), "selected_with_atr": sum(row["atr"] is not None for row in signal),
            "price_basis": "raw_ohlc_times_official_adjustment_factor",
        },
    }
