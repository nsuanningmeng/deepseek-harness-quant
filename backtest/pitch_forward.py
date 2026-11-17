"""Run an isolated Pitch paper comparison from immutable, pre-open signals."""

import argparse
from bisect import bisect_left, bisect_right
from copy import deepcopy
from datetime import datetime, time, timedelta
import json
import logging
from pathlib import Path
from zoneinfo import ZoneInfo

from autotrade.locking import ProcessLock
from autotrade.monitoring import atomic_json

BASE = Path(__file__).resolve().parents[1]
DIRECTORY = BASE / "runtime" / "pitch_forward"
STATE = DIRECTORY / "state.json"
CONFIG = BASE / "config" / "pitch_forward.yaml"
MARKET_TZ = ZoneInfo("Asia/Shanghai")
AUCTION = time(9, 15)
DAILY_PUBLICATION = time(17)
LOG = logging.getLogger(__name__)


def _now():
    return datetime.now(MARKET_TZ)


def _read_state():
    try:
        return json.loads(STATE.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None


def _new_state():
    import yaml

    from backtest.pitch_forward_engine import initial_state

    source = CONFIG if CONFIG.is_file() else CONFIG.with_suffix(".yaml.example")
    policy = yaml.safe_load(source.read_text(encoding="utf-8"))
    return {
        "created_at": _now().isoformat(), "policy": policy, "engine": initial_state(policy),
        "start_session": None, "pending": None, "signals": {}, "skipped": [],
        "readiness": {}, "status": "waiting_data",
    }


def _publish(state, status, message):
    state.update(status=status, message=message, as_of=_now().isoformat())
    atomic_json(STATE, state)


def _auction_at(day):
    return datetime.combine(datetime.fromisoformat(day).date(), AUCTION, MARKET_TZ)


def _record_readiness(state, snapshot, day):
    details = snapshot["readiness"]
    state["readiness"] = {
        **details, "close_date": day, "covered_symbols": details.get("priced_universe_count"),
        "expected_symbols": details.get("universe_count"),
    }


def _advance(state):
    clock = _now()
    today = clock.date().isoformat()
    pending = state["pending"]
    if pending and state["status"] in ("ready", "running"):
        if today < pending or (today == pending and clock.time() < DAILY_PUBLICATION):
            return
    from backtest.pitch_forward_data import market_calendar, snapshot
    from backtest.pitch_forward_engine import apply_session, held_symbols

    recent = (clock.date() - timedelta(days=35)).isoformat()
    anchor = state["engine"]["last_session"] or state["start_session"] or recent
    sessions = market_calendar(start=min(anchor, recent))
    position = bisect_left(sessions, today)
    latest = bisect_right(sessions, today) - 1 if clock.time() >= DAILY_PUBLICATION else position - 1
    if latest < 0 or latest + 1 >= len(sessions) or sessions[latest + 1] < today:
        raise ValueError("缺少最近已结束交易日或下一交易日的官方日历")

    policy = state["policy"]
    signal_day, next_session = sessions[latest:latest + 2]
    start = state["start_session"]
    read_kwargs = {"topn": policy["top_n"], "now": clock, "settings": state.get("tech_settings")}
    if start and start not in sessions:
        raise ValueError("模拟起始交易日未包含在官方日历中")
    last = state["engine"]["last_session"]
    if last and last not in sessions:
        raise ValueError("最后已结算交易日未包含在官方日历中")
    first_unsettled = bisect_right(sessions, last) if last else bisect_left(sessions, start) if start else latest + 1

    current = None
    for day in sessions[first_unsettled:latest + 1]:
        frozen = state["signals"].get(day)
        orders = frozen["orders"] if frozen else []
        symbols = held_symbols(state["engine"]) | {order["code"] for order in orders}
        current = snapshot(day, symbols=symbols, **read_kwargs)
        _record_readiness(state, current, day)
        engine = deepcopy(state["engine"])
        apply_session(engine, day, current["bars"], orders, policy)
        state["engine"] = engine
        state["pending"] = None
        if frozen is None:
            state["skipped"].append(day)
        _publish(state, "running", f"已结算 {day} 的扣费后净值")

    frozen = state["signals"].get(next_session)
    if frozen is not None:
        state["pending"] = next_session
        status = "running" if state["engine"]["last_session"] else "ready"
        _publish(state, status, f"已锁定 {next_session} 的开盘信号，待盘后官方行情结算")
        return
    if _now() >= _auction_at(next_session):
        _publish(state, "waiting_data", f"{next_session} 已过信号锁定时点，等待下一次收盘数据")
        return

    if current is None:
        current = snapshot(signal_day, symbols=held_symbols(state["engine"]), **read_kwargs)
    _record_readiness(state, current, signal_day)
    orders = current["signal"][:policy["top_n"]]
    committed = _now()
    if committed >= _auction_at(next_session):
        _publish(state, "waiting_data", f"{next_session} 的行情核验超过信号锁定时点")
        return
    state["signals"][next_session] = {
        "signal_date": signal_day, "trade_date": next_session,
        "committed_at": committed.isoformat(), "orders": orders,
    }
    state["start_session"] = start or next_session
    state["pending"] = next_session
    status = "running" if state["engine"]["last_session"] else "ready"
    _publish(state, status, f"已锁定 {next_session} 的开盘信号，待盘后官方行情结算")


def run_once():
    DIRECTORY.mkdir(mode=0o700, parents=True, exist_ok=True)
    with ProcessLock(DIRECTORY / "run.lock"):
        state = _read_state() or _new_state()
        if "tech_settings" not in state:
            from etf.etf_map import execution_config

            state["tech_settings"] = execution_config()["tech"]
            if state["start_session"]:
                atomic_json(STATE, state)
        try:
            _advance(state)
        except (ValueError, OSError) as exc:
            LOG.warning("Pitch forward data waiting: %s", exc)
            _publish(state, "waiting_data", str(exc))
        except Exception:
            LOG.exception("Pitch forward cycle failed")
            _publish(state, "waiting_data", "前瞻模拟更新失败，等待下次本地重试")
            raise


def public_status():
    state = _read_state()
    if state is None:
        return {"ok": True, "status": "not_started", "message": "前瞻模拟尚未初始化"}
    from backtest.pitch_forward_engine import summary

    current = state["signals"].get(state["pending"])
    signal = {key: current[key] for key in ("signal_date", "trade_date", "committed_at")} if current else None
    if signal is not None:
        signal["count"] = len(current["orders"])
        signal["atr_ready"] = sum(order["atr"] is not None for order in current["orders"])
    return {
        "ok": True, **summary(state["engine"]),
        **{key: state[key] for key in ("status", "message", "as_of", "start_session", "readiness")},
        "next_signal": signal,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("run-once", "status"))
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    if args.command == "run-once":
        run_once()
    else:
        print(json.dumps(public_status(), ensure_ascii=False, allow_nan=False))
