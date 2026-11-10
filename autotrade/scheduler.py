"""Asia/Shanghai 盘后调度；显式交易日历，失败下个轮询重试同一订单 ID。"""
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .data import load_day
from .providers import provider_for
from .pipeline import prepare_and_run
from .monitoring import atomic_json, event, utc_now

SHANGHAI = timezone(timedelta(hours=8))


def is_session(config, day):
    if config["data"]["provider"] != "files":
        snapshot = Path(config["data"]["directory"]) / f"{day}.json"
        if snapshot.exists():
            if load_day(snapshot).date != day:
                raise ValueError("cached session date mismatch")
            return True
        return provider_for(config).is_trading_day(day)
    path = Path(config["data"]["directory"]) / "calendar.json"
    calendar = json.loads(path.read_text(encoding="utf-8"))
    if not calendar["start"] <= day <= calendar["end"]:
        raise ValueError("calendar coverage expired; refresh before scheduling")
    sessions = calendar["sessions"]
    if not isinstance(sessions, list) or len(set(sessions)) != len(sessions):
        raise ValueError("invalid trading calendar")
    return day in sessions


def tick(config, now=None):
    now = now or datetime.now(SHANGHAI)
    if now.tzinfo is None:
        raise ValueError("scheduler clock must be timezone aware")
    now = now.astimezone(SHANGHAI)
    if now.strftime("%H:%M") < config["schedule"]["after_close"]:
        return {"state": "WAITING_FOR_CLOSE", "date": now.date().isoformat()}
    day = now.date().isoformat()
    if not is_session(config, day):
        return {"state": "MARKET_CLOSED", "date": day}
    return prepare_and_run(config, day)


def run(config, once=False):
    while True:
        try:
            result = tick(config)
            atomic_json(Path(config["runtime_dir"]) / "scheduler.json", {"ok": True, "checked_at": utc_now(), "result": result})
            if once:
                return result
        except Exception as exc:
            event(config["runtime_dir"], "scheduler_failed", error_type=type(exc).__name__)
            atomic_json(Path(config["runtime_dir"]) / "scheduler.json", {"ok": False, "checked_at": utc_now(), "error_type": type(exc).__name__})
            if once:
                raise
        time.sleep(config["schedule"]["poll_seconds"])
