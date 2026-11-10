"""只准备行情，不执行任何订单；部分范围失败后可复用已完整封存日期。"""
from pathlib import Path
import json

from .data import _date, load_day
from .locking import ProcessLock
from .monitoring import atomic_json, event
from .providers import provider_for


def bootstrap_market(config, start_date, end_date, provider=None):
    """按明确交易日历补齐快照；返回不含原始行情的运行摘要。"""
    _date(start_date)
    _date(end_date)
    if start_date > end_date:
        raise ValueError("start date is after end date")
    directory = Path(config["data"]["directory"])
    runtime = Path(config["runtime_dir"])
    provider = provider or provider_for(config)
    with ProcessLock(directory / "bootstrap.lock"):
        if hasattr(provider, "prepare"):
            provider.prepare(start_date, end_date)
        calendar = provider.calendar(start_date, end_date)
        sessions = calendar["sessions"]
        if (calendar["start"] != start_date or calendar["end"] != end_date
                or sessions != sorted(set(sessions))
                or any(not start_date <= _date(day) <= end_date for day in sessions)):
            raise ValueError("invalid provider calendar")
        if not sessions:
            raise ValueError("requested interval contains no trading sessions")
        merged_calendar = calendar
        calendar_path = directory / "calendar.json"
        if calendar_path.exists():
            old = json.loads(calendar_path.read_text(encoding="utf-8"))
            old_start, old_end = _date(old["start"]), _date(old["end"])
            old_sessions = old["sessions"]
            if (old_start > old_end or not isinstance(old_sessions, list)
                    or old_sessions != sorted(set(old_sessions))
                    or any(not old_start <= _date(day) <= old_end for day in old_sessions)):
                raise ValueError("existing calendar is invalid")
            union_start, union_end = min(old_start, start_date), max(old_end, end_date)
            # Ask the provider to verify every day in the union, including any gap.
            # A new short catch-up range must not erase earlier calendar coverage.
            merged_calendar = provider.calendar(union_start, union_end)
            merged_sessions = merged_calendar["sessions"]
            if (merged_calendar["start"] != union_start or merged_calendar["end"] != union_end
                    or merged_sessions != sorted(set(merged_sessions))
                    or any(not union_start <= _date(day) <= union_end for day in merged_sessions)
                    or [day for day in merged_sessions if old_start <= day <= old_end] != old_sessions
                    or [day for day in merged_sessions if start_date <= day <= end_date] != sessions):
                raise ValueError("calendar history changed or union coverage is incomplete")
        created, reused = [], []
        previous = None
        for day in sessions:
            exists = (directory / f"{day}.json").exists()
            snapshot = provider.update(day, directory)
            expected_universe = set(config["data"]["universe"] or snapshot.bars)
            if snapshot.date != day or set(snapshot.bars) != expected_universe:
                raise ValueError("downloaded snapshot identity/universe differs")
            sealed = load_day(directory / f"{day}.json")
            if sealed != snapshot:
                raise ValueError("snapshot was not atomically persisted")
            if previous and (snapshot.previous_session != previous.date or previous.next_session != day):
                raise ValueError("downloaded snapshots are not consecutive sessions")
            previous = snapshot
            (reused if exists else created).append(day)
            event(runtime, "market_snapshot_ready", session=day, reused=exists,
                  provider=config["data"]["provider"], symbols=len(snapshot.bars))
        # Calendar only covers the explicitly verified interval, never inferred weekdays.
        atomic_json(calendar_path, merged_calendar)
        result = {"ok": True, "provider": config["data"]["provider"], "start": start_date,
                  "end": end_date, "sessions": sessions, "created": created, "reused": reused,
                  "symbols": len(previous.bars), "orders_submitted": 0}
        atomic_json(runtime / "bootstrap-report.json", result)
        return result
