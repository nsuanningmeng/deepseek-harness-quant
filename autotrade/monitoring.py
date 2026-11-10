"""本地结构化日志、持久健康快照；不发送账户数据到第三方。"""
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def atomic_json(path, payload):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + "." + uuid4().hex + ".tmp")
    try:
        with temporary.open("x", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, default=str, allow_nan=False)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def event(runtime, name, **fields):
    runtime = Path(runtime)
    runtime.mkdir(parents=True, exist_ok=True)
    payload = {"time": utc_now(), "event": name, **fields}
    with (runtime / "events.jsonl").open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False, default=str, allow_nan=False) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    return payload
