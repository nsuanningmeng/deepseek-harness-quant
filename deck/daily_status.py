"""Read-only completeness evidence for every existing daily-bar database.

The expensive query runs once per physical input version in a background worker.
No DailyCache constructor, schema creation, database writes, or sampled readiness.
"""
import os
import sqlite3
from datetime import date
from pathlib import Path

from deck.background_snapshot import BackgroundSnapshot

BASE = Path(__file__).resolve().parent.parent
_snapshot = BackgroundSnapshot("daily-completeness")


def cache_dir():
    import yaml
    override = os.environ.get("LWQUANT_CACHE_DIR")
    if override:
        return Path(override).expanduser().resolve()
    config_path = Path(os.environ.get("LWQUANT_RESEARCH_CONFIG", BASE / "config/research.yaml"))
    try:
        config = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
        value = config.get("cache_dir", "data/cache")
    except (OSError, ValueError, TypeError):
        value = "data/cache"
    path = Path(value).expanduser()
    return path if path.is_absolute() else BASE / path


def databases():
    root = cache_dir()
    return tuple(path for path in (root / "bars.db", root / "bars_incr.db", *sorted(root.glob("bars_incr_*.db"))) if path.is_file())


def file_versions(paths):
    out = []
    for path in paths:
        try:
            stat = path.stat()
            out.append((str(path), stat.st_mtime_ns, stat.st_size))
        except OSError:
            out.append((str(path), None, None))
    return tuple(out)


def source_version(paths=None):
    sources = databases() if paths is None else paths
    return file_versions([part for path in sources for part in (path, Path(str(path) + "-wal"))])


def scan_complete_date(paths, minimum_symbols):
    """Count all observed dates; deduplicate across increments before certifying a date."""
    if not paths:
        return {"ready": False, "state": "missing_data", "date": None, "reason": "尚无真实日线数据库"}
    counts = {}
    by_date = {}
    condition = "(code GLOB '[0-9][0-9][0-9][0-9][0-9][0-9].SH' OR code GLOB '[0-9][0-9][0-9][0-9][0-9][0-9].SZ')"
    for path in paths:
        con = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=3)
        try:
            rows = con.execute("SELECT date,COUNT(DISTINCT code) FROM daily_bar WHERE " + condition + " GROUP BY date").fetchall()
        finally:
            con.close()
        counts[str(path)] = dict(rows)
        for day, count in rows:
            if not isinstance(day, str) or date.fromisoformat(day).isoformat() != day:
                raise ValueError("日线包含无效内容日期，无法核验")
            if day > date.today().isoformat():
                raise ValueError("日线包含未来内容日期，无法核验")
            by_date.setdefault(day, []).append(count)
    latest_observed = max(by_date, default=None)
    for day in sorted(by_date, reverse=True):
        if sum(by_date[day]) < minimum_symbols:
            continue
        # A complete single source is already sufficient proof, without double-counting.
        observed_minimum = max(by_date[day])
        exact_count = observed_minimum if len(paths) == 1 else None
        if observed_minimum < minimum_symbols:
            codes = set()
            for path in paths:
                if counts[str(path)].get(day, 0) == 0:
                    continue
                con = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=3)
                try:
                    codes.update(row[0] for row in con.execute("SELECT DISTINCT code FROM daily_bar WHERE date=? AND " + condition, (day,)))
                finally:
                    con.close()
            exact_count = observed_minimum = len(codes)
        if observed_minimum >= minimum_symbols:
            return {"ready": True, "state": "ready", "date": day, "latest_observed": latest_observed,
                    "minimum_symbols": minimum_symbols, "observed_minimum": observed_minimum,
                    "symbol_count": exact_count, "source_count": len(paths),
                    "reason": "已逐库核验内容日期与股票覆盖，增量重复代码不重复计数"}
    return {"ready": False, "state": "missing_data", "date": None,
            "latest_observed": latest_observed, "minimum_symbols": minimum_symbols,
            "reason": "尚无股票覆盖达到完整性门槛的日线日期；未把局部下载当作完整行情"}


def latest_complete_status(minimum_symbols=4000, *, wait=False):
    paths = databases()
    version = (source_version(paths), minimum_symbols)
    result = _snapshot.read(version, lambda: scan_complete_date(paths, minimum_symbols), wait=wait)
    data = result["data"] or {}
    if not result["current"]:
        return {**data, "ready": False, "state": result["state"], "stale": result["stale"],
                "loading": result["loading"], "date": None, "previous_date": data.get("date"),
                "reason": "正在后台核验完整日线日期与股票覆盖" if result["loading"] else "日线内容核验失败",
                "error": result["error"]}
    return {**data, "stale": False, "loading": False, "checked_at": result["updated_at"]}
