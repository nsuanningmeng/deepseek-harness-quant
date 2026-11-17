# -*- coding: utf-8 -*-
"""deck/system_live.py — 实时系统状态聚合（2026-08-10 用户需求 1）

供 /api/system_live 返回实时 JSON（dashboard_live.html 每 5 秒轮询）：
  1. databases: 各数据库心跳（可连接 + 最近交易日）
  2. dev_auto: 最近一次运行日志尾部（正在干什么）
  3. scheduled: 计划任务下次运行时间
  4. api_health: 各 API 端点存活
  5. deck: 自身状态
"""
import json
import hashlib
import math
import os
import re
import socket
import subprocess
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent

DBS = {
    "bars": r"data/cache/bars.db",
    "finance": r"data/cache/finance.db",
    "finance_quality": r"data/cache/finance_quality.db",
    "minute": r"data/cache/minute.db",
    "hist_mv": r"data/cache/hist_mv.db",
}

SCHEDULED_TASKS = [
    ("LWQuant-DevDriver", "每 4h 巡检"),
    ("LWQuant-AfterCloseScan", "17:35 盘后机会扫描"),
    ("LWQuant-FactorDaily", "19:15 因子池"),
    ("LWQuant-DailyPipeline", "18:30 每日全链"),
    ("LWQuant-DeckGuard", "每 30min 守护"),
]


_db_cache = {"ts": 0, "data": None}
_db_probe_pool = ThreadPoolExecutor(max_workers=5, thread_name_prefix='research-health')
_db_probe_jobs = {}
_db_probe_lock = threading.Lock()
_DB_PROBE_RETRY_SECONDS = 5.0


def _minute_job_status():
    try:
        from data.minute_readiness import public_status
        status = public_status()
        if (status.get('research_runtime_dir')
                and Path(status['research_runtime_dir']).resolve() == (BASE / 'runtime/research').resolve()):
            return status
        return {}
    except Exception:
        return {}


def _probe_one(name, path, minute_scope=None):
    """Read the actual consumer table; an empty metadata table is not data readiness."""
    import sqlite3
    t0 = time.time()
    try:
        # Minute counts must include committed WAL rows in the selected job.
        uri = path.resolve().as_uri() + ('?mode=ro' if name == 'minute' else '?mode=ro&immutable=1')
        con = sqlite3.connect(uri, uri=True, timeout=0.2)
        tabs = con.execute(
            "SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        expected = {"bars": ("daily_bar", "date"), "finance": ("finance_report", "period"),
                    "finance_quality": ("quality", "period"), "minute": ("minute_1m", "trade_time"),
                    "hist_mv": ("hist_mv", "month")}
        table, date_col = expected[name]
        minute_5m = name == "minute" and "minute_5m" in {row[0] for row in tabs}
        if minute_5m or name == 'minute' and minute_scope is not None:
            table = "minute_5m"
        if table not in {row[0] for row in tabs}:
            con.close()
            return {name: {"online": True, "ready": False, "state": "missing_schema", "rows": 0,
                           "tables": len(tabs), "reason": f"缺少业务表 {table}",
                           "ms": round((time.time() - t0) * 1000)}}
        where, params = '1=1', ()
        if name == 'minute' and minute_scope is not None:
            from deck.minute_scope import sql_filter
            where, params = sql_filter(minute_scope)
        n, first, last = con.execute(f'SELECT COUNT(*), MIN("{date_col}"), MAX("{date_col}") FROM "{table}" WHERE {where}', params).fetchone()
        valid_rows = n
        if name in ("bars", "minute"):
            valid_rows = con.execute(f'SELECT COUNT(*) FROM "{table}" WHERE ({where}) AND close IS NOT NULL AND close > 0', params).fetchone()[0]
        symbols = con.execute(f'SELECT COUNT(DISTINCT code) FROM "{table}" WHERE {where}', params).fetchone()[0] if name == 'minute' else None
        total_cache_rows = con.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0] if name == 'minute' else None
        con.close()
        date_valid = _content_date(str(last) + "-01" if name == "hist_mv" and len(str(last)) == 7 else last)
        ready = bool(valid_rows and date_valid and (name != 'minute' or valid_rows == n))
        row = {"online": True, "ready": ready, "state": "ready" if ready else "missing_data",
                       "rows": n, "valid_rows": valid_rows, "tables": len(tabs), "table": table, "first_date": first, "data_date": last,
                       "reason": "已读取业务表内容；完整覆盖范围见初始化清单" if ready else "业务表存在但尚无有效业务数值或内容日期",
                       "ms": round((time.time() - t0) * 1000)}
        if name == "minute":
            row.update(freq="5min" if minute_5m else "1min", scope="selected_job" if minute_scope else "unscoped_cache",
                       covered_symbols=symbols, total_cache_rows=total_cache_rows)
            if minute_scope:
                row.update(job_id=minute_scope[0], window_start=minute_scope[1], window_end=minute_scope[2],
                           universe_fingerprint=minute_scope[3])
        return {name: row}
    except Exception as e:
        if "con" in locals():
            con.close()
        return {name: {"online": False, "ready": False, "state": "missing_data", "error": str(e)[:50],
                       "probe_retryable": True, "reason": "业务表读取失败，冷却后重新核验；尚未判定就绪",
                       "ms": round((time.time() - t0) * 1000)}}


def _db_health() -> dict:
    """Verify each actual DB version once in background; HTTP never waits for scans.

    File metadata only invalidates cached verification. It never establishes readiness.
    A currently-mutating initialization is explicitly pending, not inferred healthy.
    """
    from data.cache import CACHE_DIR
    bootstrap = _research_document('bootstrap-state.json')
    if bootstrap.get('status') == 'running':
        return {name: {'online': (CACHE_DIR / Path(path).name).exists(), 'ready': False,
                       'state': 'initializing', 'rows': None,
                       'reason': '数据初始化正在写入，完成后对实际业务表核验'}
                for name, path in DBS.items()}
    out = {}
    minute_job = _minute_job_status()
    minute_scope, minute_scope_error = None, None
    if minute_job.get('database_path'):
        from deck.minute_scope import query_scope
        try:
            minute_scope = query_scope(minute_job)
        except (ValueError, TypeError):
            minute_scope_error = '分钟任务日期或实际股票范围尚未核验，不能使用全库计数代替'
    with _db_probe_lock:
        for name, relative in DBS.items():
            path = (Path(minute_job['database_path']) if name == 'minute' and minute_job.get('database_path')
                    else CACHE_DIR / Path(relative).name)
            if name == 'minute' and minute_scope_error:
                out[name] = {'online': path.exists(), 'ready': False, 'state': 'incomplete', 'rows': None,
                             'reason': minute_scope_error}
                continue
            if name == 'minute' and minute_scope is not None and not all(
                    minute_job.get(key) is True for key in ('deep_verified','cache_verified','current_date_ready')):
                state = 'failed' if minute_job.get('verification_status') == 'failed' else 'probing' if minute_job.get('verification_status') == 'verifying' else 'initializing'
                out[name] = {'online': path.exists(), 'ready': False, 'state': state, 'rows': None,
                             'total_cache_rows': None, 'database_path': str(path), 'source': minute_job.get('source'),
                             'completed_symbols': minute_job.get('completed_symbols'),
                             'expected_symbols': minute_job.get('expected_symbols'),
                             'reason': '分钟任务尚未通过当前窗口强验；显示回执进度，不扫描正在写入的行情库'}
                continue
            try:
                stat = path.stat()
                version = (str(path.resolve()), stat.st_mtime_ns, stat.st_size)
                if name == 'minute':
                    wal = Path(str(path) + '-wal')
                    wal_stat = wal.stat() if wal.exists() else None
                    version += (minute_scope, (wal_stat.st_mtime_ns, wal_stat.st_size) if wal_stat else None)
            except OSError:
                out[name] = {'online': False, 'ready': False, 'state': 'missing_data', 'reason': '数据库尚不存在'}
                continue
            job = _db_probe_jobs.get(name)
            retry_due = bool(job and job['future'].done() and job.get('retry_at') is not None
                             and time.monotonic() >= job['retry_at'])
            if not job or job['version'] != version or retry_due:
                if job and not job['future'].done():
                    job['future'].cancel()
                args = (name, path, minute_scope) if name == 'minute' else (name, path)
                job = {'version': version, 'future': _db_probe_pool.submit(_probe_one, *args)}
                _db_probe_jobs[name] = job
            if job['future'].done():
                try:
                    out.update({key: dict(value) for key, value in job['future'].result().items()})
                except Exception:
                    out[name] = {'online': False, 'ready': False, 'state': 'failed', 'probe_retryable': True,
                                 'reason': '实际业务表核验失败，冷却后重新核验'}
                if out.get(name, {}).get('probe_retryable') is True:
                    job.setdefault('retry_at', time.monotonic() + _DB_PROBE_RETRY_SECONDS)
                    out[name]['retry_in_seconds'] = round(max(0, job['retry_at'] - time.monotonic()), 1)
            else:
                out[name] = {'online': True, 'ready': False, 'state': 'probing',
                             'reason': '正在后台读取实际业务表；尚未判定就绪'}
    if out.get('minute', {}).get('ready'):
        row = out['minute']
        if minute_job.get('database_path'):
            verified = bool(minute_job.get('current_date_ready') and minute_job.get('cache_verified')
                            and minute_job.get('deep_verified') is True
                            and row.get('rows') == minute_job.get('rows')
                            and row.get('covered_symbols') == minute_job.get('covered_symbols')
                            and minute_job.get('completed_symbols') == minute_job.get('expected_symbols') == 300)
            row.update(ready=verified, state='ready' if verified else 'incomplete',
                       source=minute_job.get('source'), database_path=minute_job.get('database_path'),
                       reason='当前300只历史5分钟覆盖与实际数据库已核验' if verified else '历史5分钟仍在补齐或等待当前日期和股票范围核验')
        else:
            manifest = _research_document('bootstrap-manifest.json')
            target = (manifest.get('coverage',{}).get('daily') or {}).get('end')
            if manifest.get('stages',{}).get('minute') != 'complete' or manifest.get('stage_dates',{}).get('minute') != target:
                row.update(ready=False, state='rate_limited' if bootstrap.get('status') == 'rate_limited' else 'incomplete',
                           reason='历史分钟线仅部分下载，全部股票覆盖尚未核验；接口额度限制见初始化状态')
    _db_cache['ts'] = time.time()
    _db_cache["data"] = out
    return out


def _dev_auto_tail(n=8) -> list:
    """dev_auto 最近运行日志尾部（显示系统在干什么）"""
    for p in (BASE / "logs" / "dev_auto.log", BASE / "logs" / "dev_auto_console.log"):
        if p.exists():
            try:
                lines = p.read_text(encoding="utf-8", errors="replace").strip().splitlines()
                return lines[-n:]
            except Exception:
                pass
    return []


_sched_cache = {"ts": 0, "data": None}


def _paperlive_job(pid, config_path, now):
    """Read an already-running live-paper loop; never import runtime or its ledger."""
    from paperlive.config import load_config

    job = {"kind": "paperlive", "provider": "腾讯实时行情", "pid": pid, "running": True,
           "config": config_path.name, "mode": "paper"}
    try:
        cfg = load_config(config_path)
        job['provider'] = cfg['market']['source_label']
        runtime = Path(cfg["runtime_dir"])
        state_path = runtime / "state.json"
        state = json.loads(state_path.read_text(encoding="utf-8")) if state_path.is_file() else {}
        market = state.get("market") if isinstance(state.get("market"), dict) else {}
        checked_at = state.get("updated_at")
        checked, age = None, None
        if isinstance(checked_at, str):
            checked = datetime.fromisoformat(checked_at)
            if checked.tzinfo is not None and checked <= now:
                age = round((now - checked).total_seconds())
            else:
                checked = None
        pid_matches = None
        pid_file = runtime / "runner.pid"
        if pid_file.is_file():
            try:
                value = json.loads(pid_file.read_text(encoding="utf-8"))
                recorded_pid = value.get("pid") if isinstance(value, dict) else value
                pid_matches = type(recorded_pid) is int and recorded_pid == pid
            except (ValueError, OSError):
                pid_matches = False
        calendar_verified = market.get("calendar_verified") is True
        open_now = (market.get("open") is True and calendar_verified
                    and market.get("date") == now.date().isoformat())
        interval = cfg["market"]["refresh_seconds"] if open_now else cfg["market"].get("closed_refresh_seconds", cfg["market"]["refresh_seconds"])
        stale = checked is None or age > max(cfg["market"]["max_quote_age_seconds"], 3 * interval)
        next_check, schedule_kind = None, "paperlive_open"
        if open_now and not stale:
            next_check = max(now, checked + timedelta(seconds=cfg["market"]["refresh_seconds"]))
            schedule_kind = "paperlive_refresh"
        elif calendar_verified and isinstance(market.get("next_open"), str):
            candidate = datetime.fromisoformat(market["next_open"])
            if candidate.tzinfo is not None and candidate > now:
                next_check = candidate
        count = market.get("quote_count")
        quote_count = count if type(count) is int and count >= 0 else len(state.get("quotes", []))
        count = market.get("universe_count")
        universe_count = count if type(count) is int and count >= 0 else len(state.get("universe", []))
        phase = market.get("phase") if isinstance(market.get("phase"), str) else "实时模拟盘启动中"
        status = "STARTING" if not state else "ERROR" if market.get("error") else "MARKET_OPEN" if open_now else "MARKET_CLOSED"
        job.update({
            "checked_at": checked_at, "status": status, "status_label": phase,
            "scheduler_ok": bool(state) and not market.get("error") and not stale and pid_matches is not False,
            "heartbeat_age_seconds": age, "heartbeat_stale": stale, "pid_file_matches": pid_matches,
            "has_ledger": (runtime / "paper.sqlite3").is_file(),
            "market_open": open_now, "market_ready": quote_count > 0 and calendar_verified and not market.get("error"),
            "quote_count": quote_count, "universe_count": universe_count,
            "calendar_verified": calendar_verified, "calendar_date": market.get("date"),
            "is_trading_day": True if open_now else False if phase == "非交易日" else None,
            "next_check_is_trading_day": calendar_verified and next_check is not None,
            "next_check": next_check.isoformat(timespec="seconds") if next_check else None,
            "mins_left": max(0, round((next_check - now).total_seconds() / 60)) if next_check else None,
            "schedule_kind": schedule_kind, "paused": state.get("paused") is True,
        })
        if market.get("error"):
            job["error"] = str(market["error"])[:250]
    except Exception as exc:
        job.update({"status": "STATUS_UNAVAILABLE", "status_label": "实时模拟盘状态暂不可读",
                    "error_type": type(exc).__name__})
    return job


def _autotrade_status() -> dict:
    """只读发现本项目模拟盘进程及已落盘状态，不初始化账户或请求行情。"""
    out = {"running": False, "jobs": [], "message": "未发现本项目的自动模拟盘调度进程"}
    try:
        import psutil
        if str(BASE) not in sys.path:
            sys.path.insert(0, str(BASE))
        from autotrade.config import load_config
        from autotrade.data import load_day
    except ImportError:
        out["message"] = "进程检查依赖不可用，无法确认模拟盘运行状态"
        out["discovery_available"] = False
        return out
    out["discovery_available"] = True
    now = datetime.now(timezone(timedelta(hours=8)))
    for proc in psutil.process_iter(["pid", "cmdline", "cwd"]):
        try:
            # psutil caches Process instances; another concurrent iterator may replace .info.
            info = dict(proc.info)
            pid = info.get("pid", proc.pid)
            args = info.get("cmdline") or []
            if "-m" not in args:
                continue
            module = args.index("-m") + 1
            module_name = args[module] if module < len(args) else None
            cli_args, skip_config = [], False
            for arg in args[module + 1:]:
                if skip_config:
                    skip_config = False
                elif arg == "--config":
                    skip_config = True
                elif not arg.startswith("--config="):
                    cli_args.append(arg)
            if not ((module_name == "autotrade" and "schedule" in args[module + 1:])
                    or (module_name == "paperlive" and cli_args == ["run"])):
                continue
            if not info.get("cwd"):
                continue
            cwd = Path(info["cwd"]).resolve()
            config_arg = next((arg.split("=", 1)[1] for arg in args if arg.startswith("--config=")), None)
            if "--config" in args:
                config_arg = args[args.index("--config") + 1]
            default_config = "paperlive.yaml" if module_name == "paperlive" else "autotrade.yaml.example"
            config_path = (cwd / config_arg).resolve() if config_arg else BASE / "config" / default_config
            if cwd != BASE.resolve() and (not config_arg or not config_path.is_relative_to(BASE.resolve())):
                continue
            if module_name == "paperlive":
                out["jobs"].append(_paperlive_job(pid, config_path, now))
                continue
            job = {"pid": pid, "running": True, "config": config_path.name}
            try:
                cfg = load_config(config_path)
                job.update({"provider": cfg["data"]["provider"], "mode": cfg["mode"]})
                runtime = Path(cfg["runtime_dir"])
                market = Path(cfg["data"]["directory"])
                scheduler_path = runtime / "scheduler.json"
                saved = json.loads(scheduler_path.read_text(encoding="utf-8")) if scheduler_path.exists() else {}
                result = saved.get("result") if isinstance(saved.get("result"), dict) else {}
                status = "ERROR" if saved.get("ok") is False else result.get("state", "RUNNING")
                if not isinstance(status, str):
                    status = "RUNNING"
                checked_at = saved.get("checked_at")
                age = None
                if isinstance(checked_at, str):
                    checked = datetime.fromisoformat(checked_at)
                    if checked.tzinfo is not None:
                        age = max(0, round((now - checked).total_seconds()))
                hour, minute = map(int, cfg["schedule"]["after_close"].split(":"))
                after_close = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
                next_check = after_close if now < after_close else now + timedelta(seconds=cfg["schedule"]["poll_seconds"])
                once = "--once" in args[module + 1:]
                snapshots = sorted(p for p in market.glob("????-??-??.json") if p.stem <= now.date().isoformat())
                warmup = cfg["strategy"]["lookback"]
                market_ready = False
                if len(snapshots) >= warmup:
                    days = [load_day(path) for path in snapshots[-warmup:]]
                    universe = set(cfg["data"]["universe"])
                    market_ready = (all(not universe or set(day.bars) == universe for day in days)
                                    and all(a.next_session == b.date and b.previous_session == a.date
                                            for a, b in zip(days, days[1:])))
                calendar_verified, is_trading_day = False, None
                calendar_path = market / "calendar.json"
                if calendar_path.exists():
                    calendar = json.loads(calendar_path.read_text(encoding="utf-8"))
                    check_date = next_check.date().isoformat()
                    if calendar.get("start", "") <= check_date <= calendar.get("end", ""):
                        calendar_verified = isinstance(calendar.get("sessions"), list)
                        if calendar_verified:
                            is_trading_day = check_date in calendar["sessions"]
                job.update({
                    "provider": cfg["data"]["provider"], "mode": cfg["mode"],
                    "checked_at": checked_at, "status": status,
                    "status_label": {"WAITING_FOR_CLOSE": "等待盘后检查", "MARKET_CLOSED": "当日非交易日",
                                     "ERROR": "最近一次调度失败", "RUNNING": "调度进程运行中"}.get(status, status),
                    "scheduler_ok": saved.get("ok"), "heartbeat_age_seconds": age,
                    "heartbeat_stale": age is not None and age > max(180, 3 * cfg["schedule"]["poll_seconds"]),
                    "has_ledger": (runtime / "paper.sqlite3").is_file(),
                    "market_ready": market_ready, "market_sessions": len(snapshots), "warmup_sessions": warmup,
                    "last_market_date": snapshots[-1].stem if snapshots else None,
                    "after_close": cfg["schedule"]["after_close"],
                    "next_check": None if once else next_check.isoformat(timespec="seconds"),
                    "mins_left": None if once else max(0, round((next_check - now).total_seconds() / 60)),
                    "calendar_verified": calendar_verified, "is_trading_day": is_trading_day,
                    "schedule_kind": "paper_after_close_check" if now < after_close else "paper_poll",
                })
                if saved.get("ok") is False:
                    job["error_type"] = saved.get("error_type", "UnknownError")
            except Exception as exc:
                job.update({"status": "STATUS_UNAVAILABLE", "status_label": "模拟盘状态暂不可读",
                            "error_type": type(exc).__name__})
            out["jobs"].append(job)
        except (psutil.Error, IndexError, OSError, ValueError):
            continue
    out["jobs"].sort(key=lambda job: job["pid"])
    out["running"] = bool(out["jobs"])
    if out["running"]:
        out["message"] = "已发现自动模拟盘调度；交易日与行情是否就绪以实际校验为准"
    return out


def _autotrade_schedule_entries(status=None) -> list:
    status = status if status is not None else _autotrade_status()
    return [{"name": f"{job.get('kind', 'autotrade')}-{job['pid']}",
             "desc": (("实时模拟盘行情刷新" if job.get("schedule_kind") == "paperlive_refresh" else "实时模拟盘下次连续竞价")
                      if job.get("kind") == "paperlive" else "自动模拟盘盘后检查（交易日待校验）"
                      if not job.get("calendar_verified") else "自动模拟盘检查（已读取本地交易日历）"),
             "next": job["next_check"], "mins_left": job.get("mins_left"),
             "kind": job["schedule_kind"], "calendar_verified": job.get("calendar_verified", False),
             "is_trading_day": job.get("next_check_is_trading_day") if job.get("kind") == "paperlive" else job.get("is_trading_day")}
            for job in status["jobs"] if job.get("next_check")]


def _scheduled(autotrade=None) -> dict:
    """运行中的模拟盘计划 + Windows 已注册任务；其他平台不执行 schtasks。"""
    out = {entry["name"]: entry for entry in _autotrade_schedule_entries(autotrade)}
    try:
        from data.research_service import public_status
        research = public_status()
        out['research-service'] = {
            'name': 'research-service', 'desc': '研究数据与分析产物定期核验',
            'kind': 'research_update_check', 'next': research.get('next_run_at'),
            'running': research.get('service_alive', False), 'status': research.get('status'),
            'message': research.get('message'),
        }
    except (OSError, ValueError):
        out['research-service'] = {'name': 'research-service', 'desc': '研究数据更新',
                                   'running': False, 'status': 'unavailable'}
    if os.name != "nt":
        return out
    now = time.time()
    if _sched_cache["data"] is not None and now - _sched_cache["ts"] < 300:
        return {**out, **_sched_cache["data"]}
    windows = {}
    for name, desc in SCHEDULED_TASKS:
        try:
            r = subprocess.run(["schtasks", "/Query", "/TN", name, "/FO", "LIST", "/V"],
                               capture_output=True, text=True, errors="replace",
                               encoding="gbk", timeout=8)
            m = re.search(r"下次运行时间:\s*(.+)", r.stdout)
            windows[name] = {"desc": desc, "next": m.group(1).strip() if m else "—"}
        except Exception as e:
            windows[name] = {"desc": desc, "next": f"ERR {str(e)[:30]}"}
    _sched_cache["ts"] = now
    _sched_cache["data"] = windows
    return {**out, **windows}


def _research_document(name):
    """Only public progress documents are read here; never provider credentials."""
    path = BASE / "runtime" / "research" / name
    if not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {"status": "failed", "last_error": f"{name} 无法解析"}


def _safe_health_text(value):
    text = str(value or "")
    text = re.sub(r"(?i)(bearer\s+)\S+", r"\1[已隐藏]", text)
    text = re.sub(r"(?i)((?:token|api[_-]?key|secret|password)\s*[=:]\s*)[^\s,;]+", r"\1[已隐藏]", text)
    return text[:500]


def _content_date(value):
    value = str(value or "")[:10]
    if len(value) == 8 and value.isdigit():
        value = f"{value[:4]}-{value[4:6]}-{value[6:]}"
    try:
        datetime.strptime(value, "%Y-%m-%d")
        return value
    except ValueError:
        return ""


def _product_schema(name, data):
    """Validate consumer schemas, rather than treating any fresh JSON as a result."""
    if not isinstance(data, dict) or not data:
        return False, 0, "空内容或未知 JSON 结构"
    if data.get("error") or data.get("err") or data.get("ok") is False:
        return False, 0, _safe_health_text(data.get("error") or data.get("err") or "产物报告执行失败")
    if name == 'signal' and data.get('kind') == 'research_summary':
        collections = [data.get(key) for key in ('watch','candidate','decision','pitch')]
        if any(not isinstance(value,list) for value in collections):
            return False, 0, '研究信号汇总缺少实际观察、候选、决策或Pitch列表'
        rows = sum(map(len,collections))
        if any(not isinstance(row,dict) or not row.get('code') for value in collections for row in value):
            return False, rows, '研究信号结果缺少证券标识'
        regime = data.get('regime') or {}
        if (not _content_date(data.get('date')) or not data.get('source_files')
                or not isinstance(regime.get('pos'),(int,float)) or not 0 <= regime['pos'] <= 1
                or (data.get('audit') or {}).get('blocked') is not False):
            return False, rows, '研究信号缺少行情日期、来源或已验证审计/择时状态'
        return True, rows, ''
    arrays = {"pitch_v2": ("pitch",), "opp_pool": ("opportunities",), "beneish": ("results",),
              "pitch_track": ("entries",), "signal": ("hold_plan", "buy_plan", "signals"),
              "audit": ("items",), "factor_pool": ("factors",), "pool_layers": ("watch", "candidate", "decision")}
    if name in arrays:
        values = [data[k] for k in arrays[name] if isinstance(data.get(k), list)]
        if not values:
            return False, 0, "缺少业务结果列表：" + "/".join(arrays[name])
        rows = sum(len(value) for value in values)
        identity = "id" if name == "audit" else "name" if name == "factor_pool" else "code"
        if any(not isinstance(row, dict) or not (row.get(identity) or identity == "code" and row.get("symbol")) for value in values for row in value):
            return False, rows, "结果行缺少业务标识，不能把占位行视为真实样本"
        if name == "audit" and (not rows or data.get("blocked") or data.get("n_fail", 0)):
            return False, rows, _safe_health_text(data.get("block_reason") or "审计未完成或存在失败项")
        if name == "beneish" and not any(isinstance(row, dict) and row.get("m_score") is not None for row in data["results"]):
            return False, rows, "缺少可计算的财务风险样本"
        return True, rows, ""
    if name == "regime":
        rows = [v for k, v in data.items() if re.fullmatch(r"\d{4}-\d{2}", str(k)) and isinstance(v, dict) and isinstance(v.get("pos"), (float, int))]
        for month, row in data.items():
            if not re.fullmatch(r"\d{4}-\d{2}", str(month)):
                continue
            signal = str(row.get('signal_month', ''))
            if not re.fullmatch(r"\d{4}-\d{2}", signal):
                return False, len(rows), "缺少仓位信号生成月份"
            year, mon = map(int, signal.split('-'))
            expected = f"{year + mon // 12:04d}-{mon % 12 + 1:02d}"
            if month != expected or not 0 <= row['pos'] <= 1:
                return False, len(rows), "仓位生效月或比例不符合已验证的次月执行规则"
        return bool(rows), len(rows), "缺少按月仓位序列" if not rows else ""
    if name == "traffic_light":
        valid = all(data.get(k) is not None for k in ("state", "asof", "ma6", "ma12"))
        return valid, int(valid), "缺少实际均线和行情日期" if not valid else ""
    if name == "sim_tracks":
        tracks = data.get("tracks")
        if isinstance(tracks, dict):
            curves = [v.get("curve") for v in tracks.values() if isinstance(v, dict) and isinstance(v.get("curve"), list)]
            return bool(curves), sum(map(len, curves)), "缺少真实曲线序列" if not curves else ""
        if isinstance(data.get("positions"), list) and isinstance(data.get("portfolio"), dict):
            return True, len(data["positions"]), ""
        return False, 0, "缺少跟踪业务结构"
    return False, 0, "未知业务结构，尚未验收"


def _pipeline_evidence(pipeline, path):
    """Zero candidates are valid only after a recorded successful algorithm run."""
    steps = pipeline.get("steps") or []
    if isinstance(steps, dict):
        steps = list(steps.values())
    for step in steps:
        if not isinstance(step, dict) or step.get("status") not in ("success", "complete", "completed", "succeeded", "empty_valid") or step.get("returncode") != 0:
            continue
        evidence = step.get("evidence") or []
        if isinstance(evidence, dict):
            evidence = [evidence] if "path" in evidence else list(evidence.values())
        for item in evidence:
            if not isinstance(item, dict) or not item.get("path"):
                continue
            candidate = Path(item["path"])
            candidate = candidate if candidate.is_absolute() else BASE / candidate
            if candidate.resolve() == path.resolve() and item.get("ok") is not False and item.get("source") and item.get("algorithm") and item.get('sha256'):
                try:
                    if hashlib.sha256(path.read_bytes()).hexdigest() == item['sha256']:
                        return item
                except OSError:
                    pass
    return {}


def _has_forward_observation(data):
    for entry in data.get('entries', []):
        horizons = entry.get('fwd') if isinstance(entry, dict) else None
        if not isinstance(horizons, dict):
            continue
        for horizon in ('t1', 't5', 't20', 't60'):
            result = horizons.get(horizon)
            value = result.get('ret') if isinstance(result, dict) else None
            if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value):
                return True
    return False


def _api_health() -> dict:
    """File presence, content coverage and actual analysis completion are separate."""
    import glob
    checks = {
        "pitch_v2": ("logs/pitch_v2_*.json", 48),
        "opp_pool": ("logs/opp_pool_*.json", 48),
        "beneish": ("logs/beneish_report*.json", 24 * 7),      # 月度报告，7 天内都新鲜
        "sim_tracks": ("logs/sim_tracks*.json", 24 * 7),        # 样本跟踪低频
        "pitch_track": ("logs/pitch_track_pool_*.json", 48),
        "regime": ("output/dynamic_regime.json", 72),
        "traffic_light": ("output/traffic_light.json", 72),   # ★2026-08-14 择时红绿灯
        "signal": ("output/daily_signal_*.json", 48),          # ★#351 时间戳文件（无戳旧版残留勿用）
        "audit": ("report/data_audit_report_*.json", 72),       # ★F2：时间戳文件名 glob
        "factor_pool": ("report/factor_pool_report.json", 72),
        "pool_layers": ("output/pool_layers_*.json", 48),       # ★#351 时间戳文件
    }
    bootstrap = _research_document("bootstrap-state.json")
    manifest = _research_document("bootstrap-manifest.json")
    pipeline = _research_document("pipeline-state.json")
    target = _content_date((manifest.get("coverage", {}).get("daily") or {}).get("end"))
    out = {}
    for name, (pat, stale_h) in checks.items():
        full = str(BASE / pat)
        if "*" in pat:
            files = sorted(glob.glob(full), key=os.path.getmtime)
            ok = bool(files)
            p = Path(files[-1]) if ok else None
            age = time.time() - p.stat().st_mtime if ok else None
        else:
            p = Path(full)
            ok = p.exists()
            age = time.time() - p.stat().st_mtime if ok else None
        row = {"online": ok, "ready": False, "initialized": False, "state": "missing_data", "data_date": "", "rows": 0,
                     "age_h": round(age / 3600, 1) if age is not None else None,
                     "stale_h": stale_h,
                     "stale": (not ok) or (age is not None and age > stale_h * 3600),
                     "reason": "尚未生成产物", "file": str(p.relative_to(BASE)) if ok else None}
        if not ok:
            if bootstrap.get("status") == "running" or pipeline.get("status") == "running":
                row.update(state="initializing", reason="初始化或分析进行中，尚未生成本项产物")
            elif name in ("sim_tracks", "pitch_track") and pipeline.get("status") == "complete":
                row.update(state="pending_history", reason="基础分析完成，尚无真实入池或持仓跟踪历史")
            out[name] = row
            continue
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            if not isinstance(data, dict):
                row.update(state="unknown", reason="产物不是已知业务对象，尚未验收")
                out[name] = row
                continue
            valid, rows, reason = _product_schema(name, data)
            row["rows"] = rows
            evidence = _pipeline_evidence(pipeline, p)
            meta = data.get("meta") if isinstance(data.get("meta"), dict) else {}
            date = next((_content_date(v) for v in (data.get("data_date"), meta.get("data_date"), evidence.get("data_date"), data.get("last_market_date"), data.get("asof"), data.get("pool_date"), data.get("date")) if _content_date(v)), "")
            if name == "regime":
                date = _content_date(evidence.get("data_date"))
            row['initialized'] = bool(valid and evidence and date and target and date == target)
            row.update(data_date=date, generated_at=data.get("generated_at") or data.get("ts") or data.get("updated"),
                       evidence={k: evidence[k] for k in ("source", "algorithm", "data_date", "rows") if k in evidence})
            if not valid:
                row.update(state="failed" if data.get("error") or data.get("blocked") else "unknown", reason=reason)
            elif (rows == 0 and name == 'sim_tracks') or (name == 'pitch_track' and not _has_forward_observation(data)):
                row.update(state="pending_history" if evidence else "unknown", reason="已初始化前向观察；尚无到期收益样本，需随真实交易日积累" if evidence else "尚缺当前跟踪产物的可核验运行证据")
            elif not date:
                row.update(state="unknown", reason="内容缺少可验证的数据日期，文件更新时间不能替代行情日期")
            elif not target:
                row.update(state="unknown", reason="研究行情覆盖范围尚未验收，不能仅凭新产物判定就绪")
            elif target and date < target and name not in ("beneish", "factor_pool"):
                row.update(state="stale", stale=True, reason=f"内容至 {date}，研究行情至 {target}")
            elif date > target:
                row.update(state="unknown", reason=f"产物声称的数据日期 {date} 超出已验证行情范围 {target}")
            elif rows == 0:
                row.update(state="empty_valid" if evidence else "unknown", ready=bool(evidence),
                           reason="算法已成功运行，本次没有满足条件的候选" if evidence else "结果为空，尚缺算法成功运行的可核验证据")
            elif not evidence:
                row.update(state="unknown", reason="缺少当前产物的算法运行及真实数据来源证据")
            else:
                row.update(state="ready", ready=True, stale=False, reason="业务结构、样本与内容日期已核验")
        except (OSError, ValueError, TypeError) as exc:
            row.update(state="failed", reason="产物无法读取或解析：" + _safe_health_text(exc))
        out[name] = row
    return out


def _research_status(products=None):
    bootstrap = _research_document("bootstrap-state.json")
    manifest = _research_document("bootstrap-manifest.json")
    pipeline = _research_document("pipeline-state.json")
    products = _api_health() if products is None else products
    public_bootstrap = {k: bootstrap[k] for k in ("schema_version", "status", "phase", "stage", "started_at", "updated_at", "completed_units", "total_units", "rows") if k in bootstrap}
    public_bootstrap["last_error"] = _safe_health_text(bootstrap.get("last_error"))
    steps = pipeline.get("steps") or []
    if isinstance(steps, dict):
        steps = list(steps.values())
    public_steps = [{**{k: step[k] for k in ("id", "label", "status", "started_at", "finished_at", "returncode") if k in step}, "error": _safe_health_text(step.get("error"))} for step in steps if isinstance(step, dict)]
    all_ready = bool(products) and all(row.get("ready") for row in products.values())
    finished = bootstrap.get("status") == "complete" and pipeline.get("status") == "complete"
    required_inputs = pipeline.get('required_inputs', ('daily', 'finance', 'assets', 'minute'))
    target = (manifest.get('coverage', {}).get('daily') or {}).get('end')
    inputs_ready = bool(target and all(manifest.get('stages',{}).get(key) == 'complete'
                        and manifest.get('stage_dates',{}).get(key) == target for key in required_inputs))
    minute_job = _minute_job_status()
    if minute_job.get('database_path'):
        finished = bool(inputs_ready and pipeline.get('status') == 'complete'
                        and minute_job.get('current_date_ready') and minute_job.get('deep_verified') is True)
    # A successful core analysis does not certify the separate minute download.
    initialized = bool((finished or inputs_ready and pipeline.get('status') == 'complete')
                       and products and all(row.get('initialized') for row in products.values()))
    status = ("ready" if finished and all_ready else "limited" if initialized and not finished
              else "initialized" if initialized else "initializing" if "running" in (bootstrap.get("status"), pipeline.get("status"))
              else "failed" if "failed" in (bootstrap.get("status"), pipeline.get("status")) else "incomplete")
    return {"status": status, "ready": status == "ready", "initialized": initialized,
            "initialized_count": sum(bool(row.get('initialized')) for row in products.values()),
            "uninitialized_count": sum(not row.get('initialized') for row in products.values()),
            "bootstrap": public_bootstrap,
            "pipeline": {"status": pipeline.get("status", "not_started"), "running": pipeline.get("running", False), "current_step": pipeline.get("current_step"), "steps": public_steps},
            "coverage": manifest.get("coverage") or {}, "minute_job": minute_job, "asof": manifest.get("asof"),
            "ready_count": sum(bool(row.get("ready")) for row in products.values()), "total": len(products),
            "pending_history_count": sum(row.get("state") == "pending_history" for row in products.values()),
            "message": "研究数据和全部分析产物已核验" if status == "ready" else f"{len(products)}项核心研究已初始化；历史分钟数据正在独立补齐，进度见数据页" if status == "limited" else "全部研究模块已初始化，前向观察等待真实交易日样本" if initialized else "初始化尚未完成；每项缺失、失败及待积累原因保留在数据健康明细中"}


def _deck_pid() -> int:
    try:
        if os.name != "nt":
            r = subprocess.run(["lsof", "-nP", "-t", "-iTCP:8787", "-sTCP:LISTEN"],
                               capture_output=True, text=True, timeout=3)
            return next((int(pid) for pid in r.stdout.split() if pid.isdigit()), 0)
        r = subprocess.run(["netstat", "-ano"], capture_output=True, text=True, errors="replace", timeout=15)
        for line in r.stdout.splitlines():
            if ":8787" in line and "LISTENING" in line:
                return int(line.split()[-1])
    except Exception:
        pass
    return 0


def _activity_feed(minutes: int = 60) -> list:
    """★2026-08-10 实时活动流：logs/ 目录最近 N 分钟修改的产出文件（显示系统在干活）"""
    import glob
    now = time.time()
    items = []
    for pat in ("logs/*.json", "logs/*.md", "logs/*.csv", "output/*.json", "report/*.json"):
        for p_str in glob.glob(str(BASE / pat)):
            try:
                p = Path(p_str)
                mt = p.stat().st_mtime
                age = now - mt
                if age <= minutes * 60:
                    items.append({
                        "file": p.name, "age_min": round(age / 60, 1),
                        "ts": time.strftime("%H:%M:%S", time.localtime(mt)),
                    })
            except Exception:
                pass
    items.sort(key=lambda x: x["age_min"])
    return items[:15]


def _next_schedule(autotrade=None) -> list:
    """★2026-08-10 距下次各计划任务倒计时（分钟）
    ★2026-08-14 修复：schtasks 输出"下次运行时间"为本地化格式（如 2026/8/14 10:00:00，
    月份/日期不补零），strptime 在部分环境解析失败或类型异常导致 portal_dash 间歇 500。
    改为容错解析：手写拆分（年/月/日/时/分/秒）→ datetime；任何失败返回 None 不抛错。"""
    out = _autotrade_schedule_entries(autotrade)
    if os.name != "nt":
        return out
    try:
        import re as _re
        from datetime import datetime as _dt
        for name, desc in SCHEDULED_TASKS:
            try:
                r = subprocess.run(["schtasks", "/Query", "/TN", name, "/FO", "LIST", "/V"],
                                   capture_output=True, text=True, errors="replace",
                                   encoding="gbk", timeout=8)
                m = _re.search(r"下次运行时间:\s*(.+)", r.stdout)
                if not m:
                    continue
                nxt = m.group(1).strip()
                # ★容错解析：拆分数字（兼容 2026/8/14 10:00:00 和 2026-08-14 10:00:00）
                parts = _re.findall(r"\d+", nxt)
                if len(parts) >= 5:
                    y, mo, d, h, mi = (int(parts[0]), int(parts[1]), int(parts[2]),
                                       int(parts[3]), int(parts[4]))
                    s = int(parts[5]) if len(parts) >= 6 else 0
                    try:
                        nxt_dt = _dt(y, mo, d, h, mi, s)
                        mins = round((nxt_dt - _dt.now()).total_seconds() / 60, 0)
                        out.append({"name": name, "desc": desc, "next": nxt,
                                    "mins_left": int(mins)})
                    except Exception:
                        out.append({"name": name, "desc": desc, "next": nxt,
                                    "mins_left": None})
            except Exception:
                continue
    except Exception:
        pass
    return out


_cache = {"ts": 0.0, "data": None}


def collect() -> dict:
    """系统实时状态聚合（★2026-08-10 性能：10s 短缓存——页面 5s 轮询降频，避免每次 1.6s 全量探测）"""
    import time as _t
    now = _t.time()
    if _cache["data"] is not None and now - _cache["ts"] < 10:
        _cache["data"]["ts"] = time.strftime("%Y-%m-%d %H:%M:%S")
        return _cache["data"]
    autotrade = _autotrade_status()
    products = _api_health()
    out = {
        "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
        "databases": _db_health(),
        "dev_auto_tail": _dev_auto_tail(),
        "scheduled": _scheduled(autotrade),
        "autotrade": autotrade,
        "api_health": products,
        "research": _research_status(products),
        "deck_pid": _deck_pid(),
        "activity_feed": _activity_feed(),      # ★实时活动流
        "next_schedule": _next_schedule(autotrade),      # ★倒计时
    }
    _cache.update({"ts": now, "data": out})
    return out


if __name__ == "__main__":
    print(json.dumps(collect(), ensure_ascii=False, indent=1))
