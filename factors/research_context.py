"""Local, read-only inputs shared by research jobs (never a trading entry point)."""
import json
import math
import hashlib
import os
import sqlite3
from pathlib import Path

import pandas as pd
import yaml

BASE = Path(__file__).resolve().parent.parent


def config():
    path = Path(os.environ.get("LWQUANT_RESEARCH_CONFIG", BASE / "config/research.yaml"))
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict) or not isinstance(raw.get("analysis"), dict):
        raise ValueError("研究配置缺少 analysis 节")
    return {**raw, **raw["analysis"]}


def path(value):
    p = Path(str(value)).expanduser()
    return p if p.is_absolute() else BASE / p


def cache_dir():
    return path(os.environ.get("LWQUANT_CACHE_DIR") or config()["cache_dir"])


def connect(db):
    """mode=ro prevents a missing input from silently becoming an empty SQLite DB."""
    return sqlite3.connect(Path(db).resolve().as_uri() + "?mode=ro", uri=True, timeout=10)


def universe():
    cfg = config()
    raw = json.loads(path(cfg["universe_file"]).read_text(encoding="utf-8"))
    symbols = raw.get("symbols") if isinstance(raw, dict) else raw
    if not isinstance(symbols, list) or not symbols:
        raise ValueError("研究股票池为空，需先获取真实当前成分股")
    out = {str(s).upper() for s in symbols if isinstance(s, str)}
    if not out or any(not (len(s) == 9 and s[:6].isdigit() and s[6:] in (".SH", ".SZ")) for s in out):
        raise ValueError("研究股票池代码格式无效")
    return out


def scope_metadata():
    cfg = config()
    universe_file = path(cfg["universe_file"])
    try:
        universe_file = universe_file.relative_to(BASE)
    except ValueError:
        pass
    return {"scope": cfg["scope"], "universe_count": len(universe()),
            "universe_file": str(universe_file),
            "universe_fingerprint": hashlib.sha256("\n".join(sorted(universe())).encode()).hexdigest(),
            "limitation": "使用当前成分股回看历史，存在成分股选择和幸存者偏差；不是历史时点成分股回测"}


def read_bars(columns="code,date,open,close,volume,turn,is_st", adjust="qfq", scoped=True, symbols=None):
    root = cache_dir()
    frames = []
    # Every increment can contain distinct days; keep all instead of assuming the last three are cumulative.
    symbols = list(symbols) if symbols is not None else (sorted(universe()) if scoped else [])
    condition = " AND code IN (" + ",".join("?" for _ in symbols) + ")" if symbols else ""
    for db in [root / "bars.db", root / "bars_incr.db", *sorted(root.glob("bars_incr_*.db"))]:
        if not db.is_file():
            continue
        with connect(db) as con:
            frames.append(pd.read_sql_query(f"SELECT {columns} FROM daily_bar WHERE adjust=?" + condition,
                                            con, params=(adjust, *symbols)))
    if not frames:
        raise ValueError("缺少真实本地日线数据库")
    df = pd.concat(frames, ignore_index=True).drop_duplicates(["code", "date"], keep="last")
    return df.sort_values(["date", "code"])


def latest_published(db, table, end, date_column="period"):
    """Latest record per security that was actually published by end; no coverage quota."""
    with connect(db) as con:
        df = pd.read_sql_query(f"SELECT * FROM {table}", con)
    if df.empty:
        return df
    pub_col = next((c for c in ("pub_date", "ann_date", "f_ann_date") if c in df), None)
    if pub_col is None:
        raise ValueError(f"{table} 缺公告日期，不能确认时点可得性")
    dates = pd.to_datetime(df[pub_col], errors="coerce")
    periods = pd.to_datetime(df[date_column], errors="coerce")
    df = df[(dates <= pd.Timestamp(end)) & (periods <= pd.Timestamp(end))].copy()
    if df.empty:
        return df
    return df.sort_values(["code", date_column, pub_col]).drop_duplicates("code", keep="last")


def valuations(end):
    db = cache_dir() / "daily_basic.db"
    if not db.is_file():
        return pd.DataFrame()
    symbols = sorted(universe())
    placeholders = ",".join("?" for _ in symbols)
    with connect(db) as con:
        return pd.read_sql_query(
            "SELECT d.* FROM daily_basic d JOIN (SELECT code,MAX(date) date FROM daily_basic "
            f"WHERE code IN ({placeholders}) AND date<=? GROUP BY code) m ON d.code=m.code AND d.date=m.date",
            con, params=(*symbols, str(end)[:10]))


def json_safe(value):
    """Missing/non-finite observations stay null, never zero or invalid JSON NaN."""
    if isinstance(value, dict):
        return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(v) for v in value]
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if hasattr(value, "item"):
        return json_safe(value.item())
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value
