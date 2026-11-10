"""未复权日行情快照：完整校验、原子封存、禁止悄悄覆盖历史。"""
from dataclasses import asdict
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import json
import os
from pathlib import Path
import re
import tempfile

from autotrade.models import Bar, D, MarketDay


def _date(value: str) -> str:
    if not isinstance(value, str) or date.fromisoformat(value).isoformat() != value:
        raise ValueError("date must use YYYY-MM-DD")
    return value


def _number(value) -> Decimal:
    if isinstance(value, bool) or value is None:
        raise ValueError("invalid numeric value")
    try:
        return D(value)
    except ArithmeticError:
        raise ValueError("invalid numeric value") from None


def validate_day(day: MarketDay) -> None:
    """拒绝日期错位、未知板块、缺字段、脏价格及不完整日历。"""
    _date(day.date)
    if not _date(day.previous_session) < day.date < _date(day.next_session):
        raise ValueError("invalid previous/next trading session")
    if not isinstance(day.source, str) or not day.source.strip() or not day.bars:
        raise ValueError("source and nonempty bars are required")
    for symbol, bar in day.bars.items():
        if not isinstance(symbol, str) or not re.fullmatch(r"\d{6}\.(SH|SZ|BJ)", symbol):
            raise ValueError("invalid A-share symbol")
        if bar.symbol != symbol or bar.date != day.date:
            raise ValueError("bar symbol/date does not match snapshot")
        if bar.board not in {"MAIN", "CHINEXT", "STAR", "BSE"}:
            raise ValueError("unknown board")
        if _date(bar.list_date) > day.date or not isinstance(bar.sector, str) or not bar.sector.strip():
            raise ValueError("invalid listing date or sector")
        if type(bar.is_st) is not bool or type(bar.suspended) is not bool:
            raise ValueError("status flags must be booleans")
        if type(bar.volume) is not int or bar.volume < 0 or _number(bar.turnover) < 0:
            raise ValueError("invalid volume or turnover")
        if bar.suspended and bar.volume != 0:
            raise ValueError("suspended bar has nonzero volume")
        prices = [bar.open, bar.high, bar.low, bar.close, bar.prev_close]
        if any(_number(price) <= 0 for price in prices):
            raise ValueError("prices must be finite and positive")
        if not bar.low <= min(bar.open, bar.close) <= max(bar.open, bar.close) <= bar.high:
            raise ValueError("inconsistent OHLC prices")
        if (bar.limit_up is None) != (bar.limit_down is None):
            raise ValueError("both price limits must be present or absent")
        if bar.limit_up is not None:
            if not 0 < _number(bar.limit_down) < _number(bar.limit_up):
                raise ValueError("invalid price limits")
            if bar.low < bar.limit_down or bar.high > bar.limit_up:
                raise ValueError("OHLC outside exchange price limits")


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def load_day(path) -> MarketDay:
    """只读取指定快照，不回退到最新可用日期。"""
    raw = json.loads(Path(path).read_text(encoding="utf-8"), object_pairs_hook=_unique_object)
    required = {"schema_version", "date", "previous_session", "next_session", "source", "universe", "bars"}
    if not isinstance(raw, dict) or set(raw) != required or raw["schema_version"] != 1:
        raise ValueError("invalid snapshot schema")
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", Path(path).stem) and Path(path).stem != raw["date"]:
        raise ValueError("snapshot date differs from filename")
    universe = raw["universe"]
    if not isinstance(universe, list) or not universe or any(not isinstance(s, str) for s in universe):
        raise ValueError("invalid universe manifest")
    if len(set(universe)) != len(universe) or not isinstance(raw["bars"], list):
        raise ValueError("duplicate universe or invalid bars")
    bars = {}
    decimal_fields = ("open", "high", "low", "close", "prev_close", "turnover", "limit_up", "limit_down")
    for row in raw["bars"]:
        if not isinstance(row, dict) or set(row) != set(Bar.__dataclass_fields__):
            raise ValueError("invalid bar fields")
        row = dict(row)
        for key in decimal_fields:
            if row[key] is not None or key not in {"limit_up", "limit_down"}:
                row[key] = _number(row[key])
        bar = Bar(**row)
        if bar.symbol in bars:
            raise ValueError("duplicate bar symbol")
        bars[bar.symbol] = bar
    if set(universe) != set(bars):
        raise ValueError("snapshot is missing expected symbols")
    day = MarketDay(raw["date"], raw["previous_session"], raw["next_session"], bars, raw["source"])
    validate_day(day)
    return day


def save_day(day: MarketDay, path) -> None:
    """完整写入临时文件后原子发布；同内容幂等，已封存内容禁止修改。"""
    validate_day(day)
    path = Path(path)
    payload = {
        "schema_version": 1, "date": day.date, "previous_session": day.previous_session,
        "next_session": day.next_session, "source": day.source,
        "universe": sorted(day.bars), "bars": [asdict(day.bars[s]) for s in sorted(day.bars)],
    }
    content = json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2,
                         default=lambda x: str(x) if isinstance(x, Decimal) else x, allow_nan=False) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=".snapshot-", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            # 链接只能创建新目录项，避免两个并发写入者相互覆盖。
            os.link(temporary, path)
        except FileExistsError:
            if load_day(path) != day:
                raise ValueError("immutable snapshot already exists with different content") from None
    finally:
        Path(temporary).unlink(missing_ok=True)


class TushareProvider:
    """显式小股票池的盘后更新器；构造时不联网，可注入测试客户端。"""

    def __init__(self, config, client=None, now=None):
        self.config = config.get("data", config)
        self.client = client
        self.now = now or (lambda: datetime.now(timezone(timedelta(hours=8))))
        self.universe = self.config.get("universe", [])
        if not isinstance(self.universe, list) or not self.universe:
            raise ValueError("configure a nonempty data.universe")
        if len(set(self.universe)) != len(self.universe) or any(
                not isinstance(s, str) or not re.fullmatch(r"\d{6}\.(SH|SZ|BJ)", s) for s in self.universe):
            raise ValueError("invalid or duplicate configured universe")

    def _call(self, endpoint, fields, **params):
        try:
            if self.client is None:
                token = os.environ.get("TUSHARE_TOKEN")
                if not token:
                    raise ValueError("TUSHARE_TOKEN is required")
                import tushare
                self.client = tushare.pro_api(token, timeout=30)
            frame = getattr(self.client, endpoint)(fields=",".join(fields), **params)
        except Exception:
            # 上游异常可能包含认证信息，不把原异常正文写进运行日志。
            raise ValueError(f"Tushare {endpoint} request failed; check credentials/permissions/connectivity") from None
        if frame is None or not hasattr(frame, "columns") or not set(fields) <= set(frame.columns):
            raise ValueError(f"Tushare {endpoint} missing fields")
        return frame.to_dict("records")

    def _one(self, endpoint, fields, symbol, trade_date=None):
        params = {"ts_code": symbol}
        if trade_date:
            params["trade_date"] = trade_date
        rows = self._call(endpoint, fields, **params)
        if len(rows) != 1 or rows[0]["ts_code"] != symbol:
            raise ValueError(f"Tushare {endpoint}: missing/duplicate symbol {symbol}")
        if trade_date and rows[0]["trade_date"] != trade_date:
            raise ValueError(f"Tushare {endpoint}: stale trade date")
        return rows[0]

    def is_trading_day(self, trade_date: str) -> bool:
        """显式查询交易所日历；空响应或不明状态视为失败。"""
        compact = _date(trade_date).replace("-", "")
        rows = self._call("trade_cal", ("cal_date", "is_open"), exchange="SSE",
                          start_date=compact, end_date=compact)
        if len(rows) != 1 or rows[0]["cal_date"] != compact or str(rows[0]["is_open"]) not in {"0", "1"}:
            raise ValueError("exchange calendar missing or ambiguous")
        return str(rows[0]["is_open"]) == "1"

    def update(self, trade_date: str, output_dir: Path) -> MarketDay:
        """当前交易日盘后封存；历史回放使用此前封存的 files 快照。"""
        _date(trade_date)
        now = self.now()
        if now.tzinfo is None:
            raise ValueError("provider clock must be timezone-aware")
        now = now.astimezone(timezone(timedelta(hours=8)))
        if trade_date != now.date().isoformat() or (now.hour, now.minute) < (15, 30):
            raise ValueError("Tushare update requires today's completed session in Asia/Shanghai")
        compact = trade_date.replace("-", "")
        center = date.fromisoformat(trade_date)
        cal = self._call("trade_cal", ("cal_date", "is_open", "pretrade_date"), exchange="SSE",
                         start_date=(center - timedelta(days=30)).strftime("%Y%m%d"),
                         end_date=(center + timedelta(days=30)).strftime("%Y%m%d"))
        calendar = {}
        for row in cal:
            d = _date(datetime.strptime(str(row["cal_date"]), "%Y%m%d").date().isoformat())
            if d in calendar or str(row["is_open"]) not in {"0", "1"}:
                raise ValueError("invalid or duplicate exchange calendar")
            calendar[d] = row
        sessions = sorted(d for d, row in calendar.items() if str(row["is_open"]) == "1")
        if trade_date not in sessions:
            raise ValueError("requested day is not an exchange trading session")
        index = sessions.index(trade_date)
        if index == 0 or index == len(sessions) - 1:
            raise ValueError("exchange calendar lacks adjacent sessions")
        previous, following = sessions[index - 1], sessions[index + 1]
        if str(calendar[trade_date]["pretrade_date"]) != previous.replace("-", ""):
            raise ValueError("exchange calendar previous session mismatch")
        bars = {}
        boards = {"主板": "MAIN", "创业板": "CHINEXT", "科创板": "STAR", "北交所": "BSE"}
        for symbol in sorted(self.universe):
            raw = self._one("daily", ("ts_code", "trade_date", "open", "high", "low", "close", "pre_close", "vol"), symbol, compact)
            basic = self._one("daily_basic", ("ts_code", "trade_date", "turnover_rate"), symbol, compact)
            limits = self._one("stk_limit", ("ts_code", "trade_date", "up_limit", "down_limit"), symbol, compact)
            info = self._one("stock_basic", ("ts_code", "name", "market", "industry", "list_date"), symbol)
            statuses = self._call("stock_st", ("ts_code", "trade_date"), ts_code=symbol, trade_date=compact)
            if len(statuses) > 1 or any(r["ts_code"] != symbol or r["trade_date"] != compact for r in statuses):
                raise ValueError("invalid ST status response")
            if info["market"] not in boards:
                raise ValueError("unsupported or unknown market")
            volume = _number(raw["vol"]) * 100
            if volume != volume.to_integral_value():
                raise ValueError("volume cannot be expressed in whole shares")
            bars[symbol] = Bar(
                symbol, trade_date, *(_number(raw[k]) for k in ("open", "high", "low", "close", "pre_close")),
                int(volume), _number(basic["turnover_rate"]), boards[info["market"]], bool(statuses),
                volume == 0, _number(limits["up_limit"]), _number(limits["down_limit"]),
                datetime.strptime(str(info["list_date"]), "%Y%m%d").date().isoformat(), info["industry"],
            )
        day = MarketDay(trade_date, previous, following, bars,
                        "tushare:daily(raw),daily_basic,stk_limit,stock_basic(current),stock_st,trade_cal(SSE);volume=shares")
        save_day(day, Path(output_dir) / f"{trade_date}.json")
        return day
