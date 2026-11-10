"""真实行情适配：仅请求行情，不持有券商交易能力。

官方契约：tushare.pro/document/2?doc_id=397,183,214,26,25,27,32
XtQuant: https://dict.thinktrader.net/nativeApi/xtdata.html
历史证券池由用户明确配置，不宣称无幸存者偏差的全市场历史样本。
"""
from datetime import date, datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re
import time
import urllib.request

from .data import TushareProvider, _date, _number, load_day, save_day, validate_day
from .models import Bar, D, MarketDay
from .monitoring import atomic_json
from .rules import board_for_symbol

SHANGHAI = timezone(timedelta(hours=8))


class Records:
    """DataFrame最小读取协议；HTTP适配无需引入pandas。"""
    def __init__(self, fields, rows):
        self.columns = list(fields)
        self._rows = rows

    def to_dict(self, orient):
        if orient != "records":
            raise ValueError("only records orientation is supported")
        return [dict(row) for row in self._rows]


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise ValueError("Tushare API redirects are not accepted")


class TushareHttpClient:
    """官方HTTP请求格式、固定HTTPS主机，令牌不写日志且禁止重定向。"""
    def __init__(self, token, timeout=30, opener=None):
        if not isinstance(token, str) or not token.strip():
            raise ValueError("TUSHARE_TOKEN is required")
        if not 0 < float(timeout) <= 120:
            raise ValueError("invalid Tushare timeout")
        self._token, self.timeout = token, float(timeout)
        self._opener = opener or urllib.request.build_opener(_NoRedirect())

    def query(self, endpoint, fields="", **params):
        if not re.fullmatch(r"[a-z][a-z0-9_]*", endpoint):
            raise ValueError("invalid API endpoint")
        payload = json.dumps({"api_name": endpoint, "token": self._token,
                              "params": params, "fields": fields}).encode()
        request = urllib.request.Request("https://api.tushare.pro", data=payload,
                                         headers={"Content-Type": "application/json"}, method="POST")
        try:
            with self._opener.open(request, timeout=self.timeout) as response:
                if response.geturl().rstrip("/") != "https://api.tushare.pro":
                    raise ValueError("unexpected API response host")
                raw = response.read(8_000_001)
                if len(raw) > 8_000_000:
                    raise ValueError("API response too large")
                result = json.loads(raw)
            if not isinstance(result, dict) or result.get("code") != 0:
                raise ValueError("API permission or request failure")
            data = result["data"]
            columns, values = data["fields"], data["items"]
            if (not isinstance(columns, list) or any(not isinstance(c, str) for c in columns)
                    or len(set(columns)) != len(columns) or not isinstance(values, list)
                    or any(not isinstance(row, list) or len(row) != len(columns) for row in values)):
                raise ValueError("invalid API response shape")
            return Records(columns, [dict(zip(columns, row)) for row in values])
        except Exception:
            raise ValueError(f"Tushare {endpoint} request failed; check permission/network/response") from None

    def __getattr__(self, endpoint):
        return lambda fields="", **params: self.query(endpoint, fields, **params)


def _dates(start, end):
    first, last = date.fromisoformat(_date(start)), date.fromisoformat(_date(end))
    if first > last:
        raise ValueError("start date is after end date")
    return [(first + timedelta(days=n)).isoformat() for n in range((last-first).days + 1)]


def _iso_compact(value):
    return datetime.strptime(str(value), "%Y%m%d").date().isoformat()


class TushareHistoricalProvider(TushareProvider):
    """完整历史预热/补漏，历史ST和停牌均按交易日查询。"""
    def __init__(self, config, client=None, now=None, sleeper=time.sleep):
        super().__init__(config, client=client, now=now)
        self._sleep = sleeper
        self.attempts = self.config.get("request_attempts", 3)
        self.retry_seconds = self.config.get("retry_seconds", 1)
        if type(self.attempts) is not int or not 1 <= self.attempts <= 5 or not 0 <= float(self.retry_seconds) <= 30:
            raise ValueError("invalid retry policy")
        self._calendar = {}
        self._stock_info = {}
        default_cache = Path(self.config.get("directory", "runtime/autotrade/market")) / "calendar-cache.json"
        self.cache_path = Path(self.config.get("calendar_cache") or default_cache)
        if self.cache_path.exists():
            cache = json.loads(self.cache_path.read_text(encoding="utf-8"))
            if cache.get("schema_version") != 1 or cache.get("provider") != "tushare:SSE":
                raise ValueError("invalid trading calendar cache")
            self._calendar = self._validate_calendar_rows(cache["rows"])
        for symbol in self.universe:
            board_for_symbol(symbol)

    def _call(self, endpoint, fields, **params):
        if self.client is None:
            self.client = TushareHttpClient(os.environ.get("TUSHARE_TOKEN"), self.config.get("request_timeout", 30))
        for attempt in range(self.attempts):
            try:
                result = getattr(self.client, endpoint)(fields=",".join(fields), **params)
                if result is None or not hasattr(result, "columns") or not set(fields) <= set(result.columns):
                    raise ValueError("missing fields")
                rows = result.to_dict("records")
                if not isinstance(rows, list) or any(not isinstance(row, dict) or not set(fields) <= set(row) for row in rows):
                    raise ValueError("invalid row shape")
                return rows
            except Exception:
                if attempt + 1 == self.attempts:
                    raise ValueError(f"Tushare {endpoint} request failed after {self.attempts} attempts; check permission/network/fields") from None
                self._sleep(min(30, float(self.retry_seconds) * 2 ** attempt))

    @staticmethod
    def _validate_calendar_rows(rows):
        result = {}
        for row in rows:
            day = _iso_compact(row["cal_date"])
            previous = _iso_compact(row["pretrade_date"])
            if day in result or str(row["is_open"]) not in {"0", "1"} or previous >= day:
                raise ValueError("invalid or duplicate trading calendar row")
            result[day] = {"cal_date": day.replace("-", ""), "is_open": int(row["is_open"]),
                           "pretrade_date": previous.replace("-", "")}
        return result

    def calendar(self, start_date, end_date):
        """返回覆盖闭市日的明确日历摘要；缺日不作为休市推断。"""
        requested = _dates(start_date, end_date)
        if len(requested) > 3660:
            raise ValueError("calendar request exceeds ten years; use smaller batches")
        if set(requested) - set(self._calendar):
            fetched = self._call("trade_cal", ("cal_date", "is_open", "pretrade_date"), exchange="SSE",
                                 start_date=start_date.replace("-", ""), end_date=end_date.replace("-", ""))
            incoming = self._validate_calendar_rows(fetched)
            if set(incoming) != set(requested):
                raise ValueError("exchange calendar response has missing or extra days")
            for day, row in incoming.items():
                if day in self._calendar and self._calendar[day] != row:
                    raise ValueError("exchange calendar changed; inspect cache before retrying")
            self._calendar.update(incoming)
            atomic_json(self.cache_path, {"schema_version": 1, "provider": "tushare:SSE",
                                         "rows": [self._calendar[key] for key in sorted(self._calendar)]})
        return {"start": start_date, "end": end_date,
                "sessions": [d for d in requested if self._calendar[d]["is_open"] == 1]}

    def is_trading_day(self, trade_date):
        _date(trade_date)
        return bool(self.calendar(trade_date, trade_date)["sessions"])

    def prepare(self, start_date, end_date):
        """预取相邻日历与稳定证券资料；不请求或覆盖交易快照。"""
        first, last = date.fromisoformat(_date(start_date)), date.fromisoformat(_date(end_date))
        self.calendar((first-timedelta(days=30)).isoformat(), (last+timedelta(days=30)).isoformat())
        for symbol in sorted(self.universe):
            self._info(symbol)

    def _adjacent(self, trade_date):
        center = date.fromisoformat(_date(trade_date))
        calendar = self.calendar((center-timedelta(days=30)).isoformat(), (center+timedelta(days=30)).isoformat())
        sessions = calendar["sessions"]
        if trade_date not in sessions:
            raise ValueError("requested date is not a trading session")
        index = sessions.index(trade_date)
        if index == 0 or index + 1 == len(sessions):
            raise ValueError("calendar lacks adjacent trading sessions")
        previous, following = sessions[index-1], sessions[index+1]
        if self._calendar[trade_date]["pretrade_date"] != previous.replace("-", ""):
            raise ValueError("calendar previous-session mismatch")
        return previous, following

    def _symbol_rows(self, endpoint, fields, symbol, compact, **params):
        rows = self._call(endpoint, fields, ts_code=symbol, trade_date=compact, **params)
        if any(row["ts_code"] != symbol or row["trade_date"] != compact for row in rows):
            raise ValueError(f"Tushare {endpoint}: stale or mismatched row")
        return rows

    def _info(self, symbol):
        if symbol not in self._stock_info:
            fields = ("ts_code", "name", "market", "industry", "list_date", "delist_date")
            rows = []
            for status in ("L", "D", "P"):
                rows = self._call("stock_basic", fields, ts_code=symbol, list_status=status)
                if rows:
                    break
            if len(rows) != 1 or rows[0]["ts_code"] != symbol:
                raise ValueError("stock metadata missing or ambiguous")
            self._stock_info[symbol] = rows[0]
        return self._stock_info[symbol]

    def fetch_day(self, trade_date, previous_day=None):
        _date(trade_date)
        now = self.now()
        if now.tzinfo is None:
            raise ValueError("provider clock must be timezone-aware")
        now = now.astimezone(SHANGHAI)
        if trade_date > now.date().isoformat() or (trade_date == now.date().isoformat() and now.strftime("%H:%M") < "15:30"):
            raise ValueError("only completed trading sessions may be downloaded")
        if trade_date < "2019-01-01":
            raise ValueError("turnover history before 2019 is unsupported")
        previous, following = self._adjacent(trade_date)
        if previous_day is not None and previous_day.date != previous:
            raise ValueError("previous snapshot is not the adjacent session")
        compact, bars = trade_date.replace("-", ""), {}
        for symbol in sorted(self.universe):
            info = self._info(symbol)
            listed = _iso_compact(info["list_date"])
            delisted = info.get("delist_date")
            if listed > trade_date or (delisted not in (None, "") and _iso_compact(delisted) <= trade_date):
                raise ValueError("configured stock did not exist on requested date")
            board = board_for_symbol(symbol)
            market_map = {"主板": "MAIN", "创业板": "CHINEXT", "科创板": "STAR", "北交所": "BSE"}
            if market_map.get(info["market"]) != board:
                raise ValueError("stock board metadata conflicts with ordinary A-share code")
            limit_rows = self._symbol_rows("stk_limit", ("ts_code", "trade_date", "pre_close", "up_limit", "down_limit"), symbol, compact)
            if len(limit_rows) != 1:
                raise ValueError("daily price limits missing or ambiguous")
            limits = limit_rows[0]
            reference = _number(limits["pre_close"])
            upper, lower = _number(limits["up_limit"]), _number(limits["down_limit"])
            statuses = self._symbol_rows("stock_st", ("ts_code", "trade_date"), symbol, compact)
            if len(statuses) > 1:
                raise ValueError("ST status is ambiguous")
            suspensions = self._symbol_rows("suspend_d", ("ts_code", "trade_date", "suspend_type", "suspend_timing"), symbol, compact)
            if any(row["suspend_type"] not in {"S", "R"} for row in suspensions):
                raise ValueError("unknown suspension type")
            halted = [row for row in suspensions if row["suspend_type"] == "S"]
            full_halt = any(row["suspend_timing"] in (None, "") for row in halted)
            if any(row["suspend_timing"] not in (None, "") for row in halted):
                raise ValueError("intraday suspension requires intraday execution modelling")
            raw_rows = self._symbol_rows("daily", ("ts_code", "trade_date", "open", "high", "low", "close", "pre_close", "vol"), symbol, compact)
            if full_halt:
                if raw_rows and (len(raw_rows) != 1 or _number(raw_rows[0]["vol"]) != 0):
                    raise ValueError("halt status conflicts with traded volume")
                opening = high = low = close = reference
                volume, turnover = 0, D(0)
            else:
                if len(raw_rows) != 1:
                    raise ValueError("daily bar missing without a confirmed full-day suspension")
                raw = raw_rows[0]
                if _number(raw["pre_close"]) != reference:
                    raise ValueError("daily and price-limit reference prices disagree")
                opening, high, low, close = (_number(raw[k]) for k in ("open", "high", "low", "close"))
                shares = _number(raw["vol"]) * 100
                if shares != shares.to_integral_value() or shares <= 0:
                    raise ValueError("trading volume must be positive whole shares")
                volume = int(shares)
                basic = self._symbol_rows("daily_basic", ("ts_code", "trade_date", "turnover_rate"), symbol, compact)
                if len(basic) != 1:
                    raise ValueError("daily turnover missing or ambiguous")
                turnover = _number(basic[0]["turnover_rate"])
            # 即使是当日也统一unknown，避免同一历史文件随下载日期产生不同metadata。
            bars[symbol] = Bar(symbol, trade_date, opening, high, low, close, reference, volume,
                               turnover, board, bool(statuses), full_halt, upper, lower, listed, "unknown")
        result = MarketDay(trade_date, previous, following, bars,
                           "tushare:historical-v2;raw;volume=shares;ST/suspension/limits=dated;sector=unknown;universe=user-selected")
        validate_day(result)
        return result

    def update(self, trade_date, output_dir):
        path = Path(output_dir) / f"{_date(trade_date)}.json"
        if path.exists():
            snapshot = load_day(path)
            if set(snapshot.bars) != set(self.universe) or not snapshot.source.startswith("tushare:"):
                raise ValueError("existing snapshot provenance or universe differs")
            return snapshot
        previous = None
        previous_date, _ = self._adjacent(trade_date)
        previous_path = Path(output_dir) / f"{previous_date}.json"
        if previous_path.exists():
            previous = load_day(previous_path)
        snapshot = self.fetch_day(trade_date, previous_day=previous)
        save_day(snapshot, path)
        return snapshot

    def diagnose(self, trade_date):
        """只读全链路权限探测，不封存、不返回原始行情或令牌。"""
        try:
            snapshot = self.fetch_day(trade_date)
            return {"ok": True, "provider": "tushare", "date": snapshot.date,
                    "symbols": len(snapshot.bars), "historical_metadata": "dated ST/suspension/limits; industry unknown"}
        except (ValueError, OSError) as exc:
            return {"ok": False, "provider": "tushare", "error": str(exc)}


class FilesProvider:
    """只读取明确封存快照与覆盖完整的本地交易日历。"""
    def __init__(self, config):
        self.config = config["data"]
        self.directory = Path(self.config["directory"])

    def calendar(self, start_date, end_date):
        _dates(start_date, end_date)
        data = json.loads((self.directory / "calendar.json").read_text(encoding="utf-8"))
        if not _date(data["start"]) <= start_date <= end_date <= _date(data["end"]):
            raise ValueError("local calendar does not cover requested interval")
        sessions = data["sessions"]
        if (not isinstance(sessions, list) or sessions != sorted(set(sessions))
                or any(not data["start"] <= _date(day) <= data["end"] for day in sessions)):
            raise ValueError("invalid local trading calendar")
        return {"start": start_date, "end": end_date,
                "sessions": [day for day in sessions if start_date <= day <= end_date]}

    def is_trading_day(self, trade_date):
        return bool(self.calendar(trade_date, trade_date)["sessions"])

    def update(self, trade_date, output_dir=None):
        if output_dir is not None and Path(output_dir).resolve() != self.directory.resolve():
            raise ValueError("files provider cannot republish data to a different directory")
        if not self.is_trading_day(trade_date):
            raise ValueError("requested date is not a local trading session")
        snapshot = load_day(self.directory / f"{trade_date}.json")
        if self.config.get("universe") and set(snapshot.bars) != set(self.config["universe"]):
            raise ValueError("local snapshot universe differs")
        return snapshot

    def diagnose(self, trade_date):
        try:
            snapshot = self.update(trade_date)
            return {"ok": True, "provider": "files", "date": snapshot.date, "symbols": len(snapshot.bars), "source": snapshot.source}
        except (ValueError, OSError, KeyError, TypeError) as exc:
            return {"ok": False, "provider": "files", "error_type": type(exc).__name__}


def provider_for(config, client=None, now=None):
    provider = config["data"]["provider"]
    if provider == "tushare":
        return TushareHistoricalProvider(config, client=client, now=now)
    if provider == "files":
        return FilesProvider(config)
    if provider == "baostock":
        from .baostock_provider import BaostockProvider
        return BaostockProvider(config, client=client, now=now)
    raise ValueError(f"no downloadable provider configured: {provider}")


class QmtLiveMarketProvider:
    """当前tick与当前合约信息，仅供实时风控，不生成历史行情快照。

    Tick单位由qmt.tick_volume_multiplier显式配置；默认手转股，需终端验收。
    只接受连续竞价状态13及逐证券新鲜tick；盘前/盘后/暂停/未知状态失败关闭。
    """
    def __init__(self, config, xtdata=None, calendar_provider=None, now=None):
        self.config = config
        self.xtdata = xtdata
        self.calendar_provider = calendar_provider
        self.now = now or (lambda: datetime.now(SHANGHAI))

    def _sdk(self):
        if self.xtdata is None:
            try:
                from xtquant import xtdata
            except ImportError:
                raise ValueError("XtQuant is not installed in this Python environment") from None
            self.xtdata = xtdata
        return self.xtdata

    def _clock(self):
        current = self.now()
        if current.tzinfo is None:
            raise ValueError("quote clock must be timezone aware")
        return current.astimezone(SHANGHAI)

    def _adjacent(self, current, sdk):
        first, last = (current-timedelta(days=30)).isoformat(), (current+timedelta(days=30)).isoformat()
        if self.calendar_provider is not None:
            sessions = self.calendar_provider.calendar(first, last)["sessions"]
        else:
            raw = sdk.get_trading_calendar("SH", start_time=first.replace("-", ""), end_time=last.replace("-", ""))
            if not isinstance(raw, list) or not raw:
                raise ValueError("QMT trading calendar missing; refresh terminal holiday data")
            sessions = [_iso_compact(day) for day in raw]
        if sessions != sorted(set(sessions)) or current.isoformat() not in sessions:
            raise ValueError("QMT calendar is ambiguous or current session is closed")
        i = sessions.index(current.isoformat())
        if i == 0 or i + 1 == len(sessions):
            raise ValueError("QMT calendar lacks adjacent sessions")
        return sessions[i-1], sessions[i+1]

    def __call__(self, symbols):
        from .qmt import LiveMarketSnapshot

        if not isinstance(symbols, (set, frozenset)) or not symbols:
            raise ValueError("live market request needs a nonempty symbol set")
        for symbol in symbols:
            board_for_symbol(symbol)
        now = self._clock()
        session_date = now.date()
        sdk = self._sdk()
        try:
            previous, following = self._adjacent(now.date(), sdk)
            ticks = sdk.get_full_tick(sorted(symbols))
            if not isinstance(ticks, dict) or set(ticks) != set(symbols):
                raise ValueError("QMT ticks missing or extra symbols")
            bars, quote_times = {}, {}
            maximum_age = float(self.config.get("qmt", {}).get("max_quote_age_seconds", 5))
            multiplier = _number(self.config.get("qmt", {}).get("tick_volume_multiplier", 100))
            if not 0 < maximum_age <= 60 or multiplier not in (D(1), D(100)):
                raise ValueError("invalid QMT quote age or volume configuration")
            for symbol in sorted(symbols):
                detail = sdk.get_instrument_detail(symbol, iscomplete=True)
                kind = sdk.get_instrument_type(symbol)
                if (not isinstance(detail, dict) or not isinstance(kind, dict)
                        or kind.get("stock") is not True
                        or any(kind.get(key, False) is not False for key in ("fund", "etf", "index"))):
                    raise ValueError("QMT instrument is not a verified ordinary stock")
                code, exchange = symbol.split(".")
                if detail["InstrumentID"] != code or detail["ExchangeID"] != exchange:
                    raise ValueError("QMT instrument identity mismatch")
                tick = ticks[symbol]
                timestamp = _number(tick["time"])
                quote_time = datetime.fromtimestamp(float(timestamp / 1000), SHANGHAI)
                # A newly received tick may be later than the pre-I/O clock.
                now = self._clock()
                age = (now - quote_time).total_seconds()
                if now.date() != session_date or quote_time.date() != session_date or not 0 <= age <= maximum_age:
                    raise ValueError("QMT quote is stale or future dated")
                if type(detail["IsTrading"]) is not bool or not detail["IsTrading"] or tick["stockStatus"] != 13:
                    raise ValueError("QMT instrument is not in continuous trading")
                if _number(detail["PriceTick"]) != D("0.01"):
                    raise ValueError("QMT instrument tick size is unsupported")
                opening, high, low, close, reference = (_number(tick[key]) for key in ("open", "high", "low", "lastPrice", "lastClose"))
                if _number(detail["PreClose"]) != reference:
                    raise ValueError("QMT tick and instrument reference prices disagree")
                upper, lower = _number(detail["UpStopPrice"]), _number(detail["DownStopPrice"])
                shares = _number(tick["volume"]) * multiplier
                if shares <= 0 or shares != shares.to_integral_value():
                    raise ValueError("QMT quote volume must be positive whole shares")
                if "pvolume" in tick and _number(tick["pvolume"]) != shares:
                    raise ValueError("QMT volume units disagree; inspect terminal configuration")
                floating = _number(detail["FloatVolume"])
                if floating <= 0:
                    raise ValueError("QMT circulating shares missing")
                name = detail["InstrumentName"]
                if not isinstance(name, str) or not name.strip() or "退" in name:
                    raise ValueError("QMT instrument name/status is invalid")
                listed = _iso_compact(detail["OpenDate"])
                bars[symbol] = Bar(symbol, now.date().isoformat(), opening, high, low, close,
                                   reference, int(shares), shares / floating * 100,
                                   board_for_symbol(symbol), "ST" in name.upper(), False,
                                   upper, lower, listed, "unknown")
                quote_times[symbol] = quote_time
            day = MarketDay(now.date().isoformat(), previous, following, bars,
                            "qmt:live-tick;current-instrument-limits;sector=unknown;NOT historical data")
            validate_day(day)
            # Re-sample after all calls so elapsed I/O cannot make old ticks appear fresh.
            observed = self._clock()
            if observed.date() != session_date or any(not 0 <= (observed-ts).total_seconds() <= maximum_age for ts in quote_times.values()):
                raise ValueError("QMT quotes became stale while collecting snapshot")
            return LiveMarketSnapshot(observed_at=observed, day=day, quote_times=quote_times)
        except (KeyError, TypeError, ArithmeticError, OverflowError):
            raise ValueError("QMT market response has missing or invalid fields") from None
