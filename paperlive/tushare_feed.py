"""Tushare-only current market feed; no anonymous-source fallback.

Contracts checked against official Tushare documentation: rt_k (doc 372),
index_weight (96), stock_basic (25), trade_cal (26), stk_limit (183).
rt_k's volume AND first-level book quantities are shares, not Tencent lots.
Its A-share daily RT permission is separate from ordinary point permissions.
"""
from dataclasses import asdict
from datetime import date, datetime, timedelta
from decimal import Decimal
import re
import threading
import time

from autotrade.monitoring import atomic_json
from autotrade.providers import TushareHttpClient, TushareRequestError
from .market import MarketFeed, SHANGHAI, _aware, _board, _day, _decimal, _symbol
from .models import Instrument, Quote

RT_FIELDS = ("ts_code", "name", "pre_close", "high", "open", "low", "close", "vol", "amount", "num",
             "ask_price1", "ask_volume1", "bid_price1", "bid_volume1", "trade_time")
BASIC_FIELDS = ("ts_code", "name", "industry", "list_date", "list_status", "delist_date", "market", "exchange")
INDEX_FIELDS = ("index_code", "con_code", "trade_date", "weight")
LIMIT_FIELDS = ("trade_date", "ts_code", "pre_close", "up_limit", "down_limit")
CALENDAR_FIELDS = ("exchange", "cal_date", "is_open")
_REQUEST_LOCK = threading.Lock()
_LAST_REQUEST = None


def _iso_date(value):
    if not isinstance(value, str):
        raise ValueError("Tushare日期缺失")
    if re.fullmatch(r"\d{8}", value):
        return datetime.strptime(value, "%Y%m%d").date().isoformat()
    return _day(value)


def _compact(value):
    return _iso_date(value).replace("-", "")


def _integer(value):
    if isinstance(value, bool):
        raise ValueError("Tushare股数无效")
    result = _decimal(value, zero=True)
    if result != result.to_integral_value():
        raise ValueError("Tushare股数必须是整数")
    return int(result)


def _timestamp(value):
    if not isinstance(value, str):
        raise ValueError("Tushare缺少真实快照时间trade_time")
    value = value.strip()
    if re.fullmatch(r"\d{14}", value):
        result = datetime.strptime(value, "%Y%m%d%H%M%S")
    elif re.fullmatch(r"\d{8} \d{2}:\d{2}:\d{2}", value):
        result = datetime.strptime(value, "%Y%m%d %H:%M:%S")
    elif re.match(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}", value):
        result = datetime.fromisoformat(value)
    else:
        raise ValueError("trade_time必须包含完整日期和时间，不以接收时间替代")
    return _aware(result.replace(tzinfo=SHANGHAI) if result.tzinfo is None else result)


def parse_rt_quote(row, received_at, daily_limit=None):
    """Keep rt_k share units and the supplier timestamp, including stale data."""
    symbol = _symbol(row["ts_code"])
    _board(symbol)
    received_at = _aware(received_at)
    timestamp = _timestamp(row["trade_time"])
    last, previous, opening, high, low = (_decimal(row[key]) for key in ("close", "pre_close", "open", "high", "low"))
    bid, ask = (_decimal(row[key], zero=True) for key in ("bid_price1", "ask_price1"))
    if not isinstance(row.get("name"), str) or not row["name"].strip():
        raise ValueError("Tushare证券名称缺失")
    if low > high or not low <= last <= high or not low <= opening <= high or (bid and ask and bid > ask):
        raise ValueError("Tushare价格或盘口不一致")
    upper = lower = None
    if daily_limit is not None and timestamp.date() == received_at.date():
        if daily_limit["date"] != timestamp.date().isoformat() or daily_limit["symbol"] != symbol:
            raise ValueError("涨跌停价格与当前证券或交易日不一致")
        upper, lower = _decimal(daily_limit["up_limit"]), _decimal(daily_limit["down_limit"])
        if not lower < upper or not lower <= last <= upper or previous != _decimal(daily_limit["pre_close"]):
            raise ValueError("实时行情与当日实际涨跌停基准不一致")
    return Quote(symbol=symbol, name=row["name"].strip(), last=last, bid=bid, ask=ask,
                 bid_size=_integer(row["bid_volume1"]), ask_size=_integer(row["ask_volume1"]),
                 previous_close=previous, open=opening, high=high, low=low, volume=_integer(row["vol"]),
                 timestamp=timestamp, limit_up=upper, limit_down=lower,
                 source="tushare:rt_k", received_at=received_at)


class TushareMarketFeed(MarketFeed):
    def __init__(self, cfg, *, client=None, now=None, token_loader=None, sleeper=None, monotonic=None):
        # Shared session-window and timezone logic only. All data-fetching
        # methods below are overridden, so this never invokes BaoStock/Tencent.
        super().__init__(cfg, client=client, now=now)
        self.cfg = cfg
        self.settings = cfg.get("tushare", {})
        self.directory = self.directory / "tushare"
        self._token_loader = token_loader
        self._http = None
        self._http_token = None
        self._sleep = sleeper or time.sleep
        self._monotonic = monotonic or time.monotonic
        self._limits = None
        self.status.update({"provider": "tushare", "source_label": self.config.get("source_label", "Tushare实时行情")})

    def _bs(self, *args, **kwargs):
        raise RuntimeError("Tushare行情源禁止回退BaoStock")

    def _client(self):
        if self.client is not None:  # Explicit dependency injection for tests.
            return self.client
        try:
            if self._token_loader is None:
                from .data_settings import tushare_connection
                token = tushare_connection(self.cfg)
            else:
                token = self._token_loader(self.cfg)
            if not isinstance(token, str) or not token.strip():
                raise ValueError("missing credential")
        except Exception:
            self.status["auth"] = {"configured": False, "error": "Tushare Token未配置或本机凭据不可读，请先在数据源设置中配置"}
            raise ValueError(self.status["auth"]["error"]) from None
        self.status["auth"] = {"configured": True}
        if token != self._http_token:
            self._http = TushareHttpClient(token, self.timeout)
            self._http_token = token
        return self._http

    def _call(self, endpoint, fields, **params):
        global _LAST_REQUEST
        client = self._client()  # Re-read managed credentials before every call.
        interval = float(self.settings.get("min_call_interval_seconds", 0.25))
        if not 0 < interval <= 60:
            raise ValueError("Tushare最小调用间隔配置无效")
        try:
            with _REQUEST_LOCK:
                current = self._monotonic()
                if _LAST_REQUEST is not None:
                    delay = interval - (current - _LAST_REQUEST)
                    if delay > 0:
                        self._sleep(delay)
                _LAST_REQUEST = self._monotonic()
                result = client.query(endpoint, fields=",".join(fields), **params)
            if not set(fields) <= set(result.columns):
                raise ValueError("required fields missing")
            rows = result.to_dict("records")
            if not isinstance(rows, list) or any(not isinstance(row, dict) or not set(fields) <= set(row) for row in rows):
                raise ValueError("invalid response shape")
            self.status.setdefault("requests", {})[endpoint] = {"ok": True}
            return rows
        except Exception as exc:
            hint = "；rt_k需另行开通A股日线RT权限，并返回盘口及trade_time字段" if endpoint == "rt_k" else "；检查网络及对应接口权限"
            if isinstance(exc, TushareRequestError) and exc.category == 'permission_denied':
                hint = '；当前Token没有该接口访问权限' + hint
            message = "Tushare " + endpoint + "请求失败或返回字段不完整" + hint
            self.status.setdefault("requests", {})[endpoint] = {"ok": False, "error": message}
            raise ValueError(message) from None

    def _read_cache(self, name):
        payload = super()._read_cache(name)
        if payload is None or not payload.get("source", "").startswith("tushare:"):
            return None
        return payload

    def _basic(self, force=False):
        payload = self._basics or self._read_cache("stock_basic.json")
        records = payload.get("records") if payload else None
        valid = isinstance(records, list) and bool(records) and all(isinstance(row, dict) and set(BASIC_FIELDS) <= set(row) for row in records)
        age = (self._clock() - datetime.fromisoformat(payload["fetched_at"])).total_seconds() if valid else None
        if valid and not force and 0 <= age < float(self.config.get("universe_refresh_seconds", 21600)):
            self._basics = payload
            self.status["stock_basic"] = {"source": payload["source"], "fetched_at": payload["fetched_at"], "cache_stale": False}
            return {row["ts_code"]: row for row in records}
        try:
            rows = []
            # Each exchange is below the documented 6000-row response ceiling.
            for exchange, suffix in (("SSE", ".SH"), ("SZSE", ".SZ")):
                batch = self._call("stock_basic", BASIC_FIELDS, exchange=exchange, list_status="L")
                if any(not isinstance(row["ts_code"], str) or not row["ts_code"].endswith(suffix) or row["exchange"] != exchange for row in batch):
                    raise ValueError("Tushare证券资料交易所不一致")
                rows.extend(batch)
            if not rows or len({row["ts_code"] for row in rows}) != len(rows):
                raise ValueError("Tushare证券资料为空或重复")
            payload = {"schema_version": 1, "source": "tushare:stock_basic", "fetched_at": self._clock().isoformat(), "records": rows}
            atomic_json(self.directory / "stock_basic.json", payload)
            self._basics = payload
            self.status["stock_basic"] = {"source": payload["source"], "fetched_at": payload["fetched_at"], "cache_stale": False}
            return {row["ts_code"]: row for row in rows}
        except Exception as exc:
            self.status["stock_basic"] = {"source": "tushare:stock_basic", "cache_stale": True, "last_error": str(exc)[:200]}
            if valid:
                return {row["ts_code"]: row for row in records}
            raise

    def _instrument(self, row):
        symbol = _symbol(row["ts_code"])
        listed = _iso_date(row["list_date"])
        if listed > self._clock().date().isoformat() or not isinstance(row["name"], str) or not row["name"].strip():
            raise ValueError("Tushare上市日期或证券名称无效")
        return Instrument(symbol, row["name"].strip(), _board(symbol), listed, row.get("industry") or "unknown")

    def universe(self, force=False):
        with self._lock:
            now = self._clock()
            index_code = self.settings.get("index_code", "000300.SH")
            if index_code != "000300.SH":
                raise ValueError("当前Tushare股票池只支持沪深300指数000300.SH")
            max_age = int(self.settings.get("index_max_age_days", 45))
            payload = self._universe or self._read_cache("universe.json")
            cached = self._cached_instruments(payload)
            if cached and (payload.get("index_code") != index_code or payload.get("member_count") != 300):
                cached = None
            age = (now - datetime.fromisoformat(payload["fetched_at"])).total_seconds() if cached else None
            effective_age = (now.date() - date.fromisoformat(payload["effective_date"])).days if cached else None
            if (cached and not force and not payload.get("metadata_stale") and 0 <= effective_age <= max_age
                    and 0 <= age < float(self.config.get("universe_refresh_seconds", 21600))):
                self._universe = payload
                self.status["universe"] = {key: payload[key] for key in ("source", "effective_date", "fetched_at", "member_count")}
                self.status["universe"].update({"count": len(cached), "cache_stale": False, "last_error": None,
                                                 "note": "Tushare月度指数权重快照，不是每日实时成分"})
                return cached
            try:
                rows = self._call("index_weight", INDEX_FIELDS, index_code=index_code,
                                  start_date=(now.date() - timedelta(days=max_age)).strftime("%Y%m%d"),
                                  end_date=now.strftime("%Y%m%d"))
                snapshots = {}
                for row in rows:
                    if row["index_code"] != index_code:
                        raise ValueError("Tushare返回了错误指数的成分")
                    day = _iso_date(row["trade_date"])
                    if day <= now.date().isoformat():
                        snapshots.setdefault(day, []).append(row)
                if not snapshots:
                    raise ValueError("Tushare未返回截至当前日期的沪深300权重")
                effective = max(snapshots)
                members = snapshots[effective]
                if (now.date() - date.fromisoformat(effective)).days > max_age:
                    raise ValueError("沪深300指数权重快照已超过允许天数")
                symbols = [row["con_code"] for row in members]
                if len(members) != 300 or len(set(symbols)) != 300:
                    raise ValueError("最新沪深300快照必须恰有300个不重复成分，拒绝残缺快照")
                for row in members:
                    _symbol(row["con_code"])
                    _decimal(row["weight"])
                basics = self._basic(force=force)
                if not set(symbols) <= set(basics):
                    raise ValueError("沪深300成分缺少真实证券基础资料")
                instruments = {}
                for symbol in symbols:
                    row = basics[symbol]
                    try:
                        _board(symbol)
                    except ValueError:
                        continue  # Count the complete index before ordinary-A-share eligibility.
                    instrument = self._instrument(row)
                    if row["list_status"] != "L" or row["delist_date"]:
                        continue
                    if (now.date() - date.fromisoformat(instrument.listed_date)).days < int(self.config.get("min_listing_days", 180)):
                        continue
                    if re.match(r"\*?ST|退", instrument.name.replace(" ", "").upper()):
                        continue
                    instruments[symbol] = instrument
                if not instruments:
                    raise ValueError("沪深300快照中没有合格普通A股")
                stale = self.status.get("stock_basic", {}).get("cache_stale", True)
                payload = {"schema_version": 1, "source": "tushare:index_weight+stock_basic", "index_code": index_code,
                           "effective_date": effective, "fetched_at": self._clock().isoformat(), "member_count": 300,
                           "metadata_stale": stale, "min_listing_days": int(self.config.get("min_listing_days", 180)),
                           "instruments": [asdict(row) for row in instruments.values()]}
                atomic_json(self.directory / "universe.json", payload)
                self._universe = payload
                self.status["universe"] = {key: payload[key] for key in ("source", "effective_date", "fetched_at", "member_count")}
                self.status["universe"].update({"count": len(instruments), "cache_stale": stale,
                                                 "note": "Tushare月度指数权重快照，不是每日实时成分"})
                return instruments
            except Exception as exc:
                self.status["universe"] = {"source": "tushare:index_weight+stock_basic", "count": len(cached or {}),
                                           "cache_stale": True, "effective_date": payload.get("effective_date") if payload else None,
                                           "last_error": str(exc)[:220]}
                if cached:
                    return cached
                raise ValueError("Tushare沪深300股票池未就绪：" + str(exc)[:200]) from None

    def lookup(self, symbols):
        symbols = list(symbols)
        for symbol in symbols:
            _board(symbol)
        with self._lock:
            errors, result = {}, {}
            try:
                basics = self._basic()
            except Exception as exc:
                self.status["lookup"] = {"source": "tushare:stock_basic", "cache_stale": True, "last_error": str(exc)[:200]}
                return {}
            for symbol in symbols:
                try:
                    result[symbol] = self._instrument(basics[symbol])
                except (ValueError, KeyError, TypeError):
                    errors[symbol] = "Tushare未提供该持仓的有效证券资料"
            self.status["lookup"] = {"source": "tushare:stock_basic", "count": len(result), "errors": errors,
                                     "cache_stale": self.status["stock_basic"].get("cache_stale", True),
                                     "last_error": self.status["stock_basic"].get("last_error")}
            return result

    def _daily_limits(self, symbols, now):
        today = now.date().isoformat()
        payload = self._limits or self._read_cache("limits.json")
        values = payload.get("values", {}) if payload and payload.get("date") == today else {}
        age = (now - datetime.fromisoformat(payload["fetched_at"])).total_seconds() if payload and values else None
        if values and (set(symbols) <= set(values) or (age is not None and 0 <= age < 60)):
            self._limits = payload
            return values
        try:
            rows = self._call("stk_limit", LIMIT_FIELDS, trade_date=_compact(today))
            limits, invalid, seen = {}, {}, set()
            for row in rows:
                symbol = row["ts_code"]
                try:
                    _board(symbol)
                except ValueError:
                    continue  # The endpoint also serves B shares and funds.
                if symbol in seen:
                    limits.pop(symbol, None)
                    invalid[symbol] = "Tushare当日限价证券重复"
                    continue
                seen.add(symbol)
                try:
                    if _iso_date(row["trade_date"]) != today:
                        raise ValueError("涨跌停字段不属于当日")
                    lower, upper, previous = (_decimal(row[key]) for key in ("down_limit", "up_limit", "pre_close"))
                    if lower >= upper:
                        raise ValueError("Tushare实际限价无效")
                    limits[symbol] = {"date": today, "symbol": symbol, "down_limit": str(lower),
                                      "up_limit": str(upper), "pre_close": str(previous)}
                except (ValueError, TypeError, KeyError, ArithmeticError):
                    # IPOs without price limits and malformed unrelated rows do
                    # not invalidate the actual limits of the other securities.
                    invalid[symbol] = "该证券未返回有效的当日实际限价"
            payload = {"schema_version": 1, "source": "tushare:stk_limit", "date": today,
                       "fetched_at": self._clock().isoformat(), "values": limits}
            # Empty results are never cache hits: a later post-09:00 poll retries.
            atomic_json(self.directory / "limits.json", payload)
            self._limits = payload
            self.status["limits"] = {"source": "tushare:stk_limit", "date": today, "count": len(limits),
                                     "errors": {symbol: invalid[symbol] for symbol in symbols if symbol in invalid},
                                     "missing_symbols": sorted(set(symbols) - set(limits))}
            return limits
        except Exception as exc:
            self.status["limits"] = {"source": "tushare:stk_limit", "date": today, "last_error": str(exc)[:200],
                                     "missing_symbols": sorted(set(symbols) - set(values))}
            return values  # Only the same day's Tushare cache can be retained.

    def quotes(self, symbols):
        symbols = list(symbols)
        if len(symbols) != len(set(symbols)):
            raise ValueError("报价证券不能重复")
        for symbol in symbols:
            _board(symbol)
        now = self._clock()
        result, errors, stale = {}, {}, []
        if not symbols:
            self.status["quotes"] = {"source": "tushare:rt_k", "requested": 0, "received": 0, "errors": {}}
            return {}
        limits = self._daily_limits(symbols, now)
        batch_size = max(1, min(500, int(self.settings.get("query_batch_size", 80))))
        for start in range(0, len(symbols), batch_size):
            batch = symbols[start:start + batch_size]
            try:
                rows = self._call("rt_k", RT_FIELDS, ts_code=",".join(batch))
                received = self._clock()
                if any(row["ts_code"] not in batch for row in rows):
                    raise ValueError("Tushare实时行情返回未请求的证券")
                seen = set()
                for row in rows:
                    symbol = row["ts_code"]
                    if symbol in seen:
                        result.pop(symbol, None)
                        errors[symbol] = "Tushare实时行情证券重复"
                        continue
                    seen.add(symbol)
                    try:
                        quote = parse_rt_quote(row, received, limits.get(symbol))
                        age = (received - quote.timestamp).total_seconds()
                        if age < -float(self.config.get("max_future_seconds", 3)):
                            raise ValueError("Tushare快照时间晚于接收时间")
                        if age > float(self.config.get("max_quote_age_seconds", 30)):
                            stale.append(symbol)
                        if symbol not in errors:
                            result[symbol] = quote
                    except (ValueError, KeyError, ArithmeticError, TypeError):
                        errors[symbol] = "Tushare实时报价价格、盘口、单位、时间或当日限价校验未通过"
                for symbol in set(batch) - seen:
                    errors[symbol] = "Tushare未返回该证券的实时行情"
            except Exception as exc:
                for symbol in batch:
                    errors[symbol] = str(exc)[:220]
        self.status["quotes"] = {"source": "tushare:rt_k", "requested": len(symbols), "received": len(result),
                                 "fetched_at": self._clock().isoformat(), "cache_stale": bool(stale),
                                 "stale_symbols": stale, "errors": errors,
                                 "missing_limit_symbols": sorted(symbol for symbol, quote in result.items()
                                                                  if quote.limit_up is None or quote.limit_down is None),
                                 "last_error": next(iter(errors.values()), self.status.get("limits", {}).get("last_error")),
                                 "note": "rt_k成交量及盘口数量均为股；无当日stk_limit实际限价时不授权成交"}
        return result

    def _load_calendar(self, now):
        with self._lock:
            today = now.date().isoformat()
            payload = self._calendar or self._read_cache("calendar.json")
            dates = payload.get("dates", {}) if payload else {}
            try:
                if not isinstance(dates, dict) or any(_day(day) != day or type(value) is not bool for day, value in dates.items()):
                    dates = {}
            except (ValueError, TypeError):
                dates = {}
            age = (now - datetime.fromisoformat(payload["fetched_at"])).total_seconds() if payload and dates else None
            if today in dates and 0 <= age < float(self.config.get("calendar_refresh_seconds", 86400)):
                self._calendar = payload
                self.status["calendar"] = {"source": "tushare:trade_cal", "fetched_at": payload["fetched_at"], "cache_stale": False}
                return dates
            try:
                start, end = now.date() - timedelta(days=7), now.date() + timedelta(days=45)
                rows = self._call("trade_cal", CALENDAR_FIELDS, exchange="SSE", start_date=_compact(start.isoformat()), end_date=_compact(end.isoformat()))
                values = {}
                for row in rows:
                    day = _iso_date(row["cal_date"])
                    if (row["exchange"] != "SSE" or day in values or not start.isoformat() <= day <= end.isoformat()
                            or str(row["is_open"]) not in ("0", "1")):
                        raise ValueError("Tushare交易日历不一致")
                    values[day] = str(row["is_open"]) == "1"
                if today not in values:
                    raise ValueError("Tushare交易日历未核实当前日期")
                payload = {"schema_version": 1, "source": "tushare:trade_cal", "fetched_at": self._clock().isoformat(), "dates": values}
                atomic_json(self.directory / "calendar.json", payload)
                self._calendar = payload
                self.status["calendar"] = {"source": "tushare:trade_cal", "fetched_at": payload["fetched_at"], "cache_stale": False}
                return values
            except Exception as exc:
                self.status["calendar"] = {"source": "tushare:trade_cal", "cache_stale": True, "last_error": str(exc)[:220]}
                return dates

    def session(self, now):
        result = super().session(now)
        result["calendar_source"] = "tushare:trade_cal"
        return result
