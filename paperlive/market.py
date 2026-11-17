"""Current HS300 metadata and timestamped public quotes for paper trading.

No credentials, brokerage calls, inferred trading holidays or derived limit
prices are used. Cached constituent lists are current snapshots, not historical
index membership. An old quote remains visible but is never made fresh locally.
"""
from contextlib import redirect_stdout
from dataclasses import asdict
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
import io
import json
from pathlib import Path
import re
import socket
import threading
import urllib.request

from autotrade.monitoring import atomic_json
from autotrade.rules import board_for_symbol
from .models import Instrument, Quote

ROOT = Path(__file__).resolve().parent.parent
SHANGHAI = timezone(timedelta(hours=8))
_BAOSTOCK_LOCK = threading.RLock()
_QUOTE_ORIGIN = "https://qt.gtimg.cn"
_STATEMENT = re.compile(r'v_((?:sh|sz)\d{6})="([^"\r\n]*)"\s*;?')


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise ValueError("quote redirects are not accepted")


def _aware(value):
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("market clock must be timezone aware")
    return value.astimezone(SHANGHAI)


def _day(value):
    if not isinstance(value, str) or date.fromisoformat(value).isoformat() != value:
        raise ValueError("invalid market date")
    return value


def _symbol(value):
    if not isinstance(value, str) or not re.fullmatch(r"\d{6}\.(SH|SZ)", value):
        raise ValueError("only ordinary Shanghai/Shenzhen A-share symbols are supported")
    return value


def _board(symbol):
    board = board_for_symbol(_symbol(symbol))
    if board not in {"MAIN", "CHINEXT", "STAR"}:
        raise ValueError("unsupported ordinary A-share board")
    return board


def _from_bs(value):
    if not isinstance(value, str) or not re.fullmatch(r"(?:sh|sz)\.\d{6}", value):
        raise ValueError("invalid constituent symbol")
    return value[3:] + "." + value[:2].upper()


def _wire(symbol):
    code, exchange = _symbol(symbol).split(".")
    return exchange.lower() + code


def _decimal(value, zero=False):
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise ValueError("invalid quote number") from None
    if not number.is_finite() or number < 0 or (not zero and number == 0):
        raise ValueError("invalid quote number")
    return number


def _shares(value):
    shares = _decimal(value, zero=True) * 100
    if shares != shares.to_integral_value():
        raise ValueError("quote lot quantity does not resolve to whole shares")
    return int(shares)


def _cumulative_shares(fields, last, low, high):
    """Resolve Tencent's mixed share/lot counter from its exact turnover in yuan."""
    summary = fields[35].split("/") if len(fields) > 36 else []
    if len(summary) != 3:
        raise ValueError("missing supplier turnover summary")
    raw = _decimal(fields[6], zero=True)
    amount = _decimal(summary[2], zero=True)
    if (raw != raw.to_integral_value() or raw != _decimal(fields[36], zero=True)
            or raw != _decimal(summary[1], zero=True) or last != _decimal(summary[0])):
        raise ValueError("inconsistent supplier turnover fields")
    if raw == amount == 0:
        return 0
    if raw == 0 or amount == 0:
        raise ValueError("supplier cumulative volume and turnover disagree")

    # Each traded share contributed between low and high yuan to the amount.
    # The lot counter omits any final partial lot; do not report guessed shares.
    lots = raw * 100
    uses_shares = low * raw <= amount <= high * raw
    uses_lots = low * lots <= amount <= high * (lots + 99)
    if uses_shares == uses_lots:
        raise ValueError("supplier cumulative volume unit cannot be verified")
    return int(raw if uses_shares else lots)


def parse_tencent_quote(symbol, payload, received_at):
    """Validate one supplier record without replacing missing prices or limits."""
    symbol = _symbol(symbol)
    received_at = _aware(received_at)
    fields = payload.split("~")
    if len(fields) < 35 or fields[2] != symbol[:6] or not fields[1].strip():
        raise ValueError("missing quote fields or mismatched stock code")
    market_code = "1" if symbol.endswith(".SH") else "51"
    if fields[0] != market_code:
        raise ValueError("quote exchange does not match requested symbol")
    stamp = fields[30]
    if not re.fullmatch(r"\d{14}", stamp):
        raise ValueError("invalid supplier quote timestamp")
    timestamp = datetime.strptime(stamp, "%Y%m%d%H%M%S").replace(tzinfo=SHANGHAI)
    last, previous, opening, high, low = (_decimal(fields[i]) for i in (3, 4, 5, 33, 34))
    bid, ask = (_decimal(fields[i], zero=True) for i in (9, 19))
    if low > high or not low <= last <= high or not low <= opening <= high:
        raise ValueError("inconsistent supplier OHLC prices")
    if bid and ask and bid > ask:
        raise ValueError("crossed supplier order book")

    def limit(index):
        if len(fields) <= index or fields[index] in ("", "0", "0.00", "-", "--"):
            return None
        return _decimal(fields[index])

    upper, lower = limit(47), limit(48)
    if (upper is not None and last > upper) or (lower is not None and last < lower):
        raise ValueError("last price is outside supplier daily limits")
    if upper is not None and lower is not None and lower >= upper:
        raise ValueError("invalid supplier daily limits")
    return Quote(symbol=symbol, name=fields[1].strip(), last=last, bid=bid, ask=ask,
                 bid_size=_shares(fields[10]), ask_size=_shares(fields[20]),
                 previous_close=previous, open=opening, high=high, low=low,
                 volume=_cumulative_shares(fields, last, low, high), timestamp=timestamp,
                 limit_up=upper, limit_down=lower, source="tencent", received_at=received_at)


class MarketFeed:
    def __init__(self, config, *, client=None, opener=None, now=None):
        if config.get("mode") != "paper":
            raise ValueError("public market feed is enabled only for paper mode")
        self.config = config.get("market", {})
        if self.config.get("universe", "hs300") != "hs300":
            raise ValueError("only the current HS300 universe is supported")
        runtime = Path(config.get("runtime_dir", "runtime/paperlive"))
        self.directory = (runtime if runtime.is_absolute() else ROOT / runtime) / "market"
        self.timeout = float(self.config.get("request_timeout", 10))
        if not 0 < self.timeout <= 120:
            raise ValueError("market request_timeout must be in (0,120]")
        self.client = client
        self.opener = opener or urllib.request.build_opener(_NoRedirect(), urllib.request.ProxyHandler({}))
        self.now = now or (lambda: datetime.now(SHANGHAI))
        self._lock = threading.RLock()
        self._universe = None
        self._basics = None
        self._calendar = None
        self.status = {"universe": {}, "calendar": {}, "quotes": {}}

    def _clock(self):
        return _aware(self.now())

    def _bs(self, requests):
        """One anonymous session, serialized across all MarketFeed instances."""
        with _BAOSTOCK_LOCK:
            if self.client is None:
                import baostock
                self.client = baostock
            previous_timeout = socket.getdefaulttimeout()
            logged_in = False
            try:
                socket.setdefaulttimeout(self.timeout)
                with redirect_stdout(io.StringIO()):
                    result = self.client.login()
                if str(result.error_code) != "0":
                    raise ValueError("anonymous login failed")
                logged_in = True
                responses = []
                for name, required, args in requests:
                    result = getattr(self.client, name)(**args)
                    fields = list(result.fields)
                    if str(result.error_code) != "0" or not set(required) <= set(fields) or len(set(fields)) != len(fields):
                        raise ValueError("provider response is missing required fields")
                    rows = []
                    while result.next():
                        values = result.get_row_data()
                        if len(values) != len(fields):
                            raise ValueError("malformed provider row")
                        rows.append(dict(zip(fields, values)))
                    if str(result.error_code) != "0":
                        raise ValueError("incomplete provider response")
                    responses.append(rows)
                return responses
            except Exception as exc:
                raise ValueError("BaoStock metadata/calendar request failed: " + str(exc)[:140]) from None
            finally:
                if logged_in:
                    try:
                        with redirect_stdout(io.StringIO()):
                            self.client.logout()
                    except Exception:
                        pass
                socket.setdefaulttimeout(previous_timeout)

    def _read_cache(self, filename):
        try:
            payload = json.loads((self.directory / filename).read_text(encoding="utf-8"))
            if payload.get("schema_version") != 1:
                return None
            if not isinstance(payload.get("source"), str) or not payload["source"]:
                return None
            _aware(datetime.fromisoformat(payload["fetched_at"]))
            return payload
        except (OSError, ValueError, TypeError, KeyError, AttributeError):
            return None

    def _cached_instruments(self, payload):
        if payload is None or payload.get("min_listing_days") != int(self.config.get("min_listing_days", 180)):
            return None
        try:
            _day(payload["effective_date"])
            instruments = {}
            for row in payload["instruments"]:
                instrument = Instrument(**row)
                if instrument.symbol in instruments or _board(instrument.symbol) != instrument.board:
                    return None
                _day(instrument.listed_date)
                instruments[instrument.symbol] = instrument
            return instruments or None
        except (KeyError, TypeError, ValueError):
            return None

    def universe(self, force=False):
        with self._lock:
            now = self._clock()
            payload = self._universe or self._read_cache("universe.json")
            cached = self._cached_instruments(payload)
            ttl = float(self.config.get("universe_refresh_seconds", 21600))
            age = (now - datetime.fromisoformat(payload["fetched_at"])).total_seconds() if cached else None
            if cached and not force and 0 <= age < ttl:
                self._universe = payload
                self.status["universe"] = {k: payload[k] for k in ("source", "effective_date", "fetched_at")}
                self.status["universe"].update({"cache_stale": False, "count": len(cached), "last_error": None})
                return cached
            try:
                members, basics = self._bs([
                    ("query_hs300_stocks", ["updateDate", "code", "code_name"], {}),
                    ("query_stock_basic", ["code", "code_name", "ipoDate", "outDate", "type", "status"], {})])
                if not members:
                    raise ValueError("empty HS300 membership response")
                names, effective_dates = {}, set()
                for row in members:
                    symbol = _from_bs(row["code"])
                    if symbol in names:
                        raise ValueError("duplicate HS300 member")
                    names[symbol] = row["code_name"]
                    effective_dates.add(_day(row["updateDate"]))
                if len(effective_dates) != 1 or max(effective_dates) > now.date().isoformat():
                    raise ValueError("invalid constituent effective date")
                metadata = {}
                for row in basics:
                    if row["code"] in metadata:
                        raise ValueError("duplicate security metadata")
                    metadata[row["code"]] = row
                self._basics = {"schema_version": 1, "source": "baostock:stock_basic:current",
                                "fetched_at": self._clock().isoformat(), "records": basics}
                atomic_json(self.directory / "stock_basic.json", self._basics)
                instruments = {}
                min_days = int(self.config.get("min_listing_days", 180))
                for symbol, name in names.items():
                    row = metadata.get(symbol[-2:].lower() + "." + symbol[:6])
                    if row is None:
                        raise ValueError("security metadata missing for HS300 member")
                    if row["type"] != "1" or row["status"] != "1" or row["outDate"]:
                        continue
                    if (now.date() - date.fromisoformat(_day(row["ipoDate"]))).days < min_days:
                        continue
                    if re.match(r"\*?ST|退", (row["code_name"] or name).replace(" ", "").upper()):
                        continue
                    try:
                        board = _board(symbol)
                    except ValueError:
                        continue
                    instruments[symbol] = Instrument(symbol, row["code_name"] or name, board, row["ipoDate"])
                if not instruments:
                    raise ValueError("no eligible ordinary A-share constituents")
                payload = {"schema_version": 1, "source": "baostock:hs300+stock_basic:current",
                           "effective_date": next(iter(effective_dates)), "fetched_at": self._clock().isoformat(),
                           "min_listing_days": min_days, "member_count": len(members),
                           "instruments": [asdict(row) for row in instruments.values()]}
                atomic_json(self.directory / "universe.json", payload)
                self._universe = payload
                self.status["universe"] = {k: payload[k] for k in ("source", "effective_date", "fetched_at", "member_count")}
                self.status["universe"].update({"cache_stale": False, "count": len(instruments), "last_error": None})
                return instruments
            except Exception as exc:
                self.status["universe"] = {"source": "baostock:hs300+stock_basic:current", "cache_stale": True,
                                           "effective_date": payload.get("effective_date") if payload else None,
                                           "fetched_at": payload.get("fetched_at") if payload else None,
                                           "count": len(cached or {}), "last_error": str(exc)[:200]}
                if cached:
                    return cached
                raise ValueError("HS300 universe unavailable: " + str(exc)[:160]) from None

    def lookup(self, symbols):
        """Resolve held securities independently of today's buy-candidate pool.

        Index deletion, becoming ST or having a short listing history must not
        erase a held position's identity. This is metadata, not a claim that a
        suspended/delisted security has an executable market.
        """
        symbols = list(symbols)
        for symbol in symbols:
            _board(symbol)
        with self._lock:
            now = self._clock()
            payload = self._basics or self._read_cache("stock_basic.json")
            records = payload.get("records") if payload else None
            if not isinstance(records, list) or not all(isinstance(row, dict) for row in records):
                records = None
            age = (now - datetime.fromisoformat(payload["fetched_at"])).total_seconds() if records is not None else None
            error = None
            if records is None or age is None or not 0 <= age < float(self.config.get("universe_refresh_seconds", 21600)):
                try:
                    rows = self._bs([("query_stock_basic", ["code", "code_name", "ipoDate", "outDate", "type", "status"], {})])[0]
                    if not rows or len({row["code"] for row in rows}) != len(rows):
                        raise ValueError("empty or duplicate security metadata")
                    payload = {"schema_version": 1, "source": "baostock:stock_basic:current",
                               "fetched_at": self._clock().isoformat(), "records": rows}
                    atomic_json(self.directory / "stock_basic.json", payload)
                    self._basics, records = payload, rows
                except Exception as exc:
                    error = str(exc)[:200]
            result, errors = {}, {}
            metadata = {row.get("code"): row for row in (records or [])}
            for symbol in symbols:
                row = metadata.get(symbol[-2:].lower() + "." + symbol[:6])
                try:
                    if row is None or row.get("type") != "1" or not row.get("code_name"):
                        raise ValueError("ordinary-stock metadata unavailable")
                    listed = _day(row["ipoDate"])
                    if listed > now.date().isoformat():
                        raise ValueError("security listing date is in the future")
                    result[symbol] = Instrument(symbol, row["code_name"], _board(symbol), listed)
                except (KeyError, ValueError) as exc:
                    errors[symbol] = str(exc)
            self.status["lookup"] = {"source": "baostock:stock_basic:current", "cache_stale": error is not None,
                                     "fetched_at": payload.get("fetched_at") if payload else None,
                                     "last_error": error, "errors": errors, "count": len(result)}
            return result

    def quotes(self, symbols):
        symbols = list(symbols)
        if len(symbols) != len(set(symbols)):
            raise ValueError("quote symbols must be unique")
        for symbol in symbols:
            _board(symbol)
        result, errors, stale = {}, {}, []
        batch_size = max(1, min(100, int(self.config.get("quote_batch_size", 80))))
        for start in range(0, len(symbols), batch_size):
            batch = symbols[start:start + batch_size]
            expected = {_wire(symbol): symbol for symbol in batch}
            url = _QUOTE_ORIGIN + "/q=" + ",".join(expected)
            try:
                request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0", "Referer": "https://gu.qq.com/"})
                with self.opener.open(request, timeout=self.timeout) as response:
                    if response.geturl() != url:
                        raise ValueError("unexpected quote response URL")
                    raw = response.read(4_000_001)
                    if len(raw) > 4_000_000:
                        raise ValueError("quote response too large")
                received = self._clock()
                text = raw.decode("gb18030")
                records = list(_STATEMENT.finditer(text))
                if any(record.group(1) not in expected for record in records):
                    raise ValueError("quote response contains an unrequested symbol")
                seen = set()
                for record in records:
                    symbol = expected[record.group(1)]
                    if symbol in seen:
                        result.pop(symbol, None)
                        errors[symbol] = "duplicate supplier quote"
                        continue
                    seen.add(symbol)
                    try:
                        quote = parse_tencent_quote(symbol, record.group(2), received)
                        age = (received - quote.timestamp).total_seconds()
                        if age < -float(self.config.get("max_future_seconds", 3)):
                            raise ValueError("supplier quote timestamp is in the future")
                        if age > float(self.config.get("max_quote_age_seconds", 30)):
                            stale.append(symbol)
                        if symbol not in errors:
                            result[symbol] = quote
                    except (ValueError, InvalidOperation) as exc:
                        errors[symbol] = str(exc)
                for symbol in batch:
                    if symbol not in seen:
                        errors[symbol] = "supplier did not return this symbol"
            except Exception as exc:
                for symbol in batch:
                    errors[symbol] = str(exc)[:180]
        self.status["quotes"] = {"source": "tencent:qt.gtimg.cn", "requested": len(symbols), "received": len(result),
                                 "fetched_at": self._clock().isoformat(), "stale_symbols": stale,
                                 "cache_stale": bool(stale), "errors": errors,
                                 "last_error": next(iter(errors.values()), None),
                                 "note": "公开快照；盘口不代表已核验真实停牌状态或排队成交顺序"}
        return result

    def _load_calendar(self, now):
        with self._lock:
            payload = self._calendar or self._read_cache("calendar.json")
            today = now.date().isoformat()
            dates = {}
            if payload:
                try:
                    dates = {_day(key): value for key, value in payload["dates"].items()}
                    if any(type(value) is not bool for value in dates.values()):
                        dates = {}
                except (KeyError, ValueError, AttributeError):
                    dates = {}
            age = (now - datetime.fromisoformat(payload["fetched_at"])).total_seconds() if payload and dates else None
            if today in dates and age is not None and 0 <= age < float(self.config.get("calendar_refresh_seconds", 86400)):
                self._calendar = payload
                self.status["calendar"] = {"source": payload["source"], "fetched_at": payload["fetched_at"],
                                           "cache_stale": False, "last_error": None}
                return dates
            try:
                start, end = (now.date() - timedelta(days=7)), (now.date() + timedelta(days=45))
                rows = self._bs([("query_trade_dates", ["calendar_date", "is_trading_day"],
                                  {"start_date": start.isoformat(), "end_date": end.isoformat()})])[0]
                values = {}
                for row in rows:
                    day = _day(row["calendar_date"])
                    if day in values or not start.isoformat() <= day <= end.isoformat() or row["is_trading_day"] not in ("0", "1"):
                        raise ValueError("invalid exchange calendar row")
                    values[day] = row["is_trading_day"] == "1"
                if today not in values:
                    raise ValueError("exchange calendar did not verify the current day")
                payload = {"schema_version": 1, "source": "baostock:query_trade_dates",
                           "fetched_at": self._clock().isoformat(), "dates": values}
                atomic_json(self.directory / "calendar.json", payload)
                self._calendar = payload
                self.status["calendar"] = {"source": payload["source"], "fetched_at": payload["fetched_at"],
                                           "cache_stale": False, "last_error": None}
                return values
            except Exception as exc:
                self.status["calendar"] = {"source": "baostock:query_trade_dates", "cache_stale": True,
                                           "fetched_at": payload.get("fetched_at") if payload else None,
                                           "last_error": str(exc)[:200]}
                # Cached dates are explicit exchange facts. Never extrapolate a
                # missing date from its weekday, even if the service is down.
                return dates

    def session(self, now):
        now = _aware(now)
        dates = self._load_calendar(now)
        today = now.date().isoformat()
        verified = today in dates
        windows = []
        for start, end in self.config.get("trading_sessions", [["09:30", "11:30"], ["13:00", "14:57"]]):
            left, right = time.fromisoformat(start), time.fromisoformat(end)
            if left >= right or left.tzinfo is not None or right.tzinfo is not None:
                raise ValueError("invalid continuous-trading session window")
            windows.append((left, right))
        if not windows or windows != sorted(windows) or any(a[1] > b[0] for a, b in zip(windows, windows[1:])):
            raise ValueError("invalid/overlapping trading sessions")
        clock = now.time().replace(tzinfo=None)
        is_open = verified and dates[today] and any(left <= clock < right for left, right in windows)
        if not verified:
            phase = "交易日历未核实"
        elif not dates[today]:
            phase = "非交易日"
        elif is_open:
            phase = "连续竞价"
        elif clock < windows[0][0]:
            phase = "盘前／非连续竞价"
        elif clock >= windows[-1][1]:
            phase = "连续竞价已结束"
        else:
            phase = "午间休市"
        following = []
        if verified:
            if dates[today]:
                following.extend(datetime.combine(now.date(), left, SHANGHAI) for left, _ in windows if left > clock)
            following.extend(datetime.combine(date.fromisoformat(day), windows[0][0], SHANGHAI)
                             for day, trading in sorted(dates.items()) if trading and day > today)
        return {"open": bool(is_open), "phase": phase, "date": today,
                "next_open": min(following).isoformat() if following else None,
                "calendar_verified": verified, "calendar_source": "baostock:query_trade_dates",
                "calendar_cache_stale": self.status["calendar"].get("cache_stale", True),
                "error": self.status["calendar"].get("last_error")}
