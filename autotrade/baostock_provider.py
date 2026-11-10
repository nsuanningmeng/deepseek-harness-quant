"""Public raw market data for an explicitly opted-in PAPER simulation only.

BaoStock has no exchange daily-limit fields. Derived bounds below are a
restricted simulation assumption, never a live-order authorization or an
assertion that the exchange has supplied these prices. IPO, ST, corporate
actions and unsupported markets fail closed or remain untradeable.

Primary sources checked 2026-11-10:
https://baostock.com/mainContent?file=pythonAPI.md
https://www.sse.com.cn/lawandrules/sselawsrules2025/stocks/exchange/c/c_20260424_10816482.shtml
https://docs.static.szse.cn/www/lawrules/rule/trade/W020260424690713155663.pdf
"""
from contextlib import redirect_stdout
from datetime import date, datetime, timedelta, timezone
import io
from pathlib import Path
import re
import socket

from .data import load_day, save_day
from .models import Bar, D, MarketDay, money

SOURCE = "baostock:raw;volume=shares;limits=derived-paper-only;stock_basic=current;sector=unknown;survivorship_bias=yes"
DAILY_FIELDS = "date,code,open,high,low,close,preclose,volume,adjustflag,turn,tradestatus,isST"
SHANGHAI = timezone(timedelta(hours=8))


def _iso(value):
    if not isinstance(value, str) or date.fromisoformat(value).isoformat() != value:
        raise ValueError("date must use YYYY-MM-DD")
    return value


def _board(symbol):
    if not isinstance(symbol, str) or not re.fullmatch(r"\d{6}\.(SH|SZ)", symbol):
        raise ValueError("BaoStock paper source supports only ordinary Shanghai/Shenzhen A-shares")
    code, exchange = symbol.split(".")
    if exchange == "SH" and code[:3] in {"600", "601", "603", "605"}:
        return "MAIN"
    if exchange == "SZ" and code[:3] in {"000", "001", "002", "003"}:
        return "MAIN"
    if exchange == "SZ" and code[:3] in {"300", "301"}:
        return "CHINEXT"
    raise ValueError("unsupported instrument/board for derived-limit paper simulation")


def _bs_code(symbol):
    code, exchange = symbol.split(".")
    return exchange.lower() + "." + code


class BaostockProvider:
    """Anonymous public data; derived limits require an explicit paper opt-in."""

    def __init__(self, config, client=None, now=None):
        self.config = config
        self.settings = config["data"]
        if config.get("mode") != "paper" or self.settings.get("allow_derived_limits") is not True:
            raise ValueError("BaoStock derived-limit simulation requires mode=paper and allow_derived_limits=true")
        if config.get("qmt", {}).get("enabled", False):
            raise ValueError("derived market limits cannot be combined with an enabled QMT adapter")
        self.universe = self.settings.get("universe", [])
        if not isinstance(self.universe, list) or not self.universe or len(set(self.universe)) != len(self.universe):
            raise ValueError("configure a nonempty unique universe")
        self.boards = {symbol: _board(symbol) for symbol in self.universe}
        configured = self.settings.get("derived_limit_rates", {})
        self.rates = {}
        for board in set(self.boards.values()):
            rate = D(configured.get(board))
            expected = {"MAIN": D("0.10"), "CHINEXT": D("0.20")}[board]
            if rate != expected:
                raise ValueError("derived rates must match the supported normal-stock regime")
            self.rates[board] = rate
        self.min_listing_days = max(180, int(config.get("strategy", {}).get("min_listing_days", 180)))
        self.timeout = float(self.settings.get("request_timeout", 20))
        if not 0 < self.timeout <= 120:
            raise ValueError("request_timeout must be in (0,120]")
        self.client = client
        self.now = now or (lambda: datetime.now(SHANGHAI))
        self._cal = {}
        self._basic_cache = {}
        self._bars = {}
        self._bar_ranges = []
        self._factor_ranges = []

    def _call(self, name, expected, **kwargs):
        # BaoStock uses a process-global socket. Calls are deliberately serialized
        # by the outer pipeline process lock; restore the process timeout always.
        timeout = socket.getdefaulttimeout()
        logged_in = False
        try:
            socket.setdefaulttimeout(self.timeout)
            if self.client is None:
                import baostock
                self.client = baostock
            with redirect_stdout(io.StringIO()):
                login = self.client.login()
                if str(login.error_code) != "0":
                    raise ValueError("login failed")
                logged_in = True
                result = getattr(self.client, name)(**kwargs)
                if str(result.error_code) != "0" or not set(expected) <= set(result.fields):
                    raise ValueError("provider error or missing fields")
                fields = result.fields
                if len(set(fields)) != len(fields):
                    raise ValueError("duplicate response field")
                rows = []
                while result.next():
                    values = result.get_row_data()
                    if len(values) != len(fields):
                        raise ValueError("malformed response row")
                    rows.append(dict(zip(fields, values)))
                if str(result.error_code) != "0":
                    raise ValueError("incomplete provider response")
                return rows
        except Exception:
            raise ValueError(f"BaoStock {name} failed or returned invalid data") from None
        finally:
            if logged_in:
                try:
                    with redirect_stdout(io.StringIO()):
                        self.client.logout()
                except Exception:
                    pass
            socket.setdefaulttimeout(timeout)

    def calendar(self, start, end):
        start, end = _iso(start), _iso(end)
        if start > end:
            raise ValueError("calendar start exceeds end")
        dates = []
        current = date.fromisoformat(start)
        while current <= date.fromisoformat(end):
            dates.append(current.isoformat())
            current += timedelta(days=1)
        if any(day not in self._cal for day in dates):
            rows = self._call("query_trade_dates", ["calendar_date", "is_trading_day"], start_date=start, end_date=end)
            values = {}
            for row in rows:
                day = _iso(row["calendar_date"])
                if day in values or day not in dates or row["is_trading_day"] not in {"0", "1"}:
                    raise ValueError("invalid exchange calendar response")
                values[day] = row["is_trading_day"] == "1"
            if set(values) != set(dates):
                raise ValueError("incomplete exchange calendar response")
            self._cal.update(values)
        return {"start": start, "end": end, "sessions": [day for day in dates if self._cal[day]]}

    def is_trading_day(self, trade_date):
        return trade_date in self.calendar(trade_date, trade_date)["sessions"]

    def _basic(self, symbol):
        if symbol not in self._basic_cache:
            rows = self._call("query_stock_basic", ["code", "ipoDate", "outDate", "type", "status"], code=_bs_code(symbol))
            if len(rows) != 1 or rows[0]["code"] != _bs_code(symbol):
                raise ValueError("missing or duplicate stock basic record")
            row = rows[0]
            if row["type"] != "1" or row["status"] != "1" or row["outDate"]:
                raise ValueError("only currently listed ordinary stocks are supported; historical survivorship bias applies")
            _iso(row["ipoDate"])
            self._basic_cache[symbol] = row
        return self._basic_cache[symbol]

    def _load_range(self, symbol, start, end):
        if not any(s == symbol and a <= start and end <= b for s, a, b in self._bar_ranges):
            rows = self._call("query_history_k_data_plus", DAILY_FIELDS.split(","), code=_bs_code(symbol), fields=DAILY_FIELDS,
                              start_date=start, end_date=end, frequency="d", adjustflag="3")
            seen = set()
            for row in rows:
                day = _iso(row["date"])
                if row["code"] != _bs_code(symbol) or day in seen or not start <= day <= end or row["adjustflag"] != "3":
                    raise ValueError("invalid/duplicate raw daily record")
                seen.add(day)
                key = (symbol, day)
                if key in self._bars and self._bars[key] != row:
                    raise ValueError("provider revised a previously loaded market record")
                self._bars[key] = row
            self._bar_ranges.append((symbol, start, end))
        if not any(s == symbol and a <= start and end <= b for s, a, b in self._factor_ranges):
            factors = self._call("query_adjust_factor", ["code", "dividOperateDate"], code=_bs_code(symbol), start_date=start, end_date=end)
            for row in factors:
                if row["code"] != _bs_code(symbol) or not start <= _iso(row["dividOperateDate"]) <= end:
                    raise ValueError("invalid corporate-action response")
            if factors:
                raise ValueError("corporate action in requested interval; derived-limit simulation is blocked")
            self._factor_ranges.append((symbol, start, end))

    def prepare(self, start, end):
        """Batch download into memory only; no partially validated snapshots."""
        start, end = _iso(start), _iso(end)
        if start > end:
            raise ValueError("start exceeds end")
        calendar = self.calendar((date.fromisoformat(start) - timedelta(days=32)).isoformat(),
                                 (date.fromisoformat(end) + timedelta(days=32)).isoformat())
        previous = [day for day in calendar["sessions"] if day < start]
        if not previous:
            raise ValueError("calendar lacks previous session")
        for symbol in self.universe:
            self._basic(symbol)
            self._load_range(symbol, previous[-1], end)

    def _completed(self, trade_date):
        _iso(trade_date)
        if trade_date < "2020-08-24":
            raise ValueError("derived-limit simulation supports sessions from 2020-08-24 only")
        now = self.now()
        if now.tzinfo is None:
            raise ValueError("provider clock must be timezone aware")
        now = now.astimezone(SHANGHAI)
        if trade_date > now.date().isoformat() or (trade_date == now.date().isoformat() and (now.hour, now.minute) < (15, 30)):
            raise ValueError("requested session is not complete")

    def fetch_day(self, trade_date, previous_day=None):
        self._completed(trade_date)
        middle = date.fromisoformat(trade_date)
        sessions = self.calendar((middle - timedelta(days=32)).isoformat(), (middle + timedelta(days=32)).isoformat())["sessions"]
        if trade_date not in sessions:
            raise ValueError("requested date is not an exchange trading session")
        index = sessions.index(trade_date)
        if index == 0 or index == len(sessions) - 1:
            raise ValueError("calendar lacks adjacent sessions")
        previous, following = sessions[index - 1], sessions[index + 1]
        bars = {}
        for symbol in self.universe:
            info = self._basic(symbol)
            if (middle - date.fromisoformat(info["ipoDate"])).days < self.min_listing_days:
                raise ValueError("newly listed stock is unsupported by derived-limit simulation")
            self._load_range(symbol, previous, trade_date)
            raw = self._bars.get((symbol, trade_date))
            prior = self._bars.get((symbol, previous))
            if raw is None or prior is None:
                raise ValueError("daily data missing for expected universe/session")
            if raw["tradestatus"] not in {"0", "1"} or raw["isST"] not in {"0", "1"}:
                raise ValueError("unknown suspension/ST status")
            previous_close = D(raw["preclose"])
            if previous_close <= 0 or previous_close != D(prior["close"]):
                raise ValueError("reference-price change requires corporate-action reconciliation")
            volume = D(raw["volume"])
            if volume < 0 or volume != volume.to_integral_value():
                raise ValueError("volume must be whole shares")
            suspended = raw["tradestatus"] == "0"
            is_st = raw["isST"] == "1"
            if suspended:
                if volume != 0:
                    raise ValueError("suspended instrument has nonzero volume")
                # The provider's positive prior close is a valuation, not a trade.
                if any(raw[field] not in {"", "0", "0.0000"} and D(raw[field]) != previous_close for field in ["open", "high", "low", "close"]):
                    raise ValueError("inconsistent suspended valuation")
                opening = high = low = close = previous_close
                turnover = D(0)
            else:
                opening, high, low, close = [D(raw[field]) for field in ["open", "high", "low", "close"]]
                turnover = D(raw["turn"])
            rate = self.rates[self.boards[symbol]]
            upper = None if is_st or suspended else money(previous_close * (1 + rate))
            lower = None if is_st or suspended else money(previous_close * (1 - rate))
            if upper is not None and (high > upper or low < lower):
                raise ValueError("observed prices exceed derived bounds; unsupported special session")
            bars[symbol] = Bar(symbol, trade_date, opening, high, low, close, previous_close,
                               int(volume), turnover, self.boards[symbol], is_st, suspended,
                               upper, lower, info["ipoDate"], "unknown")
        return MarketDay(trade_date, previous, following, bars, SOURCE)

    def update(self, trade_date, output_dir):
        self._completed(trade_date)
        path = Path(output_dir) / f"{trade_date}.json"
        if path.exists():
            existing = load_day(path)
            if existing.source != SOURCE or set(existing.bars) != set(self.universe):
                raise ValueError("existing snapshot source/universe differs")
            return existing
        day = self.fetch_day(trade_date)
        save_day(day, path)
        return day

    def diagnose(self, trade_date):
        day = self.fetch_day(trade_date)
        return {"ok": True, "provider": "baostock", "date": day.date, "symbols": len(day.bars),
                "paper_only": True, "limits": "derived-not-exchange-data", "survivorship_bias": True,
                "warning": "Not suitable for real orders. Current listing metadata; unknown sectors; unsupported exceptions are blocked where detectable."}
