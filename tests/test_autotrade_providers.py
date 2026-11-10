"""真实接口的离线协议验收；只使用合成响应，不分发第三方行情。"""
from datetime import date, datetime, timedelta
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from autotrade.bootstrap import bootstrap_market
from autotrade.data import TushareProvider, load_day
from autotrade.demo import build_demo
from autotrade.models import D
from autotrade.providers import (FilesProvider, QmtLiveMarketProvider, Records, SHANGHAI,
                                TushareHistoricalProvider, TushareHttpClient, board_for_symbol)


class FakeClient:
    def __init__(self):
        self.calls, self.mutations = [], {}

    def __getattr__(self, endpoint):
        def query(fields, **params):
            self.calls.append((endpoint, params))
            symbol, current = params.get("ts_code", "600001.SH"), params.get("trade_date", "20261110")
            rows = {
                "daily": [{"ts_code": symbol, "trade_date": current, "open": 10, "high": 10.2, "low": 9.8,
                           "close": 10, "pre_close": 10, "vol": 1000.01}],
                "daily_basic": [{"ts_code": symbol, "trade_date": current, "turnover_rate": 1.2}],
                "stk_limit": [{"ts_code": symbol, "trade_date": current, "pre_close": 10, "up_limit": 11, "down_limit": 9}],
                "stock_st": [], "suspend_d": [],
                "stock_basic": [{"ts_code": symbol, "name": "当前ST合成证券", "market": "主板", "industry": "当前行业",
                                 "list_date": "20200102", "delist_date": None}],
            }.get(endpoint, [])
            if endpoint == "trade_cal":
                first, last = (datetime.strptime(params[k], "%Y%m%d").date() for k in ("start_date", "end_date"))
                for offset in range((last-first).days + 1):
                    current_day = first + timedelta(days=offset)
                    previous = current_day-timedelta(days=1)
                    while previous.weekday() >= 5:
                        previous -= timedelta(days=1)
                    # Synthetic calendar fixture, never used as a production exchange calendar.
                    rows.append({"cal_date": current_day.strftime("%Y%m%d"), "is_open": int(current_day.weekday() < 5),
                                 "pretrade_date": previous.strftime("%Y%m%d")})
            if endpoint in self.mutations:
                rows = self.mutations[endpoint](rows, params)
            return Records(fields.split(","), rows)
        return query


class ProviderTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name) / "market"
        self.cfg = {"runtime_dir": self.temp.name, "data": {"provider": "tushare", "directory": str(self.directory),
                    "universe": ["600001.SH"], "calendar_cache": str(Path(self.temp.name)/"calendar-cache.json"),
                    "request_attempts": 2, "retry_seconds": 0, "request_timeout": 10}}
        self.client = FakeClient()
        self.clock = lambda: datetime(2026, 11, 10, 18, tzinfo=SHANGHAI)

    def provider(self):
        return TushareHistoricalProvider(self.cfg, client=self.client, now=self.clock)

    def test_historical_fetch_uses_dated_metadata_and_unknown_industry(self):
        self.client.mutations["stock_st"] = lambda rows, p: ([{"ts_code": p["ts_code"], "trade_date": p["trade_date"]}]
                                                            if p["trade_date"] == "20261106" else [])
        provider = self.provider()
        old = provider.fetch_day("2026-11-05")
        st = provider.fetch_day("2026-11-06")
        self.assertFalse(old.bars["600001.SH"].is_st)
        self.assertTrue(st.bars["600001.SH"].is_st)
        self.assertEqual(old.bars["600001.SH"].sector, "unknown")
        self.assertEqual(old.bars["600001.SH"].volume, 100001)
        self.assertIn("ST/suspension/limits=dated", old.source)

    def test_bootstrap_then_missing_day_recovery_and_idempotence(self):
        first = bootstrap_market(self.cfg, "2026-11-02", "2026-11-06", self.provider())
        self.assertEqual(len(first["created"]), 5)
        self.assertEqual(first["orders_submitted"], 0)
        second = bootstrap_market(self.cfg, "2026-11-02", "2026-11-06", self.provider())
        self.assertEqual(second["created"], [])
        self.assertEqual(second["reused"], first["sessions"])
        (self.directory/"2026-11-04.json").unlink()
        third = bootstrap_market(self.cfg, "2026-11-02", "2026-11-06", self.provider())
        self.assertEqual(third["created"], ["2026-11-04"])
        self.assertFalse((Path(self.temp.name)/"paper.sqlite3").exists())

    def test_confirmed_suspension_uses_dated_reference_with_no_volume(self):
        self.client.mutations["daily"] = lambda *_: []
        self.client.mutations["suspend_d"] = lambda rows, p: [{"ts_code": p["ts_code"], "trade_date": p["trade_date"],
                                                             "suspend_type": "S", "suspend_timing": None}]
        bar = self.provider().fetch_day("2026-11-09").bars["600001.SH"]
        self.assertTrue(bar.suspended)
        self.assertEqual((bar.close, bar.open, bar.volume, bar.turnover), (D(10), D(10), 0, D(0)))
        self.assertFalse(any(endpoint == "daily_basic" for endpoint, _ in self.client.calls))

    def test_incremental_bootstrap_keeps_verified_calendar_history(self):
        bootstrap_market(self.cfg, "2026-11-02", "2026-11-06", self.provider())
        bootstrap_market(self.cfg, "2026-11-09", "2026-11-10", self.provider())
        calendar = json.loads((self.directory/"calendar.json").read_text())
        self.assertEqual((calendar["start"], calendar["end"]), ("2026-11-02", "2026-11-10"))
        self.assertEqual(len(calendar["sessions"]), 7)
        self.assertNotIn("2026-11-07", calendar["sessions"])
        files = FilesProvider({"data": {"directory": str(self.directory), "universe": ["600001.SH"]}})
        self.assertEqual(files.calendar("2026-11-02", "2026-11-03")["start"], "2026-11-02")
        self.assertEqual(files.calendar("2026-11-02", "2026-11-03")["end"], "2026-11-03")

    def test_calendar_union_conflict_cannot_replace_previous_coverage(self):
        bootstrap_market(self.cfg, "2026-11-02", "2026-11-06", self.provider())
        path = self.directory/"calendar.json"
        before = path.read_bytes()
        provider = self.provider()
        original = provider.calendar
        def changed(first, last):
            result = original(first, last)
            if first == "2026-11-02" and last == "2026-11-10":
                result["sessions"].remove("2026-11-03")
            return result
        provider.calendar = changed
        with self.assertRaisesRegex(ValueError, "calendar history changed"):
            bootstrap_market(self.cfg, "2026-11-09", "2026-11-10", provider)
        self.assertEqual(path.read_bytes(), before)

    def test_missing_daily_without_confirmed_halt_rejects(self):
        self.client.mutations["daily"] = lambda *_: []
        with self.assertRaisesRegex(ValueError, "without a confirmed"):
            self.provider().update("2026-11-09", self.directory)
        self.assertFalse((self.directory/"2026-11-09.json").exists())

    def test_intraday_halt_and_conflicting_reference_reject(self):
        self.client.mutations["suspend_d"] = lambda rows, p: [{"ts_code": p["ts_code"], "trade_date": p["trade_date"],
                                                            "suspend_type": "S", "suspend_timing": "09:30-10:00"}]
        with self.assertRaisesRegex(ValueError, "intraday suspension"):
            self.provider().fetch_day("2026-11-09")
        self.client.mutations = {"stk_limit": lambda rows, _: [{**rows[0], "pre_close": 9.9}]}
        with self.assertRaisesRegex(ValueError, "reference prices disagree"):
            self.provider().fetch_day("2026-11-09")

    def test_future_and_unfinished_current_day_are_not_downloaded(self):
        provider = self.provider()
        with self.assertRaises(ValueError):
            provider.fetch_day("2026-11-11")
        provider.now = lambda: datetime(2026, 11, 10, 15, 29, tzinfo=SHANGHAI)
        with self.assertRaises(ValueError):
            provider.fetch_day("2026-11-10")
        self.assertFalse(self.client.calls)

    def test_calendar_missing_or_duplicate_rows_fail_closed(self):
        for mutation in (lambda rows, _: rows[:-1], lambda rows, _: rows + rows[:1]):
            self.client.mutations["trade_cal"] = mutation
            with self.assertRaises(ValueError):
                self.provider().calendar("2026-11-01", "2026-11-10")

    def test_calendar_cache_survives_offline_restart(self):
        first = self.provider().calendar("2026-11-01", "2026-11-10")
        self.client.mutations["trade_cal"] = lambda *_: (_ for _ in ()).throw(RuntimeError("offline"))
        self.assertEqual(self.provider().calendar("2026-11-01", "2026-11-10"), first)

    def test_retries_are_bounded_and_errors_redacted(self):
        attempts = []
        def fail(*_):
            attempts.append(1)
            raise RuntimeError("PRIVATE_TOKEN_SENTINEL")
        self.client.mutations["daily"] = fail
        result = self.provider().diagnose("2026-11-09")
        self.assertFalse(result["ok"])
        self.assertEqual(len(attempts), 2)
        self.assertNotIn("PRIVATE_TOKEN_SENTINEL", result["error"])
        self.assertFalse(self.directory.exists())

    def test_metadata_stale_duplicates_and_listing_date_reject(self):
        mutations = [("daily", lambda rows, _: [{**rows[0], "trade_date": "20261106"}]),
                     ("daily_basic", lambda rows, _: rows * 2),
                     ("stock_basic", lambda rows, _: [{**rows[0], "list_date": "20261110"}]),
                     ("stock_basic", lambda rows, _: [{**rows[0], "market": "科创板"}]),
                     ("stk_limit", lambda rows, _: [{**rows[0], "up_limit": None}])]
        for endpoint, mutation in mutations:
            with self.subTest(endpoint=endpoint):
                self.client.mutations = {endpoint: mutation}
                with self.assertRaises(ValueError):
                    self.provider().fetch_day("2026-11-09")

    def test_existing_source_cannot_be_mixed(self):
        build_demo(self.directory)
        with self.assertRaisesRegex(ValueError, "provenance or universe"):
            self.provider().update("2026-09-01", self.directory)

    def test_http_transport_uses_https_timeout_and_validates_shape(self):
        class Response(io.BytesIO):
            def geturl(self):
                return "https://api.tushare.pro/"
        class Opener:
            def open(inner, request, timeout):
                self.assertEqual(request.full_url, "https://api.tushare.pro")
                self.assertEqual(timeout, 12)
                payload = json.loads(request.data)
                self.assertEqual(payload["token"], "fake-token")
                return Response(json.dumps({"code": 0, "data": {"fields": ["ts_code"], "items": [["600001.SH"]]}}).encode())
        client = TushareHttpClient("fake-token", timeout=12, opener=Opener())
        self.assertEqual(client.daily(fields="ts_code").to_dict("records"), [{"ts_code": "600001.SH"}])

    def test_http_errors_never_echo_remote_message_or_redirect(self):
        class Response(io.BytesIO):
            def geturl(self):
                return "https://untrusted.example"
        class Opener:
            def open(inner, *_args, **_kwargs):
                return Response(b'{"code":1,"msg":"PRIVATE_TOKEN_SENTINEL"}')
        with self.assertRaises(ValueError) as caught:
            TushareHttpClient("fake-token", opener=Opener()).daily(fields="ts_code")
        self.assertNotIn("PRIVATE_TOKEN_SENTINEL", str(caught.exception))

    def test_legacy_provider_initializes_http_without_invalid_sdk_timeout(self):
        old = TushareProvider({"data": {"universe": ["600001.SH"]}})
        with patch.dict(os.environ, {"TUSHARE_TOKEN": "fake-token"}), patch("autotrade.providers.TushareHttpClient", return_value=self.client) as ctor:
            old._call("daily", ("ts_code", "trade_date"), ts_code="600001.SH", trade_date="20261110")
            ctor.assert_called_once_with("fake-token", timeout=30)

    def test_file_provider_validates_calendar_and_exact_date(self):
        dates = build_demo(self.directory)
        cfg = {"data": {"directory": str(self.directory), "universe": []}}
        provider = FilesProvider(cfg)
        self.assertTrue(provider.diagnose(dates[0])["ok"])
        self.assertFalse(provider.is_trading_day("2026-10-01"))
        self.assertFalse(provider.diagnose("2027-01-01")["ok"])

    def test_nonstock_codes_are_rejected(self):
        for symbol in ("510300.SH", "000001.SH", "600001.SZ", "110001.SH", "689001.SH"):
            with self.subTest(symbol=symbol), self.assertRaises(ValueError):
                board_for_symbol(symbol)


class FakeXtData:
    def __init__(self, now):
        self.now = now
        self.change = lambda symbol, tick: tick

    def get_trading_calendar(self, *args, **kwargs):
        return ["20261109", "20261110", "20261111"]

    def get_full_tick(self, symbols):
        return {symbol: self.change(symbol, {"time": int(self.now.timestamp()*1000), "open": 10, "high": 10.2,
                 "low": 9.8, "lastPrice": 10.1, "lastClose": 10, "volume": 1000,
                 "pvolume": 100000, "stockStatus": 13}) for symbol in symbols}

    def get_instrument_detail(self, symbol, iscomplete=True):
        code, exchange = symbol.split(".")
        return {"InstrumentID": code, "ExchangeID": exchange, "InstrumentName": "合成证券", "OpenDate": "20200102",
                "PreClose": 10, "UpStopPrice": 11, "DownStopPrice": 9, "FloatVolume": 10000000,
                "IsTrading": True, "PriceTick": .01}

    def get_instrument_type(self, symbol):
        return {"stock": True, "fund": False}


class QmtMarketTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 11, 10, 10, 1, tzinfo=SHANGHAI)
        self.sdk = FakeXtData(self.now)
        self.provider = QmtLiveMarketProvider({"qmt": {"max_quote_age_seconds": 5}}, xtdata=self.sdk, now=lambda: self.now)

    def test_live_quotes_are_timestamped_per_symbol(self):
        result = self.provider({"600001.SH", "000001.SZ"})
        self.assertEqual(set(result.quote_times), {"600001.SH", "000001.SZ"})
        self.assertEqual(result.day.bars["600001.SH"].close, D("10.1"))
        self.assertEqual(result.day.bars["600001.SH"].volume, 100000)
        self.assertEqual(result.day.previous_session, "2026-11-09")

    def test_one_stale_symbol_blocks_whole_snapshot(self):
        self.sdk.change = lambda symbol, tick: {**tick, "time": tick["time"]-6000} if symbol == "000001.SZ" else tick
        with self.assertRaisesRegex(ValueError, "stale"):
            self.provider({"600001.SH", "000001.SZ"})

    def test_tick_arriving_during_io_uses_receipt_clock(self):
        original = self.sdk.get_full_tick
        def receive(symbols):
            self.now += timedelta(seconds=1)
            self.sdk.now = self.now
            return original(symbols)
        self.sdk.get_full_tick = receive
        result = self.provider({"600001.SH"})
        self.assertEqual(result.quote_times["600001.SH"], self.now)
        self.assertEqual(result.observed_at, self.now)

    def test_elapsed_metadata_io_can_expire_received_ticks(self):
        original = self.sdk.get_instrument_detail
        def receive(symbol, iscomplete=True):
            self.now += timedelta(seconds=6)
            return original(symbol, iscomplete=iscomplete)
        self.sdk.get_instrument_detail = receive
        with self.assertRaisesRegex(ValueError, "stale"):
            self.provider({"600001.SH"})

    def test_future_timestamp_wrong_units_unknown_status_and_missing_fields(self):
        mutations = [lambda s, t: {**t, "time": t["time"]+1000},
                     lambda s, t: {**t, "pvolume": 1000},
                     lambda s, t: {**t, "stockStatus": 0},
                     lambda s, t: {k: v for k, v in t.items() if k != "lastClose"}]
        for mutation in mutations:
            self.sdk.change = mutation
            with self.assertRaises(ValueError):
                self.provider({"600001.SH"})

    def test_no_terminal_import_on_construction(self):
        with patch.dict("sys.modules", {"xtquant": None}):
            provider = QmtLiveMarketProvider({})
            self.assertIsNone(provider.xtdata)


if __name__ == "__main__":
    unittest.main()
