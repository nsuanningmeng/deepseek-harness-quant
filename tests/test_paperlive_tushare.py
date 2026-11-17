"""Tushare contract tests with synthetic responses; no token or live API calls."""
from datetime import datetime, timedelta
from decimal import Decimal
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from autotrade.providers import Records
from paperlive.market import SHANGHAI
from paperlive.tushare_feed import TushareMarketFeed, RT_FIELDS, parse_rt_quote


NOW = datetime(2026, 11, 11, 10, 0, tzinfo=SHANGHAI)


def quote_row(symbol="600000.SH", stamp="2026-11-11 10:00:00"):
    return {"ts_code": symbol, "name": "合成测试证券", "pre_close": 10, "high": 10.2, "open": 10,
            "low": 9.9, "close": 10.1, "vol": 12345, "amount": 124684.5, "num": 42,
            "ask_price1": 10.11, "ask_volume1": 1200, "bid_price1": 10.09, "bid_volume1": 1300,
            "trade_time": stamp}


def limit_row(symbol="600000.SH", day="20261111"):
    return {"trade_date": day, "ts_code": symbol, "pre_close": 10, "up_limit": 11, "down_limit": 9}


class Clock:
    def __init__(self):
        self.now, self.monotonic_value, self.waits = NOW, 100.0, []

    def sleep(self, amount):
        self.waits.append(amount)
        self.monotonic_value += amount


class Client:
    def __init__(self):
        self.calls = []
        self.symbols = [f"600{index:03d}.SH" for index in range(300)]
        self.weights = [{"index_code": "000300.SH", "con_code": symbol, "trade_date": "20261030", "weight": "0.3333"}
                        for symbol in self.symbols]
        self.basics = [{"ts_code": symbol, "name": "合成测试证券", "industry": "测试行业", "list_date": "20200101",
                        "list_status": "L", "delist_date": "", "market": "主板", "exchange": "SSE"} for symbol in self.symbols]
        self.basics.append({"ts_code": "000001.SZ", "name": "*ST测试持仓", "industry": "测试", "list_date": "20261101",
                            "list_status": "L", "delist_date": "", "market": "主板", "exchange": "SZSE"})
        self.calendar = [{"exchange": "SSE", "cal_date": day, "is_open": flag} for day, flag in
                         [("20261111", "1"), ("20261112", "1"), ("20261113", "1"),
                          ("20261114", "0"), ("20261115", "0"), ("20261116", "1")]]
        self.limit_rows = [limit_row(symbol) for symbol in self.symbols + ["000001.SZ"]]
        self.rt_rows = None
        self.fail = None
        self.omit_time = False

    def query(self, endpoint, fields="", **params):
        self.calls.append((endpoint, fields.split(","), params))
        if endpoint == self.fail:
            raise ValueError("secret-that-must-never-appear-in-status")
        if endpoint == "index_weight":
            rows = self.weights
        elif endpoint == "stock_basic":
            rows = [row for row in self.basics if row["exchange"] == params["exchange"]]
        elif endpoint == "trade_cal":
            rows = [row for row in self.calendar if params["start_date"] <= row["cal_date"] <= params["end_date"]]
        elif endpoint == "stk_limit":
            rows = self.limit_rows
        elif endpoint == "rt_k":
            rows = self.rt_rows if self.rt_rows is not None else [quote_row(symbol) for symbol in params["ts_code"].split(",")]
        else:
            raise AssertionError("unexpected endpoint")
        columns = fields.split(",")
        if endpoint == "rt_k" and self.omit_time:
            columns.remove("trade_time")
        return Records(columns, rows)


class TushareTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.client, self.clock = Client(), Clock()
        self.cfg = {"mode": "paper", "runtime_dir": self.temp.name,
                    "market": {"universe": "hs300", "provider": "tushare", "source_label": "Tushare测试",
                               "min_listing_days": 180, "request_timeout": 2, "max_quote_age_seconds": 30,
                               "max_future_seconds": 3, "universe_refresh_seconds": 21600,
                               "trading_sessions": [["09:30", "11:30"], ["13:00", "14:57"]]},
                    "tushare": {"index_code": "000300.SH", "index_max_age_days": 45,
                                "query_batch_size": 80, "min_call_interval_seconds": 0.25}}
        patcher = patch("paperlive.tushare_feed._LAST_REQUEST", None)
        patcher.start()
        self.addCleanup(patcher.stop)

    def feed(self, **kwargs):
        return TushareMarketFeed(self.cfg, client=kwargs.pop("client", self.client), now=lambda: self.clock.now,
                                 sleeper=self.clock.sleep, monotonic=lambda: self.clock.monotonic_value, **kwargs)

    def test_rt_volume_and_books_are_shares_without_tencent_lot_scaling(self):
        daily = {"date": "2026-11-11", "symbol": "600000.SH", "up_limit": "11", "down_limit": "9", "pre_close": "10"}
        quote = parse_rt_quote(quote_row(), NOW, daily)
        self.assertEqual((quote.volume, quote.ask_size, quote.bid_size), (12345, 1200, 1300))
        self.assertEqual(quote.timestamp, NOW)
        self.assertEqual(quote.received_at, NOW)
        self.assertEqual(quote.source, "tushare:rt_k")
        self.assertEqual((quote.limit_up, quote.limit_down), (Decimal(11), Decimal(9)))

    def test_rt_fields_are_explicit_and_missing_time_is_not_replaced(self):
        feed = self.feed()
        self.assertEqual(len(feed.quotes(["600000.SH"])), 1)
        fields = next(fields for endpoint, fields, _ in self.client.calls if endpoint == "rt_k")
        self.assertTrue(set(RT_FIELDS) <= set(fields))
        self.client.omit_time = True
        self.assertEqual(feed.quotes(["600000.SH"]), {})
        self.assertIn("A股日线RT", feed.status["quotes"]["last_error"])

    def test_missing_or_bad_quote_data_cannot_become_a_tradeable_quote(self):
        for field, value in [("trade_time", "10:00:00"), ("trade_time", None), ("vol", "1.5"),
                             ("bid_volume1", -2), ("close", "NaN"), ("ask_price1", None),
                             ("trade_time", "2026-11-11 10:00:04")]:
            row = quote_row()
            row[field] = value
            self.client.rt_rows = [row]
            with self.subTest(field=field, value=value):
                self.assertEqual(self.feed().quotes(["600000.SH"]), {})

    def test_actual_limits_are_cached_only_for_their_day_and_never_derived(self):
        feed = self.feed()
        self.assertIsNotNone(feed.quotes(["600000.SH"])["600000.SH"].limit_up)
        feed.quotes(["600000.SH"])
        self.assertEqual(sum(endpoint == "stk_limit" for endpoint, _, _ in self.client.calls), 1)
        self.clock.now += timedelta(days=1)
        self.client.limit_rows = []
        self.client.rt_rows = [quote_row(stamp="2026-11-12 10:00:00")]
        quote = feed.quotes(["600000.SH"])["600000.SH"]
        self.assertIsNone(quote.limit_up)
        self.assertIsNone(quote.limit_down)
        self.assertEqual(feed.status["quotes"]["missing_limit_symbols"], ["600000.SH"])

    def test_pre_market_empty_limits_are_retried_and_base_mismatch_is_rejected(self):
        feed = self.feed()
        self.client.limit_rows = []
        self.assertIsNone(feed.quotes(["600000.SH"])["600000.SH"].limit_up)
        self.client.limit_rows = [limit_row()]
        self.assertEqual(feed.quotes(["600000.SH"])["600000.SH"].limit_up, Decimal(11))
        feed._limits = None
        (feed.directory / "limits.json").unlink()
        self.client.limit_rows[0]["pre_close"] = 9.5
        self.assertEqual(feed.quotes(["600000.SH"]), {})

    def test_unrelated_ipo_bad_limits_do_not_poison_requested_symbol(self):
        self.client.limit_rows += [{**limit_row("688999.SH"), "up_limit": 0, "down_limit": 0},
                                   {**limit_row("600999.SH"), "up_limit": None},
                                   {**limit_row("600998.SH"), "trade_date": "invalid"}]
        feed = self.feed()
        quote = feed.quotes(["600000.SH"])["600000.SH"]
        self.assertEqual(quote.limit_up, Decimal(11))
        self.assertEqual(feed.status["limits"]["missing_symbols"], [])
        self.assertEqual(feed.status["limits"]["errors"], {})

    def test_requested_bad_or_duplicate_limit_row_is_missing_without_inference(self):
        self.client.limit_rows[0]["up_limit"] = None
        self.client.limit_rows.append(limit_row("600001.SH"))
        feed = self.feed()
        quotes = feed.quotes(["600000.SH", "600001.SH", "600002.SH"])
        self.assertIsNone(quotes["600000.SH"].limit_up)
        self.assertIsNone(quotes["600001.SH"].limit_up)
        self.assertEqual(quotes["600002.SH"].limit_up, Decimal(11))
        self.assertEqual(set(feed.status["limits"]["errors"]), {"600000.SH", "600001.SH"})

    def test_supplier_failure_is_redacted_and_never_falls_back(self):
        feed = self.feed()
        self.client.fail = "rt_k"
        with patch("paperlive.market.MarketFeed._bs", side_effect=AssertionError("BaoStock fallback forbidden")):
            self.assertEqual(feed.quotes(["600000.SH"]), {})
        status = json.dumps(feed.status, ensure_ascii=False)
        self.assertNotIn("secret-that", status)
        self.assertIn("Tushare rt_k", status)
        self.assertEqual(feed.status["provider"], "tushare")

    def test_quote_code_matching_duplicates_missing_and_stale_data(self):
        feed = self.feed()
        self.client.rt_rows = [quote_row("000001.SZ")]
        self.assertEqual(feed.quotes(["600000.SH"]), {})
        self.client.rt_rows = [quote_row(), quote_row()]
        self.assertEqual(feed.quotes(["600000.SH"]), {})
        self.client.rt_rows = [quote_row(stamp="2026-11-11 09:59:00")]
        result = feed.quotes(["600000.SH", "000001.SZ"])
        self.assertEqual(set(result), {"600000.SH"})
        self.assertEqual(feed.status["quotes"]["stale_symbols"], ["600000.SH"])
        self.assertIn("000001.SZ", feed.status["quotes"]["errors"])

    def test_requests_are_batched_and_rate_limited(self):
        self.cfg["tushare"]["query_batch_size"] = 2
        feed = self.feed()
        result = feed.quotes(self.client.symbols[:5])
        self.assertEqual(len(result), 5)
        calls = [params["ts_code"] for endpoint, _, params in self.client.calls if endpoint == "rt_k"]
        self.assertEqual([len(value.split(",")) for value in calls], [2, 2, 1])
        self.assertEqual(self.clock.waits, [0.25, 0.25, 0.25])

    def test_latest_complete_monthly_index_and_metadata_cache(self):
        self.client.weights += [{"index_code": "000300.SH", "con_code": "600001.SH", "trade_date": "20261231", "weight": 1}]
        feed = self.feed()
        self.assertEqual(len(feed.universe()), 300)
        self.assertEqual(feed.status["universe"]["effective_date"], "2026-10-30")
        calls = len(self.client.calls)
        self.assertEqual(len(self.feed().universe()), 300)
        self.assertEqual(len(self.client.calls), calls)
        self.assertTrue((Path(self.temp.name) / "market" / "tushare" / "universe.json").exists())

    def test_latest_partial_duplicate_or_old_index_is_rejected(self):
        scenarios = [self.client.weights[:-1], self.client.weights[:-1] + [self.client.weights[0]],
                     [{**row, "trade_date": "20260801"} for row in self.client.weights],
                     self.client.weights + [{**self.client.weights[0], "trade_date": "20261110"}]]
        for weights in scenarios:
            self.client.weights = weights
            with self.subTest(count=len(weights)), self.assertRaises(ValueError):
                self.feed().universe()

    def test_non_tushare_cache_is_never_relabelled_as_tushare(self):
        feed = self.feed()
        feed.universe()
        path = feed.directory / "universe.json"
        payload = json.loads(path.read_text())
        payload["source"] = "baostock:hs300+stock_basic:current"
        path.write_text(json.dumps(payload))
        self.client.fail = "index_weight"
        with self.assertRaises(ValueError):
            self.feed().universe()

    def test_expired_monthly_snapshot_remains_explicitly_stale_on_outage(self):
        feed = self.feed()
        original = feed.universe()
        self.clock.now = datetime(2027, 1, 1, 10, tzinfo=SHANGHAI)
        self.client.fail = "index_weight"
        self.assertEqual(feed.universe(), original)
        self.assertTrue(feed.status["universe"]["cache_stale"])
        self.assertEqual(feed.status["universe"]["effective_date"], "2026-10-30")

    def test_all_300_are_validated_before_buy_eligibility_and_lookup_is_independent(self):
        self.client.basics[0]["list_date"] = "20261101"
        self.client.basics[1]["name"] = "*ST合成"
        self.client.weights[-1]["con_code"] = "689009.SH"
        self.client.basics[-2]["ts_code"] = "689009.SH"
        self.client.basics[-2]["market"] = "CDR"
        feed = self.feed()
        self.assertEqual(len(feed.universe()), 297)
        self.assertEqual(feed.status["universe"]["member_count"], 300)
        held = feed.lookup(["600000.SH", "600001.SH", "000001.SZ"])
        self.assertEqual(set(held), {"600000.SH", "600001.SH", "000001.SZ"})
        self.assertEqual(held["000001.SZ"].listed_date, "2026-11-01")

    def test_calendar_lunch_weekend_holiday_and_missing_day_are_explicit(self):
        feed = self.feed()
        self.assertTrue(feed.session(NOW)["open"])
        self.assertFalse(feed.session(NOW.replace(hour=12))["open"])
        self.assertFalse(feed.session(NOW.replace(hour=14, minute=57))["open"])
        saturday = datetime(2026, 11, 14, 10, tzinfo=SHANGHAI)
        self.clock.now = saturday
        weekend = feed.session(saturday)
        self.assertFalse(weekend["open"])
        self.assertTrue(weekend["calendar_verified"])
        self.assertEqual(weekend["next_open"], "2026-11-16T09:30:00+08:00")
        self.assertEqual(weekend["calendar_source"], "tushare:trade_cal")
        self.clock.now = datetime(2026, 10, 1, 10, tzinfo=SHANGHAI)
        self.client.calendar = [{"exchange": "SSE", "cal_date": "20261001", "is_open": "0"}]
        feed._calendar = None
        result = feed.session(self.clock.now)
        self.assertTrue(result["calendar_verified"])
        self.assertFalse(result["open"])
        self.clock.now = datetime(2027, 1, 1, 10, tzinfo=SHANGHAI)
        self.client.calendar = []
        self.assertFalse(feed.session(self.clock.now)["calendar_verified"])
        self.assertFalse(feed.session(self.clock.now)["open"])

    def test_token_is_lazy_and_missing_token_never_leaks_loader_exception(self):
        def missing(cfg):
            raise ValueError("secret-that-must-never-appear-in-status")
        feed = self.feed(client=None, token_loader=missing)
        self.assertFalse((Path(self.temp.name) / "market").exists())
        with self.assertRaisesRegex(ValueError, "Token未配置"):
            feed.universe()
        self.assertNotIn("secret-that", json.dumps(feed.status, ensure_ascii=False))
        self.assertFalse(feed.session(NOW)["calendar_verified"])

    def test_token_update_rebuilds_client_before_next_call(self):
        tokens = iter(["first-dummy", "second-dummy"])
        feed = self.feed(client=None, token_loader=lambda cfg: next(tokens))
        with patch("paperlive.tushare_feed.TushareHttpClient", return_value=self.client) as factory:
            feed._call("index_weight", ("index_code", "con_code", "trade_date", "weight"), index_code="000300.SH")
            feed._call("index_weight", ("index_code", "con_code", "trade_date", "weight"), index_code="000300.SH")
            self.assertEqual([call.args[0] for call in factory.call_args_list], ["first-dummy", "second-dummy"])


if __name__ == "__main__":
    unittest.main()
