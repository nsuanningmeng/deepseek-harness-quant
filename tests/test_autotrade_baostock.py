import copy
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import tempfile
import unittest

from autotrade.baostock_provider import BaostockProvider, SOURCE
from autotrade.data import save_day
from dataclasses import replace


class Result:
    def __init__(self, fields=(), rows=(), error="0"):
        self.fields = list(fields)
        self.rows = list(rows)
        self.error_code = error
        self.index = -1

    def next(self):
        self.index += 1
        return self.index < len(self.rows)

    def get_row_data(self):
        return self.rows[self.index]


class FakeClient:
    def __init__(self):
        self.calls = []
        self.overrides = {}
        self.ipo = "2000-01-01"
        self.actions = []
        self.fail_endpoint = None
        self.drop_calendar = False
        self.missing_day = None

    def login(self):
        return Result()

    def logout(self):
        return Result()

    def query_trade_dates(self, start_date, end_date):
        self.calls.append("calendar")
        rows = []
        current = date.fromisoformat(start_date)
        while current <= date.fromisoformat(end_date):
            rows.append([current.isoformat(), "1" if current.weekday() < 5 else "0"])
            current += timedelta(days=1)
        if self.drop_calendar:
            rows.pop()
        return Result(["calendar_date", "is_trading_day"], rows)

    def query_stock_basic(self, code):
        self.calls.append("basic")
        return Result(["code", "ipoDate", "outDate", "type", "status"], [[code, self.ipo, "", "1", "1"]])

    def query_adjust_factor(self, code, start_date, end_date):
        self.calls.append("factors")
        return Result(["code", "dividOperateDate"], [[code, day] for day in self.actions if start_date <= day <= end_date])

    def query_history_k_data_plus(self, code, fields, start_date, end_date, frequency, adjustflag):
        self.calls.append("daily")
        if self.fail_endpoint == "daily":
            return Result(fields.split(","), error="400")
        rows = []
        current = date.fromisoformat(start_date)
        while current <= date.fromisoformat(end_date):
            day = current.isoformat()
            if current.weekday() < 5 and day != self.missing_day:
                row = dict(date=day, code=code, open="10", high="10.2", low="9.8", close="10",
                           preclose="10", volume="1000000", adjustflag="3", turn="1", tradestatus="1", isST="0")
                row.update(self.overrides.get(day, {}))
                rows.append([row[field] for field in fields.split(",")])
            current += timedelta(days=1)
        return Result(fields.split(","), rows)


class BaostockTests(unittest.TestCase):
    def setUp(self):
        self.config = {"mode": "paper", "qmt": {"enabled": False}, "strategy": {"min_listing_days": 180},
                       "data": {"universe": ["600000.SH"], "allow_derived_limits": True,
                                "derived_limit_rates": {"MAIN": 0.1, "CHINEXT": 0.2}, "request_timeout": 10}}
        self.client = FakeClient()
        self.now = lambda: datetime(2026, 11, 10, 17, tzinfo=timezone(timedelta(hours=8)))
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)

    def provider(self):
        return BaostockProvider(self.config, client=self.client, now=self.now)

    def test_explicit_paper_optin_required(self):
        for section, key, value in [("data", "allow_derived_limits", False), ("qmt", "enabled", True)]:
            cfg = copy.deepcopy(self.config)
            cfg[section][key] = value
            with self.assertRaises(ValueError):
                BaostockProvider(cfg, client=self.client)
        self.config["mode"] = "live"
        with self.assertRaises(ValueError):
            self.provider()
        self.assertFalse(self.client.calls)

    def test_only_ordinary_main_and_chinext_are_supported(self):
        for symbol in ["688001.SH", "920001.BJ", "510300.SH", "600000.SZ", "300001.SH"]:
            self.config["data"]["universe"] = [symbol]
            with self.assertRaises(ValueError):
                self.provider()

    def test_normal_raw_day_has_visible_derived_limits(self):
        day = self.provider().update("2026-11-09", Path(self.temp.name))
        bar = day.bars["600000.SH"]
        self.assertEqual(str(bar.limit_up), "11.00")
        self.assertEqual(str(bar.limit_down), "9.00")
        self.assertIn("limits=derived-paper-only", day.source)
        self.assertEqual(bar.sector, "unknown")
        self.assertEqual(bar.volume, 1000000)
        self.assertEqual(day.previous_session, "2026-11-06")
        self.assertEqual(day.next_session, "2026-11-10")

    def test_chinext_uses_configured_normal_regime(self):
        self.config["data"]["universe"] = ["300001.SZ"]
        bar = self.provider().fetch_day("2026-11-09").bars["300001.SZ"]
        self.assertEqual(str(bar.limit_up), "12.00")
        self.assertEqual(str(bar.limit_down), "8.00")

    def test_st_and_suspended_are_not_assigned_tradable_bounds(self):
        self.client.overrides["2026-11-09"] = {"isST": "1"}
        self.assertIsNone(self.provider().fetch_day("2026-11-09").bars["600000.SH"].limit_up)
        self.client.overrides["2026-11-09"] = {"tradestatus": "0", "volume": "0", "turn": "", "open": "0", "high": "0", "low": "0", "close": "10"}
        bar = self.provider().fetch_day("2026-11-09").bars["600000.SH"]
        self.assertTrue(bar.suspended)
        self.assertIsNone(bar.limit_up)
        self.assertEqual(bar.open, bar.prev_close)

    def test_unknown_status_and_prices_outside_bounds_reject(self):
        for override in [{"tradestatus": "unknown"}, {"isST": ""}, {"high": "12"}, {"preclose": "NaN"}, {"volume": "1.5"}]:
            self.client.overrides["2026-11-09"] = override
            with self.assertRaises((ValueError, ArithmeticError)):
                self.provider().update("2026-11-09", Path(self.temp.name))
            self.assertFalse((Path(self.temp.name) / "2026-11-09.json").exists())

    def test_corporate_action_interval_and_reference_change_reject(self):
        self.client.actions = ["2026-11-09"]
        with self.assertRaisesRegex(ValueError, "corporate action"):
            self.provider().prepare("2026-11-02", "2026-11-09")
        self.client.actions = []
        self.client.overrides["2026-11-09"] = {"preclose": "9.9"}
        with self.assertRaisesRegex(ValueError, "reference-price change"):
            self.provider().fetch_day("2026-11-09")

    def test_new_listing_unsupported_and_future_session_rejected(self):
        self.client.ipo = "2026-10-01"
        with self.assertRaisesRegex(ValueError, "newly listed"):
            self.provider().fetch_day("2026-11-09")
        with self.assertRaisesRegex(ValueError, "not complete"):
            self.provider().fetch_day("2026-11-11")
        with self.assertRaisesRegex(ValueError, "2020-08-24"):
            self.provider().fetch_day("2020-08-21")

    def test_calendar_unknown_or_missing_day_fails_closed(self):
        self.client.drop_calendar = True
        with self.assertRaisesRegex(ValueError, "incomplete exchange calendar"):
            self.provider().calendar("2026-11-01", "2026-11-10")
        self.client.drop_calendar = False
        self.client.missing_day = "2026-11-09"
        with self.assertRaisesRegex(ValueError, "daily data missing"):
            self.provider().fetch_day("2026-11-09")

    def test_cached_snapshot_is_offline_but_source_checked(self):
        provider = self.provider()
        day = provider.update("2026-11-09", Path(self.temp.name))
        self.client.fail_endpoint = "daily"
        self.assertEqual(self.provider().update("2026-11-09", Path(self.temp.name)), day)
        foreign = Path(self.temp.name) / "foreign"
        save_day(replace(day, source="unrelated-feed"), foreign / "2026-11-09.json")
        with self.assertRaisesRegex(ValueError, "source/universe"):
            self.provider().update("2026-11-09", foreign)

    def test_prepare_uses_batched_history_without_daily_refetch(self):
        provider = self.provider()
        provider.prepare("2026-11-02", "2026-11-09")
        calls = list(self.client.calls)
        provider.fetch_day("2026-11-02")
        provider.fetch_day("2026-11-09")
        self.assertEqual(self.client.calls, calls)

    def test_diagnose_contains_no_market_prices(self):
        summary = self.provider().diagnose("2026-11-09")
        self.assertTrue(summary["ok"])
        self.assertTrue(summary["paper_only"])
        self.assertEqual(summary["limits"], "derived-not-exchange-data")
        self.assertNotIn("bars", summary)


if __name__ == "__main__":
    unittest.main()
