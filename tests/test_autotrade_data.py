"""离线行情契约、快照完整性和严格时点测试。"""
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from autotrade.data import load_day, save_day, validate_day, TushareProvider
from autotrade.demo import build_demo
from autotrade.models import Bar, D, MarketDay
from factors.turnover_signal import low_turnover_scores


def sample_day():
    bar = Bar("600001.SH", "2026-11-10", D(10), D("10.2"), D("9.9"), D("10.1"),
              D(10), 100000, D(1), "MAIN", False, False, D(11), D(9), "2020-01-02", "synthetic")
    return MarketDay(bar.date, "2026-11-09", "2026-11-11", {bar.symbol: bar}, "synthetic-test")


class FakeFrame:
    def __init__(self, rows, fields):
        self.rows = rows
        self.columns = list(fields)

    def to_dict(self, _):
        return self.rows


class FakeTushare:
    """符合 DataFrame 最小协议，无网络或账户依赖。"""
    def __init__(self):
        self.mutations = {}
        self.calls = []

    def __getattr__(self, endpoint):
        def call(fields, **params):
            self.calls.append(endpoint)
            symbol = params.get("ts_code", "600001.SH")
            payloads = {
                "daily": [{"ts_code": symbol, "trade_date": "20261110", "open": "10", "high": "10.2",
                           "low": "9.9", "close": "10.1", "pre_close": "10", "vol": "1000.03"}],
                "daily_basic": [{"ts_code": symbol, "trade_date": "20261110", "turnover_rate": "1.2"}],
                "stk_limit": [{"ts_code": symbol, "trade_date": "20261110", "up_limit": "11", "down_limit": "9"}],
                "stock_basic": [{"ts_code": symbol, "name": "合成股份", "market": "主板", "industry": "合成行业", "list_date": "20200102"}],
                "stock_st": [],
                "trade_cal": [{"cal_date": "20261109", "is_open": 1, "pretrade_date": "20261106"},
                              {"cal_date": "20261110", "is_open": 1, "pretrade_date": "20261109"},
                              {"cal_date": "20261111", "is_open": 1, "pretrade_date": "20261110"}],
            }
            rows = payloads[endpoint]
            if endpoint == "trade_cal" and params["start_date"] == params["end_date"]:
                rows = [r for r in rows if r["cal_date"] == params["start_date"]]
            if endpoint in self.mutations:
                rows = self.mutations[endpoint](rows)
            return FakeFrame(rows, fields.split(","))
        return call


class DataTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.directory = Path(self.tmp.name)
        self.day = sample_day()
        self.path = self.directory / f"{self.day.date}.json"

    def tearDown(self):
        self.tmp.cleanup()

    def test_roundtrip_and_identical_retry(self):
        save_day(self.day, self.path)
        original = self.path.read_bytes()
        save_day(self.day, self.path)
        self.assertEqual(original, self.path.read_bytes())
        self.assertEqual(load_day(self.path), self.day)

    def test_changed_snapshot_cannot_overwrite(self):
        save_day(self.day, self.path)
        with self.assertRaisesRegex(ValueError, "immutable"):
            save_day(replace(self.day, source="changed"), self.path)
        self.assertEqual(load_day(self.path), self.day)

    def test_failed_publish_leaves_no_partial_snapshot(self):
        with patch("autotrade.data.os.link", side_effect=OSError("disk unavailable")):
            with self.assertRaises(OSError):
                save_day(self.day, self.path)
        self.assertEqual(list(self.directory.iterdir()), [])

    def test_missing_exact_day_does_not_fallback(self):
        save_day(self.day, self.path)
        with self.assertRaises(FileNotFoundError):
            load_day(self.directory / "2026-11-11.json")

    def test_validation_rejects_bad_prices_dates_and_status(self):
        bar = next(iter(self.day.bars.values()))
        cases = [
            replace(bar, close=Decimal("NaN")),
            replace(bar, high=D(9)), replace(bar, date="2026-11-09"),
            replace(bar, symbol="000001.SZ"), replace(bar, volume=-1),
            replace(bar, volume=True), replace(bar, is_st="false"),
            replace(bar, turnover=D(-1)), replace(bar, suspended=True),
            replace(bar, limit_up=D(9)), replace(bar, board="UNKNOWN"),
            replace(bar, list_date="2027-01-01"), replace(bar, low=D("8.9")),
        ]
        for invalid in cases:
            with self.subTest(bar=invalid), self.assertRaises(ValueError):
                validate_day(replace(self.day, bars={bar.symbol: invalid}))

    def test_invalid_calendar(self):
        for kwargs in ({"previous_session": self.day.date}, {"next_session": "2026-11-09"},
                       {"next_session": "2026-11-31"}, {"date": "20261110"}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                validate_day(replace(self.day, **kwargs))

    def test_manifest_missing_duplicate_and_invalid_numeric_rejected(self):
        save_day(self.day, self.path)
        original = json.loads(self.path.read_text())
        for change in (lambda r: r["universe"].append("000001.SZ"),
                       lambda r: r["bars"].append(r["bars"][0]),
                       lambda r: r["bars"][0].update(close="NaN"),
                       lambda r: r["bars"][0].update(close="bad"),
                       lambda r: r["bars"][0].update(close=None)):
            raw = json.loads(json.dumps(original))
            change(raw)
            self.path.write_text(json.dumps(raw), encoding="utf-8")
            with self.assertRaises(ValueError):
                load_day(self.path)

    def test_duplicate_json_fields_rejected(self):
        self.path.write_text('{"date":"2026-11-10","date":"2026-11-11"}', encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "duplicate"):
            load_day(self.path)

    def test_filename_cannot_disguise_stale_or_future_snapshot(self):
        save_day(self.day, self.path)
        moved = self.directory / "2026-11-11.json"
        moved.write_bytes(self.path.read_bytes())
        with self.assertRaisesRegex(ValueError, "filename"):
            load_day(moved)

    def test_provider_errors_do_not_expose_credentials(self):
        client = FakeTushare()
        def broken(_):
            raise RuntimeError("token=PRIVATE_SENTINEL")
        client.mutations["daily"] = broken
        with self.assertRaises(ValueError) as error:
            self.provider(client).update("2026-11-10", self.directory)
        self.assertNotIn("PRIVATE_SENTINEL", str(error.exception))
        self.assertFalse(self.path.exists())

    def test_demo_has_complete_history_and_changing_scores(self):
        dates = build_demo(self.directory)
        self.assertGreaterEqual(len(dates), 23)
        history = {}
        for d in dates:
            day = load_day(self.directory / f"{d}.json")
            for symbol, bar in day.bars.items():
                history.setdefault(symbol, []).append(bar)
        first = low_turnover_scores(history, dates[19], 20)
        last = low_turnover_scores(history, dates[-1], 20)
        self.assertEqual(len(first), 6)
        self.assertNotEqual(max(first, key=first.get), max(last, key=last.get))
        self.assertEqual(build_demo(self.directory), dates)

    def test_factor_excludes_future_and_stale_rows(self):
        bar = next(iter(self.day.bars.values()))
        past = replace(bar, date="2026-11-09", turnover=D(2))
        future = replace(bar, date="2026-11-11", turnover=D(100))
        self.assertEqual(low_turnover_scores({bar.symbol: [future, bar, past]}, bar.date, 2),
                         {bar.symbol: D("-1.5")})
        self.assertEqual(low_turnover_scores({bar.symbol: [past]}, bar.date, 1), {})
        self.assertEqual(low_turnover_scores({bar.symbol: [bar]}, bar.date, 2), {})
        with self.assertRaisesRegex(ValueError, "duplicate"):
            low_turnover_scores({bar.symbol: [bar, bar]}, bar.date, 2)

    def provider(self, client):
        return TushareProvider({"data": {"universe": ["600001.SH"]}}, client=client,
                              now=lambda: datetime(2026, 11, 10, 18, tzinfo=timezone(timedelta(hours=8))))

    def test_provider_lazy_offline_injection_units_and_st(self):
        client = FakeTushare()
        provider = self.provider(client)
        self.assertEqual(client.calls, [])
        client.mutations["stock_st"] = lambda _: [{"ts_code": "600001.SH", "trade_date": "20261110"}]
        day = provider.update("2026-11-10", self.directory)
        self.assertEqual(day.bars["600001.SH"].volume, 100003)
        self.assertTrue(day.bars["600001.SH"].is_st)
        self.assertEqual(load_day(self.path), day)

    def test_provider_any_missing_or_stale_response_writes_nothing(self):
        for endpoint, mutation in [
            ("daily", lambda _: []), ("daily_basic", lambda rows: rows * 2),
            ("stk_limit", lambda rows: [{**rows[0], "up_limit": None}]),
            ("daily", lambda rows: [{**rows[0], "trade_date": "20261109"}]),
            ("stock_basic", lambda rows: [{**rows[0], "industry": None}]),
            ("trade_cal", lambda rows: rows[:2]),
        ]:
            client = FakeTushare()
            client.mutations[endpoint] = mutation
            with self.subTest(endpoint=endpoint), self.assertRaises(ValueError):
                self.provider(client).update("2026-11-10", self.directory)
            self.assertFalse(self.path.exists())

    def test_provider_rejects_historical_live_metadata_and_early_close(self):
        client = FakeTushare()
        provider = self.provider(client)
        with self.assertRaises(ValueError):
            provider.update("2026-11-09", self.directory)
        provider.now = lambda: datetime(2026, 11, 10, 14, 59, tzinfo=timezone(timedelta(hours=8)))
        with self.assertRaises(ValueError):
            provider.update("2026-11-10", self.directory)
        self.assertEqual(client.calls, [])

    def test_trading_day_requires_explicit_calendar(self):
        client = FakeTushare()
        provider = self.provider(client)
        self.assertTrue(provider.is_trading_day("2026-11-10"))
        client.mutations["trade_cal"] = lambda rows: [{**rows[0], "is_open": 0}]
        self.assertFalse(provider.is_trading_day("2026-11-10"))
        client.mutations["trade_cal"] = lambda _: []
        with self.assertRaises(ValueError):
            provider.is_trading_day("2026-11-10")


if __name__ == "__main__":
    unittest.main()
