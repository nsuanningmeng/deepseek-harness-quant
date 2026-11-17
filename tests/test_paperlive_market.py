"""Supplier parsing, explicit exchange calendars and cache failure boundaries."""
from datetime import datetime, timedelta
from decimal import Decimal
import json
from pathlib import Path
import tempfile
import unittest

from paperlive.market import MarketFeed, SHANGHAI, _NoRedirect, parse_tencent_quote


NOW = datetime(2026, 11, 11, 10, 0, tzinfo=SHANGHAI)


def payload(symbol="600000.SH", timestamp="20261111100000", **updates):
    fields = [""] * 50
    values = {0: "1" if symbol.endswith(".SH") else "51", 1: "测试股票", 2: symbol[:6],
              3: "10.10", 4: "10.00", 5: "10.00", 6: "12345", 9: "10.09", 10: "12",
              19: "10.11", 20: "10", 30: timestamp, 33: "10.20", 34: "9.90",
              35: "10.10/12345/12468450", 36: "12345", 47: "11.00", 48: "9.00"}
    values.update({int(key): value for key, value in updates.items()})
    for key, value in values.items():
        fields[key] = value
    return "~".join(fields)


def statement(symbol, body):
    code, exchange = symbol.split(".")
    return 'v_' + exchange.lower() + code + '="' + body + '";'


class Response:
    def __init__(self, body, url):
        self.body, self.url = body, url

    def read(self, limit):
        return self.body[:limit]

    def geturl(self):
        return self.url

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class Opener:
    def __init__(self, body, response_url=None):
        self.body, self.response_url, self.requests = body, response_url, []

    def open(self, request, timeout):
        self.requests.append((request, timeout))
        return Response(self.body.encode("gb18030"), self.response_url or request.full_url)


class Result:
    error_code = "0"

    def __init__(self, rows, fields):
        self.rows, self.fields, self.index = rows, fields, -1

    def next(self):
        self.index += 1
        return self.index < len(self.rows)

    def get_row_data(self):
        return [self.rows[self.index].get(field, "") for field in self.fields]


class BaoStock:
    def __init__(self):
        self.calls = []
        self.fail = False
        self.members = [{"updateDate": "2026-11-11", "code": code, "code_name": "测试股票"}
                        for code in ("sh.600000", "sz.300001", "sh.688001", "sh.510000")]
        self.basics = [{"code": row["code"], "code_name": row["code_name"], "ipoDate": "2020-01-01",
                        "outDate": "", "type": "1" if row["code"] != "sh.510000" else "2", "status": "1"}
                       for row in self.members]
        self.calendar = [{"calendar_date": day, "is_trading_day": value} for day, value in
                         [("2026-11-11", "1"), ("2026-11-12", "1"), ("2026-11-13", "1"),
                          ("2026-11-14", "0"), ("2026-11-15", "0"), ("2026-11-16", "1")]]

    def login(self):
        self.calls.append(("login", {}))
        if self.fail:
            raise OSError("network unavailable")
        return Result([], [])

    def logout(self):
        self.calls.append(("logout", {}))

    def query_hs300_stocks(self, **kwargs):
        self.calls.append(("query_hs300_stocks", kwargs))
        return Result(self.members, ["updateDate", "code", "code_name"])

    def query_stock_basic(self, **kwargs):
        self.calls.append(("query_stock_basic", kwargs))
        return Result(self.basics, ["code", "code_name", "ipoDate", "outDate", "type", "status"])

    def query_trade_dates(self, **kwargs):
        self.calls.append(("query_trade_dates", kwargs))
        rows = [row for row in self.calendar if kwargs["start_date"] <= row["calendar_date"] <= kwargs["end_date"]]
        return Result(rows, ["calendar_date", "is_trading_day"])


class MarketTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.config = {"mode": "paper", "runtime_dir": self.temp.name,
                       "market": {"universe": "hs300", "min_listing_days": 180, "request_timeout": 2,
                                  "max_quote_age_seconds": 30, "max_future_seconds": 3,
                                  "trading_sessions": [["09:30", "11:30"], ["13:00", "14:57"]]}}

    def feed(self, client=None, opener=None, now=NOW):
        return MarketFeed(self.config, client=client or BaoStock(), opener=opener, now=lambda: now)

    def test_quote_values_are_supplier_prices_and_share_units(self):
        quote = parse_tencent_quote("600000.SH", payload(), NOW)
        self.assertEqual(quote.last, Decimal("10.10"))
        self.assertEqual((quote.volume, quote.bid_size, quote.ask_size), (1234500, 1200, 1000))
        self.assertEqual((quote.limit_up, quote.limit_down), (Decimal("11.00"), Decimal("9.00")))
        self.assertEqual(quote.received_at, NOW)
        self.assertEqual(quote.timestamp, NOW)
        self.assertEqual(quote.source, "tencent")

    def test_cumulative_volume_resolves_observed_main_chinext_and_star_units(self):
        cases = [
            ("600519.SH", "1407.91", "1394.00", "1418.00", "8508", "1197840207", 850800),
            ("300750.SZ", "240.20", "237.12", "242.00", "105574", "2529738296", 10557400),
            ("688256.SH", "1027.50", "989.00", "1033.18", "7333629", "7438391088", 7333629),
            ("688981.SH", "115.00", "111.72", "116.28", "30553486", "3515996103", 30553486),
        ]
        for symbol, last, low, high, counter, turnover, expected in cases:
            with self.subTest(symbol=symbol):
                quote = parse_tencent_quote(symbol, payload(symbol, **{
                    "3": last, "4": last, "5": last, "6": counter, "9": last, "19": last,
                    "33": high, "34": low, "35": f"{last}/{counter}/{turnover}", "36": counter,
                    "47": "", "48": "",
                }), NOW)
                self.assertEqual(quote.volume, expected)
                self.assertEqual((quote.bid_size, quote.ask_size), (1200, 1000))

    def test_cumulative_volume_boundaries_and_no_trades(self):
        for counter, turnover, expected in (("0", "0", 0), ("1", "9.9", 1),
                                            ("1", "990", 100), ("1", "2029.80", 100)):
            with self.subTest(counter=counter, turnover=turnover):
                quote = parse_tencent_quote("600000.SH", payload(**{
                    "6": counter, "35": f"10.10/{counter}/{turnover}", "36": counter,
                }), NOW)
                self.assertEqual(quote.volume, expected)

    def test_cumulative_volume_rejects_missing_conflicting_or_unprovable_fields(self):
        cases = (
            {"35": ""}, {"35": "10.10/1/12468450"}, {"36": "12344"},
            {"35": "10.09/12345/12468450"}, {"35": "10.10/12345/NaN"},
            {"35": "10.10/12345/0"}, {"35": "10.10/12345/1000000"},
            {"6": "0", "35": "10.10/0/10.10", "36": "0"},
            {"6": "1", "35": "10.10/1/1000", "36": "1", "33": "1000", "34": "1"},
        )
        for updates in cases:
            with self.subTest(updates=updates), self.assertRaisesRegex(ValueError, "turnover|volume|quote number"):
                parse_tencent_quote("600000.SH", payload(**updates), NOW)

        bad = statement("600000.SH", payload(**{"36": "12344"}))
        feed = self.feed(opener=Opener(bad))
        self.assertEqual(feed.quotes(["600000.SH"]), {})
        self.assertIn("turnover", feed.status["quotes"]["errors"]["600000.SH"])

    def test_missing_limits_and_empty_book_are_never_derived(self):
        quote = parse_tencent_quote("600000.SH", payload(**{"47": "", "48": "0", "9": "0", "10": "0"}), NOW)
        self.assertIsNone(quote.limit_up)
        self.assertIsNone(quote.limit_down)
        self.assertEqual((quote.bid, quote.bid_size), (0, 0))

    def test_bad_quote_fields_fail_closed(self):
        for updates in ({"2": "000001"}, {"0": "51"}, {"3": "NaN"}, {"3": "-1"},
                        {"30": "20260230093000"}, {"47": "9"}, {"9": "11", "19": "10"},
                        {"10": "0.001"}, {"33": "8"}, {"1": ""}):
            with self.subTest(updates=updates), self.assertRaises(ValueError):
                parse_tencent_quote("600000.SH", payload(**updates), NOW)

    def test_batch_matches_exchange_and_reports_missing_and_stale_quotes(self):
        old = payload("000001.SZ", timestamp="20261111095900")
        opener = Opener(statement("600000.SH", payload()) + statement("000001.SZ", old))
        feed = self.feed(opener=opener)
        quotes = feed.quotes(["600000.SH", "000001.SZ", "688001.SH"])
        self.assertEqual(set(quotes), {"600000.SH", "000001.SZ"})
        request, timeout = opener.requests[0]
        self.assertEqual(request.full_url, "https://qt.gtimg.cn/q=sh600000,sz000001,sh688001")
        self.assertEqual(timeout, 2)
        self.assertEqual(feed.status["quotes"]["stale_symbols"], ["000001.SZ"])
        self.assertIn("688001.SH", feed.status["quotes"]["errors"])

    def test_duplicate_bad_and_future_records_are_omitted(self):
        cases = [statement("600000.SH", payload()) * 2,
                 statement("600000.SH", payload(timestamp="20261111100004")),
                 statement("600000.SH", payload(**{"3": "NaN"}))]
        for body in cases:
            with self.subTest(body=body):
                feed = self.feed(opener=Opener(body))
                self.assertEqual(feed.quotes(["600000.SH"]), {})
                self.assertIn("600000.SH", feed.status["quotes"]["errors"])

    def test_unknown_symbol_or_redirect_rejects_batch(self):
        for opener in [Opener(statement("000001.SZ", payload("000001.SZ"))),
                       Opener(statement("600000.SH", payload()), "https://another.example/quotes")]:
            feed = self.feed(opener=opener)
            self.assertEqual(feed.quotes(["600000.SH"]), {})
            self.assertIsNotNone(feed.status["quotes"]["last_error"])
        with self.assertRaises(ValueError):
            _NoRedirect().redirect_request(None, None, 302, "redirect", {}, "https://another.example")

    def test_universe_uses_one_bulk_metadata_query_and_filters_boards(self):
        client = BaoStock()
        feed = self.feed(client=client)
        instruments = feed.universe()
        self.assertEqual({row.board for row in instruments.values()}, {"MAIN", "CHINEXT", "STAR"})
        self.assertEqual(len(instruments), 3)
        self.assertEqual([args for name, args in client.calls if name == "query_stock_basic"], [{}])
        self.assertEqual([name for name, _ in client.calls].count("login"), 1)
        self.assertEqual(feed.status["universe"]["effective_date"], "2026-11-11")
        self.assertTrue((Path(self.temp.name) / "market" / "universe.json").exists())
        client.fail = True
        self.assertEqual(self.feed(client=client).universe(), instruments)

    def test_universe_excludes_recent_st_and_delisted_instruments(self):
        client = BaoStock()
        client.basics[0]["ipoDate"] = "2026-10-01"
        client.basics[1]["code_name"] = "*ST测试"
        client.basics[3]["status"] = "0"
        feed = self.feed(client=client)
        self.assertEqual(set(feed.universe()), {"688001.SH"})

    def test_refresh_failure_exposes_stale_membership_without_fabrication(self):
        client = BaoStock()
        feed = self.feed(client=client)
        original = feed.universe()
        client.fail = True
        self.assertEqual(feed.universe(force=True), original)
        self.assertTrue(feed.status["universe"]["cache_stale"])
        self.assertIn("network unavailable", feed.status["universe"]["last_error"])
        empty = self.feed(client=client)
        empty.directory = Path(self.temp.name) / "empty"
        with self.assertRaisesRegex(ValueError, "unavailable"):
            empty.universe()

    def test_lookup_keeps_off_index_recent_and_st_held_securities(self):
        client = BaoStock()
        client.basics.append({"code": "sz.000001", "code_name": "*ST持仓", "ipoDate": "2026-11-01",
                              "outDate": "", "type": "1", "status": "1"})
        feed = self.feed(client=client)
        self.assertNotIn("000001.SZ", feed.universe())
        held = feed.lookup(["000001.SZ"])
        self.assertEqual(held["000001.SZ"].listed_date, "2026-11-01")
        self.assertEqual(held["000001.SZ"].name, "*ST持仓")
        self.assertEqual([name for name, _ in client.calls].count("query_stock_basic"), 1)
        client.fail = True
        restarted = self.feed(client=client)
        self.assertEqual(restarted.lookup(["000001.SZ"]), held)

    def test_lookup_without_universe_uses_one_bulk_call_and_no_identity_guess(self):
        client = BaoStock()
        feed = self.feed(client=client)
        found = feed.lookup(["600000.SH", "688001.SH", "000099.SZ"])
        self.assertEqual(set(found), {"600000.SH", "688001.SH"})
        self.assertIn("000099.SZ", feed.status["lookup"]["errors"])
        self.assertEqual([args for name, args in client.calls if name == "query_stock_basic"], [{}])
        with self.assertRaises(ValueError):
            feed.lookup(["510000.SH"])

    def test_session_boundaries_lunch_and_closing_auction(self):
        feed = self.feed()
        for hour, minute, expected in [(9, 29, False), (9, 30, True), (11, 29, True),
                                       (11, 30, False), (12, 30, False), (13, 0, True),
                                       (14, 56, True), (14, 57, False), (15, 0, False)]:
            with self.subTest(hour=hour, minute=minute):
                result = feed.session(NOW.replace(hour=hour, minute=minute))
                self.assertEqual(result["open"], expected)
                self.assertTrue(result["calendar_verified"])
        lunch = feed.session(NOW.replace(hour=12))
        self.assertEqual(lunch["phase"], "午间休市")
        self.assertEqual(lunch["next_open"], "2026-11-11T13:00:00+08:00")

    def test_weekend_and_weekday_holiday_require_real_calendar_records(self):
        client = BaoStock()
        saturday = datetime(2026, 11, 14, 10, tzinfo=SHANGHAI)
        weekend = self.feed(client=client, now=saturday).session(saturday)
        self.assertFalse(weekend["open"])
        self.assertTrue(weekend["calendar_verified"])
        self.assertEqual(weekend["next_open"], "2026-11-16T09:30:00+08:00")
        holiday = datetime(2026, 10, 1, 10, tzinfo=SHANGHAI)
        client.calendar = [{"calendar_date": "2026-10-01", "is_trading_day": "0"}]
        feed = self.feed(client=client, now=holiday)
        feed.directory = Path(self.temp.name) / "holiday"
        self.assertFalse(feed.session(holiday)["open"])
        self.assertEqual(feed.session(holiday)["phase"], "非交易日")

    def test_missing_calendar_day_and_naive_clock_never_open(self):
        client = BaoStock()
        client.calendar = []
        feed = self.feed(client=client)
        session = feed.session(NOW)
        self.assertFalse(session["open"])
        self.assertFalse(session["calendar_verified"])
        self.assertIsNone(session["next_open"])
        with self.assertRaises(ValueError):
            feed.session(NOW.replace(tzinfo=None))

    def test_cached_explicit_calendar_survives_provider_outage(self):
        client = BaoStock()
        feed = self.feed(client=client)
        self.assertTrue(feed.session(NOW)["open"])
        client.fail = True
        tomorrow = NOW + timedelta(days=1, minutes=1)
        feed.now = lambda: tomorrow
        session = feed.session(tomorrow)
        self.assertTrue(session["calendar_verified"])
        self.assertTrue(session["calendar_cache_stale"])
        unknown = datetime(2027, 1, 1, 10, tzinfo=SHANGHAI)
        feed.now = lambda: unknown
        self.assertFalse(feed.session(unknown)["calendar_verified"])
        self.assertFalse(feed.session(unknown)["open"])


if __name__ == "__main__":
    unittest.main()
