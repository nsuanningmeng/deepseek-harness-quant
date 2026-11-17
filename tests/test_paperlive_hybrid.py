"""Synthetic hybrid feed checks; no credentials, live requests or accounts."""
from decimal import Decimal
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import yaml

from paperlive.config import load_config
from paperlive.hybrid_feed import HybridMarketFeed
from paperlive.ledger import Ledger
from tests.test_paperlive_market import NOW, Opener, payload, statement
from tests.test_paperlive_tushare import Client, Clock


class HybridTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        example = Path(__file__).resolve().parents[1] / "config/paperlive.yaml.example"
        self.cfg = yaml.safe_load(example.read_text(encoding="utf-8"))
        self.cfg["runtime_dir"] = self.temp.name
        self.cfg["market"].update(provider="tencent_tushare", source_label="Synthetic hybrid", request_timeout=2)
        self.client, self.clock = Client(), Clock()
        self.patch = patch("paperlive.tushare_feed._LAST_REQUEST", None)
        self.patch.start()
        self.addCleanup(self.patch.stop)

    def feed(self, body=None, **kwargs):
        return HybridMarketFeed(self.cfg, client=self.client, now=lambda: self.clock.now,
                                sleeper=self.clock.sleep, monotonic=lambda: self.clock.monotonic_value,
                                opener=Opener(body if body is not None else statement("600000.SH", payload())), **kwargs)

    def trade_error(self, quote, now=NOW):
        # Exercise the real consumer guard without constructing any account.
        ledger = object.__new__(Ledger)
        ledger.market = self.cfg["market"]
        return ledger._quote_error(quote, now, trading=True)

    def test_provider_configuration_is_accepted_without_changing_default(self):
        path = Path(self.temp.name) / "paper.yaml"
        path.write_text("market:\n  provider: tencent_tushare\n", encoding="utf-8")
        self.assertEqual(load_config(path)["market"]["provider"], "tencent_tushare")

    def test_live_book_units_and_limits_use_actual_sources_without_rt_k(self):
        feed = self.feed()
        quote = feed.quotes(["600000.SH"])["600000.SH"]
        self.assertEqual((quote.volume, quote.bid_size, quote.ask_size), (1234500, 1200, 1000))
        self.assertEqual((quote.limit_up, quote.limit_down), (Decimal("11"), Decimal("9")))
        self.assertEqual((quote.timestamp, quote.received_at, quote.source), (NOW, NOW, "tencent"))
        self.assertIsNone(self.trade_error(quote))
        self.assertEqual([endpoint for endpoint, _, _ in self.client.calls], ["stk_limit"])
        self.assertEqual(feed.status["quotes"]["limit_source"], "tushare:stk_limit")

    def test_universe_lookup_and_calendar_remain_tushare_and_cache_is_shared(self):
        feed = self.feed()
        self.assertEqual(len(feed.universe()), 300)
        self.assertEqual(set(feed.lookup(["000001.SZ"])), {"000001.SZ"})
        self.assertTrue(feed.session(NOW)["calendar_verified"])
        self.assertEqual(feed.session(NOW)["calendar_source"], "tushare:trade_cal")
        self.assertTrue(feed.session(NOW)["open"])
        self.assertTrue((Path(self.temp.name) / "market/tushare/universe.json").is_file())
        self.assertFalse({"rt_k", "query_hs300_stocks", "query_trade_dates"}.intersection(
            endpoint for endpoint, _, _ in self.client.calls))

    def test_missing_tushare_limits_keep_price_visible_without_trading_permission(self):
        self.client.limit_rows = []
        feed = self.feed()
        quote = feed.quotes(["600000.SH"])["600000.SH"]
        self.assertEqual(quote.last, Decimal("10.10"))
        self.assertIsNone(quote.limit_up)
        self.assertIsNone(quote.limit_down)
        self.assertEqual(self.trade_error(quote), "missing_actual_price_limits")
        self.assertEqual(feed.status["quotes"]["missing_limit_symbols"], ["600000.SH"])

    def test_missing_public_limits_can_use_verified_same_day_actual_limits(self):
        feed = self.feed(statement("600000.SH", payload(**{"47": "", "48": ""})))
        quote = feed.quotes(["600000.SH"])["600000.SH"]
        self.assertEqual((quote.limit_up, quote.limit_down), (Decimal("11"), Decimal("9")))
        self.assertIsNone(self.trade_error(quote))

    def test_real_price_or_previous_close_disagreement_rejects_symbol(self):
        for updates in ({"4": "10.01"}, {"47": "11.01"}, {"48": "9.01"}):
            with self.subTest(updates=updates):
                feed = self.feed(statement("600000.SH", payload(**updates)))
                self.assertEqual(feed.quotes(["600000.SH"]), {})
                self.assertIn("600000.SH", feed.status["quotes"]["limit_conflicts"])

    def test_stale_or_prior_session_is_not_refreshed_by_receive_time(self):
        for stamp, expected in (("20261111095900", "stale_quote"),
                                ("20261110100000", "quote_session_mismatch")):
            with self.subTest(stamp=stamp):
                feed = self.feed(statement("600000.SH", payload(timestamp=stamp)))
                quote = feed.quotes(["600000.SH"])["600000.SH"]
                self.assertEqual(quote.received_at, NOW)
                self.assertEqual(self.trade_error(quote), expected)
                self.assertEqual(feed.status["quotes"]["stale_symbols"], ["600000.SH"])
                if stamp.startswith("20261110"):
                    self.assertIsNone(quote.limit_up)

    def test_future_and_duplicate_responses_cannot_become_quotes(self):
        for body in (statement("600000.SH", payload(timestamp="20261111100004")),
                     statement("600000.SH", payload()) * 2):
            feed = self.feed(body)
            self.assertEqual(feed.quotes(["600000.SH"]), {})

    def test_empty_book_and_missing_symbol_are_visible_but_not_tradeable(self):
        feed = self.feed(statement("600000.SH", payload(**{"19": "0", "20": "0"})))
        quotes = feed.quotes(["600000.SH", "600001.SH"])
        self.assertEqual(set(quotes), {"600000.SH"})
        self.assertEqual(self.trade_error(quotes["600000.SH"]), "missing_or_invalid_two_sided_book")
        self.assertIn("600001.SH", feed.status["quotes"]["errors"])

    def test_limit_outage_does_not_call_other_provider_or_expose_secret(self):
        self.client.fail = "stk_limit"
        feed = self.feed()
        quote = feed.quotes(["600000.SH"])["600000.SH"]
        self.assertEqual(self.trade_error(quote), "missing_actual_price_limits")
        self.assertNotIn("secret-that-must-never-appear-in-status", str(feed.status))
        self.assertEqual([endpoint for endpoint, _, _ in self.client.calls], ["stk_limit"])


if __name__ == "__main__":
    unittest.main()
