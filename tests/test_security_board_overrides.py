"""Reviewed identity correction only; no broker, order or actual runtime access."""
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import yaml

from autotrade import rules
from autotrade.rules import board_for_symbol, validate_quantity, round_buy_quantity
from paperlive.market import _board
from paperlive.ledger import Ledger
from paperlive.models import Instrument
from paperlive.tushare_feed import TushareMarketFeed
from tests.test_autotrade_risk import config as quantity_config
from tests.test_paperlive_ledger import quote, START, EXAMPLE
from tests.test_paperlive_tushare import Client, Clock, NOW, quote_row


ROOT = Path(__file__).resolve().parents[1]
SYMBOL = '302132.SZ'


class ReviewedBoardTests(unittest.TestCase):
    def tearDown(self):
        rules._reviewed_symbol_boards.cache_clear()

    def test_exact_reviewed_security_is_shared_but_no_prefix_is_expanded(self):
        self.assertEqual(board_for_symbol(SYMBOL), 'CHINEXT')
        self.assertEqual(_board(SYMBOL), 'CHINEXT')
        self.assertEqual(board_for_symbol('300114.SZ'), 'CHINEXT')
        for symbol in ('302000.SZ', '302131.SZ', '302133.SZ', '399001.SZ', '302132.SH',
                       '302132.BJ', '３０２１３２.SZ', '30213٢.SZ', '510300.SH'):
            with self.subTest(symbol=symbol):
                with self.assertRaises(ValueError): board_for_symbol(symbol)
                with self.assertRaises(ValueError): _board(symbol)

    def test_local_configuration_cannot_add_unreviewed_security_or_board(self):
        baseline = yaml.safe_load((ROOT/'config/security_board_overrides.yaml.example').read_text())
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); (root/'config').mkdir(); (root/'autotrade').mkdir()
            (root/'config/security_board_overrides.yaml.example').write_text(yaml.safe_dump(baseline))
            for altered in ({**baseline['overrides'], '302133.SZ': deepcopy(baseline['overrides'][SYMBOL])},
                            {SYMBOL: {**baseline['overrides'][SYMBOL], 'board': 'MAIN'}}):
                (root/'config/security_board_overrides.yaml').write_text(yaml.safe_dump({'schema_version': 1, 'overrides': altered}))
                rules._reviewed_symbol_boards.cache_clear()
                with patch.object(rules, '__file__', str(root/'autotrade/rules.py')):
                    with self.assertRaisesRegex(ValueError, 'invalid_reviewed'):
                        board_for_symbol(SYMBOL)

    def test_shared_chinext_quantity_boundaries_are_unchanged(self):
        cfg = quantity_config(); board = board_for_symbol(SYMBOL)
        self.assertEqual(round_buy_quantity(199, board, cfg), 100)
        self.assertEqual(validate_quantity('BUY', 100, 0, board, cfg), [])
        self.assertIn('invalid_lot_quantity', validate_quantity('BUY', 101, 0, board, cfg))
        self.assertIn('max_order_quantity', validate_quantity('BUY', 300100, 0, board, cfg))

    def test_existing_listing_age_and_actual_quote_limits_still_fail_closed(self):
        cfg = yaml.safe_load(EXAMPLE.read_text())
        ledger = Ledger.__new__(Ledger)  # Validators only: no SQLite construction or order entry.
        ledger.config = cfg; ledger.market = cfg['market']
        inst = Instrument(SYMBOL, '中航成飞', 'CHINEXT', '2010-08-27')
        self.assertIsNone(ledger._instrument_error(inst, START))
        self.assertEqual(ledger._instrument_error(replace(inst, board='MAIN'), START), 'invalid_instrument_identity')
        self.assertEqual(ledger._instrument_error(replace(inst, listed_date='2026-11-10'), START), 'listing_age_buy_blocked')
        observed = quote(START, inst, upper='11.73', lower='8.27')
        self.assertIsNone(ledger._quote_error(observed, START, trading=True))
        self.assertEqual(ledger._quote_error(replace(observed, limit_up=None), START, trading=True), 'missing_actual_price_limits')
        self.assertEqual(ledger._quote_error(replace(observed, ask=rules.D('12')), START, trading=True), 'missing_or_invalid_two_sided_book')


class TushareReviewedUniverseTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.client, self.clock = Client(), Clock()
        old = self.client.symbols[-1]; self.client.symbols[-1] = SYMBOL
        for row in self.client.weights:
            if row['con_code'] == old: row['con_code'] = SYMBOL
        for row in self.client.basics:
            if row['ts_code'] == old:
                row.update(ts_code=SYMBOL, name='中航成飞', market='创业板', exchange='SZSE', list_date='20100827')
        for row in self.client.limit_rows:
            if row['ts_code'] == old:
                row.update(ts_code=SYMBOL, up_limit=11.73, down_limit=8.27)
        self.cfg = {'mode': 'paper', 'runtime_dir': self.tmp.name,
                    'market': {'universe': 'hs300', 'provider': 'tushare', 'min_listing_days': 180,
                               'request_timeout': 2, 'max_quote_age_seconds': 30, 'max_future_seconds': 3,
                               'universe_refresh_seconds': 21600},
                    'tushare': {'index_code': '000300.SH', 'index_max_age_days': 45,
                                'query_batch_size': 80, 'min_call_interval_seconds': .25}}
        p = patch('paperlive.tushare_feed._LAST_REQUEST', None); p.start(); self.addCleanup(p.stop)

    def feed(self):
        return TushareMarketFeed(self.cfg, client=self.client, now=lambda: self.clock.now,
                                 sleeper=self.clock.sleep, monotonic=lambda: self.clock.monotonic_value)

    def test_complete_300_member_snapshot_retains_reviewed_long_listed_security(self):
        feed = self.feed(); instruments = feed.universe()
        self.assertEqual(len(instruments), 300)
        self.assertEqual(feed.status['universe']['member_count'], 300)
        self.assertEqual(instruments[SYMBOL].board, 'CHINEXT')
        self.assertEqual(instruments[SYMBOL].listed_date, '2010-08-27')

    def test_cache_force_refresh_repairs_299_without_touching_any_ledger(self):
        def previous_classifier(symbol):
            if symbol == SYMBOL: raise ValueError('old prefix-only classification')
            return _board(symbol)
        with patch('paperlive.tushare_feed._board', side_effect=previous_classifier):
            self.assertEqual(len(self.feed().universe()), 299)
        self.assertEqual(len(self.feed().universe()), 299)  # A fresh legacy cache needs an explicit refresh.
        self.assertEqual(len(self.feed().universe(force=True)), 300)
        self.assertFalse(list(Path(self.tmp.name).rglob('*.db')))

    def test_tushare_explicit_stk_limit_values_are_not_derived_from_chinext_percentage(self):
        feed = self.feed(); actual = feed.quotes([SYMBOL])[SYMBOL]
        self.assertEqual(actual.limit_up, rules.D('11.73'))
        self.assertEqual(actual.limit_down, rules.D('8.27'))
        self.assertTrue(any(endpoint == 'stk_limit' for endpoint, _, _ in self.client.calls))
        feed._limits = None
        (feed.directory/'limits.json').unlink()
        self.client.limit_rows = []
        missing = feed.quotes([SYMBOL])[SYMBOL]
        self.assertIsNone(missing.limit_up)
        self.assertIsNone(missing.limit_down)


if __name__ == '__main__':
    unittest.main()
