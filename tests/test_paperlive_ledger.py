"""Offline paper execution invariants; every quote and instrument is synthetic."""
from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

import yaml

from paperlive.ledger import Ledger
from paperlive.models import D, Instrument, Quote

ZONE = timezone(timedelta(hours=8))
START = datetime(2026, 11, 11, 10, 0, tzinfo=ZONE)
EXAMPLE = Path(__file__).resolve().parents[1] / "config" / "paperlive.yaml.example"


def instrument(symbol="600000.SH", board="MAIN", name="Synthetic security"):
    return Instrument(symbol, name, board, "2000-01-01", "synthetic")


def quote(at, inst=None, ask="10", bid="9.99", last="10", size=10000, volume=100000,
          previous="10", upper="11", lower="9"):
    inst = inst or instrument()
    return Quote(inst.symbol, inst.name, D(last), D(bid), D(ask), size, size, D(previous),
                 D(10), D(10), D(10), volume, at, D(upper) if upper is not None else None,
                 D(lower) if lower is not None else None, source="synthetic-test", received_at=at)


class PaperLiveLedgerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.cfg = yaml.safe_load(EXAMPLE.read_text(encoding="utf-8"))
        self.cfg["runtime_dir"] = str(Path(self.temp.name) / "account")
        self.cfg["risk"]["kill_switch_file"] = str(Path(self.temp.name) / "STOP")
        self.patch_clock = patch("paperlive.ledger._now", return_value=START)
        self.patch_clock.start()
        self.addCleanup(self.patch_clock.stop)
        self.addCleanup(self.temp.cleanup)
        self.inst = instrument()

    def make(self):
        return Ledger(self.cfg)

    def submit(self, ledger, quantity=1000, price="10.05", at=START, key="buy", side="BUY", inst=None):
        inst = inst or self.inst
        return ledger.submit(side, inst.symbol, quantity, price, inst, at, True, key)

    def match(self, ledger, at, inst=None, **kwargs):
        inst = inst or self.inst
        q = quote(at, inst, **kwargs)
        return ledger.match({inst.symbol: q}, {inst.symbol: inst}, at, True)

    def close(self, ledger, value="10", day=START):
        at = day.replace(hour=15, minute=0, second=0)
        q = quote(at, self.inst, last=value, bid=value, ask=value)
        ledger.snapshot({self.inst.symbol: q}, at)
        ledger.end_session(at + timedelta(seconds=1))

    def test_initial_account_and_restart_are_not_backfilled(self):
        ledger = self.make()
        snap = ledger.snapshot(now=START)
        self.assertEqual(snap["account"]["cash"], "1000000.00")
        self.assertEqual(snap["started_at"], START.isoformat())
        self.assertEqual(snap["fills"], [])
        self.assertEqual(snap["positions"], [])
        self.assertEqual(len(snap["curve"]), 1)
        self.assertTrue(self.make().snapshot(now=START)["reconciliation"]["ok"])

    def test_cash_freeze_cancel_and_intent_idempotency(self):
        ledger = self.make()
        order = self.submit(ledger)
        self.assertEqual(order["status"], "OPEN")
        snap = ledger.snapshot(now=START)
        self.assertEqual(D(snap["account"]["frozen_cash"]), D("10055.10"))
        self.assertEqual(self.submit(self.make()), order)
        self.assertEqual(self.submit(ledger, price=D("10.050")), order)
        with self.assertRaisesRegex(ValueError, "intent_id_conflicts"):
            self.submit(ledger, quantity=2000)
        cancelled = ledger.cancel(order["order_id"], START + timedelta(seconds=1))
        self.assertEqual(cancelled["status"], "CANCELLED")
        self.assertEqual(cancelled["reason"], "user_cancelled")
        self.assertEqual(D(ledger.snapshot(now=START + timedelta(seconds=1))["account"]["frozen_cash"]), 0)
        self.assertEqual(ledger.symbols(), [])

    def test_exposure_proof_includes_active_orders_outside_display_history_and_positions(self):
        self.cfg['ui']['history_limits']['orders'] = 1
        ledger = self.make()
        older = self.submit(ledger, key='older-active')
        at = START + timedelta(seconds=1)
        newer = self.submit(ledger, key='newer-terminal', inst=instrument('600001.SH'), at=at)
        ledger.cancel(newer['order_id'], at + timedelta(seconds=1))
        snap = ledger.snapshot(now=at + timedelta(seconds=1))
        self.assertEqual([row['order_id'] for row in snap['orders']], [newer['order_id']])
        self.assertNotIn(older['order_id'], [row['order_id'] for row in snap['orders']])
        self.assertEqual(snap['exposure_symbols'], [self.inst.symbol])
        self.match(ledger, at + timedelta(seconds=2), size=1000)
        partial = ledger.snapshot(now=at + timedelta(seconds=2))
        self.assertTrue(partial['positions'])
        self.assertEqual(partial['exposure_symbols'], [self.inst.symbol])
        self.match(ledger, at + timedelta(seconds=3), size=100000)
        filled = ledger.snapshot(now=at + timedelta(seconds=3))
        self.assertEqual(filled['exposure_symbols'], [self.inst.symbol])
        self.assertEqual(filled['account']['frozen_cash'], '0.00')

    def test_unheld_corporate_action_still_blocks_direct_buy_and_pending_match(self):
        ledger = self.make()
        order = self.submit(ledger)
        at = START + timedelta(seconds=1)
        corporate = replace(quote(at), name='XD Synthetic')
        ledger.snapshot({corporate.symbol:corporate}, at)
        denied = self.submit(ledger, key='after-corporate-label', at=at)
        self.assertEqual(denied['status'], 'REJECTED')
        self.assertEqual(denied['reason'], 'corporate_action_requires_reconciliation')
        self.assertEqual(ledger.match({corporate.symbol:corporate}, {self.inst.symbol:self.inst}, at, True), [])
        self.assertEqual(ledger.snapshot(now=at)['fills'], [])

    def test_guard_cancellation_records_reason_and_preserves_terminal_history(self):
        ledger = self.make()
        order = self.submit(ledger)
        with self.assertRaisesRegex(ValueError, "invalid_cancellation_reason"):
            ledger.cancel(order['order_id'], START, reason='untrusted arbitrary text')
        self.assertEqual(ledger.snapshot(now=START)['orders'][0]['status'], 'OPEN')
        at = START + timedelta(seconds=1)
        cancelled = ledger.cancel(order['order_id'], at, reason='activation_not_ready')
        self.assertEqual(cancelled['reason'], 'activation_not_ready')
        snap = ledger.snapshot(now=at)
        self.assertEqual(D(snap['account']['frozen_cash']), 0)
        self.assertTrue(snap['reconciliation']['ok'])
        self.assertEqual(snap['fills'], [])
        self.assertEqual(ledger.cancel(order['order_id'], at + timedelta(seconds=1)), cancelled)
        with sqlite3.connect(ledger.path) as con:
            events = con.execute("SELECT payload FROM audit WHERE event='order_cancelled'").fetchall()
        self.assertEqual(len(events), 1)
        import json
        self.assertEqual(json.loads(events[0][0])['reason'], 'activation_not_ready')

    def test_match_diagnostics_explain_limit_then_fill_without_duplicate_frames(self):
        ledger = self.make()
        order = self.submit(ledger, price='10.00')
        at = START + timedelta(seconds=1)
        q = quote(at)
        self.assertEqual(ledger.match({q.symbol: q}, {q.symbol: self.inst}, at, True), [])
        snapshot = ledger.snapshot(now=at)
        check = snapshot['orders'][0]['last_match_check']
        self.assertEqual(check['reason'], 'limit_price_not_reached')
        self.assertEqual(check['bid'], '9.99')
        self.assertEqual(check['ask'], '10')
        self.assertEqual(D(check['execution_price']), D('10.01'))
        self.assertEqual(check['limit_price'], '10.00')
        self.assertEqual(check['remaining_before'], 1000)
        self.assertEqual(check['capacity'], 1000)
        self.assertEqual(check['quote_time'], at.isoformat())
        self.assertEqual(check['received_at'], at.isoformat())
        self.assertEqual(check['filled_quantity'], 0)
        self.assertIsNone(check['fill_id'])
        # Polling the same quote must preserve the informative first reason.
        ledger.match({q.symbol: q}, {q.symbol: self.inst}, at + timedelta(seconds=1), True)
        self.assertEqual(ledger.snapshot(now=at + timedelta(seconds=1))['execution_diagnostics']['total'], 1)
        fills = self.match(ledger, at + timedelta(seconds=2), ask='9.98', bid='9.97', last='9.98')
        snapshot = ledger.snapshot(now=at + timedelta(seconds=2))
        self.assertEqual(snapshot['execution_diagnostics']['total'], 2)
        latest = snapshot['orders'][0]['last_match_check']
        self.assertEqual(latest['outcome'], 'filled')
        self.assertEqual(latest['fill_id'], fills[0]['fill_id'])
        self.assertEqual(latest['execution_price'], fills[0]['price'])
        self.assertEqual(latest['filled_quantity'], 1000)
        self.assertTrue(snapshot['reconciliation']['ok'])
        with sqlite3.connect(ledger.path) as con:
            for command in ('UPDATE match_checks SET reason="forged"', 'DELETE FROM match_checks'):
                with self.assertRaisesRegex(sqlite3.IntegrityError, 'immutable match checks'):
                    con.execute(command)

    def test_match_diagnostics_record_capacity_and_partial_fill_separately(self):
        ledger = self.make()
        self.submit(ledger, key='first')
        self.submit(ledger, key='second', at=START + timedelta(microseconds=1))
        at = START + timedelta(seconds=1)
        self.match(ledger, at, size=2000)
        snap = ledger.snapshot(now=at)
        checks = {row['order_id']: row for row in snap['execution_diagnostics']['records']}
        self.assertEqual(checks['first']['reason'], 'partially_filled')
        self.assertEqual(checks['first']['filled_quantity'], 200)
        self.assertEqual(checks['first']['capacity'], 200)
        self.assertEqual(checks['second']['reason'], 'insufficient_visible_capacity')
        self.assertEqual(checks['second']['capacity'], 0)
        self.assertEqual(checks['second']['filled_quantity'], 0)
        self.assertTrue(snap['reconciliation']['ok'])

    def test_match_diagnostics_keep_quote_validation_and_order_timing_reasons(self):
        ledger = self.make()
        self.submit(ledger)
        self.match(ledger, START)
        at = START + timedelta(seconds=1)
        q = quote(at, upper=None)
        ledger.match({q.symbol: q}, {q.symbol: self.inst}, at, True)
        # Same invalid frame is not counted repeatedly.
        ledger.match({q.symbol: q}, {q.symbol: self.inst}, at + timedelta(seconds=1), True)
        stale = quote(at + timedelta(seconds=2))
        ledger.match({stale.symbol: stale}, {stale.symbol: self.inst}, at + timedelta(seconds=60), True)
        snap = ledger.snapshot(now=at + timedelta(seconds=60))
        reasons = [row['reason'] for row in snap['execution_diagnostics']['records']]
        self.assertEqual(reasons, ['stale_quote', 'missing_actual_price_limits', 'quote_not_after_order'])
        self.assertEqual(snap['fills'], [])
        self.assertTrue(snap['reconciliation']['ok'])

    def test_existing_account_adds_diagnostic_schema_without_fabricating_history(self):
        ledger = self.make()
        order = self.submit(ledger)
        self.match(ledger, START + timedelta(seconds=1))
        tables = ('account', 'orders', 'fills', 'lots', 'audit', 'quote_books', 'meta')
        with sqlite3.connect(ledger.path) as con:
            before = {name: con.execute('SELECT * FROM ' + name).fetchall() for name in tables}
            # Model an existing account from before the diagnostic migration.
            con.execute('DROP TABLE match_checks')
        migrated = Ledger(self.cfg, existing_only=True)
        with sqlite3.connect(ledger.path) as con:
            after = {name: con.execute('SELECT * FROM ' + name).fetchall() for name in tables}
        self.assertEqual(before, after)
        snap = migrated.snapshot(now=START + timedelta(seconds=1))
        self.assertEqual(snap['execution_diagnostics']['total'], 0)
        self.assertIsNone(snap['orders'][0]['last_match_check'])
        self.assertEqual(snap['orders'][0]['status'], 'FILLED')
        self.assertEqual(len(snap['fills']), 1)

    def test_diagnostic_api_is_bounded_and_empty_accounts_do_not_add_checks(self):
        self.cfg['ui']['history_limits']['fills'] = 1
        ledger = self.make()
        self.match(ledger, START)
        self.assertEqual(ledger.snapshot(now=START)['execution_diagnostics']['total'], 0)
        self.submit(ledger, price='10.00')
        self.match(ledger, START + timedelta(seconds=1))
        self.match(ledger, START + timedelta(seconds=2))
        snap = ledger.snapshot(now=START + timedelta(seconds=2))
        self.assertEqual(snap['execution_diagnostics']['total'], 2)
        self.assertEqual(len(snap['execution_diagnostics']['records']), 1)
        self.assertEqual(snap['execution_diagnostics']['reason_counts'], [
            {'reason': 'limit_price_not_reached', 'count': 2}])
        self.assertEqual(snap['orders'][0]['last_match_check'], snap['execution_diagnostics']['records'][0])

    def test_match_diagnostic_and_fill_commit_atomically(self):
        ledger = self.make()
        self.submit(ledger)
        with patch.object(ledger, '_match_check', side_effect=RuntimeError('synthetic diagnostic write failure')):
            with self.assertRaisesRegex(RuntimeError, 'synthetic diagnostic'):
                self.match(ledger, START + timedelta(seconds=1))
        snap = ledger.snapshot(now=START + timedelta(seconds=1))
        self.assertEqual(snap['fills'], [])
        self.assertEqual(snap['execution_diagnostics']['total'], 0)
        self.assertEqual(snap['orders'][0]['status'], 'OPEN')
        self.assertEqual(snap['account']['cash'], '1000000.00')
        self.assertTrue(snap['reconciliation']['ok'])

    def test_match_diagnostics_record_each_missing_quote_transition_once(self):
        ledger = self.make()
        self.submit(ledger, price='10.00')
        for offset in (1, 2):
            ledger.match({}, {self.inst.symbol: self.inst}, START + timedelta(seconds=offset), True)
        self.assertEqual(ledger.snapshot(now=START + timedelta(seconds=2))['execution_diagnostics']['total'], 1)
        q = quote(START + timedelta(seconds=3))
        ledger.match({q.symbol: q}, {q.symbol: self.inst}, q.timestamp, True)
        ledger.match({q.symbol: q}, {q.symbol: self.inst}, START + timedelta(seconds=4), True)
        for offset in (5, 6):
            ledger.match({}, {self.inst.symbol: self.inst}, START + timedelta(seconds=offset), True)
        # Replaying a previously processed quote is not new market recovery.
        ledger.match({q.symbol: q}, {q.symbol: self.inst}, START + timedelta(seconds=7), True)
        snap = ledger.snapshot(now=START + timedelta(seconds=7))
        self.assertEqual(snap['execution_diagnostics']['total'], 3)
        records = snap['execution_diagnostics']['records']
        self.assertEqual([row['reason'] for row in records], [
            'missing_quote', 'limit_price_not_reached', 'missing_quote'])
        self.assertEqual(records[0]['checked_at'], (START + timedelta(seconds=5)).isoformat())
        self.assertEqual(snap['orders'][0]['last_match_check'], records[0])
        self.assertEqual(snap['orders'][0]['status'], 'OPEN')
        self.assertEqual(snap['fills'], [])
        # A genuinely new eligible quote may still fill the unchanged order.
        fills = self.match(ledger, START + timedelta(seconds=8), ask='9.98', bid='9.97', last='9.98')
        snap = ledger.snapshot(now=START + timedelta(seconds=8))
        self.assertEqual(len(fills), 1)
        self.assertEqual(snap['execution_diagnostics']['total'], 4)
        self.assertEqual(snap['orders'][0]['last_match_check']['fill_id'], fills[0]['fill_id'])
        self.assertTrue(snap['reconciliation']['ok'])

    def test_partial_fills_pay_order_minimum_once_and_preserve_reservation(self):
        ledger = self.make()
        self.submit(ledger)
        first = self.match(ledger, START + timedelta(seconds=1), size=2000)
        second = self.match(ledger, START + timedelta(seconds=2), size=2000)
        final = self.match(ledger, START + timedelta(seconds=3), size=10000)
        self.assertEqual([fills[0]["quantity"] for fills in (first, second, final)], [200, 200, 600])
        self.assertEqual(sum(D(fill["fees"]) for fill in first + second + final), D("5.10"))
        snap = ledger.snapshot(now=START + timedelta(seconds=3))
        self.assertEqual(snap["orders"][0]["status"], "FILLED")
        self.assertEqual(D(snap["account"]["frozen_cash"]), 0)
        self.assertEqual(snap["positions"][0]["quantity"], 1000)
        self.assertEqual(snap["positions"][0]["available"], 0)
        self.assertTrue(snap["reconciliation"]["ok"])

    def test_visible_book_capacity_is_shared_not_cumulative_volume(self):
        ledger = self.make()
        self.submit(ledger, key="first")
        self.submit(ledger, key="second", at=START + timedelta(microseconds=1))
        fills = self.match(ledger, START + timedelta(seconds=1), size=2000, volume=10**12)
        self.assertEqual(sum(fill["quantity"] for fill in fills), 200)
        self.assertEqual(len(fills), 1)
        self.assertEqual(fills[0]["order_id"], "first")

    def test_no_fills_from_before_order_stale_future_or_repeated_quotes(self):
        ledger = self.make()
        self.submit(ledger, quantity=2000)
        for q, now in [(quote(START), START),
                       (quote(START + timedelta(seconds=2)), START + timedelta(seconds=1)),
                       (replace(quote(START + timedelta(seconds=1)), received_at=None), START + timedelta(seconds=2)),
                       (quote(START + timedelta(seconds=1)), START + timedelta(seconds=60))]:
            with self.subTest(timestamp=q.timestamp):
                self.assertEqual(ledger.match({q.symbol: q}, {q.symbol: self.inst}, now, True), [])
        # Separate account: chronological execution resumes on a new valid quote.
        at = START + timedelta(seconds=61)
        q = quote(at)
        fills = ledger.match({q.symbol:q}, {q.symbol:self.inst}, at, True)
        self.assertEqual(fills[0]["quantity"], 1000)
        self.assertEqual(self.make().match({q.symbol:q}, {q.symbol:self.inst}, at, True), [])
        older = quote(at - timedelta(seconds=1), size=999999)
        self.assertEqual(ledger.match({q.symbol:older}, {q.symbol:self.inst}, at + timedelta(seconds=1), True), [])
        same_time = replace(q, ask_size=999999, volume=q.volume + 1, received_at=at + timedelta(seconds=1))
        self.assertEqual(ledger.match({q.symbol:same_time}, {q.symbol:self.inst}, at + timedelta(seconds=1), True), [])
        self.assertEqual(len(self.match(ledger, at + timedelta(seconds=2))), 1)

    def test_limit_price_slippage_and_actual_limits_are_all_required(self):
        ledger = self.make()
        self.submit(ledger, price="10.00")
        self.assertEqual(self.match(ledger, START + timedelta(seconds=1)), [])
        self.assertEqual(self.match(ledger, START + timedelta(seconds=2), upper=None), [])
        self.assertEqual(self.match(ledger, START + timedelta(seconds=3), size=0), [])
        self.assertEqual(self.match(ledger, START + timedelta(seconds=4), ask="11", bid="10.99", last="11"), [])
        filled = self.match(ledger, START + timedelta(seconds=5), ask="9.98", bid="9.97", last="9.98")
        self.assertEqual(D(filled[0]["price"]), D("9.99"))

    def test_main_chinext_star_and_metadata_rules(self):
        ledger = self.make()
        cases = [(instrument(), 101), (instrument(name="*ST synthetic"), 100),
                 (replace(instrument(), listed_date="2026-11-01"), 100),
                 (instrument("688001.SH", "STAR"), 199),
                 (instrument("430001.BJ", "BSE"), 100)]
        for index, (inst, quantity) in enumerate(cases):
            self.assertEqual(self.submit(ledger, quantity=quantity, inst=inst, key=f"bad{index}")["status"], "REJECTED")
        chinext = instrument("300001.SZ", "CHINEXT")
        self.assertEqual(self.submit(ledger, quantity=100, inst=chinext, key="chinext")["status"], "OPEN")
        star = instrument("688001.SH", "STAR")
        self.assertEqual(self.submit(ledger, quantity=201, inst=star, key="star")["status"], "OPEN")
        fills = self.match(ledger, START + timedelta(seconds=1), inst=star, size=10)
        self.assertEqual(fills[0]["quantity"], 1)

    def test_t1_sell_freeze_fifo_and_exact_roundtrip_cash(self):
        ledger = self.make()
        self.submit(ledger)
        self.match(ledger, START + timedelta(seconds=1))
        self.assertEqual(self.submit(ledger, side="SELL", at=START + timedelta(seconds=2), key="same-day")["status"], "REJECTED")
        self.close(ledger)
        tomorrow = START + timedelta(days=1)
        ledger.snapshot({self.inst.symbol:quote(tomorrow)}, tomorrow)
        sell = self.submit(ledger, side="SELL", price="9.95", at=tomorrow, key="sell")
        self.assertEqual(sell["reserved_quantity"], 1000)
        denied = self.submit(ledger, side="SELL", quantity=100, price="9.95", at=tomorrow, key="over-sell")
        self.assertEqual(denied["reason"], "insufficient_sellable_shares_t1")
        self.match(ledger, tomorrow + timedelta(seconds=1))
        snap = ledger.snapshot(now=tomorrow + timedelta(seconds=1))
        self.assertEqual(snap["positions"], [])
        self.assertEqual(D(snap["account"]["cash"]), D("999954.81"))
        self.assertEqual(D(snap["account"]["realized_pnl"]), D("-45.19"))
        self.assertTrue(snap["reconciliation"]["ok"])

    def test_sell_cancel_releases_reserved_stock(self):
        ledger = self.make();self.submit(ledger);self.match(ledger, START + timedelta(seconds=1));self.close(ledger)
        tomorrow = START + timedelta(days=1)
        ledger.snapshot({self.inst.symbol:quote(tomorrow)}, tomorrow)
        sell = self.submit(ledger, side="SELL", price="9.95", at=tomorrow, key="sell")
        self.assertEqual(ledger.snapshot(now=tomorrow)["positions"][0]["available"], 0)
        ledger.cancel(sell["order_id"], tomorrow)
        self.assertEqual(ledger.snapshot(now=tomorrow)["positions"][0]["available"], 1000)

    def test_repeated_partial_sales_reconcile_fifo_cost_and_fee_minimum(self):
        ledger = self.make();self.submit(ledger)
        for offset in range(1, 6):
            self.match(ledger, START + timedelta(seconds=offset), size=2000)
        self.close(ledger)
        tomorrow = START + timedelta(days=1)
        ledger.snapshot({self.inst.symbol:quote(tomorrow)}, tomorrow)
        self.submit(ledger, side="SELL", price="9.95", at=tomorrow, key="sell")
        sold = []
        for offset in range(1, 6):
            sold.extend(self.match(ledger, tomorrow + timedelta(seconds=offset), size=2000))
        self.assertEqual(len(sold), 5)
        self.assertEqual(sum(D(fill["fees"]) for fill in sold), D("10.09"))
        snap = ledger.snapshot(now=tomorrow + timedelta(seconds=5))
        self.assertEqual(D(snap["account"]["cash"]), D("999954.81"))
        self.assertTrue(snap["reconciliation"]["ok"])

    def test_closed_lunch_and_session_end_do_not_execute(self):
        ledger = self.make();self.submit(ledger)
        for at, flag in [(START + timedelta(seconds=1), False), (START.replace(hour=12), True), (START.replace(hour=14,minute=57), True)]:
            q = quote(at)
            self.assertEqual(ledger.match({q.symbol:q}, {q.symbol:self.inst}, at, flag), [])
        cancelled = ledger.end_session(START.replace(hour=15))
        self.assertEqual(cancelled[0]["status"], "CANCELLED")
        self.assertEqual(D(ledger.snapshot(now=START.replace(hour=15))["account"]["frozen_cash"]), 0)

    def test_order_position_and_exposure_limits_include_pending_orders(self):
        self.cfg["risk"]["max_positions"] = 1
        ledger = self.make();self.submit(ledger, quantity=100)
        other = instrument("600001.SH")
        denied = self.submit(ledger, quantity=100, inst=other, key="second-symbol")
        self.assertEqual(denied["reason"], "max_positions")
        denied = self.submit(ledger, quantity=20000, key="large")
        self.assertEqual(denied["reason"], "max_order_notional")
        denied = self.submit(ledger, quantity=14900, key="weight")
        self.assertEqual(denied["reason"], "max_symbol_weight")

    def test_daily_order_cap_and_insufficient_available_cash(self):
        self.cfg["risk"]["max_daily_orders"] = 2
        ledger = self.make()
        self.submit(ledger, quantity=100, key="one");self.submit(ledger, quantity=100, key="two")
        self.assertEqual(self.submit(ledger, quantity=100, key="three")["reason"], "max_daily_orders")
        cfg = deepcopy(self.cfg);cfg["runtime_dir"] = str(Path(self.temp.name)/"small")
        cfg["broker"]["initial_cash"] = 1010
        cfg["risk"].update(max_symbol_weight=1,max_total_weight=1,max_daily_orders=20)
        small = Ledger(cfg)
        self.assertEqual(self.submit(small, quantity=1000, price="1.00", key="cash")["status"], "OPEN")
        star = instrument("688001.SH", "STAR")
        denied = self.submit(small, quantity=200, price="0.01", key="no-cash", inst=star)
        self.assertIn(denied["reason"], {"insufficient_available_cash", "max_total_weight"})
        self.assertTrue(small.snapshot(now=START)["reconciliation"]["ok"])

    def test_drawdown_and_bad_reference_block_new_execution(self):
        self.cfg["risk"].update(max_symbol_weight=1,max_total_weight=1,max_order_notional=900000)
        ledger = self.make();self.submit(ledger, quantity=50000)
        self.match(ledger, START + timedelta(seconds=1), size=500000)
        down = quote(START + timedelta(seconds=2), last="6", bid="5.99", ask="6", lower="5", upper="15")
        ledger.snapshot({down.symbol:down}, START + timedelta(seconds=2))
        denied = self.submit(ledger, quantity=100, key="drawdown", at=START + timedelta(seconds=2))
        self.assertEqual(denied["reason"], "drawdown_buy_blocked")
        self.close(ledger, value="6")
        tomorrow = START + timedelta(days=1)
        changed = quote(tomorrow, previous="5", last="6", bid="5.99", ask="6", lower="4", upper="8")
        snap = ledger.snapshot({changed.symbol:changed}, tomorrow)
        self.assertFalse(snap["account"]["valuation_complete"])
        self.assertIn("reference_price_changed", snap["positions"][0]["valuation_warning"])
        self.assertEqual(self.make().snapshot(now=tomorrow)["positions"][0]["valuation_warning"], "reference_price_changed_requires_reconciliation")

    def test_corporate_action_prefix_and_missing_close_are_not_silently_valued(self):
        ledger = self.make();self.submit(ledger);self.match(ledger, START + timedelta(seconds=1))
        tomorrow = START + timedelta(days=1)
        q = quote(tomorrow)
        snap = ledger.snapshot({q.symbol:q}, tomorrow)
        self.assertFalse(snap["account"]["valuation_complete"])
        self.assertEqual(snap["positions"][0]["valuation_warning"], "previous_close_unverified")
        corporate = replace(q, name="XD Synthetic")
        snap = ledger.snapshot({q.symbol:corporate}, tomorrow)
        self.assertFalse(snap["account"]["valuation_complete"])
        self.assertEqual(snap["positions"][0]["valuation_warning"], "corporate_action_requires_reconciliation")

    def test_frozen_policy_and_reconciliation_prevent_mutation_and_tampering(self):
        ledger = self.make();self.submit(ledger);self.match(ledger, START + timedelta(seconds=1))
        changed = deepcopy(self.cfg);changed["broker"]["commission_rate"] = "0.001"
        with self.assertRaisesRegex(ValueError, "execution_config_changed"):
            Ledger(changed)
        with sqlite3.connect(ledger.path) as con:
            with self.assertRaises(sqlite3.IntegrityError):
                con.execute("UPDATE fills SET price='1'")
            con.execute("UPDATE account SET cash='999999999' WHERE id=1")
        snap = ledger.snapshot(now=START + timedelta(seconds=2))
        self.assertFalse(snap["reconciliation"]["ok"])
        with self.assertRaisesRegex(ValueError, "reconciliation_failed"):
            self.submit(ledger, quantity=100, at=START + timedelta(seconds=2), key="tampered")

    def test_idle_quotes_do_not_create_execution_book_history(self):
        ledger = self.make()
        for offset in range(1, 5):
            self.assertEqual(self.match(ledger, START + timedelta(seconds=offset)), [])
        with sqlite3.connect(ledger.path) as con:
            self.assertEqual(con.execute("SELECT COUNT(*) FROM quote_books").fetchone()[0], 0)

    def test_ui_history_is_bounded_without_truncating_accounting(self):
        self.cfg["ui"]["history_limits"] = {"orders": 2, "fills": 2, "curve": 2}
        ledger = self.make()
        for index in range(4):
            at = START + timedelta(seconds=2 * index)
            self.submit(ledger, quantity=100, at=at, key=f"order{index}")
            self.match(ledger, at + timedelta(seconds=1))
        snap = ledger.snapshot(now=START + timedelta(seconds=8))
        self.assertEqual([len(snap[key]) for key in ("orders", "fills", "curve")], [2, 2, 2])
        self.assertEqual(snap["history_totals"]["orders"], 4)
        self.assertEqual(snap["history_totals"]["fills"], 4)
        self.assertEqual(snap["positions"][0]["quantity"], 400)
        self.assertTrue(snap["reconciliation"]["ok"])
        cfg = deepcopy(self.cfg);cfg["ui"]["history_limits"]["curve"] = 3
        cfg["ai"]["timeout_seconds"] += 1
        cfg["market"]["refresh_seconds"] += 1
        self.assertTrue(Ledger(cfg).snapshot(now=START + timedelta(seconds=8))["reconciliation"]["ok"])

    def test_kill_switch_cancels_open_orders_and_blocks_new_orders(self):
        path = Path(self.temp.name) / "STOP"
        self.cfg["risk"]["kill_switch_file"] = str(path)
        ledger = self.make();self.submit(ledger)
        path.touch()
        self.assertEqual(self.match(ledger, START + timedelta(seconds=1)), [])
        snap = ledger.snapshot(now=START + timedelta(seconds=1))
        self.assertEqual(snap["orders"][0]["status"], "CANCELLED")
        self.assertEqual(D(snap["account"]["frozen_cash"]), 0)
        self.assertTrue(snap["kill_switch_active"])
        denied = self.submit(ledger, quantity=100, key="blocked", at=START + timedelta(seconds=2))
        self.assertEqual(denied["reason"], "kill_switch_active")

    def test_concurrent_duplicate_match_has_one_fill_and_one_cash_debit(self):
        ledger = self.make();self.submit(ledger)
        at = START + timedelta(seconds=1)
        q = quote(at)
        def run(_):
            return Ledger(self.cfg).match({q.symbol:q}, {q.symbol:self.inst}, at, True)
        with ThreadPoolExecutor(max_workers=4) as pool:
            batches = list(pool.map(run, range(8)))
        self.assertEqual(sum(len(batch) for batch in batches), 1)
        snap = ledger.snapshot(now=at)
        self.assertEqual(len(snap["fills"]), 1)
        self.assertEqual(snap["positions"][0]["quantity"], 1000)
        self.assertTrue(snap["reconciliation"]["ok"])

    def test_same_day_bar_high_low_and_total_volume_cannot_change_a_fill(self):
        outcomes = []
        for index in range(2):
            cfg = deepcopy(self.cfg);cfg["runtime_dir"] = str(Path(self.temp.name)/f"path{index}")
            ledger = Ledger(cfg);self.submit(ledger)
            at = START + timedelta(seconds=1)
            q = quote(at, size=2000, volume=1 if index == 0 else 10**15)
            q = replace(q, high=D(10000) if index else D(10), low=D("0.01") if index else D(10))
            fill = ledger.match({q.symbol:q}, {q.symbol:self.inst}, at, True)[0]
            outcomes.append((fill["quantity"],fill["price"],fill["fees"]))
        self.assertEqual(outcomes[0], outcomes[1])

    def test_runtime_policy_mutation_and_past_valuation_are_rejected(self):
        ledger = self.make()
        with self.assertRaisesRegex(ValueError, "before_account_start"):
            ledger.snapshot(now=START - timedelta(days=1))
        ledger.config["risk"]["max_drawdown"] = 1
        with self.assertRaisesRegex(ValueError, "config_changed_during_runtime"):
            self.submit(ledger)


if __name__ == "__main__":
    unittest.main()
