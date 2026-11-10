"""No XtQuant terminal or real account is used by these tests."""
import copy
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timedelta
import hashlib
import json
from pathlib import Path
import tempfile
import threading
import time
from types import SimpleNamespace as NS
import unittest
from unittest.mock import patch

from autotrade.config import load_config
from autotrade.models import Bar, D, MarketDay, OrderIntent
from autotrade.qmt import LiveMarketSnapshot, QmtError, QmtSession, SHANGHAI


CONSTANTS = NS(STOCK_BUY=23, STOCK_SELL=24, FIX_PRICE=11,
               ORDER_UNREPORTED=48, ORDER_WAIT_REPORTING=49, ORDER_REPORTED=50,
               ORDER_REPORTED_CANCEL=51, ORDER_PARTSUCC_CANCEL=52,
               ORDER_PART_CANCEL=53, ORDER_CANCELED=54, ORDER_PART_SUCC=55,
               ORDER_SUCCEEDED=56, ORDER_JUNK=57, ORDER_UNKNOWN=255)
ACCOUNT = "123456789012"
NOW = datetime(2026, 11, 10, 10, 0, tzinfo=SHANGHAI)


def broker_order(broker_id=1, status=50, filled=0, quantity=100, remark="manual"):
    return NS(account_id=ACCOUNT, stock_code="600001.SH", order_id=broker_id,
              order_type=23, order_volume=quantity, price=10.05, price_type=50,
              traded_volume=filled, order_status=status, order_remark=remark,
              order_time=int(NOW.timestamp()))


def broker_trade(trade_id="1", broker_id=1, quantity=100, remark="manual"):
    return NS(account_id=ACCOUNT, stock_code="600001.SH", order_id=broker_id,
              order_type=23, traded_id=trade_id, traded_volume=quantity,
              traded_price=10.0, traded_amount=10.0 * quantity,
              order_remark=remark, traded_time=int(NOW.timestamp()))


class FakeTrader:
    def __init__(self):
        self.callback = None
        self.orders = []
        self.trades = []
        self.positions = []
        self.asset = NS(account_id=ACCOUNT, cash=1000000, frozen_cash=0,
                        market_value=0, total_asset=1000000)
        self.order_calls = []
        self.submitted_price_types = []
        self.cancel_calls = []
        self.stop_count = 0
        self.connect_result = 0
        self.subscribe_result = 0
        self.submit_mode = "normal"
        self.query_delay = 0
        self.status_on_subscribe = 0

    def register_callback(self, callback): self.callback = callback
    def start(self): pass
    def connect(self): return self.connect_result
    def subscribe(self, account):
        if self.status_on_subscribe is not None:
            self.callback.on_account_status(NS(account_id=ACCOUNT, status=self.status_on_subscribe))
        return self.subscribe_result
    def stop(self): self.stop_count += 1
    def query_stock_asset(self, account):
        time.sleep(self.query_delay)
        return copy.deepcopy(self.asset)
    def query_stock_positions(self, account): return copy.deepcopy(self.positions)
    def query_stock_orders(self, account): return copy.deepcopy(self.orders)
    def query_stock_trades(self, account): return copy.deepcopy(self.trades)
    def order_stock(self, account, symbol, side, quantity, price_type, price, strategy, remark):
        self.order_calls.append((symbol, side, quantity, price, remark))
        self.submitted_price_types.append(price_type)
        if self.submit_mode == "reject":
            return -1
        order = broker_order(len(self.orders) + 1, quantity=quantity, remark=remark)
        order.stock_code, order.order_type, order.price = symbol, side, price
        self.orders.append(order)
        if self.submit_mode == "timeout":
            time.sleep(0.3)
        if self.submit_mode == "exception":
            raise OSError("sensitive SDK detail " + ACCOUNT)
        return order.order_id
    def cancel_order_stock(self, account, broker_id):
        self.cancel_calls.append(broker_id)
        return 0


class QmtTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.cfg = load_config()
        self.cfg["runtime_dir"] = self.temp.name
        self.cfg["risk"]["kill_switch_file"] = str(Path(self.temp.name) / "STOP")
        self.cfg["qmt"].update(enabled=True, read_only=True, live_trading_enabled=False,
                               account_id=ACCOUNT, userdata_path=self.temp.name,
                               query_timeout_seconds=10, session_id=100,
                               max_quote_age_seconds=5, reconciliation_tolerance=1,
                               authorization_file=str(Path(self.temp.name) / "authorization.json"))
        self.fake = FakeTrader()
        self.factory_calls = []
        self.sessions = []
        self.addCleanup(self.close_sessions)
        self.clock = NOW

    def close_sessions(self):
        for session in self.sessions:
            session.close()

    def factory(self, path, session_id, account_id, callback, account_type):
        self.factory_calls.append((path, session_id, account_type))
        return NS(trader=self.fake, account=NS(account_id=account_id), constants=CONSTANTS, callback=callback)

    def market(self, symbols):
        bars = {}
        for symbol in symbols:
            bars[symbol] = Bar(symbol, NOW.date().isoformat(), D("9.95"), D("10.1"), D("9.9"), D(10),
                               D(10), 100000, D(1), "MAIN", False, False,
                               D(11), D(9), "2020-01-02", "synthetic")
        return LiveMarketSnapshot(self.clock, MarketDay(NOW.date().isoformat(), "2026-11-09", "2026-11-11", bars, "qmt:live-tick;current-instrument-limits;test-fixture"),
                                  {symbol: self.clock for symbol in symbols})

    def session(self, connect=True, market_provider=None):
        session = QmtSession(self.cfg, sdk_factory=self.factory,
                             market_provider=market_provider or self.market, now=lambda: self.clock)
        self.sessions.append(session)
        if connect:
            session.connect()
        return session

    def allow_live(self):
        self.cfg["mode"] = "live"
        self.cfg["qmt"].update(read_only=False, live_trading_enabled=True,
                               broker_permission_confirmed=True, manual_authorization=True)
        authorization = {"authorization_id": "synthetic-test-authorization", "account_sha256": hashlib.sha256(ACCOUNT.encode()).hexdigest(),
                         "issued_at": (NOW - timedelta(minutes=1)).isoformat(), "expires_at": (NOW + timedelta(minutes=10)).isoformat(),
                         "scopes": ["submit", "cancel"], "symbols": ["600001.SH"],
                         "max_order_notional": 20000, "max_daily_notional": 40000}
        Path(self.cfg["qmt"]["authorization_file"]).write_text(json.dumps(authorization), encoding="utf-8")

    def intent(self, local_id="local-1", quantity=100):
        return OrderIntent(local_id, "plan-1", "600001.SH", "BUY", quantity,
                           NOW.date().isoformat(), "2026-11-09", D("10.05"))

    def test_import_and_construction_do_not_connect(self):
        self.session(connect=False)
        self.assertEqual(self.factory_calls, [])

    def test_default_disabled_does_not_import_sdk(self):
        self.cfg["qmt"]["enabled"] = False
        with self.assertRaises(PermissionError):
            self.session()
        self.assertEqual(self.factory_calls, [])

    def test_connect_subscribe_snapshot_reconcile_and_mask(self):
        session = self.session()
        snapshot = session.snapshot()
        self.assertEqual(snapshot["asset"]["account"], "****9012")
        self.assertTrue(session.reconcile(snapshot)["ok"])
        self.assertNotIn(ACCOUNT, json.dumps(snapshot))
        self.assertNotIn(ACCOUNT, json.dumps(session.events()))
        session.close()
        self.assertEqual(self.fake.stop_count, 1)
        self.assertEqual(self.fake.order_calls, [])

    def test_none_is_never_silently_empty(self):
        session = self.session()
        for name in ("positions", "orders", "trades"):
            setattr(self.fake, name, None)
            with self.assertRaisesRegex(QmtError, "ambiguous None"):
                session.snapshot()
            setattr(self.fake, name, [])

    def test_connection_subscription_errors(self):
        for field in ("connect_result", "subscribe_result"):
            setattr(self.fake, field, -1)
            with self.assertRaises(QmtError):
                self.session()
            setattr(self.fake, field, 0)
        self.assertEqual(self.fake.order_calls, [])

    def test_query_timeout_disconnects_and_cannot_reuse(self):
        session = self.session()
        release, finished = threading.Event(), threading.Event()
        query = self.fake.query_stock_asset
        def blocked_query(account):
            try:
                release.wait(10)
                return query(account)
            finally:
                finished.set()
        try:
            with patch.object(self.fake, "query_stock_asset", side_effect=blocked_query):
                with patch.object(session, "_timeout", return_value=0.1):
                    with self.assertRaisesRegex(QmtError, "timed out"):
                        session.snapshot()
        finally:
            release.set()
        self.assertTrue(finished.wait(10))
        with self.assertRaises(QmtError):
            session.connect()
        self.assertFalse(session.connected)

    def test_account_mismatch_never_accepted(self):
        session = self.session()
        self.fake.asset.account_id = "different"
        with self.assertRaisesRegex(QmtError, "account mismatch"):
            session.snapshot()

    def test_callbacks_deduplicate_and_do_not_regress_fill_count(self):
        session = self.session()
        callback = self.fake.callback
        callback.on_stock_order(broker_order(status=55, filled=50))
        callback.on_stock_order(broker_order(status=50, filled=0))
        callback.on_stock_order(broker_order(status=55, filled=50))
        callback.on_stock_trade(broker_trade(quantity=50))
        callback.on_stock_trade(broker_trade(quantity=50))
        self.assertEqual(len(session.store.facts("trade", NOW.date().isoformat())), 1)
        self.assertEqual(session.store.facts("order", NOW.date().isoformat())[0]["filled_quantity"], 50)
        self.assertEqual(len([e for e in session.events() if e["kind"] == "trade"]), 1)

    def test_conflicting_duplicate_trade_blocks_session(self):
        session = self.session()
        self.fake.callback.on_stock_trade(broker_trade(quantity=50))
        self.fake.callback.on_stock_trade(broker_trade(quantity=60))
        self.assertFalse(session.connected)
        with self.assertRaises(QmtError):
            session.snapshot()

    def test_disconnect_callback_blocks_queries(self):
        session = self.session()
        self.fake.callback.on_disconnected()
        with self.assertRaises(QmtError):
            session.snapshot()

    def test_reconciliation_detects_missing_trade_and_balance(self):
        session = self.session()
        self.fake.orders = [broker_order(status=56, filled=100)]
        report = session.reconcile()
        self.assertIn("ORDER_TRADE_QUANTITY", report["errors"])
        self.fake.trades = [broker_trade()]
        self.assertTrue(session.reconcile()["ok"])
        self.fake.asset.total_asset += 10
        self.assertIn("ACCOUNT_BALANCE_IDENTITY", session.reconcile()["errors"])

    def test_callbacks_not_present_in_queries_fail_reconciliation(self):
        session = self.session()
        self.fake.callback.on_stock_order(broker_order())
        self.assertIn("BROKER_QUERY_BEHIND_CALLBACK", session.reconcile()["errors"])

    def test_read_only_blocks_submit_cancel_and_daily_execute(self):
        session = self.session()
        for action in (lambda: session.submit(self.intent()), lambda: session.cancel("local-1"),
                       lambda: session.execute(self.intent(), self.market({"600001.SH"}).day)):
            with self.assertRaises(PermissionError):
                action()
        self.assertEqual(self.fake.order_calls, [])
        self.assertEqual(self.fake.cancel_calls, [])

    def test_live_requires_authorization_file_even_with_all_flags(self):
        self.allow_live()
        Path(self.cfg["qmt"]["authorization_file"]).unlink()
        session = self.session()
        with self.assertRaises(PermissionError):
            session.submit(self.intent())
        self.assertFalse(self.fake.order_calls)

    def test_live_fake_submission_is_persistent_and_idempotent(self):
        self.allow_live()
        session = self.session()
        first = session.submit(self.intent())
        second = session.submit(self.intent())
        self.assertEqual(first["broker_id"], second["broker_id"])
        self.assertEqual(len(self.fake.order_calls), 1)
        self.assertEqual(len(first["remark"]), 24)
        session.close()
        restarted = self.session()
        restarted.submit(self.intent())
        self.assertEqual(len(self.fake.order_calls), 1)
        with self.assertRaises(ValueError):
            restarted.submit(self.intent(quantity=200))

    def test_submit_timeout_reconciles_without_resending(self):
        self.allow_live()
        session = self.session()
        release, finished = threading.Event(), threading.Event()
        call, send = session._call, self.fake.order_stock
        def delayed_reply(*args):
            try:
                result = send(*args)
                release.wait(10)
                return result
            finally:
                finished.set()
        def short_submit_timeout(method, *args, **kwargs):
            if method == "order_stock":
                with patch.object(session, "_timeout", return_value=0.1):
                    return call(method, *args, **kwargs)
            return call(method, *args, **kwargs)
        try:
            with patch.object(self.fake, "order_stock", side_effect=delayed_reply):
                with patch.object(session, "_call", side_effect=short_submit_timeout):
                    with self.assertRaisesRegex(QmtError, "unknown"):
                        session.submit(self.intent())
        finally:
            release.set()
        self.assertTrue(finished.wait(10))
        self.assertEqual(session.store.intents()[0]["state"], "UNKNOWN")
        session.close()
        restarted = self.session()
        result = restarted.submit(self.intent())
        self.assertEqual(result["state"], "SUBMITTED")
        self.assertEqual(len(self.fake.order_calls), 1)

    def test_unknown_absent_order_blocks_new_submission(self):
        self.allow_live()
        session = self.session()
        session.store.reserve(self.intent().order_id, self.intent().__dict__, NOW.date().isoformat())
        with self.assertRaises(PermissionError):
            session.submit(self.intent("local-2"))
        self.assertFalse(self.fake.order_calls)

    def test_live_rejects_stale_ticks_daily_input_market_hours_and_kill(self):
        self.allow_live()
        session = self.session()
        good = self.market
        session.market_provider = lambda symbols: replace(good(symbols), quote_times={s: NOW - timedelta(seconds=60) for s in symbols})
        with self.assertRaises(PermissionError):
            session.submit(self.intent())
        session.market_provider = lambda symbols: good(symbols).day
        with self.assertRaises(PermissionError):
            session.submit(self.intent())
        session.market_provider = good
        self.clock = NOW.replace(hour=12)
        with self.assertRaises(PermissionError):
            session.submit(self.intent())
        self.clock = NOW
        Path(self.cfg["risk"]["kill_switch_file"]).touch()
        with self.assertRaises(PermissionError):
            session.submit(self.intent())
        self.assertFalse(self.fake.order_calls)

    def test_live_runs_independent_risk_and_unknown_status_blocks(self):
        self.allow_live()
        session = self.session()
        with self.assertRaisesRegex(PermissionError, "independent risk"):
            session.submit(self.intent(quantity=101))
        self.fake.callback.on_account_status(NS(account_id=ACCOUNT, status=6))
        with self.assertRaises(PermissionError):
            session.submit(self.intent())
        self.assertFalse(self.fake.order_calls)

    def test_unfinished_external_order_blocks_new_live_order(self):
        self.allow_live()
        session = self.session()
        self.fake.orders = [broker_order()]
        with self.assertRaisesRegex(PermissionError, "outstanding"):
            session.submit(self.intent())
        self.assertFalse(self.fake.order_calls)

    def test_cancel_request_is_not_final_and_not_duplicated(self):
        self.allow_live()
        session = self.session()
        session.submit(self.intent())
        result = session.cancel("local-1")
        self.assertEqual(result["state"], "CANCEL_PENDING")
        session.cancel("local-1")
        self.assertEqual(self.fake.cancel_calls, [1])
        self.fake.orders[0].order_status = 54
        self.assertTrue(session.reconcile()["ok"])
        self.assertEqual(session.store.intents()[0]["state"], "CANCELLED")

    def test_permission_scope_account_expiry_and_notional(self):
        self.allow_live()
        session = self.session()
        path = Path(self.cfg["qmt"]["authorization_file"])
        original = json.loads(path.read_text())
        for key, value in (("account_sha256", "wrong"), ("scopes", ["cancel"]),
                           ("symbols", []), ("expires_at", (NOW - timedelta(seconds=1)).isoformat()),
                           ("max_order_notional", 10), ("max_daily_notional", 10)):
            raw = {**original, key: value}
            path.write_text(json.dumps(raw))
            with self.assertRaises(PermissionError):
                session.submit(self.intent())
        self.assertFalse(self.fake.order_calls)

    def test_provider_io_advancing_clock_accepts_current_ticks(self):
        self.allow_live()
        session = self.session()
        def delayed_market(symbols):
            self.clock += timedelta(seconds=1)
            return self.market(symbols)
        session.market_provider = delayed_market
        self.assertEqual(session.submit(self.intent())["state"], "SUBMITTED")
        self.assertEqual(len(self.fake.order_calls), 1)

    def test_paper_derived_price_limits_cannot_authorize_live_order(self):
        self.allow_live()
        session = self.session()
        def derived(symbols):
            live = self.market(symbols)
            return replace(live, day=replace(live.day, source="baostock:derived-paper-only"))
        session.market_provider = derived
        with self.assertRaisesRegex(PermissionError, "provenance"):
            session.submit(self.intent())
        self.assertFalse(self.fake.order_calls)

    def test_terminal_callback_cannot_regress_to_stale_query(self):
        self.allow_live()
        session = self.session()
        result = session.submit(self.intent())
        self.fake.callback.on_stock_order(broker_order(status=54, remark=result["remark"]))
        self.assertEqual(session.store.intents()[0]["state"], "CANCELLED")
        self.assertIn("BROKER_QUERY_BEHIND_CALLBACK", session.reconcile()["errors"])
        self.assertEqual(session.store.intents()[0]["state"], "CANCELLED")

    def test_slow_market_collection_expires_account_snapshot(self):
        self.allow_live()
        session = self.session()
        def delayed_market(symbols):
            self.clock += timedelta(seconds=20)
            return self.market(symbols)
        session.market_provider = delayed_market
        with self.assertRaisesRegex(PermissionError, "expired"):
            session.submit(self.intent())
        self.assertFalse(self.fake.order_calls)

    def test_callback_during_risk_blocks_submission(self):
        from autotrade.risk import check_order
        self.allow_live()
        session = self.session()
        def risk_with_callback(*args, **kwargs):
            decision = check_order(*args, **kwargs)
            self.fake.callback.on_account_status(NS(account_id=ACCOUNT, status=0))
            return decision
        with patch("autotrade.risk.check_order", side_effect=risk_with_callback):
            with self.assertRaisesRegex(PermissionError, "changed"):
                session.submit(self.intent())
        self.assertFalse(self.fake.order_calls)

    def test_authorization_expiry_after_reservation_cannot_submit(self):
        self.allow_live()
        session = self.session()
        reserve = session.store.reserve
        def delayed_reserve(*args, **kwargs):
            result = reserve(*args, **kwargs)
            self.clock += timedelta(minutes=11)
            return result
        with patch.object(session.store, "reserve", side_effect=delayed_reserve):
            with self.assertRaises(PermissionError):
                session.submit(self.intent())
        self.assertFalse(self.fake.order_calls)
        self.assertEqual(session.store.intents()[0]["state"], "REJECTED")

    def test_authorization_expiry_after_cancel_reservation_cannot_cancel(self):
        self.allow_live()
        session = self.session()
        session.submit(self.intent())
        reserve = session.store.reserve_cancel
        def delayed_reserve(*args, **kwargs):
            result = reserve(*args, **kwargs)
            self.clock += timedelta(minutes=11)
            return result
        with patch.object(session.store, "reserve_cancel", side_effect=delayed_reserve):
            with self.assertRaises(PermissionError):
                session.cancel("local-1")
        self.assertFalse(self.fake.cancel_calls)
        self.assertFalse(session.store.intents()[0]["cancel_requested"])

    def test_disconnect_during_connect_is_not_reported_as_connected(self):
        connect = self.fake.connect
        def interrupted_connect():
            self.fake.callback.on_disconnected()
            return connect()
        self.fake.connect = interrupted_connect
        with self.assertRaisesRegex(QmtError, "lost during initialization"):
            self.session()

    def test_reconciliation_requires_exact_local_limit_price(self):
        self.allow_live()
        session = self.session()
        session.submit(self.intent())
        self.fake.orders[0].price = 10.50
        self.assertIn("BROKER_INTENT_MISMATCH", session.reconcile()["errors"])
        self.assertEqual(len(self.fake.order_calls), 1)

    def test_reconciliation_requires_counter_limit_type_not_submission_enum(self):
        self.allow_live()
        session = self.session()
        session.submit(self.intent())
        self.assertEqual(self.fake.submitted_price_types, [11])
        self.assertEqual(self.fake.orders[0].price_type, 50)
        # A counter value of 11 is not BROKER_PRICE_LIMIT, despite matching the input enum.
        self.fake.orders[0].price_type = 11
        self.assertIn("BROKER_INTENT_MISMATCH", session.reconcile()["errors"])

    def test_recorded_broker_order_price_is_immutable(self):
        self.allow_live()
        session = self.session()
        session.submit(self.intent())
        self.assertTrue(session.reconcile()["ok"])
        self.fake.orders[0].price = 10.50
        with self.assertRaisesRegex(ValueError, "identity changed"):
            session.reconcile()

    def test_inconsistent_order_callback_blocks_session(self):
        self.allow_live()
        session = self.session()
        session.submit(self.intent())
        wrong = copy.deepcopy(self.fake.orders[0])
        wrong.price_type = 49
        self.fake.callback.on_stock_order(wrong)
        self.assertFalse(session.connected)
        self.assertIsNone(session.connection_status()["account_status"])
        with self.assertRaises(QmtError):
            session.reconcile()

    def test_missing_counter_price_type_fails_closed(self):
        session = self.session()
        self.fake.orders = [broker_order()]
        del self.fake.orders[0].price_type
        with self.assertRaisesRegex(QmtError, "price_type"):
            session.snapshot()

    def sell_position(self):
        self.fake.positions = [NS(account_id=ACCOUNT, stock_code="600001.SH", volume=200,
                                  can_use_volume=200, frozen_volume=0, on_road_volume=0, market_value=2000)]
        self.fake.asset.market_value = 2000
        self.fake.asset.total_asset = self.fake.asset.cash + 2000

    def authorization_caps(self, single, daily):
        path = Path(self.cfg["qmt"]["authorization_file"])
        raw = json.loads(path.read_text())
        raw.update(max_order_notional=single, max_daily_notional=daily)
        path.write_text(json.dumps(raw))

    def test_sell_authorization_uses_daily_upper_price(self):
        self.allow_live()
        self.sell_position()
        self.authorization_caps(950, 950)
        session = self.session()
        with self.assertRaisesRegex(PermissionError, "authorization"):
            session.submit(replace(self.intent(), side="SELL", limit_price=D(9)))
        self.assertFalse(self.fake.order_calls)
        self.assertEqual(session.store.intents(), [])

    def test_sell_budget_survives_cancel_restart_and_repeated_id(self):
        self.allow_live()
        self.sell_position()
        self.authorization_caps(2000, 2000)
        session = self.session()
        order = replace(self.intent(), side="SELL", limit_price=D(9))
        result = session.submit(order)
        self.assertEqual(D(result["authorized_notional"]), D(1100))
        self.fake.orders[0].order_status = 54
        self.assertTrue(session.reconcile()["ok"])
        session.close()
        restarted = self.session()
        restarted.submit(order)
        with self.assertRaisesRegex(PermissionError, "authorization"):
            restarted.submit(replace(order, order_id="local-2"))
        self.assertEqual(len(self.fake.order_calls), 1)
        self.assertEqual(restarted.store.reserved_notional(order.trade_date), D(1100))

    def test_reservation_atomically_checks_daily_budget(self):
        self.allow_live()
        session = self.session()
        store = session.store
        store.reserve("first", self.intent("first").__dict__, NOW.date().isoformat(), 1100, 2000)
        from autotrade.qmt_store import QmtStore
        competing = QmtStore(session.store_path, hashlib.sha256(ACCOUNT.encode()).hexdigest())
        with self.assertRaisesRegex(PermissionError, "budget"):
            competing.reserve("second", self.intent("second").__dict__, NOW.date().isoformat(), 1100, 2000)
        self.assertEqual(len(store.intents()), 1)

    def test_authorization_cap_rechecked_after_reservation(self):
        self.allow_live()
        self.sell_position()
        session = self.session()
        reserve = session.store.reserve
        def revoke_budget(*args, **kwargs):
            result = reserve(*args, **kwargs)
            self.authorization_caps(950, 950)
            return result
        with patch.object(session.store, "reserve", side_effect=revoke_budget):
            with self.assertRaisesRegex(PermissionError, "authorization"):
                session.submit(replace(self.intent(), side="SELL", limit_price=D(9)))
        self.assertFalse(self.fake.order_calls)
        self.assertEqual(session.store.intents()[0]["state"], "REJECTED")

    def test_reconnect_requires_new_account_status_and_ignores_old_callbacks(self):
        self.allow_live()
        session = self.session()
        old_callback = self.fake.callback
        session.close()
        self.assertIsNone(session.connection_status()["account_status"])
        self.fake.status_on_subscribe = None
        session.connect()
        old_callback.on_account_status(NS(account_id=ACCOUNT, status=0))
        old_callback.on_disconnected()
        old_callback.on_stock_order(broker_order())
        self.assertTrue(session.connected)
        self.assertIsNone(session.connection_status()["account_status"])
        self.assertEqual(session.store.facts("order", NOW.date().isoformat()), [])
        with self.assertRaisesRegex(PermissionError, "normal trading state"):
            session.submit(self.intent())
        self.assertFalse(self.fake.order_calls)
        self.fake.callback.on_account_status(NS(account_id=ACCOUNT, status=0))
        self.assertEqual(session.submit(self.intent())["state"], "SUBMITTED")

    def test_disconnect_clears_account_status_and_ignores_late_normal(self):
        session = self.session()
        callback = self.fake.callback
        callback.on_disconnected()
        callback.on_account_status(NS(account_id=ACCOUNT, status=0))
        self.assertIsNone(session.connection_status()["account_status"])
        self.assertFalse(session.connected)

    def test_snapshot_detects_fill_between_balances_and_first_order_query(self):
        self.allow_live()
        session = self.session()
        query = self.fake.query_stock_orders
        def filled_before_orders(account):
            if not self.fake.orders:
                self.fake.orders = [broker_order(status=56, filled=100)]
                self.fake.trades = [broker_trade()]
                self.sell_position()
                self.fake.asset.cash -= 1000
                self.fake.asset.total_asset -= 1000
            return query(account)
        self.fake.query_stock_orders = filled_before_orders
        with self.assertRaisesRegex(QmtError, "changed during snapshot"):
            session.submit(self.intent())
        self.assertFalse(self.fake.order_calls)

    def test_callback_immediately_after_snapshot_is_not_folded_into_its_revision(self):
        self.allow_live()
        session = self.session()
        snapshot = session.snapshot
        def raced_snapshot():
            result = snapshot()
            self.fake.callback.on_account_status(NS(account_id=ACCOUNT, status=0))
            return result
        with patch.object(session, "snapshot", side_effect=raced_snapshot):
            with self.assertRaisesRegex(PermissionError, "changed"):
                session.submit(self.intent())
        self.assertFalse(self.fake.order_calls)

    def test_emergency_stop_blocks_submit_but_allows_authorized_cancel(self):
        self.allow_live()
        session = self.session()
        session.submit(self.intent())
        Path(self.cfg["risk"]["kill_switch_file"]).touch()
        with self.assertRaisesRegex(PermissionError, "emergency stop"):
            session.submit(self.intent("local-2"))
        self.assertEqual(session.cancel("local-1")["state"], "CANCEL_PENDING")
        self.assertEqual(self.fake.cancel_calls, [1])

    def test_sell_daily_upper_bound_enforces_independent_risk_caps(self):
        self.allow_live()
        self.sell_position()
        self.cfg["risk"]["max_order_notional"] = 1050
        session = self.session()
        with self.assertRaisesRegex(PermissionError, "max_order_notional"):
            session.submit(replace(self.intent(), side="SELL", limit_price=D(9)))
        self.assertFalse(self.fake.order_calls)

    def test_order_remark_cannot_disappear_after_correlation(self):
        self.allow_live()
        session = self.session()
        session.submit(self.intent())
        self.fake.orders[0].order_status = 54
        self.assertTrue(session.reconcile()["ok"])
        self.fake.orders[0].order_remark = "manual"
        with self.assertRaisesRegex(ValueError, "identity changed"):
            session.reconcile()

    def test_current_session_terminal_order_cannot_disappear_from_queries(self):
        self.allow_live()
        session = self.session()
        session.submit(self.intent())
        self.fake.orders[0].order_status = 54
        self.fake.callback.on_stock_order(self.fake.orders[0])
        # A restart retains correlation but the broker lost its query record.
        session.close()
        self.fake.orders = []
        restarted = self.session()
        self.assertFalse(restarted.reconcile()["ok"])

    def test_broker_id_cannot_change_between_callback_and_return(self):
        self.allow_live()
        session = self.session()
        send = self.fake.order_stock
        def callback_before_return(*args):
            broker_id = send(*args)
            self.fake.callback.on_stock_order(self.fake.orders[0])
            return broker_id + 1
        self.fake.order_stock = callback_before_return
        with self.assertRaisesRegex((ValueError, QmtError), "broker ID"):
            session.submit(self.intent())
        self.assertEqual(session.store.intents()[0]["broker_id"], 1)
        self.assertEqual(len(self.fake.order_calls), 1)

    def test_limit_trade_cannot_execute_outside_its_order_price(self):
        session = self.session()
        self.fake.orders = [broker_order(status=56, filled=100)]
        trade = broker_trade()
        trade.traded_price = 10.5
        trade.traded_amount = 1050
        self.fake.trades = [trade]
        self.assertIn("TRADE_OUTSIDE_ORDER_LIMIT", session.reconcile()["errors"])

    def test_external_config_mutation_cannot_enable_live_session(self):
        session = self.session()
        self.allow_live()
        with self.assertRaisesRegex(PermissionError, "disabled"):
            session.submit(self.intent())
        self.assertFalse(self.fake.order_calls)

    def test_risk_configuration_cannot_change_within_connected_session(self):
        self.allow_live()
        session = self.session()
        session.config["risk"]["max_order_notional"] *= 2
        with self.assertRaisesRegex(PermissionError, "configuration changed"):
            session.submit(self.intent())
        self.assertFalse(self.fake.order_calls)

    def test_available_and_frozen_shares_cannot_overlap(self):
        self.sell_position()
        self.fake.positions[0].frozen_volume = 1
        session = self.session()
        with self.assertRaisesRegex(QmtError, "position availability"):
            session.snapshot()

    def test_concurrent_sessions_cannot_duplicate_reserved_submission(self):
        self.allow_live()
        first, second = self.session(), self.session()
        sending, release = threading.Event(), threading.Event()
        send = self.fake.order_stock
        def delayed_send(*args):
            sending.set()
            if not release.wait(5):
                raise RuntimeError("test release timed out")
            return send(*args)
        self.fake.order_stock = delayed_send
        with ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(first.submit, self.intent())
            try:
                self.assertTrue(sending.wait(5))
                with self.assertRaisesRegex(RuntimeError, "another autotrade cycle"):
                    second.submit(self.intent())
            finally:
                release.set()
            self.assertEqual(pending.result(timeout=5)["state"], "SUBMITTED")
        second.submit(self.intent())
        self.assertEqual(len(self.fake.order_calls), 1)

    def test_partial_fill_callback_preceding_sync_return_remains_partial(self):
        self.allow_live()
        session = self.session()
        send = self.fake.order_stock
        def callback_before_return(*args):
            broker_id = send(*args)
            self.fake.orders[0].order_status = CONSTANTS.ORDER_PART_SUCC
            self.fake.orders[0].traded_volume = 50
            self.fake.callback.on_stock_order(self.fake.orders[0])
            return broker_id
        self.fake.order_stock = callback_before_return
        self.assertEqual(session.submit(self.intent())["state"], "PARTIALLY_FILLED")

    def test_disconnect_between_main_guard_and_submit_dispatch_blocks_write(self):
        self.allow_live()
        session = self.session()
        call = session._call
        def disconnect_before_dispatch(method, *args, **kwargs):
            if method == "order_stock":
                self.fake.callback.on_disconnected()
            return call(method, *args, **kwargs)
        with patch.object(session, "_call", side_effect=disconnect_before_dispatch):
            with self.assertRaises((QmtError, PermissionError)):
                session.submit(self.intent())
        self.assertFalse(self.fake.order_calls)
        self.assertEqual(session.store.intents()[0]["state"], "REJECTED")

    def test_disconnect_between_main_guard_and_cancel_dispatch_blocks_write(self):
        self.allow_live()
        session = self.session()
        session.submit(self.intent())
        call = session._call
        def disconnect_before_dispatch(method, *args, **kwargs):
            if method == "cancel_order_stock":
                self.fake.callback.on_disconnected()
            return call(method, *args, **kwargs)
        with patch.object(session, "_call", side_effect=disconnect_before_dispatch):
            with self.assertRaises((QmtError, PermissionError)):
                session.cancel("local-1")
        self.assertFalse(self.fake.cancel_calls)
        self.assertFalse(session.store.intents()[0]["cancel_requested"])

    def test_worker_dispatch_rechecks_authorization_and_stop(self):
        self.allow_live()
        for reason in ("expired", "stale", "stop", "revision"):
            with self.subTest(reason=reason):
                session = self.session()
                call = session._call
                def revoke_before_dispatch(method, *args, **kwargs):
                    if method == "order_stock":
                        if reason == "expired":
                            self.clock += timedelta(minutes=11)
                        elif reason == "stale":
                            self.clock += timedelta(seconds=6)
                        elif reason == "stop":
                            Path(self.cfg["risk"]["kill_switch_file"]).touch()
                        else:
                            self.fake.callback.on_account_status(NS(account_id=ACCOUNT, status=0))
                    return call(method, *args, **kwargs)
                with patch.object(session, "_call", side_effect=revoke_before_dispatch):
                    with self.assertRaises((QmtError, PermissionError)):
                        session.submit(self.intent(reason))
                self.assertFalse(self.fake.order_calls)
                self.clock = NOW
                Path(self.cfg["risk"]["kill_switch_file"]).unlink(missing_ok=True)
                session.close()

    def test_timed_out_worker_cannot_dispatch_when_it_eventually_runs(self):
        self.allow_live()
        session = self.session()
        call, thread_type = session._call, threading.Thread
        release, finished = threading.Event(), threading.Event()
        def delayed_thread(*args, **kwargs):
            target = kwargs["target"]
            def delayed():
                release.wait(5)
                try:
                    target()
                finally:
                    finished.set()
            return thread_type(*args, **{**kwargs, "target": delayed})
        def delayed_dispatch(method, *args, **kwargs):
            if method == "order_stock":
                with patch.object(session, "_timeout", return_value=0.2):
                    with patch("autotrade.qmt.threading.Thread", side_effect=delayed_thread):
                        return call(method, *args, **kwargs)
            return call(method, *args, **kwargs)
        try:
            with patch.object(session, "_call", side_effect=delayed_dispatch):
                with self.assertRaisesRegex(QmtError, "unknown"):
                    session.submit(self.intent())
            self.assertFalse(session.connected)
        finally:
            release.set()
        self.assertTrue(finished.wait(5))
        self.assertFalse(self.fake.order_calls)
        self.assertEqual(session.store.intents()[0]["state"], "UNKNOWN")

    def test_invalid_timeout_is_rejected_before_any_sdk_call(self):
        session = self.session()
        session.settings["query_timeout_seconds"] = -1
        with self.assertRaisesRegex(QmtError, "timeout"):
            session._call("cancel_order_stock", self.fake, 1, pre_dispatch=lambda: None)
        self.assertFalse(self.fake.cancel_calls)


if __name__ == "__main__":
    unittest.main()
