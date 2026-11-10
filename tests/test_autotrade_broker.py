"""Offline execution, failure recovery, accounting and real-order boundary tests."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace
from decimal import Decimal
import sqlite3
import tempfile
from pathlib import Path
import unittest
from unittest.mock import patch

from autotrade.broker import PaperBroker, QmtAdapter
from autotrade.models import Bar, D, MarketDay, OrderIntent


def config():
    return {
        "broker": {"initial_cash": "100000", "commission_rate": "0.0003",
                   "min_commission": "5", "stamp_tax_rate": "0.0005",
                   "transfer_fee_rate": "0.00001", "slippage_bps": 0,
                   "max_volume_fraction": "0.1"},
        "risk": {"max_symbol_weight": "0.9", "max_total_weight": "0.95",
                 "max_order_weight": "0.9", "max_sector_weight": "0.9",
                 "max_drawdown": "0.2", "max_daily_orders": 100,
                 "max_order_notional": "90000", "kill_switch_file": ""},
        "boards": {"MAIN": {"min_buy": 100, "buy_step": 100,
                            "max_order": 1000000, "price_tick": "0.01"},
                   "STAR": {"min_buy": 200, "buy_step": 1,
                            "max_order": 100000, "price_tick": "0.01"}},
        "qmt": {"enabled": False, "manual_authorization": False},
    }


def market(trade_date="2026-11-10", previous="2026-11-09", following="2026-11-11",
           volume=100000, price="10", symbol="600000.SH", board="MAIN"):
    p = D(price)
    bar = Bar(symbol, trade_date, p, p, p, p, p, volume, D("1"), board,
              False, False, D("11"), D("9"), "2000-01-01", "bank")
    return MarketDay(trade_date, previous, following, {symbol: bar}, "synthetic")


def intent(key="buy1", side="BUY", quantity=1000, trade_date="2026-11-10",
           signal_date="2026-11-09", symbol="600000.SH", price="10"):
    return OrderIntent(key, "plan-" + signal_date, symbol, side, quantity,
                       trade_date, signal_date, D(price))


class PaperBrokerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "paper.sqlite"
        self.cfg = config()
        self.broker = PaperBroker(self.path, self.cfg)

    def tearDown(self):
        self.tmp.cleanup()

    def test_buy_sell_t1_and_exact_fees(self):
        result = self.broker.execute(intent(), market())
        self.assertEqual(result["status"], "FILLED")
        self.assertEqual(D(result["fees"]), D("5.10"))
        account = self.broker.account({"600000.SH": D(10)}, "2026-11-10")
        self.assertEqual(account.cash, D("89994.90"))
        self.assertEqual(account.positions["600000.SH"].available, 0)
        denied = self.broker.execute(intent("early", "SELL"), market())
        self.assertEqual(denied["status"], "REJECTED")
        sold = self.broker.execute(intent("sell", "SELL", trade_date="2026-11-11", signal_date="2026-11-10"),
                                   market("2026-11-11", "2026-11-10", "2026-11-12"))
        self.assertEqual(sold["status"], "FILLED")
        self.assertEqual(D(sold["fees"]), D("10.10"))
        self.assertEqual(self.broker.account({}, "2026-11-11").cash, D("99984.80"))
        self.assertTrue(self.broker.reconcile()["ok"])

    def test_idempotency_survives_restart_and_concurrent_writers(self):
        order = intent()
        day = market()
        def submit(_):
            return PaperBroker(self.path, self.cfg).execute(order, day)
        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(submit, range(8)))
        self.assertTrue(all(result == results[0] for result in results))
        self.assertEqual(len(self.broker.orders()), 1)
        self.assertEqual(self.broker.reconcile()["fills"], 1)
        self.assertEqual(self.broker.account({"600000.SH": D(10)}, day.date).positions["600000.SH"].quantity, 1000)

    def test_id_reuse_with_changed_intent_fails(self):
        self.broker.execute(intent(), market())
        with self.assertRaisesRegex(ValueError, "conflicts"):
            self.broker.execute(intent(quantity=2000), market())
        self.assertEqual(self.broker.reconcile()["fills"], 1)

    def test_partial_fill_is_terminal_and_capacity_is_shared(self):
        day = market(volume=2500)
        first = self.broker.execute(intent(), day)
        self.assertEqual(first["status"], "PARTIALLY_FILLED_CANCELLED")
        self.assertEqual(first["filled_quantity"], 200)
        again = self.broker.execute(intent(), market(volume=100000))
        self.assertEqual(first, again)
        second = self.broker.execute(intent("second", quantity=100), day)
        self.assertEqual(second["status"], "CANCELLED")
        self.assertTrue(self.broker.reconcile()["ok"])

    def test_star_201_shares_and_fifo_lots(self):
        day = market(symbol="688001.SH", board="STAR")
        self.assertEqual(self.broker.execute(intent(symbol="688001.SH", quantity=201), day)["status"], "FILLED")
        next_day = market("2026-11-11", "2026-11-10", "2026-11-12", symbol="688001.SH", board="STAR")
        self.broker.execute(intent("b2", quantity=200, trade_date=next_day.date,
                                   signal_date=next_day.previous_session, symbol="688001.SH"), next_day)
        sold = self.broker.execute(intent("s2", "SELL", 201, next_day.date,
                                          next_day.previous_session, "688001.SH"), next_day)
        self.assertEqual(sold["status"], "FILLED")
        position = self.broker.account({"688001.SH": D(10)}, next_day.date).positions["688001.SH"]
        self.assertEqual((position.quantity, position.available), (200, 0))
        self.assertTrue(self.broker.reconcile()["ok"])

    def test_risk_failure_is_recorded_and_never_bypassed(self):
        with patch("autotrade.risk.check_order", side_effect=RuntimeError("private details")):
            result = self.broker.execute(intent(), market())
        self.assertEqual(result["status"], "REJECTED")
        self.assertEqual(result["reason"], "RISK_ERROR: RuntimeError")
        self.assertEqual(self.broker.execute(intent(), market()), result)
        self.assertEqual(self.broker.reconcile()["fills"], 0)

    def test_atomic_rollback_when_ledger_write_fails(self):
        with closing(sqlite3.connect(self.path)) as con, con:
            con.execute("CREATE TRIGGER fail_lot BEFORE INSERT ON paper_lots BEGIN SELECT RAISE(ABORT,'simulated crash'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            self.broker.execute(intent(), market())
        self.assertEqual(self.broker.orders(), [])
        self.assertEqual(self.broker.reconcile()["fills"], 0)
        self.assertEqual(self.broker.account({}, "2026-11-10").cash, D(100000))
        with closing(sqlite3.connect(self.path)) as con, con:
            con.execute("DROP TRIGGER fail_lot")
        self.assertEqual(self.broker.execute(intent(), market())["status"], "FILLED")

    def test_reconciliation_cash_corruption_blocks_new_trade(self):
        self.broker.execute(intent(), market())
        with closing(sqlite3.connect(self.path)) as con, con:
            con.execute("UPDATE paper_account SET cash='1000000'")
        self.assertIn("CASH_MISMATCH", self.broker.reconcile()["errors"])
        result = self.broker.execute(intent("after-corruption"), market())
        self.assertEqual(result["status"], "REJECTED")
        self.assertIn("RECONCILIATION_FAILED", result["reason"])
        self.assertEqual(self.broker.reconcile()["fills"], 1)

    def test_reconciliation_position_corruption(self):
        self.broker.execute(intent(), market())
        with closing(sqlite3.connect(self.path)) as con, con:
            con.execute("UPDATE paper_lots SET quantity=999")
        self.assertIn("POSITION_LOTS_MISMATCH", self.broker.reconcile()["errors"])

    def test_fill_history_is_immutable(self):
        self.broker.execute(intent(), market())
        with closing(sqlite3.connect(self.path)) as con, con:
            with self.assertRaises(sqlite3.IntegrityError):
                con.execute("DELETE FROM paper_fills")
            with self.assertRaises(sqlite3.IntegrityError):
                con.execute("UPDATE paper_fills SET quantity=2000")
            with self.assertRaises(sqlite3.IntegrityError):
                con.execute("INSERT OR REPLACE INTO paper_fills SELECT * FROM paper_fills")
            with self.assertRaises(sqlite3.IntegrityError):
                con.execute("INSERT OR REPLACE INTO paper_audit SELECT * FROM paper_audit")
        self.assertTrue(self.broker.reconcile()["ok"])

    def test_order_intent_and_terminal_status_corruption_are_detected(self):
        self.broker.execute(intent(), market())
        with closing(sqlite3.connect(self.path)) as con, con:
            con.execute("UPDATE paper_orders SET plan_id='different',status='SUBMITTED'")
        errors = self.broker.reconcile()["errors"]
        self.assertIn("ORDER_INTENT_MISMATCH:buy1", errors)
        self.assertIn("ORDER_STATUS_MISMATCH:buy1", errors)

    def test_fee_funded_cash_check(self):
        self.cfg["broker"]["min_commission"] = "100000"
        broker = PaperBroker(self.path, self.cfg)
        self.assertEqual(broker.execute(intent(), market())["status"], "REJECTED")
        self.assertEqual(broker.account({}, "2026-11-10").cash, D(100000))

    def test_partial_sell_must_fund_actual_minimum_fee(self):
        cfg = config()
        cfg["broker"].update(initial_cash="105.01", commission_rate=0,
                             transfer_fee_rate=0, stamp_tax_rate=0)
        for field in ("max_symbol_weight", "max_total_weight", "max_order_weight", "max_sector_weight"):
            cfg["risk"][field] = 1
        broker = PaperBroker(Path(self.tmp.name) / "penny.sqlite", cfg)
        first = market(price="0.02")
        first = replace(first, bars={"600000.SH": replace(first.bars["600000.SH"],
                        limit_up=D("0.03"), limit_down=D("0.01"))})
        self.assertEqual(broker.execute(intent(quantity=5000, price="0.02"), first)["status"], "FILLED")
        account = broker.account({"600000.SH": D("0.02")}, "2026-11-11")
        self.assertEqual(account.cash, D("0.01"))
        next_day = market("2026-11-11", "2026-11-10", "2026-11-12", volume=1000, price="0.02")
        next_day = replace(next_day, bars={"600000.SH": replace(next_day.bars["600000.SH"],
                           limit_up=D("0.03"), limit_down=D("0.01"))})
        # The 5,000-share request can cover fees; its actual 100-share fill cannot.
        result = broker.execute(intent("penny-sell", "SELL", 5000, next_day.date,
                                        next_day.previous_session, price="0.02"), next_day)
        self.assertEqual(result["status"], "REJECTED")
        self.assertEqual(result["reason"], "INSUFFICIENT_CASH_WITH_SELL_FEES")
        after = broker.account({"600000.SH": D("0.02")}, next_day.date)
        self.assertEqual(after.cash, D("0.01"))
        self.assertEqual(after.positions["600000.SH"].quantity, 5000)
        self.assertTrue(broker.reconcile()["ok"])

    def test_daily_limits_suspension_and_slippage(self):
        day = market()
        bar = day.bars["600000.SH"]
        suspended = replace(day, bars={bar.symbol: replace(bar, suspended=True)})
        self.assertEqual(self.broker.execute(intent("suspended"), suspended)["status"], "REJECTED")
        up = market(price="11")
        self.assertEqual(self.broker.execute(intent("limit-up", price="11"), up)["status"], "REJECTED")
        self.cfg["broker"]["slippage_bps"] = 10
        self.assertEqual(self.broker.execute(intent("slip"), day)["status"], "REJECTED")

    def test_missing_held_mark_stops_risk(self):
        self.broker.execute(intent(), market())
        with self.assertRaisesRegex(ValueError, "mark"):
            self.broker.account({}, "2026-11-10")
        missing = market(symbol="600001.SH")
        result = self.broker.execute(intent("missing", symbol="600001.SH"), missing)
        self.assertEqual(result["status"], "REJECTED")

    def test_peak_equity_persists(self):
        self.broker.execute(intent(), market())
        self.broker.mark_equity({"600000.SH": D(20)}, "2026-11-10")
        broker = PaperBroker(self.path, self.cfg)
        account = broker.account({"600000.SH": D(10)}, "2026-11-11")
        self.assertEqual(account.peak_equity, D("109994.90"))
        broker.mark_equity({"600000.SH": D(9)}, "2026-11-10")
        self.assertTrue(broker.reconcile()["ok"])
        with closing(sqlite3.connect(self.path)) as con, con:
            con.execute("UPDATE paper_account SET peak_equity='100000'")
        self.assertIn("PEAK_EQUITY_MISMATCH", broker.reconcile()["errors"])

    def test_initial_cash_cannot_reset_existing_account(self):
        self.cfg["broker"]["initial_cash"] = "200000"
        with self.assertRaisesRegex(ValueError, "fixed"):
            PaperBroker(self.path, self.cfg)

    def test_qmt_never_submits_real_order(self):
        with self.assertRaises(PermissionError):
            QmtAdapter(self.cfg).execute(intent(), market())
        self.cfg["qmt"].update(enabled=True, broker_permission_confirmed=True, manual_authorization=True)
        with self.assertRaises(NotImplementedError):
            QmtAdapter(self.cfg).execute(intent(), market())


if __name__ == "__main__":
    unittest.main()
