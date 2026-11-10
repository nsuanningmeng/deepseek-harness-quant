"""Independent risk tests use synthetic prices only; no network or broker calls."""
from copy import deepcopy
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from autotrade.models import Account, Bar, D, MarketDay, OrderIntent, Position
from autotrade.risk import check_order
from autotrade.rules import round_buy_quantity, validate_quantity


def config():
    return {
        "boards": {
            "MAIN": {"min_buy": 100, "buy_step": 100, "max_order": 1000000, "price_tick": "0.01"},
            "CHINEXT": {"min_buy": 100, "buy_step": 100, "max_order": 300000, "price_tick": "0.01"},
            "STAR": {"min_buy": 200, "buy_step": 1, "max_order": 100000, "price_tick": "0.01"},
            "BSE": {"min_buy": 100, "buy_step": 1, "max_order": 1000000, "price_tick": "0.01"},
        },
        "broker": {"commission_rate": "0.0003", "min_commission": "5", "stamp_tax_rate": "0.0005", "transfer_fee_rate": "0.00001"},
        "risk": {"max_symbol_weight": "0.15", "max_total_weight": "0.8", "max_order_weight": "0.15", "max_drawdown": "0.15", "max_daily_orders": 100, "max_order_notional": "100000", "kill_switch_file": "", "max_sector_weight": "0.4"},
    }


def bar(**changes):
    original = Bar("600000.SH", "2026-11-10", D(10), D("10.2"), D("9.8"),
                   D("10.1"), D(10), 100000, D("0.5"), "MAIN", False,
                   False, D(11), D(9), "2020-01-01")
    return replace(original, **changes)


class QuantityRulesTests(unittest.TestCase):
    def setUp(self):
        self.config = config()

    def test_main_and_chinext_round_lots(self):
        for board in ("MAIN", "CHINEXT"):
            for requested, expected in ((99, 0), (100, 100), (199, 100), (300, 300)):
                with self.subTest(board=board, requested=requested):
                    self.assertEqual(round_buy_quantity(requested, board, self.config), expected)
            self.assertIn("invalid_lot_quantity", validate_quantity("BUY", 101, 0, board, self.config))

    def test_star_and_bse_are_not_round_lot_multiples(self):
        self.assertEqual(round_buy_quantity(199, "STAR", self.config), 0)
        self.assertEqual(round_buy_quantity(201, "STAR", self.config), 201)
        self.assertEqual(round_buy_quantity(101, "BSE", self.config), 101)
        self.assertEqual(validate_quantity("BUY", 201, 0, "STAR", self.config), [])
        self.assertEqual(validate_quantity("BUY", 101, 0, "BSE", self.config), [])

    def test_sell_odd_balance_once_and_no_short(self):
        self.assertEqual(validate_quantity("SELL", 150, 150, "MAIN", self.config), [])
        self.assertEqual(validate_quantity("SELL", 50, 50, "MAIN", self.config), [])
        self.assertIn("invalid_lot_quantity", validate_quantity("SELL", 50, 150, "MAIN", self.config))
        self.assertIn("insufficient_sellable_shares_t1", validate_quantity("SELL", 200, 100, "MAIN", self.config))

    def test_invalid_types_and_unknown_board_fail_closed(self):
        for quantity in (True, 1.5, "100", 0, -1):
            self.assertTrue(validate_quantity("BUY", quantity, 0, "MAIN", self.config))
        self.assertTrue(validate_quantity("BUY", 100, 0, "UNKNOWN", self.config))
        self.assertTrue(validate_quantity("BUY", 100001, 0, "STAR", self.config))


class RiskGateTests(unittest.TestCase):
    def setUp(self):
        self.config = config()
        self.bar = bar()
        self.day = MarketDay("2026-11-10", "2026-11-09", "2026-11-11", {self.bar.symbol: self.bar}, "synthetic")
        self.account = Account(D(100000), D(100000), D(100000))
        self.order = OrderIntent("order-1", "plan-1", self.bar.symbol, "BUY", 100,
                                 self.day.date, self.day.previous_session, D(10))

    def check(self, *, order=None, day=None, account=None, config_override=None, count=0):
        return check_order(order or self.order, day or self.day, account or self.account,
                           config_override or self.config, daily_order_count=count)

    def changed_day(self, **changes):
        changed = replace(self.bar, **changes)
        return replace(self.day, bars={self.bar.symbol: changed})

    def test_valid_order_passes_without_mutating_input(self):
        before = deepcopy((self.order, self.day, self.account, self.config))
        self.assertEqual(self.check(), [])
        self.assertEqual((self.order, self.day, self.account, self.config), before)

    def test_same_day_or_stale_signal_cannot_execute(self):
        same_day = replace(self.order, signal_date=self.day.date)
        self.assertIn("signal_must_precede_execution_t1", self.check(order=same_day))
        self.assertIn("session_mismatch", self.check(order=replace(self.order, signal_date="2026-11-06")))
        self.assertIn("session_mismatch", self.check(order=replace(self.order, trade_date="2026-11-11")))

    def test_t1_frozen_shares_cannot_sell(self):
        account = replace(self.account, positions={self.bar.symbol: Position(200, 100)})
        self.assertIn("insufficient_sellable_shares_t1", self.check(
            order=replace(self.order, side="SELL", quantity=200), account=account))
        self.assertEqual(self.check(order=replace(self.order, side="SELL"), account=account), [])

    def test_suspension_zero_volume_and_missing_bounds_reject(self):
        cases = [({"suspended": True}, "suspended"), ({"volume": 0}, "no_liquidity"),
                 ({"limit_up": None}, "missing_price_limits"), ({"limit_down": None}, "missing_price_limits")]
        for changes, reason in cases:
            with self.subTest(changes=changes):
                self.assertIn(reason, self.check(day=self.changed_day(**changes)))

    def test_directional_limit_lock_and_explicit_st_price_bounds(self):
        up = self.changed_day(open=D(11), high=D(11), is_st=True)
        self.assertIn("limit_up_buy_unfilled", self.check(day=up, order=replace(self.order, limit_price=D(11))))
        account = replace(self.account, positions={self.bar.symbol: Position(100, 100)})
        sell = replace(self.order, side="SELL", limit_price=D(11))
        self.assertEqual(self.check(order=sell, day=up, account=account), [])
        down = self.changed_day(open=D(9), low=D(9))
        self.assertIn("limit_down_sell_unfilled", self.check(order=replace(sell, limit_price=D(9)), day=down, account=account))

    def test_invalid_nan_negative_and_fractional_prices_reject(self):
        from decimal import Decimal
        for price in (Decimal("NaN"), Decimal("Infinity"), D(-1), D("10.001")):
            with self.subTest(price=price):
                self.assertTrue(self.check(order=replace(self.order, limit_price=price)))
        self.assertTrue(self.check(day=self.changed_day(open=Decimal("NaN"))))
        self.assertIn("order_outside_price_limits", self.check(order=replace(self.order, limit_price=D(12))))

    def test_invalid_metadata_and_market_identity_reject(self):
        for changes in ({"board": "UNKNOWN"}, {"is_st": None}, {"suspended": "false"},
                        {"symbol": "600001.SH"}, {"date": "2026-11-09"}, {"list_date": "2027-01-01"}):
            with self.subTest(changes=changes):
                self.assertTrue(self.check(day=self.changed_day(**changes)))

    def test_any_missing_position_mark_blocks_buy_and_sell(self):
        account = replace(self.account, positions={"000001.SZ": Position(100, 100), self.bar.symbol: Position(100, 100)})
        for side in ("BUY", "SELL"):
            self.assertIn("missing_position_mark", self.check(order=replace(self.order, side=side), account=account))

    def test_insufficient_cash_includes_minimum_commission(self):
        account = Account(D(1000), D(100000), D(100000))
        self.assertIn("insufficient_cash", self.check(account=account))
        # Strategy-provided equity cannot inflate the independent account mark.
        self.assertIn("max_order_weight", self.check(account=account))

    def test_symbol_and_total_exposure_use_current_marks(self):
        account = Account(D(86000), D(100000), D(100000), {self.bar.symbol: Position(1400, 1400)})
        self.assertIn("max_symbol_weight", self.check(account=account))
        self.config["risk"].update(max_symbol_weight="1", max_sector_weight="1")
        account = Account(D(20000), D(100000), D(100000), {self.bar.symbol: Position(8000, 8000)})
        self.assertIn("max_total_weight", self.check(account=account))

    def test_unknown_sector_does_not_bypass_concentration(self):
        symbols = ("600001.SH", "600002.SH", "600003.SH", "600004.SH")
        bars = {symbol: bar(symbol=symbol) for symbol in symbols}
        bars[self.bar.symbol] = self.bar
        account = Account(D(60000), D(100000), D(100000), {symbol: Position(1000, 1000) for symbol in symbols})
        self.assertIn("max_sector_weight", self.check(account=account, day=replace(self.day, bars=bars)))

    def test_drawdown_blocks_buys_but_allows_reducing_position(self):
        account = Account(D(79000), D(80000), D(100000), {self.bar.symbol: Position(100, 100)})
        self.assertIn("max_drawdown_buy_blocked", self.check(account=account))
        self.assertEqual(self.check(order=replace(self.order, side="SELL"), account=account), [])

    def test_kill_switch_blocks_both_directions(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "STOP"
            path.touch()
            self.config["risk"]["kill_switch_file"] = str(path)
            account = replace(self.account, positions={self.bar.symbol: Position(100, 100)})
            for side in ("BUY", "SELL"):
                self.assertIn("kill_switch_active", self.check(order=replace(self.order, side=side), account=account))

    def test_daily_order_and_notional_limits(self):
        self.assertIn("max_daily_orders", self.check(count=100))
        self.config["risk"]["max_order_notional"] = "999"
        self.assertIn("max_order_notional", self.check())

    def test_sell_limit_cannot_understate_order_exposure(self):
        account = replace(self.account, positions={self.bar.symbol: Position(100, 100)})
        self.config["risk"]["max_order_notional"] = "999"
        self.assertIn("max_order_notional", self.check(
            order=replace(self.order, side="SELL", limit_price=D(9)), account=account))

    def test_unmarketable_limit_is_not_filled(self):
        self.assertIn("buy_limit_below_open", self.check(order=replace(self.order, limit_price=D("9.99"))))
        account = replace(self.account, positions={self.bar.symbol: Position(100, 100)})
        self.assertIn("sell_limit_above_open", self.check(
            order=replace(self.order, side="SELL", limit_price=D("10.01")), account=account))


if __name__ == "__main__":
    unittest.main()
