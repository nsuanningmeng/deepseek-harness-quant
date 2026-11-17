"""Verify forward portfolios keep the backtest's event and cash ordering."""

from copy import deepcopy
from datetime import date, timedelta
import unittest

from backtest.pitch_forward_engine import apply_session, held_symbols, initial_state, summary


CONFIG = {
    "initial_cash": 50000.0,
    "fees": {"commission_rate": 0.00026, "slippage_rate": 0.001, "sell_tax_rate": 0.0005},
    "exit_grace": 10,
    "strategies": {
        "challenger": {"label": "10日", "hold": 10, "size": .85, "stop": None},
        "baseline": {"label": "5日", "hold": 5, "size": 1., "stop": 2.},
    },
}
CODE = "600000.SH"
OTHER = "600001.SH"
SIGNAL = [{"code": CODE, "score": 90, "close": 10., "atr": 1.}]


def bar(price=10., low=9., close=10., *, buy=True, sell=True, intraday=True, volume=100):
    return {
        "open": price, "high": max(price, close), "low": low, "close": close, "volume": volume,
        "buyable": buy, "open_sellable": sell, "intraday_sellable": intraday,
    }


class PitchForwardEngineTests(unittest.TestCase):
    def test_sleeve_and_blocked_slots_with_initial_capital_drawdown(self):
        state = initial_state(CONFIG)
        signal = [*SIGNAL, {"code": OTHER, "score": 80, "close": 10., "atr": 1.}]
        apply_session(state, "2026-11-13", {CODE: bar(), OTHER: bar(buy=False)}, signal, CONFIG)
        challenger = state["accounts"]["challenger"]
        slot = 50000 * .85 / 11 / 2
        self.assertAlmostEqual(challenger["cash"], 50000 - slot)
        self.assertEqual(challenger["daily"][0]["skipped_entries"], [{"code": OTHER, "reason": "untradeable_open"}])
        gross = slot / (1 + .00126)
        self.assertAlmostEqual(challenger["lots"][0]["quantity"], gross / 10)
        equity = 50000 - slot + gross * (1 - .00176)
        stats = summary(state)["accounts"]["challenger"]
        self.assertAlmostEqual(stats["equity"], equity)
        self.assertAlmostEqual(stats["max_drawdown"], equity / 50000 - 1)
        self.assertAlmostEqual(stats["fees_paid"], gross * .00126)
        self.assertEqual(held_symbols(state), {CODE})

    def test_t_plus_one_and_deferred_baseline_stop(self):
        state = initial_state(CONFIG)
        apply_session(state, "2026-11-13", {CODE: bar(low=5.)}, SIGNAL, CONFIG)
        self.assertEqual(len(state["accounts"]["baseline"]["lots"]), 1)
        apply_session(state, "2026-11-16", {CODE: bar(7., 7., 7., buy=False, sell=False, intraday=False)}, [], CONFIG)
        self.assertTrue(state["accounts"]["baseline"]["lots"][0]["pending_stop"])
        apply_session(state, "2026-11-17", {CODE: bar(price=9.)}, [], CONFIG)
        self.assertEqual(state["accounts"]["baseline"]["fills"][-1]["reason"], "atr_stop")
        self.assertEqual(state["accounts"]["baseline"]["fills"][-1]["price"], 9.)
        self.assertEqual(len(state["accounts"]["challenger"]["lots"]), 1)
        self.assertIsNone(state["accounts"]["challenger"]["lots"][0]["stop_price"])

    def test_exit_sessions_count_from_entry_and_duplicate_is_idempotent(self):
        state = initial_state(CONFIG)
        opening = date(2026, 11, 13)
        for offset in range(11):
            day = (opening + timedelta(days=offset)).isoformat()
            apply_session(state, day, {CODE: bar()}, SIGNAL if offset == 0 else [], CONFIG)
            if offset == 0:
                snapshot = deepcopy(state)
                apply_session(state, day, {}, [], CONFIG)
                self.assertEqual(state, snapshot)
        for key, hold in (("baseline", 5), ("challenger", 10)):
            account = state["accounts"][key]
            self.assertEqual(len(account["lots"]), 0)
            self.assertEqual(account["fills"][-1]["reason"], "holding_limit")
            self.assertEqual(account["fills"][-1]["date"], (opening + timedelta(days=hold)).isoformat())
        self.assertEqual(state["session_count"], 11)

    def test_missing_held_quote_stops_update_but_confirmed_suspension_carries_mark(self):
        state = initial_state(CONFIG)
        apply_session(state, "2026-11-13", {CODE: bar()}, SIGNAL, CONFIG)
        snapshot = deepcopy(state)
        with self.assertRaisesRegex(ValueError, "持仓行情不完整"):
            apply_session(state, "2026-11-16", {}, [], CONFIG)
        self.assertEqual(state, snapshot)
        suspended = {"suspended": True, "volume": 0}
        apply_session(state, "2026-11-16", {CODE: suspended}, [], CONFIG)
        stats = summary(state)["accounts"]["baseline"]
        self.assertEqual(stats["stale_lots"], 1)
        self.assertEqual(stats["equity"], snapshot["accounts"]["baseline"]["equity"])


if __name__ == "__main__":
    unittest.main()
