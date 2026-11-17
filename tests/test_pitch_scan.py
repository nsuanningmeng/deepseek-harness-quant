"""Guard the scan's split boundary and chronological cash accounting."""

import unittest
from types import SimpleNamespace

import numpy as np
import pandas as pd

from backtest.pitch_scan import _drawdown_shortlist, _portfolio, _window, validate_request
from backtest.short_rule_study import EXIT_GRACE


class PitchScanTests(unittest.TestCase):
    def test_signal_window_purges_maximum_exit_delay(self):
        dates = pd.bdate_range("2024-01-01", periods=200).strftime("%Y-%m-%d")
        first, signal_end, end = _window(dates, dates[4], 125, 10, 60)
        self.assertEqual(first, 4)
        self.assertEqual((signal_end - 1) + 10 + 1 + EXIT_GRACE, end - 1)
        with self.assertRaises(ValueError):
            _window(dates, dates[110], end, 10, 60)

    def test_buy_next_open_reserves_blocked_slots_and_uses_real_exit(self):
        rows, cols = np.array([0]), np.array([0])
        opens = np.array([[1.], [2.], [8.], [7.]])
        marks = np.array([[1.], [2.], [8.], [7.]])
        replay = SimpleNamespace(op=opens, close=marks, volume=np.ones_like(opens))
        fees = {"commission_rate": 0., "slippage_rate": 0., "sell_tax_rate": 0.}
        exits = pd.DataFrame({"offset": [2.], "net": [1.]})
        # Two slots were selected; only one opened. Buy 1/4 of equity at 2,
        # then receive twice that stake at exit, irrespective of the exit day's mark.
        returns, stale, open_lots, funded = _portfolio(
            rows, cols, exits, np.array([2, 0, 0, 0]), replay, marks, fees, (0, 1, 4), 1,
        )
        np.testing.assert_allclose(returns, [0., 0., .25, 0.])
        self.assertEqual((stale, open_lots, funded.tolist()), (0, 0, [True]))
        resized, _, _, _ = _portfolio(
            rows, cols, exits, np.array([2, 0, 0, 0]), replay, marks, fees, (0, 1, 4), 1, .8,
        )
        np.testing.assert_allclose(resized, [0., 0., .20, 0.])

    def test_drawdown_preset_and_shortlist_use_training_only(self):
        grid = validate_request({"profile": "drawdown"})["grid"]
        self.assertEqual(len(grid["topn"]), 1)
        self.assertEqual(len(grid["hold"]), 1)
        with self.assertRaises(ValueError):
            validate_request({"profile": "drawdown", "grid": {**grid, "size": [1.01]}})

        def candidate(stop, size, annual, drawdown):
            return {"params": {"topn": 6, "hold": 10, "stop": stop, "target": None, "size": size},
                    "train": {"annual_return": annual, "max_drawdown": drawdown}}
        baseline = candidate(2, 1, .2, -.4)
        reference = candidate(None, 1, .3, -.45)
        invalid = candidate(1.5, 1, .35, -.41)
        size_only = candidate(None, .9, .29, -.39)
        both = candidate(1.5, .9, .28, -.36)
        smaller = candidate(1.5, .8, .27, -.32)
        exit_only = candidate(3, 1, .25, -.36)
        alternatives, counts = _drawdown_shortlist(
            [invalid, reference, size_only, both, smaller, exit_only], baseline, reference["params"], 2,
        )
        self.assertEqual([item["group"] for item in alternatives], ["size", "both", "exit"])
        self.assertEqual(counts, {"size": 1, "both": 2, "exit": 1})

    def test_validation_deduplicates_disabled_atr_and_rejects_bad_dates(self):
        request = validate_request({"grid": {"topn": [6, 6], "hold": [3], "stop": [0, None], "target": [1.5]}})
        self.assertEqual(request["grid"]["topn"], [6])
        self.assertEqual(request["grid"]["stop"], [None])
        with self.assertRaises(ValueError):
            validate_request({"train_start": "2018-01-01"})
        with self.assertRaises(ValueError):
            validate_request({"test_start": "20241101"})


if __name__ == "__main__":
    unittest.main()
