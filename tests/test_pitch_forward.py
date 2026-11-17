"""Check that paper orders are committed before the auction and settle once."""

from datetime import datetime
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from backtest import pitch_forward as forward


class PitchForwardTests(unittest.TestCase):
    def test_preauction_commit_settles_only_after_close(self):
        sessions = ["2026-11-12", "2026-11-13", "2026-11-16"]
        symbol = "600519.SH"

        def snapshot(day, symbols, **kwargs):
            return {
                "readiness": {"close_date": day},
                "signal": [{"code": symbol, "close": 100., "atr": 3.}],
                "bars": {symbol: {"open": 100., "high": 103., "low": 99., "close": 102.,
                                  "volume": 1000, "buyable": True}},
            }

        adapter = SimpleNamespace(market_calendar=lambda **kwargs: sessions, snapshot=snapshot)
        with tempfile.TemporaryDirectory() as directory:
            with patch.object(forward, "STATE", Path(directory) / "state.json"), patch.dict(
                sys.modules, {"backtest.pitch_forward_data": adapter},
            ), patch.object(forward, "_now") as now:
                now.return_value = datetime(2026, 11, 12, 20, tzinfo=forward.MARKET_TZ)
                state = forward._new_state()
                forward._advance(state)
                self.assertEqual(state["start_session"], "2026-11-13")
                self.assertEqual(state["engine"]["session_count"], 0)
                signal = state["signals"]["2026-11-13"]
                self.assertEqual(signal["signal_date"], "2026-11-12")

                now.return_value = datetime(2026, 11, 13, 12, tzinfo=forward.MARKET_TZ)
                forward._advance(state)
                self.assertEqual(state["engine"]["session_count"], 0)

                now.return_value = datetime(2026, 11, 13, 18, tzinfo=forward.MARKET_TZ)
                forward._advance(state)
                forward._advance(state)
                self.assertEqual(state["engine"]["session_count"], 1)
                self.assertEqual(state["pending"], "2026-11-16")
                for account in state["engine"]["accounts"].values():
                    self.assertEqual(len(account["fills"]), 1)
                    self.assertGreater(account["equity"], 50000)

    def test_after_auction_cannot_create_same_day_orders(self):
        def snapshot(*args, **kwargs):
            self.fail("过锁定时点后不应读取行情并生成当日信号")

        adapter = SimpleNamespace(
            market_calendar=lambda **kwargs: ["2026-11-12", "2026-11-13", "2026-11-16"],
            snapshot=snapshot,
        )
        with tempfile.TemporaryDirectory() as directory:
            with patch.object(forward, "STATE", Path(directory) / "state.json"), patch.dict(
                sys.modules, {"backtest.pitch_forward_data": adapter},
            ), patch.object(forward, "_now", return_value=datetime(
                2026, 11, 13, 9, 15, tzinfo=forward.MARKET_TZ,
            )):
                state = forward._new_state()
                forward._advance(state)
                self.assertEqual(state["status"], "waiting_data")
                self.assertEqual(state["signals"], {})
                self.assertIsNone(state["start_session"])


if __name__ == "__main__":
    unittest.main()
