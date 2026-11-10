import copy
import json
import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from autotrade.config import load_config, validate_config
from autotrade.demo import build_demo
from autotrade.data import load_day
from autotrade.engine import Engine
from autotrade.locking import ProcessLock
from autotrade.scheduler import is_session, tick
from autotrade.models import Account, D, Position


class EngineTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.cfg = load_config()
        self.cfg["runtime_dir"] = self.temp.name
        self.cfg["data"]["directory"] = str(Path(self.temp.name) / "market")
        self.cfg["risk"]["kill_switch_file"] = str(Path(self.temp.name) / "STOP")
        self.cfg["strategy"]["top_n"] = 3
        self.cfg["strategy"]["rebalance_sessions"] = 2
        self.dates = build_demo(self.cfg["data"]["directory"])

    def test_complete_loop_replay_and_deterministic_plan(self):
        engine = Engine(self.cfg)
        reports = [engine.cycle(day) for day in self.dates]
        orders = engine.broker.orders()
        self.assertTrue(orders)
        self.assertEqual({order["side"] for order in orders}, {"BUY", "SELL"})
        self.assertTrue(engine.broker.reconcile()["ok"])
        self.assertEqual(reports[0]["state"], "WARMUP")
        self.assertEqual(reports[19]["state"], "PLANNED")
        replay = Engine(self.cfg).cycle(self.dates[-1])
        self.assertTrue(replay["replayed"])
        self.assertEqual(replay["cash"], reports[-1]["cash"])
        self.assertEqual(engine.broker.orders(), orders)
        with engine.connect() as con:
            plans = con.execute("SELECT payload FROM auto_plans ORDER BY date").fetchall()
        # Fresh account with same history/config produces byte-identical first plan.
        other = copy.deepcopy(self.cfg)
        other["runtime_dir"] = str(Path(self.temp.name) / "second")
        second = Engine(other)
        for day in self.dates[:20]:
            second.cycle(day)
        with second.connect() as con:
            self.assertEqual(plans[0][0], con.execute("SELECT payload FROM auto_plans").fetchone()[0])

    def test_crash_after_committed_fill_reuses_persisted_intents(self):
        engine = Engine(self.cfg)
        for day in self.dates[:20]:
            engine.cycle(day)
        original = engine.broker.execute
        calls = []

        def commit_then_crash(order, day):
            result = original(order, day)
            calls.append(order.order_id)
            raise RuntimeError("injected process crash after broker commit")

        with patch.object(engine.broker, "execute", commit_then_crash):
            with self.assertRaises(RuntimeError):
                engine.cycle(self.dates[20])
        restarted = Engine(self.cfg)
        result = restarted.cycle(self.dates[20])
        ids = [order["order_id"] for order in restarted.broker.orders()]
        self.assertEqual(ids.count(calls[0]), 1)
        self.assertEqual(len(ids), len(set(ids)))
        self.assertTrue(result["reconciliation"]["ok"])

    def test_revised_sealed_data_stops_without_order(self):
        engine = Engine(self.cfg)
        engine.cycle(self.dates[19])
        path = Path(self.cfg["data"]["directory"]) / f"{self.dates[0]}.json"
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["bars"][0]["turnover"] = "99"
        path.write_text(json.dumps(payload), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "sealed market data changed"):
            engine.cycle(self.dates[20])
        self.assertFalse(engine.broker.orders())

    def test_missing_history_or_skipped_cycle_stops(self):
        engine = Engine(self.cfg)
        engine.cycle(self.dates[19])
        with self.assertRaisesRegex(ValueError, "sessions in order"):
            engine.cycle(self.dates[21])
        (Path(self.cfg["data"]["directory"]) / f"{self.dates[18]}.json").unlink()
        with self.assertRaisesRegex(ValueError, "missing or conflicting"):
            engine.cycle(self.dates[20])
        self.assertFalse(engine.broker.orders())

    def test_config_is_pinned_to_account(self):
        Engine(self.cfg)
        self.cfg["risk"]["max_symbol_weight"] = 0.5
        with self.assertRaisesRegex(ValueError, "configuration changed"):
            Engine(self.cfg)

    def test_concurrent_first_start_cannot_pin_two_configs(self):
        other = copy.deepcopy(self.cfg)
        other["strategy"]["top_n"] = 4

        def create(cfg):
            try:
                Engine(cfg)
                return True
            except ValueError:
                return False

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(create, [self.cfg, other]))
        self.assertEqual(sum(results), 1)

    def test_process_lock_and_crash_release(self):
        path = Path(self.temp.name) / "test.lock"
        with ProcessLock(path):
            with self.assertRaises(RuntimeError):
                with ProcessLock(path):
                    pass
        with ProcessLock(path):
            pass

    def test_kill_switch_rejects_all_pending_orders(self):
        engine = Engine(self.cfg)
        engine.cycle(self.dates[19])
        Path(self.cfg["risk"]["kill_switch_file"]).touch()
        engine.cycle(self.dates[20])
        orders = engine.broker.orders()
        self.assertTrue(orders)
        self.assertEqual({order["status"] for order in orders}, {"REJECTED"})
        self.assertTrue(engine.broker.reconcile()["ok"])

    def test_scheduler_uses_calendar_and_shanghai_time(self):
        self.assertFalse(is_session(self.cfg, "2026-10-01"))
        with self.assertRaisesRegex(ValueError, "calendar coverage"):
            is_session(self.cfg, "2027-01-01")
        clock = datetime(2026, 10, 1, 9, 0, tzinfo=timezone.utc)
        self.assertEqual(tick(self.cfg, clock)["state"], "MARKET_CLOSED")
        clock = datetime(2026, 9, 28, 7, 0, tzinfo=timezone.utc)
        self.assertEqual(tick(self.cfg, clock)["state"], "WAITING_FOR_CLOSE")
        self.assertFalse((Path(self.temp.name) / "paper.sqlite3").exists())

    def test_live_and_invalid_config_cannot_construct_engine(self):
        cases = [("mode", None, "live"), ("qmt", "enabled", True),
                 ("qmt", "manual_authorization", True), ("risk", "max_total_weight", "NaN"),
                 ("strategy", "lookback", True), ("broker", "initial_cash", -1)]
        for section, key, value in cases:
            cfg = copy.deepcopy(self.cfg)
            if key is None:
                cfg[section] = value
            else:
                cfg[section][key] = value
            with self.assertRaises((ValueError, ArithmeticError)):
                Engine(cfg)
        self.assertFalse((Path(self.temp.name) / "paper.sqlite3").exists())

    def test_report_recreated_after_publish_crash(self):
        engine = Engine(self.cfg)
        engine.cycle(self.dates[19])
        report = Path(self.temp.name) / "reports" / f"{self.dates[19]}.json"
        report.unlink()
        engine.cycle(self.dates[19])
        self.assertTrue(report.exists())

    def test_existing_tushare_snapshot_replay_is_offline(self):
        day = load_day(Path(self.cfg["data"]["directory"]) / f"{self.dates[19]}.json")
        self.cfg["data"].update(provider="tushare", universe=sorted(day.bars))
        with patch("autotrade.engine.TushareProvider") as provider:
            engine = Engine(self.cfg)
            engine.cycle(day.date)
            engine.cycle(day.date)
            provider.assert_not_called()

    def test_corporate_action_on_held_stock_stops(self):
        engine = Engine(self.cfg)
        engine.cycle(self.dates[19])
        engine.cycle(self.dates[20])
        path = Path(self.cfg["data"]["directory"]) / f"{self.dates[21]}.json"
        raw = json.loads(path.read_text(encoding="utf-8"))
        held = engine.broker.orders()[0]["symbol"]
        next(row for row in raw["bars"] if row["symbol"] == held)["prev_close"] = "9.99"
        path.write_text(json.dumps(raw), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "corporate action"):
            engine.cycle(self.dates[21])

    def test_star_liquidation_keeps_odd_lot_chunk_last(self):
        self.cfg["boards"]["STAR"]["max_order"] = 300
        engine = Engine(self.cfg)
        day = load_day(Path(self.cfg["data"]["directory"]) / f"{self.dates[20]}.json")
        symbol = "688001.SH"
        account = Account(D(60000), D(70000), D(70000), {symbol: Position(401, 401)})
        plan = {"plan_id": "1", "signal_date": day.previous_session, "targets": {},
                "reference_prices": {symbol: str(day.bars[symbol].prev_close)}}
        with patch.object(engine.broker, "account", return_value=account):
            intents = engine._intents(day, plan)
        self.assertEqual([order.quantity for order in intents], [300, 101])

    def test_removed_sealed_history_prefix_stops(self):
        engine = Engine(self.cfg)
        engine.cycle(self.dates[19])
        for day in self.dates[:10]:
            (Path(self.cfg["data"]["directory"]) / f"{day}.json").unlink()
        with self.assertRaisesRegex(ValueError, "sealed market history has missing"):
            engine.cycle(self.dates[20])

    def test_historical_universe_must_be_complete(self):
        path = Path(self.cfg["data"]["directory"]) / f"{self.dates[10]}.json"
        raw = json.loads(path.read_text(encoding="utf-8"))
        removed = raw["bars"].pop()["symbol"]
        raw["universe"].remove(removed)
        path.write_text(json.dumps(raw), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "historical snapshot universe"):
            Engine(self.cfg).cycle(self.dates[19])

    def test_scheduler_cached_tushare_day_survives_network_failure(self):
        self.cfg["data"]["provider"] = "tushare"
        with patch("autotrade.scheduler.TushareProvider", side_effect=RuntimeError("offline")):
            self.assertTrue(is_session(self.cfg, self.dates[19]))


if __name__ == "__main__":
    unittest.main()
