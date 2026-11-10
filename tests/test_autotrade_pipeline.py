import copy
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from autotrade.config import load_config, validate_config
from autotrade.demo import build_demo
from autotrade.diagnostics import configuration_report
from autotrade.pipeline import prepare_and_run, replay_range
from autotrade.providers import FilesProvider


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.cfg = load_config()
        self.cfg["runtime_dir"] = self.tmp.name
        self.cfg["risk"]["kill_switch_file"] = str(Path(self.tmp.name) / "STOP")
        self.cfg["data"]["directory"] = str(Path(self.tmp.name) / "market")
        self.cfg["data"]["calendar_cache"] = str(Path(self.tmp.name) / "calendar-cache.json")
        self.days = build_demo(self.cfg["data"]["directory"])

    def test_doctor_never_connects_and_never_outputs_secrets(self):
        with patch.dict(os.environ, {"TUSHARE_TOKEN": "secret-sentinel", "QMT_ACCOUNT_ID": "account-sentinel"}):
            result = configuration_report(self.cfg)
        encoded = json.dumps(result)
        self.assertNotIn("secret-sentinel", encoded)
        self.assertNotIn("account-sentinel", encoded)
        self.assertFalse(result["real_trading_enabled"])
        self.assertFalse(result["qmt"]["terminal_verified"])
        self.assertFalse((Path(self.tmp.name) / "paper.sqlite3").exists())

    def test_cli_config_allows_only_read_only_qmt_connection(self):
        self.cfg["qmt"]["enabled"] = True
        validate_config(self.cfg)
        self.cfg["qmt"]["read_only"] = False
        with self.assertRaises(ValueError):
            validate_config(self.cfg)

    def test_derived_limits_are_explicit_paper_opt_in(self):
        self.cfg["data"].update(provider="baostock", universe=["600000.SH"])
        with self.assertRaises(ValueError):
            validate_config(self.cfg)
        self.cfg["data"]["allow_derived_limits"] = True
        validate_config(self.cfg)
        self.cfg["mode"] = "live"
        with self.assertRaises(ValueError):
            validate_config(self.cfg)

    def test_first_start_prepares_history_without_fictitious_past_trades(self):
        self.cfg["data"]["provider"] = "tushare"
        self.cfg["data"]["universe"] = ["600001.SH", "000001.SZ", "300001.SZ", "688001.SH", "920001.BJ", "600002.SH"]
        provider = FilesProvider(self.cfg)
        real_calendar = provider.calendar
        provider.calendar = lambda start, end: real_calendar(max(start, self.days[0]), end)
        with patch("autotrade.pipeline.bootstrap_market") as bootstrap:
            report = prepare_and_run(self.cfg, self.days[19], provider)
        self.assertEqual(report["state"], "PLANNED")
        self.assertEqual(report["orders"], [])
        self.assertEqual(bootstrap.call_args.args[1:3], (self.days[0], self.days[19]))
        with patch("autotrade.pipeline.bootstrap_market") as bootstrap:
            next_report = prepare_and_run(self.cfg, self.days[22], provider)
        self.assertEqual(next_report["session"], self.days[22])
        self.assertTrue(next_report["reconciliation"]["ok"])
        self.assertEqual(bootstrap.call_args.args[1:3], (self.days[19], self.days[22]))
        with patch("autotrade.pipeline.provider_for", side_effect=RuntimeError("offline")):
            self.assertTrue(prepare_and_run(self.cfg, self.days[22])["replayed"])

    def test_explicit_replay_produces_only_paper_orders(self):
        result = replay_range(self.cfg, self.days[0], self.days[-1])
        self.assertEqual(result["real_orders_submitted"], 0)
        self.assertEqual(result["sessions"], 27)
        self.assertGreater(result["orders"], 0)
        self.assertTrue(result["reconciliation"]["ok"])

    def test_catchup_has_bounded_scope(self):
        self.cfg["data"].update(provider="tushare", max_catchup_sessions=1)
        self.cfg["data"]["universe"] = ["600001.SH", "000001.SZ", "300001.SZ", "688001.SH", "920001.BJ", "600002.SH"]
        provider = FilesProvider(self.cfg)
        real_calendar = provider.calendar
        provider.calendar = lambda start, end: real_calendar(max(start, self.days[0]), end)
        with patch("autotrade.pipeline.bootstrap_market"):
            prepare_and_run(self.cfg, self.days[19], provider)
        with self.assertRaisesRegex(ValueError, "catch-up exceeds"):
            prepare_and_run(self.cfg, self.days[22], provider)

    def test_invalid_provider_limits_fail_before_network(self):
        for key, value in [("request_attempts", 6), ("request_timeout", 121), ("retry_seconds", 31)]:
            cfg = copy.deepcopy(self.cfg)
            cfg["data"][key] = value
            with self.assertRaises(ValueError):
                validate_config(cfg)


if __name__ == "__main__":
    unittest.main()
