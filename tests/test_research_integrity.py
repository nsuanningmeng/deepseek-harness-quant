import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import yaml

from factors import research_context as context
from factors.opportunities import backtest_winrate as calibration, pitch_v2, scan
from factors.opportunities import pitch_track
from factors.pool import eval_ts
from factors.policy import timing_system
from report import daily_signal
from risk import beneish_full, stock_risk
from strategy import pool_layers, sim_tracks


class ResearchIntegrityTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        (self.root / "universe.json").write_text(json.dumps({"symbols": ["000001.SZ"]}))
        self.cfg = {"cache_dir": str(self.root), "analysis": {
            "scope": "hs300", "universe_file": str(self.root / "universe.json"),
            "finance_parquet_dir": str(self.root / "financials"), "min_history_days": 5,
            "calibration_min_samples": 30, "min_price": 1.5,
            "sim_tracks": {"start": "auto", "hold_months": 6, "top_n": 5},
            "pool_layers": {"above_ma50": True, "max_dist_high52_pct": -20, "min_vol_ratio": .8,
                            "industry_cap": 3, "defensive_cash": .5,
                            "capital_bands": [{"below": 100000, "count": 5}], "max_holdings": 20}}}
        self.cfg_file = self.root / "research.yaml"
        self.cfg_file.write_text(yaml.safe_dump(self.cfg))
        env = patch.dict(os.environ, {"LWQUANT_RESEARCH_CONFIG": str(self.cfg_file),
                                      "LWQUANT_CACHE_DIR": str(self.root)})
        env.start()
        self.addCleanup(env.stop)

    def test_readonly_missing_input_does_not_create_database(self):
        missing = self.root / "missing.db"
        with self.assertRaises(sqlite3.OperationalError):
            context.connect(missing)
        self.assertFalse(missing.exists())

    def test_latest_published_is_per_security_and_respects_announcement(self):
        db = self.root / "finance.db"
        with sqlite3.connect(db) as con:
            con.execute("CREATE TABLE finance_report(code TEXT,period TEXT,pub_date TEXT,roe REAL)")
            con.executemany("INSERT INTO finance_report VALUES (?,?,?,?)", [
                ("000001", "2024-12-31", "2025-03-20", .1),
                ("000001", "2025-03-31", "2025-04-25", .2),
                ("000002", "2024-12-31", "2025-04-01", .3)])
        rows = context.latest_published(db, "finance_report", "2025-04-10").set_index("code")
        self.assertEqual(set(rows.index), {"000001", "000002"})
        self.assertEqual(rows.loc["000001", "roe"], .1)

    def test_missing_announcement_cannot_be_treated_as_pit(self):
        db = self.root / "bad.db"
        with sqlite3.connect(db) as con:
            con.execute("CREATE TABLE finance_report(code TEXT,period TEXT)")
            con.execute("INSERT INTO finance_report VALUES ('000001','2024-12-31')")
        with self.assertRaisesRegex(ValueError, "公告日期"):
            context.latest_published(db, "finance_report", "2025-01-01")

    def test_valuations_scope_sql_preserves_latest_date_and_excludes_future(self):
        db = self.root / "daily_basic.db"
        with sqlite3.connect(db) as con:
            con.execute("CREATE TABLE daily_basic(code TEXT,date TEXT,pb REAL,PRIMARY KEY(code,date))")
            con.executemany("INSERT INTO daily_basic VALUES(?,?,?)", [
                ("000001.SZ", "2024-01-01", 1.), ("000001.SZ", "2025-01-01", 2.),
                ("000001.SZ", "2026-01-01", 99.), ("000002.SZ", "2025-01-01", 88.)])
            reference = pd.read_sql_query("SELECT * FROM daily_basic WHERE date<=?", con, params=("2025-06-01",))
        reference = reference[reference.code.isin(context.universe())].sort_values('date').drop_duplicates('code', keep='last')
        statements = []
        original = context.connect
        def traced(path):
            connection = original(path)
            connection.set_trace_callback(statements.append)
            return connection
        with patch.object(context, "connect", side_effect=traced):
            actual = context.valuations("2025-06-01")
        self.assertEqual(actual.to_dict('records'), reference.to_dict('records'))
        self.assertEqual(actual.iloc[0].pb, 2.)
        self.assertIn("WHERE code IN ('000001.SZ') AND date<=", statements[0])

    def test_calibration_finance_and_quality_load_only_current_scope(self):
        with sqlite3.connect(self.root/'finance.db') as con:
            con.execute('CREATE TABLE finance_report(code TEXT,period TEXT,pub_date TEXT,roe REAL,sq_net_yoy REAL,sq_rev_yoy REAL)')
            con.executemany('INSERT INTO finance_report VALUES(?,?,?,?,?,?)',
                            [(code, '2024-12-31', '2025-03-01', .1, .2, .3) for code in ('000001', '000002')])
        with sqlite3.connect(self.root/'finance_quality.db') as con:
            con.execute('CREATE TABLE quality(code TEXT,period TEXT,pub_date TEXT,roe_avg REAL,gp_margin REAL,liability_to_asset REAL,cfo_to_np REAL)')
            con.executemany('INSERT INTO quality VALUES(?,?,?,?,?,?,?)',
                            [(code, '2024-12-31', '2025-03-01', .1, .2, .3, 1.) for code in ('000001.SZ', '000002.SZ')])
        with sqlite3.connect(self.root/'stock_basic.db') as con:
            con.execute('CREATE TABLE stock_basic(code TEXT,name TEXT,industry TEXT)')
            con.executemany('INSERT INTO stock_basic VALUES(?,?,?)', [(code, code, 'test') for code in ('000001.SZ', '000002.SZ')])
        fake_bars = pd.DataFrame({'code': ['000001.SZ'], 'date': ['2025-01-01']})
        with patch.object(calibration, 'FIN_DB', self.root/'finance.db'), \
             patch.object(calibration, 'QD_DB', self.root/'finance_quality.db'), \
             patch.object(calibration, 'BASIC_DB', self.root/'stock_basic.db'), \
             patch.object(calibration, 'read_bars', return_value=fake_bars):
            _, finance, quality, basic = calibration._load_all()
        self.assertEqual(set(finance.code), {'000001'})
        self.assertEqual(set(quality.code), {'000001.SZ'})
        self.assertEqual(set(basic.index), {'000001.SZ'})
        self.assertEqual(finance.iloc[0].disclose, pd.Timestamp('2025-03-01'))

    def test_incomplete_holding_period_is_not_a_winrate_sample(self):
        dates = pd.date_range("2025-01-01", periods=10)
        self.assertIsNone(calibration._holding_stats(pd.Series(range(100, 110)), 0, 100, dates, 6))

    def test_completed_holding_period_uses_real_exit(self):
        dates = pd.date_range("2025-01-01", periods=200)
        result = calibration._holding_stats(pd.Series([100.] * 199 + [120.]), 0, 100., dates, 6)
        self.assertIsNotNone(result)
        self.assertEqual(result[0], 0.)  # Actual first day after six months, not the final data point.

    def test_no_calibration_does_not_create_static_probability(self):
        with patch.object(scan, "_WR_CACHE", {}):
            self.assertIsNone(scan.winrate_approx("quality_gap"))
            self.assertIsNone(scan.winrate_approx("unknown"))

    def test_pit_panel_supports_every_trigger_without_fake_valuation(self):
        dates = pd.bdate_range("2020-01-01", periods=320)
        px = pd.DataFrame({"000001.SZ": [10 + i / 100 for i in range(320)]}, index=dates)
        vx = pd.DataFrame({"000001.SZ": [100.] * 320}, index=dates)
        with patch.object(calibration, "valuations", return_value=pd.DataFrame()):
            panel = calibration.compute_factors_pit(px, vx, {"000001": {"roe": .2, "nyoy": .3, "ryoy": .2}},
                                                    {"000001.SZ": {"liab": .3, "cfo": 1., "roe": .2, "gp": .2}}, set(),
                                                    pd.DataFrame({"name": ["sample"], "industry": ["test"]}, index=["000001.SZ"]))
        self.assertTrue(pd.isna(panel.iloc[0]["pe_ttm"]))
        self.assertTrue(pd.isna(panel.iloc[0]["pb_pct"]))
        for trigger in scan.triggers().values():
            trigger(panel.iloc[0])  # All mandatory keys (including non_lowprice) must exist.
        self.assertFalse(scan.triggers()["value"](panel.iloc[0]))

    def test_categorical_security_axis_cannot_turn_numeric_factors_into_categories(self):
        dates = pd.bdate_range('2020-01-01', periods=320)
        columns = pd.CategoricalIndex(['000001.SZ', '000002.SZ'])
        prices = pd.DataFrame({code: [10+i/100 for i in range(320)] for code in columns}, index=dates)
        prices.columns = columns
        volume = prices*100
        finance = {code[:6]: {'roe': .2, 'nyoy': .3, 'ryoy': .2} for code in columns}
        quality = {'000001.SZ': {'liab': .3, 'cfo': 1.2}, '000002.SZ': {'liab': .4, 'cfo': -.5}}
        valuation = pd.DataFrame({'code': list(columns), 'pb': [1., 2.], 'pe_ttm': [10., 20.], 'dv_ttm': [2., 3.]})
        basic = pd.DataFrame({'name': list(columns), 'industry': 'test'}, index=columns)
        with patch.object(calibration, 'valuations', return_value=valuation):
            panel = calibration.compute_factors_pit(prices, volume, finance, quality, set(), basic)
            pre = calibration.precompute_tech(prices, volume)
            pre_panel = calibration.panel_from_pre(dates[-1], pre, finance, quality, set(), basic)
        for result in (panel, pre_panel):
            for field in ('cfo_to_np', 'liability', 'pb', 'pe_ttm', 'div_yield'):
                self.assertEqual(result[field].dtype.kind, 'f')
            self.assertEqual(result['cfo_health'].tolist(), [1, 0])
            for trigger in scan.triggers().values():
                result.apply(trigger, axis=1)

    def test_current_scan_runs_with_small_announced_scope_not_4000_reports(self):
        (self.root / "logs").mkdir()
        dates = pd.bdate_range("2020-01-01", periods=400).strftime("%Y-%m-%d")
        with sqlite3.connect(self.root / "bars.db") as con:
            con.execute("CREATE TABLE daily_bar(code TEXT,date TEXT,open REAL,high REAL,low REAL,close REAL,volume REAL,amount REAL,turn REAL,pct_chg REAL,preclose REAL,is_st INTEGER,adjust TEXT,source TEXT)")
            con.executemany("INSERT INTO daily_bar VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                            [("000001.SZ", d, 10+i/100, 11+i/100, 9+i/100, 10+i/100,
                              100., 100., .01, 0., 10., 0, "qfq", "tushare") for i, d in enumerate(dates)])
        with sqlite3.connect(self.root / "finance.db") as con:
            con.execute("CREATE TABLE finance_report(code TEXT,period TEXT,pub_date TEXT,net_profit REAL,revenue REAL,sq_net_yoy REAL,sq_rev_yoy REAL,roe REAL)")
            con.execute("INSERT INTO finance_report VALUES ('000001','2020-12-31','2021-03-01',10,20,.6,.2,.2)")
        with sqlite3.connect(self.root / "finance_quality.db") as con:
            con.execute("CREATE TABLE quality(code TEXT,period TEXT,pub_date TEXT,gp_margin REAL,np_margin REAL,current_ratio REAL,liability_to_asset REAL,cfo_to_np REAL,cfo_to_or REAL,roe_avg REAL)")
            con.execute("INSERT INTO quality VALUES ('000001.SZ','2020-12-31','2021-03-01',.3,.2,2,.3,1.2,.2,.2)")
        with sqlite3.connect(self.root / "stock_basic.db") as con:
            con.execute("CREATE TABLE stock_basic(code TEXT,name TEXT,industry TEXT,ipo_date TEXT)")
            con.execute("INSERT INTO stock_basic VALUES ('000001.SZ','sample','test','2000-01-01')")
        previous_cwd = Path.cwd()
        try:
            os.chdir(self.root)
            with patch.object(scan, "BASE", self.root), patch.object(scan, "OUT", self.root / "logs/opp_pool_1.json"), \
                 patch.object(scan, "FIN_DB", str(self.root / "finance.db")), \
                 patch.object(scan, "QD_DB", str(self.root / "finance_quality.db")), \
                 patch.object(scan, "BASIC_DB", str(self.root / "stock_basic.db")), \
                 patch.object(scan, "BARS_DB", str(self.root / "bars.db")), \
                 patch('data.cache.DailyCache') as latest_cache, \
                 patch.object(scan, "_WR_CACHE", {"event": .7}), \
                 patch.object(scan, "_risk_loaded", False), patch.object(scan, "_risk_cache", {}):
                # DailyCache's default path is bound at module import time, so
                # the environment-only fixture does not isolate this fallback.
                latest_cache.return_value.latest_trade_date.return_value=dates[-1]
                result = scan.scan(types=["event"], date=dates[-1], pitch_only=True)
        finally:
            os.chdir(previous_cwd)
        self.assertNotIn("error", result)
        self.assertEqual(result["n"], 1)
        self.assertEqual(result["opportunities"][0]["winrate_est"], .7)
        self.assertEqual(result["opportunities"][0]["code"], "000001.SZ")

    def test_walkforward_excludes_future_outcomes_and_insufficient_samples(self):
        rows = [{"otype": "breakout", "horizon_months": 6, "exit_date": "2025-01-01", "ret": .1}] * 29
        rows += [{"otype": "breakout", "horizon_months": 6, "exit_date": "2025-03-01", "ret": -.5}]
        self.assertEqual(sim_tracks._load_winrates(rows, pd.Timestamp("2025-02-01"), 30), {})
        self.assertAlmostEqual(sim_tracks._load_winrates(rows, pd.Timestamp("2025-03-01"), 30)["breakout"], 29/30)

    def test_latest_empty_pool_does_not_resurrect_old_candidates(self):
        old = self.root / "opp_pool_old.json"
        new = self.root / "opp_pool_new.json"
        old.write_text(json.dumps({"pitch": [{"code": "000001.SZ"}]}))
        new.write_text(json.dumps({"pitch": []}))
        os.utime(old, (1, 1)); os.utime(new, (2, 2))
        with patch.object(pitch_v2, "LOGS", self.root):
            self.assertEqual(pitch_v2.get_latest_pool(), new)

    def test_real_empty_scan_creates_explicit_empty_valid_pitch(self):
        pool = self.root / "opp_pool.json"
        pool.write_text(json.dumps({"date": "2025-01-01", "status": "no_qualifying_signals",
                                  "research_scope": {"scope": "hs300"}, "pitch": [], "stats": {}, "thresholds": {}}))
        out = self.root / "pitch.json"
        with patch.object(pitch_v2, "OUT_FILE", out):
            result, produced = pitch_v2.build(pool)
        self.assertEqual(produced, out)
        self.assertEqual(result["status"], "empty_valid")
        self.assertEqual(result["source_pool"], pool.name)
        self.assertEqual(result["n"], 0)

    def test_corrupt_empty_pool_is_not_empty_valid(self):
        pool = self.root / "opp_pool.json"
        pool.write_text(json.dumps({"pitch": []}))
        result, produced = pitch_v2.build(pool)
        self.assertIn("error", result)
        self.assertIsNone(produced)

    def test_financial_industry_is_not_a_fabricated_beneish_failure(self):
        self.cfg["analysis"]["beneish"] = {"excluded_industries": ["银行"]}
        self.cfg_file.write_text(yaml.safe_dump(self.cfg))
        with sqlite3.connect(self.root / "stock_basic.db") as con:
            con.execute("CREATE TABLE stock_basic(code TEXT,industry TEXT)")
            con.execute("INSERT INTO stock_basic VALUES ('000001.SZ','银行')")
        with patch.object(beneish_full, "load_company", side_effect=AssertionError("inapplicable model must not run")):
            result = beneish_full.compute_full("000001.SZ")
        self.assertEqual(result["mode"], "not_applicable")
        self.assertIsNone(result["m_score"])

    def test_beneish_coalesces_announcement_and_keeps_future_revision_out(self):
        for api in ("income", "balancesheet", "cashflow"):
            target = self.root / api / "data" / f"{api}__ts_code=000001.SZ.parquet"
            target.parent.mkdir(parents=True)
            target.touch()
        raw = pd.DataFrame([
            {"end_date": "20241231", "ann_date": "20250301", "f_ann_date": None, "report_type": "1", "revenue": 120.},
            {"end_date": "20241231", "ann_date": "20250301", "f_ann_date": "20990101", "report_type": "4", "revenue": 999.},
            {"end_date": "20231231", "ann_date": "20240301", "f_ann_date": None, "report_type": "1", "revenue": 100.}])
        with patch.object(beneish_full, "PARQUET_BASE", self.root), patch.object(beneish_full.pd, "read_parquet", return_value=raw):
            result = beneish_full.load_company("000001.SZ")
        self.assertEqual(len(result["income"]), 2)
        self.assertEqual(result["income"][0]["revenue"], 120.)

    def test_forward_track_does_not_backdate_to_latest_available_history(self):
        import data.cache
        (self.root / "logs").mkdir()
        db = self.root / "bars.db"
        with sqlite3.connect(db) as con:
            con.execute("CREATE TABLE daily_bar(code TEXT,date TEXT,close REAL,adjust TEXT)")
            con.execute("INSERT INTO daily_bar VALUES ('000001.SZ','2020-01-02',100,'qfq')")
        source = self.root / "logs/pitch_track_pool_1.json"
        source.write_text(json.dumps({"entries": [{"code": "000001.SZ", "entry_date": "2030-01-01",
                                                  "entry_close": None, "fwd": {}, "age_days": 0}]}))
        with patch.object(pitch_track, "BASE", self.root), patch.object(pitch_track, "BARS_DB", db), \
             patch.object(data.cache, "CACHE_DIR", self.root):
            pool = pitch_track.update_fwd()
        self.assertEqual(pool["entries"][0]["entry_date"], "2030-01-01")
        self.assertIsNone(pool["entries"][0]["entry_close"])
        self.assertEqual(pool["entries"][0]["fwd"], {})

    def test_insufficient_factor_history_has_no_neutral_score(self):
        factor = pd.Series(range(12), index=[f"2024-{m:02d}" for m in range(1, 13)])
        result = eval_ts.evaluate_time_series("test", factor, factor / 100)
        self.assertIsNone(result["score"])
        self.assertFalse(result["ready"])

    def test_missing_timing_dimension_cannot_be_50_or_green(self):
        with patch.object(timing_system, "policy_score", return_value={"score": None, "note": "missing"}), \
             patch.object(timing_system, "macro_score", return_value={"score": 90}), \
             patch.object(timing_system, "sentiment_score", return_value={"score": 90}), \
             patch.object(timing_system, "breadth_score", return_value={"score": 90}), \
             patch.object(timing_system, "_latest_trade_date", return_value="2025-01-01"):
            result = timing_system.evaluate()
        self.assertFalse(result["ready"])
        self.assertIsNone(result["score"])
        self.assertEqual(result["status"], "unavailable")

    def test_timing_breadth_counts_only_raw_sh_sz_stock_metadata_and_known_changes(self):
        with sqlite3.connect(self.root/'stock_basic.db') as con:
            con.execute('CREATE TABLE stock_basic(code TEXT)')
            con.executemany('INSERT INTO stock_basic VALUES(?)', [(c,) for c in
                            ('600000.SH', '000001.SZ', '300001.SZ', '301001.SZ', '920001.BJ')])
        with sqlite3.connect(self.root/'bars.db') as con:
            con.execute('CREATE TABLE daily_bar(code TEXT,date TEXT,adjust TEXT,pct_chg REAL)')
            rows = [('600000.SH', 'none', 1.), ('000001.SZ', 'none', -1.),
                    ('300001.SZ', 'none', 0.), ('301001.SZ', 'none', None),
                    ('510300.SH', 'none', 20.), ('000300.SH', 'none', 10.),
                    ('920001.BJ', 'none', 10.), ('600000.SH', 'qfq', -99.)]
            con.executemany('INSERT INTO daily_bar VALUES(?,?,?,?)',
                            [(code, '2025-01-01', adjust, change) for code, adjust, change in rows])
        with patch.object(timing_system, 'BARS_DB', str(self.root/'bars.db')), \
             patch.object(timing_system, 'cache_dir', return_value=self.root), \
             patch.object(timing_system, '_merged_max_date', return_value='2025-01-01'), \
             patch.object(timing_system.glob, 'glob', return_value=[]):
            result = timing_system.breadth_score()
        self.assertEqual((result['up'], result['down'], result['flat'], result['total'], result['unknown']), (1, 1, 1, 3, 1))
        self.assertIn('1/3', result['note'])
        self.assertEqual(result['scope'], 'all_sh_sz_a_shares')

    def test_missing_risk_values_are_not_pass(self):
        result = stock_risk._check_row("000001.SZ", None, None, None, None, None, None)
        self.assertEqual(result["level"], "NO_DATA")
        self.assertIsNone(result["score"])

    def test_blocked_audit_cannot_publish_signal(self):
        for name in ("logs", "output", "report"):
            (self.root / name).mkdir()
        (self.root / "logs/opp_pool_1.json").write_text(json.dumps({"date": "2025-01-01"}))
        (self.root / "output/pool_layers_1.json").write_text(json.dumps({"date": "2025-01-01"}))
        (self.root / "report/data_audit_report_1.json").write_text(json.dumps({"blocked": True}))
        with patch.object(daily_signal, "BASE", self.root), self.assertRaisesRegex(ValueError, "审计已阻断"):
            daily_signal.build()

    def test_beneish_uses_two_period_aqi_and_fixed_assets_in_depi(self):
        data = {
            "income": [{"end_date": "20241231", "revenue": 120., "oper_cost": 70., "sell_exp": 2., "admin_exp": 3., "net_profit": 10.},
                       {"end_date": "20231231", "revenue": 100., "oper_cost": 50., "sell_exp": 2., "admin_exp": 2.}],
            "balancesheet": [{"end_date": "20241231", "accounts_receiv": 12., "total_assets": 200., "total_cur_assets": 50., "fix_assets": 60., "total_liab": 80.},
                             {"end_date": "20231231", "accounts_receiv": 10., "total_assets": 100., "total_cur_assets": 20., "fix_assets": 40., "total_liab": 30.}],
            "cashflow": [{"end_date": "20241231", "n_cashflow_act": 8., "depreciation": 10.},
                         {"end_date": "20231231", "n_cashflow_act": 7., "depreciation": 5.}]}
        with patch.object(beneish_full, "load_company", return_value=data):
            result = beneish_full.compute_full("000001.SZ")
        self.assertEqual(result["mode"], "full")
        self.assertAlmostEqual(result["components"]["aqi"], 1.125)
        self.assertAlmostEqual(result["components"]["depi"], .7778)
        del data["cashflow"][0]["depreciation"]
        with patch.object(beneish_full, "load_company", return_value=data):
            result = beneish_full.compute_full("000001.SZ")
        self.assertIsNone(result["m_score"])
        self.assertIn("depi", result["missing"])

    def test_pool_requires_regime_and_zero_distance_is_valid(self):
        ranking = {"date": "2025-01-01", "top": [{"code": "000001.SZ", "industry": "bank", "score": 80,
                   "tech": {"above_ma50": True, "dist_high52_pct": 0., "vol_ratio_20_60": 1.}}]}
        with self.assertRaisesRegex(ValueError, "regime"):
            pool_layers.build_layers(ranking)
        self.assertEqual(pool_layers.build_layers(ranking, regime_cash=0.)["n_decision"], 1)
        self.assertEqual(pool_layers.build_layers(ranking, regime_cash=1.)["n_decision"], 0)

    def test_sim_monthly_mark_does_not_compound_lifetime_return_again(self):
        (self.root / "logs").mkdir()
        observations = [{"otype": "breakout", "horizon_months": 6,
                         "exit_date": "2019-01-01", "ret": .1}] * 30
        (self.root / "logs/opportunity_winrates.json").write_text(json.dumps({"observations": observations}))
        dates = pd.to_datetime(["2020-01-02", "2020-01-31", "2020-02-03", "2020-02-28", "2020-03-02", "2020-03-31",
                                "2020-04-01", "2020-04-30", "2020-05-01", "2020-05-29"])
        closes = [100., 100., 100., 100., 100., 110., 110., 121., 121., 133.1]
        bars = pd.DataFrame({"date": dates, "code": "000001.SZ", "open": closes, "close": closes, "volume": 100., "is_st": 0})
        f = pd.DataFrame({"drawdown_60d": [.1], "vol60": [.3], "sq_nyoy": [.1]}, index=["000001.SZ"])
        with patch.object(sim_tracks, "BASE", self.root), patch.object(sim_tracks, "OUT", self.root / "logs/sim_tracks.json"), \
             patch.object(sim_tracks, "_load_all", return_value=(bars, pd.DataFrame(), pd.DataFrame(), pd.DataFrame())), \
             patch.object(sim_tracks, "_load_regime", return_value={"2020-03": 1., "2020-04": 1., "2020-05": 1.}), \
             patch.object(sim_tracks, "compute_factors_pit", return_value=f), \
             patch.object(sim_tracks, "triggers", return_value={"breakout": lambda row: True}), \
             patch.object(sim_tracks, "_pit_finance", return_value={}), patch.object(sim_tracks, "_pit_quality", return_value={}):
            result = sim_tracks.run("2020-03")
        curve = result["tracks"]["pitch"]["curve"]
        self.assertAlmostEqual(curve[-2]["ret"], .1)
        self.assertAlmostEqual(curve[-1]["ret"], .1)
        self.assertAlmostEqual(curve[-1]["nav"], 1.21)
        self.assertEqual(result["kind"], "historical_backtest")


if __name__ == "__main__":
    unittest.main()
