"""Isolated public-view fixtures; no real accounts, downloads or research jobs."""
import csv
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import types
import unittest
from unittest.mock import patch

from deck import live_api, research_views as views


class ResearchViewsTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.output=self.root/"factors";self.cache=self.root/"cache"
        self.output.mkdir();self.cache.mkdir()
        patcher=patch.object(views,"BASE",self.root);patcher.start();self.addCleanup(patcher.stop)
        patcher=patch.object(live_api,"BASE",self.root);patcher.start();self.addCleanup(patcher.stop)
        patcher=patch.object(views,"locations",return_value=(self.output,self.cache));patcher.start();self.addCleanup(patcher.stop)
        patcher=patch.object(views,"_minute_background_job",return_value={});patcher.start();self.addCleanup(patcher.stop)
        snapshot=views.BackgroundSnapshot('isolated-minute-samples')
        patcher=patch.object(views,'_minute_samples',snapshot);patcher.start();self.addCleanup(patcher.stop)
        self.addCleanup(snapshot._executor.shutdown)
        self.date="2026-11-10"

    def json(self,path,value):
        path=Path(path);path.parent.mkdir(parents=True,exist_ok=True);path.write_text(json.dumps(value));return path

    def inputs(self):
        self.json(self.output/"factor_manifest_test.json",{"date":self.date,"data_date":self.date,"health_date":self.date,"source":"isolated-fixture","analysis":{"scope":"test-only"},"factors":[
            {"code":"mom_20","direction":-1,"category":"a_share_alpha"},{"code":"new_high_250","direction":0,"category":"a_share_alpha"}]})
        path=self.output/"health"/("health_"+self.date+".csv");path.parent.mkdir()
        with path.open("w",newline="") as handle:
            writer=csv.DictWriter(handle,fieldnames=["factor","direction","icir60","icir120","t120","status"]);writer.writeheader()
            writer.writerows([{"factor":"mom_20","direction":-1,"icir60":".4","icir120":".5","t120":"2.1","status":"研究有效"},
                             {"factor":"new_high_250","direction":0,"icir60":"","icir120":"","t120":"","status":"停用"}])
        self.json(self.output/"crowding_test.json",{"date":self.date,"crowding_pctile_252":50,"crowding_mkt":1.3})

    def test_real_factor_count_missing_numbers_and_disabled_rank(self):
        self.inputs();result=views.factor_dashboard()
        self.assertEqual(result["manifest"]["n"],2)
        self.assertIsNone(result["health"]["rows"][1]["icir120"])
        self.assertAlmostEqual(result["crowding"]["pctile"],.5)
        ranking=views.factor_ranking()["ranking"]
        self.assertEqual(ranking[0]["factor"],"mom_20")
        self.assertIsNone(ranking[1]["rank"]);self.assertIsNone(ranking[1]["composite"])
        self.assertFalse((self.cache/"unified.db").exists())

    def test_optional_style_is_not_reported_as_ready(self):
        self.inputs()
        for prefix,payload in {"factor_usage":{"layers":{"deferred":{"mom_20":"research"}}},"factor_lifecycle":{"factors":{"mom_20":{}}},
                               "factor_corr":{"matrix":[[1]],"factors":["mom_20"]},"frc_mine_rules":{"rules":[{"factor":"mom_20","coefficient":.5}]},
                               "factor_data_freshness":{"updated":self.date},"factor_alert_rules":{"rules":[{"factor":"mom_20"}]}}.items():
            self.json(self.output/"ui_data"/(prefix+"_test.json"),payload)
        result=views.factor_ui_pack()
        self.assertTrue(result["ready"])
        self.assertEqual(result["modules"]["style_rotation"]["state"],"optional_unconfigured")
        self.assertFalse(result["modules"]["style_rotation"]["ready"])

    def test_initialized_forward_tables_wait_for_real_samples_without_zero_returns(self):
        self.inputs()
        with sqlite3.connect(self.cache/"unified.db") as con:
            con.execute("CREATE TABLE factor_agg(factor TEXT,n_pitch INT,n_done_t5 INT,t1_avg REAL,t5_avg REAL,t5_win REAL,excess_avg REAL)")
            con.execute("CREATE TABLE factor_pitch(factor TEXT,code TEXT,entry_date TEXT,pool_type TEXT,t1 REAL,t5 REAL)")
        result=views.factor_performance()
        self.assertEqual(result["state"],"pending_history");self.assertTrue(result["initialized"]);self.assertFalse(result["ready"])
        self.assertEqual(result["factors"],[]);self.assertEqual(result["detail"],[])

    def test_minute_http_uses_receipts_until_deep_proof_and_never_scans_old_rows(self):
        path=self.cache/'minute.db'
        with sqlite3.connect(path) as con:
            con.execute('CREATE TABLE minute_metadata(name TEXT,value TEXT)')
            con.executemany('INSERT INTO minute_metadata VALUES(?,?)',
                            [('frequency','5min'),('volume_unit','shares'),('amount_unit','yuan')])
            con.execute('CREATE TABLE minute_5m(code TEXT,trade_time TEXT,open REAL,high REAL,low REAL,close REAL,volume REAL,amount REAL,source TEXT,PRIMARY KEY(code,trade_time))')
            con.executemany("INSERT INTO minute_5m VALUES(?,?,10,11,9,10,1000,10000,'isolated-fixture')",
                            [('600000.SH',self.date+' 15:00:00'),('600000.SH','2026-08-09 15:00:00'),('999999.SZ',self.date+' 15:00:00')])
        job={'job_id':'isolated','database_path':str(path),'status':'waiting','service_alive':True,
             'start_date':'2026-08-10','target_date':self.date,'current_core_data_date':self.date,
             'expected_symbols':1,'completed_symbols':1,'symbols_with_rows':1,'rows':1,'covered_symbols':1,
             'symbols':['600000.SH'],'universe_fingerprint':hashlib.sha256(b'600000.SH').hexdigest(),
             'deep_verified':False,'cache_verified':False,'current_date_ready':False,'current_universe_match':True}
        with patch.object(views,'_minute_background_job',return_value=job):
            with patch('sqlite3.connect',side_effect=AssertionError('HTTP must not open a downloading database')):
                partial=views.minute_snapshot()
            self.assertFalse(partial['ready']);self.assertIsNone(partial['rows']);self.assertIsNone(partial['total_cache_rows'])
            self.assertIsNone(partial['covered_symbols']);self.assertEqual(partial['completed_symbols'],1)
            job['core_update_in_progress']=True
            busy=views.minute_snapshot();self.assertFalse(busy['ready']);self.assertIn('行情缓存正被后台任务使用',busy['reason'])
            self.assertNotIn('核心行情正在更新',busy['reason']);job['core_update_in_progress']=False
            job.update(status='complete',cache_verified=True,deep_verified=True,frozen_window_complete=True)
            pending=views.minute_snapshot();self.assertFalse(pending['ready'])
            views._minute_samples._future.result(timeout=3)
            stale=views.minute_snapshot();self.assertEqual(stale['state'],'stale');self.assertTrue(stale['initialized'])
            job['current_date_ready']=True
            complete=views.minute_snapshot();self.assertTrue(complete['ready']);self.assertEqual(complete['rows'],1)
            self.assertEqual({row['code'] for row in complete['latest']},{'600000.SH'})
            self.assertIsNone(complete['total_cache_rows']);self.assertFalse(complete['one_minute']['ready'])
            with sqlite3.connect(path) as con:con.execute("UPDATE minute_metadata SET value='1min' WHERE name='frequency'")
            pending=views.minute_snapshot();self.assertFalse(pending['ready'])
            try:views._minute_samples._future.result(timeout=3)
            except ValueError:pass
            failed=views.minute_snapshot();self.assertFalse(failed['ready']);self.assertEqual(failed['state'],'failed')
        with patch('sqlite3.connect',side_effect=AssertionError('No legacy fallback scan')):
            missing=views.minute_snapshot()
        self.assertFalse(missing['ready']);self.assertIsNone(missing['rows'])

    def test_archive_requires_successful_algorithm_and_current_hash(self):
        path=self.json(self.root/"report/factor_pool_report.json",{"date":self.date,"data_date":self.date,"analysis":{"scope":"isolated"},"evaluations":{"mom_20":{"20":{"n_sections":120}}}})
        self.assertEqual(views.research_archives(),[])

        self.json(self.root/"runtime/research/pipeline-state.json",{"steps":[{"status":"complete","returncode":0,"evidence":[{"path":str(path),"source":"isolated-fixture","algorithm":"test","sha256":hashlib.sha256(path.read_bytes()).hexdigest()}]}]})
        records=views.research_archives();self.assertEqual(len(records),1)
        self.assertIsNone(records[0]["annual_return"]);self.assertEqual(records[0]["validation_stage"],"in_sample_research")
        self.json(self.root/"runtime/research/pipeline-state.json",{"steps":[{"status":"success","returncode":0,"evidence":[{"path":str(path),"source":"isolated-fixture","algorithm":"test","sha256":hashlib.sha256(path.read_bytes()).hexdigest()}]}]})
        self.assertEqual(len(views.research_archives()),1)
        path.write_text(path.read_text()+" ")
        self.assertEqual(views.research_archives(),[])


    def test_pool_history_only_uses_real_generated_dates_and_preserves_market_date(self):
        path=self.json(self.root/'logs/opp_pool_20261112_001000.json',{'date':self.date,'opportunities':[{'code':'600000.SH'}],'pitch':[]})
        self.json(self.root/'logs/opp_pool_19990101_fake.json',{'date':'1999-01-01','opportunities':[],'pitch':[]})
        self.json(self.root/'runtime/research/pipeline-state.json',{'steps':[{'status':'success','returncode':0,'evidence':[{'path':str(path),'source':'isolated','algorithm':'actual pool generator','sha256':hashlib.sha256(path.read_bytes()).hexdigest()}]}]})
        result=views.opportunity_pool_history()
        self.assertTrue(result['ready']);self.assertEqual(result['n_days'],1)
        self.assertEqual(result['history'][0]['date'],'2026-11-12')
        self.assertEqual(result['history'][0]['data_date'],self.date)
        self.assertEqual(result['history'][0]['n'],1);self.assertFalse(result['reconstructed_backtest'])
    def test_audit_route_reads_timestamped_report_in_declared_report_directory(self):
        from deck import deck_server
        directory=self.root/"report"
        actual=self.json(directory/"data_audit_report_20261111_192438.json",{"checks":[{"ok":True}]})
        with patch.dict(deck_server.ROUTES,{"/api/audit":directory/"data_audit_report.json"}):
            self.assertEqual(deck_server._resolve_route("/api/audit"),actual)
        self.assertFalse((directory/"data_audit_report.json").exists())

    def test_portfolio_get_never_invents_or_saves_missing_cost(self):
        portfolio={"positions":[{"code":"600000.SH","status":"holding","entry_date":"2026-11-01"}]}
        with patch("strategy.portfolio._load",return_value=portfolio),patch("strategy.portfolio._save") as save,patch("strategy.portfolio._entry_price_of") as backfill,patch("strategy.portfolio._latest_close_of",return_value=20),patch.object(live_api,"live_realtime",return_value={"ok":False}):
            result=live_api.portfolio_pnl()
        save.assert_not_called();backfill.assert_not_called()
        self.assertIsNone(result["rows"][0]["entry_price"]);self.assertIsNone(result["rows"][0]["ret"]);self.assertIsNone(result["total_cost"])
        self.assertNotIn("entry_price",portfolio["positions"][0])

    def test_forward_get_does_not_update_business_history(self):
        with patch("factors.opportunities.pitch_track.load_latest",return_value={"ts":"test","entries":[]}),patch("factors.opportunities.pitch_track.summary",return_value={}),patch("factors.opportunities.pitch_track.update_fwd") as update,patch.object(live_api,"_bars_version",return_value=0),patch.object(live_api,"_latest_file",return_value=None),patch.object(live_api,"_fwd_cache",{"ts":0,"data":None}),patch.object(live_api,"_complete_daily_status",return_value={"ready":True,"state":"ready","date":self.date}),patch("data.cache.DailyCache") as cache:
            result=live_api.live_forward(force=True)
        update.assert_not_called();cache.assert_not_called();self.assertEqual(result["entries"],[])

    def test_research_signal_does_not_become_an_implicit_hold_or_buy_plan(self):
        signal={"kind":"research_summary","date":self.date,"watch":[{"code":"600000.SH"}],"candidate":[],"decision":[],"pitch":[],"regime":{"pos":.5},"source_files":["actual.json"]}
        result=live_api._research_signal_view(signal)
        self.assertTrue(result["research_only"]);self.assertFalse(result["auto_trade_allowed"])
        self.assertFalse(result["has_hold_plan"]);self.assertFalse(result["has_buy_plan"])
        self.assertEqual(result["counts"]["watch"],1);self.assertEqual(result["counts"]["candidate"],0)
        self.assertNotIn("hold_plan",signal);self.assertNotIn("n_passed",signal)
        missing=live_api._research_signal_view({"kind":"research_summary"})
        self.assertIsNone(missing["counts"]["candidate"])

    def test_purification_disabled_and_weak_health_override_positive_icir(self):
        rows=[{"factor":"disabled","direction":0,"status":"停用","icir120":.22},
              {"factor":"weak","direction":1,"status":"研究弱有效","icir120":.4},
              {"factor":"valid","direction":-1,"status":"研究有效","icir120":.7}]
        supported={row["factor"]:{"direction":row["direction"]} for row in rows}
        result=views.factor_purification(rows,supported,{"disabled":{"disabled":"no trading"},"deferred":{"weak":"research","valid":"research"},"ext_decision":{"disabled":"contradictory old setting"}})
        self.assertEqual(result["counts"],{"停用":1,"研究弱有效":1,"研究有效":1})
        self.assertTrue(result["rows"][0]["disabled"]);self.assertIn("不参与选股",result["rows"][0]["execution"])
        self.assertIn("仅作研究参考",result["rows"][1]["execution"])
        self.assertNotIn("正常入选股",json.dumps(result,ensure_ascii=False))

    def test_funnel_reads_verified_inventory_without_recounting_millions_of_bars(self):
        inventory=self.json(self.root/"report/data_asset_audit_20261111.json",{"dbs":[{"name":"bars.db","tables":[{"name":"daily_bar","rows":12345}]}]})
        self.json(self.root/"runtime/research/pipeline-state.json",{"steps":[{"status":"success","returncode":0,"evidence":[{"path":str(inventory),"source":"isolated","algorithm":"inventory","sha256":hashlib.sha256(inventory.read_bytes()).hexdigest()}]}]})
        with patch.object(live_api,"BASE",self.root),patch("sqlite3.connect",side_effect=AssertionError("HTTP must not scan bars")):
            result=live_api.live_funnel()
        self.assertEqual(result["funnel"][0]["n"],12345)
        inventory.write_text(inventory.read_text()+" ")
        with patch.object(live_api,"BASE",self.root),patch("sqlite3.connect",side_effect=AssertionError("HTTP must not scan bars")):
            stale=live_api.live_funnel()
        self.assertIsNone(stale["funnel"][0]["n"])

    def test_no_recorded_approval_portfolio_is_unknown_not_zero_stop_warnings(self):
        with patch.object(live_api,"_read",return_value={}),patch("strategy.portfolio._load",return_value={"positions":[]}),patch("strategy.portfolio._latest_glob",return_value=self.root/"not-created.json"):
            result=live_api.live_pools()
        self.assertFalse(result["stop_alerts"]["portfolio_recorded"])
        self.assertIsNone(result["stop_alerts"]["held_n"]);self.assertIsNone(result["stop_alerts"]["held_near"])

    def test_ticker_inputs_normalize_crowding_and_preserve_real_breadth_without_temperature(self):
        timing=self.json(self.root/"output/timing_system_20261111.json",{"score":53.5,"dims":{"情绪":{"score":72.9,"note":"actual measured proxy"},"宽度":{"score":71.3,"up":65,"total":100,"source":"isolated","asof":self.date}}})
        crowd=self.json(self.output/"crowding_test.json",{"crowding_pctile_252":14.2857})
        legacy_temp=self.json(self.output/"market_emotion_temp_legacy.json",{"temp":99,"zone":"unverified legacy"})
        def files(pattern):
            if "timing_system_" in pattern:return [str(timing)]
            if "crowding_" in pattern:return [str(crowd)]
            if "market_emotion_temp" in pattern:return [str(legacy_temp)]
            return []
        calendar=types.SimpleNamespace(get_window=lambda:None,upcoming=lambda **kwargs:[])
        with patch.object(live_api,"BASE",self.root),patch.object(live_api,"_bars_version",return_value=0),patch.object(live_api,"_td_cache",{"ts":0,"data":None,"ver":None}),patch("glob.glob",side_effect=files),patch.dict("sys.modules",{"data.calendar_hook":calendar}):
            result=live_api.live_timing_dash()
        self.assertAlmostEqual(result["crowding"]["pctile"],.142857)
        self.assertAlmostEqual(result["crowd_hist"][-1],.142857)
        self.assertEqual(result["breadth"]["score"],71.3);self.assertEqual(result["breadth"]["up_fraction"],.65)
        self.assertIsNone(result["temperature"]["value"])
        self.assertEqual(result["temperature"]["state"],"legacy_unsupported")
        self.assertEqual(result["sentiment"]["score"],72.9)

    def test_hybrid_quote_reader_preserves_armed_state_without_switching_supplier(self):
        self.json(self.root/"state.json",{})
        cfg={"runtime_dir":str(self.root),"market":{"provider":"tencent_tushare","source_label":"dynamic hybrid source"}}
        state={"paused":True,"activation":{"enabled":True,"status":"waiting_data"},"market":{"error":"old rt_k failure"}}
        with patch("paperlive.config.load_config",return_value=cfg),patch("paperlive.runtime.public_state",return_value=state),patch.object(live_api,"_fetch_realtime") as fetch:
            result=live_api.live_realtime()
        fetch.assert_not_called();self.assertEqual(result["source"],"dynamic hybrid source")
        self.assertEqual(result["state"],"waiting_data");self.assertFalse(result["ready"])
        self.assertNotIn("error",result);self.assertEqual(result["historical_error"],"old rt_k failure")

    def test_opportunity_consumer_does_not_invent_a_passed_gate_from_summary(self):
        signal={"kind":"research_summary","date":self.date,"watch":[],"candidate":[],"decision":[],"pitch":[]}
        def read(pattern,subdir="logs"):
            if pattern=="daily_signal_*.json":return signal
            if pattern=="opp_pool_*.json":return {"date":self.date,"opportunities":[],"pitch":[]}
            return {}
        with patch.object(live_api,"_read",side_effect=read),patch.object(live_api,"_latest_file",return_value=None),patch.object(live_api,"live_validation",return_value={}):
            result=live_api.live_opp()
        self.assertIsNone(result["n_suggested"])
        self.assertIsNone(result["overview"]["gate_ok"]);self.assertIsNone(result["overview"]["n_passed"])
        self.assertTrue(result["overview"]["signal_summary"]["research_only"])


if __name__=="__main__":
    unittest.main()
