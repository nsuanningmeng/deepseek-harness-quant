import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd


class ResearchDeliveryTests(unittest.TestCase):
    def setUp(self):
        # Summary and probe fixtures must not launch the configured production
        # minute verifier, including when the live core is no longer updating.
        patcher=patch('deck.system_live._minute_job_status',return_value={})
        patcher.start();self.addCleanup(patcher.stop)

    def test_research_summary_schema_does_not_require_fabricated_orders(self):
        from deck.system_live import _product_schema
        summary={'kind':'research_summary','date':'2026-11-11','watch':[{'code':'600000.SH'}],
                 'candidate':[],'decision':[],'pitch':[],'regime':{'pos':0},
                 'source_files':['output/real-input.json'],'audit':{'blocked':False}}
        self.assertTrue(_product_schema('signal',summary)[0])
        self.assertNotIn('buy_plan',summary)
        summary['audit']['blocked']=True
        self.assertFalse(_product_schema('signal',summary)[0])

    def test_opportunity_panel_preserves_adjusted_and_raw_price_meanings(self):
        from factors.opportunities import scan
        import numpy as np
        dates=pd.date_range('2025-01-01',periods=260)
        prices=pd.DataFrame({'000001.SZ':np.linspace(5,10,260),'600000.SH':np.linspace(8,12,260)},index=dates)
        finance=pd.DataFrame({'roe':[.2,.1],'sq_net_yoy':[.3,.1],'sq_rev_yoy':[.2,.1],
                              'gross_margin':[.4,.3]},index=['000001','600000'])
        basic=pd.DataFrame({'name':['A','B'],'industry':['I','I']},index=prices.columns)
        values={code:{'close':30.,'pb':1.,'pe_ttm':10.,'dv_ratio':None,'total_mv':10000.} for code in prices}
        with patch.object(scan,'load_quality',return_value=None),patch.object(scan,'load_valuation',return_value=values),patch.object(scan,'research_config',return_value={'min_price':1.5}):
            result=scan.compute_factors(prices,prices*100,finance,basic,set())
        self.assertEqual(result.loc['000001.SZ','close'],10.)
        self.assertEqual(result.loc['000001.SZ','valuation_close'],30.)
        self.assertTrue(pd.isna(result.loc['000001.SZ','div_yield']))
        self.assertEqual(result.loc['000001.SZ','total_mv_yi'],1.)

    def test_complete_core_does_not_certify_rate_limited_minutes(self):
        from deck import system_live as module
        target='2026-11-11'
        documents={
            'bootstrap-state.json':{'status':'rate_limited','stage':'minute'},
            'bootstrap-manifest.json':{'coverage':{'daily':{'end':target}},
                'stages':{key:'complete' for key in ('daily','finance','assets')},
                'stage_dates':{key:target for key in ('daily','finance','assets')}},
            'pipeline-state.json':{'status':'complete','required_inputs':['daily','finance','assets']},
        }
        with patch.object(module,'_research_document',side_effect=documents.get):
            result=module._research_status({'signal':{'initialized':True,'ready':True}})
        self.assertTrue(result['initialized'])
        self.assertFalse(result['ready'])
        self.assertEqual(result['status'],'limited')
        self.assertEqual(result['uninitialized_count'],0)

    def test_slow_database_probe_does_not_block_status_or_repeat_unchanged_version(self):
        from deck import system_live as module
        from concurrent.futures import ThreadPoolExecutor
        import threading,time
        gate=threading.Event()
        def slow_probe(name,path):
            gate.wait(2)
            return {name:{'ready':True,'rows':1}}
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);(root/'bars.db').write_text('version-one')
            pool=ThreadPoolExecutor(max_workers=1)
            try:
                with patch('data.cache.CACHE_DIR',root),patch.object(module,'DBS',{'bars':'bars.db'}),patch.object(module,'_db_probe_jobs',{}),patch.object(module,'_db_probe_pool',pool),patch.object(module,'_research_document',return_value={}),patch.object(module,'_probe_one',side_effect=slow_probe) as probe:
                    started=time.monotonic();result=module._db_health()
                    self.assertLess(time.monotonic()-started,0.5)
                    self.assertEqual(result['bars']['state'],'probing')
                    self.assertFalse(result['bars']['ready'])
                    module._db_health()
                    gate.set();pool.shutdown(wait=True)
                    self.assertTrue(module._db_health()['bars']['ready'])
                    self.assertEqual(probe.call_count,1)
            finally:
                gate.set();pool.shutdown(wait=True)

    def test_registered_but_immature_forward_entries_are_not_performance(self):
        from deck.system_live import _has_forward_observation
        self.assertFalse(_has_forward_observation({'entries':[{'code':'600000.SH','fwd':{'t1':None,'t5':None}}]}))
        self.assertTrue(_has_forward_observation({'entries':[{'code':'600000.SH','fwd':{'t1':{'ret':0.0}}}]}))

    def test_database_acceptance_rejects_stale_dates_and_null_prices(self):
        from validation import check_research_complete as module
        with tempfile.TemporaryDirectory() as tmp:
            cache=Path(tmp)
            with sqlite3.connect(cache/'bars.db') as con:
                con.execute('CREATE TABLE daily_bar(date TEXT,adjust TEXT,source TEXT,open REAL,high REAL,low REAL,close REAL)')
                con.execute("INSERT INTO daily_bar VALUES('2000-01-01','qfq','tushare',1,1,1,1)")
            manifest={'coverage':{'daily':{'rows':1,'end':'2026-11-11'}}}
            with patch.object(module,'CACHE_DIR',cache):
                self.assertFalse(module.database_coverage(manifest)[0]['initialized'])
                with sqlite3.connect(cache/'bars.db') as con:
                    con.execute("UPDATE daily_bar SET date='2026-11-11',close=NULL")
                self.assertFalse(module.database_coverage(manifest)[0]['initialized'])
                with sqlite3.connect(cache/'bars.db') as con:
                    con.execute('UPDATE daily_bar SET close=1')
                self.assertTrue(module.database_coverage(manifest)[0]['initialized'])

    def test_retry_rebuilds_transitive_dependants(self):
        from data.research_pipeline import selected_steps
        specs=[{'id':'prices'}, {'id':'signal','depends':['prices']},
               {'id':'report','depends':['signal']}, {'id':'independent'}]
        self.assertEqual([s['id'] for s in selected_steps(specs,['prices'])],['prices','signal','report'])

    def test_forward_attribution_uses_registration_snapshot_only(self):
        from data import build_factor_pitch_db as module
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'forward.db'
            entries=[{'code':'600000.SH','entry_date':'2026-11-11','factors_snapshot':{'original':1},'fwd':{}},
                     {'code':'600001.SH','entry_date':'2026-11-10','fwd':{}}]
            with patch.object(module,'DB',path),patch('factors.opportunities.pitch_track.load_latest',return_value={'entries':entries}),patch.object(module,'load_pitch_factors_map',return_value={'600000.SH':{'factors':{'future':1}}}) as future:
                self.assertEqual(module.build(),(1,1))
                future.assert_not_called()
            with sqlite3.connect(path) as con:
                self.assertEqual(con.execute('SELECT factor,n_pitch,t1_avg FROM factor_agg').fetchall(),[('original',1,None)])

    def test_missing_monthly_warmup_never_becomes_full_position(self):
        from data.traffic_light import compute_signal, run_state_machine
        series=pd.Series(range(100,111),index=pd.date_range('2020-01-31',periods=11,freq='ME'))
        _,_,ratio=compute_signal(series)
        states,_=run_state_machine(series,ratio)
        self.assertEqual(states,{})

    def test_month_end_signal_is_effective_next_month(self):
        from data.traffic_light import build_regime
        series=pd.Series(range(100,124),index=pd.date_range('2020-01-31',periods=24,freq='ME'))
        with patch('data.traffic_light.load_monthly',return_value=series):
            regime=build_regime()
        self.assertEqual(regime['2021-01']['signal_month'],'2020-12')
        self.assertNotIn('2020-12',regime)

    def test_unified_macro_uses_dates_not_values_as_period(self):
        from data import build_unified_db as module
        with tempfile.TemporaryDirectory() as tmp:
            cache=Path(tmp);(cache/'policy').mkdir()
            with sqlite3.connect(cache/'macro.db') as con:
                con.execute('CREATE TABLE social_finance(month TEXT,sf_increment REAL)')
                con.execute("INSERT INTO social_finance VALUES('202609',1000)")
                con.execute('CREATE TABLE bond_yield(date TEXT,y10 REAL,y2 REAL)')
                con.execute("INSERT INTO bond_yield VALUES('2026-11-10',1.8,1.3)")
            with sqlite3.connect(cache/'policy/epu.db') as con:
                con.execute('CREATE TABLE epu_monthly(month TEXT,epu REAL)')
                con.execute("INSERT INTO epu_monthly VALUES('2026-10',100)")
            con=sqlite3.connect(':memory:')
            with patch.object(module,'CACHE',cache):
                self.assertEqual(module.build_macro_series(con),4)
            self.assertEqual(con.execute("SELECT period,value FROM macro_series WHERE series='social_finance'").fetchone(),('202609',1000))
            con.close()

    def test_tushare_live_permission_error_does_not_switch_supplier(self):
        from deck import live_api
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp)/'state.json').write_text('{}')
            cfg={'runtime_dir':tmp,'market':{'provider':'tushare','max_quote_age_seconds':30}}
            state={'market':{'error':'rt_k permission denied'},'quotes':[]}
            with patch('paperlive.config.load_config',return_value=cfg),patch('paperlive.runtime.public_state',return_value=state),patch.object(live_api,'_fetch_realtime') as fetch:
                result=live_api.live_realtime()
            self.assertFalse(result['ok'])
            self.assertEqual(result['source'],'tushare:rt_k')
            fetch.assert_not_called()

    def test_pipeline_rejects_old_and_error_artifacts(self):
        from data import research_pipeline as module
        import time,os
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);path=root/'result.json'
            path.write_text(json.dumps({'ok':False,'error':'source incomplete'}))
            with patch.object(module,'ROOT',root):
                with self.assertRaises(ValueError):module.evidence({'path':'result.json'},time.time()-1,'2026-11-10','test')
                path.write_text(json.dumps({'results':[{'code':'600000.SH'}]}))
                os.utime(path,(1,1))
                with self.assertRaises(ValueError):module.evidence({'path':'result.json'},time.time(),'2026-11-10','test')

    def test_initialization_does_not_fabricate_forward_observations(self):
        from deck import system_live as module
        import hashlib
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);(root/'logs').mkdir();(root/'runtime/research').mkdir(parents=True)
            path=root/'logs/pitch_track_pool_20261111.json'
            path.write_text(json.dumps({'entries':[], 'data_date':'2026-11-11', 'status':'waiting_signal'}))
            proof={'path':str(path),'ok':True,'source':'test-source','algorithm':'test-algorithm',
                   'data_date':'2026-11-11','sha256':hashlib.sha256(path.read_bytes()).hexdigest()}
            docs={'bootstrap-state.json':{'status':'complete'},
                  'bootstrap-manifest.json':{'coverage':{'daily':{'end':'2026-11-11'}}},
                  'pipeline-state.json':{'status':'complete','steps':[{'status':'success','returncode':0,'evidence':[proof]}]}}
            for name,value in docs.items(): (root/'runtime/research'/name).write_text(json.dumps(value))
            with patch.object(module,'BASE',root):
                product=module._api_health()['pitch_track']
                self.assertTrue(product['initialized'])
                self.assertFalse(product['ready'])
                self.assertEqual(product['state'],'pending_history')
                status=module._research_status({'pitch_track':product})
                self.assertTrue(status['initialized'])
                self.assertEqual(status['pending_history_count'],1)
                self.assertEqual(status['uninitialized_count'],0)
                path.write_text(json.dumps({'entries':[], 'data_date':'2026-11-12'}))
                self.assertEqual(module._pipeline_evidence(docs['pipeline-state.json'],path),{})


if __name__=='__main__':
    unittest.main()
