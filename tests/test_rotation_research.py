"""Real input boundaries for global ETF and size-style research."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

import numpy as np
import pandas as pd

from autotrade.providers import Records
from data import global_rotation as rotation, style_diagnostic as style
from strategy.style_timing import small_large_strength


class RotationResearchTests(unittest.TestCase):
    def test_momentum_uses_observed_log_slope_and_never_fills_gaps(self):
        prices=np.exp(np.arange(25)*.002)
        ann,r2=rotation.momentum(prices)
        self.assertAlmostEqual(ann,.002*252)
        self.assertAlmostEqual(r2,1.)
        self.assertEqual(rotation.momentum(np.ones(25)),(0.,0.))
        prices[10]=np.nan
        self.assertIsNone(rotation.momentum(prices))
        self.assertIsNone(rotation.momentum([1]*24))

    def test_global_build_preserves_core_and_uses_real_adjustment_factors(self):
        with TemporaryDirectory() as tmp:
            root=Path(tmp);sessions=pd.bdate_range('2026-08-01',periods=61).strftime('%Y-%m-%d').tolist()
            target=sessions[-1];core={'runtime_dir':str(root),'cache_dir':str(root),'request_timeout':5}
            cfg=deepcopy(rotation.load_config())
            cfg.update(cache_database=str(root/'global.db'),identity_file=str(root/'identities.json'),output_file=str(root/'output.json'),history_start=sessions[0])
            cfg['assets']=[{'symbol':'510300.SH','name':'代理1','identity_contains':'代理1','category':'测试','core_cache':True},
                           {'symbol':'513520.SH','name':'代理2','identity_contains':'代理2','category':'测试','core_cache':False}]
            cfg['indices']=[{'symbol':'000300.SH','name':'测试指数'}]
            with sqlite3.connect(root/'bars.db') as con:
                con.execute('CREATE TABLE daily_bar(code TEXT,date TEXT,close REAL,adjust TEXT,source TEXT)')
                con.executemany('INSERT INTO daily_bar VALUES(?,?,?,?,?)',[(code,d,10+i*.01,adjust,'tushare')
                    for code,adjust in [('510300.SH','qfq'),('000300.SH','none')] for i,d in enumerate(sessions)])
            before=hashlib.sha256((root/'bars.db').read_bytes()).hexdigest()
            class Client:
                def query(self,endpoint,fields='',**params):
                    if endpoint=='fund_basic':
                        rows=[{'ts_code':a['symbol'],'name':a['name'],'market':'E','list_date':'20100101'} for a in cfg['assets']]
                    elif endpoint=='fund_daily':
                        rows=[{'ts_code':'513520.SH','trade_date':d.replace('-',''),'close':10.} for d in sessions]
                    elif endpoint=='fund_adj':
                        rows=[{'ts_code':'513520.SH','trade_date':d.replace('-',''),'adj_factor':1+i*.01} for i,d in enumerate(sessions)]
                    else:raise AssertionError(endpoint)
                    return Records(fields.split(','),rows)
            with patch.object(rotation,'inputs',return_value=(core,target,sessions)):
                result=rotation.build(cfg,client=Client(),sleeper=lambda _:None)
            self.assertEqual(hashlib.sha256((root/'bars.db').read_bytes()).hexdigest(),before)
            self.assertEqual(result['data_date'],target)
            self.assertEqual(result['asset_count'],2)
            proxy=next(row for row in result['assets'] if row['symbol']=='513520.SH')
            self.assertAlmostEqual(proxy['ret60'],.6)
            self.assertFalse(result['calibrated'])
            self.assertFalse(result['auto_trade_allowed'])
            self.assertIsNone(result['defensive'])
            # A successful download with a missing trading session is not ready.
            with sqlite3.connect(root/'global.db') as con:
                con.execute('DELETE FROM raw_fund WHERE date=?',(sessions[-20],))
            with patch.object(rotation,'inputs',return_value=(core,target,sessions)):
                with self.assertRaisesRegex(ValueError,'近期交易日缺口'):
                    rotation.build(cfg,client=Client(),sleeper=lambda _:None)

    def test_global_widget_rejects_old_dates_and_changed_asset_configuration(self):
        with TemporaryDirectory() as tmp:
            cfg=deepcopy(rotation.load_config());cfg['output_file']=str(Path(tmp)/'result.json')
            result={'ready':True,'data_date':'2026-11-11','assets':[{'symbol':a['symbol']} for a in cfg['assets']],
                    'configuration_fingerprint':hashlib.sha256(json.dumps(cfg,sort_keys=True).encode()).hexdigest()}
            Path(cfg['output_file']).write_text(json.dumps(result))
            with patch.object(rotation,'load_config',return_value=cfg),patch.object(rotation,'inputs',return_value=({},'2026-11-11',[])):
                self.assertTrue(rotation.widget()['ready'])
                cfg['momentum_window']=30
                with self.assertRaises(ValueError):rotation.widget()
            cfg['momentum_window']=25
            with patch.object(rotation,'load_config',return_value=cfg),patch.object(rotation,'inputs',return_value=({},'2026-11-12',[])):
                with self.assertRaises(ValueError):rotation.widget()

    def test_style_has_no_future_price_or_missing_price_substitution(self):
        cfg=deepcopy(style.load_config())
        cfg.update(minimum_members=4,minimum_group_members=1,minimum_universe_coverage=1.)
        dates=pd.bdate_range('2026-01-01',periods=85).strftime('%Y-%m-%d')
        panel=pd.DataFrame({f'{i:06}.SH':np.exp(np.arange(85)*(5-i)*.001) for i in range(1,5)},index=dates)
        values=pd.Series({f'{i:06}.SH':i for i in range(1,5)})
        asof=dates[79];first=style.classify(panel,values,cfg,asof,4)
        self.assertEqual(style.classify(panel.astype(object),values,cfg,asof,4),first)
        panel.loc[dates[80]:]=1e9
        self.assertEqual(style.classify(panel,values,cfg,asof,4),first)
        self.assertGreater(first['strength'],0)
        panel.loc[dates[10],'000002.SH']=np.nan
        with self.assertRaisesRegex(ValueError,'覆盖不足'):style.classify(panel,values,cfg,asof,4)

    def test_style_missing_or_overlapping_groups_are_unknown_not_neutral(self):
        closes=pd.DataFrame({f'{i:06}.SH':np.exp(np.arange(80)*.001*i) for i in range(1,5)})
        self.assertTrue(small_large_strength(closes,{},minimum_members=4,minimum_group=1).isna().all())
        same={code:10 for code in closes}
        self.assertTrue(small_large_strength(closes,same,minimum_members=4,minimum_group=1).isna().all())

    def test_style_public_view_binds_config_date_universe_and_no_trading_weights(self):
        with TemporaryDirectory() as tmp:
            root=Path(tmp);cfg=deepcopy(style.load_config());cfg['output_file']=str(root/'result.json')
            (root/'bootstrap-manifest.json').write_text(json.dumps({'coverage':{'daily':{'end':'2026-11-11'}}}))
            result={'ready':True,'data_date':'2026-11-11','kind':'size_style_diagnostic','strength':.1,
                    'configuration_fingerprint':style.configuration_fingerprint(cfg),
                    'research_scope':{'universe_fingerprint':'actual-pool'},'weights':{},
                    'weights_enabled':False,'calibrated':False,'auto_trade_allowed':False}
            path=Path(cfg['output_file']);path.write_text(json.dumps(result))
            with patch.object(style,'load_config',return_value=cfg),patch('data.research_bootstrap.load_config',return_value={'runtime_dir':str(root)}),patch.object(style.context,'scope_metadata',return_value={'universe_fingerprint':'actual-pool'}):
                self.assertTrue(style.public_state()['ready'])
                cfg['smooth_window']=10
                self.assertFalse(style.public_state()['ready'])
                cfg['smooth_window']=20
                result['weights']={'boost':['value']};path.write_text(json.dumps(result))
                self.assertFalse(style.public_state()['ready'])
                result['weights']={};result['research_scope']['universe_fingerprint']='other-pool';path.write_text(json.dumps(result))
                self.assertFalse(style.public_state()['ready'])

    def test_new_pipeline_steps_invalidate_timing_and_report(self):
        import yaml
        from data.research_pipeline import ROOT,selected_steps
        cfg=yaml.safe_load((ROOT/'config/research_pipeline.yaml.example').read_text())
        selected={row['id'] for row in selected_steps(cfg['steps'],['style_diagnostic','global_rotation'])}
        self.assertTrue({'style_diagnostic','global_rotation','timing','market_kb','daily_report'}<=selected)
        self.assertNotIn('calibration',selected)


if __name__=='__main__':unittest.main()
