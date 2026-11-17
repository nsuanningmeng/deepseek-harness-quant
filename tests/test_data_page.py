"""Data-page units and research evidence; all databases live in temp fixtures."""
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from deck import live_api, research_views as views


class DataPageTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(); self.addCleanup(temp.cleanup)
        self.root = Path(temp.name); self.cache = self.root/'cache'; self.cache.mkdir()
        self.date = '2026-11-12'
        for target, attr, value in ((views, 'BASE', self.root), (live_api, 'BASE', self.root)):
            p = patch.object(target, attr, value); p.start(); self.addCleanup(p.stop)
        p = patch.object(views, 'locations', return_value=(self.root/'output', self.cache))
        p.start(); self.addCleanup(p.stop)

    def write(self, path, value):
        path.parent.mkdir(parents=True, exist_ok=True); path.write_text(json.dumps(value)); return path

    def database(self):
        path = self.cache/'unified.db'
        with sqlite3.connect(path) as con:
            con.executescript('CREATE TABLE asset_inventory(name TEXT,kind TEXT,size REAL,detail TEXT);'
                              'CREATE TABLE data_lineage(db_name TEXT,consumers TEXT,updated TEXT);'
                              'CREATE TABLE event_series(date TEXT); CREATE TABLE market_style_daily(date TEXT);')
            con.executemany('INSERT INTO asset_inventory VALUES(?,?,?,?)', [
                ('daily_bar','table',17784176,'raw + qfq rows'), ('bars.db','db',4192866304,'source file'),
                ('empty.csv','csv',0,''), ('duplicates','dup_group',0,'')])
            con.executemany('INSERT INTO data_lineage VALUES(?,?,?)', [
                ('bars.db','data/cache.py;tests/test_cache.py;runtime/web/backups/app.py;backtest/engine.py',self.date),
                ('test.db','tests/test_x.py;runtime/ai/backups/x.py',self.date)])
        return path

    def test_inventory_units_are_independent_and_do_not_mutate_database(self):
        path = self.database(); before = path.read_bytes()
        result = views.data_assets()
        self.assertTrue(result['ok']); self.assertEqual(before, path.read_bytes())
        items = {x['name']:x for x in result['inventory']}
        self.assertEqual(items['daily_bar']['row_count'],17784176)
        self.assertIsNone(items['daily_bar']['size_bytes'])
        self.assertEqual(items['bars.db']['size_bytes'],4192866304)
        self.assertIsNone(items['bars.db']['row_count'])
        self.assertEqual(items['empty.csv']['size_bytes'],0)
        self.assertIsNone(items['duplicates']['size_unit'])
        self.assertTrue(all(x['state']=='optional_unused' and x['rows']==0 for x in result['tables'] if x['optional']))
        self.assertEqual(result['lineage'][0]['consumer_paths'],['backtest/engine.py','data/cache.py'])
        self.assertEqual(len(result['lineage']),1)

    def test_units_reject_invalid_values_and_lineage_keeps_real_backup_conversion(self):
        for value in (None, True, -1, float('nan'), 'oops', 1.5):
            self.assertIsNone(views.inventory_units({'kind':'table','size':value})['row_count'])
        actual=views.production_consumers('data/convert_backup_raw.py;backtest/engine.py;runtime/a.py;tests/x.py;/tmp/a.py;../a.py;test_x.py;conftest.py')
        self.assertEqual(actual,['backtest/engine.py','data/convert_backup_raw.py'])

    def history_fixture(self):
        (self.root/'config').mkdir()
        (self.root/'config/research_factors.yaml').write_text('report_dir: custom-reports\nprimary_horizon: 20\n')
        self.write(self.root/'runtime/research/bootstrap-manifest.json',{'coverage':{'daily':{'end':self.date}}})
        supported={'mom_20':{'direction':-1},'new_high_250':{'direction':0}}
        inputs=(self.root/'output',self.cache,{'data_date':self.date},[],supported)
        p=patch.object(views,'factor_inputs',return_value=inputs);p.start();self.addCleanup(p.stop)
        evaluations={name:{str(n):{'holding_sessions':n,'top_fraction':.2,'mean_top_net_return':.012,
                     'mean_top_excess':None,'mean_ic':.03,'n_sections':123,'first_signal_date':'2020-01-01',
                     'last_signal_date':'2026-10-01','state':'evaluated'} for n in (20,120)} for name in supported}
        report={'data_date':self.date,'source':'isolated-current-hs300','evaluations':evaluations}
        path=self.write(self.root/'custom-reports/factor_pool_report.json',report)
        proof={'steps':[{'status':'success','returncode':0,'evidence':[{'path':str(path),'source':report['source'],
               'algorithm':'fixed holding period evaluation','sha256':hashlib.sha256(path.read_bytes()).hexdigest()}]}]}
        self.write(self.root/'runtime/research/pipeline-state.json',proof)
        return path

    def test_history_uses_verified_fixed_horizons_and_disabled_factors_stay_diagnostic(self):
        path=self.history_fixture(); actual=views.factor_history()
        self.assertTrue(actual['ready']);self.assertEqual(actual['source_file'],str(path))
        self.assertEqual(actual['holding_horizons'],[20,120]);self.assertEqual(actual['primary_horizon'],20)
        self.assertEqual(len(actual['rows']),4)
        self.assertTrue(all(x['state']=='diagnostic_only' for x in actual['rows'] if x['direction']==0))
        self.assertTrue(all(x['mean_top_excess'] is None for x in actual['rows']))
        self.assertIn('不是持有120天',actual['icir_note'])
        path.write_text(path.read_text()+' ')
        self.assertFalse(views.factor_history()['ready'])

    def test_stale_report_cannot_populate_current_history(self):
        self.history_fixture()
        self.write(self.root/'runtime/research/bootstrap-manifest.json',{'coverage':{'daily':{'end':'2026-11-13'}}})
        result=views.factor_history();self.assertFalse(result['ready']);self.assertEqual(result['rows'],[])

    def test_api_uses_configured_inventory_path_and_new_historical_contract(self):
        self.database(); self.history_fixture()
        with patch.object(live_api,'live_factor_ranking',return_value={'ranking':[]}), \
                patch.object(live_api,'live_holdings',return_value={}), patch.object(live_api,'_read',return_value={}):
            actual=live_api.live_db_view()
        self.assertTrue(actual['ok']);self.assertTrue(actual['factor_history']['ready'])
        self.assertNotIn('factor_backtest',actual['tables'])
        with patch.object(live_api,'_data_assets_cache',{'ts':0,'data':None}):
            first=live_api.live_data_assets()
            with sqlite3.connect(self.cache/'unified.db') as con:
                con.execute("UPDATE asset_inventory SET size=4 WHERE name='daily_bar'")
            second=live_api.live_data_assets()
        self.assertEqual(first['inventory'][0]['row_count'],17784176)
        self.assertEqual(second['inventory'][0]['row_count'],4)

    def test_page_renders_real_units_horizon_switch_and_missing_rank(self):
        self.database(); self.history_fixture()
        page=(Path(__file__).resolve().parents[1]/'ui_v2/pages/data.html').read_text()
        script=re.findall(r'<script>(.*?)</script>',page,re.S)[0]
        responses={'/api/live/minute':{},'/api/live/data_assets':views.data_assets(),
                   '/api/live/db_view':{'factor_history':views.factor_history(),'ranking':[{
                       'factor':'new_high_250','rank':None,'n_pitch':None,'icir120':None,'composite':None}]}}
        # Execute the actual inline page with fixture APIs; no browser/network needed.
        harness="""const vm=require('vm'),assert=require('assert');
const elements={};const document={getElementById(id){return elements[id]||(elements[id]={innerHTML:'',textContent:''});}};
const responses=RESPONSES;
const LW={sidebar:{render(){}},minuteStatus:{render(){}},render:{_f(v){return v==null?'—':String(v);},color(){return 'gray';}},api:{get(url){return Promise.resolve(responses[url]);}}};
vm.runInNewContext(SCRIPT,{LW,document,setInterval(){}});
setImmediate(()=>{
assert(elements['inventory-box'].innerHTML.includes('17,784,176 行'));
assert(elements['inventory-box'].innerHTML.includes('4.19 GB'));
assert(!elements['inventory-box'].innerHTML.includes('17367'));
assert(elements['tables-box'].innerHTML.includes('当前研究链未采用'));
assert(!elements['lineage-box'].innerHTML.includes('tests/'));
assert(!elements['lineage-box'].innerHTML.includes('runtime/'));
assert(elements['bt-box'].innerHTML.includes('20个交易日'));
assert(elements['bt-box'].innerHTML.includes('停用 · 仅诊断'));
assert(elements['bt-box'].innerHTML.includes('+1.20%'));
assert(!elements['bt-box'].innerHTML.includes('暂无回测数据'));
assert(!elements['rank-box'].innerHTML.includes('null'));
assert(elements['rank-box'].innerHTML.includes('<b>—</b>'));
elements['history-horizon'].onchange.call({value:'120'});
assert(elements['bt-box'].innerHTML.includes('value="120" selected'));
});""".replace('RESPONSES',json.dumps(responses)).replace('SCRIPT',json.dumps(script))
        result=subprocess.run(['node','-e',harness],capture_output=True,text=True)
        self.assertEqual(result.returncode,0,result.stderr)
        self.assertNotIn('rt_k',page)
        self.assertNotIn('top20% → T+1',page)
