"""Synthetic, isolated research data: resume, source units, splits and safety."""
from datetime import datetime, timedelta
import json
from pathlib import Path
import sqlite3
import tempfile
import threading
import time
import unittest
import yaml

from autotrade.providers import Records, TushareRequestError
from data.research_bootstrap import ResearchBootstrap, ResearchSourceBlocked, load_config, TZ, a_share


class Client:
    def __init__(self):
        self.calls=[]
        self.fail=None
        self.symbols=[f'600{i:03}.SH' for i in range(300)]

    def query(self,endpoint,fields='',**params):
        self.calls.append((endpoint,params))
        if endpoint==self.fail:
            raise ValueError('secret-must-not-appear')
        rows=[]
        if endpoint=='trade_cal':
            first=datetime.strptime(params['start_date'],'%Y%m%d').date()
            last=datetime.strptime(params['end_date'],'%Y%m%d').date()
            while first<=last:
                rows.append({'exchange':'SSE','cal_date':first.strftime('%Y%m%d'),'is_open':'1'})
                first+=timedelta(days=1)
        elif endpoint=='stock_basic':
            if params['exchange']=='SSE' and params['list_status']=='L':
                rows=[{'ts_code':code,'name':'合成测试证券','industry':'合成行业','list_date':'20100101','delist_date':'','list_status':'L','market':'主板','exchange':'SSE'} for code in self.symbols]
        elif endpoint=='index_weight':
            rows=[{'index_code':'000300.SH','con_code':code,'trade_date':'20261030','weight':0.3333} for code in self.symbols]
        elif endpoint in ('daily','adj_factor','daily_basic'):
            for code in self.symbols:
                split=params['trade_date']=='20261111'
                row={'ts_code':code,'trade_date':params['trade_date'],'open':50 if split else 100,'high':51 if split else 101,
                     'low':49 if split else 99,'close':50 if split else 100,'pre_close':50 if split else 99,'pct_chg':0,
                     'vol':100,'amount':1000,'adj_factor':2 if split else 1,'turnover_rate':1,'circ_mv':100000,'pe_ttm':15,'pb':2}
                rows.append(row)
        elif endpoint=='stock_st':
            raise ValueError('permission denied secret-must-not-appear')
        else:
            raise AssertionError('unexpected endpoint '+endpoint)
        columns=fields.split(',') if fields else list(rows[0]) if rows else []
        return Records(columns,[{key:row.get(key) for key in columns} for row in rows])


class BootstrapTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.cfg=load_config()
        self.cfg.update({'cache_dir':str(Path(self.temp.name)/'cache'),'runtime_dir':str(Path(self.temp.name)/'runtime'),
                         'finance_parquet_dir':str(Path(self.temp.name)/'financials'),'daily_start':'2026-11-10',
                         'end':'2026-11-11','request_attempts':1,'min_daily_rows':300,'historical_st':'optional'})
        self.client=Client()

    def boot(self):
        return ResearchBootstrap(self.cfg,client=self.client,now=lambda:datetime(2026,11,11,19,tzinfo=TZ),sleeper=lambda _:None)

    def test_raw_and_qfq_split_preserve_units_and_turnover(self):
        boot=self.boot();state=boot.run(('daily',))
        con=sqlite3.connect(Path(self.cfg['cache_dir'])/'bars.db')
        rows=con.execute("SELECT date,close,preclose,volume,amount,turn,is_st FROM daily_bar WHERE code='600000.SH' AND adjust='qfq' ORDER BY date").fetchall()
        self.assertEqual([row[1] for row in rows],[50,50])
        self.assertEqual(rows[1][2],50)
        self.assertEqual(rows[1][3:6],(100,1000,1))
        self.assertIsNone(rows[1][6])
        self.assertEqual(con.execute("SELECT COUNT(*) FROM daily_bar WHERE adjust='none'").fetchone()[0],600)
        con.close()
        con=sqlite3.connect(Path(self.cfg['cache_dir'])/'hist_mv.db')
        self.assertEqual(con.execute('SELECT circ_mv FROM hist_mv LIMIT 1').fetchone()[0],10)
        con.close()
        self.assertEqual(state['status'],'partial_complete')
        self.assertFalse(boot.manifest['coverage']['daily']['historical_st_verified'])

    def test_restart_skips_sealed_daily_requests(self):
        self.boot().run(('daily',))
        count=sum(name in ('daily','daily_basic','adj_factor') for name,_ in self.client.calls)
        self.boot().run(('daily',))
        self.assertEqual(sum(name in ('daily','daily_basic','adj_factor') for name,_ in self.client.calls),count)

    def test_api_failure_never_marks_slice_completed_or_leaks_key(self):
        self.client.fail='daily_basic'
        boot=self.boot()
        with self.assertRaisesRegex(ValueError,'daily_basic'):
            boot.run(('daily',))
        self.assertNotIn('daily:2026-11-10',boot.manifest['completed'])
        self.assertNotIn('secret-must-not-appear',json.dumps(boot.state))
        self.assertEqual(boot.state['status'],'failed')

    def test_config_change_requires_review_of_existing_checkpoints(self):
        self.boot().run(('daily',))
        self.cfg['daily_start']='2026-11-09'
        with self.assertRaisesRegex(ValueError,'configuration changed'):
            self.boot()

    def test_configured_close_cutoff_never_accepts_today_early(self):
        self.cfg.update(end='latest',completed_session_cutoff='17:00')
        clock=[datetime(2026,11,11,16,59,tzinfo=TZ)]
        boot=ResearchBootstrap(self.cfg,client=self.client,now=lambda:clock[0],sleeper=lambda _:None)
        boot.prepare()
        self.assertEqual(boot.end,'2026-11-10')
        clock[0]=clock[0].replace(hour=17,minute=0)
        boot.prepare()
        self.assertEqual(boot.end,'2026-11-11')

    def test_bootstrap_close_time_rejects_intraday_and_malformed_values(self):
        path=Path(self.temp.name)/'clock.yaml'
        for value in ('09:30','14:59','15:00','15:04','24:00','17:60','17:0',1700):
            path.write_text(yaml.safe_dump(dict(self.cfg,completed_session_cutoff=value)))
            with self.subTest(value=value),self.assertRaises(ValueError):load_config(path)
        for value in ('15:05','17:00','23:59'):
            path.write_text(yaml.safe_dump(dict(self.cfg,completed_session_cutoff=value)))
            self.assertEqual(load_config(path)['completed_session_cutoff'],value)

    def test_schedule_change_preserves_preexisting_source_checkpoints(self):
        # A pre-configuration manifest must survive the operational time change.
        self.cfg.pop('completed_session_cutoff',None)
        initial=self.boot();initial.run(('daily',))
        fingerprint=initial.manifest['config_fingerprint']
        count=len(self.client.calls)
        self.cfg['completed_session_cutoff']='17:00'
        updated=self.boot();updated.run(('daily',))
        self.assertEqual(updated.manifest['config_fingerprint'],fingerprint)
        self.assertFalse(any(api in ('daily','adj_factor','daily_basic') for api,_ in self.client.calls[count:]))

    def test_nonstandard_old_identity_and_new_chinext_code(self):
        self.client.symbols[-1]='302132.SZ'
        original=self.client.query
        def query(endpoint,fields='',**params):
            reply=original(endpoint,fields,**params)
            if endpoint=='stock_basic':
                rows=reply.to_dict('records')
                rows=[r for r in rows if r['ts_code']!='302132.SZ']
                if params['exchange']=='SSE' and params['list_status']=='D':
                    rows.append(dict(zip(fields.split(','),['T600018.SH','退市旧代码','交通','20000101','20061001','D','主板','SSE'])))
                if params['exchange']=='SZSE' and params['list_status']=='L':
                    rows.append(dict(zip(fields.split(','),['302132.SZ','新证券','制造','20200101','','L','创业板','SZSE'])))
                return Records(fields.split(','),rows)
            return reply
        self.client.query=query
        boot=self.boot();boot.prepare()
        self.assertIn('302132.SZ',boot.universe)
        self.assertTrue(a_share('302132.SZ'))
        self.assertEqual(boot.manifest['coverage']['stock_basic']['excluded_metadata'][0]['code'],'T600018.SH')

    def test_bj_missing_factors_cannot_poison_declared_sh_sz_scope(self):
        original=self.client.query
        def query(endpoint,fields='',**params):
            reply=original(endpoint,fields,**params)
            if endpoint=='daily':
                rows=reply.to_dict('records');extra=dict(rows[0]);extra['ts_code']='920047.BJ';rows.append(extra)
                return Records(fields.split(','),rows)
            return reply
        self.client.query=query
        boot=self.boot();boot.run(('daily',))
        self.assertEqual(boot.manifest['coverage']['daily']['scope'],'all_sh_sz_a_shares_including_delisted')
        con=sqlite3.connect(Path(self.cfg['cache_dir'])/'bars.db')
        self.assertEqual(con.execute("SELECT COUNT(*) FROM daily_bar WHERE code LIKE '%.BJ'").fetchone()[0],0);con.close()

    def test_analysis_config_change_does_not_invalidate_data_resume(self):
        self.boot().run(('daily',))
        self.cfg['analysis']['timing']={'max_macro_lag_months':2}
        self.boot().run(('daily',))

    def test_empty_st_is_unknown_not_historically_non_st(self):
        original=self.client.query
        def query(endpoint,fields='',**params):
            if endpoint=='stock_st':
                return Records(fields.split(','),[])
            return original(endpoint,fields,**params)
        self.client.query=query
        boot=self.boot();boot.run(('daily',))
        self.assertFalse(boot.manifest['coverage']['daily']['historical_st_verified'])

    def test_signal_finishes_atomic_day_and_resumes_without_false_success(self):
        boot=self.boot();original=self.client.query
        def query(endpoint,fields='',**params):
            if endpoint=='daily_basic':
                boot.stop_event.set()
            return original(endpoint,fields,**params)
        self.client.query=query
        state=boot.run(('daily',))
        self.assertEqual(state['status'],'interrupted')
        self.assertIn('daily:2026-11-10',boot.manifest['completed'])
        self.assertNotIn('daily:2026-11-11',boot.manifest['completed'])
        con=sqlite3.connect(Path(self.cfg['cache_dir'])/'bars.db')
        self.assertEqual(con.execute("SELECT COUNT(*) FROM daily_bar WHERE date='2026-11-10' AND adjust='none'").fetchone()[0],300);con.close()
        self.client.query=original
        resumed=self.boot().run(('daily',))
        self.assertEqual(resumed['status'],'partial_complete')

    def test_independent_daily_sources_overlap_while_writes_stay_atomic(self):
        original=self.client.query;guard=threading.Lock();active=0;peak=0
        def query(endpoint,fields='',**params):
            nonlocal active,peak
            if endpoint in ('daily','adj_factor','daily_basic','stock_st'):
                with guard:
                    active+=1;peak=max(peak,active)
                try:
                    time.sleep(.03)
                    return original(endpoint,fields,**params)
                finally:
                    with guard:
                        active-=1
            return original(endpoint,fields,**params)
        self.client.query=query
        self.boot().run(('daily',))
        self.assertGreaterEqual(peak,3)
        self.assertLessEqual(peak,12)

    def test_later_day_failure_cannot_skip_or_seal_it(self):
        original=self.client.query
        def query(endpoint,fields='',**params):
            if endpoint=='daily_basic' and params.get('trade_date')=='20261111':
                raise ValueError('second-day source failed')
            return original(endpoint,fields,**params)
        self.client.query=query;boot=self.boot()
        with self.assertRaisesRegex(ValueError,'daily_basic'):
            boot.run(('daily',))
        self.assertIn('daily:2026-11-10',boot.manifest['completed'])
        self.assertNotIn('daily:2026-11-11',boot.manifest['completed'])
        con=sqlite3.connect(Path(self.cfg['cache_dir'])/'bars.db')
        self.assertEqual(con.execute("SELECT COUNT(*) FROM daily_bar WHERE date='2026-11-11'").fetchone()[0],0);con.close()

    def test_future_date_can_arrive_first_but_checkpoints_commit_in_order(self):
        original=self.client.query;arrivals=[]
        def query(endpoint,fields='',**params):
            if endpoint=='daily':
                if params.get('trade_date')=='20261110':
                    time.sleep(.05)
                arrivals.append(params['trade_date'])
            return original(endpoint,fields,**params)
        self.client.query=query;boot=self.boot();commits=[];complete=boot._complete
        def record(key,rows,**facts):
            if key.startswith('daily:'):
                commits.append(key)
            return complete(key,rows,**facts)
        boot._complete=record;boot.run(('daily',))
        self.assertEqual(arrivals[0],'20261111')
        self.assertEqual(commits,['daily:2026-11-10','daily:2026-11-11'])

    def test_request_start_throttle_is_shared_between_threads(self):
        starts=[];guard=threading.Lock()
        class TimedClient:
            def query(self,endpoint,fields='',**params):
                with guard:
                    starts.append(time.monotonic())
                time.sleep(.04)
                return Records(['ok'],[{'ok':1}])
        cfg=dict(self.cfg,min_call_interval_seconds=.025)
        boot=ResearchBootstrap(cfg,client=TimedClient(),now=lambda:datetime(2026,11,11,19,tzinfo=TZ))
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(lambda _:boot._query('daily'),range(4)))
        self.assertEqual(len(starts),4)
        self.assertTrue(all(b-a>=.02 for a,b in zip(starts,starts[1:])))

    def test_running_pipeline_blocks_cache_mutation_and_source_requests(self):
        from autotrade.locking import ProcessLock
        with ProcessLock(Path(self.cfg['runtime_dir'])/'pipeline.lock'):
            with self.assertRaises(RuntimeError):
                self.boot().run(('daily',))
        self.assertFalse(self.client.calls)

    def test_hourly_minute_limit_has_no_short_retries_and_survives_restart(self):
        self.cfg['request_attempts']=3;attempts=[];sleeps=[];clock=[datetime(2026,11,11,19,tzinfo=TZ)]
        class Limited:
            def query(self,endpoint,fields='',**params):
                attempts.append(endpoint)
                raise TushareRequestError(endpoint,'rate_limited')
        boot=ResearchBootstrap(self.cfg,client=Limited(),now=lambda:clock[0],sleeper=sleeps.append)
        with self.assertRaises(ResearchSourceBlocked) as caught:
            boot._query('stk_mins')
        self.assertEqual(caught.exception.category,'rate_limited')
        self.assertEqual(datetime.fromisoformat(caught.exception.retry_at)-clock[0],timedelta(hours=1))
        self.assertEqual(attempts,['stk_mins']);self.assertEqual(sleeps,[])
        boot._save()
        resumed=ResearchBootstrap(self.cfg,client=Limited(),now=lambda:clock[0],sleeper=sleeps.append)
        with self.assertRaises(ResearchSourceBlocked):resumed._query('stk_mins')
        self.assertEqual(attempts,['stk_mins']);self.assertEqual(sleeps,[])

    def test_official_permission_error_is_distinct_and_never_short_retried(self):
        self.cfg['request_attempts']=3;attempts=[]
        class Denied:
            def query(self,endpoint,fields='',**params):
                attempts.append(endpoint);raise TushareRequestError(endpoint,'permission_denied')
        boot=ResearchBootstrap(self.cfg,client=Denied(),now=lambda:datetime(2026,11,11,19,tzinfo=TZ),sleeper=lambda _:None)
        with self.assertRaises(ResearchSourceBlocked) as caught:boot._query('income_vip')
        self.assertEqual(caught.exception.category,'permission_denied');self.assertIsNone(caught.exception.retry_at)
        self.assertEqual(attempts,['income_vip'])


class ExtendedClient(Client):
    def __init__(self):
        super().__init__();self.bad_minute=False;self.empty_minute=False;self.bad_factor=False

    def query(self,endpoint,fields='',**params):
        if endpoint in ('index_daily','fund_daily','fund_adj','stk_mins') or endpoint.endswith('_vip'):
            self.calls.append((endpoint,params))
            if endpoint==self.fail:
                raise ValueError('secret-must-not-appear')
            rows=[]
            if endpoint in ('index_daily','fund_daily','fund_adj'):
                for day,price,factor in [('20261110',100,1),('20261111',50,2)]:
                    if params['start_date']<=day<=params['end_date']:
                        rows.append({'ts_code':params['ts_code'],'trade_date':day,'open':price,'high':price+1,'low':price-1,
                                     'close':price,'pre_close':price,'pct_chg':0,'vol':100,'amount':1000,'adj_factor':None if self.bad_factor else factor})
            elif endpoint=='stk_mins' and not self.empty_minute:
                rows=[{'ts_code':params['ts_code'],'trade_time':'2026-11-10 09:36:00' if self.bad_minute else '2026-11-10 09:35:00',
                       'open':10,'high':11,'low':9,'close':10,'vol':1234,'amount':12340}]
            elif endpoint.endswith('_vip'):
                period=params['period'];year=int(period[:4]);quarter={'0331':1,'0630':2,'0930':3,'1231':4}[period[4:]]
                pub=f'{year}{quarter*3+1:02}30' if quarter<4 else f'{year+1}0330'
                profit=[1,3,6,10][quarter-1]*1e8*(1+(year-2024)*.2)
                row={'ts_code':'600000.SH','end_date':period,'ann_date':pub,'f_ann_date':pub,'report_type':'1','comp_type':'1',
                     'n_income_attr_p':profit,'n_income':profit,'revenue':profit*10,'total_revenue':profit*10,
                     'basic_eps':1,'roe':12,'grossprofit_margin':30,'netprofit_margin':10,'current_ratio':2,'debt_to_assets':40,
                     'n_cashflow_act':profit*1.5,'total_hldr_eqy_exc_min_int':30e8,'depr_fa_coga_dpba':1e7,
                     'total_assets':50e8,'total_liab':20e8,'total_cur_assets':20e8,'fix_assets':10e8,'accounts_receiv':1e8,'inventories':1e8,
                     'oper_cost':profit*7,'sell_exp':1e7,'admin_exp':2e7}
                rows=[row]
                if endpoint=='income_vip':
                    old=dict(row,report_type='5',n_income_attr_p=profit*.8);rows.append(old)
                future=dict(row,ann_date='20261201',f_ann_date='20261201');rows.append(future)
            if endpoint.endswith('_vip') or endpoint in ('fund_adj','fund_daily','index_daily'):
                rows=rows[params.get('offset',0):params.get('offset',0)+params.get('limit',3000)]
            columns=fields.split(',') if fields else list(rows[0]) if rows else []
            return Records(columns,[{key:row.get(key) for key in columns} for row in rows])
        return super().query(endpoint,fields,**params)


class ExtendedTests(BootstrapTests):
    def setUp(self):
        super().setUp();self.client=ExtendedClient()
        self.cfg.update({'finance_start':'2024-01-01','index_start':'2026-11-10','index_symbols':['000300.SH']})
        pool=Path(self.temp.name)/'etf.yaml';pool.write_text('pool:\n  - {code: "510300", name: "合成ETF"}\n')
        self.cfg['etf_pool_file']=str(pool)

    def test_finance_retains_versions_publication_dates_and_real_units(self):
        import pandas as pd
        boot=self.boot();boot.run(('finance',))
        con=sqlite3.connect(Path(self.cfg['cache_dir'])/'finance.db')
        row=con.execute("SELECT net_profit,sq_net_profit,sq_net_yoy,pub_date,roe FROM finance_report WHERE code='600000' AND period='2025-06-30'").fetchone();con.close()
        self.assertAlmostEqual(row[0],3.6);self.assertAlmostEqual(row[1],2.4);self.assertAlmostEqual(row[2],.2)
        self.assertEqual(row[3],'2025-07-30')
        self.assertAlmostEqual(row[4],.12, msg='Tushare ROE 12 percent must be stored as the ratio 0.12')
        con=sqlite3.connect(Path(self.cfg['cache_dir'])/'finance_quality.db')
        row=con.execute("SELECT roe_avg,gp_margin,np_margin,liability_to_asset,cfo_to_np,cfo_to_or FROM quality WHERE period='2025-06-30'").fetchone();con.close()
        self.assertEqual(row,(.12,.3,.1,.4,1.5,.15))
        frame=pd.read_parquet(Path(self.cfg['finance_parquet_dir'])/'income/data/income__ts_code=600000.SH.parquet')
        self.assertEqual(set(frame.report_type),{'1','5'})
        self.assertFalse((frame.f_ann_date>'20261111').any())
        self.assertIn('depr_fa_coga_dpba',frame)
        self.assertEqual(boot.manifest['coverage']['beneish']['symbols_with_two_annual_three_table_reports'],1)
        self.assertEqual(len(boot.manifest['coverage']['beneish']['missing_two_annual_reports']),299)
        self.assertEqual(boot.manifest['coverage']['income']['scope'],'all_market_available_reports')

    def test_roe_conversion_preserves_source_and_is_idempotent_across_view_rebuilds(self):
        cases = {'20240331':24.995, '20240630':0, '20240930':-3.2, '20241231':None, '20250331':150}
        class UnitClient(ExtendedClient):
            def query(self, endpoint, fields='', **params):
                reply = super().query(endpoint, fields, **params)
                if endpoint == 'fina_indicator_vip' and params['period'] in cases:
                    records = reply.to_dict('records')
                    for row in records:
                        row['roe'] = cases[params['period']]
                    return Records(reply.columns, records)
                return reply
        self.client = UnitClient()
        boot = self.boot(); boot.run(('finance',))
        calls = len(self.client.calls)
        boot._finance_views()
        self.assertEqual(len(self.client.calls), calls, 'Rebuilding stored views must not contact a provider')
        for filename, table, code, column in [('finance.db','finance_report','600000','roe'),
                                               ('finance_quality.db','quality','600000.SH','roe_avg')]:
            with sqlite3.connect(Path(self.cfg['cache_dir'])/filename) as con:
                for period, percentage in cases.items():
                    day = period[:4]+'-'+period[4:6]+'-'+period[6:]
                    actual = con.execute(f'SELECT {column} FROM {table} WHERE code=? AND period=?', (code,day)).fetchone()[0]
                    if percentage is None:
                        self.assertIsNone(actual)
                    else:
                        self.assertAlmostEqual(actual, percentage/100)
        with sqlite3.connect(Path(self.cfg['cache_dir'])/'financial_raw.db') as con:
            for period, payload in con.execute("SELECT period,payload FROM financial_raw WHERE api='fina_indicator'"):
                if period in cases:
                    self.assertEqual(json.loads(payload)['roe'], cases[period], 'Official source units remain unmodified')

    def test_pagination_rejects_repeated_pages_instead_of_partial_success(self):
        boot=self.boot()
        boot._query=lambda *a,**k:[{'ts_code':'600000.SH','end_date':'20241231'},{'ts_code':'600001.SH','end_date':'20241231'}]
        with self.assertRaisesRegex(ValueError,'duplicate pagination'):
            boot._pages('income_vip',limit=2,period='20241231')

    def test_etf_qfq_uses_real_fund_factors_index_stays_price_index(self):
        boot=self.boot();boot.run(('assets',))
        con=sqlite3.connect(Path(self.cfg['cache_dir'])/'bars.db')
        prices=con.execute("SELECT close FROM daily_bar WHERE code='510300.SH' AND adjust='qfq' ORDER BY date").fetchall()
        self.assertEqual(prices,[(50,),(50,)])
        self.assertEqual(con.execute("SELECT COUNT(*) FROM daily_bar WHERE code='000300.SH' AND adjust='qfq'").fetchone()[0],0);con.close()
        self.assertEqual(boot.manifest['coverage']['index']['symbols'],1)
        self.assertIn('fund_adj',boot.manifest['coverage']['etf']['adjustment'])

    def test_daily_rerun_excludes_index_and_etf_from_stock_qfq_scope(self):
        self.cfg['index_symbols']=['399006.SZ']
        boot=self.boot();boot.run(('assets',))
        boot=self.boot();boot.run(('daily',))
        self.assertEqual(boot.manifest['coverage']['daily']['rows'],600)
        import pandas as pd
        frame=pd.read_parquet(Path(self.cfg['cache_dir'])/'daily_close.parquet')
        self.assertNotIn('399006.SZ',frame.columns)
        self.assertNotIn('510300.SH',frame.columns)
        self.assertEqual(len(frame.columns),300)

    def test_missing_etf_factor_never_creates_fake_qfq(self):
        self.client.bad_factor=True;boot=self.boot()
        with self.assertRaises(ValueError):
            boot.run(('assets',))
        self.assertNotEqual(boot.state['stages'].get('assets'),'complete')

    def test_5minute_rows_keep_share_yuan_units_and_true_frequency(self):
        boot=self.boot();boot.run(('minute',))
        con=sqlite3.connect(Path(self.cfg['cache_dir'])/'minute.db')
        self.assertEqual(con.execute('SELECT volume,amount FROM minute_5m LIMIT 1').fetchone(),(1234,12340))
        self.assertEqual(con.execute("SELECT value FROM minute_metadata WHERE name='frequency'").fetchone()[0],'5min');con.close()
        self.assertEqual(boot.manifest['coverage']['minute']['scope'],'current_hs300')
        self.assertEqual(boot.manifest['coverage']['minute']['symbols_with_rows'],300)

    def test_bad_minute_timestamp_is_not_sealed(self):
        self.client.bad_minute=True;boot=self.boot()
        with self.assertRaisesRegex(ValueError,'invalid minute'):
            boot.run(('minute',))
        self.assertFalse(any(k.startswith('minute:') for k in boot.manifest['completed']))

    def test_empty_minute_does_not_count_as_complete_without_suspension_evidence(self):
        self.client.empty_minute=True;boot=self.boot()
        with self.assertRaisesRegex(ValueError,'empty minute'):
            boot.run(('minute',))

    def test_new_day_adds_assets_finance_and_minute_despite_previous_completion(self):
        boot=self.boot();boot.run(('daily','finance','assets','minute'),target_date='2026-11-10')
        count=len(self.client.calls)
        boot=self.boot();boot.run(('daily','finance','assets','minute'),target_date='2026-11-11')
        new=self.client.calls[count:]
        self.assertTrue(any(api=='daily' and p.get('trade_date')=='20261111' for api,p in new))
        self.assertFalse(any(api=='daily' and p.get('trade_date')=='20261110' for api,p in new))
        self.assertTrue(any(api=='income_vip' for api,_ in new))
        self.assertTrue(any(api=='fund_daily' and p['start_date']=='20261111' for api,p in new))
        self.assertTrue(any(api=='fund_adj' and p['start_date']=='20261111' for api,p in new))
        self.assertTrue(any(api=='stk_mins' and p['end_date'].startswith('2026-11-11') for api,p in new))
        self.assertEqual(boot.manifest['stage_dates'],{stage:'2026-11-11' for stage in ('daily','finance','assets','minute')})
        self.assertEqual(boot.state['status'],'complete')

    def test_asset_download_full_range_does_not_probe_empty_years(self):
        self.cfg['daily_start']='2019-01-01';self.cfg['index_start']='2010-01-01'
        boot=self.boot();boot.run(('assets',))
        funds=[p for api,p in self.client.calls if api=='fund_daily']
        self.assertEqual(len(funds),2)  # Actual range plus empty offset verification.
        self.assertTrue(all(p['start_date']=='20190101' for p in funds))

    def test_each_progress_phase_uses_its_actual_units(self):
        boot=self.boot();seen=[];save=boot._save
        def record():
            if 'completed_units' in boot.state:
                seen.append((boot.state.get('phase'),boot.state['completed_units'],boot.state.get('total_units')))
            return save()
        boot._save=record;boot.run(('daily','finance','assets','minute'))
        valid=[(phase,count,total) for phase,count,total in seen if total is not None]
        self.assertTrue(valid)
        self.assertTrue(all(0<=count<=total for _,count,total in valid))
        self.assertIn(('building_qfq',300,300),valid)
        self.assertIn(('downloading_index_etf',2,2),valid)

    def test_new_day_failure_invalidates_old_stage_complete_flags(self):
        boot=self.boot();boot.run(('daily','finance','assets','minute'),target_date='2026-11-10')
        self.client.fail='income_vip';boot=self.boot()
        with self.assertRaises(ValueError):
            boot.run(('daily','finance','assets','minute'),target_date='2026-11-11')
        self.assertEqual(boot.manifest['stage_dates']['daily'],'2026-11-11')
        self.assertEqual(boot.manifest['stages']['finance'],'running')
        self.assertEqual(boot.manifest['stages']['assets'],'pending')
        self.assertEqual(boot.state['status'],'failed')

    def test_backwards_cutoff_cannot_mix_different_qfq_anchors(self):
        self.boot().run(('daily',),target_date='2026-11-11')
        with self.assertRaisesRegex(ValueError,'predates accepted'):
            self.boot().run(('daily',),target_date='2026-11-10')

    def test_impossible_source_announcement_is_quarantined_without_date_repair(self):
        original=self.client.query
        def query(endpoint,fields='',**params):
            reply=original(endpoint,fields,**params)
            if endpoint=='fina_indicator_vip' and params['period']=='20260630' and params.get('offset',0)==0:
                rows=reply.to_dict('records');bad=dict(rows[0],ts_code='603400.SH',ann_date='20260422',f_ann_date=None)
                rows.append(bad);return Records(reply.columns,rows)
            return reply
        self.client.query=query;boot=self.boot();boot.run(('finance',))
        con=sqlite3.connect(Path(self.cfg['cache_dir'])/'financial_raw.db')
        row=con.execute("SELECT pub_date,payload FROM financial_quarantine WHERE code='603400.SH'").fetchone()
        self.assertEqual(row[0],'2026-04-22');self.assertEqual(json.loads(row[1])['ann_date'],'20260422')
        self.assertEqual(con.execute("SELECT COUNT(*) FROM financial_raw WHERE code='603400.SH'").fetchone()[0],0);con.close()
        coverage=boot.manifest['coverage']['financial_quarantine']
        self.assertEqual(coverage['rows'],1);self.assertEqual(coverage['current_hs300_affected'],[])
        count=len(self.client.calls);self.boot().run(('finance',));self.assertEqual(len(self.client.calls)-count,1) # calendar only

    def test_all_bad_source_versions_are_not_sealed_as_completed(self):
        original=self.client.query
        def query(endpoint,fields='',**params):
            reply=original(endpoint,fields,**params)
            if endpoint=='fina_indicator_vip' and params['period']=='20260630':
                rows=[dict(row,ann_date='20260422',f_ann_date=None) for row in reply.to_dict('records')]
                return Records(reply.columns,rows)
            return reply
        self.client.query=query;boot=self.boot()
        with self.assertRaisesRegex(ValueError,'no valid already-published'):
            boot.run(('finance',))
        self.assertNotIn('finance:fina_indicator:20260630:2026-11-11',boot.manifest['completed'])
        self.assertEqual(boot.manifest['coverage']['financial_quarantine']['current_hs300_affected'],['600000.SH'])
        self.assertGreater(boot.manifest['coverage']['financial_quarantine']['rows'],0)

    def test_initial_close_only_index_point_is_separate_from_ohlc_bars(self):
        original=self.client.query
        def query(endpoint,fields='',**params):
            reply=original(endpoint,fields,**params)
            if endpoint=='index_daily':
                rows=reply.to_dict('records')
                for row in rows:
                    if row['trade_date']=='20261110':
                        row.update({field:None for field in ('open','high','low','pre_close','vol','amount')})
                return Records(reply.columns,rows)
            return reply
        self.client.query=query;boot=self.boot();boot.run(('assets',))
        con=sqlite3.connect(Path(self.cfg['cache_dir'])/'bars.db')
        baseline=con.execute('SELECT date,close,payload FROM index_baseline').fetchone()
        self.assertEqual(baseline[:2],('2026-11-10',100));self.assertIsNone(json.loads(baseline[2])['open'])
        self.assertEqual(con.execute("SELECT COUNT(*) FROM daily_bar WHERE code='000300.SH'").fetchone()[0],1);con.close()
        self.assertEqual(boot.manifest['coverage']['index_baseline']['rows'],1)

    def test_index_missing_preclose_stays_null_and_return_unknown(self):
        original=self.client.query
        def query(endpoint,fields='',**params):
            reply=original(endpoint,fields,**params)
            if endpoint=='index_daily':
                rows=reply.to_dict('records')
                for row in rows:
                    if row['trade_date']=='20261110':row['pre_close']=None
                return Records(reply.columns,rows)
            return reply
        self.client.query=query;self.boot().run(('assets',))
        con=sqlite3.connect(Path(self.cfg['cache_dir'])/'bars.db')
        self.assertEqual(con.execute("SELECT preclose,pct_chg FROM daily_bar WHERE code='000300.SH' AND date='2026-11-10'").fetchone(),(None,None));con.close()

    def test_later_partial_index_ohlc_is_not_relabelled_as_baseline(self):
        original=self.client.query
        def query(endpoint,fields='',**params):
            reply=original(endpoint,fields,**params)
            if endpoint=='index_daily':
                rows=reply.to_dict('records')
                for row in rows:
                    if row['trade_date']=='20261111':row['open']=None
                return Records(reply.columns,rows)
            return reply
        self.client.query=query;boot=self.boot()
        with self.assertRaisesRegex(ValueError,'missing required source number'):
            boot.run(('assets',))
        self.assertNotEqual(boot.state['stages'].get('assets'),'complete')

    def test_minute_rate_limit_keeps_first_checkpoint_and_resumes_after_hour(self):
        self.cfg['request_attempts']=3;original=self.client.query;attempts=[];limited=[True];clock=[datetime(2026,11,11,19,tzinfo=TZ)]
        def query(endpoint,fields='',**params):
            if endpoint=='stk_mins':
                attempts.append(params['ts_code'])
                if limited[0] and params['ts_code']==self.client.symbols[1]:
                    raise TushareRequestError(endpoint,'rate_limited')
            return original(endpoint,fields,**params)
        self.client.query=query
        def boot():
            return ResearchBootstrap(self.cfg,client=self.client,now=lambda:clock[0],sleeper=lambda _:None)
        first=boot();state=first.run(('minute',))
        self.assertEqual(state['status'],'rate_limited');self.assertEqual(state['completed_units'],1)
        self.assertEqual(state['source_endpoint'],'stk_mins');self.assertEqual(state['source_status']['cooldown_seconds'],3600)
        self.assertNotEqual(first.manifest['stages']['minute'],'complete');self.assertEqual(len(attempts),2)
        self.assertEqual(boot().run(('minute',))['status'],'rate_limited');self.assertEqual(len(attempts),2)
        clock[0]+=timedelta(hours=1);limited[0]=False
        completed=boot();completed.run(('minute',))
        self.assertEqual(completed.manifest['stages']['minute'],'complete')
        self.assertEqual(attempts.count(self.client.symbols[0]),1)
        self.assertNotIn('stk_mins',completed.manifest['source_cooldowns'])

    def test_newest_quarter_empty_is_explicit_pending_and_never_sealed(self):
        original=self.client.query
        def query(endpoint,fields='',**params):
            if endpoint.endswith('_vip') and params['period']=='20260930':
                self.client.calls.append((endpoint,params));return Records([],[])
            return original(endpoint,fields,**params)
        self.client.query=query;boot=self.boot();boot.run(('finance',))
        disclosure=boot.manifest['coverage']['financial_disclosure']
        self.assertEqual(disclosure['status'],'pending_disclosure');self.assertEqual(disclosure['rows'],0)
        self.assertEqual(len(disclosure['pending']),4)
        self.assertTrue(all(row['rows']==0 and row['verified_prior_period']=='20260630' for row in disclosure['pending']))
        self.assertTrue(all('finance:'+api+':20260930:2026-11-11' not in boot.manifest['completed'] for api in ('income','balancesheet','cashflow','fina_indicator')))
        con=sqlite3.connect(Path(self.cfg['cache_dir'])/'financial_raw.db')
        self.assertEqual(con.execute("SELECT COUNT(*) FROM financial_raw WHERE period='20260930'").fetchone()[0],0);con.close()
        con=sqlite3.connect(Path(self.cfg['cache_dir'])/'finance.db')
        self.assertEqual(con.execute('SELECT MAX(period) FROM finance_report').fetchone()[0],'2026-06-30');con.close()
        before=len(self.client.calls);self.boot().run(('finance',))
        self.assertEqual(sum(api.endswith('_vip') and p.get('period')=='20260930' for api,p in self.client.calls[before:]),4)

    def test_pending_quarter_is_queried_again_on_next_date_and_clears_when_available(self):
        self.cfg['end']='latest';clock=[datetime(2026,11,11,19,tzinfo=TZ)];pending=[True];original=self.client.query
        def query(endpoint,fields='',**params):
            if endpoint.endswith('_vip') and params['period']=='20260930' and pending[0]:
                self.client.calls.append((endpoint,params));return Records([],[])
            return original(endpoint,fields,**params)
        self.client.query=query
        def boot():return ResearchBootstrap(self.cfg,client=self.client,now=lambda:clock[0],sleeper=lambda _:None)
        first=boot();first.run(('finance',));self.assertEqual(first.manifest['coverage']['financial_disclosure']['rows'],0)
        count=len(self.client.calls);clock[0]+=timedelta(days=1);pending[0]=False
        second=boot();second.run(('finance',))
        self.assertTrue(any(api=='income_vip' and p.get('period')=='20260930' for api,p in self.client.calls[count:]))
        self.assertEqual(second.manifest['coverage']['financial_disclosure']['status'],'available')
        self.assertEqual(second.manifest['coverage']['financial_disclosure']['pending'],[])
        self.assertGreater(second.manifest['coverage']['financial_disclosure']['rows'],0)

    def test_old_quarter_empty_cannot_be_hidden_even_with_prior_cached_data(self):
        self.cfg['end']='latest';clock=[datetime(2026,11,11,19,tzinfo=TZ)]
        def boot():return ResearchBootstrap(self.cfg,client=self.client,now=lambda:clock[0],sleeper=lambda _:None)
        boot().run(('finance',));original=self.client.query
        def query(endpoint,fields='',**params):
            if endpoint=='income_vip' and params['period']=='20260630':return Records([],[])
            return original(endpoint,fields,**params)
        self.client.query=query;clock[0]+=timedelta(days=1);failed=boot()
        with self.assertRaisesRegex(ValueError,'no reports for 20260630'):failed.run(('finance',))
        self.assertEqual(failed.state['status'],'failed')

    def test_empty_newest_quarter_without_verified_history_is_not_accepted(self):
        self.cfg['finance_start']='2026-09-01';original=self.client.query
        def query(endpoint,fields='',**params):
            if endpoint.endswith('_vip'):return Records([],[])
            return original(endpoint,fields,**params)
        self.client.query=query;boot=self.boot()
        with self.assertRaisesRegex(ValueError,'no reports'):boot.run(('finance',))
        self.assertNotEqual(boot.manifest.get('stages',{}).get('finance'),'complete')

    def test_newest_quarter_permission_error_is_not_pending_disclosure(self):
        original=self.client.query
        def query(endpoint,fields='',**params):
            if endpoint=='income_vip' and params['period']=='20260930':raise TushareRequestError(endpoint,'permission_denied')
            return original(endpoint,fields,**params)
        self.client.query=query;boot=self.boot();state=boot.run(('finance',))
        self.assertEqual(state['status'],'permission_denied')
        self.assertNotEqual(boot.manifest.get('coverage',{}).get('financial_disclosure',{}).get('status'),'pending_disclosure')


if __name__=='__main__':
    unittest.main()
