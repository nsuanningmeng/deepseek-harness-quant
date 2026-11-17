"""Isolated fixed-window slots, receipts and BaoStock completeness tests."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime,timedelta
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
import yaml
from autotrade.providers import Records,TushareRequestError
from autotrade.monitoring import atomic_json
from autotrade.locking import ProcessLock
from data.research_bootstrap import TZ,load_config as core_config
from data.research_minute_service import MinuteService,load_config,_digest,_db_signature,FIELDS,CoreDataBusy,_core_update_busy
from data.research_minute_baostock import BaostockMinuteService,load_config as bs_config,_slots


class Crash(BaseException):pass


class Client:
    def __init__(self):self.calls=[];self.error=None;self.delay=0
    def query(self,api,fields='',**params):
        self.calls.append(params);time.sleep(self.delay)
        if self.error:raise self.error
        rows=[{'ts_code':params['ts_code'],'trade_time':day+' 09:35:00','open':10,'high':11,'low':9,'close':10,'vol':4800,'amount':48000}
              for day in ('2026-11-10','2026-11-11')]
        return Records(FIELDS,rows)


class MinuteTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.base=Path(self.temp.name)
        self.cache=self.base/'cache';self.core_runtime=self.base/'core';self.cache.mkdir();self.core_runtime.mkdir()
        self.symbols=[f'{i:06}.SZ' for i in range(1,301)];self.clock=[datetime(2026,11,11,20,tzinfo=TZ)]
        self.core=core_config();self.core.update(cache_dir=str(self.cache),runtime_dir=str(self.core_runtime),finance_parquet_dir=str(self.base/'financials'))
        path=self.base/'research.yaml';path.write_text(yaml.safe_dump(self.core))
        self.cfg=load_config();self.cfg.update(research_config=str(path),runtime_dir=str(self.base/'worker'))
        self.client=Client()
        self.manifest={'source':'tushare-official','stages':{'daily':'complete'},'coverage':{
            'daily':{'start':'2019-01-02','end':'2026-11-11'},'minute':{'requested_start':'2026-08-11','requested_end':'2026-11-11'}},
            'completed':{'daily:2026-11-10':{'rows':300},'daily:2026-11-11':{'rows':300},
                         'prepare:stock_basic:2026-11-11':{'effective_date':'2026-10-30'},
                         'minute:000001.SZ:2026-08-11:2026-11-11':{'rows':2,'completed_at':'2026-11-11T19:18:00+08:00'}},
            'source_cooldowns':{'stk_mins':{'retry_at':'2026-11-11T20:43:12+08:00'}}}
        atomic_json(self.core_runtime/'bootstrap-manifest.json',self.manifest)
        atomic_json(self.core_runtime/'bootstrap-state.json',{'status':'rate_limited','unit':'000002.SZ'})
        atomic_json(self.core_runtime/'universe.json',{'codes':self.symbols,'source':'tushare:index_weight','effective_date':'2026-10-30','asof':'2026-11-11T18:00:00+08:00'})
        with sqlite3.connect(self.cache/'bars.db') as con:
            con.execute('CREATE TABLE daily_bar(code TEXT,date TEXT,open REAL,close REAL,high REAL,low REAL,volume REAL,amount REAL,adjust TEXT,source TEXT)')
            con.executemany('INSERT INTO daily_bar VALUES(?,?,?,?,?,?,?,?,?,?)',[(c,d,10,10,11,9,48,48,'none','tushare') for c in self.symbols for d in ('2026-11-10','2026-11-11')])
        with sqlite3.connect(self.cache/'minute.db') as con:
            con.execute('CREATE TABLE minute_5m(code TEXT,trade_time TEXT,open REAL,high REAL,low REAL,close REAL,volume REAL,amount REAL,source TEXT,PRIMARY KEY(code,trade_time))')
            con.execute('CREATE TABLE minute_metadata(name TEXT PRIMARY KEY,value TEXT)')
            con.executemany('INSERT INTO minute_metadata VALUES(?,?)',[('frequency','5min'),('volume_unit','shares'),('amount_unit','yuan'),('source','tushare:stk_mins')])
            con.executemany('INSERT INTO minute_5m VALUES(?,?,?,?,?,?,?,?,?)',[('000001.SZ',d+' 09:35:00',10,11,9,10,4800,48000,'tushare:stk_mins:5min') for d in ('2026-11-10','2026-11-11')])

    def worker(self):return MinuteService(self.cfg,now=lambda:self.clock[0],client=self.client)
    def due(self):self.clock[0]=datetime(2026,11,11,21,tzinfo=TZ)

    def test_exact_legacy_checkpoint_and_cooldown_are_adopted_without_core_writes(self):
        before={p.name:p.read_bytes() for p in self.core_runtime.glob('*.json')};job=self.worker().cycle()
        self.assertEqual(job['completed_symbols'],1);self.assertEqual(job['rows'],2);self.assertFalse(self.client.calls)
        self.assertEqual(job['next_request_at'],'2026-11-11T20:43:12+08:00')
        self.assertEqual(before,{p.name:p.read_bytes() for p in self.core_runtime.glob('*.json')})

    def test_success_restart_and_sleep_never_burst_or_move_window(self):
        self.due();first=self.worker().cycle();self.assertEqual(len(self.client.calls),1);self.assertEqual(first['completed_symbols'],2)
        self.worker().cycle();self.assertEqual(len(self.client.calls),1)
        self.clock[0]+=timedelta(days=3);job=self.worker().cycle()
        self.assertEqual(len(self.client.calls),2);self.assertEqual(job['completed_symbols'],3)
        self.assertEqual((job['start_date'],job['target_date']),('2026-08-11','2026-11-11'))
        self.assertGreaterEqual(datetime.fromisoformat(job['next_request_at']),self.clock[0]+timedelta(hours=1))

    def test_network_error_never_short_retries_or_leaks_secret(self):
        self.due();self.client.error=ValueError('private-token-never-print');job=self.worker().cycle()
        self.assertEqual(len(self.client.calls),1);self.assertEqual(job['completed_symbols'],1)
        self.assertNotIn('private-token',json.dumps(job));self.worker().cycle();self.assertEqual(len(self.client.calls),1)

    def test_rate_limit_retains_at_least_hour_and_one_request(self):
        self.due();self.client.error=TushareRequestError('stk_mins','rate_limited');job=self.worker().cycle()
        self.assertEqual(job['status'],'rate_limited');self.assertEqual(len(self.client.calls),1)
        self.assertGreaterEqual(datetime.fromisoformat(job['next_request_at']),self.clock[0]+timedelta(hours=1))

    def test_two_instances_share_atomic_slot(self):
        self.due();self.client.delay=.05
        with ThreadPoolExecutor(max_workers=8) as pool:list(pool.map(lambda _:self.worker().cycle(),range(8)))
        self.assertEqual(len(self.client.calls),1)

    def test_crash_after_reservation_does_not_reissue_on_restart(self):
        self.due();worker=self.worker()
        with patch.object(worker,'_request',side_effect=Crash):
            with self.assertRaises(Crash):worker.cycle()
        self.worker().cycle();self.assertFalse(self.client.calls)

    def test_committed_receipt_recovers_when_json_publication_crashes(self):
        self.due();worker=self.worker();save=worker._save
        def crash(job):
            if job.get('last_attempt',{}).get('status')=='committed':raise Crash()
            return save(job)
        with patch.object(worker,'_save',crash):
            with self.assertRaises(Crash):worker.cycle()
        recovered=self.worker().cycle();self.assertEqual(recovered['completed_symbols'],2);self.assertEqual(len(self.client.calls),1)
        self.assertEqual(recovered['last_attempt']['status'],'recovered_from_atomic_receipt')

    def test_metadata_conflict_is_not_relabelled(self):
        with sqlite3.connect(self.cache/'minute.db') as con:con.execute("UPDATE minute_metadata SET value='lots' WHERE name='volume_unit'")
        self.assertEqual(self.worker().cycle()['status'],'failed');self.assertFalse(self.client.calls)
        with sqlite3.connect(self.cache/'minute.db') as con:self.assertEqual(con.execute("SELECT value FROM minute_metadata WHERE name='volume_unit'").fetchone()[0],'lots')

    def test_extra_cache_row_does_not_receive_false_receipt(self):
        self.due()
        with sqlite3.connect(self.cache/'minute.db') as con:
            con.execute("INSERT INTO minute_5m VALUES('000002.SZ','2026-11-10 09:40:00',10,11,9,10,0,0,'tushare:stk_mins:5min')")
        job=self.worker().cycle();self.assertEqual(job['completed_symbols'],1)
        with sqlite3.connect(self.cache/'minute.db') as con:self.assertEqual(con.execute("SELECT COUNT(*) FROM minute_download_receipt WHERE code='000002.SZ'").fetchone()[0],0)

    def test_core_write_retains_receipts_without_loading_or_requesting(self):
        worker=self.worker();worker.cycle();before=worker.path.read_bytes()
        for name in ('update.lock','bootstrap.lock','pipeline.lock'):
            with self.subTest(lock=name),ProcessLock(self.core_runtime/name), \
                 patch.object(worker,'_load') as load,patch.object(worker,'_request') as request:
                result=worker.cycle()
                self.assertEqual(result['phase'],'waiting_core_update')
                load.assert_not_called();request.assert_not_called()
            self.assertEqual(worker.path.read_bytes(),before)

    def test_core_read_guard_excludes_a_writer_after_the_initial_busy_check(self):
        worker=self.worker()
        with worker.core_read_guard():
            for name in ('bootstrap.lock','pipeline.lock'):
                with self.subTest(lock=name),self.assertRaises(RuntimeError):
                    with ProcessLock(self.core_runtime/name):
                        self.fail('a core writer entered during minute verification')

    @unittest.skipIf(os.name=='nt','Windows conservatively retains exclusive locking')
    def test_two_independent_verifiers_share_guard_without_reporting_core_update(self):
        barrier=threading.Barrier(2)
        def verify(_):
            worker=self.worker()
            with worker.core_read_guard():
                barrier.wait(timeout=3)
                self.assertFalse(_core_update_busy(worker.core))
                for name in ('bootstrap.lock','pipeline.lock'):
                    with self.assertRaises(RuntimeError):
                        with ProcessLock(self.core_runtime/name):
                            self.fail('writer entered during concurrent verification')
                barrier.wait(timeout=3)
            return True
        with ThreadPoolExecutor(max_workers=2) as pool:
            self.assertEqual(list(pool.map(verify,range(2))),[True,True])
        with ProcessLock(self.core_runtime/'bootstrap.lock'):
            self.assertTrue(_core_update_busy(self.core))

    def test_guard_rechecks_update_after_initial_busy_check(self):
        worker=self.worker()
        with ProcessLock(self.core_runtime/'update.lock'), \
             patch('data.research_minute_service._core_update_busy',return_value=False):
            with self.assertRaises(CoreDataBusy):
                with worker.core_read_guard():
                    self.fail('update arriving after initial check must block reader')
        for name in ('bootstrap.lock','pipeline.lock'):
            with self.subTest(lock=name),ProcessLock(self.core_runtime/name), \
                 patch('data.research_minute_service._core_update_busy',return_value=False):
                with self.assertRaises(CoreDataBusy):
                    with worker.core_read_guard():
                        self.fail('writer arriving after initial check must block reader')
            # In particular, failing to acquire pipeline must release the
            # bootstrap shared lock already acquired by ExitStack.
            with ProcessLock(self.core_runtime/'bootstrap.lock'):
                pass

    def test_readonly_verifier_busy_snapshot_is_not_cached_as_failure(self):
        from data import minute_readiness
        worker=self.worker();key=('busy-core-regression',)
        minute_readiness._ERRORS.pop(key,None);minute_readiness._VERIFYING.add(key)
        with ProcessLock(self.core_runtime/'bootstrap.lock'),patch.object(minute_readiness,'_verify_window') as verify:
            minute_readiness._verify(None,worker,{},key,None,None,None,None)
        verify.assert_not_called();self.assertNotIn(key,minute_readiness._ERRORS)
        self.assertNotIn(key,minute_readiness._VERIFYING)


class BaostockTests(MinuteTests):
    def bs_worker(self):
        cfg=bs_config();cfg.update(research_config=self.cfg['research_config'],runtime_dir=str(self.base/'bsworker'))
        return BaostockMinuteService(cfg,now=lambda:self.clock[0],client=object())
    def records(self,code):
        return [(code,day+' '+clock+':00',10.,11.,9.,10.,100.,1000.,'baostock:5min:raw') for day in ('2026-11-10','2026-11-11') for clock in sorted(_slots())]

    def test_exact48_crosscheck_and_high_low_warning_preserve_source_prices(self):
        worker=self.bs_worker();job=worker._new_job();rows=self.records('000001.SZ')
        worker._validate_coverage(job,'000001.SZ',rows)
        changed=[tuple([*row[:3],10.5,9.5,*row[5:]]) for row in rows]
        worker._validate_coverage(job,'000001.SZ',changed)
        self.assertEqual(job['quality_checks']['000001.SZ']['high_low_cross_source_difference_days'],2)
        self.assertEqual(changed[0][3:5],(10.5,9.5))

    def test_one_missing_intraday_slot_is_rejected_even_with_daily_date(self):
        worker=self.bs_worker();job=worker._new_job()
        with self.assertRaisesRegex(ValueError,'48'):worker._validate_coverage(job,'000001.SZ',self.records('000001.SZ')[1:])

    def test_wrong_share_or_yuan_units_rejected(self):
        worker=self.bs_worker();job=worker._new_job();rows=self.records('000001.SZ')
        wrong=[tuple([*row[:6],row[6]/100,row[7],row[8]]) for row in rows]
        with self.assertRaisesRegex(ValueError,'volume'):worker._validate_coverage(job,'000001.SZ',wrong)
        wrong=[tuple([*row[:7],row[7]/1000,row[8]]) for row in rows]
        with self.assertRaisesRegex(ValueError,'amount'):worker._validate_coverage(job,'000001.SZ',wrong)

    def test_baostock_staging_does_not_claim_tushare_checkpoint(self):
        worker=self.bs_worker();job=worker._new_job()
        self.assertEqual(job['completed'],{});self.assertEqual(job['source'],worker.source_id)
        self.assertEqual(worker.database_path.name,'minute_baostock.db')
        with sqlite3.connect(self.cache/'minute.db') as con:self.assertEqual(con.execute('SELECT COUNT(*) FROM minute_5m').fetchone()[0],2)

    def test_production_sdk_request_is_bounded_subprocess(self):
        from types import SimpleNamespace
        worker=self.bs_worker();worker.client=None;job=worker._new_job()
        with patch.object(worker,'_repair_if_needed',return_value=[]),patch('data.research_minute_baostock.subprocess.run',return_value=SimpleNamespace(returncode=0,stdout=b'{"ok":true,"rows":[]}')) as run:
            self.assertEqual(worker._request(job,'000001.SZ'),[])
        self.assertEqual(run.call_args.kwargs['timeout'],45);self.assertTrue(run.call_args.kwargs['start_new_session'])

    def test_sdk_always_logs_out_after_malformed_rows(self):
        from types import SimpleNamespace
        from unittest.mock import Mock
        sdk=Mock();sdk.login.return_value=SimpleNamespace(error_code='0')
        response=sdk.query_history_k_data_plus.return_value
        response.error_code='0';response.next.return_value=True
        response.fields=['code','adjustflag','time'];response.get_row_data.return_value=['sz.000001','3','bad']
        with self.assertRaises(ValueError):BaostockMinuteService._sdk_request({'start_date':'2026-08-11','target_date':'2026-11-11'},'000001.SZ',sdk)
        sdk.logout.assert_called_once();sdk.query_history_k_data_plus.assert_called_once()

    def test_timeout_never_unbounds_the_sdk_process(self):
        import subprocess
        worker=self.bs_worker();worker.client=None;job=worker._new_job()
        with patch('data.research_minute_baostock.subprocess.run',side_effect=subprocess.TimeoutExpired('public-minute',45)):
            with self.assertRaises(subprocess.TimeoutExpired):worker._request(job,'000001.SZ')

    def test_next_core_day_reuses_proven_prefix_and_requests_only_tail(self):
        from types import SimpleNamespace
        worker=self.bs_worker();previous=worker._new_job();code='000001.SZ';rows=self.records(code)
        worker._validate_coverage(previous,code,rows);worker._receipt(previous,code,rows,self.clock[0].isoformat())
        atomic_json(worker.runtime/'history'/(previous['job_id']+'.json'),previous)
        job={**previous,'target_date':'2026-11-12','predecessor_job_id':previous['job_id']}
        worker.client=None
        with patch('data.research_minute_baostock.subprocess.run',return_value=SimpleNamespace(returncode=0,stdout=b'{"ok":true,"rows":[]}')) as run:
            result=worker._request(job,code)
        self.assertEqual(len(result),96)
        self.assertEqual(run.call_args.args[0][-2:],['2026-11-12','2026-11-12'])
        # An unchanged window/universe addition reuses only a verified receipt;
        # it cannot turn cached rows into an unproven new HTTP success.
        job['target_date']='2026-11-11'
        with patch('data.research_minute_baostock.subprocess.run') as run:
            self.assertEqual(len(worker._request(job,code)),96);run.assert_not_called()

    def test_incremental_prefix_content_change_is_rejected(self):
        worker=self.bs_worker();previous=worker._new_job();code='000001.SZ';rows=self.records(code)
        worker._receipt(previous,code,rows,self.clock[0].isoformat())
        atomic_json(worker.runtime/'history'/(previous['job_id']+'.json'),previous)
        with sqlite3.connect(worker.database_path) as con:con.execute("UPDATE minute_5m SET amount=amount+100 WHERE code=?",(code,))
        with self.assertRaises(ValueError):worker._verified_prefix({**previous,'target_date':'2026-11-12','predecessor_job_id':previous['job_id']},code)

    def test_core_date_moves_only_after_full_predecessor_revalidation(self):
        worker=self.bs_worker();previous=worker._new_job()
        previous['completed']={code:{'rows':96} for code in self.symbols}
        previous['next_request_at']='2026-11-12T20:00:00+08:00';worker._save(previous)
        self.manifest['coverage']['daily']['end']='2026-11-12';self.manifest['completed']['daily:2026-11-12']={'rows':300}
        atomic_json(self.core_runtime/'bootstrap-manifest.json',self.manifest)
        with patch.object(worker,'_reconcile') as check:
            new=worker._load()
        check.assert_called_once();self.assertEqual(new['target_date'],'2026-11-12')
        self.assertEqual(new['predecessor_job_id'],previous['job_id']);self.assertEqual(new['completed'],{})
        self.assertEqual(new['next_request_at'],'2026-11-12T20:00:00+08:00')
        self.assertTrue((worker.runtime/'history'/(previous['job_id']+'.json')).exists())

    def test_rollover_first_publication_preserves_predecessor_and_slot(self):
        worker=self.bs_worker();previous=worker._new_job()
        previous['completed']={code:{'rows':96} for code in self.symbols}
        previous['next_request_at']='2026-11-12T20:00:00+08:00';worker._save(previous)
        self.manifest['coverage']['daily']['end']='2026-11-12';self.manifest['completed']['daily:2026-11-12']={'rows':300}
        atomic_json(self.core_runtime/'bootstrap-manifest.json',self.manifest)
        def crash_before_publication(job):
            self.assertEqual(job['predecessor_job_id'],previous['job_id'])
            self.assertEqual(job['next_request_at'],previous['next_request_at'])
            raise Crash()
        with patch.object(worker,'_reconcile'),patch.object(worker,'_save',side_effect=crash_before_publication):
            with self.assertRaises(Crash):worker._load()
        self.assertEqual(json.loads(worker.path.read_text())['job_id'],previous['job_id'])

    def test_weak_preupgrade_job_is_not_publicly_ready(self):
        from data.research_minute_baostock import public_status
        worker=self.bs_worker();job=worker._new_job()
        job['completed']={code:{'rows':96} for code in self.symbols};job['status']='complete';worker._save(job)
        status=public_status(worker.cfg)
        self.assertFalse(status['cache_verified']);self.assertFalse(status['current_date_ready'])

    def test_unified_readiness_deep_validates_rows_and_invalidates_on_daily_change(self):
        from types import SimpleNamespace
        from data import minute_readiness
        from data.research_minute_baostock import public_status,VALIDATION_VERSION
        worker=self.bs_worker();job=worker._new_job();completed={}
        with sqlite3.connect(worker.database_path) as con:
            for code in self.symbols:
                rows=self.records(code);digest=_digest(rows)
                con.executemany('INSERT INTO minute_5m VALUES(?,?,?,?,?,?,?,?,?)',rows)
                con.execute('INSERT INTO minute_download_receipt VALUES(?,?,?,?,?,?,?)',(job['job_id'],code,job['start_date'],job['target_date'],len(rows),digest,self.clock[0].isoformat()))
                completed[code]={'rows':len(rows),'sha256':digest}
        job.update(completed=completed,status='complete',cache_signature=_db_signature(worker.database_path));worker._save(job)
        module=SimpleNamespace(load_config=lambda:worker.cfg,public_status=public_status,BaostockMinuteService=BaostockMinuteService,VALIDATION_VERSION=VALIDATION_VERSION)
        minute_readiness._VERIFIED.clear()
        with patch.object(minute_readiness,'selected_service',return_value=module):
            status=minute_readiness.public_status(wait=True)
            self.assertTrue(status['deep_verified']);self.assertTrue(status['current_date_ready']);self.assertEqual(status['rows'],28800)
            with patch.object(BaostockMinuteService,'_validate_coverage',side_effect=AssertionError('should use unchanged verified cache')):
                self.assertTrue(minute_readiness.public_status()['current_date_ready'])
                if os.name!='nt':
                    # An independent cold reader must not discard this
                    # consumer's complete proof. A real update intent still
                    # blocks readiness, then recovers without revalidating.
                    with self.bs_worker().core_read_guard():
                        concurrent=minute_readiness.public_status()
                        self.assertFalse(concurrent['core_update_in_progress'])
                        self.assertTrue(concurrent['deep_verified'])
                        self.assertTrue(concurrent['current_date_ready'])
                        with ProcessLock(self.core_runtime/'update.lock'):
                            blocked=minute_readiness.public_status()
                            self.assertEqual(blocked['verification_status'],'waiting_core_update')
                            self.assertFalse(blocked['current_date_ready'])
                            with self.assertRaises(CoreDataBusy):
                                with self.bs_worker().core_read_guard():
                                    self.fail('new verifier entered during real update')
                        self.assertTrue(minute_readiness.public_status()['current_date_ready'])
            worker.cfg['repairs']['amount_relative_tolerance']=0
            with patch.object(BaostockMinuteService,'_validate_coverage',side_effect=ValueError('stricter policy requires revalidation')):
                self.assertFalse(minute_readiness.public_status(wait=True)['deep_verified'])
            worker.cfg['repairs']['amount_relative_tolerance']=1e-8
            with sqlite3.connect(self.cache/'bars.db') as con:con.execute("UPDATE daily_bar SET volume=volume+1 WHERE code='000001.SZ'")
            status=minute_readiness.public_status(wait=True);self.assertFalse(status['current_date_ready']);self.assertFalse(status['deep_verified'])

    def sina_rows(self,worker,code,*,difference=0):
        from data.research_minute_baostock import SINA_SOURCE
        raw=[{'day':row[1],**dict(zip(('open','high','low','close'),row[2:6])),'volume':row[6],'amount':1000000.} for row in self.records(code)]
        raw[0]['amount']+=difference
        directory=worker.runtime/'source-repairs';raw_file=code+'-sina-raw-'+_digest(raw)+'.json';atomic_json(directory/raw_file,raw)
        atomic_json(directory/(code+'-sina-repair.json'),{'symbol':code,'source':SINA_SOURCE,'raw_file':raw_file,'raw_sha256':_digest(raw),'days':['2026-11-10','2026-11-11']})
        return [(code,row['day'],row['open'],row['high'],row['low'],row['close'],row['volume'],row['amount'],SINA_SOURCE) for row in raw]

    def test_sina_specific_precision_and_source_evidence_keep_bs_strict(self):
        worker=self.bs_worker();job=worker._new_job();code='000001.SZ'
        with sqlite3.connect(self.cache/'bars.db') as con:con.execute('UPDATE daily_bar SET amount=48000 WHERE code=?',(code,))
        rows=self.sina_rows(worker,code,difference=.3)
        worker._validate_coverage(job,code,rows)
        repair=job['quality_checks'][code]['source_repairs'][0]
        self.assertAlmostEqual(repair['amount_tolerance_yuan'],.48);self.assertFalse(repair['amount_exact_agreement'])
        bs_rows=[tuple([*row[:8],worker.row_source]) for row in rows]
        with self.assertRaisesRegex(ValueError,'amount'):worker._validate_coverage(job,code,bs_rows)
        rows=self.sina_rows(worker,code,difference=.6)
        with self.assertRaisesRegex(ValueError,'amount'):worker._validate_coverage(job,code,rows)

    def test_whole_day_sina_replacement_archives_raw_bs_and_preserves_source(self):
        worker=self.bs_worker();job=worker._new_job();code='000001.SZ';original=self.records(code)
        worker._receipt(job,code,original,self.clock[0].isoformat())
        rows=self.sina_rows(worker,code)
        worker._receipt(job,code,rows,self.clock[0].isoformat())
        with sqlite3.connect(worker.database_path) as con:
            self.assertEqual(con.execute('SELECT COUNT(*) FROM minute_repair_archive').fetchone()[0],2)
            self.assertEqual(con.execute('SELECT DISTINCT source FROM minute_5m').fetchone()[0],'sina:getKLineData:5min:raw')
        self.assertEqual(worker._cached_rows(job,code),rows)

    def test_async_readiness_returns_without_blocking_and_single_flies(self):
        from data import minute_readiness
        # The complete-row case above covers the real verifier; this isolates
        # web dispatch so slow I/O never becomes a synchronous HTTP wait.
        from types import SimpleNamespace
        import threading
        worker=self.bs_worker();job=worker._new_job();job['completed']={c:{'rows':96} for c in self.symbols};worker._save(job)
        from data.research_minute_baostock import public_status,VALIDATION_VERSION
        module=SimpleNamespace(load_config=lambda:worker.cfg,public_status=public_status,BaostockMinuteService=BaostockMinuteService,VALIDATION_VERSION=VALIDATION_VERSION)
        minute_readiness._VERIFIED.clear();minute_readiness._ERRORS.clear();event=threading.Event()
        def slow(*args):event.wait(2)
        with patch.object(minute_readiness,'selected_service',return_value=module),patch.object(minute_readiness,'_verify',side_effect=slow) as verify:
            start=time.monotonic();status=minute_readiness.public_status();minute_readiness.public_status()
            self.assertLess(time.monotonic()-start,.5);self.assertFalse(status['deep_verified']);self.assertEqual(status['verification_status'],'verifying')
            event.set()
            for _ in range(100):
                if verify.call_count:break
                time.sleep(.001)
            self.assertEqual(verify.call_count,1)

    def test_public_readiness_does_not_scan_during_core_update(self):
        from data import minute_readiness
        from types import SimpleNamespace
        from data.research_minute_baostock import public_status,VALIDATION_VERSION
        worker=self.bs_worker();job=worker._new_job();job['completed']={c:{'rows':96} for c in self.symbols};worker._save(job)
        before=worker.path.read_bytes()
        module=SimpleNamespace(load_config=lambda:worker.cfg,public_status=public_status,
                               BaostockMinuteService=BaostockMinuteService,VALIDATION_VERSION=VALIDATION_VERSION)
        with ProcessLock(self.core_runtime/'update.lock'), \
             patch.object(minute_readiness,'selected_service',return_value=module), \
             patch.object(minute_readiness,'_verify') as verify:
            status=minute_readiness.public_status(wait=True)
        verify.assert_not_called();self.assertEqual(status['verification_status'],'waiting_core_update')
        self.assertFalse(status['deep_verified']);self.assertFalse(status['current_date_ready'])
        self.assertEqual(worker.path.read_bytes(),before)

    def test_new_repair_day_does_not_replace_old_day_raw_proof(self):
        from unittest.mock import Mock
        worker=self.bs_worker();job=worker._new_job();code='000001.SZ';self.sina_rows(worker,code)
        first=worker._sina_repairs(job,code,['2026-11-10']);self.assertEqual(len(first),48)
        path=worker.runtime/'source-repairs'/(code+'-2026-11-10-sina-repair.json');before=path.read_bytes()
        legacy=worker.runtime/'source-repairs'/(code+'-sina-repair.json');proof=json.loads(legacy.read_text());proof['days']=['2026-11-10'];atomic_json(legacy,proof)
        raw=worker._raw_evidence(code,proof);next_raw=[r for r in raw if r['day'].startswith('2026-11-11')]
        response=Mock();response.json.return_value=next_raw
        with patch('requests.get',return_value=response) as get:
            second=worker._sina_repairs(job,code,['2026-11-11']);get.assert_called_once()
        self.assertEqual(len(second),48);self.assertEqual(before,path.read_bytes())
        self.assertNotEqual(worker._day_proof(code,'2026-11-10')['raw_sha256'],worker._day_proof(code,'2026-11-11')['raw_sha256'])
        worker._raw_evidence(code,worker._day_proof(code,'2026-11-10'))

    def test_preserved_raw_source_changes_invalidate_evidence_signature(self):
        worker=self.bs_worker();job=worker._new_job();code='000001.SZ';self.sina_rows(worker,code)
        self.assertTrue(worker.evidence_signature(job))
        proof=worker._day_proof(code,'2026-11-10');path=worker.runtime/'source-repairs'/proof['raw_file']
        raw=json.loads(path.read_text());raw[0]['amount']+=1;atomic_json(path,raw)
        with self.assertRaisesRegex(ValueError,'evidence changed'):worker.evidence_signature(job)

    def test_corrupt_public_job_has_no_false_ready_or_exception(self):
        from data.research_minute_baostock import public_status
        worker=self.bs_worker();atomic_json(worker.path,{'symbols':1,'completed':{'000001.SZ':{'rows':'bad'}}})
        status=public_status(worker.cfg)
        self.assertFalse(status['current_date_ready']);self.assertEqual(status['completed_symbols'],0)
        self.assertEqual(status['symbols'],[])

    def test_public_scope_symbols_match_valid_immutable_job(self):
        from data.research_minute_baostock import public_status
        worker=self.bs_worker();job=worker._new_job()
        status=public_status(worker.cfg)
        self.assertEqual(status['symbols'],job['symbols'])
        self.assertEqual(status['job_id'],job['job_id'])
        self.assertEqual(len(status['symbols']),300)

    def test_missing_daily_session_requires_official_suspension_not_only_absent_minute(self):
        worker=self.bs_worker();code='000001.SZ'
        with sqlite3.connect(self.cache/'bars.db') as con:
            con.execute('DELETE FROM daily_bar WHERE code=? AND date=?',(code,'2026-11-10'))
        job=worker._new_job();rows=self.records(code)[48:]
        with self.assertRaisesRegex(ValueError,'official suspension'):
            worker._validate_coverage(job,code,rows)
        records=[{'ts_code':code,'trade_date':'20261110','suspend_type':kind,'suspend_timing':None} for kind in ('S','R')]
        worker._store_suspension_response(code,records,'2026-11-10','2026-11-10',self.clock[0].isoformat())
        worker._validate_coverage(job,code,rows)
        evidence=job['quality_checks'][code]['nontrading_sessions']
        self.assertEqual(len(evidence),1);self.assertTrue(evidence[0]['mixed_suspend_resume'])
        self.assertEqual(evidence[0]['record_types'],['R','S'])
        self.assertEqual(len(rows),48)  # No invented zero-volume bars for the absent day.

    def test_intraday_suspension_does_not_explain_entire_missing_day(self):
        worker=self.bs_worker();code='000001.SZ'
        worker._store_suspension_response(code,[{'ts_code':code,'trade_date':'20261110','suspend_type':'S','suspend_timing':'09:30-10:00'}],
                                           '2026-11-10','2026-11-10',self.clock[0].isoformat())
        with self.assertRaisesRegex(ValueError,'official suspension'):worker._nontrading_proof(code,'2026-11-10')

    def test_missing_day_metadata_fetch_is_cached_and_failures_keep_durable_cooldown(self):
        from unittest.mock import Mock
        worker=self.bs_worker();code='000001.SZ'
        with sqlite3.connect(self.cache/'bars.db') as con:
            con.execute('DELETE FROM daily_bar WHERE code=? AND date=?',(code,'2026-11-10'))
        job=worker._new_job();worker.metadata_client=Mock()
        worker.metadata_client.query.side_effect=TushareRequestError('suspend_d','rate_limited')
        with self.assertRaises(TushareRequestError):worker._ensure_nontrading_evidence(job)
        with self.assertRaisesRegex(ValueError,'retry pending'):worker._ensure_nontrading_evidence(job)
        self.assertEqual(worker.metadata_client.query.call_count,1)
        self.clock[0]+=timedelta(hours=1)
        worker.metadata_client.query.side_effect=None
        worker.metadata_client.query.return_value=Records(('ts_code','trade_date','suspend_timing','suspend_type'),
                [{'ts_code':code,'trade_date':'20261110','suspend_timing':None,'suspend_type':'S'}])
        worker._ensure_nontrading_evidence(job);worker._ensure_nontrading_evidence(job)
        self.assertEqual(worker.metadata_client.query.call_count,2)
        call=worker.metadata_client.query.call_args
        self.assertEqual(call.args,('suspend_d',));self.assertEqual(call.kwargs['ts_code'],code)

    def test_suspension_raw_evidence_change_invalidates_completed_proof(self):
        worker=self.bs_worker();job=worker._new_job();code='000001.SZ'
        worker._store_suspension_response(code,[{'ts_code':code,'trade_date':'20261110','suspend_type':'S','suspend_timing':None}],
                                           '2026-11-10','2026-11-10',self.clock[0].isoformat())
        before=worker.evidence_signature(job);self.assertTrue(before)
        proof=json.loads((worker.runtime/'nontrading-proof'/(code+'-2026-11-10.json')).read_text())
        raw=worker.runtime/'nontrading-raw'/proof['raw_file'];payload=json.loads(raw.read_text())
        payload['records'][0]['suspend_type']='R';atomic_json(raw,payload)
        with self.assertRaisesRegex(ValueError,'evidence changed'):worker.evidence_signature(job)


if __name__=='__main__':unittest.main()
