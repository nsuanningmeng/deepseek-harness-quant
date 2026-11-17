"""Selected historical-minute coverage cannot inherit a legacy provider's marker."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch
import hashlib
import sqlite3

from deck import system_live


class ActiveMinuteHealthTests(TestCase):
    def test_unchanged_database_recovers_from_read_lock_after_cooldown_without_rescanning_success(self):
        with TemporaryDirectory() as directory, ThreadPoolExecutor(max_workers=1) as pool:
            path=Path(directory)/'minute_baostock.db'
            symbols=[f'{i:06}.SZ' for i in range(300)]
            with sqlite3.connect(path) as con:
                con.execute('CREATE TABLE minute_5m(code TEXT,trade_time TEXT,close REAL)')
                con.executemany('INSERT INTO minute_5m VALUES(?,?,10)',[(code,'2026-11-12 15:00:00') for code in symbols])
            status={'database_path':str(path),'job_id':'same-job','start_date':'2026-08-12','target_date':'2026-11-12',
                    'symbols':symbols,'universe_fingerprint':hashlib.sha256('\n'.join(symbols).encode()).hexdigest(),
                    'current_date_ready':True,'cache_verified':True,'deep_verified':True,'rows':300,
                    'covered_symbols':300,'completed_symbols':300,'expected_symbols':300,'source':'isolated'}
            version=(path.stat().st_mtime_ns,path.stat().st_size)
            jobs={};clock=[100.0]
            with patch.object(system_live,'DBS',{'minute':'minute.db'}), \
                 patch.object(system_live,'_db_probe_jobs',jobs),patch.object(system_live,'_db_probe_pool',pool), \
                 patch.object(system_live,'_research_document',return_value={}), \
                 patch.object(system_live,'_minute_job_status',return_value=status), \
                 patch.object(system_live.time,'monotonic',side_effect=lambda:clock[0]), \
                 patch.object(system_live,'_probe_one',wraps=system_live._probe_one) as probe:
                writer=sqlite3.connect(path)
                try:
                    writer.execute('BEGIN EXCLUSIVE')
                    system_live._db_health();jobs['minute']['future'].result(timeout=5)
                    blocked=system_live._db_health()['minute']
                    self.assertFalse(blocked['ready']);self.assertTrue(blocked['probe_retryable'])
                    self.assertIn('locked',blocked['error'])
                    for _ in range(3):system_live._db_health()
                    self.assertEqual(probe.call_count,1)
                finally:
                    writer.rollback();writer.close()
                self.assertEqual((path.stat().st_mtime_ns,path.stat().st_size),version)
                clock[0]+=system_live._DB_PROBE_RETRY_SECONDS-.01
                self.assertFalse(system_live._db_health()['minute']['ready']);self.assertEqual(probe.call_count,1)
                clock[0]+=.02
                system_live._db_health();jobs['minute']['future'].result(timeout=5)
                recovered=system_live._db_health()['minute']
                self.assertTrue(recovered['ready']);self.assertEqual(probe.call_count,2)
                clock[0]+=100
                for _ in range(3):self.assertTrue(system_live._db_health()['minute']['ready'])
                self.assertEqual(probe.call_count,2)

    def test_selected_database_needs_current_receipts_and_matching_actual_rows(self):
        with TemporaryDirectory() as directory, ThreadPoolExecutor(max_workers=1) as pool:
            path = Path(directory) / 'minute_baostock.db'
            with sqlite3.connect(path) as con:
                con.execute('CREATE TABLE minute_5m(code TEXT,trade_time TEXT,close REAL)')
                con.executemany('INSERT INTO minute_5m VALUES(?,?,?)',
                                [(f'{i:06}.SZ', '2026-11-11 15:00:00', 10) for i in range(300)])
            status = {'database_path': str(path), 'current_date_ready': True,
                      'cache_verified': True, 'deep_verified': True, 'rows': 300, 'covered_symbols': 300,
                      'completed_symbols': 300, 'expected_symbols': 300, 'source': 'baostock'}
            symbols=[f'{i:06}.SZ' for i in range(300)]
            status.update(job_id='selected-job',start_date='2026-08-11',target_date='2026-11-11',symbols=symbols,
                          universe_fingerprint=hashlib.sha256('\n'.join(symbols).encode()).hexdigest())
            jobs = {}
            with patch.object(system_live, 'DBS', {'minute': 'minute.db'}), \
                 patch.object(system_live, '_db_probe_jobs', jobs), \
                 patch.object(system_live, '_db_probe_pool', pool), \
                 patch.object(system_live, '_research_document', return_value={}), \
                 patch.object(system_live, '_minute_job_status', return_value=status):
                system_live._db_health()
                jobs['minute']['future'].result(timeout=5)
                self.assertTrue(system_live._db_health()['minute']['ready'])
                status['deep_verified'] = False
                with patch.object(system_live,'_probe_one',side_effect=AssertionError('No scan before deep verification')) as probe:
                    pending=system_live._db_health()['minute']
                    self.assertFalse(pending['ready']);self.assertIsNone(pending['rows']);probe.assert_not_called()
                status['deep_verified'] = True
                status['current_date_ready'] = False
                self.assertFalse(system_live._db_health()['minute']['ready'])
                status['current_date_ready'] = True
                status['rows'] = 301
                self.assertFalse(system_live._db_health()['minute']['ready'])
                status['rows'] = 300
                status['completed_symbols'] = 299
                self.assertFalse(system_live._db_health()['minute']['ready'])

    def test_rollover_scopes_actual_rows_and_invalidates_scope_cache_without_deleting_history(self):
        with TemporaryDirectory() as directory, ThreadPoolExecutor(max_workers=1) as pool:
            path=Path(directory)/'minute_baostock.db'
            symbols=[f'{i:06}.SZ' for i in range(300)]
            with sqlite3.connect(path) as con:
                con.execute('CREATE TABLE minute_5m(code TEXT,trade_time TEXT,close REAL)')
                con.executemany('INSERT INTO minute_5m VALUES(?,?,10)',
                                [(f'{i:06}.SZ',day+' 15:00:00') for i in range(301)
                                 for day in ('2026-08-11','2026-08-12','2026-11-11','2026-11-12')])
                con.execute("INSERT INTO minute_5m VALUES('999999.SZ','2026-11-12 15:00:00',-1)")
            before=path.read_bytes()
            status={'database_path':str(path),'job_id':'prior-job','start_date':'2026-08-11','target_date':'2026-11-11',
                    'symbols':symbols,'universe_fingerprint':hashlib.sha256('\n'.join(symbols).encode()).hexdigest(),
                    'current_date_ready':True,'cache_verified':True,'deep_verified':True,'rows':900,
                    'covered_symbols':300,'completed_symbols':300,'expected_symbols':300,'source':'isolated'}
            jobs={}
            with patch.object(system_live,'DBS',{'minute':'minute.db'}), \
                 patch.object(system_live,'_db_probe_jobs',jobs),patch.object(system_live,'_db_probe_pool',pool), \
                 patch.object(system_live,'_research_document',return_value={}), \
                 patch.object(system_live,'_minute_job_status',return_value=status):
                def observed():
                    system_live._db_health();jobs['minute']['future'].result(timeout=5)
                    return system_live._db_health()['minute']
                prior=observed();self.assertTrue(prior['ready']);self.assertEqual(prior['rows'],900)
                self.assertEqual(prior['total_cache_rows'],1205)
                previous_future=jobs['minute']['future']
                status['job_id']='rolled-job'
                observed();self.assertIsNot(jobs['minute']['future'],previous_future)
                previous_future=jobs['minute']['future']
                status.update(start_date='2026-08-12',target_date='2026-11-12')
                rolled=observed();self.assertIsNot(jobs['minute']['future'],previous_future)
                self.assertEqual(rolled['first_date'],'2026-08-12 15:00:00')
                self.assertEqual(rolled['data_date'],'2026-11-12 15:00:00')
                previous_future=jobs['minute']['future']
                status['symbols']=[f'{i:06}.SZ' for i in range(1,301)]
                status['universe_fingerprint']=hashlib.sha256('\n'.join(status['symbols']).encode()).hexdigest()
                current=observed();self.assertIsNot(jobs['minute']['future'],previous_future)
                self.assertTrue(current['ready']);self.assertEqual(current['rows'],900)
                self.assertEqual(current['covered_symbols'],300);self.assertEqual(current['total_cache_rows'],1205)
                status['universe_fingerprint']='wrong-scope'
                invalid=system_live._db_health()['minute'];self.assertFalse(invalid['ready']);self.assertIsNone(invalid['rows'])
            self.assertEqual(path.read_bytes(),before)
