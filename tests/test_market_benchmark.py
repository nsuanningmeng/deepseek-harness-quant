import json
from datetime import date, timedelta
import math
from pathlib import Path
import sqlite3
import statistics
import tempfile
import threading
import time
from unittest import TestCase
from unittest.mock import patch

from deck import live_api, market_benchmark as market
from deck.background_snapshot import BackgroundSnapshot


class MarketBenchmarkTests(TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.path=self.root/'bars.db'
        self.snapshot=BackgroundSnapshot('isolated-market-benchmark',retry_seconds=0)
        for target,value in (('_snapshot',self.snapshot),('databases',lambda:(self.path,)),
                             ('_writer_active',lambda *args:False),('_runtime_directory',lambda:self.root)):
            p=patch.object(market,target,value);p.start();self.addCleanup(p.stop)
        self.addCleanup(self.snapshot._executor.shutdown)
        self.pairs=(('2026-11-11','2026-11-12'),)

    def database(self):
        with sqlite3.connect(self.path) as con:
            con.execute('CREATE TABLE daily_bar(date TEXT, code TEXT,close REAL,adjust TEXT)')
            con.executemany('INSERT INTO daily_bar VALUES(?,?,?,?)',[
                ('2026-11-11','000001.SZ',10,'qfq'),('2026-11-12','000001.SZ',11,'qfq'),
                ('2026-11-11','600000.SH',20,'qfq'),('2026-11-12','600000.SH',18,'qfq'),
                ('2026-11-12','600000.SH',999,'none'),('2026-11-11','600002.SH',0,'qfq')])

    def test_cold_reads_deduplicate_and_never_publish_stale_market_comparison(self):
        self.database();release=threading.Event();self.addCleanup(release.set)
        calculate=market._calculate
        def slow(*args):release.wait(2);return calculate(*args)
        with patch.object(market,'_calculate',side_effect=slow) as call:
            begin=time.monotonic();pending=market.public_state(self.pairs)
            self.assertLess(time.monotonic()-begin,.2);self.assertFalse(pending['ready']);self.assertEqual(pending['values'],{})
            for _ in range(4):self.assertFalse(market.public_state(self.pairs)['ready'])
            release.set();self.snapshot._future.result(timeout=3)
            ready=market.public_state(self.pairs);self.assertTrue(ready['ready']);self.assertEqual(call.call_count,1)
            value=ready['values']['2026-11-11|2026-11-12'];self.assertEqual(value,{'value':0.0,'matched_symbols':2})
            self.assertTrue(market.public_state(self.pairs)['ready']);self.assertEqual(call.call_count,1)
            with sqlite3.connect(self.path) as con:con.execute("UPDATE daily_bar SET close=12 WHERE code='000001.SZ' AND date='2026-11-12'")
            changed=market.public_state(self.pairs);self.assertFalse(changed['ready']);self.assertEqual(changed['values'],{})
            self.snapshot._future.result(timeout=3)
            self.assertEqual(market.public_state(self.pairs)['values']['2026-11-11|2026-11-12']['value'],.05)

    def test_failed_scan_retries_and_core_writer_does_not_start_a_read(self):
        with patch.object(market,'_writer_active',return_value=True),patch.object(market,'_calculate') as call:
            pending=market.public_state(self.pairs);self.assertEqual(pending['state'],'waiting_core_update');call.assert_not_called()
        self.database()
        with patch.object(market,'_calculate',side_effect=sqlite3.OperationalError('locked')):
            market.public_state(self.pairs)
            try:self.snapshot._future.result(timeout=2)
            except sqlite3.OperationalError:pass
        self.assertFalse(market.public_state(self.pairs)['ready'])
        self.snapshot._future.result(timeout=3)
        self.assertTrue(market.public_state(self.pairs)['ready'])

    def test_validation_cache_refreshes_only_pending_comparison_and_keeps_sample_statistics(self):
        logs=self.root/'logs';logs.mkdir()
        entries=[{'entry_date':'2026-11-11','otype':'value','fwd':{'t1':{'date':'2026-11-12','ret':.03}}}]
        path=logs/'pitch_track_pool_fixture.json';path.write_text(json.dumps({'entries':entries}))
        cached={'diagnosis':{'marker':'unchanged'},'rows':[{'act_avg':.03}]}
        pending={'state':'probing','ready':False,'loading':True,'values':{},'reason':'后台核验中'}
        with patch.object(live_api,'BASE',self.root),patch.object(live_api,'_validation_cache',{'data':cached,'ver':path.stat().st_mtime,'ts':time.time()}),patch.object(market,'public_state',return_value=pending):
            result=live_api.live_validation()
        self.assertEqual(result['diagnosis'],cached['diagnosis']);self.assertEqual(result['rows'],cached['rows'])
        self.assertFalse(result['readiness']['ready']);self.assertEqual(result['readiness']['scope'],'market_comparison')
        batch=result['batches_review']['batches'][0]
        self.assertEqual(batch['t1_avg'],.03);self.assertEqual(batch['market_state'],'probing')
        self.assertIsNone(batch['mkt_t1_median']);self.assertIsNone(batch['excess'])
        self.assertNotIn('batches_review',cached)

    def _indexed_database(self, path):
        con = sqlite3.connect(path)
        self.addCleanup(con.close)
        con.execute('CREATE TABLE daily_bar(code TEXT NOT NULL,date TEXT NOT NULL,close REAL,'
                    'adjust TEXT NOT NULL,PRIMARY KEY(code,date,adjust))')
        con.execute('CREATE INDEX idx_daily_bar ON daily_bar(code,adjust,date)')
        return con

    def _legacy_result(self, paths):
        prices = {day: {} for pair in self.pairs for day in pair}
        for path in paths:
            with sqlite3.connect(path) as con:
                for day, code, close in con.execute('SELECT date,code,close FROM daily_bar WHERE adjust=? AND date IN (?,?)',
                                                    ('qfq', *self.pairs[0])):
                    if isinstance(close, (int, float)) and math.isfinite(close) and close > 0:
                        prices[day][code] = close
        first, last = self.pairs[0]
        returns = [prices[last][code]/prices[first][code]-1 for code in prices[first].keys() & prices[last].keys()]
        return {first+'|'+last: {'value': round(statistics.median(returns), 4) if returns else None,
                               'matched_symbols': len(returns)}}

    def test_index_seeks_preserve_full_market_increment_overrides_and_invalid_quotes(self):
        con = self._indexed_database(self.path)
        first, last = self.pairs[0]
        symbols = [f'{index:06d}.SZ' for index in range(1, 513)]
        history = [(date(2022, 1, 1)+timedelta(days=day)).isoformat() for day in range(300)]
        con.executemany('INSERT INTO daily_bar VALUES(?,?,?,?)',
                        ((code, day, 10, 'qfq') for code in symbols for day in history))
        con.executemany('INSERT INTO daily_bar VALUES(?,?,?,?)',
                        ((code, day, 10 if day == first else 11, 'qfq') for code in symbols for day in (first, last)))
        # Includes a delisted-style symbol outside any current constituent fixture.
        con.executemany('INSERT INTO daily_bar VALUES(?,?,?,?)', [
            ('900001.SH', first, 100, 'qfq'), ('900001.SH', last, 50, 'qfq'),
            ('000001.SZ', last, 999, 'none'), ('000003.SZ', '2025-01-01', 9999, 'qfq'),
            ('800001.SH', first, 0, 'qfq'), ('800001.SH', last, 1, 'qfq'),
            ('800002.SH', first, 1, 'qfq'), ('800002.SH', last, float('inf'), 'qfq'),
            ('800003.SH', first, None, 'qfq'), ('800003.SH', last, 1, 'qfq'),
            ('800004.SH', first, 10, 'qfq'), ('800004.SH', last, -1, 'qfq')])
        con.commit()
        increment = self.root/'bars_incr.db'
        inc = self._indexed_database(increment)
        inc.executemany('INSERT INTO daily_bar VALUES(?,?,?,?)', [
            ('000001.SZ', last, 20, 'qfq'), ('900001.SH', last, 200, 'qfq'),
            ('000002.SZ', last, -50, 'qfq'),  # An invalid increment cannot erase a valid base quote.
            ('700001.SH', first, 20, 'qfq'), ('700001.SH', last, 22, 'qfq')])
        inc.commit()
        query = market._price_query(con, (first, last))
        plan = [row[3] for row in con.execute('EXPLAIN QUERY PLAN '+query, ('qfq', first, last))]
        self.assertTrue(any('SEARCH b USING INDEX' in row for row in plan), plan)
        self.assertFalse(any('SCAN daily_bar' in row or 'SCAN b' in row for row in plan), plan)
        paths = (self.path, increment)
        actual = market._calculate(paths, self.pairs, market.source_version(paths))
        self.assertEqual(actual, self._legacy_result(paths))
        self.assertEqual(actual[first+'|'+last]['matched_symbols'], 514)
        self.assertEqual(actual[first+'|'+last]['value'], .1)

    def test_unindexed_and_nullable_legacy_tables_keep_original_scan_semantics(self):
        self.database()
        with sqlite3.connect(self.path) as con:
            query = market._price_query(con, self.pairs[0])
            self.assertTrue(query.startswith('SELECT date,code,close FROM daily_bar WHERE'))
            con.executemany('INSERT INTO daily_bar VALUES(?,?,?,?)', [
                ('2026-11-11', None, 10, 'qfq'), ('2026-11-12', None, 20, 'qfq')])
            con.execute('CREATE INDEX nullable_code_index ON daily_bar(code,adjust,date)')
            self.assertEqual(market._price_query(con, self.pairs[0]), query)
        actual = market._calculate((self.path,), self.pairs, market.source_version((self.path,)))
        self.assertEqual(actual, self._legacy_result((self.path,)))
        self.assertEqual(actual['2026-11-11|2026-11-12']['matched_symbols'], 3)

    def test_partial_code_index_does_not_claim_complete_symbol_enumeration(self):
        with sqlite3.connect(self.path) as con:
            con.execute('CREATE TABLE daily_bar(code TEXT NOT NULL,date TEXT,close REAL,adjust TEXT)')
            con.execute("CREATE INDEX partial_code ON daily_bar(code,date,adjust) WHERE adjust='qfq'")
            self.assertTrue(market._price_query(con, self.pairs[0]).startswith('SELECT date,code,close FROM daily_bar WHERE'))
