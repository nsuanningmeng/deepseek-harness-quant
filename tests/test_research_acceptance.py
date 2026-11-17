"""The independent CLI must use the selected source and real quote freshness."""
from datetime import datetime,timedelta,timezone
import hashlib
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from validation import check_research_complete as checks
from validation import check_portal_readiness as portal


class ResearchAcceptanceTests(unittest.TestCase):
    def test_declared_ready_cannot_bypass_actual_paper_feed_or_required_contents(self):
        body={'state':'ready','ready':True,'market':{'calendar_verified':True,'quote_count':300}}
        with patch('paperlive.config.load_config',return_value={}), \
             patch.object(checks,'paper_feed_status',return_value={'ready':False,'reason':'quotes are old'}) as feed:
            state,reason=portal.classify('/api/paperlive/state',200,body)
        self.assertEqual(state,'waiting_data');self.assertEqual(reason,'quotes are old');feed.assert_called_once()
        state,_=portal.classify('/api/live/wufu_rotation',200,{'state':'ready','ready':True,'assets':[]})
        self.assertEqual(state,'missing_data')

    def test_selected_minute_database_requires_deep_receipts_and_actual_pool(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache=Path(tmp);db=cache/'selected-free-minute.db';day='2026-11-11'
            symbols={'600000.SH','000001.SZ'}
            with sqlite3.connect(db) as con:
                con.execute('CREATE TABLE minute_5m(code TEXT,trade_time TEXT,open REAL,high REAL,low REAL,close REAL,volume REAL,amount REAL)')
                con.executemany('INSERT INTO minute_5m VALUES(?,?,10,12,8,11,100,1100)',[(symbol,day+' 15:00:00') for symbol in symbols])
                con.executemany('INSERT INTO minute_5m VALUES(?,?,10,12,8,11,100,1100)',
                                [('600000.SH','2026-08-10 15:00:00'),('999999.SZ',day+' 15:00:00')])
            status={'database_path':str(db),'source':'isolated-free-source','start_date':'2026-08-11','target_date':day,
                    'expected_symbols':2,'rows':2,'deep_verified':True,'cache_verified':True,'frozen_window_complete':True,
                    'current_date_ready':True,'current_universe_match':True,'status':'complete'}
            status.update(job_id='isolated-job',symbols=sorted(symbols),
                          universe_fingerprint=hashlib.sha256('\n'.join(sorted(symbols)).encode()).hexdigest())
            manifest={'coverage':{'daily':{'end':day}}}
            with patch.object(checks,'CACHE_DIR',cache),patch('factors.research_context.universe',return_value=symbols):
                good=next(row for row in checks.database_coverage(manifest,status) if row['table']=='minute_5m')
                self.assertTrue(good['initialized']);self.assertEqual(good['database'],db.name)
                self.assertEqual(good['rows'],2);self.assertEqual(good['total_cache_rows'],4)
                status['deep_verified']=False
                failed=next(row for row in checks.database_coverage(manifest,status) if row['table']=='minute_5m')
                self.assertFalse(failed['initialized'])
            self.assertFalse((cache/'minute.db').exists())

    def test_quote_count_cannot_make_old_or_paused_quotes_intraday_ready(self):
        now=datetime(2026,11,12,10,tzinfo=timezone(timedelta(hours=8)))
        quote={'symbol':'600000.SH','name':'isolated','last':'10','bid':'9.99','ask':'10.01','bid_size':100,'ask_size':100,
               'previous_close':'10','open':'10','high':'11','low':'9','volume':1000,
               'timestamp':(now-timedelta(seconds=61)).isoformat(),'received_at':now.isoformat(),
               'source':'isolated-public-source','limit_up':'11','limit_down':'9'}
        cfg={'market':{'provider':'tencent_tushare','max_quote_age_seconds':30,'trading_sessions':[['09:30','11:30'],['13:00','14:57']]},
             'activation':{'expected_universe_count':1}}
        state={'paused':False,'market':{'provider':'tencent_tushare','open':True,'date':'2026-11-12','calendar_verified':True,'heartbeat_stale':False,'quote_count':300},
               'activation':{'ready':True,'status':'active','tradable_symbols':['600000.SH']},'quotes':[quote],
               'universe':[{'symbol':'600000.SH'}],'eligible_symbols':['600000.SH']}
        self.assertFalse(checks.paper_feed_status(cfg,state,now)['ready'])
        quote['timestamp']=now.isoformat()
        self.assertTrue(checks.paper_feed_status(cfg,state,now)['ready'])
        state['paused']=True
        self.assertFalse(checks.paper_feed_status(cfg,state,now)['ready'])
        state['paused']=False;quote['bid_size']=0
        self.assertFalse(checks.paper_feed_status(cfg,state,now)['ready'])
        quote['bid_size']=100;cfg['activation']['expected_universe_count']=300
        self.assertFalse(checks.paper_feed_status(cfg,state,now)['ready'])
        cfg['activation']['expected_universe_count']=1
        quote['received_at']=(now-timedelta(seconds=31)).isoformat()
        self.assertFalse(checks.paper_feed_status(cfg,state,now)['ready'])
        late=now.replace(hour=15);quote.update(timestamp=late.isoformat(),received_at=late.isoformat())
        self.assertFalse(checks.paper_feed_status(cfg,state,late)['ready'])


if __name__=='__main__':unittest.main()
