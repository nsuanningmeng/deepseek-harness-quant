"""Resumable official Tushare research initialization, separate from paper cash.

No orders, backfilled paper profits, mirror endpoints or credential logging.
Daily source quantities retain the existing cache's Tushare units (lots and
thousand yuan); raw prices and adjustment factors are retained separately.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import signal
import sqlite3
import sys
import time
import threading

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import yaml
from autotrade.locking import ProcessLock
from autotrade.monitoring import atomic_json
from autotrade.providers import TushareHttpClient, TushareRequestError
from data.cache import _SCHEMA as BAR_SCHEMA

TZ = timezone(timedelta(hours=8))
DAILY = ('ts_code','trade_date','open','high','low','close','pre_close','pct_chg','vol','amount')
BASIC = ('ts_code','trade_date','close','turnover_rate','turnover_rate_f','volume_ratio','pe','pe_ttm','pb',
         'ps','ps_ttm','dv_ratio','dv_ttm','total_share','float_share','free_share','total_mv','circ_mv')


class ResearchInterrupted(Exception):
    pass


class ResearchSourceBlocked(ValueError):
    def __init__(self,endpoint,category,retry_at=None):
        self.endpoint,self.category,self.retry_at=endpoint,category,retry_at
        labels={'rate_limited':'官方接口频次/额度受限，保留断点并等待冷却',
                'permission_denied':'官方接口权限未开通，请核对本机账号权限',
                'authentication':'本机数据凭据未通过验证，请核对Token'}
        message='Tushare '+endpoint+'：'+labels.get(category,'官方数据源暂不可用')
        if retry_at:
            message+='；下次最早重试 '+retry_at
        super().__init__(message)


def iso(value):
    value = str(value)
    return datetime.strptime(value, '%Y%m%d').date().isoformat() if re.fullmatch(r'\d{8}', value) else date.fromisoformat(value).isoformat()


def compact(value):
    return iso(value).replace('-', '')


def number(value, nullable=True):
    if value is None or value == '':
        if nullable:
            return None
        raise ValueError('missing required source number')
    if isinstance(value, bool):
        raise ValueError('boolean source number')
    result = float(value)
    if not math.isfinite(result):
        raise ValueError('non-finite source number')
    return result


def percentage_ratio(value):
    """Convert source percentage points to the fractional research-table unit."""
    parsed = number(value)
    return parsed / 100 if parsed is not None else None


def a_share(code):
    return (isinstance(code,str) and re.fullmatch(r'\d{6}\.(SH|SZ|BJ)',code) is not None and
            (code.endswith('.BJ') or (code.endswith('.SH') and code.startswith('6'))
             or (code.endswith('.SZ') and code.startswith(('0','3')))))


def clean_json(value):
    if isinstance(value,dict):
        return {key:clean_json(item) for key,item in value.items()}
    if isinstance(value,(list,tuple)):
        return [clean_json(item) for item in value]
    if isinstance(value,float) and not math.isfinite(value):
        return None
    return value


def quarters(start,end):
    return [f'{year}{suffix}' for year in range(int(start[:4]),int(end[:4])+1)
            for suffix in ('0331','0630','0930','1231') if compact(start)<=f'{year}{suffix}'<=compact(end)]


def year_ranges(start,end):
    return [(max(start,f'{year}-01-01'),min(end,f'{year}-12-31'))
            for year in range(int(start[:4]),int(end[:4])+1)]


def validate_minute_rows(code,rows,start,end,open_days,*,source='tushare:stk_mins:5min'):
    """Pure source-unit/schema validation shared by both historical loaders."""
    tuples=[];seen=set()
    for row in rows:
        stamp=datetime.strptime(str(row['trade_time']),'%Y-%m-%d %H:%M:%S')
        day=stamp.date().isoformat();clock=stamp.strftime('%H:%M')
        if row['ts_code']!=code or not start<=day<=end or day not in open_days or stamp in seen or stamp.second or stamp.minute%5 or not ('09:30'<=clock<='11:30' or '13:00'<=clock<='15:00'):
            raise ValueError('invalid minute symbol/session/timestamp')
        seen.add(stamp)
        prices=[number(row[f],False) for f in ('open','high','low','close')]
        vol=number(row['vol'],False);amount=number(row['amount'],False)
        if any(v<=0 for v in prices) or prices[2]>min(prices[0],prices[3]) or prices[1]<max(prices[0],prices[3]) or min(vol,amount)<0:
            raise ValueError('invalid minute OHLC/quantity')
        tuples.append((code,stamp.isoformat(sep=' '),*prices,vol,amount,source))
    return sorted(tuples,key=lambda row:row[1])


def validate_close_cutoff(value):
    """Allow completed-session checks only after the A-share close settles."""
    if not isinstance(value,str) or not re.fullmatch(r'\d{2}:\d{2}',value):
        raise ValueError('completed-session cutoff must use HH:MM')
    hour,minute=map(int,value.split(':'))
    if not (0<=hour<=23 and 0<=minute<60 and hour*60+minute>=15*60+5):
        raise ValueError('completed-session cutoff must be between 15:05 and 23:59')
    return value


def load_config(path=None):
    cfg = yaml.safe_load((Path(path) if path else ROOT / 'config/research.yaml').read_text())
    for key in ('cache_dir','runtime_dir','finance_parquet_dir','etf_pool_file'):
        item = Path(cfg[key])
        cfg[key] = str(item if item.is_absolute() else ROOT / item)
    for key in ('daily_start','index_start','finance_start'):
        cfg[key] = iso(cfg[key])
    if cfg['daily_start'] < '2019-01-01':
        raise ValueError('turnover research before 2019 is unsupported')
    if cfg['end'] != 'latest':
        cfg['end'] = iso(cfg['end'])
    cfg['completed_session_cutoff']=validate_close_cutoff(cfg.get('completed_session_cutoff','18:00'))
    if not 0 < float(cfg['min_call_interval_seconds']) <= 60 or not 0 < float(cfg['request_timeout']) <= 120:
        raise ValueError('invalid research request policy')
    if type(cfg['request_attempts']) is not int or not 1 <= cfg['request_attempts'] <= 5:
        raise ValueError('invalid research retry policy')
    if set(cfg.get('data_exchanges',[])) not in ({'SSE','SZSE'},{'SSE','SZSE','BSE'}):
        raise ValueError('research daily scope must explicitly include both SSE and SZSE')
    if type(cfg.get('minute_months')) is not int or not 1<=cfg['minute_months']<=12:
        raise ValueError('invalid minute history window')
    return cfg


class ResearchBootstrap:
    def __init__(self, cfg, *, client=None, now=None, sleeper=None):
        self.cfg, self.client = cfg, client
        self.now = now or (lambda: datetime.now(TZ))
        self.sleep = sleeper or time.sleep
        self.cache, self.runtime = Path(cfg['cache_dir']), Path(cfg['runtime_dir'])
        self.parquet = Path(cfg['finance_parquet_dir'])
        self._last_call = None
        self._call_lock=threading.Lock()
        self.stop_event=threading.Event()
        self._client_token = None
        self._st_enabled = cfg.get('historical_st') != 'disabled'
        self._end_limit=None
        self._progress_keys=None
        # The scheduling clock does not change source identity or data windows.
        # Excluding it also preserves checkpoints made before it was configurable.
        fingerprint = hashlib.sha256(json.dumps({k:v for k,v in cfg.items() if k not in ('analysis','completed_session_cutoff')}, sort_keys=True, default=str).encode()).hexdigest()
        legacy_fingerprint=hashlib.sha256(json.dumps({k:v for k,v in cfg.items() if k!='completed_session_cutoff'},sort_keys=True,default=str).encode()).hexdigest()
        try:
            self.manifest = json.loads((self.runtime/'bootstrap-manifest.json').read_text())
        except (OSError, ValueError):
            self.manifest = {'schema_version':1,'source':'tushare-official','completed':{},'coverage':{},'warnings':[]}
        if self.manifest.get('config_fingerprint') not in (None, fingerprint,legacy_fingerprint):
            sealed=any(key.startswith(('daily:','qfq:','finance:','assets:','minute:')) for key in self.manifest.get('completed',{}))
            if sealed:
                raise ValueError('Research configuration changed; use a separate runtime_dir or review prior checkpoints')
        self.manifest['config_fingerprint'] = fingerprint
        self._source_cooldowns=dict(self.manifest.get('source_cooldowns',{}))
        self.state = {'schema_version':1,'source':'tushare-official','phase':'initializing','status':'idle',
                      'started_at':self.now().isoformat(),'updated_at':self.now().isoformat(),'stages':dict(self.manifest.get('stages',{}))}

    def _save(self):
        self.state['updated_at'] = self.now().isoformat()
        self.manifest['asof'] = self.state['updated_at']
        # Workers only replace entries in the separate cooldown map; publish
        # an immutable snapshot from the main writer, never from HTTP threads.
        with self._call_lock:
            self.manifest['source_cooldowns']=dict(self._source_cooldowns)
        atomic_json(self.runtime/'bootstrap-manifest.json', self.manifest)
        atomic_json(self.runtime/'bootstrap-state.json', self.state)

    def _warn(self, message):
        if message not in self.manifest['warnings']:
            self.manifest['warnings'].append(message)

    def _query(self, endpoint, fields=(), **params):
        from paperlive.config import load_config as paper_config
        from paperlive.data_settings import tushare_connection
        for attempt in range(self.cfg['request_attempts']):
            try:
                # Serialize credential reload and request starts, while each
                # independent HTTP response is allowed to arrive concurrently.
                with self._call_lock:
                    quota=self._source_cooldowns.get(endpoint,{})
                    retry_at=quota.get('retry_at')
                    if retry_at:
                        deadline=datetime.fromisoformat(retry_at)
                        if deadline.tzinfo is None:
                            raise ValueError('invalid source cooldown timestamp')
                        if self.now()<deadline:
                            raise ResearchSourceBlocked(endpoint,'rate_limited',retry_at)
                    if self.client is None or self._client_token is not None:
                        token = tushare_connection(paper_config())
                        if token != self._client_token:
                            self.client = TushareHttpClient(token, self.cfg['request_timeout'])
                            self._client_token = token
                    current = time.monotonic()
                    if self._last_call is not None:
                        self.sleep(max(0, self.cfg['min_call_interval_seconds']-(current-self._last_call)))
                    self._last_call = time.monotonic()
                    client=self.client
                reply = client.query(endpoint, fields=','.join(fields), **params)
                if not set(fields) <= set(reply.columns):
                    raise ValueError('missing required fields')
                rows = reply.to_dict('records')
                if any(not isinstance(row,dict) or not set(fields) <= set(row) for row in rows):
                    raise ValueError('invalid response records')
                with self._call_lock:
                    old=self._source_cooldowns.get(endpoint,{}).get('retry_at')
                    if old and datetime.fromisoformat(old)<=self.now():
                        self._source_cooldowns.pop(endpoint,None)
                return rows
            except ResearchSourceBlocked:
                raise
            except TushareRequestError as exc:
                if exc.category=='rate_limited':
                    # This account was verified to allow stk_mins once/hour.
                    # A real rate error never triggers the generic 3s retry.
                    cooldown=3600 if endpoint=='stk_mins' else 60
                    checked=self.now();retry_at=(checked+timedelta(seconds=cooldown)).isoformat()
                    with self._call_lock:
                        old=self._source_cooldowns.get(endpoint,{}).get('retry_at')
                        if old and datetime.fromisoformat(old)>datetime.fromisoformat(retry_at):
                            retry_at=old
                        self._source_cooldowns[endpoint]={'category':'rate_limited','observed_at':checked.isoformat(),
                                'cooldown_seconds':cooldown,'retry_at':retry_at,'automatic_short_retries':False}
                    raise ResearchSourceBlocked(endpoint,'rate_limited',retry_at) from None
                if exc.category in ('permission_denied','authentication'):
                    raise ResearchSourceBlocked(endpoint,exc.category) from None
                if attempt+1 < self.cfg['request_attempts']:
                    self.sleep(self.cfg['retry_seconds']*(attempt+1))
            except Exception:
                if attempt+1 < self.cfg['request_attempts']:
                    self.sleep(self.cfg['retry_seconds']*(attempt+1))
        raise ValueError('Tushare '+endpoint+' failed; check official API permission/network/required fields') from None

    @contextmanager
    def _db(self, filename):
        self.cache.mkdir(parents=True,exist_ok=True)
        con = sqlite3.connect(self.cache/filename, timeout=60)
        try:
            yield con
            con.commit()
        except Exception:
            con.rollback()
            raise
        finally:
            con.close()

    def _done(self,key):
        return key in self.manifest['completed']

    def _complete(self,key,rows,**facts):
        self.manifest['completed'][key]={'rows':int(rows),'completed_at':self.now().isoformat(),**facts}
        if self._progress_keys is not None:
            self.state['completed_units']=sum(self._done(name) for name in self._progress_keys)
        self.state['rows'] = int(rows)
        self._save()
        self._check_stop()

    def _check_stop(self):
        if self.stop_event.is_set():
            raise ResearchInterrupted('maintenance signal received; committed checkpoints retained')

    def _progress(self,stage,phase,keys):
        self._progress_keys=tuple(keys)
        self.state.update(stage=stage,phase=phase,total_units=len(self._progress_keys),
                          completed_units=sum(self._done(key) for key in self._progress_keys))
        self._save()

    def calendar(self,start,end):
        rows=self._query('trade_cal',('exchange','cal_date','is_open'),exchange='SSE',start_date=compact(start),end_date=compact(end))
        days={}
        for row in rows:
            day=iso(row['cal_date'])
            if row['exchange']!='SSE' or day in days or not start<=day<=end or str(row['is_open']) not in ('0','1'):
                raise ValueError('invalid official exchange calendar')
            days[day]=str(row['is_open'])=='1'
        expected=(date.fromisoformat(end)-date.fromisoformat(start)).days+1
        if len(days)!=expected:
            raise ValueError('official calendar coverage is incomplete')
        return sorted(day for day,value in days.items() if value)

    def prepare(self):
        now=self.now().astimezone(TZ)
        completed_cutoff=validate_close_cutoff(self.cfg.get('completed_session_cutoff','18:00'))
        cutoff=now.date() if now.strftime('%H:%M')>=completed_cutoff else now.date()-timedelta(days=1)
        end=min(iso(self.cfg['end']),cutoff.isoformat()) if self.cfg['end']!='latest' else cutoff.isoformat()
        if self._end_limit is not None:
            end=min(end,self._end_limit)
        self.days=self.calendar(self.cfg['daily_start'],end)
        if not self.days:
            raise ValueError('no completed research trading session')
        self.end=self.days[-1]
        previous_end=self.manifest.get('coverage',{}).get('daily',{}).get('end')
        accepted=[day for day in [previous_end,*self.manifest.get('stage_dates',{}).values()] if day]
        if accepted and max(accepted)>self.end:
            raise ValueError('requested cutoff predates accepted research data; use an isolated runtime/cache')
        dates=self.manifest.setdefault('stage_dates',{})
        for stage,status in list(self.state['stages'].items()):
            if status=='complete' and dates.get(stage,previous_end)!=self.end:
                self.state['stages'][stage]='pending'
        self.manifest['stages']=dict(self.state['stages'])
        self.manifest['target_end']=self.end
        self.state.update({'status':'running','phase':'preparing','end':self.end})
        self._save()
        metadata_key='prepare:stock_basic:'+self.end
        if self._done(metadata_key) and (self.runtime/'universe.json').exists():
            self.universe=json.loads((self.runtime/'universe.json').read_text())['codes']
            return
        fields=('ts_code','name','industry','list_date','delist_date','list_status','market','exchange')
        records={};excluded=[]
        for exchange in ('SSE','SZSE','BSE'):
            for status in ('L','D','P'):
                for row in self._query('stock_basic',fields,exchange=exchange,list_status=status):
                    symbol=row['ts_code']
                    if not isinstance(symbol,str) or not re.fullmatch(r'\d{6}\.(SH|SZ|BJ)',symbol):
                        excluded.append({'code':str(symbol),'reason':'nonstandard_historical_identity'})
                        continue
                    if row['exchange']!=exchange:
                        raise ValueError('invalid security identity')
                    if a_share(symbol):
                        records[symbol]=row
        if not records:
            raise ValueError('empty official securities metadata')
        with self._db('stock_basic.db') as con:
            con.execute('CREATE TABLE IF NOT EXISTS stock_basic(code TEXT PRIMARY KEY,name TEXT,industry TEXT,ipo_date TEXT,out_date TEXT,status TEXT)')
            con.executemany('INSERT OR REPLACE INTO stock_basic VALUES(?,?,?,?,?,?)',[(code,row['name'],row.get('industry') or '',iso(row['list_date']),iso(row['delist_date']) if row.get('delist_date') else '', '1' if row['list_status']=='L' else '0') for code,row in records.items()])
        weights=self._query('index_weight',('index_code','con_code','trade_date','weight'),index_code=self.cfg['index_code'],
                            start_date=compact((date.fromisoformat(self.end)-timedelta(days=self.cfg['index_max_age_days'])).isoformat()),end_date=compact(self.end))
        snapshots={}
        for row in weights:
            day=iso(row['trade_date'])
            if row['index_code']!=self.cfg['index_code']:
                raise ValueError('wrong HS300 index response')
            if day<=self.end:
                snapshots.setdefault(day,[]).append(row['con_code'])
        if not snapshots:
            raise ValueError('no usable HS300 snapshot')
        effective=max(snapshots); codes=snapshots[effective]
        if len(codes)!=300 or len(set(codes))!=300 or not set(codes)<=set(records):
            raise ValueError('HS300 requires exactly 300 unique known constituents')
        self.universe=sorted(codes)
        payload={'source':'tushare:index_weight','effective_date':effective,'asof':now.isoformat(),'codes':self.universe,'symbols':self.universe,
                 'constituents':[{'symbol':c,'name':records[c]['name'],'industry':records[c].get('industry'),'listed_date':iso(records[c]['list_date'])} for c in self.universe]}
        atomic_json(self.runtime/'universe.json',payload)
        self.manifest['coverage']['stock_basic']={'scope':'current_and_delisted_a_shares','rows':len(records),'start':min(iso(r['list_date']) for r in records.values()),'end':self.end,
                                                 'excluded_metadata':excluded}
        self._complete(metadata_key,len(records),effective_date=effective)

    def _daily_schema(self):
        with self._db('bars.db') as con:
            con.executescript(BAR_SCHEMA)
            con.execute('CREATE TABLE IF NOT EXISTS adjustment_factor(code TEXT,date TEXT,factor REAL,PRIMARY KEY(code,date))')
            con.execute('CREATE TABLE IF NOT EXISTS source_units(name TEXT PRIMARY KEY,volume_unit TEXT,amount_unit TEXT,price_adjustment TEXT)')
            con.execute('CREATE TABLE IF NOT EXISTS index_baseline(code TEXT,date TEXT,close REAL,source TEXT,reason TEXT,payload TEXT,PRIMARY KEY(code,date))')
            con.execute("INSERT OR REPLACE INTO source_units VALUES('tushare','lots','thousand_yuan','none or qfq fixed to bootstrap cutoff; original factors retained')")
        with self._db('daily_basic.db') as con:
            con.execute('CREATE TABLE IF NOT EXISTS daily_basic(code TEXT,date TEXT,trade_date TEXT,'+','.join(name+' REAL' for name in BASIC[2:])+',PRIMARY KEY(code,date))')
        with self._db('hist_mv.db') as con:
            con.execute('CREATE TABLE IF NOT EXISTS hist_mv(month TEXT,code TEXT,circ_mv REAL,PRIMARY KEY(month,code))')
            con.execute('CREATE TABLE IF NOT EXISTS hist_mv_ts(month TEXT,code TEXT,circ_mv REAL,PRIMARY KEY(month,code))')

    def _daily_rows(self,endpoint,fields,day):
        rows=self._query(endpoint,fields,trade_date=compact(day))
        # A full page cannot prove completeness. Continue with generic paging;
        # reject an ignored offset rather than silently sealing a partial day.
        if len(rows)>=6000:
            seen={row['ts_code'] for row in rows};offset=len(rows)
            while True:
                page=self._query(endpoint,fields,trade_date=compact(day),offset=offset,limit=6000)
                if any(row['ts_code'] in seen for row in page):
                    raise ValueError(endpoint+' ignored pagination; completeness unverified')
                rows.extend(page);seen.update(row['ts_code'] for row in page);offset+=len(page)
                if len(page)<6000:
                    break
        by_code={}
        for row in rows:
            code=row['ts_code']
            if not re.fullmatch(r'\d{6}\.(SH|SZ|BJ)',code) or iso(row['trade_date'])!=day or code in by_code:
                raise ValueError('invalid or duplicate daily identity')
            if not a_share(code):
                continue
            exchanges=self.cfg.get('data_exchanges',['SSE','SZSE'])
            if (code.endswith('.SH') and 'SSE' not in exchanges or code.endswith('.SZ') and 'SZSE' not in exchanges
                    or code.endswith('.BJ') and 'BSE' not in exchanges):
                continue
            by_code[code]=row
        return by_code

    def _stock_codes(self):
        with self._db('stock_basic.db') as con:
            return {row[0] for row in con.execute('SELECT code FROM stock_basic')
                    if not row[0].endswith('.BJ') or 'BSE' in self.cfg.get('data_exchanges',[])}

    def _historical_st(self,day):
        st_rows=self._query('stock_st',('ts_code','trade_date','name'),trade_date=compact(day),limit=1000,offset=0)
        seen={row['ts_code'] for row in st_rows};offset=len(st_rows)
        while st_rows and offset%1000==0:
            page=self._query('stock_st',('ts_code','trade_date','name'),trade_date=compact(day),limit=1000,offset=offset)
            if any(row['ts_code'] in seen for row in page):
                raise ValueError('historical ST pagination ignored')
            st_rows.extend(page);seen.update(row['ts_code'] for row in page);offset+=len(page)
            if len(page)<1000:
                break
        if not st_rows or any(iso(row['trade_date'])!=day for row in st_rows):
            raise ValueError('historical ST empty/wrong date response is unverified')
        return {row['ts_code'] for row in st_rows}

    def _daily_bundle(self,day):
        with ThreadPoolExecutor(max_workers=4,thread_name_prefix='research-source') as pool:
            return self._resolve_daily(self._submit_daily(pool,day))

    def _submit_daily(self,pool,day):
        return (pool.submit(self._daily_rows,'daily',DAILY,day),
                pool.submit(self._daily_rows,'adj_factor',('ts_code','trade_date','adj_factor'),day),
                pool.submit(self._daily_rows,'daily_basic',BASIC,day),
                pool.submit(self._historical_st,day) if self._st_enabled else None)

    def _resolve_daily(self,futures):
        raw,factor,basic,st_future=futures
        bars,factors,basics=raw.result(),factor.result(),basic.result()
        st=None
        if st_future:
            try:
                st=st_future.result()
            except Exception:
                if self.cfg['historical_st']=='required':
                    raise
                self._st_enabled=False
                self._warn('Historical ST unavailable: is_st stays NULL for unverified dates; current names are never backfilled')
        return bars,factors,basics,st

    def _prefetched_daily(self,days):
        # The bounded window overlaps HTTP latency across three dates. The
        # shared _query lock still spaces every request start by >=0.35s.
        # Workers never publish state, write SQLite, or seal a checkpoint.
        iterator=iter(days);pending={}
        with ThreadPoolExecutor(max_workers=12,thread_name_prefix='research-prefetch') as pool:
            self._check_stop()
            def fill():
                while len(pending)<3:
                    day=next(iterator,None)
                    if day is None:
                        break
                    pending[day]=self._submit_daily(pool,day)
            fill()
            while pending:
                day=next(iter(pending));futures=pending.pop(day)
                self.state['unit']=day;self._save()
                yield day,self._resolve_daily(futures)
                if self.stop_event.is_set():
                    return
                fill()

    def daily(self):
        self._daily_schema()
        self._start_stage('daily')
        self._progress('daily','downloading_raw_daily',('daily:'+day for day in self.days))
        month_ends={day[:7]:day for day in self.days}
        pending_days=[day for day in self.days if not self._done('daily:'+day)]
        for day,bundle in self._prefetched_daily(pending_days):
            key='daily:'+day
            self.state['unit']=day;self._save()
            bars,factors,basics,st=bundle
            if len(bars)<self.cfg['min_daily_rows'] or not set(bars)<=set(factors) or not set(bars)<=set(basics):
                raise ValueError('daily/adjustment/basic coverage incomplete for '+day)
            tuples=[];factor_rows=[];basic_rows=[];mv_rows=[]
            for code,row in bars.items():
                prices=[number(row[field],False) for field in ('open','high','low','close','pre_close')]
                if any(v<=0 for v in prices) or prices[2]>min(prices[:2]+prices[3:4]) or prices[1]<max(prices[0],prices[3]):
                    raise ValueError('invalid daily OHLC')
                factor=number(factors[code]['adj_factor'],False)
                if factor<=0:
                    raise ValueError('invalid adjustment factor')
                basic=basics[code]
                if number(row['vol'],False)<0 or number(row['amount'],False)<0:
                    raise ValueError('negative source volume/amount')
                tuples.append((code,day,*prices,number(row['vol'],False),number(row['amount'],False),number(basic['turnover_rate']),number(row['pct_chg']),int(code in st) if st is not None else None,'none','tushare'))
                factor_rows.append((code,day,factor))
                basic_rows.append((code,day,compact(day),*(number(basic[field]) for field in BASIC[2:])))
                if month_ends[day[:7]]==day and number(basic['circ_mv']) is not None:
                    mv_rows.append((day[:7],code,number(basic['circ_mv'])/10000))
            with self._db('bars.db') as con:
                con.execute('ATTACH DATABASE ? AS dbasic',(str(self.cache/'daily_basic.db'),))
                con.execute('ATTACH DATABASE ? AS hmv',(str(self.cache/'hist_mv.db'),))
                con.executemany('INSERT OR REPLACE INTO daily_bar VALUES('+','.join('?'*14)+')',tuples)
                con.executemany('INSERT OR REPLACE INTO adjustment_factor VALUES(?,?,?)',factor_rows)
                con.executemany('INSERT OR REPLACE INTO dbasic.daily_basic VALUES('+','.join('?'*(len(BASIC)+1))+')',basic_rows)
                for table in ('hist_mv','hist_mv_ts'):
                    con.executemany('INSERT OR REPLACE INTO hmv.'+table+' VALUES(?,?,?)',mv_rows)
            self._complete(key,len(tuples),historical_st_verified=st is not None)
        self._qfq()
        self._daily_panel()
        stock_codes=self._stock_codes()
        with self._db('bars.db') as con:
            con.execute("INSERT OR REPLACE INTO bar_meta SELECT code,adjust,MIN(date),MAX(date),COUNT(*),? FROM daily_bar GROUP BY code,adjust",(self.now().isoformat(),))
            con.execute('CREATE TEMP TABLE research_stocks(code TEXT PRIMARY KEY)')
            con.executemany('INSERT INTO research_stocks VALUES(?)',[(c,) for c in stock_codes])
            count,start,end=con.execute("SELECT COUNT(*),MIN(date),MAX(date) FROM daily_bar JOIN research_stocks USING(code) WHERE source='tushare' AND adjust='qfq'").fetchone()
        self.manifest['coverage']['daily']={'scope':'all_sh_sz_a_shares_including_delisted' if 'BSE' not in self.cfg.get('data_exchanges',[]) else 'all_a_shares_including_delisted','rows':count,'start':start,'end':end,'days':len(self.days),
                                           'adjustment':'qfq fixed bootstrap-end anchor; raw none prices and per-date factors retained',
                                           'historical_st_verified':all(v.get('historical_st_verified') for k,v in self.manifest['completed'].items() if k.startswith('daily:'))}
        for filename,table,column,key in (('daily_basic.db','daily_basic','date','daily_basic'),('hist_mv.db','hist_mv','month','hist_mv')):
            with self._db(filename) as con:
                rows,first,last=con.execute(f'SELECT COUNT(*),MIN({column}),MAX({column}) FROM {table}').fetchone()
            self.manifest['coverage'][key]={'scope':self.manifest['coverage']['daily']['scope'],'rows':rows,'start':first,'end':last,
                                           'units':'official Tushare daily_basic units (market cap in ten-thousand yuan)' if key=='daily_basic' else 'circ_mv in hundred-million yuan; last available session per month'}
        self._finish_stage('daily')

    def _qfq(self,codes=None):
        self.state['phase']='building_qfq';self._save()
        if codes is None:
            stock_codes=self._stock_codes()
            with self._db('bars.db') as con:
                codes=[row[0] for row in con.execute("SELECT DISTINCT code FROM daily_bar WHERE adjust='none' AND source='tushare'") if row[0] in stock_codes]
        ordered=sorted(codes,key=lambda code:(code not in set(self.universe),code))
        self._progress(self.state.get('stage','daily'),'building_qfq',('qfq:'+code+':'+self.end for code in ordered))
        for code in ordered:
            key='qfq:'+code+':'+self.end
            if self._done(key):
                continue
            self.state['unit']=code
            with self._db('bars.db') as con:
                anchor=con.execute('SELECT factor FROM adjustment_factor WHERE code=? AND date<=? ORDER BY date DESC LIMIT 1',(code,self.end)).fetchone()
                if not anchor or anchor[0]<=0:
                    raise ValueError('missing qfq anchor')
                raw=con.execute("SELECT b.date,b.open,b.high,b.low,b.close,b.preclose,b.volume,b.amount,b.turn,b.pct_chg,b.is_st,a.factor FROM daily_bar b JOIN adjustment_factor a ON a.code=b.code AND a.date=b.date WHERE b.code=? AND b.adjust='none' AND b.source='tushare' AND b.date<=? ORDER BY b.date",(code,self.end)).fetchall()
                rows=[];previous=None
                for day,op,hi,lo,cl,pre,vol,amt,turn,pct,st,factor in raw:
                    ratio=factor/anchor[0];adjusted=[value*ratio for value in (op,hi,lo,cl)]
                    pre_adjusted=previous if previous is not None else pre*ratio
                    change=(adjusted[3]/pre_adjusted-1)*100 if pre_adjusted else None
                    rows.append((code,day,*adjusted,pre_adjusted,vol,amt,turn,change,st,'qfq','tushare'))
                    previous=adjusted[3]
                con.executemany('INSERT OR REPLACE INTO daily_bar VALUES('+','.join('?'*14)+')',rows)
            self._complete(key,len(rows))

    def _atomic_parquet(self,frame,path):
        path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
        temporary=path.with_suffix(path.suffix+'.tmp')
        try:
            frame.to_parquet(temporary,index=True)
            os.replace(temporary,path)
        finally:
            if temporary.exists():
                temporary.unlink()

    def _daily_panel(self):
        import pandas as pd
        import numpy as np
        self.state['phase']='building_daily_close_panel';self._save()
        stock_codes=self._stock_codes()
        with self._db('bars.db') as con:
            dates=[row[0] for row in con.execute("SELECT DISTINCT date FROM daily_bar WHERE adjust='qfq' AND source='tushare' ORDER BY date")]
            codes=[row[0] for row in con.execute("SELECT DISTINCT code FROM daily_bar WHERE adjust='qfq' AND source='tushare' ORDER BY code") if row[0] in stock_codes]
            self._progress_keys=None
            self.state.update(total_units=len(codes),completed_units=0)
            # Build column-wise instead of loading millions of Python records.
            panel=pd.DataFrame(np.nan,index=pd.to_datetime(dates),columns=codes)
            for i,code in enumerate(codes,1):
                values=con.execute("SELECT date,close FROM daily_bar WHERE code=? AND adjust='qfq' AND source='tushare' ORDER BY date",(code,)).fetchall()
                panel.loc[pd.to_datetime([r[0] for r in values]),code]=[r[1] for r in values]
                self.state.update(unit=code,completed_units=i)
                if i%100==0 or i==len(codes):
                    self._save()
        panel.index.name='date'
        self._atomic_parquet(panel,self.cache/'daily_close.parquet')
        self.manifest['coverage']['daily_close']={'scope':'all_sh_sz_a_shares_including_delisted' if 'BSE' not in self.cfg.get('data_exchanges',[]) else 'all_a_shares_including_delisted','rows':int(panel.count().sum()),
                                                  'start':dates[0] if dates else None,'end':dates[-1] if dates else None,'symbols':len(codes)}

    def _start_stage(self,stage):
        self.state['stages'][stage]='running';self.manifest['stages']=dict(self.state['stages']);self._save()

    def _finish_stage(self,stage):
        self.state['stages'][stage]='complete';self.manifest['stages']=dict(self.state['stages'])
        self.manifest.setdefault('stage_dates',{})[stage]=self.end
        self._save()

    def _pages(self,endpoint,*,fields=(),limit=3000,**params):
        rows=[];seen=set();offset=0
        while True:
            page=self._query(endpoint,fields,limit=limit,offset=offset,**params)
            keys=[hashlib.sha256(json.dumps(clean_json(row),sort_keys=True,default=str).encode()).hexdigest() for row in page]
            if page and set(keys)<=seen:
                raise ValueError(endpoint+' returned duplicate pagination; completeness unverified')
            for row,key in zip(page,keys):
                if key not in seen:
                    rows.append(row);seen.add(key)
            offset+=len(page)
            # Probe the next offset even after a short nonempty page. Some
            # permission tiers cap below the requested limit; short != final.
            if not page:
                return rows
            if offset>200000:
                raise ValueError(endpoint+' exceeded expected page bounds')

    def finance(self):
        if not self.cfg.get('finance_use_vip') or self.cfg.get('finance_scope')!='all_market':
            raise ValueError('This bootstrap requires verified all-market financial VIP permissions')
        self._start_stage('finance')
        periods=quarters(self.cfg['finance_start'],self.end)
        apis=('income','balancesheet','cashflow','fina_indicator')
        pending=[];latest_rows={}
        self._progress('finance','downloading_financial_reports',('finance:'+api+':'+period+':'+self.end for period in periods for api in apis))
        with self._db('financial_raw.db') as con:
            con.execute('CREATE TABLE IF NOT EXISTS financial_raw(api TEXT,code TEXT,period TEXT,pub_date TEXT,version TEXT,payload TEXT,PRIMARY KEY(api,version))')
            con.execute('CREATE INDEX IF NOT EXISTS financial_raw_code ON financial_raw(code,api,period)')
            con.execute('CREATE TABLE IF NOT EXISTS financial_quarantine(api TEXT,code TEXT,period TEXT,pub_date TEXT,reason TEXT,version TEXT,payload TEXT,first_seen_at TEXT,last_seen_at TEXT,PRIMARY KEY(api,version))')
        for period in periods:
            for api in apis:
                key='finance:'+api+':'+period+':'+self.end
                if self._done(key):
                    if period==periods[-1]:
                        latest_rows[api]=self.manifest['completed'][key]['rows']
                    continue
                self.state['unit']=api+':'+period;self._save()
                rows=self._pages(api+'_vip',period=period)
                if not rows:
                    prior=periods[-2] if len(periods)>1 else None
                    prior_key='finance:'+api+':'+str(prior)+':'+self.end
                    with self._db('financial_raw.db') as con:
                        previous=con.execute('SELECT COUNT(*) FROM financial_raw WHERE api=? AND period=?',(api,prior)).fetchone()[0] if prior else 0
                    if period==periods[-1] and previous>0 and self._done(prior_key):
                        # A successful empty response for the newest quarter is
                        # not a fabricated report and is never sealed. A later
                        # retry/day queries it again, unlike completed slices.
                        pending.append({'api':api,'period':period,'rows':0,'status':'pending_disclosure',
                                        'checked_cutoff':self.end,'verified_prior_period':prior,'verified_prior_rows':previous})
                        latest_rows[api]=0
                        self.state['pending_disclosures']=list(pending)
                        self._disclosure_coverage(periods[-1],pending,latest_rows)
                        self._save()
                        continue
                    raise ValueError(api+' returned no reports for '+period)
                tuples=[];withheld=0;quarantined=[]
                for row in rows:
                    if not {'ts_code','end_date','ann_date'}<=set(row):
                        raise ValueError(api+' report identity/publication fields missing')
                    if not a_share(row['ts_code']):
                        continue
                    if compact(row['end_date'])!=period:
                        raise ValueError(api+' wrong report period')
                    pub=iso(row.get('f_ann_date') or row['ann_date'])
                    payload=json.dumps(clean_json(row),sort_keys=True,ensure_ascii=False,allow_nan=False)
                    version=hashlib.sha256(payload.encode()).hexdigest()
                    if pub<iso(row['end_date']):
                        seen=self.now().isoformat()
                        quarantined.append((api,row['ts_code'],period,pub,'publication_predates_period',version,payload,seen,seen))
                        continue
                    if pub>self.end:
                        withheld+=1;continue
                    tuples.append((api,row['ts_code'],period,pub,version,payload))
                with self._db('financial_raw.db') as con:
                    con.executemany('INSERT OR IGNORE INTO financial_raw VALUES(?,?,?,?,?,?)',tuples)
                    con.executemany('INSERT INTO financial_quarantine VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(api,version) DO UPDATE SET last_seen_at=excluded.last_seen_at',quarantined)
                if quarantined:
                    self._warn('Official financial source contains impossible publication dates; quarantined original versions are excluded from research views')
                self._quarantine_coverage()
                if not tuples:
                    self._save()
                    raise ValueError(api+' no valid already-published A-share reports for '+period)
                valid_codes={row[1] for row in tuples}
                affected={row[1] for row in quarantined}&set(self.universe)
                if period==periods[-1]:
                    latest_rows[api]=len(tuples)
                self._complete(key,len(tuples),future_publications_withheld=withheld,quarantined_rows=len(quarantined),
                               quarantined_current_hs300=sorted(affected),current_hs300_without_valid_period_report=sorted(affected-valid_codes))
        self._disclosure_coverage(periods[-1] if periods else None,pending,latest_rows)
        self._finance_views()
        self._finish_stage('finance')

    def _disclosure_coverage(self,period,pending,available):
        self.state['pending_disclosures']=list(pending)
        self.manifest['coverage']['financial_disclosure']={'scope':'newest_quarter_official_response_availability',
                'rows':sum(available.values()),'start':iso(period) if period else None,'end':iso(period) if period else None,
                'checked_cutoff':self.end,'status':'pending_disclosure' if pending else 'available',
                'available_rows_by_api':dict(available),'pending':list(pending),
                'note':'Newest-quarter successful empty responses remain unsealed and are queried again; stored reports and dates are never fabricated.'}

    def _quarantine_coverage(self):
        with self._db('financial_raw.db') as con:
            count,first,last=con.execute('SELECT COUNT(*),MIN(period),MAX(period) FROM financial_quarantine').fetchone()
            by_api=dict(con.execute('SELECT api,COUNT(*) FROM financial_quarantine GROUP BY api'))
            rows=con.execute('SELECT api,code,period,pub_date,reason FROM financial_quarantine ORDER BY period,code,api LIMIT 50').fetchall()
            affected={row[0] for row in con.execute('SELECT DISTINCT code FROM financial_quarantine')}&set(self.universe)
        self.manifest['coverage']['financial_quarantine']={'scope':'unusable_original_official_financial_versions','rows':count,
                'start':iso(first) if first else None,'end':iso(last) if last else None,'rows_by_api':by_api,
                'current_hs300_affected':sorted(affected),'excluded_from_financial_raw_and_views':True,
                'records':[dict(zip(('api','code','period','publication_date','reason'),row)) for row in rows],
                'records_truncated':count>len(rows)}

    @staticmethod
    def _latest_reports(rows):
        chosen={}
        for row in rows:
            # Cumulative consolidated reports only; do not mix single-quarter
            # or parent-only tables into cumulative accounting differences.
            report=str(row.get('report_type','1')).removesuffix('.0')
            if report not in ('1','4','5'):
                continue
            period=compact(row['end_date']);pub=iso(row.get('f_ann_date') or row['ann_date'])
            rank=(pub,report!='5',str(row.get('update_flag','0')))
            if period not in chosen or rank>chosen[period][0]:
                chosen[period]=(rank,row)
        return {period:row for period,(_,row) in chosen.items()}

    def _finance_views(self):
        import pandas as pd
        self.state['phase']='materializing_financial_views';self._save()
        with self._db('finance.db') as con:
            con.execute('CREATE TABLE IF NOT EXISTS finance_report(code TEXT,period TEXT,net_profit REAL,revenue REAL,sq_net_profit REAL,sq_net_yoy REAL,sq_revenue REAL,sq_rev_yoy REAL,eps REAL,roe REAL,source TEXT,pub_date TEXT,PRIMARY KEY(code,period))')
            if 'pub_date' not in {r[1] for r in con.execute('PRAGMA table_info(finance_report)')}:
                con.execute('ALTER TABLE finance_report ADD COLUMN pub_date TEXT')
        with self._db('finance_quality.db') as con:
            con.execute('CREATE TABLE IF NOT EXISTS quality(code TEXT,period TEXT,roe_avg REAL,gp_margin REAL,np_margin REAL,current_ratio REAL,liability_to_asset REAL,cfo_to_np REAL,cfo_to_or REAL,pub_date TEXT,PRIMARY KEY(code,period))')
        with self._db('finance_ts.db') as con:
            con.execute('CREATE TABLE IF NOT EXISTS financials_ts(code TEXT,end_date TEXT,total_hldr_eqy_exc_min_int REAL,ann_date TEXT,f_ann_date TEXT,ts_code TEXT,PRIMARY KEY(code,end_date))')
        with self._db('financial_raw.db') as con:
            codes=[r[0] for r in con.execute('SELECT DISTINCT code FROM financial_raw ORDER BY code')]
        self._progress_keys=None
        self.state.update(total_units=len(codes),completed_units=0)
        coverage={api:{'scope':'all_market_available_reports','rows':0,'symbols':0,'start':None,'end':None} for api in ('income','balancesheet','cashflow','fina_indicator')}
        annual_coverage={};missing=[]
        for i,code in enumerate(sorted(codes,key=lambda c:(c not in set(self.universe),c)),1):
            selected={}
            with self._db('financial_raw.db') as con:
                for api in coverage:
                    rows=[json.loads(r[0]) for r in con.execute('SELECT payload FROM financial_raw WHERE code=? AND api=? ORDER BY period,pub_date,version',(code,api))]
                    selected[api]=self._latest_reports(rows)
                    if rows:
                        self._atomic_parquet(pd.DataFrame(rows),self.parquet/api/'data'/f'{api}__ts_code={code}.parquet')
                        info=coverage[api];info['rows']+=len(rows);info['symbols']+=1
                        first=min(iso(r['end_date']) for r in rows);last=max(iso(r['end_date']) for r in rows)
                        info['start']=min(info['start'] or first,first);info['end']=max(info['end'] or last,last)
            income=selected['income'];indicator=selected['fina_indicator'];cash=selected['cashflow'];balance=selected['balancesheet']
            common=set(income)&set(cash)&set(balance)
            annual_coverage[code]=sorted(p for p in common if p.endswith('1231'))
            financial=[];quality=[];nav=[];quarter={}
            for period,row in sorted(income.items()):
                net=number(row.get('n_income_attr_p'))
                if net is None:
                    net=number(row.get('n_income'))
                revenue=number(row.get('revenue'))
                if revenue is None:
                    revenue=number(row.get('total_revenue'))
                previous={'0630':'0331','0930':'0630','1231':'0930'}.get(period[4:])
                prior=quarter.get(period[:4]+previous) if previous else None
                sqn=net if previous is None else net-prior[0] if prior and net is not None and prior[0] is not None else None
                sqr=revenue if previous is None else revenue-prior[1] if prior and revenue is not None and prior[1] is not None else None
                lastyear=quarter.get(str(int(period[:4])-1)+period[4:])
                yoy=lambda value,base:(value-base)/abs(base) if value is not None and base is not None and base!=0 else None
                quarter[period]=(net,revenue,sqn,sqr)
                ind=indicator.get(period,{})
                pub=max(iso(r.get('f_ann_date') or r['ann_date']) for r in (row,ind) if r)
                if prior:
                    pub=max(pub,iso(income[period[:4]+previous].get('f_ann_date') or income[period[:4]+previous]['ann_date']))
                if lastyear:
                    oldperiod=str(int(period[:4])-1)+period[4:]
                    dependencies=[income[oldperiod]]
                    if previous and oldperiod[:4]+previous in income:
                        dependencies.append(income[oldperiod[:4]+previous])
                    pub=max(pub,*(iso(r.get('f_ann_date') or r['ann_date']) for r in dependencies))
                financial.append((code[:6],iso(period),net/1e8 if net is not None else None,revenue/1e8 if revenue is not None else None,
                                  sqn/1e8 if sqn is not None else None,yoy(sqn,lastyear[2] if lastyear else None),
                                  sqr/1e8 if sqr is not None else None,yoy(sqr,lastyear[3] if lastyear else None),number(row.get('basic_eps')),percentage_ratio(ind.get('roe')),'tushare',pub))
            for period,ind in sorted(indicator.items()):
                inc=income.get(period,{});cf=cash.get(period,{})
                net=number(inc.get('n_income'));revenue=number(inc.get('revenue'))
                if revenue is None:
                    revenue=number(inc.get('total_revenue'))
                cfo=number(cf.get('n_cashflow_act'))
                ratio=lambda a,b:a/b if a is not None and b is not None and b!=0 else None
                pct=lambda field:percentage_ratio(ind.get(field))
                pub=max(iso(r.get('f_ann_date') or r['ann_date']) for r in (ind,inc,cf) if r)
                quality.append((code,iso(period),pct('roe'),pct('grossprofit_margin'),pct('netprofit_margin'),number(ind.get('current_ratio')),
                                pct('debt_to_assets'),ratio(cfo,net),ratio(cfo,revenue),pub))
            for period,row in balance.items():
                nav.append((code[:6],period,number(row.get('total_hldr_eqy_exc_min_int')),row.get('ann_date'),row.get('f_ann_date'),code))
            with self._db('finance.db') as con:
                con.executemany('INSERT OR REPLACE INTO finance_report(code,period,net_profit,revenue,sq_net_profit,sq_net_yoy,sq_revenue,sq_rev_yoy,eps,roe,source,pub_date) VALUES('+','.join('?'*12)+')',financial)
            with self._db('finance_quality.db') as con:
                con.executemany('INSERT OR REPLACE INTO quality VALUES('+','.join('?'*10)+')',quality)
            with self._db('finance_ts.db') as con:
                con.executemany('INSERT OR REPLACE INTO financials_ts VALUES(?,?,?,?,?,?)',nav)
            self.state.update(unit=code,completed_units=i)
            if i%10==0 or i==len(codes):
                self._save()
            self._check_stop()
            annual=annual_coverage[code]
            consecutive=len(annual)>=2 and int(annual[-1][:4])-int(annual[-2][:4])==1
            if code in self.universe and not consecutive:
                missing.append(code)
        for api,info in coverage.items():
            if not info['rows']:
                raise ValueError('empty financial output '+api)
            self.manifest['coverage'][api]=info
        for filename,table,key in (('finance.db','finance_report','finance'),('finance_quality.db','quality','quality'),('finance_ts.db','financials_ts','finance_ts')):
            column='end_date' if table=='financials_ts' else 'period'
            with self._db(filename) as con:
                count,start,end=con.execute(f'SELECT COUNT(*),MIN({column}),MAX({column}) FROM {table}').fetchone()
            self.manifest['coverage'][key]={'scope':'all_market_available_reports','rows':count,'start':iso(start) if start else None,'end':iso(end) if end else None,
                                           'point_in_time':'publication dates retained; compatibility view contains latest announced versions; original versions in financial_raw/parquet'}
        self.manifest['coverage']['beneish']={'scope':'current_hs300_evaluation_on_all_market_financials','rows':sum(len(v) for v in annual_coverage.values()),
                                              'start':coverage['income']['start'],'end':coverage['income']['end'],'requested_symbols':len(self.universe),
                                              'symbols_with_two_annual_three_table_reports':len(self.universe)-len(set(self.universe)-set(codes)|set(missing)),
                                              'missing_two_annual_reports':sorted(set(self.universe)-set(codes)|set(missing))}

    def assets(self):
        self._daily_schema()
        self._start_stage('assets')
        pool=yaml.safe_load(Path(self.cfg['etf_pool_file']).read_text()).get('pool',[])
        etfs=[]
        for item in pool:
            code=str(item['code'])
            if not re.fullmatch(r'\d{6}',code) or not code.startswith(('1','5')):
                raise ValueError('invalid configured ETF code')
            etfs.append(code+('.SH' if code.startswith('5') else '.SZ'))
        if not etfs or len(etfs)!=len(set(etfs)):
            raise ValueError('ETF universe empty or duplicated')
        indices=list(self.cfg['index_symbols'])
        if len(indices)!=len(set(indices)) or any(not re.fullmatch(r'\d{6}\.(SH|SZ)',c) for c in indices):
            raise ValueError('invalid configured index universe')
        self._progress('assets','downloading_index_etf',('assets:'+api+':'+code+':'+start+':'+self.end for api,codes,start in
                       (('index_daily',indices,self.cfg['index_start']),('fund_daily',etfs,self.cfg['daily_start'])) for code in codes))
        for endpoint,codes,start in (('index_daily',indices,self.cfg['index_start']),('fund_daily',etfs,self.cfg['daily_start'])):
            for code in codes:
                for first,last in [(start,self.end)]:
                    key='assets:'+endpoint+':'+code+':'+first+':'+last
                    if self._done(key):
                        continue
                    prefix='assets:'+endpoint+':'+code+':'+first+':'
                    earlier=[name[len(prefix):] for name in self.manifest['completed'] if name.startswith(prefix) and name[len(prefix):]<last]
                    fetch_first=(date.fromisoformat(max(earlier))+timedelta(days=1)).isoformat() if earlier else first
                    self.state['unit']=key;self._save()
                    rows=self._pages(endpoint,fields=DAILY,limit=5000,ts_code=code,start_date=compact(fetch_first),end_date=compact(last))
                    by_date={}
                    for row in rows:
                        day=iso(row['trade_date'])
                        if row['ts_code']!=code or not fetch_first<=day<=last or day in by_date:
                            raise ValueError(endpoint+' invalid asset/date identity')
                        by_date[day]=row
                    factors={}
                    if endpoint=='fund_daily':
                        for row in self._pages('fund_adj',fields=('ts_code','trade_date','adj_factor'),limit=2000,
                                               ts_code=code,start_date=compact(fetch_first),end_date=compact(last)):
                            day=iso(row['trade_date']);factor=number(row['adj_factor'],False)
                            if row['ts_code']!=code or not fetch_first<=day<=last or day in factors or factor<=0:
                                raise ValueError('invalid ETF factor identity/value')
                            factors[day]=factor
                        if not set(by_date)<=set(factors):
                            raise ValueError('ETF daily/fund_adj coverage incomplete for '+code)
                    tuples=[];baselines=[]
                    with self._db('bars.db') as con:
                        prior_prices=con.execute("SELECT COUNT(*) FROM daily_bar WHERE code=? AND adjust='none'",(code,)).fetchone()[0]
                    first_source_day=min(by_date,default=None)
                    for day,row in by_date.items():
                        close_only=(endpoint=='index_daily' and fetch_first==first and not prior_prices and day==first_source_day
                                    and all(row.get(f) in (None,'') for f in ('open','high','low','pre_close','vol','amount')))
                        if close_only:
                            close=number(row['close'],False)
                            if close<=0:
                                raise ValueError('invalid initial index baseline')
                            baselines.append((code,day,close,'tushare:index_daily','source_initial_close_only',json.dumps(clean_json(row),ensure_ascii=False,sort_keys=True,allow_nan=False)))
                            continue
                        prices=[number(row[f],False) for f in ('open','high','low','close')]
                        pre=number(row['pre_close'],endpoint=='index_daily')
                        volume=number(row['vol'],False);amount=number(row['amount'],False)
                        if any(v<=0 for v in prices) or pre is not None and pre<=0 or prices[2]>min(prices[0],prices[3]) or prices[1]<max(prices[0],prices[3]) or min(volume,amount)<0:
                            raise ValueError(endpoint+' invalid OHLC/quantity')
                        tuples.append((code,day,*prices,pre,volume,amount,None,number(row['pct_chg']) if pre is not None else None,None,'none','tushare'))
                    with self._db('bars.db') as con:
                        con.executemany('INSERT OR REPLACE INTO daily_bar VALUES('+','.join('?'*14)+')',tuples)
                        con.executemany('INSERT OR REPLACE INTO adjustment_factor VALUES(?,?,?)',[(code,day,factor) for day,factor in factors.items()])
                        con.executemany('INSERT OR REPLACE INTO index_baseline VALUES(?,?,?,?,?,?)',baselines)
                    self._complete(key,len(tuples),fetched_start=fetch_first,fetched_end=last,history_start=first,history_end=last,
                                   initial_close_only_baselines=len(baselines))
        with self._db('bars.db') as con:
            for code in indices+etfs:
                count=con.execute("SELECT COUNT(*) FROM daily_bar WHERE code=? AND source='tushare' AND adjust='none'",(code,)).fetchone()[0]
                if not count:
                    raise ValueError('configured index/ETF has no real history: '+code)
        self._qfq(etfs)
        with self._db('bars.db') as con:
            con.execute("INSERT OR REPLACE INTO bar_meta SELECT code,adjust,MIN(date),MAX(date),COUNT(*),? FROM daily_bar GROUP BY code,adjust",(self.now().isoformat(),))
            for label,codes,adjust in (('index',indices,'none'),('etf',etfs,'qfq')):
                rows,start,end=con.execute('SELECT COUNT(*),MIN(date),MAX(date) FROM daily_bar WHERE code IN ('+','.join('?'*len(codes))+') AND source=? AND adjust=?',(*codes,'tushare',adjust)).fetchone()
                self.manifest['coverage'][label]={'scope':'configured_'+label+'_universe','rows':rows,'start':start,'end':end,'symbols':len(codes),
                                                  'adjustment':'none (price index)' if label=='index' else 'fund_daily * dated fund_adj / latest fund_adj; original prices and factors retained',
                                                  'codes':codes}
            rows,start,end=con.execute('SELECT COUNT(*),MIN(date),MAX(date) FROM index_baseline').fetchone()
            self.manifest['coverage']['index_baseline']={'scope':'source_initial_index_close_only_points','rows':rows,'start':start,'end':end,
                                                         'excluded_from_daily_bar':True,'not_ohlc_or_tradable_bars':True}
        self._finish_stage('assets')

    def _minute_rows(self,code,first,last):
        fields=('ts_code','trade_time','open','high','low','close','vol','amount')
        rows=self._query('stk_mins',fields,ts_code=code,freq='5min',start_date=first+' 09:00:00',end_date=last+' 15:00:00')
        if len(rows)>=8000:
            begin=date.fromisoformat(first);end=date.fromisoformat(last)
            if begin==end:
                raise ValueError('minute page truncated within one session')
            middle=begin+(end-begin)//2
            return self._minute_rows(code,first,middle.isoformat())+self._minute_rows(code,(middle+timedelta(days=1)).isoformat(),last)
        return rows

    def minute(self):
        import calendar
        self._start_stage('minute')
        end=date.fromisoformat(self.end)
        absolute=end.year*12+end.month-1-int(self.cfg['minute_months'])
        year,month=divmod(absolute,12);month+=1
        start=date(year,month,min(end.day,calendar.monthrange(year,month)[1])).isoformat()
        self._minute_window=(start,self.end,tuple(self.universe))
        open_days=set(self.calendar(start,self.end))
        self._progress('minute','downloading_5min',('minute:'+code+':'+start+':'+self.end for code in self.universe))
        with self._db('minute.db') as con:
            con.execute('CREATE TABLE IF NOT EXISTS minute_5m(code TEXT,trade_time TEXT,open REAL,high REAL,low REAL,close REAL,volume REAL,amount REAL,source TEXT,PRIMARY KEY(code,trade_time))')
            con.execute('CREATE TABLE IF NOT EXISTS minute_metadata(name TEXT PRIMARY KEY,value TEXT)')
            con.executemany('INSERT OR REPLACE INTO minute_metadata VALUES(?,?)',[('frequency','5min'),('scope','current_hs300'),('volume_unit','shares'),('amount_unit','yuan'),('source','tushare:stk_mins')])
        empty=[]
        for code in self.universe:
            key='minute:'+code+':'+start+':'+self.end
            if self._done(key):
                if self.manifest['completed'][key]['rows']==0:
                    empty.append(code)
                continue
            self.state['unit']=code;self._save()
            rows=self._minute_rows(code,start,self.end)
            tuples=validate_minute_rows(code,rows,start,self.end,open_days)
            if not tuples:
                # Empty history can be a real full-window suspension, but an
                # API silently returning nothing while daily traded is failure.
                with self._db('bars.db') as con:
                    try:
                        traded=con.execute("SELECT COUNT(*) FROM daily_bar WHERE code=? AND date BETWEEN ? AND ? AND adjust='none' AND source='tushare'",(code,start,self.end)).fetchone()[0]
                    except sqlite3.OperationalError:
                        raise ValueError('empty minute response needs verified daily coverage') from None
                if traded:
                    raise ValueError('empty minute history despite observed daily trading: '+code)
                if not all(self._done('daily:'+day) for day in open_days):
                    raise ValueError('empty minute response cannot verify full-window suspension')
                empty.append(code)
            with self._db('minute.db') as con:
                con.executemany('INSERT OR REPLACE INTO minute_5m VALUES('+','.join('?'*9)+')',tuples)
            self._complete(key,len(tuples))
        with self._db('minute.db') as con:
            scope='code IN ('+','.join('?'*len(self.universe))+') AND trade_time BETWEEN ? AND ?'
            params=(*self.universe,start+' 00:00:00',self.end+' 23:59:59')
            rows,first,last=con.execute('SELECT COUNT(*),MIN(trade_time),MAX(trade_time) FROM minute_5m WHERE '+scope,params).fetchone()
            actual=con.execute('SELECT COUNT(DISTINCT code) FROM minute_5m WHERE '+scope,params).fetchone()[0]
        if not rows:
            raise ValueError('no real minute history downloaded')
        self.manifest['coverage']['minute']={'scope':'current_hs300','frequency':'5min','volume_unit':'shares','amount_unit':'yuan','rows':rows,
                                            'start':first,'end':last,'requested_start':start,'requested_end':self.end,
                                            'requested_symbols':len(self.universe),'expected_symbols':len(self.universe),
                                            'symbols_with_rows':actual,'covered_symbols':actual,'verified_no_daily_trades':empty,
                                            'status':'complete','coverage_complete':True,'rate_limited':False,'retry_at':None}
        self._finish_stage('minute')

    def _partial_minute_coverage(self,source):
        if not hasattr(self,'_minute_window') or not (self.cache/'minute.db').exists():
            return
        start,end,codes=self._minute_window
        with self._db('minute.db') as con:
            clause='code IN ('+','.join('?'*len(codes))+') AND trade_time BETWEEN ? AND ?'
            params=(*codes,start+' 00:00:00',end+' 23:59:59')
            count,first,last,covered=con.execute('SELECT COUNT(*),MIN(trade_time),MAX(trade_time),COUNT(DISTINCT code) FROM minute_5m WHERE '+clause,params).fetchone()
        self.manifest['coverage']['minute']={'scope':'current_hs300','frequency':'5min','volume_unit':'shares','amount_unit':'yuan',
                'rows':count,'start':first,'end':last,'requested_start':start,'requested_end':end,
                'requested_symbols':len(codes),'expected_symbols':len(codes),'symbols_with_rows':covered,'covered_symbols':covered,
                'status':source.category,'coverage_complete':False,'rate_limited':source.category=='rate_limited',
                'retry_at':source.retry_at,'cooldown_seconds':self._source_cooldowns.get(source.endpoint,{}).get('cooldown_seconds')}

    def run(self, stages=('daily',), *, target_date=None):
        self._end_limit=iso(target_date) if target_date is not None else None
        self.runtime.mkdir(parents=True,exist_ok=True)
        # Reserve the pipeline lock while mutating its input caches. This also
        # prevents a standalone CLI bootstrap from racing a running analysis.
        with ProcessLock(self.runtime/'bootstrap.lock'),ProcessLock(self.runtime/'pipeline.lock'):
            try:
                self._check_stop()
                self.prepare()
                for stage in stages:
                    method=getattr(self,stage,None)
                    if method is None:
                        raise ValueError('stage not implemented: '+stage)
                    method()
                complete=all(self.state['stages'].get(stage)=='complete' for stage in ('daily','finance','assets','minute'))
                self.state.update({'status':'complete' if complete else 'partial_complete','phase':'requested_stages_complete','last_error':None,
                                   'requested_stages':list(stages)})
                self._save()
                return self.state
            except ResearchInterrupted:
                self.state.update({'status':'interrupted','phase':'interrupted_after_checkpoint','last_error':None,
                                   'interruption_reason':'maintenance signal; resume from committed checkpoints'})
                self._save()
                return self.state
            except ResearchSourceBlocked as exc:
                stage=self.state.get('stage')
                if stage:
                    self.state['stages'][stage]=exc.category
                    self.manifest['stages']=dict(self.state['stages'])
                if stage=='minute' and exc.endpoint=='stk_mins':
                    self._partial_minute_coverage(exc)
                self.state.update(status=exc.category,phase='awaiting_official_source',last_error=str(exc),
                                  source_endpoint=exc.endpoint,retry_at=exc.retry_at,
                                  source_status={'category':exc.category,'retry_at':exc.retry_at,
                                                 'cooldown_seconds':self._source_cooldowns.get(exc.endpoint,{}).get('cooldown_seconds')})
                self._save()
                return self.state
            except Exception as exc:
                self.state.update({'status':'failed','last_error':str(exc)[:240]})
                self._save()
                raise


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config')
    parser.add_argument('--stage',choices=('daily','finance','assets','minute','all'),default='daily')
    args=parser.parse_args()
    cfg=load_config(args.config)
    stages=('daily','finance','assets','minute') if args.stage=='all' else (args.stage,)
    bootstrap=ResearchBootstrap(cfg)
    def stop(signum,frame):
        bootstrap.stop_event.set()
    previous={signum:signal.signal(signum,stop) for signum in (signal.SIGTERM,signal.SIGINT)}
    try:
        result=bootstrap.run(stages)
    finally:
        for signum,handler in previous.items():
            signal.signal(signum,handler)
    print(json.dumps(result,ensure_ascii=False,indent=2))
    if result['status']=='interrupted':
        raise SystemExit(130)
    if result['status'] in ('rate_limited','permission_denied','authentication'):
        raise SystemExit(75 if result['status']=='rate_limited' else 1)


if __name__=='__main__':
    main()
