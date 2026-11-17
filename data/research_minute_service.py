"""One fixed HS300 historical-minute job, one official request per hour.

No bootstrap prepare/run/save, core manifest writes, orders, or model calls.
The slot is durably reserved before HTTP; SQLite receipts commit with the real
bars, allowing a process restart to recover a committed unit without requery.
"""
import argparse
from contextlib import contextmanager,ExitStack
from datetime import date, datetime, timedelta
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import sqlite3
import subprocess
import sys
import threading

import yaml
from autotrade.locking import ProcessLock,lock_is_busy
from autotrade.monitoring import atomic_json
from autotrade.providers import TushareHttpClient,TushareRequestError
from data.research_bootstrap import ROOT,TZ,iso,number,validate_minute_rows,load_config as research_config
from data.research_service import _busy,_json,_stamp

SOURCE='tushare:stk_mins:5min'
FIELDS=('ts_code','trade_time','open','high','low','close','vol','amount')
IMMUTABLE=('job_id','source','frequency','start_date','target_date','symbols','universe_effective_date',
           'universe_source','calendar_source','open_sessions','traded_sessions','cache_dir','database_file')


class CoreDataBusy(RuntimeError):
    """A core update owns the input snapshot; retain all prior minute receipts."""


def _core_update_busy(core):
    runtime=Path(core['runtime_dir'])
    # Read-only strong verifiers share the bootstrap lock; only writers make
    # the underlying snapshot unavailable. The update intent remains exclusive.
    return (_busy(runtime/'update.lock')
            or any(lock_is_busy(runtime/name,shared=True) for name in ('bootstrap.lock','pipeline.lock')))


def _now():return datetime.now(TZ)


def load_config(path=None):
    cfg=yaml.safe_load((Path(path) if path else ROOT/'config/research_minute_service.yaml').read_text(encoding='utf-8'))
    if not isinstance(cfg,dict) or type(cfg.get('enabled')) is not bool:
        raise ValueError('分钟后台配置无效')
    for name in ('research_config','runtime_dir'):
        value=Path(cfg[name]);cfg[name]=str(value if value.is_absolute() else ROOT/value)
    for name in ('request_interval_seconds','rate_limit_cooldown_seconds'):
        if type(cfg.get(name)) is not int or not 3600<=cfg[name]<=604800:
            raise ValueError('分钟请求与限频冷却均不得短于一小时')
    for name in ('poll_seconds','heartbeat_seconds'):
        if type(cfg.get(name)) is not int or not 1<=cfg[name]<=60:
            raise ValueError('分钟后台检查间隔无效')
    if type(cfg.get('heartbeat_grace_seconds')) is not int or cfg['heartbeat_grace_seconds']<3*cfg['heartbeat_seconds']:
        raise ValueError('分钟后台心跳宽限无效')
    if cfg.get('maximum_source_rows')!=8000:
        raise ValueError('分钟源单次上限必须遵循官方8000行契约')
    return cfg


def _digest(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,ensure_ascii=False,allow_nan=False,separators=(',',':')).encode()).hexdigest()


def universe_fingerprint(symbols):
    return hashlib.sha256('\n'.join(sorted(symbols)).encode()).hexdigest()


def _job_identity(job):
    return _digest({key:job[key] for key in IMMUTABLE if key!='job_id'})


def _db_signature(path):
    if not path.exists():return None
    def stat(item):
        if not item.exists():return None
        value=item.stat();return {'size':value.st_size,'mtime_ns':value.st_mtime_ns}
    return {'main':stat(path),'wal':stat(Path(str(path)+'-wal'))}


def _summary(job):
    symbols=job.get('symbols') if isinstance(job.get('symbols'),list) else []
    raw=job.get('completed') if isinstance(job.get('completed'),dict) else {}
    done={code:value for code,value in raw.items() if code in symbols and isinstance(value,dict)
          and type(value.get('rows')) is int and value['rows']>=0}
    return {'completed_symbols':len(done),'symbols_with_rows':sum(v.get('rows',0)>0 for v in done.values()),
            'rows':sum(v.get('rows',0) for v in done.values()),'coverage_complete':bool(symbols and len(done)==len(symbols))}


def _valid_job(job):
    try:
        symbols=job['symbols']
        return (all(key in job for key in IMMUTABLE) and isinstance(symbols,list) and len(set(symbols))==len(symbols)==300
                and all(isinstance(code,str) and re.fullmatch(r'\d{6}\.(SH|SZ)',code) for code in symbols)
                and isinstance(job['open_sessions'],list) and isinstance(job['traded_sessions'],dict)
                and set(job['traded_sessions'])==set(symbols) and _job_identity(job)==job['job_id'])
    except (ValueError,TypeError,KeyError):return False


def _public_status(cfg):
    runtime=Path(cfg['runtime_dir']);job=_json(runtime/'job.json');state=_json(runtime/'service-state.json')
    now=_now();stamp=_stamp(state.get('updated_at'));age=(now-stamp).total_seconds() if stamp else None
    alive=bool(_busy(runtime/'service.lock') and age is not None and 0<=age<=cfg['heartbeat_grace_seconds'])
    summary=_summary(job);valid=_valid_job(job)
    core=research_config(cfg['research_config']);manifest=_json(Path(core['runtime_dir'])/'bootstrap-manifest.json')
    current=_json(Path(core['runtime_dir'])/'universe.json').get('codes',[])
    core_date=manifest.get('coverage',{}).get('daily',{}).get('end')
    universe_match=bool(valid and isinstance(current,list) and set(current)==set(job['symbols']))
    database=Path(job['cache_dir'])/job['database_file'] if valid else None
    cache_unchanged=bool(valid and job.get('cache_signature')==_db_signature(database))
    complete=bool(valid and summary['coverage_complete'] and cache_unchanged)
    core_busy=_core_update_busy(core)
    fresh=bool(complete and job['target_date']==core_date and universe_match and not core_busy)
    status=job.get('status','not_started') if valid else 'failed' if (runtime/'job.json').exists() else 'not_started'
    phase=job.get('phase','initializing')
    busy=bool(alive and status=='downloading' and phase in ('requesting','validating','committing'))
    if not alive and status not in ('complete','not_started','failed'):
        status='stopped';busy=False
    return {'ok':True,'service_alive':alive,'busy':busy,'status':status,'phase':phase,
            'job_id':job.get('job_id'),'scope':'fixed_hs300_snapshot','source':job.get('source',cfg.get('provider','tushare')),'frequency':'5min',
            'database_path':str(database) if database else None,'covered_symbols':summary['symbols_with_rows'],
            'symbols':list(job['symbols']) if valid else [],
            'research_runtime_dir':str(Path(core['runtime_dir']).resolve()),
            'universe_fingerprint':universe_fingerprint(job['symbols']) if valid else None,
            'start_date':job.get('start_date'),'target_date':job.get('target_date'),'expected_symbols':len(job['symbols']) if isinstance(job.get('symbols'),list) else 0,
            **summary,'coverage_complete':complete,'fresh_for_current_core':fresh,'current_core_data_date':core_date,
            'frozen_window_complete':complete,'current_date_ready':fresh,'core_update_in_progress':core_busy,
            'current_universe_match':universe_match,'cache_verified':cache_unchanged,
            'last_request_at':job.get('last_request_at'),'next_request_at':job.get('next_request_at'),
            'request_interval_seconds':cfg['request_interval_seconds'],'cooldown_seconds':cfg['rate_limit_cooldown_seconds'],
            'last_error':job.get('last_error') or state.get('last_error'),'updated_at':state.get('updated_at') or job.get('updated_at'),
            'limitations':['固定本轮日期与沪深300快照，不随核心日更移动窗口。',f"每次请求至少间隔{cfg['request_interval_seconds']}秒，睡眠后不补发积压请求。",
                           '旧窗口补全不代表当前最新300只覆盖；不用于盘中撮合。']}


def public_status(cfg=None):return _public_status(cfg or load_config())


class MinuteService:
    source_id='tushare:stk_mins'
    row_source=SOURCE
    database_file='minute.db'
    adopt_bootstrap=True
    use_legacy_window=True
    def __init__(self,cfg,*,now=None,client=None):
        self.cfg=cfg;self.now=now or _now;self.client=client
        self.runtime=Path(cfg['runtime_dir']);self.core=research_config(cfg['research_config'])
        self.cache=Path(self.core['cache_dir']);self.path=self.runtime/'job.json'
        self.database_path=self.cache/self.database_file
        self.stop_event=threading.Event();self._guard=threading.Lock();self._verified_signature=None
        self.state={'schema_version':1,'pid':os.getpid(),'status':'starting','phase':'initializing','started_at':self.now().isoformat()}

    def _state(self,**values):
        with self._guard:
            self.state.update(values);self.state['updated_at']=self.now().isoformat()
            atomic_json(self.runtime/'service-state.json',self.state)

    def _save(self,job):
        job.update(updated_at=self.now().isoformat(),**_summary(job))
        atomic_json(self.path,job)
        self._state(status=job['status'],phase=job['phase'],last_error=job.get('last_error'))

    @contextmanager
    def core_read_guard(self):
        # Hold the same lock order as Bootstrap, also excluding standalone
        # pipeline writers. Shared readers coexist; acquiring both locks closes
        # the race after the initial busy check without changing input caches.
        if _core_update_busy(self.core):
            raise CoreDataBusy('core research update is in progress')
        with ExitStack() as locks:
            try:
                for name in ('bootstrap.lock','pipeline.lock'):
                    locks.enter_context(ProcessLock(Path(self.core['runtime_dir'])/name,shared=True))
            except RuntimeError as exc:
                raise CoreDataBusy('core research update is in progress') from exc
            if _busy(Path(self.core['runtime_dir'])/'update.lock'):
                raise CoreDataBusy('core research update is in progress')
            yield

    @contextmanager
    def _db(self,readonly=False):
        path=self.database_path
        con=sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True,timeout=10) if readonly else sqlite3.connect(path,timeout=30)
        try:
            if con.execute('PRAGMA journal_mode').fetchone()[0].lower()=='wal':
                raise ValueError('minute cache WAL requires explicit checkpoint review before background use')
            yield con
            if not readonly:con.commit()
        except BaseException:
            if not readonly:con.rollback()
            raise
        finally:con.close()

    def _schema(self):
        self.cache.mkdir(parents=True,exist_ok=True)
        with self._db() as con:
            con.execute('CREATE TABLE IF NOT EXISTS minute_5m(code TEXT,trade_time TEXT,open REAL,high REAL,low REAL,close REAL,volume REAL,amount REAL,source TEXT,PRIMARY KEY(code,trade_time))')
            con.execute('CREATE TABLE IF NOT EXISTS minute_metadata(name TEXT PRIMARY KEY,value TEXT)')
            metadata=dict(con.execute('SELECT * FROM minute_metadata'))
            if con.execute('SELECT COUNT(*) FROM minute_5m').fetchone()[0] and any(metadata.get(key)!=value for key,value in
                    [('frequency','5min'),('volume_unit','shares'),('amount_unit','yuan'),('source',self.source_id)]):
                raise ValueError('existing nonempty minute cache has conflicting or unknown source units')
            con.executemany('INSERT OR REPLACE INTO minute_metadata VALUES(?,?)',[('frequency','5min'),('volume_unit','shares'),('amount_unit','yuan'),('source',self.source_id),('scope','fixed_hs300_snapshot')])
            con.execute('CREATE TABLE IF NOT EXISTS minute_download_receipt(job_id TEXT,code TEXT,start_date TEXT,end_date TEXT,rows INTEGER,sha256 TEXT,committed_at TEXT,PRIMARY KEY(job_id,code))')

    def _new_job(self,*,publish=True):
        root=Path(self.core['runtime_dir']);manifest=_json(root/'bootstrap-manifest.json');universe=_json(root/'universe.json')
        if manifest.get('source')!='tushare-official' or manifest.get('stages',{}).get('daily')!='complete':
            raise ValueError('分钟后台需要已验收的官方日线和300成分快照')
        coverage=(manifest.get('coverage',{}).get('minute') or {}) if self.use_legacy_window else {}
        target=iso(coverage.get('requested_end') or manifest.get('coverage',{}).get('daily',{}).get('end'))
        start=coverage.get('requested_start')
        if not start:
            import calendar
            end=date.fromisoformat(target);absolute=end.year*12+end.month-1-int(self.core['minute_months']);year,month=divmod(absolute,12);month+=1
            start=date(year,month,min(end.day,calendar.monthrange(year,month)[1])).isoformat()
        start=iso(start);symbols=universe.get('codes')
        if not isinstance(symbols,list) or len(symbols)!=300 or len(set(symbols))!=300 or any(not re.fullmatch(r'\d{6}\.(SH|SZ)',str(code)) for code in symbols):
            raise ValueError('固定分钟任务必须有300个唯一沪深股票代码')
        effective=iso(universe.get('effective_date'))
        if effective>target or start>target or target>manifest['coverage']['daily']['end'] or start<manifest['coverage']['daily']['start']:
            raise ValueError('分钟任务窗口或成分快照不在已验证日线范围')
        legacy_effective=manifest.get('completed',{}).get('prepare:stock_basic:'+target,{}).get('effective_date')
        if coverage.get('requested_end') and legacy_effective!=effective:
            raise ValueError('旧分钟任务与当前成分快照无法对应，不能移动其范围')
        sessions=sorted(key[6:] for key in manifest.get('completed',{}) if key.startswith('daily:') and start<=key[6:]<=target)
        if not sessions or target not in sessions:
            raise ValueError('固定窗口缺少已验证官方交易日清单')
        traded={}
        bars=self.cache/'bars.db'
        with sqlite3.connect(bars.resolve().as_uri()+'?mode=ro',uri=True,timeout=10) as con:
            for code in symbols:
                traded[code]=[row[0] for row in con.execute("SELECT date FROM daily_bar WHERE code=? AND adjust='none' AND source='tushare' AND date BETWEEN ? AND ? AND volume>0 ORDER BY date",(code,start,target))]
        job={'schema_version':1,'source':self.source_id,'frequency':'5min','scope':'fixed_hs300_snapshot',
             'start_date':start,'target_date':target,'symbols':sorted(symbols),'universe_effective_date':effective,
             'universe_source':universe.get('source'),'calendar_source':'verified-tushare-bootstrap-daily-checkpoints',
             'open_sessions':sessions,'traded_sessions':traded,'cache_dir':str(self.cache.resolve()),'database_file':self.database_file,'completed':{},
             'status':'waiting','phase':'waiting_request_slot','created_at':self.now().isoformat(),'last_error':None,
             'origin_manifest_sha256':_digest(manifest),'adoption_warnings':[]}
        job['job_id']=_job_identity(job)
        lower=[]
        quota=manifest.get('source_cooldowns',{}).get('stk_mins') or {}
        if self.adopt_bootstrap and _stamp(quota.get('retry_at')):lower.append(_stamp(quota['retry_at']))
        for key,value in manifest.get('completed',{}).items():
            if not self.adopt_bootstrap or not key.startswith('minute:'):continue
            stamp=_stamp(value.get('completed_at'))
            if stamp:lower.append(stamp+timedelta(seconds=self.cfg['request_interval_seconds']))
        job['next_request_at']=max(lower).isoformat() if lower else self.now().isoformat()
        self._schema()
        for code in symbols:
            if not self.adopt_bootstrap:break
            key='minute:'+code+':'+start+':'+target;proof=manifest.get('completed',{}).get(key)
            if not proof:continue
            try:
                tuples=self._cached_rows(job,code)
                if len(tuples)!=proof['rows']:raise ValueError('legacy row count mismatch')
                self._validate_coverage(job,code,tuples)
                self._receipt(job,code,tuples,proof.get('completed_at') or self.now().isoformat(),write_bars=False)
                job['completed'][code]={'rows':len(tuples),'sha256':_digest(tuples),'completed_at':proof.get('completed_at'),
                                        'origin':'verified_bootstrap_checkpoint','legacy_key':key}
            except (ValueError,KeyError,sqlite3.Error):
                job['adoption_warnings'].append({'code':code,'reason':'legacy_checkpoint_cache_validation_failed'})
        job['cache_signature']=_db_signature(self.database_path)
        if publish:self._save(job)
        return job

    def _load(self):
        if not self.path.exists():return self._new_job()
        job=_json(self.path)
        if not _valid_job(job) or _stamp(job.get('next_request_at')) is None:
            raise ValueError('分钟固定任务文件损坏或不可变快照被修改')
        if Path(job['cache_dir']).resolve()!=self.cache.resolve() or job['database_file']!=self.database_file or job['source']!=self.source_id:
            raise ValueError('分钟任务缓存目录已变更，不能重用断点')
        return job

    def _cached_rows(self,job,code):
        with self._db(readonly=True) as con:
            metadata=dict(con.execute('SELECT * FROM minute_metadata'))
            if metadata.get('frequency')!='5min' or metadata.get('volume_unit')!='shares' or metadata.get('amount_unit')!='yuan' or metadata.get('source')!=self.source_id:
                raise ValueError('分钟缓存单位或频度未验证')
            rows=con.execute('SELECT code,trade_time,open,high,low,close,volume,amount,source FROM minute_5m WHERE code=? AND trade_time BETWEEN ? AND ? ORDER BY trade_time',
                             (code,job['start_date']+' 00:00:00',job['target_date']+' 23:59:59')).fetchall()
        if any(row[8]!=self.row_source for row in rows):raise ValueError('分钟缓存来源不符')
        records=[dict(zip(FIELDS,row[:8])) for row in rows]
        return validate_minute_rows(code,records,job['start_date'],job['target_date'],set(job['open_sessions']),source=self.row_source)

    @staticmethod
    def _validate_coverage(job,code,tuples):
        observed={row[1][:10] for row in tuples};required=set(job['traded_sessions'][code])
        if required-observed:raise ValueError('minute response misses verified daily trading sessions')
        if not tuples and required:raise ValueError('empty minute response despite verified daily trading')

    def _receipt(self,job,code,tuples,stamp,write_bars=True):
        with self._db() as con:
            if write_bars:con.executemany('INSERT OR REPLACE INTO minute_5m VALUES(?,?,?,?,?,?,?,?,?)',tuples)
            stored=con.execute('SELECT code,trade_time,open,high,low,close,volume,amount,source FROM minute_5m WHERE code=? AND trade_time BETWEEN ? AND ? ORDER BY trade_time',
                               (code,job['start_date']+' 00:00:00',job['target_date']+' 23:59:59')).fetchall()
            if _digest(stored)!=_digest(tuples):
                raise ValueError('cached minute window conflicts with the validated source response')
            con.execute('INSERT OR REPLACE INTO minute_download_receipt VALUES(?,?,?,?,?,?,?)',
                        (job['job_id'],code,job['start_date'],job['target_date'],len(tuples),_digest(tuples),stamp))

    def _reconcile(self,job):
        with self._db(readonly=True) as con:
            receipts=con.execute('SELECT code,rows,sha256,committed_at FROM minute_download_receipt WHERE job_id=? AND start_date=? AND end_date=?',
                                  (job['job_id'],job['start_date'],job['target_date'])).fetchall()
        valid={}
        lower=[_stamp(job.get('next_request_at'))] if _stamp(job.get('next_request_at')) else []
        for code,count,digest,stamp in receipts:
            if _stamp(stamp):lower.append(_stamp(stamp)+timedelta(seconds=self.cfg['request_interval_seconds']))
            if code not in job['symbols']:continue
            try:
                tuples=self._cached_rows(job,code);self._validate_coverage(job,code,tuples)
                if len(tuples)!=count or _digest(tuples)!=digest:continue
                valid[code]={'rows':count,'sha256':digest,'completed_at':stamp,'origin':job.get('completed',{}).get(code,{}).get('origin','sqlite_atomic_receipt')}
            except (ValueError,sqlite3.Error,KeyError):continue
        job['completed']=valid;job['cache_signature']=_db_signature(self.database_path)
        attempt=job.get('last_attempt') or {}
        if attempt.get('status')=='request_started':
            if attempt.get('code') in valid:
                attempt.update(status='recovered_from_atomic_receipt',finished_at=valid[attempt['code']]['completed_at'])
            else:
                lower.append(self.now()+timedelta(seconds=self.cfg['request_interval_seconds']))
                attempt.update(status='unknown_after_restart')
        if lower:job['next_request_at']=max(lower).isoformat()

    def _request(self,job,code):
        client=self.client
        if client is None:
            from paperlive.config import load_config as paper_config
            from paperlive.data_settings import tushare_connection
            client=TushareHttpClient(tushare_connection(paper_config()),self.core['request_timeout'])
        table=client.query('stk_mins',fields=','.join(FIELDS),ts_code=code,freq='5min',
                           start_date=job['start_date']+' 09:00:00',end_date=job['target_date']+' 15:00:00')
        if not set(FIELDS)<=set(table.columns):raise ValueError('minute required response fields missing')
        rows=table.to_dict('records')
        if len(rows)>=self.cfg['maximum_source_rows']:raise ValueError('minute response hit source cap; no automatic extra request')
        return rows

    def _normalize(self,job,code,rows):
        return validate_minute_rows(code,rows,job['start_date'],job['target_date'],set(job['open_sessions']),source=self.row_source)

    def _verification_signature(self,job):
        return _db_signature(self.database_path)

    def cycle(self):
        self.runtime.mkdir(parents=True,exist_ok=True)
        try:
            with self.core_read_guard(),ProcessLock(self.runtime/'request.lock'):
                job=self._load()
                signature=self._verification_signature(job)
                if self._verified_signature!=signature:
                    self._reconcile(job);self._verified_signature=self._verification_signature(job)
                now=self.now()
                if now.tzinfo is None:raise ValueError('minute clock must be timezone-aware')
                if len(job['completed'])==len(job['symbols']):
                    job.update(status='complete',phase='finalized',last_error=None);self._save(job);return job
                deadline=_stamp(job.get('next_request_at'))
                if deadline and now<deadline:
                    if job.get('status') not in ('rate_limited','failed'):job.update(status='waiting',phase='waiting_request_slot')
                    self._save(job);return job
                failed=job.get('failed_symbols',{})
                candidates=[code for code in job['symbols'] if code not in job['completed'] and
                            (not _stamp(failed.get(code,{}).get('retry_at')) or now>=_stamp(failed[code]['retry_at']))]
                if not candidates:
                    job['next_request_at']=min(_stamp(value['retry_at']) for code,value in failed.items() if code not in job['completed']).isoformat()
                    job.update(status='waiting',phase='waiting_failed_symbol_retry');self._save(job);return job
                code=candidates[0]
                # Reserve durably BEFORE HTTP. An unknown/crashed attempt keeps
                # this deadline; waking after hours never drains a backlog.
                job.update(status='downloading',phase='requesting',last_request_at=now.isoformat(),current_symbol=code,
                           next_request_at=(now+timedelta(seconds=self.cfg['request_interval_seconds'])).isoformat(),
                           last_attempt={'code':code,'started_at':now.isoformat(),'status':'request_started'})
                self._save(job)
                try:
                    rows=self._request(job,code)
                    job['next_request_at']=max(_stamp(job['next_request_at']),self.now()+timedelta(seconds=self.cfg['request_interval_seconds'])).isoformat()
                    job['last_attempt']['response_at']=self.now().isoformat()
                    job['phase']='validating';self._save(job)
                    tuples=self._normalize(job,code,rows)
                    self._validate_coverage(job,code,tuples)
                    job['phase']='committing';self._save(job)
                    stamp=self.now().isoformat();self._receipt(job,code,tuples,stamp)
                    job['completed'][code]={'rows':len(tuples),'sha256':_digest(tuples),'completed_at':stamp,'origin':'official_single_request'}
                    job.setdefault('failed_symbols',{}).pop(code,None)
                    job['last_attempt'].update(status='committed',rows=len(tuples),finished_at=stamp)
                    job.update(status='complete' if len(job['completed'])==len(job['symbols']) else 'waiting',
                               phase='finalized' if len(job['completed'])==len(job['symbols']) else 'waiting_request_slot',last_error=None)
                except TushareRequestError as exc:
                    category=exc.category
                    job.update(status='rate_limited' if category=='rate_limited' else 'failed',phase=category,
                               last_error={'category':category,'message':'官方分钟接口限频，保留已完成记录' if category=='rate_limited' else '官方分钟接口尚不可用，请核对权限或凭据'})
                    job['last_attempt'].update(status=category,finished_at=self.now().isoformat())
                except Exception as exc:
                    job.update(status='failed',phase='source_or_validation_failed',last_error={'category':'source_or_validation_failed','message':'本次分钟请求或真实数据校验未通过，未标记该证券完成'})
                    detail=self.safe_error_detail(exc)
                    if detail:job['last_error']['detail']=detail
                    job['last_attempt'].update(status='failed',finished_at=self.now().isoformat())
                if job['status']=='failed':
                    job.setdefault('failed_symbols',{})[code]={'retry_at':(self.now()+timedelta(seconds=self.cfg.get('error_backoff_seconds',3600))).isoformat(),'category':job.get('phase')}
                interval=max(self.cfg['request_interval_seconds'],self.cfg['rate_limit_cooldown_seconds'] if job['status']=='rate_limited' else self.cfg.get('global_error_backoff_seconds',0) if job['status']=='failed' else 0)
                job['next_request_at']=max(_stamp(job['next_request_at']),self.now()+timedelta(seconds=interval)).isoformat()
                job['cache_signature']=_db_signature(self.database_path);self._verified_signature=self._verification_signature(job)
                self._save(job);return job
        except CoreDataBusy:
            self._state(status='waiting',phase='waiting_core_update',last_error=None,
                        message='核心研究正在更新；保留分钟回执，等待输入快照稳定')
            return dict(self.state)
        except RuntimeError:
            return {'status':'waiting','phase':'another_worker_owns_request_slot'}
        except Exception:
            self._state(status='failed',phase='initializing',last_error='固定分钟任务初始化或本机缓存验证失败；未请求供应商')
            return dict(self.state)

    @staticmethod
    def safe_error_detail(exc):return None

    def run(self,once=False):
        self.runtime.mkdir(parents=True,exist_ok=True)
        with ProcessLock(self.runtime/'service.lock'):
            self._state(status='waiting',phase='initializing')
            def heartbeat():
                while not self.stop_event.wait(self.cfg['heartbeat_seconds']):self._state()
            thread=threading.Thread(target=heartbeat,name='minute-heartbeat',daemon=True);thread.start()
            try:
                while not self.stop_event.is_set():
                    result=self.cycle()
                    if once:return result
                    self.stop_event.wait(60 if result.get('status')=='complete' else self.cfg['poll_seconds'])
            finally:
                self.stop_event.set();thread.join(timeout=2);self._state(status='stopped',phase='stopped')


def start_service(base=None):
    base=Path(base or ROOT);path=base/'config/research_minute_service.yaml';cfg=load_config(path)
    if not cfg['enabled']:return None
    runtime=Path(cfg['runtime_dir']);runtime.mkdir(parents=True,exist_ok=True)
    if _busy(runtime/'service.lock'):return None
    fd=os.open(runtime/'service.log',os.O_CREAT|os.O_WRONLY|os.O_APPEND,0o600)
    with os.fdopen(fd,'ab') as log:
        proc=subprocess.Popen([sys.executable,'-u','-m','data.research_minute_service','run','--config',str(path)],
                              cwd=base,stdin=subprocess.DEVNULL,stdout=log,stderr=log,start_new_session=True)
    (runtime/'service.pid').write_text(str(proc.pid)+'\n',encoding='utf-8');return proc


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('command',choices=('run','once','status'));parser.add_argument('--config')
    args=parser.parse_args();cfg=load_config(args.config)
    if args.command=='status':result=public_status(cfg)
    else:
        service=MinuteService(cfg)
        previous={sig:signal.signal(sig,lambda *_:service.stop_event.set()) for sig in (signal.SIGTERM,signal.SIGINT)}
        try:result=service.run(once=args.command=='once')
        finally:
            for sig,handler in previous.items():signal.signal(sig,handler)
    if result is not None:
        if args.command!='status':result={key:result.get(key) for key in ('job_id','status','phase','completed_symbols','rows','next_request_at','last_error')}
        print(json.dumps(result,ensure_ascii=False,indent=2))


if __name__=='__main__':main()
