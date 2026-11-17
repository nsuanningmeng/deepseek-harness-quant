"""Anonymous BaoStock history in a separate, receipt-verified staging cache."""
import argparse
from datetime import date,datetime,timedelta
import contextlib
import io
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import yaml
from data.research_bootstrap import ROOT,number,validate_minute_rows
from data.research_minute_service import MinuteService,_public_status,_json,_digest,_db_signature,_valid_job,FIELDS
from data.research_service import _busy,_stamp
from data.research_bootstrap import atomic_json

VALIDATION_VERSION='baostock-sina-48slots-raw-daily-and-suspension-v3'
SINA_SOURCE='sina:getKLineData:5min:raw'


def _slots():
    return {f'{minute//60:02}:{minute%60:02}' for start,end in ((9*60+35,11*60+30),(13*60+5,15*60)) for minute in range(start,end+1,5)}


def load_config(path=None):
    cfg=yaml.safe_load((Path(path) if path else ROOT/'config/research_minute_baostock.yaml').read_text(encoding='utf-8'))
    if cfg.get('provider')!='baostock' or type(cfg.get('enabled')) is not bool:raise ValueError('invalid minute provider config')
    for key in ('research_config','runtime_dir'):
        value=Path(cfg[key]);cfg[key]=str(value if value.is_absolute() else ROOT/value)
    if not 1<=cfg['request_interval_seconds']<=3600 or not 1<=cfg['poll_seconds']<=60:raise ValueError('invalid minute cadence')
    if cfg.get('maximum_source_rows')!=8000:raise ValueError('invalid minute source bound')
    if cfg['error_backoff_seconds']<30 or cfg['global_error_backoff_seconds']<1:raise ValueError('invalid minute backoff')
    if not 1<=cfg.get('request_timeout_seconds',0)<=120:raise ValueError('invalid minute network deadline')
    repair=cfg.get('repairs',{})
    if repair.get('source')!='sina' or repair.get('endpoint')!='https://quotes.sina.cn/cn/api/json_v2.php/CN_MarketDataService.getKLineData':raise ValueError('unsupported public minute repair endpoint')
    if not 0<=repair.get('amount_absolute_tolerance_yuan',-1)<=1 or not 0<=repair.get('amount_relative_tolerance',-1)<=1e-6:raise ValueError('invalid repair precision bounds')
    if not isinstance(repair.get('maximum_rows'),int) or not 48<=repair['maximum_rows']<=1023:raise ValueError('invalid repair source bound')
    suspension=cfg.get('suspension_proof',{})
    if (type(suspension.get('retry_seconds')) is not int or suspension['retry_seconds']<300
            or suspension.get('maximum_rows')!=5000 or not 1<=suspension.get('request_timeout_seconds',0)<=120):
        raise ValueError('invalid official nontrading evidence policy')
    return cfg


class BaostockMinuteService(MinuteService):
    source_id='baostock:query_history_k_data_plus:5min:raw'
    row_source='baostock:5min:raw'
    database_file='minute_baostock.db'
    adopt_bootstrap=False
    use_legacy_window=False

    def _load(self):
        job=super()._load()
        if not self.cfg.get('follow_core_updates') or len(job.get('completed',{}))!=300:return job
        manifest=_json(Path(self.core['runtime_dir'])/'bootstrap-manifest.json')
        universe=_json(Path(self.core['runtime_dir'])/'universe.json')
        target=manifest.get('coverage',{}).get('daily',{}).get('end')
        if target==job['target_date'] and set(universe.get('codes',[]))==set(job['symbols']):return job
        if not target or target<job['target_date']:raise ValueError('core date cannot roll a fixed minute job backwards')
        # Only a completely revalidated predecessor may seed a new fixed round.
        # The archive + SQLite receipts remain the authority for each reused code.
        self._reconcile(job)
        if len(job['completed'])!=300:return job
        atomic_json(self.runtime/'history'/(job['job_id']+'.json'),job)
        next_job=self._new_job(publish=False)
        next_job['predecessor_job_id']=job['job_id']
        next_job['next_request_at']=max(_stamp(next_job['next_request_at']),_stamp(job['next_request_at'])).isoformat()
        self._save(next_job);self._verified_signature=None
        return next_job

    def _verified_prefix(self,job,code):
        previous_id=job.get('predecessor_job_id')
        if not previous_id:return [],job['start_date']
        if not isinstance(previous_id,str) or not re.fullmatch('[0-9a-f]{64}',previous_id):raise ValueError('incremental minute predecessor identity invalid')
        previous=_json(self.runtime/'history'/(previous_id+'.json'))
        if not _valid_job(previous) or previous['job_id']!=previous_id or previous['source']!=self.source_id:
            raise ValueError('incremental minute predecessor proof invalid')
        if code not in previous['symbols'] or previous['start_date']>job['start_date']:return [],job['start_date']
        if previous['target_date']>job['target_date']:raise ValueError('incremental minute window moved backwards')
        with self._db(readonly=True) as con:
            receipt=con.execute('SELECT rows,sha256 FROM minute_download_receipt WHERE job_id=? AND code=? AND start_date=? AND end_date=?',
                                (previous_id,code,previous['start_date'],previous['target_date'])).fetchone()
        if not receipt:return [],job['start_date']
        tuples=self._cached_rows(previous,code);self._validate_coverage(previous,code,tuples)
        if len(tuples)!=receipt[0] or _digest(tuples)!=receipt[1]:raise ValueError('incremental minute predecessor content changed')
        prefix=[{**dict(zip(FIELDS,row[:8])),'_source':row[8]} for row in tuples if row[1][:10]>=job['start_date']]
        return prefix,(date.fromisoformat(previous['target_date'])+timedelta(days=1)).isoformat()

    def _request(self,job,code):
        prefix,start=self._verified_prefix(job,code)
        if start>job['target_date']:return prefix
        request_job={**job,'start_date':start}
        if self.client is None:
            # Isolate BaoStock's process-global socket and bound login/query/
            # logout together. No blocked socket can outlive this deadline.
            command=[sys.executable,'-u','-m','data.research_minute_baostock','fetch-once',code,start,job['target_date']]
            result=subprocess.run(command,cwd=ROOT,stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE,
                                  timeout=self.cfg['request_timeout_seconds'],check=False,start_new_session=True)
            if result.returncode:raise ValueError('bounded minute provider process failed')
            data=json.loads(result.stdout)
            if not data.get('ok'):raise ValueError('bounded minute provider response failed')
            rows=prefix+data['rows']
        else:rows=prefix+self._sdk_request(request_job,code,self.client)
        return self._repair_if_needed(job,code,rows)

    def _normalize(self,job,code,rows):
        groups={}
        for row in rows:groups.setdefault(row.get('_source',self.row_source),[]).append(row)
        result=[]
        for source,values in groups.items():
            if source not in (self.row_source,SINA_SOURCE):raise ValueError('unverified minute row source')
            result.extend(validate_minute_rows(code,values,job['start_date'],job['target_date'],set(job['open_sessions']),source=source))
        result.sort(key=lambda row:row[1])
        if len({row[1] for row in result})!=len(result):raise ValueError('duplicate mixed-source minute timestamp')
        return result

    def _cached_rows(self,job,code):
        with self._db(readonly=True) as con:
            metadata=dict(con.execute('SELECT * FROM minute_metadata'))
            if any(metadata.get(key)!=value for key,value in [('frequency','5min'),('volume_unit','shares'),('amount_unit','yuan'),('source',self.source_id)]):raise ValueError('minute cache metadata mismatch')
            rows=con.execute('SELECT code,trade_time,open,high,low,close,volume,amount,source FROM minute_5m WHERE code=? AND trade_time BETWEEN ? AND ? ORDER BY trade_time',
                             (code,job['start_date']+' 00:00:00',job['target_date']+' 23:59:59')).fetchall()
        return self._normalize(job,code,[{**dict(zip(FIELDS,row[:8])),'_source':row[8]} for row in rows])

    def evidence_signature(self,job):
        evidence={};directory=self.runtime/'source-repairs'
        for path in directory.glob('*-sina-repair.json'):
            proof=_json(path);code=proof.get('symbol')
            if code not in job['symbols']:continue
            day=proof.get('repair_date')
            if day and not job['start_date']<=day<=job['target_date']:continue
            raw=self._raw_evidence(code,proof)
            evidence[path.name]={'proof':_digest(proof),'raw':_digest(raw)}
        for path in (self.runtime/'nontrading-proof').glob('*.json'):
            proof=_json(path)
            if proof.get('symbol') not in job['symbols'] or not job['start_date']<=proof.get('date','')<=job['target_date']:continue
            verified=self._nontrading_proof(proof['symbol'],proof['date'])
            evidence['nontrading/'+path.name]=verified['evidence_sha256']
        return _digest(evidence)

    def _verification_signature(self,job):
        return (_db_signature(self.database_path),_db_signature(self.cache/'bars.db'),self.evidence_signature(job),_digest(self.cfg))

    def _nontrading_proof(self,code,day):
        proof=_json(self.runtime/'nontrading-proof'/(code+'-'+day+'.json'))
        digest=proof.get('raw_sha256','')
        filename=code+'-suspend-d-'+digest+'.json'
        if (proof.get('source')!='tushare:suspend_d' or proof.get('symbol')!=code or proof.get('date')!=day
                or not re.fullmatch('[0-9a-f]{64}',digest) or proof.get('raw_file')!=filename):
            raise ValueError('missing official suspension proof for absent daily session')
        raw=_json(self.runtime/'nontrading-raw'/filename)
        if (raw.get('source')!='tushare:suspend_d' or raw.get('symbol')!=code or _digest(raw)!=digest
                or not raw.get('start_date','9999')<=day<=raw.get('end_date','')):
            raise ValueError('official suspension response evidence changed')
        records=[row for row in raw.get('records',[]) if row.get('ts_code')==code and row.get('trade_date')==day.replace('-','')]
        if not any(row.get('suspend_type')=='S' and row.get('suspend_timing') in (None,'') for row in records):
            raise ValueError('absent daily session lacks official full-day suspension record')
        return {'date':day,'source':'tushare:suspend_d','raw_sha256':digest,
                'record_types':sorted({row.get('suspend_type') for row in records}),
                'mixed_suspend_resume':any(row.get('suspend_type')=='R' for row in records),
                'evidence_sha256':_digest({'proof':proof,'raw':raw}),
                'meaning':'官方存在无日内时段的停牌记录，且日线无成交记录；不据此推定盘中停复牌先后顺序'}

    def _store_suspension_response(self,code,records,start,end,retrieved_at):
        raw={'source':'tushare:suspend_d','symbol':code,'start_date':start,'end_date':end,
             'retrieved_at':retrieved_at,'records':records}
        digest=_digest(raw);filename=code+'-suspend-d-'+digest+'.json'
        atomic_json(self.runtime/'nontrading-raw'/filename,raw)
        for row in records:
            if row.get('ts_code')!=code or row.get('suspend_type')!='S' or row.get('suspend_timing') not in (None,''):continue
            stamp=str(row.get('trade_date',''))
            if not re.fullmatch('[0-9]{8}',stamp):raise ValueError('official suspension date invalid')
            day=datetime.strptime(stamp,'%Y%m%d').date().isoformat()
            if not start<=day<=end:raise ValueError('official suspension date outside requested scope')
            atomic_json(self.runtime/'nontrading-proof'/(code+'-'+day+'.json'),
                        {'source':'tushare:suspend_d','symbol':code,'date':day,'raw_file':filename,'raw_sha256':digest})

    def _ensure_nontrading_evidence(self,job):
        import sqlite3
        missing={}
        with sqlite3.connect((self.cache/'bars.db').resolve().as_uri()+'?mode=ro',uri=True) as con:
            for code in job['symbols']:
                known={row[0] for row in con.execute("SELECT date FROM daily_bar WHERE code=? AND adjust='none' AND source='tushare' AND date BETWEEN ? AND ?",(code,job['start_date'],job['target_date']))}
                for day in sorted(set(job['open_sessions'])-known):
                    try:self._nontrading_proof(code,day)
                    except ValueError:missing.setdefault(code,[]).append(day)
        for code,days in missing.items():
            policy=self.cfg['suspension_proof'];slot=self.runtime/'nontrading-proof'/(code+'-request.json')
            previous=_json(slot);deadline=_stamp(previous.get('next_request_at'))
            if deadline and self.now()<deadline:raise ValueError('official suspension evidence retry pending')
            atomic_json(slot,{'next_request_at':(self.now()+timedelta(seconds=policy['retry_seconds'])).isoformat()})
            client=getattr(self,'metadata_client',None)
            if client is None:
                from autotrade.providers import TushareHttpClient
                from paperlive.config import load_config as paper_config
                from paperlive.data_settings import tushare_connection
                client=TushareHttpClient(tushare_connection(paper_config()),policy['request_timeout_seconds'])
            table=client.query('suspend_d',fields='ts_code,trade_date,suspend_timing,suspend_type',ts_code=code,
                               start_date=min(days).replace('-',''),end_date=max(days).replace('-',''))
            if not {'ts_code','trade_date','suspend_timing','suspend_type'}<=set(table.columns):raise ValueError('official suspension fields missing')
            records=table.to_dict('records')
            if len(records)>=policy['maximum_rows']:raise ValueError('official suspension response hit safe bound')
            self._store_suspension_response(code,records,min(days),max(days),self.now().isoformat())
            for day in days:self._nontrading_proof(code,day)

    def _reconcile(self,job):
        self._ensure_nontrading_evidence(job)
        return super()._reconcile(job)

    def _day_proof(self,code,day):
        path=self.runtime/'source-repairs'/(code+'-'+day+'-sina-repair.json')
        return _json(path) if path.exists() else _json(self.runtime/'source-repairs'/(code+'-sina-repair.json'))

    def _raw_evidence(self,code,proof):
        digest=proof.get('raw_sha256','')
        expected=code+'-sina-raw-'+digest+'.json'
        if proof.get('symbol')!=code or proof.get('source')!=SINA_SOURCE or not re.fullmatch('[0-9a-f]{64}',digest) or proof.get('raw_file')!=expected:
            raise ValueError('invalid preserved minute source identity')
        raw=json.loads((self.runtime/'source-repairs'/expected).read_text())
        if not isinstance(raw,list) or _digest(raw)!=digest:raise ValueError('raw repair source evidence changed')
        if proof.get('rejected_sha256'):
            rejected_digest=proof['rejected_sha256'];rejected_file=code+'-baostock-rejected-'+rejected_digest+'.json'
            if not re.fullmatch('[0-9a-f]{64}',rejected_digest) or proof.get('rejected_raw_file')!=rejected_file:raise ValueError('invalid rejected source proof')
            rejected=json.loads((self.runtime/'source-repairs'/rejected_file).read_text())
            if _digest(rejected)!=rejected_digest:raise ValueError('rejected source evidence changed')
        return raw

    def _repair_if_needed(self,job,code,rows):
        tuples=self._normalize(job,code,rows)
        try:self._validate_coverage(job,code,tuples);return rows
        except ValueError:
            if not self.cfg['repairs'].get('enabled'):raise
        grouped={}
        for row in tuples:grouped.setdefault(row[1][:10],[]).append(row)
        failed=[]
        for day in sorted(set(grouped)|set(job['traded_sessions'][code])):
            probe={**job,'start_date':day,'target_date':day,'open_sessions':[day],
                   'traded_sessions':{code:[day] if day in job['traded_sessions'][code] else []},'quality_checks':{}}
            try:self._validate_coverage(probe,code,grouped.get(day,[]))
            except ValueError:failed.append(day)
        if not failed:raise ValueError('minute source window invalid')
        rejected=[row for row in rows if row['trade_time'][:10] in failed]
        rejected_sha=_digest(rejected);rejected_file=code+'-baostock-rejected-'+rejected_sha+'.json'
        atomic_json(self.runtime/'source-repairs'/rejected_file,rejected)
        job.setdefault('repair_proofs',{}).setdefault(code,{}).update(rejected_sha256=rejected_sha,rejected_raw_file=rejected_file)
        records=self._sina_repairs(job,code,failed)
        combined=[row for row in rows if row['trade_time'][:10] not in failed]+records
        self._validate_coverage(job,code,self._normalize(job,code,combined))
        return combined

    def _sina_repairs(self,job,code,days):
        import requests
        directory=self.runtime/'source-repairs';directory.mkdir(parents=True,exist_ok=True)
        proofs={}
        for day in days:
            value=self._day_proof(code,day)
            try:
                self._raw_evidence(code,value)
                if day in value.get('days',[]):proofs[day]=value
            except (ValueError,OSError,TypeError):pass
        missing=set(days)-set(proofs)
        if missing:
            options=self.cfg['repairs'];native=('sh' if code.endswith('.SH') else 'sz')+code[:6]
            response=requests.get(options['endpoint'],params={'symbol':native,'scale':5,'ma':'no','datalen':options['maximum_rows']},
                                  timeout=self.cfg['request_timeout_seconds'],allow_redirects=False)
            response.raise_for_status();raw=response.json()
            if not isinstance(raw,list):raise ValueError('public repair source did not return historical rows')
            digest=_digest(raw);raw_file=code+'-sina-raw-'+digest+'.json';atomic_json(directory/raw_file,raw)
            available=sorted({str(row.get('day',''))[:10] for row in raw})
            if not missing<=set(available):raise ValueError('public repair source cannot cover requested missing dates')
            proof={'symbol':code,'source':SINA_SOURCE,'endpoint':options['endpoint'],'raw_file':raw_file,'raw_sha256':digest,'days':available,'retrieved_at':self.now().isoformat()}
            for day in missing:proofs[day]=proof
        result=[];snapshots={}
        for day in days:
            proof={**proofs[day],'repair_date':day,'days':[day]}
            proof.update({key:value for key,value in job.get('repair_proofs',{}).get(code,{}).items() if key in ('rejected_sha256','rejected_raw_file')})
            atomic_json(directory/(code+'-'+day+'-sina-repair.json'),proof)
            raw=self._raw_evidence(code,proof)
            result.extend({'ts_code':code,'trade_time':row['day'],**{key:row[key] for key in ('open','high','low','close','amount')},'vol':row['volume'],'_source':SINA_SOURCE}
                          for row in raw if row['day'][:10]==day)
            snapshots[day]={'raw_sha256':proof['raw_sha256'],'raw_file':proof['raw_file']}
        job.setdefault('repair_proofs',{}).setdefault(code,{}).update(source=SINA_SOURCE,snapshots=snapshots)
        return result

    def _receipt(self,job,code,tuples,stamp,write_bars=True):
        # Keep every superseded provider day; replacement and final receipt
        # share the same SQLite transaction and can be recovered after a crash.
        with self._db() as con:
            con.execute('CREATE TABLE IF NOT EXISTS minute_repair_archive(code TEXT,day TEXT,source TEXT,sha256 TEXT,rows_json TEXT,archived_at TEXT,PRIMARY KEY(code,day,source,sha256))')
            repair_days={row[1][:10] for row in tuples if row[8]==SINA_SOURCE}
            for day in repair_days:
                prior=con.execute('SELECT * FROM minute_5m WHERE code=? AND trade_time BETWEEN ? AND ? ORDER BY trade_time',(code,day+' 00:00:00',day+' 23:59:59')).fetchall()
                if prior and any(row[8]!=SINA_SOURCE for row in prior):
                    con.execute('INSERT OR IGNORE INTO minute_repair_archive VALUES(?,?,?,?,?,?)',(code,day,self.row_source,_digest(prior),json.dumps(prior,allow_nan=False),stamp))
            if write_bars:con.executemany('INSERT OR REPLACE INTO minute_5m VALUES(?,?,?,?,?,?,?,?,?)',tuples)
            stored=con.execute('SELECT * FROM minute_5m WHERE code=? AND trade_time BETWEEN ? AND ? ORDER BY trade_time',(code,job['start_date']+' 00:00:00',job['target_date']+' 23:59:59')).fetchall()
            if _digest(stored)!=_digest(tuples):raise ValueError('persisted mixed-source minute window mismatch')
            if repair_days:con.execute('INSERT OR REPLACE INTO minute_metadata VALUES(?,?)',('supplementary_sources',SINA_SOURCE))
            con.execute('INSERT OR REPLACE INTO minute_download_receipt VALUES(?,?,?,?,?,?,?)',(job['job_id'],code,job['start_date'],job['target_date'],len(tuples),_digest(tuples),stamp))

    @staticmethod
    def _sdk_request(job,code,client):
        logged=False
        try:
            login=client.login()
            if login.error_code!='0':raise ValueError('anonymous minute provider login failed')
            logged=True
            fields='date,time,code,open,high,low,close,volume,amount,adjustflag'
            native=('sh.' if code.endswith('.SH') else 'sz.')+code[:6]
            result=client.query_history_k_data_plus(native,fields,start_date=job['start_date'],end_date=job['target_date'],frequency='5',adjustflag='3')
            if result.error_code!='0':raise ValueError('anonymous minute provider response failed')
            rows=[]
            while result.next():
                row=dict(zip(result.fields,result.get_row_data()))
                if row.get('code')!=native or row.get('adjustflag')!='3' or not str(row.get('time','')).isdigit() or len(row['time'])!=17:
                    raise ValueError('minute provider identity/adjustment/timestamp mismatch')
                stamp=datetime.strptime(row['time'][:14],'%Y%m%d%H%M%S')
                if stamp.date().isoformat()!=row['date'] or row['time'][14:]!='000':raise ValueError('minute provider date mismatch')
                rows.append({'ts_code':code,'trade_time':stamp.isoformat(sep=' '),**{key:row[key] for key in ('open','high','low','close','amount')},'vol':row['volume']})
                if len(rows)>=8000:raise ValueError('minute response reached safe bound')
            if result.error_code!='0':raise ValueError('minute provider interrupted response')
            return rows
        finally:
            if logged:
                try:client.logout()
                except Exception:pass

    def _validate_coverage(self,job,code,tuples):
        super()._validate_coverage(job,code,tuples)
        import sqlite3
        with sqlite3.connect((self.cache/'bars.db').resolve().as_uri()+'?mode=ro',uri=True) as con:
            daily={row[0]:row[1:] for row in con.execute("SELECT date,open,close,high,low,volume,amount FROM daily_bar WHERE code=? AND adjust='none' AND source='tushare' AND date BETWEEN ? AND ?",(code,job['start_date'],job['target_date']))}
        grouped={}
        for row in tuples:grouped.setdefault(row[1][:10],[]).append(row)
        nontrading=[self._nontrading_proof(code,day) for day in sorted(set(job['open_sessions'])-set(daily))]
        if {day for day,values in daily.items() if number(values[4]) is not None and number(values[4])>0}-set(grouped):
            raise ValueError('minute response misses current verified daily trading sessions')
        warnings=[];checked=0;repairs=[]
        for day,rows in grouped.items():
            if day not in daily:raise ValueError('minute source date absent from verified daily cache')
            op,cl,hi,lo,vol,amount=map(number,daily[day])
            if vol is None or amount is None:raise ValueError('daily source quantities unknown')
            if vol<=0:
                if any(row[6]!=0 or row[7]!=0 for row in rows):raise ValueError('minute trading contradicts zero-volume daily data')
                continue
            if {row[1][11:16] for row in rows}!=_slots() or len(rows)!=48:raise ValueError('minute traded day lacks exact 48 source slots')
            if op is None or cl is None or abs(rows[0][2]-op)>1e-8 or abs(rows[-1][5]-cl)>1e-8:raise ValueError('minute open/close contradict verified raw daily')
            if abs(sum(row[6] for row in rows)-vol*100)>=1:raise ValueError('minute share volume contradicts daily lots converted to shares')
            sources={row[8] for row in rows}
            if len(sources)!=1:raise ValueError('a minute source repair must replace a whole trading day')
            difference=sum(row[7] for row in rows)-amount*1000
            tolerance=.1
            if sources=={SINA_SOURCE}:
                options=self.cfg['repairs'];tolerance=max(options['amount_absolute_tolerance_yuan'],abs(amount*1000)*options['amount_relative_tolerance'])
                proof=self._day_proof(code,day)
                if proof.get('symbol')!=code or proof.get('source')!=SINA_SOURCE or day not in proof.get('days',[]):raise ValueError('missing raw repair provenance')
                raw=self._raw_evidence(code,proof)
                source_rows=[{'ts_code':code,'trade_time':row['day'],**{key:row[key] for key in ('open','high','low','close','amount')},'vol':row['volume']}
                             for row in raw if row['day'][:10]==day]
                original=validate_minute_rows(code,source_rows,day,day,{day},source=SINA_SOURCE)
                if _digest(original)!=_digest(rows):raise ValueError('minute repair rows differ from preserved raw source')
                repairs.append({'date':day,'source':SINA_SOURCE,'raw_sha256':proof['raw_sha256'],'amount_difference_yuan':difference,
                                'replaced_source_sha256':proof.get('rejected_sha256'),
                                'amount_relative_difference':difference/(amount*1000) if amount else None,'amount_tolerance_yuan':tolerance,
                                'amount_exact_agreement':abs(difference)<=.1,'precision_explanation':'跨源金额存在实际差异；原因未确认，不宣称逐分金额精确'})
            if abs(difference)>tolerance:raise ValueError('minute yuan amount contradicts daily thousand-yuan amount')
            checked+=1
            if hi is not None and lo is not None and (abs(max(row[3] for row in rows)-hi)>1e-8 or abs(min(row[4] for row in rows)-lo)>1e-8):
                warnings.append(day)
        job.setdefault('quality_checks',{})[code]={'validation_version':VALIDATION_VERSION,'trading_days_checked':checked,
                'high_low_cross_source_difference_days':len(warnings),'difference_date_examples':warnings[:5],
                'open_close_verified':True,'volume_tolerance_shares':'strictly less than 1','amount_tolerance_yuan':.1,
                'price_comparison_float_tolerance_yuan':1e-8,'not_for_precise_execution_or_auction':True}
        job['quality_checks'][code]['source_repairs']=repairs
        job['quality_checks'][code]['nontrading_sessions']=nontrading

    @staticmethod
    def safe_error_detail(exc):
        allowed={'minute traded day lacks exact 48 source slots','minute open/close contradict verified raw daily',
                 'minute share volume contradicts daily lots converted to shares','minute yuan amount contradicts daily thousand-yuan amount',
                 'minute source date absent from verified daily cache','daily source quantities unknown',
                 'minute trading contradicts zero-volume daily data','minute response misses current verified daily trading sessions'}
        return str(exc) if str(exc) in allowed else None


def public_status(cfg=None):
    cfg=cfg or load_config();status=_public_status(cfg)
    # Lightweight callers must not promote pre-upgrade weak receipts. The
    # unified readiness entry performs the full real-row verification once.
    job=_json(Path(cfg['runtime_dir'])/'job.json')
    quality=job.get('quality_checks') or {}
    strict=bool(_valid_job(job) and len(job.get('completed',{}))==300 and all(
        isinstance(quality.get(code),dict) and quality[code].get('validation_version')==VALIDATION_VERSION for code in job['symbols']))
    if not strict:status.update(cache_verified=False,frozen_window_complete=False,coverage_complete=False,current_date_ready=False,fresh_for_current_core=False)
    status['strict_validation_version']=VALIDATION_VERSION
    sources=[BaostockMinuteService.source_id]
    if any(value.get('source_repairs') for value in quality.values() if isinstance(value,dict)):sources.append(SINA_SOURCE)
    status['sources']=sources
    if len(sources)>1:status['source']=';'.join(sources)
    return status


def start_service(base=None):
    base=Path(base or ROOT);path=base/'config/research_minute_baostock.yaml';cfg=load_config(path)
    if not cfg['enabled']:return None
    runtime=Path(cfg['runtime_dir']);runtime.mkdir(parents=True,exist_ok=True)
    if _busy(runtime/'service.lock'):return None
    fd=os.open(runtime/'service.log',os.O_CREAT|os.O_WRONLY|os.O_APPEND,0o600)
    with os.fdopen(fd,'ab') as log:
        proc=subprocess.Popen([sys.executable,'-u','-m','data.research_minute_baostock','run'],cwd=base,
                              stdin=subprocess.DEVNULL,stdout=log,stderr=log,start_new_session=True)
    (runtime/'service.pid').write_text(str(proc.pid)+'\n');return proc


def main():
    if len(sys.argv)==5 and sys.argv[1]=='fetch-once':
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                import baostock as client
                rows=BaostockMinuteService._sdk_request({'start_date':sys.argv[3],'target_date':sys.argv[4]},sys.argv[2],client)
            print(json.dumps({'ok':True,'rows':rows},allow_nan=False))
        except Exception:print(json.dumps({'ok':False}));raise SystemExit(1)
        return
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('command',choices=('run','once','status'));args=parser.parse_args()
    cfg=load_config()
    if args.command=='status':result=public_status(cfg)
    else:
        service=BaostockMinuteService(cfg)
        old={sig:signal.signal(sig,lambda *_:service.stop_event.set()) for sig in (signal.SIGTERM,signal.SIGINT)}
        try:result=service.run(once=args.command=='once')
        finally:
            for sig,handler in old.items():signal.signal(sig,handler)
    if result is not None:print(json.dumps({key:result.get(key) for key in ('status','phase','completed_symbols','rows','next_request_at','last_error')},ensure_ascii=False))


if __name__=='__main__':main()
