"""Single read-only selected-minute-source entry point for every consumer."""
from pathlib import Path
import threading
import yaml
from data.research_bootstrap import ROOT
from data.research_minute_service import universe_fingerprint
from data.research_minute_service import _json,_valid_job,_digest,_db_signature,CoreDataBusy
from data.research_service import _stamp

_VERIFIED={}
_ERRORS={}
_VERIFYING=set()
_GUARD=threading.Lock()


def selected_service():
    provider=yaml.safe_load((ROOT/'config/research_minute_source.yaml').read_text())['provider']
    if provider=='baostock':
        from data import research_minute_baostock as module
    elif provider=='tushare':
        from data import research_minute_service as module
    else:raise ValueError('unsupported selected minute provider')
    return module


def _verify(module,worker,job,key,minute_sig,daily_sig,evidence_sig,config_sig):
    try:
        with worker.core_read_guard():
            _verify_window(module,worker,job,key,minute_sig,daily_sig,evidence_sig,config_sig)
    except CoreDataBusy:
        # A moving core snapshot is pending, not evidence of corrupt minute
        # rows. Do not cache a failure or touch durable receipts during a write.
        with _GUARD:_ERRORS.pop(key,None)
    except Exception:
        with _GUARD:_ERRORS[key]='分钟真实窗口/receipt/日线交叉验证未通过，不能标记就绪'
    finally:
        with _GUARD:_VERIFYING.discard(key)


def _verify_window(module,worker,job,key,minute_sig,daily_sig,evidence_sig,config_sig):
    with worker._db(readonly=True) as con:
        records=con.execute('SELECT code,rows,sha256 FROM minute_download_receipt WHERE job_id=? AND start_date=? AND end_date=?',
                            (job['job_id'],job['start_date'],job['target_date'])).fetchall()
    receipts={code:(rows,digest) for code,rows,digest in records}
    if set(receipts)!=set(job['symbols']):raise ValueError('minute receipt pool mismatch')
    count=0;nonempty=0
    for code in job['symbols']:
        tuples=worker._cached_rows(job,code);worker._validate_coverage(job,code,tuples)
        if len(tuples)!=receipts[code][0] or _digest(tuples)!=receipts[code][1]:raise ValueError('minute receipt hash mismatch')
        count+=len(tuples);nonempty+=bool(tuples)
    if _db_signature(worker.database_path)!=minute_sig or _db_signature(worker.cache/'bars.db')!=daily_sig:
        raise ValueError('minute or daily cache changed during final verification')
    if getattr(worker,'evidence_signature',lambda _:None)(job)!=evidence_sig:raise ValueError('raw source evidence changed during final verification')
    if _digest(module.load_config())!=config_sig:raise ValueError('minute validation config changed during final verification')
    quality=list(job.get('quality_checks',{}).values())
    proof={'cache_verified':True,'deep_verified':True,'frozen_window_complete':True,'coverage_complete':True,'rows':count,
           'covered_symbols':nonempty,'symbols_with_rows':nonempty,'verified_receipts':len(receipts),
           'validation_version':getattr(module,'VALIDATION_VERSION','tushare-minute-v1'),
           'verified_trading_sessions':sum(row.get('trading_days_checked',0) for row in quality),
           'verified_nontrading_sessions':sum(len(row.get('nontrading_sessions',[])) for row in quality),
           'quality_warnings':{'high_low_cross_source_difference_days':sum(row.get('high_low_cross_source_difference_days',0) for row in quality),
                               'source_repairs':[repair for row in quality for repair in row.get('source_repairs',[])],
                               'mixed_suspension_resume_dates':[record for row in quality for record in row.get('nontrading_sessions',[]) if record.get('mixed_suspend_resume')],
                               'not_for_precise_execution_or_auction':True}}
    with _GUARD:_VERIFIED.clear();_VERIFIED[key]=proof;_ERRORS.pop(key,None)


def public_status(*,wait=False):
    module=selected_service();cfg=module.load_config();status=module.public_status(cfg)
    status.update(current_date_ready=False,fresh_for_current_core=False,deep_verified=False)
    if status.get('core_update_in_progress'):
        status.update(cache_verified=False,frozen_window_complete=False,coverage_complete=False,
                      verification_status='waiting_core_update',verification_error=None)
        return status
    if status.get('completed_symbols')!=status.get('expected_symbols') or status.get('expected_symbols')!=300:
        status.update(cache_verified=False,frozen_window_complete=False,coverage_complete=False)
        return status
    try:
        cls=getattr(module,'BaostockMinuteService',None) or module.MinuteService
        worker=cls(cfg);job=_json(Path(cfg['runtime_dir'])/'job.json')
        if not _valid_job(job):raise ValueError('invalid immutable minute job')
        minute_sig=_db_signature(worker.database_path);daily_sig=_db_signature(worker.cache/'bars.db')
        evidence_sig=getattr(worker,'evidence_signature',lambda _:None)(job)
        config_sig=_digest(cfg)
        key=(job['job_id'],str(minute_sig),str(daily_sig),_digest(job.get('completed',{})),
             status.get('current_core_data_date'),status.get('current_universe_match'),getattr(module,'VALIDATION_VERSION','tushare-minute-v1'),evidence_sig,config_sig)
        with _GUARD:
            proof=_VERIFIED.get(key);error=_ERRORS.get(key)
            launch=proof is None and error is None and key not in _VERIFYING
            if launch:_VERIFYING.add(key)
        if launch:
            args=(module,worker,job,key,minute_sig,daily_sig,evidence_sig,config_sig)
            if wait:_verify(*args)
            else:threading.Thread(target=_verify,args=args,name='minute-readonly-verifier',daemon=True).start()
        elif wait and key in _VERIFYING:
            # Explicit audit callers may wait; HTTP callers never enter here.
            import time
            while True:
                with _GUARD:running=key in _VERIFYING
                if not running:break
                time.sleep(.05)
        with _GUARD:proof=_VERIFIED.get(key);error=_ERRORS.get(key)
        if proof is None:
            status.update(cache_verified=False,frozen_window_complete=False,coverage_complete=False,
                          verification_status='failed' if error else 'verifying',verification_error=error)
            return status
        status.update(proof,verification_status='verified')
        fresh=bool(status['target_date']==status.get('current_core_data_date') and status.get('current_universe_match')
                   and status['covered_symbols']==status['expected_symbols'])
        status.update(current_date_ready=fresh,fresh_for_current_core=fresh)
    except Exception:
        status.update(cache_verified=False,frozen_window_complete=False,coverage_complete=False,current_date_ready=False,
                      fresh_for_current_core=False,verification_error='分钟真实窗口/receipt/日线交叉验证未通过，不能标记就绪')
    return status


def start_service(base=None):
    return selected_service().start_service(base)
