"""Local after-close research updates; no orders, paper cash, or legacy workers.

The launcher starts one daemon. HTTP handlers only enqueue a local request;
the daemon verifies official calendar and completed daily source coverage before
resuming the bootstrap and the content-checked research pipeline.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import threading
from uuid import uuid4

from autotrade.locking import ProcessLock
from autotrade.monitoring import atomic_json
from data.research_bootstrap import ROOT, TZ, DAILY, BASIC, ResearchBootstrap, load_config as research_config, number, compact, validate_close_cutoff

_REQUEST_GUARD=threading.Lock()
CORE_STAGES=('daily','finance','assets')


@dataclass(frozen=True)
class _CoreCompletion:
    target: str
    manifest: dict
    idle: bool = False


def load_config(path=None):
    import yaml
    path=Path(path) if path else ROOT/'config/research_service.yaml'
    cfg=yaml.safe_load(path.read_text(encoding='utf-8'))
    if not isinstance(cfg,dict) or type(cfg.get('enabled')) is not bool:
        raise ValueError('研究服务配置无效')
    for key in ('research_config','runtime_dir'):
        value=Path(cfg[key]);cfg[key]=str(value if value.is_absolute() else ROOT/value)
    validate_close_cutoff(cfg.get('earliest_close_check'))
    for key in ('poll_seconds','retry_seconds','heartbeat_seconds','heartbeat_grace_seconds','calendar_lookback_days'):
        if type(cfg.get(key)) is not int or not 1<=cfg[key]<=86400:
            raise ValueError('研究服务时间配置无效')
    if cfg['poll_seconds']>60 or cfg['heartbeat_seconds']>60 or cfg['heartbeat_grace_seconds']<cfg['heartbeat_seconds']*3:
        raise ValueError('研究服务心跳配置无效')
    return cfg


def _json(path):
    try:
        value=json.loads(Path(path).read_text(encoding='utf-8'))
        return value if isinstance(value,dict) else {}
    except (OSError,ValueError):
        return {}


def _busy(path):
    """Probe an existing advisory lock without creating a state or lock file."""
    path=Path(path)
    if not path.exists():
        return False
    try:
        with path.open('r+b') as handle:
            if os.name=='nt':
                import msvcrt
                msvcrt.locking(handle.fileno(),msvcrt.LK_NBLCK,1)
                msvcrt.locking(handle.fileno(),msvcrt.LK_UNLCK,1)
            else:
                import fcntl
                fcntl.flock(handle.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
                fcntl.flock(handle.fileno(),fcntl.LOCK_UN)
        return False
    except OSError:
        return True


def _now():
    return datetime.now(TZ)


def _stamp(value):
    try:
        parsed=datetime.fromisoformat(value)
        return parsed.astimezone(TZ) if parsed.tzinfo else None
    except (TypeError,ValueError):
        return None


def _verified_pipeline(pipeline,target):
    """A completion marker cannot replace the current configured artifacts."""
    try:
        import yaml
        from data.research_pipeline import still_verified
        configuration=yaml.safe_load((ROOT/'config/research_pipeline.yaml').read_text(encoding='utf-8'))
        specifications=configuration['steps']
        inputs=configuration['required_inputs']
        if (not isinstance(inputs,list) or not inputs or len(set(inputs))!=len(inputs)
                or set(pipeline.get('required_inputs',[]))!=set(inputs)):
            return False
        required={spec['id'] for spec in specifications}
        if not required or len(required)!=len(specifications) or pipeline.get('status')!='complete' or pipeline.get('data_date')!=target:
            return False
        steps=pipeline.get('steps')
        if not isinstance(steps,list) or any(not isinstance(step,dict) for step in steps):
            return False
        indexed={step.get('id'):step for step in steps}
        if len(indexed)!=len(steps) or not required<=set(indexed):
            return False
        return all(still_verified(indexed[identity],target) for identity in required)
    except (OSError,ValueError,TypeError,KeyError,AttributeError):
        return False


def _core_data_ready(manifest,target):
    return bool(target and manifest.get('coverage',{}).get('daily',{}).get('end')==target
                and all(manifest.get('stages',{}).get(stage)=='complete'
                        and manifest.get('stage_dates',{}).get(stage)==target for stage in CORE_STAGES))


def _minute_status(runtime,manifest,target,now=None):
    try:
        from data.minute_readiness import public_status as minute_status
        selected = minute_status()
        same_runtime = (selected.get('research_runtime_dir')
                        and Path(selected['research_runtime_dir']).resolve() == Path(runtime).resolve())
        if same_runtime:
            ready = bool(selected.get('current_date_ready') and selected.get('cache_verified')
                         and selected.get('deep_verified') is True
                         and selected.get('target_date') == target
                         and selected.get('completed_symbols') == selected.get('expected_symbols') == 300)
            done, total = selected.get('completed_symbols', 0), selected.get('expected_symbols', 300)
            deadline = _stamp(selected.get('next_request_at'))
            return {**selected, 'ready': ready, 'blocked': not ready,
                    'start': selected.get('start_date'), 'end': selected.get('target_date'),
                    'requested_end': selected.get('target_date'), 'retry_at': selected.get('next_request_at'),
                    'cooldown_active': bool(deadline and (now or _now()) < deadline),
                    'automatic_completion_enabled': selected.get('service_alive') is True,
                    'message': '当前历史5分钟覆盖已核验' if ready else
                    f"历史5分钟已完成 {done}/{total} 只；" + ('后台持续补齐' if selected.get('service_alive') else '下载服务等待恢复')}
    except (ImportError, OSError, ValueError, KeyError, TypeError):
        pass
    coverage=manifest.get('coverage',{}).get('minute') or {}
    quota=manifest.get('source_cooldowns',{}).get('stk_mins') or {}
    universe=_json(Path(runtime)/'universe.json').get('codes')
    expected=len(set(universe)) if isinstance(universe,list) and len(set(universe))==300 else 300
    covered=coverage.get('covered_symbols',coverage.get('symbols_with_rows',0))
    covered=covered if type(covered) is int and covered>=0 else 0
    ready=bool(target and covered==expected and coverage.get('coverage_complete') is True and coverage.get('scope')=='current_hs300'
               and coverage.get('requested_end')==target and coverage.get('frequency')=='5min'
               and manifest.get('stages',{}).get('minute')=='complete'
               and manifest.get('stage_dates',{}).get('minute')==target)
    retry_at=quota.get('retry_at') or coverage.get('retry_at');deadline=_stamp(retry_at)
    category=quota.get('category') or coverage.get('status') or 'not_complete'
    return {'ready':ready,'blocked':not ready,'status':'complete' if ready else category,
            'covered_symbols':covered,'expected_symbols':expected,'rows':coverage.get('rows',0),
            'start':coverage.get('start'),'end':coverage.get('end'),'requested_end':coverage.get('requested_end'),
            'retry_at':retry_at,'cooldown_seconds':quota.get('cooldown_seconds') or coverage.get('cooldown_seconds'),
            'cooldown_active':bool(deadline and (now or _now())<deadline),'automatic_completion_enabled':False,
            'message':'当前历史5分钟覆盖已核验' if ready else f'历史5分钟仅覆盖{covered}/{expected}只；权限升级前不自动按小时补全'}


def public_status(cfg=None):
    cfg=cfg or load_config();runtime=Path(cfg['runtime_dir']);state=_json(runtime/'service-state.json')
    bootstrap=_json(runtime/'bootstrap-state.json');pipeline=_json(runtime/'pipeline-state.json')
    queue=_json(runtime/'update-request.json');heartbeat=_stamp(state.get('updated_at'))
    age=(_now()-heartbeat).total_seconds() if heartbeat else None
    alive=bool(_busy(runtime/'service.lock') and age is not None and 0<=age<=cfg['heartbeat_grace_seconds'])
    bootstrap_busy=_busy(runtime/'bootstrap.lock');pipeline_busy=_busy(runtime/'pipeline.lock')
    busy=bool(bootstrap_busy or pipeline_busy or alive and state.get('busy'))
    status=state.get('status','not_started') if alive else 'stopped'
    phase=state.get('phase') if alive else 'service_not_running'
    message=state.get('message','研究服务尚未启动') if alive else '研究更新服务未运行，请通过本地启动器启动'
    if bootstrap_busy and not state.get('busy'):
        phase='initializing';message='研究数据初始化正在运行，日常服务等待现有任务完成'
    progress={key:bootstrap.get(key) for key in ('stage','phase','unit','completed_units','total_units','rows','updated_at')}
    progress.update(completed=bootstrap.get('completed_units'),total=bootstrap.get('total_units'))
    manifest=_json(runtime/'bootstrap-manifest.json')
    target=state.get('target_date') if alive and state.get('busy') else manifest.get('coverage',{}).get('daily',{}).get('end')
    core_ready=_core_data_ready(manifest,target) and _verified_pipeline(pipeline,target)
    minute=_minute_status(runtime,manifest,target)
    if core_ready or pipeline_busy or bootstrap.get('stage') not in CORE_STAGES:
        steps = pipeline.get('steps') or []
        steps = list(steps.values()) if isinstance(steps, dict) else steps
        completed = sum(step.get('status') == 'success' for step in steps)
        progress = {'stage': 'core_research', 'phase': pipeline.get('status'),
                    'unit': pipeline.get('current_step'), 'completed': completed, 'total': len(steps),
                    'completed_units': completed, 'total_units': len(steps), 'updated_at': pipeline.get('updated_at')}
    if core_ready and alive and not busy:
        status = 'complete' if minute['ready'] else 'core_complete'
        message = '核心研究已核验；' + minute['message']
    return {'ok':True,'busy':busy,'status':status,'phase':phase,'message':message,'service_alive':alive,
            'queued':bool(queue.get('id')),'requested_at':queue.get('requested_at'),'next_run_at':state.get('next_run_at') if alive else None,
            'last_error':state.get('last_error'),'data_date':state.get('last_success_date'),'progress':progress,
            'core_ready':core_ready,'overall_ready':bool(core_ready and minute['ready']),
            'minute_blocked':minute['blocked'],'minute_coverage':minute,'minute_retry_at':minute['retry_at'],
            'bootstrap':{key:bootstrap.get(key) for key in ('status','phase','stage','end','target_end','stages','last_error','updated_at')},
            'pipeline':{key:pipeline.get(key) for key in ('status','running','data_date','current_step','updated_at','finished_at')}}


def request_update(payload=None,cfg=None):
    if payload is not None and (not isinstance(payload,dict) or payload):
        raise ValueError('更新请求只接受空对象；数据日期与来源由服务核实')
    cfg=cfg or load_config();runtime=Path(cfg['runtime_dir']);runtime.mkdir(parents=True,exist_ok=True)
    with _REQUEST_GUARD:
        try:
            with ProcessLock(runtime/'request.lock'):
                status=public_status(cfg)
                if not cfg['enabled'] or not status['service_alive']:
                    return {**status,'ok':False,'accepted':False,'http_status':503,'message':'研究服务未运行，请通过本地启动器启动'}
                if status['queued']:
                    return {**status,'accepted':False,'http_status':200}
                request={'id':uuid4().hex,'requested_at':_now().isoformat(),'source':'local_manual_request'}
                atomic_json(runtime/'update-request.json',request)
        except RuntimeError:
            return {**public_status(cfg),'accepted':False,'http_status':200,'message':'另一更新请求正在排队'}
    return {**public_status(cfg),'accepted':True,'http_status':202,'message':'已排队；核实官方收盘数据后更新，现有任务不会并发启动'}


class ResearchService:
    def __init__(self,cfg,*,now=None,bootstrap_factory=None,pipeline_runner=None):
        self.cfg=cfg;self.runtime=Path(cfg['runtime_dir']);self.now=now or _now
        self.bootstrap_factory=bootstrap_factory or (lambda config:ResearchBootstrap(config,now=self.now))
        self.pipeline_runner=pipeline_runner
        self.stop_event=threading.Event();self.current_bootstrap=None;self._guard=threading.Lock()
        old=_json(self.runtime/'service-state.json')
        self.state={'schema_version':1,'pid':os.getpid(),'source':'tushare-official','status':'starting','phase':'starting','busy':False,
                    'started_at':self.now().isoformat(),'last_success_date':old.get('last_success_date'),'last_error':old.get('last_error')}

    def save(self,**changes):
        with self._guard:
            self.state.update(changes);self.state['updated_at']=self.now().isoformat()
            atomic_json(self.runtime/'service-state.json',self.state)

    def stop(self):
        self.stop_event.set()
        if self.current_bootstrap is not None:
            self.current_bootstrap.stop_event.set()

    def _calendar(self,boot,now):
        path=self.runtime/'service-calendar.json';cached=_json(path)
        today=now.date().isoformat()
        if cached.get('through')==today and cached.get('source')=='tushare:trade_cal:SSE':
            sessions=cached.get('sessions')
            if isinstance(sessions,list) and sessions and all(isinstance(day,str) and re.fullmatch(r'\d{4}-\d{2}-\d{2}',day) for day in sessions):
                return sessions
        start=(now.date()-timedelta(days=self.cfg['calendar_lookback_days'])).isoformat()
        sessions=boot.calendar(start,today)
        if not sessions:
            raise ValueError('官方交易日历没有可用收盘交易日')
        atomic_json(path,{'source':'tushare:trade_cal:SSE','checked_at':now.isoformat(),'through':today,'start':start,'sessions':sessions})
        return sessions

    def _ready(self,boot,day):
        # Readiness does not mutate the bootstrap's optional historical-ST
        # policy or write any market cache before all three sources are ready.
        with ThreadPoolExecutor(max_workers=3) as pool:
            tasks=[pool.submit(boot._daily_rows,api,fields,day) for api,fields in
                   (('daily',DAILY),('adj_factor',('ts_code','trade_date','adj_factor')),('daily_basic',BASIC))]
            bars,factors,basics=[task.result() for task in tasks]
        if len(bars)<boot.cfg['min_daily_rows'] or not set(bars)<=set(factors) or not set(bars)<=set(basics):
            return False
        for code,row in bars.items():
            close=number(row['close'],False);factor=number(factors[code]['adj_factor'],False)
            basic_close=number(basics[code]['close'],False)
            if close<=0 or factor<=0 or abs(close-basic_close)>max(.011,close*1e-6):
                return False
        return True

    def _ack(self,request):
        if not request.get('id'):
            return
        with ProcessLock(self.runtime/'request.lock'):
            if _json(self.runtime/'update-request.json').get('id')==request['id']:
                (self.runtime/'update-request.json').unlink(missing_ok=True)

    def _core_finished(self,target,manifest,*,idle=False):
        # Called after update.lock is released: minute verification deliberately
        # rejects any active core writer, including this service's write phase.
        minute=_minute_status(self.runtime,manifest,target,self.now())
        if (not _core_data_ready(_json(self.runtime/'bootstrap-manifest.json'),target)
                or not _verified_pipeline(_json(self.runtime/'pipeline-state.json'),target)):
            self.save(status='waiting',phase='waiting_existing_job',busy=False,core_ready=False,overall_ready=False,
                      message='研究输入或分析产物已变化，等待重新核验',
                      next_run_at=(self.now()+timedelta(seconds=self.cfg['poll_seconds'])).isoformat())
            return dict(self.state)
        full=minute['ready']
        self.save(status='complete' if full else 'core_complete',phase='up_to_date' if idle else 'core_complete',busy=False,
                  target_date=target,last_success_date=target,last_error=None,core_ready=True,overall_ready=full,
                  minute_blocked=not full,minute_coverage=minute,minute_retry_at=minute['retry_at'],finished_at=self.state.get('finished_at') if idle else self.now().isoformat(),
                  message='核心研究已核验；'+minute['message'],next_run_at=(self.now()+timedelta(seconds=self.cfg['poll_seconds'])).isoformat())
        return dict(self.state)

    def cycle(self):
        """One serialized attempt; dependency locks are checked before API use."""
        self.runtime.mkdir(parents=True,exist_ok=True)
        try:
            # Serialize the entire cycle independently of its write phase, so
            # another cycle cannot start between unlock and minute verification.
            with ProcessLock(self.runtime/'cycle.lock'):
                with ProcessLock(self.runtime/'update.lock'):
                    result=self._cycle()
                if isinstance(result,_CoreCompletion):
                    return self._core_finished(result.target,result.manifest,idle=result.idle)
                return result
        except RuntimeError:
            return {'status':'busy','busy':True,'message':'另一研究更新正在运行'}

    def _cycle(self):
        now=self.now()
        if now.tzinfo is None:
            raise ValueError('研究服务需要带时区的本机时间')
        now=now.astimezone(TZ)
        due=_stamp(self.state.get('next_run_at'))
        if due is not None and now<due:
            return dict(self.state)
        request=_json(self.runtime/'update-request.json')
        if _busy(self.runtime/'bootstrap.lock') or _busy(self.runtime/'pipeline.lock'):
            self.save(status='waiting',phase='waiting_existing_job',busy=False,message='等待现有初始化或研究分析完成',next_run_at=(now+timedelta(seconds=self.cfg['poll_seconds'])).isoformat())
            return dict(self.state)
        try:
            config=research_config(self.cfg['research_config'])
            if Path(config['runtime_dir']).resolve()!=self.runtime.resolve() or config['end']!='latest':
                raise ValueError('研究服务目录或动态截止日配置不一致')
            boot=self.bootstrap_factory(config)
            sessions=self._calendar(boot,now)
            cutoff=now.date().isoformat() if now.strftime('%H:%M')>=self.cfg['earliest_close_check'] else (now.date()-timedelta(days=1)).isoformat()
            eligible=[day for day in sessions if day<=cutoff]
            if not eligible:
                raise ValueError('官方日历中尚无已收盘交易日')
            target=max(eligible)
            pipeline=_json(self.runtime/'pipeline-state.json')
            manifest=_json(self.runtime/'bootstrap-manifest.json')
            missing=[stage for stage in CORE_STAGES if manifest.get('stages',{}).get(stage)!='complete'
                     or manifest.get('stage_dates',{}).get(stage)!=target]
            if manifest.get('coverage',{}).get('daily',{}).get('end')!=target and 'daily' not in missing:
                missing.insert(0,'daily')
            finished=_verified_pipeline(pipeline,target) and not missing
            if finished and not request.get('id'):
                return _CoreCompletion(target,manifest,idle=True)
            self.save(status='checking',phase='verifying_official_close',busy=True,target_date=target,core_ready=False,overall_ready=False,
                      message='核实官方收盘行情、复权因子和每日指标')
            if not self._ready(boot,target):
                self.save(status='waiting',phase='awaiting_close_data',busy=False,message='官方收盘数据尚未齐备，保留待更新请求',
                          next_run_at=(now+timedelta(seconds=self.cfg['retry_seconds'])).isoformat())
                return dict(self.state)
            if self.stop_event.is_set():
                self.save(status='interrupted',phase='interrupted',busy=False,message='研究服务停止，待更新请求已保留')
                return dict(self.state)
            actual=manifest
            if missing:
                self.current_bootstrap=boot
                self.save(status='running',phase='bootstrap',busy=True,message='仅更新缺失或过期的核心研究缓存；分钟补全保持独立')
                result=boot.run(tuple(missing),target_date=target)
                self.current_bootstrap=None
                if result.get('status')=='interrupted' or self.stop_event.is_set():
                    self.save(status='interrupted',phase='interrupted',busy=False,message='研究更新已在断点处停止，待更新请求保留')
                    return dict(self.state)
                if result.get('status') in ('rate_limited','permission_denied','authentication'):
                    self.save(status=result['status'],phase='awaiting_official_source',busy=False,last_error=result.get('last_error'),
                              message='核心数据源尚不可用，保留断点与请求',next_run_at=result.get('retry_at') or (self.now()+timedelta(seconds=self.cfg['retry_seconds'])).isoformat())
                    return dict(self.state)
                actual=boot.manifest
                if result.get('status') not in ('complete','partial_complete'):
                    raise ValueError('核心研究缓存尚未完成')
            if not _core_data_ready(actual,target):
                raise ValueError('真实研究数据未完成本轮日期验收')
            self.save(status='running',phase='pipeline',busy=True,message='运行并核验研究分析产物')
            runner=self.pipeline_runner
            if runner is None:
                from data.research_pipeline import run
                runner=run
            result=runner()
            if not _verified_pipeline(result,target):
                raise ValueError('研究分析有未通过步骤')
            self._ack(request)
            return _CoreCompletion(target,actual)
        except Exception:
            self.current_bootstrap=None
            self.save(status='failed',phase='failed',busy=False,last_error='研究更新未完成；请查看本机初始化或分析状态，不沿用旧完成标记',
                      message='更新失败，断点和待更新请求已保留',next_run_at=(self.now()+timedelta(seconds=self.cfg['retry_seconds'])).isoformat())
        return dict(self.state)

    def run(self,once=False):
        self.runtime.mkdir(parents=True,exist_ok=True)
        with ProcessLock(self.runtime/'service.lock'):
            self.save(message='研究服务已启动，等待核实官方收盘数据')
            def heartbeat():
                while not self.stop_event.wait(self.cfg['heartbeat_seconds']):
                    self.save()
            thread=threading.Thread(target=heartbeat,name='research-heartbeat',daemon=True);thread.start()
            try:
                while not self.stop_event.is_set():
                    state=self.cycle()
                    if once:
                        return state
                    stamp=_stamp(state.get('next_run_at'))
                    delay=max(1,min(60,(stamp-self.now()).total_seconds())) if stamp else self.cfg['poll_seconds']
                    self.stop_event.wait(delay)
            finally:
                self.stop_event.set();thread.join(timeout=2)
                self.save(status='stopped',phase='stopped',busy=False,message='研究服务已停止；研究缓存和模拟账户均保留')


def start_service(base=None):
    """Launcher entry point. Starting the daemon does not perform an HTTP call."""
    base=Path(base or ROOT);cfg=load_config(base/'config/research_service.yaml')
    if not cfg['enabled']:
        return None
    runtime=Path(cfg['runtime_dir']);runtime.mkdir(parents=True,exist_ok=True)
    if _busy(runtime/'service.lock'):
        return None
    fd=os.open(runtime/'service.log',os.O_CREAT|os.O_WRONLY|os.O_APPEND,0o600)
    with os.fdopen(fd,'ab') as log:
        proc=subprocess.Popen([sys.executable,'-u','-m','data.research_service','run','--config',str(base/'config/research_service.yaml')],
                              cwd=base,stdin=subprocess.DEVNULL,stdout=log,stderr=log,start_new_session=True)
    (runtime/'service.pid').write_text(str(proc.pid)+'\n',encoding='utf-8')
    return proc


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command',choices=('run','once','status','request'))
    parser.add_argument('--config');args=parser.parse_args();cfg=load_config(args.config)
    if args.command in ('status','request'):
        result=public_status(cfg) if args.command=='status' else request_update(cfg=cfg)
    else:
        service=ResearchService(cfg)
        previous={signum:signal.signal(signum,lambda *_:service.stop()) for signum in (signal.SIGTERM,signal.SIGINT)}
        try:
            result=service.run(once=args.command=='once')
        finally:
            for signum,handler in previous.items():
                signal.signal(signum,handler)
    print(json.dumps(result,ensure_ascii=False,indent=2))
    if isinstance(result,dict) and (result.get('status') in ('failed','interrupted') or result.get('ok') is False):
        raise SystemExit(1)


if __name__=='__main__':
    main()
