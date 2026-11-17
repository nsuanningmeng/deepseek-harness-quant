"""Run the configured research chain with explicit, content-checked evidence.

Research outputs are independent of paperlive orders and cash. A failed stage
does not publish a success marker, and an earlier output cannot validate a run.
"""
import argparse
from datetime import datetime, timezone, timedelta
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
import yaml
from autotrade.locking import ProcessLock
from autotrade.monitoring import atomic_json

RUNTIME = ROOT / 'runtime/research'
ZONE = timezone(timedelta(hours=8))


def now():
    return datetime.now(ZONE).isoformat()


def load_json(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def latest(pattern):
    paths = list(ROOT.glob(pattern))
    if not paths:
        raise ValueError('尚无输入或输出：' + pattern)
    return max(paths, key=lambda path: path.stat().st_mtime_ns)


def file_digest(path):
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for chunk in iter(lambda: handle.read(1024*1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def still_verified(step, asof):
    if step.get('status') != 'success' or step.get('returncode') != 0 or not step.get('evidence'):
        return False
    for item in step['evidence']:
        path = ROOT / item.get('path', '')
        if (not path.is_file() or item.get('data_date') != asof
                or not item.get('sha256') or file_digest(path) != item['sha256']):
            return False
    return True


def selected_steps(specifications, only):
    """A changed input invalidates every downstream artifact, even on the same date."""
    if not only:
        return list(specifications)
    selected = set(only)
    if selected - {step['id'] for step in specifications}:
        raise ValueError('未知分析步骤')
    while True:
        expanded = selected | {step['id'] for step in specifications
                               if selected.intersection(step.get('depends', []))}
        if expanded == selected:
            break
        selected = expanded
    return [step for step in specifications if step['id'] in selected]


def evidence(spec, started, asof, algorithm):
    path = latest(spec['path'])
    if path.stat().st_mtime < started - 1:
        raise ValueError('本轮没有更新产物：' + spec['path'])
    if path.suffix == '.json':
        data = load_json(path)
        if not isinstance(data, (dict, list)) or not data:
            raise ValueError('产物为空或结构无效：' + path.name)
        if isinstance(data, dict) and (data.get('ok') is False or data.get('ready') is False or data.get('error') or data.get('blocked')):
            raise ValueError('产物报告失败或审计阻断：' + path.name)
        if isinstance(data, dict) and data.get('data_date') and data['data_date'] != asof:
            raise ValueError('产物数据日期与本轮已验证行情不一致：' + path.name)
        kind = spec.get('kind')
        if kind:
            from deck.system_live import _product_schema
            valid, rows, reason = _product_schema(kind, data)
            if not valid:
                raise ValueError(path.name + '：' + reason)
        elif isinstance(data, dict):
            rows = max([len(value) for value in data.values() if isinstance(value, (list, dict))] or [len(data)])
        else:
            rows = len(data)
    else:
        if path.stat().st_size == 0:
            raise ValueError('产物文件没有内容：' + path.name)
        rows = None
    return {'path':str(path.relative_to(ROOT)), 'source':'verified-local-research-cache',
            'algorithm':algorithm, 'data_date':asof, 'rows':rows, 'ok':True,
            'sha256':file_digest(path), 'verified_at':now()}


def run(only=None):
    RUNTIME.mkdir(parents=True, exist_ok=True)
    config = yaml.safe_load((ROOT/'config/research_pipeline.yaml').read_text())
    manifest = load_json(RUNTIME/'bootstrap-manifest.json')
    stages = manifest.get('stages', {})
    required_inputs = config['required_inputs']
    if any(stages.get(key) != 'complete' for key in required_inputs):
        raise ValueError('本研究链所需的日线、财报或指数ETF尚未完成初始化')
    asof = manifest['coverage']['daily']['end']
    if any(manifest.get('stage_dates', {}).get(key) != asof for key in required_inputs):
        raise ValueError('各输入阶段没有按同一目标交易日完成核验')
    from paperlive.config import load_config
    capital = str(load_config()['broker']['initial_cash'])
    todo = selected_steps(config['steps'], only)
    previous = load_json(RUNTIME/'pipeline-state.json') if (RUNTIME/'pipeline-state.json').exists() else {}
    retained = [step for step in previous.get('steps', []) if step.get('id') not in {item['id'] for item in todo}] if previous.get('data_date') == asof else []
    for step in retained:
        if step.get('status') == 'success' and not still_verified(step, asof):
            step.update(status='failed', error='原验收产物已变化，需要重新运行')
    state = {'schema_version':1, 'status':'running', 'running':True, 'started_at':now(),
             'data_date':asof, 'source':'tushare-official', 'steps':retained if only else []}
    state['required_inputs'] = required_inputs
    state['input_status'] = dict(stages)
    with ProcessLock(RUNTIME/'pipeline.lock'):
        atomic_json(RUNTIME/'pipeline-state.json',state)
        for spec in todo:
            step = {'id':spec['id'], 'label':spec['label'], 'status':'running', 'started_at':now(), 'evidence':[]}
            state['steps'].append(step)
            state['current_step']=spec['id'];state['updated_at']=now()
            atomic_json(RUNTIME/'pipeline-state.json',state)
            started=time.time()
            log_path=RUNTIME/(spec['id']+'.log')
            step['log']=str(log_path.relative_to(ROOT))
            print(f"[{now()}] {spec['label']} 开始",flush=True)
            try:
                succeeded={item['id'] for item in state['steps'] if item['status']=='success'}
                missing=set(spec.get('depends',[]))-succeeded
                if missing:
                    raise ValueError('前置步骤尚未通过：'+','.join(sorted(missing)))
                arguments=[]
                for arg in spec.get('args', []):
                    if arg=='$capital':arg=capital
                    elif arg=='$latest_pitch':arg=str(latest('logs/pitch_v2_*.json'))
                    arguments.append(str(arg))
                command=[sys.executable,'-u','-m',spec['module'],*arguments]
                fd=os.open(log_path,os.O_CREAT|os.O_WRONLY|os.O_TRUNC,0o600)
                with os.fdopen(fd,'wb') as log:
                    result=subprocess.run(command,cwd=ROOT,stdin=subprocess.DEVNULL,stdout=log,stderr=log,
                                          timeout=spec.get('timeout_seconds',1800))
                step['returncode']=result.returncode
                if result.returncode:
                    raise ValueError('分析进程退出失败；详情保存在本机步骤日志')
                step['evidence']=[evidence(item,started,asof,spec['module']) for item in spec.get('outputs',[])]
                if not step['evidence']:
                    raise ValueError('没有可核验的输出，不标记成功')
                step['status']='success'
                print(f"[{now()}] {spec['label']} 已验证",flush=True)
            except Exception as exc:
                step.update(status='failed',error=str(exc)[:300])
                print(f"[{now()}] {spec['label']} 失败：{step['error']}",flush=True)
            step['finished_at']=now()
            state['updated_at']=now()
            atomic_json(RUNTIME/'pipeline-state.json',state)
        required={spec['id'] for spec in config['steps']}
        succeeded={step['id'] for step in state['steps'] if step['status']=='success'}
        state.update(status='complete' if required<=succeeded else 'incomplete',running=False,
                     finished_at=now(),current_step=None)
        atomic_json(RUNTIME/'pipeline-state.json',state)
    return state


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--only',help='Comma-separated step identifiers for a verified retry')
    args=parser.parse_args()
    result=run(args.only.split(',') if args.only else None)
    print(json.dumps({'status':result['status'],'steps':[(s['id'],s['status']) for s in result['steps']]},ensure_ascii=False))
    raise SystemExit(0 if result['status']=='complete' else 1)
