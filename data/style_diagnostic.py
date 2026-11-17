"""Observed size-strength diagnostic; no calibrated factor weights or orders."""
import argparse,hashlib,json
from datetime import datetime
from pathlib import Path
import numpy as np
import pandas as pd
import yaml
from autotrade.monitoring import atomic_json
from factors import research_context as context
from strategy.style_timing import small_large_strength

BASE=Path(__file__).resolve().parents[1]

def load_config(path=None):
    cfg=yaml.safe_load((Path(path) if path else BASE/'config/style_diagnostic.yaml').read_text())
    if cfg['scope']!='current_hs300' or not 0<cfg['large_fraction']<.5 or not 0<cfg['small_fraction']<.5:raise ValueError('风格范围或分组无效')
    if min(cfg['strength_window'],cfg['smooth_window'],cfg['minimum_members'],cfg['minimum_group_members'])<1 or not 0<cfg['minimum_universe_coverage']<=1:raise ValueError('风格历史或样本要求无效')
    return cfg

def configuration_fingerprint(cfg):
    return hashlib.sha256(json.dumps(cfg,sort_keys=True,ensure_ascii=False).encode()).hexdigest()

def classify(panel,values,cfg,asof,expected):
    need=cfg['strength_window']+cfg['smooth_window']
    window=panel.loc[:asof].tail(need).apply(pd.to_numeric,errors='coerce')
    if len(window)!=need:raise ValueError('风格缺少完整预热')
    capital=pd.to_numeric(values,errors='coerce')
    capital=capital.where(np.isfinite(capital))
    valid=window.columns[(np.isfinite(window) & (window>0)).all() & capital.reindex(window.columns).gt(0)]
    if len(valid)<cfg['minimum_members'] or len(valid)/expected<cfg['minimum_universe_coverage']:raise ValueError('风格完整价格/当期市值覆盖不足')
    strength=small_large_strength(window[valid],capital.to_dict(),cfg['strength_window'],cfg['smooth_window'],
               large_pct=cfg['large_fraction'],small_pct=cfg['small_fraction'],minimum_members=cfg['minimum_members'],minimum_group=cfg['minimum_group_members']).iloc[-1]
    if not np.isfinite(strength):raise ValueError('风格分组重叠、样本或平滑历史不足')
    label=cfg['labels']['small'] if strength>cfg['neutral_band'] else cfg['labels']['large'] if strength<-cfg['neutral_band'] else cfg['labels']['neutral']
    return {'date':asof,'dominant':label,'strength':float(strength),'members':len(valid),'universe_count':expected,'coverage':len(valid)/expected}

def build(cfg=None):
    cfg=cfg or load_config()
    from data.research_bootstrap import load_config as research_config
    core=research_config(context.path(cfg['research_config']));runtime=Path(core['runtime_dir'])
    manifest=json.loads((runtime/'bootstrap-manifest.json').read_text());target=manifest.get('coverage',{}).get('daily',{}).get('end')
    if (manifest.get('source')!='tushare-official' or manifest.get('stages',{}).get('daily')!='complete'
            or not target or manifest.get('stage_dates',{}).get('daily')!=target):raise ValueError('风格输入日线尚未验收')
    codes=sorted(context.universe());sessions=sorted(k[6:] for k in manifest['completed'] if k.startswith('daily:') and k[6:]<=target)
    frame=context.read_bars('code,date,close',adjust='qfq',symbols=codes)
    frame=frame[frame['date']<=target];panel=frame.pivot(index='date',columns='code',values='close').reindex(sessions)
    month_ends=pd.Series(sessions,index=pd.to_datetime(sessions)).resample('ME').last().dropna()
    # Only completed calendar months enter history; the current date is a separate diagnostic.
    historical=[value for value in month_ends if str(value)[:7]<target[:7]][-cfg['history_months']:]
    points=sorted(set([*historical,target]));placeholders=','.join('?' for _ in codes);days=','.join('?' for _ in points)
    with context.connect(Path(core['cache_dir'])/'daily_basic.db') as con:
        values=pd.read_sql_query(f'SELECT code,date,circ_mv FROM daily_basic WHERE code IN ({placeholders}) AND date IN ({days})',con,params=(*codes,*points))
    capital=values.pivot(index='date',columns='code',values='circ_mv')
    history=[];missing=[]
    for point in points:
        try:
            if point not in capital.index:raise ValueError('该日缺少真实市值')
            row=classify(panel,capital.loc[point],cfg,point,len(codes))
            if point==target:current=row
            else:history.append(row)
        except ValueError as exc:
            if point==target:raise
            missing.append({'date':point,'reason':str(exc)})
    scope=context.scope_metadata()
    result={'ok':True,'ready':True,'initialized':True,'state':'ready','kind':'size_style_diagnostic','date':target,'data_date':target,
            'current_style':current['dominant'],'style':current['dominant'],'strength':current['strength'],'coverage':current['coverage'],
            'members':current['members'],'style_history':history,'missing_history':missing,'research_scope':scope,
            'weights':{},'weights_enabled':False,'calibrated':False,'auto_trade_allowed':False,
            'source':'tushare:qfq daily prices + same-date daily_basic.circ_mv',
            'configuration_fingerprint':configuration_fingerprint(cfg),
            'source_files':[str(Path(core['cache_dir'])/'bars.db'),str(Path(core['cache_dir'])/'daily_basic.db')],
            'method':{'strength_window':cfg['strength_window'],'smooth_window':cfg['smooth_window'],'large_fraction':cfg['large_fraction'],
                      'small_fraction':cfg['small_fraction'],'grouping':'same-asof market-cap quantiles; fixed groups over backward observation window',
                      'returns':'equal-weight observed returns, no ffill; complete member price windows'},
            'generated_at':datetime.now().astimezone().isoformat(),
            'note':'当前沪深300内部市值组的60日相对强弱、20日平滑；是规则诊断，未校准，不产生因子加减权或委托。',
            'limitations':[scope['limitation'],'按观察日已知市值分组描述此前收益，不是可执行的前向策略或旧F3/H27/H30实证复现。']}
    atomic_json(context.path(cfg['output_file']),result)
    atomic_json(context.path(cfg['ui_output_directory'])/('factor_style_rotation_'+target+'.json'),result)
    return result

def public_state():
    try:
        from data.research_bootstrap import load_config as research_config
        cfg=load_config();path=context.path(cfg['output_file'])
        result=json.loads(path.read_text());core=research_config(context.path(cfg['research_config']))
        manifest=json.loads((Path(core['runtime_dir'])/'bootstrap-manifest.json').read_text())
        target=manifest.get('coverage',{}).get('daily',{}).get('end')
        if (not result.get('ready') or result.get('data_date')!=target or result.get('kind')!='size_style_diagnostic'
                or result.get('configuration_fingerprint')!=configuration_fingerprint(cfg)
                or result.get('research_scope',{}).get('universe_fingerprint')!=context.scope_metadata()['universe_fingerprint']
                or result.get('weights') or result.get('weights_enabled') is not False
                or result.get('calibrated') is not False or result.get('auto_trade_allowed') is not False
                or not np.isfinite(float(result.get('strength',float('nan'))))):raise ValueError('stale')
        return {**result,'fresh':True,'snap_file':str(path),'divergence_60':None,'ind_breadth_60':None,
                'evidence':'实际当期市值/价格规则诊断；未校准，不提供加减权'}
    except (OSError,ValueError,KeyError,TypeError):return {'style':'未知','ready':False,'fresh':False,'note':'市值风格诊断日期、配置或数据范围尚未通过验收'}

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--config');args=parser.parse_args();r=build(load_config(args.config))
    print(json.dumps({k:r[k] for k in ('ok','data_date','current_style','strength','members')},ensure_ascii=False))
