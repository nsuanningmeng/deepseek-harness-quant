"""Named CNY ETF proxies, official adjusted prices, no trading entry point."""
import argparse
from datetime import datetime,timedelta,date
import hashlib,json,re,sqlite3,time
from pathlib import Path
import numpy as np
import pandas as pd
import yaml
from autotrade.monitoring import atomic_json
from autotrade.providers import TushareHttpClient
from factors import research_context as context

BASE=Path(__file__).resolve().parents[1]

def load_config(path=None):
    cfg=yaml.safe_load((Path(path) if path else BASE/'config/global_rotation.yaml').read_text())
    codes=[row['symbol'] for row in cfg['assets']]
    if not codes or len(set(codes))!=len(codes) or any(not re.fullmatch(r'\d{6}\.(SH|SZ)',c) for c in codes):raise ValueError('资产必须有明确唯一代码')
    if min(cfg['momentum_window'],cfg['moving_average_window'],*cfg['return_windows'])<2 or not 0<=cfg['r2_minimum']<=1:raise ValueError('轮动窗口无效')
    return cfg

def read(path):
    try:
        value=json.loads(Path(path).read_text());return value if isinstance(value,dict) else {}
    except (OSError,ValueError):return {}

def inputs(cfg):
    from data.research_bootstrap import load_config as research_config
    core=research_config(context.path(cfg['research_config']));m=read(Path(core['runtime_dir'])/'bootstrap-manifest.json')
    target=m.get('coverage',{}).get('daily',{}).get('end')
    if m.get('source')!='tushare-official' or not target or m.get('stages',{}).get('assets')!='complete' or m.get('stage_dates',{}).get('assets')!=target:raise ValueError('当前指数与ETF输入未验收')
    sessions=sorted(k[6:] for k in m.get('completed',{}) if k.startswith('daily:') and k[6:]<=target)
    return core,target,sessions

def momentum(series,window=25,annualization=252,weight_start=1.,weight_end=2.):
    values=pd.to_numeric(pd.Series(series),errors='coerce').tail(window).to_numpy(dtype=float)
    if len(values)!=window or not np.isfinite(values).all() or (values<=0).any():return None
    x=np.arange(window);y=np.log(values);w=np.linspace(weight_start,weight_end,window)
    xm,ym=np.average(x,weights=w),np.average(y,weights=w)
    variance=np.average((x-xm)**2,weights=w)
    if variance<=0:return None
    slope=np.average((x-xm)*(y-ym),weights=w)/variance
    total=np.sum(w*(y-ym)**2);r2=1-np.sum(w*(y-ym-slope*(x-xm))**2)/total if total>0 else 0.
    return float(slope*annualization),float(np.clip(r2,0,1))

def query(client,endpoint,fields,**params):
    try:
        table=client.query(endpoint,fields=','.join(fields),**params)
        if not set(fields)<=set(table.columns):raise ValueError('fields')
        rows=table.to_dict('records')
        if not isinstance(rows,list) or any(not isinstance(r,dict) or not set(fields)<=set(r) for r in rows):raise ValueError('records')
        return rows
    except Exception:raise ValueError('官方全球资产接口 '+endpoint+' 未完成；检查权限、网络或字段') from None

def identity(cfg,target,client):
    fingerprint=hashlib.sha256(json.dumps(cfg['assets'],sort_keys=True,ensure_ascii=False).encode()).hexdigest()
    path=context.path(cfg['identity_file']);cached=read(path)
    if cached.get('date')==target and cached.get('configuration')==fingerprint:return cached['assets']
    rows=query(client,'fund_basic',('ts_code','name','market','list_date'),market='E',status='L')
    bycode={row['ts_code']:row for row in rows};result={}
    for item in cfg['assets']:
        row=bycode.get(item['symbol'])
        if not row or row['market']!='E' or item['identity_contains'] not in row['name']:raise ValueError('代理证券官方身份不匹配：'+item['symbol'])
        result[item['symbol']]={'official_name':row['name'],'listed_date':str(row['list_date']),'source':'tushare:fund_basic'}
    atomic_json(path,{'date':target,'configuration':fingerprint,'assets':result,'source':'tushare:fund_basic'})
    return result

def build(cfg=None,*,client=None,sleeper=None):
    cfg=cfg or load_config();core,target,sessions=inputs(cfg);sleeper=sleeper or time.sleep
    if client is None:
        from paperlive.config import load_config as paper_config
        from paperlive.data_settings import tushare_connection
        client=TushareHttpClient(tushare_connection(paper_config()),core['request_timeout'])
    names=identity(cfg,target,client);cache=context.path(cfg['cache_database']);cache.parent.mkdir(parents=True,exist_ok=True)
    frames={};sources=[]
    with sqlite3.connect(cache) as local:
        local.execute('CREATE TABLE IF NOT EXISTS raw_fund(symbol TEXT,date TEXT,close REAL,adj_factor REAL,PRIMARY KEY(symbol,date))')
        for item in cfg['assets']:
            code=item['symbol']
            if item['core_cache']:
                with context.connect(Path(core['cache_dir'])/'bars.db') as con:
                    frame=pd.read_sql_query("SELECT date,close FROM daily_bar WHERE code=? AND adjust='qfq' AND source='tushare' AND date BETWEEN ? AND ? ORDER BY date",con,params=(code,cfg['history_start'],target))
                source='core:tushare:fund_daily+fund_adj:qfq'
            else:
                last=local.execute('SELECT MAX(date) FROM raw_fund WHERE symbol=?',(code,)).fetchone()[0]
                if not last or last<target:
                    first=(date.fromisoformat(last)+timedelta(days=1)).isoformat() if last else cfg['history_start']
                    params={'ts_code':code,'start_date':first.replace('-',''),'end_date':target.replace('-','')}
                    rows=query(client,'fund_daily',('ts_code','trade_date','close'),**params);sleeper(cfg['request_interval_seconds'])
                    adj=query(client,'fund_adj',('ts_code','trade_date','adj_factor'),**params);sleeper(cfg['request_interval_seconds'])
                    if max(len(rows),len(adj))>=cfg['maximum_source_rows']:raise ValueError('基金响应达到上限，拒绝截断')
                    factors={};tuples=[];seen=set()
                    for row in adj:
                        day=datetime.strptime(str(row['trade_date']),'%Y%m%d').date().isoformat();factor=float(row['adj_factor'])
                        if row['ts_code']!=code or not first<=day<=target or day in factors or not np.isfinite(factor) or factor<=0:raise ValueError('基金复权因子无效')
                        factors[day]=factor
                    for row in rows:
                        day=datetime.strptime(str(row['trade_date']),'%Y%m%d').date().isoformat();price=float(row['close'])
                        if row['ts_code']!=code or not first<=day<=target or day in seen or not np.isfinite(price) or price<=0 or day not in factors:raise ValueError('基金行情/实际复权因子缺失')
                        seen.add(day);tuples.append((code,day,price,factors[day]))
                    if not tuples:raise ValueError('基金新增日期行情为空：'+code)
                    local.executemany('INSERT OR REPLACE INTO raw_fund VALUES(?,?,?,?)',tuples);local.commit()
                frame=pd.read_sql_query('SELECT date,close,adj_factor FROM raw_fund WHERE symbol=? AND date<=? ORDER BY date',local,params=(code,target))
                if frame.empty:raise ValueError('基金缓存为空')
                frame['close']=frame['close']*frame['adj_factor']/frame['adj_factor'].iloc[-1]
                source='tushare:fund_daily+fund_adj:qfq'
            if frame.empty or frame['date'].iloc[-1]!=target:raise ValueError('基金数据未覆盖当前研究日：'+code)
            frames[code]=frame.set_index('date')['close'];sources.append({'symbol':code,'source':source,'rows':len(frame)})
    needed=max(cfg['momentum_window'],cfg['moving_average_window'],max(cfg['return_windows'])+1);expected=sessions[-needed:]
    if len(expected)!=needed:raise ValueError('缺少最少完整交易日历史')
    assets=[]
    for item in cfg['assets']:
        code=item['symbol'];s=frames[code].reindex(expected)
        if s.isna().any() or not np.isfinite(s).all() or (s<=0).any():raise ValueError('近期交易日缺口，不补价：'+code)
        value=momentum(s,cfg['momentum_window'],cfg['annualization_sessions'],cfg['weight_start'],cfg['weight_end'])
        if value is None:raise ValueError('动量缺少真实最少历史')
        ann,r2=value
        row={'symbol':code,'name':item['name'],'official_name':names[code]['official_name'],'category':item['category'],'asof':target,
             'last':float(s.iloc[-1]),'mom_ann':ann,'r2':r2,'mom_score':ann*r2,'above_ma10':bool(s.iloc[-1]>s.tail(cfg['moving_average_window']).mean())}
        row.update({'ret'+str(n):float(s.iloc[-1]/s.iloc[-n-1]-1) for n in cfg['return_windows']});assets.append(row)
    indices=[]
    with context.connect(Path(core['cache_dir'])/'bars.db') as con:
        for item in cfg['indices']:
            rows=con.execute("SELECT date,close FROM daily_bar WHERE code=? AND adjust='none' AND source='tushare' AND date<=? ORDER BY date DESC LIMIT ?",(item['symbol'],target,cfg['moving_average_window'])).fetchall()
            if len(rows)!=cfg['moving_average_window'] or {r[0] for r in rows}!=set(sessions[-cfg['moving_average_window']:]):raise ValueError('四指数投票历史缺失')
            indices.append({**item,'asof':target,'below_ma':bool(rows[0][1]<np.mean([r[1] for r in rows]))})
    votes=sum(r['below_ma'] for r in indices);weak=votes>=cfg['weak_votes_required'];assets.sort(key=lambda r:r['mom_score'],reverse=True)
    candidates=[r for r in assets if r['r2']>=cfg['r2_minimum'] and r['above_ma10'] and r['mom_score']>0]
    defensive={**candidates[0],'code':candidates[0]['symbol'],'score':candidates[0]['mom_score'],'note':'规则观察，未校准，不产生自动委托'} if weak and candidates else None
    result={'ok':True,'ready':True,'initialized':True,'state':'ready','date':target,'data_date':target,'kind':'global_proxy_rotation',
            'source':'tushare:fund_daily+fund_adj; verified index_daily','generated_at':datetime.now().astimezone().isoformat(),
            'assets':assets,'asset_count':len(assets),'indices':indices,'a_share_weak':weak,'weak_vote':votes,'global_rotation':defensive,'defensive':defensive,'source_files':sources,
            'configuration_fingerprint':hashlib.sha256(json.dumps(cfg,sort_keys=True).encode()).hexdigest(),
            'method':{'momentum_window':cfg['momentum_window'],'annualization_sessions':cfg['annualization_sessions'],'formula':'weighted log-price slope * annualization * R2',
                      'weights':[cfg['weight_start'],cfg['weight_end']],'r2_minimum':cfg['r2_minimum'],'moving_average_window':cfg['moving_average_window']},
            'calibrated':False,'auto_trade_allowed':False,'limitations':['境内人民币ETF代理，包含汇率、溢折价和跟踪误差，不等同海外本体收益。',
            '两只黄金ETF具有重复风险敞口，不构成独立分散资产。','只有规则截面，无收益实证或自动调仓许可；不使用旧乱码身份推断。']}
    atomic_json(context.path(cfg['output_file']),result);return result

def widget():
    cfg=load_config();result=read(context.path(cfg['output_file']));_,target,_=inputs(cfg)
    if (not result.get('ready') or result.get('data_date')!=target
            or {r.get('symbol') for r in result.get('assets',[])}!={r['symbol'] for r in cfg['assets']}
            or result.get('configuration_fingerprint')!=hashlib.sha256(json.dumps(cfg,sort_keys=True).encode()).hexdigest()):raise ValueError('全球轮动产物日期、配置或资产覆盖不完整')
    return result

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--config');args=parser.parse_args();result=build(load_config(args.config))
    print(json.dumps({k:result[k] for k in ('ok','data_date','asset_count','weak_vote')},ensure_ascii=False))
