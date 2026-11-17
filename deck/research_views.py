"""Read-only adapters for initialized research artifacts. Never starts data or trading jobs."""
import csv
from contextlib import closing
import hashlib
from datetime import datetime
import json
import math
import re
from pathlib import Path
import sqlite3

import yaml

BASE = Path(__file__).resolve().parent.parent


def locations():
    path = BASE / "config/research_factors.yaml"
    cfg = yaml.safe_load(path.read_text(encoding="utf-8")) if path.is_file() else {}
    def local(value):
        path = Path(value)
        return path if path.is_absolute() else BASE / path
    from factors.research_context import cache_dir
    return local((cfg or {}).get("output_dir", "data/factorpool/output")), cache_dir()


def read_json(path):
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}


def newest(directory, pattern):
    files = list(Path(directory).glob(pattern))
    return max(files, key=lambda p:p.stat().st_mtime) if files else None


def number(value):
    try:
        value = float(value)
        return value if math.isfinite(value) else None
    except (TypeError, ValueError):
        return None


def unavailable(reason, optional=False):
    bootstrap = read_json(BASE/"runtime/research/bootstrap-state.json")
    pipeline = read_json(BASE/"runtime/research/pipeline-state.json")
    state = "optional_unconfigured" if optional else "initializing" if "running" in (bootstrap.get("status"),pipeline.get("status")) else "missing_data"
    return {"ok": optional, "state":state,"initialized":False,"ready":False,"reason":reason,
            "message":reason,"readiness":{"state":state,"initialized":False,"ready":False,"reason":reason}}


def factor_inputs():
    output, cache = locations()
    path = newest(output,"factor_manifest_*.json")
    manifest = read_json(path) if path else {}
    declared = manifest.get("factors") or []
    health_path = output/"health"/("health_"+str(manifest.get("health_date"))+".csv")
    if not health_path.is_file():
        health_path = newest(output/"health","health_*.csv")
    rows=[]
    if health_path:
        with health_path.open(encoding="utf-8-sig",newline="") as handle:
            for raw in csv.DictReader(handle):
                row=dict(raw)
                for key in row:
                    if key.startswith(("icir","t20","t60","t120")) or key in ("n_days","direction","coverage","crowding","ic_win_rate","primary_holding_sessions"):
                        row[key]=number(row[key])
                rows.append(row)
    supported={row.get("code"):row for row in declared if isinstance(row,dict) and row.get("code")}
    rows=[row for row in rows if row.get("factor") in supported]
    return output,cache,manifest,rows,supported


def factor_dashboard():
    output,_,manifest,rows,supported=factor_inputs()
    if not supported or len(rows)!=len(supported) or not manifest.get("analysis"):
        return {**unavailable("真实因子截面与历史评估尚未生成"),"manifest":{},"health":{}}
    date=manifest.get("data_date") or manifest.get("date")
    path=newest(output,"crowding_*.json");crowd=read_json(path) if path else {}
    pct=number(crowd.get("crowding_pctile_252"))
    daily_path=output/"daily_scores"/("daily_"+str(date)+".csv")
    flags={};total=0
    if daily_path.is_file():
        with daily_path.open(encoding="utf-8-sig",newline="") as handle:
            for row in csv.DictReader(handle):
                total+=1
                for key,value in row.items():
                    if key.startswith("flag_") and str(value).strip() in ("1","1.0","true","True"):
                        flags[key]=flags.get(key,0)+1
    risk_path=newest(output/"risk","risk_multiplier_*.json")
    usage_path=newest(output/"ui_data","factor_usage_*.json")
    usage=read_json(usage_path) if usage_path else {}
    purification=factor_purification(rows,supported,usage.get("layers") or {})
    return {"ok":True,"state":"ready","initialized":True,"ready":True,"ts":datetime.now().isoformat(timespec="seconds"),
            "source":manifest.get("source"),"data_date":date,"analysis":manifest.get("analysis"),
            "manifest":{"date":date,"n":len(supported),"factors":list(supported.values()),"health_date":manifest.get("health_date"),"health_stale":manifest.get("health_date")!=date},
            "health":{"date":date,"n":len(rows),"rows":rows},"categories":{category:sum(row.get("category")==category for row in supported.values()) for category in {row.get("category") for row in supported.values()}},
            "purification":purification,
            "flags":{"date":date,"total":total,"counts":flags,"note":"只展示当前已支持公式实际生成的标记；空标记列表不代表其他风险已排除。"},
            "crowding":{"mkt":number(crowd.get("crowding_mkt")),"pctile":pct/100 if pct is not None else None,"pctile_percent":pct,
                        "zone":crowd.get("zone"),"n_crowded":crowd.get("n_crowded_stocks"),"date":crowd.get("data_date") or crowd.get("date"),"scope":crowd.get("scope"),"method":crowd.get("method")},
            "risk":read_json(risk_path) if risk_path else {"ready":False,"reason":"研究风险系数尚未生成"}}


def factor_purification(rows,supported,layers):
    """Use health's real classification; a statistic never overrides disabled use."""
    result=[];counts={}
    for row in rows:
        code=row.get("factor");meta=supported.get(code) or {};status=str(row.get("status") or "尚未核验")
        disabled=(number(meta.get("direction"))==0 or number(row.get("direction"))==0
                  or code in (layers.get("disabled") or {}) or any(word in status for word in ("停用","禁用")))
        state="停用" if disabled else status
        if disabled:
            execution="停用，不参与选股；ICIR仅保留诊断值"
        elif code in (layers.get("deferred") or {}):
            execution="暂缓，仅作研究参考，不自动参与交易"
        elif code in (layers.get("frc_mine") or {}):
            execution="仅作风险研究，不代表交易放行"
        elif code in (layers.get("ext_decision") or {}):
            execution="按配置进入研究决策；尚不代表自动交易放行"
        else:
            execution="用途尚未核验，不进入自动决策"
        counts[state]=counts.get(state,0)+1
        result.append({"factor":code,"state":state,"health_status":status,"disabled":disabled,
                       "direction":number(meta.get("direction")),"icir120":number(row.get("icir120")),
                       "execution":execution})
    return {"rows":result,"counts":counts,"note":"分类沿用本轮实际健康检验；停用与用途设置优先于ICIR数值，统计有效不等于可自动交易。"}


def style_diagnostic_state():
    """Only the current configured diagnostic can supply public style state."""
    if not (BASE / 'config/style_diagnostic.yaml').is_file():
        return {"ready":False,"fresh":False,"reason":"尚未配置独立市值风格诊断"}
    try:
        from data import style_diagnostic
        if style_diagnostic.BASE.resolve() != BASE.resolve():
            return {"ready":False,"fresh":False,"reason":"风格数据不属于当前研究目录"}
        result=style_diagnostic.public_state()
        if (result.get('kind')!='size_style_diagnostic' or result.get('ready') is not True
                or result.get('fresh') is not True):
            return {"ready":False,"fresh":False,"reason":result.get('note') or '市值风格诊断尚未通过当前日期和配置验收'}
        return result
    except (OSError,ValueError,KeyError,TypeError):
        return {"ready":False,"fresh":False,"reason":"市值风格诊断暂不可读"}


def factor_ui_pack():
    output,_,manifest,_,supported=factor_inputs()
    fields={"usage":("factor_usage","layers"),"lifecycle":("factor_lifecycle","factors"),"corr":("factor_corr","matrix"),
            "frc_rules":("frc_mine_rules","rules"),"freshness":("factor_data_freshness","updated"),
            "alert_rules":("factor_alert_rules","rules"),"style_rotation":("factor_style_rotation","current_style")}
    result={"ok":True,"data_date":manifest.get("data_date") or manifest.get("date"),"factor_count":len(supported),"modules":{}}
    style_configured=(BASE/'config/style_diagnostic.yaml').is_file()
    for key,(prefix,required) in fields.items():
        path=newest(output/"ui_data",prefix+"_*.json")
        data=read_json(path) if path else {}
        if key=='style_rotation':
            data=style_diagnostic_state()
            path=None
        valid=bool(data.get(required))
        if key=='style_rotation':
            valid=bool(valid and data.get('ready') is True and data.get('fresh') is True
                       and data.get('data_date')==result['data_date'])
        reason="已读取实际研究产物" if valid else "尚未配置独立风格轮动研究，未生成加减权建议" if key=="style_rotation" else "所需研究产物尚未生成或业务结构为空"
        if key=='style_rotation' and not valid and style_configured:
            reason=data.get('reason') or '当前日期的市值风格诊断尚未通过验收'
        state="limited_research" if valid and key=="style_rotation" and (data.get("calibrated") is False or data.get("weights_enabled") is False) else "ready" if valid else "optional_unconfigured" if key=="style_rotation" and not style_configured else "missing_data"
        if state=="limited_research":reason="市值风格诊断未校准，不产生加减权或自动交易"
        result[key]=data if valid else {"state":state,"ready":False,"reason":reason}
        result["modules"][key]={"state":state,"initialized":valid,"ready":valid,"reason":reason,"data_date":data.get("data_date") or data.get("date"),"file":path.name if path else None}
    required=[value for key,value in result["modules"].items() if key!="style_rotation" or style_configured]
    complete=all(value["ready"] for value in required)
    result.update(ok=complete,state="ready" if complete else "missing_data",initialized=complete,ready=complete,
                  reason="已读取实际支持因子的研究数据包" if complete else "必要因子研究数据包尚未完整生成；逐项原因见modules")
    return result


def factor_performance():
    _,cache,_,_,_=factor_inputs()
    path=cache/"unified.db"
    if not path.is_file():
        return {**unavailable("因子前向归因库尚未初始化"),"factors":[],"detail":[],"n_factors":0,"n_rows":0}
    try:
        with sqlite3.connect(path.resolve().as_uri()+"?mode=ro",uri=True,timeout=2) as connection:
            connection.row_factory=sqlite3.Row
            aggs=[dict(row) for row in connection.execute("SELECT factor,n_pitch,n_done_t5,t1_avg,t5_avg,t5_win,excess_avg FROM factor_agg ORDER BY n_pitch DESC")]
            detail=[dict(row) for row in connection.execute("SELECT factor,code,entry_date,pool_type,t1,t5 FROM factor_pitch ORDER BY entry_date DESC LIMIT 80")]
        mature=any(row.get("t1_avg") is not None or row.get("t5_avg") is not None for row in aggs)
        state="ready" if mature else "pending_history"
        reason="已读取实际前向样本" if mature else "归因库已初始化，真实入池样本尚未到期；没有填充收益和胜率"
        return {"ok":True,"state":state,"initialized":True,"ready":mature,"reason":reason,
                "n_factors":len(aggs),"n_rows":len(detail),"factors":aggs,"detail":detail,
                "readiness":{"state":state,"initialized":True,"ready":mature,"reason":reason}}
    except sqlite3.Error:
        return {**unavailable("因子归因库缺少所需业务表"),"factors":[],"detail":[],"n_factors":0,"n_rows":0}


def factor_ranking():
    _,cache,manifest,rows,supported=factor_inputs()
    if not rows:
        return {**unavailable("真实因子评估尚未生成"),"ranking":[],"config":{}}
    weights={};performance={};history_initialized=False
    try:
        with sqlite3.connect((cache/"unified.db").resolve().as_uri()+"?mode=ro",uri=True,timeout=2) as connection:
            weights={key:number(value) for key,value in connection.execute("SELECT key,value FROM evidence_weights")}
            performance={factor:{"n":n,"t1":t1,"t5":t5,"win":win} for factor,n,t1,t5,win in connection.execute("SELECT factor,n_pitch,t1_avg,t5_avg,t5_win FROM factor_agg")}
            history_initialized=True
    except sqlite3.Error:
        pass
    ranked=[]
    for item in rows:
        name=item["factor"];meta=supported[name];perf=performance.get(name,{})
        direction=number(meta.get("direction"));icir=number(item.get("icir120"))
        usable=direction not in (0,None) and icir is not None
        metric=perf.get("t5") if perf.get("t5") is not None else perf.get("t1")
        ranked.append({"factor":name,"name_cn":meta.get("name_cn"),"direction":direction,"status":item.get("status"),
                       "icir120":icir,"n_pitch":perf.get("n",0 if history_initialized else None),"live_metric":metric,
                       "t1_avg":perf.get("t1"),"t5_avg":perf.get("t5"),"t5_win":perf.get("win"),"src_factors":[name] if perf else [],
                       "bt_rank":None,"live_rank":None,"composite":None,"rank":None,"live_eff":None,"eligible":usable})
    historical=sorted([row for row in ranked if row["eligible"]],key=lambda row:-row["icir120"])
    for index,row in enumerate(historical,1):row["bt_rank"]=index
    live=sorted([row for row in ranked if row["eligible"] and row["live_metric"] is not None],key=lambda row:-row["live_metric"])
    for index,row in enumerate(live,1):row["live_rank"]=index
    weight,minimum=weights.get("live_w"),weights.get("min_live_samples")
    for row in historical:
        eff=weight*min(1,row["n_pitch"]/minimum) if row["live_rank"] is not None and weight is not None and minimum and row["n_pitch"] is not None else 0.
        row["live_eff"]=eff
        row["composite"]=row["bt_rank"]*(1-eff)+row["live_rank"]*eff if row["live_rank"] is not None else float(row["bt_rank"])
    historical.sort(key=lambda row:row["composite"])
    for index,row in enumerate(historical,1):row["rank"]=index
    excluded=[row for row in ranked if not row["eligible"]]
    return {"ok":True,"state":"ready","initialized":True,"ready":True,"data_date":manifest.get("data_date") or manifest.get("date"),
            "ranking":historical+excluded,"config":{**weights,"note":"仅真实成熟IC参与历史排序；停用或缺IC的因子保留但不分配名次。未成熟前向样本不产生实战排名。"},
            "forward_state":"ready" if live else "pending_history" if history_initialized else "missing_data"}


def factor_history():
    """Expose verified fixed-horizon evaluations, not the retired T+1 table."""
    _, _, manifest, _, supported = factor_inputs()
    config_path = BASE/'config/research_factors.yaml'
    cfg = yaml.safe_load(config_path.read_text()) if config_path.is_file() else {}
    directory = Path((cfg or {}).get('report_dir', 'report'))
    path = (directory if directory.is_absolute() else BASE/directory)/'factor_pool_report.json'
    report = read_json(path)
    target = read_json(BASE/'runtime/research/bootstrap-manifest.json').get('coverage', {}).get('daily', {}).get('end')
    date = manifest.get('data_date') or manifest.get('date')
    if (not target or date != target or report.get('data_date') != target
            or not supported or not verified_artifact(path)):
        return {**unavailable('当前因子历史检验尚无同日且通过产物校验的证据'), 'rows': []}
    rows = []
    for name, meta in supported.items():
        for key, value in (report.get('evaluations', {}).get(name) or {}).items():
            if not isinstance(value, dict):
                continue
            horizon = value.get('holding_sessions')
            fraction = number(value.get('top_fraction'))
            if type(horizon) is not int or horizon <= 0 or str(horizon) != str(key) or fraction is None or not 0 < fraction < 1:
                continue
            direction = number(meta.get('direction'))
            rows.append({'factor': name, 'name_cn': meta.get('name_cn'), 'direction': direction,
                         'holding_sessions': horizon, 'top_fraction': fraction,
                         'mean_top_net_return': number(value.get('mean_top_net_return')),
                         'mean_top_excess': number(value.get('mean_top_excess')),
                         'mean_ic': number(value.get('mean_ic')), 'n_sections': value.get('n_sections'),
                         'first_signal_date': value.get('first_signal_date'), 'last_signal_date': value.get('last_signal_date'),
                         'state': 'diagnostic_only' if direction == 0 else value.get('state'),
                         'return_note': value.get('return_note')})
    horizons = sorted({row['holding_sessions'] for row in rows})
    primary = (cfg or {}).get('primary_horizon')
    if primary not in horizons:
        primary = horizons[0] if horizons else None
    return {'ok': bool(rows), 'initialized': bool(rows), 'ready': bool(rows),
            'state': 'ready' if rows else 'missing_data', 'data_date': target,
            'source': report.get('source'), 'source_file': str(path), 'factor_count': len({row['factor'] for row in rows}),
            'primary_horizon': primary, 'holding_horizons': horizons, 'rows': rows,
            'note': '当前沪深300回看存在成分选择偏差。收益为各信号截面的固定持有期净收益均值，不是连续净值或年化；方向0仅作诊断。',
            'icir_note': 'ICIR120是主持有期最近120个成熟截面的ICIR，不是持有120天的收益。'}


def inventory_units(item):
    """The legacy size column means rows for tables and bytes for files."""
    raw = item.get('size')
    value = None if isinstance(raw, bool) else number(raw)
    value = int(value) if value is not None and value >= 0 and value.is_integer() else None
    table = item.get('kind') == 'table'
    file = item.get('kind') in ('db', 'csv', 'other', 'cold_file')
    return {**item, 'row_count': value if table else None, 'size_bytes': value if file else None,
            'size_unit': 'rows' if table else 'bytes' if file else None}


def production_consumers(value):
    excluded = {'tests', 'test', 'runtime', 'backups', 'backup', '__pycache__',
                'site-packages', 'node_modules', '.git', '.venv', 'venv'}
    result = set()
    for raw in str(value or '').split(';'):
        text = raw.strip().replace('\\', '/')
        path = Path(text)
        parts = {part.lower() for part in path.parts}
        if (not text or path.is_absolute() or '..' in parts or parts & excluded
                or path.name.startswith('test_') or path.name == 'conftest.py'):
            continue
        result.add(text)
    return sorted(result)


def data_assets():
    """Read only the small unified inventory; never rescan market databases."""
    _, cache = locations()
    out = {'ok': True, 'ts': datetime.now().isoformat(timespec='seconds'),
           'tables': [], 'inventory': [], 'lineage': [], 'inventory_schema': 2,
           'lineage_note': '静态代码引用快照，已排除测试和运行备份；不是运行时全部依赖。'}
    optional = {'event_series': '可选旧事件库索引；当前研究链未采用此表',
                'market_style_daily': '旧版市场风格快照；当前市值风格使用独立诊断产物'}
    try:
        with closing(sqlite3.connect((cache/'unified.db').resolve().as_uri()+'?mode=ro', uri=True, timeout=3)) as con:
            tables = [row[0] for row in con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")]
            for name in tables:
                count = con.execute('SELECT COUNT(*) FROM "'+name.replace('"', '""')+'"').fetchone()[0]
                row = {'name': name, 'rows': count, 'optional': name in optional}
                if name in optional:
                    row.update(state='optional_unused' if count == 0 else 'optional_available',
                               note=optional[name] if count == 0 else '可选旧数据表，已保留实际记录')
                out['tables'].append(row)
            con.row_factory = sqlite3.Row
            out['inventory'] = [inventory_units(dict(row)) for row in con.execute('SELECT * FROM asset_inventory')]
            for item in con.execute('SELECT * FROM data_lineage'):
                row = dict(item); consumers = production_consumers(row.get('consumers'))
                if consumers:
                    out['lineage'].append({**row, 'consumers': ';'.join(consumers), 'consumer_paths': consumers})
    except sqlite3.Error:
        out.update(ok=False, error='数据资产快照暂不可读')
    return out


def _minute_background_job():
    try:
        from data.minute_readiness import public_status
        result=public_status()
        return result if isinstance(result,dict) else {"ok":False,"status":"failed","service_alive":False}
    except ImportError:
        return {"ok":False,"status":"not_started","service_alive":False,"limitations":["分钟下载服务尚未安装或启动"]}
    except (OSError,ValueError,KeyError):
        return {"ok":False,"status":"failed","service_alive":False,"last_error":"分钟后台状态暂不可读"}


from deck.background_snapshot import BackgroundSnapshot
_minute_samples = BackgroundSnapshot('minute-display-samples', retry_seconds=5)


def _read_minute_samples(path, scope):
    """At most 30 indexed point reads, only after the independent deep proof."""
    _, start, end, _, symbols = scope
    with sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True,timeout=.1) as connection:
        metadata=dict(connection.execute('SELECT name,value FROM minute_metadata'))
        if metadata.get('frequency')!='5min':
            raise ValueError('分钟库未声明真实5分钟频率')
        connection.row_factory=sqlite3.Row
        latest=[]
        for code in symbols[:30]:
            row=connection.execute('SELECT code,trade_time,open,high,low,close,volume,amount,source FROM minute_5m '
                                   'WHERE code=? AND trade_time>=? AND trade_time<=? ORDER BY trade_time DESC LIMIT 1',
                                   (code,start+' 00:00:00',end+' 23:59:59')).fetchone()
            if row:latest.append(dict(row))
    return {'metadata':metadata,'latest':latest}


def minute_snapshot(limit=30):
    """Fast receipt progress; full-window validation belongs to the minute verifier."""
    from deck.minute_scope import query_scope
    job=_minute_background_job()
    common={'ok':True,'ready':False,'initialized':False,'state':'initializing','frequency':'5min',
            'background_job':job,'database_path':job.get('database_path'),'source':job.get('source'),
            'expected_symbols':job.get('expected_symbols'),'covered_symbols':None,
            'completed_symbols':job.get('completed_symbols',0),'downloaded_rows':job.get('rows'),
            'rows':None,'total_cache_rows':None,'rows_basis':'pending_deep_verification',
            'window_start':job.get('start_date'),'window_end':job.get('target_date'),
            'current_core_data_date':job.get('current_core_data_date'),'fresh_for_current_core':False,
            'download_status':job.get('status'),'coverage_complete':False,'latest':[],
            'data_date':None,'asof':None,'volume_unit':None,'amount_unit':None,
            'one_minute':{'state':'optional_unconfigured','ready':False,'reason':'没有生成或宣称1分钟竞价数据'}}
    try:
        scope=query_scope(job)
        path=Path(job['database_path']).expanduser()
        path=path if path.is_absolute() else BASE/path
        stat=path.stat()
    except (KeyError,TypeError,ValueError,OSError):
        return {**common,**unavailable('所选分钟任务或缓存范围尚未通过核验；不读取旧库冒充当前进度'),
                'rows':None,'total_cache_rows':None}
    deep=bool(all(job.get(key) is True for key in ('deep_verified','cache_verified','frozen_window_complete'))
              and job.get('status')=='complete' and job.get('completed_symbols')==job.get('expected_symbols'))
    fresh=bool(deep and job.get('current_date_ready') is True and job.get('current_universe_match') is True)
    ready=False;state='initializing';reason='后台正在按本轮日期和股票池补齐；回执进度不等于全池强验通过'
    sample_data={};sample_state='not_started'
    if job.get('core_update_in_progress'):
        reason='行情缓存正被后台任务使用，稍后核验；暂不进行完整覆盖核验'
    elif job.get('verification_status')=='failed' or job.get('status')=='failed':
        state='failed';reason=job.get('verification_error') or job.get('last_error') or '分钟业务核验未通过'
    elif deep:
        wal=Path(str(path)+'-wal');wal_stat=wal.stat() if wal.exists() else None
        version=(scope,stat.st_mtime_ns,stat.st_size,(wal_stat.st_mtime_ns,wal_stat.st_size) if wal_stat else None)
        sample=_minute_samples.read(version,lambda:_read_minute_samples(path,scope))
        sample_state=sample['state']
        if sample['current']:
            sample_data=sample['data'];ready=fresh;state='ready' if ready else 'stale'
            reason=('当前研究日和股票池的完整5分钟窗口已通过独立强验；不表示盘中撮合已启用' if ready
                    else '固定历史窗口已通过强验，尚未匹配当前研究日或股票池')
        else:
            state='probing' if sample['loading'] else 'failed'
            reason='完整窗口已通过独立强验，正在后台读取少量行情样本' if sample['loading'] else '历史行情样本读取失败，将冷却后重试；未把旧样本视为当前结果'
    elif job.get('verification_status')=='verifying':
        state='probing';reason='下载回执已齐，后台正在逐股核验真实窗口与日线；通过前不视为全池就绪'
    elif job.get('status')=='rate_limited':
        state='rate_limited';reason='数据源限频，后台按许可时间继续补齐本轮回执'
    metadata=sample_data.get('metadata') or {};latest=(sample_data.get('latest') or [])[:max(1,min(30,int(limit)))]
    common.update(ready=ready,initialized=deep,state=state,reason=reason,
                  rows=job.get('rows') if deep else None,rows_basis='deep_verification' if deep else 'pending_deep_verification',
                  covered_symbols=job.get('covered_symbols') if deep else None,coverage_complete=deep,
                  fresh_for_current_core=ready,rate_limited=state=='rate_limited',retry_at=job.get('next_request_at'),
                  data_date=job.get('target_date') if deep else None,latest=latest,sample_state=sample_state,
                  asof=max((row['trade_time'] for row in latest),default=None),
                  volume_unit=metadata.get('volume_unit'),amount_unit=metadata.get('amount_unit'),
                  scope='fixed_hs300_snapshot',readiness={'state':state,'initialized':deep,'ready':ready,'reason':reason})
    return common


def turnlow_top(top_n=20):
    output,cache,manifest,_,_=factor_inputs()
    date=manifest.get("data_date") or manifest.get("date")
    path=output/"daily_scores"/("daily_"+str(date)+".csv")
    if not path.is_file():
        return {**unavailable("实际低换手截面尚未生成"),"items":[]}
    with path.open(encoding="utf-8-sig",newline="") as handle:
        rows=list(csv.DictReader(handle))
    values=[{"code":row.get("code"),"rank":number(row.get("turn_low_rank")),"turn20":number(row.get("turn_low"))} for row in rows]
    values=[row for row in values if row["code"] and row["rank"] is not None and row["turn20"] is not None]
    if not values:
        return {**unavailable("低换手实际数值和排序均缺失，未填充中性排名"),"items":[]}
    names={}
    try:
        with sqlite3.connect((cache/"stock_basic.db").resolve().as_uri()+"?mode=ro",uri=True,timeout=2) as connection:
            names=dict(connection.execute("SELECT code,name FROM stock_basic"))
    except sqlite3.Error:
        pass
    values.sort(key=lambda row:-row["rank"])
    items=[{**row,"name":names.get(row["code"],row["code"])} for row in values[:max(1,min(int(top_n),len(values)))]]
    return {"ok":True,"state":"ready","initialized":True,"ready":True,"date":date,"data_date":date,"file":path.name,
            "coverage_pct":len(values)/len(rows)*100 if rows else None,"top_n":len(items),"items":items,
            "note":"真实20日平均换手截面，排名越高换手越低；仅2019年起研究数据，不是买入指令，未声称已通过组合收益验证。"}


def verified_artifact(path):
    """A source file is research evidence only after the recorded algorithm/hash check."""
    if path is None or not Path(path).is_file():
        return False
    state=read_json(BASE/"runtime/research/pipeline-state.json")
    for step in state.get("steps") or []:
        if step.get("status") not in ("complete","success") or step.get("returncode")!=0:
            continue
        evidence=step.get("evidence") or []
        if isinstance(evidence,dict):evidence=[evidence]
        for item in evidence:
            candidate=Path(item.get("path",""))
            candidate=candidate if candidate.is_absolute() else BASE/candidate
            if candidate.resolve()==Path(path).resolve() and item.get("source") and item.get("algorithm") and item.get("sha256"):
                return hashlib.sha256(Path(path).read_bytes()).hexdigest()==item["sha256"]
    return False


def opportunity_pool_history():
    """Observed local snapshots only; no historical prices are backfilled into pool history."""
    observations=[]
    for path in sorted((BASE/"logs").glob("opp_pool_*.json")):
        match=re.fullmatch(r"opp_pool_(\d{8})_(\d{6})\.json",path.name)
        if not match:continue
        try:
            generated=datetime.strptime(''.join(match.groups()),'%Y%m%d%H%M%S')
            data=read_json(path)
            day=datetime.strptime(str(data.get('date')),'%Y-%m-%d').date().isoformat()
            ops=data.get('opportunities');pitch=data.get('pitch')
            if not isinstance(ops,list) or not isinstance(pitch,list):continue
            if any(not isinstance(row,dict) or not row.get('code') for row in ops+pitch):continue
            observations.append({'date':generated.date().isoformat(),'generated_at':generated.isoformat(),
                                 'data_date':day,'n':len(ops),'pitch_n':len(pitch),
                                 'source_file':str(path.relative_to(BASE)),
                                 'sha256':hashlib.sha256(path.read_bytes()).hexdigest(),
                                 'current_pipeline_verified':verified_artifact(path)})
        except (OSError,ValueError,TypeError):continue
    latest={}
    for row in sorted(observations,key=lambda item:item['generated_at']):latest[row['date']]=row
    history=list(latest.values())
    valid=bool(history and any(row['current_pipeline_verified'] for row in observations))
    result={'kind':'observed_opportunity_pool_history','history':history,'observations':observations,
            'n_days':len(history),'first_date':history[0]['date'] if history else None,'last_date':history[-1]['date'] if history else None,
            'source':'actual_local_generated_pool_snapshots','reconstructed_backtest':False,
            'note':'仅记录本机真实生成过的机会池快照；生成日与所用行情日分别保留。没有倒造更早日期或历史收益，旧快照是否仍有当前管道证据逐项标明。'}
    return {**({'ok':True,'state':'ready','initialized':True,'ready':True,'reason':'已读取实际生成的机会池快照日期'} if valid else unavailable('尚无带当前运行证据的真实机会池快照')),**result}


def research_archives():
    """Expose completed research evidence without claiming an unrun standalone strategy."""
    records=[]
    def record(name,title,category,path,data,url,summary,**metrics):
        analysis=data.get("analysis") if isinstance(data.get("analysis"),dict) else data.get("meta") or {}
        records.append({"name":name,"title":title,"category":category,"verdict":"历史研究，未独立验证",
                        "kind":"research_evidence","validation_stage":"in_sample_research",
                        "generated_at":data.get("generated_at") or data.get("ts") or data.get("date"),
                        "data_date":data.get("data_date") or data.get("date"),"source":str(path.relative_to(BASE)),
                        "analysis":analysis,"evidence_summary":summary,"evidence_url":url,
                        "limitations":analysis.get("limitations") or [analysis.get("limitation")] if analysis.get("limitation") else analysis.get("limitations") or [],
                        "annual_return":metrics.get("annual_return"),"max_drawdown":metrics.get("max_drawdown"),"sharpe":metrics.get("sharpe"),
                        "factors":metrics.get("factors",[]),"json":str(path),"html":None,"has_html":False,"is_latest":True,"key":name})
    path=newest(BASE/"logs","sim_tracks*.json")
    if verified_artifact(path):
        data=read_json(path)
        for key,track in (data.get("tracks") or {}).items():
            curve=track.get("curve") or []
            if len(curve)<2 or any(number(point.get("nav")) is None for point in curve):
                continue
            stats=track.get("stats") or {}
            record("historical_track_"+key,"历史策略路径 · "+key,"历史研究",path,data,"/api/sim",
                   str(len(curve))+"个实际历史区间；与真实纸面账户前向收益分开，执行与费用口径以来源元数据为准。",
                   annual_return=number(stats.get("ann_ret")),max_drawdown=number(stats.get("max_dd")),sharpe=number(stats.get("sharpe")))
    path=BASE/"report/factor_pool_report.json"
    if verified_artifact(path):
        data=read_json(path)
        evaluations=data.get("evaluations") or {}
        if evaluations:
            record("supported_factor_evaluations","实际支持因子的RankIC评估","因子研究",path,data,"/factors",
                   str(len(evaluations))+"个实际公式的成熟持有窗口评估；固定窗口收益不转换成假年化，未视为实盘前向结果。",factors=list(evaluations))
    path=BASE/"logs/opportunity_winrates.json"
    if verified_artifact(path):
        data=read_json(path)
        if data.get("results"):
            record("opportunity_rule_calibration","机会规则历史校准","规则校准",path,data,"/api/winrates",
                   "本机真实历史样本校准；样本数量、持有窗口、当前股票池选择偏差及执行口径以来源记录为准。",factors=list(data["results"]))
    return records
