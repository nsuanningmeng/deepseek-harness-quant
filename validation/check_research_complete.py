"""Read-only, content-based acceptance of local research initialization.

No downloads, model calls, orders, synthetic fills, or credential reads occur here.
Use --output to save a local, public-metadata-only audit artifact.
"""
import argparse
from datetime import datetime, timezone, timedelta
import json
from pathlib import Path
import sqlite3
import sys
import time

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from data.cache import CACHE_DIR
from deck.system_live import _api_health, _research_status
from data.research_pipeline import still_verified


def read_json(path):
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return {}


def _wait_for_minute_status(reader, wait_seconds=180, *, clock=time.monotonic, pause=time.sleep):
    """Wait for cold read verification without treating a busy cache as missing data."""
    deadline = clock() + max(0, wait_seconds)
    while True:
        status = reader(wait=True)
        if all(status.get(key) is True for key in ('deep_verified', 'cache_verified', 'current_date_ready')):
            return status
        if status.get('verification_status') == 'failed' or status.get('verification_error'):
            return status
        remaining = deadline - clock()
        if remaining <= 0:
            return status
        pause(min(1, remaining))


def selected_minute_status():
    from data.minute_readiness import public_status
    return _wait_for_minute_status(public_status)


def database_coverage(manifest=None, minute_status=None):
    # Explicit fixture manifests never reach into a running production minute job.
    supplied_manifest=manifest is not None
    manifest = manifest or read_json(ROOT / 'runtime/research/bootstrap-manifest.json')
    minute_status=(minute_status if minute_status is not None else {} if supplied_manifest else selected_minute_status())
    coverage = manifest.get('coverage', {})
    asof = (coverage.get('daily') or {}).get('end')
    direct_dates = {'daily_bar': asof, 'daily_basic': asof, 'minute_5m': asof,
                    'market_daily': asof, 'factor_health_snap': asof,
                    'hist_mv': asof[:7] if asof else None}
    source_keys = {'finance_report':'finance', 'quality':'quality', 'financials_ts':'finance_ts',
                   'daily_basic':'daily_basic', 'hist_mv':'hist_mv', 'stock_basic':'stock_basic'}
    valid_values = {
        'daily_bar':'close>0 AND open>0 AND low>0 AND high>=close AND high>=open AND low<=close AND low<=open',
        'daily_basic':'total_mv>0 AND circ_mv>0', 'finance_report':'net_profit IS NOT NULL AND revenue IS NOT NULL',
        'quality':'roe_avg IS NOT NULL', 'financials_ts':'total_hldr_eqy_exc_min_int IS NOT NULL',
        'hist_mv':'circ_mv>0', 'minute_5m':'close>0 AND open>0 AND low>0 AND high>=close AND high>=open AND low<=close AND low<=open AND volume>=0 AND amount>=0',
        'social_finance':'sf_increment IS NOT NULL', 'bond_yield':'y10 IS NOT NULL AND y2 IS NOT NULL',
        'epu_monthly':'epu>0', 'market_daily':'timing_score IS NOT NULL',
        'factor_health_snap':"factor IS NOT NULL AND factor!=''", 'macro_series':'value IS NOT NULL',
    }
    specifications = [
        ('bars.db', 'daily_bar', 'date', "adjust='qfq' AND source='tushare'", True),
        ('daily_basic.db', 'daily_basic', 'date', '1=1', True),
        ('stock_basic.db', 'stock_basic', 'ipo_date', '1=1', True),
        ('finance.db', 'finance_report', 'period', '1=1', True),
        ('finance_quality.db', 'quality', 'period', '1=1', True),
        ('finance_ts.db', 'financials_ts', 'end_date', '1=1', True),
        ('hist_mv.db', 'hist_mv', 'month', '1=1', True),
        (minute_status.get('database_path') or 'minute.db', 'minute_5m', 'trade_time', '1=1', True),
        ('macro.db', 'social_finance', 'month', '1=1', True),
        ('macro.db', 'bond_yield', 'date', '1=1', True),
        ('policy/epu.db', 'epu_monthly', 'month', '1=1', True),
        ('unified.db', 'macro_series', 'period', '1=1', True),
        ('unified.db', 'market_daily', 'date', '1=1', True),
        ('unified.db', 'factor_health_snap', 'date', '1=1', True),
        ('factor_pitch_perf.db', 'factor_agg', None, '1=1', False),
    ]
    out = []
    for filename, table, column, where, needs_rows in specifications:
        path = Path(filename)
        path=path if path.is_absolute() else (ROOT/path if table=='minute_5m' and minute_status.get('database_path') else CACHE_DIR/path)
        row = {'database': path.name, 'path':str(path), 'table': table, 'initialized': False}
        params=[]
        try:
            if table=='minute_5m':
                from deck.minute_scope import query_scope, sql_filter
                where,params=sql_filter(query_scope(minute_status))
            with sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=10) as con:
                query = 'COUNT(*)' + (f',MIN({column}),MAX({column})' if column else '')
                values = con.execute(f'SELECT {query} FROM [{table}] WHERE {where}',params).fetchone()
                row.update(rows=values[0], start=values[1] if column else None,
                           end=values[2] if column else None,
                           initialized=bool(values[0]) or not needs_rows)
                if not values[0] and not needs_rows:
                    row['state'] = 'pending_history'
                if table in valid_values:
                    valid = con.execute(f'SELECT COUNT(*) FROM [{table}] WHERE ({where}) AND ({valid_values[table]})',params).fetchone()[0]
                    row['valid_numeric_rows'] = valid
                    if not valid or table in ('daily_bar','minute_5m') and valid != values[0]:
                        row.update(initialized=False, reason='必需数值为空或行情数值无效')
                if table in direct_dates:
                    target = direct_dates[table]
                    actual = str(row.get('end') or '')[:len(target or '')]
                    if not target or actual != target:
                        row.update(initialized=False, reason='源库最新日期与已验证目标日期不一致', expected_end=target)
                source = coverage.get(source_keys.get(table, ''), {})
                if source:
                    if values[0] != source.get('rows'):
                        row.update(initialized=False, reason='源库行数与初始化覆盖记录不一致', expected_rows=source.get('rows'))
                    if table in ('finance_report','quality','financials_ts') and str(row.get('end')).replace('-','') != str(source.get('end')).replace('-',''):
                        row.update(initialized=False, reason='财报覆盖日期与初始化记录不一致', expected_end=source.get('end'))
                if table == 'minute_5m':
                    row['total_cache_rows']=con.execute('SELECT COUNT(*) FROM minute_5m').fetchone()[0]
                    symbols={value[0] for value in con.execute('SELECT DISTINCT code FROM minute_5m WHERE '+where,params)}
                    from factors.research_context import universe
                    expected=universe() if minute_status else set()
                    row.update(symbols=len(symbols),expected_symbols=len(expected),source=minute_status.get('source'),
                               fixed_window=[minute_status.get('start_date'),minute_status.get('target_date')],
                               deep_verified=minute_status.get('deep_verified') is True)
                    proof_ok=all(minute_status.get(key) is True for key in ('deep_verified','cache_verified','frozen_window_complete','current_date_ready','current_universe_match'))
                    if (not proof_ok or not expected or symbols!=expected or minute_status.get('expected_symbols')!=len(expected)
                            or values[0]!=minute_status.get('rows') or minute_status.get('target_date')!=asof):
                        row.update(initialized=False,state=minute_status.get('status') or 'missing_data',reason='所选历史分钟源尚未通过实际全池、receipt深验、固定窗口与当前研究日期验收')
                if table == 'daily_bar':
                    expected = (coverage.get('daily',{}).get('rows') or 0) + (coverage.get('etf',{}).get('rows') or 0)
                    if values[0] != expected:
                        row.update(initialized=False, reason='前复权股票/ETF行数与已验证覆盖不一致', expected_rows=expected)
                if table in ('social_finance','epu_monthly') and row.get('end') and asof:
                    month = str(row['end']).replace('-','')
                    lag = int(asof[:4])*12+int(asof[5:7])-int(month[:4])*12-int(month[4:6])
                    if not 0 <= lag <= 3:
                        row.update(initialized=False, reason='宏观月度数据超出允许发布滞后')
                if table == 'bond_yield' and row.get('end') and asof:
                    lag = (datetime.fromisoformat(asof)-datetime.fromisoformat(row['end'])).days
                    if not 0 <= lag <= 10:
                        row.update(initialized=False, reason='国债数据超出允许发布滞后')
        except (OSError, ValueError, sqlite3.Error) as exc:
            row['initialized'] = False
            row['error_type'] = type(exc).__name__
        out.append(row)
    return out


def paper_feed_status(cfg,state,now=None):
    """A quote counter is not proof of current, executable intraday prices."""
    from paperlive.models import Quote
    from paperlive.activation import quote_acceptance
    now=now or datetime.now(timezone(timedelta(hours=8)))
    market=state.get('market') or {};activation=state.get('activation') or {}
    parsed={}
    for raw in state.get('quotes') or []:
        try:
            quote=Quote.from_dict(raw)
            if quote.symbol in parsed:raise ValueError('duplicate quote')
            parsed[quote.symbol]=quote
        except (ValueError,TypeError,KeyError,OverflowError):
            return {'ready':False,'state':'not_ready','reason':'逐股报价结构或唯一性未通过验收'}
    universe={row.get('symbol'):row for row in state.get('universe') or [] if isinstance(row,dict) and row.get('symbol')}
    eligible=set(state.get('eligible_symbols') or [])
    expected_count=(cfg.get('activation') or {}).get('expected_universe_count',300)
    universe_ok=bool(len(eligible)==expected_count and eligible<=set(universe))
    accepted=quote_acceptance(cfg,parsed,{symbol:universe[symbol] for symbol in eligible if symbol in universe},now)
    tradable=set(accepted['tradable']);expected=set(activation.get('tradable_symbols') or [])
    local_open=any(start<=now.strftime('%H:%M')<end for start,end in cfg['market']['trading_sessions'])
    ready=bool(market.get('provider')==cfg['market']['provider'] and market.get('open') is True
               and market.get('date')==now.date().isoformat() and local_open
               and market.get('calendar_verified') is True and not market.get('heartbeat_stale')
               and not market.get('error') and not state.get('paused')
               and activation.get('ready') is True and activation.get('status')=='active'
               and universe_ok and accepted['ok'] and expected and expected==tradable)
    return {'ready':ready,'fresh_executable_quotes':len(tradable),'required_tradable_symbols':len(expected),
            'expected_universe_count':expected_count,'eligible_symbols':len(eligible),'full_quote_coverage':accepted['ok'],
            'state':'ready' if ready else 'waiting_data' if activation.get('armed') and activation.get('status')=='waiting_data' else 'deferred' if state.get('paused') else 'not_ready',
            'reason':'全池身份、逐股源和接收时效、盘口、实际限价及当前连续交易时段均已核验' if ready else '报价数量或旧门控记录不能证明当前盘中可执行性；全池覆盖、逐股时效与交易时段仍须通过'}


def audit():
    runtime = ROOT / 'runtime/research'
    products = _api_health()
    research = _research_status(products)
    pipeline = read_json(runtime / 'pipeline-state.json')
    asof = pipeline.get('data_date')
    import yaml
    configured = yaml.safe_load((ROOT / 'config/research_pipeline.yaml').read_text())['steps']
    required_steps = {step['id'] for step in configured}
    observed_ids = [step.get('id') for step in pipeline.get('steps', [])]
    pipeline_contract_ok = (set(observed_ids) == required_steps and len(observed_ids) == len(required_steps))
    stages = [{
        'id': step.get('id'), 'status': step.get('status'),
        'artifact_integrity': still_verified(step, asof),
    } for step in pipeline.get('steps', [])]
    minute_status=selected_minute_status()
    databases = database_coverage(minute_status=minute_status)
    ai = read_json(runtime / 'ai-verification.json')
    rt_probe = read_json(runtime / 'rt-permission-verification.json')
    from paperlive.config import load_config
    from paperlive.runtime import public_state
    cfg=load_config()
    raw_paper = read_json(Path(cfg['runtime_dir'])/'state.json')
    state=public_state(cfg,raw_paper,{})
    market = state.get('market', {})
    paper = {key: market.get(key) for key in
             ('provider', 'source', 'open', 'phase', 'quote_count', 'calendar_verified')}
    paper['has_source_error'] = bool(market.get('error'))
    paper['permission_blocked'] = any(word in str(market.get('error', '')).lower()
                                      for word in ('权限', 'permission', '未开通'))
    paper.update(runtime_dir=cfg['runtime_dir'],paused=state.get('paused'),activation=state.get('activation'))
    feed=paper_feed_status(cfg,state)
    result = {
        'checked_at': datetime.now(timezone(timedelta(hours=8))).isoformat(),
        'data_date': asof,
        'research_initialized': bool(research.get('initialized') and pipeline_contract_ok and stages
                                     and all(row['artifact_integrity'] for row in stages)
                                     and all(row['initialized'] for row in databases)),
        'core_research_initialized': bool(research.get('initialized') and pipeline_contract_ok and stages
                                          and all(row['artifact_integrity'] for row in stages)
                                          and all(row['initialized'] for row in databases if row['table'] != 'minute_5m')),
        'initialized_products': research.get('initialized_count', 0),
        'uninitialized_products': research.get('uninitialized_count', len(products)),
        'pending_forward_history': research.get('pending_history_count', 0),
        'products': products, 'stages': stages, 'databases': databases,
        'selected_minute':minute_status,
        'pipeline_contract_ok': pipeline_contract_ok,
        'required_step_count': len(required_steps),
        'ai': {key: ai.get(key) for key in ('ok', 'model', 'checked_at', 'elapsed_seconds')},
        'paper_market': paper,
        'realtime_permission_check': {key: rt_probe.get(key) for key in ('endpoint','checked_at','ok','phase')},
        'realtime_permission_check_scope':'历史Tushare专项探测，不代表当前混合行情源状态' if cfg['market']['provider']!='tushare' else 'Tushare专项探测；新鲜度仍由当前逐股报价核验',
        'limitations': [
            '当前成分股的历史研究存在成分股选择偏差。',
            '分钟源为历史5分钟，不是1分钟竞价或盘中实时盘口。',
            '历史修订财报、成本假设、统计关联与实际成交的边界见各产物。',
            '前向收益从真实登记日起积累，尚未发生的收益保持空值。',
        ],
    }
    result['intraday_paper_feed_ready'] = feed['ready']
    result['intraday_paper_feed_checks'] = feed
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    result = audit()
    if args.output:
        from autotrade.monitoring import atomic_json
        atomic_json(args.output, result)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    raise SystemExit(0 if result['research_initialized'] else 1)
