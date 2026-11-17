"""Sector research from local prices and real, dated opportunity observations.

The public functions keep the dashboard contract. No static historical success
rates, latest financial snapshots applied to past dates, or network fallbacks.
"""
import json
import hashlib
from datetime import datetime
import sys
from pathlib import Path

import numpy as np
import pandas as pd

BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE))
from factors.research_context import (cache_dir, connect, read_bars as _read_bars, universe,
                                      scope_metadata, json_safe)
from factors.opportunities.registry import OPPORTUNITY_TYPES
from etf.etf_map import execution_config, _panels, _tradeable


_BUILD_CONTEXT = {}
_ARTIFACT_CACHE = {"key": None, "data": None}


def _bars(columns, **kwargs):
    key = "none_bars" if kwargs.get("adjust") == "none" else "bars"
    if not kwargs.get("symbols") and key in _BUILD_CONTEXT:
        return _BUILD_CONTEXT[key][columns.split(",")].copy()
    return _read_bars(columns, **kwargs)


def _cfg():
    return execution_config()['sector']


def _to_bar_code(code):
    value = str(code).strip().upper()
    if len(value) == 9 and value[6:] in ('.SH', '.SZ', '.BJ'):
        return value
    if len(value) != 6 or not value.isdigit():
        raise ValueError('证券代码无效')
    return value + ('.SH' if value.startswith(('6', '9')) else '.BJ' if value.startswith(('4', '8')) else '.SZ')


def _load_sector_detail():
    if "sector_map" in _BUILD_CONTEXT:
        return _BUILD_CONTEXT["sector_map"]
    import re
    with connect(cache_dir()/'stock_basic.db') as con:
        rows = con.execute("SELECT code,industry FROM stock_basic WHERE industry IS NOT NULL AND industry!=''").fetchall()
    allowed = universe()
    return {code: ((ind[:3], ind[3:]) if re.match(r'^[A-S]\d{2}.+', ind) else (ind, ind))
            for code, ind in rows if code in allowed}


def _names():
    if "names" in _BUILD_CONTEXT:
        return _BUILD_CONTEXT["names"]
    with connect(cache_dir()/'stock_basic.db') as con:
        return dict(con.execute('SELECT code,name FROM stock_basic'))


def _compute_list_sectors():
    from collections import Counter
    mapping = _load_sector_detail()
    counts = Counter(value[0] for value in mapping.values())
    names = {value[0]: value[1] for value in mapping.values()}
    return [{'code': code, 'name': names[code], 'n': counts[code]} for code in sorted(counts)]


def _daily():
    if "daily" in _BUILD_CONTEXT:
        return _BUILD_CONTEXT["daily"]
    frame = _bars('code,date,close')
    frame['date'] = pd.to_datetime(frame['date'])
    return frame.pivot(index='date', columns='code', values='close').sort_index()


def _sector_index(daily, sector):
    codes = [c for c, (sc, _) in _load_sector_detail().items() if sc == sector and c in daily]
    if len(codes) < int(_cfg()['min_members']):
        return None, len(codes)
    returns = daily[codes].pct_change(fill_method=None).iloc[1:]
    # No forward fill or zero return for missing sessions. This is a descriptive index, not a traded portfolio.
    if returns.isna().any().any():
        return None, len(codes)
    valid = returns.mean(axis=1)
    return pd.concat([pd.Series([1.], index=[daily.index[0]]), (1+valid).cumprod()]), len(codes)


def _compute_sector_timing(sector):
    daily = _daily()
    codes = [c for c, (sc, _) in _load_sector_detail().items() if sc == sector and c in daily]
    cfg = _cfg(); slow = int(cfg['timing_slow']); fast = int(cfg['timing_fast'])
    if len(codes) < int(cfg['min_members']) or len(daily) < slow+1:
        return {'ok': False, 'n': len(codes), 'note': '板块成分或真实日历史不足'}
    sub = daily[codes].tail(slow+1)
    if sub.isna().any().any():
        return {'ok': False, 'n': len(codes), 'note': '板块价格窗口有缺口，未ffill或记0收益'}
    returns = sub.pct_change(fill_method=None).iloc[1:].mean(axis=1)
    nav = pd.concat([pd.Series([1.], index=[sub.index[0]]), (1+returns).cumprod()])
    mom = nav.iloc[-1]/nav.iloc[0]-1
    ma_state = '多头' if nav.tail(fast).mean() > nav.tail(slow).mean() else '空头'
    state, label, color = ('in', '可进', 'green') if mom > 0 and ma_state == '多头' else (
        ('out', '回避', 'red') if mom < 0 and ma_state == '空头' else ('wait', '观望', 'yellow'))
    return {'ok': True, 'n': len(codes), 'state': state, 'label': label, 'color': color,
            'mom60': float(mom*100), 'ma_state': ma_state, 'asof': str(daily.index[-1].date()),
            'kind': 'descriptive_rule', 'auto_trade_allowed': False}


def _monthly_factors(daily):
    # Roll on actual DAILY observations before choosing monthly signal dates.
    close = daily.resample('ME').last()
    momentum = daily.pct_change(20, fill_method=None).resample('ME').last()
    near_high = (daily/daily.rolling(250, min_periods=250).max()-1).resample('ME').last()
    drawdown = (daily/daily.rolling(60, min_periods=60).max()-1).resample('ME').last()
    return close, momentum, near_high, drawdown


def _fwd_winrate(daily, hs300, events, sector_map, horizon=None, *, opens=None,
                 benchmark_opens=None, bars=None, fees=None, return_diagnostics=False, panels=None):
    """Signal close -> next session OPEN -> close on the Hth held session.

    Actual dated benchmark prices use the identical entry/exit dates. Unfinished
    horizons never contribute. Exit/benchmark data gaps remain explicit unresolved
    observations; callers must not publish a validated score from censored winners.
    """
    horizon = int(horizon or _cfg()['horizon_sessions'])
    if horizon < 2 or opens is None or benchmark_opens is None or bars is None:
        raise ValueError('真实next-open检验需要open/OHLC/基准open和至少跨日持有期')
    fees = fees or execution_config()['stock_fees']
    commission, tax, slip = (float(fees[k]) for k in ('commission_rate', 'sell_tax_rate', 'slippage_rate'))
    dates = pd.DatetimeIndex(daily.index); p = panels if panels is not None else _panels(bars)
    buckets, diagnostics = {}, {}
    for code, day in set((str(c), pd.Timestamp(d)) for c, d in events):
        sector = sector_map.get(code[:6], '其他')
        diag = diagnostics.setdefault(sector, {'signals': 0, 'completed': 0, 'unmatured': 0,
                                              'entry_untradeable': 0, 'exit_untradeable': 0, 'benchmark_missing': 0})
        diag['signals'] += 1
        if code not in daily or day not in dates:
            diag['entry_untradeable'] += 1; continue
        i = dates.get_loc(day); entry_i = i+1; exit_i = entry_i+horizon-1
        if exit_i >= len(dates):
            diag['unmatured'] += 1; continue
        entry, exit_day = dates[entry_i], dates[exit_i]
        if not _tradeable(p, entry, code):
            diag['entry_untradeable'] += 1; continue
        if not _tradeable(p, exit_day, code):
            diag['exit_untradeable'] += 1; continue
        try:
            buy = float(opens.at[entry, code]); sell = float(daily.at[exit_day, code])
            bench_buy = float(benchmark_opens.loc[entry]); bench_sell = float(hs300.loc[exit_day])
        except (KeyError, TypeError, ValueError):
            diag['benchmark_missing'] += 1; continue
        if not all(np.isfinite(v) and v > 0 for v in (buy, sell, bench_buy, bench_sell)):
            diag['benchmark_missing'] += 1; continue
        net = sell*(1-slip)*(1-commission-tax)/(buy*(1+slip)*(1+commission))-1
        benchmark = bench_sell/bench_buy-1
        buckets.setdefault(sector, []).append(net-benchmark)
        diag['completed'] += 1
    return (buckets, diagnostics) if return_diagnostics else buckets


def _compute_sector_strong_factors(sector):
    if "strong_by_sector" in _BUILD_CONTEXT:
        return _BUILD_CONTEXT["strong_by_sector"].get(sector, [])
    cfg = _cfg(); source = BASE/'logs/opportunity_winrates.json'
    base = [{'otype': ot, 'name': spec['name'], 'n': 0, 'winrate': None, 'excess': None, 'ok': False,
             'source': str(source.relative_to(BASE)), 'sample_horizon': f"{cfg['horizon_sessions']} sessions",
             'execution': 'signal_close -> next_open -> Hth_close; stock fees/slippage; gross HS300 index proxy',
             'note': '缺真实带日期的校准观察记录'} for ot, spec in OPPORTUNITY_TYPES.items()]
    if not source.is_file():
        return base
    data = json.loads(source.read_text(encoding='utf-8'))
    scope = (data.get('meta') or {}).get('research_scope') or {}
    if data.get('kind') != 'historical_calibration' or scope.get('universe_fingerprint') != scope_metadata()['universe_fingerprint']:
        return base
    observations = data.get('observations') or []
    frame = _bars('code,date,open,high,low,close,volume')
    frame['date'] = pd.to_datetime(frame['date'])
    daily = frame.pivot(index='date', columns='code', values='close').sort_index()
    opens = frame.pivot(index='date', columns='code', values='open').sort_index()
    benchmark = _bars('code,date,open,close', adjust='none', symbols=['SH.000300', '000300.SH'])
    benchmark['date'] = pd.to_datetime(benchmark['date'])
    benchmark = benchmark.sort_values('date').drop_duplicates('date').set_index('date')
    mapping = {c[:6]: value[0] for c, value in _load_sector_detail().items()}
    for result in base:
        # One-month source records provide actual past signal dates, not a fabricated new trigger series.
        events = [(row['code'], row['signal_date']) for row in observations
                  if row.get('otype') == result['otype'] and row.get('horizon_months') == 1 and
                  mapping.get(row['code'][:6]) == sector]
        buckets, diag = _fwd_winrate(daily, benchmark['close'], events, mapping, opens=opens,
                                     benchmark_opens=benchmark['open'], bars=frame, return_diagnostics=True)
        values = np.asarray(buckets.get(sector, []), dtype=float)
        details = diag.get(sector, {})
        result.update({'n': len(values), 'diagnostics': details,
                       'data_limits': ['当前行业分类与成分股回看历史存在选择偏差',
                                       '源策略只过滤已知ST；财务修订与交易所队列未完整重建',
                                       '仅对源校准文件已存完整1月观察记录重测20交易日；不是完整事件总体',
                                       '比例费用未模拟最低佣金/整手，历史印花税使用配置常数']})
        if details.get('exit_untradeable') or details.get('benchmark_missing'):
            result['note'] = '存在无法确认退出/基准的持有样本，未只展示可计算赢家'
        elif len(values) < int(cfg['min_samples']):
            result['note'] = f"完整可执行样本不足({len(values)}/{cfg['min_samples']})"
        else:
            result.update({'winrate': float((values > 0).mean()*100), 'excess': float(values.mean()*100),
                           'ok': True, 'evidence_ready': True, 'note': '真实有日期信号的板块内固定持有期净超额描述统计；非样本外预测'})
    return sorted(base, key=lambda x: -(x['winrate'] if x['winrate'] is not None else -1))


def _compute_stock_search(q, limit=20):
    q = str(q or '').strip()
    if not q:
        return []
    with connect(cache_dir()/'stock_basic.db') as con:
        rows = con.execute('SELECT code,name FROM stock_basic WHERE code LIKE ? OR name LIKE ? LIMIT ?',
                           ('%'+q+'%', '%'+q+'%', int(limit))).fetchall()
    allowed = universe()
    return [{'code': c, 'name': name} for c, name in rows if c in allowed]


def _current_panel():
    if "current_panel" in _BUILD_CONTEXT:
        return _BUILD_CONTEXT["current_panel"]
    from factors.opportunities import scan
    daily = _daily(); end = str(daily.index[-1].date())
    px, vx = scan.load_panel(end=end, days=int(_cfg()['min_history']))
    if px is None or px.empty:
        raise ValueError('当前研究范围缺足量真实日线')
    factors = scan.compute_factors(px, vx, scan.load_fundamentals(end), scan.load_basic(), scan.load_st_codes())
    return factors, end, scan.triggers()


def _compute_stock_factors(code):
    code = _to_bar_code(code)
    if code not in universe():
        return {'ok': False, 'error': '证券不在当前研究范围'}
    panel, asof, triggers = _current_panel()
    if code not in panel.index:
        return {'ok': False, 'error': '历史或已公告基本面不足'}
    row = panel.loc[code]; sc, sn = _load_sector_detail().get(code, ('', ''))
    evidence = {r['otype']: r for r in _compute_sector_strong_factors(sc)} if sc else {}
    hits = []
    for ot, spec in OPPORTUNITY_TYPES.items():
        proof = evidence.get(ot, {})
        met = bool(triggers[ot](row)) if ot in triggers else None
        hits.append({'otype': ot, 'name': spec['name'], 'met': met, 'winrate': proof.get('winrate'),
                     'excess': proof.get('excess'), 'n': proof.get('n', 0), 'evidence_ready': bool(proof.get('ok')),
                     'note': proof.get('note') or '外部共识触发未接入，未使用低波冒充',
                     'neg': bool(proof.get('excess') is not None and proof['excess'] < 0)})
    hits.sort(key=lambda x: (x['met'] is not True, -(x['winrate'] if x['winrate'] is not None else -1)))
    return json_safe({'ok': True, 'code': code, 'name': _names().get(code, ''), 'sector': f'{sc} {sn}',
                      'price': float(row['close']), 'factors': {'dd60': row.get('drawdown_60d')*100,
                      'mom20': row.get('mom20')*100, 'nh250': row.get('near_high_250')*100,
                      'vol20': None, 'pb': row.get('pb'), 'roe': row.get('roe')*100,
                      'sq_nyoy': row.get('sq_nyoy')*100}, 'hits': hits, 'asof': asof, 'auto_trade_allowed': False})


def _compute_sector_pitch(sector, topn=20, strong_factors=None):
    strong = strong_factors if strong_factors is not None else _compute_sector_strong_factors(sector)
    valid = [r for r in strong if r.get('ok') and r.get('evidence_ready') and r.get('n', 0) >= int(_cfg()['min_samples'])
             and r.get('winrate') is not None][:int(_cfg()['strong_count'])]
    codes = [c for c, (s, _) in _load_sector_detail().items() if s == sector]
    if not valid:
        return {'ok': True, 'status': 'awaiting_evidence', 'n_total': len(codes), 'n_pitch': 0,
                'strong_factors': [], 'pitch': [], 'auto_trade_allowed': False,
                'note': '没有足量真实板块检验；未按旧静态胜率生成买入评分'}
    panel, asof, triggers = _current_panel(); names = _names(); picks = []
    weights = {r['otype']: r for r in valid}
    for code in codes:
        if code not in panel.index:
            continue
        row = panel.loc[code]
        met = [ot for ot in weights if ot in triggers and bool(triggers[ot](row))]
        if met:
            picks.append({'code': code, 'name': names.get(code, ''), 'price': float(row['close']),
                          'hits': [OPPORTUNITY_TYPES[ot]['name'] for ot in met], 'hits_otype': met,
                          'score': float(sum(weights[ot]['winrate'] for ot in met)), 'n_hits': len(met)})
    picks.sort(key=lambda r: (-r['n_hits'], -r['score']))
    return json_safe({'ok': True, 'status': 'limited', 'n_total': len(codes), 'n_pitch': len(picks), 'n_excluded': 0,
                      'strong_factors': valid, 'pitch': picks[:topn], 'asof': asof, 'auto_trade_allowed': False,
                      'note': '当前规则命中×真实板块历史统计；启发式排名分不等于成功概率'})


def _compute_sector_retail(sector, topn=20):
    codes = [c for c, (s, _) in _load_sector_detail().items() if s == sector]
    if len(codes) < int(_cfg()['min_members']):
        return {'ok': False, 'note': '板块成分不足', 'n': len(codes)}
    window = int(_cfg()['turnover_window'])
    if "turn_means" in _BUILD_CONTEXT:
        means = _BUILD_CONTEXT["turn_means"]
    else:
        frame = _bars('code,date,turn'); frame['date'] = pd.to_datetime(frame['date'])
        turn = frame[frame['code'].isin(codes)].pivot(index='date', columns='code', values='turn').sort_index()
        means = turn.tail(window).mean().where(turn.tail(window).count() == window)
    panel, asof, _ = _current_panel(); names = _names(); low = []
    for code in codes:
        if code in panel.index and panel.at[code, 'non_st'] == 1 and panel.at[code, 'roe'] > 0 and pd.notna(means.get(code)):
            low.append({'code': code, 'name': names.get(code, ''), 'turnover': float(means[code]),
                        'roe': float(panel.at[code, 'roe']*100)})
    low.sort(key=lambda r: r['turnover'])
    return {'ok': True, 'status': 'limited', 'n_total': len(codes), 'n_inflow': None, 'n_outflow': None,
            'n_filtered': len(codes)-len(low), 'inflow': [], 'outflow': [], 'lowturn': low[:topn],
            'holder_data_available': False, 'asof': asof,
            'note': '换手取官方turn真实列；股东户数尚未提供可验证公告序列，未填0或推断散户流入'}


def _compute_sector_crowd(sector):
    mapping = _load_sector_detail(); members = [c for c, (s, _) in mapping.items() if s == sector]
    if len(members) < int(_cfg()['min_members']):
        return {'ok': False, 'note': '板块成分不足'}
    if "amount_panel" in _BUILD_CONTEXT:
        grouped = _BUILD_CONTEXT["amount_panel"]
    else:
        from data.cache import normalize_units
        frame = normalize_units(_bars('code,date,amount,source', adjust='none'))
        grouped = frame.pivot(index='date', columns='code', values='amount').sort_index().tail(int(_cfg()['crowd_window']))
    if grouped.empty or grouped.iloc[-1].isna().any() or grouped.iloc[-1].sum() <= 0:
        return {'ok': False, 'note': '当前研究池成交额缺失，不推断市场热度'}
    share = grouped[members].sum(axis=1, min_count=len(members))/grouped.sum(axis=1, min_count=len(grouped.columns))
    current = float(share.iloc[-1]); cfg = _cfg()
    return {'ok': True, 'share_pct': current*100, 'avg_share_pct': float(share.mean()*100),
            'heat': '热门' if current > cfg['hot_share'] else ('中性' if current > cfg['neutral_share'] else '冷门'),
            'n': len(members), 'denominator': '当前研究股票池',
            'note': '真实成交额占比的描述性特征；未引用旧收益数字，也不据此宣称因子已验证'}


def _method_fingerprint():
    cfg = execution_config()
    return hashlib.sha256(json.dumps({'sector': cfg['sector'], 'fees': cfg['stock_fees']},
                                     sort_keys=True).encode()).hexdigest()


def _factor_template(source, cfg):
    return [{'otype': ot, 'name': spec['name'], 'n': 0, 'winrate': None, 'excess': None,
             'ok': False, 'evidence_ready': False, 'source': source,
             'sample_horizon': f"{cfg['horizon_sessions']} sessions",
             'execution': 'signal_close -> next_open -> Hth_close; net stock vs gross HS300 index proxy',
             'note': '缺真实带日期的校准观察记录'} for ot, spec in OPPORTUNITY_TYPES.items()]


def _precompute_strong(calibration, frame, mapping, sectors, benchmark):
    cfg = _cfg(); price_panels = _panels(frame)
    daily, opens = price_panels['close'], price_panels['open']
    by_sector = {code: _factor_template('logs/opportunity_winrates.json', cfg) for code in sectors}
    observations = calibration.get('observations') or []
    for ot in OPPORTUNITY_TYPES:
        events = [(row['code'], row['signal_date']) for row in observations
                  if row.get('otype') == ot and row.get('horizon_months') == 1 and row['code'][:6] in mapping]
        buckets, diagnostics = _fwd_winrate(daily, benchmark['close'], events, mapping,
                                           opens=opens, benchmark_opens=benchmark['open'], bars=frame,
                                           panels=price_panels, return_diagnostics=True)
        for sector in sectors:
            result = next(item for item in by_sector[sector] if item['otype'] == ot)
            values = np.asarray(buckets.get(sector, []), dtype=float)
            diag = diagnostics.get(sector, {})
            result.update(n=len(values), diagnostics=diag,
                          data_limits=['当前行业与成分股回看历史存在选择偏差', '仅重测源已存完整1月样本子集',
                                       '历史ST/财务修订/实际队列未完整验证；比例费用与固定税率有近似'])
            if diag.get('exit_untradeable') or diag.get('benchmark_missing'):
                result['note'] = '退出或基准存在未解决样本，未只展示可计算结果'
            elif len(values) < int(cfg['min_samples']):
                result['note'] = f"完整可执行样本不足({len(values)}/{cfg['min_samples']})"
            else:
                result.update(ok=True, evidence_ready=True, winrate=float((values > 0).mean()*100),
                              excess=float(values.mean()*100), note='真实历史净超额描述统计；非样本外预测')
    return {code: sorted(values, key=lambda row: -(row['winrate'] if row['winrate'] is not None else -1))
            for code, values in by_sector.items()}


def build():
    """Offline job. Load price matrices once and evaluate all industries together."""
    from factors.research_context import config as research_config
    from data.cache import normalize_units
    source = BASE/'logs/opportunity_winrates.json'
    calibration_bytes = source.read_bytes()
    calibration = json.loads(calibration_bytes)
    scope = scope_metadata()
    if calibration.get('kind') != 'historical_calibration' or (calibration.get('meta', {}).get('research_scope') or {}).get('universe_fingerprint') != scope['universe_fingerprint']:
        raise ValueError('缺本轮当前股票池的真实校准证据')
    if not isinstance(calibration.get('observations'), list) or not calibration['observations']:
        raise ValueError('缺带真实信号日期的校准观察记录')
    try:
        frame = _read_bars('code,date,open,high,low,close,volume,turn,is_st,source')
        if frame.empty:
            raise ValueError('研究范围行情为空')
        frame['date'] = pd.to_datetime(frame['date'])
        daily = frame.pivot(index='date', columns='code', values='close').sort_index()
        metadata = _load_sector_detail(); names = _names()
        if not metadata:
            raise ValueError('缺当前股票池的实际行业元数据')
        _BUILD_CONTEXT.update(bars=frame, daily=daily, sector_map=metadata, names=names)
        sectors = _compute_list_sectors()
        _BUILD_CONTEXT['current_panel'] = _current_panel()
        benchmark = _read_bars('code,date,open,close', adjust='none', symbols=[research_config()['index_code']])
        benchmark['date'] = pd.to_datetime(benchmark['date'])
        benchmark = benchmark.sort_values('date').drop_duplicates('date').set_index('date')
        if benchmark.empty:
            raise ValueError('缺配置指定HS300指数的真实open/close')
        mapping = {code[:6]: value[0] for code, value in metadata.items()}
        _BUILD_CONTEXT['strong_by_sector'] = _precompute_strong(calibration, frame, mapping,
                                                               [row['code'] for row in sectors], benchmark)
        turn = frame.pivot(index='date', columns='code', values='turn').sort_index().tail(int(_cfg()['turnover_window']))
        _BUILD_CONTEXT['turn_means'] = turn.mean().where(turn.count() == int(_cfg()['turnover_window']))
        raw = _read_bars('code,date,amount,source', adjust='none')
        raw['date'] = pd.to_datetime(raw['date'])
        amounts = normalize_units(raw).pivot(index='date', columns='code', values='amount').sort_index()
        _BUILD_CONTEXT['amount_panel'] = amounts.tail(int(_cfg()['crowd_window']))
        analyses = {}
        for item in sectors:
            code = item['code']; evidence = _compute_sector_strong_factors(code)
            analyses[code] = {'timing': _compute_sector_timing(code), 'factors': evidence,
                              'pitch': _compute_sector_pitch(code, strong_factors=evidence),
                              'retail': _compute_sector_retail(code), 'crowd': _compute_sector_crowd(code)}
        stocks = {code: _compute_stock_factors(code) for code in sorted(universe())}
        for code, diagnostic in stocks.items():
            diagnostic.setdefault('code', code)
            diagnostic.setdefault('name', names.get(code, code))
        result = json_safe({'schema_version': 1, 'kind': 'sector_research_evidence', 'status': 'limited',
                           'date': str(daily.index[-1].date()), 'generated_at': datetime.now().isoformat(timespec='seconds'),
                           'research_scope': scope, 'method_fingerprint': _method_fingerprint(),
                           'calibration': {'path': 'logs/opportunity_winrates.json',
                                           'sha256': hashlib.sha256(calibration_bytes).hexdigest()},
                           'source_values': sorted(set(frame['source'].dropna().astype(str))),
                           'sectors': sectors, 'analyses': analyses, 'stocks': stocks,
                           'auto_trade_allowed': False,
                           'note': '实际校准及日线的离线研究；公开查询只读本产物，不同步执行回测'})
        target = BASE/'output/sector_evidence.json'; target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_suffix('.json.tmp')
        temporary.write_text(json.dumps(result, ensure_ascii=False, allow_nan=False), encoding='utf-8')
        temporary.replace(target)
        return target
    finally:
        _BUILD_CONTEXT.clear()


def _artifact():
    target = BASE/'output/sector_evidence.json'; source = BASE/'logs/opportunity_winrates.json'
    if not target.is_file() or not source.is_file():
        raise ValueError('板块离线研究尚未生成；请运行data.sector_research --build')
    current_scope = scope_metadata()['universe_fingerprint']; methods = _method_fingerprint()
    key = (str(target), target.stat().st_mtime_ns, target.stat().st_size,
           source.stat().st_mtime_ns, source.stat().st_size, current_scope, methods)
    if _ARTIFACT_CACHE['key'] == key:
        return _ARTIFACT_CACHE['data']
    raw = json.loads(target.read_text(encoding='utf-8'))
    if (raw.get('kind') != 'sector_research_evidence' or raw.get('schema_version') != 1 or
        (raw.get('research_scope') or {}).get('universe_fingerprint') != current_scope or
        raw.get('method_fingerprint') != methods or
        (raw.get('calibration') or {}).get('sha256') != hashlib.sha256(source.read_bytes()).hexdigest()):
        raise ValueError('板块研究股票池、方法或校准源已变化；需要重新离线计算')
    if not isinstance(raw.get('analyses'), dict) or not isinstance(raw.get('stocks'), dict):
        raise ValueError('板块离线研究结构不完整')
    _ARTIFACT_CACHE.update(key=key, data=raw)
    return raw


def list_sectors():
    return _artifact()['sectors']


def _analysis(sector, field):
    result = _artifact()['analyses'].get(str(sector))
    if result is None:
        return [] if field == 'factors' else {'ok': False, 'error': '行业不在已计算研究范围'}
    return result[field]


def sector_timing(sector):
    return _analysis(sector, 'timing')


def sector_strong_factors(sector):
    return _analysis(sector, 'factors')


def sector_pitch(sector, topn=20, strong_factors=None):
    result = dict(_analysis(sector, 'pitch'))
    result['pitch'] = result.get('pitch', [])[:max(0, int(topn))]
    return result


def sector_retail(sector, topn=20):
    result = dict(_analysis(sector, 'retail'))
    for key in ('lowturn', 'inflow', 'outflow'):
        result[key] = result.get(key, [])[:max(0, int(topn))]
    return result


def sector_crowd(sector):
    return _analysis(sector, 'crowd')


def stock_search(q, limit=20):
    text = str(q or '').strip().lower()
    if not text:
        return []
    rows = [{'code': code, 'name': row.get('name', '')} for code, row in _artifact()['stocks'].items()
            if text in code.lower() or text in str(row.get('name') or '').lower()]
    return rows[:max(0, int(limit))]


def stock_factors(code):
    return _artifact()['stocks'].get(_to_bar_code(code), {'ok': False, 'error': '证券不在已计算研究范围'})


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build', action='store_true', help='计算真实板块证据并原子保存，API不会调用本计算')
    args = parser.parse_args()
    if args.build:
        print('板块离线研究已生成:', build())
    else:
        print(json.dumps({'sectors': list_sectors()}, ensure_ascii=False))
