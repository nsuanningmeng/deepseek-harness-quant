"""Local ETF exposure research with next-open execution and a chronological holdout.

No network calls and no broker/order entry points. Missing held prices stop a
simulation instead of becoming zero returns or redistributed portfolio weights.
"""
import datetime
import json
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE))
from factors.research_context import (config as research_config, connect, cache_dir,
                                      read_bars, universe, scope_metadata, json_safe, path)

OUT_PATH = BASE / 'output/etf_map.json'


def execution_config():
    target = Path(os.environ.get('LWQUANT_EXECUTION_CONFIG', BASE / 'config/research_execution.yaml'))
    cfg = yaml.safe_load(target.read_text(encoding='utf-8'))
    if not isinstance(cfg, dict):
        raise ValueError('研究执行配置无效')
    return cfg


def _load_pool():
    cfg = yaml.safe_load(path(research_config()['etf_pool_file']).read_text(encoding='utf-8'))
    items = cfg.get('pool') or []
    pool = [(str(x['code']), str(x.get('name') or x['code']), str(x.get('category') or '其他')) for x in items]
    if not pool or len({x[0] for x in pool}) != len(pool):
        raise ValueError('ETF配置池为空或重复；未回退到内置股票名单')
    if any(len(c) != 6 or not c.isdigit() or not c.startswith(('1', '5')) for c, _, _ in pool):
        raise ValueError('ETF配置代码无效')
    return pool


def _panels(frame):
    frame = frame.copy()
    frame['date'] = pd.to_datetime(frame['date'])
    if frame.duplicated(['date', 'code']).any():
        raise ValueError('价格输入存在重复日期/证券')
    return {col: frame.pivot(index='date', columns='code', values=col).sort_index()
            for col in ('open', 'high', 'low', 'close', 'volume')}


def _tradeable(panels, date, code):
    try:
        values = [float(panels[k].at[date, code]) for k in ('open', 'high', 'low', 'close', 'volume')]
    except (KeyError, TypeError, ValueError):
        return False
    op, high, low, close, volume = values
    return (all(np.isfinite(x) and x > 0 for x in values) and
            low <= min(op, close) <= max(op, close) <= high and high > low)


def simulate_targets(frame, schedules, fees, *, end_date=None):
    """Shares/cash accounting with delayed exits, never a fabricated fill.

    Blocked sell orders retain their actual holdings until the first observable,
    non-one-price session open. Reinvestment waits for the old basket to exit;
    a later close cannot retrospectively execute an earlier order. Missing marks
    stay NaN and invalidate path-risk statistics rather than becoming flat days.
    """
    p = _panels(frame)
    dates = p['close'].index
    if end_date is not None:
        dates = dates[dates <= pd.Timestamp(end_date)]
    if not schedules:
        raise ValueError('没有已成熟、可执行的持有期')
    commission, tax, slip = (float(fees[k]) for k in ('commission_rate', 'sell_tax_rate', 'slippage_rate'))
    if any(not 0 <= x < 1 for x in (commission, tax, slip)) or commission+tax >= 1:
        raise ValueError('研究费率无效')
    entries = {pd.Timestamp(s['entry']): s for s in schedules}
    exits = {pd.Timestamp(s['exit']): s for s in schedules}
    if len(entries) != len(schedules) or len(exits) != len(schedules):
        raise ValueError('重复的调仓计划日期')
    for schedule in schedules:
        if pd.Timestamp(schedule['entry']) >= pd.Timestamp(schedule['exit']):
            raise ValueError('买入与卖出必须跨交易日')
        if (not schedule['weights'] or any(not np.isfinite(w) or w < 0 for w in schedule['weights'].values())
                or sum(schedule['weights'].values()) > 1+1e-10):
            raise ValueError('目标权重无效')
    cash, previous_nav, holdings = 1., 1., {}
    records, rejected, delayed_exits, delayed_entries, expired, gaps = [], [], [], [], [], []
    pending_sells = {}; queued_entry = None; active = False
    final_exit = max(exits)
    for day in dates:
        if day in entries:
            active = True
        if not active:
            continue
        # Scheduled exits create persistent intentions; old shares cannot disappear.
        if day in exits and not exits[day].get('exit_close'):
            for code in holdings:
                pending_sells.setdefault(code, day)
        for code in list(pending_sells):
            if not _tradeable(p, day, code):
                continue
            requested = pending_sells.pop(code)
            shares = holdings.pop(code)
            price = float(p['open'].at[day, code])
            cash += shares*price*(1-slip)*(1-commission-tax)
            if day > requested:
                delayed_exits.append({'code': code, 'requested': str(requested.date()),
                                      'executed': str(day.date()), 'actual_open': price})
        if day in entries:
            if queued_entry is not None:
                expired.append({'requested': str(pd.Timestamp(queued_entry['entry']).date()),
                                'reason': 'superseded_after_unexecuted_rebalance'})
            queued_entry = entries[day]
        if queued_entry is not None and day >= pd.Timestamp(queued_entry['exit']):
            expired.append({'requested': str(pd.Timestamp(queued_entry['entry']).date()),
                            'reason': 'entry_window_expired_without_execution'})
            queued_entry = None
        # Do not reallocate the blocked old holdings' value to newly selected names.
        if queued_entry is not None and not holdings and not pending_sells:
            budget = cash
            requested = pd.Timestamp(queued_entry['entry'])
            if day > requested:
                delayed_entries.append({'requested': str(requested.date()), 'executed': str(day.date())})
            for code, weight in queued_entry['weights'].items():
                if not _tradeable(p, day, code):
                    rejected.append({'date': str(day.date()), 'code': code, 'reason': 'missing_or_one_price_entry_bar'})
                    continue
                allocation = budget*float(weight)
                price = float(p['open'].at[day, code])*(1+slip)
                holdings[code] = allocation/(price*(1+commission))
                cash -= allocation
            queued_entry = None
        if day in exits and exits[day].get('exit_close'):
            for code in list(holdings):
                if _tradeable(p, day, code):
                    cash += holdings.pop(code)*float(p['close'].at[day, code])*(1-slip)*(1-commission-tax)
                else:
                    pending_sells.setdefault(code, day)  # Earliest possible execution is a later open.
        equity = cash; missing = []
        for code, shares in holdings.items():
            value = p['close'].at[day, code] if code in p['close'] else np.nan
            volume = p['volume'].at[day, code] if code in p['volume'] else np.nan
            if not np.isfinite(value) or value <= 0 or not np.isfinite(volume) or volume <= 0:
                missing.append(code)
            else:
                equity += shares*float(value)
        if missing:
            gaps.append({'date': str(day.date()), 'symbols': missing})
            equity = np.nan
        ret = equity/previous_nav-1 if np.isfinite(equity) and np.isfinite(previous_nav) else np.nan
        records.append((day, ret, equity, cash))
        previous_nav = equity
        if day >= final_exit and not holdings and queued_entry is None:
            break
    if not records:
        raise ValueError('没有可验证的模拟日期')
    result = pd.DataFrame(records, columns=['date', 'ret', 'nav', 'cash']).set_index('date')
    result.attrs.update({'rejected_entries': rejected, 'fees': fees, 'schedules': len(schedules),
                         'execution_model': 'next_open_with_persistent_blocked_exits',
                         'delayed_exits': delayed_exits, 'delayed_entries': delayed_entries,
                         'unexecuted_entry_windows': expired, 'valuation_gaps': gaps,
                         'closed': not holdings and queued_entry is None,
                         'unresolved_holdings': sorted(holdings),
                         'unit_model': 'fractional research units, proportional fees; no minimum commission or lot rounding'})
    return result


def load_turn_low_nav():
    cfg = execution_config()
    method = cfg['etf']
    frame = read_bars('code,date,open,high,low,close,volume,turn,is_st')
    frame = frame[frame['date'] >= method['start']].copy()
    frame['date'] = pd.to_datetime(frame['date'])
    if frame.empty:
        raise ValueError('当前研究股票池缺真实历史日线')
    close = frame.pivot(index='date', columns='code', values='close').sort_index()
    turn = frame.pivot(index='date', columns='code', values='turn').reindex(close.index)
    st = frame.pivot(index='date', columns='code', values='is_st').reindex(close.index)
    dates = close.index
    window, holding, top_n = (int(method[k]) for k in ('turnover_window', 'rebalance_days', 'top_n'))
    if window < 1 or holding < 1 or top_n < 1:
        raise ValueError('换手窗口/调仓间隔/选股数必须为正')
    signal = turn.rolling(window, min_periods=window).mean()
    schedules, pending = [], []
    for i in range(window-1, len(dates), holding):
        entry, exit_i = i+1, i+1+holding
        if exit_i >= len(dates):
            pending.append(str(dates[i].date()))
            continue
        row = signal.iloc[i].where((close.iloc[i] >= float(method['min_price'])) & (st.iloc[i] != 1)).dropna()
        codes = row.nsmallest(top_n).index.tolist()
        if not codes:
            continue
        schedules.append({'signal_date': dates[i], 'entry': dates[entry], 'exit': dates[exit_i],
                          'weights': {c: 1/top_n for c in codes}})
    result = simulate_targets(frame, schedules, cfg['stock_fees'], end_date=dates[-1])
    result.attrs.update({'pending_signals': pending, 'historical_st_known_ratio': float(frame['is_st'].notna().mean()),
                         'research_scope': scope_metadata(), 'method': method, 'source_asof': str(dates[-1].date()),
                         'simulation_asof': str(result.index[-1].date())})
    return result


def fetch_etf(code):
    symbol = code + ('.SH' if code.startswith('5') else '.SZ')
    frame = read_bars('code,date,open,high,low,close,amount,volume,source', symbols=[symbol])
    if frame.empty:
        raise ValueError('ETF复权历史尚未初始化: ' + code)
    from data.cache import normalize_units
    return normalize_units(frame)


def etf_ret_series(code):
    df = fetch_etf(code).copy()
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').set_index('date')
    if df.index.duplicated().any():
        raise ValueError('ETF日期重复')
    df['ret'] = df['close'].pct_change(fill_method=None)
    return df


def corr_stats(strat_ret, etf_ret):
    method = execution_config()['etf']
    m = pd.concat([strat_ret, etf_ret], axis=1, keys=['s', 'e']).dropna()
    if len(m) < int(method['min_correlation_rows']) or m['s'].std() <= 0 or m['e'].std() <= 0:
        return None
    corr = float(m['s'].corr(m['e']))
    roll = m['s'].rolling(int(method['rolling_window'])).corr(m['e']).dropna()
    yearly = {str(y): round(float(g['s'].corr(g['e'])), 3) for y, g in m.groupby(m.index.year)
              if len(g) >= int(method['rolling_window']) and g['s'].std() > 0 and g['e'].std() > 0}
    return {'corr': corr, 'corr_roll_mean': float(roll.mean()) if len(roll) else None,
            'corr_roll_last': float(roll.iloc[-1]) if len(roll) else None,
            'corr_roll_vol': float(roll.std()) if len(roll) > 1 else None,
            'beta': float(m['s'].cov(m['e']) / m['e'].var()), 'yearly': yearly, 'n': len(m)}


def _metrics(curve):
    returns, nav = curve['ret'], curve['nav']
    complete_marks = bool(nav.notna().all() and returns.notna().all())
    closed = bool(curve.attrs.get('closed', True))
    return {'annual': float(nav.iloc[-1]**(252/len(nav))-1) if closed and pd.notna(nav.iloc[-1]) else None,
            'max_dd': float((nav/nav.cummax().clip(lower=1.)-1).min()) if complete_marks else None,
            'sharpe': float(returns.mean()/returns.std()*np.sqrt(252)) if complete_marks and returns.std() > 0 else None,
            'days': len(curve), 'complete_marks': complete_marks, 'closed': closed,
            'valuation_gap_days': int(nav.isna().sum())}


def main():
    cfg = execution_config()
    method = cfg['etf']
    strat = load_turn_low_nav()
    minimum = int(method['min_oos_days'])
    cutoff_i = int(len(strat) * float(method['train_fraction']))
    if cutoff_i < int(method['min_correlation_rows']) or len(strat)-cutoff_i < minimum:
        raise ValueError('ETF映射缺足量训练与未参与筛选的后续留出期')
    cutoff = strat.index[cutoff_i-1]
    train = strat['ret'].loc[:cutoff]
    series, failures, rows = {}, [], []
    for code, name, category in _load_pool():
        try:
            df = etf_ret_series(code)
            if len(df.loc[:cutoff]) < int(method['min_etf_rows']):
                raise ValueError('训练期历史不足')
            stats = corr_stats(train, df['ret'].loc[:cutoff])
            if stats is None or stats['corr_roll_mean'] is None or stats['corr_roll_vol'] is None:
                raise ValueError('训练期相关性不可估计')
            amount = float(df.loc[:cutoff, 'amount'].tail(int(method['liquidity_window'])).mean())/1e8
            if not np.isfinite(amount) or amount <= 0:
                raise ValueError('训练期成交额缺失')
            stats.update({'code': code, 'name': name, 'category': category, 'avg_amount_yi': amount})
            rows.append(stats); series[code] = df
        except (ValueError, KeyError) as exc:
            failures.append({'code': code, 'reason': str(exc)})
    if not rows:
        raise ValueError('所有ETF训练输入均不足，未发布空成功映射')
    corr_df = pd.DataFrame(rows).sort_values('corr', ascending=False)
    top_codes = corr_df.head(int(method['matrix_count']))['code'].tolist()
    matrix = pd.DataFrame({c: series[c]['ret'].loc[:cutoff] for c in top_codes}).corr()
    cards = []
    for row in rows:
        match = max(0., row['corr'])*100
        stable = max(0., row['corr_roll_mean'])*100*(1-min(1., row['corr_roll_vol']/float(method['stability_scale'])))
        liq = min(100., float(method['liquidity_base'])+float(method['liquidity_slope'])*np.log1p(row['avg_amount_yi'])/np.log1p(float(method['liquidity_scale_yi'])))
        score = float(method['match_weight'])*match+float(method['stability_weight'])*stable+float(method['liquidity_weight'])*liq
        row.update({'score': score, 'tier': 'A' if score >= method['tier_a'] else ('B' if score >= method['tier_b'] else 'C')})
        if row['corr'] > float(method['min_correlation']):
            cards.append(row)
    cards.sort(key=lambda r: -r['score'])
    selected = []
    for card in cards:
        if len(selected) >= int(method['max_selected']):
            break
        if any(h['code'] in matrix and card['code'] in matrix.index and
               matrix.at[card['code'], h['code']] > method['redundancy_max'] for h in selected):
            continue
        selected.append(card)
    allocations = {}
    if selected:
        equal = {c['code']: 1/len(selected) for c in selected}
        corr_w = {c['code']: c['corr']/sum(x['corr'] for x in selected) for c in selected}
        score_w = {c['code']: c['score']/sum(x['score'] for x in selected) for c in selected}
        oos_dates = strat.index[strat.index > cutoff]
        frame = pd.concat([series[c['code']].reset_index() for c in selected], ignore_index=True)
        for name, weights in [('equal', equal), ('corr_w', corr_w), ('corr_liq_w', score_w)]:
            symbols = {code+('.SH' if code.startswith('5') else '.SZ'): weight for code, weight in weights.items()}
            curve = simulate_targets(frame, [{'entry': oos_dates[0], 'exit': oos_dates[-1], 'weights': symbols, 'exit_close': True}],
                                     cfg['etf_fees'], end_date=oos_dates[-1])
            curve = curve.loc[oos_dates[0]:oos_dates[-1]]
            if len(curve) < minimum:
                raise ValueError('ETF组合留出期交易日不足')
            allocations[name] = {'weights': weights, **_metrics(curve), 'kind': 'chronological_holdout',
                                 'start': str(curve.index[0].date()), 'end': str(curve.index[-1].date()),
                                 'corr_to_strategy': float(curve['ret'].corr(strat['ret'])),
                                 'rejected_entries': curve.attrs.get('rejected_entries', []),
                                 'execution': curve.attrs}
    out = {'generated_at': datetime.datetime.now().isoformat(timespec='seconds'), 'status': 'limited',
           'kind': 'etf_exposure_research', 'research_scope': scope_metadata(), 'data_source': 'local_tushare_qfq',
           'asof': strat.attrs['source_asof'], 'simulation_asof': strat.attrs['simulation_asof'],
           'training_end': str(cutoff.date()), 'strategy': {'name': 'turn_low_research', 't1': True,
               'rebalance_days': method['rebalance_days'], 'top_n': method['top_n'], **_metrics(strat),
               'execution': strat.attrs}, 'etf_corr_top': corr_df.head(int(method['report_count'])).to_dict('records'),
           'etf_pitch': cards, 'etf_corr_matrix': matrix.to_dict(), 'allocation': allocations,
           'recommend': allocations.get('corr_liq_w'), 'failed_inputs': failures, 'auto_trade_allowed': False,
           'notes': ['训练期决定ETF/权重，之后留出期按次日开盘买入，末日收盘退出，含配置比例费用与滑点。',
                     '股票组合真实份额持有；受限卖出保留仓位并延至可验证开盘，期间按真实收盘估值；新调仓等待旧仓退出。',
                     '收盘缺口保留NaN，缺口日及恢复首日日收益不虚填；路径风险统计不可验证时返回null。',
                     '当前成分股/ETF候选池回看历史存在选择偏差；不把旧年化或旧样本量作为本次证据。',
                     '单价日过滤是保守日线代理，不等于交易所队列/涨跌停/停牌状态已逐笔验证。',
                     '历史ST未知只剔除已知ST；比例费用未模拟最低佣金/整手/冲击；股票印花税使用配置常数，未分段。',
                     '完整持有期不足的末尾信号不纳入策略净值；ETF候选规则分不是投资收益预测。']}
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    (OUT_PATH.parent/'turn_low_nav.csv').write_text(strat.to_csv(), encoding='utf-8')
    OUT_PATH.write_text(json.dumps(json_safe(out), ensure_ascii=False, indent=1, allow_nan=False), encoding='utf-8')
    print('ETF研究已生成，训练截止', out['training_end'], '留出方案', len(allocations))


if __name__ == '__main__':
    main()
