"""Historical event study for the short-term Pitch rules. Reads local caches only."""

import argparse
from functools import lru_cache
from pathlib import Path

import numpy as np
import pandas as pd

from autotrade.rules import board_for_symbol
from etf.etf_map import execution_config
from factors.opportunities import tech_pitch_v3 as pitch
from factors.research_context import cache_dir, connect, read_bars, scope_metadata
from data.research_factors import atomic_json, rank_ic

HORIZONS = (1, 3, 5, 10)
PRIMARY = 5
EXIT_GRACE = 10
BOOTSTRAP_RUNS = 4000
BARS = 'code,date,open,high,low,close,preclose,pct_chg,volume,turn,is_st'


def load_inputs():
    index = read_bars('code,date,close', adjust='none', symbols=['000300.SH'])
    daily = read_bars(BARS)
    raw = read_bars('code,date,open,high,low,close,preclose', adjust='none')
    dates = pd.Index(sorted(set(index.date) & set(daily.date)))
    dates = dates[(dates >= '2019-01-01') & (dates <= str(daily.date.max()))]
    codes = pd.Index(sorted(daily.code.unique()))

    def panel(frame, field):
        return (frame.pivot(index='date', columns='code', values=field)
                .reindex(index=dates, columns=codes).astype(float))

    fields = {key: panel(daily, key) for key in BARS.split(',')[2:]}
    original = {key: panel(raw, key) for key in ('open', 'high', 'low', 'close', 'preclose')}
    return dates, codes, fields, original


def features(f, raw, codes, cfg):
    n = cfg['limup_days']
    thresholds = {row['board']: row['gain_pct'] for row in cfg['board_proxies']}
    proxy = (f['pct_chg'] >= [thresholds[board_for_symbol(c)] for c in codes]) & (f['volume'] > 0) & (f['is_st'] != 1)
    one = proxy & (f['high'] == f['low']) & (f['high'] > 0)
    gains = proxy.rolling(n).sum()
    one_count = one.rolling(n).sum()
    consecutive = sum(proxy.rolling(k).sum().eq(k).astype(int) for k in range(1, cfg['consec_days'] + 1))
    turnover = f['turn'].rolling(n).mean()
    # Only gaps whose next opening was already observable at the signal close.
    gaps = (f['open'] / f['close'].shift() - 1).where(proxy.shift(fill_value=False))
    premium = gaps.ffill(limit=max(cfg['atr_n'], cfg['consec_days'] - 1, n - 1))
    premium_age = pd.DataFrame(np.broadcast_to(np.arange(len(proxy))[:, None], proxy.shape), index=proxy.index, columns=codes)
    premium = premium.where(premium_age - premium_age.where(gaps.notna()).ffill() < max(cfg['atr_n'], cfg['consec_days'] - 1, n - 1))

    rule = cfg['short_rule']
    short = pd.DataFrame(np.select(
        [one_count > 0, (gains == 1) & (consecutive <= 1), gains <= 2],
        [rule['one_price_base'] - rule['one_price_penalty'] * one_count.clip(upper=2),
         rule['single'], rule['two']],
        default=rule['multiple_base'] - rule['multiple_penalty'] * (gains - 3)),
        index=proxy.index, columns=codes)
    short = (short - rule['hot_penalty'] * (consecutive >= rule['hot_count'])).clip(rule['floor'], rule['ceiling'])
    short = short.where(~(premium > cfg['premium_trap_fraction']), short * cfg['premium_rule_multiplier'])

    close = f['close']
    prior = close.shift()
    tr = np.maximum(f['high'] - f['low'], np.maximum((f['high'] - prior).abs(), (f['low'] - prior).abs()))
    atr = tr.rolling(cfg['atr_n']).mean()
    complete = ((f['volume'] > 0) & f['high'].notna() & f['low'].notna() & close.notna())
    atr = atr.where((complete.rolling(cfg['atr_n'] + 1).sum() == cfg['atr_n'] + 1) & (atr > 0))
    rule = cfg['stop_rule']
    stop_line = close - atr * rule['atr_multiplier']
    stop_score = ((1 - stop_line / close - rule['space_start']) / rule['space_scale']).clip(rule['floor'], rule['ceiling'])
    stop_score = stop_score.where(~(raw['close'] < rule['low_price']), stop_score * rule['low_price_multiplier'])
    stop_score = stop_score.where(~(atr / close > rule['high_atr_fraction']), stop_score * rule['high_atr_multiplier'])
    stop_score = stop_score.where(stop_line > 0)

    rule = cfg['emotion_rule']
    active = gains * (1 + turnover / rule['turn_scale'])
    emotion = (rule['base'] + gains * rule['gain_count_weight'] +
               consecutive.clip(upper=rule['consecutive_cap']) * rule['consecutive_weight'] +
               active.clip(upper=rule['active_cap']) * rule['active_weight']).clip(rule['floor'], rule['ceiling'])
    dimensions = {'short_rule': short, 'stop_rule': stop_score, 'emotion_rule': emotion}
    weights = cfg['weights']
    total = sum(x.fillna(0) * weights[key] for key, x in dimensions.items())
    denominator = sum(x.notna() * weights[key] for key, x in dimensions.items())
    score = 100 * total / denominator.astype(float).where(denominator > 0)
    valid = (close > 0) & (f['volume'] > 0) & (f['is_st'] != 1)
    candidates = valid & ((gains >= 1) | turnover.gt(turnover.quantile(cfg['hot_turn_quantile'], axis=1), axis=0))
    return score, candidates, valid, atr, dimensions


def parity_check(f, score, candidates, atr, cfg):
    """Compare full-window historical replay with the production scalar formulas."""
    dates = f['close'].index[-(cfg['atr_n'] + 1):]
    sample = pd.concat({field: f[field].loc[dates].stack() for field in
                        ('open', 'high', 'low', 'close', 'volume', 'turn', 'pct_chg', 'is_st')}, axis=1)
    sample.index.names = ['date', 'code']
    sample = sample.dropna(subset=['close']).reset_index()
    latest = sample[sample.date == dates[-1]].set_index('code')
    production = pitch._emotion_features(sample)
    hot = production.turn_mean.quantile(cfg['hot_turn_quantile'])
    disagreements = 0
    maximum_error = 0.
    compared = 0
    for row in production.to_dict('records'):
        code = row['code']
        if code not in latest.index or len(sample[sample.code == code]) != len(dates):
            continue
        last = latest.loc[code]
        expected = ((row['limup_cnt'] >= 1 or (row['turn_mean'] is not None and row['turn_mean'] > hot))
                    and last.is_st != 1 and last.volume > 0 and last.close > 0)
        disagreements += expected != candidates.at[dates[-1], code]
        if not expected:
            continue
        short = pitch._short_term_score(row)
        if pitch._open_premium_trap(code, sample)['trap']:
            short *= cfg['premium_rule_multiplier']
        stop, _, actual_atr = pitch._stop_safety_score(code, sample)
        values = {'short_rule': short, 'stop_rule': stop}
        if row['active'] is not None:
            r = cfg['emotion_rule']
            values['emotion_rule'] = np.clip(r['base'] + row['limup_cnt'] * r['gain_count_weight'] +
                min(row['consec_max'], r['consecutive_cap']) * r['consecutive_weight'] +
                min(row['active'], r['active_cap']) * r['active_weight'], r['floor'], r['ceiling'])
        available = {key: x for key, x in values.items() if x is not None}
        expected_score = 100 * sum(x * cfg['weights'][key] for key, x in available.items()) / sum(cfg['weights'][key] for key in available)
        maximum_error = max(maximum_error, abs(expected_score - score.at[dates[-1], code]))
        if actual_atr is not None:
            np.testing.assert_allclose(atr.at[dates[-1], code], actual_atr)
        compared += 1
    assert compared and disagreements == 0 and maximum_error < 1e-8, (compared, disagreements, maximum_error)
    return {'date': dates[-1], 'scores_checked': compared, 'max_score_error': maximum_error}


class MinuteOrder:
    """Use raw minutes only when all bars reconcile to the daily raw OHLC."""

    def __init__(self, dates, codes, f, raw):
        self.dates, self.codes, self.f, self.raw = dates, codes, f, raw
        db = cache_dir() / 'minute_baostock.db'
        self.db = connect(db) if db.is_file() else None
        self.first = self.db.execute('SELECT MIN(trade_time) FROM minute_5m').fetchone()[0][:10] if self.db else None
        self.last = self.db.execute('SELECT MAX(trade_time) FROM minute_5m').fetchone()[0][:10] if self.db else None

    @lru_cache(maxsize=20000)
    def day(self, i, j):
        date = self.dates[i]
        if self.first is None or not self.first <= date <= self.last:
            return None
        rows = self.db.execute(
            'SELECT trade_time,open,high,low,close FROM minute_5m '
            'WHERE code=? AND trade_time>=? AND trade_time<=? ORDER BY trade_time',
            (self.codes[j], date + ' 09:35:00', date + ' 15:00:00')).fetchall()
        morning = pd.date_range(date + ' 09:35', date + ' 11:30', freq='5min')
        afternoon = pd.date_range(date + ' 13:05', date + ' 15:00', freq='5min')
        if [row[0] for row in rows] != morning.append(afternoon).strftime('%Y-%m-%d %H:%M:%S').tolist():
            return None
        bars = np.asarray([row[1:] for row in rows], dtype=float)
        daily = [self.raw[key].iat[i, j] for key in ('open', 'high', 'low', 'close')]
        if not np.isfinite(bars).all() or not np.allclose(
            [bars[0, 0], bars[:, 1].max(), bars[:, 2].min(), bars[-1, 3]], daily, rtol=0, atol=.011
        ):
            return None
        return bars * (self.f['open'].iat[i, j] / daily[0])

    def first_touch(self, day, col, stop, target):
        bars = self.day(int(day), int(col))
        if bars is None:
            return None
        for op, high, low, _ in bars:
            if op <= stop:
                return False, op
            if op >= target:
                return True, op
            hit_stop, hit_target = low <= stop, high >= target
            if hit_stop and hit_target:
                return None
            if hit_stop:
                return False, stop
            if hit_target:
                return True, target
        return None


class Replay:
    def __init__(self, f, raw, codes, dates, atr, fees):
        self.op, self.high, self.low, self.close, self.volume, self.st = (
            f[key].to_numpy() for key in ('open', 'high', 'low', 'close', 'volume', 'is_st'))
        self.atr = atr.to_numpy()
        prices_ok = (self.op > 0) & (self.low > 0) & (self.high >= np.maximum(self.op, self.close)) & (
            self.low <= np.minimum(self.op, self.close)) & (self.volume > 0)
        one = prices_ok & (self.high == self.low)
        boards = np.array([board_for_symbol(c) for c in codes])
        twenty = (boards == 'STAR') | ((boards == 'CHINEXT') & (np.asarray(dates)[:, None] >= '2020-08-24'))
        limit = np.where(twenty, .20, .10)
        before_main_st_change = (boards != 'MAIN') | (np.asarray(dates)[:, None] < '2026-07-06')
        limit = np.where((self.st == 1) & ~twenty & before_main_st_change, .05, limit)
        last = raw['preclose'].to_numpy()
        raw_open = raw['open'].to_numpy()
        upper = np.floor(last * (1 + limit) * 100 + .50000001) / 100
        lower = np.floor(last * (1 - limit) * 100 + .50000001) / 100
        self.buyable = prices_ok & ~one & (self.st != 1) & (raw_open > 0) & (last > 0) & (raw_open < upper - 1e-8)
        self.open_sellable = prices_ok & ~one & (raw_open > 0) & (last > 0) & (raw_open > lower + 1e-8)
        self.intraday_sellable = prices_ok & ~one
        self.minute = MinuteOrder(dates, codes, f, raw)
        commission, tax, slip = (fees[key] for key in ('commission_rate', 'sell_tax_rate', 'slippage_rate'))
        self.net_factor = (1 - commission - tax - slip) / (1 + commission + slip)

    def run(self, row, col, horizon, stop_multiple=None, target_multiple=None, tie='minute'):
        entry = self.op[row + 1, col]
        stop = self.close[row, col] - self.atr[row, col] * stop_multiple if stop_multiple is not None else np.full(len(row), -np.inf)
        target = entry + self.atr[row, col] * target_multiple if target_multiple is not None else np.full(len(row), np.inf)
        price = np.full(len(row), np.nan)
        reason = np.full(len(row), '', dtype='U8')
        pending_stop = np.zeros(len(row), dtype=bool)
        ambiguous = np.zeros(len(row), dtype=bool)
        resolved = np.zeros(len(row), dtype=bool)
        delay = np.full(len(row), np.nan)
        for offset in range(2, horizon + 2 + EXIT_GRACE):
            active = np.flatnonzero(np.isnan(price))
            if not len(active):
                break
            day = np.minimum(row[active] + offset, self.op.shape[0] - 1)
            in_sample = row[active] + offset < self.op.shape[0]
            stock = col[active]
            op, high, low = (field[day, stock] for field in (self.op, self.high, self.low))
            open_ok = self.open_sellable[day, stock] & in_sample
            at_stop = pending_stop[active] | (op <= stop[active])
            at_target = op >= target[active]
            timed = offset >= horizon + 1
            fill_open = open_ok & (at_stop | at_target | timed)
            opened = active[fill_open]
            price[opened] = op[fill_open]
            reason[opened] = np.where(at_stop[fill_open], 'stop', np.where(at_target[fill_open], 'target', 'time'))
            delay[opened] = offset
            if timed:
                continue

            touch_stop = (low <= stop[active]) | at_stop
            touch_target = high >= target[active]
            sellable = self.intraday_sellable[day, stock] & in_sample
            blocked_stop = touch_stop & ~fill_open & (~sellable | (~open_ok & at_stop))
            pending_stop[active[blocked_stop]] = True
            eligible = ~fill_open & sellable & ~blocked_stop
            only_stop = active[eligible & touch_stop & ~touch_target]
            only_target = active[eligible & touch_target & ~touch_stop]
            price[only_stop], reason[only_stop], delay[only_stop] = stop[only_stop], 'stop', offset
            price[only_target], reason[only_target], delay[only_target] = target[only_target], 'target', offset
            for k in np.flatnonzero(eligible & touch_stop & touch_target):
                ix = active[k]
                ambiguous[ix] = True
                first = self.minute.first_touch(day[k], stock[k], stop[ix], target[ix]) if tie == 'minute' else None
                if first is not None:
                    resolved[ix] = True
                    target_first, fill = first
                else:
                    target_first = tie == 'target'
                    fill = target[ix] if target_first else stop[ix]
                price[ix], reason[ix], delay[ix] = fill, 'target' if target_first else 'stop', offset
        return pd.DataFrame({'net': price / entry * self.net_factor - 1, 'reason': reason,
                             'ambiguous': ambiguous, 'minute_resolved': resolved, 'offset': delay})


def interval(series):
    values = series.dropna()
    if not len(values):
        return [None, None]
    grouped = values.groupby(pd.to_datetime(values.index).to_period('M')).agg(['sum', 'size'])
    rng = np.random.default_rng(17)
    indices = rng.integers(0, len(grouped), size=(BOOTSTRAP_RUNS, len(grouped)))
    draws = grouped['sum'].to_numpy()[indices].sum(axis=1) / grouped['size'].to_numpy()[indices].sum(axis=1)
    return np.quantile(draws, [.025, .975]).tolist()


def stats(values, dates):
    series = pd.Series(np.asarray(values), index=dates).dropna()
    daily = series.groupby(level=0).mean()
    return {'events': len(series), 'dates': len(daily), 'mean': daily.mean(), 'ci': interval(daily),
            'event_mean': series.mean(), 'win': (series > 0).mean(), 'p05': series.quantile(.05),
            'tail_mean': series[series <= series.quantile(.05)].mean()}


def score_study(base, score, dimensions, candidates, top, dates, codes, test):
    label_array = np.full((len(dates), len(codes)), np.nan)
    label_array[base['row'], base['col']] = base['net']
    label = pd.DataFrame(label_array, index=dates, columns=codes)
    by_day = label.where(candidates).mean(axis=1)
    top_day = label.where(top).mean(axis=1)
    population = label.mean(axis=1)
    out = {}
    for name, mask in test.items():
        delta = (top_day - by_day)[mask]
        candidate_delta = (by_day - population)[mask]
        factors = {}
        for key, factor in {'score': score, **dimensions}.items():
            ic, _ = rank_ic(factor.where(candidates).loc[mask], label.loc[mask], 8)
            factors[key] = {'mean': ic.mean(), 'ci': interval(ic), 'dates': int(ic.notna().sum())}
        selected = mask[base['row']] & base['top']
        out[name] = {
            'top': stats(base['net'][selected], dates[base['row'][selected]]),
            'top_minus_candidates': delta.mean(), 'top_minus_candidates_ci': interval(delta),
            'candidate_mean': by_day[mask].mean(), 'universe_mean': population[mask].mean(),
            'candidates_minus_universe': candidate_delta.mean(), 'candidates_minus_universe_ci': interval(candidate_delta),
            'rank_ic': factors}
    return out


def exits_study(result, base, dates, splits):
    out = {}
    for name, mask in splits.items():
        r, b = result.loc[mask], base.loc[mask]
        day = dates[b['row']]
        valid = r.net.notna() & b.net.notna()
        diff = pd.Series((r.net[valid] - b.net[valid]).to_numpy(), index=day[valid]).groupby(level=0).mean()
        reasons = r.reason[valid].value_counts().to_dict()
        out[name] = {
            **stats(r.net[valid], day[valid]), 'matched_baseline': stats(b.net[valid], day[valid]),
            'delta': diff.mean(), 'delta_ci': interval(diff), 'unresolved': int((~valid).sum()),
            'stops': reasons.get('stop', 0), 'targets': reasons.get('target', 0),
            'collisions': int(r.ambiguous[valid].sum()), 'minute_resolved': int(r.minute_resolved[valid].sum()),
            'delayed': int((r.offset[valid] > PRIMARY + 1).sum())}
    return out


def run(args):
    settings = execution_config()
    cfg = settings['tech']
    dates, codes, fields, raw = load_inputs()
    score, candidates, valid, atr, dimensions = features(fields, raw, codes, cfg)
    parity = parity_check(fields, score, candidates, atr, cfg)
    top = score.where(candidates).rank(axis=1, method='first', ascending=False) <= cfg['top_n']
    replay = Replay(fields, raw, codes, dates, atr, settings['stock_fees'])
    historical = np.asarray(dates) >= args.start
    eligible = valid.to_numpy() & historical[:, None]
    eligible[-(max(HORIZONS) + EXIT_GRACE + 1):] = False
    row, col = np.where(eligible)
    traded = replay.buyable[row + 1, col]
    selection = top.to_numpy()[row, col]
    counts = {'signals': int(selection.sum()), 'blocked_entry': int((selection & ~traded).sum())}
    row, col = row[traded], col[traded]

    date_array = np.asarray(dates)
    dev = date_array < args.holdout
    # Purge the boundary by the maximum modeled time from signal to exit.
    cut = np.searchsorted(date_array, args.holdout)
    dev[max(0, cut - max(HORIZONS) - EXIT_GRACE - 1):cut] = False
    test = {'development': dev, 'holdout': date_array >= args.holdout}
    for year in sorted(set(d[:4] for d in date_array[test['holdout']])):
        test[year] = test['holdout'] & np.char.startswith(date_array.astype(str), year)
    print('Inputs and score parity verified', parity, flush=True)
    score_results = {}
    for horizon in HORIZONS:
        fixed = replay.run(row, col, horizon)
        base = fixed.assign(row=row, col=col, top=top.to_numpy()[row, col])
        score_results[str(horizon)] = score_study(base, score, dimensions, candidates, top, dates, codes, test)
        if horizon == PRIMARY:
            primary = base
    print('Fixed-horizon studies complete', flush=True)

    # Match every exit comparison to top-ranked entries with an observable ATR.
    score_labels = primary[primary.net.notna()].copy()
    score_labels['score'] = score.to_numpy()[score_labels.row, score_labels.col]
    score_labels = score_labels[candidates.to_numpy()[score_labels.row, score_labels.col]]
    score_labels['band'] = pd.cut(score_labels.score, [0, 40, 50, 60, 70, 80, 100], include_lowest=True)
    bands = {}
    for split in ('development', 'holdout'):
        by_band = score_labels[test[split][score_labels.row]].groupby('band', observed=True)
        bands[split] = {str(key): stats(group.net, dates[group.row]) for key, group in by_band}

    train_labels = score_labels[test['development'][score_labels.row]]
    holdout_labels = score_labels[test['holdout'][score_labels.row]].copy()
    overall_probability = (train_labels.net > 0).mean()
    train_probabilities = (train_labels.assign(win=train_labels.net > 0)
                           .groupby('band', observed=True).win.mean())
    predicted = holdout_labels.band.map(train_probabilities).astype(float).fillna(overall_probability)
    actual = (holdout_labels.net > 0).astype(float)
    brier = (predicted - actual) ** 2
    brier_constant = (overall_probability - actual) ** 2
    calibration_diff = pd.Series((brier - brier_constant).to_numpy(),
                                 index=dates[holdout_labels.row]).groupby(level=0).mean()
    probability_check = {
        'events': len(actual), 'baseline_probability': overall_probability,
        'brier': brier.mean(), 'constant_brier': brier_constant.mean(),
        'daily_brier_delta': calibration_diff.mean(), 'daily_brier_delta_ci': interval(calibration_diff),
    }

    primary = primary[primary['top'] & (atr.to_numpy()[row, col] > 0)].reset_index(drop=True)
    sr, sc = primary.row.to_numpy(), primary.col.to_numpy()
    splits = {key: mask[sr] for key, mask in test.items()}
    splits['minute_window'] = test['holdout'][sr] & (date_array[sr] >= replay.minute.first)
    results = {}

    def examine(stop, target, tie='minute', split=splits):
        key = f'{stop or 0:g}x_stop_{target or 0:g}x_target' + (f'_{tie}' if tie != 'minute' else '')
        data = replay.run(sr, sc, PRIMARY, stop, target, tie=tie)
        results[key] = exits_study(data, primary, dates, split)
        return key

    original = cfg['stop_rule']['atr_multiplier']
    examine(None, None)
    examine(original, None)
    development = {'development': splits['development']}
    stop_keys = {x: examine(x, None, split=development) for x in set(args.stops) - {original}}
    stop_keys[original] = f'{original:g}x_stop_0x_target'
    target_keys = {x: examine(None, x, split=development) for x in args.targets}
    combo_keys = {(stop, target): examine(stop, target, split=development)
                  for stop in sorted(stop_keys) for target in args.targets}

    def choose(grid):
        return max(grid, key=lambda item: results[grid[item]]['development']['delta'])

    best_stop, best_target = choose(stop_keys), choose(target_keys)
    combo = choose(combo_keys)
    original_target = choose({target: combo_keys[original, target] for target in args.targets})
    selected = {
        'stop': examine(best_stop, None),
        'target': examine(None, best_target),
        'combined': examine(*combo),
        'original_stop_with_target': examine(original, original_target),
    }
    examine(*combo, tie='stop')
    examine(*combo, tie='target')
    examine(original, original_target, tie='stop')
    examine(original, original_target, tie='target')
    stop_alone = replay.run(sr, sc, PRIMARY, original).assign(row=sr)
    stop_with_target = replay.run(sr, sc, PRIMARY, original, original_target)
    incremental_target = exits_study(stop_with_target, stop_alone, dates, splits)
    bounds = [dates[row.min()], dates[row.max()]]
    sample = {
        key: [dates[row[mask[row]].min()], dates[row[mask[row]].max()]]
        for key, mask in test.items() if mask[row].any()
    }
    atomic_json(args.output, {
        'scope': scope_metadata(), 'data_dates': [dates[0], dates[-1]], 'signal_dates': bounds,
        'dates_by_split': sample, 'start': args.start, 'holdout': args.holdout, 'parity': parity,
        'bars_last_symbols': int(fields['close'].iloc[-1].notna().sum()),
        'historical_st_coverage': float(fields['is_st'].where(fields['close'].notna()).count().sum() /
                                        fields['close'].count().sum()),
        'fixed_horizons': HORIZONS, 'primary_horizon': PRIMARY, 'exit_grace': EXIT_GRACE,
        'minutes': [replay.minute.first, replay.minute.last], 'top_n': cfg['top_n'], 'counts': counts,
        'fee': settings['stock_fees'], 'score_study': score_results, 'score_bands': bands,
        'exit_study': results, 'selected': selected, 'incremental_target': incremental_target,
        'probability_check': probability_check,
    })
    print(args.output, flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--start', default='2019-01-01')
    parser.add_argument('--holdout', default='2024-01-01')
    parser.add_argument('--stops', type=float, nargs='+', default=(1, 1.5, 2, 2.5, 3))
    parser.add_argument('--targets', type=float, nargs='+', default=(1, 1.5, 2, 3, 4))
    parser.add_argument('--output', type=Path, required=True)
    run(parser.parse_args())
