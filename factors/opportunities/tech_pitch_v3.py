"""Uncalibrated, read-only short-term rule features for the current research scope.

No historical performance badges, external model calls, or automatic buy signal.
Daily gain thresholds are proxies, not verified exchange limit-up metadata.
"""
import json
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

BASE = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(BASE))
from factors.research_context import (read_bars, cache_dir, connect, universe, scope_metadata,
                                      valuations, json_safe)
from etf.etf_map import execution_config
from autotrade.rules import board_for_symbol


def _cfg():
    return execution_config()['tech']


def _board(code):
    board_id = board_for_symbol(code)
    candidates = [rule for rule in _cfg()['board_proxies'] if rule.get('board') == board_id]
    if len(candidates) != 1:
        raise ValueError('缺少或重复的证券板块代理阈值配置')
    return candidates[0]


def _board_pct(code):
    return float(_board(code)['gain_pct'])


def load_recent(days=None):
    cfg = _cfg()
    days = int(days or max(int(cfg['atr_n'])+1, int(cfg['consec_days']), int(cfg['limup_days'])))
    # Exactly one adjustment mode, merged increments, and the configured HS300 scope.
    frame = read_bars('code,date,open,high,low,close,pct_chg,volume,turn,is_st', adjust='qfq')
    if frame.empty:
        raise ValueError('研究范围没有真实日线，未发布空成功科技池')
    dates = sorted(frame['date'].unique())[-days:]
    if len(dates) < max(int(cfg['atr_n'])+1, int(cfg['limup_days'])):
        raise ValueError('真实ATR/情绪窗口历史不足')
    frame = frame[frame['date'].isin(dates)].copy()
    if frame.duplicated(['code', 'date']).any():
        raise ValueError('同证券/日期重复，不能混合none与qfq计算信号')
    return frame


def _with_proxy(frame):
    out = frame.copy().sort_values(['code', 'date'])
    boards = _cfg()['board_proxies']
    by_id = {rule['board']: rule for rule in boards}
    if len(by_id) != len(boards):
        raise ValueError('重复的证券板块代理阈值配置')
    thresholds = {}
    for code in out['code'].unique():
        board_id = board_for_symbol(code)
        if board_id not in by_id:
            raise ValueError('缺证券板块代理阈值配置')
        thresholds[code] = float(by_id[board_id]['gain_pct'])
    pct = pd.to_numeric(out['pct_chg'], errors='coerce')
    out['limit_up_proxy'] = np.isfinite(pct) & (pct >= out['code'].map(thresholds)) & (out['volume'] > 0) & (out['is_st'] != 1)
    out['one_price'] = out['limit_up_proxy'] & (out['high'] == out['low']) & (out['high'] > 0)
    return out


def _emotion_features(frame):
    cfg = _cfg(); out = []
    for code, group in _with_proxy(frame).groupby('code', sort=False):
        recent = group.tail(int(cfg['limup_days'])); consecutive = group.tail(int(cfg['consec_days']))
        count = 0
        for observed in reversed(consecutive['limit_up_proxy'].tolist()):
            if not observed:
                break
            count += 1
        turn = pd.to_numeric(recent['turn'], errors='coerce')
        mean_turn = float(turn.mean()) if turn.notna().all() else None
        gains = int(recent['limit_up_proxy'].sum())
        active = gains*(1+mean_turn/float(cfg['emotion_rule']['turn_scale'])) if mean_turn is not None else None
        out.append({'code': code, 'limup_cnt': gains, 'consec_max': count,
                    'yizi_cnt': int(recent['one_price'].sum()), 'turn_mean': mean_turn,
                    'vol_mean': float(recent['volume'].mean()), 'active': active,
                    'observed_days': len(recent), 'st_known_days': int(recent['is_st'].notna().sum())})
    return pd.DataFrame(out)


def _short_term_score(row):
    rule = _cfg()['short_rule']
    if row['yizi_cnt'] > 0:
        value = rule['one_price_base']-rule['one_price_penalty']*min(row['yizi_cnt'], 2)
    elif row['limup_cnt'] == 1 and row['consec_max'] <= 1:
        value = rule['single']
    elif row['limup_cnt'] <= 2:
        value = rule['two']
    else:
        value = rule['multiple_base']-rule['multiple_penalty']*(row['limup_cnt']-3)
    if row['consec_max'] >= rule['hot_count']:
        value -= rule['hot_penalty']
    return float(np.clip(value, rule['floor'], rule['ceiling']))


def _open_premium_trap(code, frame):
    group = _with_proxy(frame[frame['code'] == code]).reset_index(drop=True)
    for i in reversed(range(max(0, len(group)-1))):
        if group.iloc[i]['limit_up_proxy']:
            previous = float(group.iloc[i]['close']); next_open = float(group.iloc[i+1]['open'])
            if not np.isfinite(previous) or not np.isfinite(next_open) or previous <= 0:
                continue
            premium = next_open/previous-1
            return {'premium_pct': premium, 'trap': bool(premium > _cfg()['premium_trap_fraction']),
                    'note': '已发生的高开特征；未验证其后续收益或因果'}
    return {'premium_pct': None, 'trap': None, 'note': '没有完整已发生的相邻交易日样本'}


def _stop_safety_score(code, frame):
    cfg = _cfg(); rule = cfg['stop_rule']; n = int(cfg['atr_n'])
    group = frame[frame['code'] == code].sort_values('date').tail(n+1)
    if len(group) < n+1 or group[['high', 'low', 'close', 'volume']].isna().any().any() or (group['volume'] <= 0).any():
        return None, None, None
    previous = group['close'].shift(1)
    tr = pd.concat([group['high']-group['low'], (group['high']-previous).abs(), (group['low']-previous).abs()], axis=1).max(axis=1).iloc[-n:]
    close = float(group['close'].iloc[-1]); atr = float(tr.mean())
    if not np.isfinite(atr) or atr <= 0 or close <= 0:
        return None, None, None
    stop = close-float(rule['atr_multiplier'])*atr
    if stop <= 0:
        return None, None, atr
    space = (close-stop)/close
    score = float(np.clip((space-rule['space_start'])/rule['space_scale'], rule['floor'], rule['ceiling']))
    if close < rule['low_price']:
        score *= rule['low_price_multiplier']
    if atr/close > rule['high_atr_fraction']:
        score *= rule['high_atr_multiplier']
    return score, stop, atr


def _dragon_tiger_score(code):
    # No verified, dated local top_list source exists in this initialization contract.
    # Unknown is not a neutral 0/0.5 and must not cause a network request.
    return None


def _name_map():
    with connect(cache_dir()/'stock_basic.db') as con:
        rows = con.execute('SELECT code,name,industry FROM stock_basic').fetchall()
    return {c: (name or c, industry or '') for c, name, industry in rows if c in universe()}


def _report_map(pattern, asof):
    files = list((BASE/'logs').glob(pattern))
    if not files:
        return {}
    raw = json.loads(max(files, key=lambda p: p.stat().st_mtime).read_text(encoding='utf-8'))
    if str(raw.get('date') or '') != asof:
        return {}
    return {r['code']: r for r in raw.get('results', []) if r.get('code')}


def build():
    cfg = _cfg(); frame = load_recent(); asof = str(frame['date'].max())[:10]
    names = _name_map(); features = _emotion_features(frame)
    latest = frame.sort_values('date').drop_duplicates('code', keep='last').set_index('code')
    actual_st = {c for c, (name, _) in names.items() if 'ST' in name.upper() or name.startswith('退')}
    hot_threshold = features['turn_mean'].dropna().quantile(float(cfg['hot_turn_quantile']))
    candidates = features[(features['limup_cnt'] >= 1) | (features['turn_mean'] > hot_threshold)]
    risk = _report_map('stock_risk_map*.json', asof); beneish = _report_map('beneish_report_full.json', asof)
    val = valuations(asof); mv = val.set_index('code')['total_mv'].to_dict() if not val.empty else {}
    entries, excluded = [], []
    for row in candidates.to_dict('records'):
        code = row['code']; last = latest.loc[code]
        if code in actual_st or last['is_st'] == 1 or str(last['date'])[:10] != asof or risk.get(code, {}).get('level') == 'BLOCK':
            excluded.append({'code': code, 'reason': 'ST/陈旧行情/已知风险BLOCK'}); continue
        stop_score, stop, atr = _stop_safety_score(code, frame)
        short = _short_term_score(row); premium = _open_premium_trap(code, frame)
        if premium['trap']:
            short *= float(cfg['premium_rule_multiplier'])
        emotion = None
        if row['active'] is not None:
            rule = cfg['emotion_rule']
            emotion = float(np.clip(rule['base']+row['limup_cnt']*rule['gain_count_weight']+
                                   min(row['consec_max'], rule['consecutive_cap'])*rule['consecutive_weight']+
                                   min(row['active'], rule['active_cap'])*rule['active_weight'], rule['floor'], rule['ceiling']))
        dimensions = {'short_rule': short, 'stop_rule': stop_score, 'emotion_rule': emotion,
                      'dragon_tiger': _dragon_tiger_score(code)}
        available = {key: value for key, value in dimensions.items() if value is not None}
        used_weight = sum(cfg['weights'][key] for key in available)
        score = 100*sum(value*cfg['weights'][key] for key, value in available.items())/used_weight if used_weight else None
        volume = last['volume']; mark = last['close']
        if pd.isna(volume) or volume <= 0 or pd.isna(mark) or mark <= 0:
            excluded.append({'code': code, 'reason': '最新行情/可见成交量无效'}); continue
        rc = risk.get(code, {}); bn = beneish.get(code)
        entries.append({'code': code, 'name': names.get(code, (code, ''))[0], 'industry': names.get(code, ('', ''))[1],
                        'otype': 'tech_sentiment', 'otype_name': '短线规则观察（未校准）', 'channel_tag': '短线·规则研究',
                        'horizon_hint': '持有期尚未本地验证', 'mechanism': '涨幅、换手、连续强势和ATR的描述性规则；收益方向未验证',
                        'signal_family': '情绪/动量代理', 'score': score, 'score_kind': 'uncalibrated_rule',
                        'calibrated': False, 'auto_trade_allowed': False, 'confidence': '未校准',
                        'score_breakdown': {**dimensions, '龙虎榜': dimensions['dragon_tiger'], 'weights': cfg['weights']},
                        'score_coverage': used_weight/sum(cfg['weights'].values()),
                        'dragon_tiger': {'available': False, 'score': None, 'note': '没有本地已验证龙虎榜源'},
                        'total_mv_yi': float(mv[code])/10000 if code in mv and pd.notna(mv[code]) else None,
                        'size_tier': None, 'board': _board(code)['name'], 'tech_label': names.get(code, ('', ''))[1],
                        'trigger': f"近{cfg['limup_days']}交易日涨幅阈值代理{row['limup_cnt']}次；换手观测{row['turn_mean']}",
                        'features': row, 'price': float(mark), 'tech_badges': [],
                        'evidence': '本次仅有实际日线规则特征；没有本地收益/ICIR/胜率证据，不引用旧实证数字',
                        'risk_level': rc.get('level', 'NO_DATA'), 'risk_score': rc.get('score'),
                        'risk_flags': [x.get('id') for x in rc.get('flags', [])],
                        'risk_notice': '研究草稿不可自动买入；日线阈值不等于已验证涨跌停；缺龙虎榜/短线收益校准',
                        'risk_data_available': bool(rc), 'beneish': bn,
                        'open_premium_trap': premium['trap'], 'open_premium_observation': premium,
                        'stop_plan': {'status': 'draft_rule', 'atr_stop': stop, 'atr': atr, 'stop_loss_pct': None,
                                      'short_line': True, 'auto_execute': False, 'note': '实际ATR推导的观察线，未模拟止损成交'},
                        'add_date': asof, 'pitch_line': 'short', 'pitch_sub': 'score', 'tier': 'temp', 'is_new': True})
    entries.sort(key=lambda r: -(r['score'] if r['score'] is not None else -1))
    entries = entries[:int(cfg['top_n'])]
    previous = list((BASE/'logs').glob('tech_pitch_*.json')); previous_codes = set()
    if previous:
        raw = json.loads(max(previous, key=lambda p: p.stat().st_mtime).read_text(encoding='utf-8'))
        previous_codes = {e['code'] for e in raw.get('entries', [])}
    for entry in entries:
        entry['is_new'] = entry['code'] not in previous_codes
    result = {'ts': datetime.now().isoformat(timespec='seconds'), 'pool_date': asof, 'date': asof,
              'status': 'limited' if entries else 'empty_valid', 'kind': 'uncalibrated_rule_observations',
              'research_scope': scope_metadata(), 'n_candidates': len(entries), 'entries': entries,
              'new_codes': [e['code'] for e in entries if e['is_new']], 'excluded': excluded,
              'auto_trade_allowed': False, 'calibrated': False,
              'source': {'provider': 'local_tushare_qfq', 'rows': len(frame), 'symbols': frame['code'].nunique(), 'adjust': 'qfq'},
              'tech_filters': {'engine': 'current-scope descriptive rules', 'parameters': cfg},
              'note': '真实规则特征已计算；龙虎榜/后续收益未验证，规则分不是概率，也不会产生自动委托'}
    target = BASE/'logs'/f'tech_pitch_{datetime.now():%Y%m%d_%H%M%S}.json'
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(json_safe(result), ensure_ascii=False, allow_nan=False), encoding='utf-8')
    return target


if __name__ == '__main__':
    print('短线规则观察已生成:', build().name)
