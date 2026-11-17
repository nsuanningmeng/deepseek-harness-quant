"""A single quote loop, independent JSON-only AI planning, and a paper ledger."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path
from dataclasses import asdict
import hashlib
import json
import time
from copy import deepcopy
from autotrade.locking import ProcessLock

from autotrade.monitoring import atomic_json, event
from .config import load_config
from .models import Quote, Instrument
from .planner import AiPlanner
from .activation import ActivationGate, public_activation, verify_ai_connection, ACTIVATION_CHECK_IDS

SHANGHAI = ZoneInfo('Asia/Shanghai')


def _market_expected_open(market, session, now):
    if session.get('calendar_verified') is True:
        return session.get('open') is True
    clock = now.strftime('%H:%M')
    return now.weekday() < 5 and any(start <= clock < end for start, end in market['trading_sessions'])


def _poll_delay(market, session, now):
    clock = now.strftime('%H:%M')
    close_hour, close_minute = map(int, market['close_time'].split(':'))
    close = now.replace(hour=close_hour, minute=close_minute, second=0, microsecond=0)
    near_close = (clock >= market['trading_sessions'][-1][1]
                  and (now - close).total_seconds() <= market['close_observation_seconds'])
    if session.get('open') or session.get('expected_open') or near_close:
        return market['refresh_seconds']
    delay = market['closed_refresh_seconds']
    if session.get('next_open'):
        until_open = (datetime.fromisoformat(session['next_open']) - now).total_seconds()
        if until_open > 0:
            return min(delay, max(1, until_open))
    elif not session.get('calendar_verified') and now.weekday() < 5:
        for start, _ in market['trading_sessions']:
            if clock < start:
                hour, minute = map(int, start.split(':'))
                opening = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
                return min(delay, max(market['refresh_seconds'], (opening - now).total_seconds()))
    return delay


def _runner_event(root, name, now, **fields):
    try:
        event(root, name, observed_at=now.isoformat(), **fields)
    except OSError:
        pass


def _failure_kind(detail):
    """Classify diagnostics without retaining any exception or provider text."""
    detail = str(detail).lower()
    for kind, hints in (
        ('timeout', ('timeout', 'timed out')),
        ('network', ('network is unreachable', 'connection refused', 'connection reset', 'name resolution', 'nodename', 'certificate')),
        ('permission', ('没有该接口访问权限', '权限不足', 'permission denied', '凭据不可读')),
        ('invalid_response', ('turnover', 'cannot be verified', 'limit mismatch', '不一致', '无法验证')),
    ):
        if any(hint in detail for hint in hints):
            return kind
    return 'unavailable'


def _quote_readiness_hold(activation, session, feed_status):
    """Keep resting orders during an isolated quote gap; never permit matching.

    All non-quote gates must still pass. This only changes cancellation into
    waiting, not the data required to submit a plan or execute an order.
    """
    if (activation.get('enabled') is not True or activation.get('armed') is not True
            or activation.get('manual_override') or activation.get('ready') is not False
            or session.get('open') is not True or session.get('calendar_verified') is not True
            or feed_status.get('universe', {}).get('cache_stale', True)):
        return False
    checks = activation.get('checks') or []
    if (len(checks) != len(ACTIVATION_CHECK_IDS)
            or any(not isinstance(row, dict) or type(row.get('ok')) is not bool for row in checks)
            or {row.get('id') for row in checks} != set(ACTIVATION_CHECK_IDS)):
        return False
    failed = {row.get('id') for row in checks if not row['ok']}
    return bool(failed) and failed <= {'quotes', 'executable'}


def _write_now(operation, *args, **kwargs):
    """Re-sample a write clock after contention; never retry a business failure.

    Ledger raises this exact error before its transaction can mutate anything.
    A concurrent API write may have advanced that clock after the runner sampled
    it. Keep the ledger guard, and make at most three fresh-clock attempts.
    """
    for attempt in range(3):
        try:
            return operation(*args, now=datetime.now(SHANGHAI), **kwargs)
        except ValueError as exc:
            if exc.args != ('paper_clock_moved_backwards',) or attempt == 2:
                raise


def normalise(snapshot):
    account = dict(snapshot.get('account', {}))
    account['profit'] = account.get('pnl')
    account['pnl_percent'] = account.get('return_pct')
    account['positions'] = [{**p, 'frozen': p.get('frozen_quantity'), 'cost': p.get('average_cost'),
                             'last': p.get('last_price'), 'pnl': p.get('unrealized_pnl')}
                            for p in snapshot.get('positions', [])]
    account['orders'] = [{**o, 'id': o.get('id', o.get('order_id')), 'filled': o.get('filled', o.get('filled_quantity', 0))}
                         for o in snapshot.get('orders', [])]
    account['fills'] = [{**f, 'time': f.get('time', f.get('timestamp', f.get('executed_at', f.get('filled_at')))),
                        'fee': f.get('fee', f.get('fees'))} for f in snapshot.get('fills', [])]
    account['curve'] = [{**p, 'time': p.get('timestamp')} for p in snapshot.get('curve', [])]
    account['reconciliation'] = snapshot.get('reconciliation')
    account['execution_diagnostics'] = deepcopy(snapshot.get('execution_diagnostics') or {})
    return account


def _merge_council_status(ai, description):
    """Keep observed analysis separate from current configuration and probe evidence."""
    previous = ai.get('council') or {}
    trace = previous.get('latest_trace') if isinstance(previous, dict) else None
    ai.update(description)
    if description.get('configured') is True:
        ai.pop('error', None)
    policy = deepcopy(description.get('council') or {'enabled': False, 'configured': False, 'roles': []})
    if isinstance(trace, dict):
        policy['latest_trace'] = deepcopy(trace)
        policy['latest_trace_current_config'] = (trace.get('config_fingerprint') == policy.get('config_fingerprint'))
    ai['council'] = policy


def _remember_council(ai, trace):
    if isinstance(trace, dict):
        policy = deepcopy(ai.get('council') or {})
        policy['latest_trace'] = deepcopy(trace)
        policy['latest_trace_current_config'] = trace.get('config_fingerprint') == policy.get('config_fingerprint')
        ai['council'] = policy


def public_state(cfg, raw, snapshot, now=None):
    now = now or datetime.now(SHANGHAI)
    market, ai = dict(raw.get('market', {})), dict(raw.get('ai', {}))
    from .council import describe_policy
    try:
        policy = describe_policy(cfg)
    except ValueError as exc:
        policy = {'enabled': bool(cfg.get('council', {}).get('enabled')), 'configured': False,
                  'roles': [], 'config_fingerprint': None, 'error': str(exc)}
    _merge_council_status(ai, {'council': policy})
    changed_source = market.get('provider', 'tencent_baostock') != cfg['market']['provider']
    age, stale = None, True
    try:
        updated = datetime.fromisoformat(raw['updated_at'])
        if updated.utcoffset() is None:
            raise ValueError('heartbeat requires a timezone')
        age = (now - updated).total_seconds()
        grace = cfg['market']['heartbeat_grace_seconds']
        close = updated.replace(hour=int(cfg['market']['close_time'][:2]),
                                minute=int(cfg['market']['close_time'][3:]), second=0, microsecond=0)
        last_end = cfg['market']['trading_sessions'][-1][1]
        auction = updated.replace(hour=int(last_end[:2]), minute=int(last_end[3:]), second=0, microsecond=0)
        near_close = auction <= updated and (updated - close).total_seconds() <= cfg['market']['close_observation_seconds']
        cadence = cfg['market']['refresh_seconds'] if market.get('open') or market.get('expected_open') or near_close else cfg['market']['closed_refresh_seconds']
        deadline = updated + timedelta(seconds=cadence + grace)
        if not market.get('open') and market.get('next_open'):
            next_open = datetime.fromisoformat(market['next_open'])
            if next_open.utcoffset() is None:
                raise ValueError('next open requires a timezone')
            deadline = min(deadline, next_open + timedelta(seconds=grace))
        stale = age < 0 or now > deadline
    except (ValueError, TypeError, KeyError, OverflowError):
        pass
    stale = stale or changed_source
    if changed_source:
        raw = {**raw, 'quotes': [], 'universe': [], 'eligible_symbols': []}
        market.update({'source': cfg['market']['source_label'], 'provider': cfg['market']['provider'],
                       'quote_count': 0, 'universe_count': 0, 'asof': None, 'next_open': None})
    market.update({'heartbeat_age_seconds': round(age, 1) if age is not None else None,
                   'heartbeat_stale': stale})
    if stale:
        market.update({'open': False, 'buy_ready': False,
                       'phase': '数据源已切换，等待所选行情进程' if changed_source else '行情进程心跳已过期'})
        ai.update({'phase': '等待行情进程恢复', 'next_run_at': None})
    activation = public_activation(cfg)
    if stale and activation.get('enabled'):
        activation.update(ready=False, status='paused' if activation.get('status') == 'paused' else 'waiting_data',
                          blocking_reasons=[*activation.get('blocking_reasons', []), '行情后台心跳未通过当前时效验收'])
    paused = Path(cfg['risk']['kill_switch_file']).exists() or raw.get('paused') is True
    if paused:
        waiting = activation.get('enabled') and activation.get('armed') and activation.get('status') == 'waiting_data'
        market.update({'open': False, 'buy_ready': False,
                       'phase': '等待数据验收后自动启动' if waiting else '盘中模拟已暂停'})
        ai.update({'phase': '等待数据验收后自动启动' if waiting else '已暂停，未生成交易计划', 'next_run_at': None})
    risk_policy = deepcopy(snapshot.get('risk_policy') or {})
    risk_policy['effective'] = bool(not stale and snapshot.get('execution_config_fingerprint')
                                    and raw.get('execution_config_fingerprint') == snapshot['execution_config_fingerprint'])
    from .readiness import market_wait_status
    readiness = market_wait_status({'activation': activation, 'market': market, 'ai': ai,
                                   'account_snapshot': snapshot}, now)
    return {**raw, 'market': market, 'ai': ai, 'activation': activation, 'paused': paused, 'ok': True, 'mode': 'paper',
            'readiness': readiness,
            'started_at': snapshot.get('started_at'), 'account': normalise(snapshot),
            'risk_policy': risk_policy,
            'navigation': {'label': cfg['ui']['label'], 'path': '/paperlive'}}


def read_state(cfg=None):
    cfg = cfg or load_config()
    root = Path(cfg['runtime_dir'])
    try:
        raw = json.loads((root / 'state.json').read_text())
    except (OSError, ValueError):
        raw = {'market': {'open': False, 'phase': '实时行情服务尚未启动'}, 'ai': {}, 'quotes': [], 'universe': []}
    from .ledger import Ledger
    quotes = {}
    same_source = raw.get('market', {}).get('provider', 'tencent_baostock') == cfg['market']['provider']
    for value in raw.get('quotes', []) if same_source else []:
        try:
            quote = Quote.from_dict(value)
            quotes[quote.symbol] = quote
        except (ValueError, TypeError, KeyError):
            continue
    snapshot = Ledger(cfg).snapshot(quotes, now=datetime.now(SHANGHAI))
    return public_state(cfg, raw, snapshot)


def manual_order(payload, cfg=None):
    cfg = cfg or load_config()
    raw = json.loads((Path(cfg['runtime_dir']) / 'state.json').read_text())
    now = datetime.now(SHANGHAI)
    market = raw['market']
    if market.get('provider', 'tencent_baostock') != cfg['market']['provider']:
        raise ValueError('所选数据源尚未提供已验证行情')
    if not market.get('open') or not market.get('calendar_verified') or market.get('date') != now.date().isoformat():
        raise ValueError('当前不在已验证的连续交易时段')
    updated = datetime.fromisoformat(raw['updated_at'])
    if not 0 <= (now - updated).total_seconds() <= cfg['market']['max_quote_age_seconds']:
        raise ValueError('实时行情进程状态已过期，请等待恢复')
    if Path(cfg['risk']['kill_switch_file']).exists():
        raise ValueError('模拟账户已暂停')
    instruments = {v['symbol']: Instrument(**v) for v in raw['universe']}
    symbol = str(payload.get('symbol', '')).upper()
    if symbol not in instruments:
        raise ValueError('证券不在当前允许范围')
    if str(payload.get('side', '')).upper() == 'BUY' and symbol not in raw.get('eligible_symbols', []):
        raise ValueError('买入证券必须属于当前合格的沪深300候选范围')
    if str(payload.get('side', '')).upper() == 'BUY' and not market.get('buy_ready'):
        raise ValueError('当前成分资料未就绪，暂停新买入')
    if str(payload.get('side', '')).upper() == 'BUY' and not public_activation(cfg).get('ready'):
        raise ValueError('自动启用验收尚未通过，暂停新买入')
    quantity = payload.get('quantity')
    if not isinstance(quantity, int) or isinstance(quantity, bool):
        raise ValueError('委托数量必须为整数')
    from .ledger import Ledger
    result = _write_now(Ledger(cfg).submit, str(payload.get('side', '')).upper(), symbol, quantity,
                        payload.get('limit_price'), instruments[symbol], session_open=True,
                        intent_id=payload.get('intent_id'))
    if result.get('status') == 'REJECTED':
        raise ValueError(result.get('reason') or '委托未通过模拟账户风控')
    return result


def cancel_order(payload, cfg=None):
    from .ledger import Ledger
    return _write_now(Ledger(cfg or load_config()).cancel, str(payload.get('order_id', '')))


def run(cfg=None, once=False):
    cfg = cfg or load_config()
    root = Path(cfg['runtime_dir'])
    root.mkdir(parents=True, exist_ok=True)
    lock = ProcessLock(root / 'runner.lock')
    lock.__enter__()
    from .market import MarketFeed
    from .ledger import Ledger
    if cfg['market']['provider'] == 'tushare':
        from .tushare_feed import TushareMarketFeed
        feed = TushareMarketFeed(cfg)
    elif cfg['market']['provider'] == 'tencent_tushare':
        from .hybrid_feed import HybridMarketFeed
        feed = HybridMarketFeed(cfg)
    else:
        feed = MarketFeed(cfg)
    ledger, planner, gate = Ledger(cfg), AiPlanner(cfg), ActivationGate(cfg)
    instruments, quotes = {}, {}
    try:
        previous = json.loads((root / 'state.json').read_text())
        ai = previous.get('ai', {})
    except (OSError, ValueError):
        ai = {}
    ai.update({'enabled': cfg['strategy']['mode'] == 'ai', 'interval_seconds': cfg['strategy']['ai_interval_seconds']})
    _merge_council_status(ai, planner.describe())
    last_attempt = float(ai.get('last_attempt_epoch', 0))
    future = None
    probe_future = None
    pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix='paper-ai')
    last_issue = None
    held_order_ids = []
    was_expected_open = False
    try:
        _runner_event(root, 'runner_started', datetime.now(SHANGHAI), provider=cfg['market']['provider'])
        while True:
            now = datetime.now(SHANGHAI)
            error = None
            issue = None
            write_blocked = False
            eligible = {}
            stage = 'universe'
            try:
                eligible = feed.universe()
                instruments = dict(eligible)
                stage = 'position_quote_scope'
                extra = set(ledger.symbols()) - set(instruments)
                if extra:
                    instruments.update(feed.lookup(extra))
                # Existing positions and orders retain a quote when index membership changes.
                symbols = sorted(set(instruments) | set(ledger.symbols()))
                stage = 'quotes'
                quotes = feed.quotes(symbols)
                stage = 'calendar'
                session = feed.session(datetime.now(SHANGHAI))
                session['expected_open'] = _market_expected_open(cfg['market'], session, datetime.now(SHANGHAI))
                if cfg['market']['provider'] in {'tushare', 'tencent_tushare'}:
                    if symbols and not quotes:
                        error = feed.status.get('quotes', {}).get('last_error') or 'Tushare 尚未返回可验证的实时盘口'
                        issue = ('quotes', _failure_kind(error))
                    elif not session.get('calendar_verified'):
                        error = session.get('error') or 'Tushare 交易日历尚未核实'
                        issue = ('calendar', _failure_kind(error))
                    if error:
                        session['open'] = False
                        session['phase'] = 'Tushare 数据尚未就绪，停止模拟撮合'
            except Exception as exc:
                error = str(exc)[:250]
                issue = (stage, _failure_kind(exc))
                session = {'open': False, 'phase': '行情或交易日历不可用，停止撮合',
                           'date': now.date().isoformat(), 'calendar_verified': False, 'next_open': None}
                session['expected_open'] = _market_expected_open(cfg['market'], session, datetime.now(SHANGHAI))
            now = datetime.now(SHANGHAI)
            _merge_council_status(ai, planner.describe())
            paused = Path(cfg['risk']['kill_switch_file']).exists()
            buy_ready = False
            session['buy_ready'] = False
            activation = public_activation(cfg)
            try:
                before = ledger.snapshot(quotes, now=datetime.now(SHANGHAI))
                calendar = getattr(feed, '_calendar', None) or {}
                if not calendar and hasattr(feed, '_read_cache'):
                    calendar = feed._read_cache('calendar.json') or {}
                activation = gate.evaluate(before, quotes, eligible, session, feed.status,
                                           calendar.get('dates', {}), ai, datetime.now(SHANGHAI))
                paused = Path(cfg['risk']['kill_switch_file']).exists()
                buy_ready = (error is None and not feed.status.get('universe', {}).get('cache_stale', True)
                             and activation.get('ready') is True)
                session['buy_ready'] = buy_ready
                session['research_target_date'] = activation.get('target_date')
                quote_hold = (error is None and not paused
                              and _quote_readiness_hold(activation, session, feed.status))
                current_held_ids = []
                for order in before.get('orders', []):
                    if order['status'] not in {'OPEN', 'PARTIALLY_FILLED'}:
                        continue
                    if paused or (error is None and order['side'] == 'BUY'
                                  and ((not buy_ready and not quote_hold) or order['symbol'] not in eligible)):
                        reason = ('kill_switch_active' if paused else
                                  'activation_not_ready' if not buy_ready and not quote_hold else 'instrument_not_eligible')
                        _write_now(ledger.cancel, order['order_id'], reason=reason)
                    elif quote_hold:
                        current_held_ids.append(order['order_id'])
                session['execution_hold'] = {'active': quote_hold, 'reason': 'quote_readiness_unavailable' if quote_hold else None,
                                             'order_ids': current_held_ids}
                if current_held_ids != held_order_ids:
                    _runner_event(root, 'orders_waiting_for_quotes' if current_held_ids else 'order_quote_wait_ended',
                                  datetime.now(SHANGHAI), order_ids=current_held_ids,
                                  previous_order_ids=held_order_ids, matching_ready=buy_ready and not paused)
                    held_order_ids = current_held_ids
                if error is None and not paused and not quote_hold:
                    _write_now(ledger.match, quotes, instruments, session_open=bool(session.get('open')) and not paused)
            except Exception as exc:
                error = '模拟委托或撮合已阻断：' + str(exc)[:180]
                issue = ('ledger', _failure_kind(exc))
                write_blocked = True
                session['open'] = False
                session['buy_ready'] = buy_ready = False
                session['phase'] = '委托或撮合检查未通过'
            now = datetime.now(SHANGHAI)
            snapshot = ledger.snapshot(quotes, now=now)
            if probe_future is not None and probe_future.done():
                try:
                    probe_future.result()
                    ai['verification_error'] = None
                except Exception:
                    ai['verification_error'] = 'AI轻量JSON验证未通过；已按配置退避，不产生委托'
                probe_future = None
            if future is None and probe_future is None:
                fingerprint = gate.reserve_ai_probe(ai, now)
                if fingerprint:
                    probe_future = pool.submit(verify_ai_connection, cfg, planner, fingerprint)
            if (not write_blocked and session.get('calendar_verified')
                    and now.strftime('%H:%M') >= cfg['market']['close_time']):
                try:
                    _write_now(ledger.end_session)
                except Exception as exc:
                    error = '收盘撤余已阻断：' + str(exc)[:180]
                    issue = ('end_session', _failure_kind(exc))
                    write_blocked = True
                    session['open'] = False
                    session['buy_ready'] = buy_ready = False
                    session['phase'] = '收盘撤余未完成'
                now = datetime.now(SHANGHAI)
                snapshot = ledger.snapshot(quotes, now=now)
            if future is not None and future.done():
                plan = None
                try:
                    plan = future.result()
                    from .council import validate_execution_plan
                    validate_execution_plan(plan, cfg)
                    _remember_council(ai, plan.get('council'))
                    elapsed = (now - datetime.fromisoformat(plan['asof'])).total_seconds()
                    if (elapsed < 0 or elapsed > cfg['strategy']['max_plan_age_seconds'] or not session.get('open') or paused
                            or activation.get('enabled') and not activation.get('ready')):
                        ai.update({'last_error': '计划返回时已过期、休市、暂停或数据验收失效，未提交委托', 'phase': '计划未执行'})
                    else:
                        accepted, rejected = [], []
                        for i, order in enumerate(plan['orders']):
                            try:
                                validate_execution_plan(plan, cfg)
                                symbol = order['symbol']
                                instrument = instruments.get(symbol)
                                if instrument is None:
                                    raise ValueError('证券已退出本轮允许范围')
                                if order['side'] == 'BUY' and (not buy_ready or symbol not in eligible):
                                    raise ValueError('证券已退出当前候选池，或成分资料过期')
                                intent = hashlib.sha256((plan['asof'] + str(i) + symbol).encode()).hexdigest()
                                result = _write_now(ledger.submit, order['side'], symbol, order['quantity'], order['limit_price'],
                                                    instrument, session_open=True, intent_id=intent)
                                if result.get('status') == 'REJECTED':
                                    rejected.append({'symbol': symbol, 'reason': result.get('reason')})
                                else:
                                    accepted.append(result)
                            except Exception as exc:
                                rejected.append({'symbol': order.get('symbol'), 'reason': str(exc)[:200]})
                        ai.update({'last_plan_at': plan['asof'], 'last_plan_summary': plan['summary'],
                                   'last_error': None, 'phase': '计划已风控处理',
                                   'accepted': accepted, 'rejected': rejected})
                    if isinstance(plan.get('council'), dict):
                        plan['council'].update(execution_status='risk_processed' if ai.get('last_error') is None else 'not_executed',
                                               execution_error=ai.get('last_error'))
                        _remember_council(ai, plan['council'])
                    atomic_json(root / 'plans' / (plan['asof'].replace(':', '-') + '.json'),
                                {**plan, 'execution': 'risk_processed' if ai.get('last_error') is None else 'not_executed',
                                 'accepted': ai.get('accepted', []) if ai.get('last_error') is None else [],
                                 'rejected': ai.get('rejected', []) if ai.get('last_error') is None else [],
                                 'execution_error': ai.get('last_error'), 'processed_at': now.isoformat()})
                except Exception as exc:
                    trace = getattr(exc, 'council', None) or (plan or {}).get('council')
                    if isinstance(trace, dict):
                        trace = {**trace, 'execution_status': 'not_executed', 'execution_error': str(exc)[:250]}
                        _remember_council(ai, trace)
                        atomic_json(root / 'plans' / ('failed-' + now.isoformat().replace(':', '-') + '.json'),
                                    {'asof': now.isoformat(), 'council': trace, 'orders': [],
                                     'execution': 'not_executed', 'execution_error': str(exc)[:250]})
                    ai.update({'last_error': str(exc)[:250], 'phase': 'AI计划失败，等待重试'})
                future = None
            if not session.get('open'):
                ai.update({'phase': '等待行情恢复' if error else '休市等待',
                           'next_run_at': None if error else session.get('next_open')})
            elif paused:
                ai.update({'phase': '账户已暂停'})
            elif not buy_ready:
                ai.update({'phase': '等待数据验收后自动启动' if activation.get('enabled') else '成分资料过期，暂停新AI计划',
                           'next_run_at': None})
            elif ai['enabled'] and ai.get('configured') and future is None and probe_future is None and time.time() - last_attempt >= cfg['strategy']['ai_interval_seconds']:
                if snapshot.get('reconciliation', {}).get('ok') and snapshot.get('account', {}).get('valuation_complete'):
                    last_attempt = time.time()
                    ai.update({'phase': 'AI 分析当前行情', 'last_attempt_epoch': last_attempt})
                    candidates = ({symbol: value for symbol, value in eligible.items()
                                   if symbol in activation.get('tradable_symbols', [])}
                                  if activation.get('enabled') else dict(eligible))
                    future = pool.submit(planner.propose, snapshot, dict(quotes), candidates, dict(session), now)
            elif ai['enabled'] and ai.get('configured'):
                # A temporary data gate must not leave an obsolete waiting
                # label after recovery or trigger a second plan before due.
                if future is not None:
                    ai['phase'] = 'AI 分析当前行情'
                elif probe_future is not None:
                    ai['phase'] = 'AI 连接验证中'
                elif ai.get('last_error'):
                    trace = (ai.get('council') or {}).get('latest_trace') or {}
                    ai['phase'] = '计划未执行' if trace.get('status') not in (None, 'failed') else 'AI计划失败，等待重试'
                else:
                    ai['phase'] = '计划已风控处理' if ai.get('last_plan_at') else '等待下一轮AI计划'
            if session.get('open') and ai['enabled'] and not paused and buy_ready:
                ai['next_run_at'] = datetime.fromtimestamp(last_attempt + cfg['strategy']['ai_interval_seconds'], SHANGHAI).isoformat()
            now = datetime.now(SHANGHAI)
            delay = _poll_delay(cfg['market'], session, now)
            expected_open = session['expected_open']
            if issue is None and expected_open and activation.get('status') == 'waiting_data':
                failed = {row.get('id') for row in activation.get('checks', []) if row.get('ok') is False}
                allowed = ('paper', 'calendar', 'research_products', 'pipeline', 'universe', 'minute',
                           'quotes', 'executable', 'session', 'ai', 'account')
                issue = ('activation', *(name for name in allowed if name in failed))
            name = None
            if issue and (issue != last_issue or expected_open and not was_expected_open):
                name = 'market_unavailable'
            elif not issue and session.get('open'):
                if last_issue:
                    name = 'market_recovered'
                elif not was_expected_open:
                    name = 'market_open_observed'
            if name:
                _runner_event(root, name, now, reasons=issue or last_issue or (), expected_open=expected_open,
                              session_open=bool(session.get('open')), calendar_verified=bool(session.get('calendar_verified')),
                              paused=paused, quote_count=len(quotes), universe_count=len(instruments), retry_seconds=delay)
            last_issue, was_expected_open = issue, expected_open
            raw = {'updated_at': now.isoformat(), 'market': {**session, 'source': cfg['market']['source_label'],
                   'provider': cfg['market']['provider'],
                   'quote_count': len(quotes), 'universe_count': len(instruments),
                   'asof': max((q.timestamp.isoformat() for q in quotes.values()), default=None),
                   'error': error, 'details': feed.status}, 'ai': ai,
                   'eligible_symbols': sorted(eligible) if error is None else [],
                   'quotes': [q.to_dict() for q in quotes.values()],
                   'universe': [asdict(v) for v in instruments.values()], 'paused': paused, 'activation': activation,
                   'execution_config_fingerprint': ledger.fingerprint}
            atomic_json(root / 'state.json', raw)
            if once:
                return public_state(cfg, raw, ledger.snapshot(quotes, now=now))
            time.sleep(delay)
    finally:
        pool.shutdown(wait=False, cancel_futures=True)
        lock.__exit__(None, None, None)
