"""Presentation-only readiness; never grants permission to plan or trade."""
from datetime import datetime


def market_wait_status(state, now):
    """Recognize a verified closed session without hiding missing prerequisites."""
    activation, market = state.get('activation') or {}, state.get('market') or {}
    if not isinstance(activation, dict) or not isinstance(market, dict):
        return None
    ai = state.get('ai') or {}
    council = (ai.get('council') or {}) if isinstance(ai, dict) else {}
    snapshot = state.get('account_snapshot') or {}
    if (not isinstance(ai, dict) or ai.get('configured') is False or ai.get('error')
            or not isinstance(council, dict)
            or council.get('enabled') and (council.get('configured') is not True or council.get('error'))):
        return None
    if not isinstance(snapshot, dict):
        return None
    if ('reconciliation' in snapshot and (not isinstance(snapshot['reconciliation'], dict)
                                         or snapshot['reconciliation'].get('ok') is not True)
            or isinstance(snapshot.get('account'), dict) and snapshot['account'].get('valuation_complete') is False):
        return None
    base = {'paper', 'calendar', 'research_products', 'pipeline', 'universe', 'minute', 'ai', 'account'}
    expected = base | {'quotes', 'executable', 'session'}
    checks = activation.get('checks')
    if (activation.get('enabled') is not True or activation.get('armed') is not True
            or activation.get('ready') is not False or activation.get('stop_owned') is not True
            or activation.get('status') != 'waiting_data'
            or market.get('open') is not False or market.get('calendar_verified') is not True
            or market.get('heartbeat_stale') is not False or market.get('error')
            or not isinstance(checks, list) or len(checks) != len(expected)):
        return None
    indexed = {}
    for row in checks:
        if (not isinstance(row, dict) or not isinstance(row.get('id'), str) or row.get('id') not in expected
                or row['id'] in indexed or type(row.get('ok')) is not bool):
            return None
        indexed[row['id']] = row['ok']
    if not all(indexed[key] for key in base) or indexed['session'] is not False:
        return None
    try:
        next_open = datetime.fromisoformat(market['next_open'])
        if next_open.utcoffset() is None or now.utcoffset() is None or next_open <= now:
            return None
    except (KeyError, ValueError, TypeError, AttributeError):
        return None
    return {'state': 'waiting_market', 'ready': False, 'initialized': True,
            'reason': '基础数据、AI与账户已核验，等待下一交易时段再核验实时行情',
            'next_open': next_open.isoformat()}


def classify_market_wait(path, body):
    """Recognize the display state only when its HTTP body is not contradictory."""
    if not isinstance(body, dict):
        return None
    readiness = body.get('readiness') if isinstance(body.get('readiness'), dict) else {}
    if (readiness.get('state') or body.get('state')) != 'waiting_market':
        return None
    proof = readiness or body
    def current_errors(value, depth=0):
        if not isinstance(value, dict) or depth > 2:
            return []
        result = [str(value[key]) for key in ('error', 'err') if value.get(key)]
        for key, nested in value.items():
            if key not in ('last_check', 'history', 'orders', 'fills') and isinstance(nested, dict):
                result.extend(current_errors(nested, depth + 1))
        return result
    errors = current_errors(body)
    if errors or body.get('ok') is False:
        return {'state': 'failed', 'initialized': False, 'reason': '；'.join(errors) or '接口明确报告失败，不能判作正常闭市等待'}
    if body.get('stale'):
        return {'state': 'stale', 'initialized': False, 'reason': '当前返回旧快照，不能确认仅在等待开市'}
    if (path not in ('/api/paperlive/state', '/api/live/realtime') or proof.get('ready') is not False
            or proof.get('initialized') is not True or body.get('ready') is True
            or body.get('initialized') is False):
        return {'state': 'unknown', 'initialized': False, 'reason': '等待开市状态与接口范围或就绪字段不一致'}
    return {'state': 'waiting_market', 'initialized': True,
            'reason': proof.get('reason') or '等待下一交易时段', 'next_open': proof.get('next_open')}
