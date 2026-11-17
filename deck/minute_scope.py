"""Bind read-only minute queries to one published immutable job's actual scope."""
from datetime import date
import hashlib
import re


def query_scope(job):
    symbols = job.get('symbols')
    if (not job.get('job_id') or not isinstance(symbols, list) or not symbols
            or len(symbols) != job.get('expected_symbols')
            or any(not isinstance(code, str) or not re.fullmatch(r'\d{6}\.(SH|SZ)', code) for code in symbols)
            or len(set(symbols)) != len(symbols)):
        raise ValueError('分钟任务的实际股票范围尚未核验')
    symbols = tuple(sorted(symbols))
    fingerprint = hashlib.sha256('\n'.join(symbols).encode()).hexdigest()
    if fingerprint != job.get('universe_fingerprint'):
        raise ValueError('分钟任务股票范围与指纹不一致')
    start, end = job.get('start_date'), job.get('target_date')
    if (not isinstance(start, str) or not isinstance(end, str)
            or date.fromisoformat(start).isoformat() != start
            or date.fromisoformat(end).isoformat() != end or start > end):
        raise ValueError('分钟任务固定日期范围无效')
    return (job['job_id'], start, end, fingerprint, symbols)


def sql_filter(scope):
    _, start, end, _, symbols = scope
    where = 'trade_time>=? AND trade_time<=? AND code IN (' + ','.join('?' for _ in symbols) + ')'
    return where, (start + ' 00:00:00', end + ' 23:59:59', *symbols)
