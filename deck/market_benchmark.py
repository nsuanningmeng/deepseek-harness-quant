"""Versioned market comparisons; page reads never wait for a full daily-bar scan."""
import fcntl
from contextlib import closing
import math
import os
from pathlib import Path
import sqlite3
import statistics

from deck.background_snapshot import BackgroundSnapshot
from deck.daily_status import BASE, databases, source_version

_snapshot = BackgroundSnapshot('market-benchmark', retry_seconds=30)


def _runtime_directory():
    import yaml
    config_path = Path(os.environ.get('LWQUANT_RESEARCH_CONFIG', BASE/'config/research.yaml'))
    cfg = yaml.safe_load(config_path.read_text()) if config_path.is_file() else {}
    runtime = Path((cfg or {}).get('runtime_dir', 'runtime/research'))
    if not runtime.is_absolute():
        runtime = BASE/runtime
    return runtime


def _writer_active(runtime=None):
    """Inspect existing locks without creating or changing any runtime files."""
    runtime = _runtime_directory() if runtime is None else runtime
    for name in ('update.lock', 'bootstrap.lock', 'pipeline.lock'):
        try:
            with (runtime/name).open('rb') as handle:
                try:
                    fcntl.flock(handle, fcntl.LOCK_SH | fcntl.LOCK_NB)
                except BlockingIOError:
                    return True
                finally:
                    fcntl.flock(handle, fcntl.LOCK_UN)
        except FileNotFoundError:
            pass
    return False


def _price_query(con, days):
    """Skip historical rows using existing symbol indexes, without changing a DB.

    The real cache has non-null codes and code-leading indexes. Enumerating its
    complete code domain with MIN/range seeks includes old and delisted symbols;
    it does not replace the market with a current index-membership list.
    """
    suffix = ' WHERE adjust=? AND date IN (' + ','.join('?' for _ in days) + ')'
    fallback = 'SELECT date,code,close FROM daily_bar' + suffix
    columns = {row[1]: row for row in con.execute('PRAGMA table_info(daily_bar)')}
    if not columns.get('code') or columns['code'][3] != 1:
        # NULL codes had dictionary semantics in the legacy scan; preserve them.
        return fallback
    indexed = False
    for row in con.execute('PRAGMA index_list(daily_bar)'):
        if row[4]:
            continue  # A partial index cannot enumerate the complete code domain.
        name = '"' + row[1].replace('"', '""') + '"'
        parts = con.execute('PRAGMA index_xinfo(' + name + ')').fetchall()
        if parts and parts[0][2] == 'code' and parts[0][4] == 'BINARY':
            indexed = True
            break
    if not indexed:
        return fallback
    return (
        'WITH RECURSIVE symbols(code) AS ('
        'SELECT MIN(code) FROM daily_bar UNION ALL '
        'SELECT (SELECT MIN(code) FROM daily_bar WHERE code>symbols.code) '
        'FROM symbols WHERE code IS NOT NULL) '
        'SELECT b.date,b.code,b.close FROM symbols s CROSS JOIN daily_bar b '
        'WHERE b.code=s.code AND b.adjust=? AND b.date IN (' + ','.join('?' for _ in days) + ')'
    )


def _calculate(paths, pairs, version):
    runtime = _runtime_directory()
    if _writer_active(runtime):
        raise RuntimeError('行情缓存正被后台任务使用，市场对照稍后核验')
    if not paths:
        raise ValueError('尚无真实日线数据库')
    days = sorted({day for pair in pairs for day in pair})
    prices = {day: {} for day in days}
    for path in paths:
        with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro', uri=True, timeout=.2)) as con:
            # Abort promptly if an updater acquires its lock during this read.
            # One indexed read covers all requested dates; successful versions are reused.
            con.set_progress_handler(lambda: int(_writer_active(runtime)), 100000)
            try:
                query = _price_query(con, days)
                for day, code, close in con.execute(query, ('qfq', *days)):
                    if isinstance(close, (int, float)) and math.isfinite(close) and close > 0:
                        prices[day][code] = close
            finally:
                con.set_progress_handler(None, 0)
    if source_version(paths) != version or _writer_active(runtime):
        raise RuntimeError('日线版本在核验期间变化，等待新版本重新计算')
    out = {}
    for first, last in pairs:
        a, b = prices[first], prices[last]
        returns = [b[code]/a[code]-1 for code in a.keys() & b.keys()]
        out[first+'|'+last] = {'value': round(statistics.median(returns), 4) if returns else None,
                              'matched_symbols': len(returns)}
    return out


def public_state(pairs):
    pairs = tuple(sorted(set(pairs)))
    if not pairs:
        return {'state': 'pending_history', 'ready': False, 'loading': False, 'values': {},
                'reason': '尚无到期 T+1 样本，无需市场对照'}
    if _writer_active():
        return {'state': 'waiting_core_update', 'ready': False, 'loading': False, 'values': {},
                'reason': '行情缓存正被后台任务使用，市场对照稍后核验'}
    paths = databases()
    version = source_version(paths)
    result = _snapshot.read((version, pairs), lambda: _calculate(paths, pairs, version))
    if not result['current']:
        return {'state': result['state'], 'ready': False, 'loading': result['loading'], 'values': {},
                'reason': '全市场对照正在后台计算，完成后自动显示' if result['loading'] else '全市场对照核验失败，稍后重试',
                'error': result['error']}
    values = result['data']
    ready = all(row['value'] is not None for row in values.values())
    return {'state': 'ready' if ready else 'missing_data', 'ready': ready, 'loading': False,
            'values': values, 'checked_at': result['updated_at'],
            'reason': '按同一日线版本逐股对齐，计算全市场前复权收益中位数' if ready else '对照日期缺少可对齐的有效收盘价'}
