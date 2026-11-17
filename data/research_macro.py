"""Initialize the macro inputs using real published data; never fill missing scores."""
from datetime import datetime
import json
from pathlib import Path
import sqlite3
import sys

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
from data.cache import CACHE_DIR
from data.fetcher_tushare import _pro, _call
from autotrade.monitoring import atomic_json


def run():
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    config_path = ROOT / 'config/macro.yaml'
    import yaml
    cfg = yaml.safe_load(config_path.read_text())
    checks = []
    now = datetime.now()
    sf = _call(_pro().sf_month, start_m=str(cfg['start_month']), end_m=now.strftime('%Y%m'))
    if sf.empty or not {'month', 'inc_month'} <= set(sf):
        raise ValueError('社融数据未返回完整字段')
    rows = [(str(r.month), float(r.inc_month)) for r in sf.itertuples() if pd.notna(r.inc_month)]
    if len(rows) < 36:
        raise ValueError('社融历史不足36个月')
    with sqlite3.connect(CACHE_DIR / 'macro.db') as con:
        con.execute('CREATE TABLE IF NOT EXISTS social_finance(month TEXT PRIMARY KEY,sf_increment REAL)')
        con.executemany('INSERT OR REPLACE INTO social_finance VALUES (?,?)', rows)
    checks.append({'id': 'social_finance', 'source': 'tushare:sf_month', 'rows': len(rows),
                   'data_date': max(r[0] for r in rows), 'unit': '亿元', 'ok': True})

    # yc_cb requires a separate permission. The original macro module uses this
    # public historical yield source; it never substitutes live stock quotations.
    import akshare as ak
    yields = ak.bond_zh_us_rate(start_date=str(cfg['yield_start']))
    required = ['日期', '中国国债收益率10年', '中国国债收益率2年']
    if not set(required) <= set(yields):
        raise ValueError('国债收益率字段不完整')
    yields = yields.dropna(subset=required)
    rows = [(str(r['日期']), float(r['中国国债收益率10年']), float(r['中国国债收益率2年']))
            for r in yields.to_dict('records')]
    if len(rows) < 250:
        raise ValueError('国债收益率历史不足')
    with sqlite3.connect(CACHE_DIR / 'macro.db') as con:
        con.execute('CREATE TABLE IF NOT EXISTS bond_yield(date TEXT PRIMARY KEY,y10 REAL,y2 REAL)')
        con.executemany('INSERT OR REPLACE INTO bond_yield VALUES (?,?,?)', rows)
    checks.append({'id': 'bond_yield', 'source': cfg['yield_source'], 'rows': len(rows),
                   'data_date': max(r[0] for r in rows), 'unit': '%', 'ok': True})

    from factors.policy.epu_fetcher import init_db, sync, POLICY_DIR
    con = init_db()
    try:
        sync(con)
    finally:
        con.close()
    epu = json.loads((POLICY_DIR / 'source.json').read_text())
    checks.append({'id': 'epu', 'source': epu['source'], 'rows': epu['rows'],
                   'data_date': epu['latest_month'], 'ok': True})
    result = {'ok': True, 'generated_at': now.isoformat(), 'checks': checks,
              'note': '宏观按供应方发布频率更新；保存的是当前公开历史版本，不宣称无修订PIT历史。'}
    atomic_json(ROOT / 'runtime/research/macro-manifest.json', result)
    print(json.dumps(result, ensure_ascii=False))
    return result


if __name__ == '__main__':
    run()
