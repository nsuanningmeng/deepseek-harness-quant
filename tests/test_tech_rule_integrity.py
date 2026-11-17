import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import yaml

from factors.opportunities import tech_pitch_v3 as tech


class TechRuleIntegrityTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        (self.root/'universe.json').write_text(json.dumps({'symbols': ['000001.SZ']}))
        cfg = {'cache_dir': str(self.root), 'analysis': {'scope': 'hs300', 'cache_dir': str(self.root),
                                                        'universe_file': str(self.root/'universe.json')}}
        self.config_file = self.root/'research.yaml'; self.config_file.write_text(yaml.safe_dump(cfg))
        env = patch.dict(os.environ, {'LWQUANT_RESEARCH_CONFIG': str(self.config_file),
                                      'LWQUANT_CACHE_DIR': str(self.root)})
        env.start(); self.addCleanup(env.stop)
        self.dates = pd.bdate_range('2020-01-01', periods=25).strftime('%Y-%m-%d')
        with sqlite3.connect(self.root/'bars.db') as con:
            con.execute('CREATE TABLE daily_bar(code TEXT,date TEXT,open REAL,high REAL,low REAL,close REAL,pct_chg REAL,volume REAL,turn REAL,is_st INTEGER,adjust TEXT)')
            for code in ('000001.SZ', '000002.SZ'):
                for adjust, multiplier in (('qfq', 1), ('none', 100)):
                    for i, day in enumerate(self.dates):
                        close = 10*(1.1**i)*multiplier
                        con.execute('INSERT INTO daily_bar VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                                    (code, day, close*.99, close*1.01, close*.98, close, 10., 100., 2., None, adjust))

    def test_recent_prices_are_single_adjustment_and_current_scope(self):
        frame = tech.load_recent()
        self.assertEqual(set(frame.code), {'000001.SZ'})
        self.assertFalse(frame.duplicated(['code', 'date']).any())
        self.assertEqual(len(frame), int(tech._cfg()['atr_n'])+1)
        self.assertAlmostEqual(frame.close.iloc[-1], 10*(1.1**24))

    def test_five_day_features_do_not_sum_all_atr_history(self):
        frame = tech.load_recent()
        result = tech._emotion_features(frame).iloc[0]
        self.assertEqual(result.limup_cnt, int(tech._cfg()['limup_days']))
        self.assertEqual(result.consec_max, int(tech._cfg()['consec_days']))
        self.assertEqual(result.st_known_days, 0)

    def test_missing_lhb_is_null_not_neutral_or_zero(self):
        self.assertIsNone(tech._dragon_tiger_score('000001.SZ'))

    def test_reviewed_302132_uses_shared_chinext_identity_and_existing_proxy(self):
        self.assertEqual(tech._board('302132.SZ')['board'], 'CHINEXT')
        self.assertEqual(tech._board('302132.SZ')['name'], '创业板')
        self.assertEqual(tech._board_pct('302132.SZ'), tech._board_pct('300114.SZ'))
        frame = pd.DataFrame({'code': ['302132.SZ', '302132.SZ'], 'date': self.dates[:2],
                              'pct_chg': [10., 20.], 'volume': [100., 100.], 'is_st': [None, None],
                              'high': [11., 12.], 'low': [10., 10.]})
        observed = tech._with_proxy(frame)
        self.assertEqual(observed['limit_up_proxy'].tolist(), [False, True])
        with self.assertRaises(ValueError):
            tech._board('302133.SZ')

    def test_atr_uses_actual_range_without_synthetic_price_floor(self):
        frame = pd.DataFrame({'code': ['000001.SZ']*21, 'date': self.dates[:21],
                              'high': [10.01]*21, 'low': [9.99]*21, 'close': [10.]*21, 'volume': [100.]*21})
        _, stop, atr = tech._stop_safety_score('000001.SZ', frame)
        self.assertAlmostEqual(atr, .02)
        self.assertAlmostEqual(stop, 9.96)
        self.assertEqual(tech._stop_safety_score('000001.SZ', frame.iloc[:5]), (None, None, None))

    def test_build_is_uncalibrated_and_cannot_auto_buy(self):
        with patch.object(tech, 'BASE', self.root), \
             patch.object(tech, '_name_map', return_value={'000001.SZ': ('sample', 'test')}), \
             patch.object(tech, '_report_map', return_value={}), patch.object(tech, 'valuations', return_value=pd.DataFrame()):
            output = tech.build()
        data = json.loads(output.read_text())
        self.assertFalse(data['calibrated'])
        self.assertFalse(data['auto_trade_allowed'])
        self.assertEqual(data['source']['symbols'], 1)
        self.assertEqual(data['source']['adjust'], 'qfq')
        self.assertEqual(len(data['entries']), 1)
        entry = data['entries'][0]
        self.assertIsNone(entry['score_breakdown']['龙虎榜'])
        self.assertFalse(entry['dragon_tiger']['available'])
        self.assertIsNone(entry['dragon_tiger']['score'])
        self.assertEqual(entry['tech_badges'], [])
        self.assertEqual(entry['confidence'], '未校准')
        self.assertFalse(entry['stop_plan']['auto_execute'])
        self.assertFalse(entry['auto_trade_allowed'])
        self.assertAlmostEqual(entry['score_coverage'], .9)
        for old_claim in ('1.53', '+2.07%', '5.1万'):
            self.assertNotIn(old_claim, entry['evidence'])

    def test_empty_real_window_is_valid_without_old_candidates(self):
        with sqlite3.connect(self.root/'bars.db') as con:
            con.execute('UPDATE daily_bar SET pct_chg=0')
        with patch.object(tech, 'BASE', self.root), \
             patch.object(tech, '_name_map', return_value={'000001.SZ': ('sample', 'test')}), \
             patch.object(tech, '_report_map', return_value={}), patch.object(tech, 'valuations', return_value=pd.DataFrame()):
            output = tech.build()
        data = json.loads(output.read_text())
        self.assertEqual(data['status'], 'empty_valid')
        self.assertEqual(data['entries'], [])
        self.assertEqual(data['source']['rows'], 21)


if __name__ == '__main__':
    unittest.main()
