"""Research financial ratios reach human/AI summaries as correctly scaled percentages."""
from unittest import TestCase
from unittest.mock import patch
import math
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory

import pandas as pd

from strategy import ranking_v2


class RankingFinancialUnitTests(TestCase):
    def test_published_growth_ratio_is_converted_once_without_rescaling_roe(self):
        source = pd.DataFrame([
            {'code':'600001.SH', 'roe':.24995, 'sq_net_yoy':.3},
            {'code':'600002.SH', 'roe':-.032, 'sq_net_yoy':-.25},
            {'code':'600003.SH', 'roe':None, 'sq_net_yoy':None},
            {'code':'600004.SH', 'roe':0, 'sq_net_yoy':0},
        ])
        with patch.object(ranking_v2, 'latest_published', return_value=source.copy()) as published:
            actual = ranking_v2.load_fundamentals('2026-11-12')
        self.assertEqual(published.call_args.args[1:], ('finance_report','2026-11-12'))
        self.assertAlmostEqual(actual.loc['600001','roe'], .24995)
        self.assertAlmostEqual(actual.loc['600001','nyoy'], 30)
        self.assertAlmostEqual(actual.loc['600002','nyoy'], -25)
        self.assertTrue(pd.isna(actual.loc['600003','nyoy']))
        self.assertEqual(actual.loc['600004','nyoy'], 0)
        self.assertAlmostEqual(source.loc[0,'sq_net_yoy'], .3)

    def test_research_reason_uses_percentages_and_real_quality_threshold(self):
        source = pd.DataFrame([{'code':'600001.SH', 'roe':.24995, 'sq_net_yoy':.3}])
        with patch.object(ranking_v2, 'latest_published', return_value=source):
            row = ranking_v2.load_fundamentals('2026-11-12').loc['600001'].copy()
        row['score'] = 70
        scores = pd.Series({'quality':.8, 'growth':.6, 'lowvol':.7, 'reversal':.7})
        reason = ranking_v2.build_reason(row, scores, {'name':'Synthetic security'})
        self.assertIn('ROE 25.0%', reason)
        self.assertIn('净利润同比 +30%', reason)
        self.assertNotIn('2499', reason)
        row['roe'], row['nyoy'] = .049, 19
        reason = ranking_v2.build_reason(row, scores, {'name':'Synthetic security'})
        self.assertNotIn('质量良好', reason)
        self.assertNotIn('成长强劲', reason)

    def test_growth_extremes_retain_true_percentages_at_the_read_boundary(self):
        source = pd.DataFrame([
            {'code':'600001.SH','roe':.2,'sq_net_yoy':7.3},
            {'code':'600002.SH','roe':.2,'sq_net_yoy':-7.5},
            {'code':'600003.SH','roe':.2,'sq_net_yoy':-12},
        ])
        with patch.object(ranking_v2, 'latest_published', return_value=source):
            rows = ranking_v2.load_fundamentals('2026-11-12')
        self.assertEqual(rows['nyoy'].tolist(), [730,-750,-1200])

    def test_rank_preserves_true_growth_in_output_and_caps_only_the_score(self):
        codes = [f'60000{i}.SH' for i in range(1,6)]
        dates = pd.bdate_range(end='2026-11-12', periods=300).strftime('%Y-%m-%d')
        prices = pd.DataFrame({code:[100*(1+.001*day+.015*math.sin(day/(5+i)))
                                    for day in range(len(dates))] for i,code in enumerate(codes)}, index=dates)
        volumes = prices*0+1000000
        source = pd.DataFrame([{'code':code,'roe':.2,'sq_net_yoy':growth,'revenue':10,'net_profit':1}
                               for code,growth in zip(codes,[7.3,12,.1,-7.5,-12])])
        basic = pd.DataFrame({'name':['Synthetic']*5,'industry':['Synthetic industry']*5}, index=codes)
        with TemporaryDirectory() as tmp:
            database = Path(tmp)/'bars.db'
            with sqlite3.connect(database) as con:
                con.execute('CREATE TABLE daily_bar(code TEXT,date TEXT,adjust TEXT,is_st INTEGER)')
                con.executemany('INSERT INTO daily_bar VALUES(?,?,?,?)',
                                [(code,'2026-11-12','qfq',0) for code in codes])
            with (patch.object(ranking_v2,'load_close_panel',return_value=(prices,volumes,prices*1.02,prices*.98)),
                  patch.object(ranking_v2,'latest_published',return_value=source),
                  patch.object(ranking_v2,'load_basic',return_value=basic),
                  patch.object(ranking_v2,'research_config',return_value={'min_history_days':300}),
                  patch.object(ranking_v2,'scope_metadata',return_value={'scope':'synthetic'}),
                  patch.object(ranking_v2,'BARS_DB',database),
                  patch.object(ranking_v2,'MV_CSV',Path(tmp)/'absent.csv')):
                result = ranking_v2.rank('2026-11-12',n=5)
        output = {row['code']:row for row in result['top']}
        self.assertEqual(set(output), set(codes[:3]), 'Negative extreme growth still fails the existing positive-growth filter')
        for code,actual_growth in zip(codes[:2],[730,1200]):
            self.assertEqual(output[code]['factors']['nyoy'], actual_growth)
            self.assertIn(f'净利润同比 +{actual_growth}%', output[code]['reason'])
            self.assertNotIn('同比 +300%', output[code]['reason'])
            self.assertEqual(output[code]['factors']['growth_rank'], .9,
                             'Both high extremes remain tied after the existing +/-300 scoring bound')
        self.assertEqual(output[codes[2]]['factors']['growth_rank'], .6)
