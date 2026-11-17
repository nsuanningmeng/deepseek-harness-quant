import json
import hashlib
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd
import yaml

from etf import etf_map as etf
from data import sector_research as sector

ZERO = {'commission_rate': 0., 'sell_tax_rate': 0., 'slippage_rate': 0.}


def bars(prices, dates=None):
    dates = dates if dates is not None else pd.bdate_range('2020-01-01', periods=len(next(iter(prices.values()))))
    rows = []
    for code, values in prices.items():
        for day, value in zip(dates, values):
            op, close = value if isinstance(value, tuple) else (value, value)
            rows.append({'code': code, 'date': day, 'open': op, 'close': close,
                         'high': max(op, close)+1, 'low': min(op, close)-1,
                         'volume': 100., 'turn': 1., 'is_st': 0})
    return pd.DataFrame(rows)


class ETFAndSectorTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        cfg = yaml.safe_load((Path(__file__).parents[1]/'config/research_execution.yaml').read_text())
        self.cfg = cfg
        self.config_file = self.root/'execution.yaml'
        self.config_file.write_text(yaml.safe_dump(cfg))
        env = patch.dict(os.environ, {'LWQUANT_EXECUTION_CONFIG': str(self.config_file)})
        env.start(); self.addCleanup(env.stop)

    def test_next_open_does_not_collect_signal_close_to_open_gap(self):
        frame = bars({'000001.SZ': [10., 20., 22., 22.]}); days = sorted(frame.date.unique())
        result = etf.simulate_targets(frame, [{'entry': days[1], 'exit': days[3], 'weights': {'000001.SZ': 1.}}], ZERO)
        self.assertAlmostEqual(result.ret.iloc[0], 0.)
        self.assertAlmostEqual(result.nav.iloc[-1], 1.1)

    def test_held_overnight_gap_is_earned_before_selling_at_open(self):
        frame = bars({'000001.SZ': [10., 10., 10., (20., 30.)]}); days = sorted(frame.date.unique())
        result = etf.simulate_targets(frame, [{'entry': days[1], 'exit': days[3], 'weights': {'000001.SZ': 1.}}], ZERO)
        self.assertAlmostEqual(result.nav.iloc[-1], 2.)  # Sold at 20; not 10, and not the later 30 close.

    def test_proportional_fees_and_both_sides_slippage_are_charged(self):
        frame = bars({'000001.SZ': [100.] * 4}); days = sorted(frame.date.unique())
        fees = {'commission_rate': .01, 'sell_tax_rate': .02, 'slippage_rate': .01}
        result = etf.simulate_targets(frame, [{'entry': days[1], 'exit': days[3], 'weights': {'000001.SZ': 1.}}], fees)
        expected = (1-.01)*(1-.01-.02)/((1+.01)*(1+.01))
        self.assertAlmostEqual(result.nav.iloc[-1], expected)

    def test_share_holdings_do_not_implicitly_equal_weight_every_day(self):
        frame = bars({'000001.SZ': [10., 10., 20., 20., 20.],
                      '000002.SZ': [10., 10., 10., 20., 20.]})
        days = sorted(frame.date.unique())
        result = etf.simulate_targets(frame, [{'entry': days[1], 'exit': days[4],
                                               'weights': {'000001.SZ': .5, '000002.SZ': .5}}], ZERO)
        self.assertAlmostEqual(result.nav.iloc[-1], 2.)
        self.assertAlmostEqual(result.ret.iloc[2], 1/3)

    def test_unbuyable_target_weight_stays_in_cash(self):
        frame = bars({'000001.SZ': [10.] * 4, '000002.SZ': [10., 10., 20., 20.]})
        days = sorted(frame.date.unique())
        mask = (frame.code == '000001.SZ') & (frame.date == days[1])
        frame.loc[mask, ['high', 'low']] = 10.
        result = etf.simulate_targets(frame, [{'entry': days[1], 'exit': days[3],
                                               'weights': {'000001.SZ': .5, '000002.SZ': .5}}], ZERO)
        self.assertAlmostEqual(result.nav.iloc[-1], 1.5)
        self.assertEqual(len(result.attrs['rejected_entries']), 1)

    def test_missing_held_mark_is_nan_and_untradeable_exit_remains_held(self):
        frame = bars({'000001.SZ': [10.] * 4}); days = sorted(frame.date.unique())
        schedule = [{'entry': days[1], 'exit': days[3], 'weights': {'000001.SZ': 1.}}]
        frame.loc[frame.date == days[2], 'close'] = np.nan
        result = etf.simulate_targets(frame, schedule, ZERO)
        self.assertTrue(pd.isna(result.loc[days[2], 'nav']))
        self.assertTrue(pd.isna(result.loc[days[3], 'ret']))
        self.assertEqual(result.attrs['valuation_gaps'][0]['symbols'], ['000001.SZ'])
        self.assertIsNone(etf._metrics(result)['max_dd'])
        self.assertIsNone(etf._metrics(result)['sharpe'])
        frame.loc[frame.date == days[2], 'close'] = 10.
        frame.loc[frame.date == days[3], ['high', 'low']] = 10.
        result = etf.simulate_targets(frame, schedule, ZERO)
        self.assertFalse(result.attrs['closed'])
        self.assertEqual(result.attrs['unresolved_holdings'], ['000001.SZ'])
        self.assertIsNone(etf._metrics(result)['annual'])

    def test_non_session_exit_cannot_masquerade_as_mature(self):
        frame = bars({'000001.SZ': [10.] * 4}); days = sorted(frame.date.unique())
        result = etf.simulate_targets(frame, [{'entry': days[1], 'exit': days[-1]+pd.Timedelta(days=10),
                                              'weights': {'000001.SZ': 1.}}], ZERO)
        self.assertFalse(result.attrs['closed'])
        self.assertIsNone(etf._metrics(result)['annual'])

    def test_blocked_exit_keeps_real_shares_and_sells_at_first_tradeable_open(self):
        frame = bars({'000001.SZ': [10., 10., 9., 8., 7., 99.]})
        days = sorted(frame.date.unique())
        frame.loc[frame.date == days[3], ['high', 'low']] = 8.
        curve = etf.simulate_targets(frame, [{'entry': days[1], 'exit': days[3],
                                              'weights': {'000001.SZ': 1.}}], ZERO)
        self.assertAlmostEqual(curve.loc[days[3], 'nav'], .8)
        self.assertAlmostEqual(curve.nav.iloc[-1], .7)
        self.assertEqual(curve.index[-1], days[4])
        self.assertTrue(curve.attrs['closed'])
        self.assertEqual(curve.attrs['delayed_exits'], [{'code': '000001.SZ', 'requested': str(pd.Timestamp(days[3]).date()),
                                                       'executed': str(pd.Timestamp(days[4]).date()), 'actual_open': 7.}])

    def test_cash_blocked_by_old_shares_delays_next_entry_without_earning_its_gap(self):
        frame = bars({'000001.SZ': [10., 10., 9., 8., 7., 7.],
                      '000002.SZ': [10., 10., 10., 10., 20., 22.]})
        days = sorted(frame.date.unique())
        frame.loc[(frame.code == '000001.SZ') & (frame.date == days[3]), ['high', 'low']] = 8.
        schedules = [{'entry': days[1], 'exit': days[3], 'weights': {'000001.SZ': 1.}},
                     {'entry': days[3], 'exit': days[5], 'weights': {'000002.SZ': 1.}}]
        curve = etf.simulate_targets(frame, schedules, ZERO)
        self.assertAlmostEqual(curve.nav.iloc[-1], .77)  # .7 realized old basket * (22 / actual 20 open).
        self.assertEqual(curve.attrs['delayed_entries'], [{'requested': str(pd.Timestamp(days[3]).date()),
                                                         'executed': str(pd.Timestamp(days[4]).date())}])

    def test_turnover_strategy_excludes_trailing_unmatured_cohort(self):
        self.cfg['etf'].update({'turnover_window': 2, 'rebalance_days': 3, 'top_n': 1})
        self.cfg['stock_fees'] = ZERO; self.config_file.write_text(yaml.safe_dump(self.cfg))
        frame = bars({'000001.SZ': [10.] * 10})
        frame['date'] = frame.date.dt.strftime('%Y-%m-%d')
        with patch.object(etf, 'read_bars', return_value=frame), patch.object(etf, 'scope_metadata', return_value={'scope': 'test'}):
            result = etf.load_turn_low_nav()
        self.assertEqual(result.index[-1], pd.Timestamp(frame.date.unique()[8]))
        self.assertEqual(result.attrs['pending_signals'], [str(frame.date.unique()[7])])
        self.assertEqual(result.attrs['schedules'], 2)

    def test_etf_main_fits_only_training_and_executes_later_holdout(self):
        self.cfg['etf'].update({'min_correlation_rows': 3, 'rolling_window': 2,
                                'min_etf_rows': 3, 'min_oos_days': 3, 'train_fraction': .6})
        self.cfg['etf_fees'] = ZERO
        self.config_file.write_text(yaml.safe_dump(self.cfg))
        dates = pd.bdate_range('2020-01-01', periods=12)
        returns = pd.Series([.01, -.005, .02, -.01, .015, .003, -.007, .02, -.008, .004, .01, -.003], index=dates)
        strategy = pd.DataFrame({'ret': returns, 'nav': (1+returns).cumprod(), 'cash': 0.})
        strategy.attrs.update(source_asof='2020-01-16', simulation_asof='2020-01-16')
        close = 100*(1+returns).cumprod()
        df = pd.DataFrame({'code': '510300.SH', 'open': close*.999, 'high': close+1,
                           'low': close-1, 'close': close, 'volume': 100., 'amount': 1e8,
                           'ret': returns}, index=dates)
        df.index.name = 'date'
        with patch.object(etf, 'OUT_PATH', self.root/'etf_map.json'), \
             patch.object(etf, 'load_turn_low_nav', return_value=strategy), \
             patch.object(etf, '_load_pool', return_value=[('510300', 'sample', 'test')]), \
             patch.object(etf, 'etf_ret_series', return_value=df), \
             patch.object(etf, 'scope_metadata', return_value={'scope': 'test'}), \
             patch.object(etf, 'corr_stats', wraps=etf.corr_stats) as fitted:
            etf.main()
        result = json.loads((self.root/'etf_map.json').read_text())
        cutoff = pd.Timestamp(result['training_end'])
        self.assertEqual(fitted.call_args.args[0].index.max(), cutoff)
        self.assertEqual(fitted.call_args.args[1].index.max(), cutoff)
        holdout = result['allocation']['equal']
        self.assertEqual(holdout['kind'], 'chronological_holdout')
        self.assertGreater(pd.Timestamp(holdout['start']), cutoff)
        self.assertFalse(result['auto_trade_allowed'])

    def test_sector_forward_horizon_uses_next_open_and_matching_benchmark_dates(self):
        frame = bars({'000001.SZ': [10., 20., 22., 100.]})
        p = etf._panels(frame); dates = p['close'].index
        bench_close = pd.Series([50., 100., 110., 110.], index=dates)
        bench_open = pd.Series([50., 100., 110., 110.], index=dates)
        result = sector._fwd_winrate(p['close'], bench_close, [('000001.SZ', dates[0])], {'000001': 'X'}, 2,
                                     opens=p['open'], benchmark_opens=bench_open, bars=frame, fees=ZERO)
        self.assertAlmostEqual(result['X'][0], 0.)  # 22/20 - 110/100, not 22/10 or 100/20.

    def test_unmatured_and_unexecutable_sector_outcomes_remain_explicit(self):
        frame = bars({'000001.SZ': [10.] * 4}); p = etf._panels(frame); dates = p['close'].index
        frame.loc[frame.date == dates[2], ['high', 'low']] = 10.
        _, diag = sector._fwd_winrate(p['close'], p['close']['000001.SZ'],
                                     [('000001.SZ', dates[0]), ('000001.SZ', dates[2])], {'000001': 'X'}, 2,
                                     opens=p['open'], benchmark_opens=p['open']['000001.SZ'], bars=frame,
                                     fees=ZERO, return_diagnostics=True)
        self.assertEqual(diag['X']['exit_untradeable'], 1)
        self.assertEqual(diag['X']['unmatured'], 1)
        self.assertEqual(diag['X']['completed'], 0)

    def test_monthly_sampling_does_not_turn_daily_windows_into_months(self):
        index = pd.bdate_range('2020-01-01', periods=300)
        prices = pd.DataFrame({'000001.SZ': np.arange(300)+100.}, index=index)
        _, momentum, high, _ = sector._monthly_factors(prices)
        self.assertAlmostEqual(momentum.iloc[-1, 0], 399/379-1)
        self.assertAlmostEqual(high.iloc[-1, 0], 0.)

    def test_no_real_sector_evidence_does_not_restore_old_success_rates(self):
        with patch.object(sector, 'BASE', self.root):
            result = sector._compute_sector_strong_factors('X')
        self.assertTrue(all(r['winrate'] is None and not r['ok'] for r in result))
        with patch.object(sector, '_load_sector_detail', return_value={'000001.SZ': ('X', 'test')}):
            pitch = sector._compute_sector_pitch('X', strong_factors=result)
        self.assertEqual(pitch['n_pitch'], 0)
        self.assertEqual(pitch['status'], 'awaiting_evidence')
        self.assertFalse(pitch['auto_trade_allowed'])

    def test_current_stock_diagnostic_uses_actual_sector_evidence(self):
        row = {'close': 10., 'drawdown_60d': -.1, 'mom20': .1, 'near_high_250': -.05,
               'pb': 2., 'roe': .1, 'sq_nyoy': .3}
        proof = [{'otype': 'event', 'winrate': 13.7, 'excess': -2.1, 'n': 45, 'ok': True}]
        with patch.object(sector, 'universe', return_value={'000001.SZ'}), \
             patch.object(sector, '_current_panel', return_value=(pd.DataFrame([row], index=['000001.SZ']), '2020-01-01', {'event': lambda r: True})), \
             patch.object(sector, '_load_sector_detail', return_value={'000001.SZ': ('X', 'test')}), \
             patch.object(sector, '_names', return_value={'000001.SZ': 'sample'}), \
             patch.object(sector, '_compute_sector_strong_factors', return_value=proof):
            result = sector._compute_stock_factors('000001.SZ')
        hit = next(r for r in result['hits'] if r['otype'] == 'event')
        self.assertEqual(hit['winrate'], 13.7)
        self.assertTrue(hit['met'])
        self.assertTrue(hit['evidence_ready'])
        self.assertTrue(all(r['winrate'] is None for r in result['hits'] if r['otype'] != 'event'))

    def test_short_timing_history_does_not_report_ready(self):
        with patch.object(sector, '_daily', return_value=pd.DataFrame({'000001.SZ': range(10)})), \
             patch.object(sector, '_load_sector_detail', return_value={'000001.SZ': ('X', 'test')}):
            result = sector._compute_sector_timing('X')
        self.assertFalse(result['ok'])

    def test_sector_batch_matches_per_sector_without_future_exit_leakage(self):
        self.cfg['sector'].update(horizon_sessions=2, min_samples=1)
        self.cfg['stock_fees'] = ZERO
        self.config_file.write_text(yaml.safe_dump(self.cfg))
        frame = bars({'000001.SZ': [10., 20., 22., 1000.], '000002.SZ': [10., 10., 9., 2000.]})
        panels = etf._panels(frame); dates = panels['close'].index
        benchmark = pd.DataFrame({'open': 100., 'close': 100.}, index=dates)
        calibration = {'observations': [{'code': code, 'otype': 'event', 'horizon_months': 1,
                                         'signal_date': str(dates[0].date())} for code in ('000001.SZ', '000002.SZ')]}
        mapping = {'000001': 'X', '000002': 'Y'}
        result = sector._precompute_strong(calibration, frame, mapping, ['X', 'Y'], benchmark)
        for code, category in [('000001.SZ', 'X'), ('000002.SZ', 'Y')]:
            expected = sector._fwd_winrate(panels['close'], benchmark['close'], [(code, dates[0])], mapping,
                                           opens=panels['open'], benchmark_opens=benchmark['open'],
                                           bars=frame, fees=ZERO)
            actual = next(row for row in result[category] if row['otype'] == 'event')
            self.assertAlmostEqual(actual['excess'], expected[category][0]*100)
            self.assertTrue(actual['evidence_ready'])
        changed = frame.copy()
        changed.loc[changed.date == dates[-1], 'close'] *= 100
        second = sector._precompute_strong(calibration, changed, mapping, ['X', 'Y'], benchmark)
        self.assertEqual(result, second)

    def test_public_sector_apis_only_read_artifact_and_invalidate_changed_calibration(self):
        (self.root/'logs').mkdir(); (self.root/'output').mkdir()
        source = self.root/'logs/opportunity_winrates.json'; source.write_text('{}')
        artifact = {'schema_version': 1, 'kind': 'sector_research_evidence',
                    'research_scope': {'universe_fingerprint': 'test-scope'}, 'method_fingerprint': 'test-method',
                    'calibration': {'sha256': hashlib.sha256(source.read_bytes()).hexdigest()},
                    'sectors': [{'code': 'X', 'name': 'test', 'n': 1}],
                    'analyses': {'X': {'timing': {'ok': True}, 'factors': [], 'pitch': {'pitch': [], 'n_pitch': 0},
                                        'retail': {'lowturn': [], 'inflow': [], 'outflow': []}, 'crowd': {'ok': True}}},
                    'stocks': {'000001.SZ': {'ok': True, 'name': 'sample'}}}
        (self.root/'output/sector_evidence.json').write_text(json.dumps(artifact))
        with patch.object(sector, 'BASE', self.root), \
             patch.object(sector, 'scope_metadata', return_value={'universe_fingerprint': 'test-scope'}), \
             patch.object(sector, '_method_fingerprint', return_value='test-method'), \
             patch.object(sector, '_read_bars', side_effect=AssertionError('API must not compute')), \
             patch.object(sector, '_current_panel', side_effect=AssertionError('API must not compute')):
            self.assertEqual(sector.list_sectors()[0]['code'], 'X')
            self.assertTrue(sector.sector_timing('X')['ok'])
            self.assertEqual(sector.sector_strong_factors('X'), [])
            self.assertEqual(sector.sector_pitch('X')['n_pitch'], 0)
            self.assertEqual(sector.stock_search('sample')[0]['code'], '000001.SZ')
            self.assertTrue(sector.stock_factors('000001')['ok'])
            self.assertTrue(sector.sector_crowd('X')['ok'])
            source.write_text('{"changed": true}')
            with self.assertRaisesRegex(ValueError, '需要重新离线计算'):
                sector.list_sectors()

    def test_offline_build_reuses_one_panel_for_all_sectors_and_publishes_atomically(self):
        self.cfg['sector'].update(horizon_sessions=2, min_samples=1)
        self.cfg['stock_fees'] = ZERO
        self.config_file.write_text(yaml.safe_dump(self.cfg))
        frame = bars({'000001.SZ': [10., 20., 22., 22.], '000002.SZ': [10., 10., 9., 9.]})
        frame['source'] = 'tushare'
        dates = sorted(frame.date.unique())
        benchmark = pd.DataFrame({'code': '000300.SH', 'date': dates, 'open': 100., 'close': 100.})
        raw = frame[['code', 'date', 'source']].copy(); raw['amount'] = 100.
        fingerprint = {'universe_fingerprint': 'test-scope'}
        calibration = {'kind': 'historical_calibration', 'meta': {'research_scope': fingerprint},
                       'observations': [{'code': code, 'otype': 'event', 'horizon_months': 1,
                                         'signal_date': str(pd.Timestamp(dates[0]).date())}
                                        for code in ('000001.SZ', '000002.SZ')]}
        (self.root/'logs').mkdir()
        (self.root/'logs/opportunity_winrates.json').write_text(json.dumps(calibration))
        def load(columns, **kwargs):
            return (benchmark if kwargs.get('symbols') else raw if kwargs.get('adjust') == 'none' else frame)[columns.split(',')].copy()
        row = {'close': 10., 'drawdown_60d': -.1, 'mom20': .1, 'near_high_250': -.05,
               'pb': 2., 'roe': .1, 'sq_nyoy': .3}
        panel = pd.DataFrame([row, row], index=['000001.SZ', '000002.SZ'])
        with patch.object(sector, 'BASE', self.root), patch.object(sector, '_read_bars', side_effect=load) as reads, \
             patch.object(sector, 'scope_metadata', return_value=fingerprint), \
             patch.object(sector, 'universe', return_value=set(panel.index)), \
             patch.object(sector, '_load_sector_detail', return_value={'000001.SZ': ('X', 'one'), '000002.SZ': ('Y', 'two')}), \
             patch.object(sector, '_names', return_value={'000001.SZ': 'one', '000002.SZ': 'two'}), \
             patch.object(sector, '_current_panel', return_value=(panel, '2020-01-06', {'event': lambda row: True})), \
             patch('factors.research_context.config', return_value={'index_code': '000300.SH'}), \
             patch.object(sector, '_panels', wraps=sector._panels) as matrices:
            target = sector.build()
            self.assertEqual(len(sector.list_sectors()), 2)
        result = json.loads(target.read_text())
        self.assertEqual(set(result['analyses']), {'X', 'Y'})
        self.assertEqual(set(result['stocks']), set(panel.index))
        self.assertEqual(reads.call_count, 3)  # Scoped qfq, configured benchmark, raw amount; never per industry.
        self.assertEqual(matrices.call_count, 1)
        self.assertFalse(target.with_suffix('.json.tmp').exists())
        self.assertEqual(sector._BUILD_CONTEXT, {})


if __name__ == '__main__':
    unittest.main()
