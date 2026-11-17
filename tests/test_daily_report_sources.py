"""Report source units/date binding, using only temporary JSON fixtures."""
import json
from copy import deepcopy
from datetime import datetime
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from data import daily_report_auto as report
from data.research_bootstrap import TZ


class DailyReportSourcesTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.date = '2026-11-12'
        self.clock = datetime(2026, 11, 12, 19, tzinfo=TZ)
        self.cfg = {'output_dir': str(self.root), 'crowding_sessions': 252, 'turnover_window': 20}
        self.path = self.root / f'crowding_{self.date}.json'
        self.source = {'ok': True, 'data_date': self.date, 'crowding_pctile_252': 15.079365079365079,
                       'n_history_sessions': 252}

    def write_source(self, **changes):
        self.path.write_text(json.dumps({**self.source, **changes}), encoding='utf-8')

    def test_market_percentile_uses_percent_units_and_valid_zero(self):
        self.write_source()
        result = report.load_market_crowding(self.date, self.cfg)
        self.assertAlmostEqual(result['percent'], 15.079365079365079)
        self.assertEqual((result['turnover_window'], result['sessions']), (20, 252))
        self.write_source(crowding_pctile_252=0)
        self.assertEqual(report.load_market_crowding(self.date, self.cfg)['percent'], 0)

    def test_missing_stale_failed_and_invalid_values_never_get_filled(self):
        self.assertIsNone(report.load_market_crowding(self.date, self.cfg))
        for changes in ({'data_date': '2026-11-11'}, {'ok': False}, {'n_history_sessions': 100},
                        *({'crowding_pctile_252': value} for value in (None, True, -1, 101, float('nan'), float('inf')))):
            with self.subTest(changes=changes):
                self.write_source(**changes)
                self.assertIsNone(report.load_market_crowding(self.date, self.cfg))

    def test_report_displays_the_market_value_not_legacy_factor_average(self):
        self.write_source()
        (self.root/'market_kb_dump.json').write_text(json.dumps({'data_date': self.date,
            'market_daily_recent': [{'date': self.date, 'crowd_pctile': 99, 'n_up': 1, 'n_down': 2,
                                     'turnover_亿': 3}], 'factor_perf_top': []}), encoding='utf-8')
        timing = {'score': 40, 'level': '观察', 'dims': {}, 'reason': '合成测试'}
        with patch.object(report, 'BASE', self.root), patch.object(report, 'REPORT', self.root), \
                patch('data.research_factors.load_config', return_value=self.cfg), \
                patch.object(report, 'report_data_status_lines', return_value=[]):
            text = report.build_report(self.date, timing, [], '观察', [], [], True).read_text()
        market_line = next(line for line in text.splitlines() if '市场拥挤度' in line)
        self.assertIn('15.1%（20日平均换手的252日历史分位）', market_line)
        self.assertNotIn('99', market_line)
        self.assertNotIn('尚无', market_line)

    def test_report_page_does_not_promise_a_fixed_completion_time(self):
        page = (Path(__file__).resolve().parents[1]/'ui_v2/pages/report.html').read_text()
        self.assertNotIn('18:30', page)
        self.assertIn('收盘研究完成后自动更新', page)

    def verified_manifest(self):
        return {'source': 'tushare-official', 'target_end': self.date,
                'coverage': {'daily': {'end': self.date}},
                'completed': {'daily:' + self.date: {'rows': 5218}},
                'stages': {key: 'complete' for key in ('daily', 'finance', 'assets')},
                'stage_dates': {key: self.date for key in ('daily', 'finance', 'assets')}}

    def test_date_uses_sealed_official_checkpoint_without_opening_bars(self):
        (self.root/'bootstrap-manifest.json').write_text(json.dumps(self.verified_manifest()))
        with patch('data.cache.DailyCache') as bars:
            actual = report._trade_date({'runtime_dir': str(self.root), 'min_daily_rows': 2000}, now=self.clock)
        self.assertEqual(actual, self.date)
        bars.assert_not_called()

    def test_date_rejects_incomplete_mismatched_or_unproven_core(self):
        original = self.verified_manifest()
        variants = []
        for stage in ('daily', 'finance', 'assets'):
            item = deepcopy(original); item['stages'][stage] = 'running'; variants.append(item)
            item = deepcopy(original); item['stage_dates'][stage] = '2026-11-11'; variants.append(item)
        item = deepcopy(original); item['completed'] = {}; variants.append(item)
        item = deepcopy(original); item['completed']['daily:' + self.date]['rows'] = 1; variants.append(item)
        item = deepcopy(original); item['source'] = 'unverified'; variants.append(item)
        for item in variants:
            with self.subTest(manifest=item):
                (self.root/'bootstrap-manifest.json').write_text(json.dumps(item))
                with self.assertRaises(ValueError):
                    report._trade_date({'runtime_dir': str(self.root), 'min_daily_rows': 2000}, now=self.clock)

    def test_completed_markers_cannot_override_close_cutoff_or_future_date(self):
        cfg = {'runtime_dir': str(self.root), 'min_daily_rows': 2000, 'completed_session_cutoff': '17:00'}
        (self.root/'bootstrap-manifest.json').write_text(json.dumps(self.verified_manifest()))
        with self.assertRaises(ValueError):
            report._trade_date(cfg, now=self.clock.replace(hour=16, minute=59))
        self.assertEqual(report._trade_date(cfg, now=self.clock.replace(hour=17, minute=0)), self.date)
        # The exact same internally complete manifest cannot be used one day early.
        with self.assertRaises(ValueError):
            report._trade_date(cfg, now=self.clock.replace(day=11))

    def test_date_clock_must_be_aware_and_cutoff_must_be_after_market_close(self):
        (self.root/'bootstrap-manifest.json').write_text(json.dumps(self.verified_manifest()))
        cfg = {'runtime_dir': str(self.root), 'min_daily_rows': 2000, 'completed_session_cutoff': '17:00'}
        with self.assertRaises(ValueError):
            report._trade_date(cfg, now=self.clock.replace(tzinfo=None))
        with self.assertRaises(ValueError):
            report._trade_date({**cfg, 'completed_session_cutoff': '15:04'}, now=self.clock)

    def data_status(self, provider, done=125, valid=True):
        runtime = self.root/'runtime/research'
        runtime.mkdir(parents=True, exist_ok=True)
        (runtime/'bootstrap-manifest.json').write_text(json.dumps({'stages': {'minute': 'rate_limited'}}))
        (runtime/'rt-permission-verification.json').write_text(json.dumps({
            'endpoint': 'rt_k', 'checked_at': '2026-11-11T18:29:57+08:00', 'ok': False}))
        status = {'database_path': str(self.root/'minute.db') if valid else None,
                  'completed_symbols': done, 'expected_symbols': 300, 'start_date': '2026-08-12',
                  'target_date': self.date, 'source': 'baostock:history', 'sources': ['baostock:history', 'sina:getKLineData:5min:raw'],
                  'current_date_ready': True, 'cache_verified': True}
        module = SimpleNamespace(load_config=lambda: {'provider': 'baostock'}, public_status=lambda cfg: status)
        market = {'provider': provider, 'source_label': '腾讯实时盘口 + Tushare基础' if provider != 'tushare' else 'Tushare实时盘口'}
        with patch.object(report, 'BASE', self.root), patch('data.minute_readiness.selected_service', return_value=module), \
                patch('data.minute_readiness.public_status') as deep_verifier, \
                patch('paperlive.config.load_config', return_value={'market': market}):
            result = '\n'.join(report.report_data_status_lines())
        deep_verifier.assert_not_called()
        return result

    def test_selected_hybrid_sources_ignore_legacy_tushare_limits(self):
        for provider in ('tencent_tushare', 'tencent_baostock'):
            with self.subTest(provider=provider):
                text = self.data_status(provider)
                self.assertIn('BaoStock（含新浪分钟数据修补）', text)
                self.assertIn('生成时下载回执 125/300', text)
                self.assertIn('2026-08-12 至 2026-11-12', text)
                self.assertNotIn('rt_k', text)
                self.assertNotIn('接口权限限制', text)
                self.assertIn('[数据页](/data)', text)
                self.assertIn('[实时模拟盘](/paperlive)', text)

    def test_full_receipt_count_never_certifies_deep_or_live_readiness(self):
        text = self.data_status('tencent_tushare', done=300)
        self.assertIn('回执 300/300', text)
        self.assertIn('下载回执数量不等于当前窗口已通过完整核验', text)
        self.assertIn('静态日报不判定实时可交易性', text)
        invalid = self.data_status('tencent_tushare', done=300, valid=False)
        self.assertNotIn('300/300', invalid)
        self.assertIn('暂无可核对的下载回执', invalid)

    def test_only_pure_tushare_displays_timestamped_rt_probe(self):
        text = self.data_status('tushare')
        self.assertIn('Tushare rt_k 历史权限探测：2026-11-11T18:29:57+08:00 未通过', text)
        self.assertIn('仅代表当次探测，不代表当前盘口可交易', text)


if __name__ == '__main__':
    unittest.main()
