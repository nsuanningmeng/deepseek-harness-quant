"""Fresh-clock writes and runner failure recovery, entirely in temporary files."""
from copy import deepcopy
from dataclasses import asdict
from datetime import datetime, timedelta
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import Mock, patch
from zoneinfo import ZoneInfo

from autotrade.monitoring import atomic_json
from paperlive.config import load_config
from paperlive.ledger import Ledger
from paperlive.models import Instrument
import paperlive.runtime as runtime

ZONE = ZoneInfo('Asia/Shanghai')
START = datetime(2026, 11, 11, 10, 0, tzinfo=ZONE)
SECURITY = Instrument('600000.SH', '合成测试证券', 'MAIN', '2000-01-01')


class MovingClock(datetime):
    base = START
    ticks = 0

    @classmethod
    def now(cls, tz=None):
        cls.ticks += 1
        value = cls.base + timedelta(microseconds=cls.ticks)
        return value.astimezone(tz) if tz else value.replace(tzinfo=None)


class StopTestLoop(Exception):
    pass


class HeartbeatTests(TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.cfg = load_config(Path(self.temp.name) / 'isolated-paper.yaml')
        self.cfg['risk']['kill_switch_file'] = str(Path(self.temp.name) / 'STOP')
        self.cfg['market']['provider'] = 'tencent_baostock'
        self.snapshot = {'account': {'cash': '1000000.00'}, 'positions': [],
                         'orders': [], 'fills': [], 'curve': []}

    def state(self, updated, now, open=True, next_open=None):
        raw = {'updated_at': updated, 'market': {'open': open, 'buy_ready': True,
               'phase': '连续竞价' if open else '休市', 'next_open': next_open},
               'ai': {'phase': '分析中', 'next_run_at': 'scheduled'}}
        result = runtime.public_state(self.cfg, raw, self.snapshot, now=now)
        # Serving stale status must not alter the runner's persisted state.
        self.assertEqual(raw['market']['open'], open)
        return result

    def test_open_heartbeat_expires_without_losing_account(self):
        fresh = self.state(START.isoformat(), START + timedelta(seconds=9))
        self.assertFalse(fresh['market']['heartbeat_stale'])
        stale = self.state(START.isoformat(), START + timedelta(seconds=71))
        self.assertTrue(stale['ok'])
        self.assertTrue(stale['market']['heartbeat_stale'])
        self.assertFalse(stale['market']['open'])
        self.assertFalse(stale['market']['buy_ready'])
        self.assertEqual(stale['account']['cash'], '1000000.00')
        self.assertEqual(stale['ai']['phase'], '等待行情进程恢复')
        self.assertIsNone(stale['ai']['next_run_at'])

    def test_closed_heartbeat_uses_closed_cadence(self):
        closed = START.replace(hour=16)
        self.assertFalse(self.state(closed.isoformat(), closed + timedelta(seconds=301), open=False)['market']['heartbeat_stale'])
        self.assertTrue(self.state(closed.isoformat(), closed + timedelta(seconds=361), open=False)['market']['heartbeat_stale'])

    def test_persisted_pause_overrides_old_or_stopped_runner_without_losing_account(self):
        Path(self.cfg['risk']['kill_switch_file']).write_text('paused')
        for offset in (9, 9999):
            result = self.state(START.isoformat(), START + timedelta(seconds=offset))
            self.assertTrue(result['paused'])
            self.assertFalse(result['market']['open'])
            self.assertFalse(result['market']['buy_ready'])
            self.assertEqual(result['market']['phase'], '盘中模拟已暂停')
            self.assertIsNone(result['ai']['next_run_at'])
            self.assertEqual(result['account']['cash'], '1000000.00')

    def test_launcher_preserves_user_pause_without_starting_runner(self):
        import launcher
        root = Path(self.temp.name)
        (root / 'config').mkdir()
        (root / 'config/paperlive.yaml').touch()
        Path(self.cfg['risk']['kill_switch_file']).touch()
        with patch('paperlive.config.load_config', return_value=self.cfg), patch.object(launcher.subprocess, 'Popen') as spawn:
            self.assertIsNone(launcher.start_paperlive(root))
            spawn.assert_not_called()

    def test_next_open_caps_closed_tolerance(self):
        before = START.replace(hour=9, minute=29)
        opening = before.replace(minute=30)
        result = self.state(before.isoformat(), opening + timedelta(seconds=61),
                            open=False, next_open=opening.isoformat())
        self.assertTrue(result['market']['heartbeat_stale'])

    def test_close_observation_uses_fast_cadence(self):
        auction = START.replace(hour=14, minute=58)
        result = self.state(auction.isoformat(), auction + timedelta(seconds=71), open=False)
        self.assertTrue(result['market']['heartbeat_stale'])

    def test_expected_open_heartbeat_expires_at_intraday_cadence_on_quote_failure(self):
        raw = {'updated_at': START.isoformat(), 'market': {'open': False, 'expected_open': True}}
        result = runtime.public_state(self.cfg, raw, self.snapshot, now=START + timedelta(seconds=71))
        self.assertTrue(result['market']['heartbeat_stale'])
        self.assertFalse(result['market']['open'])

    def test_bad_missing_naive_and_future_heartbeats_fail_closed(self):
        for updated in (None, 'invalid', START.replace(tzinfo=None).isoformat(),
                        (START + timedelta(seconds=1)).isoformat()):
            with self.subTest(updated=updated):
                result = self.state(updated, START)
                self.assertTrue(result['market']['heartbeat_stale'])
                self.assertFalse(result['market']['open'])

    def test_provider_switch_cannot_relabel_previous_suppliers_quotes(self):
        self.cfg['market']['provider'] = 'tushare'
        raw = {'updated_at': START.isoformat(), 'market': {'provider': 'tencent_baostock',
               'source': 'previous supplier', 'open': True, 'buy_ready': True, 'quote_count': 1,
               'universe_count': 1}, 'ai': {'phase': '分析中'}, 'quotes': [{'source': 'tencent'}],
               'universe': [{'symbol': SECURITY.symbol}], 'eligible_symbols': [SECURITY.symbol]}
        result = runtime.public_state(self.cfg, raw, self.snapshot, now=START)
        self.assertTrue(result['market']['heartbeat_stale'])
        self.assertFalse(result['market']['open'])
        self.assertEqual(result['market']['provider'], 'tushare')
        self.assertEqual(result['market']['quote_count'], 0)
        self.assertEqual(result['quotes'], [])
        self.assertEqual(result['universe'], [])
        self.assertEqual(result['account']['cash'], '1000000.00')


class PaperRuntimeTests(TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.cfg = load_config(self.root / 'isolated-paper.yaml')
        self.cfg['market']['provider'] = 'tencent_baostock'
        self.cfg['runtime_dir'] = str(self.root)
        self.cfg['risk']['kill_switch_file'] = str(self.root / 'STOP')
        self.cfg['strategy']['mode'] = 'manual'
        MovingClock.base, MovingClock.ticks = START, 0

    def test_write_retries_only_exact_precommit_clock_error_with_new_time(self):
        moments = []
        def operation(*, now):
            moments.append(now)
            if len(moments) == 1:
                raise ValueError('paper_clock_moved_backwards')
            return 'accepted'
        with patch.object(runtime, 'datetime', MovingClock):
            self.assertEqual(runtime._write_now(operation), 'accepted')
        self.assertEqual(len(moments), 2)
        self.assertGreater(moments[1], moments[0])
        for error in (ValueError('risk_rejected'), ValueError('paper_clock_moved_backwards: other'),
                      RuntimeError('paper_clock_moved_backwards')):
            operation = Mock(side_effect=error)
            with self.subTest(error=error), patch.object(runtime, 'datetime', MovingClock):
                with self.assertRaises(type(error)):
                    runtime._write_now(operation)
                self.assertEqual(operation.call_count, 1)

    def test_persistent_clock_contention_is_bounded(self):
        operation = Mock(side_effect=ValueError('paper_clock_moved_backwards'))
        with patch.object(runtime, 'datetime', MovingClock):
            with self.assertRaisesRegex(ValueError, '^paper_clock_moved_backwards$'):
                runtime._write_now(operation)
        self.assertEqual(operation.call_count, 3)
        self.assertEqual(len({call.kwargs['now'] for call in operation.call_args_list}), 3)

    def test_sqlite_api_write_between_runner_clock_and_cancel_is_retried_safely(self):
        with patch('paperlive.ledger._now', return_value=START):
            ledger, api_ledger = Ledger(self.cfg), Ledger(self.cfg)
        ledger.submit('BUY', SECURITY.symbol, 100, '10', SECURITY, START, True, 'runner-order')
        calls = []
        def cancel_after_api_commit(*, now):
            calls.append(now)
            if len(calls) == 1:
                # Another connection commits after the runner captured its clock.
                api_ledger.submit('BUY', SECURITY.symbol, 100, '10', SECURITY,
                                  START + timedelta(seconds=2), True, 'api-order')
            return ledger.cancel('runner-order', now=now)
        with patch.object(runtime, 'datetime') as clock:
            clock.now.side_effect = [START + timedelta(seconds=1), START + timedelta(seconds=3)]
            result = runtime._write_now(cancel_after_api_commit)
        self.assertEqual(result['status'], 'CANCELLED')
        snapshot = ledger.snapshot(now=START + timedelta(seconds=3))
        self.assertTrue(snapshot['reconciliation']['ok'])
        self.assertEqual(snapshot['fills'], [])
        statuses = {order['order_id']: order['status'] for order in snapshot['orders']}
        self.assertEqual(statuses, {'runner-order': 'CANCELLED', 'api-order': 'OPEN'})
        self.assertEqual(len(calls), 2)

    def test_manual_api_submit_and_cancel_use_fresh_keyword_clocks(self):
        raw = {'updated_at': START.isoformat(), 'market': {'open': True, 'calendar_verified': True,
               'date': START.date().isoformat(), 'buy_ready': True},
               'universe': [asdict(SECURITY)], 'eligible_symbols': [SECURITY.symbol]}
        (self.root / 'state.json').write_text(json.dumps(raw))
        with patch.object(runtime, 'datetime', MovingClock), patch('paperlive.ledger._now', return_value=START):
            order = runtime.manual_order({'side': 'BUY', 'symbol': SECURITY.symbol, 'quantity': 100,
                                          'limit_price': '10', 'intent_id': 'manual-fixture'}, self.cfg)
            cancelled = runtime.cancel_order({'order_id': order['order_id']}, self.cfg)
        self.assertEqual(order['status'], 'OPEN')
        self.assertEqual(cancelled['status'], 'CANCELLED')
        self.assertGreater(datetime.fromisoformat(cancelled['updated_at']), datetime.fromisoformat(order['created_at']))

    def _runner_fixtures(self, cancel_errors=0, end_errors=0, pending=True):
        class FakeLedger:
            fingerprint = 'synthetic-execution-policy'

            def __init__(self):
                self.pending = pending
                self.cancel_calls, self.match_calls, self.end_calls = [], [], []
                self.cancel_reasons = []

            def symbols(self):
                return [SECURITY.symbol] if self.pending else []

            def snapshot(self, quotes, now):
                order = {'order_id': 'fixture-order', 'side': 'BUY', 'symbol': SECURITY.symbol, 'status': 'OPEN'}
                return {'started_at': START.isoformat(), 'account': {'cash': '1000000.00', 'valuation_complete': True},
                        'positions': [], 'orders': [order] if self.pending else [], 'fills': [], 'curve': [],
                        'reconciliation': {'ok': True, 'errors': []}}

            def cancel(self, order_id, *, now, reason='user_cancelled'):
                self.cancel_calls.append(now)
                self.cancel_reasons.append(reason)
                if len(self.cancel_calls) <= cancel_errors:
                    raise ValueError('paper_clock_moved_backwards')
                self.pending = False
                return {'order_id': order_id, 'status': 'CANCELLED'}

            def match(self, quotes, instruments, *, now, session_open):
                self.match_calls.append(now)
                return []

            def end_session(self, *, now):
                self.end_calls.append(now)
                if len(self.end_calls) <= end_errors:
                    raise ValueError('paper_clock_moved_backwards')
                return []

        feed = SimpleNamespace(status={'universe': {'cache_stale': False}},
                               universe=lambda: {}, lookup=lambda symbols: {SECURITY.symbol: SECURITY},
                               quotes=lambda symbols: {}, session=lambda now: {
                                   'open': False, 'calendar_verified': True, 'date': now.date().isoformat(),
                                   'phase': '连续竞价已结束', 'next_open': None})
        planner = SimpleNamespace(describe=lambda: {'configured': False},
                                  propose=Mock(side_effect=AssertionError('No AI call allowed in offline tests')))
        return FakeLedger(), feed, planner

    def _run_fake(self, ledger, feed, planner, once=True, sleeps=None, start=START.replace(hour=16)):
        MovingClock.base, MovingClock.ticks = start, 0
        writes = []
        def save(path, payload):
            writes.append(deepcopy(payload))
            atomic_json(path, payload)
        with patch('paperlive.ledger.Ledger', return_value=ledger), \
             patch('paperlive.market.MarketFeed', return_value=feed), \
             patch('paperlive.hybrid_feed.HybridMarketFeed', return_value=feed), \
             patch.object(runtime, 'AiPlanner', return_value=planner), \
             patch.object(runtime, 'datetime', MovingClock), \
             patch.object(runtime, 'atomic_json', side_effect=save), \
             patch.object(runtime.time, 'sleep', side_effect=sleeps):
            if once:
                result = runtime.run(self.cfg, once=True)
            else:
                with self.assertRaises(StopTestLoop):
                    runtime.run(self.cfg)
                result = None
        planner.propose.assert_not_called()
        self.assertFalse((self.root / 'paper.sqlite3').exists())
        return result, writes

    def test_opening_quote_failure_retries_at_open_cadence_and_logs_recovery(self):
        ledger, feed, planner = self._runner_fixtures(pending=False)
        self.cfg['market']['provider'] = 'tencent_tushare'
        stop = Path(self.cfg['risk']['kill_switch_file'])
        stop.write_text('Synthetic user pause')
        feed.universe = lambda: {SECURITY.symbol: SECURITY}
        quote = SimpleNamespace(timestamp=START, to_dict=lambda: {'symbol': SECURITY.symbol})
        feed.quotes = Mock(side_effect=[{}, {}, {SECURITY.symbol: quote}])
        feed.status['quotes'] = {'last_error': 'request timed out, credential=not-for-events'}
        feed.session = lambda now: {'open': True, 'calendar_verified': True, 'date': now.date().isoformat()}
        delays = []
        def wait(delay):
            delays.append(delay)
            if len(delays) == 3:
                raise StopTestLoop()
        _, writes = self._run_fake(ledger, feed, planner, once=False, sleeps=wait, start=START)
        self.assertEqual(delays, [self.cfg['market']['refresh_seconds']] * 3)
        self.assertEqual([row['market']['open'] for row in writes], [False, False, True])
        self.assertTrue(all(row['market']['expected_open'] for row in writes))
        self.assertTrue(all(row['paused'] for row in writes))
        self.assertTrue(stop.exists())
        self.assertEqual(ledger.match_calls, [])
        text = (self.root / 'events.jsonl').read_text()
        events = [json.loads(line) for line in text.splitlines()]
        self.assertEqual([row['event'] for row in events], ['runner_started', 'market_unavailable', 'market_recovered'])
        self.assertEqual(events[1]['reasons'], ['quotes', 'timeout'])
        self.assertEqual(events[1]['quote_count'], 0)
        self.assertEqual(events[2]['quote_count'], 1)
        self.assertNotIn('not-for-events', text)

    def test_failed_provider_uses_clock_only_for_retry_and_keeps_closed_cadence(self):
        for now, expected_open, cadence in (
            (START, True, 'refresh_seconds'),
            (START.replace(hour=12), False, 'closed_refresh_seconds'),
            (START.replace(hour=16), False, 'closed_refresh_seconds'),
            (START + timedelta(days=3), False, 'closed_refresh_seconds'),
        ):
            with self.subTest(now=now):
                ledger, feed, planner = self._runner_fixtures(pending=False)
                feed.universe = Mock(side_effect=OSError('network is unreachable'))
                def wait(delay):
                    self.assertEqual(delay, self.cfg['market'][cadence])
                    raise StopTestLoop()
                _, writes = self._run_fake(ledger, feed, planner, once=False, sleeps=wait, start=now)
                self.assertEqual(writes[0]['market']['expected_open'], expected_open)
                self.assertFalse(writes[0]['market']['open'])
                self.assertEqual(ledger.match_calls, [])

    def test_data_recovery_restores_plan_status_without_an_extra_ai_call(self):
        self.cfg['strategy']['mode'] = 'ai'
        self.cfg['activation']['enabled'] = False
        for failure, expected in ((None, '计划已风控处理'), ('synthetic binding failure', 'AI计划失败，等待重试')):
            with self.subTest(failure=failure):
                ledger, feed, planner = self._runner_fixtures(pending=False)
                planner.describe = lambda: {'configured': True}
                feed.session = lambda now: {'open': True, 'calendar_verified': True,
                                           'date': now.date().isoformat(), 'next_open': None}
                def quotes(symbols):
                    feed.status['universe']['cache_stale'] = quotes.calls == 0
                    quotes.calls += 1
                    return {}
                quotes.calls = 0
                feed.quotes = quotes
                prior = {'ai': {'last_attempt_epoch': START.timestamp(), 'last_plan_at': START.isoformat(),
                                'last_error': failure, 'phase': expected,
                                'council': {'latest_trace': {'status': 'failed' if failure else 'hold'}}}}
                (self.root / 'state.json').write_text(json.dumps(prior))
                with patch.object(runtime.time, 'time', return_value=START.timestamp() + 1):
                    _, writes = self._run_fake(ledger, feed, planner, once=False,
                                               sleeps=[None, StopTestLoop()], start=START)
                self.assertFalse(writes[0]['market']['buy_ready'])
                self.assertTrue(writes[1]['market']['buy_ready'])
                self.assertEqual(writes[1]['ai']['phase'], expected)
                self.assertEqual(writes[1]['ai']['last_error'], failure)
                self.assertEqual(writes[1]['ai']['last_attempt_epoch'], START.timestamp())
                self.assertEqual(writes[1]['ai']['last_plan_at'], START.isoformat())

    def test_verified_closed_day_cannot_be_opened_by_local_clock(self):
        market = self.cfg['market']
        session = {'open': False, 'calendar_verified': True}
        self.assertFalse(runtime._market_expected_open(market, session, START))
        self.assertEqual(runtime._poll_delay(market, session, START), market['closed_refresh_seconds'])

    def test_runner_cancel_contention_writes_error_and_continues_next_iteration(self):
        ledger, feed, planner = self._runner_fixtures(cancel_errors=100)
        _, writes = self._run_fake(ledger, feed, planner, once=False, sleeps=[None, StopTestLoop()])
        self.assertEqual(len(writes), 2)
        self.assertEqual(len(ledger.cancel_calls), 6)
        self.assertEqual(ledger.match_calls, [])
        self.assertEqual(ledger.end_calls, [])
        self.assertTrue(all('paper_clock_moved_backwards' in item['market']['error'] for item in writes))
        self.assertTrue(all(item['market']['open'] is False for item in writes))
        self.assertIn('paper_clock_moved_backwards', json.loads((self.root/'state.json').read_text())['market']['error'])

    def test_runner_guard_cancellations_are_not_labelled_user_actions(self):
        for paused, stale, expected in ((False, True, 'activation_not_ready'),
                                        (False, False, 'instrument_not_eligible'),
                                        (True, False, 'kill_switch_active')):
            with self.subTest(paused=paused, stale=stale):
                stop = Path(self.cfg['risk']['kill_switch_file'])
                if paused:
                    stop.write_text('Synthetic user pause')
                else:
                    stop.unlink(missing_ok=True)
                ledger, feed, planner = self._runner_fixtures()
                feed.status['universe']['cache_stale'] = stale
                self._run_fake(ledger, feed, planner)
                self.assertEqual(ledger.cancel_reasons, [expected])

    def test_quote_gap_keeps_orders_but_blocks_matching_until_gate_recovers(self):
        ledger, feed, planner = self._runner_fixtures()
        feed.universe = lambda: {SECURITY.symbol: SECURITY}
        feed.session = lambda now: {'open': True, 'calendar_verified': True,
                                   'date': now.date().isoformat(), 'next_open': None}
        blocked = {'enabled': True, 'armed': True, 'ready': False, 'status': 'waiting_data',
                   'checks': [{'id': key, 'ok': key != 'quotes'} for key in runtime.ACTIVATION_CHECK_IDS]}
        ready = {**deepcopy(blocked), 'ready': True, 'status': 'active'}
        for row in ready['checks']: row['ok'] = True
        gate = SimpleNamespace(evaluate=Mock(side_effect=[blocked, blocked, ready]),
                               reserve_ai_probe=lambda *args: None)
        with patch.object(runtime, 'ActivationGate', return_value=gate):
            _, writes = self._run_fake(ledger, feed, planner, once=False,
                                       sleeps=[None, None, StopTestLoop()], start=START)
        self.assertEqual(ledger.cancel_calls, [])
        self.assertEqual(len(ledger.match_calls), 1)
        self.assertEqual([s['market']['execution_hold']['active'] for s in writes], [True, True, False])
        self.assertEqual(writes[0]['market']['execution_hold']['order_ids'], ['fixture-order'])
        events = [json.loads(line) for line in (self.root/'events.jsonl').read_text().splitlines()]
        self.assertEqual(sum(e['event'] == 'orders_waiting_for_quotes' for e in events), 1)
        self.assertEqual(sum(e['event'] == 'order_quote_wait_ended' for e in events), 1)

    def test_quote_wait_does_not_retain_orders_after_other_gate_or_user_stop(self):
        for reason in ('research_products', 'account', 'ai', 'pipeline', 'universe', 'paper', 'calendar', 'minute', 'manual_stop', 'removed'):
            with self.subTest(reason=reason):
                ledger, feed, planner = self._runner_fixtures()
                feed.universe = lambda: {SECURITY.symbol: SECURITY} if reason != 'removed' else {}
                feed.session = lambda now: {'open': True, 'calendar_verified': True,
                                           'date': now.date().isoformat(), 'next_open': None}
                checks = [{'id': key, 'ok': key not in ('quotes', reason)} for key in runtime.ACTIVATION_CHECK_IDS]
                blocked = {'enabled': True, 'armed': True, 'ready': False, 'checks': checks}
                gate = SimpleNamespace(evaluate=lambda *args: blocked, reserve_ai_probe=lambda *args: None)
                stop = Path(self.cfg['risk']['kill_switch_file'])
                if reason == 'manual_stop': stop.write_text('Synthetic user stop')
                else: stop.unlink(missing_ok=True)
                with patch.object(runtime, 'ActivationGate', return_value=gate):
                    self._run_fake(ledger, feed, planner, start=START)
                expected = ('kill_switch_active' if reason == 'manual_stop' else
                            'instrument_not_eligible' if reason == 'removed' else 'activation_not_ready')
                self.assertEqual(ledger.cancel_reasons, [expected])

    def test_quote_hold_requires_armed_verified_continuous_session(self):
        activation = {'enabled': True, 'armed': True, 'ready': False,
                      'checks': [{'id': key, 'ok': key != 'quotes'} for key in runtime.ACTIVATION_CHECK_IDS]}
        session = {'open': True, 'calendar_verified': True}
        feed = {'universe': {'cache_stale': False}}
        self.assertTrue(runtime._quote_readiness_hold(activation, session, feed))
        duplicated = deepcopy(activation['checks']); duplicated[0] = deepcopy(duplicated[1])
        for change in ({'armed': False}, {'manual_override': True}, {'checks': []},
                       {'checks': activation['checks'][1:]}, {'checks': duplicated},
                       {'checks': [{'id': 'quotes', 'ok': None}]}):
            self.assertFalse(runtime._quote_readiness_hold({**activation, **change}, session, feed))
        self.assertFalse(runtime._quote_readiness_hold(activation, {**session, 'open': False}, feed))
        self.assertFalse(runtime._quote_readiness_hold(activation, {**session, 'calendar_verified': False}, feed))
        self.assertFalse(runtime._quote_readiness_hold(activation, session, {'universe': {'cache_stale': True}}))

    def test_runner_end_session_contention_is_reported_without_exiting(self):
        ledger, feed, planner = self._runner_fixtures(end_errors=100, pending=False)
        result, writes = self._run_fake(ledger, feed, planner)
        self.assertEqual(len(ledger.end_calls), 3)
        self.assertEqual(result['market']['phase'], '收盘撤余未完成')
        self.assertIn('paper_clock_moved_backwards', writes[0]['market']['error'])
        self.assertFalse(result['market']['open'])

    def test_runner_transient_cancel_race_recovers_with_new_clock(self):
        ledger, feed, planner = self._runner_fixtures(cancel_errors=1)
        result, _ = self._run_fake(ledger, feed, planner)
        self.assertEqual(len(ledger.cancel_calls), 2)
        self.assertGreater(ledger.cancel_calls[1], ledger.cancel_calls[0])
        self.assertEqual(len(ledger.match_calls), 1)
        self.assertEqual(len(ledger.end_calls), 1)
        self.assertIsNone(result['market']['error'])
