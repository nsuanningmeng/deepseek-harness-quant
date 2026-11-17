"""Closed-market presentation cannot hide missing prerequisites or enable orders."""
from copy import deepcopy
from datetime import datetime, timedelta
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from zoneinfo import ZoneInfo

from paperlive.readiness import market_wait_status
from paperlive.config import load_config
from paperlive.runtime import public_state
from deck import live_api
from validation.check_portal_readiness import classify

NOW = datetime(2026, 11, 12, 20, 0, tzinfo=ZoneInfo('Asia/Shanghai'))
BASE = ('paper', 'calendar', 'research_products', 'pipeline', 'universe', 'minute', 'ai', 'account')


def waiting_state():
    return {'activation': {'enabled': True, 'armed': True, 'ready': False, 'stop_owned': True,
                          'status': 'waiting_data', 'checks': [{'id': key, 'ok': key in BASE}
                              for key in BASE + ('quotes', 'executable', 'session')]},
            'market': {'open': False, 'calendar_verified': True, 'heartbeat_stale': False,
                       'error': None, 'next_open': (NOW + timedelta(hours=13, minutes=30)).isoformat()}}


class MarketWaitReadinessTests(unittest.TestCase):
    def test_closed_complete_prerequisites_are_waiting_market_not_ready(self):
        state = waiting_state(); original = deepcopy(state)
        result = market_wait_status(state, NOW)
        self.assertEqual(result['state'], 'waiting_market')
        self.assertFalse(result['ready']); self.assertTrue(result['initialized'])
        self.assertEqual(state, original, 'Display classification must not mutate the activation gate')

    def test_every_missing_base_requirement_remains_data_wait(self):
        for key in BASE:
            state = waiting_state()
            next(row for row in state['activation']['checks'] if row['id'] == key)['ok'] = False
            with self.subTest(requirement=key): self.assertIsNone(market_wait_status(state, NOW))

    def test_open_session_manual_pause_stale_heartbeat_and_errors_cannot_be_hidden(self):
        for group, key, value in [('market', 'heartbeat_stale', True), ('market', 'calendar_verified', False),
                                 ('market', 'error', 'quote downloader failed'), ('market', 'open', True),
                                 ('activation', 'armed', False), ('activation', 'stop_owned', False),
                                 ('activation', 'status', 'paused'), ('activation', 'ready', True)]:
            state = waiting_state(); state[group][key] = value
            with self.subTest(group=group, key=key): self.assertIsNone(market_wait_status(state, NOW))
        state = waiting_state(); state['activation']['checks'][-1]['ok'] = True
        self.assertIsNone(market_wait_status(state, NOW))

    def test_incomplete_duplicate_unknown_and_malformed_checks_fail_closed(self):
        original = waiting_state()
        for checks in [[], original['activation']['checks'][:-1],
                       original['activation']['checks'][:-1] + [original['activation']['checks'][0]],
                       original['activation']['checks'][:-1] + [{'id': 'future-check', 'ok': False}],
                       original['activation']['checks'][:-1] + [{'id': [], 'ok': False}],
                       original['activation']['checks'][:-1] + [{'id': 'session', 'ok': 0}]]:
            state = waiting_state(); state['activation']['checks'] = checks
            self.assertIsNone(market_wait_status(state, NOW))

    def test_next_session_must_be_known_future_and_timezone_aware(self):
        for value in (None, '', 'invalid', NOW.isoformat(), (NOW-timedelta(seconds=1)).isoformat(), '2026-11-13T09:30:00'):
            state = waiting_state(); state['market']['next_open'] = value
            with self.subTest(next_open=value): self.assertIsNone(market_wait_status(state, NOW))

    def test_public_state_keeps_stop_and_execution_gate_and_ages_out_readiness(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = load_config(Path(tmp)/'isolated.yaml')
            cfg['runtime_dir'] = tmp; cfg['risk']['kill_switch_file'] = str(Path(tmp)/'STOP')
            Path(cfg['risk']['kill_switch_file']).write_text('synthetic owned stop')
            state = waiting_state(); state['updated_at'] = NOW.isoformat()
            state['market']['provider'] = cfg['market']['provider']
            with patch('paperlive.runtime.public_activation', return_value=state['activation']):
                result = public_state(cfg, state, {}, now=NOW)
                self.assertEqual(result['readiness']['state'], 'waiting_market')
                self.assertTrue(result['paused']); self.assertFalse(result['activation']['ready'])
                self.assertFalse(result['market']['open']); self.assertFalse(result['market']['buy_ready'])
                stale = public_state(cfg, state, {}, now=NOW+timedelta(hours=1))
                self.assertIsNone(stale['readiness'])
            self.assertEqual(Path(cfg['risk']['kill_switch_file']).read_text(), 'synthetic owned stop')

    def test_live_alias_and_health_probe_preserve_wait_state_without_quotes(self):
        state = waiting_state(); state.update(paused=True, readiness=market_wait_status(state, NOW))
        with tempfile.TemporaryDirectory() as tmp:
            cfg = {'runtime_dir': tmp, 'market': {'provider': 'tencent_tushare', 'source_label': 'synthetic'}}
            with patch('paperlive.config.load_config', return_value=cfg), patch('paperlive.runtime.public_state', return_value=state):
                alias = live_api.live_realtime()
        self.assertEqual(alias['state'], 'waiting_market'); self.assertFalse(alias['ready'])
        self.assertEqual(alias['quotes'], {}); self.assertFalse(alias['market_open'])
        class Response:
            code = 200
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def read(self): return json.dumps(alias).encode()
        with patch('urllib.request.OpenerDirector.open', return_value=Response()):
            row = live_api._probe_one('/api/live/realtime')
        self.assertTrue(row['reachable']); self.assertFalse(row['ok'])
        self.assertEqual(row['state'], 'waiting_market')
        self.assertEqual(classify('/api/live/realtime', 200, alias)[0], 'waiting_market')
        self.assertEqual(classify('/api/paperlive/state', 200, state)[0], 'waiting_market')
        self.assertEqual(live_api._endpoint_counts([row], 1)['waiting_market_count'], 1)
        missing = deepcopy(state); missing['readiness'] = None
        self.assertEqual(classify('/api/paperlive/state', 200, missing)[0], 'waiting_data')

    def test_current_ai_and_account_failures_override_cached_passed_checks(self):
        for extra in ({'ai': {'configured': False}}, {'ai': {'error': 'current provider config failed'}},
                      {'ai': {'council': {'enabled': True, 'configured': False, 'error': 'catalog invalid'}}},
                      {'account_snapshot': {'reconciliation': {'ok': False}}},
                      {'account_snapshot': {'account': {'valuation_complete': False}}}):
            state = waiting_state(); state.update(extra)
            with self.subTest(extra=extra): self.assertIsNone(market_wait_status(state, NOW))
        with tempfile.TemporaryDirectory() as tmp:
            cfg = load_config(Path(tmp)/'isolated.yaml'); cfg['risk']['kill_switch_file'] = str(Path(tmp)/'STOP')
            state = waiting_state(); state['updated_at'] = NOW.isoformat()
            state['market']['provider'] = cfg['market']['provider']
            with patch('paperlive.runtime.public_activation', return_value=state['activation']):
                state['ai'] = {'configured': False}
                self.assertIsNone(public_state(cfg, state, {}, now=NOW)['readiness'])
                state['ai'] = {'configured': True}
                self.assertIsNone(public_state(cfg, state, {'reconciliation': {'ok': False}}, now=NOW)['readiness'])

    def test_health_and_auditor_do_not_hide_errors_behind_waiting_market(self):
        valid = {'ok': True, **market_wait_status(waiting_state(), NOW)}
        class Response:
            code = 200
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def read(self): return json.dumps(body).encode()
        for changes, expected in (({'error': 'real download failed'}, 'failed'), ({'err': 'failed'}, 'failed'),
                                  ({'ok': False}, 'failed'), ({'stale': True}, 'stale'),
                                  ({'market': {'error': 'calendar failed'}}, 'failed'),
                                  ({'market': {'provider': {'error': 'current upstream failed'}}}, 'failed'),
                                  ({'ready': True}, 'unknown'), ({'initialized': False}, 'unknown')):
            body = {**valid, **changes}
            with patch('urllib.request.OpenerDirector.open', return_value=Response()):
                row = live_api._probe_one('/api/live/realtime')
            with self.subTest(changes=changes):
                self.assertTrue(row['reachable']); self.assertFalse(row['ok'])
                self.assertEqual(row['state'], expected); self.assertFalse(row['initialized'])
                self.assertEqual(classify('/api/live/realtime', 200, body)[0], expected)
        body = valid
        with patch('urllib.request.OpenerDirector.open', return_value=Response()):
            self.assertEqual(live_api._probe_one('/api/live/factors')['state'], 'unknown')
        self.assertEqual(classify('/api/live/factors', 200, body)[0], 'unknown')
        nested = {'ok': True, 'initialized': False, 'readiness': valid}
        self.assertEqual(classify('/api/live/realtime', 200, nested)[0], 'unknown')

    def test_recovered_ai_configuration_drops_old_configuration_error_only(self):
        from paperlive.runtime import _merge_council_status
        ai = {'configured': False, 'error': 'old provider failure', 'last_error': 'historical plan rejection'}
        _merge_council_status(ai, {'configured': True, 'council': {'enabled': False}})
        self.assertNotIn('error', ai)
        self.assertEqual(ai['last_error'], 'historical plan rejection')
