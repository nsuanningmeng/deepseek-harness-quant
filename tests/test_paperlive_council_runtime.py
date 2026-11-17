"""Synthetic two-role decisions through the real runner and temporary paper ledger.

Only the market transport, model transport and executor/clock are replaced. The
planner, council validation, runtime submission and ledger accounting stay real.
No local provider settings, credentials, research stores or external APIs are read.
"""
from concurrent.futures import Future
from contextlib import nullcontext
from copy import deepcopy
from datetime import datetime, timedelta
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

import yaml

from paperlive.config import load_config
from paperlive.ledger import Ledger
from paperlive.models import D
from paperlive.planner import AiPlanner, bind_json_response
import paperlive.runtime as runtime
from tests.test_paperlive_ledger import START, instrument, quote


class SyntheticClock(datetime):
    value = START

    @classmethod
    def now(cls, tz=None):
        return cls.value.astimezone(tz) if tz else cls.value.replace(tzinfo=None)


class InlineExecutor:
    """A completed future still reaches runtime on its next market iteration."""

    def __init__(self, **kwargs):
        self.futures = []

    def submit(self, function, *args, **kwargs):
        future = Future()
        try:
            future.set_result(function(*args, **kwargs))
        except Exception as exc:
            future.set_exception(exc)
        self.futures.append(future)
        return future

    def shutdown(self, **kwargs):
        pass


class FinishedIterations(Exception):
    pass


class CouncilRuntimeTests(TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.cfg = load_config(self.root / 'absent-config.yaml')
        self.cfg['runtime_dir'] = str(self.root / 'account')
        self.cfg['risk'].update(kill_switch_file=str(self.root / 'STOP'),
                               max_symbol_weight=.25, max_total_weight=.95,
                               max_order_notional=12500, max_drawdown=.20)
        self.cfg['broker']['initial_cash'] = 50000
        self.cfg['market'].update(provider='tencent_baostock', source_label='Synthetic test feed')
        self.cfg['activation']['enabled'] = False
        self.cfg['ai']['settings_file'] = str(self.root / 'absent-settings.yaml')
        self.cfg['ai']['credentials_file'] = str(self.root / 'absent-credentials.yaml')
        self.catalog = self.root / 'catalog.yaml'
        self.catalog.write_text(yaml.safe_dump({'schema_version': 1, 'personas': [
            {'id': 'fixture_analyst', 'name': 'Synthetic analyst', 'tag': 'research', 'style': 'Use provided evidence'},
            {'id': 'fixture_reviewer', 'name': 'Synthetic reviewer', 'tag': 'review', 'style': 'Verify every proposal'},
        ]}))
        policy = self.root / 'council.yaml'
        policy.write_text(yaml.safe_dump({'schema_version': 1, 'enabled': True,
            'catalog_file': str(self.catalog), 'analyst_id': 'fixture_analyst', 'reviewer_id': 'fixture_reviewer',
            'max_decisions': 3, 'max_response_tokens': 1200, 'max_duration_seconds': 80}))
        self.cfg['council'] = {'enabled': True, 'config_file': str(policy)}
        self.security = instrument()
        self.proposal = {'proposal_id': 'P1', 'side': 'BUY', 'symbol': self.security.symbol,
                         'quantity': 1200, 'limit_price': '10.05', 'reason': 'Synthetic quote evidence',
                         'evidence_refs': ['quote:' + self.security.symbol]}
        self.review = {'proposal_id': 'P1', 'decision': 'approve', 'reason': 'Review of supplied quote',
                       'evidence_refs': ['quote:' + self.security.symbol]}
        self.calls, self.states = [], []
        self.executor = InlineExecutor()
        self.planner = AiPlanner(self.cfg)
        SyntheticClock.value = START

    def _response(self, system, data, **kwargs):
        self.calls.append(deepcopy(data))
        output = {'summary': 'Synthetic decision'}
        output['reviews' if 'analysis' in data else 'proposals'] = deepcopy(
            [self.review] if 'analysis' in data else [self.proposal])
        return bind_json_response(system, data, output, binding=kwargs['binding'],
                                  model='synthetic-model', provider=self.cfg['ai']['provider'],
                                  max_tokens=kwargs['max_tokens'], timeout=kwargs['timeout'],
                                  response_format=kwargs.get('response_format'))

    def _quotes(self, symbols):
        return {self.security.symbol: quote(SyntheticClock.value, self.security, size=20000)}

    def _run(self, *, iterations=3, after_first=None, first_delay=10, responder=None, gate_evaluate=None):
        feed = SimpleNamespace(status={'universe': {'cache_stale': False}},
            universe=lambda: {self.security.symbol: self.security}, lookup=lambda symbols: {},
            quotes=self._quotes, session=lambda now: {'open': True, 'calendar_verified': True,
                'date': now.date().isoformat(), 'phase': 'Synthetic continuous session', 'next_open': None})

        def advance(delay):
            self.states.append(json.loads((Path(self.cfg['runtime_dir']) / 'state.json').read_text()))
            if len(self.states) >= iterations:
                raise FinishedIterations()
            if len(self.states) == 1 and after_first:
                after_first()
            SyntheticClock.value += timedelta(seconds=first_delay if len(self.states) == 1 else delay)

        with (patch('paperlive.market.MarketFeed', return_value=feed),
              patch.object(self.planner, 'connection', return_value=('https://synthetic.invalid/v1', 'synthetic-key', 'synthetic-model')),
              patch.object(self.planner, '_request_json', side_effect=responder or self._response),
              patch.object(runtime, 'AiPlanner', return_value=self.planner),
              patch.object(runtime, 'ThreadPoolExecutor', return_value=self.executor),
              patch.object(runtime, 'datetime', SyntheticClock),
              patch('paperlive.ledger._now', side_effect=lambda: SyntheticClock.value),
              patch.object(runtime.time, 'time', side_effect=lambda: SyntheticClock.value.timestamp()),
              patch.object(runtime.time, 'sleep', side_effect=advance),
              patch.object(runtime.ActivationGate, 'evaluate', side_effect=gate_evaluate) if gate_evaluate else nullcontext()):
            with self.assertRaises(FinishedIterations):
                runtime.run(self.cfg)
            snapshot = Ledger(self.cfg, existing_only=True).snapshot(self._quotes([]), now=SyntheticClock.value)
        self.assertEqual(len(self.executor.futures), 1, 'No unexpected probe or repeated model plan')
        plans = sorted((Path(self.cfg['runtime_dir']) / 'plans').glob('*.json'))
        self.assertEqual(len(plans), 1, 'Every completed council plan must have one execution trace')
        self.plan = json.loads(plans[0].read_text())
        self.state = self.states[-1]
        self.snapshot = snapshot
        self.assertTrue(snapshot['reconciliation']['ok'])
        self.assertEqual(snapshot['started_at'], START.isoformat())
        return snapshot

    def _assert_no_execution(self):
        self.assertEqual(self.snapshot['orders'], [])
        self.assertEqual(self.snapshot['fills'], [])
        self.assertEqual(self.snapshot['positions'], [])
        self.assertEqual(D(self.snapshot['account']['cash']), D(50000))
        self.assertEqual(self.plan['execution'], 'not_executed')
        self.assertEqual(self.plan.get('accepted', []), [])
        trace = self.state['ai']['council']['latest_trace']
        self.assertEqual(trace['execution_status'], 'not_executed')
        self.assertTrue(trace['execution_error'])

    def test_dual_approved_plan_is_risk_checked_then_filled_on_later_quote(self):
        snapshot = self._run()
        self.assertEqual(len(self.calls), 2)
        self.assertEqual(self.calls[0]['input'], self.calls[1]['input'])
        self.assertEqual(self.calls[0]['context_id'], self.calls[1]['context_id'])
        self.assertEqual(D(self.calls[0]['input']['account']['cash']), D(50000))
        self.assertEqual(self.calls[0]['input']['risk']['max_order_notional'], 12500)
        self.assertEqual(snapshot['orders'][0]['status'], 'FILLED')
        self.assertEqual(snapshot['orders'][0]['quantity'], 1200)
        self.assertEqual(len(snapshot['fills']), 1)
        fill = snapshot['fills'][0]
        self.assertGreater(datetime.fromisoformat(fill['executed_at']), datetime.fromisoformat(snapshot['orders'][0]['created_at']))
        self.assertEqual(D(snapshot['account']['cash']) + D(fill['price']) * fill['quantity'] + D(fill['fees']), D(50000))
        self.assertGreater(D(fill['fees']), 0)
        self.assertEqual(snapshot['positions'][0]['available'], 0, 'Same-day purchased shares remain subject to T+1')
        self.assertEqual(self.plan['execution'], 'risk_processed')
        self.assertEqual(self.plan['council']['approved_proposal_ids'], ['P1'])
        self.assertEqual(len(self.plan['accepted']), 1)
        self.assertEqual(self.plan['rejected'], [])
        public = runtime.public_state(self.cfg, self.state, snapshot, now=SyntheticClock.value)
        self.assertTrue(public['risk_policy']['effective'])
        self.assertEqual(public['risk_policy']['max_symbol_weight'], .25)
        self.assertEqual(public['risk_policy']['max_total_weight'], .95)
        self.assertEqual(public['risk_policy']['max_order_notional'], 12500)
        self.assertEqual(public['risk_policy']['max_drawdown'], .20)

    def test_reviewer_veto_never_reaches_ledger_submission(self):
        self.review['decision'] = 'reject'
        snapshot = self._run()
        self.assertEqual(len(self.calls), 2)
        self.assertEqual(snapshot['orders'], [])
        self.assertEqual(snapshot['fills'], [])
        self.assertEqual(self.plan['council']['status'], 'review_rejected')
        self.assertEqual(self.plan['orders'], [])
        self.assertEqual(self.plan['accepted'], [])
        self.assertEqual(D(snapshot['account']['cash']), D(50000))

    def test_pending_order_survives_quote_gap_then_fills_only_after_recovery(self):
        observed = []
        def evaluate(before, *args):
            gap = len(self.states) == 2
            observed.append(deepcopy(before))
            return {'enabled': True, 'armed': True, 'ready': not gap,
                    'tradable_symbols': [self.security.symbol],
                    'checks': [{'id': key, 'ok': not (gap and key == 'quotes')}
                               for key in runtime.ACTIVATION_CHECK_IDS]}
        snapshot = self._run(iterations=5, gate_evaluate=evaluate)
        self.assertTrue(self.states[2]['market']['execution_hold']['active'])
        self.assertFalse(self.states[3]['market']['execution_hold']['active'])
        self.assertEqual(observed[2]['orders'][0]['status'], 'OPEN')
        self.assertEqual(observed[3]['orders'][0]['status'], 'OPEN')
        self.assertEqual(observed[2]['account']['frozen_cash'], observed[3]['account']['frozen_cash'])
        self.assertGreater(D(observed[2]['account']['frozen_cash']), 0)
        self.assertEqual(observed[3]['fills'], [])
        self.assertEqual(snapshot['orders'][0]['status'], 'FILLED')
        self.assertEqual(len(snapshot['fills']), 1)
        self.assertEqual(snapshot['positions'][0]['available'], 0)
        self.assertEqual(len(self.calls), 2, 'Quote recovery must not generate another AI round')

    def test_quote_gap_when_ai_returns_still_blocks_new_submission(self):
        def evaluate(*args):
            gap = len(self.states) == 1
            return {'enabled': True, 'armed': True, 'ready': not gap,
                    'tradable_symbols': [self.security.symbol],
                    'checks': [{'id': key, 'ok': not (gap and key == 'quotes')}
                               for key in runtime.ACTIVATION_CHECK_IDS]}
        self._run(gate_evaluate=evaluate)
        self._assert_no_execution()
        self.assertEqual(self.plan['council']['status'], 'approved')
        self.assertEqual(len(self.calls), 2, 'Rejected stale round must not be retried after recovery')

    def test_two_role_approval_cannot_override_ledger_notional_limit(self):
        self.proposal['quantity'] = 1300
        snapshot = self._run()
        self.assertEqual(self.plan['council']['status'], 'approved')
        self.assertEqual(snapshot['orders'][0]['status'], 'REJECTED')
        self.assertEqual(snapshot['orders'][0]['reason'], 'max_order_notional')
        self.assertEqual(snapshot['fills'], [])
        self.assertEqual(snapshot['positions'], [])
        self.assertEqual(D(snapshot['account']['cash']), D(50000))
        self.assertEqual(self.plan['accepted'], [])
        self.assertEqual(self.plan['rejected'][0]['reason'], 'max_order_notional')

    def test_expired_approved_plan_is_saved_without_submission(self):
        self._run(first_delay=self.cfg['strategy']['max_plan_age_seconds'] + 1)
        self._assert_no_execution()
        self.assertEqual(self.plan['council']['approved_proposal_ids'], ['P1'])

    def test_catalog_change_before_submission_invalidates_approved_plan(self):
        def change_catalog():
            catalog = yaml.safe_load(self.catalog.read_text())
            catalog['personas'][1]['style'] = 'Changed review policy'
            self.catalog.write_text(yaml.safe_dump(catalog))
        self._run(after_first=change_catalog)
        self._assert_no_execution()
        self.assertFalse(self.state['ai']['council']['latest_trace_current_config'])

    def test_disabling_council_does_not_execute_its_inflight_plan(self):
        self._run(after_first=lambda: self.cfg['council'].update(enabled=False))
        self._assert_no_execution()
        self.assertFalse(self.state['ai']['council']['enabled'])

    def test_pause_arriving_while_council_runs_prevents_submission(self):
        stop = Path(self.cfg['risk']['kill_switch_file'])
        self._run(after_first=lambda: stop.write_text('Synthetic user pause'))
        self._assert_no_execution()
        self.assertEqual(stop.read_text(), 'Synthetic user pause')

    def test_incomplete_review_preserves_analysis_but_never_submits(self):
        def failing_review(system, data, **kwargs):
            if 'analysis' in data:
                raise TimeoutError('Synthetic reviewer timeout')
            return self._response(system, data, **kwargs)
        self._run(responder=failing_review)
        self._assert_no_execution()
        self.assertEqual(self.plan['council']['status'], 'failed')
        self.assertIsNotNone(self.plan['council']['analyst'])
        self.assertIsNone(self.plan['council']['reviewer'])
        self.assertEqual(self.state['ai']['phase'], 'AI计划失败，等待重试')
        self.assertTrue(self.state['ai']['last_error'])

    def test_public_risk_effective_requires_current_runner_binding(self):
        snapshot = self._run()
        raw = deepcopy(self.state)
        raw['execution_config_fingerprint'] = 'different-runner-policy'
        public = runtime.public_state(self.cfg, raw, snapshot, now=SyntheticClock.value)
        self.assertFalse(public['risk_policy']['effective'])
        self.assertEqual(public['risk_policy']['max_order_notional'], 12500)
        public = runtime.public_state(self.cfg, self.state, snapshot,
                                      now=SyntheticClock.value + timedelta(hours=1))
        self.assertFalse(public['risk_policy']['effective'])
