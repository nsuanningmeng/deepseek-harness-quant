import json
import hashlib
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import Mock, patch
from urllib.error import HTTPError, URLError
from zoneinfo import ZoneInfo
import yaml

from paperlive.config import load_config
from paperlive.models import Quote, Instrument, D
from paperlive.planner import AiPlanner, AiRequestError, BoundJsonResponse, _json_digest, validate_bound_json_response


class PlannerTests(TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.cfg = load_config(root / 'isolated-paper.yaml')
        self.cfg['activation']['enabled'] = False
        self.cfg['council'] = {'enabled': False, 'config_file': str(root / 'council.yaml')}
        self.cfg['ai']['settings_file'] = str(root / 'settings.yaml')
        self.cfg['ai']['credentials_file'] = str(root / 'credentials.yaml')
        self.settings = {'agent-default-model': {'provider': 'lemonapi', 'model': 'fixture-model'},
                         'llm-pi-ai': {'providers': {'lemonapi': {'api': 'openai-completions',
                           'baseURL': 'https://new.lemonapi.site/v1', 'apiKeyEnv': 'FIXTURE_KEY'}}}}
        (root / 'settings.yaml').write_text(yaml.safe_dump(self.settings))
        (root / 'credentials.yaml').write_text('FIXTURE_KEY: fixture-only-not-a-secret\n')
        self.now = datetime(2026, 11, 11, 10, 30, tzinfo=ZoneInfo('Asia/Shanghai'))
        self.quote = Quote('600000.SH', '示例证券', *[D(v) for v in ['10', '9.99', '10.01']],
                           10000, 10000, *[D(v) for v in ['10', '10', '10.1', '9.9']],
                           100000, self.now, D('11'), D('9'), received_at=self.now)
        self.instrument = Instrument('600000.SH', '示例证券', 'MAIN', '2000-01-01')

    def propose(self, orders):
        content = json.dumps({'summary': 'fixture', 'orders': orders})
        response = Mock()
        response.geturl.return_value = 'https://new.lemonapi.site/v1/chat/completions'
        response.read.return_value = json.dumps({'choices': [{'message': {'content': content}}]}).encode()
        response.__enter__ = Mock(return_value=response)
        response.__exit__ = Mock(return_value=None)
        opener = Mock()
        opener.open.return_value = response
        with patch('paperlive.planner.build_opener', return_value=opener):
            return AiPlanner(self.cfg).propose({'account': {}, 'positions': []},
                {self.quote.symbol: self.quote}, {self.instrument.symbol: self.instrument},
                {'open': True, 'calendar_verified': True}, self.now)

    def test_no_network_outside_verified_market_session(self):
        with patch('paperlive.planner.build_opener') as network:
            with self.assertRaises(ValueError):
                AiPlanner(self.cfg).propose({}, {}, {}, {'open': False}, self.now)
            network.assert_not_called()

    def test_only_authorized_relay_origin_gets_credentials(self):
        self.settings['llm-pi-ai']['providers']['lemonapi']['baseURL'] = 'https://unapproved.example/v1'
        Path(self.cfg['ai']['settings_file']).write_text(yaml.safe_dump(self.settings))
        with self.assertRaises(ValueError):
            AiPlanner(self.cfg).connection()

    def test_unknown_stock_and_boolean_quantity_are_rejected(self):
        for order in [{'side': 'BUY', 'symbol': '000001.SZ', 'quantity': 100, 'limit_price': '10'},
                      {'side': 'BUY', 'symbol': '600000.SH', 'quantity': True, 'limit_price': '10'},
                      {'side': 'BUY', 'symbol': '600000.SH', 'quantity': 100, 'limit_price': 'NaN'}]:
            with self.subTest(order=order), self.assertRaises(ValueError):
                self.propose([order])

    def test_hold_is_valid_and_never_fabricates_orders(self):
        self.assertEqual(self.propose([])['orders'], [])

    def test_valid_proposal_remains_only_a_proposal(self):
        order = {'side': 'BUY', 'symbol': '600000.SH', 'quantity': 100, 'limit_price': '10'}
        self.assertEqual(self.propose([order])['orders'], [order])

    def test_connection_probe_sends_no_account_and_rejects_nonempty_orders(self):
        for orders in ([], [{'side':'BUY','symbol':'600000.SH','quantity':100,'limit_price':'10'}]):
            response=Mock()
            response.geturl.return_value='https://new.lemonapi.site/v1/chat/completions'
            response.read.return_value=json.dumps({'choices':[{'message':{'content':json.dumps({'summary':'probe','orders':orders})}}]}).encode()
            response.__enter__=Mock(return_value=response)
            response.__exit__=Mock(return_value=None)
            opener=Mock()
            opener.open.return_value=response
            with patch('paperlive.planner.build_opener',return_value=opener):
                if orders:
                    with self.assertRaises(ValueError):AiPlanner(self.cfg).verify_connection()
                else:self.assertTrue(AiPlanner(self.cfg).verify_connection()['ok'])
            payload=json.loads(opener.open.call_args.args[0].data)
            self.assertNotIn('account',payload)
            self.assertNotIn('candidates',payload)
            self.assertEqual(payload['max_tokens'],self.cfg['activation']['ai_probe_max_tokens'])

    def test_transport_rejects_truncated_tool_or_redirected_response(self):
        for changes in ({'finish_reason': 'length'}, {'tool_calls': [{'type': 'function'}]},
                        {'target': 'https://unapproved.example/chat/completions'}):
            with self.subTest(changes=changes):
                choice = {'message': {'content': '{"summary":"fixture","orders":[]}'}}
                if 'tool_calls' in changes:
                    choice['message']['tool_calls'] = changes['tool_calls']
                if 'finish_reason' in changes:
                    choice['finish_reason'] = changes['finish_reason']
                response = Mock()
                response.geturl.return_value = changes.get('target', 'https://new.lemonapi.site/v1/chat/completions')
                response.read.return_value = json.dumps({'choices': [choice]}).encode()
                response.__enter__ = Mock(return_value=response)
                response.__exit__ = Mock(return_value=None)
                opener = Mock(); opener.open.return_value = response
                with patch('paperlive.planner.build_opener', return_value=opener), self.assertRaises(ValueError):
                    planner = AiPlanner(self.cfg)
                    planner._request_json('fixture', {}, connection=planner.connection(), max_tokens=20, timeout=1)

    def test_changed_connection_after_request_is_rejected_without_echoing_credential(self):
        response = Mock()
        response.geturl.return_value = 'https://new.lemonapi.site/v1/chat/completions'
        response.read.return_value = json.dumps({'choices': [{'message': {'content': '{"orders":[]}'}}]}).encode()
        response.__enter__ = Mock(return_value=response); response.__exit__ = Mock(return_value=None)
        opener = Mock(); opener.open.return_value = response
        planner = AiPlanner(self.cfg)
        original = ('https://new.lemonapi.site/v1', 'before-fixture', 'fixture-model')
        changed = ('https://new.lemonapi.site/v1', 'after-fixture', 'fixture-model')
        with patch('paperlive.planner.build_opener', return_value=opener), patch.object(planner, 'connection', side_effect=[original, changed]):
            with self.assertRaises(ValueError) as caught:
                planner._request_json('fixture', {}, connection=original, max_tokens=20, timeout=1)
        self.assertNotIn('fixture', str(caught.exception))

    def test_response_format_forwarded_exactly_and_optional_for_legacy(self):
        schema = {'type': 'json_schema', 'json_schema': {'name': 'fixture', 'strict': True,
                  'schema': {'type': 'object', 'properties': {}, 'required': [], 'additionalProperties': False}}}
        for response_format in (None, schema):
            with self.subTest(response_format=response_format):
                response = Mock()
                response.geturl.return_value = 'https://new.lemonapi.site/v1/chat/completions'
                response.read.return_value = json.dumps({'choices': [{'message': {'content': '{}'}}]}).encode()
                response.__enter__ = Mock(return_value=response); response.__exit__ = Mock(return_value=None)
                opener = Mock(); opener.open.return_value = response
                planner = AiPlanner(self.cfg)
                with patch('paperlive.planner.build_opener', return_value=opener):
                    result = planner._request_json('fixture', {}, connection=planner.connection(),
                        max_tokens=20, timeout=1, response_format=response_format)
                self.assertEqual(result, {})
                payload = json.loads(opener.open.call_args.args[0].data)
                if response_format is None:
                    self.assertNotIn('response_format', payload)
                else:
                    self.assertEqual(payload['response_format'], schema)

    def test_provider_rejecting_schema_does_not_retry_or_downgrade(self):
        opener = Mock()
        opener.open.side_effect = HTTPError('https://new.lemonapi.site/v1/chat/completions', 400,
                                            'fixture unsupported schema', {}, None)
        planner = AiPlanner(self.cfg)
        with patch('paperlive.planner.build_opener', return_value=opener), self.assertRaises(ValueError):
            planner._request_json('fixture', {}, connection=planner.connection(), max_tokens=20, timeout=1,
                                  response_format={'type': 'json_schema'})
        self.assertEqual(opener.open.call_count, 1)

    def test_bound_transport_records_exact_public_request_and_original_content(self):
        content = '  {"summary": "fixture", "proposals": []}\n'
        response = Mock()
        response.geturl.return_value = 'https://new.lemonapi.site/v1/chat/completions'
        response.read.return_value = json.dumps({'choices': [{'finish_reason': 'stop',
            'message': {'content': content}}]}).encode()
        response.__enter__ = Mock(return_value=response); response.__exit__ = Mock(return_value=None)
        opener = Mock(); opener.open.return_value = response
        planner = AiPlanner(self.cfg)
        binding = {'request_id': 'a' * 32, 'context_id': 'b' * 64, 'stage': 'analysis'}
        data = {'input': {'now': self.now.isoformat()}}
        schema = {'type': 'json_schema'}
        with patch('paperlive.planner.build_opener', return_value=opener):
            result = planner._request_json('fixture-system', data, connection=planner.connection(),
                max_tokens=100, timeout=5, response_format=schema, binding=binding)
        self.assertIsInstance(result, BoundJsonResponse)
        self.assertEqual(result.raw_content, content)
        actual_payload = json.loads(opener.open.call_args.args[0].data)
        self.assertEqual(result.receipt['request_sha256'], _json_digest(actual_payload))
        self.assertEqual(result.receipt['binding'], binding)
        self.assertNotIn('fixture-only-not-a-secret', json.dumps(result.receipt))
        self.assertEqual(validate_bound_json_response(result, 'fixture-system', data,
            binding=binding, provider=self.cfg['ai']['provider'], max_tokens=100,
            response_format=schema), {'summary': 'fixture', 'proposals': []})
        # Changing the request association is a hard error, even with the same
        # successful business response; there is no identity repair/retry.
        with self.assertRaises(ValueError):
            validate_bound_json_response(result, 'fixture-system', data,
                binding={**binding, 'request_id': 'c' * 32}, provider=self.cfg['ai']['provider'],
                max_tokens=100, response_format=schema)
        self.assertEqual(opener.open.call_count, 1)

    def test_safe_transport_categories_do_not_expose_error_detail(self):
        for error, category, status in [
                (HTTPError('https://sensitive-fixture.invalid', 429, 'sensitive-fixture-body', {}, None), 'http_error', 429),
                (TimeoutError('sensitive-fixture-timeout'), 'timeout', None),
                (URLError(TimeoutError('sensitive-fixture-timeout')), 'timeout', None),
                (URLError('sensitive-fixture-host'), 'network_error', None),
                (OSError('sensitive-fixture-path'), 'transport_failure', None)]:
            with self.subTest(category=category):
                opener = Mock(); opener.open.side_effect = error
                planner = AiPlanner(self.cfg)
                with patch('paperlive.planner.build_opener', return_value=opener), self.assertRaises(AiRequestError) as caught:
                    planner._request_json('fixture', {}, connection=planner.connection(), max_tokens=2000, timeout=45)
                diagnostic = caught.exception.transport_diagnostics
                self.assertEqual(diagnostic['category'], category)
                self.assertEqual(diagnostic.get('http_status'), status)
                self.assertNotIn('sensitive-fixture', str(caught.exception) + json.dumps(diagnostic))
                self.assertEqual(opener.open.call_count, 1)

    def test_truncated_completion_records_only_usage_and_lengths(self):
        raw = json.dumps({'usage': {'prompt_tokens': 1234, 'completion_tokens': 2000,
                           'completion_tokens_details': {'reasoning_tokens': 1800}},
                          'choices': [{'finish_reason': 'length', 'message': {
                              'content': 'sensitive-fixture-content', 'reasoning_content': 'sensitive-fixture-reasoning'}}]}).encode()
        response = Mock(); response.status = 200
        response.geturl.return_value = 'https://new.lemonapi.site/v1/chat/completions'
        response.read.return_value = raw
        response.__enter__ = Mock(return_value=response); response.__exit__ = Mock(return_value=None)
        opener = Mock(); opener.open.return_value = response
        planner = AiPlanner(self.cfg)
        with patch('paperlive.planner.build_opener', return_value=opener), self.assertRaises(AiRequestError) as caught:
            planner._request_json('fixture', {}, connection=planner.connection(), max_tokens=2000, timeout=45)
        diagnostic = caught.exception.transport_diagnostics
        self.assertEqual(diagnostic['category'], 'completion_truncated')
        self.assertEqual(diagnostic['http_status'], 200)
        self.assertEqual(diagnostic['finish_reason'], 'length')
        self.assertEqual(diagnostic['response_bytes'], len(raw))
        self.assertEqual(diagnostic['completion_tokens'], 2000)
        self.assertEqual(diagnostic['reasoning_tokens'], 1800)
        self.assertNotIn('sensitive-fixture', json.dumps(diagnostic))

    def test_json_parse_diagnostics_distinguish_provider_and_model_without_body(self):
        for raw, phase in [(b'{"sensitive-fixture":', 'provider_json'),
                           (json.dumps({'choices': [{'finish_reason': 'stop', 'message': {'content': '{"sensitive-fixture":'}}]}).encode(), 'model_json')]:
            response = Mock()
            response.geturl.return_value = 'https://new.lemonapi.site/v1/chat/completions'
            response.read.return_value = raw
            response.__enter__ = Mock(return_value=response); response.__exit__ = Mock(return_value=None)
            opener = Mock(); opener.open.return_value = response
            planner = AiPlanner(self.cfg)
            with patch('paperlive.planner.build_opener', return_value=opener), self.assertRaises(AiRequestError) as caught:
                planner._request_json('fixture', {}, connection=planner.connection(), max_tokens=2000, timeout=45)
            diagnostic = caught.exception.transport_diagnostics
            self.assertEqual(diagnostic['category'], 'invalid_json')
            self.assertEqual(diagnostic['phase'], phase)
            self.assertIsInstance(diagnostic['json_position'], int)
            self.assertNotIn('sensitive-fixture', json.dumps(diagnostic))
            if phase == 'provider_json':
                for key in ('json_error_code', 'parsed_content_chars', 'raw_content_sha256'):
                    self.assertNotIn(key, diagnostic)

    def test_model_json_error_positions_use_unfenced_text_but_digest_original_content(self):
        body = '{"summary":"sensitive-fixture-中文","proposals":[}'
        with self.assertRaises(json.JSONDecodeError) as expected:
            json.loads(body)
        for content in (body, ' \n' + body + '\t ', ' \n```json\n' + body + '\n```\t '):
            with self.subTest(content_shape='fenced' if '```' in content else 'plain'):
                response = Mock(); response.status = 200
                response.geturl.return_value = 'https://new.lemonapi.site/v1/chat/completions'
                response.read.return_value = json.dumps({'choices': [{'finish_reason': 'stop',
                    'message': {'content': content}}]}, ensure_ascii=False).encode()
                response.__enter__ = Mock(return_value=response); response.__exit__ = Mock(return_value=None)
                opener = Mock(); opener.open.return_value = response
                planner = AiPlanner(self.cfg)
                with patch('paperlive.planner.build_opener', return_value=opener), self.assertRaises(AiRequestError) as caught:
                    planner._request_json('fixture', {}, connection=planner.connection(), max_tokens=2000, timeout=60)
                diagnostic = caught.exception.transport_diagnostics
                self.assertEqual(diagnostic['category'], 'invalid_json')
                self.assertEqual(diagnostic['phase'], 'model_json')
                self.assertEqual(diagnostic['http_status'], 200)
                self.assertEqual(diagnostic['finish_reason'], 'stop')
                self.assertEqual(diagnostic['json_error_code'], 'expected_value')
                self.assertEqual(diagnostic['parsed_content_chars'], len(body))
                self.assertEqual(diagnostic['content_chars'], len(content))
                self.assertEqual(diagnostic['json_position'], expected.exception.pos)
                self.assertEqual(diagnostic['json_line'], expected.exception.lineno)
                self.assertEqual(diagnostic['json_column'], expected.exception.colno)
                self.assertEqual(diagnostic['raw_content_sha256'], hashlib.sha256(content.encode()).hexdigest())
                self.assertNotIn('sensitive-fixture', str(caught.exception) + json.dumps(diagnostic))
                self.assertNotIn('中文', json.dumps(diagnostic, ensure_ascii=False))
                self.assertEqual(opener.open.call_count, 1)
