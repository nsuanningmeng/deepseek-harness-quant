"""Offline credential storage and permission-check privacy regressions."""
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import stat
from tempfile import TemporaryDirectory
import threading
from unittest import TestCase
from unittest.mock import Mock, patch
from zoneinfo import ZoneInfo

import yaml

from autotrade.providers import Records
from paperlive.config import load_config
import paperlive.data_settings as settings

TOKEN_A = 'a' * 56
TOKEN_B = 'b' * 56
TOKEN_ENV = 'e' * 56
ENV_NAME = 'PAPERLIVE_UNIT_TEST_TOKEN'
NOW = datetime(2026, 11, 11, 10, 30, tzinfo=ZoneInfo('Asia/Shanghai'))


class FixedClock(datetime):
    @classmethod
    def now(cls, tz=None):
        return NOW.astimezone(tz) if tz else NOW.replace(tzinfo=None)


class DataSettingsTests(TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.cfg = load_config(Path(__file__).resolve().parents[1] / 'config/paperlive.yaml.example')
        self.cfg['runtime_dir'] = str(self.root / 'runtime')
        self.cfg['tushare']['credentials_file'] = str(self.root / 'private' / 'data-key.yaml')
        self.cfg['tushare']['token_env'] = ENV_NAME
        self.credential_path = Path(self.cfg['tushare']['credentials_file'])
        self.report_path = Path(self.cfg['runtime_dir']) / 'tushare-check.json'
        self.environment = patch.dict(os.environ, {ENV_NAME: ''})
        self.environment.start();self.addCleanup(self.environment.stop)
        self.clock = patch.object(settings, 'datetime', FixedClock)
        self.clock.start();self.addCleanup(self.clock.stop)
        self.sleep = patch.object(settings.time, 'sleep')
        self.sleep.start();self.addCleanup(self.sleep.stop)

    def assert_private(self, public):
        body = json.dumps(public, ensure_ascii=False)
        for secret in (TOKEN_A, TOKEN_B, TOKEN_ENV,
                       hashlib.sha256(TOKEN_A.encode()).hexdigest(), hashlib.sha256(TOKEN_B.encode()).hexdigest()):
            self.assertNotIn(secret, body)
        if isinstance(public.get('last_check'), dict):
            self.assertNotIn('credential_fingerprint', public['last_check'])

    def assert_not_ready(self, public):
        self.assertFalse((public.get('last_check') or {}).get('ok', False))
        self.assert_private(public)

    def success_client(self):
        client = Mock()
        def query(endpoint, *, fields, **params):
            columns = fields.split(',')
            return Records(columns, [{column: 'synthetic-fixture' for column in columns}])
        client.query.side_effect = query
        return client

    def check_with(self, client):
        with patch('autotrade.providers.TushareHttpClient', return_value=client) as factory:
            result = settings.check_connection(self.cfg)
        return result, factory

    def test_atomic_save_has_mode_600_and_never_returns_a_token(self):
        original_replace = os.replace
        observations = []
        def replace(source, destination):
            if Path(destination) == self.credential_path:
                observations.append(Path(source))
                self.assertEqual(Path(source).parent, self.credential_path.parent)
                self.assertNotEqual(Path(source), self.credential_path)
                if os.name != 'nt':
                    self.assertEqual(stat.S_IMODE(Path(source).stat().st_mode), 0o600)
                self.assertEqual(yaml.safe_load(Path(source).read_text()), {ENV_NAME: TOKEN_A})
            return original_replace(source, destination)
        with patch.object(settings.os, 'replace', side_effect=replace):
            result = settings.save_token({'token': '  ' + TOKEN_A + '  ', 'untrusted_url': 'https://example.invalid'}, self.cfg)
        self.assertEqual(len(observations), 1)
        self.assertTrue(all(not path.exists() for path in observations))
        self.assertEqual(yaml.safe_load(self.credential_path.read_text()), {ENV_NAME: TOKEN_A})
        if os.name != 'nt':
            self.assertEqual(stat.S_IMODE(self.credential_path.stat().st_mode), 0o600)
        self.assertEqual(settings.tushare_connection(self.cfg), TOKEN_A)
        self.assertEqual(result['token_source'], 'local_file')
        self.assert_not_ready(result)

    def test_empty_and_malicious_tokens_cannot_create_files(self):
        invalid = [None, '', ' ', 'short', 123, [], {}, '<your-token-here-is-not-real>',
                   TOKEN_A + '\nextra', TOKEN_A + '\x00', TOKEN_A + '\x1b', '$(curl_attacker.invalid)',
                   'a;' + 'b' * 30, 'a/' + 'b' * 30, '中文令牌' * 8, 'z' * 513]
        for value in invalid:
            with self.subTest(value=repr(value)):
                with self.assertRaises(ValueError):
                    settings.save_token({'token': value}, self.cfg)
                self.assertFalse(self.credential_path.exists())
                self.assertFalse(self.report_path.exists())

    def test_local_file_overrides_environment_and_invalid_local_falls_back(self):
        os.environ[ENV_NAME] = TOKEN_ENV
        before = settings.public_settings(self.cfg)
        self.assertEqual(before['token_source'], 'environment')
        self.assertEqual(settings.tushare_connection(self.cfg), TOKEN_ENV)
        saved = settings.save_token({'token': TOKEN_A}, self.cfg)
        self.assertEqual(saved['token_source'], 'local_file')
        self.assertEqual(settings.tushare_connection(self.cfg), TOKEN_A)
        self.credential_path.write_text(yaml.safe_dump({ENV_NAME: TOKEN_A + '\x00'}))
        self.assertEqual(settings.tushare_connection(self.cfg), TOKEN_ENV)
        self.assert_private(settings.public_settings(self.cfg))
        os.environ[ENV_NAME] = TOKEN_ENV + ';invalid'
        with self.assertRaises(ValueError):
            settings.tushare_connection(self.cfg)
        self.assertFalse(settings.public_settings(self.cfg)['configured'])

    def test_replace_failure_preserves_previous_token_report_and_cleans_temporary_file(self):
        settings.save_token({'token': TOKEN_A}, self.cfg)
        report_before = self.report_path.read_bytes()
        original_replace = os.replace
        def fail_credential_only(source, destination):
            if Path(destination) == self.credential_path:
                raise OSError('synthetic atomic replacement failure')
            return original_replace(source, destination)
        with patch.object(settings.os, 'replace', side_effect=fail_credential_only):
            with self.assertRaises(OSError):
                settings.save_token({'token': TOKEN_B}, self.cfg)
        self.assertEqual(settings.tushare_connection(self.cfg), TOKEN_A)
        self.assertEqual(self.report_path.read_bytes(), report_before)
        self.assertEqual(list(self.credential_path.parent.glob('.data-key-*')), [])

    def test_permission_queries_have_explicit_fields_and_resolved_parameters(self):
        settings.save_token({'token': TOKEN_A}, self.cfg)
        client = self.success_client()
        result, factory = self.check_with(client)
        self.assertTrue(result['last_check']['ok'])
        self.assertEqual(factory.call_args.args[:2], (TOKEN_A, self.cfg['market']['request_timeout']))
        self.assertEqual(result['api_origin'], 'https://api.tushare.pro')
        expected = self.cfg['tushare']['required_checks']
        self.assertEqual(client.query.call_count, len(expected))
        for request, spec in zip(client.query.call_args_list, expected):
            self.assertEqual(request.args, (spec['endpoint'],))
            self.assertEqual(request.kwargs['fields'], spec['fields'])
            self.assertTrue(request.kwargs['fields'])
            self.assertFalse(any(isinstance(value, str) and value.startswith('$') for value in request.kwargs.values()))
        report = json.loads(self.report_path.read_text())
        self.assertEqual(report['credential_fingerprint'], hashlib.sha256(TOKEN_A.encode()).hexdigest())
        self.assert_private(result)

    def test_query_failures_and_missing_fields_do_not_expose_raw_errors(self):
        settings.save_token({'token': TOKEN_A}, self.cfg)
        client = Mock()
        client.query.side_effect = RuntimeError('raw upstream exception with ' + TOKEN_A + ' and private details')
        result, _ = self.check_with(client)
        self.assert_not_ready(result)
        self.assertTrue(all(item['phase'] == 'unavailable' for item in result['last_check']['checks']))

        report_body = self.report_path.read_text()
        self.assertNotIn(TOKEN_A, report_body)
        self.assertNotIn('raw upstream exception', report_body)
        client = self.success_client()
        client.query.side_effect = lambda endpoint, **kwargs: Records(['unexpected'], [{'unexpected': 'fixture'}])
        result, _ = self.check_with(client)
        self.assert_not_ready(result)
        self.assertTrue(all(item['phase'] == 'unavailable' for item in result['last_check']['checks']))

    def test_hybrid_checks_metadata_without_paid_rt_and_provider_change_invalidates_result(self):
        self.cfg['market']['provider'] = 'tencent_tushare'
        settings.save_token({'token': TOKEN_A}, self.cfg)
        client = self.success_client()
        result, _ = self.check_with(client)
        self.assertTrue(result['last_check']['ok'])
        self.assertNotIn('rt_k', [item.args[0] for item in client.query.call_args_list])
        self.assertNotIn('rt_k', [item['endpoint'] for item in result['requirements']])
        self.assertIn('单独验收', result['check_scope'])
        self.cfg['market']['provider'] = 'tushare'
        self.assert_not_ready(settings.public_settings(self.cfg))

    def test_empty_responses_do_not_claim_permission_readiness(self):
        settings.save_token({'token': TOKEN_A}, self.cfg)
        client = Mock()
        client.query.side_effect = lambda endpoint, *, fields, **kwargs: Records(fields.split(','), [])
        result, _ = self.check_with(client)
        self.assert_not_ready(result)
        self.assertTrue(all(item['phase'] == 'awaiting_data' and item['rows'] == 0 for item in result['last_check']['checks']))

    def test_saving_new_token_invalidates_a_previously_successful_check(self):
        settings.save_token({'token': TOKEN_A}, self.cfg)
        checked, _ = self.check_with(self.success_client())
        self.assertTrue(checked['last_check']['ok'])
        result = settings.save_token({'token': TOKEN_B}, self.cfg)
        self.assertEqual(settings.tushare_connection(self.cfg), TOKEN_B)
        self.assert_not_ready(result)
        self.assert_not_ready(settings.public_settings(self.cfg))

    def test_environment_token_change_invalidates_persisted_result(self):
        os.environ[ENV_NAME] = TOKEN_A
        checked, _ = self.check_with(self.success_client())
        self.assertTrue(checked['last_check']['ok'])
        os.environ[ENV_NAME] = TOKEN_B
        changed = settings.public_settings(self.cfg)
        self.assertEqual(changed['token_source'], 'environment')
        self.assert_not_ready(changed)
        os.environ[ENV_NAME] = ''
        missing = settings.public_settings(self.cfg)
        self.assertFalse(missing['configured'])
        self.assert_not_ready(missing)

    def test_old_inflight_check_cannot_overwrite_new_token_invalidation(self):
        settings.save_token({'token': TOKEN_A}, self.cfg)
        entered, release = threading.Event(), threading.Event()
        client = self.success_client()
        query = client.query.side_effect
        first = True
        def wait_for_new_credential(endpoint, **kwargs):
            nonlocal first
            if first:
                first = False
                entered.set()
                if not release.wait(timeout=5):
                    raise RuntimeError('test synchronization timed out')
            return query(endpoint, **kwargs)
        client.query.side_effect = wait_for_new_credential
        results, errors = [], []
        def run_check():
            try:
                results.append(settings.check_connection(self.cfg))
            except Exception as exc:
                errors.append(exc)
        with patch('autotrade.providers.TushareHttpClient', return_value=client):
            worker = threading.Thread(target=run_check)
            worker.start()
            try:
                self.assertTrue(entered.wait(timeout=5))
                settings.save_token({'token': TOKEN_B}, self.cfg)
            finally:
                release.set()
                worker.join(timeout=5)
        self.assertFalse(worker.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(len(results), 1)
        self.assertEqual(settings.tushare_connection(self.cfg), TOKEN_B)
        self.assert_not_ready(results[0])
        self.assert_not_ready(settings.public_settings(self.cfg))
        self.assertFalse(json.loads(self.report_path.read_text())['ok'])

    def test_private_report_metadata_is_not_in_public_settings(self):
        settings.save_token({'token': TOKEN_A}, self.cfg)
        settings_report = {'checked_at': NOW.isoformat(), 'ok': True, 'checks': [], 'message': 'fixture',
                           'credential_fingerprint': hashlib.sha256(TOKEN_A.encode()).hexdigest(),
                           'token': TOKEN_A, 'private_transport': 'secret-value'}
        self.report_path.write_text(json.dumps(settings_report))
        result = settings.public_settings(self.cfg)
        self.assertNotIn('token', result['last_check'])
        self.assertNotIn('private_transport', result['last_check'])
        self.assert_private(result)

    def test_parallel_check_request_is_rejected_without_an_extra_client(self):
        settings.save_token({'token': TOKEN_A}, self.cfg)
        with patch('autotrade.providers.TushareHttpClient') as factory:
            self.assertTrue(settings._CHECK_LOCK.acquire(blocking=False))
            try:
                with self.assertRaises(ValueError):
                    settings.check_connection(self.cfg)
            finally:
                settings._CHECK_LOCK.release()
        factory.assert_not_called()

    def test_non_mapping_old_reports_are_omitted_not_echoed(self):
        settings.save_token({'token': TOKEN_A}, self.cfg)
        for value in ([TOKEN_A], TOKEN_A, 123, True, None):
            with self.subTest(value_type=type(value).__name__):
                self.report_path.write_text(json.dumps(value))
                result = settings.public_settings(self.cfg)
                self.assertIsNone(result['last_check'])
                self.assert_private(result)

    def test_empty_required_checks_are_rejected_without_a_ready_report(self):
        settings.save_token({'token': TOKEN_A}, self.cfg)
        before = self.report_path.read_bytes()
        self.cfg['tushare']['required_checks'] = []
        client = self.success_client()
        with patch('autotrade.providers.TushareHttpClient', return_value=client):
            with self.assertRaises(ValueError):
                settings.check_connection(self.cfg)
        client.query.assert_not_called()
        self.assertEqual(self.report_path.read_bytes(), before)
        self.assert_not_ready(settings.public_settings(self.cfg))

    def test_permission_denied_is_distinct_and_never_exposes_raw_secret(self):
        from autotrade.providers import TushareRequestError
        settings.save_token({'token': TOKEN_A}, self.cfg)
        client = self.success_client()
        success = client.query.side_effect
        def query(endpoint, **kwargs):
            if endpoint == 'rt_k':
                failure = TushareRequestError(endpoint, 'permission_denied')
                # A typed error can still carry private diagnostic context.
                failure.args = ('raw upstream response echoed ' + TOKEN_A,)
                raise failure
            return success(endpoint, **kwargs)
        client.query.side_effect = query
        result, _ = self.check_with(client)
        checks = {item['endpoint']: item for item in result['last_check']['checks']}
        self.assertFalse(result['last_check']['ok'])
        self.assertEqual(checks['rt_k']['phase'], 'permission_denied')
        self.assertIn('权限', checks['rt_k']['error'])
        self.assertTrue(all(item['ok'] for endpoint, item in checks.items() if endpoint != 'rt_k'))
        self.assert_private(result)
        stored = self.report_path.read_text()
        self.assertNotIn(TOKEN_A, stored)
        self.assertNotIn('raw upstream response', stored)
