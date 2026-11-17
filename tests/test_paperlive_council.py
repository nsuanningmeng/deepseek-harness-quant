import json
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch
from zoneinfo import ZoneInfo

import yaml

from paperlive.config import load_config
from paperlive.council import CouncilError, ReviewCouncil, config_fingerprint, describe_policy, validate_execution_plan
from paperlive.models import D, Instrument, Quote
from paperlive.planner import AiPlanner, AiRequestError, BoundJsonResponse, bind_json_response


class CouncilTests(TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.cfg = load_config(self.root / 'absent-paper.yaml')
        self.cfg['activation']['enabled'] = False
        self.cfg['ai']['settings_file'] = str(self.root / 'settings.yaml')
        self.cfg['ai']['credentials_file'] = str(self.root / 'credentials.yaml')
        self.catalog = self.root / 'catalog.yaml'
        self.catalog.write_text(yaml.safe_dump({'schema_version': 1, 'personas': [
            {'id': 'analyst_fixture', 'name': '分析角色', 'tag': '研究', 'style': '依据当前输入研究'},
            {'id': 'reviewer_fixture', 'name': '复核角色', 'tag': '审查', 'style': '逐条核实证据'},
        ]}, allow_unicode=True))
        self.policy = self.root / 'council.yaml'
        self.policy.write_text(yaml.safe_dump({'schema_version': 1, 'enabled': True,
            'catalog_file': str(self.catalog), 'analyst_id': 'analyst_fixture', 'reviewer_id': 'reviewer_fixture',
            'max_decisions': 3, 'max_response_tokens': 1200, 'max_duration_seconds': 80}))
        self.cfg['council'] = {'enabled': True, 'config_file': str(self.policy)}
        self.now = datetime(2026, 11, 12, 10, 30, tzinfo=ZoneInfo('Asia/Shanghai'))
        self.snapshot = {'now': self.now.isoformat(), 'candidates': [{'symbol': '600000.SH'}],
                         'positions': [{'symbol': '000001.SZ', 'sellable_quantity': 100}],
                         'risk': deepcopy(self.cfg['risk']), 'session': {'open': True, 'calendar_verified': True},
                         'research': {'sources': [{'step': 'signal', 'path': 'fixture.json', 'data_date': '2026-11-11'}]}}
        self.proposal = {'proposal_id': 'P1', 'side': 'BUY', 'symbol': '600000.SH', 'quantity': 100,
                         'limit_price': '10.01', 'reason': '已提供报价与研究证据',
                         'evidence_refs': ['quote:600000.SH', 'research:signal']}
        self.review = {'proposal_id': 'P1', 'decision': 'approve', 'reason': '输入支持且仍受程序风控',
                       'evidence_refs': ['quote:600000.SH']}
        self.calls = []

    def responder(self, proposals=None, reviews=None, mutation=None):
        proposals = [self.proposal] if proposals is None else proposals
        reviews = [self.review] if reviews is None else reviews
        def respond(system, data, **budget):
            self.calls.append((system, deepcopy(data), budget))
            out = {'summary': 'fixture summary'}
            out['reviews' if 'analysis' in data else 'proposals'] = deepcopy(reviews if 'analysis' in data else proposals)
            if mutation:
                mutation(out, data)
            return bind_json_response(system, data, out, binding=budget['binding'],
                provider=self.cfg['ai']['provider'], model='fixture-model', max_tokens=budget['max_tokens'],
                timeout=budget['timeout'], response_format=budget['response_format'])
        return respond

    def run_plan(self, **kwargs):
        return ReviewCouncil(self.cfg).run(self.snapshot, self.responder(**kwargs))

    def format_retry_policy(self, value):
        policy = yaml.safe_load(self.policy.read_text())
        policy['max_format_retries'] = value
        self.policy.write_text(yaml.safe_dump(policy))
        self.cfg['ai']['timeout_seconds'] = 60

    @staticmethod
    def malformed_json_error(category='invalid_json', **changes):
        diagnostics = {'phase': 'model_json', 'http_status': 200, 'finish_reason': 'stop',
                       'max_tokens': 1200, 'timeout_seconds': 60, 'content_chars': 200,
                       'json_position': 180, 'json_line': 5, 'json_column': 3}
        diagnostics.update(changes)
        return AiRequestError(category, diagnostics)

    def scripted_role_requests(self, steps):
        clock, attempts = [0], []
        pending = iter(steps)
        underlying = self.responder()
        def request(system, data, **budget):
            attempts.append({'system': system, 'data': deepcopy(data), 'budget': deepcopy(budget)})
            step = next(pending)
            clock[0] += step.get('seconds', 0)
            if step.get('change'):
                step['change']()
            if 'error' in step:
                raise step['error']
            return underlying(system, data, **budget)
        return request, attempts, clock

    def relay_responder(self, *, wrapped_roles=('analyst', 'reviewer'), proposals=None,
                        reviews=None, transform=None):
        underlying = self.responder(proposals=proposals, reviews=reviews)
        self.relay_responses = []
        def respond(system, data, **budget):
            original = underlying(system, data, **budget)
            stage = 'reviewer' if 'analysis' in data else 'analyst'
            name = budget['response_format']['json_schema']['name']
            value = {name: original.value} if stage in wrapped_roles else original.value
            if transform:
                value = transform(value, stage, name)
            response = bind_json_response(system, data, value, binding=budget['binding'],
                provider=self.cfg['ai']['provider'], model='fixture-model', max_tokens=budget['max_tokens'],
                timeout=budget['timeout'], response_format=budget['response_format'],
                raw_content=json.dumps(value, ensure_ascii=False, indent=2))
            self.relay_responses.append(response)
            return response
        return respond

    def test_shared_context_and_exact_approved_order(self):
        plan = self.run_plan()
        self.assertEqual(len(self.calls), 2)
        self.assertEqual(self.calls[0][1]['context_id'], self.calls[1][1]['context_id'])
        self.assertEqual(self.calls[0][1]['input'], self.calls[1][1]['input'])
        self.assertEqual(plan['orders'][0]['quantity'], 100)
        self.assertEqual(plan['council']['approved_proposal_ids'], ['P1'])
        self.assertTrue(validate_execution_plan(plan, self.cfg))
        self.assertIn('分析角色', self.calls[0][0])
        self.assertIn('复核角色', self.calls[1][0])

    def test_reviewer_veto_removes_original_order(self):
        rejected = {**self.review, 'decision': 'reject', 'evidence_refs': []}
        plan = self.run_plan(reviews=[rejected])
        self.assertEqual(plan['orders'], [])
        self.assertEqual(plan['council']['status'], 'review_rejected')
        self.assertTrue(validate_execution_plan(plan, self.cfg))

    def test_empty_analysis_is_valid_hold_after_both_roles(self):
        plan = self.run_plan(proposals=[], reviews=[])
        self.assertEqual(len(self.calls), 2)
        self.assertEqual(plan['orders'], [])
        self.assertEqual(plan['council']['status'], 'hold')

    def test_exact_relay_wrapper_or_bare_body_preserves_raw_transport_for_both_roles(self):
        for wrapped in ((), ('analyst',), ('reviewer',), ('analyst', 'reviewer')):
            for scenario in ('probe', 'hold', 'approved'):
                with self.subTest(wrapped=wrapped, scenario=scenario):
                    self.calls.clear()
                    has_orders = scenario == 'approved'
                    callback = self.relay_responder(wrapped_roles=wrapped,
                        proposals=[self.proposal] if has_orders else [],
                        reviews=[self.review] if has_orders else [])
                    snapshot = {'now': self.now.isoformat()} if scenario == 'probe' else self.snapshot
                    plan = ReviewCouncil(self.cfg).run(snapshot, callback, verification_only=scenario == 'probe')
                    self.assertEqual(len(self.calls), 2)
                    self.assertEqual(plan['council']['status'], 'approved' if has_orders else 'hold')
                    self.assertEqual(len(plan['orders']), int(has_orders))
                    if scenario == 'probe':
                        with self.assertRaises(ValueError):
                            validate_execution_plan(plan, self.cfg)
                    else:
                        self.assertTrue(validate_execution_plan(plan, self.cfg))
                    for index, stage in enumerate(('analyst', 'reviewer')):
                        transport = plan['council'][stage]['transport']
                        response = self.relay_responses[index]
                        name = self.calls[index][2]['response_format']['json_schema']['name']
                        self.assertEqual(transport['raw_response'], response.value)
                        self.assertEqual(transport['raw_content'], response.raw_content)
                        self.assertEqual(transport['receipt'], response.receipt)
                        self.assertEqual(json.loads(transport['raw_content']), transport['raw_response'])
                        collection = 'proposals' if stage == 'analyst' else 'reviews'
                        if stage in wrapped:
                            self.assertEqual(set(transport['raw_response']), {name})
                            self.assertNotIn(collection, transport['raw_response'])
                            body = transport['raw_response'][name]
                        else:
                            body = transport['raw_response']
                        self.assertEqual(set(body), {'summary', collection})
                        self.assertEqual(body[collection], plan['council'][stage][collection])

    def test_relay_wrapper_rejects_wrong_stage_extra_keys_recursion_and_nonobjects(self):
        for stage in ('analyst', 'reviewer'):
            for variant in ('wrong_stage', 'outer_extra', 'inner_extra', 'nested',
                            'null', 'array', 'string', 'number'):
                with self.subTest(stage=stage, variant=variant):
                    self.calls.clear()
                    def malformed(value, actual_stage, name):
                        if actual_stage != stage:
                            return value
                        body = value[name]
                        if variant == 'wrong_stage':
                            other = 'paper_council_review' if stage == 'analyst' else 'paper_council_analysis'
                            return {other: body}
                        if variant == 'outer_extra':
                            return {name: body, 'extra': True}
                        if variant == 'inner_extra':
                            return {name: {**body, 'context_id': 'not allowed'}}
                        if variant == 'nested':
                            return {name: {name: body}}
                        return {name: {'null': None, 'array': [], 'string': 'invalid', 'number': 1}[variant]}
                    with self.assertRaises(CouncilError) as caught:
                        ReviewCouncil(self.cfg).run(self.snapshot, self.relay_responder(transform=malformed))
                    trace = caught.exception.council
                    self.assertEqual(trace['status'], 'failed')
                    self.assertIsNone(trace['reviewer'])
                    self.assertEqual(trace['approved_proposal_ids'], [])
                    self.assertEqual(len(self.calls), 1 if stage == 'analyst' else 2)

    def test_wrapped_transport_and_saved_decisions_cannot_be_rewritten_before_execution(self):
        plan = ReviewCouncil(self.cfg).run(self.snapshot, self.relay_responder())
        for variant in ('quantity', 'limit_price', 'review', 'replace_wrapper_with_body'):
            for rewrite_content in (False, True):
                with self.subTest(variant=variant, rewrite_content=rewrite_content):
                    changed = deepcopy(plan)
                    stage = 'reviewer' if variant == 'review' else 'analyst'
                    transport = changed['council'][stage]['transport']
                    name = 'paper_council_review' if stage == 'reviewer' else 'paper_council_analysis'
                    body = transport['raw_response'][name]
                    if variant == 'review':
                        body['reviews'][0]['decision'] = 'reject'
                    elif variant == 'replace_wrapper_with_body':
                        transport['raw_response'] = body
                    else:
                        body['proposals'][0][variant] = 200 if variant == 'quantity' else '11.00'
                    if rewrite_content:
                        transport['raw_content'] = json.dumps(transport['raw_response'], ensure_ascii=False, indent=2)
                    with self.assertRaises(ValueError):
                        validate_execution_plan(changed, self.cfg)
        for stage, collection, key, value in (
                ('analyst', 'proposals', 'quantity', 200),
                ('analyst', 'proposals', 'limit_price', '11.00'),
                ('reviewer', 'reviews', 'decision', 'reject')):
            changed = deepcopy(plan)
            changed['council'][stage][collection][0][key] = value
            with self.subTest(saved_role=stage, key=key), self.assertRaises(ValueError):
                validate_execution_plan(changed, self.cfg)

    def test_missing_duplicate_and_unknown_reviews_fail_closed(self):
        for reviews in ([], [self.review, self.review], [{**self.review, 'proposal_id': 'other'}]):
            with self.subTest(reviews=reviews), self.assertRaises(CouncilError) as caught:
                self.run_plan(reviews=reviews)
            self.assertEqual(caught.exception.council['status'], 'failed')
            self.assertIsNotNone(caught.exception.council['analyst'])
            self.assertIsNone(caught.exception.council['reviewer'])

    def test_reviewer_cannot_change_quantity_price_or_symbol(self):
        for key, value in [('quantity', 200), ('limit_price', '20'), ('symbol', '000001.SZ'), ('orders', [])]:
            with self.subTest(key=key), self.assertRaises(CouncilError):
                self.run_plan(reviews=[{**self.review, key: value}])

    def test_transport_owned_binding_and_model_output_has_no_identity_echo(self):
        plan = self.run_plan()
        request_ids = []
        for index, (prompt, data, budget) in enumerate(self.calls):
            stage = 'analyst' if index == 0 else 'reviewer'
            transport = plan['council'][stage]['transport']
            raw = transport['raw_response']
            self.assertNotIn('context_id', raw)
            self.assertNotIn('asof', raw)
            self.assertNotIn('binding_to_echo', data)
            self.assertEqual(transport['receipt']['binding'], budget['binding'])
            self.assertEqual(budget['binding']['context_id'], plan['council']['context_id'])
            self.assertEqual(budget['binding']['asof'], plan['asof'])
            request_ids.append(budget['binding']['request_id'])
        self.assertNotEqual(*request_ids)
        self.assertTrue(validate_execution_plan(plan, self.cfg))

    def test_seconds_binding_preserves_full_observation_and_original_input(self):
        self.snapshot['now'] = '2026-11-13T11:19:47.627646+08:00'
        original = deepcopy(self.snapshot)
        plan = self.run_plan()
        self.assertEqual(self.snapshot, original)
        self.assertEqual(plan['asof'], '2026-11-13T11:19:47+08:00')
        self.assertEqual(plan['council']['input_snapshot']['observed_at'], original['now'])
        self.snapshot['now'] = '2026-11-13T11:19:47.627647+08:00'
        later = self.run_plan()
        self.assertEqual(later['asof'], plan['asof'])
        self.assertNotEqual(later['council']['context_id'], plan['council']['context_id'])
        plan['council']['input_snapshot']['observed_at'] = self.snapshot['now']
        with self.assertRaises(ValueError):
            validate_execution_plan(plan, self.cfg)

    def test_unbound_plain_json_response_is_rejected_not_repaired(self):
        calls = []
        def plain(system, data, **budget):
            calls.append(data)
            return {'summary': 'fixture', 'proposals': []}
        with self.assertRaises(CouncilError):
            ReviewCouncil(self.cfg).run(self.snapshot, plain)
        self.assertEqual(len(calls), 1)

    def test_old_or_cross_role_receipt_is_rejected_without_retry(self):
        for variant in ('old_request', 'other_role', 'other_snapshot', 'other_time', 'other_policy'):
            self.calls.clear()
            underlying = self.responder()
            def wrong(system, data, **budget):
                result = underlying(system, data, **budget)
                binding = result.receipt['binding']
                key, value = {'old_request': ('request_id', '0' * 32),
                    'other_role': ('role_id', 'other'), 'other_snapshot': ('context_id', '0' * 64),
                    'other_time': ('asof', '2026-11-13T11:19:48+08:00'),
                    'other_policy': ('config_fingerprint', '0' * 64)}[variant]
                binding[key] = value
                return result
            with self.subTest(variant=variant), self.assertRaises(CouncilError):
                ReviewCouncil(self.cfg).run(self.snapshot, wrong)
            self.assertEqual(len(self.calls), 1)

    def test_previous_completion_cannot_be_reused_for_new_invocation(self):
        saved = []
        underlying = self.responder()
        def capture(system, data, **budget):
            response = underlying(system, data, **budget)
            saved.append(response)
            return response
        ReviewCouncil(self.cfg).run(self.snapshot, capture)
        with self.assertRaises(CouncilError):
            ReviewCouncil(self.cfg).run(self.snapshot, lambda *args, **kwargs: saved[0])
        with self.assertRaises(CouncilError):
            ReviewCouncil(self.cfg).run(self.snapshot, lambda *args, **kwargs: saved[1])

    def test_mutated_request_payload_cannot_keep_the_original_context_label(self):
        original = deepcopy(self.snapshot)
        underlying = self.responder()
        def mutated(system, data, **budget):
            data['input']['risk']['max_order_notional'] = 999999
            return underlying(system, data, **budget)
        with self.assertRaises(CouncilError):
            ReviewCouncil(self.cfg).run(self.snapshot, mutated)
        self.assertEqual(self.snapshot, original)
        self.assertEqual(len(self.calls), 1)

    def test_provider_schema_only_constrains_business_output(self):
        self.run_plan()
        for index, (_, data, budget) in enumerate(self.calls):
            response_format = budget['response_format']
            self.assertEqual(response_format['type'], 'json_schema')
            self.assertIs(response_format['json_schema']['strict'], True)
            schema = response_format['json_schema']['schema']
            props = schema['properties']
            collection = 'proposals' if index == 0 else 'reviews'
            self.assertEqual(set(props), {'summary', collection})
            self.assertEqual(props[collection]['maxItems'], 3)
            for node in (schema, props[collection]['items']):
                self.assertIs(node['additionalProperties'], False)
                self.assertEqual(set(node['required']), set(node['properties']))

    def test_unknown_and_legacy_echo_fields_fail_locally_without_correction(self):
        for stage in ('analyst', 'reviewer'):
            for key in ('unexpected', 'context_id', 'asof'):
                with self.subTest(stage=stage, key=key), self.assertRaises(CouncilError):
                    def extra(out, data):
                        if ('analysis' in data) == (stage == 'reviewer'):
                            out[key] = 'sensitive-fixture-wrong-value'
                    self.run_plan(mutation=extra)
        with self.assertRaises(CouncilError):
            self.run_plan(proposals=[{**self.proposal, 'unexpected': True}])
        with self.assertRaises(CouncilError):
            self.run_plan(reviews=[{**self.review, 'unexpected': True}])

    def test_original_response_and_receipt_are_rechecked_before_execution(self):
        plan = self.run_plan()
        for mutation in ('raw_content', 'raw_response', 'receipt_hash', 'role', 'request_id', 'request_data', 'budget', 'model', 'record'):
            changed = deepcopy(plan)
            transport = changed['council']['analyst']['transport']
            if mutation == 'raw_content': transport['raw_content'] += ' '
            elif mutation == 'raw_response': transport['raw_response']['proposals'][0]['quantity'] = 200
            elif mutation == 'receipt_hash': transport['receipt']['response_sha256'] = '0' * 64
            elif mutation == 'role': transport['receipt']['binding']['role_id'] = 'reviewer_fixture'
            elif mutation == 'request_id': transport['receipt']['binding']['request_id'] = '0' * 32
            elif mutation == 'request_data': transport['receipt']['request_data_sha256'] = '0' * 64
            elif mutation == 'budget': transport['receipt']['timeout_seconds'] -= 1
            elif mutation == 'model': changed['model'] = 'different-model'
            else: changed['council']['analyst']['summary'] = 'altered explanation'
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                validate_execution_plan(changed, self.cfg)

    def test_failed_business_response_is_not_written_into_trace(self):
        def invalid(out, data):
            out.update(context_id='sensitive-fixture-value', summary='sensitive-response-summary')
        with self.assertRaises(CouncilError) as caught:
            self.run_plan(mutation=invalid)
        self.assertNotIn('sensitive-', json.dumps(caught.exception.council))

    def test_protocol_change_invalidates_existing_plan_and_proof_fingerprint(self):
        plan = self.run_plan()
        with patch('paperlive.council.PROTOCOL_VERSION', 999):
            self.assertNotEqual(config_fingerprint(self.cfg), plan['council']['config_fingerprint'])
            with self.assertRaises(ValueError):
                validate_execution_plan(plan, self.cfg)

    def test_out_of_scope_and_fabricated_evidence_fail(self):
        for proposal in ({**self.proposal, 'symbol': '600001.SH'},
                         {**self.proposal, 'evidence_refs': ['quote:600000.SH', 'research:made_up']},
                         {**self.proposal, 'evidence_refs': ['risk_policy']},
                         {**self.proposal, 'quantity': True},
                         {**self.proposal, 'limit_price': 'NaN'}):
            with self.subTest(proposal=proposal), self.assertRaises(CouncilError):
                self.run_plan(proposals=[proposal])

    def test_approved_review_requires_original_evidence(self):
        with self.assertRaises(CouncilError):
            self.run_plan(reviews=[{**self.review, 'evidence_refs': ['risk_policy']}])

    def test_selling_only_owned_symbols_requires_position_reference(self):
        proposal = {**self.proposal, 'side': 'SELL', 'symbol': '000001.SZ',
                    'evidence_refs': ['position:000001.SZ']}
        review = {**self.review, 'evidence_refs': ['position:000001.SZ']}
        self.assertEqual(self.run_plan(proposals=[proposal], reviews=[review])['orders'][0]['side'], 'SELL')
        with self.assertRaises(CouncilError):
            self.run_plan(proposals=[{**proposal, 'symbol': '600000.SH'}], reviews=[review])

    def test_elapsed_budget_is_shared_and_no_third_request(self):
        clock = [0]
        underlying = self.responder()
        def slow(system, data, **budget):
            result = underlying(system, data, **budget)
            clock[0] += 41
            return result
        with self.assertRaises(CouncilError) as caught:
            ReviewCouncil(self.cfg, clock=lambda: clock[0]).run(self.snapshot, slow)
        self.assertEqual(len(self.calls), 2)
        self.assertEqual(self.calls[1][2]['timeout'], 39)
        self.assertEqual(caught.exception.council['status'], 'failed')

    def test_larger_request_cap_only_uses_remaining_shared_budget(self):
        self.cfg['ai']['timeout_seconds'] = 60
        # Role deadlines reject a late first result even when the shared plan
        # budget would otherwise permit another request.
        cases = ((29, 50, 51, True, 2), (59, 20, 21, True, 2),
                 (61, 18, None, False, 1), (29, 52, 51, False, 2), (59, 22, 21, False, 2))
        for analyst_seconds, reviewer_seconds, remaining, valid, call_count in cases:
            with self.subTest(analyst=analyst_seconds, reviewer=reviewer_seconds):
                self.calls.clear()
                clock = [0]
                underlying = self.responder()
                def timed(system, data, **budget):
                    result = underlying(system, data, **budget)
                    clock[0] += reviewer_seconds if 'analysis' in data else analyst_seconds
                    return result
                council = ReviewCouncil(self.cfg, clock=lambda: clock[0])
                if valid:
                    plan = council.run(self.snapshot, timed)
                    self.assertEqual(plan['council']['status'], 'approved')
                    self.assertEqual(plan['council']['elapsed_seconds'], 79)
                    self.assertTrue(validate_execution_plan(plan, self.cfg))
                else:
                    with self.assertRaises(CouncilError) as caught:
                        council.run(self.snapshot, timed)
                    trace = caught.exception.council
                    self.assertEqual(trace['status'], 'failed')
                    self.assertEqual(trace['elapsed_seconds'], analyst_seconds + (reviewer_seconds if call_count == 2 else 0))
                    self.assertIsNone(trace['reviewer'])
                    self.assertEqual(trace['approved_proposal_ids'], [])
                self.assertEqual(len(self.calls), call_count)
                self.assertEqual(self.calls[0][2]['timeout'], 60)
                if call_count == 2:
                    self.assertEqual(self.calls[1][2]['timeout'], remaining)

    def test_one_format_retry_recovers_each_role_with_new_request_identity(self):
        self.format_retry_policy(1)
        for stage in ('analyst', 'reviewer'):
            with self.subTest(stage=stage):
                self.calls.clear()
                steps = [{'seconds': 7, 'error': self.malformed_json_error()}, {'seconds': 10}, {'seconds': 8}]
                if stage == 'reviewer':
                    steps = [{'seconds': 10}, {'seconds': 7, 'error': self.malformed_json_error()}, {'seconds': 8}]
                request, attempts, clock = self.scripted_role_requests(steps)
                plan = ReviewCouncil(self.cfg, clock=lambda: clock[0]).run(self.snapshot, request)
                trace = plan['council']; retry = trace['format_retries'][0]
                failed_index = 0 if stage == 'analyst' else 1
                successful_index = failed_index + 1
                self.assertEqual(len(attempts), 3)
                self.assertEqual(len(trace['format_retries']), 1)
                self.assertEqual(retry['stage'], stage)
                self.assertEqual(retry['role_id'], stage + '_fixture')
                self.assertEqual(retry['context_id'], trace['context_id'])
                self.assertEqual(retry['failed_request_id'], attempts[failed_index]['budget']['binding']['request_id'])
                self.assertEqual(retry['retry_request_id'], attempts[successful_index]['budget']['binding']['request_id'])
                self.assertEqual(retry['elapsed_seconds'], 7 if stage == 'analyst' else 17)
                self.assertEqual(retry['diagnostics']['category'], 'invalid_json')
                self.assertEqual(retry['diagnostics']['http_status'], 200)
                self.assertEqual(retry['diagnostics']['phase'], 'model_json')
                self.assertEqual(attempts[failed_index]['data'], attempts[successful_index]['data'])
                self.assertEqual(attempts[failed_index]['system'], attempts[successful_index]['system'])
                self.assertEqual(attempts[failed_index]['budget']['response_format'],
                                 attempts[successful_index]['budget']['response_format'])
                self.assertEqual(len({row['budget']['binding']['request_id'] for row in attempts}), 3)
                self.assertEqual(trace[stage]['transport']['receipt']['binding']['request_id'], retry['retry_request_id'])
                self.assertEqual(trace[stage]['transport']['receipt']['binding']['retry_of_request_id'], retry['failed_request_id'])
                self.assertNotIn('transport_error', trace)
                self.assertEqual(plan['orders'][0]['quantity'], self.proposal['quantity'])
                self.assertTrue(validate_execution_plan(plan, self.cfg))
                for field, value in (('retry_request_id', retry['failed_request_id']),
                                     ('context_id', '0' * 64), ('role_id', 'other_role')):
                    tampered = deepcopy(plan)
                    tampered['council']['format_retries'][0][field] = value
                    with self.subTest(tampered=field), self.assertRaises(ValueError):
                        validate_execution_plan(tampered, self.cfg)

    def test_successful_retry_receipt_requires_its_failed_request_history(self):
        self.format_retry_policy(1)
        for stage in ('analyst', 'reviewer'):
            steps = ([{}] if stage == 'reviewer' else []) + [
                {'error': self.malformed_json_error()}, {}] + ([{}] if stage == 'analyst' else [])
            request, attempts, clock = self.scripted_role_requests(steps)
            plan = ReviewCouncil(self.cfg, clock=lambda: clock[0]).run(self.snapshot, request)
            self.assertTrue(validate_execution_plan(plan, self.cfg))
            for variant in ('delete_records', 'empty_records', 'delete_retry_binding',
                            'wrong_retry_binding', 'self_retry_binding'):
                with self.subTest(stage=stage, variant=variant):
                    changed = deepcopy(plan)
                    binding = changed['council'][stage]['transport']['receipt']['binding']
                    if variant == 'delete_records':
                        del changed['council']['format_retries']
                    elif variant == 'empty_records':
                        changed['council']['format_retries'] = []
                    elif variant == 'delete_retry_binding':
                        del binding['retry_of_request_id']
                    elif variant == 'wrong_retry_binding':
                        binding['retry_of_request_id'] = '0' * 32
                    else:
                        binding['retry_of_request_id'] = binding['request_id']
                    with self.assertRaises(ValueError):
                        validate_execution_plan(changed, self.cfg)

    def test_format_retry_limit_is_shared_by_entire_plan_and_exhaustion_never_returns_orders(self):
        self.format_retry_policy(1)
        cases = (([True, True], 2, False),
                 ([True, False, True], 3, True),
                 ([False, True, True], 3, True))
        for errors, count, analyst_complete in cases:
            with self.subTest(errors=errors):
                steps = [{'error': self.malformed_json_error()} if failure else {} for failure in errors]
                request, attempts, clock = self.scripted_role_requests(steps)
                with self.assertRaises(CouncilError) as caught:
                    ReviewCouncil(self.cfg, clock=lambda: clock[0]).run(self.snapshot, request)
                trace = caught.exception.council
                self.assertEqual(len(attempts), count)
                self.assertEqual(len(trace['format_retries']), 1)
                self.assertEqual(trace['status'], 'failed')
                self.assertEqual(bool(trace['analyst']), analyst_complete)
                self.assertIsNone(trace['reviewer'])
                self.assertEqual(trace['approved_proposal_ids'], [])

    def test_format_retry_only_accepts_completed_http_200_model_json_parse_failure(self):
        self.format_retry_policy(1)
        errors = [self.malformed_json_error(phase='provider_json'),
                  self.malformed_json_error(http_status=401), self.malformed_json_error(http_status=429),
                  self.malformed_json_error(http_status=None), self.malformed_json_error(finish_reason='length'),
                  self.malformed_json_error(finish_reason=None), self.malformed_json_error(category='timeout'),
                  self.malformed_json_error(category='http_error'), self.malformed_json_error(category='invalid_response_structure'),
                  self.malformed_json_error(category='completion_truncated'), TimeoutError('synthetic timeout')]
        for stage in ('analyst', 'reviewer'):
            for error in errors:
                with self.subTest(stage=stage, error=str(error)):
                    steps = ([{}] if stage == 'reviewer' else []) + [{'error': error}]
                    request, attempts, clock = self.scripted_role_requests(steps)
                    with self.assertRaises(CouncilError) as caught:
                        ReviewCouncil(self.cfg, clock=lambda: clock[0]).run(self.snapshot, request)
                    self.assertEqual(len(attempts), 1 if stage == 'analyst' else 2)
                    self.assertEqual(caught.exception.council.get('format_retries', []), [])

    def test_valid_veto_hold_business_schema_and_binding_errors_are_not_retried(self):
        self.format_retry_policy(1)
        for outcome in ('hold', 'veto', 'schema', 'binding', 'model'):
            with self.subTest(outcome=outcome):
                self.calls.clear()
                if outcome == 'hold':
                    result = self.run_plan(proposals=[], reviews=[])
                elif outcome == 'veto':
                    result = self.run_plan(reviews=[{**self.review, 'decision': 'reject'}])
                elif outcome == 'schema':
                    with self.assertRaises(CouncilError) as caught:
                        self.run_plan(mutation=lambda out, data: out.update(unexpected=True))
                else:
                    underlying = self.responder()
                    def corrupted(system, data, **budget):
                        response = underlying(system, data, **budget)
                        if outcome == 'model':
                            if 'analysis' not in data:
                                return response
                            return bind_json_response(system, data, response.value, binding=budget['binding'],
                                provider=self.cfg['ai']['provider'], model='different-model',
                                max_tokens=budget['max_tokens'], timeout=budget['timeout'],
                                response_format=budget['response_format'])
                        response.receipt['binding']['request_id'] = '0' * 32
                        return response
                    with self.assertRaises(CouncilError) as caught:
                        ReviewCouncil(self.cfg).run(self.snapshot, corrupted)
                if outcome in ('hold', 'veto'):
                    self.assertEqual(len(self.calls), 2)
                    self.assertEqual(result['orders'], [])
                    self.assertEqual(result['council'].get('format_retries', []), [])
                else:
                    self.assertEqual(len(self.calls), 2 if outcome == 'model' else 1)
                    self.assertEqual(caught.exception.council.get('format_retries', []), [])

    def test_format_retry_cannot_reuse_completion_bound_to_failed_request(self):
        self.format_retry_policy(1)
        underlying = self.responder(); saved = []; attempts = []
        def replay(system, data, **budget):
            attempts.append(deepcopy(budget['binding']))
            if not saved:
                saved.append(underlying(system, data, **budget))
                raise self.malformed_json_error()
            return saved[0]
        with self.assertRaises(CouncilError) as caught:
            ReviewCouncil(self.cfg).run(self.snapshot, replay)
        self.assertEqual(len(attempts), 2)
        self.assertNotEqual(attempts[0]['request_id'], attempts[1]['request_id'])
        self.assertEqual(len(caught.exception.council['format_retries']), 1)
        self.assertIsNone(caught.exception.council['analyst'])
        self.assertEqual(caught.exception.council['approved_proposal_ids'], [])

    def test_format_retry_disabled_by_default_or_zero_and_invalid_limits_rejected(self):
        original = yaml.safe_load(self.policy.read_text())
        for policy in (original, {**original, 'max_format_retries': 0}):
            self.policy.write_text(yaml.safe_dump(policy))
            request, attempts, clock = self.scripted_role_requests([{'error': self.malformed_json_error()}])
            with self.assertRaises(CouncilError):
                ReviewCouncil(self.cfg, clock=lambda: clock[0]).run(self.snapshot, request)
            self.assertEqual(len(attempts), 1)
        for value in (True, False, -1, 2, 1.0, 0.0, '1', None):
            with self.subTest(value=value):
                self.policy.write_text(yaml.safe_dump({**original, 'max_format_retries': value}))
                with self.assertRaises(ValueError):
                    ReviewCouncil(self.cfg)

    def test_format_retry_uses_remaining_role_and_shared_budgets_without_reset(self):
        self.format_retry_policy(1)
        cases = (([40, 15, 10], 0, [60, 20, 25]),
                 ([35, 20, 20], 1, [60, 45, 25]),
                 ([5, 45, 10], 1, [60, 60, 15]),
                 ([40, 20, 20], 1, [60, 40, 20]))
        for durations, failed_index, timeouts in cases:
            with self.subTest(durations=durations):
                steps = [{'seconds': seconds, **({'error': self.malformed_json_error()} if index == failed_index else {})}
                         for index, seconds in enumerate(durations)]
                request, attempts, clock = self.scripted_role_requests(steps)
                plan = ReviewCouncil(self.cfg, clock=lambda: clock[0]).run(self.snapshot, request)
                self.assertEqual([row['budget']['timeout'] for row in attempts], timeouts)
                self.assertEqual(plan['council']['elapsed_seconds'], sum(durations))
                self.assertTrue(validate_execution_plan(plan, self.cfg))

    def test_format_retry_does_not_start_after_role_or_shared_budget_is_exhausted(self):
        self.format_retry_policy(1)
        for durations, failed_index in (([60], 0), ([61], 0), ([10, 60], 1), ([40, 40], 1)):
            with self.subTest(durations=durations):
                steps = [{'seconds': seconds, **({'error': self.malformed_json_error()} if index == failed_index else {})}
                         for index, seconds in enumerate(durations)]
                request, attempts, clock = self.scripted_role_requests(steps)
                with self.assertRaises(CouncilError) as caught:
                    ReviewCouncil(self.cfg, clock=lambda: clock[0]).run(self.snapshot, request)
                self.assertEqual(len(attempts), len(durations))
                self.assertEqual(caught.exception.council['approved_proposal_ids'], [])
        for durations, failed_index in (([55, 6], 0), ([35, 30, 16], 1)):
            with self.subTest(late_success=durations):
                steps = [{'seconds': seconds, **({'error': self.malformed_json_error()} if index == failed_index else {})}
                         for index, seconds in enumerate(durations)]
                request, attempts, clock = self.scripted_role_requests(steps)
                with self.assertRaises(CouncilError) as caught:
                    ReviewCouncil(self.cfg, clock=lambda: clock[0]).run(self.snapshot, request)
                self.assertEqual(len(attempts), len(durations))
                self.assertEqual(caught.exception.council['approved_proposal_ids'], [])

    def test_configuration_change_after_format_error_stops_before_retry(self):
        self.format_retry_policy(1)
        def change():
            self.cfg['risk']['max_order_notional'] += 1
        request, attempts, clock = self.scripted_role_requests([
            {'error': self.malformed_json_error(), 'change': change}])
        with self.assertRaises(CouncilError) as caught:
            ReviewCouncil(self.cfg, clock=lambda: clock[0]).run(self.snapshot, request)
        self.assertEqual(len(attempts), 1)
        self.assertEqual(caught.exception.council['approved_proposal_ids'], [])

    def test_skipped_format_retry_preserves_first_error_and_skip_reason(self):
        self.format_retry_policy(1)
        original_cap = self.cfg['risk']['max_order_notional']
        for stage in ('analyst', 'reviewer'):
            for reason in ('budget_exhausted', 'configuration_changed'):
                with self.subTest(stage=stage, reason=reason):
                    self.cfg['risk']['max_order_notional'] = original_cap
                    def change():
                        self.cfg['risk']['max_order_notional'] += 1
                    failure = {'error': self.malformed_json_error(),
                               'seconds': 60 if reason == 'budget_exhausted' else 7}
                    if reason == 'configuration_changed':
                        failure['change'] = change
                    steps = ([{'seconds': 10}] if stage == 'reviewer' else []) + [failure]
                    request, attempts, clock = self.scripted_role_requests(steps)
                    with self.assertRaises(CouncilError) as caught:
                        ReviewCouncil(self.cfg, clock=lambda: clock[0]).run(self.snapshot, request)
                    trace = caught.exception.council
                    self.assertEqual(len(attempts), 1 if stage == 'analyst' else 2)
                    self.assertEqual(trace['format_retries'], [])
                    self.assertEqual(trace['retry_skipped_reason'], reason)
                    diagnostic = trace['transport_error']
                    self.assertEqual(diagnostic['stage'], stage)
                    self.assertEqual(diagnostic['category'], 'invalid_json')
                    self.assertEqual(diagnostic['phase'], 'model_json')
                    self.assertEqual(diagnostic['http_status'], 200)
                    self.assertEqual(diagnostic['finish_reason'], 'stop')
                    self.assertEqual(diagnostic['json_position'], 180)
                    self.assertEqual(trace['approved_proposal_ids'], [])

    def test_configuration_change_during_analysis_stops_review(self):
        def change(out, data):
            self.cfg['risk']['max_order_notional'] += 1
        with self.assertRaises(CouncilError):
            self.run_plan(mutation=change)
        self.assertEqual(len(self.calls), 1)

    def test_catalog_change_invalidates_execution(self):
        plan = self.run_plan()
        catalog = yaml.safe_load(self.catalog.read_text())
        catalog['personas'][0]['style'] = 'changed style'
        self.catalog.write_text(yaml.safe_dump(catalog))
        with self.assertRaises(ValueError):
            validate_execution_plan(plan, self.cfg)

    def test_changed_risk_invalidates_execution(self):
        plan = self.run_plan()
        self.cfg['risk']['max_total_weight'] = .9
        with self.assertRaises(ValueError):
            validate_execution_plan(plan, self.cfg)

    def test_disabled_council_does_not_execute_its_prior_plan(self):
        plan = self.run_plan()
        self.cfg['council']['enabled'] = False
        with self.assertRaises(ValueError):
            validate_execution_plan(plan, self.cfg)
        self.assertTrue(validate_execution_plan({'orders': []}, self.cfg))

    def test_model_settings_metadata_change_invalidates_execution_without_reading_keys(self):
        settings = self.root / 'settings.yaml'
        self.cfg['ai']['settings_file'] = str(settings)
        settings.write_text('model: one\n')
        plan = self.run_plan()
        settings.write_text('model: another\n')
        with self.assertRaises(ValueError):
            validate_execution_plan(plan, self.cfg)

    def test_tampered_snapshot_added_or_changed_order_rejected(self):
        plan = self.run_plan()
        variants = []
        changed = deepcopy(plan); changed['orders'][0]['quantity'] = 200; variants.append(changed)
        changed = deepcopy(plan); changed['orders'].append(deepcopy(changed['orders'][0])); variants.append(changed)
        changed = deepcopy(plan); changed['council']['input_snapshot']['risk']['max_order_notional'] = 999999; variants.append(changed)
        changed = deepcopy(plan); changed['council']['approved_proposal_ids'] = []; variants.append(changed)
        changed = deepcopy(plan); changed['council']['enabled'] = False; variants.append(changed)
        for changed in variants:
            with self.assertRaises(ValueError):
                validate_execution_plan(changed, self.cfg)

    def test_missing_review_evidence_never_uses_legacy_single_model(self):
        with self.assertRaises(ValueError):
            validate_execution_plan({'asof': self.now.isoformat(), 'orders': []}, self.cfg)

    def test_policy_requires_distinct_present_roles_and_total_budget(self):
        original = yaml.safe_load(self.policy.read_text())
        for updates in ({'reviewer_id': 'analyst_fixture'}, {'analyst_id': 'absent'}, {'max_duration_seconds': 1000}):
            self.policy.write_text(yaml.safe_dump({**original, **updates}))
            with self.assertRaises(ValueError):
                describe_policy(self.cfg)

    def test_planner_description_of_invalid_council_is_not_configured(self):
        self.catalog.write_text('schema_version: 1\npersonas: [null]\n')
        planner = AiPlanner(self.cfg)
        with patch.object(planner, 'connection', return_value=('https://relay.invalid/v1', 'fixture-key', 'fixture-model')):
            description = planner.describe()
        self.assertFalse(description['configured'])
        self.assertIn('error', description)
        self.assertNotIn('fixture-key', json.dumps(description))

    def test_probe_has_no_account_candidates_or_orders_and_cannot_execute(self):
        planner = AiPlanner(self.cfg)
        callback = self.responder(proposals=[], reviews=[])
        with (patch.object(planner, 'connection', return_value=('https://relay.invalid/v1', 'fixture-key', 'fixture-model')),
              patch.object(planner, '_request_json', side_effect=lambda system, data, **budget: callback(system, data, **budget))):
            probe = planner.verify_connection()
        self.assertTrue(probe['council_verified'])
        self.assertTrue(probe['council']['verification_only'])
        for _, request, _ in self.calls:
            for key in ('account', 'candidates', 'positions', 'orders'):
                self.assertNotIn(key, request['input'])
        with self.assertRaises(ValueError):
            validate_execution_plan({'council': probe['council'], 'orders': [], 'asof': probe['council']['asof']}, self.cfg)

    def test_transport_error_preserves_only_completed_stage_and_never_falls_back(self):
        underlying = self.responder()
        def fail(system, data, **budget):
            if 'analysis' in data:
                raise OSError('sensitive transport fixture detail')
            return underlying(system, data, **budget)
        with self.assertRaises(CouncilError) as caught:
            ReviewCouncil(self.cfg).run(self.snapshot, fail)
        self.assertIsNotNone(caught.exception.council['analyst'])
        self.assertIsNone(caught.exception.council['reviewer'])
        self.assertNotIn('sensitive', str(caught.exception))

    def test_safe_transport_diagnostics_preserve_failed_role(self):
        underlying = self.responder()
        def fail(system, data, **budget):
            if 'analysis' in data:
                raise AiRequestError('completion_truncated', {'phase': 'completion', 'finish_reason': 'length',
                                                             'completion_tokens': 2000, 'response_bytes': 12000})
            return underlying(system, data, **budget)
        with self.assertRaises(CouncilError) as caught:
            ReviewCouncil(self.cfg).run(self.snapshot, fail)
        trace = caught.exception.council
        self.assertEqual(trace['status'], 'failed')
        self.assertEqual(trace['transport_error']['stage'], 'reviewer')
        self.assertEqual(trace['transport_error']['category'], 'completion_truncated')
        self.assertEqual(trace['transport_error']['completion_tokens'], 2000)
        self.assertIsNone(trace['reviewer'])
        self.assertEqual(trace['approved_proposal_ids'], [])

    def test_planner_routes_fresh_session_through_two_roles(self):
        # Filtering must retain the original clock: this quote is after the
        # truncated second but before the actual decision observation.
        self.now = self.now.replace(microsecond=900000)
        quote_time = self.now.replace(microsecond=800000)
        quote = Quote('600000.SH', 'fixture', *[D(v) for v in ['10', '9.99', '10.01']],
                      10000, 10000, *[D(v) for v in ['10', '10', '10.1', '9.9']],
                      100000, quote_time, D('11'), D('9'), received_at=self.now)
        instrument = Instrument('600000.SH', 'fixture', 'MAIN', '2000-01-01')
        planner = AiPlanner(self.cfg)
        callback = self.responder(proposals=[{**self.proposal, 'evidence_refs': ['quote:600000.SH']}])
        with (patch.object(planner, 'connection', return_value=('https://relay.invalid/v1', 'fixture-key', 'fixture-model')),
              patch.object(planner, '_request_json', side_effect=lambda system, data, **budget: callback(system, data, **budget))):
            plan = planner.propose({'account': {}, 'positions': []}, {quote.symbol: quote},
                                   {instrument.symbol: instrument}, {'open': True, 'calendar_verified': True}, self.now)
        self.assertEqual(plan['model'], 'fixture-model')
        self.assertEqual(len(self.calls), 2)
        self.assertEqual(plan['orders'][0]['symbol'], '600000.SH')
        self.assertEqual(plan['asof'], self.now.isoformat(timespec='seconds'))
        self.assertEqual(plan['council']['input_snapshot']['observed_at'], self.now.isoformat())
        self.assertEqual(plan['council']['input_snapshot']['candidates'][0]['timestamp'], quote_time.isoformat())
        reference = plan['council']['input_snapshot']['candidates'][0]['execution_reference']
        self.assertEqual(D(reference['buy_price']), D('10.02'))
        self.assertEqual(D(reference['sell_price']), D('9.98'))
        self.assertTrue(reference['requires_new_quote_after_order'])
        self.assertEqual(reference['slippage_bps'], self.cfg['broker']['slippage_bps'])
        # A resting limit below the reference is still a valid proposal; the
        # planner must never silently raise a user/model limit to force a fill.
        self.assertEqual(plan['orders'][0]['limit_price'], self.proposal['limit_price'])
