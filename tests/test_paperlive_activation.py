"""Automatic paper activation against temporary markers and synthetic evidence."""
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from paperlive.activation import (ActivationGate, ai_configuration_fingerprint, public_activation,
                                  verify_ai_connection, verified_research_summary)
from paperlive.config import load_config
from tests.test_paperlive_ledger import START, instrument, quote


class ActivationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.cfg = load_config(self.root/'isolated-paper.yaml')
        self.cfg['runtime_dir'] = str(self.root)
        self.cfg['risk']['kill_switch_file'] = str(self.root/'STOP')
        self.cfg['broker']['initial_cash'] = 50000
        self.cfg['activation'].update(enabled=True, expected_initial_cash=50000, expected_universe_count=3,
                                      ai_verification_file=str(self.root/'verification.json'))
        for field in ('settings_file', 'credentials_file'):
            path = self.root/field
            path.write_text('synthetic-local-file', encoding='utf-8')
            self.cfg['ai'][field] = str(path)
        self.ai = {'configured':True, 'provider':self.cfg['ai']['provider'], 'model':'synthetic-model'}
        self.proof = {'ok':True, 'json_plan_validated':True, 'provider':self.cfg['ai']['provider'],
                      'model':self.ai['model'], 'checked_at':START.isoformat(),
                      'config_fingerprint':ai_configuration_fingerprint(self.cfg, self.ai)}
        self.write_proof()
        self.stop = self.root/'STOP'
        self.stop.write_text(self.cfg['activation']['waiting_stop_text'], encoding='utf-8')
        self.instruments = {symbol:instrument(symbol) for symbol in ('600000.SH','600001.SH','600002.SH')}
        self.quotes = {symbol:quote(START, inst) for symbol,inst in self.instruments.items()}
        self.snapshot = {'account':{'initial_cash':'50000.00','valuation_complete':True}, 'reconciliation':{'ok':True}}
        self.session = {'date':START.date().isoformat(),'calendar_verified':True,'open':True}
        self.status = {'universe':{'member_count':3,'cache_stale':False},'calendar':{'source':'tushare:trade_cal'}}
        self.calendar = {'2026-11-10':True,'2026-11-11':True,'2026-11-12':True}
        self.evidence = {'pipeline':True,'products':True}
        self.minute = {'current_date_ready':True,'cache_verified':True,'deep_verified':True,'target_date':'2026-11-10',
                       'current_core_data_date':'2026-11-10','expected_symbols':3,'completed_symbols':3,
                       'universe_fingerprint':hashlib.sha256('\n'.join(sorted(self.instruments)).encode()).hexdigest()}
        self.gate = self.make()

    def write_proof(self):
        Path(self.cfg['activation']['ai_verification_file']).write_text(json.dumps(self.proof), encoding='utf-8')

    def make(self):
        return ActivationGate(self.cfg, evidence_loader=lambda target:self.evidence, minute_loader=lambda:self.minute)

    def evaluate(self, now=START):
        return self.gate.evaluate(self.snapshot,self.quotes,self.instruments,self.session,self.status,self.calendar,self.ai,now)

    def test_missing_minute_keeps_exact_owned_marker_and_waiting_status(self):
        self.minute['current_date_ready'] = False
        before = self.stop.read_bytes()
        result = self.evaluate()
        self.assertEqual(result['status'],'waiting_data')
        self.assertTrue(result['armed'])
        self.assertTrue(result['stop_owned'])
        self.assertFalse(result['ready'])
        self.assertEqual(self.stop.read_bytes(),before)
        self.assertIn('minute',[row['id'] for row in result['checks'] if not row['ok']])

    def test_all_evidence_releases_owned_stop_once_and_survives_restart(self):
        result = self.evaluate()
        self.assertTrue(result['ready'])
        self.assertEqual(result['status'],'active')
        self.assertFalse(self.stop.exists())
        self.gate = self.make()
        self.assertTrue(self.evaluate()['ready'])
        self.assertEqual(self.evaluate()['activated_at'],result['activated_at'])

    def test_loss_of_freshness_blocks_again_and_recovers_without_resetting_account(self):
        self.assertTrue(self.evaluate()['ready'])
        self.quotes['600000.SH'] = replace(self.quotes['600000.SH'], timestamp=START-timedelta(minutes=1))
        self.assertFalse(self.evaluate()['ready'])
        self.assertFalse(self.stop.exists())
        self.quotes['600000.SH'] = quote(START, self.instruments['600000.SH'])
        self.assertTrue(self.evaluate()['ready'])

    def test_manual_stop_after_activation_is_never_removed_or_rearmed(self):
        self.evaluate()
        self.stop.write_text('manual stop', encoding='utf-8')
        result = self.evaluate()
        self.assertEqual(result['status'],'paused')
        self.assertFalse(result['armed'])
        self.assertEqual(self.stop.read_text(),'manual stop')
        self.stop.unlink()
        self.assertFalse(self.evaluate()['ready'])

    def test_rewritten_same_marker_is_a_manual_override(self):
        self.minute['current_date_ready'] = False
        self.evaluate()
        old = self.stop.stat()
        os.utime(self.stop,ns=(old.st_atime_ns,old.st_mtime_ns+1000000))
        self.minute['current_date_ready'] = True
        self.assertEqual(self.evaluate()['status'],'paused')
        self.assertTrue(self.stop.exists())

    def test_user_removing_waiting_marker_does_not_bypass_acceptance(self):
        self.minute['current_date_ready'] = False
        self.evaluate()
        self.stop.unlink()
        self.minute['current_date_ready'] = True
        self.assertEqual(self.evaluate()['status'],'paused')

    def test_missing_quote_blocks_all_but_observed_zero_book_is_excluded(self):
        missing = self.quotes.pop('600000.SH')
        self.assertFalse(self.evaluate()['ready'])
        self.quotes['600000.SH'] = replace(missing,bid=0,bid_size=0)
        result = self.evaluate()
        self.assertTrue(result['ready'])
        self.assertIn('600000.SH',result['untradeable_symbols'])
        self.assertNotIn('600000.SH',result['tradable_symbols'])

    def test_entire_pool_untradeable_cannot_start_ai(self):
        self.quotes = {symbol:replace(value,ask=0,ask_size=0) for symbol,value in self.quotes.items()}
        self.assertFalse(self.evaluate()['ready'])
        self.assertTrue(self.stop.exists())

    def test_unheld_corporate_action_stock_is_excluded_without_blocking_other_stocks(self):
        for prefix in ('XD', 'XR', 'DR'):
            with self.subTest(prefix=prefix):
                self.snapshot['exposure_symbols'] = []
                self.quotes['600000.SH'] = replace(self.quotes['600000.SH'], name=prefix+' Synthetic')
                result = self.evaluate()
                self.assertTrue(result['ready'])
                self.assertIn('600000.SH', result['untradeable_symbols'])
                self.assertNotIn('600000.SH', result['tradable_symbols'])
                self.assertIn('600001.SH', result['tradable_symbols'])

    def test_corporate_action_requires_complete_ledger_exposure_proof(self):
        self.quotes['600000.SH'] = replace(self.quotes['600000.SH'], name='XD Synthetic')
        for exposures in (None, '600001.SH', [None], ['600000.SH']):
            with self.subTest(exposures=exposures):
                self.snapshot['exposure_symbols'] = exposures
                self.assertFalse(self.evaluate()['ready'])
        self.snapshot['exposure_symbols'] = ['600001.SH']
        self.assertTrue(self.evaluate()['ready'])

    def test_corporate_action_exclusion_does_not_mask_stale_quotes_or_account_errors(self):
        self.snapshot['exposure_symbols'] = []
        original = self.quotes['600000.SH']
        self.quotes['600000.SH'] = replace(original, name='XD Synthetic', timestamp=START-timedelta(minutes=1))
        self.assertFalse(self.evaluate()['ready'])
        self.quotes['600000.SH'] = replace(original, name='XD Synthetic')
        self.snapshot['account']['valuation_complete'] = False
        self.assertFalse(self.evaluate()['ready'])

    def test_entire_pool_corporate_actions_cannot_start_ai(self):
        self.snapshot['exposure_symbols'] = []
        self.quotes = {symbol:replace(value, name='XD Synthetic') for symbol,value in self.quotes.items()}
        result = self.evaluate()
        self.assertFalse(result['ready'])
        self.assertTrue(next(row for row in result['checks'] if row['id']=='quotes')['ok'])
        self.assertFalse(next(row for row in result['checks'] if row['id']=='executable')['ok'])

    def test_stale_or_different_scope_minutes_cannot_pass_current_data(self):
        for key,value in (('target_date','2026-11-09'),('universe_fingerprint','wrong'),('completed_symbols',2),('deep_verified',False)):
            original = deepcopy(self.minute)
            self.minute[key] = value
            self.assertFalse(self.evaluate()['ready'])
            self.minute = original

    def test_ai_proof_is_bound_to_model_file_versions_and_age(self):
        for key,value in (('model','different-model'),('checked_at',(START-timedelta(days=2)).isoformat()),
                          ('json_plan_validated',False)):
            original = deepcopy(self.proof)
            self.proof[key] = value
            self.write_proof()
            self.assertFalse(self.evaluate()['ready'])
            self.proof = original
        self.write_proof()
        Path(self.cfg['ai']['credentials_file']).write_text('changed-local-credential',encoding='utf-8')
        self.assertFalse(self.evaluate()['ready'])

    def test_current_day_only_becomes_required_research_after_official_close(self):
        result = self.evaluate()
        self.assertEqual(result['target_date'],'2026-11-10')
        result = self.evaluate(START.replace(hour=15,minute=1))
        self.assertEqual(result['target_date'],'2026-11-11')
        self.assertFalse(result['ready'])

    def test_unverified_calendar_research_or_wrong_initial_capital_stops_activation(self):
        self.session['calendar_verified'] = False
        self.assertFalse(self.evaluate()['ready'])
        self.session['calendar_verified'] = True
        self.evidence['products'] = False
        self.assertFalse(self.evaluate()['ready'])
        self.evidence['products'] = True
        self.snapshot['account']['initial_cash'] = '1000000'
        self.assertFalse(self.evaluate()['ready'])

    def test_symlink_or_custom_stop_is_not_claimed(self):
        self.stop.write_text('manual stop',encoding='utf-8')
        self.assertEqual(self.evaluate()['status'],'paused')
        self.assertEqual(self.stop.read_text(),'manual stop')

    def test_public_state_detects_manual_pause_without_mutating_files(self):
        self.evaluate()
        self.stop.write_text('manual stop',encoding='utf-8')
        before = (self.root/'activation.json').read_bytes()
        result = public_activation(self.cfg)
        self.assertEqual(result['status'],'paused')
        self.assertFalse(result['ready'])
        self.assertEqual((self.root/'activation.json').read_bytes(),before)

    def test_disabled_template_cannot_release_any_marker(self):
        self.cfg['activation']['enabled'] = False
        self.assertEqual(self.evaluate()['status'],'disabled')
        self.assertTrue(self.stop.exists())
        self.assertFalse((self.root/'activation.json').exists())

    def test_expired_ai_probe_slot_is_persistent_and_independent_of_data_gate(self):
        self.minute['current_date_ready'] = False
        self.proof['checked_at'] = (START-timedelta(days=2)).isoformat()
        self.write_proof()
        self.evaluate()
        fingerprint = self.gate.reserve_ai_probe(self.ai, START)
        self.assertTrue(fingerprint)
        self.assertIsNone(self.make().reserve_ai_probe(self.ai, START+timedelta(seconds=1)))
        self.assertTrue(self.make().reserve_ai_probe(self.ai, START+timedelta(seconds=self.cfg['activation']['ai_probe_retry_seconds'])))
        self.assertTrue(self.stop.exists())

    def test_manual_stop_suppresses_automatic_ai_verification(self):
        self.stop.write_text('manual stop',encoding='utf-8')
        self.evaluate()
        self.proof['checked_at'] = (START-timedelta(days=2)).isoformat()
        self.write_proof()
        self.assertIsNone(self.gate.reserve_ai_probe(self.ai,START))

    def test_json_probe_only_publishes_when_configuration_stays_bound(self):
        from unittest.mock import Mock
        planner = Mock()
        planner.describe.return_value = self.ai
        fingerprint = ai_configuration_fingerprint(self.cfg,self.ai)
        verify_ai_connection(self.cfg,planner,fingerprint)
        planner.verify_connection.assert_called_once_with()
        report = json.loads(Path(self.cfg['activation']['ai_verification_file']).read_text())
        self.assertTrue(report['json_plan_validated'])
        self.assertNotIn('orders',report)
        before = Path(self.cfg['activation']['ai_verification_file']).read_bytes()
        planner.verify_connection.side_effect = lambda:Path(self.cfg['ai']['settings_file']).write_text('changed config')
        with self.assertRaises(ValueError):
            verify_ai_connection(self.cfg,planner,fingerprint)
        self.assertEqual(Path(self.cfg['activation']['ai_verification_file']).read_bytes(),before)

    def enable_synthetic_council(self):
        policy = self.root / 'council.yaml'
        policy.write_text('synthetic-policy')
        self.cfg['council'].update(enabled=True, config_file=str(policy))
        self.ai['council'] = {'enabled': True, 'configured': True, 'config_fingerprint': 'synthetic-council-v1',
                              'roles': [{'id': 'fengliu'}, {'id': 'methodology'}]}
        self.proof['config_fingerprint'] = ai_configuration_fingerprint(self.cfg, self.ai)
        self.write_proof()

    def test_legacy_ai_proof_cannot_enable_two_role_trading(self):
        self.enable_synthetic_council()
        self.assertFalse(self.evaluate()['ready'])
        self.assertTrue(self.stop.exists())
        self.proof.update(council_verified=True, council_fingerprint='synthetic-council-v1',
                          council_roles=['fengliu', 'methodology'])
        self.write_proof()
        self.assertTrue(self.evaluate()['ready'])

    def test_council_proof_is_bound_to_roles_and_current_policy(self):
        self.enable_synthetic_council()
        self.proof.update(council_verified=True, council_fingerprint='synthetic-council-v1',
                          council_roles=['methodology', 'fengliu'])
        self.write_proof()
        self.assertFalse(self.evaluate()['ready'])
        self.proof['council_roles'] = ['fengliu', 'methodology']
        self.proof['council_fingerprint'] = 'old-policy'
        self.write_proof()
        self.assertFalse(self.evaluate()['ready'])
        self.proof['council_fingerprint'] = 'synthetic-council-v1'
        self.write_proof()
        self.assertTrue(self.evaluate()['ready'])
        self.ai['council']['config_fingerprint'] = 'changed-risk-policy'
        self.assertFalse(self.evaluate()['ready'])

    def test_real_council_probe_is_separate_from_execution_plans(self):
        from unittest.mock import Mock
        self.enable_synthetic_council()
        planner = Mock()
        planner.describe.return_value = self.ai
        planner.verify_connection.return_value = {'council_verified': True,
            'council_fingerprint': 'synthetic-council-v1', 'roles': ['fengliu', 'methodology'],
            'council': {'verification_only': True, 'status': 'hold'}}
        fingerprint = ai_configuration_fingerprint(self.cfg, self.ai)
        verify_ai_connection(self.cfg, planner, fingerprint)
        report = json.loads(Path(self.cfg['activation']['ai_verification_file']).read_text())
        self.assertTrue(report['council_verified'])
        self.assertEqual(report['council_roles'], ['fengliu', 'methodology'])
        trace = json.loads((self.root / 'council-verification.json').read_text())
        self.assertEqual(trace['kind'], 'connection_verification')
        self.assertTrue(trace['council']['verification_only'])
        self.assertFalse((self.root / 'plans').exists())
        before = Path(self.cfg['activation']['ai_verification_file']).read_bytes()
        planner.verify_connection.return_value['council_fingerprint'] = 'wrong-policy'
        with self.assertRaisesRegex(ValueError, '双角色验证'):
            verify_ai_connection(self.cfg, planner, fingerprint)
        self.assertEqual(Path(self.cfg['activation']['ai_verification_file']).read_bytes(), before)

    def test_research_summary_checks_hashes_and_only_attaches_existing_candidate_rows(self):
        documents = {
            'signal':{'kind':'research_summary','regime':{'pos':.5},'watch':[
                {'code':'600000.SH','score':1,'factors':{'real':1}},
                {'code':'600099.SH','score':999,'factors':{'excluded':1}}]},
            'traffic_light':{'signal_month':'2026-10','signal':'hold','source':'synthetic-local'},
            'factor_pool':{'data_date':'2026-11-10','health':[],'factors':[],'stats':{}},
        }
        steps=[]
        for key,value in documents.items():
            path=self.root/(key+'.json')
            path.write_text(json.dumps(value),encoding='utf-8')
            steps.append({'id':key,'status':'success','returncode':0,'evidence':[
                {'path':path.name,'data_date':'2026-11-10','sha256':hashlib.sha256(path.read_bytes()).hexdigest()}]})
        (self.root/'pipeline-state.json').write_text(json.dumps({'status':'complete','data_date':'2026-11-10','steps':steps}))
        with patch('data.research_bootstrap.load_config',return_value={'runtime_dir':str(self.root)}),patch('data.research_pipeline.ROOT',self.root):
            result=verified_research_summary(self.cfg,'2026-11-10',{'600000.SH'})
            self.assertEqual([row['code'] for row in result['current_candidate_research']],['600000.SH'])
            self.assertNotIn('orders',result)
            (self.root/'signal.json').write_text('{}')
            with self.assertRaises(ValueError):
                verified_research_summary(self.cfg,'2026-11-10',{'600000.SH'})

    def test_runner_can_observe_under_owned_stop_without_matching_or_ai(self):
        self.minute['current_date_ready'] = False
        result,ledger,pool = self.run_fixture()
        self.assertEqual(result['activation']['status'],'waiting_data')
        self.assertEqual(result['market']['phase'],'等待数据验收后自动启动')
        ledger.match.assert_not_called()
        ledger.submit.assert_not_called()
        pool.submit.assert_not_called()
        self.assertTrue(self.stop.exists())

    def test_ready_runner_uses_same_target_and_excludes_untradeable_candidate(self):
        self.quotes['600000.SH'] = replace(self.quotes['600000.SH'],bid=0,bid_size=0)
        result,ledger,pool = self.run_fixture()
        self.assertTrue(result['activation']['ready'])
        self.assertFalse(self.stop.exists())
        args=pool.submit.call_args.args
        self.assertEqual(set(args[3]),{'600001.SH','600002.SH'})
        self.assertEqual(args[4]['research_target_date'],'2026-11-10')
        ledger.submit.assert_not_called()

    def run_fixture(self, repeat=False, plan=None):
        from types import SimpleNamespace
        from unittest.mock import Mock
        import paperlive.runtime as runtime
        class Frozen(datetime):
            @classmethod
            def now(cls,tz=None):return START.astimezone(tz) if tz else START.replace(tzinfo=None)
        snapshot={**self.snapshot,'started_at':START.isoformat(),'positions':[],'orders':[],'fills':[],'curve':[]}
        ledger=Mock()
        ledger.fingerprint='synthetic-execution-policy'
        ledger.symbols.return_value=[]
        ledger.snapshot.return_value=snapshot
        feed=SimpleNamespace(status=self.status,_calendar={'dates':self.calendar},universe=lambda:self.instruments,
                             quotes=lambda symbols:self.quotes,session=lambda now:dict(self.session))
        planner=Mock()
        planner.describe.return_value=self.ai
        pool=Mock()
        if plan is not None:
            pool.submit.return_value.done.return_value=True
            pool.submit.return_value.result.return_value=plan
        with patch('paperlive.ledger.Ledger',return_value=ledger),patch('paperlive.hybrid_feed.HybridMarketFeed',return_value=feed),\
             patch.object(runtime,'AiPlanner',return_value=planner),patch.object(runtime,'ActivationGate',return_value=self.gate),\
             patch.object(runtime,'datetime',Frozen),patch.object(runtime,'ThreadPoolExecutor',return_value=pool):
            self.cfg['market']['provider']='tencent_tushare'
            if repeat:
                pauses=[]
                def wait(_):
                    pauses.append(True)
                    if len(pauses)==1:self.evidence['products']=False
                    else:raise StopIteration('isolated loop complete')
                with patch.object(runtime.time,'sleep',side_effect=wait):
                    with self.assertRaises(StopIteration):runtime.run(self.cfg,once=False)
                result=json.loads((self.root/'state.json').read_text())
            else:
                result=runtime.run(self.cfg,once=True)
        return result,ledger,pool

    def test_launcher_uses_configured_new_runtime_and_only_starts_observer(self):
        import launcher
        from types import SimpleNamespace
        config_dir=self.root/'config'
        config_dir.mkdir()
        (config_dir/'paperlive.yaml').write_text('synthetic')
        with patch('paperlive.config.load_config',return_value=self.cfg),patch.object(launcher.subprocess,'Popen',return_value=SimpleNamespace(pid=12345)) as start:
            launcher.start_paperlive(self.root)
        args=start.call_args.args[0]
        self.assertIn('--config',args)
        self.assertTrue((self.root/'runner.pid').exists())
        self.assertTrue(self.stop.exists())
        self.assertTrue(start.call_args.kwargs['start_new_session'])

    def test_completed_ai_plan_is_fully_rejected_if_acceptance_fails_while_waiting(self):
        plan={'asof':START.isoformat(),'summary':'synthetic','orders':[
            {'side':'SELL','symbol':'600001.SH','quantity':100,'limit_price':'10'}]}
        result,ledger,_=self.run_fixture(repeat=True,plan=plan)
        self.assertFalse(result['activation']['ready'])
        self.assertIn('数据验收失效',result['ai']['last_error'])
        ledger.submit.assert_not_called()

    def test_current_pipeline_requires_current_capital_and_configured_algorithm(self):
        from autotrade.monitoring import atomic_json
        import yaml
        spec={'required_inputs':['daily'],'steps':[{'id':'pool_layers','module':'strategy.pool_layers',
              'args':['--capital','$capital'],'outputs':[{'path':'pool_layers.json'}]}]}
        specpath=self.root/'pipeline.yaml'
        specpath.write_text(yaml.safe_dump(spec),encoding='utf-8')
        self.cfg['activation']['pipeline_config']=str(specpath)
        self.cfg['activation']['required_products']=['pool_layers']
        atomic_json(self.root/'bootstrap-manifest.json',{'coverage':{'daily':{'end':'2026-11-10'}},
                    'stages':{'daily':'complete'},'stage_dates':{'daily':'2026-11-10'}})
        artifact=self.root/'pool_layers.json'
        def evidence(capital,algorithm='strategy.pool_layers'):
            atomic_json(artifact,{'capital':capital})
            atomic_json(self.root/'pipeline-state.json',{'status':'complete','data_date':'2026-11-10','required_inputs':['daily'],
                'steps':[{'id':'pool_layers','status':'success','returncode':0,'evidence':[
                    {'path':'pool_layers.json','data_date':'2026-11-10','algorithm':algorithm,
                     'sha256':hashlib.sha256(artifact.read_bytes()).hexdigest()}]}]})
        gate=ActivationGate(self.cfg)
        with patch('data.research_bootstrap.load_config',return_value={'runtime_dir':str(self.root)}),\
             patch('data.research_pipeline.ROOT',self.root),patch('paperlive.activation.ROOT',self.root),\
             patch('deck.system_live._api_health',return_value={'pool_layers':{'initialized':True,'data_date':'2026-11-10'}}):
            evidence(1000000)
            self.assertFalse(gate._evidence('2026-11-10')['pipeline'])
            evidence(50000)
            self.assertTrue(gate._evidence('2026-11-10')['pipeline'])
            evidence(50000,'unconfigured.module')
            self.assertFalse(gate._evidence('2026-11-10')['pipeline'])


if __name__ == '__main__':
    unittest.main()
