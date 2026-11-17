"""Isolated schedule/queue/lock/source/failure tests; no credentials or HTTP."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import hashlib
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

import yaml
from autotrade.locking import ProcessLock
from autotrade.monitoring import atomic_json
from data.research_bootstrap import TZ, load_config as bootstrap_config
from data.research_service import ResearchService, load_config, public_status, request_update, start_service


class FakeBootstrap:
    def __init__(self,cfg,sessions):
        self.cfg=cfg;self.sessions=sessions;self.stop_event=threading.Event();self.manifest={}
        self.runs=[];self.queries=[];self.available=True;self.error=False;self.interrupt=False;self.delay=0;self.bad_date=False

    def calendar(self,start,end):
        self.queries.append(('trade_cal',start,end))
        return [day for day in self.sessions if start<=day<=end]

    def _daily_rows(self,api,fields,day):
        self.queries.append((api,day))
        if self.error:
            raise ValueError('credential-value-must-not-leak')
        return {'600000.SH':{'close':10,'adj_factor':1}} if self.available else {}

    def run(self,stages,target_date=None):
        self.runs.append((stages,target_date));time.sleep(self.delay)
        path=Path(self.cfg['runtime_dir'])/'bootstrap-manifest.json'
        self.manifest=json.loads(path.read_text()) if path.exists() else {}
        self.manifest.setdefault('coverage',{})['daily']={'end':'2026-11-09' if self.bad_date else target_date}
        self.manifest.setdefault('stages',{}).update({key:'complete' for key in stages})
        self.manifest.setdefault('stage_dates',{}).update({key:target_date for key in stages})
        atomic_json(path,self.manifest)
        return {'status':'interrupted' if self.interrupt else 'partial_complete'}


class ServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        # The selected minute provider is a separate service. Its real completed
        # job would otherwise start a verifier against the production database.
        minute_patch=patch('data.minute_readiness.public_status',return_value={})
        minute_patch.start();self.addCleanup(minute_patch.stop)
        self.root=Path(self.temp.name);self.runtime=self.root/'runtime'
        (self.root/'config').mkdir()
        (self.root/'config/research_pipeline.yaml').write_text('required_inputs: [daily, finance, assets]\nsteps:\n- {id: analysis, module: isolated.analysis, outputs: [{path: result.json}]}\n')
        root_patch=patch('data.research_service.ROOT',self.root);root_patch.start();self.addCleanup(root_patch.stop)
        self.clock=datetime(2026,11,11,18,10,tzinfo=TZ)
        from data.research_bootstrap import ROOT as project_root
        self.cfg=load_config(project_root/'config/research_service.yaml');self.cfg['runtime_dir']=str(self.runtime)
        self.cfg['earliest_close_check']='18:05'
        config=bootstrap_config();config.update(cache_dir=str(self.root/'cache'),runtime_dir=str(self.runtime),
                                              finance_parquet_dir=str(self.root/'financials'),end='latest',min_daily_rows=1)
        path=self.root/'research.yaml';path.write_text(yaml.safe_dump(config));self.cfg['research_config']=str(path)
        self.boot=FakeBootstrap(config,['2026-11-06','2026-11-09','2026-11-10','2026-11-11','2026-11-12'])
        self.pipeline_calls=[];self.pipeline_status='complete'
        def pipeline():
            self.pipeline_calls.append(True)
            result=self.proven_pipeline(self.service.state['target_date'])
            result['status']=self.pipeline_status
            atomic_json(self.runtime/'pipeline-state.json',result)
            return result
        self.service=ResearchService(self.cfg,now=lambda:self.clock,bootstrap_factory=lambda _:self.boot,pipeline_runner=pipeline)

    def request(self):
        atomic_json(self.runtime/'update-request.json',{'id':'synthetic-request','requested_at':self.clock.isoformat()})

    def proven_pipeline(self,target):
        path=self.root/'result.json';path.write_text(json.dumps({'data_date':target,'results':[{'code':'600000.SH'}]}))
        return {'status':'complete','data_date':target,'required_inputs':['daily','finance','assets'],'steps':[{'id':'analysis','status':'success','returncode':0,
                'evidence':[{'path':str(path),'data_date':target,'sha256':hashlib.sha256(path.read_bytes()).hexdigest()}]}]}

    def mark_completed(self):
        atomic_json(self.runtime/'pipeline-state.json',self.proven_pipeline('2026-11-11'))
        atomic_json(self.runtime/'bootstrap-manifest.json',{'coverage':{'daily':{'end':'2026-11-11'}},
                    'stages':{s:'complete' for s in ('daily','finance','assets','minute')},
                    'stage_dates':{s:'2026-11-11' for s in ('daily','finance','assets','minute')}})

    def test_before_configured_time_only_previous_verified_session_is_used(self):
        self.clock=self.clock.replace(hour=18,minute=4)
        state=self.service.cycle()
        self.assertEqual(state['status'],'core_complete')
        self.assertEqual(self.boot.runs[-1][1],'2026-11-10')

    def test_after_cutoff_runs_all_stages_and_verifies_pipeline_date(self):
        self.request();state=self.service.cycle()
        self.assertEqual(self.boot.runs,[(('daily','finance','assets'),'2026-11-11')])
        self.assertEqual(state['last_success_date'],'2026-11-11')
        self.assertEqual(self.pipeline_calls,[True])
        self.assertFalse((self.runtime/'update-request.json').exists())

    def test_earlier_after_close_time_still_requires_complete_official_sources(self):
        self.cfg['earliest_close_check']='17:00'
        self.clock=self.clock.replace(hour=16,minute=59)
        self.service.cycle()
        self.assertEqual(self.boot.runs[-1][1],'2026-11-10')
        self.clock=self.clock.replace(hour=17,minute=0)
        self.boot.available=False
        state=self.service.cycle()
        self.assertEqual(state['phase'],'awaiting_close_data')
        self.assertEqual(len(self.boot.runs),1)

    def test_service_close_time_rejects_intraday_and_malformed_values(self):
        path=self.root/'service-clock.yaml'
        for value in ('09:30','14:59','15:00','15:04','24:00','17:60','17:0',1700):
            path.write_text(yaml.safe_dump(dict(self.cfg,earliest_close_check=value)))
            with self.subTest(value=value),self.assertRaises(ValueError):load_config(path)
        for value in ('15:05','17:00','23:59'):
            path.write_text(yaml.safe_dump(dict(self.cfg,earliest_close_check=value)))
            self.assertEqual(load_config(path)['earliest_close_check'],value)

    def test_weekend_and_weekday_holiday_follow_calendar_not_weekday_guess(self):
        self.clock=datetime(2026,11,14,8,tzinfo=TZ)
        self.boot.sessions=['2026-11-10']  # Official fixture says Wed-Fri closed.
        self.service.cycle()
        self.assertEqual(self.boot.runs[-1][1],'2026-11-10')

    def test_empty_close_source_retains_queue_without_running_research(self):
        self.request();self.boot.available=False;state=self.service.cycle()
        self.assertEqual(state['phase'],'awaiting_close_data')
        self.assertFalse(self.boot.runs);self.assertFalse(self.pipeline_calls)
        self.assertTrue((self.runtime/'update-request.json').exists())

    def test_source_failure_never_leaks_secret_or_marks_completion(self):
        self.request();self.boot.error=True;state=self.service.cycle()
        self.assertEqual(state['status'],'failed')
        self.assertNotIn('credential-value',json.dumps(state))
        self.assertIsNone(state['last_success_date'])
        self.assertFalse(self.pipeline_calls)

    def test_pipeline_incomplete_keeps_request_and_old_success_date(self):
        self.request();self.pipeline_status='incomplete';self.service.state['last_success_date']='2026-11-10'
        state=self.service.cycle()
        self.assertEqual(state['status'],'failed')
        self.assertEqual(state['last_success_date'],'2026-11-10')
        self.assertTrue((self.runtime/'update-request.json').exists())

    def test_bootstrap_wrong_cutoff_cannot_run_pipeline(self):
        self.boot.bad_date=True;state=self.service.cycle()
        self.assertEqual(state['status'],'failed');self.assertFalse(self.pipeline_calls)

    def test_external_initialization_lock_blocks_all_queries(self):
        with ProcessLock(self.runtime/'bootstrap.lock'):
            state=self.service.cycle()
        self.assertEqual(state['phase'],'waiting_existing_job')
        self.assertFalse(self.boot.queries);self.assertFalse(self.boot.runs)

    def test_concurrent_updates_execute_only_one_bootstrap(self):
        self.boot.delay=.1
        with ThreadPoolExecutor(max_workers=12) as pool:
            states=list(pool.map(lambda _:self.service.cycle(),range(12)))
        self.assertEqual(len(self.boot.runs),1)
        self.assertEqual(sum(s['status']=='core_complete' for s in states),1)
        self.assertTrue(all(s['status'] in ('core_complete','busy') for s in states))

    def test_retry_delay_prevents_repeated_network_calls_before_due_time(self):
        self.boot.available=False;self.service.cycle();calls=len(self.boot.queries)
        self.service.cycle();self.assertEqual(len(self.boot.queries),calls)
        self.clock+=timedelta(seconds=self.cfg['retry_seconds'])
        self.service.cycle();self.assertGreater(len(self.boot.queries),calls)

    def test_interrupted_bootstrap_never_runs_pipeline_or_consumes_queue(self):
        self.request();self.boot.interrupt=True;state=self.service.cycle()
        self.assertEqual(state['status'],'interrupted');self.assertFalse(self.pipeline_calls)
        self.assertTrue((self.runtime/'update-request.json').exists())

    def test_same_date_completed_pipeline_is_not_automatically_recomputed(self):
        self.mark_completed()
        state=self.service.cycle()
        self.assertEqual(state['phase'],'up_to_date');self.assertFalse(self.boot.runs)

    def test_minute_verification_runs_after_real_write_lock_release(self):
        from data.research_service import _busy
        self.mark_completed();observations=[]
        def selected_status():
            observations.append(_busy(self.runtime/'update.lock'))
            # The actual write lock is free, but a concurrent full cycle cannot
            # sneak between the core completion and its read-only verification.
            with ProcessLock(self.runtime/'update.lock'):pass
            self.assertEqual(self.service.cycle()['status'],'busy')
            return {'research_runtime_dir':str(self.runtime),'current_date_ready':True,
                    'cache_verified':True,'deep_verified':not observations[-1],
                    'target_date':'2026-11-11','completed_symbols':300,'expected_symbols':300,
                    'service_alive':True}
        with patch('data.minute_readiness.public_status',side_effect=selected_status):
            state=self.service.cycle()
        self.assertEqual(observations,[False]);self.assertEqual(state['status'],'complete')
        self.assertTrue(state['overall_ready']);self.assertFalse(self.boot.runs)

    def test_unlocked_minute_summary_still_requires_deep_proof(self):
        self.mark_completed()
        selected={'research_runtime_dir':str(self.runtime),'current_date_ready':True,
                  'cache_verified':True,'deep_verified':False,'target_date':'2026-11-11',
                  'completed_symbols':300,'expected_symbols':300,'service_alive':True}
        with patch('data.minute_readiness.public_status',return_value=selected):
            state=self.service.cycle()
        self.assertEqual(state['status'],'core_complete');self.assertFalse(state['overall_ready'])

    def test_core_artifact_changed_during_minute_check_is_not_completed(self):
        self.mark_completed()
        def selected_status():
            (self.root/'result.json').write_text('{"changed":true}')
            return {'research_runtime_dir':str(self.runtime),'current_date_ready':True,
                    'cache_verified':True,'deep_verified':True,'target_date':'2026-11-11',
                    'completed_symbols':300,'expected_symbols':300,'service_alive':True}
        with patch('data.minute_readiness.public_status',side_effect=selected_status):
            state=self.service.cycle()
        self.assertEqual(state['status'],'waiting');self.assertFalse(state['overall_ready'])
        self.assertFalse(state['core_ready'])

    def test_deleted_verified_artifact_triggers_repair(self):
        self.mark_completed();(self.root/'result.json').unlink()
        state=self.service.cycle()
        self.assertEqual(state['status'],'core_complete');self.assertFalse(self.boot.runs)
        self.assertEqual(self.pipeline_calls,[True]);self.assertTrue((self.root/'result.json').is_file())

    def test_modified_verified_artifact_triggers_repair(self):
        self.mark_completed();(self.root/'result.json').write_text('{"changed":true}')
        self.assertEqual(self.service.cycle()['status'],'core_complete')
        self.assertEqual(self.pipeline_calls,[True])

    def test_missing_required_step_triggers_repair(self):
        self.mark_completed();path=self.runtime/'pipeline-state.json';data=json.loads(path.read_text());data['steps']=[];atomic_json(path,data)
        self.service.cycle();self.assertEqual(self.pipeline_calls,[True])

    def test_wrong_artifact_date_cannot_be_hidden_by_pipeline_date(self):
        self.mark_completed();path=self.runtime/'pipeline-state.json';data=json.loads(path.read_text());data['steps'][0]['evidence'][0]['data_date']='2026-11-10';atomic_json(path,data)
        self.service.cycle();self.assertEqual(self.pipeline_calls,[True])

    def test_old_bootstrap_stage_date_triggers_repair(self):
        self.mark_completed();path=self.runtime/'bootstrap-manifest.json';data=json.loads(path.read_text());data['stage_dates']['assets']='2026-11-10';atomic_json(path,data)
        self.service.cycle();self.assertEqual(len(self.boot.runs),1)
        self.assertEqual(self.boot.runs[0][0],('assets',))

    def test_minute_hourly_quota_does_not_block_or_repeat_core_work(self):
        self.mark_completed();path=self.runtime/'bootstrap-manifest.json';data=json.loads(path.read_text())
        data['stages']['minute']='rate_limited'
        data['coverage']['minute']={'scope':'current_hs300','frequency':'5min','covered_symbols':1,'expected_symbols':300,
                'rows':2989,'requested_end':'2026-11-11','coverage_complete':False,'status':'rate_limited'}
        deadline=(self.clock+timedelta(hours=1)).isoformat()
        data['source_cooldowns']={'stk_mins':{'category':'rate_limited','retry_at':deadline,'cooldown_seconds':3600}}
        atomic_json(path,data)
        state=self.service.cycle()
        self.assertEqual(state['status'],'core_complete');self.assertTrue(state['core_ready']);self.assertFalse(state['overall_ready'])
        self.assertTrue(state['minute_blocked']);self.assertEqual(state['minute_coverage']['covered_symbols'],1)
        self.assertEqual(state['minute_retry_at'],deadline);self.assertFalse(self.boot.runs);self.assertFalse(self.pipeline_calls)
        self.clock+=timedelta(hours=2)
        self.service.cycle();self.assertFalse(self.boot.runs);self.assertFalse(self.pipeline_calls)

    def test_public_status_separates_core_ready_from_minute_completeness(self):
        self.mark_completed()
        with patch('data.research_service._now',lambda:self.clock):status=public_status(self.cfg)
        self.assertTrue(status['core_ready']);self.assertTrue(status['minute_blocked']);self.assertFalse(status['overall_ready'])

    def test_changed_required_inputs_cannot_reuse_prior_pipeline_completion(self):
        self.mark_completed();path=self.runtime/'pipeline-state.json';data=json.loads(path.read_text());data['required_inputs']=['daily'];atomic_json(path,data)
        state=self.service.cycle()
        self.assertEqual(self.pipeline_calls,[True]);self.assertFalse(self.boot.runs)
        self.assertEqual(state['status'],'core_complete')

    def test_new_day_updates_only_core_and_never_attempts_minute_source(self):
        self.mark_completed();self.clock=datetime(2026,11,12,18,10,tzinfo=TZ)
        state=self.service.cycle()
        self.assertEqual(self.boot.runs,[(('daily','finance','assets'),'2026-11-12')])
        self.assertFalse(any('minute' in stages for stages,_ in self.boot.runs))
        self.assertEqual(state['last_success_date'],'2026-11-12');self.assertFalse(state['overall_ready'])

    def test_runner_complete_without_valid_evidence_still_fails(self):
        self.request()
        self.service.pipeline_runner=lambda:{'status':'complete','data_date':'2026-11-11','steps':[]}
        state=self.service.cycle()
        self.assertEqual(state['status'],'failed');self.assertTrue((self.runtime/'update-request.json').exists())

    def test_launcher_child_detaches_from_terminal_without_starting_it(self):
        from types import SimpleNamespace
        with patch('data.research_service.load_config',return_value=self.cfg),patch('data.research_service.subprocess.Popen',return_value=SimpleNamespace(pid=12345)) as popen:
            start_service(self.root)
        self.assertTrue(popen.call_args.kwargs['start_new_session'])
        self.assertEqual(popen.call_args.args[0][3:5],['data.research_service','run'])

    def test_parallel_manual_requests_create_one_queue_and_never_run_work(self):
        self.service.save(status='idle',busy=False)
        with patch('data.research_service._now',lambda:self.clock),ProcessLock(self.runtime/'service.lock'):
            with ThreadPoolExecutor(max_workers=8) as pool:
                responses=list(pool.map(lambda _:request_update({},self.cfg),range(8)))
        self.assertEqual(sum(r['accepted'] for r in responses),1)
        self.assertEqual(sum(r['http_status']==202 for r in responses),1)
        self.assertTrue((self.runtime/'update-request.json').exists());self.assertFalse(self.boot.runs)

    def test_offline_or_stale_service_does_not_accept_manual_queue(self):
        self.service.save(status='running',busy=True)
        with patch('data.research_service._now',lambda:self.clock+timedelta(minutes=5)),ProcessLock(self.runtime/'service.lock'):
            status=public_status(self.cfg);response=request_update({},self.cfg)
        self.assertFalse(status['service_alive']);self.assertEqual(response['http_status'],503)
        self.assertFalse((self.runtime/'update-request.json').exists())

    def test_manual_payload_cannot_force_intraday_date_or_other_source(self):
        with self.assertRaises(ValueError):
            request_update({'force':True},self.cfg)
        self.assertFalse((self.runtime/'update-request.json').exists())


if __name__=='__main__':
    unittest.main()
