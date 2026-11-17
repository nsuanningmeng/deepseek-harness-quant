"""Forward-chain readiness must follow real content evidence, never review time."""
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from deck import live_api as live, research_views, system_live


class ForwardChainReadinessTests(unittest.TestCase):
    def setUp(self):
        temp=tempfile.TemporaryDirectory();self.addCleanup(temp.cleanup)
        self.root=Path(temp.name);(self.root/'logs').mkdir()
        (self.root/'runtime/research').mkdir(parents=True)
        self.path=self.root/'logs/pitch_track_pool_20261113_101836_204107.json'
        self.target='2026-11-12'
        self.data={'data_date':self.target,'ts':'2026-11-13 10:18:36','entries':[
            {'code':'600000.SH','entry_date':'2026-11-11','fwd':{'t1':{'date':self.target,'ret':0.0}}}]}
        self.publish()

    def publish(self):
        self.path.write_text(json.dumps(self.data))
        self.pipeline={'steps':[{'status':'success','returncode':0,'evidence':[
            {'path':str(self.path),'source':'isolated-fixture','algorithm':'real forward review',
             'data_date':self.data['data_date'],'sha256':hashlib.sha256(self.path.read_bytes()).hexdigest()}]}]}
        (self.root/'runtime/research/pipeline-state.json').write_text(json.dumps(self.pipeline))

    def result(self):
        return live._forward_chain_readiness(self.path,self.data,self.pipeline,self.target)

    def test_review_timestamp_is_not_market_date_and_real_zero_return_is_mature(self):
        for label in ('远期池','实盘裁决'):
            self.assertEqual(live._extract_content_date(label,self.data),self.target)
            self.assertEqual(live._extract_content_date(label,{'ts':self.data['ts']}),'')
        actual=self.result()
        self.assertTrue(actual['ok']);self.assertTrue(actual['initialized'])
        self.assertEqual(actual['state'],'ready');self.assertEqual(actual['date'],self.target)

    def test_missing_and_tampered_proof_never_becomes_ready(self):
        self.assertFalse(live._forward_chain_readiness(self.path,self.data,{},self.target)['ok'])
        self.path.write_text(self.path.read_text()+' ')
        actual=self.result();self.assertFalse(actual['ok']);self.assertFalse(actual['initialized'])
        self.assertEqual(actual['state'],'unknown');self.assertIn('哈希',actual['note'])

    def test_stale_and_future_content_are_rejected_even_with_matching_hash(self):
        for date,state in [('2026-11-11','stale'),('2026-11-13','unknown')]:
            with self.subTest(date=date):
                self.data['data_date']=date;self.publish()
                actual=self.result();self.assertFalse(actual['ok'])
                self.assertFalse(actual['initialized']);self.assertEqual(actual['state'],state)

    def test_initialized_immature_entries_remain_pending_history(self):
        self.data['entries'][0]['fwd']={'t1':None,'latest':{'date':self.target,'ret':0.0}}
        self.publish();actual=self.result()
        self.assertTrue(actual['initialized']);self.assertFalse(actual['ok'])
        self.assertEqual(actual['state'],'pending_history')

    def test_invalid_business_rows_cannot_be_rescued_by_good_dates(self):
        self.data['entries']=[{'fwd':{'t1':{'ret':.2}}}];self.publish()
        actual=self.result();self.assertFalse(actual['ok']);self.assertEqual(actual['state'],'failed')

    def test_chain_both_nodes_use_evidence_and_cache_detects_modified_source(self):
        names=['观察池','新择时','今日信号','竞价信号','机会池','Pitch 长线','Pitch 短线','持仓','止盈引擎','风控']
        optional={name:{'state':'optional_unconfigured','reason':'isolated optional input'} for name in names}
        with patch.object(live,'BASE',self.root),patch.object(system_live,'BASE',self.root), \
                patch.object(live,'_bars_version',return_value=0), \
                patch.object(live,'_chain_cache',{'ts':0,'data':None}), \
                patch.object(live,'_health_config',return_value={'chain_optional_inputs':optional}), \
                patch.object(research_views,'_minute_background_job',return_value={}), \
                patch.object(live,'_minute_node',return_value={'name':'分钟','ok':True,'required_for_core':False,'state':'ready'}), \
                patch.object(live,'live_validation',return_value={'diagnosis':{'down_warn':[]}}):
            status={'ready':True,'state':'ready','date':self.target}
            result=live.live_chain(_date_status=status)
            nodes={n['name']:n for n in result['chain']}
            self.assertTrue(nodes['远期池']['ok']);self.assertTrue(nodes['实盘裁决']['ok'])
            self.assertEqual(nodes['远期池']['date'],self.target)
            self.assertTrue(result['core_ready'])
            self.path.write_text(self.path.read_text()+' ')
            result=live.live_chain(_date_status=status)
            nodes={n['name']:n for n in result['chain']}
            self.assertFalse(nodes['远期池']['ok']);self.assertFalse(nodes['实盘裁决']['ok'])
            self.assertFalse(result['core_ready'])
            self.data['entries'][0]['fwd']={'t1':None};self.publish()
            result=live.live_chain(_date_status=status)
            nodes={n['name']:n for n in result['chain']}
            for label in ('远期池','实盘裁决'):
                self.assertEqual(nodes[label]['state'],'pending_history')
                self.assertNotIn('无降权类型',nodes[label]['note'])
