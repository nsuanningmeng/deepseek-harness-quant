"""Idempotent reviews publish receipts without changing real forward history."""
from copy import deepcopy
from datetime import datetime
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from data.research_bootstrap import TZ
from factors.opportunities import pitch_track as track


class ForwardSnapshotRefreshTests(unittest.TestCase):
    def setUp(self):
        temp=tempfile.TemporaryDirectory(); self.addCleanup(temp.cleanup)
        self.root=Path(temp.name); (self.root/'logs').mkdir()
        self.clock=datetime(2026,11,13,10,20,tzinfo=TZ)
        self.cfg={'runtime_dir':str(self.root),'min_daily_rows':2000,'completed_session_cutoff':'17:00'}
        manifest={'source':'tushare-official','target_end':'2026-11-12',
                  'coverage':{'daily':{'end':'2026-11-12'}},'completed':{'daily:2026-11-12':{'rows':5218}},
                  'stages':{k:'complete' for k in ('daily','finance','assets')},
                  'stage_dates':{k:'2026-11-12' for k in ('daily','finance','assets')}}
        (self.root/'bootstrap-manifest.json').write_text(json.dumps(manifest))
        self.entries=[{'code':'600000.SH','entry_date':'2026-11-11','entry_close':12.5,
                       'registered_at':'2026-11-11T19:00:00','factors_snapshot':{'roe':.13},
                       'score':94,'decided':'buy','source_pitch':'original.json','stop_plan':{'pct':-.08},
                       'fwd':{'t1':{'date':'2026-11-12','ret':-.03},'t5':None,'latest':{'date':'2026-11-12','ret':-.03}}}]
        self.original=self.root/'logs/pitch_track_pool_20261112_180000.json'
        self.original.write_text(json.dumps({'ts':'2026-11-12 18:00:00','entries':self.entries}))
        self.pitch=self.root/'logs/pitch_v2_20261113_102000.json'
        self.pitch.write_text(json.dumps({'date':'2026-11-12','pitch':[]}))
        p=patch.object(track,'BASE',self.root);p.start();self.addCleanup(p.stop)

    def review(self):
        return track.publish_reviewed_snapshot(self.pitch,core_cfg=self.cfg,now=self.clock)

    def test_repeated_noop_review_preserves_all_entries_and_original_file(self):
        before=self.original.read_bytes();mtime=self.original.stat().st_mtime_ns
        first=self.review();first_bytes=first.read_bytes();second=self.review()
        self.assertNotEqual(first,second);self.assertEqual(first.read_bytes(),first_bytes)
        self.assertEqual(self.original.read_bytes(),before);self.assertEqual(self.original.stat().st_mtime_ns,mtime)
        result=json.loads(second.read_text())
        self.assertEqual(result['entries'],self.entries);self.assertEqual(result['data_date'],'2026-11-12')
        expected=hashlib.sha256(json.dumps(self.entries,ensure_ascii=False,sort_keys=True).encode()).hexdigest()
        self.assertEqual(result['review']['entries_sha256'],expected)
        self.assertEqual(result['review']['entry_count'],1)

    def test_new_registration_today_is_preserved_without_backdated_returns(self):
        entries=deepcopy(self.entries)+[{'code':'600001.SH','entry_date':'2026-11-13',
                  'registered_at':self.clock.isoformat(),'entry_close':None,'factors_snapshot':{'roe':.08},
                  'fwd':{'t1':None,'t5':None,'t20':None,'t60':None,'latest':None}}]
        self.original.write_text(json.dumps({'entries':entries}))
        result=json.loads(self.review().read_text())
        self.assertEqual(result['entries'],entries)
        self.assertEqual(result['data_date'],'2026-11-12')

    def test_pipeline_cli_publishes_a_review_even_when_append_and_update_are_noops(self):
        publish=track.publish_reviewed_snapshot
        with patch.object(track,'append_pitch',return_value={'entries':self.entries}) as append, \
                patch.object(track,'update_fwd',return_value={'entries':self.entries}) as update, \
                patch.object(track,'publish_reviewed_snapshot',side_effect=lambda path: publish(
                    path,core_cfg=self.cfg,now=self.clock)) as review:
            track.main(['--append',str(self.pitch),'--update'])
        append.assert_called_once_with(self.pitch);update.assert_called_once_with()
        review.assert_called_once_with(str(self.pitch))
        result=track.load_latest()
        self.assertEqual(result['entries'],self.entries)
        self.assertEqual(result['review']['data_date'],'2026-11-12')

    def test_unclosed_outcome_or_mismatched_signal_cannot_publish(self):
        before=set((self.root/'logs').iterdir())
        entries=deepcopy(self.entries);entries[0]['fwd']['t1']['date']='2026-11-13'
        self.original.write_text(json.dumps({'entries':entries}))
        with self.assertRaises(ValueError):self.review()
        self.assertEqual(set((self.root/'logs').iterdir()),before)
        self.original.write_text(json.dumps({'entries':self.entries}))
        self.pitch.write_text(json.dumps({'date':'2026-11-13','pitch':[]}))
        with self.assertRaises(ValueError):self.review()
        self.assertEqual(set((self.root/'logs').iterdir()),before)

    def test_missing_or_invalid_close_checkpoint_never_becomes_a_success_receipt(self):
        manifest=self.root/'bootstrap-manifest.json'
        manifest.write_text('{}')
        with self.assertRaises(ValueError):self.review()
        self.assertEqual(len(list((self.root/'logs').glob('pitch_track_pool_*.json'))),1)
