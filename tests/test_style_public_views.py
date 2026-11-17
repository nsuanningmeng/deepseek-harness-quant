"""Legacy files cannot bypass the current style producer's evidence checks."""
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from deck import live_api, research_views


class StylePublicViewsTests(TestCase):
    def test_rotation_rejects_stale_legacy_files_when_current_producer_is_not_ready(self):
        with TemporaryDirectory() as folder:
            root=Path(folder);(root/'config').mkdir()
            (root/'config/style_diagnostic.yaml').write_text('configured: true\n')
            legacy=root/'legacy.csv';legacy.write_text('date,dominant\n2099-01-01,unexpected old style\n')
            (root/'factor_style_rotation_old.json').write_text(json.dumps({'current_style':'unexpected old style','weights':{'boost':['all']}}))
            with patch.object(live_api,'BASE',root), patch.object(live_api,'_STYLE_STATE_CSV',legacy), \
                 patch.object(live_api,'_UI_DATA_DIR',root), \
                 patch.object(research_views,'style_diagnostic_state',return_value={'ready':False,'reason':'current configuration differs'}), \
                 patch.object(live_api,'live_timing_dash',return_value={}):
                result=live_api.live_rotation_calendar()
            self.assertEqual(result['state'],'missing_data')
            self.assertFalse(result['initialized']);self.assertIsNone(result['current_style'])
            self.assertEqual(result['weights'],{})

    def test_current_diagnostic_is_initialized_but_never_becomes_calibrated_weights(self):
        state={'kind':'size_style_diagnostic','ready':True,'fresh':True,'current_style':'isolated actual style',
               'style_history':[{'date':'2026-10-30','dominant':'isolated actual style'}],
               'calibrated':False,'weights_enabled':False,'auto_trade_allowed':False,
               'weights':{'boost':['must not be used']},'data_date':'2026-11-11'}
        with patch.object(research_views,'style_diagnostic_state',return_value=state), \
             patch.object(live_api,'live_timing_dash',return_value={}):
            result=live_api.live_rotation_calendar()
        self.assertTrue(result['initialized']);self.assertTrue(result['ready'])
        self.assertEqual(result['state'],'limited_research');self.assertEqual(result['weights'],{})
        self.assertFalse(result['auto_trade_allowed'])

    def test_style_adapter_requires_the_producer_ready_fresh_and_kind_contract(self):
        from data import style_diagnostic
        with TemporaryDirectory() as folder:
            root=Path(folder);(root/'config').mkdir()
            (root/'config/style_diagnostic.yaml').write_text('configured: true\n')
            with patch.object(research_views,'BASE',root),patch.object(style_diagnostic,'BASE',root):
                for value in ({'kind':'legacy','ready':True,'fresh':True},
                              {'kind':'size_style_diagnostic','ready':True,'fresh':False}):
                    with patch.object(style_diagnostic,'public_state',return_value=value):
                        self.assertFalse(research_views.style_diagnostic_state()['ready'])
