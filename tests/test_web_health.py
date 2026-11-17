"""Check transport health separately from missing data and application failures."""
import json
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch

from deck import live_api
from deck.background_snapshot import BackgroundSnapshot


class ResponseHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        responses = {
            "/ready": (200, {"ok": True}),
            "/missing": (404, {"error": "report not generated"}),
            "/failed": (200, {"ok": False, "error": "upstream unavailable"}),
            "/nested": (200, {"ok": True, "pool": {"error": "missing data"}}),
            "/paused": (200, {"ok":True,"paused":True,"market":{"error":"old permission failure"}}),
            "/armed": (200, {"ok":True,"paused":True,"activation":{"enabled":True,"status":"waiting_data"},"market":{"error":"old permission failure"}}),
        }
        status, body = responses.get(self.path, (200, None))
        self.send_response(status)
        self.end_headers()
        self.wfile.write(json.dumps(body).encode() if body is not None else b"invalid json")

    def log_message(self, *args):
        pass


class HealthTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), ResponseHandler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join()

    def test_response_status_and_business_errors(self):
        for path, status, ready in [("/ready", 200, True), ("/missing", 404, False),
                                    ("/failed", 200, False), ("/nested", 200, False),
                                    ("/invalid", 200, False)]:
            with self.subTest(path=path):
                result = live_api._probe_one(path, self.server.server_port)
                self.assertTrue(result["reachable"])
                self.assertEqual(result["status"], status)
                self.assertEqual(result["ok"], ready)
                if not ready:
                    self.assertTrue(result["error"])

    def test_connection_failure_is_distinct(self):
        with patch("urllib.request.OpenerDirector.open", side_effect=ConnectionRefusedError("refused")):
            result = live_api._probe_one("/ready", self.server.server_port)
        self.assertFalse(result["reachable"])
        self.assertEqual(result["status"], 0)
        self.assertEqual(result["state"], "connection_error")

    def test_explicit_pause_is_deferred_without_reclassifying_old_permission_error(self):
        result=live_api._probe_one("/paused",self.server.server_port)
        self.assertTrue(result["reachable"]);self.assertFalse(result["ok"])
        self.assertEqual(result["state"],"deferred");self.assertNotIn("old permission",result["error"])
        armed=live_api._probe_one("/armed",self.server.server_port)
        self.assertEqual(armed["state"],"waiting_data");self.assertNotIn("old permission",armed["error"])


class BackgroundHealthTests(unittest.TestCase):
    def setUp(self):
        self.snapshot=BackgroundSnapshot('isolated-endpoint-monitor')
        patcher=patch.object(live_api,'_endpoint_snapshot',self.snapshot);patcher.start();self.addCleanup(patcher.stop)
        patcher=patch.object(live_api,'_endpoint_progress',{'version':None,'endpoints':[],'started_at':0});patcher.start();self.addCleanup(patcher.stop)
        self.addCleanup(self.snapshot._executor.shutdown)

    def test_cold_monitor_returns_immediately_and_never_executes_unsafe_configured_gets(self):
        release=threading.Event();started=threading.Event();self.addCleanup(release.set)
        safe=['/api/live/factors','/api/live/pools']
        unsafe=['/api/manual_update','/api/export_data','/api/live/backtest_run','/api/subj_quant','/api/live/endpoints']
        def probe(path):
            started.set();release.wait(2)
            return {'path':path,'ok':True,'reachable':True,'status':200,'state':'ready'}
        with patch.object(live_api,'_health_config',return_value={'endpoints':safe+unsafe,'max_workers':2}), \
             patch.object(live_api,'_probe_one',side_effect=probe) as call, \
             patch.object(live_api,'live_portal_dash',side_effect=AssertionError('No synchronous warm-up')) as warm:
            begin=time.monotonic();cold=live_api.live_endpoints()
            self.assertLess(time.monotonic()-begin,.2);self.assertFalse(cold['ready']);self.assertTrue(cold['loading'])
            self.assertEqual(cold['total'],2);self.assertEqual(cold['checked_count'],0)
            self.assertEqual(cold['excluded_routes'],unsafe);self.assertTrue(started.wait(1))
            for _ in range(4):self.assertFalse(live_api.live_endpoints()['ready'])
            release.set();self.snapshot._future.result(timeout=3)
            ready=live_api.live_endpoints();self.assertTrue(ready['ready']);self.assertEqual(ready['checked_count'],2)
            self.assertEqual({args.args[0] for args in call.call_args_list},set(safe));self.assertEqual(call.call_count,2)
            self.assertTrue(live_api.live_endpoints()['ready']);self.assertEqual(call.call_count,2);warm.assert_not_called()

    def test_partial_probe_results_are_progress_not_completed_readiness(self):
        release=threading.Event();self.addCleanup(release.set)
        def probe(path):
            if path.endswith('pools'):release.wait(2)
            return {'path':path,'ok':True,'reachable':True,'status':200,'state':'ready'}
        with patch.object(live_api,'_health_config',return_value={'endpoints':['/api/live/factors','/api/live/pools'],'max_workers':2}), \
             patch.object(live_api,'_probe_one',side_effect=probe):
            live_api.live_endpoints()
            for _ in range(50):
                partial=live_api.live_endpoints()
                if partial['checked_count']==1:break
                time.sleep(.01)
            self.assertEqual(partial['checked_count'],1);self.assertFalse(partial['ready']);self.assertTrue(partial['loading'])
            release.set();self.snapshot._future.result(timeout=3)


if __name__ == "__main__":
    unittest.main()
