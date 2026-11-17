"""Verify the job lifecycle and publication through the loopback route."""

import json
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import ProxyHandler, Request, build_opener

from deck.deck_server import Handler, ThreadingHTTPServer
from deck import pitch_scan_jobs


class QuietHandler(Handler):
    def log_message(self, *args):
        pass


class PitchScanApiTests(unittest.TestCase):
    def setUp(self):
        self.release = threading.Event()
        self.called = threading.Event()
        self.failure = False
        self.defaults = {"grid": {"topn": [6]}}
        self.result = {"candidate_count": 1}

        def validate(payload):
            if payload != self.defaults:
                raise ValueError("参数无效")
            return payload

        def run(_params):
            self.called.set()
            if not self.release.wait(3):
                raise RuntimeError("test did not release scan")
            if self.failure:
                raise ValueError("样本不足")
            return self.result

        module = SimpleNamespace(get_defaults=lambda: self.defaults, validate_request=validate, run_scan=run)
        modules = patch.dict(sys.modules, {"backtest.pitch_scan": module})
        modules.start()
        self.addCleanup(modules.stop)

        directory = self.enterContext(tempfile.TemporaryDirectory())
        self.jobs = pitch_scan_jobs.PitchScanJobs(directory)
        self.enterContext(patch.object(pitch_scan_jobs, "jobs", self.jobs))
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), QuietHandler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.close)
        self.url = f"http://127.0.0.1:{self.server.server_port}"

    def close(self):
        self.release.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=3)

    def request(self, payload=None, *, origin=True):
        headers = {"Content-Type": "application/json"}
        if origin:
            headers["Origin"] = self.url
        body = json.dumps(payload).encode() if payload is not None else None
        request = Request(self.url + "/api/live/pitch_scan", data=body, headers=headers)
        try:
            response = build_opener(ProxyHandler({})).open(request, timeout=3)
        except HTTPError as exc:
            response = exc
        with response:
            return response.code, json.load(response)

    def wait_for_status(self, expected):
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            code, status = self.request()
            self.assertEqual(code, 200)
            if (status["job"] or {}).get("status") == expected:
                return status
            time.sleep(0.01)
        self.fail(f"未观察到 {expected} 状态")

    def test_get_does_not_run_and_post_publishes_once(self):
        self.assertEqual(self.request(), (200, {
            "ok": True, "defaults": self.defaults, "latest": None, "job": None,
        }))
        self.assertEqual(self.request(self.defaults, origin=False)[0], 403)
        self.assertEqual(self.request({})[0], 400)
        self.assertFalse(self.called.is_set())
        code, started = self.request(self.defaults)
        self.assertEqual(code, 202)
        self.assertTrue(self.called.wait(2))
        self.assertEqual(started["job"]["status"], "running")
        self.assertEqual(self.request(self.defaults)[0], 409)
        self.assertIsNone(self.request()[1]["latest"])
        self.release.set()
        self.assertEqual(self.wait_for_status("completed")["latest"], self.result)
        fresh_jobs = pitch_scan_jobs.PitchScanJobs(self.jobs.latest_path.parent)
        self.assertEqual(fresh_jobs.status()["latest"], self.result)

    def test_failed_run_preserves_previous_result_and_can_retry(self):
        previous = {"candidate_count": 3}
        pitch_scan_jobs.atomic_json(self.jobs.latest_path, previous)
        self.failure = True
        self.release.set()
        with self.assertLogs("deck.pitch_scan_jobs", level="ERROR"):
            self.assertEqual(self.request(self.defaults)[0], 202)
            failed = self.wait_for_status("failed")
        self.assertEqual(failed["job"]["error"], "样本不足")
        self.assertEqual(failed["latest"], previous)
        self.failure = False
        self.assertEqual(self.request(self.defaults)[0], 202)
        self.assertEqual(self.wait_for_status("completed")["latest"], self.result)


if __name__ == "__main__":
    unittest.main()
