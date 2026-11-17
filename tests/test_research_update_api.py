"""Loopback-only route tests; the service is mocked and no job is queued."""
import json
import sys
import threading
import types
import unittest
from unittest.mock import patch
import urllib.error
import urllib.request

from deck.deck_server import Handler, ThreadingHTTPServer


class QuietHandler(Handler):
    def log_message(self, *args):
        pass


class ResearchUpdateApiTests(unittest.TestCase):
    def setUp(self):
        self.calls=[]
        self.state={"ok":True,"busy":False,"status":"idle","service_alive":True,"progress":None}
        def request_update(payload):
            self.calls.append(payload)
            return {**self.state,"accepted":True,"queued":True,"http_status":202}
        module=types.SimpleNamespace(public_status=lambda:self.state,request_update=request_update)
        self.patch=patch.dict(sys.modules,{"data.research_service":module});self.patch.start();self.addCleanup(self.patch.stop)
        self.server=ThreadingHTTPServer(("127.0.0.1",0),QuietHandler)
        self.thread=threading.Thread(target=self.server.serve_forever,daemon=True);self.thread.start()
        self.addCleanup(self.close)
        self.host=f"127.0.0.1:{self.server.server_port}"

    def close(self):
        self.server.shutdown();self.server.server_close();self.thread.join()

    def request(self, method="POST", body=b"{}", origin=True, content_type="application/json", extra=None,path="/api/research_update"):
        headers={"Content-Type":content_type}
        if origin is not False:
            headers["Origin"]="http://"+self.host if origin is True else origin
        headers.update(extra or {})
        request=urllib.request.Request("http://"+self.host+path,data=body if method=="POST" else None,method=method,headers=headers)
        opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
        try:
            response=opener.open(request,timeout=2)
        except urllib.error.HTTPError as exc:
            response=exc
        with response:
            return response.code,json.loads(response.read())

    def test_status_only_reads_and_valid_post_only_queues(self):
        code,body=self.request(method="GET",origin=False)
        self.assertEqual(code,200);self.assertEqual(body["status"],"idle");self.assertFalse(self.calls)
        code,body=self.request()
        self.assertEqual(code,202);self.assertTrue(body["queued"]);self.assertNotIn("http_status",body)
        self.assertEqual(self.calls,[{}])

    def test_origin_is_required_and_must_match_exact_host(self):
        for origin in (False,"https://evil.example","null","http://[",f"http://localhost:{self.server.server_port}","http://"+self.host+"/path"):
            with self.subTest(origin=origin):
                self.assertEqual(self.request(origin=origin)[0],403)
        self.assertEqual(self.request(extra={"Sec-Fetch-Site":"cross-site"})[0],403)
        self.assertFalse(self.calls)

    def test_only_an_empty_json_object_is_accepted(self):
        for body in (b"[]",b"null",b'{"force":true}',b"invalid",b"{}"*600):
            with self.subTest(body=body[:20]):
                self.assertEqual(self.request(body=body)[0],400)
        self.assertEqual(self.request(content_type="text/plain")[0],415)
        self.assertFalse(self.calls)

    def test_legacy_get_cannot_start_a_task_and_post_uses_the_same_queue(self):
        self.assertEqual(self.request(method="GET",path="/api/manual_update")[0],405)
        self.assertFalse(self.calls)
        self.assertEqual(self.request(path="/api/manual_update",origin=False)[0],403)
        code,body=self.request(path="/api/manual_update")
        self.assertEqual(code,202);self.assertTrue(body["queued"]);self.assertEqual(self.calls,[{}])


if __name__=="__main__":
    unittest.main()
