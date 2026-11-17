"""Slow content verification must remain complete without blocking HTTP callers."""
import sqlite3
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from deck import daily_status, live_api
from deck.background_snapshot import BackgroundSnapshot


class SnapshotTests(unittest.TestCase):
    def test_slow_build_is_deduplicated_and_changed_version_is_explicitly_stale(self):
        snapshot = BackgroundSnapshot("isolated-snapshot")
        self.addCleanup(snapshot._executor.shutdown)
        started, release = threading.Event(), threading.Event()
        calls = []
        def slow():
            calls.append(1);started.set();release.wait(2)
            return {"actual": 12}
        t = time.monotonic()
        result = snapshot.read("a", slow)
        self.assertLess(time.monotonic()-t, .1)
        self.assertTrue(result["loading"]);self.assertIsNone(result["data"])
        self.assertTrue(started.wait(1))
        for _ in range(5):snapshot.read("a", slow)
        self.assertEqual(len(calls), 1)
        release.set()
        result = snapshot.read("a", slow, wait=True)
        self.assertTrue(result["current"]);self.assertEqual(result["data"], {"actual":12})
        release.clear()
        stale=snapshot.read("b", slow)
        self.assertTrue(stale["stale"]);self.assertFalse(stale["current"])
        release.set();snapshot.read("b", slow, wait=True)

    def test_failed_refresh_never_makes_old_snapshot_current(self):
        snapshot=BackgroundSnapshot("isolated-failure")
        self.addCleanup(snapshot._executor.shutdown)
        snapshot.read("a", lambda:{"actual":1}, wait=True)
        def failed():raise ValueError("actual input failed")
        result=snapshot.read("b",failed,wait=True)
        self.assertEqual(result["state"],"failed");self.assertTrue(result["stale"])
        self.assertFalse(result["current"]);self.assertEqual(result["data"],{"actual":1})

    def test_portal_cold_call_returns_progress_without_waiting_for_database(self):
        snapshot=BackgroundSnapshot("isolated-portal")
        self.addCleanup(snapshot._executor.shutdown)
        release=threading.Event()
        def collect():
            release.wait(2)
            return {"ts":"actual snapshot", "data_date":"2026-11-10", "daily_status":{"ready":True}, "pools":{"actual":4}}
        public={"research_data":{"status":"limited","ready":False,"message":"minute coverage pending"},"api_health":{"factor_pool":{"initialized":True,"ready":True}}}
        with patch.object(live_api,"_portal_snapshot",snapshot),patch.object(live_api,"_portal_version",return_value="v"),patch.object(live_api,"_collect_portal_dash",side_effect=collect),patch.object(live_api,"_portal_public_status",return_value=public):
            t=time.monotonic();cold=live_api.live_portal_dash()
            self.assertLess(time.monotonic()-t,.1);self.assertEqual(cold["state"],"probing")
            self.assertFalse(cold["ready"]);self.assertNotIn("pools",cold)
            self.assertEqual(cold["research_data"]["message"],"minute coverage pending")
            release.set();snapshot.read("v",collect,wait=True)
            warm=live_api.live_portal_dash()
            self.assertEqual(warm["state"],"limited");self.assertFalse(warm["ready"])
            self.assertEqual(warm["pools"],{"actual":4})

    def test_date_scan_reads_all_dates_deduplicates_increments_and_never_creates_a_db(self):
        with tempfile.TemporaryDirectory() as root:
            paths=[Path(root)/"bars.db",Path(root)/"bars_incr_older.db",Path(root)/"bars_incr_newer.db"]
            rows=[[("600001.SH","2026-11-09"),("600002.SH","2026-11-09"),("600003.SH","2026-11-09"),("600001.SH","2026-11-10")],
                  [("600001.SH","2026-11-10"),("600002.SH","2026-11-10")],
                  [("600001.SH","2026-11-10"),("600004.SH","2026-11-11")]]
            for path,values in zip(paths,rows):
                with sqlite3.connect(path) as con:
                    con.execute("CREATE TABLE daily_bar(code TEXT,date TEXT)")
                    con.executemany("INSERT INTO daily_bar VALUES(?,?)",values)
            before=[p.read_bytes() for p in paths]
            result=daily_status.scan_complete_date(paths,3)
            self.assertTrue(result["ready"]);self.assertEqual(result["date"],"2026-11-09")
            self.assertEqual(result["latest_observed"],"2026-11-11")
            self.assertEqual(before,[p.read_bytes() for p in paths])
            missing=Path(root)/"never-created.db"
            with self.assertRaises(sqlite3.OperationalError):daily_status.scan_complete_date([missing],3)
            self.assertFalse(missing.exists())

    def test_partial_day_has_no_best_date_fallback(self):
        with tempfile.TemporaryDirectory() as root:
            path=Path(root)/"bars.db"
            with sqlite3.connect(path) as con:
                con.execute("CREATE TABLE daily_bar(code TEXT,date TEXT)")
                con.execute("INSERT INTO daily_bar VALUES('600001.SH','2026-11-10')")
            result=daily_status.scan_complete_date([path],2)
            self.assertFalse(result["ready"]);self.assertIsNone(result["date"])


if __name__=="__main__":unittest.main()
