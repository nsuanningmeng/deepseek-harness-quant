"""Run one requested Pitch scan at a time and publish the latest finished result."""

import json
import logging
from pathlib import Path
import threading

from autotrade.locking import ProcessLock
from autotrade.monitoring import atomic_json, utc_now


_LOG = logging.getLogger(__name__)


class PitchScanJobs:
    def __init__(self, directory):
        directory = Path(directory)
        self.latest_path = directory / "latest.json"
        self.lock_path = directory / "scan.lock"
        self._guard = threading.Lock()
        self._job = None

    def status(self):
        from backtest.pitch_scan import get_defaults

        defaults = get_defaults()
        with self._guard:
            try:
                latest = json.loads(self.latest_path.read_text(encoding="utf-8"))
            except FileNotFoundError:
                latest = None
            return {"ok": True, "defaults": defaults, "latest": latest, "job": self._job}

    def start(self, payload):
        from backtest.pitch_scan import run_scan, validate_request

        params = validate_request(payload)
        with self._guard:
            if self._job and self._job["status"] == "running":
                return self._busy(self._job)
            process_lock = ProcessLock(self.lock_path)
            try:
                process_lock.__enter__()
            except RuntimeError:
                return self._busy({"status": "running"})

            job = {"status": "running", "started_at": utc_now()}
            worker = threading.Thread(
                target=self._finish, args=(job, run_scan, params, process_lock),
                name="pitch-scan", daemon=True,
            )
            try:
                worker.start()
            except Exception:
                process_lock.__exit__()
                raise
            self._job = job
            return {"ok": True, "job": job}, 202

    @staticmethod
    def _busy(job):
        return {"ok": False, "error": "已有短线 Pitch 扫参正在运行", "job": job}, 409

    def _finish(self, job, run_scan, params, process_lock):
        completion = {"status": "completed"}
        try:
            result = run_scan(params)
            atomic_json(self.latest_path, result)
        except Exception as exc:
            _LOG.exception("Pitch scan failed")
            error = str(exc)[:240] if isinstance(exc, ValueError) else "扫参运行失败，请查看本机门户日志"
            completion = {"status": "failed", "error": error}
        finally:
            with self._guard:
                process_lock.__exit__()
                self._job = {**job, **completion, "finished_at": utc_now()}


jobs = PitchScanJobs(Path(__file__).resolve().parents[1] / "runtime" / "pitch_scan")
