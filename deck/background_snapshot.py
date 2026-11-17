"""Versioned, non-blocking snapshots. File versions invalidate, never certify data."""
import threading
import time
from concurrent.futures import ThreadPoolExecutor


class BackgroundSnapshot:
    def __init__(self, name, retry_seconds=30):
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix=name)
        self._lock = threading.Lock()
        self._future = None
        self._version = None
        self._data = None
        self._updated_at = None
        self._failed_version = None
        self._error = None
        self._failed_at = 0
        self._retry_seconds = retry_seconds

    def read(self, version, build, *, wait=False, max_age=None):
        """Only one worker exists per snapshot, including when inputs change mid-scan."""
        with self._lock:
            if self._future is not None and self._future.done():
                future, future_version = self._future, self._future_version
                self._future = None
                try:
                    self._data = future.result()
                    self._version = future_version
                    self._updated_at = time.time()
                    self._error = None
                    self._failed_version = None
                except Exception as exc:
                    self._error = str(exc)[:240]
                    self._failed_version = future_version
                    self._failed_at = time.time()
            expired = max_age is not None and self._updated_at is not None and time.time() - self._updated_at >= max_age
            current = self._data is not None and self._version == version and not expired
            retry = self._failed_version != version or time.time() - self._failed_at >= self._retry_seconds
            if not current and self._future is None and retry:
                self._future_version = version
                self._future = self._executor.submit(build)
            future = self._future
            result = {"data": self._data, "current": current, "loading": future is not None,
                      "stale": self._data is not None and not current,
                      "updated_at": self._updated_at, "error": self._error,
                      "state": "ready" if current else "probing" if future is not None else "failed"}
        if wait and future is not None:
            # Never wait while holding the lock; the HTTP caller uses wait=False.
            try:
                future.result()
            except Exception:
                pass
            return self.read(version, build, wait=False, max_age=max_age)
        return result

    def invalidate(self):
        with self._lock:
            self._version = None
            self._failed_version = None
