"""进程退出自动释放的锁；POSIX 支持共享读锁，Windows 保守使用互斥锁。"""
import os
from pathlib import Path


def _acquire(handle, *, shared=False):
    handle.seek(0)
    if os.name == "nt":
        import msvcrt
        # Microsoft documents LK_NBRLCK as equivalent to LK_NBLCK, not a
        # shared reader lock. Keep the existing conservative Windows behavior.
        msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
    else:
        import fcntl
        mode = fcntl.LOCK_SH if shared else fcntl.LOCK_EX
        fcntl.flock(handle.fileno(), mode | fcntl.LOCK_NB)


def lock_is_busy(path, *, shared=False):
    """Probe an existing lock without creating or changing its contents.

    A shared POSIX probe conflicts only with an exclusive holder. Windows
    retains the exclusive fallback. Any open/acquire race fails closed.
    """
    path = Path(path)
    if not path.exists():
        return False
    try:
        with path.open("r+b" if os.name == "nt" else "rb") as handle:
            _acquire(handle, shared=shared)
        return False
    except OSError:
        return True


class ProcessLock:
    def __init__(self, path, *, shared=False):
        self.path = Path(path)
        self.shared = shared
        self.handle = None

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = self.path.open("a+b")
        try:
            _acquire(self.handle, shared=self.shared)
            if not self.shared:
                self.handle.seek(0, 2)
                if not self.handle.tell():
                    self.handle.write(b"0")
                    self.handle.flush()
        except OSError as exc:
            self.handle.close()
            self.handle = None
            raise RuntimeError("another autotrade cycle is running") from exc
        return self

    def __exit__(self, *args):
        if self.handle:
            self.handle.close()
            self.handle = None
