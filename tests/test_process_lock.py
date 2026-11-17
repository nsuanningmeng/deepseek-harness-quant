"""Real advisory locks in temporary files; no application services or databases."""
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase,skipIf
from unittest.mock import patch

from autotrade.locking import ProcessLock,lock_is_busy


class ProcessLockTests(TestCase):
    def setUp(self):
        self.temp=TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.path=Path(self.temp.name)/'coordination.lock'

    def test_probe_does_not_create_and_open_race_fails_closed(self):
        self.assertFalse(lock_is_busy(self.path,shared=True))
        self.assertFalse(self.path.exists())
        self.path.write_bytes(b'original')
        with patch.object(Path,'open',side_effect=FileNotFoundError):
            self.assertTrue(lock_is_busy(self.path,shared=True))

    def test_exclusive_default_still_excludes_readers_and_writers(self):
        with ProcessLock(self.path):
            self.assertTrue(lock_is_busy(self.path))
            self.assertTrue(lock_is_busy(self.path,shared=True))
            for shared in (False,True):
                with self.subTest(shared=shared),self.assertRaises(RuntimeError):
                    with ProcessLock(self.path,shared=shared):
                        self.fail('an exclusive holder must exclude every peer')
        self.assertFalse(lock_is_busy(self.path))

    @skipIf(os.name=='nt','Windows conservatively retains exclusive locking')
    def test_shared_readers_coexist_without_changing_contents_or_mtime(self):
        for content in (b'',b'original lock metadata'):
            with self.subTest(content=content):
                self.path.write_bytes(content);before=self.path.stat().st_mtime_ns
                with ProcessLock(self.path,shared=True),ProcessLock(self.path,shared=True):
                    self.assertFalse(lock_is_busy(self.path,shared=True))
                    self.assertTrue(lock_is_busy(self.path))
                    with self.assertRaises(RuntimeError):
                        with ProcessLock(self.path):
                            self.fail('a writer must not enter a shared read section')
                    self.assertEqual(self.path.read_bytes(),content)
                    self.assertEqual(self.path.stat().st_mtime_ns,before)
                self.assertEqual(self.path.read_bytes(),content)
                self.assertEqual(self.path.stat().st_mtime_ns,before)
                with ProcessLock(self.path):
                    self.assertTrue(lock_is_busy(self.path,shared=True))
