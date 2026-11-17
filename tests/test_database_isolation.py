"""The unit-test runner must reject project database I/O before opening files."""
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
import threading
from unittest import TestCase

from validation.run_isolated_tests import DatabaseIsolationGuard, ProductionDatabaseAccess, ProductionNetworkAccess


class DatabaseIsolationTests(TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.protected = self.root/'fake-project'; self.protected.mkdir()
        self.guard = DatabaseIsolationGuard([self.protected]).install()
        self.addCleanup(setattr, self.guard, 'active', False)

    def test_network_audit_rejects_production_ports_and_external_destinations_before_connect(self):
        self.guard.protect_network=True
        forbidden=[('127.0.0.1',8787),('localhost',3080),('::1',8787,0,0),
                   ('198.51.100.1',80),('external.invalid',443)]
        for address in forbidden:
            with self.assertRaises(ProductionNetworkAccess):self.guard._audit('socket.connect',(object(),address))
        self.assertEqual(len(self.guard.network_violations),len(forbidden))
        for address in (('127.0.0.1',49231),('::1',49232,0,0),('localhost',49233)):
            self.guard._audit('socket.connect',(object(),address))
        self.assertEqual(len(self.guard.network_violations),len(forbidden))

    def test_path_uri_alias_and_thread_attempts_are_blocked_before_file_creation(self):
        path = self.protected/'never-created.db'
        from sqlite3 import dbapi2
        for database in (path, str(path), path.as_uri()+'?mode=ro&immutable=1'):
            with self.assertRaises(ProductionDatabaseAccess): dbapi2.connect(database)
        caught = []
        def worker():
            try: sqlite3.connect(path)
            except ProductionDatabaseAccess: caught.append(True)
        thread=threading.Thread(target=worker); thread.start(); thread.join(timeout=2)
        self.assertEqual(caught,[True]); self.assertEqual(len(self.guard.violations),4)
        self.assertFalse(path.exists())

    def test_symlink_and_percent_encoded_uri_cannot_escape_protected_root(self):
        link=self.root/'alias'; link.symlink_to(self.protected,target_is_directory=True)
        path=link/'space name.db'
        with self.assertRaises(ProductionDatabaseAccess): sqlite3.connect(path.as_uri()+'?mode=rwc',uri=True)
        self.assertFalse((self.protected/'space name.db').exists())

    def test_memory_and_external_fixture_databases_remain_usable(self):
        for database, uri in ((':memory:',False),('file::memory:?cache=shared',True),
                              ((self.protected/'memory-only.db').as_uri()+'?mode=memory',True),
                              (self.root/'fixture.db',False)):
            with sqlite3.connect(database,uri=uri) as con:
                con.execute('CREATE TABLE actual_fixture(value INTEGER)')
                con.execute('INSERT INTO actual_fixture VALUES(7)')
                self.assertEqual(con.execute('SELECT value FROM actual_fixture').fetchone()[0],7)
        self.assertEqual(self.guard.violations,[])

    def test_attached_database_is_also_blocked_before_io(self):
        path=self.protected/'never-attached.db'
        with sqlite3.connect(':memory:') as con:
            with self.assertRaises(sqlite3.DatabaseError):
                con.execute("ATTACH DATABASE '"+str(path).replace("'","''")+"' AS forbidden")
            with self.assertRaises(ProductionDatabaseAccess): con.execute('ATTACH DATABASE ? AS forbidden',(str(path),))
            with self.assertRaises(ProductionDatabaseAccess): con.cursor().execute('ATTACH DATABASE :path AS forbidden',{'path':str(path)})
        self.assertEqual(len(self.guard.violations),3); self.assertFalse(path.exists())

    def test_attaching_an_explicit_temporary_fixture_is_allowed(self):
        path=self.root/'attached-fixture.db'
        with sqlite3.connect(':memory:') as con:
            con.execute('ATTACH DATABASE ? AS fixture',(str(path),))
            con.execute('CREATE TABLE fixture.values_table(value INTEGER)')
            con.execute('INSERT INTO fixture.values_table VALUES(9)')
            self.assertEqual(con.execute('SELECT value FROM fixture.values_table').fetchone()[0],9)
            con.commit();con.execute('DETACH DATABASE fixture')
            with self.assertRaises(ProductionDatabaseAccess):
                con.execute('ATTACH DATABASE ? AS fixture',(str(self.protected/'cached-statement.db'),))
        self.assertEqual(len(self.guard.violations),1)
        self.assertFalse((self.protected/'cached-statement.db').exists())
