"""Run unit tests with a pre-I/O SQLite guard against all project databases.

Usage: python validation/run_isolated_tests.py [tests.test_module ...]
The guard starts before discovery/import, also covers sqlite3 aliases and worker
threads, and fails the run even when application code catches its exception.
Fixtures must use temporary directories outside the checkout or memory databases.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
import ipaddress
import os
from pathlib import Path
import re
import sqlite3
import sqlite3.dbapi2
import sys
import threading
import time
import traceback
import unittest
import weakref
from urllib.parse import parse_qs, unquote, urlsplit

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


class ProductionDatabaseAccess(AssertionError):
    pass


class ProductionNetworkAccess(AssertionError):
    pass


def _execute(connection, method, statement, parameters):
    previous = getattr(connection, '_test_attach_target', None)
    match = re.match(r'^\s*ATTACH(?:\s+DATABASE)?\s+(\?\d*|[:@$]\w+)\s+AS\b', statement, re.I)
    target = None
    if match:
        binding = match[1]
        try:
            target = (parameters[int(binding[1:] or 1)-1] if binding.startswith('?')
                      else parameters[binding[1:]])
        except (KeyError, IndexError, TypeError):
            pass
        if target is None:
            sys.audit('quant_test.sqlite_attach_unknown')
        else:
            # SQLite may cache prepared ATTACH statements without invoking the
            # authorizer again when the next execution binds a different path.
            sys.audit('quant_test.sqlite_attach', target)
    connection._test_attach_target = target
    try:
        return method(statement, parameters)
    finally:
        connection._test_attach_target = previous


class IsolatedCursor(sqlite3.Cursor):
    def execute(self, statement, parameters=()):
        return _execute(self.connection, super().execute, statement, parameters)


class IsolatedConnection(sqlite3.Connection):
    def execute(self, statement, parameters=()):
        return _execute(self, super().execute, statement, parameters)

    def cursor(self, factory=IsolatedCursor):
        return super().cursor(factory)


class DatabaseIsolationGuard:
    def __init__(self, protected_roots, *, protect_network=False):
        self.roots = tuple(Path(root).resolve() for root in protected_roots)
        self.violations = []
        self.network_violations = []
        self.protect_network = protect_network
        self.current_test = None
        self.active = False
        self._lock = threading.Lock()

    @staticmethod
    def database_path(database):
        try:
            value = os.fsdecode(os.fspath(database))
        except TypeError:
            return None
        if value in ('', ':memory:'):
            return None
        if value.startswith('file:'):
            uri = urlsplit(value)
            if parse_qs(uri.query).get('mode') == ['memory']:
                return None
            value = unquote(uri.path)
            if value == ':memory:':
                return None
        return Path(value).resolve()

    def check(self, database):
        path = self.database_path(database)
        if path is None or not any(path.is_relative_to(root) for root in self.roots):
            return
        self._deny(str(path))

    def _deny(self, database):
        record = {'database': database, 'test': self.current_test,
                  'thread': threading.current_thread().name,
                  'stack': traceback.format_stack(limit=24)[:-1]}
        with self._lock:
            self.violations.append(record)
        raise ProductionDatabaseAccess('Test attempted project database I/O: ' + database)

    def _audit(self, event, args):
        if self.active and event in ('sqlite3.connect', 'quant_test.sqlite_attach'):
            self.check(args[0])
        elif self.active and event == 'quant_test.sqlite_attach_unknown':
            self._deny('<unresolved ATTACH database target>')
        elif self.active and self.protect_network and event == 'socket.connect':
            self.check_network(args[1])

    def check_network(self, address):
        # Unix-domain sockets are local IPC; the production HTTP services are
        # explicitly denied even though their addresses are loopback.
        if not isinstance(address, tuple) or len(address) < 2:
            return
        host, port = str(address[0]).lower().rstrip('.'), int(address[1])
        try:
            ip = ipaddress.ip_address(host.split('%')[0])
            loopback = ip.is_loopback or bool(getattr(ip,'ipv4_mapped',None) and ip.ipv4_mapped.is_loopback)
        except ValueError:
            loopback = host == 'localhost'
        if loopback and port not in (8787,3080):
            return
        record={'address':[host,port],'test':self.current_test,'thread':threading.current_thread().name,
                'stack':traceback.format_stack(limit=24)[:-1]}
        with self._lock:self.network_violations.append(record)
        raise ProductionNetworkAccess(f'Test attempted production or external connection: {host}:{port}')

    def install(self):
        self.active = True
        sys.addaudithook(self._audit)
        if not getattr(sqlite3.connect, '_unit_isolation_wrapped', False):
            original = sqlite3.connect
            def connect(*args, **kwargs):
                if len(args) < 6:
                    kwargs.setdefault('factory', IsolatedConnection)
                connection = original(*args, **kwargs)
                try:
                    connection_ref = weakref.ref(connection)
                except TypeError:
                    connection_ref = lambda: None
                def authorize(action, value, _second, _database, _source):
                    if action == sqlite3.SQLITE_ATTACH:
                        value = value if value is not None else getattr(connection_ref(), '_test_attach_target', None)
                        if value is None:
                            sys.audit('quant_test.sqlite_attach_unknown')
                        else:
                            sys.audit('quant_test.sqlite_attach', value)
                    return sqlite3.SQLITE_OK
                connection.set_authorizer(authorize)
                return connection
            connect._unit_isolation_wrapped = True
            sqlite3.connect = sqlite3.dbapi2.connect = connect
        return self


class IsolatedResult(unittest.TextTestResult):
    guard = None

    def startTest(self, test):
        self.guard.current_test = test.id()
        super().startTest(test)

    def stopTest(self, test):
        super().stopTest(test)
        self.guard.current_test = None


def drain_background_checks():
    pools = set()
    for name, module in list(sys.modules.items()):
        if not name.startswith(('deck.', 'data.')) or module is None:
            continue
        for value in list(vars(module).values()):
            pool = value if isinstance(value, ThreadPoolExecutor) else getattr(value, '_executor', None)
            if isinstance(pool, ThreadPoolExecutor):
                pools.add(pool)
    for pool in pools:
        pool.shutdown(wait=True)
    deadline = time.monotonic() + 10
    for thread in threading.enumerate():
        if thread.name == 'minute-readonly-verifier':
            thread.join(timeout=max(0, deadline-time.monotonic()))
    return [thread.name for thread in threading.enumerate() if thread.name == 'minute-readonly-verifier']


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('modules', nargs='*')
    parser.add_argument('--report', default=str(ROOT/'runtime/research/test-isolation.json'))
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()
    guard = DatabaseIsolationGuard([ROOT],protect_network=True).install()
    IsolatedResult.guard = guard
    suite = (unittest.defaultTestLoader.loadTestsFromNames(args.modules) if args.modules
             else unittest.defaultTestLoader.discover(str(ROOT/'tests')))
    result = unittest.TextTestRunner(verbosity=2 if args.verbose else 1, resultclass=IsolatedResult).run(suite)
    # Wait for imported background executors while protection remains active.
    # This prevents a successful-looking exit before a leaked async read occurs.
    unfinished = drain_background_checks()
    report = {'observed_at': datetime.now(timezone.utc).isoformat(),
              'tests': result.testsRun, 'test_success': result.wasSuccessful(),
              'protected_roots': [str(root) for root in guard.roots],
              'production_database_attempts': len(guard.violations),
              'production_network_attempts': len(guard.network_violations),
              'unfinished_background_checks': unfinished,
              'violations': guard.violations,
              'network_violations': guard.network_violations,
              'passed': result.wasSuccessful() and not guard.violations and not guard.network_violations and not unfinished}
    path = Path(args.report)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2)+'\n')
    print(f"Isolation: {len(guard.violations)} production database attempts, {len(guard.network_violations)} production/external network attempts; report: {path}")
    # Keep the audit hook active until process exit, including late daemon work.
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
