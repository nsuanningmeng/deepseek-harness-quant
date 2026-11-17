from copy import deepcopy
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import Mock, patch
import os
import plistlib
import sys
import subprocess

import psutil

from data import local_services


class LoginServiceTests(TestCase):
    def setUp(self):
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.base = Path(self.directory.name)
        python = self.base / '.venv/bin/python'
        python.parent.mkdir(parents=True)
        python.touch()
        self.policy = {'label_prefix': 'local.test-quant', 'restart_delay_seconds': 30,
                       'path': '/usr/bin:/bin', 'services': {'paperlive': {
                           'enabled': True, 'command': ['{python}', '-m', 'paperlive', '--config', '{root}/config/paperlive.yaml', 'run'],
                           'pid_file': 'runtime/new-account/runner.pid',
                           'log_file': 'runtime/new-account/runner.log'}}}

    def test_login_definition_keeps_workspace_arguments_and_crash_restart(self):
        value = local_services.launch_definition('paperlive', self.policy, self.base)
        self.assertEqual(plistlib.loads(plistlib.dumps(value)), value)
        self.assertEqual(value['WorkingDirectory'], str(self.base.resolve()))
        self.assertEqual(value['KeepAlive'], {'SuccessfulExit': False})
        self.assertNotIn('sh', value['ProgramArguments'])
        self.assertEqual(value['ProgramArguments'][-1], 'paperlive')

    def test_state_paths_cannot_escape_runtime(self):
        policy = deepcopy(self.policy)
        policy['services']['paperlive']['pid_file'] = '../outside.pid'
        with self.assertRaises(ValueError):
            local_services.resolve_service('paperlive', policy, self.base)

    def test_launchd_startup_log_is_separate_from_protected_project_log(self):
        policy = deepcopy(self.policy)
        policy['bootstrap_log_directory'] = str(self.base / 'Library/Logs/quant')
        definition = local_services.launch_definition('paperlive', policy, self.base)
        self.assertEqual(definition['StandardOutPath'],
                         str(self.base / 'Library/Logs/quant/local.test-quant.paperlive.log'))
        self.assertEqual(definition['StandardErrorPath'], definition['StandardOutPath'])
        self.assertNotEqual(definition['StandardOutPath'],
                            str(local_services.resolve_service('paperlive', policy, self.base)['log_file']))

    def test_running_service_preserves_pause_and_account(self):
        service = local_services.resolve_service('paperlive', self.policy, self.base)
        service['pid_file'].parent.mkdir(parents=True)
        stop = service['pid_file'].parent / 'STOP'
        stop.write_text('user pause')
        ledger = stop.parent / 'paper.sqlite3'
        ledger.write_bytes(b'preserved ledger bytes')
        with patch.object(local_services, 'ROOT', self.base), \
             patch.object(local_services, 'load_policy', return_value=self.policy), \
             patch.object(local_services, 'resolve_service', return_value=service), \
             patch.object(os, 'chdir'), patch.object(os, 'umask'), patch.object(os, 'execve') as execute:
            local_services.run_service('paperlive')
        self.assertEqual(int(service['pid_file'].read_text()), os.getpid())
        self.assertEqual(stop.read_text(), 'user pause')
        self.assertEqual(ledger.read_bytes(), b'preserved ledger bytes')
        self.assertEqual(execute.call_args.args[1], service['command'])

    def test_disabled_service_does_not_write_or_run(self):
        service = local_services.resolve_service('paperlive', self.policy, self.base)
        service['enabled'] = False
        with patch.object(local_services, 'load_policy', return_value=self.policy), \
             patch.object(local_services, 'resolve_service', return_value=service), \
             patch.object(os, 'execve') as execute:
            local_services.run_service('paperlive')
        execute.assert_not_called()
        self.assertFalse(service['pid_file'].exists())

    def test_existing_other_workspace_is_not_overwritten(self):
        path = self.base / 'test.plist'
        value = local_services.launch_definition('paperlive', self.policy, self.base)
        path.write_bytes(plistlib.dumps(value))
        local_services.check_ownership(path, 'paperlive', self.base)
        value['WorkingDirectory'] = '/another-workspace'
        path.write_bytes(plistlib.dumps(value))
        with self.assertRaises(ValueError):
            local_services.check_ownership(path, 'paperlive', self.base)

    def test_duplicate_entry_preserves_active_pid_pause_and_ledger(self):
        service = local_services.resolve_service('paperlive', self.policy, self.base)
        service['pid_file'].parent.mkdir(parents=True)
        service['pid_file'].write_text('12345\n')
        stop = service['pid_file'].parent / 'STOP'
        ledger = stop.parent / 'paper.sqlite3'
        stop.write_bytes(b'user pause')
        ledger.write_bytes(b'account retained')
        with patch.object(local_services, 'load_policy', return_value=self.policy), \
             patch.object(local_services, 'resolve_service', return_value=service), \
             patch.object(local_services, 'running_service', return_value=Mock(pid=12345)), \
             patch.object(os, 'chdir'), patch.object(os, 'umask'), patch.object(os, 'execve') as execute:
            with self.assertRaisesRegex(RuntimeError, 'already running'):
                local_services.run_service('paperlive')
        execute.assert_not_called()
        self.assertEqual(service['pid_file'].read_text(), '12345\n')
        self.assertEqual(stop.read_bytes(), b'user pause')
        self.assertEqual(ledger.read_bytes(), b'account retained')

    def test_process_identity_requires_workspace_executable_and_every_argument(self):
        service = local_services.resolve_service('paperlive', self.policy, self.base)
        valid = {'pid': 12345, 'cmdline': service['command'], 'cwd': str(self.base),
                 'exe': service['command'][0], 'status': psutil.STATUS_RUNNING}
        unrelated = [dict(valid, cwd='/another-workspace'),
                     dict(valid, cmdline=service['command'] + ['--another-option']),
                     dict(valid, exe='/another-python'), dict(valid, status=psutil.STATUS_ZOMBIE)]
        for info in unrelated:
            with self.subTest(info=info), patch.object(psutil, 'process_iter', return_value=[Mock(info=info)]):
                self.assertIsNone(local_services.running_service(service, self.base))
        process = Mock(info=valid)
        process.is_running.return_value = True
        with patch.object(psutil, 'process_iter', return_value=[process]):
            self.assertIs(local_services.running_service(service, self.base), process)
        self.assertFalse(service['pid_file'].exists())

    def test_current_interpreter_framework_binary_is_recognized(self):
        command = [sys.executable, '-u', '-m', 'paperlive', 'run']
        actual_exe = psutil.Process(os.getpid()).exe()
        process = Mock(info={'pid': 12345, 'cmdline': [actual_exe, *command[1:]],
                             'cwd': str(self.base), 'exe': actual_exe, 'status': psutil.STATUS_RUNNING})
        process.is_running.return_value = True
        with patch.object(psutil, 'process_iter', return_value=[process]):
            self.assertIs(local_services.running_service({'command': command}, self.base), process)

    def test_entry_lock_is_exclusive_and_inherited_at_exec(self):
        from autotrade.locking import ProcessLock
        service = local_services.resolve_service('paperlive', self.policy, self.base)
        lock = local_services.entry_lock(service)

        def verify_exec(*_):
            self.assertTrue(os.get_inheritable(lock.handle.fileno()))
            with self.assertRaises(RuntimeError):
                with ProcessLock(service['pid_file'].with_name(service['pid_file'].name + '.entry.lock')):
                    self.fail('entry lock allowed a competing startup')

        with patch.object(local_services, 'load_policy', return_value=self.policy), \
             patch.object(local_services, 'resolve_service', return_value=service), \
             patch.object(local_services, 'running_service', return_value=None), \
             patch.object(local_services, 'entry_lock', return_value=lock), \
             patch.object(os, 'chdir'), patch.object(os, 'umask'), \
             patch.object(os, 'execve', side_effect=verify_exec):
            local_services.run_service('paperlive')

    def test_remove_and_status_work_without_executable_and_select_only_one(self):
        policy = deepcopy(self.policy)
        policy['services']['other'] = deepcopy(policy['services']['paperlive'])
        directory = self.base / 'Library/LaunchAgents'
        directory.mkdir(parents=True)
        path = directory / 'local.test-quant.paperlive.plist'
        path.write_bytes(plistlib.dumps(local_services.launch_definition('paperlive', policy, self.base)))
        (self.base / '.venv/bin/python').unlink()
        resolve = local_services.resolve_service
        ownership = local_services.check_ownership
        with patch.object(local_services.sys, 'platform', 'darwin'), \
             patch.object(local_services, 'load_policy', return_value=policy), \
             patch.object(local_services.Path, 'home', return_value=self.base), \
             patch.object(local_services, 'resolve_service', side_effect=lambda name, policy, *args, **kw: resolve(name, policy, self.base, **kw)), \
             patch.object(local_services, 'check_ownership', side_effect=lambda path, name: ownership(path, name, self.base)), \
             patch.object(local_services.subprocess, 'run', return_value=Mock(returncode=0)) as command:
            result = local_services.manage('status', only='paperlive')
            self.assertEqual([item['service'] for item in result], ['paperlive'])
            self.assertTrue(path.exists())
            result = local_services.manage('remove', only='paperlive')
        self.assertFalse(path.exists())
        self.assertEqual(result, [{'service': 'paperlive', 'installed': False}])
        self.assertTrue(all('other' not in ' '.join(call.args[0]) for call in command.call_args_list))

    def test_manual_harness_reuses_canonical_managed_process(self):
        import launcher
        dsh = self.base / 'harness/node_modules/@deepseek-ai/dsh/lib/bin.js'
        dsh.parent.mkdir(parents=True)
        dsh.touch()
        policy_path = self.base / 'config/local_services.yaml'
        policy_path.parent.mkdir()
        policy_path.touch()
        service = {'enabled': True, 'command': ['/node', str(dsh), 'web', '--host', '127.0.0.1', '--port', '3080'],
                   'environment': {}, 'pid_file': self.base / 'runtime/ai/harness.pid',
                   'log_file': self.base / 'runtime/ai/harness.log'}
        service['pid_file'].parent.mkdir(parents=True)
        service['pid_file'].write_text('12345\n')
        existing = Mock(pid=12345)
        with patch.object(launcher.sys, 'platform', 'darwin'), \
             patch.object(launcher, '_builtin_node', return_value='/node'), \
             patch.object(local_services, 'load_policy', return_value=self.policy), \
             patch.object(local_services, 'resolve_service', return_value=service), \
             patch.object(local_services, 'running_service', return_value=existing) as find, \
             patch.object(launcher.subprocess, 'Popen') as spawn:
            self.assertIs(launcher.start_harness(self.base), existing)
        spawn.assert_not_called()
        self.assertEqual(find.call_args.args[0]['command'], service['command'])
        self.assertEqual(service['pid_file'].read_text(), '12345\n')

    def test_loaded_identical_definition_restarts_without_bootout_race(self):
        directory = self.base / 'Library/LaunchAgents'
        directory.mkdir(parents=True)
        path = directory / 'local.test-quant.paperlive.plist'
        definition = local_services.launch_definition
        path.write_bytes(plistlib.dumps(definition('paperlive', self.policy, self.base)))
        resolve, ownership = local_services.resolve_service, local_services.check_ownership
        with patch.object(local_services.sys, 'platform', 'darwin'), \
             patch.object(local_services, 'load_policy', return_value=self.policy), \
             patch.object(local_services.Path, 'home', return_value=self.base), \
             patch.object(local_services, 'resolve_service', side_effect=lambda name, policy, *args, **kw: resolve(name, policy, self.base, **kw)), \
             patch.object(local_services, 'launch_definition', side_effect=lambda name, policy: definition(name, policy, self.base)), \
             patch.object(local_services, 'check_ownership', side_effect=lambda path, name: ownership(path, name, self.base)), \
             patch.object(local_services.subprocess, 'run', return_value=Mock(returncode=0)) as command:
            result = local_services.manage('install', only='paperlive')
        self.assertTrue(result[0]['restarted'])
        self.assertEqual([call.args[0][1] for call in command.call_args_list], ['print', 'kickstart'])
        self.assertEqual(command.call_args.args[0][2], '-k')

    def test_changed_definition_retries_asynchronous_bootout_eio(self):
        directory = self.base / 'Library/LaunchAgents'
        directory.mkdir(parents=True)
        path = directory / 'local.test-quant.paperlive.plist'
        definition = local_services.launch_definition
        path.write_bytes(plistlib.dumps(definition('paperlive', self.policy, self.base)))
        policy = deepcopy(self.policy)
        policy['restart_delay_seconds'] = 60
        resolve, ownership = local_services.resolve_service, local_services.check_ownership
        attempts = []

        def run(command, **kwargs):
            if command[1] == 'bootstrap':
                attempts.append(command)
                if len(attempts) == 1:
                    raise subprocess.CalledProcessError(5, command)
            return Mock(returncode=0)

        with patch.object(local_services.sys, 'platform', 'darwin'), \
             patch.object(local_services, 'load_policy', return_value=policy), \
             patch.object(local_services.Path, 'home', return_value=self.base), \
             patch.object(local_services, 'resolve_service', side_effect=lambda name, policy, *args, **kw: resolve(name, policy, self.base, **kw)), \
             patch.object(local_services, 'launch_definition', side_effect=lambda name, policy: definition(name, policy, self.base)), \
             patch.object(local_services, 'check_ownership', side_effect=lambda path, name: ownership(path, name, self.base)), \
             patch.object(local_services.subprocess, 'run', side_effect=run), \
             patch.object(local_services.time, 'sleep') as sleep:
            result = local_services.manage('install', only='paperlive')
        self.assertTrue(result[0]['installed'])
        self.assertEqual(len(attempts), 2)
        sleep.assert_called_once_with(0.25)
        self.assertEqual(plistlib.loads(path.read_bytes())['ThrottleInterval'], 60)
